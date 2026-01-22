/*
 * main.c - Entry point and utilities for pg_gateway
 *
 * Linux-only PostgreSQL TCP load balancer using zero-copy splice().
 *
 * Build:   make all
 * Run:     
 *          $ CANDIDATES=10.0.0.10:5432,10.0.0.11:5432 \
 *            PGUSER=health PGPASSWORD=secret PGDATABASE=postgres \
 *            ./pg_gateway :: 5432
 */

#include "gateway.h"

/* --- Global Definitions --- */

volatile sig_atomic_t g_running = 1;
int g_epfd = -1;

candidate_t *g_candidates = NULL;
size_t g_ncand = 0;

target_addr_t g_current_primary = {0};
pthread_rwlock_t g_primary_lock = PTHREAD_RWLOCK_INITIALIZER;
int g_epoch = 0;

/* --- Logging & Utils --- */

void die(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap); fprintf(stderr, "\n");
    exit(1);
}

void warnx(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    fprintf(stderr, "[%ld.%03ld] ", ts.tv_sec, ts.tv_nsec/1000000);
    vfprintf(stderr, fmt, ap);
    va_end(ap); fprintf(stderr, "\n");
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void set_tcp_opts(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    
    // Optional: Keepalives
    const char *ka = getenv("TCP_KEEPALIVE");
    if (!ka || atoi(ka) != 0) {
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
#ifdef TCP_KEEPIDLE
        int idle = 60; setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
        int intvl = 10; setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
        int cnt = 3; setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
#endif
    }
}

bool resolve_addr(const char *host, const char *port, target_addr_t *out) {
    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    
    if (getaddrinfo(host, port, &hints, &res) != 0) return false;
    
    // Pick the first valid one
    if (res) {
        memcpy(&out->addr, res->ai_addr, res->ai_addrlen);
        out->addr_len = res->ai_addrlen;
        snprintf(out->host_str, sizeof(out->host_str), "%s:%s", host, port);
        out->valid = true;
    }
    
    freeaddrinfo(res);
    return out->valid;
}

bool sockaddr_equal(const struct sockaddr_storage *a, socklen_t a_len,
                   const struct sockaddr_storage *b, socklen_t b_len) {
    if (a_len != b_len) return false;
    if (a->ss_family != b->ss_family) return false;
    
    if (a->ss_family == AF_INET) {
        const struct sockaddr_in *a4 = (const struct sockaddr_in *)a;
        const struct sockaddr_in *b4 = (const struct sockaddr_in *)b;
        return (a4->sin_port == b4->sin_port) &&
               (a4->sin_addr.s_addr == b4->sin_addr.s_addr);
    } else if (a->ss_family == AF_INET6) {
        const struct sockaddr_in6 *a6 = (const struct sockaddr_in6 *)a;
        const struct sockaddr_in6 *b6 = (const struct sockaddr_in6 *)b;
        return (a6->sin6_port == b6->sin6_port) &&
               (memcmp(&a6->sin6_addr, &b6->sin6_addr, sizeof(struct in6_addr)) == 0);
    }
    
    // Fallback for other families
    return memcmp(a, b, a_len) == 0;
}

/* --- Signal Handler --- */

static void hup_handler(int sig) { (void)sig; g_running = 0; }

/* --- Main --- */

int main(int argc, char **argv) {
    if (argc != 3) die("Usage: %s <listen_addr> <listen_port>", argv[0]);
    
    parse_candidates(getenv("CANDIDATES"));
    
    struct sigaction sa = {0};
    sa.sa_handler = hup_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN); // Important for splice/sockets

    // Start Health Thread
    pthread_t ht;
    if (pthread_create(&ht, NULL, health_thread_func, NULL) != 0) die("pthread_create failed");

    // Setup Listener with IPv6 support
    struct addrinfo hints = {0}, *res = NULL, *rp = NULL;
    hints.ai_family = AF_UNSPEC;     // Allow IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;     // For wildcard IP address
    
    if (getaddrinfo(argv[1], argv[2], &hints, &res) != 0) {
        die("getaddrinfo failed for %s:%s", argv[1], argv[2]);
    }
    
    int lfd = -1;
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        lfd = socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, rp->ai_protocol);
        if (lfd < 0) continue;
        
        int one = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
        setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
        
        // For IPv6, ensure we can handle both IPv4 and IPv6
        if (rp->ai_family == AF_INET6) {
            int off = 0;
            setsockopt(lfd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        }
        
        if (bind(lfd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break; // Success
        }
        
        close(lfd);
        lfd = -1;
    }
    
    freeaddrinfo(res);
    
    if (lfd < 0) die("Failed to bind to %s:%s", argv[1], argv[2]);
    if (listen(lfd, 4096) < 0) die("listen failed");

    g_epfd = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event ev_list = {.events = EPOLLIN, .data.ptr = NULL}; // NULL ptr marks listener
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, lfd, &ev_list);

    struct epoll_event *events = calloc(MAX_EVENTS, sizeof(struct epoll_event));
    warnx("LB started on %s:%s", argv[1], argv[2]);

    while (g_running) {
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait failed");
        }

        // Current epoch snapshot for this batch
        int cur_epoch = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);

        for (int i = 0; i < n; i++) {
            conn_t *c = (conn_t*)events[i].data.ptr;

            // 1. LISTENER EVENT
            if (c == NULL) { 
                while (1) {
                    struct sockaddr_storage ss; socklen_t slen = sizeof(ss);
                    int cfd = accept4(lfd, (struct sockaddr*)&ss, &slen, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (cfd < 0) break; 
                    set_tcp_opts(cfd);

                    // Fetch Primary (Non-blocking access)
                    target_addr_t target;
                    pthread_rwlock_rdlock(&g_primary_lock);
                    target = g_current_primary;
                    pthread_rwlock_unlock(&g_primary_lock);

                    if (!target.valid) {
                        // Send PostgreSQL error message instead of silent drop
                        send_pg_error(cfd, "no healthy PostgreSQL primary available");
                        close(cfd);
                        continue;
                    }

                    // Connect to backend
                    int bfd = socket(target.addr.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                    if (bfd < 0) { close(cfd); continue; }
                    set_tcp_opts(bfd);

                    int rc = connect(bfd, (struct sockaddr*)&target.addr, target.addr_len);
                    if (rc < 0 && errno != EINPROGRESS) {
                        close(cfd); close(bfd); continue;
                    }

                    // Alloc Connection
                    conn_t *nc = calloc(1, sizeof(conn_t));
                    if (!nc) { close(cfd); close(bfd); continue; }
                    
                    nc->client_fd = cfd;
                    nc->backend_fd = bfd;
                    nc->epoch_bound = cur_epoch;
                    nc->state = (rc == 0) ? STATE_ESTABLISHED : STATE_CONNECTING;
                    
                    if (make_pipe(nc->c2b_pipe) < 0 || make_pipe(nc->b2c_pipe) < 0) {
                        close_conn(nc); continue;
                    }

                    // Register Epoll
                    struct epoll_event ev = {.events = EPOLLIN | EPOLLET | EPOLLRDHUP, .data.ptr = nc};
                    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, cfd, &ev) < 0) { close_conn(nc); continue; }
                    
                    // For backend: If connecting, need EPOLLOUT. If established, EPOLLIN.
                    uint32_t be_ev = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    if (nc->state == STATE_CONNECTING) be_ev |= EPOLLOUT;
                    
                    ev.events = be_ev;
                    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, bfd, &ev) < 0) { close_conn(nc); continue; }
                }
                continue;
            }

            // 2. CONNECTION EVENT
            
            // A. Check Epoch (Kill stale connections)
            if (c->epoch_bound != cur_epoch) {
                // Must purge future events for this ptr to avoid Double-Free
                for (int j = i + 1; j < n; j++) if (events[j].data.ptr == c) events[j].data.ptr = NULL;
                close_conn(c);
                continue;
            }

            // B. Drive IO
            if (drive_connection(c) < 0) {
                // Connection died or error
                for (int j = i + 1; j < n; j++) if (events[j].data.ptr == c) events[j].data.ptr = NULL;
                close_conn(c);
                continue;
            }

            // C. Re-arm Epoll flags (handle partial writes/buffers)
            update_epoll_flags(c);
        }
    }

    warnx("Shutting down...");
    close(lfd);
    pthread_cancel(ht);
    pthread_join(ht, NULL);
    close(g_epfd);
    free(events);
    free(g_candidates);
    return 0;
}
