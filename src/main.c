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

candidate_t *g_candidates = NULL;
size_t g_ncand = 0;

int g_primary_idx = -1;  // Index into g_candidates, or -1 if none
int g_epoch = 0;
worker_thread_t *g_workers = NULL;

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
    const char *listen_addr;
    const char *listen_port;
    
    if (argc >= 3) {
        listen_addr = argv[1];
        listen_port = argv[2];
    } else if (argc == 1) {
        // Read from environment variables
        listen_addr = getenv("LISTEN_HOST");
        listen_port = getenv("LISTEN_PORT");
        if (!listen_addr) listen_addr = "localhost";
        if (!listen_port) listen_port = "5432";
    } else {
        die("Usage: %s [<listen_addr> <listen_port>]\n"
            "       Or set LISTEN_HOST and LISTEN_PORT environment variables", argv[0]);
    }
    
    parse_candidates(getenv("CANDIDATES"));
    
    // Parse number of worker threads
    int g_num_workers = 1;
    const char *num_threads_env = getenv("NUM_THREADS");
    if (num_threads_env) g_num_workers = atoi(num_threads_env);
    if (g_num_workers < 1) g_num_workers = 1;
    if (g_num_workers > 64) g_num_workers = 64;
    
    struct sigaction sa = {0};
    sa.sa_handler = hup_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN); // Important for splice/sockets

    // Start Health Thread
    pthread_t ht;
    if (pthread_create(&ht, NULL, health_thread_func, NULL) != 0) die("pthread_create failed");

    // Start Metrics Server
    pthread_t metrics_thread;
    const char *metrics_host = getenv("METRICS_HOST");
    const char *metrics_port = getenv("METRICS_PORT");
    if (!metrics_host) metrics_host = "::";
    if (!metrics_port) metrics_port = "9090";
    metrics_start_server(metrics_host, metrics_port, &metrics_thread);
    
    // Create Worker Threads
    g_workers = calloc(g_num_workers, sizeof(worker_thread_t));
    for (int i = 0; i < g_num_workers; i++) {
        worker_thread_t *w = &g_workers[i];
        w->thread_id = i;
        w->epfd = epoll_create1(EPOLL_CLOEXEC);
        if (w->epfd < 0) die("epoll_create1 failed for worker %d: %s", i, strerror(errno));
        w->active_connections = 0;
        
        if (pipe2(w->wakeup_pipe, O_NONBLOCK | O_CLOEXEC) < 0) die("pipe2 failed");
        
        // Add wakeup pipe to worker epoll
        struct epoll_event ev = {.events = EPOLLIN, .data.ptr = NULL};
        if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->wakeup_pipe[0], &ev) < 0) {
            die("epoll_ctl add wakeup failed for worker %d: %s", i, strerror(errno));
        }
        
        if (pthread_create(&w->tid, NULL, (void *(*)(void *))forwarder_thread_func, w) != 0) {
            die("pthread_create failed for worker %d", i);
        }
    }
    
    warnx("Started %d worker threads", g_num_workers);

    // Setup Listener with IPv6 support
    struct addrinfo hints = {0}, *res = NULL, *rp = NULL;
    hints.ai_family = AF_UNSPEC;     // Allow IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;     // For wildcard IP address
    
    int gai = getaddrinfo(listen_addr, listen_port, &hints, &res);
    if (gai != 0 && strcmp(listen_addr, "::") == 0) {
        // Fallback when IPv6 is disabled: try IPv4 wildcard
        warnx("IPv6 unavailable; falling back to 0.0.0.0:%s", listen_port);
        listen_addr = "0.0.0.0";
        gai = getaddrinfo(listen_addr, listen_port, &hints, &res);
    }
    if (gai != 0) {
        die("getaddrinfo failed for %s:%s", listen_addr, listen_port);
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
    
    if (lfd < 0) die("Failed to bind to %s:%s", listen_addr, listen_port);
    if (listen(lfd, 4096) < 0) die("listen failed");

    warnx("LB started on %s:%s", listen_addr, listen_port);

    // Accept loop: dispatch to least-loaded worker
    while (g_running) {
        struct sockaddr_storage ss;
        socklen_t slen = sizeof(ss);
        int cfd = accept4(lfd, (struct sockaddr*)&ss, &slen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000);
                continue;
            }
            break;
        }
        set_tcp_opts(cfd);
        
        // Fetch Primary
        int primary_idx = __atomic_load_n(&g_primary_idx, __ATOMIC_RELAXED);
        int cur_epoch = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);
        
        if (primary_idx < 0 || primary_idx >= (int)g_ncand) {
            send_pg_error(cfd, "no healthy PostgreSQL primary available");
            close(cfd);
            continue;
        }
        
        target_addr_t target = g_candidates[primary_idx].target;
        
        // Connect to backend
        int bfd = socket(target.addr.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (bfd < 0) { close(cfd); continue; }
        set_tcp_opts(bfd);
        
        int rc = connect(bfd, (struct sockaddr*)&target.addr, target.addr_len);
        if (rc < 0 && errno != EINPROGRESS) {
            close(cfd); close(bfd); continue;
        }
        
        // Alloc Connection - initialize FDs to -1 so close_conn knows what to close
        conn_t *nc = calloc(1, sizeof(conn_t));
        if (!nc) { close(cfd); close(bfd); continue; }
        
        nc->client_fd = cfd;
        nc->backend_fd = bfd;
        nc->c2b_pipe[0] = nc->c2b_pipe[1] = -1;  // Mark as uninitialized
        nc->b2c_pipe[0] = nc->b2c_pipe[1] = -1;  // Mark as uninitialized
        nc->epoch_bound = cur_epoch;
        nc->state = (rc == 0) ? STATE_ESTABLISHED : STATE_CONNECTING;
        
        if (make_pipe(nc->c2b_pipe) < 0 || make_pipe(nc->b2c_pipe) < 0) {
            close_conn(nc); continue;
        }
        
        // Find least-loaded worker
        int target_worker_index = 0;
        long best_load = __atomic_load_n(&g_workers[0].active_connections, __ATOMIC_RELAXED);
        for (int i = 1; i < g_num_workers; i++) {
            long load = __atomic_load_n(&g_workers[i].active_connections, __ATOMIC_RELAXED);
            if (load < best_load) {
                best_load = load;
                target_worker_index = i;
            }
        }

        worker_thread_t *target_worker = &g_workers[target_worker_index];
        
        // Register both FDs in worker's epoll with same connection pointer
        // drive_connection handles both directions so we don't need to distinguish
        struct epoll_event ev = {.events = EPOLLIN | EPOLLET | EPOLLRDHUP, .data.ptr = nc};
        if (epoll_ctl(target_worker->epfd, EPOLL_CTL_ADD, cfd, &ev) < 0) {
            close_conn(nc);
            continue;
        }
        
        uint32_t be_ev = EPOLLIN | EPOLLET | EPOLLRDHUP;
        if (nc->state == STATE_CONNECTING) be_ev |= EPOLLOUT;
        ev.events = be_ev;
        ev.data.ptr = nc;
        if (epoll_ctl(target_worker->epfd, EPOLL_CTL_ADD, bfd, &ev) < 0) {
            epoll_ctl(target_worker->epfd, EPOLL_CTL_DEL, cfd, NULL);
            close_conn(nc);
            continue;
        }
        
        __atomic_fetch_add(&target_worker->active_connections, 1, __ATOMIC_RELAXED);
        metrics_inc_active_connections();
        
        // Wake up worker
        char wake = 1;
        write(target_worker->wakeup_pipe[1], &wake, 1);
    }

    warnx("Shutting down...");
    close(lfd);
    
    // Gracefully stop workers by setting g_running = 0 and waiting for them to exit
    g_running = 0;
    
    // Give workers time to exit gracefully
    for (int i = 0; i < g_num_workers; i++) {
        // Wake up each worker so it doesn't block on epoll_wait
        char wake = 1;
        write(g_workers[i].wakeup_pipe[1], &wake, 1);
    }
    
    // Join worker threads
    for (int i = 0; i < g_num_workers; i++) {
        pthread_join(g_workers[i].tid, NULL);
        close(g_workers[i].epfd);
        close(g_workers[i].wakeup_pipe[0]);
        close(g_workers[i].wakeup_pipe[1]);
    }
    free(g_workers);
    
    pthread_join(ht, NULL);
    free(g_candidates);
    return 0;
}
