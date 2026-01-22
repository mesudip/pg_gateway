/*
 * pg_lb.c (Optimized Splice Edition)
 * Linux-only PostgreSQL TCP load balancer using zero-copy splice().
 *
 * improvements:
 * - Proper non-blocking connect() state machine.
 * - IPv6 support
 * - PostgreSQL error packets for unavailable primary
 *
 * Build:   gcc -O3 -pthread -Wall -Wextra -o pg_lb pg_lb.c -lpq
 * Run:     
 *    $ CANDIDATES=10.0.0.10:5432,10.0.0.11:5432 \
 *      PGUSER=health PGPASSWORD=secret PGDATABASE=postgres \
 *      ./pg_lb :: 5432
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <libpq-fe.h>

/* --- Tunables --- */
#define MAX_EVENTS      4096
#define SPLICE_CHUNK    (128 * 1024)     // 128 KiB per splice call
#define PIPE_CAPACITY   (1024 * 1024)    // 1 MiB pipe buffer (requires Linux 2.6.35+)

/* --- Globals & Types --- */

// Represents a resolved target address
typedef struct {
    struct sockaddr_storage addr;
    socklen_t addr_len;
    char host_str[256]; // For logging
    bool valid;
} target_addr_t;

// Candidate config string parsing
typedef struct {
    char host[256];
    char port[16];
} candidate_t;

static volatile sig_atomic_t g_running = 1;
static int g_epfd = -1;

// Shared State
static candidate_t *g_candidates = NULL;
static size_t g_ncand = 0;

static target_addr_t g_current_primary = {0};
static pthread_rwlock_t g_primary_lock = PTHREAD_RWLOCK_INITIALIZER;
static int g_epoch = 0; // Accessed via __atomic builtins

/* --- Logging & Utils --- */

static void die(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap); fprintf(stderr, "\n");
    exit(1);
}

static void warnx(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    fprintf(stderr, "[%ld.%03ld] ", ts.tv_sec, ts.tv_nsec/1000000);
    vfprintf(stderr, fmt, ap);
    va_end(ap); fprintf(stderr, "\n");
}

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_tcp_opts(int fd) {
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

/* --- Pipe & Splice Helpers --- */

// Creates a pipe with O_NONBLOCK and expanded buffer size
static int make_pipe(int p[2]) {
    if (pipe2(p, O_NONBLOCK | O_CLOEXEC) < 0) return -1;
    
    // Optimization: Increase pipe size to reduce context switches
    fcntl(p[0], F_SETPIPE_SZ, PIPE_CAPACITY);
    fcntl(p[1], F_SETPIPE_SZ, PIPE_CAPACITY);
    return 0;
}

static int pipe_bytes_available(int rfd) {
    int bytes = 0;
    if (ioctl(rfd, FIONREAD, &bytes) < 0) return 0;
    return bytes;
}

// Helper: Resolve hostname:port to sockaddr (blocking, used in thread)
static bool resolve_addr(const char *host, const char *port, target_addr_t *out) {
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

// Compare two sockaddr structures properly (IPv4/IPv6 aware)
static bool sockaddr_equal(const struct sockaddr_storage *a, socklen_t a_len,
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

/* --- PostgreSQL Protocol Error Packet --- */

// Send a PostgreSQL ErrorResponse packet to client
static void send_pg_error(int fd, const char *message) {
    /*
     * PostgreSQL ErrorResponse format:
     * 'E' (1 byte) - message type
     * int32 - message length (including self but not type byte)
     * field_type (1 byte) + string + '\0' (repeated)
     * '\0' - final terminator
     * 
     * Common field types:
     * 'S' - Severity
     * 'C' - SQLSTATE code
     * 'M' - Message
     */
    
    const char *severity = "FATAL";
    const char *sqlstate = "08006"; // connection_failure
    
    // Calculate total message length
    size_t msg_len = 0;
    msg_len += 1 + strlen(severity) + 1;      // 'S' + severity + '\0'
    msg_len += 1 + strlen(sqlstate) + 1;      // 'C' + sqlstate + '\0'
    msg_len += 1 + strlen(message) + 1;       // 'M' + message + '\0'
    msg_len += 1;                             // final '\0'
    
    size_t total_len = 4 + msg_len; // int32 length + fields
    
    unsigned char *buf = malloc(1 + total_len);
    if (!buf) return;
    
    unsigned char *p = buf;
    
    // Message type
    *p++ = 'E';
    
    // Message length (big-endian int32)
    uint32_t len_be = htonl(total_len);
    memcpy(p, &len_be, 4);
    p += 4;
    
    // Severity
    *p++ = 'S';
    strcpy((char *)p, severity);
    p += strlen(severity) + 1;
    
    // SQLSTATE
    *p++ = 'C';
    strcpy((char *)p, sqlstate);
    p += strlen(sqlstate) + 1;
    
    // Message
    *p++ = 'M';
    strcpy((char *)p, message);
    p += strlen(message) + 1;
    
    // Final terminator
    *p++ = '\0';
    
    // Send (ignore errors, best effort)
    ssize_t total = 1 + total_len;
    ssize_t sent = 0;
    while (sent < total) {
        ssize_t n = write(fd, buf + sent, total - sent);
        if (n <= 0) break;
        sent += n;
    }
    
    free(buf);
}

/* --- Health Check Thread --- */

static void parse_candidates(const char *s) {
    if (!s || !*s) die("CANDIDATES env var required");
    char *dup = strdup(s);
    size_t count = 1;
    for (char *p = dup; *p; ++p) if (*p == ',') ++count;
    
    g_candidates = calloc(count, sizeof(candidate_t));
    g_ncand = 0;
    
    char *save = NULL;
    char *tok = strtok_r(dup, ",", &save);
    while (tok) {
        while (*tok == ' ') ++tok; // Trim leading space
        char *c = strchr(tok, ':');
        if (!c) die("Invalid candidate format '%s' (expected host:port)", tok);
        *c = '\0';
        snprintf(g_candidates[g_ncand].host, sizeof(g_candidates[g_ncand].host), "%s", tok);
        snprintf(g_candidates[g_ncand].port, sizeof(g_candidates[g_ncand].port), "%s", c + 1);
        g_ncand++;
        tok = strtok_r(NULL, ",", &save);
    }
    free(dup);
}

static bool check_postgres_primary(const char *host, const char *port, int cto_ms, int qto_ms) {
    char conninfo[1024];
    const char *dbname = getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres";
    snprintf(conninfo, sizeof(conninfo), "host=%s port=%s connect_timeout=%d dbname=%s",
             host, port, cto_ms / 1000, dbname);
             
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return false;
    }

    // Set statement timeout
    char setto[128];
    snprintf(setto, sizeof(setto), "SET LOCAL statement_timeout=%d;", qto_ms);
    PGresult *r1 = PQexec(conn, setto);
    if (PQresultStatus(r1) != PGRES_COMMAND_OK) {
        PQclear(r1); PQfinish(conn); return false;
    }
    PQclear(r1);

    // Check read-only status
    PGresult *res = PQexec(conn, "SHOW transaction_read_only;");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res); PQfinish(conn); return false;
    }
    
    char *val = PQgetvalue(res, 0, 0);
    bool is_primary = (val && strcasecmp(val, "off") == 0);
    
    PQclear(res);
    PQfinish(conn);
    return is_primary;
}

// Cleanup handler to release lock if thread is cancelled
static void cleanup_rwlock(void *arg) {
    pthread_rwlock_t *lock = (pthread_rwlock_t *)arg;
    pthread_rwlock_unlock(lock);
}

static void *health_thread_func(void *arg) {
    (void)arg;
    int check_int = getenv("CHECK_EVERY") ? atoi(getenv("CHECK_EVERY")) : 2;
    int cto = getenv("CONNECT_TIMEOUT_MS") ? atoi(getenv("CONNECT_TIMEOUT_MS")) : 800;
    int qto = getenv("QUERY_TIMEOUT_MS") ? atoi(getenv("QUERY_TIMEOUT_MS")) : 500;

    char last_host[256] = {0};

    while (g_running) {
        candidate_t found_cand = {0};
        bool ok = false;

        // 1. Scan candidates for a primary
        for (size_t i = 0; i < g_ncand; i++) {
            if (check_postgres_primary(g_candidates[i].host, g_candidates[i].port, cto, qto)) {
                found_cand = g_candidates[i];
                ok = true;
                break;
            }
        }

        // 2. Resolve DNS (off the main loop)
        target_addr_t new_target = {0};
        if (ok) {
            if (!resolve_addr(found_cand.host, found_cand.port, &new_target)) {
                warnx("[health] Found primary %s:%s but DNS resolution failed", found_cand.host, found_cand.port);
                ok = false;
            }
        }

        // 3. Update global state with proper cleanup handler
        bool changed = false;
        
        pthread_rwlock_wrlock(&g_primary_lock);
        pthread_cleanup_push(cleanup_rwlock, &g_primary_lock);
        
        if (ok) {
            // Compare IP/Port properly using sockaddr_equal
            if (!g_current_primary.valid || 
                !sockaddr_equal(&g_current_primary.addr, g_current_primary.addr_len,
                               &new_target.addr, new_target.addr_len)) {
                
                g_current_primary = new_target;
                __atomic_fetch_add(&g_epoch, 1, __ATOMIC_RELAXED);
                changed = true;
            }
        } else {
            // Lost primary
            if (g_current_primary.valid) {
                memset(&g_current_primary, 0, sizeof(g_current_primary));
                __atomic_fetch_add(&g_epoch, 1, __ATOMIC_RELAXED);
                changed = true;
            }
        }
        
        pthread_cleanup_pop(1); // Unlock

        if (changed) {
            int e = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);
            if (ok) {
                warnx("[health] NEW PRIMARY: %s (Epoch %d)", new_target.host_str, e);
                strncpy(last_host, new_target.host_str, sizeof(last_host));
            } else {
                warnx("[health] NO PRIMARY REACHABLE (Epoch %d)", e);
            }
        }

        sleep(check_int);
    }
    return NULL;
}

/* --- Connection Logic --- */

typedef enum {
    STATE_CONNECTING,
    STATE_ESTABLISHED
} conn_state_t;

typedef struct {
    int client_fd;
    int backend_fd;
    int epoch_bound;
    
    // Pipes
    int c2b_pipe[2]; // Client -> Backend (Write to [1], Read from [0])
    int b2c_pipe[2]; // Backend -> Client
    
    conn_state_t state;
} conn_t;

static void close_conn(conn_t *c) {
    if (!c) return;
    
    // Explicitly remove from epoll before closing
    if (g_epfd >= 0) {
        if (c->client_fd >= 0) epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
        if (c->backend_fd >= 0) epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
    }
    
    if (c->client_fd >= 0) close(c->client_fd);
    if (c->backend_fd >= 0) close(c->backend_fd);
    if (c->c2b_pipe[0] >= 0) { close(c->c2b_pipe[0]); close(c->c2b_pipe[1]); }
    if (c->b2c_pipe[0] >= 0) { close(c->b2c_pipe[0]); close(c->b2c_pipe[1]); }
    free(c);
}

// Attempts to move data from 'from_fd' -> 'to_pipe_w'
static ssize_t splice_in(int from_fd, int to_pipe_w) {
    ssize_t total = 0;
    while (1) {
        ssize_t n = splice(from_fd, NULL, to_pipe_w, NULL, SPLICE_CHUNK, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return -1; // Error
        }
        if (n == 0) return 0; // EOF
        total += n;
        if (n < SPLICE_CHUNK) break; // Pipe likely full or socket empty
    }
    return total > 0 ? total : -2; // -2 indicates "no data processed but no error"
}

// Attempts to move data from 'from_pipe_r' -> 'to_fd'
static int splice_out(int from_pipe_r, int to_fd) {
    while (1) {
        ssize_t n = splice(from_pipe_r, NULL, to_fd, NULL, SPLICE_CHUNK, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return -1;
        }
        if (n == 0) break; // Pipe empty
    }
    return 0;
}

static int epoll_mod(int fd, uint32_t events, void *ptr) {
    struct epoll_event ev = {.events = events, .data.ptr = ptr};
    return epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev);
}

// Main state machine driver for a connection
static int drive_connection(conn_t *c) {
    
    // 1. Handle Connection Establishment
    if (c->state == STATE_CONNECTING) {
        int err = 0; 
        socklen_t len = sizeof(err);
        if (getsockopt(c->backend_fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
            return -1; // Connect failed
        }
        c->state = STATE_ESTABLISHED;
        // Fall through to process data immediately
    }

    // 2. Data Flow
    
    // A. Client -> Backend
    // Only read from client if backend pipe has space (implicit in splice)
    // Only write to backend if we have data in pipe
    
    ssize_t r = splice_in(c->client_fd, c->c2b_pipe[1]);
    if (r == 0) return -1; // Client EOF
    if (r == -1) return -1; // Error
    
    if (splice_out(c->c2b_pipe[0], c->backend_fd) < 0) return -1;

    // B. Backend -> Client
    r = splice_in(c->backend_fd, c->b2c_pipe[1]);
    if (r == 0) return -1; // Backend EOF
    if (r == -1) return -1;

    if (splice_out(c->b2c_pipe[0], c->client_fd) < 0) return -1;

    return 0;
}

static void update_epoll_flags(conn_t *c) {
    uint32_t ev_cli = EPOLLIN | EPOLLET | EPOLLRDHUP;
    uint32_t ev_be  = EPOLLIN | EPOLLET | EPOLLRDHUP;

    // If we are connecting, we MUST watch for EPOLLOUT on backend
    if (c->state == STATE_CONNECTING) {
        ev_be |= EPOLLOUT;
    } else {
        // If pipe has data destined for Client, watch Client EPOLLOUT
        if (pipe_bytes_available(c->b2c_pipe[0]) > 0) ev_cli |= EPOLLOUT;
        
        // If pipe has data destined for Backend, watch Backend EPOLLOUT
        if (pipe_bytes_available(c->c2b_pipe[0]) > 0) ev_be |= EPOLLOUT;
    }

    epoll_mod(c->client_fd, ev_cli, c);
    epoll_mod(c->backend_fd, ev_be, c);
}

static void hup_handler(int sig) { (void)sig; g_running = 0; }

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