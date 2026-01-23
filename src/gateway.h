/*
 * gateway.h - Shared declarations for pg_gateway
 */

#ifndef GATEWAY_H
#define GATEWAY_H

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

/* --- Debug Logging --- */
#define DEBUG_ENABLED 0

#define DEBUG_LOG(fmt, ...) do { \
    if (DEBUG_ENABLED) { \
        struct timespec ts; \
        clock_gettime(CLOCK_REALTIME, &ts); \
        fprintf(stderr, "[%ld.%03ld] [DEBUG] " fmt "\n", \
                ts.tv_sec, ts.tv_nsec/1000000, ##__VA_ARGS__); \
    } \
} while(0)

/* --- Tunables --- */
#define MAX_EVENTS      4096
#define SPLICE_CHUNK    (128 * 1024)     // 128 KiB per splice call
#define PIPE_CAPACITY   (1024 * 1024)    // 1 MiB pipe buffer (requires Linux 2.6.35+)

/* --- Types --- */

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
    char conninfo[1024];  // Pre-computed connection string
    PGconn *health_conn;  // Persistent connection for health checks
    target_addr_t target; // Resolved address
} candidate_t;

// Connection states
typedef enum {
    STATE_CONNECTING,
    STATE_ESTABLISHED
} conn_state_t;

// Connection structure
typedef struct {
    int client_fd;
    int backend_fd;
    int epoch_bound;
    int closed;          // 0 = open, 1 = closed (guards double-free)
    int registered;      // 1 after accept thread increments worker/metrics
    
    // Pipes
    int c2b_pipe[2]; // Client -> Backend (Write to [1], Read from [0])
    int b2c_pipe[2]; // Backend -> Client
    
    conn_state_t state;
} conn_t;

// Worker thread structure
typedef struct {
    pthread_t tid;
    int epfd;
    int wakeup_pipe[2];  // Pipe to receive new connections
    long active_connections;
    int thread_id;
} worker_thread_t;

/* --- Globals (defined in main.c) --- */
extern volatile sig_atomic_t g_running;
extern candidate_t *g_candidates;
extern size_t g_ncand;
extern int g_primary_idx;  // Index into g_candidates, or -1 if none
extern int g_epoch;
extern worker_thread_t *g_workers;

/* --- Utility Functions (main.c) --- */
void die(const char *fmt, ...);
void warnx(const char *fmt, ...);
int set_nonblock(int fd);
void set_tcp_opts(int fd);
bool resolve_addr(const char *host, const char *port, target_addr_t *out);
bool sockaddr_equal(const struct sockaddr_storage *a, socklen_t a_len,
                   const struct sockaddr_storage *b, socklen_t b_len);

/* --- Health Check Functions (health_check.c) --- */
void parse_candidates(const char *s);
void *health_thread_func(void *arg);

/* --- Metrics Functions (metrics.c) --- */
int metrics_start_server(const char *host, const char *port, pthread_t *thread_out);
void metrics_inc_active_connections(void);
void metrics_dec_active_connections(void);
void metrics_add_bytes_c2b(ssize_t delta);
void metrics_add_bytes_b2c(ssize_t delta);
void metrics_set_server_counts(int total, int healthy);

/* --- Gateway Functions (gateway.c) --- */
void send_pg_error(int fd, const char *message);
int make_pipe(int p[2]);
int pipe_bytes_available(int rfd);
int close_conn(conn_t *c); // returns 1 if closed now, 0 if already closed
int drive_connection(conn_t *c); // 0=ok, -1=client_closed, -2=backend_closed, -3=error
void update_epoll_flags(conn_t *c, int epfd);
int epoll_mod(int epfd, int fd, uint32_t events, void *ptr);

/* --- Forwarder Functions (forwarder.c) --- */
void *forwarder_thread_func(worker_thread_t *worker);

#endif /* GATEWAY_H */
