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
    
    // Pipes
    int c2b_pipe[2]; // Client -> Backend (Write to [1], Read from [0])
    int b2c_pipe[2]; // Backend -> Client
    
    conn_state_t state;
} conn_t;

/* --- Globals (defined in main.c) --- */
extern volatile sig_atomic_t g_running;
extern int g_epfd;
extern candidate_t *g_candidates;
extern size_t g_ncand;
extern target_addr_t g_current_primary;
extern pthread_rwlock_t g_primary_lock;
extern int g_epoch;

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

/* --- Gateway Functions (gateway.c) --- */
void send_pg_error(int fd, const char *message);
int make_pipe(int p[2]);
int pipe_bytes_available(int rfd);
void close_conn(conn_t *c);
int drive_connection(conn_t *c);
void update_epoll_flags(conn_t *c);

#endif /* GATEWAY_H */
