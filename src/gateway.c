/*
 * gateway.c - Connection proxying and splice-based data transfer
 */

#include "gateway.h"

/* --- Pipe & Splice Helpers --- */

int make_pipe(int p[2]) {
    if (pipe2(p, O_NONBLOCK | O_CLOEXEC) < 0) return -1;
    
    // Optimization: Increase pipe size to reduce context switches
    fcntl(p[0], F_SETPIPE_SZ, PIPE_CAPACITY);
    fcntl(p[1], F_SETPIPE_SZ, PIPE_CAPACITY);
    return 0;
}

int pipe_bytes_available(int rfd) {
    int bytes = 0;
    if (ioctl(rfd, FIONREAD, &bytes) < 0) return 0;
    return bytes;
}

/* --- PostgreSQL Protocol Error Packet --- */

void send_pg_error(int fd, const char *message) {
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

/* --- Connection Logic --- */

void close_conn(conn_t *c) {
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