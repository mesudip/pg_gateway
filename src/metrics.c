/*
 * metrics.c - Prometheus metrics endpoint for pg_gateway
 */

#include "gateway.h"

/* --- Metrics Counters (atomic for thread safety) --- */

static _Atomic long g_active_connections = 0;
static _Atomic long g_total_connections = 0;
static _Atomic long g_bytes_client_to_backend = 0;
static _Atomic long g_bytes_backend_to_client = 0;
static _Atomic int g_servers_total = 0;
static _Atomic int g_servers_healthy = 0;

/* --- Metrics Update Functions --- */

void metrics_inc_active_connections(void) {
    __atomic_add_fetch(&g_active_connections, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&g_total_connections, 1, __ATOMIC_RELAXED);
}

void metrics_dec_active_connections(void) {
    __atomic_sub_fetch(&g_active_connections, 1, __ATOMIC_RELAXED);
}

void metrics_add_bytes_c2b(ssize_t delta) {
    if (delta > 0) __atomic_add_fetch(&g_bytes_client_to_backend, delta, __ATOMIC_RELAXED);
}

void metrics_add_bytes_b2c(ssize_t delta) {
    if (delta > 0) __atomic_add_fetch(&g_bytes_backend_to_client, delta, __ATOMIC_RELAXED);
}

void metrics_set_server_counts(int total, int healthy) {
    __atomic_store_n(&g_servers_total, total, __ATOMIC_RELAXED);
    __atomic_store_n(&g_servers_healthy, healthy, __ATOMIC_RELAXED);
}

/* --- HTTP Response Helpers --- */

static void send_http_response(int fd, const char *status, const char *content_type, const char *body) {
    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        status, content_type, strlen(body));
    
    write(fd, header, hlen);
    write(fd, body, strlen(body));
}

static void handle_metrics_request(int fd) {
    // Use atomic loads for thread-safe reading
    long active = __atomic_load_n(&g_active_connections, __ATOMIC_RELAXED);
    long total_conn = __atomic_load_n(&g_total_connections, __ATOMIC_RELAXED);
    long bytes_c2b = __atomic_load_n(&g_bytes_client_to_backend, __ATOMIC_RELAXED);
    long bytes_b2c = __atomic_load_n(&g_bytes_backend_to_client, __ATOMIC_RELAXED);
    int servers_total = __atomic_load_n(&g_servers_total, __ATOMIC_RELAXED);
    int servers_healthy = __atomic_load_n(&g_servers_healthy, __ATOMIC_RELAXED);
    
    int servers_unhealthy = servers_total - servers_healthy;
    
    char body[4096];
    snprintf(body, sizeof(body),
        "# HELP pg_gateway_connections_active Current number of active connections\n"
        "# TYPE pg_gateway_connections_active gauge\n"
        "pg_gateway_connections_active %ld\n"
        "\n"
        "# HELP pg_gateway_connections_total Total number of connections since start\n"
        "# TYPE pg_gateway_connections_total counter\n"
        "pg_gateway_connections_total %ld\n"
        "\n"
        "# HELP pg_gateway_bytes_client_to_backend_total Total bytes transferred from clients to backend\n"
        "# TYPE pg_gateway_bytes_client_to_backend_total counter\n"
        "pg_gateway_bytes_client_to_backend_total %ld\n"
        "\n"
        "# HELP pg_gateway_bytes_backend_to_client_total Total bytes transferred from backend to clients\n"
        "# TYPE pg_gateway_bytes_backend_to_client_total counter\n"
        "pg_gateway_bytes_backend_to_client_total %ld\n"
        "\n"
        "# HELP pg_gateway_servers_total Total number of configured backend servers\n"
        "# TYPE pg_gateway_servers_total gauge\n"
        "pg_gateway_servers_total %d\n"
        "\n"
        "# HELP pg_gateway_servers_healthy Number of healthy backend servers\n"
        "# TYPE pg_gateway_servers_healthy gauge\n"
        "pg_gateway_servers_healthy %d\n"
        "\n"
        "# HELP pg_gateway_servers_unhealthy Number of unhealthy backend servers\n"
        "# TYPE pg_gateway_servers_unhealthy gauge\n"
        "pg_gateway_servers_unhealthy %d\n",
        active, total_conn, bytes_c2b, bytes_b2c,
        servers_total, servers_healthy, servers_unhealthy);
    
    send_http_response(fd, "200 OK", "text/plain; version=0.0.4; charset=utf-8", body);
}

/* --- Metrics Server Thread --- */

static void *metrics_server_thread(void *arg) {
    int lfd = *(int *)arg;
    free(arg);
    
    while (g_running) {
        struct sockaddr_storage ss;
        socklen_t slen = sizeof(ss);
        int cfd = accept(lfd, (struct sockaddr *)&ss, &slen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            break;
        }
        
        // Read HTTP request (simple: just check for GET /metrics)
        char buf[1024];
        ssize_t n = read(cfd, buf, sizeof(buf) - 1);
        if (n > 0) {
            buf[n] = '\0';
            if (strncmp(buf, "GET /metrics", 12) == 0 || strncmp(buf, "GET / ", 6) == 0) {
                handle_metrics_request(cfd);
            } else {
                send_http_response(cfd, "404 Not Found", "text/plain", "Not Found\n");
            }
        }
        
        close(cfd);
    }
    
    close(lfd);
    return NULL;
}

/* --- Start Metrics Server --- */

int metrics_start_server(const char *host, const char *port, pthread_t *thread_out) {
    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    
    if (getaddrinfo(host, port, &hints, &res) != 0) {
        warnx("[metrics] getaddrinfo failed for %s:%s", host, port);
        return -1;
    }
    
    int lfd = -1;
    for (struct addrinfo *rp = res; rp != NULL; rp = rp->ai_next) {
        lfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (lfd < 0) continue;
        
        int one = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        
        if (rp->ai_family == AF_INET6) {
            int off = 0;
            setsockopt(lfd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        }
        
        if (bind(lfd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        
        close(lfd);
        lfd = -1;
    }
    freeaddrinfo(res);
    
    if (lfd < 0) {
        warnx("[metrics] Failed to bind to %s:%s", host, port);
        return -1;
    }
    
    if (listen(lfd, 16) < 0) {
        warnx("[metrics] listen failed");
        close(lfd);
        return -1;
    }
    
    int *lfd_ptr = malloc(sizeof(int));
    *lfd_ptr = lfd;
    
    if (pthread_create(thread_out, NULL, metrics_server_thread, lfd_ptr) != 0) {
        warnx("[metrics] pthread_create failed");
        close(lfd);
        free(lfd_ptr);
        return -1;
    }
    
    warnx("[metrics] Prometheus endpoint started on %s:%s", host, port);
    return 0;
}
