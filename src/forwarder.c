/*
 * forwarder.c - Connection forwarding logic for worker threads
 *
 * This module implements the core data forwarding loop for client-backend
 * connections. It runs in worker threads, using epoll to monitor sockets
 * and perform non-blocking I/O. It handles bidirectional data transfer
 * ensuring low latency and high throughput for the PostgreSQL protocol.
 */

#include "gateway.h"

// Helper to invalidate remaining events for a connection being closed
static void invalidate_pending_events(struct epoll_event *events, int start, int n, conn_t *c) {
    for (int j = start; j < n; j++) {
        conn_t *other = (conn_t*)events[j].data.ptr;
        if (other == c) {
            events[j].data.ptr = NULL;
        }
    }
}

void *forwarder_thread_func(worker_thread_t *worker) {
    struct epoll_event *events = calloc(MAX_EVENTS, sizeof(struct epoll_event));
    if (!events) {
        warnx("[worker-%d] Failed to allocate events", worker->thread_id);
        return NULL;
    }
    
    warnx("[worker-%d] Started", worker->thread_id);
    
    while (g_running) {
        int n = epoll_wait(worker->epfd, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EBADF) {
                warnx("[worker-%d] epoll_wait EBADF on epfd=%d; shutting down", worker->thread_id, worker->epfd);
                g_running = 0; // signal all threads to exit
            } else {
                warnx("[worker-%d] epoll_wait error: %s", worker->thread_id, strerror(errno));
            }
            break;
        }
        
        int cur_epoch = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);
        
        for (int i = 0; i < n; i++) {
            conn_t *c = (conn_t*)events[i].data.ptr;
            
            // Wakeup pipe or already-invalidated event
            if (c == NULL) {
                char buf[8];
                while (read(worker->wakeup_pipe[0], buf, sizeof(buf)) > 0);
                continue;
            }
            
            // Check Epoch (Kill stale connections)
            if (c->epoch_bound != cur_epoch) {
                DEBUG_LOG("[worker-%d] Epoch mismatch: conn=%p bound_epoch=%d cur_epoch=%d", 
                          worker->thread_id, c, c->epoch_bound, cur_epoch);
                // Invalidate any remaining events for this connection to prevent UAF
                invalidate_pending_events(events, i + 1, n, c);
                epoll_ctl(worker->epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                epoll_ctl(worker->epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                if (close_conn(c)) {
                    if (c->registered) {
                        metrics_dec_active_connections();
                        long new_count = __atomic_fetch_sub(&worker->active_connections, 1, __ATOMIC_RELAXED) - 1;
                        DEBUG_LOG("[worker-%d] Decremented active_connections to %ld (epoch cleanup)", worker->thread_id, new_count);
                    } else {
                        DEBUG_LOG("[worker-%d] Skipped counter decrement (not registered) epoch cleanup conn=%p", worker->thread_id, c);
                    }
                } else {
                    DEBUG_LOG("[worker-%d] close_conn skipped (already closed) in epoch cleanup for conn=%p", worker->thread_id, c);
                }
                continue;
            }
            
            // Drive IO
            int drive_result = drive_connection(c);
            if (drive_result < 0) {
                if (drive_result == -2) {
                    warnx("[worker-%d] Backend closed connection unexpectedly: conn=%p", worker->thread_id, c);
                } else if (drive_result == -1) {
                    DEBUG_LOG("[worker-%d] Client closed connection: conn=%p", worker->thread_id, c);
                } else if (drive_result == -3) {
                    DEBUG_LOG("[worker-%d] I/O error: conn=%p", worker->thread_id, c);
                }
                
                // Invalidate any remaining events for this connection to prevent UAF
                invalidate_pending_events(events, i + 1, n, c);
                epoll_ctl(worker->epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                epoll_ctl(worker->epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                if (close_conn(c)) {
                    if (c->registered) {
                        metrics_dec_active_connections();
                        long new_count = __atomic_fetch_sub(&worker->active_connections, 1, __ATOMIC_RELAXED) - 1;
                        DEBUG_LOG("[worker-%d] Decremented active_connections to %ld (connection closed)", worker->thread_id, new_count);
                    } else {
                        DEBUG_LOG("[worker-%d] Skipped counter decrement (not registered) conn=%p", worker->thread_id, c);
                    }
                } else {
                    DEBUG_LOG("[worker-%d] close_conn skipped (already closed) for conn=%p", worker->thread_id, c);
                }
                continue;
            }
            
            // Re-arm Epoll flags
            update_epoll_flags(c, worker->epfd);
        }
    }
    
    free(events);
    warnx("[worker-%d] Stopped", worker->thread_id);
    return NULL;
}
