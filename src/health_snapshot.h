#ifndef HEALTH_SNAPSHOT_H
#define HEALTH_SNAPSHOT_H

#include "gateway.h"

typedef enum {
    BACKEND_STATUS_PRIMARY = 0,
    BACKEND_STATUS_PRIMARY_NOT_USED,
    BACKEND_STATUS_REPLICA,
    BACKEND_STATUS_UNHEALTHY
} backend_status_enum_t;

typedef struct {
    char host[256];
    char port[16];
    backend_status_enum_t status;
    char reason[256];
    double replica_lag_seconds;
} health_backend_snapshot_t;

typedef struct health_metrics_snapshot {
    int servers_total;
    int servers_healthy;
    size_t backend_count;
    struct health_metrics_snapshot *retired_next;
    health_backend_snapshot_t backends[];
} health_metrics_snapshot_t;

health_metrics_snapshot_t *health_snapshot_alloc(size_t backend_count);
void health_snapshot_publish(health_metrics_snapshot_t *snapshot);
const health_metrics_snapshot_t *health_snapshot_acquire(void);
void health_snapshot_release(const health_metrics_snapshot_t *snapshot);

#endif
