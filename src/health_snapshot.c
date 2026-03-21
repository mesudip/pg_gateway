#include "health_snapshot.h"

static _Atomic(health_metrics_snapshot_t *) g_health_snapshot = NULL;
static _Atomic(health_metrics_snapshot_t *) g_retired_health_snapshots = NULL;
static _Atomic int g_health_snapshot_readers = 0;

health_metrics_snapshot_t *health_snapshot_alloc(size_t backend_count) {
    size_t bytes = sizeof(health_metrics_snapshot_t) +
                   (backend_count * sizeof(health_backend_snapshot_t));
    health_metrics_snapshot_t *snapshot = calloc(1, bytes);
    if (!snapshot) return NULL;

    snapshot->servers_total = (int)backend_count;
    snapshot->servers_healthy = 0;
    snapshot->backend_count = backend_count;
    snapshot->retired_next = NULL;
    return snapshot;
}

static void health_snapshot_try_reap_retired(void) {
    if (__atomic_load_n(&g_health_snapshot_readers, __ATOMIC_ACQUIRE) != 0) return;

    health_metrics_snapshot_t *snapshot =
        __atomic_exchange_n(&g_retired_health_snapshots, NULL, __ATOMIC_ACQ_REL);
    while (snapshot) {
        health_metrics_snapshot_t *next = snapshot->retired_next;
        free(snapshot);
        snapshot = next;
    }
}

void health_snapshot_publish(health_metrics_snapshot_t *snapshot) {
    health_metrics_snapshot_t *old = __atomic_exchange_n(&g_health_snapshot, snapshot, __ATOMIC_ACQ_REL);
    if (old) {
        health_metrics_snapshot_t *head = NULL;
        do {
            head = __atomic_load_n(&g_retired_health_snapshots, __ATOMIC_ACQUIRE);
            old->retired_next = head;
        } while (!__atomic_compare_exchange_n(&g_retired_health_snapshots, &head, old, false,
                                              __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));
    }
    health_snapshot_try_reap_retired();
}

const health_metrics_snapshot_t *health_snapshot_acquire(void) {
    __atomic_add_fetch(&g_health_snapshot_readers, 1, __ATOMIC_ACQ_REL);
    return __atomic_load_n(&g_health_snapshot, __ATOMIC_ACQUIRE);
}

void health_snapshot_release(const health_metrics_snapshot_t *snapshot) {
    (void)snapshot;
    if (__atomic_sub_fetch(&g_health_snapshot_readers, 1, __ATOMIC_ACQ_REL) == 0) {
        health_snapshot_try_reap_retired();
    }
}
