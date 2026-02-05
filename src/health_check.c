/*
 * health_check.c - PostgreSQL primary health checking
 *
 * This module is responsible for monitoring the availability and status
 * of potential PostgreSQL backends. It periodically polls candidate
 * servers to identify the primary node, updating the global routing
 * state so that new client connections are always directed to the
 * active leader.
 */

#include "gateway.h"

/* --- Health State --- */

#ifndef DEBUG_HEALTH
#define DEBUG_HEALTH 0
#endif

#define HLOG(fmt, ...) do { \
    if (DEBUG_HEALTH) { \
        struct timespec ts; \
        clock_gettime(CLOCK_REALTIME, &ts); \
        fprintf(stderr, "[%ld.%03ld] [DEBUG] [health] " fmt "\n", \
                ts.tv_sec, ts.tv_nsec/1000000, ##__VA_ARGS__); \
    } \
} while(0)

typedef enum {
    HEALTH_UNKNOWN = 0,
    HEALTH_HEALTHY,
    HEALTH_UNHEALTHY
} health_state_t;

static const char *health_state_name(health_state_t s) {
    switch (s) {
        case HEALTH_HEALTHY: return "HEALTHY";
        case HEALTH_UNHEALTHY: return "UNHEALTHY";
        default: return "UNKNOWN";
    }
}

/* --- Candidate Parsing --- */

void parse_candidates(const char *s) {
    if (!s || !*s) die("CANDIDATES env var required");
    char *dup = strdup(s);
    size_t count = 1;
    for (char *p = dup; *p; ++p) if (*p == ',') ++count;
    
    g_candidates = calloc(count, sizeof(candidate_t));
    g_ncand = 0;
    
    const char *dbname = getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres";
    int cto = getenv("CONNECT_TIMEOUT_MS") ? atoi(getenv("CONNECT_TIMEOUT_MS")) : 800;
    
    char *save = NULL;
    char *tok = strtok_r(dup, ",", &save);
    while (tok) {
        while (*tok == ' ') ++tok; // Trim leading space
        char *c = strchr(tok, ':');
        if (!c) die("Invalid candidate format '%s' (expected host:port)", tok);
        *c = '\0';
        snprintf(g_candidates[g_ncand].host, sizeof(g_candidates[g_ncand].host), "%s", tok);
        snprintf(g_candidates[g_ncand].port, sizeof(g_candidates[g_ncand].port), "%s", c + 1);
        
        // Pre-compute connection string
        snprintf(g_candidates[g_ncand].conninfo, sizeof(g_candidates[g_ncand].conninfo),
                 "host=%s port=%s connect_timeout=%d dbname=%s application_name=pg_gateway",
                 g_candidates[g_ncand].host, g_candidates[g_ncand].port, cto / 1000, dbname);
        
        g_candidates[g_ncand].health_conn = NULL;
        
        // Initial DNS resolution
        if (!resolve_addr(g_candidates[g_ncand].host, g_candidates[g_ncand].port, &g_candidates[g_ncand].target)) {
            warnx("[config] Warning: Initial DNS resolution failed for %s:%s",
                  g_candidates[g_ncand].host, g_candidates[g_ncand].port);
        }
        
        g_ncand++;
        tok = strtok_r(NULL, ",", &save);
    }
    free(dup);

    for (size_t i = 0; i < g_ncand; i++) {
        warnx("[config] backend[%zu]=%s:%s", i, g_candidates[i].host, g_candidates[i].port);
    }
    metrics_set_server_counts((int)g_ncand, 0);
}

/* --- Primary Check --- */

static bool check_postgres_primary(candidate_t *cand, int qto_ms, char *errbuf, size_t errlen) {
    // Check if connection exists and is alive
    if (cand->health_conn != NULL) {
        if (PQstatus(cand->health_conn) != CONNECTION_OK) {
            // Connection dead, clean up and reconnect
            PQfinish(cand->health_conn);
            cand->health_conn = NULL;
        }
    }
    
    // Connect if needed
    if (cand->health_conn == NULL) {
        // Re-resolve DNS on reconnection
        if (!resolve_addr(cand->host, cand->port, &cand->target)) {
            if (errbuf && errlen > 0) snprintf(errbuf, errlen, "DNS resolution failed");
            return false;
        }
        
        cand->health_conn = PQconnectdb(cand->conninfo);
        if (PQstatus(cand->health_conn) != CONNECTION_OK) {
            if (errbuf && errlen > 0) snprintf(errbuf, errlen, "connect failed: %s", PQerrorMessage(cand->health_conn));
            PQfinish(cand->health_conn);
            cand->health_conn = NULL;
            return false;
        }
        
        // Set statement timeout on new connection
        char setto[128];
        snprintf(setto, sizeof(setto), "SET statement_timeout=%d;", qto_ms);
        PGresult *r1 = PQexec(cand->health_conn, setto);
        if (PQresultStatus(r1) != PGRES_COMMAND_OK) {
            if (errbuf && errlen > 0) snprintf(errbuf, errlen, "set statement_timeout failed: %s", PQerrorMessage(cand->health_conn));
            PQclear(r1);
            PQfinish(cand->health_conn);
            cand->health_conn = NULL;
            return false;
        }
        PQclear(r1);
    }

    // Check read-only status
    PGresult *res = PQexec(cand->health_conn, "SHOW transaction_read_only;");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (errbuf && errlen > 0) snprintf(errbuf, errlen, "read-only check failed: %s", PQerrorMessage(cand->health_conn));
        PQclear(res);
        // Connection might be bad, close it for next iteration
        PQfinish(cand->health_conn);
        cand->health_conn = NULL;
        return false;
    }
    
    char *val = PQgetvalue(res, 0, 0);
    bool is_primary = (val && strcasecmp(val, "off") == 0);

    if (!is_primary && errbuf && errlen > 0) snprintf(errbuf, errlen, "server reported read-only (standby)");
    
    PQclear(res);
    return is_primary;
}

/* --- Health Thread --- */

// Backend status enum
typedef enum {
    STATUS_PRIMARY = 0,
    STATUS_PRIMARY_NOT_USED,
    STATUS_REPLICA,
    STATUS_UNHEALTHY
} backend_status_enum_t;

static const char *status_names[] = {
    "Primary",
    "Primary(not-used)",
    "Replica",
    "Unhealthy"
};

// Backend status structure for health tracking
typedef struct {
    const char *host;
    const char *port;
    backend_status_enum_t status;
    char reason[256];
} backend_status_t;

void *health_thread_func(void *arg) {
    (void)arg;
    int check_int = getenv("CHECK_EVERY") ? atoi(getenv("CHECK_EVERY")) : 2;
    int qto = getenv("QUERY_TIMEOUT_MS") ? atoi(getenv("QUERY_TIMEOUT_MS")) : 500;

    health_state_t last_state = HEALTH_UNKNOWN;
    char last_reason[256] = {0};
    
    // Allocate status tracking on heap to avoid stack overflow
    backend_status_t *statuses = calloc(g_ncand, sizeof(backend_status_t));
    if (!statuses) {
        warnx("[health] Failed to allocate status tracking memory");
        return NULL;
    }

    while (g_running) {
        HLOG("Starting health check cycle");
        candidate_t found_cand = {0};
        bool ok = false;
        char errbuf[256] = {0};
        
        // Clear statuses for this iteration
        memset(statuses, 0, g_ncand * sizeof(backend_status_t));
        size_t status_count = g_ncand;

        // 1. Scan candidates for a primary and collect all statuses
        for (size_t i = 0; i < status_count; i++) {
            HLOG("Checking candidate[%zu]: %s:%s", i, g_candidates[i].host, g_candidates[i].port);
            statuses[i].host = g_candidates[i].host;
            statuses[i].port = g_candidates[i].port;
            
            char cand_reason[256] = {0};
            if (check_postgres_primary(&g_candidates[i], qto, cand_reason, sizeof(cand_reason))) {
                // This is a primary
                if (!ok) {  // Take the first primary found
                    found_cand = g_candidates[i];
                    ok = true;
                    statuses[i].status = STATUS_PRIMARY;
                } else {
                    // Another primary found (split brain?)
                    statuses[i].status = STATUS_PRIMARY_NOT_USED;
                }
            } else {
                // Not a primary - determine if it's a replica or unhealthy
                if (cand_reason[0] && strstr(cand_reason, "read-only")) {
                    statuses[i].status = STATUS_REPLICA;
                    strncpy(statuses[i].reason, "read-only", sizeof(statuses[i].reason) - 1);
                } else if (cand_reason[0]) {
                    statuses[i].status = STATUS_UNHEALTHY;
                    strncpy(statuses[i].reason, cand_reason, sizeof(statuses[i].reason) - 1);
                } else {
                    statuses[i].status = STATUS_UNHEALTHY;
                    strncpy(statuses[i].reason, "check failed", sizeof(statuses[i].reason) - 1);
                }
                
                if (!errbuf[0]) {
                    snprintf(errbuf, sizeof(errbuf), "candidate %s:%s %s", 
                             g_candidates[i].host, g_candidates[i].port,
                             cand_reason[0] ? cand_reason : "not primary");
                }
            }
        }

        // 2. Resolve DNS (off the main loop)
        target_addr_t new_target = {0};
        if (ok) {
            if (!resolve_addr(found_cand.host, found_cand.port, &new_target)) {
                warnx("[health] Found primary %s:%s but DNS resolution failed", found_cand.host, found_cand.port);
                if (!errbuf[0]) snprintf(errbuf, sizeof(errbuf), "primary %s:%s resolution failed", found_cand.host, found_cand.port);
                ok = false;
            }
        }

        // 3. Update global state
        bool changed = false;
        
        if (ok) {
            // Find index of new_target in candidates
            int new_idx = -1;
            for (size_t i = 0; i < g_ncand; i++) {
                if (sockaddr_equal(&g_candidates[i].target.addr, g_candidates[i].target.addr_len,
                                  &new_target.addr, new_target.addr_len)) {
                    new_idx = (int)i;
                    break;
                }
            }
            
            int cur_idx = __atomic_load_n(&g_primary_idx, __ATOMIC_RELAXED);
            if (new_idx != cur_idx) {
                HLOG("Primary changed: old_idx=%d new_idx=%d", cur_idx, new_idx);
                __atomic_store_n(&g_primary_idx, new_idx, __ATOMIC_RELEASE);
                int new_epoch = __atomic_fetch_add(&g_epoch, 1, __ATOMIC_RELAXED) + 1;
                HLOG("Epoch incremented to %d", new_epoch);
                changed = true;
            }
        } else {
            // Lost primary
            int cur_idx = __atomic_load_n(&g_primary_idx, __ATOMIC_RELAXED);
            if (cur_idx >= 0) {
                HLOG("Lost primary: old_idx=%d", cur_idx);
                __atomic_store_n(&g_primary_idx, -1, __ATOMIC_RELEASE);
                int new_epoch = __atomic_fetch_add(&g_epoch, 1, __ATOMIC_RELAXED) + 1;
                HLOG("Epoch incremented to %d", new_epoch);
                changed = true;
            }
        }

        health_state_t new_state = ok ? HEALTH_HEALTHY : HEALTH_UNHEALTHY;
        int e = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);

        // 4. Log state changes
        if (changed || new_state != last_state) {
            if (ok) {
                warnx("[health] STATE CHANGE: %s -> HEALTHY primary %s (Epoch %d)",
                      health_state_name(last_state), new_target.host_str, e);
                last_reason[0] = '\0';
            } else {
                const char *reason = errbuf[0] ? errbuf : "no primary reachable";
                warnx("[health] STATE CHANGE: %s -> UNHEALTHY (%s) (Epoch %d)",
                      health_state_name(last_state), reason, e);
                strncpy(last_reason, reason, sizeof(last_reason));
            }
            last_state = new_state;
        }
        
        // 5. Print status of all backends when primary changes
        if (changed) {
            warnx("[health] Backend Status:");
            for (size_t i = 0; i < status_count; i++) {
                if (statuses[i].reason[0]) {
                    warnx("[health]   %s:%s -> %s (%s)", 
                          statuses[i].host, statuses[i].port, 
                          status_names[statuses[i].status], statuses[i].reason);
                } else {
                    warnx("[health]   %s:%s -> %s", 
                          statuses[i].host, statuses[i].port, 
                          status_names[statuses[i].status]);
                }
            }
        }

        HLOG("Health check cycle complete, sleeping %ds", check_int);
        sleep(check_int);
    }
    
    free(statuses);
    return NULL;
}
