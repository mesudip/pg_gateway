/*
 * health_check.c - PostgreSQL primary health checking
 */

#include "gateway.h"

/* --- Health State --- */

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

/* --- Primary Check --- */

static bool check_postgres_primary(const char *host, const char *port, int cto_ms, int qto_ms, char *errbuf, size_t errlen) {
    if (errbuf && errlen > 0) errbuf[0] = '\0';
    char conninfo[1024];
    const char *dbname = getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres";
    snprintf(conninfo, sizeof(conninfo), "host=%s port=%s connect_timeout=%d dbname=%s",
             host, port, cto_ms / 1000, dbname);
             
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        if (errbuf && errlen > 0) snprintf(errbuf, errlen, "connect failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    // Set statement timeout
    char setto[128];
    snprintf(setto, sizeof(setto), "SET LOCAL statement_timeout=%d;", qto_ms);
    PGresult *r1 = PQexec(conn, setto);
    if (PQresultStatus(r1) != PGRES_COMMAND_OK) {
        if (errbuf && errlen > 0) snprintf(errbuf, errlen, "set statement_timeout failed: %s", PQerrorMessage(conn));
        PQclear(r1); PQfinish(conn); return false;
    }
    PQclear(r1);

    // Check read-only status
    PGresult *res = PQexec(conn, "SHOW transaction_read_only;");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (errbuf && errlen > 0) snprintf(errbuf, errlen, "read-only check failed: %s", PQerrorMessage(conn));
        PQclear(res); PQfinish(conn); return false;
    }
    
    char *val = PQgetvalue(res, 0, 0);
    bool is_primary = (val && strcasecmp(val, "off") == 0);

    if (!is_primary && errbuf && errlen > 0) snprintf(errbuf, errlen, "server reported read-only (standby)");
    
    PQclear(res);
    PQfinish(conn);
    return is_primary;
}

/* --- Cleanup Handler --- */

static void cleanup_rwlock(void *arg) {
    pthread_rwlock_t *lock = (pthread_rwlock_t *)arg;
    pthread_rwlock_unlock(lock);
}

/* --- Health Thread --- */

void *health_thread_func(void *arg) {
    (void)arg;
    int check_int = getenv("CHECK_EVERY") ? atoi(getenv("CHECK_EVERY")) : 2;
    int cto = getenv("CONNECT_TIMEOUT_MS") ? atoi(getenv("CONNECT_TIMEOUT_MS")) : 800;
    int qto = getenv("QUERY_TIMEOUT_MS") ? atoi(getenv("QUERY_TIMEOUT_MS")) : 500;

    health_state_t last_state = HEALTH_UNKNOWN;
    char last_host[256] = {0};
    char last_reason[256] = {0};

    while (g_running) {
        candidate_t found_cand = {0};
        bool ok = false;
        char errbuf[256] = {0};

        // 1. Scan candidates for a primary
        for (size_t i = 0; i < g_ncand; i++) {
            char cand_reason[256] = {0};
            if (check_postgres_primary(g_candidates[i].host, g_candidates[i].port, cto, qto, cand_reason, sizeof(cand_reason))) {
                found_cand = g_candidates[i];
                ok = true;
                break;
            } else if (!errbuf[0]) {
                snprintf(errbuf, sizeof(errbuf), "candidate %s:%s %s", g_candidates[i].host, g_candidates[i].port,
                         cand_reason[0] ? cand_reason : "not primary");
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

        health_state_t new_state = ok ? HEALTH_HEALTHY : HEALTH_UNHEALTHY;
        int e = __atomic_load_n(&g_epoch, __ATOMIC_RELAXED);

        if (changed || new_state != last_state) {
            if (ok) {
                warnx("[health] STATE CHANGE: %s -> HEALTHY primary %s (Epoch %d)",
                      health_state_name(last_state), new_target.host_str, e);
                strncpy(last_host, new_target.host_str, sizeof(last_host));
                last_reason[0] = '\0';
            } else {
                const char *reason = errbuf[0] ? errbuf : "no primary reachable";
                warnx("[health] STATE CHANGE: %s -> UNHEALTHY (%s) (Epoch %d)",
                      health_state_name(last_state), reason, e);
                strncpy(last_reason, reason, sizeof(last_reason));
            }
            last_state = new_state;
        }

        sleep(check_int);
    }
    return NULL;
}
