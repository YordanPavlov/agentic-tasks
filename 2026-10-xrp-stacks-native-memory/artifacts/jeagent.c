// Minimal JVMTI attach agent: reads jemalloc stats (and optionally purges) inside the JVM.
// Load with: jcmd 1 JVMTI.agent_load /path/jeagent-N.so "<mode>:<outfile>"
//   mode = stats  -> summary + per-arena dirty/muzzy, plus full malloc_stats_print
//   mode = purge  -> summary, arena.<ALL>.purge, summary again
//   mode = dump   -> summary, jemalloc heap profile to <outfile>.heap
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

typedef int (*mallctl_t)(const char *, void *, size_t *, void *, size_t);
typedef void (*stats_print_t)(void (*)(void *, const char *), void *, const char *);

static mallctl_t mc;

static void refresh(void) {
    uint64_t epoch = 1; size_t sz = sizeof(epoch);
    mc("epoch", &epoch, &sz, &epoch, sz);
}

static size_t rd(const char *name) {
    size_t v = 0, sz = sizeof(v);
    if (mc(name, &v, &sz, NULL, 0) != 0) return (size_t)-1;
    return v;
}

static unsigned rdu(const char *name) {
    unsigned v = 0; size_t sz = sizeof(v);
    if (mc(name, &v, &sz, NULL, 0) != 0) return (unsigned)-1;
    return v;
}

static long rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r"); char line[256]; long kb = -1;
    if (!f) return -1;
    while (fgets(line, sizeof line, f))
        if (strncmp(line, "VmRSS:", 6) == 0) { sscanf(line + 6, "%ld", &kb); break; }
    fclose(f);
    return kb;
}

static void summary(FILE *out, const char *label) {
    const double G = 1024.0 * 1024 * 1024;
    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    refresh();
    fprintf(out, "== %s  VmRSS=%.2fG\n", label, rss_kb() / 1024.0 / 1024.0);
    fprintf(out, "allocated=%.3fG active=%.3fG resident=%.3fG mapped=%.3fG retained=%.3fG metadata=%.3fG\n",
            rd("stats.allocated") / G, rd("stats.active") / G, rd("stats.resident") / G,
            rd("stats.mapped") / G, rd("stats.retained") / G, rd("stats.metadata") / G);
    fprintf(out, "all-arenas pdirty=%.3fG pmuzzy=%.3fG\n",
            rd("stats.arenas.4096.pdirty") * (double)page / G,
            rd("stats.arenas.4096.pmuzzy") * (double)page / G);
    unsigned n = rdu("arenas.narenas");
    fprintf(out, "narenas=%u; arenas with >64MiB dirty or allocated:\n", n);
    for (unsigned i = 0; i < n; i++) {
        char k[96]; size_t pd, small, large; unsigned nthreads;
        snprintf(k, sizeof k, "stats.arenas.%u.pdirty", i); pd = rd(k);
        snprintf(k, sizeof k, "stats.arenas.%u.small.allocated", i); small = rd(k);
        snprintf(k, sizeof k, "stats.arenas.%u.large.allocated", i); large = rd(k);
        snprintf(k, sizeof k, "stats.arenas.%u.nthreads", i); nthreads = rdu(k);
        if (pd == (size_t)-1) continue;
        double dirty = pd * (double)page / G, alloc = (small + large) / G;
        if (dirty > 0.0625 || alloc > 0.0625)
            fprintf(out, "  arena %3u threads=%3u allocated=%.3fG dirty=%.3fG\n", i, nthreads, alloc, dirty);
    }
}

static void write_cb(void *f, const char *s) { fputs(s, (FILE *)f); }

int Agent_OnAttach(void *vm, char *options, void *reserved) {
    (void)vm; (void)reserved;
    mc = (mallctl_t)dlsym(RTLD_DEFAULT, "mallctl");
    stats_print_t sp = (stats_print_t)dlsym(RTLD_DEFAULT, "malloc_stats_print");
    if (!options || !mc) return 0;
    char *colon = strchr(options, ':');
    if (!colon) return 0;
    *colon = '\0';
    FILE *out = fopen(colon + 1, "w");
    if (!out) return 0;
    if (strcmp(options, "stats") == 0) {
        summary(out, "stats");
        if (sp) { fputs("\n==== malloc_stats_print\n", out); sp(write_cb, out, "bl"); }
    } else if (strcmp(options, "dump") == 0) {
        // Heap profile goes to "<outfile>.heap"; needs MALLOC_CONF prof:true at JVM start.
        char heap[512];
        snprintf(heap, sizeof heap, "%s.heap", colon + 1);
        const char *fn = heap;
        summary(out, "dump");
        int rc = mc("prof.dump", NULL, NULL, (void *)&fn, sizeof(fn));
        fprintf(out, "\nprof.dump %s rc=%d\n", heap, rc);
    } else if (strcmp(options, "purge") == 0) {
        summary(out, "before purge");
        int rc = mc("arena.4096.purge", NULL, NULL, NULL, 0);
        fprintf(out, "\narena.4096.purge rc=%d\n\n", rc);
        summary(out, "after purge");
    }
    fclose(out);
    return 0;
}
