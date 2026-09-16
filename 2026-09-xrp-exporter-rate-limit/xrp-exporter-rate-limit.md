# XRP exporter: crash loop on XRPL public endpoint rate limits

**Started:** 2026-09-16
**Status:** fixed and deployed; stage and prod catching up to the tip (ETA 2026-09-17 morning)
**Repos:** `san-chain-exporter` (PRs #257 merged, #264 merged), `devops` (hstage/hprod xrp exporter values)

## Problem

`xrp-exporter-v3` (hprod) and `-v2` (hstage) crash-looped every few
seconds against `wss://xrplcluster.com`:

```
RippledError: rate limit: units quota (2000 per 10s) exhausted, retry in ~6353ms
/opt/app/built/lib/kafka_storage.js:51  throw reason;
```

Exporter treated the error as fatal. The stack trace pointed at
`kafka_storage.js` only because the global `unhandledRejection`
handler lived there as an import side effect.

## What changed at xrplcluster.com

- Until ≥ Oct 2023 the site said "fair use, 5 connections / 1000 msg per
  minute, for power users & commercial use run your own node". Now:
  "Free for non-commercial use. For commercial use or higher rate
  limits, contact rpc at xrpl-labs.com" (XRPL Labs / The Integrators BV).
  Search engines still index the old text — change is recent, undated.
- Hard per-IP metered quota, readable via the `quota` websocket command:
  2000 units/10s, 10000/60s, 500000/3600s. Hourly window is clock-aligned.
- **Units measure server work, not bytes.** A ledger nobody fetched
  recently costs ~10x a cached one (measured: 1649 vs 166, 499 vs 51,
  411 vs 43 units). Tip ledgers are hot (30–130 u); 8-day-old backlog
  ledgers are cold (400–1650 u). Catching up a backlog on the anonymous
  quota is therefore impossible; staying at the tip is fine.
- `binary: true` is billed ~7x MORE than JSON. Dead end.
- Paid key via Dhali (pay per call in XRP); grant keys "under special
  circumstances" by email. No self-service free tier.
- Terms/limits pages: https://xrplcluster.com/ (landing page only),
  https://xrpl.org/docs/tutorials/public-servers ("not for sustained or
  business use" for s1/s2.ripple.com),
  https://xrpl.org/docs/references/http-websocket-apis/api-conventions/rate-limiting

## Fixes (san-chain-exporter, `src/blockchains/xrp/xrp_worker.ts`)

PR #257 (`d81b310`, `84d88ae`, `62d3d0c`):
- `connectionSend` retries via `recoverFromRequestError`: rate limit →
  pause the connection's whole p-queue until the endpoint's suggested
  deadline (concurrent rejections extend it); connection errors →
  capped linear backoff + reconnect. Bounded by `XRP_ENDPOINT_RETRIES`.
- `main().catch()` in `index.ts`; `unhandledRejection` handler moved
  there from `kafka_storage.ts`.
- `quota` response logged once at init (identity + remaining tiers).

PR #264 (`a7af9fb`, `b0ba63d`):
- rippled `tooBusy`/`slowDown` treated as rate limit; 5s default pause
  when no hint.
- Raw `ws` "WebSocket is not open" error and any error while
  `!isConnected()` treated as connection error.
- `connectionTimeout` = `DEFAULT_WS_TIMEOUT` (xrpl.js default 5s caused
  init crashes); init connect retried (`connectWithRetries`).
- Fixed master build broken by Dependabot #263 (uuid 14 is ESM-only):
  dynamic `import('uuid')` in `cardano_worker.ts`, dropped `@types/uuid`.

## Deploy configuration that works (devops values.yaml)

```yaml
XRP_NODE_URLS: "wss://s2.ripple.com,wss://honeycluster.io,wss://s1.ripple.com"
CONNECTIONS_COUNT: "4"   # prod: 3
SEND_BATCH_SIZE: "10"
REQUEST_RATE_INTERVAL_MSEC: "1000"
REQUEST_RATE_INTERVAL_CAP: "1"
```

- URLs are round-robined over connections; ledgers assigned by
  `index % connections`. `CONNECTIONS_COUNT` must be ≥ number of URLs
  or trailing URLs are never used (stage ran 1 connection for a while
  by mistake). `wss://xrpl.ws` is an alias of xrplcluster — same quota.
- **xrplcluster removed from the catch-up list**: as 1 of 4 it paused
  73% of the time and exhausted the hourly quota → 12 min pause →
  exporter healthcheck (`EXPORT_TIMEOUT_MLS` 5 min) → liveness kill
  (exit 137) every hour at :57. Batches wait for the slowest connection.
- Ripple servers answer `tooBusy` ~1/min per connection at 1 req/s;
  honeycluster almost never. All served the 8-day-old backlog.

## Measured throughput (chain ≈ 16.3 ledgers/min)

| Setup | ledgers/min |
|---|---|
| xrplcluster only, 1 conn | 3–12, constant pauses |
| xrplcluster + 3 others, 4 conn | 55 in-run, 32 incl. hourly kills |
| s2 + honeycluster + s1, 4 conn (stage) | 194 |
| same, 3 conn, batch 1, 1500 ms (prod before) | 62 |
| same, 3 conn, batch 10, 1000 ms (prod now) | 118 |

## Where we left off (2026-09-16 19:10 UTC)

- Stage: pod `xrp-exporter-v2-…`, 166k behind, ~176/min net → tip
  ≈ 2026-09-17 10:40 UTC.
- Prod: pod `xrp-exporter-v3-…`, 50k behind, ~101/min net → tip
  ≈ 2026-09-17 03:20 UTC.
- Both: 0 restarts, contiguous positions, only short `tooBusy` pauses.
- Check in the morning: position vs tip, restart count, then the
  exporter drops to polling every 30 s.

## Open items

- Decide long-term source: commercial/grant key from XRPL Labs
  (rpc at xrpl-labs.com), or own rippled node with a few weeks of
  history (removes the dependency; both operators have asked commercial
  users for this since 2023).
- Optionally re-add xrplcluster once at the tip (cached ledgers cheap).
- Code follow-up idea: scheduler skips connections that are currently
  paused instead of static `index % n` assignment.
- Process: Dependabot #263 was merged red and master merges are not
  built by Jenkins → require green checks / build master.
- `npm run lint` broken in repo → see `2026-09-san-chain-exporter-lint-repair`.
- `analyze_exporter_log.py <pod log>` in this dir summarises rate,
  pauses per connection, tooBusy counts, batch durations.
