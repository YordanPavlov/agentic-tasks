# Kafka sink: migrate exactly-once transaction naming to POOLING

Eliminate the KafkaSink transaction-abort sweep on restore by switching
etherbi-flink jobs from the default `INCREMENTING` transaction naming to
`TransactionNamingStrategy.POOLING` (flink-connector-kafka 4.x).

## Motivation (incident 2026-09-18, xrp-balances-v6)

After an autoscaler 18→1 scale-down, the sink subtask sat 26 min in
INITIALIZING running the abort sweep: `INCREMENTING` mints a fresh
transactional id per checkpoint (`prefix-subtask-checkpointId`), the broker
retains each id's metadata 7 DAYS, and on restore the sink must PROBE the
whole plausible id space (union of all runs of the job name over 7 days: 18
old subtasks × counters to ~1500 × 3 topics, ~10 probes/s, serialized onto
the surviving subtask). No progress is logged at INFO; the CR shows CREATED —
indistinguishable from a hang. Details:
[2026-09-xrp-balances-async-state](../2026-09-xrp-balances-async-state/xrp-balances-async-state.md).

## What POOLING does

- Each subtask reuses a small bounded pool of transactional ids (id recycled
  after its transaction commits; pool grows only with concurrent checkpoints).
- Broker metadata: small constant set instead of ~all ids of the last 7 days.
- Restore abort becomes LISTING: ask the broker which transactions with the
  prefix are actually open (`ListTransactions` API) and abort exactly those —
  seconds at any parallelism, no sweep.

## Requirements / constraints (from connector source javadoc)

- flink-connector-kafka 4.x (new in 4.x). Flink 2.3 jobs should already be on
  it — VERIFY version in etherbi-flink build first.
- Kafka brokers 3.0+ — hprod runs cp-kafka 7.8.2 (~Kafka 3.8) ✓.
- "Additional read permissions on the target topics" (ListTransactions) —
  check Kafka ACLs if enforced.
- Migration: take a checkpoint on connector 4.x INCREMENTING first (no
  lingering old-style txns), then flip; or restore from a savepoint of any
  version. Switching BACK to INCREMENTING is UNSUPPORTED (one-way door).
- Old ids age out of the broker 7 days after the switch regardless.

## Plan

1. Verify flink-connector-kafka version in etherbi-flink; locate KafkaSink
   builder call sites (shared code — likely one place for all jobs).
2. Add `.setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)`;
   decide fleet-wide vs per-job opt-in.
3. Canary on a stage job (hstage) or xrp-balances-v6 first; verify: restore
   time after a rescale, broker `__transaction_state` footprint, no txn errors.
4. Roll out with routine image bumps; note the one-way door in the deploy PR.

## Status

- 2026-09-18: task created; not started. Interim mitigation available on the
  devops side: `job.autoscaler.vertex.min-parallelism` floor to keep sweeps
  parallel (not yet applied).
