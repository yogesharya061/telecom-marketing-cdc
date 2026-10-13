# Enterprise design and production gap analysis

## Business outcome and measurement

Hypothesis: timely, consent-aware top-up offers increase add-on conversion while reducing missed opportunities and duplicate contacts. No revenue or latency improvement has been measured in this project.

Measure the funnel: eligible source changes → Flink decisions → authorized delivery attempts → provider acceptance → confirmed delivery → purchase. Provider acceptance is not customer receipt, and receipt is not a purchase. An A/B holdout group and a stable attribution window are needed to estimate incremental revenue.

Candidate production SLOs, to validate with load tests: p95 source-commit-to-provider-acceptance under 30 seconds, 99.9% pipeline availability, zero known consent violations, and one provider request per idempotency key. These are proposed targets, not achieved results.

## Implemented versus required for production

| Concern | Implemented locally | Production action |
|---|---|---|
| Change capture | WAL CDC, named replication slot, initial snapshot | Least-privilege replication role, failover-slot planning, WAL disk alarms |
| Durability | Named volumes, source checkpoints, at-least-once Kafka sink | Multi-AZ Kafka RF=3, min ISR=2, acks=all; HA PostgreSQL; managed backups |
| Delivery | Idempotent transactional database mock | Outbox dispatcher, provider idempotency key, receipt callbacks, bounded retries |
| Consent | Atomic source-row lock and recheck | Dedicated consent authority, legal basis, suppression lists, revocation propagation SLO |
| Stream processing | Flink SQL, checkpointing, fixed-delay restart | HA JobManager, durable object-store checkpoints, savepoints and recovery drills |
| Data quality | DB constraints, decision contract validation, quarantine | Raw CDC validation stage with durable DLQ; schema registry and compatibility gates |
| Security | Synthetic IDs, localhost admin ports, non-root app | TLS/SASL, ACLs, secret manager, private networks, RBAC, audit access |
| Observability | Logs, Flink UI, reason-coded SQL audit | Prometheus/Grafana, OpenTelemetry correlation, lag/backpressure/WAL alerts |
| Scale | Three data-topic partitions, Flink parallelism two | Benchmark, size partitions, scale workers, avoid source-DB lock bottleneck |
| Business policy | One fixed campaign and frequency cap | Versioned rule catalog, campaign priorities, quiet hours, channel preferences |
| Testing | Rules and repeatable integration scenario | Failure injection, resnapshot/replay, upgrade, load, privacy and DR tests |

## Why this is not a network-CDR aggregation project

The source provides cumulative per-cycle usage. A real operator often ingests millions of individual call/data records. That extension needs event IDs, event time, deduplication, keyed aggregation, watermarks, lateness policy, billing corrections, and reconciliation against the charging system. It should publish authoritative subscriber usage before campaign evaluation.

If CRM consent and network usage arrive on separate topics, use versioned enrichment with a documented temporal policy. Missing profiles must be buffered or quarantined rather than treated as consent. A stream join alone cannot eliminate all consent races; perform a final authorized send-time check.

## Real outbound communications

Replace the database mock with:

1. A transaction that checks current policy and creates a PENDING outbox item using the customer/cycle/campaign unique key.
2. A dispatcher that claims rows with `FOR UPDATE SKIP LOCKED`, leases work, and rechecks consent immediately before provider calls.
3. A provider request carrying the same durable idempotency key on every retry.
4. Bounded exponential retry for transient failures, dead-letter escalation for exhausted retries, and provider receipt ingestion.
5. Reconciliation of uncertain requests after timeout using the provider's idempotency/lookup API.

Do not mark SENT merely because a request was queued. A provider without idempotency support creates an unavoidable ambiguity after a network timeout: retrying may duplicate, and not retrying may miss a contact. State that tradeoff explicitly.

## Failure scenarios

| Failure | Local behavior | Remaining limitation |
|---|---|---|
| Worker dies before DB commit | DB rolls back; uncommitted offset is replayed | Temporary delay |
| Worker dies after DB commit, before Kafka commit | Same event replayed; keys suppress duplicate delivery | Audits must remain retained through replay horizon |
| Consent revoked while candidate waits | Current row recheck blocks the offer | A revocation after mock delivery cannot undo it |
| Customer deleted before candidate processing | Current-state lookup suppresses delivery | Source IDs must never be reused |
| Same candidate repeated with another revision | Unique campaign/customer/cycle caps delivery | Rule versions need intentionally versioned campaign IDs |
| Flink TaskManager loss | Checkpoint recovery while JobManager remains alive | Full-cluster HA and external checkpoint restore not implemented |
| Entire local stack restarted | New Flink submission replays retained Kafka records | Not recovery beyond Kafka retention; cold replay may be expensive |
| Debezium offline | PostgreSQL retains WAL for slot | Disk can fill; monitor and act before loss |
| Bad decision payload | Quarantine then commit offset | Requires operator review; no automatic replay UI |
| Broken raw CDC JSON | Flink fails and retries, visible in UI/logs | Requires repair/reprocessing; no CDC DLQ in this version |
| Seven-day retention exceeded | Old source events unavailable | Reconcile source vs audit, controlled resnapshot; cannot reconstruct deleted history |

## Capacity and data governance

Python consumers can scale to the three decision-topic partitions. More instances than partitions do not add active consumers. Current subscriber row locks and synchronous database operations intentionally favor correctness over maximum throughput. Move delivery state to a dedicated database in production and size the source-facing consent service independently.

Do not set Kafka partitions or Flink parallelism based on an invented throughput number. Benchmark representative payloads, bursts, checkpoint intervals and provider rate limits. Check Kafka lag, Flink backpressure, database lock waits, slot lag and end-to-end latency together.

Subscriber IDs remain pseudonymous personal data in a real system. Define topic/audit/quarantine retention, access controls, erasure workflows and lawful processing. The current demo retains audit indefinitely and does not implement erasure. Avoid logging real contact details or dumping production payloads into CI artifacts.

## Rollout sequence

Run CI → scan dependencies/images → staging replay → failure and load tests → shadow decisions with no provider calls → compare with existing campaigns → limited consented cohort → gradually expand with rollback and kill switches. Maintain reconciliation of source changes, decisions and deliveries, so missing campaigns are visible rather than inferred from revenue alone.
