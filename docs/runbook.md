# Operations and troubleshooting

## Health

```bash
docker compose ps
python scripts/run.py logs
docker compose run --rm --no-deps cli python -m app.bootstrap connector
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group marketing-delivery-v1
```

The connector command reconciles the supplied configuration and waits for a RUNNING task; it is not strictly read-only. For read-only status use a browser at <http://localhost:8083/connectors/telecom-subscribers/status>. Look at task state, not just connector state.

## Diagnose in order

1. Is the source change committed? Query `crm.subscribers` and confirm the revision incremented.
2. Is Debezium's task RUNNING? Examine `docker compose logs connect` for replication/publication problems.
3. Does the raw CDC topic contain the change? Use the README consumer command.
4. Is the Flink job RUNNING with successful checkpoints? Inspect the UI and `docker compose logs taskmanager`.
5. Does `marketing.decisions` contain a reason for this revision?
6. Is the worker consuming? Inspect group lag and worker logs.
7. Was the candidate suppressed by current consent, billing cycle, or frequency cap? Query `marketing.decisions`.
8. Was it quarantined? Query `marketing.quarantine` using the topic/partition/offset.

## Common issues

| Symptom | Resolution |
|---|---|
| Docker command missing or daemon unavailable | Install/start Docker Desktop or Engine, ensure Linux containers and Compose v2 |
| Cannot download images or connector JAR | Allow the registries/Maven Central through your proxy; inspect build error; retry `up` |
| Out-of-memory / repeated container restarts | Allocate at least the recommended Docker memory; inspect exit codes and logs |
| Port already allocated | Stop conflicting service or change the host side of the localhost port mapping |
| No matching ARM manifest | Use x86-64 or explicit Docker emulation |
| PostgreSQL missing table/trigger after editing init.sql | Init scripts run only on a new volume; migrate explicitly or reset this disposable demo |
| Connector RUNNING but task FAILED | Read task trace; check publication, slot and credentials; do not delete a production slot casually |
| Flink cannot discover Kafka connector | Rebuild Flink image, confirm matching JAR exists in `/opt/flink/lib` |
| Demo timeout | Inspect every layer above; do not increase timeout until the actual bottleneck is identified |
| No additional delivery after an update | Intentional frequency cap; inspect reason and use a new billing cycle/customer for a new offer |
| SQL edited but old behavior remains | Cancel the existing job through Flink UI, then rerun `up`; existing active jobs are not replaced |

## Worker recovery exercise

After a successful demo, record the delivery count. Restart the worker:

```bash
docker compose restart worker
```

Existing deliveries should not duplicate. The demo explicitly injects a duplicate decision and verifies the ledger count, but this does not test every crash boundary. For a stronger recovery drill, repeatedly kill a worker under sustained load and reconcile by campaign key.

## Backlog and checkpoint monitoring

```bash
docker compose exec postgres psql -U demo -d telecom -c "SELECT slot_name,active,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_wal_bytes FROM pg_replication_slots;"
docker compose exec postgres psql -U demo -d telecom -c "SELECT reason,count(*) FROM marketing.decisions GROUP BY reason;"
```

The SQL count query is an audit, not a real metrics exporter. Add alerts for consumer lag growth, checkpoint failure, connector failure, quarantine growth, WAL retention, delivery error rate and eligible customers without a final decision. Set thresholds from measured traffic and latency objectives.

## Schema evolution and replay

Additive CDC fields not referenced by Flink are ignored by its projected JSON schema. Renaming/removing fields or changing types requires a coordinated schema/pipeline rollout. Never silently enable `json.ignore-parse-errors=true` merely to get a green job: that can hide lost campaigns.

The checkpoint volume is shared locally between JobManager and TaskManager. Production object-store checkpoints and HA job metadata are separate needs. A planned restore requires selecting an actual completed checkpoint/savepoint and using the corresponding Flink restore configuration; this project does not automate it.

`earliest-offset` applies on a new submission without restored state. Recovery from a checkpoint uses that checkpoint's source offsets. Keep audit and idempotency keys at least as long as the maximum replay/resnapshot horizon. Customer IDs cannot be deleted and reused with revisions reset.
