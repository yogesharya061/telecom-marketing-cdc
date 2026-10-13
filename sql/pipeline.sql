SET 'pipeline.name' = 'telecom-marketing-v1';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.checkpoints.dir' = 'file:///opt/flink/checkpoints';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5 s';
-- Deliberately consume the Debezium ENVELOPE as append-only events.
-- We need a decision per change, rather than a retracting dynamic table.
CREATE TABLE changes (
 `before` ROW<customer_id STRING, billing_cycle STRING, used_mb BIGINT, quota_mb BIGINT, marketing_consent BOOLEAN, account_status STRING, revision BIGINT>,
 `after` ROW<customer_id STRING, billing_cycle STRING, used_mb BIGINT, quota_mb BIGINT, marketing_consent BOOLEAN, account_status STRING, revision BIGINT>,
 op STRING
) WITH (
 'connector'='kafka', 'topic'='telecom.crm.subscribers',
 'properties.bootstrap.servers'='kafka:9092',
 'properties.group.id'='telecom-flink-v1',
 'scan.startup.mode'='earliest-offset',
 'format'='json', 'json.fail-on-missing-field'='false',
 'json.ignore-parse-errors'='false'
);
CREATE VIEW events AS SELECT
 COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
 COALESCE(`after`.billing_cycle, `before`.billing_cycle) AS billing_cycle,
 COALESCE(`after`.revision, `before`.revision) AS revision,
 `after`.used_mb AS used_mb, `after`.quota_mb AS quota_mb,
 `after`.marketing_consent AS consent, `after`.account_status AS account_status, op
FROM changes;
CREATE TABLE decisions (
 event_id STRING, customer_id STRING, billing_cycle STRING,
 revision BIGINT, campaign_id STRING, reason STRING
) WITH (
 'connector'='kafka', 'topic'='marketing.decisions',
 'properties.bootstrap.servers'='kafka:9092',
 'key.format'='json', 'key.fields'='customer_id',
 'value.format'='json', 'value.fields-include'='ALL',
 'sink.delivery-guarantee'='at-least-once'
);
INSERT INTO decisions
SELECT
 CONCAT(customer_id, ':', billing_cycle, ':', CAST(revision AS STRING), ':', op),
 customer_id, billing_cycle, revision, 'DATA_TOPUP_V1',
 CASE
 WHEN customer_id IS NULL OR billing_cycle IS NULL OR revision IS NULL OR op IS NULL
   OR op NOT IN ('c','u','r','d') THEN 'INVALID'
 WHEN op='d' THEN 'DELETED'
 WHEN quota_mb IS NULL OR quota_mb<=0 OR used_mb IS NULL OR used_mb<0
   OR consent IS NULL OR account_status IS NULL THEN 'INVALID'
 WHEN consent=FALSE THEN 'NO_CONSENT'
 WHEN account_status<>'ACTIVE' THEN 'INACTIVE'
 WHEN CAST(used_mb AS DOUBLE)/quota_mb >= 0.8 THEN 'ELIGIBLE'
 ELSE 'BELOW_THRESHOLD' END
FROM events;
