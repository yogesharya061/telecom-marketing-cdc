CREATE SCHEMA crm;
CREATE SCHEMA marketing;
-- One atomic source row per subscriber: no cross-topic join race in this demo.
CREATE TABLE crm.subscribers (
 customer_id TEXT PRIMARY KEY,
 billing_cycle TEXT NOT NULL,
 used_mb BIGINT NOT NULL CHECK (used_mb >= 0),
 quota_mb BIGINT NOT NULL CHECK (quota_mb > 0),
 marketing_consent BOOLEAN NOT NULL DEFAULT FALSE,
 account_status TEXT NOT NULL CHECK (account_status IN ('ACTIVE','SUSPENDED')),
 revision BIGINT NOT NULL DEFAULT 1,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE FUNCTION crm.bump_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.revision := OLD.revision + 1;
 NEW.updated_at := clock_timestamp();
 RETURN NEW;
END $$;
CREATE TRIGGER revision_update BEFORE UPDATE ON crm.subscribers
FOR EACH ROW EXECUTE FUNCTION crm.bump_revision();
ALTER TABLE crm.subscribers REPLICA IDENTITY FULL;
CREATE PUBLICATION telecom_publication FOR TABLE crm.subscribers;
CREATE TABLE marketing.decisions (
 event_id TEXT PRIMARY KEY,
 customer_id TEXT NOT NULL,
 reason TEXT NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- This ledger IS the mock provider. No SMS/email is sent.
CREATE TABLE marketing.deliveries (
 customer_id TEXT NOT NULL,
 billing_cycle TEXT NOT NULL,
 campaign_id TEXT NOT NULL,
 event_id TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'SIMULATED_SENT',
 delivered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(customer_id,billing_cycle,campaign_id)
);
CREATE TABLE marketing.quarantine (
 topic TEXT NOT NULL, partition_id INT NOT NULL, offset_id BIGINT NOT NULL,
 payload TEXT, reason TEXT NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(topic,partition_id,offset_id)
);
