"""At-least-once Kafka ingestion + transactional/idempotent mock delivery."""
import json
import logging
import os
import signal
import time

import psycopg
from psycopg.rows import dict_row
from confluent_kafka import Consumer, KafkaException
from app.policy import current_reason, validate

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
DB = os.environ.get('DATABASE_URL', 'postgresql://demo:demo@postgres:5432/telecom')

def process(conn, event):
    """Called in one DB transaction; returning does not commit Kafka offsets."""
    with conn.cursor(row_factory=dict_row) as cur:
        # Source row lock serializes delivery with consent changes.
        cur.execute('SELECT * FROM crm.subscribers WHERE customer_id=%s FOR UPDATE',
                    (event['customer_id'],))
        row = cur.fetchone()
        cur.execute('SELECT 1 FROM marketing.decisions WHERE event_id=%s', (event['event_id'],))
        if cur.fetchone():
            return 'REPLAY'
        reason = event['reason']
        if reason == 'ELIGIBLE':
            reason = current_reason(row, event['billing_cycle'])
            if reason == 'ELIGIBLE':
                cur.execute('''INSERT INTO marketing.deliveries
                    (customer_id,billing_cycle,campaign_id,event_id)
                    VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id''',
                    (event['customer_id'], event['billing_cycle'], event['campaign_id'], event['event_id']))
                reason = 'SIMULATED_SENT' if cur.fetchone() else 'FREQUENCY_CAPPED'
        cur.execute('''INSERT INTO marketing.decisions(event_id,customer_id,reason)
                       VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''',
                    (event['event_id'],event['customer_id'],reason))
        return reason

def run():
    consumer = Consumer({
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092'),
        'group.id': 'marketing-delivery-v1', 'auto.offset.reset': 'earliest',
        'enable.auto.commit': False, 'enable.auto.offset.store': False,
    })
    consumer.subscribe(['marketing.decisions'])
    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        with psycopg.connect(DB, autocommit=True) as conn:
            while running:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                # Retry this record in-place; never commit past a failed record.
                for attempt in range(5):
                    try:
                        with conn.transaction():
                            try:
                                event = validate(json.loads(msg.value()))
                            except (ValueError, TypeError, UnicodeDecodeError) as exc:
                                conn.execute('''INSERT INTO marketing.quarantine
                                    (topic,partition_id,offset_id,payload,reason)
                                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                                    (msg.topic(),msg.partition(),msg.offset(),
                                     (msg.value() or b'').decode('utf-8', errors='replace'),str(exc)))
                                result = 'QUARANTINED'
                            else:
                                result = process(conn, event)
                        consumer.commit(message=msg, asynchronous=False)
                        logging.info('decision=%s partition=%s offset=%s', result, msg.partition(), msg.offset())
                        break
                    except (psycopg.Error, KafkaException):
                        if attempt == 4:
                            raise
                        logging.exception('Transient operation failure; retrying record')
                        time.sleep(min(2 ** attempt, 8))
    finally:
        consumer.close()

if __name__ == '__main__':
    run()
