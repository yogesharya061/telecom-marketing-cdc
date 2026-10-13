"""Repeatable real-stack assertions. Each run uses a new synthetic customer prefix."""
import json
import os
import time
import uuid
import psycopg
from confluent_kafka import Producer

DB = os.environ.get('DATABASE_URL', 'postgresql://demo:demo@postgres:5432/telecom')

def eventually(query, params, expected, conn, timeout=120):
    end = time.monotonic()+timeout
    actual = None
    while time.monotonic()<end:
        actual = conn.execute(query,params).fetchone()[0]
        if actual == expected:
            return
        time.sleep(1)
    raise AssertionError(f'Expected {expected!r}, received {actual!r}: {query}')

def run():
    prefix = 'demo-'+uuid.uuid4().hex[:10]
    ids = [prefix+'-'+str(i) for i in range(1,6)]
    with psycopg.connect(DB,autocommit=True) as conn:
        for i,cid in enumerate(ids):
            conn.execute('''INSERT INTO crm.subscribers
              (customer_id,billing_cycle,used_mb,quota_mb,marketing_consent,account_status)
              VALUES (%s,'2026-10',100,1000,%s,%s)''',
              (cid,i!=1,'SUSPENDED' if i==2 else 'ACTIVE'))
        eventually('SELECT count(*) FROM marketing.decisions WHERE customer_id LIKE %s',
                   (prefix+'%',),5,conn)
        for i,cid in enumerate(ids):
            conn.execute('UPDATE crm.subscribers SET used_mb=%s WHERE customer_id=%s',
                         (790 if i==3 else 850,cid))
        def reason(cid,revision,expected,op='u'):
            eventually('SELECT max(reason) FROM marketing.decisions WHERE event_id=%s',
                       (f'{cid}:2026-10:{revision}:{op}',),expected,conn)
            print('PASS',cid,revision,expected)
        for cid,expected in zip(ids,['SIMULATED_SENT','NO_CONSENT','INACTIVE','BELOW_THRESHOLD','SIMULATED_SENT']):
            reason(cid,2,expected)
        # A fresh eligible update must be frequency-capped.
        conn.execute('UPDATE crm.subscribers SET used_mb=900 WHERE customer_id=%s',(ids[0],))
        reason(ids[0],3,'FREQUENCY_CAPPED')
        # An explicit consent change travels through CDC and prevents targeting.
        conn.execute('UPDATE crm.subscribers SET marketing_consent=false WHERE customer_id=%s',(ids[0],))
        reason(ids[0],4,'NO_CONSENT')
        conn.execute('DELETE FROM crm.subscribers WHERE customer_id=%s',(ids[4],))
        reason(ids[4],2,'DELETED','d')
        producer = Producer({'bootstrap.servers':os.environ.get('KAFKA_BOOTSTRAP','kafka:9092')})
        errors=[]
        def delivered(err,_):
            if err:
                errors.append(str(err))
        # Replay the exact eligible event: ledger and audit must not duplicate.
        event={'event_id':f'{ids[0]}:2026-10:2:u','customer_id':ids[0],
               'billing_cycle':'2026-10','revision':2,'campaign_id':'DATA_TOPUP_V1','reason':'ELIGIBLE'}
        producer.produce('marketing.decisions',key=ids[0],value=json.dumps(event),callback=delivered)
        # Model a delayed eligible candidate after consent was already revoked.
        stale = event | {'event_id':f'{ids[1]}:2026-10:1:u','customer_id':ids[1],'revision':1}
        producer.produce('marketing.decisions',key=ids[1],value=json.dumps(stale),callback=delivered)
        marker = 'invalid-'+prefix
        producer.produce('marketing.decisions',key=ids[0],value=marker,callback=delivered)
        if producer.flush(30) or errors:
            raise AssertionError(f'Produce failed: {errors}')
        eventually('SELECT count(*) FROM marketing.quarantine WHERE payload=%s',(marker,),1,conn)
        reason(ids[1],1,'NO_CONSENT')
        eventually('SELECT count(*) FROM marketing.deliveries WHERE customer_id LIKE %s',
                   (prefix+'%',),2,conn)
        print('PASS duplicate replay + malformed-record quarantine')
        print('All integration assertions passed. Synthetic run:',prefix)
        print('Deliveries:',conn.execute('SELECT customer_id,status FROM marketing.deliveries WHERE customer_id LIKE %s',
                                        (prefix+'%',)).fetchall())

if __name__ == '__main__':
    run()
