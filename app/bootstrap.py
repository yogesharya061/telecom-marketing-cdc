"""Container-side bootstrap: explicit topics, connector registration, readiness."""
import json
import sys
import time
from urllib.request import Request, urlopen
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

def http(url, data=None, method=None):
    request = Request(url, data=json.dumps(data).encode() if data is not None else None,
                      method=method, headers={'Content-Type':'application/json'})
    with urlopen(request, timeout=10) as response:
        return json.load(response)

def retry(action, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except Exception as exc:
            last = exc
        time.sleep(2)
    raise RuntimeError(f'Readiness timeout: {last}')

def main():
    mode = sys.argv[1]
    if mode == 'topics':
        admin = AdminClient({'bootstrap.servers':'kafka:9092'})
        topics = [NewTopic(name, num_partitions=parts, replication_factor=1, config=config)
            for name,parts,config in [
                ('connect-configs',1,{'cleanup.policy':'compact'}),
                ('connect-offsets',1,{'cleanup.policy':'compact'}),
                ('connect-status',1,{'cleanup.policy':'compact'}),
                ('telecom.crm.subscribers',3,{'retention.ms':'604800000'}),
                ('__debezium-heartbeat.telecom',1,{'cleanup.policy':'compact'}),
                ('marketing.decisions',3,{'retention.ms':'604800000'})]]
        for name,future in admin.create_topics(topics).items():
            try:
                future.result()
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
            print('Topic ready:', name)
    elif mode == 'connector':
        retry(lambda: http('http://connect:8083/connector-plugins'))
        with open('connect/subscribers.json') as stream:
            config = json.load(stream)
        http('http://connect:8083/connectors/telecom-subscribers/config',config,'PUT')
        def ready():
            status = http('http://connect:8083/connectors/telecom-subscribers/status')
            return status if status['connector']['state']=='RUNNING' and status['tasks'] and all(
                task['state']=='RUNNING' for task in status['tasks']) else None
        print(json.dumps(retry(ready),indent=2))
    elif mode == 'flink':
        retry(lambda: http('http://jobmanager:8081/taskmanagers')['taskmanagers'])
    elif mode == 'job':
        def running():
            jobs = http('http://jobmanager:8081/jobs/overview')['jobs']
            return any(job['name']=='telecom-marketing-v1' and job['state']=='RUNNING' for job in jobs)
        retry(running)
        print('Flink job RUNNING')
    else:
        raise ValueError(mode)

if __name__ == '__main__':
    main()
