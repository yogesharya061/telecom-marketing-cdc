"""Cross-platform Docker Compose runner. Run from any working directory."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
def compose(*args):
    subprocess.run(['docker','compose',*args], cwd=ROOT, check=True)
def cli(*args):
    compose('run','--rm','--no-deps','cli','python',*args)

def main():
    mode = sys.argv[1] if len(sys.argv)>1 else 'up'
    if mode == 'up':
        compose('build')
        compose('up','-d','--wait','postgres','kafka')
        cli('-m','app.bootstrap','topics')
        compose('up','-d','connect','jobmanager','taskmanager')
        cli('-m','app.bootstrap','connector')
        cli('-m','app.bootstrap','flink')
        # Refuse duplicate SQL submissions on repeated startup.
        result = subprocess.run(['docker','compose','exec','-T','jobmanager','curl','-fsS',
            'http://localhost:8081/jobs/overview'],cwd=ROOT,check=True,capture_output=True,text=True)
        import json
        jobs = json.loads(result.stdout)['jobs']
        active = [j for j in jobs if j['name']=='telecom-marketing-v1' and j['state'] not in
                  ('FINISHED','FAILED','CANCELED')]
        if not active:
            compose('exec','-T','jobmanager','/opt/flink/bin/sql-client.sh','-f','/opt/flink/project-sql/pipeline.sql')
        cli('-m','app.bootstrap','job')
        compose('up','-d','worker')
        print('Ready. Run: python scripts/run.py demo')
    elif mode == 'demo':
        cli('-m','app.demo')
    elif mode == 'test':
        cli('-m','unittest','discover','-s','tests','-v')
    elif mode == 'down':
        compose('down')
    elif mode == 'logs':
        compose('logs','--tail','100')
    else:
        raise SystemExit('Usage: python scripts/run.py [up|demo|test|down|logs]')

if __name__ == '__main__':
    main()
