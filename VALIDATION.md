# Validation record

Creation environment: Python 3.12.14, Linux. Project containers target Python 3.11.

Executed successfully:
- `python -m unittest discover -s tests -v`: 7 tests passed, covering threshold boundaries, consent, deletion, cycle mismatch, suspended status and invalid contracts.
- YAML/JSON parsing: Compose, GitHub workflow and connector configuration parsed successfully (not a Docker Compose semantic validation).
- `python -m compileall -q app scripts tests`: passed.

Not executed here:
- Docker build/image pull and Compose startup: Docker executable unavailable.
- Flink SQL planning/runtime, Debezium capture and Kafka integration.
- GitHub Actions results are not yet verified; inspect the repository Actions tab for the published commit.
- Load, high-availability, security, failure-injection and disaster-recovery tests.

Included executable integration gate: `python scripts/run.py up` followed by `python scripts/run.py demo`. It checks actual CDC, Flink decisions, simulated delivery, frequency caps, consent revocation, deletion, replay and quarantine. Run this before relying on the stack as verified working software.
