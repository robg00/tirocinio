import subprocess
import time
import uuid

import pytest

COMPOSE = ["docker", "compose"]


def _list_running_job_ids(name_substring: str) -> list[str]:
    result = subprocess.run(
        [*COMPOSE, "exec", "flink-jobmanager", "/opt/flink/bin/flink", "list"],
        capture_output=True, text=True, timeout=15,
    )
    ids = []
    for line in result.stdout.split("\n"):
        if name_substring in line and "RUNNING" in line:
            job_id = line.split(":")[1].strip().split()[0]
            ids.append(job_id)
    return ids


def _submit_and_wait(job_name: str, script_path: str, group_id: str) -> str | None:
    existing = _list_running_job_ids(job_name)
    if existing:
        return existing[0]

    subprocess.run(
        [
            *COMPOSE, "exec", "-e", f"GROUP_ID={group_id}",
            "flink-jobmanager", "/opt/flink/bin/flink", "run", "-py",
            script_path, "-pyfs", "/src/etl/",
        ],
        capture_output=True, text=True, timeout=30,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        job_ids = _list_running_job_ids(job_name)
        if job_ids:
            return job_ids[0]
        time.sleep(3)
    return None


@pytest.fixture(scope="session")
def etl_job():
    job_id = _submit_and_wait("ETL Sales Job", "/src/etl/pyflink_etl.py", f"etl-test-{uuid.uuid4()}")
    if job_id is None:
        pytest.skip("ETL Sales Job did not start")
    yield job_id


@pytest.fixture(scope="session")
def anomaly_job():
    job_id = _submit_and_wait("Anomaly Detection Job", "/src/etl/pyflink_datastream_anomaly.py", f"anomaly-test-{uuid.uuid4()}")
    if job_id is None:
        pytest.skip("Anomaly Detection Job did not start")
    yield job_id
