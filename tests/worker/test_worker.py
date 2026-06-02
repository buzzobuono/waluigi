import sys
import time

import requests


# ── Helpers ────────────────────────────────────────────────────────────────────

def _execute(worker_url, payload):
    ns = payload.get("namespace", "ns")
    return requests.post(f"{worker_url}/namespaces/{ns}/dispatch", json=payload, timeout=5)


def _wait_for_log(boss_url, task_id, marker, namespace="ns", timeout=8):
    """Poll Boss logs until marker appears in a log line."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{boss_url}/namespaces/{namespace}/tasks/{task_id}/logs",
                timeout=3,
            )
            if r.ok:
                entries = r.json().get("data", r.json())
                if any(marker in e.get("message", "") for e in entries):
                    return True
        except requests.RequestException:
            pass
        time.sleep(0.2)
    return False


def _run_and_wait(worker_url, boss_url, task_id, marker, **extra):
    """Submit a task and wait for the marker to appear in Boss logs."""
    r = _execute(worker_url, {
        "id": task_id,
        "job_id": "j1",
        "namespace": "ns",
        **extra,
    })
    assert r.status_code == 202, f"Expected 202, got {r.status_code}"
    ok = _wait_for_log(boss_url, task_id, marker)
    time.sleep(0.3)
    return ok


sys.argv = ["wlworker"]
from waluigi.worker.services.worker_service import _hash  # noqa: E402


def test_hash_empty_dict():
    assert _hash({}) == ""


def test_hash_single_item():
    assert _hash({"key": "value"}) == "key:value"


def test_hash_multiple_items_are_sorted():
    assert _hash({"z": "last", "a": "first"}) == "a:first z:last"


def test_hash_value_preserved_as_string():
    assert _hash({"date": "2024-01-01"}) == "date:2024-01-01"


# ── /execute — input validation ───────────────────────────────────────────────

def test_execute_400_missing_command_type_script(worker_url):
    r = _execute(worker_url, {
        "id": "v-missing",
        "job_id": "j1",
        "namespace": "ns",
    })
    assert r.status_code == 400
    assert "No type, command or script provided" in r.json()["diagnostic"]["messages"]


def test_execute_400_unknown_task_type(worker_url):
    r = _execute(worker_url, {
        "id": "v-badtype",
        "job_id": "j1",
        "namespace": "ns",
        "type": "NoSuchTask",
    })
    assert r.status_code == 400
    assert "Unknown task type" in r.json()["diagnostic"]["messages"]


def test_execute_202_command_accepted(worker_url, boss_url):
    r = _execute(worker_url, {
        "id": "v-cmd",
        "job_id": "j1",
        "namespace": "ns",
        "command": "echo DONE:v-cmd",
    })
    assert r.status_code == 202
    assert "Task submitted" in r.json()["diagnostic"]["messages"]
    assert _wait_for_log(boss_url, "v-cmd", "DONE:v-cmd")
    time.sleep(0.3)


def test_execute_202_script_accepted(worker_url, boss_url):
    r = _execute(worker_url, {
        "id": "v-script",
        "job_id": "j1",
        "namespace": "ns",
        "script": "print('DONE:v-script')",
    })
    assert r.status_code == 202
    assert _wait_for_log(boss_url, "v-script", "DONE:v-script")
    time.sleep(0.3)


def test_execute_202_known_task_type_accepted(worker_url):
    r = _execute(worker_url, {
        "id": "v-type",
        "job_id": "j1",
        "namespace": "ns",
        "type": "MergeDatasets",
    })
    assert r.status_code == 202
    time.sleep(2)


# ── /execute — command execution and Boss log callbacks ───────────────────────

def test_execute_command_output_logged_to_boss(worker_url, boss_url):
    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-logs",
        marker="DONE:e-logs",
        command="echo DONE:e-logs",
    ), "Command output never reached Boss logs"


def test_execute_failed_command_output_logged_to_boss(worker_url, boss_url):
    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-fail",
        marker="DONE:e-fail",
        command="echo DONE:e-fail && exit 1",
    ), "Failed command output never reached Boss logs"


def test_execute_injects_params_into_env(worker_url, boss_url):
    script = (
        "import os; "
        "v = os.environ.get('WALUIGI_PARAM_DATE', 'MISSING'); "
        "print(f'DONE:e-params param={v}')"
    )
    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-params",
        marker="param=2024-01-01",
        script=script,
        params={"date": "2024-01-01"},
    ), "WALUIGI_PARAM_DATE not injected into subprocess env"


def test_execute_injects_attributes_into_env(worker_url, boss_url):
    script = (
        "import os; "
        "v = os.environ.get('WALUIGI_ATTRIBUTE_OWNER', 'MISSING'); "
        "print(f'DONE:e-attrs attr={v}')"
    )
    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-attrs",
        marker="attr=team",
        script=script,
        attributes={"owner": "team"},
    ), "WALUIGI_ATTRIBUTE_OWNER not injected into subprocess env"


def test_execute_injects_task_and_job_ids(worker_url, boss_url):
    script = (
        "import os; "
        "t = os.environ.get('WALUIGI_TASK_ID', 'MISSING'); "
        "j = os.environ.get('WALUIGI_JOB_ID', 'MISSING'); "
        "print(f'DONE:e-ids task={t} job={j}')"
    )

    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-ids",
        marker="DONE:e-ids",
        script=script,
        job_id="j-test",
    )

    assert _wait_for_log(boss_url, "e-ids", "task=e-ids"), "WALUIGI_TASK_ID not injected"
    assert _wait_for_log(boss_url, "e-ids", "job=j-test"), "WALUIGI_JOB_ID not injected"


def test_execute_injects_config_as_json(worker_url, boss_url):
    script = (
        "import os, json; "
        "cfg = json.loads(os.environ.get('WALUIGI_CONFIG', '{}')); "
        "print(f\"DONE:e-cfg val={cfg.get('key', 'MISSING')}\")"
    )
    assert _run_and_wait(
        worker_url=worker_url,
        boss_url=boss_url,
        task_id="e-cfg",
        marker="val=val",
        script=script,
        config={"key": "val"},
    ), "WALUIGI_CONFIG not serialised as JSON in subprocess env"


# ── /execute — slot management ────────────────────────────────────────────────

def test_execute_429_when_slots_full(worker_url):
    time.sleep(1)

    r1 = _execute(worker_url, {
        "id": "s-busy1",
        "job_id": "j1",
        "namespace": "ns",
        "command": "sleep 10",
    })
    r2 = _execute(worker_url, {
        "id": "s-busy2",
        "job_id": "j1",
        "namespace": "ns",
        "command": "sleep 10",
    })

    assert r1.status_code == 202
    assert r2.status_code == 202

    r3 = _execute(worker_url, {
        "id": "s-busy3",
        "job_id": "j1",
        "namespace": "ns",
        "command": "echo hi",
    })
    assert r3.status_code == 429
    assert "Worker too busy. No slot available." in r3.json()["diagnostic"]["messages"]
    