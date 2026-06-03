import json
from tabulate import tabulate

from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok, data, color


def describe_job(session: WaluigiSession, namespace=None, job_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/jobs/{job_id}", headers=session.headers())
        if not ok(r): return
        job = data(r)

        r2    = session.http.get(f"/boss/namespaces/{ns}/tasks",
                                 params={"job_id": job_id}, headers=session.headers())
        tasks = data(r2) if ok(r2) else []

        if output == "json":
            print(json.dumps({"job": job, "tasks": tasks}, indent=2)); return

        meta = job.get("metadata") or {}
        spec = job.get("spec")     or {}
        print(f"\nJob: {job_id}  (namespace: {ns})")
        summary = [
            ["kind",       job.get("kind", "Job")],
            ["status",     color(job.get("status", ""))],
            ["started_at", job.get("started_at") or "-"],
            ["locked_by",  job.get("locked_by")  or "-"],
            ["root_task",  spec.get("id", "-")],
        ]
        for k, v in meta.items():
            if k not in ("namespace", "timestamp"):
                summary.append([k, v])
        print(tabulate(summary, tablefmt="plain"))

        if tasks:
            print(f"\nTasks ({len(tasks)}):")
            print(tabulate(
                [[t.get("id"), t.get("params"), color(t.get("status", "")), t.get("last_update")]
                 for t in tasks],
                headers=["ID", "PARAMS", "STATUS", "LAST UPDATE"],
                tablefmt="plain",
            ))
        else:
            print("\nNo tasks.")
    except Exception as e:
        print(f"Error: {e}")


def describe_task(session: WaluigiSession, namespace=None, task_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/tasks", headers=session.headers())
        if not ok(r): return
        task = next((t for t in data(r) if t.get("id") == task_id), None)
        if not task:
            print(f"task '{task_id}' not found in namespace '{ns}'."); return
        if output == "json":
            print(json.dumps(task, indent=2)); return
        print(tabulate([
            ["id",          task.get("id")],
            ["namespace",   ns],
            ["status",      color(task.get("status", ""))],
            ["job_id",      task.get("job_id",     "-")],
            ["parent_id",   task.get("parent_id")  or "-"],
            ["params",      task.get("params",     "-")],
            ["attributes",  task.get("attributes") or "-"],
            ["last_update", task.get("last_update", "-")],
        ], tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


def describe_task_definition(session: WaluigiSession, namespace=None,
                             defn_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/task-definitions/{defn_id}",
                             headers=session.headers())
        if not ok(r): return
        defn = data(r)
        if output == "json":
            print(json.dumps(defn, indent=2)); return
        meta = defn.get("metadata") or {}
        spec = defn.get("spec")     or {}
        rows = [
            ["id",        defn.get("id")],
            ["namespace", ns],
            ["kind",      defn.get("kind", "TaskDefinition")],
        ]
        for k, v in meta.items():
            if k not in ("name", "namespace"):
                rows.append([k, v])
        rows.append(["---", "---"])
        for k, v in spec.items():
            rows.append([k, v])
        print(tabulate(rows, tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")
