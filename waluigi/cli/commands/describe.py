import json
from tabulate import tabulate

from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok, data, color, fmt_dt

# Zero-width space: invisible, not stripped by str.strip(), lets tabulate
# preserve any leading ASCII spaces that follow it.
_ZWS = '​'


# ── tree rendering helpers ────────────────────────────────────────────────────

def _fmt_params(params):
    if not params:
        return "-"
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            return params
    if isinstance(params, dict):
        return ", ".join(f"{k}:{v}" for k, v in params.items()) or "-"
    return str(params)


def _tree_rows_job(node, by_id, rows, prefix="", is_last=True):
    """Collect rows from a nested job spec tree (root at top, deps below)."""
    tid        = node.get("id", "?")
    t          = by_id.get(tid, {})
    connector  = ("└─ " if is_last else "├─ ") if prefix else ""
    display_id = _ZWS + prefix + connector + tid
    rows.append([
        display_id,
        color(t.get("status", "-")),
        _fmt_params(t.get("params")),
        fmt_dt(t.get("last_update")),
    ])
    children = node.get("requires", [])
    for i, child in enumerate(children):
        child_prefix = prefix + ("   " if is_last else "│  ")
        _tree_rows_job(child, by_id, rows, child_prefix, i == len(children) - 1)


def _tree_rows_defn(by_id, tid, rows, prefix="", is_last=True):
    """Collect rows from a flat job-definition task list (requires by id)."""
    t          = by_id.get(tid, {})
    connector  = ("└─ " if is_last else "├─ ") if prefix else ""
    display_id = _ZWS + prefix + connector + tid
    if "taskRef" in t:
        kind = f"ref:{t['taskRef'].get('name', '?')}"
    elif "taskSpec" in t:
        kind = "inline"
    else:
        kind = "-"
    resources = ", ".join(
        f"{k}:{v}" for k, v in (t.get("resources") or {}).items()
    ) or "-"
    rows.append([display_id, kind, resources])
    children = t.get("requires") or []
    for i, child_id in enumerate(children):
        if child_id in by_id:
            child_prefix = prefix + ("   " if is_last else "│  ")
            _tree_rows_defn(by_id, child_id, rows, child_prefix, i == len(children) - 1)


def _defn_root(tasks_list):
    """Return (by_id, root_id) for a flat task list with requires-by-id."""
    by_id        = {t["id"]: t for t in tasks_list if "id" in t}
    all_required = {dep for t in by_id.values() for dep in (t.get("requires") or [])}
    roots        = [tid for tid in by_id if tid not in all_required]
    return by_id, (roots[0] if len(roots) == 1 else None)


# ── describe functions ────────────────────────────────────────────────────────

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
            ["execution_policy",   job.get("execution_policy",  "Ephemeral")],
            ["concurrency_policy", job.get("concurrency_policy", "Forbid")],
            ["status",             color(job.get("status", ""))],
            ["started_at",         fmt_dt(job.get("started_at"))],
            ["locked_by",          job.get("locked_by")  or "-"],
            ["root_task",          spec.get("id", "-")],
        ]
        for k, v in meta.items():
            if k not in ("namespace", "timestamp", "executionPolicy"):
                summary.append([k, v])
        print(tabulate(summary, tablefmt="plain"))

        if tasks:
            by_id = {t.get("id"): t for t in tasks}
            rows  = []
            _tree_rows_job(spec, by_id, rows)
            print(f"\nTasks ({len(tasks)}):")
            print(tabulate(rows, headers=["TASK ID", "STATUS", "PARAMETERS", "UPDATED"],
                           tablefmt="plain"))
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
            ["last_update", fmt_dt(task.get("last_update"))],
        ], tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


def describe_cron_job(session: WaluigiSession, namespace=None,
                      cron_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/cron-jobs/{cron_id}",
                             headers=session.headers())
        if not ok(r): return
        cj = data(r)
        if output == "json":
            print(json.dumps(cj, indent=2)); return
        spec   = cj.get("spec") or {}
        params = spec.get("params") or {}
        attrs  = spec.get("attributes") or {}
        rows = [
            ["id",                cj.get("id")],
            ["namespace",         ns],
            ["enabled",           "yes" if cj.get("enabled") else "no"],
            ["schedule",          spec.get("schedule", "-")],
            ["timezone",          spec.get("timezone", "UTC")],
            ["executionPolicy",   spec.get("executionPolicy", "Ephemeral")],
            ["concurrencyPolicy", spec.get("concurrencyPolicy", "Forbid")],
            ["jobRef",            (spec.get("jobRef") or {}).get("name", "-")],
            ["last_fire",         fmt_dt(cj.get("last_fire"))],
        ]
        print(tabulate(rows, tablefmt="plain"))
        if params:
            print("\nParams:")
            print(tabulate([[k, v] for k, v in params.items()],
                           headers=["NAME", "VALUE / FORMAT"], tablefmt="plain"))
        if attrs:
            print("\nAttributes:")
            print(tabulate([[k, v] for k, v in attrs.items()],
                           headers=["NAME", "VALUE"], tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


def describe_job_definition(session: WaluigiSession, namespace=None,
                            defn_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/job-definitions/{defn_id}",
                             headers=session.headers())
        if not ok(r): return
        defn = data(r)
        if output == "json":
            print(json.dumps(defn, indent=2)); return
        meta = defn.get("metadata") or {}
        spec = defn.get("spec")     or {}
        print(tabulate([
            ["id",        defn.get("id")],
            ["namespace", ns],
            ["workdir",   meta.get("workdir") or "-"],
        ], tablefmt="plain"))
        tasks_list = spec.get("tasks") or []
        if tasks_list:
            by_id, root_id = _defn_root(tasks_list)
            rows = []
            if root_id:
                _tree_rows_defn(by_id, root_id, rows)
            else:
                for t in tasks_list:
                    if "id" in t:
                        _tree_rows_defn(by_id, t["id"], rows)
            print(f"\nTasks ({len(tasks_list)}):")
            print(tabulate(rows, headers=["TASK ID", "TYPE", "RESOURCES"],
                           tablefmt="plain"))
        else:
            print("\nNo tasks.")
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


def describe_namespace(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}", headers=session.headers())
        if not ok(r): return
        ov = data(r)

        if output == "json":
            print(json.dumps(ov, indent=2)); return

        print(f"\nNamespace: {ns}\n")

        # Tasks by status
        tasks = ov.get("tasks") or []
        task_counts: dict[str, int] = {}
        for t in tasks:
            s = t.get("status", "-")
            task_counts[s] = task_counts.get(s, 0) + 1
        print(f"Tasks ({len(tasks)}):")
        if task_counts:
            print(tabulate([[color(s), c] for s, c in sorted(task_counts.items())],
                           headers=["STATUS", "COUNT"], tablefmt="plain"))
        else:
            print("  none")

        # Jobs by status
        jobs = ov.get("jobs") or []
        job_counts: dict[str, int] = {}
        for j in jobs:
            s = j.get("status", "-")
            job_counts[s] = job_counts.get(s, 0) + 1
        print(f"\nJobs ({len(jobs)}):")
        if job_counts:
            print(tabulate([[color(s), c] for s, c in sorted(job_counts.items())],
                           headers=["STATUS", "COUNT"], tablefmt="plain"))
        else:
            print("  none")

        # CronJobs
        cron = ov.get("cron_jobs") or []
        print(f"\nCron Jobs ({len(cron)}):")
        if cron:
            print(tabulate(
                [[cj.get("id", "-"), "yes" if cj.get("enabled") else "no",
                  (cj.get("spec") or {}).get("schedule", "-")] for cj in cron],
                headers=["NAME", "ENABLED", "SCHEDULE"], tablefmt="plain",
            ))
        else:
            print("  none")

        # Job Definitions
        jd = ov.get("job_definitions") or []
        print(f"\nJob Definitions ({len(jd)}):")
        if jd:
            print(tabulate([[d.get("id", "-")] for d in jd],
                           headers=["NAME"], tablefmt="plain"))
        else:
            print("  none")

        # Task Definitions
        td = ov.get("task_definitions") or []
        print(f"\nTask Definitions ({len(td)}):")
        if td:
            print(tabulate([[d.get("id", "-")] for d in td],
                           headers=["NAME"], tablefmt="plain"))
        else:
            print("  none")

    except Exception as e:
        print(f"Error: {e}")
