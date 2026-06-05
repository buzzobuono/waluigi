from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok, data, color, table


def get_namespaces(session: WaluigiSession, output=None) -> None:
    try:
        r = session.http.get("/boss/namespaces", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [[ns.get("namespace"), ns.get("task_count")] for ns in rows],
            headers=["NAMESPACE", "TASKS"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_jobs(session: WaluigiSession, namespace=None, status=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/jobs", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        if status:
            rows = [j for j in rows if j.get("status", "").upper() == status.upper()]
        table(
            [[j.get("job_id"), j.get("kind", "Job"), color(j.get("status", "")),
              j.get("namespace", "-"), j.get("started_at", "-")] for j in rows],
            headers=["JOB_ID", "KIND", "STATUS", "NAMESPACE", "STARTED"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_tasks(session: WaluigiSession, namespace=None, job_id=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        params = {"job_id": job_id} if job_id else {}
        r = session.http.get(f"/boss/namespaces/{ns}/tasks",
                             params=params, headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [[t.get("id"), t.get("job_id"), t.get("params"),
              color(t.get("status", "")), t.get("last_update")] for t in rows],
            headers=["ID", "JOB_ID", "PARAMS", "STATUS", "LAST UPDATE"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_resources(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/resources", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        formatted = []
        for res in rows:
            amount    = res.get("amount", 0)
            usage     = res.get("usage",  0)
            available = amount - usage
            perc      = f"{usage / amount * 100:.1f}%" if amount > 0 else "n/a"
            formatted.append([res.get("name"), amount, usage, available, perc])
        table(formatted, headers=["NAME", "AMOUNT", "USAGE", "AVAILABLE", "UTIL%"],
              output_arg=output, raw=rows)
    except Exception as e:
        print(f"Error: {e}")


def get_workers(session: WaluigiSession, output=None) -> None:
    try:
        r = session.http.get("/boss/workers", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [[w.get("url"), color(w.get("status", "")),
              w.get("max_slots"), w.get("free_slots"), w.get("last_seen")] for w in rows],
            headers=["URL", "STATUS", "MAX_SLOTS", "FREE_SLOTS", "LAST_SEEN"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_task_definitions(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/task-definitions",
                             headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [[d.get("id"), d.get("kind", "TaskDefinition"), d.get("namespace")] for d in rows],
            headers=["ID", "KIND", "NAMESPACE"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_cron_jobs(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/cron-jobs", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [
                [
                    cj.get("id"),
                    (cj.get("spec") or {}).get("jobKind", "Job"),
                    (cj.get("spec") or {}).get("schedule", "-"),
                    (cj.get("spec") or {}).get("timezone", "UTC"),
                    "yes" if cj.get("enabled") else "no",
                    (cj.get("last_fire") or "-")[:19],
                ]
                for cj in rows
            ],
            headers=["ID", "KIND", "SCHEDULE", "TZ", "ENABLED", "LAST FIRE"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_job_definitions(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.get(f"/boss/namespaces/{ns}/job-definitions",
                             headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [
                [
                    d.get("id"),
                    d.get("namespace"),
                    len((d.get("spec") or {}).get("tasks", [])),
                ]
                for d in rows
            ],
            headers=["ID", "NAMESPACE", "TASKS"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


def get_users(session: WaluigiSession, output=None) -> None:
    try:
        r = session.http.get("/auth/users", headers=session.headers())
        if not ok(r): return
        rows = data(r)
        table(
            [[u.get("userid"), u.get("username"),
              ", ".join(u.get("namespaces") or []) or "—",
              (u.get("createdate") or "")[:19]] for u in rows],
            headers=["USERID", "DISPLAY NAME", "NAMESPACES", "CREATED"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")
