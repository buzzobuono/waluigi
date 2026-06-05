from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok


def pause(session: WaluigiSession, namespace=None, job_id=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_pause",
                              headers=session.headers())
        if ok(r): print(f"job/{job_id} paused")
    except Exception as e:
        print(f"Error: {e}")


def resume(session: WaluigiSession, namespace=None, job_id=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_resume",
                              headers=session.headers())
        if ok(r): print(f"job/{job_id} resumed")
    except Exception as e:
        print(f"Error: {e}")


def cancel(session: WaluigiSession, namespace=None, job_id=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_cancel",
                              headers=session.headers())
        if ok(r): print(f"job/{job_id} cancelled")
    except Exception as e:
        print(f"Error: {e}")


def enable_cron_job(session: WaluigiSession, namespace=None, cron_id=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.post(f"/boss/namespaces/{ns}/cron-jobs/{cron_id}/_enable",
                              headers=session.headers())
        if ok(r): print(f"cronjob/{cron_id} enabled")
    except Exception as e:
        print(f"Error: {e}")


def disable_cron_job(session: WaluigiSession, namespace=None, cron_id=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    try:
        r = session.http.post(f"/boss/namespaces/{ns}/cron-jobs/{cron_id}/_disable",
                              headers=session.headers())
        if ok(r): print(f"cronjob/{cron_id} disabled")
    except Exception as e:
        print(f"Error: {e}")


def reset(session: WaluigiSession, scope: str, target: str, namespace=None) -> None:
    try:
        if scope == "task":
            ns = session.resolve_namespace(namespace)
            if not ns: return
            r = session.http.post(f"/boss/namespaces/{ns}/tasks/{target}/_reset",
                                  headers=session.headers())
        elif scope == "job":
            ns = session.resolve_namespace(namespace)
            if not ns: return
            r = session.http.post(f"/boss/namespaces/{ns}/jobs/{target}/_reset",
                                  headers=session.headers())
        else:
            r = session.http.post(f"/boss/namespaces/{target}/_reset",
                                  headers=session.headers())
        if ok(r): print(f"{scope}/{target} reset to PENDING")
    except Exception as e:
        print(f"Error: {e}")


def delete(session: WaluigiSession, scope: str, target: str, namespace=None) -> None:
    try:
        if scope == "task":
            ns = session.resolve_namespace(namespace)
            if not ns: return
            r = session.http.delete(f"/boss/namespaces/{ns}/tasks/{target}",
                                    headers=session.headers())
        elif scope == "job":
            ns = session.resolve_namespace(namespace)
            if not ns: return
            r = session.http.delete(f"/boss/namespaces/{ns}/jobs/{target}",
                                    headers=session.headers())
        elif scope == "cronjob":
            ns = session.resolve_namespace(namespace)
            if not ns: return
            r = session.http.delete(f"/boss/namespaces/{ns}/cron-jobs/{target}",
                                    headers=session.headers())
        else:
            r = session.http.delete(f"/boss/namespaces/{target}",
                                    headers=session.headers())
        if ok(r): print(f"{scope}/{target} deleted")
    except Exception as e:
        print(f"Error: {e}")
