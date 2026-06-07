import time

from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import data, fmt_dt


def get_logs(session: WaluigiSession, namespace=None, task_id=None,
             limit: int = 20, follow: bool = False) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns: return
    last_seen_id = 0
    try:
        while True:
            params = {"limit": 100} if follow else {"limit": limit}
            r = session.http.get(f"/boss/namespaces/{ns}/tasks/{task_id}/logs",
                                 params=params, headers=session.headers())
            if r.status_code != 200:
                print(f"Error: {r.status_code}"); break
            logs     = data(r) or []
            new_logs = [entry for entry in logs if entry.get("id", 0) > last_seen_id]
            for entry in new_logs:
                ts        = fmt_dt(entry.get("timestamp", ""))
                worker_id = entry.get("worker_id", "???")
                message   = entry.get("message", "")
                print(f"{ts}  [{worker_id}]  {message}")
                last_seen_id = max(last_seen_id, entry.get("id", 0))
            if not follow:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nFollow interrupted.")
