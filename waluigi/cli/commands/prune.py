from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok


def prune_workers(session: WaluigiSession) -> None:
    try:
        r = session.http.post("/boss/workers/_prune", headers=session.headers())
        if ok(r):
            data    = r.json().get("data", {})
            removed = data.get("removed", [])
            if removed:
                for url in removed:
                    print(f"worker/{url} removed")
                print(f"\n{len(removed)} ghost worker(s) pruned")
            else:
                print("No ghost workers found")
    except Exception as e:
        print(f"Error: {e}")


def prune_prepare(session: WaluigiSession, worker_url: str | None = None) -> None:
    try:
        params = {"target": worker_url} if worker_url else {}
        r = session.http.delete("/boss/workers/prepare", headers=session.headers(), params=params)
        if ok(r):
            results = r.json().get("data", [])
            for entry in results:
                url    = entry.get("url", "?")
                status = entry.get("status", "?")
                if status == "cleared":
                    cleared = entry.get("cleared_bytes", 0)
                    print(f"worker/{url} prepare cleared ({cleared} bytes freed)")
                elif status == "busy":
                    msg = entry.get("message", "busy")
                    print(f"worker/{url} skipped: {msg}")
                elif status == "unreachable":
                    msg = entry.get("message", "")
                    print(f"worker/{url} unreachable: {msg}")
                else:
                    print(f"worker/{url} {status} (code {entry.get('code', '?')})")
    except Exception as e:
        print(f"Error: {e}")
