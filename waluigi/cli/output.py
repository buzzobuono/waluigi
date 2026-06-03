import sys
import json
from tabulate import tabulate

_COLORS = {
    "PENDING":   "\033[90m",
    "READY":     "\033[96m",
    "RUNNING":   "\033[33m",
    "SUCCESS":   "\033[32m",
    "FAILED":    "\033[31m",
    "CANCELLED": "\033[35m",
    "PAUSED":    "\033[34m",
    "ALIVE":     "\033[32m",
}
_RESET = "\033[0m"


def color(status: str) -> str:
    if not sys.stdout.isatty():
        return status
    return _COLORS.get(status, "") + status + _RESET


def ok(r) -> bool:
    if r.status_code >= 400:
        try:
            body = r.json()
            msgs   = body.get("diagnostic", {}).get("messages", [])
            detail = body.get("detail")
            msg    = msgs[0] if msgs else (detail or r.text)
            print(f"Error {r.status_code}: {msg}")
        except Exception:
            print(f"Error {r.status_code}: {r.text}")
        return False
    return True


def data(r):
    body = r.json()
    return body.get("data", body)


def table(rows, headers, output_arg=None, raw=None):
    if output_arg == "json":
        print(json.dumps(raw if raw is not None else rows, indent=2))
    elif not rows:
        print("No results found.")
    else:
        print(tabulate(rows, headers=headers, tablefmt="plain"))
