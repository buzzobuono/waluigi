import sys
import json
from datetime import datetime, timezone
from tabulate import tabulate

_STATUS_NAMES = {
    400: "BadRequest",
    401: "Unauthorized",
    403: "Forbidden",
    404: "NotFound",
    409: "Conflict",
    422: "Invalid",
    500: "InternalError",
    503: "ServiceUnavailable",
}

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
        except Exception:
            msg = r.text
        reason = _STATUS_NAMES.get(r.status_code, f"Error{r.status_code}")
        print(f"Error from server ({reason}): {msg}", file=sys.stderr)
        return False
    return True


def data(r):
    body = r.json()
    return body.get("data", body)


def fmt_dt(value) -> str:
    """Convert an ISO UTC timestamp to local time — same as new Date(v).toLocaleString()."""
    if not value or value == "-":
        return "-"
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def table(rows, headers, output_arg=None, raw=None):
    if output_arg == "json":
        print(json.dumps(raw if raw is not None else rows, indent=2))
    elif not rows:
        print("No results found.")
    else:
        print(tabulate(rows, headers=headers, tablefmt="plain", disable_numparse=True))
