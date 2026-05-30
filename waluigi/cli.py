import sys
import json
import yaml
import argparse
import time
import os
import base64
from pathlib import Path
from tabulate import tabulate

from waluigi.commons.http import HttpClient

# ── ANSI status colors ────────────────────────────────────────────────────────

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


def _color(status: str) -> str:
    if not sys.stdout.isatty():
        return status
    return _COLORS.get(status, "") + status + _RESET


# ── Response helpers ──────────────────────────────────────────────────────────

def _ok(r) -> bool:
    if r.status_code >= 400:
        try:
            body = r.json()
            msgs = body.get("diagnostic", {}).get("messages", [])
            detail = body.get("detail")
            msg = msgs[0] if msgs else (detail or r.text)
            print(f"Error {r.status_code}: {msg}")
        except Exception:
            print(f"Error {r.status_code}: {r.text}")
        return False
    return True


def _data(r):
    return r.json().get("data", r.json())


def _parse_json_field(value):
    """Parse a DB field that may be a JSON string or already a dict."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return value or {}


# ── CLI ───────────────────────────────────────────────────────────────────────

class WaluigiCLI:
    def __init__(self, base_url):
        self.base_url   = base_url.rstrip('/')
        self.config_dir = Path.home() / '.waluigi'
        self.token_file = self.config_dir / 'token'
        self._http      = HttpClient(self.base_url)
        if not self.config_dir.exists():
            self.config_dir.mkdir(parents=True, exist_ok=True)

    def _save_token(self, token):
        with open(self.token_file, 'w') as f:
            f.write(token)

    def _get_token(self):
        if self.token_file.exists():
            with open(self.token_file, 'r') as f:
                return f.read().strip()
        return None

    def _headers(self):
        token = self._get_token()
        return {'Authorization': f"Bearer {token}"} if token else {}

    def _token_namespaces(self):
        """Return the namespaces claim from the stored JWT, or None on error."""
        token = self._get_token()
        if not token:
            return None
        try:
            payload = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '=='))
            return payload.get('namespaces')
        except Exception:
            return None

    def _resolve_namespace(self, namespace_arg: str | None) -> str | None:
        """
        Resolve the effective namespace:
          1. Use the explicit argument if provided.
          2. Auto-detect if the token contains exactly one namespace.
          3. Print an error and return None if ambiguous or missing.
        """
        if namespace_arg:
            return namespace_arg
        ns = self._token_namespaces()
        if ns == '*':
            print("Error: namespace required for admin users. Use -n/--namespace.")
            return None
        if isinstance(ns, list):
            if len(ns) == 1:
                return ns[0]
            if len(ns) > 1:
                print(f"Error: multiple namespaces {ns}. Use -n/--namespace.")
                return None
        print("Error: namespace required. Use -n/--namespace.")
        return None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def login(self, username, password):
        try:
            r = self._http.post("/auth/login", json={"username": username, "password": password})
            if r.status_code == 200:
                data = r.json()
                token = data.get("token")
                if token:
                    self._save_token(token)
                    ns = data.get("namespaces", [])
                    ns_str = "*" if ns == "*" else ", ".join(ns) if ns else "(none)"
                    print(f"Login successful. Namespaces: {ns_str}")
                else:
                    print("Error: No token received from server.")
            else:
                print(f"Unauthorized: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")

    def logout(self):
        try:
            if self.token_file.exists():
                os.remove(self.token_file)
                print("Logout successful. Token removed.")
            else:
                print("No active session found.")
        except Exception as e:
            print(f"Error: {e}")

    # ── Apply ─────────────────────────────────────────────────────────────────

    def apply(self, descriptor_path, namespace_override=None):
        try:
            with open(descriptor_path, 'r') as f:
                doc = yaml.safe_load(f)
            kind = doc.get('kind')

            if kind in ('StatefulJob', 'Job'):
                ns = namespace_override or doc.get('metadata', {}).get('namespace')
                if not ns:
                    ns = self._resolve_namespace(None)
                if not ns:
                    return
                r = self._http.post(
                    f"/boss/namespaces/{ns}/jobs",
                    json=doc, headers=self._headers(),
                )
            elif kind == 'ClusterResources':
                r = self._http.post("/boss/resources", json=doc, headers=self._headers())
            else:
                print(f"Error: Kind '{kind}' not supported")
                return
            print(json.dumps(r.json(), indent=2))
        except Exception as e:
            print(f"Error: {e}")

    # ── Get ───────────────────────────────────────────────────────────────────

    def get_namespaces(self, output=None):
        try:
            r = self._http.get("/boss/namespaces", headers=self._headers())
            if not _ok(r): return
            data = _data(r)
            if output == "json":
                print(json.dumps(data, indent=2)); return
            if not data:
                print("No namespaces found."); return
            table = [[ns.get("namespace"), ns.get("task_count")] for ns in data]
            print(tabulate(table, headers=["NAMESPACE", "TASKS"], tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    def get_jobs(self, namespace=None, status=None, output=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.get(f"/boss/namespaces/{ns}/jobs", headers=self._headers())
            if not _ok(r): return
            data = _data(r)
            if status:
                data = [j for j in data if j.get("status", "").upper() == status.upper()]
            if output == "json":
                print(json.dumps(data, indent=2)); return
            if not data:
                print("No jobs found."); return
            table = [
                [j.get("job_id"), _color(j.get("status", "")), j.get("started_at") or "-"]
                for j in data
            ]
            print(tabulate(table, headers=["JOB_ID", "STATUS", "STARTED"], tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    def get_tasks(self, namespace=None, job_id=None, output=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            params = {}
            if job_id: params["job_id"] = job_id
            r = self._http.get(f"/boss/namespaces/{ns}/tasks", params=params, headers=self._headers())
            if not _ok(r): return
            data = _data(r)
            if output == "json":
                print(json.dumps(data, indent=2)); return
            if not data:
                print("No tasks found."); return
            table = [
                [t.get("id"), t.get("job_id"), t.get("params"),
                 _color(t.get("status", "")), t.get("last_update")]
                for t in data
            ]
            print(tabulate(table, headers=["ID", "JOB_ID", "PARAMS", "STATUS", "LAST UPDATE"], tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    def get_resources(self, output=None):
        try:
            r = self._http.get("/boss/resources", headers=self._headers())
            if not _ok(r): return
            data = _data(r)
            if output == "json":
                print(json.dumps(data, indent=2)); return
            if not data:
                print("No resources found."); return
            table = []
            for res in data:
                amount    = res.get("amount", 0)
                usage     = res.get("usage",  0)
                available = amount - usage
                perc = f"{usage / amount * 100:.1f}%" if amount > 0 else "n/a"
                table.append([res.get("name"), amount, usage, available, perc])
            print(tabulate(table, headers=["NAME", "AMOUNT", "USAGE", "AVAILABLE", "UTIL%"], tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    def get_workers(self, output=None):
        try:
            r = self._http.get("/boss/workers", headers=self._headers())
            if not _ok(r): return
            data = _data(r)
            if output == "json":
                print(json.dumps(data, indent=2)); return
            if not data:
                print("No workers found."); return
            table = [
                [w.get("url"), _color(w.get("status", "")),
                 w.get("max_slots"), w.get("free_slots"), w.get("last_seen")]
                for w in data
            ]
            print(tabulate(table, headers=["URL", "STATUS", "MAX_SLOTS", "FREE_SLOTS", "LAST_SEEN"], tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    # ── Describe ──────────────────────────────────────────────────────────────

    def describe_job(self, namespace=None, job_id=None, output=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.get(f"/boss/namespaces/{ns}/jobs", headers=self._headers())
            if not _ok(r): return
            job = next((j for j in _data(r) if j.get("job_id") == job_id), None)
            if not job:
                print(f"job '{job_id}' not found in namespace '{ns}'."); return

            r2    = self._http.get(f"/boss/namespaces/{ns}/tasks",
                                   params={"job_id": job_id}, headers=self._headers())
            tasks = _data(r2) if _ok(r2) else []

            if output == "json":
                print(json.dumps({"job": job, "tasks": tasks}, indent=2)); return

            meta = _parse_json_field(job.get("metadata", {}))
            spec = _parse_json_field(job.get("spec", {}))

            print(f"\nJob: {job_id}  (namespace: {ns})")
            summary = [
                ["status",     _color(job.get("status", ""))],
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
                table = [
                    [t.get("id"), t.get("params"), _color(t.get("status", "")), t.get("last_update")]
                    for t in tasks
                ]
                print(tabulate(table, headers=["ID", "PARAMS", "STATUS", "LAST UPDATE"], tablefmt="plain"))
            else:
                print("\nNo tasks.")
        except Exception as e:
            print(f"Error: {e}")

    def describe_task(self, namespace=None, task_id=None, output=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.get(f"/boss/namespaces/{ns}/tasks", headers=self._headers())
            if not _ok(r): return
            task = next((t for t in _data(r) if t.get("id") == task_id), None)
            if not task:
                print(f"task '{task_id}' not found in namespace '{ns}'."); return
            if output == "json":
                print(json.dumps(task, indent=2)); return
            rows = [
                ["id",          task.get("id")],
                ["namespace",   ns],
                ["status",      _color(task.get("status", ""))],
                ["job_id",      task.get("job_id",     "-")],
                ["parent_id",   task.get("parent_id")  or "-"],
                ["params",      task.get("params",     "-")],
                ["attributes",  task.get("attributes") or "-"],
                ["last_update", task.get("last_update","-")],
            ]
            print(tabulate(rows, tablefmt="plain"))
        except Exception as e:
            print(f"Error: {e}")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def pause(self, namespace=None, job_id=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_pause", headers=self._headers())
            if _ok(r): print(f"job/{job_id} paused")
        except Exception as e:
            print(f"Error: {e}")

    def resume(self, namespace=None, job_id=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_resume", headers=self._headers())
            if _ok(r): print(f"job/{job_id} resumed")
        except Exception as e:
            print(f"Error: {e}")

    def cancel(self, namespace=None, job_id=None):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        try:
            r = self._http.post(f"/boss/namespaces/{ns}/jobs/{job_id}/_cancel", headers=self._headers())
            if _ok(r): print(f"job/{job_id} cancelled")
        except Exception as e:
            print(f"Error: {e}")

    def reset(self, scope, target, namespace=None):
        try:
            if scope == "task":
                ns = self._resolve_namespace(namespace)
                if not ns: return
                r = self._http.post(f"/boss/namespaces/{ns}/tasks/{target}/_reset", headers=self._headers())
            elif scope == "job":
                ns = self._resolve_namespace(namespace)
                if not ns: return
                r = self._http.post(f"/boss/namespaces/{ns}/jobs/{target}/_reset", headers=self._headers())
            else:
                # reset namespace: target IS the namespace
                r = self._http.post(f"/boss/namespaces/{target}/_reset", headers=self._headers())
            if _ok(r): print(f"{scope}/{target} reset to PENDING")
        except Exception as e:
            print(f"Error: {e}")

    def delete(self, scope, target, namespace=None):
        try:
            if scope == "task":
                ns = self._resolve_namespace(namespace)
                if not ns: return
                r = self._http.delete(f"/boss/namespaces/{ns}/tasks/{target}", headers=self._headers())
            elif scope == "job":
                ns = self._resolve_namespace(namespace)
                if not ns: return
                r = self._http.delete(f"/boss/namespaces/{ns}/jobs/{target}", headers=self._headers())
            else:
                # delete namespace: target IS the namespace
                r = self._http.delete(f"/boss/namespaces/{target}", headers=self._headers())
            if _ok(r): print(f"{scope}/{target} deleted")
        except Exception as e:
            print(f"Error: {e}")

    # ── Logs ──────────────────────────────────────────────────────────────────

    def get_logs(self, namespace=None, task_id=None, limit=20, follow=False):
        ns = self._resolve_namespace(namespace)
        if not ns: return
        last_seen_id = 0
        try:
            while True:
                params = {'limit': 100} if follow else {'limit': limit}
                r = self._http.get(f"/boss/namespaces/{ns}/tasks/{task_id}/logs",
                                   params=params, headers=self._headers())
                if r.status_code != 200:
                    print(f"Error: {r.status_code}"); break
                logs     = _data(r) or []
                new_logs = [l for l in logs if l.get('id', 0) > last_seen_id]
                for entry in new_logs:
                    print(f"[{entry.get('timestamp', 'N/A')}] [{entry.get('worker_id', '???')}] {entry.get('message', '')}")
                    last_seen_id = max(last_seen_id, entry.get('id', 0))
                if not follow:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nFollow interrupted.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(prog="wlctl", description="Waluigi CLI")
    parser.add_argument('--url', default='http://localhost:8080',
                        help='Console base URL (default: http://localhost:8080)')
    sub = parser.add_subparsers(dest='command')

    # login / logout
    p = sub.add_parser('login', help='Authenticate and save token')
    p.add_argument('-u', '--username', required=True)
    p.add_argument('-p', '--password', required=True)
    sub.add_parser('logout', help='Remove saved token')

    # apply
    p = sub.add_parser('apply', help='Submit a Job or ClusterResources YAML descriptor')
    p.add_argument('-f', '--file',      required=True, help='Path to YAML file')
    p.add_argument('-n', '--namespace', help='Override namespace from descriptor metadata')

    # get
    p = sub.add_parser('get', help='List resources')
    p.add_argument('type', choices=['namespaces', 'jobs', 'tasks', 'resources', 'workers'])
    p.add_argument('-n', '--namespace', help='Namespace (required for jobs/tasks; auto-detected if token has one)')
    p.add_argument('-j', '--job_id',    help='Filter tasks by job ID')
    p.add_argument('-s', '--status',    help='Filter jobs by status (PENDING|RUNNING|SUCCESS|FAILED|CANCELLED|PAUSED)')
    p.add_argument('-o', '--output',    choices=['json'], help='Output format')

    # describe
    p = sub.add_parser('describe', help='Show full details of a job or task')
    p.add_argument('type',   choices=['job', 'task'])
    p.add_argument('target', help='Job ID or task ID')
    p.add_argument('-n', '--namespace', help='Namespace (auto-detected if token has one)')
    p.add_argument('-o', '--output',    choices=['json'], help='Output format')

    # cancel / pause / resume
    for cmd in ('cancel', 'pause', 'resume'):
        p = sub.add_parser(cmd, help=f'{cmd.capitalize()} a job')
        p.add_argument('type',   choices=['job'])
        p.add_argument('target', help='Job ID')
        p.add_argument('-n', '--namespace', help='Namespace (auto-detected if token has one)')

    # reset
    p = sub.add_parser('reset', help='Reset a task, job, or namespace to PENDING')
    p.add_argument('type',   choices=['task', 'job', 'namespace'])
    p.add_argument('target', help='Task/job ID, or namespace name')
    p.add_argument('-n', '--namespace',
                   help='Namespace for task/job (not needed when resetting a namespace)')

    # delete
    p = sub.add_parser('delete', help='Delete a task, job, or namespace')
    p.add_argument('type',   choices=['task', 'job', 'namespace'])
    p.add_argument('target', help='Task/job ID, or namespace name')
    p.add_argument('-n', '--namespace',
                   help='Namespace for task/job (not needed when deleting a namespace)')

    # logs
    p = sub.add_parser('logs', help='Fetch task logs')
    p.add_argument('task_id')
    p.add_argument('-n', '--namespace', help='Namespace (auto-detected if token has one)')
    p.add_argument('-l', '--lines',  type=int, default=20, help='Number of lines (default: 20)')
    p.add_argument('-f', '--follow', action='store_true',  help='Stream logs in real time')

    args = parser.parse_args()
    if not args.command:
        parser.print_help(); return

    cli = WaluigiCLI(args.url)
    out = getattr(args, 'output', None)
    ns  = getattr(args, 'namespace', None)

    if args.command == 'login':
        cli.login(args.username, args.password)
    elif args.command == 'logout':
        cli.logout()
    elif args.command == 'apply':
        cli.apply(args.file, namespace_override=ns)
    elif args.command == 'get':
        if   args.type == 'namespaces': cli.get_namespaces(output=out)
        elif args.type == 'jobs':       cli.get_jobs(namespace=ns, status=args.status, output=out)
        elif args.type == 'tasks':      cli.get_tasks(namespace=ns, job_id=args.job_id, output=out)
        elif args.type == 'resources':  cli.get_resources(output=out)
        elif args.type == 'workers':    cli.get_workers(output=out)
    elif args.command == 'describe':
        if   args.type == 'job':  cli.describe_job(namespace=ns, job_id=args.target, output=out)
        elif args.type == 'task': cli.describe_task(namespace=ns, task_id=args.target, output=out)
    elif args.command == 'cancel':
        cli.cancel(namespace=ns, job_id=args.target)
    elif args.command == 'pause':
        cli.pause(namespace=ns, job_id=args.target)
    elif args.command == 'resume':
        cli.resume(namespace=ns, job_id=args.target)
    elif args.command == 'reset':
        cli.reset(args.type, args.target, namespace=ns)
    elif args.command == 'delete':
        cli.delete(args.type, args.target, namespace=ns)
    elif args.command == 'logs':
        cli.get_logs(namespace=ns, task_id=args.task_id, limit=args.lines, follow=args.follow)


if __name__ == "__main__":
    main()
