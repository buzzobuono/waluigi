import sys
import json
import yaml
import argparse
import time
import os
from pathlib import Path
from tabulate import tabulate

from waluigi.commons.http import HttpClient

class WaluigiCLI:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        self.config_dir = Path.home() / '.waluigi'
        self.token_file = self.config_dir / 'token'
        self._http = HttpClient(self.base_url)
        self._ensure_config_dir()

    def _ensure_config_dir(self):
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

    def _get_headers(self):
        headers = {}
        token = self._get_token()
        if token:
            headers['Authorization'] = f"Bearer {token}"
        return headers

    def login(self, username, password):
        try:
            payload = {
                "username": username,
                "password": password
            }
            r = self._http.post("/auth/login", json=payload)
            if r.status_code == 200:
                data = r.json()
                token = data.get("token")
                if token:
                    self._save_token(token)
                    print("Login successful. Token saved.")
                else:
                    print("Error: No token received from server.")
            else:
                print(f"User not authorized: {r.status_code}")
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
            print(f"Error during logout: {e}")
    
    def apply(self, descriptor_path):
        try:
            with open(descriptor_path, 'r') as f:
                doc = yaml.safe_load(f)
            
            kind = doc.get('kind')
            
            if kind in ('StatefulJob', 'Job'):
                r = self._http.post("/boss/jobs", json=doc, headers=self._get_headers())
                print(json.dumps(r.json(), indent=2))
            elif kind == 'ClusterResources':
                r = self._http.post("/boss/resources", json=doc, headers=self._get_headers())
                print(json.dumps(r.json(), indent=2))
            else:
                print(f"Error: Type '{kind}' not supported")
        except Exception as e:
            print(f"Error during apply: {e}")
        
    def describe_job(self, key):
        try:
            r = self._http.get(f"/boss/api/active/describe/{key}", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json()
                print(f"Object details in memory for: {key}")
                details = [[k, v] for k, v in data.items()]
                print(tabulate(details, tablefmt="fancy_grid"))
            else:
                print(f"Error: Key '{key}' not found in memory.")
        except Exception as e:
            print(f"Connection error: {e}")
            
    def reset(self, scope, target):
        if scope == "task":
            r = self._http.post(f"/boss/tasks/{target}/_reset", headers=self._get_headers())
        else:
            r = self._http.post(f"/boss/namespaces/{target}/_reset", headers=self._get_headers())
        print(f"Result code: {r.status_code}")

    def delete(self, scope, target):
        if scope == "task":
            r = self._http.delete(f"/boss/tasks/{target}", headers=self._get_headers())
        else:
            r = self._http.delete(f"/boss/namespaces/{target}", headers=self._get_headers())
        print(f"Result code: {r.status_code}")
        
    def get_namespaces(self):
        try:
            r = self._http.get("/boss/namespaces", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if not data:
                    print("Warning: No namespace found")
                    return
                table = []
                for ns in data:
                    namespace = ns.get("namespace")
                    task_count = ns.get("task_count")
                    table.append([namespace, task_count])
                headers = ["NAMESPACE", "TASK COUNT" ]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"Error: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")
        
    def get_jobs(self):
        try:
            r = self._http.get("/boss/jobs", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if not data:
                    print("Warning: No jobs found")
                    return
                table = []
                for job in data:
                    id = job.get("job_id")
                    status = job.get("status")
                    table.append([id, status])
                headers = ["ID", "STATUS" ]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"Error: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")
    
    def get_tasks(self, job_id=None, namespace=None):
        try:
            r = self._http.get("/boss/tasks", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if job_id:
                    data = [t for t in data if t.get("job_id") == job_id]
                if namespace:
                    data = [t for t in data if t.get("namespace") == namespace]
                if not data:
                    print("Warning: No task found")
                    return
                    
                headers = [ "ID", "JOB_ID", "PARAMS", "STATUS", "LAST UPDATE", "NAMESPACE" ]
                table = []
                for task in data:
                    table.append([
                        task["id"],
                        task["job_id"],
                        task["params"],
                        task["status"],
                        task["last_update"],
                        task['namespace']
                    ])
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"Error: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")
   
    def get_resources(self):
        try:
            r = self._http.get("/boss/resources", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if not data:
                    print("Warning: No resources found")
                    return
                table = []
                for res in data:
                    name = res.get("name")
                    amount = res.get("amount")
                    usage = res.get("usage")
                    available = res.get("available")
                    perc = (usage / amount * 100) if amount > 0 else 0
                    status = f"{perc:.1f}%"
                    table.append([name, amount, usage, available, status ])
                headers = ["NAME", "AMOUNT", "USAGE", "AVAILABLE", "STATUS"]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"Error: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")
   
    def get_workers(self):
        try:
            r = self._http.get("/boss/workers", headers=self._get_headers())
            if r.status_code == 200:
                data = r.json().get('data')
                if not data:
                    print("Warning: No worker found")
                    return
                table = []
                for worker in data:
                    url = worker.get("url")
                    status = worker.get("status")
                    max_slots = worker.get("max_slots")
                    free_slots = worker.get("free_slots")
                    last_seen = worker.get("last_seen")
                    
                    table.append([url, status, max_slots, free_slots, last_seen ])
                headers = ["URL", "STATUS", "MAX_SLOTS", "FREE_SLOTS", "LAST_SEEN",]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"Error: {r.status_code}")
        except Exception as e:
            print(f"Error: {e}")

    def get_logs(self, task_id, limit=20, follow=False):
        last_seen_id = 0
        try:
            while True:
                params = {}
                if follow and last_seen_id > 0:
                    params = {'limit': 100, 'after_id': last_seen_id} 
                else:
                    params = {'limit': limit}
                
                r = self._http.get(f"/boss/tasks/{task_id}/logs", params=params, headers=self._get_headers())
                if r.status_code != 200:
                    print(f"Error: {r.status_code}")
                    break
                logs = r.json().get('data')
                if not logs and not follow:
                    break
                new_logs = [l for l in logs if l.get('id', 0) > last_seen_id]
                for entry in new_logs:
                    ts = entry.get('timestamp', 'N/A')
                    wid = entry.get('worker_id', '???')
                    msg = entry.get('message', '')
                    print(f"[{ts}] [{wid}] {msg}")
                    last_seen_id = max(last_seen_id, entry.get('id', 0))
                if not follow:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nFollow interrupted.")

def main():
    parser = argparse.ArgumentParser(description='Waluigi CLI Control Panel')
    parser.add_argument('--url', default='http://localhost:8080', help='Console URL')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    login_p = subparsers.add_parser('login', help='Authorize user')
    login_p.add_argument('-u', '--username', required=True, help='Username')
    login_p.add_argument('-p', '--password', required=True, help='Password')
    
    logout_p = subparsers.add_parser('logout', help='Remove saved authentication token')

    apply_p = subparsers.add_parser('apply', help='Submit a job or resources')
    apply_p.add_argument('-f', '--file', required=True, help='Descriptor file path')
    
    describe_p = subparsers.add_parser('describe', help='Describe active objects')
    describe_p.add_argument('type', choices=['namespace', 'job'])
    describe_p.add_argument('target', nargs='?', help='Target object')
    
    get_p = subparsers.add_parser('get', help='List active objects')
    get_p.add_argument('type', choices=['namespaces', 'jobs', 'tasks', 'resources', 'workers'])
    get_p.add_argument('-n', '--namespace', required=False, help='Namespace')
    get_p.add_argument('-j', '--job_id', required=False, help='Job ID')
    
    logs_p = subparsers.add_parser('logs', help='View task logs')
    logs_p.add_argument('task_id', help='Task ID')
    logs_p.add_argument('-n', '--lines', type=int, default=20, help='Lines to show (default 20)')
    logs_p.add_argument('-f', '--follow', action='store_true', help='Follow logs in real time')
    
    reset_p = subparsers.add_parser('reset', help='Reset task or namespace')
    reset_p.add_argument('type', choices=['task', 'namespace'])
    reset_p.add_argument('target', help='Task ID or Namespace name')

    del_p = subparsers.add_parser('delete', help='Delete task or namespace')
    del_p.add_argument('type', choices=['task', 'namespace'])
    del_p.add_argument('target', help='Task ID or Namespace name')

    args = parser.parse_args()
    cli = WaluigiCLI(args.url)

    if args.command == 'login':
        cli.login(args.username, args.password)
    elif args.command == 'logout':
        cli.logout()
    elif args.command == 'apply':
        cli.apply(args.file)
    elif args.command == 'get':
        if args.type == 'namespaces':
            cli.get_namespaces()
        elif args.type == 'jobs':
            cli.get_jobs()
        elif args.type == 'tasks':
            cli.get_tasks(args.job_id, args.namespace)           
        elif args.type == 'resources':
            cli.get_resources()
        elif args.type == 'workers':
            cli.get_workers()
    elif args.command == 'logs':
        cli.get_logs(args.task_id, limit=args.lines, follow=args.follow)
    elif args.command == 'describe':
        if args.type == 'job':
            if args.target:
                cli.describe_job(args.target)
    elif args.command == 'reset':
        if args.target:
            cli.reset(args.type, args.target)
    elif args.command == 'delete':
        if args.target:
            cli.delete(args.type, args.target)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
