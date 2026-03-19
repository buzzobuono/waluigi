import sys
import json
import requests
import yaml
import argparse
from tabulate import tabulate

class WaluigiCLI:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        
    def apply(self, descriptor_path):
        with open(descriptor_path, 'r') as f:
            doc = yaml.safe_load(f)
          
        kind = doc.get('kind')
        spec = doc.get('spec')
        
        if kind == 'Job':
            r = requests.post(f"{self.base_url}/submit", json=doc)
            print(json.dumps(r.json(), indent=2))
        elif kind == 'ResourceQuota':
            print("Not yet implemented")
        else:
            print(f"❌ Tipo '{kind}' non supportato")
        return
        
    def describe_job(self, key):
        try:
            r = requests.get(f"{self.base_url}/api/active/describe/{key}")
            if r.status_code == 200:
                data = r.json()
                print(f"📋 Dettagli oggetto in memoria per: {key}")
            
                # Formattiamo i dettagli in una tabella verticale
                details = [[k, v] for k, v in data.items()]
                print(tabulate(details, tablefmt="fancy_grid"))
            else:
                print(f"❌ Key '{key}' non trovata in memoria.")
        except Exception as e:
            print(f"🔌 Errore: {e}")
    
    
    def get_namespaces(self):
        try:
            if r.status_code == 200:
                all_tasks = r.json()
                if not all_tasks:
                    print("📭 Nessun namespace attivo nel database.")
                    return

                # Raggruppamento per Namespace
                namespaces = {}
                for t in all_tasks:
                    ns = t['namespace']
                    if ns not in namespaces:
                        namespaces[ns] = {"tasks": 0, "completed": 0, "failed": 0, "running": 0}
                
                    namespaces[ns]["tasks"] += 1
                    if t['status'] == 'SUCCESS': namespaces[ns]["completed"] += 1
                    elif t['status'] == 'FAILED': namespaces[ns]["failed"] += 1
                    elif t['status'] == 'RUNNING': namespaces[ns]["running"] += 1

                # Preparazione tabella sintetica
                table_data = []
                for ns, stats in namespaces.items():
                    progress = f"{stats['completed']}/{stats['tasks']}"
                    status_str = "🟢 OK" if stats['failed'] == 0 else f"🔴 ERR ({stats['failed']})"
                    if stats['running'] > 0: status_str = "🟡 RUNNING"
                
                    table_data.append([ns, stats['tasks'], progress, status_str])

                headers = ["Namespace", "Total Tasks", "Progress", "State"]
                print(tabulate(table_data, headers=headers, tablefmt="outline"))
            else:
                print(f"❌ Errore del server: {r.status_code}")
        except Exception as e:
            print(f"🔌 Errore di connessione: {e}")
            
    def reset(self, scope, target):
        url = f"{self.base_url}/api/reset/{scope}/{target}"
        r = requests.post(url)
        print(f"Result {r.status_code}")

    def delete(self, scope, target):
        url = f"{self.base_url}/api/delete/{scope}/{target}"
        r = requests.post(url)
        print(f"Result {r.status_code}")
        
    def get_namespaces(self):
        try:
            r = requests.get(f"{self.base_url}/api/namespaces")
            if r.status_code == 200:
                data = r.json()
                if not data:
                    print("⚠️ No namespace found")
                    return
                table = []
                for ns in data:
                    id = ns.get("id")
                    task_count = ns.get("task_count")
                    table.append([id, task_count])
                headers = ["ID", "TASK COUNT" ]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"❌ Error: {r.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
    def get_jobs(self):
        try:
            r = requests.get(f"{self.base_url}/api/jobs")
            if r.status_code == 200:
                data = r.json()
                if not data:
                    print("⚠️ No jobs found")
                    return
                table = []
                for job in data:
                    id = job.get("id")
                    task_id = job.get("task_id")
                    params = job.get("params")
                    namespace = job.get("namespace")
                    status = job.get("status")
                    table.append([id, status, task_id, params, namespace ])
                headers = ["ID", "STATUS", "TASK ID", "PARAMS", "NAMESPACE" ]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"❌ Error: {r.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def get_tasks(self, job_id=None, namespace=None):
        try:
            r = requests.get(f"{self.base_url}/api/tasks")
            if r.status_code == 200:
                data = r.json()
                if job_id:
                    data = [t for t in data if t.get("job_id") == job_id]
                if namespace:
                    data = [t for t in data if t.get("namespace") == namespace]
                if not data:
                    print("⚠️ No task found")
                    return
                    
                headers = [ "ID", "JOB_ID", "PARAMS", "STATUS", "UPDATE", "NAMESPACE" ]
                table = []
                for task in data:
                    table.append([
                        task["id"],
                        task["job_id"],
                        task["params"],
                        task["status"],
                        task["update"],
                        task['namespace']
                    ])

                print(tabulate(table, headers=headers, tablefmt="plain"))
                
            else:
                print(f"❌ Error: {r.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
   
    def get_resources(self):
        try:
            r = requests.get(f"{self.base_url}/api/resources")
            if r.status_code == 200:
                data = r.json()
                if not data:
                    print("⚠️ No resources found")
                    return
                table = []
                for res_name in data['limits']:
                    limit = data['limits'][res_name]
                    usage = data['usage'].get(res_name, 0.0)
                    available = data['available'].get(res_name, limit)
                    perc = (usage / limit * 100) if limit > 0 else 0
                    status = f"{perc:.1f}%"
                    table.append([res_name, usage, limit, available, status])

                headers = ["ID", "USAGE", "LIMIT", "AVAILABLE", "STATUS"]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"❌ Error: {r.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")
   
    def get_workers(self):
        try:
            r = requests.get(f"{self.base_url}/api/workers")
            if r.status_code == 200:
                data = r.json()
                if not data:
                    print("⚠️ No worker found")
                    return
                table = []
                for worker in data:
                    url = worker.get("url", "N/A")
                    slots = worker.get("free_slots", "N/A")
                    status = worker.get("status", "N/A")
                    table.append([url, slots, status ])
                headers = ["URL", "SLOTS", "STATUS" ]
                print(tabulate(table, headers=headers, tablefmt="plain"))
            else:
                print(f"❌ Error: {r.status_code}")
        except Exception as e:
            print(f"❌ Error: {e}")

def main():
    parser = argparse.ArgumentParser(description='Waluigi CLI Control Panel')
    parser.add_argument('--url', default='http://localhost:8082', help='Boss URL')
    subparsers = parser.add_subparsers(dest='command', help='Comandi disponibili')

    apply_p = subparsers.add_parser('apply', help='Sottomette un job o risorse')
    apply_p.add_argument('-f', '--file', required=True, help='Path del descrittore JSON')
    
    describe_p = subparsers.add_parser('describe', help='Descrive gli oggetti attivi')
    describe_p.add_argument('type', choices=['namespace', 'job'])
    describe_p.add_argument('target', nargs='?', help='Oggetto target')
    
    get_p = subparsers.add_parser('get', help='Elenca gli oggetti attivi')
    get_p.add_argument('type', choices=['namespaces', 'jobs', 'tasks', 'resources', 'workers'])
    get_p.add_argument('-n', '--namespace', required=False, help='Namespace')
    get_p.add_argument('-j', '--job_id', required=False, help='Job ID')
    
    reset_p = subparsers.add_parser('reset', help='Resetta task o namespace')
    reset_p.add_argument('type', choices=['task', 'namespace'])
    reset_p.add_argument('target', help='ID del task o nome del namespace')

    del_p = subparsers.add_parser('delete', help='Elimina task o namespace')
    del_p.add_argument('type', choices=['task', 'namespace'])
    del_p.add_argument('target', help='ID del task o nome del namespace')

    args = parser.parse_args()
    cli = WaluigiCLI(args.url)

    if args.command == 'apply':
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
    elif args.command == 'describe':
        if args.type == 'job':
            if args.target:
                cli.describe_job(args.target)
    elif args.command == 'reset':
        if args.type == 'namespace':
            if args.target:
                cli.reset('namespace', args.target)
        if args.type == 'task':
            if args.target:
                cli.reset('task', args.target)
        
    elif args.command == 'delete':
        if args.type == 'namespace':
            if args.target:
                cli.delete('namespace', args.target)
        if args.type == 'task':
            if args.target:
                cli.delete('task', args.target)
                
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
