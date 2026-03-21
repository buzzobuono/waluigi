import importlib
import threading
import time
import sys
import os
import socket
import configargparse
from flask import Flask, request, jsonify
from waluigi.core.db import WaluigiDB
from waluigi.core.scheduler_engine import WaluigiSchedulerEngine
from waluigi.core.dynamic_task import DynamicTask

class ParseResources(configargparse.Action):
    """Trasforma 'water:100,food:16' in {'water': 100.0, 'food': 16.0}"""
    def __call__(self, parser, namespace, values, option_string=None):
        res_dict = {}
        try:
            for item in values.split(','):
                key, val = item.split(':')
                res_dict[key.strip()] = float(val)
            setattr(namespace, self.dest, res_dict)
        except Exception:
            raise parser.error(f"Formato risorse non valido: {values}. Usa k:v,k:v")
            
app = Flask(__name__)

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_')

p.add('--port', type=int, default=8082)
p.add('--host', default=socket.gethostname(), help='Host logico per URL')
p.add('--bind-address', default='0.0.0.0', help='IP per Flask')
p.add('--db-path', default=os.path.join(os.getcwd(), "waluigi.db"), help='Path del db sqlite')
p.add('--resources', action=ParseResources, default={"coin": 1}, help="Definisci i limiti: 'water:100,food:16'")
p.add('--workdir', default=os.path.join(os.getcwd(), "work"), help='Default working directory')
p.add('--sourcedir', default=os.path.join(os.getcwd(), "source"), help='Default source code directory')
    
args = p.parse_args()

URL = f"http://{args.host}:{args.port}"
DB_PATH = args.db_path
RESOURCES = args.resources
WORKDIR = args.workdir
SOURCEDIR = args.sourcedir

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)

# Inizializzazione DB
try:
    db = WaluigiDB(DB_PATH)
    log(f"🟣 Database pronto in: {DB_PATH}")
except Exception as e:
    log(f"❌ Errore critico DB: {e}")
    sys.exit(1)

engine = WaluigiSchedulerEngine(db=db, resource_limits=RESOURCES)

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    print(f"method: update, payload: {data}")
    id = data['id']
    status = data['status']
    if status == "RUNNING":
        # Tentativo di acquisizione lock atomico
        if not db.try_to_lock(id):
            return jsonify({"status": "locked"}), 409
    if status in ["SUCCESS", "FAILED"]:
        task_resources = data.get('resources', {'coin': 1.0}) 
        engine._deallocate(task_resources)
        log(f"♻️ Risorse liberate per {id}")
        
    # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
    db.update_task(id, data.get('namespace'), data.get('params'), data.get('attributes'), status)
    return jsonify({"status": "updated"}), 200
        
def planner_loop():
    boss_id = f"boss-{socket.gethostname()}"
    log(f"🧠 Planner Loop avviato: {boss_id}")

    while True:
        try:
            #if not engine.workers:
            #    time.sleep(5)
            #    continue
                
            job = db.claim_job(boss_id)
            if not job:
                time.sleep(5)
                continue

            job_id = job['job_id']
            
            task = DynamicTask(job['spec'])

            res = engine.build(
                job_metadata=job['metadata'], 
                task=task,
                parent_id=None
            )
            
            if res is True:
                log(f"🏁 Job completed: {job_id}")
                db.update_job_status(job_id, "SUCCESS")
            elif res is None:
                log(f"💀 Job {job_id} failed because blocked by an error")
                db.update_job_status(job_id, "FAILED")
            
            db.release_job(job_id)

            time.sleep(5)

        except Exception as e:
            log(f"⚠️ Errore nel loop: {e}")
            if 'job_id' in locals(): 
                db.release_job(job_id)
            time.sleep(5)

@app.route('/worker/register', methods=['POST'])
def register():
    data = request.json
    engine.registerWorker(data)
    return jsonify({"status": "ok"})

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    
    if data.get("kind") != "Job" or "spec" not in data:
        return jsonify({"status": "error", "message": "Formato non supportato. Richiesto 'kind: Job' con 'spec'."}), 400
    spec = data.get("spec", {})
    if not spec:
        return jsonify({"status": "error", "message": "Spec vuoto"}), 400
    
    metadata = data.get("metadata", {})
    workdir = metadata.get("workdir", WORKDIR)
    sourcedir = metadata.get("sourcedir", SOURCEDIR)
    try:
        task = DynamicTask(spec)
        job_id = f"job/{task.id}"
        print(db.get_job_status(job_id))
        if db.get_job_status(job_id) != 'SUCCESS' and db.get_job_status(job_id) != 'FAILED':
            log(f"⚠️ Sottomissione rifiutata: {job_id} è già in esecuzione.")
            return jsonify({"status": "rejected", "reason": "already active"}), 409
                
        metadata['job_id'] = job_id
        
        db.create_job(
            job_id=job_id,
            metadata=metadata,
            spec=spec
        )
        
        log(f"📥 Flusso sottomesso: {job_id}")
        return jsonify({
            "status": "submitted", 
            "job_id": job_id, 
            "task_id": task.id
        })

    except Exception as e:
        log(f"❌ Errore processamento YAML: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def dashboard():
    running_jobs = db.list_jobs("RUNNING")
    conn = db.conn
    query = "SELECT namespace, id, params, status, last_update, parent_id FROM tasks"
    cursor = conn.execute(query)
    rows = cursor.fetchall()

    namespaces = {}
    for r in rows:
        ns = str(r[0])  # Il namespace (ex job_id)
        t_id = str(r[1]) # L'id univoco del task
        
        if ns not in namespaces: 
            namespaces[ns] = {'tasks': {}}
        
        namespaces[ns]['tasks'][t_id] = {
            'id': t_id, 
            'params': r[2], 
            'status': r[3], 
            'update': r[4], 
            'parent': r[5]
        }
    
    html = """
    <html>
    <head>
        <title>Waluigi Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <!--meta http-equiv="refresh" content="5"-->
        <style>
            body { background-color: #1a0026; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 15px; margin: 0; }
            h1 { color: #d080ff; font-size: 1.8em; border-bottom: 2px solid #4b0082; padding-bottom: 10px; }
            .namespace-container { background-color: #2b0040; border-radius: 8px; margin-bottom: 25px; padding: 15px; overflow-x: auto; }
            .namespace-header { color: #ffcc00; font-weight: bold; font-size: 1.1em; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center; }
            table { border-collapse: collapse; width: 100%; min-width: 650px; background-color: #360052; }
            th { background-color: #4b0082; color: white; text-align: left; padding: 12px; }
            td { padding: 2px 0px 0px 8px; border-bottom: 1px solid #4b0082; font-size: 0.85em; }
            .actions { display: flex; gap: 8px; justify-content: center; flex-wrap: wrap; }
            .indent { color: #8a2be2; font-family: monospace; font-weight: bold; white-space: pre; }
            .status-READY { color: #00d4ff; font-weight: bold; }
            .status-RUNNING { color: #ffff00; font-weight: bold; animation: blink 2s infinite; }
            .status-SUCCESS { color: #00ff88; font-weight: bold; }
            .status-FAILED { color: #ff4444; font-weight: bold; }
            @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
            .btn-action { background: #ff4444; color: white; padding: 12px; border-radius: 4px; border: none; cursor: pointer; font-size: 0.8em;}
        </style>
        <script>
            function actionConfirm(url) {
                if (confirm('Sicuro di voler procedere?')) {
                    fetch(url, {method: 'POST'}).then(() => window.location.reload());
                }
            }
        </script>
    </head>
    <body>
        <h1>🟣 Waluigi Dashboard</h1>
    """
    res_status = " | ".join([f"<b>{k.upper()}</b>: {engine.usage[k]}/{v}" for k, v in engine.limits.items()])
    
    html = html + f"""
    <div style="background: #2b0040; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #d080ff;">
        <h5>Boss Running. Workers: {len(engine.workers)} | Running Jobs: {len(running_jobs)}</h5>
        <p style="margin: 0; font-size: 0.9em; color: #00d4ff;">📊 Risorse: {res_status if res_status else 'Nessun limite impostato'}</p>
    </div>
    """
    
    def render_tree(current_id, all_tasks, level=0):
        if current_id not in all_tasks: return ""
        task = all_tasks[current_id]
        indent = ("&nbsp;" * level) + ("└─ " if level > 0 else "")
        
        row_html = f"""
        <tr>
            <td><span class='indent'>{indent}</span>{task['id']}</td>
            <td>{task['params']}</td>
            <td class='status-{task['status']}'>{task['status']}</td>
            <td>{task['update']}</td>
            <td class="actions">
                <button onclick="actionConfirm('/api/reset/task/{current_id}')" class='btn-action'>Reset</button>
                <button onclick="actionConfirm('/api/delete/task/{current_id}')" class='btn-action'>Delete</button>
            </td>
        </tr>
        """
        # Filtra i figli che appartengono a questo genitore
        children = [tid for tid, t in all_tasks.items() if str(t['parent']) == str(current_id)]
        for c_id in children: 
            row_html += render_tree(c_id, all_tasks, level + 1)
        return row_html

    for ns_name, data in namespaces.items():
        html += f"""
        <div class='namespace-container'>
            <div class='namespace-header'>
                <span>📦 Namespace: {ns_name}</span>
                <div class="actions">
                    <button onclick="actionConfirm('/api/reset/namespace/{ns_name}')" class='btn-action'>Reset</button>
                    <button onclick="actionConfirm('/api/delete/namespace/{ns_name}')" class='btn-action'>Delete</button>
                </div>
            </div>
            <table>
                <tr><th>Task ID</th><th>Parameters</th><th>Status</th><th>Last Update</th><th>Action</th></tr>
        """
        roots = [tid for tid, t in data['tasks'].items() if not t['parent'] or str(t['parent']) not in data['tasks']]
        for r_id in roots: 
            html += render_tree(r_id, data['tasks'])
            
        html += "</table></div>"
    
    html += "</body></html>"
    return html

@app.route('/api/reset/namespace/<namespace>', methods=['POST']) # CAMBIATO IN POST
def reset_namespace(namespace):
    target = None if namespace == "None" else namespace
    db.reset_namespace(target)
    return jsonify({"status": "ok"})

@app.route('/api/reset/task/<id>', methods=['POST']) # CAMBIATO IN POST
def reset_task(id):
    db.reset_task(id)
    return jsonify({"status": "ok"})

@app.route('/api/delete/namespace/<namespace>', methods=['POST']) # CAMBIATO IN POST
def delete_namespace(namespace):
    target = None if namespace == "None" else namespace
    db.delete_namespace(target)
    return jsonify({"status": "ok"})

@app.route('/api/delete/task/<id>', methods=['POST']) # CAMBIATO IN POST
def delete_task(id):
    db.delete_task(id)
    return jsonify({"status": "ok"})
        
@app.route('/api/resources', methods=['GET'])
def get_resources_api():
    return jsonify({
        "limits": engine.limits,
        "usage": engine.usage,
        "available": {k: engine.limits[k] - engine.usage[k] for k in engine.limits}
    })

@app.route('/api/resources', methods=['POST'])
def apply_resources_api():
    data = request.json
    for k, v in data.items():
        engine.limits[k] = float(v)
        if k not in engine.usage:
            engine.usage[k] = 0.0
    log(f"♻️ Nuovi limiti risorse applicati: {engine.limits}")
    return jsonify({"status": "updated", "new_limits": engine.limits})

@app.route('/api/workers', methods=['GET'])
def get_workers_api():
    return jsonify(engine.workers)
    
@app.route('/api/namespaces', methods=['GET'])
def get_namespaces():
    rows = db.list_namespaces()
    ns = [{"id": r[0], "task_count": r[1]} for r in rows]
    return jsonify(ns)
          
@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    rows = db.list_tasks()
    tasks = [{"namespace": r[1], "id": r[0], "job_id": r[6], "params": r[5], "status": r[2], "update": r[3]} for r in rows]
    return jsonify(tasks)
      
@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    rows = db.list_jobs()
    jobs = [{"job_id": r[0], "status": r[1], "locked_by": r[2], "locked_until": r[3]} for r in rows]
    return jsonify(jobs)
    
def get_jobs_old():
    data = []
    for job_id, job in active_jobs.items():
        task = job['task']
        job_metadata = job['metadata']
        data.append({
            "id": job_metadata['job_id'],
            "workdir": job_metadata['workdir'],
            "sourcedir": job_metadata['sourcedir'],  
            "task_id": task.id,
            "params": vars(task.params) if hasattr(task.params, '__dict__') else task.params,
            "namespace": task.namespace,
            "status": "ACTIVE"         
        })
    for job_id, job in completed_jobs.items():
        task = job['task']
        job_metadata = job['metadata']
        data.append({
            "id": job_metadata['job_id'],
            "workdir": job_metadata['workdir'],
            "sourcedir": job_metadata['sourcedir'],  
            "task_id": task.id,
            "params": vars(task.params) if hasattr(task.params, '__dict__') else task.params,
            "namespace": task.namespace,
            "status": "COMPLETED"                 
        })
    for job_id, job in failed_jobs.items():
        task = job['task']
        job_metadata = job['metadata']
        data.append({
            "id": job_metadata['job_id'],
            "workdir": job_metadata['workdir'],
            "sourcedir": job_metadata['sourcedir'],  
            "task_id": task.id,
            "params": vars(task.params) if hasattr(task.params, '__dict__') else task.params,
            "namespace": task.namespace,
            "status": "FAILED" 
        })    
    return jsonify(data)

@app.route('/api/active/describe/<path:key>', methods=['GET'])
def describe_active(key):
    if key not in active_jobs:
        return jsonify({"error": "Job not found"}), 404
    
    task = active_jobs[key]
    # Qui descriviamo l'oggetto in memoria
    return jsonify({
        "key": key,
        "id": task.id,
        "namespace": task.namespace,
        "tags": task.tags,
        "params": vars(task.params) if hasattr(task.params, '__dict__') else task.params,
        "attributes": vars(task.attributes) if hasattr(task.attributes, '__dict__') else task.attributes,
        "resources": getattr(task, 'resources', {})
    })


def main():
    log(f"Waluigi Boss:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")
    log(f"    Resources: {args.resources}")
    log(f"    Default Source Dir: {args.sourcedir}")
    log(f"    Default Work Dir: {args.workdir}")
   
    threading.Thread(target=planner_loop, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )
    
if __name__ == "__main__":
    main()
    