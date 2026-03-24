import importlib
import threading
import time
import sys
import os
import socket
import uuid
import configargparse
from flask import Flask, request, jsonify
from waluigi.core.db import WaluigiDB
from waluigi.core.scheduler_engine import WaluigiSchedulerEngine
from waluigi.core.dynamic_task import DynamicTask

app = Flask(__name__)

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_BOSS_')

p.add('--id', default=str(uuid.uuid4()), help='ID unico')
p.add('--port', type=int, default=8082)
p.add('--host', default=socket.gethostname(), help='Host logico per URL')
p.add('--bind-address', default='0.0.0.0', help='IP per Flask')
p.add('--db-path', default=os.path.join(os.getcwd(), "waluigi.db"), help='Path del db sqlite')
  
args = p.parse_args()

BOSS_ID = args.id
URL = f"http://{args.host}:{args.port}"
DB_PATH = args.db_path

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)
    
try:
    db = WaluigiDB(DB_PATH)
    log(f"🟣 Database pronto in: {DB_PATH}")
except Exception as e:
    log(f"❌ Errore critico DB: {e}")
    sys.exit(1)

engine = WaluigiSchedulerEngine(db=db)

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    #print(f"method: update, payload: {data}")
    id = data['id']
    status = data['status']
    if status == "RUNNING":
        # Tentativo di acquisizione lock atomico
        if not db.try_to_lock(id):
            return jsonify({"status": "locked"}), 409
    if status in ["SUCCESS", "FAILED"]:
        task_resources = data.get('resources', {'coin': 1.0}) 
        db.release_resources(task_resources)
        log(f"♻️ Risorse liberate per {id}")
        
    # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
    db.update_task(id, data.get('namespace'), data.get('params'), data.get('attributes'), status)
    return jsonify({"status": "updated"}), 200
        
def planner_loop():
    log(f"🧠 Planner Loop avviato: {BOSS_ID}")

    while True:
        try:
            #if not engine.workers:
            #    time.sleep(5)
            #    continue
                
            job = db.claim_job(BOSS_ID)
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
    try:
        task = DynamicTask(spec)
        job_id = f"job/{task.id}"
        status = db.get_job_status(job_id)
        
        if status and status != 'SUCCESS' and status != 'FAILED':
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
    workers = db.list_workers()
    resources = db.list_resources()
    tasks = db.list_tasks()
    
    namespaces = {}
    for task in tasks:
        ns = task['namespace']
        t_id = task['id']
        
        if ns not in namespaces: 
            namespaces[ns] = {'tasks': {}}
        
        namespaces[ns]['tasks'][t_id] = {
            'id': t_id, 
            'params': task['params'], 
            'status': task['status'], 
            'update': task['last_update'], 
            'parent': task['parent_id']
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
    res_status = " | ".join([
        f"<b>{r['name'].upper()}</b>: {r['usage']}/{r['amount']}" 
        for r in resources
    ])
    html = html + f"""
    <div style="background: #2b0040; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #d080ff;">
        <h5>Boss Running. Workers: {len(workers)} | Running Jobs: {len(running_jobs)}</h5>
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
    resources = db.list_resources()
    return jsonify(resources)

@app.route('/api/resources', methods=['POST'])
def apply_resources_api():
    doc = request.json
    if not doc or doc.get('kind') != 'ClusterResources':
        return jsonify({"status": "error", "message": "Expected kind: ClusterResources"}), 400
    
    spec = doc.get('spec', {})
    if not spec:
        return jsonify({"status": "error", "message": "Spec vuoto"}), 400
    try:
        (success, msg) = db.update_resources(spec)
        print(success)
        if not success:
            return jsonify({"status": "error", "message": msg}), 409
            
        log(f"⚙️ Limiti del cluster aggiornati: {spec}")
        return jsonify({
            "status": "ok",
            "message": msg    
        }), 200
    except Exception as e:
        log(f"❌ Errore aggiornamento risorse: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/workers', methods=['GET'])
def get_workers_api():
    workers = db.list_workers()
    return jsonify(workers)
    
@app.route('/api/namespaces', methods=['GET'])
def get_namespaces():
    ns = db.list_namespaces()
    return jsonify(ns)
          
@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    tasks = db.list_tasks()
    return jsonify(tasks)
      
@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    jobs = db.list_jobs()
    return jsonify(jobs)
    
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
    log(f"    ID: {args.id}")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")
    
    threading.Thread(target=planner_loop, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )
    
if __name__ == "__main__":
    main()
    