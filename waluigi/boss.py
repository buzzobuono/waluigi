import importlib
import threading
import time
import sys
import os
import socket
import uuid
import configargparse
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from waluigi.core.db import WaluigiDB
from waluigi.core.scheduler_engine import WaluigiSchedulerEngine
from waluigi.core.dynamic_task import DynamicTask

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_BOSS_')

p.add('--id', default=str(uuid.uuid4()), help='Unique ID')
p.add('--port', type=int, default=8082)
p.add('--host', default=socket.gethostname(), help='Hostname')
p.add('--bind-address', default='0.0.0.0', help='Binding IP')
p.add('--db-path', default=os.path.join(os.getcwd(), "db/waluigi.db"), help='Sqlite DB Path')

args = p.parse_args()

BOSS_ID = args.id
URL = f"http://{args.host}:{args.port}"
DB_PATH = args.db_path

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)

try:
    db = WaluigiDB(DB_PATH)
    log(f"🟣 Database ready: {DB_PATH}")
except Exception as e:
    log(f"❌ Error: {e}")
    sys.exit(1)

engine = WaluigiSchedulerEngine(db=db)

@app.post('/update')
async def update(request: Request):
    data = await request.json()
    id = data['id']
    status = data['status']
    if status == "RUNNING":
        if not db.try_to_lock(id):
            return JSONResponse({"status": "locked"}, status_code=409)
    if status in ["SUCCESS", "FAILED"]:
        task_resources = data.get('resources', {'coin': 1.0})
        db.release_resources(task_resources)
        log(f"♻️ Resources released for {id}")
        
    db.update_task(id, data.get('namespace'), data.get('params'), data.get('attributes'), status)
    return JSONResponse({"status": "updated"}, status_code=200)
        
def planner_loop():
    log(f"🧠 Planner Loop started: {BOSS_ID}")

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
            log(f"❌ Error: {e}")
            if 'job_id' in locals():
                db.release_job(job_id)
            time.sleep(5)

@app.post('/worker/register')
async def register(request: Request):
    data = await request.json()
    engine.registerWorker(data)
    return JSONResponse({"status": "ok"})

@app.post('/submit')
async def submit(request: Request):
    data = await request.json()

    if data.get("kind") != "Job" or "spec" not in data:
        return JSONResponse({"status": "error", "message": "Format not supported. Neef 'kind: Job' and not empty 'spec'."}, status_code=400)
    spec = data.get("spec", {})
    if not spec:
        return JSONResponse({"status": "error", "message": "Empty 'spec'"}, status_code=400)

    metadata = data.get("metadata", {})
    try:
        task = DynamicTask(spec)
        job_id = f"job/{task.id}"
        status = db.get_job_status(job_id)

        if status and status != 'SUCCESS' and status != 'FAILED':
            log(f"⚠️ Submission rejected: {job_id} is already active.")
            return JSONResponse({"status": "rejected", "message": "already active"}, status_code=409)

        metadata['job_id'] = job_id

        db.create_job(
            job_id=job_id,
            metadata=metadata,
            spec=spec
        )

        log(f"📥 Job submitted: {job_id}")
        return JSONResponse({
            "status": "submitted",
            "job_id": job_id,
            "task_id": task.id
        })

    except Exception as e:
        log(f"❌ Error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get('/', response_class=HTMLResponse)
async def dashboard():
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
            .btn-action { background: #4b0082; color: white; padding: 6px 10px; border-radius: 4px; border: 1px solid #d080ff; cursor: pointer; font-size: 0.8em; text-decoration: none; display: inline-block;}
            .btn-action:hover { background: #d080ff; color: #1a0026; }
        </style>
        <script>
            function actionConfirm(url) {
                if (confirm('Confirm action?')) {
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
    html += f"""
    <div style="background: #2b0040; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #d080ff;">
        <h5>Boss Running. Workers: {len(workers)} | Running Jobs: {len(running_jobs)}</h5>
        <p style="margin: 0; font-size: 0.9em; color: #00d4ff;">📊 Risorse: {res_status if res_status else 'Nessun limite impostato'}</p>
    </div>
    """

    def render_tree(current_id, all_tasks, level=0):
        if current_id not in all_tasks:
            return ""
        task = all_tasks[current_id]
        indent = ("&nbsp;" * level) + ("└─ " if level > 0 else "")
        task_id_link = f"<a href='/api/logs/{task['id']}' target='_blank' style='color: #00d4ff; text-decoration: none;'>{task['id']}</a>"

        row_html = f"""
        <tr>
            <td><span class='indent'>{indent}</span>{task_id_link}</td>
            <td>{task['params']}</td>
            <td class='status-{task['status']}'>{task['status']}</td>
            <td>{task['update']}</td>
            <td class="actions">
                <button onclick="actionConfirm('/api/reset/task/{current_id}')" class='btn-action'>Reset</button>
                <button onclick="actionConfirm('/api/delete/task/{current_id}')" class='btn-action'>Delete</button>
            </td>
        </tr>
        """
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

@app.post('/api/reset/namespace/{namespace}')
async def reset_namespace(namespace: str):
    target = None if namespace == "None" else namespace
    db.reset_namespace(target)
    return JSONResponse({"status": "ok"})

@app.post('/api/reset/task/{id}')
async def reset_task(id: str):
    db.reset_task(id)
    return JSONResponse({"status": "ok"})

@app.post('/api/delete/namespace/{namespace}')
async def delete_namespace(namespace: str):
    target = None if namespace == "None" else namespace
    db.delete_namespace(target)
    return JSONResponse({"status": "ok"})

@app.post('/api/delete/task/{id}')
async def delete_task(id: str):
    db.delete_task(id)
    return JSONResponse({"status": "ok"})

@app.get('/api/resources')
async def get_resources_api():
    return db.list_resources()

@app.post('/api/resources')
async def apply_resources_api(request: Request):
    doc = await request.json()
    if not doc or doc.get('kind') != 'ClusterResources':
        return JSONResponse({"status": "error", "message": "Expected kind: ClusterResources"}, status_code=400)

    spec = doc.get('spec', {})
    if not spec:
        return JSONResponse({"status": "error", "message": "Spec vuoto"}, status_code=400)
    try:
        (success, msg) = db.update_resources(spec)
        if not success:
            return JSONResponse({"status": "error", "message": msg}, status_code=409)

        log(f"⚙️ Cluster resources updated: {spec}")
        return JSONResponse({"status": "ok", "message": msg}, status_code=200)
    except Exception as e:
        log(f"❌ Error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get('/api/workers')
async def get_workers_api():
    return db.list_workers()
    
@app.get('/api/namespaces')
async def get_namespaces():
    return db.list_namespaces()

@app.get('/api/tasks')
async def get_tasks():
    return db.list_tasks()

@app.get('/api/jobs')
async def get_jobs():
    return db.list_jobs()
    
@app.post('/api/logs/{task_id}')
async def receive_logs(task_id: str, request: Request):
    data = await request.json()
    logs = data.get('logs', [])
    worker_id = data.get('worker_id', 'unknown')

    if logs:
        try:
            db.insert_task_logs(task_id, logs, worker_id)
            return JSONResponse({"status": "ok"}, status_code=201)
        except Exception as e:
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    return JSONResponse({"status": "empty"}, status_code=200)

@app.get('/api/logs/{task_id}')
async def get_task_logs(task_id: str, limit: int = 20):
    return db.get_logs(task_id, limit=limit)

def main():
    log(f"Waluigi Boss:")
    log(f"    ID: {args.id}")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")

    threading.Thread(target=planner_loop, daemon=True).start()

    uvicorn.run(app,
        host=args.bind_address,
        port=args.port
    )
    
if __name__ == "__main__":
    main()
    