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
        job_id = f"job:{task.id}"
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

@app.get('/api/jobs/{job_id}/tasks')
async def get_job_tasks(job_id: str):
    tasks = db.list_tasks_by_job(job_id)
    return JSONResponse(tasks)
 
@app.get('/api/jobs')
async def get_jobs():
    return db.list_jobs()
    
@app.delete('/api/jobs/{job_id}')
async def delete_job(job_id: str):
    return db.delete_job(job_id)
   
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
    