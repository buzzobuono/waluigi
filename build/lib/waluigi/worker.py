import asyncio
import uuid
import threading
import time
import socket
import os
import configargparse
import uvicorn
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

active_tasks_count = 0
lock = asyncio.Lock()

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_WORKER_')

p.add('--id', default=str(uuid.uuid4()), help='Unique ID')
p.add('--port', type=int, default=5001)
p.add('--host', default=socket.gethostname(), help='Hostname')
p.add('--bind-address', default='0.0.0.0', help='Binding IP')
p.add('--boss-url', default='http://localhost:8082')
p.add('--slots', type=int, default=2)
p.add('--heartbeat', type=int, default=10)
p.add('--default-workdir', default=os.path.join(os.getcwd(), "work"), help='Default working directory')

args = p.parse_args()

WORKER_ID = args.id
BOSS_URL = args.boss_url
URL = f"http://{args.host}:{args.port}"
SLOTS = args.slots
HEARTBEAT = args.heartbeat
DEFAULT_WORKDIR = args.default_workdir

def log(msg):
    print(f"[worker 👷] {msg}", flush=True)
    
@app.post('/execute')
async def execute(request: Request):
    data = await request.json()
    workdir = data.get("workdir", DEFAULT_WORKDIR)
    command = data.get("command")
    id = data.get("id")
    job_id = data.get("job_id")
    namespace = data.get("namespace")
    params = data.get("params", {})
    attributes = data.get("attributes", {})
    resources = data.get("resources")

    if not command:
        return JSONResponse({"status": "error", "message": "No command provided"}, status_code=400)

    log(f"Task recieved: {id}")

    global active_tasks_count
    async with lock:
        if active_tasks_count >= SLOTS:
            log(f"Slot not available.")
            return JSONResponse({"status": "busy"}, status_code=429)
        active_tasks_count += 1

    try:
        asyncio.create_task(
            run_command_async(command, id, job_id, namespace, params, attributes, resources, workdir)
        )
        return JSONResponse({"status": "submitted", "id": id}, status_code=202)

    except Exception as e:
        log(f"❌ Error: {e}")
        async with lock:
            active_tasks_count -= 1
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

async def run_command_async(command, id, job_id, namespace, params, attributes, resources, workdir):
    global active_tasks_count

    try:
        await _update_boss(id, namespace, params, attributes, resources, "RUNNING")

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        for k, v in params.items():
            env[f"WALUIGI_PARAM_{k.upper()}"] = str(v)
        for k, v in attributes.items():
            env[f"WALUIGI_ATTRIBUTE_{k.upper()}"] = str(v)
        env["WALUIGI_TASK_ID"] = id
        env["WALUIGI_JOB_ID"] = job_id
        log(f"🚀 Forking: {command}")

        process = await asyncio.create_subprocess_shell(
            command,
            cwd=workdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env
        )

        log_buffer = []
        async for line in process.stdout:
            clean_line = line.decode().strip()
            if clean_line:
                print(f"[{id}] {clean_line}", flush=True)
                log_buffer.append(clean_line)
                if len(log_buffer) >= 5:
                    await _send_logs(id, log_buffer)
                    log_buffer = []

        if log_buffer:
            await _send_logs(id, log_buffer)

        await process.wait()

        if process.returncode == 0:
            log(f"✅ Task {id} succesfully terminated.")
            await _update_boss(id, namespace, params, attributes, resources, "SUCCESS")
        else:
            log(f"❌ Task {id} failed (Exit code: {process.returncode})")
            await _update_boss(id, namespace, params, attributes, resources, "FAILED")

    except Exception as e:
        log(f"❌ Error: {e}")
        await _update_boss(id, namespace, params, attributes, resources, "FAILED")
    finally:
        async with lock:
            active_tasks_count -= 1


async def _send_logs(task_id, lines):
    try:
        await _post(f"/api/logs/{task_id}", json={
            "worker_id": WORKER_ID,
            "logs": lines
        })
    except Exception as e:
        log(f"⚠️ Error in sending log for {task_id}: {e}")

async def _post(endpoint, **kwargs):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{BOSS_URL}{endpoint}", timeout=5, **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r
        
async def _update_boss(id, namespace, params, attributes, resources, status):
    return await _post("/update", json={
        "id": id,
        "namespace": namespace,
        "params": _hash(params),
        "attributes": _hash(attributes),
        "resources": resources,
        "status": status
    })

def _hash(nsdict):
    return " ".join(
        f"{k}:{v}"
        for k, v in sorted(nsdict.items())
    )
    
async def heartbeat():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.post(f"{BOSS_URL}/worker/register", json={
                    "url": URL,
                    "status": "ALIVE",
                    "max_slots": SLOTS,
                    "free_slots": SLOTS - active_tasks_count
                }, timeout=5)
                log("Registrato con successo al Boss.")
            except Exception:
                log("Boss non raggiungibile...")
            await asyncio.sleep(HEARTBEAT)
            
@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat())
    
def main():
    log(f"Waluigi Worker:")
    log(f"    ID: {args.id}")
    log(f"    Boss URL: {args.boss_url}")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    Slots: {args.slots}")
    log(f"    Heartbeat: {args.heartbeat}")
    log(f"    Default Work Dir: {args.default_workdir}")

    uvicorn.run(app, 
        host=args.bind_address,
        port=args.port
    )
    
if __name__ == "__main__":
    main()
