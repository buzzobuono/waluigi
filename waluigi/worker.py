import requests
import uuid
import threading
import time
import socket
import os
import configargparse
from flask import Flask, request, jsonify
import subprocess

active_tasks_count = 0
lock = threading.Lock()

app = Flask(__name__)

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_WORKER_')

p.add('--id', default=str(uuid.uuid4()), help='ID unico')
p.add('--port', type=int, default=5001)
p.add('--host', default=socket.gethostname(), help='Host logico per URL')
p.add('--bind-address', default='0.0.0.0', help='IP per Flask')
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
    
@app.route('/execute', methods=['POST'])
def execute():
    data = request.json
    workdir = data.get("workdir", DEFAULT_WORKDIR)
    command = data.get("command")
    id = data.get("id")
    namespace = data.get("namespace")
    params = data.get("params", {})
    attributes = data.get("attributes", {})
    resources = data.get("resources")

    if not command:
        return jsonify({"status": "error", "message": "No command provided"}), 400

    log(f"Ricevuto ordine: {id}")
    
    global active_tasks_count
    with lock:
        if active_tasks_count >= SLOTS:
            log(f"Slot non disponibile.")
            return jsonify({"status": "busy"}), 429
        active_tasks_count += 1
            
    try:
        thread = threading.Thread(
            target=run_command_async, 
            args=(command, id, namespace, params, attributes, resources, workdir)
        )
        thread.start()
    
        return jsonify({"status": "submitted", "id": id}), 202
    
    except Exception as e:
        log(f"❌ Errore: {e}")
        with lock:
            active_tasks_count -= 1
        return jsonify({"status": "error", "message": str(e)}), 500

def run_command_async(command, id, namespace, params, attributes, resources, workdir):
    global active_tasks_count
    
    try:
        _update_boss(id, namespace, params, attributes, resources, "RUNNING")
        
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        for k, v in params.items():
            env[f"WALUIGI_PARAM_{k.upper()}"] = str(v)
        for k, v in attributes.items():
            env[f"WALUIGI_ATTRIBUTE_{k.upper()}"] = str(v)

        log(f"🚀 Forking: {command}")
        
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env
        )

        log_buffer = []
        for line in iter(process.stdout.readline, ""):
            clean_line = line.strip()
            if clean_line:
                print(f"[{id}] {clean_line}", flush=True)
                log_buffer.append(clean_line)                
                if len(log_buffer) >= 5:
                    _send_logs(id, log_buffer)
                    log_buffer = []

        if log_buffer:
            _send_logs(id, log_buffer)

        process.wait()

        if process.returncode == 0:
            log(f"✅ Task {id} terminato con successo.")
            _update_boss(id, namespace, params, attributes, resources, "SUCCESS")
        else:
            log(f"❌ Task {id} fallito (Exit code: {process.returncode})")
            _update_boss(id, namespace, params, attributes, resources, "FAILED")
            
    except Exception as e:
        log(f"❌ Errore: {e}")
        _update_boss(id, namespace, params, attributes, resources, "FAILED")
    finally:
        with lock:
            active_tasks_count -= 1

def _send_logs(task_id, lines):
    try:
        return _post(f"/api/logs/{task_id}", json={
            "worker_id": WORKER_ID,
            "logs": lines
        }, timeout=5)
    except Exception as e:
        log(f"⚠️ Errore invio log per {task_id}: {e}")

def _post(endpoint, **kwargs):
    try:
        r = requests.post(f"{BOSS_URL}{endpoint}", **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"[bossd] Connection error on {endpoint}") from e
        
def _update_boss(id, namespace, params, attributes, resources, status):
    return _post(f"/update", json={
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
    
def heartbeat():
    while True:
        try:
            requests.post(f"{BOSS_URL}/worker/register", json={
                 "url": URL,
                 "status": "ALIVE",
                 "max_slots": SLOTS,
                 "free_slots": SLOTS - active_tasks_count
            })
            log("Registrato con successo al Boss.")
        except:
            log("Boss non raggiungibile...")
        time.sleep(HEARTBEAT)
        
def main():
    log(f"Waluigi Worker:")
    log(f"    ID: {args.id}")
    log(f"    Boss URL: {args.boss_url}")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    Slots: {args.slots}")
    log(f"    Heartbeat: {args.heartbeat}")
    log(f"    Default Work Dir: {args.default_workdir}")
    
    threading.Thread(target=heartbeat, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )
    
if __name__ == "__main__":
    main()
    