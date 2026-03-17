import sys
import importlib
import requests
import threading
import time
import os
import configargparse
from flask import Flask, request, jsonify

active_tasks_count = 0
lock = threading.Lock()

app = Flask(__name__)

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_')

p.add('--port', type=int, default=5001)
p.add('--host', default='localhost', help='Host logico per URL')
p.add('--bind-address', default='0.0.0.0', help='IP per Flask')
p.add('--boss-url', default='http://localhost:8082')
p.add('--slots', type=int, default=2)
p.add('--heartbeat', type=int, default=10)
p.add('--workdir', default=os.path.join(os.getcwd(), "work"), help='Default working directory')
p.add('--sourcedir', default=os.path.join(os.getcwd(), "source"), help='Default source code directory')

args = p.parse_args()

BOSS_URL = args.boss_url
URL = f"http://{args.host}:{args.port}"
SLOTS = args.slots
HEARTBEAT = args.heartbeat
WORKDIR = args.workdir
SOURCEDIR = args.sourcedir

def log(msg):
    print(f"[worker 👷] {msg}", flush=True)
    
@app.route('/execute', methods=['POST'])
def execute():
    data = request.json
    workdir = data.get("workdir", WORKDIR)
    sourcedir = data.get("sourcedir", SOURCEDIR)
    module_name = data.get("module")
    class_name = data.get("class")
    id = data.get("id", "")
    tags = data.get("tags", [])
    params = data.get("params", {})
    attributes = data.get("attributes", {})
    
    log(f"Ricevuto ordine: {module_name}.{class_name}")
    
    global active_tasks_count
    with lock:
        if active_tasks_count >= SLOTS:
            log(f"Slot non disponibile.")
            return jsonify({"status": "busy"}), 429
        active_tasks_count += 1
        
    try:
        added_to_path = False
        if sourcedir and sourcedir not in sys.path:
            sys.path.insert(0, sourcedir)
            added_to_path = True

        try:
            if module_name in sys.modules:
                del sys.modules[module_name]
            
            module = importlib.import_module(module_name)
            clazz = getattr(module, class_name)
            task = clazz(id=id, tags=tags, params=params, attributes=attributes)
            
            thread = threading.Thread(target=run_task_async, args=(task, workdir))
            thread.start()
            task_started = True
            
            return jsonify({"status": "submitted", "id": task.id}), 202

        finally:
            if added_to_path:
                sys.path.remove(sourcedir)

    except Exception as e:
        log(f"❌ Errore nel caricamento del modulo: {e}")
        with lock:
            active_tasks_count -= 1
        return jsonify({"status": "error", "message": str(e)}), 500

def run_task_async(task, workdir):
    global active_tasks_count
    try:
        class SimpleEngine:
            def __init__(self, url): self.server_url = url
        task.engine = SimpleEngine(BOSS_URL)
       
        _update_boss(task, "RUNNING")
        
        log(f"📁 Setting workdir: {workdir}")
        os.chdir(workdir)
        
        log(f"🚀 Esecuzione asincrona avviata: {task.id}")
        task.run()
        
        _update_boss(task, "SUCCESS")
        log(f"✅ Task {task.id} terminato.")
    except Exception as e:
        log(f"❌ Errore asincrono: {e}")
        # In caso di crash, riportiamo a FAILED per sbloccare il grafo
        _update_boss(task, "FAILED")
    finally:
        with lock:
            active_tasks_count -= 1
            
def _post(endpoint, **kwargs):
    try:
        r = requests.post(f"{BOSS_URL}{endpoint}", **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"[bossd] Connection error on {endpoint}") from e

def _update_boss(task, status):
    return _post(f"/update", json={
            "id": task.id,
            "namespace": task.namespace,
            "params": task.hash(task.params), 
            "attributes": task.hash(task.attributes),
            "resources": task.resources,
            "status": status
        })
        
def heartbeat():
    while True:
        try:
            requests.post(f"{BOSS_URL}/worker/register", json={
                 "url": URL,
                 "status": "ALIVE",
                 "free_slots": SLOTS - active_tasks_count
            })
            log("Registrato con successo al Boss.")
        except:
            log("Attenzione: Impossibile registrarsi al Boss. Assicurati che sia acceso.")

        time.sleep(HEARTBEAT)
        
        
def main():
    log(f"Waluigi Worker:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    Slots: {args.slots}")
    log(f"    Heartbeat: {args.heartbeat}")
    log(f"    Default Source Dir: {args.sourcedir}")
    log(f"    Default Work Dir: {args.workdir}")
    
    threading.Thread(target=heartbeat, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )
    
if __name__ == "__main__":
    main()
    