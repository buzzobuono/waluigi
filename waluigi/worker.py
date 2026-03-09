import sys
import importlib
import requests
import threading
import time
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

args = p.parse_args()

BOSS_URL = args.boss_url
URL = f"http://{args.host}:{args.port}"
SLOTS = args.slots
HEARTBEAT = args.heartbeat

def log(msg):
    print(f"[worker 👷] {msg}", flush=True)

@app.route('/execute', methods=['POST'])
def execute():
    data = request.json
    log(f"Ricevuto ordine: {data.get('module')}.{data.get('class')}")
    
    global active_tasks_count
    with lock:
        if active_tasks_count >= SLOTS:
            log(f"Slot non disponibile: {data.get('module')}.{data.get('class')}")
            return jsonify({"status": "busy"}), 429
        active_tasks_count += 1
    
    # Lanciamo l'esecuzione asincrona
    thread = threading.Thread(target=run_task_async, args=(data,))
    thread.start()
    
    # Liberiamo subito il Boss
    return jsonify({"status": "submitted", "id": data.get('id')}), 202

def run_task_async(data):
    global active_tasks_count
    try:
        # Caricamento e istanza (come prima)
        mod = importlib.import_module(data.get('module'))
        mod = importlib.reload(mod)
        cls = getattr(mod, data.get('class'))
        task = cls(id=data.get('id'), tags=data.get('tags'), params=data.get('params'), attributes=data.get('attributes'))
        # 
        class SimpleEngine:
            def __init__(self, url): self.server_url = url
        task.engine = SimpleEngine(BOSS_URL)
       
        # Notifica opzionale: il worker ha iniziato davvero (Status -> RUNNING)
        _update_boss("", task, "RUNNING")
        
        log(f"🚀 Esecuzione asincrona avviata: {task.id}")
        task.run()
        #task.complete()
        
        # Notifica finale
        _update_boss("", task, "SUCCESS")
        log(f"✅ Task {task.id} terminato.")
    except Exception as e:
        log(f"❌ Errore asincrono: {e}")
        # In caso di crash, riportiamo a FAILED per sbloccare il grafo
        _update_boss("", task, "FAILED")
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

def _update_boss(parent_id, task, status):
    return _post(f"/update", json={
            "id": task.id,
            "namespace": task.namespace, 
            "parent_id": parent_id,
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
        
if __name__ == "__main__":
    log(f"Parametri:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    Slots: {args.slots}")
    log(f"    Heartbeat: {args.heartbeat}")
    
    threading.Thread(target=heartbeat, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )