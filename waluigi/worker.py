import sys
import importlib
import requests
import threading
from flask import Flask, request, jsonify

MAX_CONCURRENT_TASKS = 2
active_tasks_count = 0
lock = threading.Lock()

app = Flask(__name__)
BOSS_URL = "http://localhost:8082" # Assicurati che l'indirizzo sia corretto

def log(msg):
    print(f"👷 [WORKER] {msg}", flush=True)

@app.route('/execute', methods=['POST'])
def execute():
    global active_tasks_count
    with lock:
        if active_tasks_count >= MAX_CONCURRENT_TASKS:
            return jsonify({"status": "busy"}), 429
        active_tasks_count += 1
        
    data = request.json
    log(f"Ricevuto ordine: {data.get('module')}.{data.get('class')}")
    
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
            "status": status
        })
        
if __name__ == "__main__":
    # Prendi la porta dagli argomenti (es: python worker.py 5001)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    
    log(f"Avvio Worker sulla porta {port}...")
    
    # Registrazione automatica al Boss
    try:
        requests.post(f"{BOSS_URL}/worker/register", json={"url": f"http://localhost:{port}"})
        log("Registrato con successo al Boss.")
    except:
        log("Attenzione: Impossibile registrarsi al Boss. Assicurati che sia acceso.")

    app.run(port=port, host='0.0.0.0', debug=False, threaded=True)
