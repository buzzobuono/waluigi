import importlib
import threading
import time
import sys
import os
import requests  # Fondamentale!
from flask import Flask, request, jsonify
from waluigi.core.db import WaluigiDB

# Configurazione
app = Flask(__name__)
PORT = 8082
DB_PATH = os.path.join(os.getcwd(), "waluigi.db")

# Inizializzazione DB
try:
    db = WaluigiDB(DB_PATH)
    print(f"🟣 [Boss] Database pronto in: {DB_PATH}")
except Exception as e:
    print(f"❌ [Boss] Errore critico DB: {e}")
    sys.exit(1)

# Stato dell'orchestratore
active_flows = {}
workers = []

# --- LOGICA DI INVIO ---

def push_to_worker(task, module_name):
    if not workers:
        print("⚠️ [Boss] Nessun worker disponibile")
        return False
        
    task_hash = task.hash(task.params)
    payload = {
        "module": module_name,
        "class": task.__class__.__name__,
        "id": task.id,
        "params": vars(task.params),
        "params_hash": task_hash,
        "attributes": vars(task.attributes)
    }
    
    for w_url in workers:
        try:
            # Timeout generoso per permettere al worker di caricare i moduli
            r = requests.post(f"{w_url}/execute", json=payload, timeout=10)
            if r.status_code == 200:
                print(f"🚀 [Boss] Inviato a {w_url}: {task.id}")
                return True
        except Exception as e:
            print(f"❌ [Boss] Worker {w_url} non ha risposto correttamente")
            continue
    return False

# --- LOGICA DI PIANIFICAZIONE ---

def scan_and_dispatch(task, module_name):
    task_hash = task.hash(task.params)
    status = db.get_task_status(task.id, task_hash)
    
    # 1. Se è già in corso o finito, ignoralo
    if status in ["SUCCESS", "RUNNING"]:
        return

    # 2. Se non esiste nel DB, registralo (Fondamentale per il primo avvio)
    if status is None:
        db.register_task(task.id, task.namespace, None, task_hash, task.hash(task.attributes))
        status = "PENDING"

    deps = task.requires()
    all_ready = True
    
    for dep in deps:
        dep_hash = dep.hash(dep.params)
        dep_status = db.get_task_status(dep.id, dep_hash)
        
        # Registra la dipendenza se ignota
        if dep_status is None:
            db.register_task(dep.id, dep.namespace, task.id, dep_hash, dep.hash(dep.attributes))
            dep_status = "PENDING"
            
        if dep_status != "SUCCESS":
            all_ready = False
            # Scendi ricorsivamente
            scan_and_dispatch(dep, module_name)

    # 3. Se tutte le dipendenze sono SUCCESS, prova a lanciarlo
    if all_ready:
        if db.try_to_lock(task.id):
            print(f"🔍 [Boss] Pronto per dispatch: {task.id}")
            success = push_to_worker(task, module_name)
            
            if not success:
                # Se l'invio fallisce, resettiamo lo stato così il loop può riprovare
                print(f"⚠️ [Boss] Invio fallito per {task.id}. Rilascio lock.")
                db.update_task(task.id, task.namespace, None, task_hash, task.hash(task.attributes), "PENDING")

def planner_loop():
    print("🧠 [Boss] Planner Loop avviato.")
    while True:
        try:
            if not active_flows:
                time.sleep(5)
                continue
            
            for key, root_task in list(active_flows.items()):
                module_name = key.split(':')[0]
                status = db.get_task_status(root_task.id, root_task.hash(root_task.params))
                
                if status == "SUCCESS":
                    print(f"🏁 [Boss] Flusso completato: {key}")
                    del active_flows[key]
                else:
                    scan_and_dispatch(root_task, module_name)
                
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ [Boss] Errore nel loop: {e}")
            time.sleep(5)

# --- ROTTE FLASK ---

@app.route('/worker/register', methods=['POST'])
def register():
    url = request.json.get("url")
    if url and url not in workers:
        workers.append(url)
        print(f"👷 [Boss] Nuovo worker registrato: {url}")
    return jsonify({"status": "ok"})

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    mod_name = data.get("module")
    cls_name = data.get("class")
    params = data.get("params", {})
    
    try:
        mod = importlib.import_module(mod_name)
        cls = getattr(mod, cls_name)
        root_task = cls(params=params)
        
        db.register_task(root_task.id, root_task.namespace, None, root_task.hash(root_task.params), root_task.hash(root_task.attributes))
        
        flow_key = f"{mod_name}:{root_task.id}"
        active_flows[flow_key] = root_task
        
        print(f"📥 [Boss] Flusso sottomesso: {flow_key}")
        return jsonify({"status": "submitted", "id": root_task.id})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    db.update_task(
        id=data['id'],
        namespace=data.get('namespace'),
        parent_id=data.get('parent_id'),
        params=data.get('params'),
        attributes=data.get('attributes'),
        status=data['status']
    )
    print(f"📝 [Boss] Task {data['id']} aggiornato a {data['status']}")
    return jsonify({"status": "ok"})
        
@app.route('/')
def dashboard():
    conn = db.conn
    # r[0]=namespace, r[1]=id, r[2]=params, r[3]=status, r[4]=last_update, r[5]=parent_id
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
            td { padding: 10px; border-bottom: 1px solid #4b0082; font-size: 0.85em; }
            .actions { display: flex; gap: 8px; justify-content: center; flex-wrap: wrap; }
            .indent { color: #8a2be2; font-family: monospace; font-weight: bold; white-space: pre; }
            .status-RUNNING { color: #ffff00; font-weight: bold; animation: blink 2s infinite; }
            .status-SUCCESS { color: #00ff88; font-weight: bold; }
            .status-FAILED { color: #ff4444; font-weight: bold; }
            @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
            .btn-action { background: #ff4444; color: white; padding: 12px; border-radius: 4px; border: none; cursor: pointer; font-size: 0.8em;}
            code { background: #1a0026; padding: 2px 4px; border-radius: 3px; color: #d080ff; }
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
    html = html + f"<h5>Boss Running. Workers: {len(workers)} | Flows: {len(active_flows)}</h5>"

    def render_tree(current_id, all_tasks, level=0):
        if current_id not in all_tasks: return ""
        task = all_tasks[current_id]
        indent = ("&nbsp;&nbsp;" * level) + ("└─ " if level > 0 else "")
        
        row_html = f"""
        <tr>
            <td><span class='indent'>{indent}</span>{task['id']}</td>
            <td><code>{task['params']}</code></td>
            <td class='status-{task['status']}'>{task['status']}</td>
            <td><small>{task['update']}</small></td>
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
        # Trova i task radice (parent nullo o parent non presente nel dizionario dei task di questo namespace)
        roots = [tid for tid, t in data['tasks'].items() if not t['parent'] or str(t['parent']) not in data['tasks']]
        for r_id in roots: 
            html += render_tree(r_id, data['tasks'])
            
        html += "</table></div>"
    
    html += "</body></html>"
    return html


# --- API DI CONTROLLO ---

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
         
if __name__ == "__main__":
    threading.Thread(target=planner_loop, daemon=True).start()
    app.run(port=PORT, host='0.0.0.0', debug=False, threaded=True)
