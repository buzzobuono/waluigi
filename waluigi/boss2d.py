import importlib
import threading
import time
import sys
import os
import requests
import configargparse
from flask import Flask, request, jsonify
from waluigi.core.db import WaluigiDB
from waluigi.core.scheduler_engine import WaluigiSchedulerEngine

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
p.add('--host', default='localhost', help='Host logico per URL')
p.add('--bind-address', default='0.0.0.0', help='IP per Flask')
p.add('--db-path', default=os.path.join(os.getcwd(), "waluigi.db"), help='Path del db sqlite')
p.add('--resources', action=ParseResources, default={"coin": 1}, help="Definisci i limiti: 'water:100,food:16'")
      
args = p.parse_args()

URL = f"http://{args.host}:{args.port}"
DB_PATH = args.db_path
RESOURCES = args.resources

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)

# Inizializzazione DB
try:
    db = WaluigiDB(DB_PATH)
    log(f"🟣 Database pronto in: {DB_PATH}")
except Exception as e:
    log(f"❌ Errore critico DB: {e}")
    sys.exit(1)
    
active_flows = {}


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
    db.update_task(id, data.get('namespace'), data.get('parent_id'), data.get('params'), data.get('attributes'), status)
    return jsonify({"status": "updated"}), 200

def planner_loop():
    log("🧠 Planner Loop avviato.")
    while True:
        try:
            if not active_flows or not engine.workers:
                time.sleep(5)
                continue
            for key, root_task in list(active_flows.items()):
                module_name = key.split(':')[0]
                res = engine.build(root_task, module_name)
                
                if res is True:
                    log(f"🏁 Flusso completato: {key}")
                    del active_flows[key]
                elif res is None:
                    log(f"💀 Flusso {key} rimosso perché bloccato da un errore.")
                    del active_flows[key]
                else:
                    pass
                
            time.sleep(2)
            #break
        except Exception as e:
            log(f"⚠️ Errore nel loop: {e}")
            time.sleep(5)

# --- ROTTE FLASK ---

@app.route('/worker/register', methods=['POST'])
def register():
    data = request.json
    engine.registerWorker(data)
    return jsonify({"status": "ok"})

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    mod_name = data.get("module")
    cls_name = data.get("class")
    params = data.get("params", {})
    
    try:
        mod = importlib.import_module(mod_name)
        mod = importlib.reload(mod)
        cls = getattr(mod, cls_name)
        root_task = cls(params=params)
        
        db.register_task(root_task.id, root_task.namespace, None, root_task.hash(root_task.params), root_task.hash(root_task.attributes))
        
        flow_key = f"{mod_name}:{root_task.id}"
        active_flows[flow_key] = root_task
        
        log(f"📥 Flusso sottomesso: {flow_key}")
        return jsonify({"status": "submitted", "id": root_task.id})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400
            
@app.route('/')
def dashboard():
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
            td { padding: 10px; border-bottom: 1px solid #4b0082; font-size: 0.85em; }
            .actions { display: flex; gap: 8px; justify-content: center; flex-wrap: wrap; }
            .indent { color: #8a2be2; font-family: monospace; font-weight: bold; white-space: pre; }
            .status-READY { color: #00d4ff; font-weight: bold; }
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
    res_status = " | ".join([f"<b>{k.upper()}</b>: {engine.usage[k]}/{v}" for k, v in engine.limits.items()])
    
    html = html + f"""
    <div style="background: #2b0040; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #d080ff;">
        <h5>Boss Running. Workers: {len(engine.workers)} | Flows: {len(active_flows)}</h5>
        <p style="margin: 0; font-size: 0.9em; color: #00d4ff;">📊 Risorse: {res_status if res_status else 'Nessun limite impostato'}</p>
    </div>
    """
    
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
    log(f"Parametri:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")
    log(f"    Risorse: {args.resources}")
    
    threading.Thread(target=planner_loop, daemon=True).start()
    
    app.run(
        host=args.bind_address, 
        port=args.port, 
        debug=False, 
        threaded=True
    )
    