from flask import Flask, request, jsonify, redirect, url_for
from waluigi.core.db import WaluigiDB
import os

app = Flask(__name__)

# Inizializziamo il DB
db = WaluigiDB(os.path.join(os.getcwd(), "waluigi.db"))

@app.route('/status/<task_id>/<params>', methods=['GET'])
def get_status(task_id, params):
    status = db.get_task_status(task_id, params) 
    return jsonify({"task_id": task_id, "status": status})

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    print(f"method: register, payload: {data}")
    
    task_id = data["task_id"]
    params = data["params"]
    status = db.get_task_status(task_id, params)
    
    if status == "SUCCESS":
        return jsonify({"status": "already_done"}), 204
    if status == "RUNNING":
        return jsonify({"status": "locked"}), 409

    db.register_task(task_id, data.get('job_id'), data.get('parent_id'), data['task_id'], data['params'])
    return jsonify({"status": "ok"})

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    print(f"method: update, payload: {data}")
    t_id = data['task_id']
    status = data['status']
    
    if status == "RUNNING":
        # Tentativo di acquisizione lock atomico
        if not db.try_to_lock(t_id):
            return jsonify({"status": "locked"}), 409
    
    # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
    db.update_task(t_id, data.get('job_id'), data.get('parent_id'), 
                   data.get("task_id"), data.get('params'), status)
    return jsonify({"status": "updated"}), 200

@app.route('/')
def dashboard():
    conn = db.conn
    # Recuperiamo i dati (task_id usato sia come nome che come chiave univoca)
    query = "SELECT job_id, task_id, params, status, last_update, task_id, parent_id FROM tasks"
    cursor = conn.execute(query)
    rows = cursor.fetchall()

    jobs = {}
    for r in rows:
        j_id = str(r[0])
        if j_id not in jobs: jobs[j_id] = {'tasks': {}, 'tree': {}}
        
        task_data = {
            'id': r[5], 'task_id': r[1], 'params': r[2], 
            'status': r[3], 'update': r[4], 'parent': r[6]
        }
        jobs[j_id]['tasks'][r[5]] = task_data
    
    html = """
    <html>
    <head>
        <title>Waluigi Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="5">
        <style>
            body { 
                background-color: #1a0026; color: #e0e0e0; 
                font-family: 'Segoe UI', Roboto, sans-serif; 
                padding: 15px; margin: 0; 
            }
            h1 { color: #d080ff; font-size: 1.8em; border-bottom: 2px solid #4b0082; padding-bottom: 10px; }
            
            .job-container { 
                background-color: #2b0040; border-radius: 8px; 
                margin-bottom: 25px; padding: 15px; 
                box-shadow: 0 4px 15px rgba(0,0,0,0.5);
                overflow-x: auto; 
            }
            
            .job-header { 
                color: #ffcc00; font-weight: bold; font-size: 1.1em;
                margin-bottom: 15px; display: flex; 
                justify-content: space-between; align-items: center; 
            }
            
            table { border-collapse: collapse; width: 100%; min-width: 650px; background-color: #360052; }
            th { background-color: #4b0082; color: white; text-align: left; padding: 12px; font-size: 0.9em; }
            td { padding: 10px; border-bottom: 1px solid #4b0082; font-size: 0.85em; vertical-align: middle; }
            
            .indent { color: #8a2be2; font-family: monospace; font-weight: bold; white-space: pre; }
            
            /* --- STILI STATI --- */
            .status-PENDING { color: #888888; font-style: italic; }
            
            .status-RUNNING { 
                color: #ffff00; font-weight: bold; 
                text-shadow: 0 0 8px rgba(255, 255, 0, 0.4);
                animation: blink 2s infinite; 
            }
            
            .status-SUCCESS { 
                color: #00ff88; font-weight: bold; 
                text-shadow: 0 0 5px rgba(0, 255, 136, 0.2);
            }
            
            .status-FAILED { 
                color: #ff4444; font-weight: bold; 
                text-shadow: 0 0 5px rgba(255, 68, 68, 0.2);
            }

            @keyframes blink { 
                0% { opacity: 1; } 
                50% { opacity: 0.4; } 
                100% { opacity: 1; } 
            }
            
            /* --- BOTTONI --- */
            .btn-reset { 
                background: #ff4444; color: white; 
                padding: 6px 12px; border-radius: 4px; 
                border: none; cursor: pointer; 
                font-size: 0.8em; font-weight: bold;
                transition: transform 0.1s, background 0.2s;
            }
            .btn-reset:hover { background: #ff6666; }
            .btn-reset:active { transform: scale(0.95); }

            code { background: #1a0026; padding: 2px 4px; border-radius: 3px; color: #d080ff; }
        </style>

        <script>
            function safeReset(url) {
                if (confirm('Sicuro di voler resettare?')) {
                    fetch(url, {method: 'POST'}).then(() => window.location.reload());
                }
            }
        </script>
    </head>
    <body>
        <h1>🟣 Waluigi Dashboard</h1>
    """

    def render_tree(t_id, all_tasks, level=0):
        if t_id not in all_tasks: return ""
        task = all_tasks[t_id]
        indent = ("&nbsp;&nbsp;" * level) + ("└─ " if level > 0 else "")
        # USIAMO BUTTON INVECE DI <A>
        row_html = f"""
        <tr>
            <td><span class='indent'>{indent}</span>{task['task_id']}</td>
            <td><small>{task['params']}</small></td>
            <td class='status-{task['status']}'>{task['status']}</td>
            <td><small>{task['update']}</small></td>
            <td><button onclick="safeReset('/api/reset/task/{t_id}')" class='btn-reset'>Reset</button></td>
        </tr>
        """
        children = [child_id for child_id, t in all_tasks.items() if t['parent'] == t_id]
        for c_id in children: row_html += render_tree(c_id, all_tasks, level + 1)
        return row_html

    for j_id, data in jobs.items():
        html += f"<div class='job-container'><div class='job-header'><span>📦 Job: {j_id}</span>"
        # ANCHE QUI BUTTON CON FETCH POST
        html += f"<button onclick=\"safeReset('/api/reset/job/{j_id}')\" class='btn-reset'>Reset Job</button></div>"
        html += "<table><tr><th>Task</th><th>Params</th><th>Status</th><th>Update</th><th>Action</th></tr>"
        
        roots = [tid for tid, t in data['tasks'].items() if not t['parent'] or t['parent'] not in data['tasks']]
        for r_id in roots: html += render_tree(r_id, data['tasks'])
        html += "</table></div>"
    
    html += "</body></html>"
    return html


# --- API DI CONTROLLO ---

@app.route('/api/reset/job/<job_id>', methods=['POST']) # CAMBIATO IN POST
def reset_job(job_id):
    target = None if job_id == "None" else job_id
    db.reset_tasks_by_job(target)
    return jsonify({"status": "ok"})

@app.route('/api/reset/task/<task_id>', methods=['POST']) # CAMBIATO IN POST
def reset_task(task_id):
    db.reset_task(task_id)
    return jsonify({"status": "ok"})
        
if __name__ == "__main__":
    print("🟣 [Waluigi] Bossd pronto al servizio.")
    app.run(port=8082, debug=False, threaded=True)
