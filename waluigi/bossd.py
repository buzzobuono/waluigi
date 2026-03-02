from flask import Flask, request, jsonify, redirect, url_for
from waluigi.core.db import WaluigiDB
import os

app = Flask(__name__)

# Inizializziamo il DB
db = WaluigiDB(os.path.join(os.getcwd(), "waluigi.db"))

@app.route('/status/<task_id>', methods=['GET'])
def get_status(task_id):
    status = db.get_task_status(task_id) 
    return jsonify({"task_id": task_id, "status": status})

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    task_id = f"{data['name']}_{data['params']}"
    status = db.get_task_status(task_id)
    
    if status == "SUCCESS":
        return jsonify({"status": "already_done"}), 204
    if status == "RUNNING":
        return jsonify({"status": "locked"}), 409

    db.register_task(task_id, data.get('job_id'), data.get('parent_id'), data['name'], data['params'])
    return jsonify({"status": "ok"})

@app.route('/update', methods=['POST'])
def api_update():
    data = request.json
    t_id = data['task_id']
    status = data['status']

    if status == "RUNNING":
        # Tentativo di acquisizione lock atomico
        if not db.try_to_lock(t_id):
            return jsonify({"status": "locked"}), 409
    
    # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
    db.update_task(t_id, data.get('job_id'), data.get('parent_id'), 
                   data.get('name'), data.get('params'), status)
    return jsonify({"status": "updated"}), 200

@app.route('/')
def dashboard():
    conn = db.conn
    # Recuperiamo anche il parent_id
    query = "SELECT job_id, name, params, status, last_update, id, parent_id FROM tasks"
    cursor = conn.execute(query)
    rows = cursor.fetchall()

    # Raggruppiamo per Job e costruiamo la mappa dei figli
    jobs = {}
    for r in rows:
        j_id = str(r[0])
        if j_id not in jobs: jobs[j_id] = {'tasks': {}, 'tree': {}}
        
        task_data = {
            'id': r[5], 'name': r[1], 'params': r[2], 
            'status': r[3], 'update': r[4], 'parent': r[6]
        }
        jobs[j_id]['tasks'][r[5]] = task_data

    html = """
    <html>
    <head>
        <title>Waluigi Bossd Dashboard</title>
        <style>
            body { background-color: #1a0026; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 30px; }
            h1 { color: #d080ff; border-bottom: 2px solid #4b0082; padding-bottom: 10px; }
            .job-container { background-color: #2b0040; border-radius: 8px; margin-bottom: 25px; padding: 15px; }
            .job-header { font-size: 1.2em; font-weight: bold; color: #ffcc00; margin-bottom: 10px; display: flex; justify-content: space-between; }
            table { border-collapse: collapse; width: 100%; background-color: #360052; font-size: 0.9em; }
            th { background-color: #4b0082; color: white; text-align: left; padding: 12px; }
            td { padding: 8px; border-bottom: 1px solid #4b0082; }
            .indent { color: #8a2be2; font-family: monospace; font-weight: bold; white-space: pre; }
            .status-PENDING { color: #888888; font-style: italic; }
            .status-RUNNING { color: #ffff00; font-weight: bold; text-shadow: 0 0 5px #ffcc00; }
            .status-SUCCESS { color: #00ff88; font-weight: bold; }
            .status-FAILED { color: #ff4444; font-weight: bold; }
            @keyframes blink { 50% { opacity: 0.5; } }
            .btn-reset { background: #ff4444; color: white; border: none; padding: 4px 8px; border-radius: 3px; cursor: pointer; text-decoration: none; font-size: 0.8em; }
        </style>
    </head>
    <body>
        <h1>🟣 Waluigi Bossd Dashboard</h1>
    """

    def render_tree(task_id, all_tasks, level=0):
        """Funzione ricorsiva per generare le righe indentate."""
        task = all_tasks[task_id]
        indent = ("&nbsp;&nbsp;&nbsp;&nbsp;" * level) + ("└─ " if level > 0 else "")
        status_class = f"status-{task['status']}"
        
        row_html = f"""
        <tr>
            <td><span class='indent'>{indent}</span>{task['name']}</td>
            <td><code>{task['params']}</code></td>
            <td class='{status_class}'>{task['status']}</td>
            <td>{task['update']}</td>
            <td><a href='/api/reset/task/{task['id']}' class='btn-reset'>Reset</a></td>
        </tr>
        """
        
        # Trova i figli di questo task nello stesso job
        children = [t_id for t_id, t in all_tasks.items() if t['parent'] == task_id]
        for child_id in children:
            row_html += render_tree(child_id, all_tasks, level + 1)
        return row_html

    for j_id, data in jobs.items():
        html += f"""
        <div class='job-container'>
            <div class='job-header'>
                <span>📦 Job: {j_id}</span>
                <a href='/api/reset/job/{j_id}' class='btn-reset' onclick="return confirm('Resettare?')">Reset Job</a>
            </div>
            <table>
                <tr><th>Task Name</th><th>Parameters</th><th>Status</th><th>Last Update</th><th>Action</th></tr>
        """
        # Partiamo dai task che non hanno parent (i Root)
        roots = [t_id for t_id, t in data['tasks'].items() if t['parent'] is None or t['parent'] not in data['tasks']]
        for r_id in roots:
            html += render_tree(r_id, data['tasks'])
            
        html += "</table></div>"
    
    html += "</body></html>"
    return html



# --- API DI CONTROLLO ---

@app.route('/api/reset/job/<job_id>')
def api_reset_job(job_id):
    # Gestione job_id nullo da URL
    target = None if job_id == "None" else job_id
    db.reset_tasks_by_job(target)
    return redirect(url_for('dashboard'))

@app.route('/api/reset/task/<task_id>')
def api_reset_task(task_id):
    db.reset_task(task_id)
    return redirect(url_for('dashboard'))
    
if __name__ == "__main__":
    print("🟣 [Waluigi] Bossd pronto al servizio.")
    app.run(port=8082, debug=False, threaded=True)
