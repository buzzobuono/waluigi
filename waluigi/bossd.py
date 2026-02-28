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
    job_id = data.get('job_id') 
    
    result = db.register_task(task_id, job_id, data['name'], data['params'])
    
    if result == "LOCKED":
        return jsonify({"status": "locked"}), 409
    if result == "ALREADY_DONE":
        return jsonify({"status": "done"}), 204 
    
    return jsonify({"status": "ok"})

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    # FIX: Passiamo anche il job_id all'update, altrimenti nel DB viene sovrascritto con NULL
    db.update_task(
        data['task_id'], 
        data.get('job_id'), # Aggiunto questo!
        data['name'], 
        data['params'], 
        data['status']
    )
    return jsonify({"status": "updated"})

@app.route('/')
def dashboard():
    conn = db.conn
    query = "SELECT job_id, name, params, status, last_update, id FROM tasks ORDER BY last_update DESC"
    cursor = conn.execute(query)
    rows = cursor.fetchall()

    jobs = {}
    for row in rows:
        j_id = str(row[0]) if row[0] else "None" # Gestione stringa per link HTML
        if j_id not in jobs:
            jobs[j_id] = []
        jobs[j_id].append(row)

    html = """
    <html>
    <head>
        <title>Waluigi Bossd Dashboard</title>
        <style>
            body { background-color: #1a0026; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 30px; }
            h1 { color: #d080ff; border-bottom: 2px solid #4b0082; padding-bottom: 10px; }
            .job-container { background-color: #2b0040; border-radius: 8px; margin-bottom: 25px; padding: 15px; }
            .job-header { font-size: 1.2em; font-weight: bold; color: #ffcc00; margin-bottom: 10px; display: flex; justify-content: space-between; }
            table { border-collapse: collapse; width: 100%; background-color: #360052; }
            th { background-color: #4b0082; color: white; text-align: left; padding: 12px; }
            td { padding: 10px; border-bottom: 1px solid #4b0082; }
            .status-SUCCESS { color: #00ff88; }
            .status-RUNNING { color: #ffff00; }
            .status-FAILED { color: #ff4444; }
            .btn-reset { background: #ff4444; color: white; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer; text-decoration: none; font-size: 0.8em; }
        </style>
    </head>
    <body>
        <h1>🟣 Waluigi Bossd Dashboard</h1>
    """

    for j_id, tasks in jobs.items():
        html += f"""
        <div class='job-container'>
            <div class='job-header'>
                <span>📦 Job: {j_id}</span>
                <a href='/api/reset/job/{j_id}' class='btn-reset' onclick="return confirm('Resettare tutto il job {j_id}?')">Reset Job</a>
            </div>
            <table>
                <tr><th>Task Name</th><th>Parameters</th><th>Status</th><th>Last Update</th><th>Action</th></tr>
        """
        for t in tasks:
            status_class = f"status-{t[3]}"
            html += f"""
                <tr>
                    <td>{t[1]}</td><td><code>{t[2]}</code></td>
                    <td class='{status_class}'>{t[3]}</td><td>{t[4]}</td>
                    <td><a href='/api/reset/task/{t[5]}' class='btn-reset'>Reset</a></td>
                </tr>
            """
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
