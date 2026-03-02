import sqlite3
from datetime import datetime
import sqlite3
from datetime import datetime

class WaluigiDB:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, job_id TEXT, parent_id TEXT,
                name TEXT, params TEXT, status TEXT, last_update TIMESTAMP
            )""")

    def get_task_status(self, task_id):
        cursor = self.conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        return row[0] if row else None

    def try_to_lock(self, task_id):
        """Tenta il passaggio a RUNNING. Ritorna True solo se ha successo atomico."""
        with self.conn:
            # Se lo stato attuale è già RUNNING, la query colpirà 0 righe -> False
            cursor = self.conn.execute("""
            UPDATE tasks 
            SET status = 'RUNNING', last_update = DATETIME('now')
            WHERE id = ? AND status != 'RUNNING'
            """, (task_id,))
            return cursor.rowcount > 0
            
    def register_task(self, task_id, job_id, parent_id, name, params):
        # Registriamo inizialmente come PENDING per non bloccare il lock ottimistico
        with self.conn:
            self.conn.execute("""
                INSERT INTO tasks (id, job_id, parent_id, name, params, status, last_update)
                VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
                ON CONFLICT(id) DO UPDATE SET 
                    job_id=excluded.job_id, parent_id=excluded.parent_id,
                    last_update=excluded.last_update
            """, (task_id, job_id, parent_id, name, params, datetime.now()))

    def update_task(self, task_id, job_id, parent_id, name, params, status):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status=?, last_update=?, job_id=?, parent_id=?
                WHERE id=?
            """, (status, datetime.now(), job_id, parent_id, task_id))

    def reset_tasks_by_job(self, job_id):
        with self.conn:
            self.conn.execute("DELETE FROM tasks WHERE job_id = ?", (job_id,))
    
    def reset_task(self, task_id):
        with self.conn:
            self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    
    def list_tasks(self):
        cursor = self.conn.execute("SELECT id, job_id, name, status, last_update, parent_id FROM tasks ORDER BY last_update DESC")
        return cursor.fetchall()
        