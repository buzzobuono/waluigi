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
                    id TEXT PRIMARY KEY,
                    job_id TEXT,
                    name TEXT,
                    params TEXT,
                    status TEXT,
                    last_update TIMESTAMP
                )
            """)

    def get_task_status(self, task_id):
        cursor = self.conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        return row[0] if row else None

    # FIX: Aggiunto job_id agli argomenti
    def register_task(self, task_id, job_id, name, params):
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            status = row[0] if row else None
            
            if status == "RUNNING":
                self.conn.rollback()
                return "LOCKED"
            
            if status == "SUCCESS":
                self.conn.rollback()
                return "ALREADY_DONE"
            
            # Qui job_id ora esiste perché è nell'argomento della funzione
            self.conn.execute("""
                INSERT INTO tasks (id, job_id, name, params, status, last_update)
                VALUES (?, ?, ?, ?, 'RUNNING', ?)
                ON CONFLICT(id) DO UPDATE SET 
                    job_id=excluded.job_id,
                    status='RUNNING', 
                    last_update=excluded.last_update
            """, (task_id, job_id, name, params, datetime.now()))
        
            self.conn.commit()
            return "OK"
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Errore DB Register: {e}")
            return "ERROR"

    # FIX: Aggiunto job_id e allineato i punti interrogativi (erano 5 per 4 colonne!)
    def update_task(self, task_id, job_id, name, params, status):
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO tasks (id, job_id, name, params, status, last_update)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET 
                        status=excluded.status, 
                        last_update=excluded.last_update,
                        job_id=excluded.job_id
                """, (task_id, job_id, name, params, status, datetime.now()))
        except Exception as e:
            print(f"❌ Errore DB Update: {e}")

    def list_tasks(self):
        cursor = self.conn.execute("SELECT id, job_id, name, status, last_update FROM tasks ORDER BY last_update DESC")
        return cursor.fetchall()

    def reset_task(self, task_id):
        with self.conn:
            cursor = self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cursor.rowcount > 0

    def reset_tasks_by_job(self, job_id):
        with self.conn:
            if job_id == "None" or job_id is None:
                cursor = self.conn.execute("DELETE FROM tasks WHERE job_id IS NULL OR job_id = 'None'")
            else:
                cursor = self.conn.execute("DELETE FROM tasks WHERE job_id = ?", (job_id,))
            return cursor.rowcount
