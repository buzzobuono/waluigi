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
                id TEXT primary key,
                namespace TEXT, 
                parent_id TEXT,
                params TEXT, 
                status TEXT, 
                last_update TIMESTAMP
            )""")

    def get_task_status(self, id, params):
        cursor = self.conn.execute("SELECT status FROM tasks WHERE id = ? and params = ?", (id, params))
        row = cursor.fetchone()
        return row[0] if row else None

    def try_to_lock(self, id):
        """Tenta il passaggio a RUNNING. Ritorna True solo se ha successo atomico."""
        with self.conn:
            # Se lo stato attuale è già RUNNING, la query colpirà 0 righe -> False
            cursor = self.conn.execute("""
            UPDATE tasks 
            SET status = 'RUNNING', last_update = DATETIME('now')
            WHERE id = ? AND status != 'RUNNING'
            """, (id,))
            return cursor.rowcount > 0
            
    def register_task(self, id, namespace, parent_id, params):
        with self.conn:
            self.conn.execute("""
                INSERT INTO tasks (id, parent_id, namespace, params, status, last_update)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET 
                    namespace=excluded.namespace, parent_id=excluded.parent_id,
                    last_update=excluded.last_update
            """, (id, parent_id, namespace, params, 'PENDING', datetime.now()))

    def update_task(self, id, namespace, parent_id, params, status):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status=?, last_update=?, namespace=?, parent_id=?, params=?
                WHERE id=?
            """, (status, datetime.now(), namespace, parent_id, params, id))

    def delete_namespace(self, namespace):
        with self.conn:
            self.conn.execute("DELETE FROM tasks WHERE namespace = ?", (namespace,))
    
    def delete_task(self, id):
        with self.conn:
            self.conn.execute("DELETE FROM tasks WHERE id = ?", (id,))
            
    def reset_namespace(self, namespace):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status='READY'
                WHERE namespace=?
            """, (namespace, ))
            
    def reset_task(self, id):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status='READY'
                WHERE id=?
            """, (id, ))

    def list_tasks(self):
        cursor = self.conn.execute("SELECT id, namespace, status, last_update, parent_id FROM tasks ORDER BY last_update DESC")
        return cursor.fetchall()
        