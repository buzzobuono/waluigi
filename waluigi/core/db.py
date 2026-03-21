import sqlite3
import threading
import json
from datetime import datetime

class WaluigiDB:
    
    def __init__(self, db_path):
        self.db_path = db_path
        # Creiamo un contenitore isolato per ogni thread
        self._local = threading.local()
        self.create_table()

    @property
    def conn(self):
        """Restituisce una connessione specifica per il thread che la chiama."""
        if not hasattr(self._local, "connection"):
            # Ogni thread (Flask, Planner, ecc.) avrà il suo tunnel privato al DB
            self._local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            # Timeout di 30 secondi per attendere che il file si sblocchi
            self._local.connection.execute("PRAGMA busy_timeout = 30000")
            # WAL mode permette a più lettori e 1 scrittore di non bloccarsi a vicenda
            self._local.connection.execute("PRAGMA journal_mode=WAL;")
        return self._local.connection
        
    def create_table(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    namespace TEXT, 
                    parent_id TEXT,
                    params TEXT, 
                    attributes TEXT, 
                    status TEXT, 
                    last_update TIMESTAMP,
                    job_id TEXT
                )""")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    metadata TEXT,    -- workdir, sourcedir, module_name
                    spec TEXT,        -- Il contenuto dello YAML o i parametri della classe
                    status TEXT,           -- PENDING, RUNNING, SUCCESS, FAILED
                    locked_by TEXT,        -- ID del Boss (es: hostname)
                    locked_until TIMESTAMP
                )""")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT,
                    task_id TEXT,
                    status TEXT,
                    last_seen TIMESTAMP,
                    PRIMARY KEY (worker_id, task_id)
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
            
    def register_task(self, id, namespace, parent_id, params, attributes, job_id):
        with self.conn:
            self.conn.execute("""
                INSERT INTO tasks (id, parent_id, namespace, params, attributes, status, last_update, job_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET 
                    namespace=excluded.namespace, parent_id=excluded.parent_id,
                    last_update=excluded.last_update
            """, (id, parent_id, namespace, params, attributes, 'PENDING', datetime.now(), job_id))

    def update_task(self, id, namespace, params, attributes, status):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status=?, last_update=?, namespace=?, params=?, attributes=?
                WHERE id=?
            """, (status, datetime.now(), namespace, params, attributes, id))
            
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
                    status='PENDING'
                WHERE namespace=?
            """, (namespace, ))
            
    def reset_task(self, id):
        with self.conn:
            self.conn.execute("""
                UPDATE tasks SET 
                    status='PENDING'
                WHERE id=?
            """, (id, ))

    def list_tasks(self):
        cursor = self.conn.execute("SELECT id, namespace, status, last_update, parent_id, params, job_id FROM tasks ORDER BY last_update DESC")
        return cursor.fetchall()
    
    def list_namespaces(self):
        cursor = self.conn.execute("SELECT namespace, count(*) FROM tasks GROUP BY namespace")
        return cursor.fetchall()
        
    def create_job(self, job_id, metadata, spec):
        with self.conn:
            query = """
            INSERT INTO jobs (
                job_id, 
                metadata, 
                spec,
                status
            ) VALUES (?, ?, ?, 'PENDING')
            ON CONFLICT(job_id) DO UPDATE SET
            metadata = excluded.metadata,
            spec = excluded.spec,
            status = 'PENDING',
            locked_by = NULL,
            locked_until = NULL
            WHERE status NOT IN ('RUNNING', 'READY')
            """
            self.conn.execute(query, (
                job_id, 
                json.dumps(metadata),
                json.dumps(spec)
            ))
            
    def claim_job(self, boss_id):
        with self.conn:
            # Questa query fa TUTTO in un colpo solo:
            # Trova, Locka e Restituisce i dati della riga modificata.
            query = """
                UPDATE jobs 
                SET locked_by = ?, 
                    locked_until = datetime('now', '+60 seconds'),
                    status = 'RUNNING'
                WHERE job_id = (
                    SELECT job_id FROM jobs 
                    WHERE status NOT IN ('SUCCESS', 'FAILED')
                    AND (locked_until IS NULL OR locked_until < datetime('now'))
                    LIMIT 1
                )
                RETURNING job_id, metadata, spec;
            """
            cursor = self.conn.execute(query, (boss_id,))
            res = cursor.fetchone()
        
            if res:
                return {
                    "job_id": res[0],
                    "metadata": json.loads(res[1]),    
                    "spec": json.loads(res[2])
                }
        return None
    
    def update_job_status(self, job_id, status):
        with self.conn:
            self.conn.execute("""
                UPDATE jobs SET 
                    status = ?, 
                    locked_by = NULL, 
                    locked_until = NULL 
                WHERE job_id = ?
            """, (status, job_id))
    
    def get_job_status(self, job_id):
        cursor = self.conn.execute("SELECT status FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        return row[0] if row else None
               
    def release_job(self, job_id):
        with self.conn:
            self.conn.execute("""
                UPDATE jobs SET 
                    locked_by = NULL, 
                    locked_until = NULL 
                WHERE job_id = ?
            """, (job_id,))
    
    def list_jobs(self, status=None):
        if status:
            cursor = self.conn.execute("SELECT job_id, status, locked_by, locked_until FROM jobs WHERE status = ?", (status,))
        else:
            cursor = self.conn.execute("SELECT job_id, status, locked_by, locked_until FROM jobs")
        return cursor.fetchall()
        