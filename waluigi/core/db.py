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
                    metadata TEXT,
                    spec TEXT,
                    status TEXT,
                    locked_by TEXT,
                    locked_until TIMESTAMP
                )""")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    url TEXT PRIMARY KEY,
                    status TEXT,
                    max_slots INTEGER,
                    free_slots INTEGER,
                    last_seen TIMESTAMP
                )""")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS resources (
                    name TEXT PRIMARY KEY,
                    amount REAL,
                    usage REAL DEFAULT 0.0
                )""")
            self.conn.execute("""
                    INSERT OR IGNORE INTO resources (name, amount, usage)
                    VALUES ('coin', 2.0, 0.0)
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    message TEXT,
                    boss_id TEXT, -- Qui salviamo il worker_id o boss_id che genera il log
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                )""")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_task_id ON task_logs(task_id)")

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
        with self.conn:
            cursor = self.conn.execute("SELECT id, namespace, status, last_update, parent_id, params, job_id FROM tasks ORDER BY last_update DESC")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def list_tasks_by_job(self, job_id):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT id, namespace, status, last_update, parent_id, params, job_id
                FROM tasks
                WHERE job_id = ?
                ORDER BY last_update ASC
            """, (job_id,))
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def list_namespaces(self):
        with self.conn:
            cursor = self.conn.execute("SELECT namespace, count(*) as task_count FROM tasks GROUP BY namespace")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        
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
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def register_worker(self, url, max_slots, free_slots):
        with self.conn:
            self.conn.execute("""
                INSERT INTO workers (url, max_slots, free_slots, status, last_seen)
                VALUES (?, ?, ?, 'ALIVE', CURRENT_TIMESTAMP)
                ON CONFLICT(url) DO UPDATE SET
                    max_slots = excluded.max_slots,
                    free_slots = excluded.free_slots,
                    status = 'ALIVE',
                    last_seen = excluded.last_seen
            """, (url, max_slots, free_slots))

    def list_workers(self):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT url, status, max_slots, free_slots, last_seen FROM workers
                ORDER BY last_seen ASC
            """)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_available_workers(self):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT url FROM workers 
                WHERE free_slots > 0 
                ORDER BY free_slots DESC, last_seen ASC
            """)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
            
    def delete_worker(self, url):
        with self.conn:
            self.conn.execute("DELETE FROM workers WHERE url = ?", (url,))
    
    def update_resources(self, resource_limits):
        with self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self.conn.execute("SELECT name, amount, usage FROM resources")
                db_state = {r[0]: (r[1], r[2]) for r in cursor.fetchall()}

                to_delete = [name for name in db_state if name not in resource_limits]
                for name in to_delete:
                    _, usage = db_state[name]
                    if usage > 0:
                        raise ValueError(f"Risorse occupate: '{name}' in uso ({usage}), impossibile rimuovere")

                for name, new_amount in resource_limits.items():
                    new_amount = float(new_amount)
                    if name in db_state:
                        _, usage = db_state[name]
                        if new_amount < usage:
                            raise ValueError(f"Risorse occupate: '{name}' uso attuale ({usage}) > richiesto ({new_amount})")
                
                    self.conn.execute("""
                        INSERT INTO resources (name, amount, usage)
                        VALUES (?, ?, 0.0)
                        ON CONFLICT(name) DO UPDATE SET amount = excluded.amount
                    """, (name, new_amount))

                for name in to_delete:
                    self.conn.execute("DELETE FROM resources WHERE name = ?", (name,))

                self.conn.execute("COMMIT")
                return True, "Risorse aggiornate con successo"

            except ValueError as ve:
                self.conn.execute("ROLLBACK")
                return False, str(ve)
            except Exception as e:
                self.conn.execute("ROLLBACK")
                return False, str(e)

    def acquire_resources(self, required_resources):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for name, amount in required_resources.items():
                res = self.conn.execute(
                    "SELECT amount, usage FROM resources WHERE name = ?", (name,)
                ).fetchone()
                
                if not res or (res[1] + amount > res[0]):
                    self.conn.execute("ROLLBACK")
                    return False

            for name, amount in required_resources.items():
                self.conn.execute(
                    "UPDATE resources SET usage = usage + ? WHERE name = ?",
                    (amount, name)
                )
            
            self.conn.execute("COMMIT")
            return True
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def release_resources(self, required_resources):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for name, amount in required_resources.items():
                self.conn.execute("""
                    UPDATE resources 
                    SET usage = MAX(0.0, usage - ?) 
                    WHERE name = ?
                """, (amount, name))
            
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
    
    def list_resources(self):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT name, amount, usage FROM resources
            """)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def insert_task_logs(self, task_id, logs, worker_id):
        with self.conn:
            try:
                self.conn.executemany("INSERT INTO task_logs (task_id, message, boss_id) VALUES (?, ?, ?)", [(task_id, line, worker_id) for line in logs])
            except Exception as e:
                print(f"❌ Errore DB durante insert_task_logs: {e}")
                raise
    
    def get_logs(self, task_id, limit=20):
        query = """
            SELECT * FROM (
                SELECT id, timestamp, boss_id, message 
                FROM task_logs 
                WHERE task_id = ? 
                ORDER BY id DESC 
                LIMIT ?
            ) ORDER BY id ASC
        """
        cursor = self.conn.execute(query, (task_id, limit))
        return [
            {"id": r[0], "timestamp": r[1], "worker_id": r[2], "message": r[3]} 
            for r in cursor.fetchall()
        ]