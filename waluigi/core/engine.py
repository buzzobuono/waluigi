import requests

class WaluigiEngine:
    def __init__(self, server_url="http://localhost:8082"):
        self.server_url = server_url

    def build(self, job_id, task, parent_id=None):
        task.engine = self
        t_id = f"{task.__class__.__name__}"

        # 1. Chiedi al Boss lo stato attuale
        r = requests.post(f"{self.server_url}/register", json={
            "job_id": job_id, "parent_id": parent_id,
            "task_id": task.__class__.__name__, "params": task.params
        }, timeout=2)

        # Se il Boss dice che sta già girando altrove, questo ramo muore qui.
        if r.status_code == 409:
            print(f"⚠️ [Waluigi] {t_id} locked")
            return False

        # 2. Check di completamento reale (Idempotenza)
        if task.is_complete():
            # Se a DB non era SUCCESS, allinealo per la dashboard
            print(f"✅ [Waluigi] {t_id} is complete")
            if r.status_code != 204:
                self._update_boss(t_id, job_id, parent_id, task, "SUCCESS")
            return True
        
        print(f"📌 [Waluigi] {t_id} is not complete")
        
        # 3. Se non è completo, risolvi le dipendenze.
        # Se anche una sola dipendenza non è True (è in corso o fallita), il padre si ferma.
        for dep in task.requires():
            if not self.build(job_id, dep, parent_id=t_id):
                # Segnaliamo PENDING per visibilità, ma il processo per questo task finisce.
                self._update_boss(t_id, job_id, parent_id, task, "PENDING")
                return False

        # 4. Tutte le dipendenze sono SUCCESS. Ora chiediamo il lock per il RUN.
        r_lock = requests.post(f"{self.server_url}/update", json={
            "task_id": t_id, "status": "RUNNING", "job_id": job_id, 
            "parent_id": parent_id, "params": task.params
        }, timeout=2)

        if r_lock.status_code == 409:
            return False # Qualcun altro ha preso il lock mentre controllavamo le deps.

        # 5. ESECUZIONE
        try:
            print(f"🚀 [Waluigi] {t_id} running")
            task.run()
            task.complete(job_id) # Scrive il flag/file di completamento
            self._update_boss(t_id, job_id, parent_id, task, "SUCCESS")
            print(f"🏆 [Waluigi] {t_id} done")
            return True
        except Exception as e:
            print(f"❌ [Waluigi] {t_id} error: {e}")
            self._update_boss(t_id, job_id, parent_id, task, "FAILED")
            return False

    def _update_boss(self, t_id, j_id, p_id, task, status):
        print(t_id)
        requests.post(f"{self.server_url}/update", json={
            "task_id": t_id,"job_id": j_id, "parent_id": p_id,
                "params": task.params,
            "status": status
        }, timeout=2)
