import requests
import time

class WaluigiEngine:
    def __init__(self, server_url="http://localhost:8082"):
        self.server_url = server_url
        self.job_id = f"job_{int(time.time())}"
        
    def build(self, task):
        task_id = f"{task.__class__.__name__}_{task.param_str}"

        # 1. CONTROLLO PREVENTIVO (Status GET)
        # Lo facciamo prima di tutto per evitare di scatenare le dipendenze se non serve
        try:
            r = requests.get(f"{self.server_url}/status/{task_id}", timeout=2)
            if r.status_code == 200 and r.json().get("status") == "SUCCESS":
                print(f"🟣 [Waluigi] {task.__class__.__name__}: Già completato (DB).")
                return
        except Exception as e:
            print(f"⚠️ Nota: Impossibile recuperare stato preventivo: {e}")

        # 2. DIPENDENZE
        # Risolviamo prima i figli
        for dep in task.requires():
            self.build(dep)

        # 3. REGISTRAZIONE E LOCK (L'unica che serve davvero)
        # Qui passiamo TUTTO: job_id, name e params
        r = requests.post(f"{self.server_url}/register", json={
            "job_id": self.job_id,  # <-- FONDAMENTALE
            "name": task.__class__.__name__,
            "params": task.param_str
        }, timeout=2)
        
        if r.status_code == 409:
            raise Exception(f"🚫 Task {task.__class__.__name__} occupato altrove.")
        
        if r.status_code == 204: 
            print(f"🟣 [Waluigi] {task.__class__.__name__}: Già completato (visto in fase di register).")
            return

        # 4. ESECUZIONE
        try:
            print(f"🚀 [Waluigi] Inizio: {task.__class__.__name__}")
            task.run()
            # Notifica Successo (Passiamo il job_id anche qui per coerenza)
            requests.post(f"{self.server_url}/update", json={
                "task_id": task_id,
                "job_id": self.job_id,
                "name": task.__class__.__name__,
                "params": task.param_str,
                "status": "SUCCESS"
            })
        except Exception as e:
            requests.post(f"{self.server_url}/update", json={
                "task_id": task_id,
                "job_id": self.job_id,
                "name": task.__class__.__name__,
                "params": task.param_str,
                "status": "FAILED"
            })
            raise e
