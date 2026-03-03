import requests

class Task:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.params = "_".join(f"{k}-{v}" for k, v in sorted(kwargs.items()))
        self.id = f"{self.__class__.__name__}"
        self.engine = None  # Iniettato dall'Engine durante il build

    def is_complete(self):
        """
        Check prima del run. 
        Default: chiede al DB tramite l'API status.
        """
        try:
            r = requests.get(f"{self.engine.server_url}/status/{self.id}/{self.params}", timeout=2)
            return r.status_code == 200 and r.json().get("status") == "SUCCESS"
        except:
            return False

    def complete(self, job_id):
        """
        Azione dopo il run. 
        Default: invia l'update SUCCESS al DB.
        """
        requests.post(f"{self.engine.server_url}/update", json={
            "task_id": self.id,
            "job_id": job_id,
            "name": self.__class__.__name__,
            "params": self.params,
            "status": "SUCCESS"
        }, timeout=2)

    def requires(self):
        return []

    def run(self):
        raise NotImplementedError
