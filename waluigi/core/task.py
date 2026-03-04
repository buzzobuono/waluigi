import requests

class Task:
    
    id = None
    namespace = "unknown"
    identities = [] # Cambiano il nome visibile
    states = [] # Cambiano l'hash (il contenuto)
    traits = [] # Metadati tecnici (non cambiano l'ID)
    
    def __init__(self, **kwargs):
        
        self.tag = kwargs.pop('tag', None)
        self.__dict__.update(kwargs)
        self.id = self.id if self.id else self.__class__.__name__
        if self.tag:
            self.id = self.id + "(" +self.tag + ")"
        self.params = " ".join(f"{k}:{v}" for k, v in sorted(kwargs.items()))
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

    def complete(self):
        """
        Azione dopo il run. 
        Default: invia l'update SUCCESS al DB.
        """
        requests.post(f"{self.engine.server_url}/update", json={
            "id": self.id,
            "namespace": self.namespace,
            "params": self.params,
            "status": "SUCCESS"
        }, timeout=2)

    def requires(self):
        return []

    def run(self):
        raise NotImplementedError
