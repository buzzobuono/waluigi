import requests
from types import SimpleNamespace

class Task:

    id = None
    namespace = "unknown"
    resources = {
       "coin": 1
    }
    def __init__(self, id = None, tags=None, params=None, attributes=None):
        self.tags = tags or []
        self.params = SimpleNamespace(**(params or {}))
        self.attributes = SimpleNamespace(**(attributes or {}))
        
        self.id = self.id if self.id else self.__class__.__name__
        
        if tags:
            self.id = self.id + " tags:" + " ".join(tags)
        self.engine = None

    def is_complete(self):
        """
        Check prima del run. 
        Default: chiede al DB tramite l'API status.
        """
        try:
            r = requests.get(f"{self.engine.server_url}/status/{self.id}/{self.hash(self.params)}", timeout=2)
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
            "params": self.hash(self.params),
            "attributes": self.hash(self.attributes),
            "status": "SUCCESS"
        }, timeout=2)

    def requires(self):
        return []

    def run(self):
        raise NotImplementedError

    def hash(self, nsdict):
        return " ".join(
           f"{k}:{v}" 
           for k, v in sorted(vars(nsdict).items())
        )
    
    #def hash(self, nsdict):
    #    if not nsdict: return ""
    #    # Converte in dizionario se è SimpleNamespace
    #    d = vars(nsdict) if not isinstance(nsdict, dict) else nsdict
    #    # ORDINA le chiavi alfabeticamente: questo è il segreto
    #    return " ".join(f"{k}:{d[k]}" for k in sorted(d.keys()))
    