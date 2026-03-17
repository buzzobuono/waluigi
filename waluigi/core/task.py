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
            
    def requires(self):
        return []

    def run(self):
        raise NotImplementedError

    def hash(self, nsdict):
        return " ".join(
           f"{k}:{v}" 
           for k, v in sorted(vars(nsdict).items())
        )
        