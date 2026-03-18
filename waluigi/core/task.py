import requests
from types import SimpleNamespace

class Task:

    id = None
    namespace = "default"
    resources = {
       "coin": 1
    }
    
    def __init__(self, id = None, params=None, attributes=None):
        self.params = SimpleNamespace(**(params or {}))
        self.attributes = SimpleNamespace(**(attributes or {}))
        self.id = id if id else self.id
        self.id = self.id if self.id else self.__class__.__name__
            
    def requires(self):
        return []

    def run(self):
        raise NotImplementedError

    def hash(self, nsdict):
        return " ".join(
           f"{k}:{v}" 
           for k, v in sorted(vars(nsdict).items())
        )
        