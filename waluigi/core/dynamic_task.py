import json
from types import SimpleNamespace

class DynamicTask:
    
    def __init__(self, data, parent=None):
        self.name = data.get('name')
        self.id = data.get('id', self.name)
        self.namespace = data.get('namespace', 'default')
        self.command = data.get('command', '')
        self.resources = data.get('resources', {'coin': 1.0})
        self.params = SimpleNamespace(**self._resolve_params(data.get('params', {}), parent))
        self.attributes = SimpleNamespace(**data.get('attributes', {}))
        
        self._raw_requires = data.get('requires', [])
        self._parent = parent

    def _resolve_params(self, params, parent):
        resolved = {}
        for k, v in params.items():
            if isinstance(v, str) and "${parent.params." in v:
                # Estrae 'source' da '${parent.params.source}'
                param_key = v.split('.')[-1].replace('}', '')
                resolved[k] = getattr(parent.params, param_key) if parent else v
            else:
                resolved[k] = v
        return resolved

    def requires(self):
        return [DynamicTask(d, parent=self) for d in self._raw_requires]
        
    def hash(self, nsdict):
        return " ".join(
           f"{k}:{v}" 
           for k, v in sorted(vars(nsdict).items())
        )