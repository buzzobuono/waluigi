import json
from types import SimpleNamespace

def _expand_pipeline(pipeline_data):
    spec = pipeline_data.get("spec", {})
    
    task_list = spec.get("tasks", [])
    pipeline_params = spec.get("params", {})

    if not task_list:
        raise ValueError("kind: Pipeline requires spec.tasks to be a list of tasks.")

    tasks = []
    for t in task_list:
        t = dict(t)
        # Injection dei parametri pipeline
        t["params"] = {**pipeline_params, **t.get("params", {})}
        
        # --- FLATTENING PER DYNAMICTASK ---
        # Se c'è un taskRef, il nome diventa il 'type'
        if "taskRef" in t:
            t["type"] = t["taskRef"].get("name")
            
        # Se c'è una taskSpec, estraiamo i campi interni portandoli al livello root
        if "taskSpec" in t:
            inner_spec = t["taskSpec"].get("spec", {})
            t["type"] = inner_spec.get("type", t.get("type"))
            t["script"] = inner_spec.get("script")
            t["command"] = inner_spec.get("command", "")
            # Se le risorse sono definite dentro taskSpec, hanno la priorità
            if "resources" in inner_spec:
                t["resources"] = inner_spec["resources"]

        tasks.append(t)

    by_id = {t["id"]: t for t in tasks}
    required = {r for t in by_id.values() for r in t.get("requires", [])}
    roots = [tid for tid in by_id if tid not in required]
    
    if len(roots) != 1:
        raise ValueError(f"Pipeline must have exactly one terminal task, found: {roots}")

    def build(task_id):
        node = dict(by_id[task_id])
        dep_ids = node.pop("requires", [])
        if dep_ids:
            # Ricorsione: i figli devono subire lo stesso trattamento di flattening
            node["requires"] = [build(dep) for dep in dep_ids]
        return node

    return build(roots[0])

class DynamicTask:

    def __init__(self, data, parent=None):
        self.id = data.get('id')
        self.namespace = data.get('namespace', 'default')
        self.type    = data.get('type')              # built-in task type — alternative to command/script
        self.command = data.get('command', '')
        self.script  = data.get('script')          # inline Python — alternative to command
        self.resources = data.get('resources', {'coin': 1.0})
        self.params = SimpleNamespace(**self._resolve_params(data.get('params', {}), parent))
        self.attributes = SimpleNamespace(**data.get('attributes', {}))
        self.config = data.get('config', {})
        
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