import json
from types import SimpleNamespace


def _expand_tasks(spec):
    """Convert flat tasks list with requires:[id] refs into nested requires tree."""
    if "tasks" not in spec:
        return spec
    by_id = {t["id"]: dict(t) for t in spec["tasks"]}
    required = {r for t in by_id.values() for r in t.get("requires", [])}
    roots = [tid for tid in by_id if tid not in required]
    if len(roots) != 1:
        raise ValueError(f"DAG must have exactly one root task, found: {roots}")
    def build(task_id):
        node = dict(by_id[task_id])
        dep_ids = node.pop("requires", [])
        if dep_ids:
            node["requires"] = [build(dep) for dep in dep_ids]
        return node
    result = {k: v for k, v in spec.items() if k != "tasks"}
    result.update(build(roots[0]))
    return result


class DynamicTask:

    def __init__(self, data, parent=None):
        data = _expand_tasks(data)
        self.name = data.get('name')
        self.id = data.get('id', self.name)
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