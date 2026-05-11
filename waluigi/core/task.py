import json
from types import SimpleNamespace


def _expand_pipeline(task_list, pipeline_params):
    """Expand a flat Pipeline task list into a nested DynamicTask spec.

    Pipeline-level params are merged into every task (task-specific params win).
    No interpolation syntax needed — params are injected directly.
    The terminal task (the one nothing else requires) becomes the DAG root.
    """
    tasks = []
    for t in task_list:
        t = dict(t)
        t["params"] = {**pipeline_params, **t.get("params", {})}
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