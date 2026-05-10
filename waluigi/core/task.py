import json
from types import SimpleNamespace


def _expand_tasks(spec):
    """Convert flat tasks list to nested requires tree.

    ${parent.params.X} in each task is resolved against the outer Pipeline params,
    since in the flat format there is no task-level parent chain to walk.
    The outer spec id/name/params are preserved as the job identity; only the
    execution keys (type, command, script, config, resources, affinity, requires)
    are taken from the terminal task (the one nothing else requires).
    """
    if "tasks" not in spec:
        return spec

    outer_params = spec.get("params", {})

    def resolve_params(params):
        result = {}
        for k, v in params.items():
            if isinstance(v, str) and "${parent.params." in v:
                key = v.split(".")[-1].rstrip("}")
                result[k] = outer_params.get(key, v)
            else:
                result[k] = v
        return result

    tasks = []
    for t in spec["tasks"]:
        t = dict(t)
        if "params" in t:
            t["params"] = resolve_params(t["params"])
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

    # Outer spec keeps id/name/namespace/params (pipeline identity).
    # Terminal task contributes type/command/script/config/resources/affinity/requires.
    result = {k: v for k, v in spec.items() if k != "tasks"}
    root = build(roots[0])
    for key in ("type", "command", "script", "config", "resources", "affinity", "requires"):
        if key in root:
            result[key] = root[key]
    return result


class DynamicTask:

    def __init__(self, data, parent=None):
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