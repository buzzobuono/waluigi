from types import SimpleNamespace


def parse_definition(pipeline_data):
    metadata = pipeline_data.get("metadata", {})
    spec = pipeline_data.get("spec", {})
    task_list = spec.get("tasks", [])
    pipeline_params = spec.get("params", {})
    pipeline_attributes = spec.get("attributes", {})

    if not task_list:
        raise ValueError("kind: Pipeline requires spec.tasks to be a list of tasks.")

    by_id = {}
    for t in task_list:
        t_flat = dict(t)
        tid = t_flat.get("id")
        
        if not tid:
            continue

        t_flat["params"] = {**pipeline_params, **t_flat.get("params", {})}
        t_flat["attributes"] = {**pipeline_attributes, **t_flat.get("attributes", {})}
        
        if "taskSpec" in t_flat:
            inner = t_flat.get("taskSpec", {})
            print(inner)
            t_flat["script"] = inner.get("script")
            t_flat["command"] = inner.get("command", "")
            if "resources" in inner:
                t_flat["resources"] = inner["resources"]
        
        elif "taskRef" in t_flat:
            t_flat["type"] = t_flat["taskRef"].get("name")
        
        t_flat["namespace"] = metadata.get("namespace", "default")

        by_id[tid] = t_flat

    all_requires = {dep for t in by_id.values() for dep in t.get("requires", [])}
    roots = [tid for tid in by_id if tid not in all_requires]

    if len(roots) != 1:
        raise ValueError(f"Pipeline must have exactly one terminal task, found: {roots}")

    def build(task_id):
        node = dict(by_id[task_id])
        dep_ids = node.get("requires", [])
        node["requires"] = [build(dep) for dep in dep_ids] if dep_ids else []
        return node

    return build(roots[0])

class DAGTask:
    def __init__(self, data, parent=None):
        self.id = data.get('id')
        self.namespace = data.get('namespace', 'default')
        self.type = data.get('type')
        self.command = data.get('command', '')
        self.script = data.get('script')
        self.resources = data.get('resources', {'coin': 1.0})
        self.params = SimpleNamespace(**data.get('params', {}))
        self.attributes = SimpleNamespace(**data.get('attributes', {}))
        self.config = data.get('config', {})
        self._raw_requires = data.get('requires', [])
        self._parent = parent

    def requires(self):
        return [DAGTask(d, parent=self) for d in self._raw_requires]
        
    def hash(self, nsdict):
        return " ".join(f"{k}:{v}" for k, v in sorted(vars(nsdict).items()))