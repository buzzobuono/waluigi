from types import SimpleNamespace


def parse_definition(definition) -> list[dict]:
    """
    Resolve a Job/JobDefinition descriptor into a flat ordered task list.
    Each task dict retains 'requires' (list of dependency task IDs).
    Tasks are returned in topological order (dependencies before dependents).
    """
    metadata = definition.get("metadata", {})
    spec = definition.get("spec", {})
    task_list = spec.get("tasks", [])
    pipeline_params = spec.get("params", {})
    pipeline_attributes = spec.get("attributes", {})

    if not task_list:
        raise ValueError("spec.tasks must be a non-empty list of tasks.")

    by_id: dict[str, dict] = {}
    for t in task_list:
        t_flat = dict(t)
        tid = t_flat.get("id")
        if not tid:
            continue

        t_flat["params"]     = {**pipeline_params,     **t_flat.get("params", {})}
        t_flat["attributes"] = {**t_flat.get("attributes", {}), **pipeline_attributes}

        if "taskSpec" in t_flat:
            inner = t_flat.get("taskSpec", {})
            t_flat["script"]  = inner.get("script")
            t_flat["command"] = inner.get("command", "")
            if "resources" in inner:
                t_flat["resources"] = inner["resources"]

        elif "taskRef" in t_flat:
            t_flat["type"] = t_flat["taskRef"].get("name")

        t_flat["namespace"] = metadata.get("namespace", "default")
        t_flat.setdefault("requires", [])
        by_id[tid] = t_flat

    # Validate: exactly one terminal task (nothing depends on it)
    all_requires = {dep for t in by_id.values() for dep in t.get("requires", [])}
    terminals = [tid for tid in by_id if tid not in all_requires]
    if len(terminals) != 1:
        raise ValueError(f"Pipeline must have exactly one terminal task, found: {terminals}")

    # Return tasks in topological order (dependencies first)
    ordered: list[dict] = []
    visited: set[str] = set()

    def visit(tid: str) -> None:
        if tid in visited:
            return
        visited.add(tid)
        for dep_id in by_id[tid].get("requires", []):
            if dep_id in by_id:
                visit(dep_id)
        ordered.append(by_id[tid])

    visit(terminals[0])
    return ordered


class DAGSpec:
    """Wraps a flat ordered task list, providing DAG traversal helpers."""

    def __init__(self, flat_tasks: list[dict]):
        self._tasks = [DAGTask(t) for t in flat_tasks]
        self._by_id: dict[str, DAGTask] = {t.id: t for t in self._tasks}

    def task(self, task_id: str) -> "DAGTask":
        return self._by_id[task_id]

    def all_tasks(self) -> list["DAGTask"]:
        return list(self._tasks)

    def deps_of(self, task_id: str) -> list["DAGTask"]:
        t = self._by_id.get(task_id)
        if not t:
            return []
        return [self._by_id[d] for d in t.requires if d in self._by_id]

    def terminal(self) -> "DAGTask":
        all_requires = {d for t in self._tasks for d in t.requires}
        terminals = [t for t in self._tasks if t.id not in all_requires]
        if len(terminals) != 1:
            raise ValueError(f"Expected 1 terminal task, found: {[t.id for t in terminals]}")
        return terminals[0]


class DAGTask:
    def __init__(self, data: dict):
        self.id         = data.get("id")
        self.namespace  = data.get("namespace", "default")
        self.type       = data.get("type")
        self.command    = data.get("command", "")
        self.script     = data.get("script")
        self.resources  = data.get("resources", {})
        self.params     = SimpleNamespace(**data.get("params", {}))
        self.attributes = SimpleNamespace(**data.get("attributes", {}))
        self.config     = data.get("config", {})
        self.requires: list[str] = data.get("requires", [])
        self.affinity: list[str] = data.get("affinity", [])

    def hash(self, nsdict) -> str:
        return " ".join(f"{k}:{v}" for k, v in sorted(vars(nsdict).items()))
