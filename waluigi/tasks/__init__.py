REGISTRY = {
    "MergeDatasets": "waluigi.tasks.merge_datasets",
    "JoinDatasets":  "waluigi.tasks.join_datasets",
}

def get_command(task_type: str) -> str:
    if task_type not in REGISTRY:
        raise ValueError(f"Unknown task type '{task_type}'. Available: {list(REGISTRY)}")
    return f"python -m {REGISTRY[task_type]}"
