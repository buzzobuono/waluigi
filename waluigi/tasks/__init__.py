REGISTRY = {
    "MergeDatasets":      "waluigi.tasks.merge_datasets",
    "JoinDatasets":       "waluigi.tasks.join_datasets",
    "FilterDataset":      "waluigi.tasks.filter_dataset",
    "SelectColumns":      "waluigi.tasks.select_columns",
    "AggregateDataset":   "waluigi.tasks.aggregate_dataset",
    "PivotDataset":       "waluigi.tasks.pivot_dataset",
    "DeduplicateDataset": "waluigi.tasks.deduplicate_dataset",
    "AddDerivedColumns":  "waluigi.tasks.add_derived_columns",
}

def get_command(task_type: str) -> str:
    if task_type not in REGISTRY:
        raise ValueError(f"Unknown task type '{task_type}'. Available: {list(REGISTRY)}")
    return f"python -m {REGISTRY[task_type]}"
