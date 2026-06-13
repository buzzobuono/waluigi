REGISTRY = {
    # — catalog admin —
    "CatalogCreateSource":    "waluigi.tasks.catalog_create_source",
    "CatalogCreateDataset":   "waluigi.tasks.catalog_create_dataset",
    "CatalogSetExpectations": "waluigi.tasks.catalog_set_expectations",
    "CatalogSetCharts":       "waluigi.tasks.catalog_set_charts",
    "CatalogDefineSchema":    "waluigi.tasks.catalog_define_schema",
    # — data transforms —
    "MergeDatasets":          "waluigi.tasks.merge_datasets",
    "JoinDatasets":           "waluigi.tasks.join_datasets",
    "FilterDataset":          "waluigi.tasks.filter_dataset",
    "SelectColumns":          "waluigi.tasks.select_columns",
    "AggregateDataset":       "waluigi.tasks.aggregate_dataset",
    "PivotDataset":           "waluigi.tasks.pivot_dataset",
    "DeduplicateDataset":     "waluigi.tasks.deduplicate_dataset",
    "AddDerivedColumns":      "waluigi.tasks.add_derived_columns",
    # — ingestion —
    "FetchHttp":              "waluigi.tasks.fetch_http",
}

def get_command(task_type: str) -> str:
    if task_type not in REGISTRY:
        raise ValueError(f"Unknown task type '{task_type}'. Available: {list(REGISTRY)}")
    return f"python -m {REGISTRY[task_type]}"
