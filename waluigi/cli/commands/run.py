import json
import os
import re
import subprocess
import sys
import time

import yaml

_BUILTIN_COMMANDS = {
    "IngestRest":             "python -m waluigi.tasks.ingest_rest",
    "FilterDataset":          "python -m waluigi.tasks.filter_dataset",
    "SelectColumns":          "python -m waluigi.tasks.select_columns",
    "AddDerivedColumns":      "python -m waluigi.tasks.add_derived_columns",
    "AggregateDataset":       "python -m waluigi.tasks.aggregate_dataset",
    "JoinDatasets":           "python -m waluigi.tasks.join_datasets",
    "MergeDatasets":          "python -m waluigi.tasks.merge_datasets",
    "PivotDataset":           "python -m waluigi.tasks.pivot_dataset",
    "DeduplicateDataset":     "python -m waluigi.tasks.deduplicate_dataset",
    "CatalogCreateSource":    "python -m waluigi.tasks.catalog_create_source",
    "CatalogCreateDataset":   "python -m waluigi.tasks.catalog_create_dataset",
    "CatalogDefineSchema":    "python -m waluigi.tasks.catalog_define_schema",
    "CatalogSetExpectations": "python -m waluigi.tasks.catalog_set_expectations",
    "CatalogSetCharts":       "python -m waluigi.tasks.catalog_set_charts",
    "SharePointExport":       "python -m waluigi.tasks.sharepoint_export",
}


def _expand_config(obj, env: dict):
    """Recursively expand ${VAR} placeholders in config string values."""
    if isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: env.get(m.group(1), m.group(0)), obj)
    if isinstance(obj, dict):
        return {k: _expand_config(v, env) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_config(i, env) for i in obj]
    return obj


def _parse_params(raw: list[str]) -> dict[str, str]:
    result = {}
    for item in (raw or []):
        if '=' not in item:
            print(f"Error: param '{item}' must be key=value", file=sys.stderr)
            sys.exit(1)
        k, v = item.split('=', 1)
        result[k.strip()] = v.strip()
    return result


def _load_docs(file_path: str) -> list:
    with open(file_path) as f:
        return [d for d in yaml.safe_load_all(f) if d]


def _collect_taskdefs(docs: list) -> dict:
    """Returns {name: {command, script, prepare}} from TaskDefinition docs in the YAML file."""
    taskdefs = {}
    for doc in docs:
        if doc.get('kind') != 'TaskDefinition':
            continue
        name = (doc.get('metadata') or {}).get('name')
        if not name:
            continue
        spec = doc.get('spec', {})
        taskdefs[name] = {
            'command': spec.get('command'),
            'script':  spec.get('script'),
            'prepare': spec.get('prepare'),
        }
    return taskdefs


def _find_job(docs: list) -> tuple[list, dict]:
    """Returns (tasks, job_params) from the first Job or JobDefinition found."""
    for doc in docs:
        kind = doc.get('kind')
        if kind not in ('Job', 'JobDefinition'):
            continue
        spec     = doc.get('spec', {})
        job_spec = spec.get('jobSpec') or spec
        return job_spec.get('tasks', []), dict(spec.get('params', {}))
    return [], {}


def _resolve_task(task: dict, taskdefs: dict) -> tuple[str | None, str | None, object]:
    """Returns (command, script, prepare) for a task, resolving taskRef if needed."""
    task_spec = task.get('taskSpec') or {}
    if task_spec:
        return task_spec.get('command'), task_spec.get('script'), task_spec.get('prepare')

    task_ref = task.get('taskRef') or {}
    ref_name = task_ref.get('name')
    if ref_name:
        # 1. TaskDefinition in the same YAML file
        defn = taskdefs.get(ref_name)
        if defn:
            return defn.get('command'), defn.get('script'), defn.get('prepare')
        # 2. Built-in task types (no prepare)
        if ref_name in _BUILTIN_COMMANDS:
            return _BUILTIN_COMMANDS[ref_name], None, None
        return None, None, None   # unresolvable

    return None, None, None


def _toposort(tasks: list) -> list:
    id_to_task = {t['id']: t for t in tasks}
    visited    = set()
    order      = []

    def visit(task_id: str):
        if task_id in visited:
            return
        visited.add(task_id)
        for dep in (id_to_task.get(task_id, {}).get('requires') or []):
            visit(dep)
        if task_id in id_to_task:
            order.append(id_to_task[task_id])

    for task in tasks:
        visit(task['id'])
    return order


def _build_base_env(namespace: str | None, catalog_url: str | None) -> tuple[dict, str, str]:
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['WALUIGI_JOB_ID']   = 'local-run'

    effective_ns      = namespace    or env.get('WALUIGI_CATALOG_NAMESPACE', '')
    effective_catalog = catalog_url  or env.get('WALUIGI_CATALOG_URL', '')

    if effective_ns:
        env['WALUIGI_CATALOG_NAMESPACE'] = effective_ns
    if effective_catalog:
        env['WALUIGI_CATALOG_URL'] = effective_catalog

    return env, effective_ns, effective_catalog


def _inject_prepare_dir(env: dict, prepare_dir: str | None) -> None:
    """Inject WALUIGI_PREPARE_DIR and prepend it to PYTHONPATH."""
    if not prepare_dir:
        return
    env['WALUIGI_PREPARE_DIR'] = prepare_dir
    existing_pp = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = f"{prepare_dir}:{existing_pp}" if existing_pp else prepare_dir


def _run_prepare(prepare, env: dict, cwd: str | None, task_id: str) -> None:
    """Run prepare steps sequentially. Exits the process on first failure."""
    if not prepare:
        return
    steps = [prepare] if isinstance(prepare, str) else prepare
    for step in steps:
        print(f"[wlrun][prepare] {step}")
        result = subprocess.run(step, shell=True, env=env, cwd=cwd)
        if result.returncode != 0:
            print(f"\n✗ [{task_id}] prepare FAILED (exit {result.returncode})", file=sys.stderr)
            sys.exit(result.returncode)


def _inject_params(env: dict, job_params: dict, task_params: dict,
                   cli_params: dict, attributes: dict, config: dict) -> dict:
    merged = {**job_params, **task_params, **cli_params}
    for k, v in merged.items():
        env[f'WALUIGI_PARAM_{k.upper()}'] = str(v)
    for k, v in attributes.items():
        env[f'WALUIGI_ATTRIBUTE_{k.upper()}'] = str(v)
    if config:
        env['WALUIGI_CONFIG'] = json.dumps(_expand_config(config, env))
    return merged


def _make_cmd(cmd: str | None, script: str | None, env: dict) -> str:
    if script:
        env['WALUIGI_SCRIPT'] = script
        return "python -c \"import os; exec(os.environ['WALUIGI_SCRIPT'])\""
    return cmd


# ── Single task ───────────────────────────────────────────────────────────────

def _extract_from_yaml(file_path: str, task_id: str) -> tuple:
    docs       = _load_docs(file_path)
    taskdefs   = _collect_taskdefs(docs)
    tasks, job_params = _find_job(docs)

    for task in tasks:
        if task.get('id') != task_id:
            continue
        cmd, script, prepare = _resolve_task(task, taskdefs)
        return (
            cmd,
            script,
            prepare,
            dict(task.get('config', {})),
            job_params,
            dict(task.get('params', {})),
            dict(task.get('attributes', {})),
        )

    return None, None, None, {}, {}, {}, {}


def run_task(cmd, file, task_id, params, namespace, catalog_url, prepare_dir=None, worker_dir=None):
    parsed_params    = _parse_params(params)
    script           = None
    prepare          = None
    config           = {}
    job_params       = {}
    task_params      = {}
    task_attributes  = {}

    if file:
        if not task_id:
            # No task_id → run the whole job
            run_job(file, params, namespace, catalog_url,
                    prepare_dir=prepare_dir, worker_dir=worker_dir)
            return
        extracted_cmd, script, prepare, config, job_params, task_params, task_attributes = \
            _extract_from_yaml(file, task_id)
        if cmd is None:
            cmd = extracted_cmd
        if cmd is None and script is None:
            print(f"Error: task '{task_id}' not found or has no command/script in '{file}'",
                  file=sys.stderr)
            sys.exit(1)

    if cmd is None and script is None:
        print("Error: provide a command or use --file + --task", file=sys.stderr)
        sys.exit(1)

    env, effective_ns, effective_catalog = _build_base_env(namespace, catalog_url)
    env['WALUIGI_TASK_ID'] = task_id or 'local-run'

    _inject_prepare_dir(env, prepare_dir)
    merged = _inject_params(env, job_params, task_params, parsed_params, task_attributes, config)
    _run_prepare(prepare, env, worker_dir, task_id or 'local-run')
    cmd = _make_cmd(cmd, script, env)

    label = '<inline script>' if script else cmd
    print(f"[wlrun] command       : {label}")
    print(f"[wlrun] params        : {merged or '(none)'}")
    print()

    result = subprocess.run(cmd, shell=True, env=env, cwd=worker_dir)
    sys.exit(result.returncode)


# ── Full job ──────────────────────────────────────────────────────────────────

def run_job(file, params, namespace, catalog_url, prepare_dir=None, worker_dir=None):
    parsed_params = _parse_params(params)
    docs          = _load_docs(file)
    taskdefs      = _collect_taskdefs(docs)
    tasks, job_params = _find_job(docs)

    if not tasks:
        print(f"Error: no tasks found in '{file}'", file=sys.stderr)
        sys.exit(1)

    ordered = _toposort(tasks)

    base_env, effective_ns, effective_catalog = _build_base_env(namespace, catalog_url)
    _inject_prepare_dir(base_env, prepare_dir)

    ids_line = " → ".join(t['id'] for t in ordered)
    print(f"[wlrun] file          : {file}")
    print(f"[wlrun] params        : {parsed_params or '(none)'}")
    print(f"[wlrun] tasks         : {len(ordered)}  {ids_line}")
    print()

    for i, task in enumerate(ordered, 1):
        task_id = task['id']
        cmd, script, prepare = _resolve_task(task, taskdefs)

        sep = '─' * max(1, 44 - len(task_id))
        print(f"── Task {i}/{len(ordered)}: {task_id} {sep}")

        if cmd is None and script is None:
            ref = (task.get('taskRef') or {}).get('name')
            if ref:
                print(f"  WARNING: TaskDefinition '{ref}' not found in file — skipping\n")
            else:
                print("  WARNING: no command or script — skipping\n")
            continue

        task_env = base_env.copy()
        task_env['WALUIGI_TASK_ID'] = task_id

        merged = _inject_params(
            task_env,
            job_params,
            dict(task.get('params', {})),
            parsed_params,
            dict(task.get('attributes', {})),
            task.get('config', {}),
        )

        t0 = time.monotonic()
        _run_prepare(prepare, task_env, worker_dir, task_id)
        cmd = _make_cmd(cmd, script, task_env)

        result = subprocess.run(cmd, shell=True, env=task_env, cwd=worker_dir)
        elapsed = time.monotonic() - t0

        if result.returncode == 0:
            print(f"\n✓ {task_id}  ({elapsed:.1f}s)\n")
        else:
            print(f"\n✗ {task_id}  FAILED (exit {result.returncode})")
            print(f"\n[wlrun] stopped — task '{task_id}' failed")
            sys.exit(result.returncode)

    print(f"[wlrun] completed — {len(ordered)}/{len(ordered)} tasks OK")
