import json
import os
import subprocess
import sys
import time

import yaml


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
    """Returns {name: {command, script, params}} from TaskDefinition docs."""
    taskdefs = {}
    for doc in docs:
        if doc.get('kind') != 'TaskDefinition':
            continue
        name = (doc.get('metadata') or {}).get('name')
        if not name:
            continue
        spec      = doc.get('spec', {})
        task_spec = spec.get('taskSpec', {})
        taskdefs[name] = {
            'command': task_spec.get('command'),
            'script':  task_spec.get('script'),
            'params':  dict(spec.get('params', {})),
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


def _resolve_task(task: dict, taskdefs: dict) -> tuple[str | None, str | None]:
    """Returns (command, script) for a task, resolving taskRef if needed."""
    task_spec = task.get('taskSpec') or {}
    if task_spec:
        return task_spec.get('command'), task_spec.get('script')

    task_ref  = task.get('taskRef') or {}
    ref_name  = task_ref.get('name')
    if ref_name:
        defn = taskdefs.get(ref_name)
        if defn:
            return defn.get('command'), defn.get('script')
        return None, None   # unresolvable

    return None, None


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


def _inject_params(env: dict, job_params: dict, task_params: dict,
                   cli_params: dict, attributes: dict, config: dict) -> dict:
    merged = {**job_params, **task_params, **cli_params}
    for k, v in merged.items():
        env[f'WALUIGI_PARAM_{k.upper()}'] = str(v)
    for k, v in attributes.items():
        env[f'WALUIGI_ATTRIBUTE_{k.upper()}'] = str(v)
    if config:
        env['WALUIGI_CONFIG'] = json.dumps(config)
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
        cmd, script = _resolve_task(task, taskdefs)
        return (
            cmd,
            script,
            dict(task.get('config', {})),
            job_params,
            dict(task.get('params', {})),
            dict(task.get('attributes', {})),
        )

    return None, None, {}, {}, {}, {}


def run_task(cmd, file, task_id, params, namespace, catalog_url):
    parsed_params    = _parse_params(params)
    script           = None
    config           = {}
    job_params       = {}
    task_params      = {}
    task_attributes  = {}

    if file:
        if not task_id:
            # No task_id → run the whole job
            run_job(file, params, namespace, catalog_url)
            return
        extracted_cmd, script, config, job_params, task_params, task_attributes = \
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

    merged = _inject_params(env, job_params, task_params, parsed_params, task_attributes, config)
    cmd    = _make_cmd(cmd, script, env)

    label = '<inline script>' if script else cmd
    print(f"[wlctl run] command   : {label}")
    print(f"[wlctl run] namespace : {effective_ns or '(not set)'}")
    print(f"[wlctl run] params    : {merged or '(none)'}")
    if effective_catalog:
        print(f"[wlctl run] catalog   : {effective_catalog}")
    print()

    result = subprocess.run(cmd, shell=True, env=env)
    sys.exit(result.returncode)


# ── Full job ──────────────────────────────────────────────────────────────────

def run_job(file, params, namespace, catalog_url):
    parsed_params = _parse_params(params)
    docs          = _load_docs(file)
    taskdefs      = _collect_taskdefs(docs)
    tasks, job_params = _find_job(docs)

    if not tasks:
        print(f"Error: no tasks found in '{file}'", file=sys.stderr)
        sys.exit(1)

    ordered = _toposort(tasks)

    base_env, effective_ns, effective_catalog = _build_base_env(namespace, catalog_url)

    ids_line = " → ".join(t['id'] for t in ordered)
    print(f"[wlctl run job] file      : {file}")
    print(f"[wlctl run job] namespace : {effective_ns or '(not set)'}")
    print(f"[wlctl run job] params    : {parsed_params or '(none)'}")
    print(f"[wlctl run job] tasks     : {len(ordered)}  {ids_line}")
    if effective_catalog:
        print(f"[wlctl run job] catalog   : {effective_catalog}")
    print()

    for i, task in enumerate(ordered, 1):
        task_id = task['id']
        cmd, script = _resolve_task(task, taskdefs)

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
        cmd = _make_cmd(cmd, script, task_env)

        t0     = time.monotonic()
        result = subprocess.run(cmd, shell=True, env=task_env)
        elapsed = time.monotonic() - t0

        if result.returncode == 0:
            print(f"\n✓ {task_id}  ({elapsed:.1f}s)\n")
        else:
            print(f"\n✗ {task_id}  FAILED (exit {result.returncode})")
            print(f"\n[wlctl run job] stopped — task '{task_id}' failed")
            sys.exit(result.returncode)

    print(f"[wlctl run job] completed — {len(ordered)}/{len(ordered)} tasks OK")
