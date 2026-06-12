import json
import os
import subprocess
import sys

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


def _extract_from_yaml(file_path: str, task_id: str) -> tuple:
    """
    Returns (command, script, config, job_params) for the given task_id.
    Supports multi-document YAMLs (---).
    """
    with open(file_path) as f:
        docs = list(yaml.safe_load_all(f))

    for doc in (docs or []):
        if not doc:
            continue
        kind = doc.get('kind')
        if kind not in ('Job', 'JobDefinition'):
            continue
        spec = doc.get('spec', {})
        job_spec = spec.get('jobSpec') or spec   # JobDefinition has tasks at spec level
        tasks = job_spec.get('tasks', [])
        job_params = dict(spec.get('params', {}))

        for task in tasks:
            if task.get('id') != task_id:
                continue
            task_spec = task.get('taskSpec', {})
            return (
                task_spec.get('command'),
                task_spec.get('script'),
                dict(task.get('config', {})),
                job_params,
            )

    return None, None, {}, {}


def run_task(cmd, file, task_id, params, namespace, catalog_url):
    parsed_params = _parse_params(params)
    script = None
    config = {}
    job_params = {}

    if file:
        if not task_id:
            print("Error: --task required when --file is specified", file=sys.stderr)
            sys.exit(1)
        extracted_cmd, script, config, job_params = _extract_from_yaml(file, task_id)
        if cmd is None:
            cmd = extracted_cmd
        if cmd is None and script is None:
            print(f"Error: task '{task_id}' not found or has no command/script in '{file}'",
                  file=sys.stderr)
            sys.exit(1)

    if cmd is None and script is None:
        print("Error: provide a command or use --file + --task", file=sys.stderr)
        sys.exit(1)

    # Build environment
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['WALUIGI_TASK_ID'] = 'local-run'
    env['WALUIGI_JOB_ID']  = 'local-run'

    effective_ns = namespace or env.get('WALUIGI_CATALOG_NAMESPACE', '')
    if effective_ns:
        env['WALUIGI_CATALOG_NAMESPACE'] = effective_ns

    effective_catalog = catalog_url or env.get('WALUIGI_CATALOG_URL', '')
    if effective_catalog:
        env['WALUIGI_CATALOG_URL'] = effective_catalog

    # CLI params override job-level params
    merged = {**job_params, **parsed_params}
    for k, v in merged.items():
        env[f'WALUIGI_PARAM_{k.upper()}'] = str(v)

    if config:
        env['WALUIGI_CONFIG'] = json.dumps(config)

    if script:
        env['WALUIGI_SCRIPT'] = script
        cmd = "python -c \"import os; exec(os.environ['WALUIGI_SCRIPT'])\""

    # Header
    label = '<inline script>' if script else cmd
    print(f"[wlctl run] command   : {label}")
    print(f"[wlctl run] namespace : {effective_ns or '(not set)'}")
    print(f"[wlctl run] params    : {merged or '(none)'}")
    if effective_catalog:
        print(f"[wlctl run] catalog   : {effective_catalog}")
    print()

    result = subprocess.run(cmd, shell=True, env=env)
    sys.exit(result.returncode)
