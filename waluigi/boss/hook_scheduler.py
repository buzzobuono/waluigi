from __future__ import annotations
import json
import time
import logging

logger = logging.getLogger("waluigi")


def fire_hooks(
    namespace: str,
    job_id: str,
    event_type: str,
    hook_svc,
    job_svc,
    job_def_svc,
    engine,
    task_repo=None,
) -> None:
    """Fire all enabled JobHooks matching the given job and event type."""
    base_name = job_id.rsplit("@", 1)[0]
    hooks = hook_svc.list_enabled_for_job(namespace, base_name)
    hooks = [h for h in hooks if event_type in (h.get("spec", {}).get("watch", {}).get("events") or [])]
    if not hooks:
        return

    failed_tasks: list[str] = []
    if task_repo and event_type == "failure":
        failed_tasks = task_repo.list_failed_for_job(namespace, job_id)

    ctx = {
        "event.status":       event_type.upper(),
        "event.job_id":       job_id,
        "event.job_name":     base_name,
        "event.namespace":    namespace,
        "event.failed_tasks": json.dumps(failed_tasks),
    }

    for hook in hooks:
        try:
            _fire_one(hook, namespace, ctx, hook_svc, job_svc, job_def_svc, engine)
        except Exception as e:
            logger.error(f"JobHook '{hook['id']}': error firing: {e}")


def _fire_one(hook, namespace, ctx, hook_svc, job_svc, job_def_svc, engine) -> None:
    from waluigi.commons.dag import DAGSpec, parse_definition

    spec    = hook["spec"]
    trigger = spec.get("trigger") or {}
    job_ref_name = (trigger.get("jobRef") or {}).get("name")
    if not job_ref_name:
        return

    job_def = job_def_svc.get(namespace, job_ref_name)
    if not job_def:
        logger.warning(f"JobHook '{hook['id']}': JobDefinition '{job_ref_name}' not found — skipping")
        return

    params = _resolve_params(trigger.get("params") or {}, ctx)

    execution_policy = trigger.get("executionPolicy", "Ephemeral")
    concurrency      = trigger.get("concurrencyPolicy", "Allow")
    tasks_list       = list(job_def["spec"].get("tasks", []))
    def_meta         = job_def.get("metadata") or {}
    metadata         = {**def_meta, "namespace": namespace}

    timestamp = None
    if execution_policy == "Ephemeral":
        timestamp = time.time()
        suffixed   = {t["id"]: f"{t['id']}@{timestamp}" for t in tasks_list if "id" in t}
        tasks_list = [
            {
                **dict(t),
                "id":       suffixed[t["id"]],
                "requires": [suffixed.get(r, r) for r in t.get("requires", [])],
            }
            for t in tasks_list
        ]
    else:
        trig_job_id = job_ref_name
        status = job_svc.get_status(namespace, trig_job_id)
        if status and status not in ("SUCCESS", "FAILED", "CANCELLED"):
            if concurrency == "Forbid":
                logger.info(f"JobHook '{hook['id']}': trigger job active — skipping (Forbid)")
                return
            elif concurrency == "Replace":
                job_svc.cancel(namespace, trig_job_id)

    trig_job_id = f"{job_ref_name}@{timestamp}" if timestamp else job_ref_name

    resolved = {
        "kind":     "Job",
        "metadata": {**metadata, "namespace": namespace, "timestamp": timestamp},
        "spec":     {"tasks": tasks_list, "params": params, "attributes": {}},
    }

    parsed = parse_definition(resolved)
    dag    = DAGSpec(parsed)
    job_svc.create(
        namespace=namespace, job_id=trig_job_id,
        execution_policy=execution_policy, concurrency_policy=concurrency,
        metadata=metadata, spec=parsed,
    )
    engine.register_job(namespace, trig_job_id, dag)
    logger.info(f"🔔 JobHook '{hook['id']}': fired {namespace}/{trig_job_id}")


def _resolve_params(raw: dict, ctx: dict) -> dict:
    result = {}
    for k, v in raw.items():
        if isinstance(v, str):
            for ctx_k, ctx_v in ctx.items():
                v = v.replace(f"${{{ctx_k}}}", str(ctx_v))
        result[k] = v
    return result
