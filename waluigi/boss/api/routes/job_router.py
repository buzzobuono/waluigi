import time
from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import (
    job_service, boss_engine, namespaces_repository, job_definition_service,
)
from waluigi.commons.dag import DAGSpec, parse_definition


router = APIRouter(
    prefix="/namespaces/{namespace}/jobs",
    tags=["Jobs"]
)


def _resolve_job(data: dict, namespace: str, job_def_svc) -> tuple[list, dict, str | None]:
    """
    Resolve jobRef or jobSpec to a flat tasks list.
    Returns (tasks_list, merged_metadata, error_message_or_None).
    """
    spec = data.get("spec", {})
    metadata = dict(data.get("metadata", {}))

    if "jobRef" in spec:
        def_name = spec["jobRef"]["name"]
        job_def = job_def_svc.get(namespace, def_name)
        if job_def is None:
            return [], {}, f"JobDefinition '{def_name}' not found in namespace '{namespace}'"
        tasks_list = job_def["spec"].get("tasks", [])
        # Definition metadata (e.g. workdir) as base; run metadata overrides.
        metadata = {**job_def.get("metadata", {}), **metadata}
    elif "jobSpec" in spec:
        tasks_list = spec["jobSpec"].get("tasks", [])
    else:
        return [], {}, "spec must contain 'jobRef' or 'jobSpec'"

    return tasks_list, metadata, None


@router.get("")
async def list_jobs(namespace: str, svc=Depends(job_service)):
    return ok(svc.list(namespace))


@router.get("/{job_id}")
async def get_job(namespace: str, job_id: str, svc=Depends(job_service)):
    job = svc.get(namespace, job_id)
    if job is None:
        return ko(f"Job '{job_id}' not found", status=404)
    return ok(job)


@router.post("")
async def submit(
    namespace: str,
    request: Request,
    job_svc=Depends(job_service),
    engine=Depends(boss_engine),
    ns_repo=Depends(namespaces_repository),
    job_def_svc=Depends(job_definition_service),
):
    if not ns_repo.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)

    data = await request.json()
    kind = data.get("kind")
    spec = data.get("spec", {})

    if kind != "Job":
        return ko("Unsupported kind. Use 'Job'", status=400)

    tasks_list, metadata, err = _resolve_job(data, namespace, job_def_svc)
    if err:
        return ko(err, status=400)

    run_params       = dict(spec.get("params", {}))
    run_attributes   = dict(spec.get("attributes", {}))
    execution_policy = spec.get("executionPolicy", "Ephemeral")
    concurrency      = spec.get("concurrencyPolicy", "Forbid")

    base_name = metadata.get("name", "unnamed")
    timestamp = None

    if execution_policy == "Ephemeral":
        timestamp  = time.time()
        suffixed   = {t["id"]: f"{t['id']}@{timestamp}" for t in tasks_list if "id" in t}
        tasks_list = [
            {
                **dict(t),
                "id":       suffixed[t["id"]],
                "requires": [suffixed.get(r, r) for r in t.get("requires", [])],
            }
            for t in tasks_list
        ]
    else:  # Stateful
        job_id = base_name
        status = job_svc.get_status(namespace, job_id)
        if status and status not in ("SUCCESS", "FAILED", "CANCELLED"):
            if concurrency == "Forbid":
                return ko(f"Job '{job_id}' is already active ({status})", status=409)
            elif concurrency == "Replace":
                job_svc.cancel(namespace, job_id)
            # Allow: proceed regardless

    job_id = f"{base_name}@{timestamp}" if timestamp else base_name

    resolved = {
        "kind":     "Job",
        "metadata": {**metadata, "namespace": namespace, "timestamp": timestamp,
                     "executionPolicy": execution_policy},
        "spec": {
            "tasks":      tasks_list,
            "params":     run_params,
            "attributes": run_attributes,
        },
    }

    try:
        flat_tasks = parse_definition(resolved)
    except ValueError as e:
        return ko(str(e), status=400)

    try:
        spec = DAGSpec(flat_tasks)
        job_svc.create(
            namespace=namespace, job_id=job_id,
            execution_policy=execution_policy, concurrency_policy=concurrency,
            metadata=metadata, spec=flat_tasks,
        )
        engine.register_job(namespace, job_id, spec)
        return ok({"job_id": job_id, "task_id": spec.terminal().id}, status=202)
    except Exception as e:
        return ko(str(e), status=500)


@router.post("/{job_id}/_reset")
async def reset_job(namespace: str, job_id: str, svc=Depends(job_service)):
    if not svc.reset(namespace, job_id):
        return ko(f"Job '{job_id}' not found or not in a terminal state (FAILED/CANCELLED)", status=409)
    return ok({"job_id": job_id, "status": "PENDING"})


@router.post("/{job_id}/_pause")
async def pause_job(namespace: str, job_id: str, svc=Depends(job_service)):
    if not svc.pause(namespace, job_id):
        return ko(f"Job '{job_id}' not found or not pausable (must be PENDING or RUNNING)", status=409)
    return ok({"job_id": job_id, "status": "PAUSED"})


@router.post("/{job_id}/_resume")
async def resume_job(namespace: str, job_id: str, svc=Depends(job_service)):
    if not svc.resume(namespace, job_id):
        return ko(f"Job '{job_id}' not found or not paused", status=409)
    return ok({"job_id": job_id, "status": "PENDING"})


@router.post("/{job_id}/_cancel")
async def cancel_job(namespace: str, job_id: str, svc=Depends(job_service)):
    if not svc.cancel(namespace, job_id):
        return ko(f"Job '{job_id}' not found or already terminal", status=409)
    return ok({"job_id": job_id, "status": "CANCELLED"})


@router.delete("/{job_id}")
async def delete_job(namespace: str, job_id: str, svc=Depends(job_service)):
    if not svc.delete(namespace, job_id):
        return ko(f"Job '{job_id}' not found or not in terminal state", status=409)
    return ok({"job_id": job_id})
