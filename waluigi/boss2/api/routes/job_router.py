from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss2.config.dependencies import job_service, boss_engine


router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"]
)


@router.get("")
async def list_jobs(svc=Depends(job_service)):
    return ok(svc.list())


@router.post("")
async def submit(
    request: Request,
    job_svc=Depends(job_service),
    engine=Depends(boss_engine),
):
    data = await request.json()
    kind = data.get("kind")
    timestamp = None

    if kind == "Job":
        # Each submission creates an independent job run with timestamp-suffixed task IDs
        timestamp = time.time()
        data = dict(data)
        spec_dict = dict(data.get("spec", {}))
        spec_dict["params"] = {**spec_dict.get("params", {}), "timestamp": timestamp}
        tasks_list = spec_dict.get("tasks", [])
        suffixed = {t["id"]: f"{t['id']}@{timestamp}" for t in tasks_list if "id" in t}
        spec_dict["tasks"] = [
            {
                **dict(t),
                "id": suffixed[t["id"]],
                "requires": [suffixed.get(r, r) for r in t.get("requires", [])],
            }
            for t in tasks_list
        ]
        data["spec"] = spec_dict

    elif kind != "StatefulJob":
        return ko("Unsupported kind. Use 'Job' or 'StatefulJob'", status=400)

    try:
        spec = parse_definition(data)
    except ValueError as e:
        return ko(str(e), status=400)

    metadata = dict(data.get("metadata", {}))
    metadata["timestamp"] = timestamp
    base_name = metadata.get("name", "unnamed")
    job_id = f"{base_name}@{timestamp}" if timestamp else base_name

    if not timestamp:
        status = job_svc.get_status(job_id)
        if status and status not in ("SUCCESS", "FAILED"):
            return ko(f"Job '{job_id}' is already active ({status})", status=409)

    try:
        task = DAGTask(spec)
        job_svc.create(job_id=job_id, metadata=metadata, spec=spec)
        engine.register_job(job_id, task, None)
        return ok({"job_id": job_id, "task_id": task.id}, status=202)
    except Exception as e:
        return ko(str(e), status=500)
        

@router.post("/{job_id}/_cancel")
async def cancel_job(job_id: str, svc=Depends(job_service)):
    if not svc.cancel(job_id):
        return ko(f"Job '{job_id}' not found or already terminal", status=409)
    return ok({"job_id": job_id, "status": "CANCELLED"})


@router.delete("/{job_id}")
async def delete_job(job_id: str, svc=Depends(job_service)):
    if not svc.delete(job_id):
        return ko(f"Job '{job_id}' not found or not in terminal state", status=409)
    return ok({"job_id": job_id})
