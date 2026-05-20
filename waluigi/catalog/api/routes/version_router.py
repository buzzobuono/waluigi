from fastapi import APIRouter, Depends
import os

from waluigi.catalog.config.args import args
from waluigi.core.utils import _model_dump
from waluigi.core.responses import ok, warn, ko
from waluigi.catalog.api.schemas import ReserveRequest, CommitRequest, VirtualRegisterRequest, ScanRequest
from waluigi.catalog.services.version_service import VersionService
from waluigi.catalog.config.dependencies import version_service

version_router = APIRouter(
    prefix="/datasets",
    tags=["Versions"]
)

@version_router.get("/{dataset_id:path}/versions",
         summary="List all committed versions (newest first)")
async def list_versions(dataset_id: str, version_service: VersionService = Depends(version_service)):
    try:
        return ok(version_service.list_versions(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)  

@version_router.post("/{dataset_id:path}/_reserve",
          summary="Reserve a new version (phase 1 of 2-phase write)",
          status_code=201)
async def dataset_reserve(dataset_id: str, body: ReserveRequest, version_service: VersionService = Depends(version_service)):
    try:
        result, skipped = version_service.reserve(
            dataset_id, body.metadata, body.force)
        if skipped:
            msg = result.pop("_skip_msg")
            return warn(result, [msg])
        return ok(result)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():  return ko(msg, 404)
        if "already exists" in msg:     return ko(msg, 409)
        return ko(msg, 500)
    except Exception as e:
        return ko(str(e), 500)


@version_router.post("/{dataset_id:path}/_commit/{version}",
          summary="Commit a reserved version (phase 2 of 2-phase write)")
async def dataset_commit(dataset_id: str, version: str, body: CommitRequest, version_service: VersionService = Depends(version_service)):
    try:
        inputs = [_model_dump(i) for i in body.inputs] if body.inputs else None
        data, warnings = version_service.commit(
            dataset_id, version,
            metadata=body.metadata,
            task_id=body.task_id,
            job_id=body.job_id,
            inputs=inputs,
        )
        return warn(data, warnings) if warnings else ok(data)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():  return ko(msg, 404)
        if "status is" in msg:          return ko(msg, 409)
        return ko(msg, 422)
    except RuntimeError as e:
        return ko(str(e), 500)


@version_router.post("/{dataset_id:path}/_fail/{version}",
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str, version_service: VersionService = Depends(version_service)):
    try:
        return ok(version_service.fail(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@version_router.get("/{dataset_id:path}/_preview/{version}",
         summary="Preview rows of Dataset Version")
async def preview(dataset_id: str, version: str, limit: int = 10, offset: int = 0, version_service: VersionService = Depends(version_service)):
    try:
        return ok(version_service.preview(dataset_id, version, limit, offset))
    except NotImplementedError as e:
        return ko(str(e), 422)
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(f"Read error: {e}", 500)
  

@version_router.delete("/{dataset_id:path}/_deprecate/{version}",
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str, version_service: VersionService = Depends(version_service)):
    try:
        return ok(version_service.deprecate(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@version_router.post("/{dataset_id:path}/_register-virtual",
          summary="Register a virtual dataset version (no local file)",
          status_code=201)
async def register_virtual(dataset_id: str, body: VirtualRegisterRequest, version_service: VersionService = Depends(version_service)):
    try:
        return ok(version_service.register_virtual(
            dataset_id, body.source_id, body.location, body.format,
            display_name=body.display_name, description=body.description,
            owner=body.owner, tags=body.tags,
            task_id=body.task_id, job_id=body.job_id,
        ))
    except ValueError as e:
        return ko(str(e), 422)
    except Exception as e:
        return ko(str(e), 500)

@version_router.post("/_scan",
          summary="Scan a filesystem path and register all dataset files found")
async def scan_api(body: ScanRequest, version_service: VersionService = Depends(version_service)):
    data_path = body.data_path or args.data_path
    if not os.path.exists(data_path):
        return ko(f"Path not found: {data_path}", 404)
    count = version_service.scan(data_path, body.prefix)
    return ok({"scanned": count, "data_path": data_path})

