from fastapi import APIRouter, Depends
import os

from waluigi.catalog.config.args import args
from waluigi.commons.utils import _model_dump
from waluigi.commons.responses import ok, warn, ko
from waluigi.catalog.api.schemas import (
    ReserveRequest, CommitRequest, VirtualRegisterRequest, ScanRequest,
)
from waluigi.catalog.services.version_service import VersionService
from waluigi.catalog.config.dependencies import version_service

version_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
    tags=["Versions"],
)


@version_router.get("/{dataset_id:path}/versions",
    summary="List all committed versions (newest first)")
async def list_versions(namespace: str, dataset_id: str,
                        svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.list_versions(namespace, dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@version_router.post("/{dataset_id:path}/_reserve",
    summary="Reserve a new version (phase 1 of 2-phase write)", status_code=201)
async def dataset_reserve(namespace: str, dataset_id: str,
                          body: ReserveRequest,
                          svc: VersionService = Depends(version_service)):
    try:
        result, skipped = svc.reserve(namespace, dataset_id, body.metadata, body.force)
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
async def dataset_commit(namespace: str, dataset_id: str, version: str,
                         body: CommitRequest,
                         svc: VersionService = Depends(version_service)):
    try:
        inputs = [_model_dump(i) for i in body.inputs] if body.inputs else None
        data, warnings = svc.commit(
            namespace, dataset_id, version,
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
async def fail_version(namespace: str, dataset_id: str, version: str,
                       svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.fail(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@version_router.get("/{dataset_id:path}/_preview/{version}",
    summary="Preview rows of Dataset Version")
async def preview(namespace: str, dataset_id: str, version: str,
                  limit: int = 10, offset: int = 0,
                  svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.preview(namespace, dataset_id, version, limit, offset))
    except NotImplementedError as e:
        return ko(str(e), 422)
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(f"Read error: {e}", 500)


@version_router.delete("/{dataset_id:path}/versions/{version}",
    summary="Hard-delete a dataset version (removes file and all related records)")
async def delete_version(namespace: str, dataset_id: str, version: str,
                         svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.delete_version(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(str(e), 500)


@version_router.delete("/{dataset_id:path}/_deprecate/{version}",
    summary="Deprecate a dataset version")
async def deprecate(namespace: str, dataset_id: str, version: str,
                    svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.deprecate(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@version_router.post("/{dataset_id:path}/_register-virtual",
    summary="Register a virtual dataset version (no local file)", status_code=201)
async def register_virtual(namespace: str, dataset_id: str,
                           body: VirtualRegisterRequest,
                           svc: VersionService = Depends(version_service)):
    try:
        return ok(svc.register_virtual(
            namespace, dataset_id, body.source_id, body.location, body.format,
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
async def scan_api(namespace: str, body: ScanRequest,
                   svc: VersionService = Depends(version_service)):
    data_path = body.data_path or args.data_path
    if not os.path.exists(data_path):
        return ko(f"Path not found: {data_path}", 404)
    count = svc.scan(namespace, data_path, source_id=body.source_id,
                     prefix=body.prefix)
    return ok({"scanned": count, "data_path": data_path})
