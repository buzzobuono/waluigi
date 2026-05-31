from fastapi import APIRouter, Depends, Query
from waluigi.commons.responses import ok, warn, ko
from waluigi.commons.utils import _model_dump
from waluigi.catalog.api.schemas import (
    DatasetCreateRequest, DatasetUpdateRequest, DatasetStatus, ApproveRequest,
)
from waluigi.catalog.services.dataset_service import DatasetService
from waluigi.catalog.config.dependencies import dataset_service

dataset_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
)


@dataset_router.get("", tags=["Datasets"],
    summary="Find datasets",
    description="status: draft | in_review | approved | deprecated")
async def find_datasets(
    namespace: str,
    status: DatasetStatus | None = Query(default=None),
    description: str | None = Query(default=None),
    svc: DatasetService = Depends(dataset_service),
):
    return ok(svc.find(namespace, status, description))


@dataset_router.post("", tags=["Datasets"],
    summary="Register a new dataset", status_code=201)
async def create_dataset(namespace: str, body: DatasetCreateRequest,
                         svc: DatasetService = Depends(dataset_service)):
    try:
        return ok(svc.create(
            namespace, body.id, body.format.value, body.description,
            body.source_id, body.dq_suite))
    except ValueError as e:
        msg = str(e)
        if "Source not found" in msg:  return ko(msg, 404)
        if "'id' not valid" in msg:    return ko(msg, 400)
        return ko(msg, 409)


@dataset_router.get("/{id:path}", tags=["Datasets"],
    summary="Get a dataset details")
async def get_dataset(namespace: str, id: str,
                      svc: DatasetService = Depends(dataset_service)):
    try:
        dataset, msgs = svc.get(namespace, id)
        return warn(dataset, msgs) if msgs else ok(dataset)
    except ValueError as e:
        return ko(str(e), 404)


@dataset_router.patch("/{id:path}", tags=["Datasets"],
    summary="Update a dataset")
async def update_dataset(namespace: str, id: str, body: DatasetUpdateRequest,
                         svc: DatasetService = Depends(dataset_service)):
    dataset = svc.update(namespace, id, **_model_dump(body))
    if not dataset:
        return ko("Dataset not found", 404)
    return ok(dataset)


@dataset_router.delete("/{id:path}", tags=["Datasets"],
    summary="Delete a dataset")
async def delete_dataset(namespace: str, id: str,
                         svc: DatasetService = Depends(dataset_service)):
    if not svc.delete(namespace, id):
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})


@dataset_router.post("/{dataset_id:path}/_approve",
    tags=["Datasets Status"],
    summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(namespace: str, dataset_id: str, body: ApproveRequest,
                          svc: DatasetService = Depends(dataset_service)):
    try:
        data, msgs = svc.approve(namespace, dataset_id, body.approved_by, body.notes)
        if data["breaking_changes"]:
            return warn(data, ["Breaking schema changes on approval"] + msgs)
        return warn(data, msgs) if msgs else ok(data)
    except ValueError as e:
        status = 409 if "deprecated" in str(e) else 404
        return ko(str(e), status)
    except RuntimeError as e:
        return ko(str(e), 500)
