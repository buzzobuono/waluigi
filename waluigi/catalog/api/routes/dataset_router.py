from fastapi import APIRouter, Depends, Query
from waluigi.core.responses import ok, warn, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.api.schemas import DatasetCreateRequest, DatasetUpdateRequest, DatasetStatus, ApproveRequest
from waluigi.catalog.services.dataset_service import DatasetService
from waluigi.catalog.api.dependencies import dataset_service

dataset_router = APIRouter(
    prefix="/datasets"
)

@dataset_router.get("/", tags=["Datasets"],
    summary="Find datasets",
    description="status: draft | in_review | approved | deprecated"
)
async def find_datasets(status: DatasetStatus | None = Query(default=None, example=DatasetStatus.DRAFT),
                        description: str | None = Query(default=None, example="sales dataset"), dataset_service: DatasetService = Depends(dataset_service)):
    return ok([d.to_dict() for d in dataset_service.find(status, description)])


@dataset_router.post("/", tags=["Datasets"],
          summary="Register a new dataset",
          status_code=201)
async def create_dataset(body: DatasetCreateRequest, dataset_service: DatasetService = Depends(dataset_service)):
    try:
        return ok(dataset_service.create(
            body.id, body.format.value, body.description, body.source_id, body.dq_suite).to_dict())
    except ValueError as e:
        msg = str(e)
        if "Source not found" in msg:      return ko(msg, 404)
        if "'id' not valid" in msg:        return ko(msg, 400)
        return ko(msg, 409)


@dataset_router.get("/{id:path}", tags=["Datasets"],
         summary="Get a dataset details")
async def get_dataset(id: str, dataset_service: DatasetService = Depends(dataset_service)):
    try:
        dataset, msgs = dataset_service.get(id)
        d = dataset.to_dict()
        return warn(d, msgs) if msgs else ok(d)
    except ValueError as e:
        return ko(str(e), 404)


@dataset_router.patch("/{id:path}", tags=["Datasets"],
           summary="Update a dataset")
async def update_dataset(id: str, body: DatasetUpdateRequest, dataset_service: DatasetService = Depends(dataset_service)):
    dataset = dataset_service.update(id, **_model_dump(body))
    if not dataset:
        return ko("Dataset not found", 404)
    return ok(dataset.to_dict())


@dataset_router.delete("/{id:path}", tags=["Datasets"],
            summary="Delete a dataset")
async def delete_dataset(id: str, dataset_service: DatasetService = Depends(dataset_service)):
    if not dataset_service.delete(id):
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})


@dataset_router.post("/{dataset_id:path}/_approve",
          tags=["Datasets Status"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest, dataset_service: DatasetService = Depends(dataset_service)):
    try:
        data, msgs = dataset_service.approve(dataset_id, body.approved_by, body.notes)
        if data["breaking_changes"]:
            return warn(data, ["⚠️ Breaking schema changes on approval"] + msgs)
        return warn(data, msgs) if msgs else ok(data)
    except ValueError as e:
        status = 409 if "deprecated" in str(e) else 404
        return ko(str(e), status)
    except RuntimeError as e:
        return ko(str(e), 500)