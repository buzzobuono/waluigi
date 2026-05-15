from fastapi import APIRouter, Depends, Query
from waluigi.core.responses import ok, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.api.schemas import ExpectationCreateRequest, ExpectationUpdateRequest
from waluigi.catalog.services.dq_service import DQService
from waluigi.catalog.api.dependencies import dq_service

dq_router = APIRouter(
    prefix=""
)

@dq_router.get("/datasets/{dataset_id:path}/expectations", tags=["DQ Expectations"],
         summary="List all DQ expectations for a dataset")
async def list_expectations(dataset_id: str, dq_service: DQService = Depends(dq_service)):
    try:
        return ok([e.to_dict() for e in dq_service.list_expectations(dataset_id)])
    except ValueError as e:
        return ko(str(e), 404)


@dq_router.post("/datasets/{dataset_id:path}/expectations", tags=["DQ Expectations"],
          summary="Add a DQ expectation to a dataset")
async def add_expectation(dataset_id: str, body: ExpectationCreateRequest, dq_service: DQService = Depends(dq_service)):
    try:
        return ok(dq_service.add_expectation(
            dataset_id, body.rule_id, body.inputs,
            body.params, body.tolerance, body.position).to_dict())
    except ValueError as e:
        return ko(str(e), 404)


@dq_router.patch("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["DQ Expectations"],
           summary="Update a DQ expectation")
async def update_expectation(dataset_id: str, exp_id: int, body: ExpectationUpdateRequest, dq_service: DQService = Depends(dq_service)):
    try:
        updates = {k: v for k, v in _model_dump(body).items() if v is not None}
        return ok(dq_service.update_expectation(dataset_id, exp_id, **updates).to_dict())
    except ValueError as e:
        return ko(str(e), 404)


@dq_router.delete("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["DQ Expectations"],
            summary="Delete a DQ expectation")
async def delete_expectation(dataset_id: str, exp_id: int, dq_service: DQService = Depends(dq_service)):
    try:
        return ok(dq_service.delete_expectation(dataset_id, exp_id))
    except ValueError as e:
        return ko(str(e), 404)
    
@dq_router.get("/datasets/{dataset_id:path}/dq", tags=["DQ Results"],
         summary="List all DQ run results for a dataset (one per version)")
async def list_dq_results(dataset_id: str, dq_service: DQService = Depends(dq_service)):
    try:
        return ok(dq_service.list_results(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@dq_router.get("/datasets/{dataset_id:path}/dq/{version}", tags=["DQ Results"],
         summary="Get the DQ result for a specific version")
async def get_dq_result(dataset_id: str, version: str, dq_service: DQService = Depends(dq_service)):
    try:
        return ok(dq_service.get_result(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)

@dq_router.get("/dq/rules", tags=["DQ Catalog"],
         summary="List all DQ rules available in the catalogue")
async def list_dq_rules(dq_service: DQService = Depends(dq_service)):
    return ok(dq_service.list_rules())


@dq_router.get("/dq/suite", tags=["DQ Catalog"],
         summary="Read a suite YAML and return its rules enriched with catalogue definitions")
async def get_dq_suite(path: str = Query(..., description="Absolute path to the suite YAML file"), dq_service: DQService = Depends(dq_service)):
    try:
        return ok(dq_service.get_suite(path))
    except ValueError as e:
        status = 404 if "not found" in str(e) else 422
        return ko(str(e), status)
