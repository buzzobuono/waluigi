from fastapi import APIRouter, Depends, Query
from waluigi.commons.responses import ok, ko
from waluigi.commons.utils import _model_dump
from waluigi.catalog.api.schemas import ExpectationCreateRequest, ExpectationUpdateRequest
from waluigi.catalog.services.dq_service import DQService
from waluigi.catalog.config.dependencies import dq_service

# Dataset-scoped DQ routes
dq_dataset_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
)

@dq_dataset_router.get("/{dataset_id:path}/expectations", tags=["DQ Expectations"],
    summary="List all DQ expectations for a dataset")
async def list_expectations(namespace: str, dataset_id: str,
                            svc: DQService = Depends(dq_service)):
    try:
        return ok(svc.list_expectations(namespace, dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@dq_dataset_router.post("/{dataset_id:path}/expectations", tags=["DQ Expectations"],
    summary="Add a DQ expectation to a dataset")
async def add_expectation(namespace: str, dataset_id: str,
                          body: ExpectationCreateRequest,
                          svc: DQService = Depends(dq_service)):
    try:
        return ok(svc.add_expectation(
            namespace, dataset_id, body.rule_id, body.inputs,
            body.params, body.tolerance, body.position))
    except ValueError as e:
        return ko(str(e), 404)


@dq_dataset_router.patch("/{dataset_id:path}/expectations/{exp_id}",
    tags=["DQ Expectations"], summary="Update a DQ expectation")
async def update_expectation(namespace: str, dataset_id: str, exp_id: int,
                             body: ExpectationUpdateRequest,
                             svc: DQService = Depends(dq_service)):
    try:
        updates = {k: v for k, v in _model_dump(body).items() if v is not None}
        return ok(svc.update_expectation(namespace, dataset_id, exp_id, **updates))
    except ValueError as e:
        return ko(str(e), 404)


@dq_dataset_router.delete("/{dataset_id:path}/expectations/{exp_id}",
    tags=["DQ Expectations"], summary="Delete a DQ expectation")
async def delete_expectation(namespace: str, dataset_id: str, exp_id: int,
                             svc: DQService = Depends(dq_service)):
    try:
        return ok(svc.delete_expectation(namespace, dataset_id, exp_id))
    except ValueError as e:
        return ko(str(e), 404)


@dq_dataset_router.get("/{dataset_id:path}/dq", tags=["DQ Results"],
    summary="List all DQ run results for a dataset (one per version)")
async def list_dq_results(namespace: str, dataset_id: str,
                          svc: DQService = Depends(dq_service)):
    try:
        return ok(svc.list_results(namespace, dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@dq_dataset_router.get("/{dataset_id:path}/dq/{version}", tags=["DQ Results"],
    summary="Get the DQ result for a specific version")
async def get_dq_result(namespace: str, dataset_id: str, version: str,
                        svc: DQService = Depends(dq_service)):
    try:
        return ok(svc.get_result(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


# Global DQ routes (no namespace)
dq_global_router = APIRouter(prefix="")

@dq_global_router.get("/dq/rules", tags=["DQ Catalog"],
    summary="List all DQ rules available in the catalogue")
async def list_dq_rules(svc: DQService = Depends(dq_service)):
    return ok(svc.list_rules())


@dq_global_router.get("/dq/suite", tags=["DQ Catalog"],
    summary="Read a suite YAML and return its rules enriched with catalogue definitions")
async def get_dq_suite(
    path: str = Query(..., description="Absolute path to the suite YAML file"),
    svc: DQService = Depends(dq_service),
):
    try:
        return ok(svc.get_suite(path))
    except ValueError as e:
        status = 404 if "not found" in str(e) else 422
        return ko(str(e), status)
