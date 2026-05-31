from fastapi import APIRouter, Depends
from waluigi.commons.responses import ok, ko
from waluigi.catalog.api.schemas import MaterializeRequest
from waluigi.catalog.services.materialize_service import MaterializeService
from waluigi.catalog.config.dependencies import materialize_service

materialize_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
    tags=["Materialize"],
)


@materialize_router.post("/{dataset_id:path}/_materialize",
    summary="Fetch a REST API and store result as a local CSV version",
    status_code=201)
async def materialize(namespace: str, dataset_id: str, body: MaterializeRequest,
                      svc: MaterializeService = Depends(materialize_service)):
    try:
        return ok(await svc.materialize(
            namespace, dataset_id, body.source_id,
            body.base_url, body.endpoint, body.params,
            display_name=body.display_name, description=body.description,
            task_id=body.task_id, job_id=body.job_id,
        ))
    except ValueError as e:
        return ko(str(e), 422)
    except Exception as e:
        return ko(str(e), 500)
