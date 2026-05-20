from fastapi import APIRouter, Depends

from waluigi.commons.utils import _model_dump
from waluigi.catalog.api.schemas import SchemaPublishRequest, SchemaColumnPatch
from waluigi.commons.responses import ok, warn, ko
from waluigi.catalog.services.schema_service import SchemaService
from waluigi.catalog.config.dependencies import schema_service

schema_router = APIRouter(
    prefix="/datasets",
    tags=["Schema"]
)


@schema_router.get("/{dataset_id:path}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(dataset_id: str, schema_service: SchemaService = Depends(schema_service)):
    try:
        data, msgs = schema_service.get_schema(dataset_id)
        return warn(data, msgs) if msgs else ok(data)
    except ValueError as e:
        return ko(str(e), 404)


@schema_router.patch("/{dataset_id:path}/schema/{column_name}",
           summary="Edit a column's semantic metadata and PII flags")
async def patch_schema_column(dataset_id: str, column_name: str, body: SchemaColumnPatch, schema_service: SchemaService = Depends(schema_service)):
    try:
        col, msgs = schema_service.patch_column(
            dataset_id, column_name, **_model_dump(body))
        return warn(col, msgs) if msgs else ok(col)
    except ValueError as e:
        return ko(str(e), 404)


@schema_router.post("/{dataset_id:path}/schema/{column_name}/approve",
          summary="Approve a single column — promotes it to 'published'")
async def approve_schema_column(dataset_id: str, column_name: str, schema_service: SchemaService = Depends(schema_service)):
    try:
        return ok(schema_service.approve_column(dataset_id, column_name))
    except ValueError as e:
        return ko(str(e), 404)


@schema_router.delete("/{dataset_id:path}/schema/{column_name}",
            summary="Delete a column from the schema definition")
async def delete_schema_column(dataset_id: str, column_name: str, schema_service: SchemaService = Depends(schema_service)):
    try:
        return ok(schema_service.delete_column(dataset_id, column_name))
    except ValueError as e:
        return ko(str(e), 404)


@schema_router.post("/{dataset_id:path}/schema/publish",
          summary="Publish schema — promotes all columns to 'published'")
async def publish_schema(dataset_id: str, body: SchemaPublishRequest, schema_service: SchemaService = Depends(schema_service)):
    try:
        return ok(schema_service.publish_schema(dataset_id, body.published_by))
    except ValueError as e:
        return ko(str(e), 404)

