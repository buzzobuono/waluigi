import os
import sys
import socket
import yaml
import configargparse
import httpx
import uvicorn
import logging
from fastapi import FastAPI, Query

from waluigi.core.responses import ok, warn, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.db import CatalogDB
from waluigi.catalog.models import *
from waluigi.catalog.services import (
    ChartService, DQService,
    DatasetService, VersionService,
    SourceService,
    CatalogBrowserService, MetadataService,
)
from waluigi.sdk.dataquality import DQManager

logger = logging.getLogger("waluigi")

app = FastAPI(
    title="Waluigi Catalog",
    description="Data Catalog service: manages source, datasets, versions, schema, lineage and metadata.",
    version="2.0.0",
)

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_CATALOG_")
p.add("--port",         type=int, default=9000)
p.add("--host",         default=socket.gethostname())
p.add("--bind-address", default="0.0.0.0")
p.add("--db-path",      default=os.path.join(os.getcwd(), "db/catalog.db"))
p.add("--data-path",    default=os.path.join(os.getcwd(), "data"))
p.add("--scan",         action="store_true", default=False)
p.add("--scan-path",    default=None)
p.add("--scan-prefix",  default=None,
      help="Dataset id prefix to assign scanned files")
p.add("--rules-path",   default=os.path.join(os.getcwd(), "rules"),
      help="Directory containing DQ rule YAML definitions")
args = p.parse_args()

DATA_PATH  = args.data_path
RULES_PATH = args.rules_path
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
os.makedirs(RULES_PATH, exist_ok=True)

try:
    db = CatalogDB(args.db_path)
    logger.info(f"Database ready: {args.db_path}")
except Exception as e:
    logger.error(f"❌ Critical DB error: {e}")
    sys.exit(1)

dq_manager       = DQManager(RULES_PATH)
chart_service    = ChartService(db)
dq_service       = DQService(db, dq_manager)
dataset_service  = DatasetService(db)
version_service  = VersionService(db, DATA_PATH, dq_service)
source_service   = SourceService(db)
browser_service  = CatalogBrowserService(db)
metadata_service = MetadataService(db)


# ---------------------------------------------------------------------------
# Routes — Browse
# ---------------------------------------------------------------------------

@app.get("/folders/{prefix:path}/", tags=["Browse"],
         summary="List datasets and virtual sub-prefixes under a prefix",
         description=(
             "Trailing slash distinguishes browse from dataset access. "
             "Returns direct child datasets and deeper virtual prefixes, "
             "exactly like S3 ListObjects with a delimiter."
         ))
async def list_folders(prefix: str):
    return ok(browser_service.list_folders(prefix))


# ---------------------------------------------------------------------------
# Routes — Sources
# ---------------------------------------------------------------------------

@app.get("/sources", tags=["Sources"],
         summary="List sources")
async def list_sources():
    return ok(source_service.list())


@app.post("/sources", tags=["Sources"],
          summary="Register or update a source (upsert)",
          status_code=200)
async def create_source(body: SourceCreateRequest):
    try:
        return ok(source_service.upsert(body.id, body.type.value, body.config, body.description))
    except ValueError as e:
        return ko(str(e), 409)


@app.get("/sources/{id}", tags=["Sources"],
         summary="Get a source details")
async def get_source(id: str):
    src = source_service.get(id)
    if not src:
        return ko("Source not found", 404)
    return ok(src)


@app.patch("/sources/{id}", tags=["Sources"],
           summary="Update a source")
async def update_source(id: str, body: SourceUpdateRequest):
    src = source_service.update(id, **_model_dump(body))
    if not src:
        return ko("Source not found", 404)
    return ok(src)


@app.delete("/sources/{id}", tags=["Sources"],
            summary="Delete a source")
async def delete_source(id: str):
    try:
        deleted = source_service.delete(id)
    except ValueError as e:
        return ko(str(e), 409)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": id})


# ---------------------------------------------------------------------------
# Routes — Version Metadata
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/versions/{version}/metadata",
         tags=["Metadata"],
         summary="Get all metadata for a version")
async def get_metadata(dataset_id: str, version: str):
    try:
        return ok(metadata_service.get_version_metadata(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@app.post("/datasets/{dataset_id:path}/versions/{version}/metadata",
          tags=["Metadata"],
          summary="Set a metadata key on a version")
async def set_metadata(dataset_id: str, version: str, body: MetadataSetRequest):
    try:
        return ok(metadata_service.set_version_metadata(
            dataset_id, version, body.key, body.value))
    except ValueError as e:
        status = 422 if "reserved" in str(e) else 404
        return ko(str(e), status)


@app.delete("/datasets/{dataset_id:path}/versions/{version}/metadata/{key}",
            tags=["Metadata"],
            summary="Delete a metadata key from a version")
async def delete_metadata(dataset_id: str, version: str, key: str):
    try:
        return ok(metadata_service.delete_version_metadata(dataset_id, version, key))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Versions
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/_preview/{version}",
         tags=["Versions"],
         summary="Preview rows of Dataset Version")
async def preview(dataset_id: str, version: str,
                  limit: int = 10, offset: int = 0):
    try:
        return ok(version_service.preview(dataset_id, version, limit, offset))
    except NotImplementedError as e:
        return ko(str(e), 422)
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(f"Read error: {e}", 500)


@app.get("/datasets/{dataset_id:path}/versions", tags=["Versions"],
         summary="List all committed versions (newest first)")
async def list_versions(dataset_id: str):
    try:
        return ok(version_service.list_versions(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Schema
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(dataset_id: str):
    try:
        data, msgs = metadata_service.get_schema(dataset_id)
        return warn(data, msgs) if msgs else ok(data)
    except ValueError as e:
        return ko(str(e), 404)


@app.patch("/datasets/{dataset_id:path}/schema/{column_name}",
           tags=["Schema"],
           summary="Edit a column's semantic metadata and PII flags")
async def patch_schema_column(dataset_id: str, column_name: str,
                               body: SchemaColumnPatch):
    try:
        col, msgs = metadata_service.patch_column(
            dataset_id, column_name, **_model_dump(body))
        return warn(col, msgs) if msgs else ok(col)
    except ValueError as e:
        return ko(str(e), 404)


@app.post("/datasets/{dataset_id:path}/schema/{column_name}/approve",
          tags=["Schema"],
          summary="Approve a single column — promotes it to 'published'")
async def approve_schema_column(dataset_id: str, column_name: str):
    try:
        return ok(metadata_service.approve_column(dataset_id, column_name))
    except ValueError as e:
        return ko(str(e), 404)


@app.delete("/datasets/{dataset_id:path}/schema/{column_name}",
            tags=["Schema"],
            summary="Delete a column from the schema definition")
async def delete_schema_column(dataset_id: str, column_name: str):
    try:
        return ok(metadata_service.delete_column(dataset_id, column_name))
    except ValueError as e:
        return ko(str(e), 404)


@app.post("/datasets/{dataset_id:path}/schema/publish",
          tags=["Schema"],
          summary="Publish schema — promotes all columns to 'published'")
async def publish_schema(dataset_id: str, body: SchemaPublishRequest):
    try:
        return ok(metadata_service.publish_schema(dataset_id, body.published_by))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Expectations
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/expectations", tags=["Expectations"],
         summary="List all DQ expectations for a dataset")
async def list_expectations(dataset_id: str):
    try:
        return ok(metadata_service.list_expectations(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@app.post("/datasets/{dataset_id:path}/expectations", tags=["Expectations"],
          summary="Add a DQ expectation to a dataset")
async def add_expectation(dataset_id: str, body: ExpectationCreateRequest):
    try:
        return ok(metadata_service.add_expectation(
            dataset_id, body.rule_id, body.inputs,
            body.params, body.tolerance, body.position))
    except ValueError as e:
        return ko(str(e), 404)


@app.patch("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["Expectations"],
           summary="Update a DQ expectation")
async def update_expectation(dataset_id: str, exp_id: int, body: ExpectationUpdateRequest):
    try:
        updates = {k: v for k, v in _model_dump(body).items() if v is not None}
        return ok(metadata_service.update_expectation(dataset_id, exp_id, **updates))
    except ValueError as e:
        return ko(str(e), 404)


@app.delete("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["Expectations"],
            summary="Delete a DQ expectation")
async def delete_expectation(dataset_id: str, exp_id: int):
    try:
        return ok(metadata_service.delete_expectation(dataset_id, exp_id))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Charts
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/charts", tags=["Charts"],
         summary="List chart definitions for a dataset")
async def list_charts(dataset_id: str):
    try:
        return ok(metadata_service.list_charts(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@app.post("/datasets/{dataset_id:path}/charts", tags=["Charts"],
          summary="Add a chart definition")
async def add_chart(dataset_id: str, body: ChartCreateRequest):
    try:
        return ok(metadata_service.add_chart(
            dataset_id, body.key, body.title, body.spec, body.position))
    except ValueError as e:
        return ko(str(e), 404)


@app.patch("/datasets/{dataset_id:path}/charts/{chart_id}", tags=["Charts"],
           summary="Update a chart definition")
async def update_chart(dataset_id: str, chart_id: int, body: ChartUpdateRequest):
    try:
        updates = {k: v for k, v in _model_dump(body).items() if v is not None}
        return ok(metadata_service.update_chart(dataset_id, chart_id, **updates))
    except ValueError as e:
        return ko(str(e), 404)


@app.delete("/datasets/{dataset_id:path}/charts/{chart_id}", tags=["Charts"],
            summary="Delete a chart definition")
async def delete_chart(dataset_id: str, chart_id: int):
    try:
        return ok(metadata_service.delete_chart(dataset_id, chart_id))
    except ValueError as e:
        return ko(str(e), 404)


@app.get("/datasets/{dataset_id:path}/charts/{chart_id}/render", tags=["Charts"],
         summary="Render a chart by ID — returns an ECharts option object")
async def render_chart(dataset_id: str, chart_id: int,
                       version: str = Query(None)):
    chart = metadata_service.get_chart(dataset_id, chart_id)
    if not chart:
        return ko("Chart not found", 404)
    try:
        return ok(chart_service.render(chart, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(str(e), 500)


@app.get("/datasets/{dataset_id:path}/charts/_render", tags=["Charts"],
         summary="Render a chart by key — returns an ECharts option object")
async def render_chart_by_key(dataset_id: str,
                               key:     str = Query(...),
                               version: str = Query(None)):
    chart = metadata_service.get_chart_by_key(dataset_id, key)
    if not chart:
        return ko("Chart not found", 404)
    try:
        return ok(chart_service.render(chart, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(str(e), 500)


# ---------------------------------------------------------------------------
# Routes — DQ Results
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/dq", tags=["DQ Results"],
         summary="List all DQ run results for a dataset (one per version)")
async def list_dq_results(dataset_id: str):
    try:
        return ok(browser_service.list_dq_results(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@app.get("/datasets/{dataset_id:path}/dq/{version}", tags=["DQ Results"],
         summary="Get the DQ result for a specific version")
async def get_dq_result(dataset_id: str, version: str):
    try:
        return ok(browser_service.get_dq_result(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Lineage
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/lineage/{version}", tags=["Lineage"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str, version: str):
    try:
        return ok(browser_service.get_lineage(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Data Quality rules catalogue
# ---------------------------------------------------------------------------

@app.get("/dq/rules", tags=["Data Quality"],
         summary="List all DQ rules available in the catalogue")
async def list_dq_rules():
    return ok(dq_service.list_rules())


@app.get("/dq/suite", tags=["Data Quality"],
         summary="Read a suite YAML and return its rules enriched with catalogue definitions")
async def get_dq_suite(path: str = Query(..., description="Absolute path to the suite YAML file")):
    try:
        return ok(dq_service.get_suite(path))
    except ValueError as e:
        status = 404 if "not found" in str(e) else 422
        return ko(str(e), status)


# ---------------------------------------------------------------------------
# Routes — Datasets
# ---------------------------------------------------------------------------

@app.get("/datasets", tags=["Datasets"],
    summary="Find datasets",
    description="status: draft | in_review | approved | deprecated"
)
async def find_datasets(status: DatasetStatus | None = Query(default=None, example=DatasetStatus.DRAFT),
                        description: str | None = Query(default=None, example="sales dataset")):
    return ok(dataset_service.find(status, description))


@app.post("/datasets", tags=["Datasets"],
          summary="Register a new dataset",
          status_code=201)
async def create_dataset(body: DatasetCreateRequest):
    try:
        return ok(dataset_service.create(
            body.id, body.format.value, body.description, body.source_id, body.dq_suite))
    except ValueError as e:
        msg = str(e)
        if "Source not found" in msg:      return ko(msg, 404)
        if "'id' not valid" in msg:        return ko(msg, 400)
        return ko(msg, 409)


@app.get("/datasets/{id:path}", tags=["Datasets"],
         summary="Get a dataset details")
async def get_dataset(id: str):
    try:
        dataset, msgs = dataset_service.get(id)
        return warn(dataset, msgs) if msgs else ok(dataset)
    except ValueError as e:
        return ko(str(e), 404)


@app.patch("/datasets/{id:path}", tags=["Datasets"],
           summary="Update a dataset")
async def update_dataset(id: str, body: DatasetUpdateRequest):
    dataset = dataset_service.update(id, **_model_dump(body))
    if not dataset:
        return ko("Dataset not found", 404)
    return ok(dataset)


@app.delete("/datasets/{id:path}", tags=["Datasets"],
            summary="Delete a dataset")
async def delete_dataset(id: str):
    if not dataset_service.delete(id):
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})


# ---------------------------------------------------------------------------
# Routes — Dataset Status
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/approve",
          tags=["Datasets Status"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest):
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


# ---------------------------------------------------------------------------
# Routes — Dataset Produce (2-phase write)
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/_reserve", tags=["Dataset Produce"],
          summary="Reserve a new version (phase 1 of 2-phase write)",
          status_code=201)
async def dataset_reserve(dataset_id: str, body: ReserveRequest):
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


@app.post("/datasets/{dataset_id:path}/_commit/{version}",
          tags=["Dataset Produce"],
          summary="Commit a reserved version (phase 2 of 2-phase write)")
async def dataset_commit(dataset_id: str, version: str, body: CommitRequest):
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


@app.post("/datasets/{dataset_id:path}/_fail/{version}",
          tags=["Dataset Produce"],
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str):
    try:
        return ok(version_service.fail(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Versions lifecycle
# ---------------------------------------------------------------------------

@app.delete("/datasets/{dataset_id:path}/deprecate/{version}",
            tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str):
    try:
        return ok(version_service.deprecate(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


# ---------------------------------------------------------------------------
# Routes — Virtual datasets
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/register-virtual",
          tags=["Virtual"],
          summary="Register a virtual dataset version (no local file)",
          status_code=201)
async def register_virtual(dataset_id: str, body: VirtualRegisterRequest):
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


# ---------------------------------------------------------------------------
# Routes — Materialize (REST API → local CSV)
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/materialize",
          tags=["Materialize"],
          summary="Fetch a REST API and store result as a local CSV version",
          status_code=201)
async def materialize(dataset_id: str, body: MaterializeRequest):
    try:
        return ok(await version_service.materialize(
            dataset_id, body.base_url, body.endpoint, body.params,
            display_name=body.display_name, description=body.description,
            task_id=body.task_id, job_id=body.job_id,
        ))
    except httpx.HTTPError as e:
        return ko(f"HTTP error: {e}", 502)
    except ValueError as e:
        return ko(str(e), 422)
    except Exception as e:
        return ko(str(e), 500)


# ---------------------------------------------------------------------------
# Routes — Scan
# ---------------------------------------------------------------------------

@app.post("/scan", tags=["Scan"],
          summary="Scan a filesystem path and register all dataset files found")
async def scan_api(body: ScanRequest):
    data_path = body.data_path or DATA_PATH
    if not os.path.exists(data_path):
        return ko(f"Path not found: {data_path}", 404)
    count = version_service.scan(data_path, body.prefix)
    return ok({"scanned": count, "data_path": data_path})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    with open("logging.yaml") as f:
        logging.config.dictConfig(yaml.safe_load(f))

    if args.scan:
        version_service.scan(args.scan_path or DATA_PATH, args.scan_prefix)
        return

    logger.info("Waluigi Catalog v2")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_path}")
    logger.info(f"  Data    : {args.data_path}")

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
