import os
import sys
import json
import socket
import yaml
from datetime import datetime, timezone
import configargparse
import httpx
import pandas as pd
import uvicorn
import logging
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from waluigi.core.responses import ok, warn, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _version_id, _infer_schema, _safe_json_value
from waluigi.catalog.models import *
from waluigi.catalog.services import ChartService, DQService, DatasetService
from waluigi.sdk.connectors import ConnectorFactory
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

dq_manager      = DQManager(RULES_PATH)
chart_service   = ChartService(db)
dq_service      = DQService(db, dq_manager)
dataset_service = DatasetService(db, DATA_PATH)


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
    return ok(db.list_folders(prefix))


# ---------------------------------------------------------------------------
# Routes — Sources
# ---------------------------------------------------------------------------

@app.get("/sources", tags=["Sources"],
         summary="List sources")
async def list_sources():
    return ok(db.list_sources())


@app.post("/sources", tags=["Sources"],
          summary="Register or update a source (upsert)",
          status_code=200)
async def create_source(body: SourceCreateRequest):
    existing = db.get_source(body.id)
    if existing and existing["type"] != body.type:
        return ko(f"Cannot change source type from '{existing['type']}' to '{body.type.value}' — create a new source instead", 409)
    db.upsert_source(body.id, body.type, body.config, body.description)
    return ok(db.get_source(body.id))


@app.get("/sources/{id}", tags=["Sources"],
         summary="Get a source details")
async def get_source(id: str):
    src = db.get_source(id)
    if not src:
        return ko("Source not found", 404)
    return ok(src)


@app.patch("/sources/{id}", tags=["Sources"],
           summary="Update a source")
async def update_source(id: str, body: SourceUpdateRequest):
    updated = db.update_source(id, **_model_dump(body))
    if not updated:
        return ko("Source not found", 404)
    return ok(db.get_source(id))


@app.delete("/sources/{id}", tags=["Sources"],
            summary="Delete a source")
async def delete_source(id: str):
    try:
        deleted = db.delete_source(id)
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
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    return ok(db.get_metadata(dataset_id, version))


@app.post("/datasets/{dataset_id:path}/versions/{version}/metadata",
          tags=["Metadata"],
          summary="Set a metadata key on a version")
async def set_metadata(dataset_id: str, version: str,
                       body: MetadataSetRequest):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    if body.key.startswith("sys."):
        return ko("sys.* keys are reserved for the server", 422)
    db.set_metadata(dataset_id, version, body.key, body.value)
    return ok({"key": body.key, "value": body.value})


@app.delete("/datasets/{dataset_id:path}/versions/{version}/metadata/{key}",
            tags=["Metadata"],
            summary="Delete a metadata key from a version")
async def delete_metadata(dataset_id: str, version: str, key: str):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    if not db.delete_metadata(dataset_id, version, key):
        return ko("Key not found or protected (sys.*)", 404)
    return ok({"key": key, "deleted": True})


# ---------------------------------------------------------------------------
# Routes — Versions
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/_preview/{version}",
         tags=["Versions"],
         summary="Preview rows of Dataset Version")
async def preview(dataset_id: str, version: str,
                  limit: int = 10, offset: int = 0):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)

    fmt = (dataset.get("format") or "").lower()

    source_id = dataset.get("source_id")
    if not source_id:
        return ko("Dataset has no source", 404)

    source = db.get_source(source_id)
    if not source:
        return ko(f"Source '{source_id}' not found", 404)

    version_record = db.get_version(dataset_id, version)
    if not version_record:
        return ko("Version not found", 404)

    location    = version_record["location"]
    source_type = source.get("type", "local")

    try:
        connector = ConnectorFactory.get(source_type, source.get("config") or {})
        result    = connector.read(location, fmt, limit=limit, offset=offset)
    except NotImplementedError as e:
        return ko(str(e), 422)
    except Exception as e:
        return ko(f"Read error: {e}", 500)

    if isinstance(result, pd.DataFrame):
        df = result
    elif isinstance(result, list):
        df = pd.DataFrame(result)
    else:
        return ko(f"Preview not supported for format '{fmt}'", 422)

    clean = [{k: _safe_json_value(v) for k, v in row.items()}
             for row in df.to_dict(orient="records")]

    return ok({
        "dataset_id": dataset_id,
        "version":    version_record["version"],
        "columns":    df.columns.tolist(),
        "rows":       clean,
        "pagination": {"limit": limit, "offset": offset, "count": len(clean)},
    })


@app.get("/datasets/{dataset_id:path}/versions", tags=["Versions"],
         summary="List all committed versions (newest first)")
async def list_versions(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok(db.list_versions(dataset_id))


# ---------------------------------------------------------------------------
# Routes — Schema
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    columns   = db.get_schema(dataset_id)
    pii_count = sum(1 for c in columns if c.get("pii"))
    inferred  = [c["column_name"] for c in columns
                 if c.get("status") == "inferred"]
    msgs = []
    if pii_count:
        msgs.append(f"{pii_count} column(s) flagged as PII")
    if inferred:
        msgs.append(
            f"{len(inferred)} column(s) still 'inferred' — "
            "review before publishing")
    data = {
        "dataset_id": dataset_id,
        "columns":    columns,
        "summary": {
            "total":     len(columns),
            "pii":       pii_count,
            "inferred":  len(inferred),
            "draft":     sum(1 for c in columns if c.get("status") == "draft"),
            "published": sum(1 for c in columns if c.get("status") == "published"),
        },
    }
    return warn(data, msgs) if msgs else ok(data)


@app.patch("/datasets/{dataset_id:path}/schema/{column_name}",
           tags=["Schema"],
           summary="Edit a column's semantic metadata and PII flags")
async def patch_schema_column(dataset_id: str, column_name: str,
                               body: SchemaColumnPatch):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    updates = _model_dump(body)
    col = db.upsert_schema_column(dataset_id, column_name, **updates)
    db.set_in_review(dataset_id)
    msgs = []
    if col and col.get("pii") and col.get("pii_type") == "none":
        msgs.append("PII flag set but pii_type is 'none' — "
                    "set it to: direct | indirect | sensitive")
    return warn(col, msgs) if msgs else ok(col)


@app.post("/datasets/{dataset_id:path}/schema/{column_name}/approve",
          tags=["Schema"],
          summary="Approve a single column — promotes it to 'published'")
async def approve_schema_column(dataset_id: str, column_name: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    updated = db.approve_schema_column(dataset_id, column_name)
    if not updated:
        return ko("Column not found in schema", 404)
    col = next((c for c in db.get_schema(dataset_id)
                if c["column_name"] == column_name), None)
    return ok(col)


@app.delete("/datasets/{dataset_id:path}/schema/{column_name}",
            tags=["Schema"],
            summary="Delete a column from the schema definition")
async def delete_schema_column(dataset_id: str, column_name: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    deleted = db.delete_schema_column(dataset_id, column_name)
    if not deleted:
        return ko("Column not found in schema", 404)
    return ok({"column_name": column_name, "deleted": True})


@app.post("/datasets/{dataset_id:path}/schema/publish",
          tags=["Schema"],
          summary="Publish schema — promotes all columns to 'published'")
async def publish_schema(dataset_id: str, body: SchemaPublishRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    db.publish_schema(dataset_id, body.published_by)
    return ok({"dataset_id": dataset_id})


# ---------------------------------------------------------------------------
# Routes — Expectations
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/expectations", tags=["Expectations"],
         summary="List all DQ expectations for a dataset")
async def list_expectations(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok(db.list_expectations(dataset_id))


@app.post("/datasets/{dataset_id:path}/expectations", tags=["Expectations"],
          summary="Add a DQ expectation to a dataset")
async def add_expectation(dataset_id: str, body: ExpectationCreateRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    exp = db.add_expectation(
        dataset_id,
        body.rule_id,
        body.inputs,
        body.params,
        body.tolerance,
        body.position,
    )
    return ok(exp)


@app.patch("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["Expectations"],
           summary="Update a DQ expectation")
async def update_expectation(dataset_id: str, exp_id: int, body: ExpectationUpdateRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    updates = {k: v for k, v in _model_dump(body).items() if v is not None}
    updated = db.update_expectation(dataset_id, exp_id, **updates)
    if not updated:
        return ko("Expectation not found", 404)
    return ok(db.get_expectation(dataset_id, exp_id))


@app.delete("/datasets/{dataset_id:path}/expectations/{exp_id}", tags=["Expectations"],
            summary="Delete a DQ expectation")
async def delete_expectation(dataset_id: str, exp_id: int):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    deleted = db.delete_expectation(dataset_id, exp_id)
    if not deleted:
        return ko("Expectation not found", 404)
    return ok({"deleted": exp_id})


# ---------------------------------------------------------------------------
# Routes — Charts
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/charts", tags=["Charts"],
         summary="List chart definitions for a dataset")
async def list_charts(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok(db.list_charts(dataset_id))


@app.post("/datasets/{dataset_id:path}/charts", tags=["Charts"],
          summary="Add a chart definition")
async def add_chart(dataset_id: str, body: ChartCreateRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    chart = db.add_chart(dataset_id, body.key, body.title, body.spec, body.position)
    return ok(chart)


@app.patch("/datasets/{dataset_id:path}/charts/{chart_id}", tags=["Charts"],
           summary="Update a chart definition")
async def update_chart(dataset_id: str, chart_id: int, body: ChartUpdateRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    updates = {k: v for k, v in _model_dump(body).items() if v is not None}
    if not db.update_chart(dataset_id, chart_id, **updates):
        return ko("Chart not found", 404)
    return ok(db.get_chart(dataset_id, chart_id))


@app.delete("/datasets/{dataset_id:path}/charts/{chart_id}", tags=["Charts"],
            summary="Delete a chart definition")
async def delete_chart(dataset_id: str, chart_id: int):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    if not db.delete_chart(dataset_id, chart_id):
        return ko("Chart not found", 404)
    return ok({"deleted": chart_id})


@app.get("/datasets/{dataset_id:path}/charts/{chart_id}/render", tags=["Charts"],
         summary="Render a chart by ID — returns an ECharts option object")
async def render_chart(dataset_id: str, chart_id: int,
                       version: str = Query(None)):
    chart = db.get_chart(dataset_id, chart_id)
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
    chart = db.get_chart_by_key(dataset_id, key)
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
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok(db.list_dq_results(dataset_id))


@app.get("/datasets/{dataset_id:path}/dq/{version}", tags=["DQ Results"],
         summary="Get the DQ result for a specific version")
async def get_dq_result(dataset_id: str, version: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    row = db.get_dq_result(dataset_id, version)
    if not row:
        return ko("No DQ result for this version", 404)
    return ok(row)


# ---------------------------------------------------------------------------
# Routes — Lineage
# ---------------------------------------------------------------------------

@app.get("/datasets/{dataset_id:path}/lineage/{version}", tags=["Lineage"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str, version: str):
    record = (db.get_version(dataset_id, version) if version
              else db.get_latest_version(dataset_id))
    if not record:
        return ko("Dataset version not found", 404)

    ver = record["version"]
    return ok({
        "dataset_id": dataset_id,
        "version":    ver,
        "upstream":   db.get_upstream(dataset_id, ver),
        "downstream": db.get_downstream(dataset_id, ver),
    })


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
    if not os.path.isfile(path):
        return ko(f"Suite file not found: {path}", 404)
    try:
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or []
    except Exception as e:
        return ko(f"Cannot read suite file: {e}", 422)

    enriched = []
    for item in raw:
        rule_id = item.get("rule_id", "?")
        defn    = dq_manager.catalogue.get(rule_id)
        enriched.append({
            "rule_id":     rule_id,
            "inputs":      item.get("inputs", {}),
            "params":      item.get("params", {}),
            "tolerance":   item.get("tolerance", 1.0),
            "description": defn.description if defn else None,
            "formula":     defn.formula.strip() if defn else None,
            "found":       defn is not None,
        })
    return ok(enriched)


# ---------------------------------------------------------------------------
# Routes — Datasets
# ---------------------------------------------------------------------------

@app.get("/datasets", tags=["Datasets"],
    summary="Find datasets",
    description="status: draft | in_review | approved | deprecated"
)
async def find_datasets(status: DatasetStatus | None = Query(default=None, example=DatasetStatus.DRAFT),
                        description: str | None = Query(default=None, example="sales dataset")):
    if not status and not description:
        return ok(db.list_datasets())
    return ok(db.find_datasets(status=status, description=description))


@app.post("/datasets", tags=["Datasets"],
          summary="Register a new dataset",
          status_code=201)
async def create_dataset(body: DatasetCreateRequest):
    if body.source_id and not db.exists_source(body.source_id):
        return ko("Source not found", 404)
    if body.id.startswith("/"):
        return ko("Dataset 'id' not valid", 400)
    existing = db.get_dataset(body.id)
    if existing and existing["format"] != body.format:
        return ko(f"Cannot change format from '{existing['format']}' to '{body.format.value}' — create a new dataset instead", 409)
    db.create_dataset(body.id, body.format, body.description, body.source_id, body.dq_suite)
    return ok(db.get_dataset(body.id))


@app.get("/datasets/{id:path}", tags=["Datasets"],
         summary="Get a dataset details")
async def get_dataset(id: str):
    dataset = db.get_dataset(id)
    if not dataset:
        return ko("Dataset not found", 404)
    msgs = []
    if dataset.get("status") != "approved":
        msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
    return warn(dataset, msgs) if msgs else ok(dataset)


@app.patch("/datasets/{id:path}", tags=["Datasets"],
           summary="Update a dataset")
async def update_dataset(id: str, body: DatasetUpdateRequest):
    updated = db.update_dataset(id, **_model_dump(body))
    if not updated:
        return ko("Dataset not found", 404)
    return ok(db.get_dataset(id))


@app.delete("/datasets/{id:path}", tags=["Datasets"],
            summary="Delete a dataset")
async def delete_dataset(id: str):
    deleted = db.delete_dataset(id)
    if not deleted:
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})


# ---------------------------------------------------------------------------
# Routes — Dataset Status
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/approve",
          tags=["Datasets Status"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    if dataset.get("status") == "deprecated":
        return ko("Cannot approve a deprecated dataset", 409)

    schema_result = db.publish_schema(dataset_id, publisher=body.approved_by)
    approved      = db.approve_dataset(dataset_id, body.approved_by)
    if not approved:
        return ko("Approval failed", 500)

    msgs = schema_result["breaking_changes"] + schema_result["warnings"]
    data = {
        "dataset_id":           dataset_id,
        "status":               "approved",
        "approved_by":          body.approved_by,
        "notes":                body.notes,
        "schema_published_at":  schema_result["published_at"],
        "breaking_changes":     schema_result["breaking_changes"],
        "warnings":             schema_result["warnings"],
    }
    logger.info(f"Approved {dataset_id} by {body.approved_by}")
    if schema_result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking schema changes on approval"] + msgs)
    return warn(data, msgs) if msgs else ok(data)


# ---------------------------------------------------------------------------
# Routes — Dataset Produce (2-phase write)
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/_reserve", tags=["Dataset Produce"],
          summary="Reserve a new version (phase 1 of 2-phase write)",
          status_code=201)
async def dataset_reserve(dataset_id: str, body: ReserveRequest):
    try:
        dataset = db.get_dataset(dataset_id)
        if not dataset:
            return ko("Dataset not found", 404)
        source = db.get_source(dataset["source_id"])

        if not body.force and body.metadata:
            existing = db.find_version_by_metadata(dataset_id, body.metadata)
            if existing:
                msg = (f"Skipped {dataset_id} new version creation because of "
                       f"identical metadata to {existing['version']} version")
                logger.info(msg)
                return warn({
                    "dataset_id": dataset_id,
                    "version":    existing["version"],
                    "source_id":  source["id"],
                    "location":   existing["location"],
                    "skipped":    True,
                }, [msg])

        connector = ConnectorFactory.get(source["type"], source["config"])
        version   = _version_id()
        location  = connector.resolve_location(dataset_id, version, dataset["format"], DATA_PATH)
        if not db.reserve_version(dataset_id, version, location):
            return ko("Version already exists", 409)
        logger.info(f"Reserved {dataset_id}@{version}")
        return ok({
            "dataset_id": dataset_id,
            "version":    version,
            "source_id":  source["id"],
            "location":   location,
            "skipped":    False,
        })
    except Exception as e:
        return ko(str(e), 500)


@app.post("/datasets/{dataset_id:path}/_commit/{version}",
          tags=["Dataset Produce"],
          summary="Commit a reserved version (phase 2 of 2-phase write)")
async def dataset_commit(dataset_id: str, version: str, body: CommitRequest):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)

    source    = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])

    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    if record["status"] != "reserved":
        return ko(f"Cannot commit - status is '{record['status']}'", 409)

    location = record["location"]
    if not connector.exists(location):
        return ko(f"Dataset Version not found at: {location}", 422)

    try:
        if not db.commit_version(dataset_id, version):
            raise Exception("commit_version returned False")

        for k, v in (body.metadata or {}).items():
            db.set_metadata(dataset_id, version, k, v)
        if body.task_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_task", body.task_id)
        if body.job_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_job", body.job_id)

        inferred = connector.infer_schema(location)
        db.upsert_schema_columns(dataset_id, inferred)
        diff = db.diff_schema_against_inferred(dataset_id, inferred)

        if body.inputs:
            db.insert_lineage(dataset_id, version,
                              [_model_dump(i) for i in body.inputs])

        dq_result    = None
        expectations = db.list_expectations(dataset_id)
        if expectations:
            dq_result = dq_service.run_on_commit(
                dataset_id, version, connector, location,
                dataset["format"], expectations,
            )

        logger.info(f"Committed {dataset_id}@{version}")

        all_warnings = diff["breaking"] + diff["warnings"]
        data = {
            "dataset_id": dataset_id,
            "version":    version,
            "location":   location,
            "dq":         dq_result,
        }

        if diff["breaking"]:
            msg = f"Schema breaking changes detected on {dataset_id}@{version}"
            logger.warning(msg)
            return warn(data, [msg] + all_warnings)
        if all_warnings:
            return warn(data, all_warnings)
        return ok(data)

    except Exception as e:
        msg = f"Failed to commit {dataset_id}@{version}: {e}"
        logger.error(msg)
        try:
            db.delete_version(dataset_id, version)
            connector.delete(location)
            logger.info(f"Cleanup: deleted orphaned location {location}")
        except Exception as cleanup_err:
            logger.warning(f"Failed to cleanup {location}: {cleanup_err}")
        return ko(f"Failed to commit {dataset_id}@{version}", 500)


@app.post("/datasets/{dataset_id:path}/_fail/{version}",
          tags=["Dataset Produce"],
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    source    = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    record    = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    location = record["location"]
    db.fail_version(dataset_id, version)
    try:
        connector.delete(location)
        db.delete_version(dataset_id, version)
        logger.info(f"Cleanup: deleted orphaned location {location}")
    except Exception as cleanup_err:
        logger.warning(f"Failed to cleanup {location}: {cleanup_err}")
    return ok({"dataset_id": dataset_id, "version": version, "status": "failed"})


# ---------------------------------------------------------------------------
# Routes — Versions lifecycle
# ---------------------------------------------------------------------------

@app.delete("/datasets/{dataset_id:path}/deprecate/{version}",
            tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str):
    if not db.deprecate(dataset_id, version):
        return ko("Version not found", 404)
    logger.info(f"Deprecated {dataset_id}@{version}")
    return ok({"dataset_id": dataset_id, "version": version, "status": "deprecated"})


# ---------------------------------------------------------------------------
# Routes — Virtual datasets
# ---------------------------------------------------------------------------

@app.post("/datasets/{dataset_id:path}/register-virtual",
          tags=["Virtual"],
          summary="Register a virtual dataset version (no local file)",
          status_code=201)
async def register_virtual(dataset_id: str, body: VirtualRegisterRequest):
    src = db.get_source(body.source_id)
    if not src:
        return ko(f"Source '{body.source_id}' not found. "
                  f"Register it first via POST /sources.", 422)
    version = _version_id()
    try:
        db.create_dataset(dataset_id,
                          display_name=body.display_name,
                          description=body.description,
                          owner=body.owner,
                          tags=body.tags)
        db.commit_virtual(dataset_id, version, body.location)
        if body.task_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_task", body.task_id)
        if body.job_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_job", body.job_id)
        logger.info(f"Virtual {dataset_id}@{version} [{src['type']}]")
        return ok({
            "dataset_id":  dataset_id,
            "version":     version,
            "source_id":   body.source_id,
            "source_type": src["type"],
            "location":    body.location,
            "format":      body.format,
        })
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
    version = _version_id()
    path    = dataset_service.local_path(dataset_id, version, "csv")
    try:
        db.create_dataset(dataset_id,
                          display_name=body.display_name,
                          description=body.description)
        db.reserve_version(dataset_id, version, path)

        rows, schema_cols = await dataset_service.fetch_and_write(
            body.base_url, body.endpoint, body.params, path)

        if rows == 0:
            db.fail_version(dataset_id, version)
            return ko("No records returned from endpoint", 422)

        committed = db.commit_version(dataset_id, version)
        if not committed:
            db.fail_version(dataset_id, version)
            return ko("Commit failed", 409)

        db.upsert_schema_columns(dataset_id, schema_cols)
        db.insert_lineage(dataset_id, version, [{
            "dataset_id": f"__external__/{body.base_url}{body.endpoint}",
            "version":    "live",
        }])
        if body.task_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_task", body.task_id)
        if body.job_id:
            db.set_metadata(dataset_id, version, "sys.produced_by_job", body.job_id)

        logger.info(f"Materialized {dataset_id}@{version} rows={rows}")
        return ok({
            "dataset_id": dataset_id,
            "version":    version,
            "path":       path,
            "rows":       rows,
            "source_url": f"{body.base_url}{body.endpoint}",
        })

    except httpx.HTTPError as e:
        db.fail_version(dataset_id, version)
        return ko(f"HTTP error: {e}", 502)
    except Exception as e:
        try:
            db.fail_version(dataset_id, version)
        except Exception:
            pass
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
    count = dataset_service.scan(data_path, body.prefix)
    return ok({"scanned": count, "data_path": data_path})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    with open("logging.yaml") as f:
        logging.config.dictConfig(yaml.safe_load(f))

    if args.scan:
        dataset_service.scan(args.scan_path or DATA_PATH, args.scan_prefix)
        return

    logger.info("Waluigi Catalog v2")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_path}")
    logger.info(f"  Data    : {args.data_path}")

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
