import os
import sys
import csv
import json
import math
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

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out",
}

try:
    db = CatalogDB(args.db_path)
    logger.info(f"Database ready: {args.db_path}")
except Exception as e:
    logger.error(f"❌ Critical DB error: {e}")
    sys.exit(1)

dq_manager = DQManager(RULES_PATH)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _safe(v):
    """Convert a value to a JSON-serialisable scalar."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        f = float(v)
        return round(f, 6)
    except (TypeError, ValueError):
        return str(v)


def _build_echarts_option(df: "pd.DataFrame", spec: dict) -> dict:
    chart_type = spec.get("type", "bar")
    x_conf     = spec.get("x", {})
    y_conf     = spec.get("y", {})
    x_field    = x_conf.get("field")
    y_field    = y_conf.get("field")
    x_label    = x_conf.get("label", x_field or "")
    y_label    = y_conf.get("label", y_field or "")
    agg        = y_conf.get("agg", "sum")
    color_field = spec.get("color")
    limit      = int(spec.get("limit", 200))

    base = {
        "tooltip": {},
        "toolbox": {"feature": {"saveAsImage": {}, "dataZoom": {}}},
        "grid":    {"containLabel": True},
    }

    # ── PIE ──────────────────────────────────────────────────────────────────
    if chart_type == "pie":
        grouped = (df.groupby(x_field)[y_field].agg(agg)
                   .reset_index().head(limit))
        data = [{"name": str(r[x_field]), "value": _safe(r[y_field])}
                for _, r in grouped.iterrows()]
        return {**base,
                "tooltip": {"trigger": "item", "formatter": "{b}: {c} ({d}%)"},
                "legend":  {"orient": "vertical", "left": "left"},
                "series":  [{"type": "pie", "data": data,
                             "radius": "60%", "label": {"formatter": "{b}\n{d}%"}}]}

    # ── HISTOGRAM ────────────────────────────────────────────────────────────
    if chart_type == "histogram":
        col  = df[x_field].dropna()
        bins = int(spec.get("bins", 20))
        cuts = pd.cut(col, bins=bins)
        counts = cuts.value_counts().sort_index()
        cats = [str(i) for i in counts.index]
        vals = [int(v) for v in counts.values]
        return {**base,
                "tooltip": {"trigger": "axis"},
                "xAxis":   {"type": "category", "data": cats,
                            "name": x_label, "axisLabel": {"rotate": 30}},
                "yAxis":   {"type": "value", "name": "Count"},
                "series":  [{"type": "bar", "data": vals, "barWidth": "99%"}]}

    # ── SCATTER ──────────────────────────────────────────────────────────────
    if chart_type == "scatter":
        data = (df[[x_field, y_field]].dropna().head(limit)
                .apply(lambda r: [_safe(r[x_field]), _safe(r[y_field])], axis=1)
                .tolist())
        return {**base,
                "tooltip": {"trigger": "item"},
                "xAxis":   {"type": "value", "name": x_label},
                "yAxis":   {"type": "value", "name": y_label},
                "series":  [{"type": "scatter", "data": data, "symbolSize": 8}]}

    # ── RADAR (spider chart) ─────────────────────────────────────────────────
    if chart_type == "radar":
        axes      = spec.get("axes", [])
        group_by  = spec.get("group_by") or color_field
        if len(axes) < 3:
            return ko("radar requires at least 3 axes to form a polygon", 422)

        ax_fields = [a["field"] for a in axes]
        ax_labels = [a.get("label", a["field"]) for a in axes]

        if group_by:
            grouped = df.groupby(group_by)[ax_fields].agg(agg if agg != "count" else "sum")
            maxes   = [grouped[f].max() for f in ax_fields]
            indicator = [
                {"name": lbl, "max": a.get("max") or (_safe(mx) * 1.2 if mx else 1)}
                for a, lbl, mx in zip(axes, ax_labels, maxes)
            ]
            series_data = [
                {"name": str(grp),
                 "value": [_safe(grouped.loc[grp, f]) for f in ax_fields]}
                for grp in grouped.index
            ]
            legend = {"data": [str(g) for g in grouped.index]}
        else:
            agged   = df[ax_fields].agg(agg if agg != "count" else "sum")
            vals    = [_safe(agged[f]) for f in ax_fields]
            maxes   = [abs(v) * 1.2 if v else 1 for v in vals]
            indicator = [
                {"name": lbl, "max": a.get("max") or mx}
                for a, lbl, mx in zip(axes, ax_labels, maxes)
            ]
            series_data = [{"name": "Total", "value": vals}]
            legend = {}

        opt = {**base,
               "tooltip": {"trigger": "item"},
               "radar":   {"indicator": indicator, "shape": "polygon"},
               "series":  [{"type": "radar", "data": series_data,
                            "areaStyle": {"opacity": 0.2}}]}
        if legend:
            opt["legend"] = legend
        return opt

    # ── BAR / LINE ───────────────────────────────────────────────────────────
    if color_field:
        if agg == "count":
            agged = (df.groupby([x_field, color_field])
                     .size().reset_index(name=y_field or "_count"))
            y_field = y_field or "_count"
        else:
            agged = (df.groupby([x_field, color_field])[y_field]
                     .agg(agg).reset_index())
        pivot    = (agged.pivot(index=x_field, columns=color_field, values=y_field)
                    .fillna(0).head(limit))
        cats     = [str(c) for c in pivot.index]
        series   = [{"name": str(col), "type": chart_type,
                     "data": [_safe(v) for v in pivot[col]]}
                    for col in pivot.columns]
        legend   = {"data": [str(c) for c in pivot.columns]}
    else:
        if agg == "count":
            grouped = (df[x_field].value_counts()
                       .reset_index().head(limit))
            grouped.columns = [x_field, "_count"]
            cats = [str(v) for v in grouped[x_field]]
            vals = [int(v) for v in grouped["_count"]]
        else:
            grouped = (df.groupby(x_field)[y_field]
                       .agg(agg).reset_index().head(limit))
            cats = [str(v) for v in grouped[x_field]]
            vals = [_safe(v) for v in grouped[y_field]]
        series = [{"type": chart_type, "data": vals, "name": y_label}]
        legend = {}

    opt = {**base,
           "tooltip": {"trigger": "axis"},
           "xAxis":   {"type": "category", "data": cats,
                       "name": x_label, "axisLabel": {"rotate": 30}},
           "yAxis":   {"type": "value", "name": y_label},
           "series":  series}
    if legend:
        opt["legend"] = legend
    return opt


def _run_dq_nonblocking(dataset_id: str, version: str,
                        connector, location, fmt: str,
                        expectations: list = None) -> dict | None:
    """Run DQ expectations against the committed dataset. Never raises — writes to dq_results table."""
    try:
        if not expectations:
            return None
        df = connector.read(location, fmt)
        result = dq_manager.run_from_db(expectations, {"this": df})
        details = [
            {"rule_id": r.rule_id, "success": r.success,
             "score": r.score, "error": r.error}
            for r in result.results
        ]
        row = db.save_dq_result(
            dataset_id, version,
            score=result.score, passed=result.passed,
            total=result.total, success=result.success,
            details=details,
        )
        logger.info(f"DQ {dataset_id}@{version}: score={result.score:.2%} ({result.passed}/{result.total})")
        return row
    except Exception as e:
        logger.warning(f"DQ run skipped for {dataset_id}@{version}: {e}")
        db.save_dq_result(
            dataset_id, version,
            score=0.0, passed=0, total=0, success=False,
            details=[], error=str(e),
        )
        return None


def _extract_items(body) -> list:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "results", "items", "records",
                    "content", "entries", "rows"):
            if key in body and isinstance(body[key], list):
                return body[key]
        values = [v for v in body.values() if isinstance(v, list)]
        if len(values) == 1:
            return values[0]
    return []


def _next_url(body, base_url: str, endpoint: str, page: int) -> str | None:
    if not isinstance(body, dict):
        return None
    for key in ("next", "next_page", "nextPage", "nextCursor"):
        val = body.get(key)
        if val and isinstance(val, str):
            return val if val.startswith("http") else f"{base_url}{val}"
    total = body.get("total_pages") or body.get("pages") or body.get("totalPages")
    if total and page < int(total):
        return f"{base_url}{endpoint}"
    return None


def _flatten(obj, prefix="", sep="_") -> dict:
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep))
        elif isinstance(v, list):
            out[key] = (str(v) if (v and isinstance(v[0], dict))
                        else ",".join(str(i) for i in v))
        else:
            out[key] = v
    return out


async def _fetch_and_write(base_url: str, endpoint: str,
                           params: dict,
                           output_path: str) -> tuple[int, list[dict]]:
    records, page = [], 1
    next_url = f"{base_url}{endpoint}"

    async with httpx.AsyncClient(timeout=30) as client:
        while next_url:
            call_params = {**params, "page": page} if page > 1 else params
            r = await client.get(next_url, params=call_params)
            r.raise_for_status()
            body  = r.json()
            items = _extract_items(body)
            if not items:
                break
            records.extend([_flatten(item) for item in items])
            next_url = _next_url(body, base_url, endpoint, page)
            if next_url:
                page += 1
            else:
                break

    if not records:
        return 0, []

    fieldnames = list(dict.fromkeys(k for row in records for k in row.keys()))
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    schema_cols = [{"name": k, "physical_type": "string",
                    "logical_type": "string"} for k in fieldnames]
    return len(records), schema_cols


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def _scan(data_path: str, prefix: str = None) -> int:
    logger.info(f"🔍 Scanning {data_path} ...")
    count = 0
    for root, dirs, files in os.walk(data_path):
        dirs.sort()
        for filename in sorted(files):
            ext = os.path.splitext(filename)[1].lower()
            if ext not in SCANNABLE_EXTENSIONS:
                continue

            filepath = os.path.join(root, filename)
            fmt      = ext.lstrip(".")
            rel_dir  = os.path.relpath(root, data_path).replace(os.sep, "/")
            name     = os.path.splitext(filename)[0]
            version  = name.replace("-", ":", 2)

            if prefix:
                dataset_id = f"{prefix.strip('/')}/{rel_dir}/{name}".replace("//", "/")
            else:
                dataset_id = f"{rel_dir}/{name}".replace("//", "/")

            try:
                file_hash = _compute_hash(filepath)
                schema    = _infer_schema(filepath, fmt)
                db.create_dataset(dataset_id)
                db.reserve_version(dataset_id, version, filepath, fmt,
                           "scanner", "scan")
                result = db.commit(dataset_id, version, file_hash, None,
                                   {c["name"]: c["physical_type"] for c in schema})
                if result and not result["skipped"]:
                    db.upsert_schema_columns(dataset_id, schema)
                count += 1
                logger.info(f"  ✅ {dataset_id}@{version[:19]} [{fmt}]")
            except Exception as e:
                logger.error(f"  ⚠️  Skipped {filepath}: {e}")

    logger.info(f"🏁 Scan complete — {count} dataset(s) registered.")
    return count


# Routes Folders

@app.get("/folders/{prefix:path}/", tags=["Browse"],
         summary="List datasets and virtual sub-prefixes under a prefix",
         description=(
             "Trailing slash distinguishes browse from dataset access. "
             "Returns direct child datasets and deeper virtual prefixes, "
             "exactly like S3 ListObjects with a delimiter."
         ))
async def list_folders(prefix: str):
    return ok(db.list_folders(prefix))


# Routes Sources

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
    deleted = db.delete_source(id)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": id})


# Routes — Version Metadata

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


# Routes - Versions

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

    location  = version_record["location"]
    source_type = source.get("type", "local")

    try:
        connector = ConnectorFactory.get(source_type, source.get("config") or {})
        result = connector.read(location, fmt, limit=limit, offset=offset)
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

# Routes - Datasets Schema

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
    updated = db.update_schema_column(dataset_id, column_name, **updates)
    if not updated:
        return ko("Column not found in schema", 404)
    # Any schema edit promotes dataset to in_review
    db.set_in_review(dataset_id)
    col  = next((c for c in db.get_schema(dataset_id)
                 if c["column_name"] == column_name), None)
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
    return ok({"dataset_id" : dataset_id})

# Routes - Expectations

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


# Routes - Charts

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
    chart = db.add_chart(dataset_id, body.title, body.spec, body.position)
    return ok(chart)


@app.patch("/datasets/{dataset_id:path}/charts/{chart_id}", tags=["Charts"],
           summary="Update a chart definition")
async def update_chart(dataset_id: str, chart_id: int, body: ChartUpdateRequest):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
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
         summary="Render a chart — returns an ECharts option object with aggregated data")
async def render_chart(dataset_id: str, chart_id: int,
                       version: str = Query(None, description="Dataset version; defaults to latest")):
    chart = db.get_chart(dataset_id, chart_id)
    if not chart:
        return ko("Chart not found", 404)
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)

    ver = db.get_version(dataset_id, version) if version else db.get_latest_version(dataset_id)
    if not ver:
        return ko("No committed version available", 404)

    source    = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    try:
        df = connector.read(ver["location"], dataset["format"])
    except Exception as e:
        return ko(f"Cannot read dataset: {e}", 500)

    try:
        option = _build_echarts_option(df, chart["spec"])
    except Exception as e:
        return ko(f"Cannot build chart: {e}", 422)

    return ok({"option": option, "version": ver["version"], "rows": len(df)})


# Routes - DQ Results

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


# Routes - Data Quality

@app.get("/dq/rules", tags=["Data Quality"],
         summary="List all DQ rules available in the catalogue")
async def list_dq_rules():
    rules = [
        {
            "id":            rule_id,
            "description":   rule.description,
            "formula":       rule.formula.strip(),
            "inputs_schema": rule.inputs_schema,
            "params_schema": rule.params_schema or {},
        }
        for rule_id, rule in sorted(dq_manager.catalogue.items())
    ]
    return ok(rules)


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


# Routes - Datasets

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
    created = db.create_dataset(body.id, body.format, body.description, body.source_id, body.dq_suite)
    # FIX ME gestire upsert in db.py analogogamemte a come fatto in source
    #if not created:
    #    return ko(f"Dataset '{body.id}' already exists", 409)
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
async def delete_source(id: str):
    deleted = db.delete_dataset(id)
    ## Fix: loop su tutte le versioni di questo dataset e esegue la cancellazione anche delle vesioni
    # la cancellazione di una versione rimuove anche il dataset fisico correlato tramite il connector specifico
    if not deleted:
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})
        
######


@app.get("/datasets/{dataset_id:path}/lineage/{version}", tags=["Lineage"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str,
                      version: str):
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



# Dataset Status

@app.post("/datasets/{dataset_id:path}/approve",
          tags=["Datasets Status"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    if dataset.get("status") == "deprecated":
        return ko("Cannot approve a deprecated dataset", 409)

    # Publish schema atomically with approval
    schema_result = db.publish_schema(dataset_id, publisher=body.approved_by)
    approved      = db.approve_dataset(dataset_id, body.approved_by)
    if not approved:
        return ko("Approval failed", 500)

    msgs = schema_result["breaking_changes"] + schema_result["warnings"]
    data = {
        "dataset_id":      dataset_id,
        "status":          "approved",
        "approved_by":     body.approved_by,
        "notes":           body.notes,
        "schema_published_at":  schema_result["published_at"],
        "breaking_changes":     schema_result["breaking_changes"],
        "warnings":             schema_result["warnings"],
    }
    logger.info(f"Approved {dataset_id} by {body.approved_by}")
    if schema_result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking schema changes on approval"] + msgs)
    return warn(data, msgs) if msgs else ok(data)



# Dataset Produce

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
                msg = f"Skipped {dataset_id} new version creation because of identical metadata to {existing['version']} version"
                logger.info(msg)
                return warn({
                    "dataset_id": dataset_id,
                    "version":    existing["version"],
                    "source_id":  source["id"],
                    "location":   existing["location"],
                    "skipped":    True    
                  }, [msg])
                  
        connector = ConnectorFactory.get(source["type"], source["config"])
    
        version = _version_id()
        location = connector.resolve_location(dataset_id, version, dataset["format"], DATA_PATH)
        if not db.reserve_version(dataset_id, version, location):
            return ko("Version already exists", 409)
        logger.info(f"Reserved {dataset_id}@{version}")
        return ok({"dataset_id": dataset_id,
                   "version":    version,
                   "source_id":  source["id"],    
                   "location":   location,
                   "skipped":    False })
    except Exception as e:
        return ko(str(e), 500)


@app.post("/datasets/{dataset_id:path}/_commit/{version}",
          tags=["Dataset Produce"],
          summary="Commit a reserved version (phase 1 of 2-phase write)")
async def dataset_commit(dataset_id: str, version: str, body: CommitRequest):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    source = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    if record["status"] != "reserved":
        return ko(f"Cannot commit - status is '{record['status']}'", 409)

    location = record["location"]
    if not connector.exists(location):
        return ko(f"Dataset Version not found at: {location}", 422)
    msgs = []        
    try:
        if not db.commit_version(dataset_id, version):
            raise Exception
        
        for k, v in (body.metadata or {}).items():
            db.set_metadata(dataset_id, version, k, v)

        inferred = connector.infer_schema(location)
        db.upsert_schema_columns(dataset_id, inferred)
        diff = db.diff_schema_against_inferred(dataset_id, inferred)
        
        if body.inputs:
            db.insert_lineage(dataset_id, version,
                              [_model_dump(i) for i in body.inputs])

        dq_result = None
        expectations = db.list_expectations(dataset_id)
        if expectations:
            dq_result = _run_dq_nonblocking(
                dataset_id, version, connector, location,
                dataset["format"], expectations
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
            msgs.append(["Schema breaking changes"] + all_warnings)
            raise Exception
        if all_warnings:
            return warn(data, all_warnings)

        return ok(data)

    except Exception as e:
        msg = f"Fail to commit {dataset_id}@{version}"
        msgs.append(msg)
        logger.error(msgs, e)
        try:
            db.delete_version(dataset_id, version)
            connector.delete(location) 
            logger.info(f"Cleanup: deleted orphaned location {location}")
        except Exception as cleanup_err:
            logger.warning(f"Failed to cleanup orphaned location {location}: {cleanup_err}")
        return ko(msg, 500)
        

@app.post("/datasets/{dataset_id:path}/_fail/{version}",
          tags=["Dataset Produce"],
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    source = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    location = record["location"]
    db.fail_version(dataset_id, version)
    try:
        connector.delete(location) 
        db.delete_version(dataset_id, version)
        logger.info(f"Cleanup: deleted orphaned location {location}")
    except Exception as cleanup_err:
        logger.warning(f"Failed to cleanup orphaned location {location}: {cleanup_err}")
    return ok({"dataset_id": dataset_id,
               "version":    version,
               "status":     "failed"})


# Versions

@app.delete("/datasets/{dataset_id:path}/deprecate/{version}",
            tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str):
    if not db.deprecate(dataset_id, version):
        return ko("Version not found", 404)
    logger.info(f"Deprecated {dataset_id}@{version}")
    return ok({"dataset_id": dataset_id,
               "version":    version,
               "status":     "deprecated"})


# ===========================================================================
# Routes — Virtual datasets
# ===========================================================================

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
        db.commit_virtual(dataset_id, version, body.source_id,
                          body.location, body.format,
                          body.task_id, body.job_id)
        logger.info(f"Virtual {dataset_id}@{version} [{src['type']}]")
        return ok({"dataset_id":  dataset_id,
                   "version":     version,
                   "source_id":   body.source_id,
                   "source_type": src["type"],
                   "location":    body.location,
                   "format":      body.format})
    except Exception as e:
        return ko(str(e), 500)



# ===========================================================================
# Routes — Materialize
# ===========================================================================

@app.post("/datasets/{dataset_id:path}/materialize",
          tags=["Materialize"],
          summary="Fetch a REST API and store result as a local CSV version",
          status_code=201)
async def materialize(dataset_id: str, body: MaterializeRequest):
    version = _version_id()
    path    = _local_path(dataset_id, version, "csv")
    try:
        db.create_dataset(dataset_id,
                          display_name=body.display_name,
                          description=body.description)
        db.reserve_version(dataset_id, version, path,
                   "csv", body.task_id, body.job_id)

        rows, schema_cols = await _fetch_and_write(
            body.base_url, body.endpoint, body.params, path)

        if rows == 0:
            db.fail(dataset_id, version)
            return ko("No records returned from endpoint", 422)

        file_hash = _compute_hash(path)
        schema_kv = {c["name"]: c["physical_type"] for c in schema_cols}
        result    = db.commit(dataset_id, version,
                              file_hash, rows, schema_kv)

        if result is None:
            db.fail_version(dataset_id, version)
            return ko("Commit failed", 409)

        if result["skipped"]:
            try:
                os.remove(path)
            except Exception:
                pass
            return ok({"dataset_id": dataset_id,
                       "version":    result["version"],
                       "rows":       rows,
                       "skipped":    True,
                       "reason":     "Identical to latest committed version",
                       "source_url": f"{body.base_url}{body.endpoint}"})

        db.upsert_schema_columns(dataset_id, schema_cols)
        db.insert_lineage(dataset_id, version, [{
            "dataset_id": f"__external__/{body.base_url}{body.endpoint}",
            "version":    "live",
        }])
        logger.info(f"Materialized {dataset_id}@{version} rows={rows}")
        return ok({"dataset_id": dataset_id,
                   "version":    version,
                   "path":       path,
                   "rows":       rows,
                   "hash":       file_hash,
                   "skipped":    False,
                   "source_url": f"{body.base_url}{body.endpoint}"})

    except httpx.HTTPError as e:
        db.fail(dataset_id, version)
        return ko(f"HTTP error: {e}", 502)
    except Exception as e:
        try:
            db.fail(dataset_id, version)
        except Exception:
            pass
        return ko(str(e), 500)


# ===========================================================================
# Routes — Scan
# ===========================================================================

@app.post("/scan", tags=["Scan"],
          summary="Scan a filesystem path and register all dataset files found")
async def scan_api(body: ScanRequest):
    data_path = body.data_path or DATA_PATH
    if not os.path.exists(data_path):
        return ko(f"Path not found: {data_path}", 404)
    count = _scan(data_path, body.prefix)
    return ok({"scanned": count, "data_path": data_path})


# ===========================================================================
# Entrypoint
# ===========================================================================
  
def main():
    with open("logging.yaml") as f:
        logging.config.dictConfig(yaml.safe_load(f))

    if args.scan:
        _scan(args.scan_path or DATA_PATH, args.scan_prefix)
        return

    logger.info("Waluigi Catalog v2")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_path}")
    logger.info(f"  Data    : {args.data_path}")
    
    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)
    
if __name__ == "__main__":
    main()
