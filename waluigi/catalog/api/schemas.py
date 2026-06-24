from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from enum import Enum

class SourceType(str, Enum):
    LOCAL      = "local"
    S3         = "s3"
    SQL        = "sql"
    SFTP       = "sftp"
    API        = "api"
    SHAREPOINT = "sharepoint"

class SourceCreateRequest(BaseModel):
    id:          str            = Field(...,  example="pg-dwh")
    type:        SourceType     = Field(...,  example=SourceType.SQL)
    config:      Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = Field(None, example="PostgreSQL DWH Catalog")

class SourceUpdateRequest(BaseModel):
    type:        Optional[SourceType]     = None
    config:      Optional[Dict[str, Any]] = None
    description: Optional[str]            = None

class DatasetFormat(str, Enum):
    PARQUET = "parquet"
    CSV     = "csv"
    TSV     = "tsv"
    JSON    = "json"
    SQL     = "sql"

class DatasetStatus(str, Enum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    DEPRECATED = "deprecated"

class DatasetCreateRequest(BaseModel):
    id:           str            = Field(...,  example="test/ex/dataset_raw")
    format:       DatasetFormat  = Field(...,  example=DatasetFormat.CSV)
    description:  str            = Field("",   example="Dataset description")
    status:       DatasetStatus  = Field(DatasetStatus.DRAFT, example=DatasetStatus.DRAFT)
    source_id:    str            = Field(...,  example="pg-dwh")
    dq_suite:     Optional[str]  = Field(None, example="/rules/suites/sales_suite.yaml")

class DatasetUpdateRequest(BaseModel):
    description:  Optional[str]           = None
    status:       Optional[DatasetStatus] = None
    dq_suite:     Optional[str]           = None

class ReserveRequest(BaseModel):
    metadata: Dict[str, str] = Field(default_factory=dict)
    force:    bool           = Field(False, description="Skip metadata-based dedup and always create a new version")

class LineageRef(BaseModel):
    dataset_id: str = Field(..., example="analytics/finance/erp/fatture")
    version:    str = Field(..., example="2026-04-11T10:00:00+00:00")

class CommitRequest(BaseModel):
    metadata: Dict[str, str]   = Field(default_factory=dict)
    inputs:   List[LineageRef] = Field(default_factory=list)
    task_id:  Optional[str]   = None
    job_id:   Optional[str]   = None

class MetadataSetRequest(BaseModel):
    key:   str = Field(..., example="source")
    value: str = Field(..., example="SAP_EXTRACT")

class SchemaColumnPatch(BaseModel):
    logical_type: Optional[str]       = None
    nullable:     Optional[bool]      = None
    pii:          Optional[bool]      = None
    pii_type:     Optional[str]       = None
    pii_notes:    Optional[str]       = None
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None


class VirtualRegisterRequest(BaseModel):
    source_id:    str            = Field(...,   example="pg-dwh")
    location:     str            = Field(...,   example="SELECT * FROM finance.fatture")
    format:       str            = Field("sql", example="sql")
    task_id:      str            = Field("unknown")
    job_id:       str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None
    owner:        Optional[str] = None
    tags:         Optional[List[str]] = None


class SchemaPublishRequest(BaseModel):
    published_by: str = Field("anonymous", example="mario.rossi")


class ChartCreateRequest(BaseModel):
    key:      str                = Field(..., example="revenue_by_category")
    title:    str                = Field(..., example="Revenue by Category")
    spec:     Dict[str, Any]     = Field(..., example={"type": "bar", "x": {"field": "category"}, "y": {"field": "revenue", "agg": "sum"}})
    position: int                = Field(0)

class ChartUpdateRequest(BaseModel):
    key:      Optional[str]            = None
    title:    Optional[str]            = None
    spec:     Optional[Dict[str, Any]] = None
    position: Optional[int]            = None

class ExpectationCreateRequest(BaseModel):
    rule_id:   str            = Field(..., example="expect_column_values_to_not_be_null")
    inputs:    Dict[str, Any] = Field(default_factory=dict)
    params:    Dict[str, Any] = Field(default_factory=dict)
    tolerance: float          = Field(1.0, example=1.0)
    position:  int            = Field(0,   example=0)

class ExpectationUpdateRequest(BaseModel):
    rule_id:   Optional[str]            = None
    inputs:    Optional[Dict[str, Any]] = None
    params:    Optional[Dict[str, Any]] = None
    tolerance: Optional[float]          = None
    position:  Optional[int]            = None


class ApproveRequest(BaseModel):
    approved_by: str = Field(...,  example="mario.rossi")
    notes:       str = Field("",   example="PII verified, schema confirmed")


class MaterializeRequest(BaseModel):
    source_id:    str            = Field("local", example="local")
    base_url:     str            = Field(..., example="https://api.example.com")
    endpoint:     str            = Field(..., example="/v1/orders")
    params:       Dict[str, Any] = Field(default_factory=dict)
    task_id:      str            = Field("unknown")
    job_id:       str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None


class ScanRequest(BaseModel):
    data_path: Optional[str] = None
    prefix:    Optional[str] = None
    source_id: str           = Field("local")


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class SourceResponse(BaseModel):
    namespace:   str
    id:          str
    type:        str
    config:      Dict[str, Any]
    description: Optional[str]
    createdate:  str
    updatedate:  str

    @classmethod
    def from_entity(cls, e) -> "SourceResponse":
        return cls(
            namespace=e.namespace, id=e.id, type=e.type, config=e.config,
            description=e.description,
            createdate=e.createdate, updatedate=e.updatedate,
        )


class DatasetResponse(BaseModel):
    namespace:   str
    id:          str
    format:      str
    description: Optional[str]
    status:      str
    source_id:   str
    dq_suite:    Optional[str]
    createdate:  str
    updatedate:  str

    @classmethod
    def from_entity(cls, e) -> "DatasetResponse":
        return cls(
            namespace=e.namespace, id=e.id, format=e.format,
            description=e.description, status=e.status,
            source_id=e.source_id, dq_suite=e.dq_suite,
            createdate=e.createdate, updatedate=e.updatedate,
        )


class VersionResponse(BaseModel):
    dataset_id: str
    version:    str
    location:   str
    status:     str
    createdate: str
    updatedate: str

    @classmethod
    def from_entity(cls, e) -> "VersionResponse":
        return cls(
            dataset_id=e.dataset_id, version=e.version,
            location=e.location, status=e.status,
            createdate=e.createdate, updatedate=e.updatedate,
        )


class ExpectationResponse(BaseModel):
    id:         int
    dataset_id: str
    rule_id:    str
    inputs:     Dict[str, Any]
    params:     Dict[str, Any]
    tolerance:  float
    position:   int
    createdate: str
    updatedate: str

    @classmethod
    def from_entity(cls, e) -> "ExpectationResponse":
        return cls(
            id=e.id, dataset_id=e.dataset_id, rule_id=e.rule_id,
            inputs=e.inputs, params=e.params,
            tolerance=e.tolerance, position=e.position,
            createdate=e.createdate, updatedate=e.updatedate,
        )
