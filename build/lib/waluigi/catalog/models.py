from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

class SourceCreateRequest(BaseModel):
    id:          str            = Field(...,  example="pg-dwh")
    type:        str            = Field(...,  example="sql")
    config:      Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None

class SourceUpdateRequest(BaseModel):
    type:        Optional[str]            = None
    config:      Optional[Dict[str, Any]] = None
    description: Optional[str]            = None

class DatasetUpdateRequest(BaseModel):
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None
    owner:        Optional[str]       = None
    status:       Optional[str]       = None
        
class ReserveRequest(BaseModel):
    format:       str            = Field("",        example="csv")
    task_id:      str            = Field("unknown", example="ingest_sales")
    job_id:       str            = Field("unknown", example="job/daily")
    source_id:    Optional[str] = None
    description:  Optional[str] = None
    owner:        Optional[str] = None
    tags:         Optional[List[str]] = None
        
class LineageRef(BaseModel):
    dataset_id: str = Field(..., example="finance/erp/fatture")
    version:    str = Field(..., example="2026-04-11T10:00:00+00:00")


class CommitRequest(BaseModel):
    rows:          Optional[int]            = None
    columns:       Optional[Dict[str, Any]] = Field(None, alias="schema")
    inputs:        List[LineageRef]         = Field(default_factory=list)
    business_meta: Dict[str, str]           = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


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


class SchemaColumnPatch(BaseModel):
    logical_type: Optional[str]       = None
    nullable:     Optional[bool]      = None
    pii:          Optional[bool]      = None
    pii_type:     Optional[str]       = None
    pii_notes:    Optional[str]       = None
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None


class SchemaPublishRequest(BaseModel):
    published_by: str = Field("anonymous", example="mario.rossi")


class ApproveRequest(BaseModel):
    approved_by: str  = Field(...,  example="mario.rossi")
    notes:       str  = Field("",   example="PII verified, schema confirmed")


class MetadataSetRequest(BaseModel):
    key:   str = Field(..., example="source")
    value: str = Field(..., example="SAP_EXTRACT")


class MaterializeRequest(BaseModel):
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

