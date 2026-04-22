from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from enum import Enum

class SourceType(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    SQL = "sql"
    SFTP = "sftp"
    API = "api"
    
class SourceCreateRequest(BaseModel):
    id:          str            = Field(...,  example="pg-dwh")
    type:        SourceType     = Field(...,  example=SourceType.SQL)
    config:      Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] =  Field(...,  example="PostgreSQL DWH Catalog")

class SourceUpdateRequest(BaseModel):
    type:        Optional[SourceType]     = None
    config:      Optional[Dict[str, Any]] = None
    description: Optional[str]            = None

class DatasetFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    XLS = "xls"
    XLSX = "xlsx"
    SAS7BDAT = "sas7bdat"
    PKL = "pkl"
    PICKLE = "pickle"
    FEATHER = "feather"
    ORC = "orc"
    TXT = "txt"
    
class DatasetStatus(str, Enum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    
class DatasetCreateRequest(BaseModel):
    id:           str            = Field(...,  example="sales/raw/sales_raw")
    format:       DatasetFormat  = Field(...,  example=DatasetFormat.CSV)
    description:  str            = Field(...,  example="Dataset description")
    status:       DatasetStatus  = Field(DatasetStatus.DRAFT, example=DatasetStatus.DRAFT)
    source_id:    Optional[str]  = Field(None,  example="pg-dwh")
        
class DatasetUpdateRequest(BaseModel):
    description:  Optional[str]  = None
    status:       Optional[DatasetStatus]  = DatasetStatus.IN_REVIEW
        
#----------

    
class LineageRef(BaseModel):
    dataset_id: str = Field(..., example="finance/erp/fatture")
    version:    str = Field(..., example="2026-04-11T10:00:00+00:00")


class CommitRequest(BaseModel):
    inputs:        List[LineageRef]         = Field(default_factory=list)
    metadata:      Dict[str, str]           = Field(default_factory=dict)

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

