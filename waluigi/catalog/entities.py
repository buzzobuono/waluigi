import json
from dataclasses import dataclass, asdict


@dataclass
class Source:
    id:          str
    type:        str
    config:      dict
    description: str | None
    username:    str
    createdate:  str
    updatedate:  str

    @classmethod
    def from_row(cls, row) -> "Source | None":
        if row is None:
            return None
        d = dict(row._mapping)
        config = d.get("config") or {}
        if isinstance(config, str):
            config = json.loads(config)
        return cls(
            id=d["id"],
            type=d["type"],
            config=config,
            description=d.get("description"),
            username=d["username"],
            createdate=d["createdate"],
            updatedate=d["updatedate"],
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Dataset:
    id:          str
    format:      str
    description: str | None
    status:      str
    source_id:   str | None
    dq_suite:    str | None
    username:    str
    createdate:  str
    updatedate:  str

    @classmethod
    def from_row(cls, row) -> "Dataset | None":
        if row is None:
            return None
        d = dict(row._mapping)
        return cls(
            id=d["id"],
            format=d["format"],
            description=d.get("description"),
            status=d["status"],
            source_id=d.get("source_id"),
            dq_suite=d.get("dq_suite"),
            username=d["username"],
            createdate=d["createdate"],
            updatedate=d["updatedate"],
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Version:
    dataset_id: str
    version:    str
    location:   str
    status:     str
    username:   str
    createdate: str
    updatedate: str

    @classmethod
    def from_row(cls, row) -> "Version | None":
        if row is None:
            return None
        d = dict(row._mapping)
        return cls(
            dataset_id=d["dataset_id"],
            version=d["version"],
            location=d["location"],
            status=d["status"],
            username=d["username"],
            createdate=d["createdate"],
            updatedate=d["updatedate"],
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Expectation:
    id:         int
    dataset_id: str
    rule_id:    str
    inputs:     dict
    params:     dict
    tolerance:  float
    position:   int
    username:   str
    createdate: str
    updatedate: str

    @classmethod
    def from_row(cls, row) -> "Expectation | None":
        if row is None:
            return None
        d = dict(row._mapping)
        inputs = d.get("inputs") or {}
        if isinstance(inputs, str):
            inputs = json.loads(inputs)
        params = d.get("params") or {}
        if isinstance(params, str):
            params = json.loads(params)
        return cls(
            id=d["id"],
            dataset_id=d["dataset_id"],
            rule_id=d["rule_id"],
            inputs=inputs,
            params=params,
            tolerance=d.get("tolerance", 1.0),
            position=d.get("position", 0),
            username=d["username"],
            createdate=d["createdate"],
            updatedate=d["updatedate"],
        )

    def to_dict(self) -> dict:
        return asdict(self)
