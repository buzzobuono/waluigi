from __future__ import annotations

import json
from dataclasses import dataclass, asdict


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
    def from_row(cls, row) -> Expectation | None:
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
