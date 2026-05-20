from __future__ import annotations

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
    def from_row(cls, row) -> Source | None:
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
