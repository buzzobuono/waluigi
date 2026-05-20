from __future__ import annotations

from dataclasses import dataclass, asdict


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
    def from_row(cls, row) -> Dataset | None:
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
