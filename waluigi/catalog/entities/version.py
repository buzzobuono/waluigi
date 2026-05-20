from __future__ import annotations

from dataclasses import dataclass, asdict


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
    def from_row(cls, row) -> Version | None:
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
