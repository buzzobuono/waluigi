from dataclasses import dataclass
from datetime import datetime


@dataclass
class JobEntity:
    job_id: str
    metadata: str   # JSON blob
    spec: str       # JSON blob
    status: str
    started_at: datetime | None
    locked_by: str | None
    locked_until: datetime | None
