from dataclasses import dataclass
from datetime import datetime


@dataclass
class TaskEntity:
    id: str
    namespace: str | None
    parent_id: str | None
    params: str
    attributes: str
    status: str
    last_update: datetime | None
    job_id: str | None
