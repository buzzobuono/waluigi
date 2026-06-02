from dataclasses import dataclass
from datetime import datetime


@dataclass
class WorkerEntity:
    url: str
    status: str
    max_slots: int
    free_slots: int
    last_seen: datetime | None
