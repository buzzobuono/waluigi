from dataclasses import dataclass
from datetime import datetime


@dataclass
class TaskLogEntity:
    id: int | None
    task_id: str
    timestamp: datetime | None
    message: str
    boss_id: str | None
