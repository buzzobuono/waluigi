from __future__ import annotations
from pydantic import BaseModel
from typing import Any


class WorkerRegisterRequest(BaseModel):
    url: str
    max_slots: int
    free_slots: int


class TaskUpdateRequest(BaseModel):
    status: str
    params: str | None = None
    attributes: str | None = None
    resources: dict = {"coin": 1.0}
    worker_url: str | None = None


class LogAppendRequest(BaseModel):
    logs: list[str] = []
    worker_id: str = "unknown"
