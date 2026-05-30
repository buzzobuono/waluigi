from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


class ExecuteTaskRequest(BaseModel):
    id: str = Field(..., description="Unique identifier of the task request")
    job_id: str = Field(..., description="ID of the associated job")
    type: Optional[str] = Field(None, description="Type of task to be executed")
    command: Optional[str] = Field(None, description="Shell command string to execute")
    script: Optional[str] = Field(None, description="Script content or path to run")
    workdir: Optional[str] = Field(None, description="Working directory path")
    params: Dict[str, Any] = Field(default_factory=dict, description="Task state parameters")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Task custom attributes")
    config: Dict[str, Any] = Field(default_factory=dict, description="Task configuration")
    resources: Dict[str, Any] = Field(default_factory=dict, description="Resource allocations or constraints")
  