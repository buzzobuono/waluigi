from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Union


class ExecuteTaskRequest(BaseModel):
    id: str = Field(..., description="Unique identifier of the task request")
    job_id: str = Field(..., description="ID of the associated job")
    command: Optional[str] = Field(None, description="Shell command string to execute")
    script: Optional[str] = Field(None, description="Script content or path to run")
    prepare: Optional[Union[str, List[str]]] = Field(None, description="Shell commands to run before the task command")
    params: Dict[str, Any] = Field(default_factory=dict, description="Task state parameters")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Task custom attributes")
    config: Dict[str, Any] = Field(default_factory=dict, description="Task configuration")
    resources: Dict[str, Any] = Field(default_factory=dict, description="Resource allocations or constraints")
    secrets: Dict[str, str] = Field(default_factory=dict, description="Namespace secrets injected as env vars")
  