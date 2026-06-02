from pydantic import BaseModel
from typing import Optional


class LoginRequest(BaseModel):
    username: str
    password: str


class UserCreateRequest(BaseModel):
    userid:     str
    username:   str
    password:   str
    namespaces: list[str] = []


class UserUpdateRequest(BaseModel):
    username:   Optional[str]       = None
    password:   Optional[str]       = None
    namespaces: Optional[list[str]] = None


class UserUpsertRequest(BaseModel):
    username:   Optional[str] = None
    password:   Optional[str] = None
    namespaces: list[str]     = []
