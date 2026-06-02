from dataclasses import dataclass


@dataclass
class User:
    userid:     str
    username:   str
    namespaces: list[str] | str  # "*" for admin, list[str] for regular users
    createdate: str
    updatedate: str
