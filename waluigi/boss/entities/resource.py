from dataclasses import dataclass


@dataclass
class ResourceEntity:
    name: str
    amount: float
    usage: float
