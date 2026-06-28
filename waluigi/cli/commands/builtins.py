import importlib.resources
import yaml

from waluigi.cli.commands.apply import _apply_one
from waluigi.cli.services.session import WaluigiSession


def apply_builtins(session: WaluigiSession, namespace: str) -> None:
    """Apply all built-in TaskDefinitions to the given namespace."""
    ref = importlib.resources.files("waluigi.tasks.data").joinpath(
        "builtin-task-definitions.yaml"
    )
    with importlib.resources.as_file(ref) as path:
        with open(path) as f:
            docs = [d for d in yaml.safe_load_all(f) if d is not None]

    for doc in docs:
        _apply_one(session, doc, namespace_override=namespace)
