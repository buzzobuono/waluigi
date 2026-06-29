import importlib.resources
import yaml

from waluigi.cli.commands.apply import _apply_one
from waluigi.cli.services.session import WaluigiSession

_CORE = "builtin-task-definitions.yaml"


def _filename(vendor: str | None) -> str:
    if vendor:
        return f"builtin-task-definitions-{vendor}.yaml"
    return _CORE


def apply_builtins(session: WaluigiSession, namespace: str,
                   vendor: str | None = None) -> None:
    filename = _filename(vendor)
    ref = importlib.resources.files("waluigi.tasks.data").joinpath(filename)
    try:
        with importlib.resources.as_file(ref) as path:
            with open(path) as f:
                docs = [d for d in yaml.safe_load_all(f) if d is not None]
    except FileNotFoundError:
        raise SystemExit(f"Error: no built-in definitions found for vendor '{vendor}' "
                         f"(expected file: {filename})")

    label = vendor or "core"
    print(f"Applying {len(docs)} built-in TaskDefinition(s) [{label}] → namespace '{namespace}'")
    for doc in docs:
        _apply_one(session, doc, namespace_override=namespace)
