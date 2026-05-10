import os
import json
from types import SimpleNamespace

def _to_namespace(obj):
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in obj.items()})
    return obj  # lists of dicts stay as plain lists — directly usable by SDK calls

class _Context:
    def __init__(self):
        self.params = SimpleNamespace(**{
            k.replace("WALUIGI_PARAM_", "").lower(): v
            for k, v in os.environ.items() if k.startswith("WALUIGI_PARAM_")
        })
        self.attributes = SimpleNamespace(**{
            k.replace("WALUIGI_ATTRIBUTE_", "").lower(): v
            for k, v in os.environ.items() if k.startswith("WALUIGI_ATTRIBUTE_")
        })
        raw = os.environ.get("WALUIGI_CONFIG", "{}")
        self.config = _to_namespace(json.loads(raw))

    def __repr__(self):
        p_count = len(vars(self.params))
        a_count = len(vars(self.attributes))
        return f"<Context: {p_count} params, {a_count} attributes>"

context = _Context()
