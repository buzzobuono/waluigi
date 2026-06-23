import os
import json
from types import SimpleNamespace


class AttrDict(dict):
    """A dict that also supports attribute-style access.

    Returned by context.config for all nested config nodes so that both
    ``context.config.output.source_id`` and ``context.config.output["source_id"]``
    (and ``.get()``) work without any conversion helper.
    """
    __slots__ = ()

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError:
            raise AttributeError(key)


def _to_attrdict(obj):
    if isinstance(obj, dict):
        return AttrDict({k: _to_attrdict(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [_to_attrdict(i) for i in obj]
    return obj


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
        self.config = _to_attrdict(json.loads(raw))

    def __repr__(self):
        p_count = len(vars(self.params))
        a_count = len(vars(self.attributes))
        return f"<Context: {p_count} params, {a_count} attributes>"


context = _Context()
