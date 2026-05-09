import os
import sys
from types import SimpleNamespace

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
        
    def __repr__(self):
        p_count = len(vars(self.params))
        a_count = len(vars(self.attributes))
        return f"<Context: {p_count} params, {a_count} attributes>"

context = _Context()
        