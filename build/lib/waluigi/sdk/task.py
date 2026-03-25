import os
import sys
from types import SimpleNamespace

class Task:
    def __init__(self):
        self.params = SimpleNamespace(**{
            k.replace("WALUIGI_PARAM_", "").lower(): v 
            for k, v in os.environ.items() if k.startswith("WALUIGI_PARAM_")
        })
        print(self.params)
        self.attributes = SimpleNamespace(**{
            k.replace("WALUIGI_ATTRIBUTE_", "").lower(): v 
            for k, v in os.environ.items() if k.startswith("WALUIGI_ATTRIBUTE_")
        })

    def run(self):
        raise NotImplementedError()

    def start(self):
        try:
            self.run()
            sys.exit(0)
        except Exception as e:
            print(f"❌ [Task Error] {e}", file=sys.stderr)
            sys.exit(1)