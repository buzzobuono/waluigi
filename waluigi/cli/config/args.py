import os
import argparse

p = argparse.ArgumentParser(prog="wlctl", description="Waluigi CLI", add_help=False)
p.add_argument("--url", default=os.environ.get("WALUIGI_CTL_URL", "http://localhost:8080"),
               help="Console base URL (env: WALUIGI_CTL_URL, default: http://localhost:8080)")
