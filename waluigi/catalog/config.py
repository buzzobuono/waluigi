import os
import configargparse
import socket

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_CATALOG_")
p.add("--port",         type=int, default=9000)
p.add("--host",         default=socket.gethostname())
p.add("--bind-address", default="0.0.0.0")
p.add("--db-url",       default=f"sqlite:///{os.path.join(os.getcwd(), 'db/catalog.db')}", help="SQLAlchemy database URL (e.g. sqlite:///./db/catalog.db or postgresql://user:pw@host/db)")
p.add("--data-path",    default=os.path.join(os.getcwd(), "data"))
p.add("--rules-path",   default=os.path.join(os.getcwd(), "rules"), help="Directory containing DQ rule YAML definitions")
args = p.parse_args()

os.makedirs(args.data_path, exist_ok=True)
if args.db_url.startswith("sqlite:///"):
    _sqlite_path = args.db_url[len("sqlite:///"):]
    os.makedirs(os.path.dirname(os.path.abspath(_sqlite_path)), exist_ok=True)
os.makedirs(args.rules_path, exist_ok=True)