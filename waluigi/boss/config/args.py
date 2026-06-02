import os
import socket
import uuid
import configargparse

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_BOSS_")
p.add("--id",           default=str(uuid.uuid4()),                                         help="Unique Boss ID")
p.add("--port",         type=int, default=8082)
p.add("--host",         default=socket.gethostname(),                                      help="Advertised hostname")
p.add("--bind-address", default="0.0.0.0")
p.add("--tick",         type=int, default=15,                                               help="Planner loop interval (seconds)")
p.add("--db-url",       default=f"sqlite:///{os.path.join(os.getcwd(), 'db/waluigi.db')}", help="SQLAlchemy DB URL")
args = p.parse_args()
