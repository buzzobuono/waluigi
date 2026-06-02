import os
import socket
import configargparse

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_CONSOLE_")
p.add("--port",           type=int, default=8080)
p.add("--host",           default=socket.gethostname())
p.add("--bind-address",   default="0.0.0.0")
p.add("--boss-url",       default="http://localhost:8082")
p.add("--catalog-url",    default="http://localhost:9000")
p.add("--secret-key",     default="change-me-in-production")
p.add("--admin-user",     default="admin")
p.add("--admin-password", default="admin")
p.add("--token-expire-h", type=int, default=8)
p.add("--db-url",         default=f"sqlite:///{os.path.join(os.getcwd(), 'db/console.db')}")
args = p.parse_args()
