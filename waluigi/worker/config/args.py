import os
import configargparse
import socket
import uuid

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_WORKER_')

p.add('--id', default=str(uuid.uuid4()), help='Unique ID')
p.add('--port', type=int, default=5001)
p.add('--host', default=socket.gethostname(), help='Hostname')
p.add('--bind-address', default='0.0.0.0', help='Binding IP')
p.add('--boss-url', default='http://localhost:8082')
p.add('--slots', type=int, default=2)
p.add('--heartbeat', type=int, default=10)
p.add('--default-workdir', default=os.path.join(os.getcwd(), "work"), help='Default working directory')

args = p.parse_args()

os.makedirs(args.default_workdir, exist_ok=True)
