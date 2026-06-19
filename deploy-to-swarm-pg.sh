#!/bin/bash

set -a && source .env && set +a

docker stack deploy -c docker-compose.pg.yml waluigi

echo "Waiting for workers to register..."
sleep 20
source env/bin/activate && wlctl --url http://127.0.0.1:8080 prune workers

