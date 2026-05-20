#!/bin/bash
# Deploy Waluigi stack with PostgreSQL-backed Catalog.
# Requires CATALOG_PG_PASSWORD to be set in the environment or in .env.pg

set -e

if [ -f .env.pg ]; then
    set -a
    source .env.pg
    set +a
fi

: "${CATALOG_PG_PASSWORD:?Please set CATALOG_PG_PASSWORD (or create .env.pg with CATALOG_PG_PASSWORD=...)}"

docker stack deploy -c docker-compose.pg.yml waluigi

docker service ls --filter "name=waluigi"
