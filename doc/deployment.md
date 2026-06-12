# Deployment

## Local development

Start all components manually:

```bash
# Boss (control plane)
wlboss --port 8082 --db-url sqlite:///./db/waluigi.db

# Worker (execution)
wlworker --boss-url http://localhost:8082 --port 5001 --slots 4

# Catalog (dataset management)
wlcatalog --port 9000 --db-url sqlite:///./db/catalog.db --data-path ./data

# Console (web UI + auth)
wlconsole --port 8080 \
  --boss-url http://localhost:8082 \
  --catalog-url http://localhost:9000 \
  --secret-key change-me-in-production
```

---

## Environment variables

All CLI options are available as environment variables with component prefixes.

### Boss (`WALUIGI_BOSS_*`)

| Variable | CLI option | Default | Description |
|----------|-----------|---------|-------------|
| `WALUIGI_BOSS_PORT` | `--port` | `8082` | Listening port |
| `WALUIGI_BOSS_HOST` | `--host` | hostname | Logical hostname |
| `WALUIGI_BOSS_BIND_ADDRESS` | `--bind-address` | `0.0.0.0` | Bind address |
| `WALUIGI_BOSS_DB_URL` | `--db-url` | `sqlite:///./db/waluigi.db` | SQLAlchemy DB URL |
| `WALUIGI_BOSS_TICK` | `--tick` | `15` | Planner loop interval (seconds) |

### Worker (`WALUIGI_WORKER_*`)

| Variable | CLI option | Default | Description |
|----------|-----------|---------|-------------|
| `WALUIGI_WORKER_PORT` | `--port` | `5001` | Listening port |
| `WALUIGI_WORKER_HOST` | `--host` | hostname | Logical hostname (used for registration) |
| `WALUIGI_WORKER_BIND_ADDRESS` | `--bind-address` | `0.0.0.0` | Bind address |
| `WALUIGI_WORKER_BOSS_URL` | `--boss-url` | `http://localhost:8082` | Boss URL |
| `WALUIGI_WORKER_SLOTS` | `--slots` | `2` | Max concurrent tasks |
| `WALUIGI_WORKER_HEARTBEAT` | `--heartbeat` | `10` | Heartbeat interval (seconds) |
| `WALUIGI_WORKER_DEFAULT_WORKDIR` | `--default-workdir` | `./work` | Working directory for subprocesses |
| `WALUIGI_WORKER_AFFINITY` | `--affinity` | `` | Comma-separated capability tags |

**Affinity example:**

```bash
WALUIGI_WORKER_AFFINITY=python,pandas,gpu
```

### Catalog (`WALUIGI_CATALOG_*`)

| Variable | CLI option | Default | Description |
|----------|-----------|---------|-------------|
| `WALUIGI_CATALOG_PORT` | `--port` | `9000` | Listening port |
| `WALUIGI_CATALOG_HOST` | `--host` | hostname | Logical hostname |
| `WALUIGI_CATALOG_BIND_ADDRESS` | `--bind-address` | `0.0.0.0` | Bind address |
| `WALUIGI_CATALOG_DB_URL` | `--db-url` | `sqlite:///./db/catalog.db` | SQLite DB path or SQLAlchemy URL |
| `WALUIGI_CATALOG_DATA_PATH` | `--data-path` | `./data` | Root directory for LOCAL datasets |
| `WALUIGI_CATALOG_RULES_PATH` | `--rules-path` | `./rules` | Directory for DQ rule YAML files |

### Console (`WALUIGI_CONSOLE_*`)

| Variable | CLI option | Default | Description |
|----------|-----------|---------|-------------|
| `WALUIGI_CONSOLE_PORT` | `--port` | `8080` | Listening port |
| `WALUIGI_CONSOLE_HOST` | `--host` | hostname | Logical hostname |
| `WALUIGI_CONSOLE_BIND_ADDRESS` | `--bind-address` | `0.0.0.0` | Bind address |
| `WALUIGI_CONSOLE_BOSS_URL` | `--boss-url` | `http://localhost:8082` | Boss URL to proxy |
| `WALUIGI_CONSOLE_CATALOG_URL` | `--catalog-url` | `http://localhost:9000` | Catalog URL to proxy |
| `WALUIGI_CONSOLE_SECRET_KEY` | `--secret-key` | `change-me-in-production` | JWT signing key (**change in production**) |
| `WALUIGI_CONSOLE_ADMIN_USER` | `--admin-user` | `admin` | Bootstrap admin username |
| `WALUIGI_CONSOLE_ADMIN_PASSWORD` | `--admin-password` | `admin` | Bootstrap admin password (**change in production**) |
| `WALUIGI_CONSOLE_TOKEN_EXPIRE_H` | `--token-expire-h` | `8` | JWT token TTL in hours |
| `WALUIGI_CONSOLE_DB_URL` | `--db-url` | `sqlite:///./db/console.db` | SQLAlchemy DB URL for user store |

### Task scripts (injected by Worker)

These are set automatically by the Worker before forking each task subprocess:

| Variable | Description |
|----------|-------------|
| `WALUIGI_PARAM_<KEY>` | Task params (uppercased) |
| `WALUIGI_ATTRIBUTE_<KEY>` | Task attributes (uppercased) |
| `WALUIGI_TASK_ID` | Task ID |
| `WALUIGI_JOB_ID` | Job ID |
| `WALUIGI_CONFIG` | Task config (JSON string) |
| `WALUIGI_CATALOG_URL` | Catalog URL (set on Worker via `WALUIGI_CATALOG_URL` env) |
| `WALUIGI_CATALOG_NAMESPACE` | Namespace of the running job |
| `PYTHONUNBUFFERED` | Always `1` |

---

## Docker Compose

The included `docker-compose.yml` runs all four components with shared volumes:

```bash
docker compose up
docker compose up --scale worker=5    # scale workers
```

**Volumes:**

| Volume | Mounted at | Used by |
|--------|-----------|---------|
| `./db` | `/db` | Boss (waluigi.db), Catalog (catalog.db) |
| `./data` | `/data` | Catalog (dataset files) |
| `./work` | `/work` | Worker (subprocess working directory) |
| `./rules` | `/rules` | Catalog (DQ rules), Worker tasks |
| `./descriptors` | `/descriptors` | (optional, for applying via CLI) |

**Build images:**

```bash
./create-docker-images.sh 1.0.0
```

Builds: `buzzobuono/waluigi-bossd`, `buzzobuono/waluigi-worker`, `buzzobuono/waluigi-catalog`, `buzzobuono/waluigi-console`.

---

## Docker Swarm

Multiple Boss replicas are safe. Each replica atomically claims a different job via SQLite `UPDATE ... RETURNING`, so no duplicate planning occurs.

```bash
docker swarm init
docker stack deploy -c docker-compose.yml waluigi

# Scale workers
docker service scale waluigi_worker=10
```

Workers reach the Boss via Swarm's ingress load balancer. The SQLite file must be on a shared volume accessible to all Boss replicas (NFS mount or Swarm volume plugin).

**Clean up:**

```bash
./clean-from-swarm.sh
```

---

## Kubernetes

### Boss Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: waluigi-boss
  namespace: waluigi
spec:
  replicas: 2
  selector:
    matchLabels:
      app: waluigi-boss
  template:
    metadata:
      labels:
        app: waluigi-boss
    spec:
      containers:
        - name: boss
          image: buzzobuono/waluigi-bossd:latest
          ports:
            - containerPort: 8082
          env:
            - name: WALUIGI_BOSS_DB_URL
              value: sqlite:////db/waluigi.db
            - name: WALUIGI_BOSS_HOST
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          volumeMounts:
            - name: db
              mountPath: /db
      volumes:
        - name: db
          persistentVolumeClaim:
            claimName: waluigi-db-pvc    # must be ReadWriteMany
---
apiVersion: v1
kind: Service
metadata:
  name: waluigi-boss
  namespace: waluigi
spec:
  selector:
    app: waluigi-boss
  ports:
    - port: 8082
      targetPort: 8082
```

### Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: waluigi-worker
  namespace: waluigi
spec:
  replicas: 4
  selector:
    matchLabels:
      app: waluigi-worker
  template:
    metadata:
      labels:
        app: waluigi-worker
    spec:
      containers:
        - name: worker
          image: buzzobuono/waluigi-worker:latest
          env:
            - name: WALUIGI_WORKER_BOSS_URL
              value: http://waluigi-boss:8082
            - name: WALUIGI_WORKER_SLOTS
              value: "4"
            - name: WALUIGI_WORKER_AFFINITY
              value: "python,pandas"
            - name: WALUIGI_CATALOG_URL
              value: http://waluigi-catalog:9000
          volumeMounts:
            - name: data
              mountPath: /data
            - name: work
              mountPath: /work
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: waluigi-data-pvc
        - name: work
          emptyDir: {}
```

### Catalog Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: waluigi-catalog
  namespace: waluigi
spec:
  replicas: 1
  selector:
    matchLabels:
      app: waluigi-catalog
  template:
    metadata:
      labels:
        app: waluigi-catalog
    spec:
      containers:
        - name: catalog
          image: buzzobuono/waluigi-catalog:latest
          ports:
            - containerPort: 9000
          env:
            - name: WALUIGI_CATALOG_DB_URL
              value: sqlite:////db/catalog.db
            - name: WALUIGI_CATALOG_DATA_PATH
              value: /data
            - name: WALUIGI_CATALOG_RULES_PATH
              value: /rules
          volumeMounts:
            - name: db
              mountPath: /db
            - name: data
              mountPath: /data
            - name: rules
              mountPath: /rules
      volumes:
        - name: db
          persistentVolumeClaim:
            claimName: waluigi-catalog-db-pvc
        - name: data
          persistentVolumeClaim:
            claimName: waluigi-data-pvc
        - name: rules
          configMap:
            name: waluigi-dq-rules
```

> **Note:** The Catalog is a single-writer service. Run exactly 1 replica unless you migrate to a shared database backend.

> **SQLite on NFS:** SQLite's WAL mode works correctly on local and NFS mounts for Boss multi-replica, but under very high write concurrency NFS locking can cause issues. For large-scale Kubernetes deployments, migrating to PostgreSQL (Boss) or a dedicated database (Catalog) is recommended.

---

## Logging configuration

All components load `logging.yaml` from the working directory if it exists, falling back to `basicConfig(level=INFO)`.

**Example `logging.yaml`:**

```yaml
version: 1
disable_existing_loggers: false
formatters:
  standard:
    format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
handlers:
  console:
    class: logging.StreamHandler
    formatter: standard
    stream: ext://sys.stdout
  file:
    class: logging.handlers.RotatingFileHandler
    formatter: standard
    filename: waluigi.log
    maxBytes: 10485760
    backupCount: 3
loggers:
  waluigi:
    level: INFO
    handlers: [console, file]
    propagate: false
root:
  level: WARNING
  handlers: [console]
```
