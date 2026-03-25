# 🟣 Waluigi

A lightweight distributed task orchestrator inspired by [Luigi](https://github.com/spotify/luigi), built on a **server-push architecture**.

Instead of workers polling for work, the Boss schedules tasks and pushes them directly to workers via HTTP. Workers are passive executors — they register themselves, wait for orders, and report back.

---

## How it works

```
┌─────────┐        POST /execute        ┌────────────┐
│  Boss   │ ─────────────────────────►  │  Worker 1  │
│         │                             └────────────┘
│ Planner │        POST /execute        ┌────────────┐
│  Loop   │ ─────────────────────────►  │  Worker 2  │
│         │                             └────────────┘
│  SQLite │ ◄──── POST /update ──────── │  Worker N  │
└─────────┘   (RUNNING / SUCCESS / FAILED)
```

The **Boss** holds the DAG state in SQLite, plans task execution, and dispatches work to registered workers. Workers execute shell commands, stream logs back to the boss, and update task status on completion.

Key design choices:
- **Server push** over worker polling — the boss decides when and where to run each task
- **Resource management** — tasks declare resource requirements, the boss enforces cluster-wide limits
- **Affinity** — tasks declare capability requirements, workers declare what they offer
- **Parameter inheritance** — child tasks reference parent params with `${parent.params.x}` syntax
- **Idempotent execution** — tasks already in `SUCCESS` are never re-run, making job resubmission safe

---

## Installation

```bash
pip install waluigi
```

---

## Components

### Boss (`wlboss`)

The control plane. Manages the DAG state, schedules tasks, dispatches them to workers, and exposes a REST API and a web dashboard.

```bash
wlboss \
  --port 8082 \
  --host localhost \
  --db-path ./db/waluigi.db
```

| Option | Default | Description |
|---|---|---|
| `--port` | `8082` | Listening port |
| `--host` | hostname | Logical host used in URLs |
| `--bind-address` | `0.0.0.0` | Bind address |
| `--db-path` | `./db/waluigi.db` | SQLite database path |

All options can be set via environment variables with the `WALUIGI_BOSS_` prefix (e.g. `WALUIGI_BOSS_PORT=8082`).

---

### Worker (`wlworker`)

The execution plane. Registers with the boss, waits for task dispatches, forks subprocesses, and streams logs back.

```bash
wlworker \
  --boss-url http://localhost:8082 \
  --port 5001 \
  --slots 2
```

| Option | Default | Description |
|---|---|---|
| `--port` | `5001` | Listening port |
| `--boss-url` | `http://localhost:8082` | Boss URL |
| `--host` | hostname | Logical host used for registration |
| `--bind-address` | `0.0.0.0` | Bind address |
| `--slots` | `2` | Max concurrent tasks |
| `--heartbeat` | `10` | Heartbeat interval in seconds |
| `--default-workdir` | `./work` | Default working directory for tasks |

All options can be set via environment variables with the `WALUIGI_WORKER_` prefix.

You can run multiple workers on different machines or ports. Each worker registers itself with the boss and is matched to tasks based on available slots and affinity labels.

---

### CLI (`wlctl`)

Command-line interface to interact with the boss.

```bash
wlctl --url http://localhost:8082 <command>
```

#### Submit a job or apply resources

```bash
wlctl apply -f descriptor.yaml
```

#### Inspect the cluster

```bash
wlctl get workers
wlctl get jobs
wlctl get tasks
wlctl get tasks --namespace analytics
wlctl get namespaces
wlctl get resources
```

#### Read task logs

```bash
wlctl logs <task_id>
wlctl logs <task_id> --follow
wlctl logs <task_id> -n 50
```

#### Reset or delete

```bash
wlctl reset task <task_id>
wlctl reset namespace <namespace>
wlctl delete task <task_id>
wlctl delete namespace <namespace>
```

---

## Defining a DAG

Jobs are described in YAML. A job is a tree of tasks, each with its own command, params, resources, and dependencies declared under `requires`.

```yaml
kind: Job
metadata:
  workdir: "work"
spec:
  name: "GlobalReport"
  id: "final_report"
  namespace: "analytics"
  command: "python analytics/global_report.py"
  params:
    date: "2026-03-20"
  resources:
    coin: 1
  affinity:
    - python
  requires:
    - name: "CleanData"
      id: "clean_erp"
      namespace: "analytics"
      command: "python analytics/clean_data.py"
      params:
        source: "ERP"
        date: "${parent.params.date}"    # inherited from parent
      resources:
        coin: 1
      affinity:
        - python
      requires:
        - name: "RawDataExtract"
          id: "extract_erp"
          namespace: "analytics"
          command: "python analytics/raw_data_extract.py"
          params:
            source: "${parent.params.source}"
            date: "${parent.params.date}"
          resources:
            coin: 1
          affinity:
            - python
```

Parameter inheritance uses `${parent.params.x}` — the boss resolves these at planning time before dispatching.

Submit with:

```bash
wlctl apply -f job.yaml
```

---

## Writing a Task

Tasks are plain Python scripts. The SDK provides a base `Task` class that reads params and attributes injected by the worker as environment variables.

```python
from waluigi.sdk.task import Task

class RawDataExtract(Task):
    def run(self):
        print(f"Extracting from: {self.params.source}")
        # self.params.*      — from the 'params' field in the descriptor
        # self.attributes.*  — from the 'attributes' field in the descriptor

if __name__ == "__main__":
    RawDataExtract().start()
```

The worker runs the task as a subprocess and injects params via `WALUIGI_PARAM_*` and attributes via `WALUIGI_ATTRIBUTE_*` environment variables. Exit code `0` means success, anything else means failure.

Tasks are language-agnostic — any executable that reads environment variables and exits with the correct code works.

---

## Resource management

The boss enforces cluster-wide resource limits. Tasks declare their consumption in the descriptor, and the boss only dispatches a task when enough resources are available. Resources are acquired before dispatch and released on completion or failure.

Define cluster resources:

```yaml
kind: ClusterResources
spec:
  coin: 10
  pdc: 4
```

```bash
wlctl apply -f resources.yaml
```

Tasks declare consumption:

```yaml
resources:
  coin: 1
  pdc: 1
```

Resources are arbitrary named slots — you can model CPU shares, GPU units, API rate limits, or any other finite resource.

---

## Affinity *(coming soon)*

Workers declare the capabilities they offer via affinity labels sent in the heartbeat. The boss uses these labels to match tasks to suitable workers.

A worker running in a Python-enabled container would declare:

```
affinity: ["python", "pandas"]
```

A worker running on a GPU node would declare:

```
affinity: ["python", "gpu"]
```

Tasks declare the capabilities they require:

```yaml
affinity:
  - python
  - gpu
```

The boss dispatches the task only to workers whose affinity set is a superset of the task's requirements. If no matching worker is available, the task waits.

---

## Web Dashboard

The boss exposes a dashboard at `http://<boss-host>:<port>/` showing live task state, worker status, and resource usage. Tasks can be reset or deleted directly from the UI.

---

## Deployment

### Docker Compose

```bash
docker compose up
```

### Docker Swarm

Waluigi supports running multiple boss replicas on Swarm. The boss uses SQLite with WAL mode and atomic `UPDATE ... RETURNING` queries for job claiming, so concurrent replicas coordinate correctly without an external lock manager — each boss atomically claims a different job.

```bash
docker swarm init
docker stack deploy -c docker-compose.yml waluigi
```

The included `docker-compose.yml` runs 4 boss replicas and 16 worker replicas by default. The boss is exposed via Swarm's ingress load balancer — workers reach it at `http://boss:8082` and the ingress routes each request to an available replica.

### Kubernetes

A Kubernetes deployment follows the same model. Mount the SQLite database on a `ReadWriteMany` volume (e.g. NFS or a cloud file store) shared across boss pods, and expose the boss via a `ClusterIP` service so workers can reach it by name.

```yaml
# boss-deployment.yaml (sketch)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: waluigi-boss
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: boss
          image: buzzobuono/waluigi-bossd:latest
          env:
            - name: WALUIGI_BOSS_DB_PATH
              value: /db/waluigi.db
          volumeMounts:
            - name: db
              mountPath: /db
      volumes:
        - name: db
          persistentVolumeClaim:
            claimName: waluigi-db-pvc   # must be ReadWriteMany
```

> **Note:** SQLite on network file systems (NFS, EFS) can have locking issues under high write concurrency. For large-scale Kubernetes deployments, migrating the state backend to PostgreSQL is recommended.

---

## Differences from Luigi

| | Luigi | Waluigi |
|---|---|---|
| Architecture | In-process scheduler | Boss/Worker over HTTP |
| Task dispatch | Worker pulls | Boss pushes |
| Execution | Python functions | Shell commands (any language) |
| State | In-memory | SQLite |
| Scaling | Single process | Multiple workers on multiple machines |
| Resource limits | Yes | Yes |
| Worker affinity | No | Coming soon |
| Multi-boss | No | Yes (via atomic job claiming) |
