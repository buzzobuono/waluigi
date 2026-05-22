import logging
import logging.config
import threading
import yaml
import uvicorn
from fastapi import FastAPI

from waluigi.boss2.config.args import args
from waluigi.boss2.config.dependencies import init_db, get_db
from waluigi.boss2.engine import BossEngine
from waluigi.boss2.planner import planner_loop
from waluigi.boss2.services.job_service import JobService

from waluigi.boss2.api.routes.namespace_router   import router as namespace_router
from waluigi.boss2.api.routes.task_router     import router as task_router
from waluigi.boss2.api.routes.job_router      import router as job_router
from waluigi.boss2.api.routes.worker_router   import router as worker_router
from waluigi.boss2.api.routes.resource_router import router as resource_router
from waluigi.boss2.api.routes.log_router      import router as log_router

logger = logging.getLogger("waluigi")

app = FastAPI(
    title="Waluigi Boss",
    description="Control plane — DAG scheduling, task dispatch, resource management.",
    version="2.0.0",
)

app.include_router(namespace_router)
app.include_router(job_router)
app.include_router(task_router)
app.include_router(worker_router)
app.include_router(resource_router)
app.include_router(log_router)


def main():
    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logger.warning("logging.yaml not found — using basicConfig")

    logger.info("Waluigi Boss v2")
    logger.info(f"  ID      : {args.id}")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_url}")
    logger.info(f"  Tick    : {args.tick}s")

    init_db(args.db_url)
    db = get_db()

    engine  = BossEngine(db.tasks, db.workers, db.resources)
    job_svc = JobService(db.jobs)

    threading.Thread(
        target=planner_loop,
        args=(args.id, args.tick, job_svc, engine),
        daemon=True,
        name="planner",
    ).start()

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
