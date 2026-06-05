import logging
import logging.config
import threading
import yaml
import uvicorn
from fastapi import FastAPI

from waluigi.boss.config.args import args
from waluigi.boss.config.dependencies import init_db, get_db
from waluigi.boss.engine import BossEngine
from waluigi.boss.planner import planner_loop
from waluigi.boss.cron_scheduler import cron_scheduler_loop
from waluigi.boss.services.job_service import JobService
from waluigi.boss.services.cron_job_service import CronJobService
from waluigi.boss.services.job_definition_service import JobDefinitionService

from waluigi.boss.api.routes.namespace_router         import router as namespace_router
from waluigi.boss.api.routes.task_router              import router as task_router
from waluigi.boss.api.routes.job_router               import router as job_router
from waluigi.boss.api.routes.worker_router            import router as worker_router
from waluigi.boss.api.routes.resource_router          import router as resource_router
from waluigi.boss.api.routes.task_definition_router   import router as task_definition_router
from waluigi.boss.api.routes.job_definition_router    import router as job_definition_router
from waluigi.boss.api.routes.cron_job_router          import router as cron_job_router

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
app.include_router(task_definition_router)
app.include_router(job_definition_router)
app.include_router(cron_job_router)


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

    engine      = BossEngine(db.tasks, db.workers, db.resources, db.task_definitions)
    job_svc     = JobService(db.jobs)
    cron_svc    = CronJobService(db.cron_jobs)
    job_def_svc = JobDefinitionService(db.job_definitions)

    threading.Thread(
        target=planner_loop,
        args=(args.id, args.tick, job_svc, engine),
        daemon=True,
        name="planner",
    ).start()

    threading.Thread(
        target=cron_scheduler_loop,
        args=(args.tick, cron_svc, job_svc, job_def_svc, engine),
        daemon=True,
        name="cron-scheduler",
    ).start()

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
