from __future__ import annotations
import time
import logging

from waluigi.commons.dag import DAGSpec
from waluigi.boss.engine import BossEngine
from waluigi.boss.services.job_service import JobService

logger = logging.getLogger("waluigi")


def planner_loop(
    boss_id: str, tick: int, job_service: JobService, engine: BossEngine,
    hook_svc=None, job_def_svc=None, task_repo=None,
) -> None:
    """Background thread: claim and plan jobs until they complete or fail."""
    while True:
        try:
            runnable = job_service.list_runnable_ids()
            if not runnable:
                logger.info("🧠 No runnable jobs")
                time.sleep(tick)
                continue

            for namespace, job_id in runnable:
                job = job_service.claim(boss_id, namespace, job_id)
                if not job:
                    continue  # another Boss claimed it first

                logger.info(f"🧠 Claimed job: {namespace}/{job_id}")
                try:
                    spec   = DAGSpec(job["spec"])
                    result = engine.build(namespace=namespace, job_metadata=job["metadata"], spec=spec)

                    if result is True:
                        logger.info(f"🏁 Job completed: {namespace}/{job_id}")
                        job_service.update_status(namespace, job_id, "SUCCESS")
                        if hook_svc:
                            _fire_hooks(namespace, job_id, "success",
                                        hook_svc, job_service, job_def_svc, engine, task_repo)
                    elif result is None:
                        logger.error(f"💀 Job failed: {namespace}/{job_id}")
                        job_service.update_status(namespace, job_id, "FAILED")
                        if hook_svc:
                            _fire_hooks(namespace, job_id, "failure",
                                        hook_svc, job_service, job_def_svc, engine, task_repo)
                    # result is False or "PAUSE" → leave as RUNNING, release lock below

                except Exception as e:
                    logger.error(f"❌ Error planning {namespace}/{job_id}: {e}")
                finally:
                    job_service.release(namespace, job_id)

            time.sleep(tick)

        except Exception as e:
            logger.error(f"❌ Planner loop error: {e}")
            time.sleep(tick)


def _fire_hooks(namespace, job_id, event_type, hook_svc, job_svc, job_def_svc, engine, task_repo):
    try:
        from waluigi.boss.hook_scheduler import fire_hooks
        fire_hooks(namespace, job_id, event_type, hook_svc, job_svc, job_def_svc, engine, task_repo)
    except Exception as e:
        logger.error(f"❌ Hook fire error for {namespace}/{job_id}: {e}")
