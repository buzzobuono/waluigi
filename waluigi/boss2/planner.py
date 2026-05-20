from __future__ import annotations
import time
import logging

from waluigi.commons.dag import DAGTask
from waluigi.boss2.engine import BossEngine
from waluigi.boss2.services.job_service import JobService

logger = logging.getLogger("waluigi")


def planner_loop(boss_id: str, tick: int, job_service: JobService, engine: BossEngine) -> None:
    """Background thread: claim and plan jobs until they complete or fail."""
    while True:
        try:
            runnable = job_service.list_runnable_ids()
            if not runnable:
                logger.info("🧠 No runnable jobs")
                time.sleep(tick)
                continue

            for job_id in runnable:
                job = job_service.claim(boss_id, job_id)
                if not job:
                    continue  # another Boss claimed it first

                logger.info(f"🧠 Claimed job: {job_id}")
                try:
                    task = DAGTask(job["spec"])
                    result = engine.build(job_metadata=job["metadata"], task=task, parent_id=None)

                    if result is True:
                        logger.info(f"🏁 Job completed: {job_id}")
                        job_service.update_status(job_id, "SUCCESS")
                    elif result is None:
                        logger.error(f"💀 Job failed: {job_id}")
                        job_service.update_status(job_id, "FAILED")
                    # result is False or "PAUSE" → leave as RUNNING, release lock below

                except Exception as e:
                    logger.error(f"❌ Error planning {job_id}: {e}")
                finally:
                    job_service.release(job_id)

            time.sleep(tick)

        except Exception as e:
            logger.error(f"❌ Planner loop error: {e}")
            time.sleep(tick)
