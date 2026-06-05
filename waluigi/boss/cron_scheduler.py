from __future__ import annotations
import time
import logging
from datetime import datetime, timezone

from waluigi.commons.dag import DAGTask, parse_definition

logger = logging.getLogger("waluigi")


def cron_scheduler_loop(tick, cron_svc, job_svc, job_def_svc, engine) -> None:
    """Background thread: fire CronJobs whose next scheduled time has passed."""
    while True:
        try:
            _fire_due(cron_svc, job_svc, job_def_svc, engine)
        except Exception as e:
            logger.error(f"cron scheduler error: {e}")
        time.sleep(tick)


def _fire_due(cron_svc, job_svc, job_def_svc, engine) -> None:
    now = datetime.now(timezone.utc)
    for cj in cron_svc.list_enabled():
        try:
            _maybe_fire(cj, now, cron_svc, job_svc, job_def_svc, engine)
        except Exception as e:
            logger.warning(f"CronJob {cj['namespace']}/{cj['id']}: {e}")


def _maybe_fire(cj, now, cron_svc, job_svc, job_def_svc, engine) -> None:
    from croniter import croniter
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        def _tz(s):
            try:    return ZoneInfo(s)
            except (ZoneInfoNotFoundError, Exception): return timezone.utc
    except ImportError:
        def _tz(_): return timezone.utc

    namespace   = cj["namespace"]
    cron_id     = cj["id"]
    spec        = cj["spec"]
    schedule    = spec.get("schedule", "0 0 * * *")
    tz          = _tz(spec.get("timezone", "UTC"))
    job_kind    = spec.get("jobKind", "Job")
    job_ref_name = (spec.get("jobRef") or {}).get("name")
    inject      = spec.get("inject") or []
    concurrency = spec.get("concurrencyPolicy", "Forbid")

    last_fire_str = cj.get("last_fire")
    if last_fire_str is None:
        # First registration: start the clock, wait for next schedule.
        cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
        return

    last_fire = datetime.fromisoformat(last_fire_str)
    if last_fire.tzinfo is None:
        last_fire = last_fire.replace(tzinfo=timezone.utc)

    cron = croniter(schedule, last_fire.astimezone(tz))
    next_fire = cron.get_next(datetime)
    if next_fire.tzinfo is None:
        next_fire = next_fire.replace(tzinfo=tz)
    if next_fire.astimezone(timezone.utc) > now:
        return

    if not job_ref_name:
        logger.warning(f"CronJob {cron_id}: missing jobRef.name — skipping")
        cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
        return

    job_def = job_def_svc.get(namespace, job_ref_name)
    if job_def is None:
        logger.warning(f"CronJob {cron_id}: JobDefinition '{job_ref_name}' not found — skipping")
        cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
        return

    tasks_list = list(job_def["spec"].get("tasks", []))
    def_meta   = job_def.get("metadata") or {}
    metadata   = {**def_meta, "namespace": namespace}

    # Build injected params / attributes from fire time
    params     = {}
    attributes = {}
    fire_local = next_fire.astimezone(tz)
    for inj in inject:
        val = fire_local.strftime(inj.get("format", "%Y-%m-%d"))
        if inj.get("as") == "attribute":
            attributes[inj["name"]] = val
        else:
            params[inj["name"]] = val

    timestamp = None
    if job_kind == "Job":
        timestamp = time.time()
        params["timestamp"] = timestamp
        suffixed   = {t["id"]: f"{t['id']}@{timestamp}" for t in tasks_list if "id" in t}
        tasks_list = [
            {
                **dict(t),
                "id":       suffixed[t["id"]],
                "requires": [suffixed.get(r, r) for r in t.get("requires", [])],
            }
            for t in tasks_list
        ]

    base_name = def_meta.get("name", job_ref_name)
    job_id    = f"{base_name}@{timestamp}" if timestamp else base_name

    # Concurrency policy (only matters for StatefulJob — same job_id across runs)
    if job_kind == "StatefulJob":
        status = job_svc.get_status(namespace, job_id)
        if status and status not in ("SUCCESS", "FAILED", "CANCELLED"):
            if concurrency == "Forbid":
                logger.info(f"CronJob {cron_id}: {job_id} still active — skipping (Forbid)")
                cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
                return
            elif concurrency == "Replace":
                logger.info(f"CronJob {cron_id}: cancelling {job_id} (Replace)")
                job_svc.cancel(namespace, job_id)

    resolved = {
        "kind":     job_kind,
        "metadata": {**metadata, "namespace": namespace, "timestamp": timestamp},
        "spec": {
            "tasks":      tasks_list,
            "params":     params,
            "attributes": attributes,
        },
    }

    try:
        parsed_spec = parse_definition(resolved)
    except ValueError as e:
        logger.error(f"CronJob {cron_id}: parse error: {e}")
        cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
        return

    dag_task = DAGTask(parsed_spec)
    job_svc.create(
        namespace=namespace, job_id=job_id, kind=job_kind,
        metadata=metadata, spec=parsed_spec,
    )
    engine.register_job(namespace, job_id, dag_task, None)
    cron_svc.set_last_fire(namespace, cron_id, now.isoformat())
    logger.info(f"🕐 CronJob {cron_id}: fired {namespace}/{job_id}")
