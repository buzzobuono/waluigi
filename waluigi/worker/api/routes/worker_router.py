import asyncio
import threading
import logging
from fastapi import APIRouter, Request, Depends

from waluigi.commons.responses import ok, ko
from waluigi.worker.services.worker_service import WorkerService
from waluigi.worker.config.dependencies import get_slot_manager, get_worker_service
from waluigi.worker.api.schemas import ExecuteTaskRequest
from waluigi.worker.config.args import args
from waluigi.worker.components.slot_manager import SlotManager

logger = logging.getLogger("waluigi")

lock = asyncio.Lock()
active_tasks_count = 0


worker_router = APIRouter(
    prefix="",
    tags=["Worker"]
)


@worker_router.post("/namespaces/{namespace}/dispatch",
    summary="Dispatch a task for async execution in a namespace",
    description=(
        "Submits a new task for async execution. Returns 202 immediately; "
        "the worker reports status back to the Boss via PATCH callback. "
        "Returns 429 if no execution slot is available."
    )
)
async def dispatch(
    namespace: str,
    body: ExecuteTaskRequest,
    slot_manager: SlotManager = Depends(get_slot_manager),
    worker_service: WorkerService = Depends(get_worker_service)
):
    try:
        command    = body.command
        script     = body.script
        prepare    = body.prepare
        id         = body.id
        job_id     = body.job_id
        params     = body.params
        attributes = body.attributes
        config     = body.config
        resources  = body.resources
        secrets    = body.secrets
        
        if script:
            command = "python -c \"import os; exec(os.environ['WALUIGI_SCRIPT'])\""
        elif not command:
            msg = "No command or script provided"
            logger.error(msg)
            return ko(msg, 400)
            
        if not await slot_manager.acquire_slot():
            msg = "Worker too busy. No slot available."
            logger.info(msg)
            return ko(msg, 429)
        
        logger.info(f"Task recieved: {id}")
    
        try:
            asyncio.create_task(
                worker_service.run_command_async(command, id, job_id, namespace, params, attributes, config, resources, script, secrets, prepare)
            )
        except Exception as e:
            logger.error(f"Error: {e}")
            await slot_manager.release_slot()
            return ko("Task execution error", 500)    
            
        return ok(None, "Task submitted", 202)
    except ValueError as e:
        return ko(str(e), 400)

@worker_router.get("/slots",
    summary="Get available slot",
    description=(
        "Get available slot number"
    )
)
async def get_slots(slot_manager: SlotManager = Depends(get_slot_manager)):
    available = await slot_manager.get_available_slots()
    return ok({"available_slots": available})


@worker_router.delete("/prepare",
    summary="Clear prepare directory",
    description=(
        "Wipes the contents of the worker's prepare directory. "
        "Returns 422 if any tasks are currently running."
    )
)
async def clear_prepare(
    slot_manager: SlotManager = Depends(get_slot_manager),
    worker_service: WorkerService = Depends(get_worker_service),
):
    available = await slot_manager.get_available_slots()
    max_slots  = args.slots
    if available < max_slots:
        running = max_slots - available
        return ko(f"Worker is busy ({running} task(s) running — try again later)", 422)
    cleared = worker_service.clear_prepare_dir()
    return ok({"cleared_bytes": cleared})
