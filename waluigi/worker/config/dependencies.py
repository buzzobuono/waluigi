import logging
from fastapi import Depends

from waluigi.worker.config.args import args
from waluigi.worker.components.slot_manager import SlotManager, slot_manager
from waluigi.worker.services.worker_service import WorkerService

logger = logging.getLogger("waluigi")


def get_slot_manager() -> SlotManager:
    return slot_manager

def get_worker_service(slot_manager: SlotManager = Depends(get_slot_manager)) -> WorkerService:
    return WorkerService(slot_manager)
