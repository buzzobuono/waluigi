import asyncio
from waluigi.worker.config.args import args

class SlotManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._active_tasks_count = 0
        self._max_slots = args.slots

    async def acquire_slot(self) -> bool:
        async with self._lock:
            if self._active_tasks_count >= self._max_slots:
                return False
            self._active_tasks_count += 1
            return True

    async def release_slot(self):
        async with self._lock:
            if self._active_tasks_count > 0:
                self._active_tasks_count -= 1

    async def get_available_slots(self) -> int:
        async with self._lock:
            return self._max_slots - self._active_tasks_count

slot_manager = SlotManager()
