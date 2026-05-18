import asyncio
import json
import logging
import os
import httpx

from waluigi.worker.config import args
from waluigi.worker.slot_manager import SlotManager

logger = logging.getLogger("waluigi")


async def _post(endpoint, **kwargs):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{args.boss_url}{endpoint}", timeout=5, **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r

def _hash(nsdict):
    return " ".join(
        f"{k}:{v}"
        for k, v in sorted(nsdict.items())
    )
        
class WorkerService:

    def __init__(self, slot_manager: SlotManager):
        self.slot_manager = slot_manager
        
    async def run_command_async(self, command, id, job_id, namespace, params, attributes, config, resources, workdir, script=None):
        try:
            await self._update_boss(id, namespace, params, attributes, resources, "RUNNING")
    
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            for k, v in params.items():
                env[f"WALUIGI_PARAM_{k.upper()}"] = str(v)
            for k, v in attributes.items():
                env[f"WALUIGI_ATTRIBUTE_{k.upper()}"] = str(v)
            env["WALUIGI_TASK_ID"] = id
            env["WALUIGI_JOB_ID"] = job_id
            env["WALUIGI_CONFIG"] = json.dumps(config)
            if script:
                env["WALUIGI_SCRIPT"] = script
            os.makedirs(workdir, exist_ok=True)
            logger.info(f"🚀 Forking: {'<inline script>' if script else command}")
    
            process = await asyncio.create_subprocess_shell(
                command,
                cwd=workdir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env
            )
    
            log_buffer = []
            async for line in process.stdout:
                clean_line = line.decode().strip()
                if clean_line:
                    print(f"[{id}] {clean_line}", flush=True)
                    log_buffer.append(clean_line)
                    if len(log_buffer) >= 5:
                        await self._send_logs(id, log_buffer)
                        log_buffer = []
    
            if log_buffer:
                await self._send_logs(id, log_buffer)
    
            await process.wait()
    
            if process.returncode == 0:
                logger.info(f"Task {id} succesfully terminated.")
                await self._update_boss(id, namespace, params, attributes, resources, "SUCCESS")
            else:
                logger.error(f"Task {id} failed (Exit code: {process.returncode})")
                await self._update_boss(id, namespace, params, attributes, resources, "FAILED")
    
        except Exception as e:
            logger.error(f"Error: {e}")
            await self._update_boss(id, namespace, params, attributes, resources, "FAILED")
            raise
        finally:
            await self.slot_manager.release_slot()
        

    async def _send_logs(self, task_id, lines):
        try:
            await _post(f"/api/logs/{task_id}", json={
                "worker_id": args.id,
                "logs": lines
            })
        except Exception as e:
            logger.error(f"Error in sending log for {task_id}: {e}")
            
    async def _update_boss(self, id, namespace, params, attributes, resources, status):
        return await _post("/update", json={
            "worker_url": f"http://{args.host}:{args.port}",
            "id": id,
            "namespace": namespace,
            "params": _hash(params),
            "attributes": _hash(attributes),
            "resources": resources,
            "status": status
        })
