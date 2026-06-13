import asyncio
import json
import logging
import os
import re

from waluigi.commons.http import AsyncHttpClient
from waluigi.worker.config.args import args
from waluigi.worker.components.slot_manager import SlotManager

logger = logging.getLogger("waluigi")

def _hash(nsdict):
    return " ".join(
        f"{k}:{v}"
        for k, v in sorted(nsdict.items())
    )

def _expand_config(obj, env: dict):
    """Recursively expand ${VAR} placeholders in config string values."""
    if isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: env.get(m.group(1), m.group(0)), obj)
    if isinstance(obj, dict):
        return {k: _expand_config(v, env) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_config(i, env) for i in obj]
    return obj

class WorkerService:

    def __init__(self, slot_manager: SlotManager):
        self.slot_manager = slot_manager
        self._boss = AsyncHttpClient(args.boss_url, timeout=5)

    async def run_command_async(self, command, id, job_id, namespace, params, attributes, config, resources, script=None, secrets=None):
        try:
            await self._update_boss(namespace, id, params, attributes, resources, "RUNNING")

            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            for k, v in params.items():
                env[f"WALUIGI_PARAM_{k.upper()}"] = str(v)
            for k, v in attributes.items():
                env[f"WALUIGI_ATTRIBUTE_{k.upper()}"] = str(v)
            env["WALUIGI_TASK_ID"] = id
            env["WALUIGI_JOB_ID"] = job_id
            env["WALUIGI_CATALOG_NAMESPACE"] = namespace
            for k, v in (secrets or {}).items():
                env[f"WALUIGI_SECRET_{k.upper()}"] = str(v)
            env["WALUIGI_CONFIG"] = json.dumps(_expand_config(config, env))
            if script:
                env["WALUIGI_SCRIPT"] = script
            logger.info(f"🚀 Forking: {'<inline script>' if script else command}")

            process = await asyncio.create_subprocess_shell(
                command,
                cwd=args.default_workdir,
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
                        await self._send_logs(namespace, id, log_buffer)
                        log_buffer = []

            if log_buffer:
                await self._send_logs(namespace, id, log_buffer)

            await process.wait()

            if process.returncode == 0:
                logger.info(f"Task {id} succesfully terminated.")
                await self._update_boss(namespace, id, params, attributes, resources, "SUCCESS")
            else:
                logger.error(f"Task {id} failed (Exit code: {process.returncode})")
                await self._update_boss(namespace, id, params, attributes, resources, "FAILED")

        except Exception as e:
            logger.error(f"Error: {e}")
            await self._update_boss(namespace, id, params, attributes, resources, "FAILED")
            raise
        finally:
            await self.slot_manager.release_slot()


    async def _post(self, endpoint, **kwargs):
        r = await self._boss.post(endpoint, **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r

    async def _patch(self, endpoint, **kwargs):
        r = await self._boss.patch(endpoint, **kwargs)
        if 500 <= r.status_code < 600:
            raise RuntimeError(f"[bossd] Server error {r.status_code} on {endpoint}")
        return r

    async def _send_logs(self, namespace: str, task_id: str, lines: list):
        try:
            await self._post(f"/namespaces/{namespace}/tasks/{task_id}/logs", json={
                "worker_id": args.id,
                "logs": lines
            })
        except Exception as e:
            logger.error(f"Error in sending log for {task_id}: {e}")

    async def _update_boss(self, namespace: str, id: str, params, attributes, resources, status: str):
        return await self._patch(f"/namespaces/{namespace}/tasks/{id}", json={
            "worker_url": f"http://{args.host}:{args.port}",
            "params": _hash(params),
            "attributes": _hash(attributes),
            "resources": resources,
            "status": status
        })
