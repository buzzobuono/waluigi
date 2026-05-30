import logging
import asyncio
import yaml
import uvicorn
from fastapi import FastAPI

from waluigi.commons.http import AsyncHttpClient
from waluigi.worker.config.args import args

from waluigi.worker.api.routes.worker_router import worker_router
from waluigi.worker.components.slot_manager import slot_manager

logger = logging.getLogger("waluigi")

app = FastAPI(
    title="Waluigi Worker",
    description="Worker service: manages task execution.",
    version="1.0.0",
)

app.include_router(worker_router)

async def heartbeat():
    async with AsyncHttpClient(args.boss_url, timeout=5) as client:
        while True:
            try:
                await client.post("/workers", json={
                    "url": f"http://{args.host}:{args.port}",
                    "status": "ALIVE",
                    "max_slots": args.slots,
                    "free_slots": await slot_manager.get_available_slots()
                })
                logger.info("Registrato con successo al Boss.")
            except Exception as e:
                logger.error("Boss non raggiungibile...")
                logger.error(e)
            await asyncio.sleep(args.heartbeat)
            
@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat())

def main():
    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logger.warning("File logging.yaml non trovato, uso configurazione base.")

    logger.info("Waluigi Worker")
    logger.info(f"  Worker Id       : {args.id}")
    logger.info(f"  Boss URL        : {args.boss_url}")
    logger.info(f"  Binding         : {args.bind_address}:{args.port}")
    logger.info(f"  URL             : http://{args.host}:{args.port}")
    logger.info(f"  Slots           : {args.slots}")
    logger.info(f"  Heartbeatb      : {args.heartbeat}")
    logger.info(f"  Default Work Dir: {args.default_workdir}")

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)
    
    
if __name__ == "__main__":
    main()
