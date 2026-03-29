import os
import socket
import configargparse
import uvicorn
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CONSOLE_')
p.add('--port', type=int, default=8080)
p.add('--host', default=socket.gethostname())
p.add('--bind-address', default='0.0.0.0')
p.add('--boss-url', default='http://localhost:8082')
p.add('--catalog-url', default='http://localhost:9000')

args = p.parse_args()

BOSS_URL = args.boss_url.rstrip('/')
CATALOG_URL = args.catalog_url.rstrip('/')

STATIC_DIR = os.path.join(os.getcwd(), "static")


def log(msg):
    print(f"[Console 🖥️] {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _boss_get(path):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{BOSS_URL}{path}")
        r.raise_for_status()
        return r.json()


async def _boss_post(path, json=None):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{BOSS_URL}{path}", json=json)
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# API proxy — /api/* → boss
# ---------------------------------------------------------------------------

@app.get('/api/jobs')
async def api_jobs():
    return JSONResponse(await _boss_get('/api/jobs'))

@app.get('/api/tasks')
async def api_tasks():
    return JSONResponse(await _boss_get('/api/tasks'))

@app.get('/api/workers')
async def api_workers():
    return JSONResponse(await _boss_get('/api/workers'))

@app.get('/api/resources')
async def api_resources():
    return JSONResponse(await _boss_get('/api/resources'))

@app.get('/api/logs/{task_id}')
async def api_logs(task_id: str, limit: int = 100):
    return JSONResponse(await _boss_get(f'/api/logs/{task_id}?limit={limit}'))

@app.post('/api/reset/task/{id}')
async def api_reset_task(id: str):
    return JSONResponse(await _boss_post(f'/api/reset/task/{id}'))

@app.post('/api/reset/namespace/{namespace}')
async def api_reset_namespace(namespace: str):
    return JSONResponse(await _boss_post(f'/api/reset/namespace/{namespace}'))

@app.post('/api/delete/task/{id}')
async def api_delete_task(id: str):
    return JSONResponse(await _boss_post(f'/api/delete/task/{id}'))

@app.post('/api/delete/namespace/{namespace}')
async def api_delete_namespace(namespace: str):
    return JSONResponse(await _boss_post(f'/api/delete/namespace/{namespace}'))


# ---------------------------------------------------------------------------
# Static assets (js, css) — must be before SPA fallback
# ---------------------------------------------------------------------------

app.mount("/js",  StaticFiles(directory=os.path.join(STATIC_DIR, "js")),  name="js")
app.mount("/css", StaticFiles(directory=os.path.join(STATIC_DIR, "css")), name="css")


# ---------------------------------------------------------------------------
# SPA fallback — all other paths return index.html so Vue Router works
# on direct access and refresh (e.g. localhost:8080/tasks)
# ---------------------------------------------------------------------------

@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log(f"Waluigi Console:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    Boss URL: {args.boss_url}")
    log(f"    Catalog URL: {args.catalog_url}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()
