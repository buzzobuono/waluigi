import os
import socket
import configargparse
import uvicorn
import logging
import httpx
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("waluigi")

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CONSOLE_')
p.add('--port', type=int, default=8080)
p.add('--host', default=socket.gethostname())
p.add('--bind-address', default='0.0.0.0')
p.add('--boss-url', default='http://localhost:8082')
p.add('--catalog-url', default='http://localhost:9000')

args = p.parse_args()

BOSS_URL    = args.boss_url.rstrip('/')
CATALOG_URL = args.catalog_url.rstrip('/')
STATIC_DIR  = os.path.join(os.getcwd(), "static")

@app.api_route("/boss/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_boss(request: Request, path: str):
    url = f"{BOSS_URL}/{path}"
    params = dict(request.query_params)
    content = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method,
            url=url,
            params=params,
            content=content,
            headers=headers
        )
    return JSONResponse(content=response.json(), status_code=response.status_code)
    
@app.api_route("/catalog/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_catalog(request: Request, path: str):
    url = f"{CATALOG_URL}/{path}"
    params = dict(request.query_params)
    content = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method,
            url=url,
            params=params,
            content=content,
            headers=headers
        )
    return JSONResponse(content=response.json(), status_code=response.status_code)
    
app.mount("/js",  StaticFiles(directory=os.path.join(STATIC_DIR, "js")),  name="js")
app.mount("/css", StaticFiles(directory=os.path.join(STATIC_DIR, "css")), name="css")

@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
    
def main():
    with open("logging.yaml") as f:
        logging.config.dictConfig(yaml.safe_load(f))

    logger.info("Waluigi Console")
    logger.info(f"    Binding: {args.bind_address}:{args.port}")
    logger.info(f"    Boss URL: {args.boss_url}")
    logger.info(f"    Catalog URL: {args.catalog_url}")
    
    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)
    

if __name__ == "__main__":
    main()
