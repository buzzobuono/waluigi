import os
import logging
import logging.config
import yaml
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from waluigi.console.config.args import args
from waluigi.console.config.dependencies import init_db
from waluigi.console.services.auth_service import AuthService
from waluigi.console.api.routes.auth_router  import router as auth_router
from waluigi.console.api.routes.proxy_router import router as proxy_router

logger = logging.getLogger("waluigi")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(
    title="Waluigi Console",
    description="Web console — JWT auth, user management, proxy to Boss and Catalog.",
    version="2.0.0",
)

_auth = AuthService(args.secret_key, args.token_expire_h)

PUBLIC_PATHS       = {"/auth/login"}
PROTECTED_PREFIXES = ("/boss/", "/catalog/", "/auth/")


@app.middleware("http")
async def require_auth(request: Request, call_next):
    path = request.url.path
    if path not in PUBLIC_PATHS and any(path.startswith(p) for p in PROTECTED_PREFIXES):
        token = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        try:
            request.state.user = _auth.decode_token(token)
        except Exception:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


app.include_router(auth_router)
app.include_router(proxy_router)

app.mount("/js",     StaticFiles(directory=os.path.join(STATIC_DIR, "js")),     name="js")
app.mount("/vendor", StaticFiles(directory=os.path.join(STATIC_DIR, "vendor")), name="vendor")


@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


def main():
    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logger.warning("logging.yaml not found — using basicConfig")

    logger.info("Waluigi Console")
    logger.info(f"  Binding     : {args.bind_address}:{args.port}")
    logger.info(f"  Boss URL    : {args.boss_url}")
    logger.info(f"  Catalog URL : {args.catalog_url}")
    logger.info(f"  DB          : {args.db_url}")

    init_db(args.db_url)

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
