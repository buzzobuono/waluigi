import os
import socket
import configargparse
import uvicorn
import logging
import httpx
import yaml
import hmac
import hashlib
import base64
import json
import time
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logger = logging.getLogger("waluigi")

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CONSOLE_')
p.add('--port',           type=int, default=8080)
p.add('--host',           default=socket.gethostname())
p.add('--bind-address',   default='0.0.0.0')
p.add('--boss-url',       default='http://localhost:8082')
p.add('--catalog-url',    default='http://localhost:9000')
p.add('--secret-key',     default='change-me-in-production')
p.add('--admin-user',     default='admin')
p.add('--admin-password', default='admin')
p.add('--token-expire-h', type=int, default=8)

args = p.parse_args()

BOSS_URL    = args.boss_url.rstrip('/')
CATALOG_URL = args.catalog_url.rstrip('/')
STATIC_DIR  = os.path.join(os.getcwd(), "static")
SECRET_KEY  = args.secret_key


# ── Minimal HS256 JWT (stdlib only) ──────────────────────────────────────────

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def _b64url_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))

def _sign(msg: str, secret: str) -> str:
    return _b64url(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest())

def jwt_encode(payload: dict, secret: str) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body   = _b64url(json.dumps(payload).encode())
    return f"{header}.{body}.{_sign(f'{header}.{body}', secret)}"

def jwt_decode(token: str, secret: str) -> dict:
    parts = token.split('.')
    if len(parts) != 3:
        raise ValueError("malformed token")
    header, body, sig = parts
    expected = _sign(f'{header}.{body}', secret)
    if not hmac.compare_digest(sig, expected):
        raise ValueError("invalid signature")
    payload = json.loads(_b64url_dec(body))
    if payload.get('exp', float('inf')) < time.time():
        raise ValueError("token expired")
    return payload


# ── Auth ─────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


@app.middleware("http")
async def require_auth(request: Request, call_next):
    if request.url.path.startswith(("/boss/", "/catalog/")):
        auth  = request.headers.get("authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        try:
            jwt_decode(token, SECRET_KEY)
        except Exception:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


@app.post("/auth/login")
async def login(body: LoginRequest):
    if body.username != args.admin_user or body.password != args.admin_password:
        return JSONResponse({"detail": "Invalid credentials"}, status_code=401)
    exp   = int((datetime.now(timezone.utc) + timedelta(hours=args.token_expire_h)).timestamp())
    token = jwt_encode({"sub": body.username, "exp": exp}, SECRET_KEY)
    return {"token": token, "username": body.username}


# ── Proxy ─────────────────────────────────────────────────────────────────────

@app.api_route("/boss/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_boss(request: Request, path: str):
    url     = f"{BOSS_URL}/{path}"
    params  = dict(request.query_params)
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method, url=url, params=params,
            content=content, headers=headers,
        )
    return JSONResponse(
        content=response.json() if response.content else None,
        status_code=response.status_code,
    )


@app.api_route("/catalog/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_catalog(request: Request, path: str):
    url     = f"{CATALOG_URL}/{path}"
    params  = dict(request.query_params)
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method, url=url, params=params,
            content=content, headers=headers,
        )
    return JSONResponse(
        content=response.json() if response.content else None,
        status_code=response.status_code,
    )


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
