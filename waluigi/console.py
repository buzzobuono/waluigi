import os
import sqlite3
import socket
import configargparse
import uvicorn
import logging
import logging.config
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
from typing import Optional

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
p.add('--db-path',        default='db/console.db')

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


# ── Password hashing (PBKDF2, stdlib only) ───────────────────────────────────

def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk   = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100_000)
    return salt.hex() + ':' + dk.hex()

def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, dk_hex = stored.split(':')
        salt = bytes.fromhex(salt_hex)
        dk   = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100_000)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


# ── SQLite users DB ───────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    return conn

def _init_db():
    os.makedirs(os.path.dirname(args.db_path) or '.', exist_ok=True)
    with _get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                userid        TEXT PRIMARY KEY,
                username      TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                namespaces    TEXT NOT NULL DEFAULT '[]',
                createdate    TEXT NOT NULL,
                updatedate    TEXT NOT NULL
            )
        """)
        # migrate existing DBs that lack the namespaces column
        try:
            conn.execute("ALTER TABLE users ADD COLUMN namespaces TEXT NOT NULL DEFAULT '[]'")
        except sqlite3.OperationalError:
            pass  # column already exists

def _row(r) -> dict | None:
    return dict(r) if r else None

def _safe_user(r: dict) -> dict:
    out = {k: v for k, v in r.items() if k != 'password_hash'}
    # parse namespaces from JSON string stored in DB
    try:
        out['namespaces'] = json.loads(out.get('namespaces', '[]'))
    except Exception:
        out['namespaces'] = []
    return out


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _current_user(request: Request) -> dict | None:
    return getattr(request.state, 'user', None)

def _is_admin(request: Request) -> bool:
    u = _current_user(request)
    return u is not None and u.get('namespaces') == "*"

def _can_access_namespace(request: Request, namespace: str) -> bool:
    u = _current_user(request)
    if u is None:
        return False
    ns = u.get('namespaces', [])
    return ns == "*" or namespace in ns


# ── Middleware ────────────────────────────────────────────────────────────────

PUBLIC_PATHS       = {"/auth/login"}
PROTECTED_PREFIXES = ("/boss/", "/catalog/", "/auth/")

@app.middleware("http")
async def require_auth(request: Request, call_next):
    path = request.url.path
    if path not in PUBLIC_PATHS and any(path.startswith(p) for p in PROTECTED_PREFIXES):
        auth  = request.headers.get("authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        try:
            request.state.user = jwt_decode(token, SECRET_KEY)
        except Exception:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ── Auth endpoints ────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateRequest(BaseModel):
    userid:     str
    username:   str
    password:   str
    namespaces: list[str] = []

class UserUpdateRequest(BaseModel):
    username:   Optional[str] = None
    password:   Optional[str] = None
    namespaces: Optional[list[str]] = None


@app.post("/auth/login")
async def login(body: LoginRequest):
    is_admin = (body.username == args.admin_user and body.password == args.admin_password)

    if is_admin:
        namespaces = "*"
    else:
        with _get_db() as conn:
            row = _row(conn.execute(
                "SELECT * FROM users WHERE userid = ?", (body.username,)
            ).fetchone())
        if not row or not verify_password(body.password, row['password_hash']):
            return JSONResponse({"detail": "Invalid credentials"}, status_code=401)
        try:
            namespaces = json.loads(row['namespaces'])
        except Exception:
            namespaces = []

    exp   = int((datetime.now(timezone.utc) + timedelta(hours=args.token_expire_h)).timestamp())
    token = jwt_encode({"sub": body.username, "exp": exp, "namespaces": namespaces}, SECRET_KEY)
    return {"token": token, "username": body.username, "namespaces": namespaces}


@app.get("/auth/users")
async def list_users(request: Request):
    if not _is_admin(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    with _get_db() as conn:
        rows = [_safe_user(dict(r)) for r in conn.execute(
            "SELECT * FROM users ORDER BY createdate DESC"
        ).fetchall()]
    return {"data": rows}


@app.post("/auth/users")
async def create_user(request: Request, body: UserCreateRequest):
    if not _is_admin(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    if not body.userid.strip() or not body.password.strip():
        return JSONResponse({"detail": "userid and password are required"}, status_code=400)
    if body.userid == args.admin_user:
        return JSONResponse({"detail": "Cannot create a user with the admin userid"}, status_code=409)
    now = _now()
    try:
        with _get_db() as conn:
            conn.execute(
                "INSERT INTO users (userid, username, password_hash, namespaces, createdate, updatedate) VALUES (?,?,?,?,?,?)",
                (body.userid.strip(), body.username.strip() or body.userid.strip(),
                 hash_password(body.password), json.dumps(body.namespaces), now, now)
            )
    except sqlite3.IntegrityError:
        return JSONResponse({"detail": "User already exists"}, status_code=409)
    return {"data": {"userid": body.userid, "username": body.username,
                     "namespaces": body.namespaces, "createdate": now}}


@app.patch("/auth/users/{userid}")
async def update_user(request: Request, userid: str, body: UserUpdateRequest):
    if not _is_admin(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    if userid == args.admin_user:
        return JSONResponse({"detail": "Cannot modify the admin user"}, status_code=409)
    updates, params = [], []
    if body.username is not None:
        updates.append("username = ?");   params.append(body.username.strip())
    if body.password is not None:
        updates.append("password_hash = ?"); params.append(hash_password(body.password))
    if body.namespaces is not None:
        updates.append("namespaces = ?"); params.append(json.dumps(body.namespaces))
    if not updates:
        return JSONResponse({"detail": "Nothing to update"}, status_code=400)
    updates.append("updatedate = ?"); params.append(_now())
    params.append(userid)
    with _get_db() as conn:
        cur = conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE userid = ?", params)
    if cur.rowcount == 0:
        return JSONResponse({"detail": "User not found"}, status_code=404)
    return {"data": {"userid": userid, "updated": [f for f in ("username", "password", "namespaces")
                                                    if getattr(body, f if f != "password" else "password") is not None]}}


@app.delete("/auth/users/{userid}")
async def delete_user(request: Request, userid: str):
    if not _is_admin(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    if userid == args.admin_user:
        return JSONResponse({"detail": "Cannot delete the admin user"}, status_code=409)
    with _get_db() as conn:
        cur = conn.execute("DELETE FROM users WHERE userid = ?", (userid,))
    if cur.rowcount == 0:
        return JSONResponse({"detail": "User not found"}, status_code=404)
    return {"data": {"deleted": userid}}


def _parse_json(response):
    if not response.content:
        return None
    try:
        return response.json()
    except Exception:
        return None


# ── Proxy ─────────────────────────────────────────────────────────────────────

def _check_boss_namespace(request: Request, path: str) -> JSONResponse | None:
    """
    Returns a 403 JSONResponse if the requesting user is not allowed to access
    the namespace embedded in the boss path, or None if access is granted.

    Boss paths that carry a namespace look like:
        namespaces/{ns}/...
    Paths without a namespace segment (workers, resources) are always allowed.
    """
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "namespaces" and parts[1]:
        namespace = parts[1]
        # bare /namespaces list — allowed for all authenticated users (filtered later)
        if namespace in ("", "_reset"):
            return None
        if not _can_access_namespace(request, namespace):
            return JSONResponse(
                {"detail": f"Access to namespace '{namespace}' is not allowed"},
                status_code=403,
            )
    return None


def _filter_namespaces_response(request: Request, body: dict) -> dict:
    """For non-admin users listing /namespaces, keep only their own namespaces."""
    if _is_admin(request):
        return body
    user_ns = _current_user(request).get("namespaces", [])
    data = body.get("data", [])
    body["data"] = [r for r in data if r.get("namespace") in user_ns]
    return body


@app.api_route("/boss/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_boss(request: Request, path: str):
    denied = _check_boss_namespace(request, path)
    if denied:
        return denied

    qs      = request.url.query
    url     = f"{BOSS_URL}/{path}" + (f"?{qs}" if qs else "")
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method, url=url,
            content=content, headers=headers,
        )

    body = _parse_json(response)

    # filter namespace list for non-admin users
    if path == "namespaces" and request.method == "GET" and body:
        body = _filter_namespaces_response(request, body)

    return JSONResponse(content=body, status_code=response.status_code)


@app.api_route("/catalog/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_catalog(request: Request, path: str):
    qs      = request.url.query
    url     = f"{CATALOG_URL}/{path}" + (f"?{qs}" if qs else "")
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method, url=url,
            content=content, headers=headers,
        )
    return JSONResponse(
        content=_parse_json(response),
        status_code=response.status_code,
    )


app.mount("/js",  StaticFiles(directory=os.path.join(STATIC_DIR, "js")),  name="js")


@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


def main():
    _init_db()

    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logger.warning("logging.yaml not found — using basicConfig")

    logger.info("Waluigi Console")
    logger.info(f"    Binding: {args.bind_address}:{args.port}")
    logger.info(f"    Boss URL: {args.boss_url}")
    logger.info(f"    Catalog URL: {args.catalog_url}")
    logger.info(f"    DB: {args.db_path}")

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    _init_db()
    main()
