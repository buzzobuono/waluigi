import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from waluigi.console.config.args import args

router = APIRouter()

_ADMIN_ONLY_BOSS = {"workers", "resources"}


def _parse_json(response):
    if not response.content:
        return None
    try:
        return response.json()
    except Exception:
        return None


def _is_admin(request: Request) -> bool:
    u = getattr(request.state, 'user', None)
    return u is not None and u.get('namespaces') == "*"


def _can_access_namespace(request: Request, namespace: str) -> bool:
    u = getattr(request.state, 'user', None)
    if u is None:
        return False
    ns = u.get('namespaces', [])
    return ns == "*" or namespace in ns


def _check_admin_paths(request: Request, path: str) -> JSONResponse | None:
    top = path.split("/")[0]
    if top in _ADMIN_ONLY_BOSS and not _is_admin(request):
        return JSONResponse({"detail": "Admin access required"}, status_code=403)
    return None


def _check_namespace(request: Request, path: str) -> JSONResponse | None:
    parts = path.split("/")
    if parts[0] != "namespaces":
        return None

    if len(parts) == 1 and request.method == "POST":
        if not _is_admin(request):
            return JSONResponse({"detail": "Admin access required"}, status_code=403)
        return None

    if len(parts) >= 2 and parts[1]:
        namespace = parts[1]
        if namespace in ("", "_reset"):
            return None
        if len(parts) == 2 and request.method == "DELETE":
            if not _is_admin(request):
                return JSONResponse({"detail": "Admin access required"}, status_code=403)
            return None
        if not _can_access_namespace(request, namespace):
            return JSONResponse(
                {"detail": f"Access to namespace '{namespace}' is not allowed"},
                status_code=403,
            )
    return None


def _filter_namespaces(request: Request, body: dict) -> dict:
    if _is_admin(request):
        return body
    user_ns = getattr(request.state, 'user', {}).get("namespaces", [])
    body["data"] = [r for r in body.get("data", []) if r.get("namespace") in user_ns]
    return body


@router.api_route("/boss/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_boss(request: Request, path: str):
    denied = _check_admin_paths(request, path)
    if denied:
        return denied
    denied = _check_namespace(request, path)
    if denied:
        return denied

    qs      = request.url.query
    url     = f"{args.boss_url.rstrip('/')}/{path}" + (f"?{qs}" if qs else "")
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method=request.method, url=url, content=content, headers=headers,
        )

    body = _parse_json(resp)
    if path == "namespaces" and request.method == "GET" and body:
        body = _filter_namespaces(request, body)
    return JSONResponse(content=body, status_code=resp.status_code)


@router.api_route("/catalog/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_catalog(request: Request, path: str):
    denied = _check_namespace(request, path)
    if denied:
        return denied

    qs      = request.url.query
    url     = f"{args.catalog_url.rstrip('/')}/{path}" + (f"?{qs}" if qs else "")
    content = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "authorization")}
    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method=request.method, url=url, content=content, headers=headers,
        )
    return JSONResponse(content=_parse_json(resp), status_code=resp.status_code)
