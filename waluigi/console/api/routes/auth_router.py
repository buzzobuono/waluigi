import json
from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.console.config.args import args
from waluigi.console.config.dependencies import auth_service, user_service, require_admin
from waluigi.console.api.schemas import (
    LoginRequest, UserCreateRequest, UserUpdateRequest, UserUpsertRequest,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/login")
async def login(body: LoginRequest,
                auth=Depends(auth_service),
                users=Depends(user_service)):
    if body.username == args.admin_user and body.password == args.admin_password:
        namespaces = "*"
    else:
        row = users.get_raw(body.username)
        if not row or not auth.verify_password(body.password, row['password_hash']):
            return ko("Invalid credentials", status=401)
        try:
            namespaces = json.loads(row['namespaces'])
        except Exception:
            namespaces = []

    token = auth.create_token(body.username, namespaces)
    # Return flat dict (no ok() envelope) — frontend reads res.token directly
    return {"token": token, "username": body.username, "namespaces": namespaces}


@router.get("/users")
async def list_users(users=Depends(user_service), _=Depends(require_admin)):
    return ok(users.list())


@router.post("/users")
async def create_user(body: UserCreateRequest,
                      users=Depends(user_service),
                      _=Depends(require_admin)):
    if not body.userid.strip() or not body.password.strip():
        return ko("userid and password are required", status=400)
    if body.userid == args.admin_user:
        return ko("Cannot create a user with the admin userid", status=409)
    try:
        data = users.create(
            body.userid.strip(),
            body.username.strip() or body.userid.strip(),
            body.password,
            body.namespaces,
        )
        return ok(data)
    except Exception as e:
        msg = str(e)
        if "UNIQUE" in msg or "unique" in msg.lower():
            return ko("User already exists", status=409)
        return ko(msg, status=500)


@router.patch("/users/{userid}")
async def update_user(userid: str, body: UserUpdateRequest,
                      users=Depends(user_service),
                      _=Depends(require_admin)):
    if userid == args.admin_user:
        return ko("Cannot modify the admin user", status=409)
    if body.username is None and body.password is None and body.namespaces is None:
        return ko("Nothing to update", status=400)
    found = users.update(userid, body.username, body.password, body.namespaces)
    if not found:
        return ko("User not found", status=404)
    updated = [f for f in ("username", "password", "namespaces")
               if getattr(body, f) is not None]
    return ok({"userid": userid, "updated": updated})


@router.delete("/users/{userid}")
async def delete_user(userid: str,
                      users=Depends(user_service),
                      _=Depends(require_admin)):
    if userid == args.admin_user:
        return ko("Cannot delete the admin user", status=409)
    found = users.delete(userid)
    if not found:
        return ko("User not found", status=404)
    return ok({"deleted": userid})


@router.put("/users/{userid}")
async def upsert_user(userid: str, body: UserUpsertRequest,
                      users=Depends(user_service),
                      _=Depends(require_admin)):
    if userid == args.admin_user:
        return ko("Cannot modify the admin user", status=409)
    if not userid.strip():
        return ko("userid is required", status=400)
    try:
        action = users.upsert(userid, body.username, body.password, body.namespaces)
        return ok({"userid": userid, "action": action})
    except ValueError as e:
        return ko(str(e), status=400)
