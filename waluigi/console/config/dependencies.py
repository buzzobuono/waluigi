from __future__ import annotations
import logging
from fastapi import Depends, Request, HTTPException

from waluigi.console.config.args import args

logger = logging.getLogger("waluigi")

_db = None


def init_db(url: str) -> None:
    global _db
    from waluigi.console.db import ConsoleDB
    _db = ConsoleDB(url)
    logger.info(f"Console DB ready: {url}")


def get_db():
    return _db


def auth_service():
    from waluigi.console.services.auth_service import AuthService
    return AuthService(args.secret_key, args.token_expire_h)


def user_service(db=Depends(get_db)):
    from waluigi.console.services.auth_service import AuthService
    from waluigi.console.services.user_service import UserService
    return UserService(db.users, AuthService(args.secret_key, args.token_expire_h))


def get_current_user(request: Request) -> dict:
    return getattr(request.state, 'user', None)


def require_admin(user=Depends(get_current_user)):
    if user is None or user.get('namespaces') != "*":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
