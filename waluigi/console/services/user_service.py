from __future__ import annotations
import json
from datetime import datetime, timezone

from waluigi.console.repositories.user_repo import UserRepository
from waluigi.console.services.auth_service import AuthService


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe(row: dict) -> dict:
    out = {k: v for k, v in row.items() if k != 'password_hash'}
    try:
        out['namespaces'] = json.loads(out.get('namespaces', '[]'))
    except Exception:
        out['namespaces'] = []
    return out


class UserService:

    def __init__(self, repo: UserRepository, auth: AuthService):
        self.repo = repo
        self.auth = auth

    def list(self) -> list[dict]:
        return [_safe(r) for r in self.repo.list()]

    def get(self, userid: str) -> dict | None:
        row = self.repo.get(userid)
        return _safe(row) if row else None

    def get_raw(self, userid: str) -> dict | None:
        return self.repo.get(userid)

    def create(self, userid: str, username: str, password: str,
               namespaces: list[str]) -> dict:
        now = _now()
        self.repo.create(userid, username, self.auth.hash_password(password), namespaces, now)
        return {"userid": userid, "username": username, "namespaces": namespaces, "createdate": now}

    def update(self, userid: str, username: str | None = None,
               password: str | None = None, namespaces: list[str] | None = None) -> bool:
        vals: dict = {"updatedate": _now()}
        if username   is not None: vals["username"]      = username.strip()
        if password   is not None: vals["password_hash"] = self.auth.hash_password(password)
        if namespaces is not None: vals["namespaces"]    = json.dumps(namespaces)
        return self.repo.update(userid, vals) > 0

    def delete(self, userid: str) -> bool:
        return self.repo.delete(userid) > 0

    def upsert(self, userid: str, username: str | None, password: str | None,
               namespaces: list[str]) -> str:
        if not self.repo.exists(userid):
            if not password:
                raise ValueError("password is required when creating a new user")
            display = (username or userid).strip()
            self.repo.create(userid, display, self.auth.hash_password(password), namespaces, _now())
            return "created"
        else:
            vals: dict = {"namespaces": json.dumps(namespaces), "updatedate": _now()}
            if username: vals["username"]      = username.strip()
            if password: vals["password_hash"] = self.auth.hash_password(password)
            self.repo.update(userid, vals)
            return "updated"
