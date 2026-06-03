from __future__ import annotations
import json
import base64
import os
from pathlib import Path

from waluigi.commons.http import HttpClient


class WaluigiSession:
    """HTTP session: stores auth token, builds request headers, resolves namespaces."""

    def __init__(self, base_url: str):
        self.base_url   = base_url.rstrip("/")
        self.config_dir = Path.home() / ".waluigi"
        self.token_file = self.config_dir / "token"
        self.http       = HttpClient(self.base_url)
        self.config_dir.mkdir(parents=True, exist_ok=True)

    # ── Token storage ─────────────────────────────────────────────────────────

    def save_token(self, token: str) -> None:
        self.token_file.write_text(token)

    def get_token(self) -> str | None:
        return self.token_file.read_text().strip() if self.token_file.exists() else None

    def delete_token(self) -> bool:
        if self.token_file.exists():
            os.remove(self.token_file)
            return True
        return False

    def headers(self) -> dict:
        token = self.get_token()
        return {"Authorization": f"Bearer {token}"} if token else {}

    # ── Namespace resolution ──────────────────────────────────────────────────

    def token_namespaces(self) -> list[str] | str | None:
        token = self.get_token()
        if not token:
            return None
        try:
            payload = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
            return payload.get("namespaces")
        except Exception:
            return None

    def resolve_namespace(self, namespace_arg: str | None) -> str | None:
        """Return namespace from arg, or auto-detect from token if unambiguous."""
        if namespace_arg:
            return namespace_arg
        ns = self.token_namespaces()
        if ns == "*":
            print("Error: namespace required for admin users. Use -n/--namespace.")
            return None
        if isinstance(ns, list):
            if len(ns) == 1:
                return ns[0]
            if len(ns) > 1:
                print(f"Error: multiple namespaces {ns}. Use -n/--namespace.")
                return None
        print("Error: namespace required. Use -n/--namespace.")
        return None
