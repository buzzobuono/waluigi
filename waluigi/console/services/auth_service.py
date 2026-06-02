from __future__ import annotations
import os
import hmac
import hashlib
import base64
import json
import time
from datetime import datetime, timedelta, timezone


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def _b64url_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))

def _sign(msg: str, secret: str) -> str:
    return _b64url(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest())


class AuthService:

    def __init__(self, secret_key: str, token_expire_h: int):
        self.secret_key     = secret_key
        self.token_expire_h = token_expire_h

    def encode_token(self, payload: dict) -> str:
        header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
        body   = _b64url(json.dumps(payload).encode())
        return f"{header}.{body}.{_sign(f'{header}.{body}', self.secret_key)}"

    def decode_token(self, token: str) -> dict:
        parts = token.split('.')
        if len(parts) != 3:
            raise ValueError("malformed token")
        header, body, sig = parts
        if not hmac.compare_digest(sig, _sign(f'{header}.{body}', self.secret_key)):
            raise ValueError("invalid signature")
        payload = json.loads(_b64url_dec(body))
        if payload.get('exp', float('inf')) < time.time():
            raise ValueError("token expired")
        return payload

    def create_token(self, sub: str, namespaces: list[str] | str) -> str:
        exp = int((datetime.now(timezone.utc) + timedelta(hours=self.token_expire_h)).timestamp())
        return self.encode_token({"sub": sub, "exp": exp, "namespaces": namespaces})

    @staticmethod
    def hash_password(password: str) -> str:
        salt = os.urandom(16)
        dk   = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100_000)
        return salt.hex() + ':' + dk.hex()

    @staticmethod
    def verify_password(password: str, stored: str) -> bool:
        try:
            salt_hex, dk_hex = stored.split(':')
            salt = bytes.fromhex(salt_hex)
            dk   = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100_000)
            return hmac.compare_digest(dk.hex(), dk_hex)
        except Exception:
            return False
