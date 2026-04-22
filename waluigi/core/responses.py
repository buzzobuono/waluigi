from typing import Any
from fastapi.responses import JSONResponse

def ok(data: Any, messages: list[str] = None) -> JSONResponse:
    return JSONResponse({
        "data": data,
        "diagnostic": {"result": "OK", "messages": messages or []},
    })


def warn(data: Any, messages: list[str]) -> JSONResponse:
    return JSONResponse({
        "data": data,
        "diagnostic": {"result": "WARN", "messages": messages},
    })


def ko(messages: list[str] | str, status: int = 400) -> JSONResponse:
    if isinstance(messages, str):
        messages = [messages]
    return JSONResponse(
        {"data": None, "diagnostic": {"result": "KO", "messages": messages}},
        status_code=status,
    )