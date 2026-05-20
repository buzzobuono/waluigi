import httpx


class HttpClient:
    """Sync HTTP client with persistent connection and configurable base URL."""

    def __init__(self, base_url: str = "", timeout: float = 10.0, headers: dict = None):
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers or {},
        )

    def get(self, path: str, **kw) -> httpx.Response:
        return self._client.get(path, **kw)

    def post(self, path: str, **kw) -> httpx.Response:
        return self._client.post(path, **kw)

    def patch(self, path: str, **kw) -> httpx.Response:
        return self._client.patch(path, **kw)

    def delete(self, path: str, **kw) -> httpx.Response:
        return self._client.delete(path, **kw)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class AsyncHttpClient:
    """Async HTTP client with persistent connection and configurable base URL."""

    def __init__(self, base_url: str = "", timeout: float = 10.0, headers: dict = None):
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers or {},
        )

    async def get(self, path: str, **kw) -> httpx.Response:
        return await self._client.get(path, **kw)

    async def post(self, path: str, **kw) -> httpx.Response:
        return await self._client.post(path, **kw)

    async def patch(self, path: str, **kw) -> httpx.Response:
        return await self._client.patch(path, **kw)

    async def delete(self, path: str, **kw) -> httpx.Response:
        return await self._client.delete(path, **kw)

    async def aclose(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()
