import asyncio
import logging
import random
import time

import httpx

logger = logging.getLogger(__name__)


class NotionAPIError(Exception):
    pass


class NotionClient:
    def __init__(
        self,
        token: str,
        api_version: str,
        max_rps: float = 2.5,
        max_concurrency: int = 3,
        max_retries: int = 5,
    ):
        self.client = httpx.AsyncClient(
            base_url="https://api.notion.com/v1",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": api_version,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self.max_retries = max(1, max_retries)
        self._semaphore = asyncio.Semaphore(max(1, max_concurrency))
        self._min_interval = 1.0 / max(max_rps, 0.1)
        self._last_request_at = 0.0
        self._rate_lock = asyncio.Lock()

    async def _throttle(self):
        async with self._rate_lock:
            now = time.monotonic()
            wait_seconds = self._last_request_at + self._min_interval - now
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_at = time.monotonic()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error = ""

        async with self._semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self._throttle()
                    response = await self.client.request(method, path, **kwargs)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    last_error = str(exc)
                    if attempt == self.max_retries - 1:
                        break
                    delay = min(2 ** attempt, 30) + random.uniform(0, 0.5)
                    logger.warning(
                        "Notion request transport error; retrying in %.2fs "
                        "(attempt %s/%s): %s %s: %s",
                        delay,
                        attempt + 1,
                        self.max_retries,
                        method,
                        path,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    continue

                if response.status_code in retryable_statuses:
                    last_error = await self._response_error_detail(response)
                    if attempt == self.max_retries - 1:
                        break
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = 1.0
                    else:
                        delay = min(2 ** attempt, 30) + random.uniform(0, 0.5)
                    logger.warning(
                        "Notion request returned %s; retrying in %.2fs "
                        "(attempt %s/%s): %s %s: %s",
                        response.status_code,
                        delay,
                        attempt + 1,
                        self.max_retries,
                        method,
                        path,
                        last_error,
                    )
                    await asyncio.sleep(delay)
                    continue

                if response.status_code >= 400:
                    detail = await self._response_error_detail(response)
                    raise NotionAPIError(
                        f"Notion API {response.status_code} for {method} {path}: {detail}"
                    )

                return response.json()

        raise NotionAPIError(
            f"Max retries ({self.max_retries}) exceeded for {method} {path}: {last_error}"
        )

    async def _response_error_detail(self, response: httpx.Response) -> str:
        try:
            body = response.json()
        except ValueError:
            return response.text[:500]
        if not isinstance(body, dict):
            return str(body)[:500]
        code = body.get("code", "")
        message = body.get("message", "")
        return f"{code}: {message}".strip(": ")

    async def get_page(self, page_id: str) -> dict:
        return await self._request("GET", f"/pages/{page_id}")

    async def update_page(self, page_id: str, properties: dict) -> dict:
        return await self._request("PATCH", f"/pages/{page_id}", json={"properties": properties})

    async def query_database(self, database_id: str, body: dict, start_cursor: str = None) -> tuple[list, str | None]:
        payload = {**body}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = await self._request("POST", f"/data_sources/{database_id}/query", json=payload)
        return result.get("results", []), result.get("next_cursor")

    async def query_database_all(self, database_id: str, body: dict) -> list:
        all_results = []
        cursor = None
        while True:
            results, cursor = await self.query_database(database_id, body, start_cursor=cursor)
            all_results.extend(results)
            if not cursor:
                break
        return all_results

    async def get_comment(self, comment_id: str) -> dict:
        return await self._request("GET", f"/comments/{comment_id}")

    async def get_me(self) -> dict:
        return await self._request("GET", "/users/me")

    async def create_page(self, parent: dict, properties: dict) -> dict:
        return await self._request("POST", "/pages", json={"parent": parent, "properties": properties})

    async def close(self):
        await self.client.aclose()
