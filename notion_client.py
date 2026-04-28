import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


class NotionAPIError(Exception):
    pass


class NotionClient:
    def __init__(self, token: str, api_version: str):
        self.client = httpx.AsyncClient(
            base_url="https://api.notion.com/v1",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": api_version,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        max_retries = 3
        for attempt in range(max_retries):
            response = await self.client.request(method, path, **kwargs)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "1"))
                logger.warning(f"Rate limited. Retrying in {retry_after}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        raise NotionAPIError(f"Max retries ({max_retries}) exceeded for {method} {path}")

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
