import logging
import time

from slowapi import Limiter
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])


class RequestLoggingMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.time()
        method = scope["method"]
        path = scope["path"]
        status_code = "?"

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)
        duration = time.time() - start
        logger.info(f"{method} {path} → {status_code} ({duration:.3f}s)")
