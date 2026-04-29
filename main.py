import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from config import (
    NOTION_API_TOKEN,
    NOTION_API_VERSION,
    SERVER_PORT,
)
from middleware import limiter, RequestLoggingMiddleware
from webhook_handler import verify_notion_signature, is_duplicate, route_event, extract_verification_token, store_verification_token
from notion_client import NotionClient
from llm_client import LLMClient
from flows.comment_trigger import handle_comment_event
from flows.checkbox_trigger import handle_checkbox_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

notion_client = NotionClient(NOTION_API_TOKEN, NOTION_API_VERSION)
llm_client = LLMClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Server starting on port {SERVER_PORT}")
    yield
    logger.info("Server shutting down")
    await notion_client.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLoggingMiddleware)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/")
@limiter.limit("60/minute")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    logger.info(
        "Webhook received: method=%s path=%s body_bytes=%s content_type=%s ua=%s",
        request.method,
        request.url.path,
        len(body),
        request.headers.get("content-type", ""),
        request.headers.get("user-agent", ""),
    )

    signature = request.headers.get("X-Notion-Signature", "")
    if not verify_notion_signature(body, signature):
        logger.warning(
            "Invalid webhook signature: signature_present=%s",
            bool(signature),
        )
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    event_id = payload.get("id", "")
    payload_type = payload.get("type", "")
    logger.info(
        "Webhook payload parsed: id=%s type=%s",
        event_id,
        payload_type,
    )

    verification_token = extract_verification_token(payload)
    if verification_token:
        store_verification_token(verification_token)
        logger.info("Webhook verification request received, token stored")
        return {"verification_token": verification_token}

    if event_id and is_duplicate(event_id):
        logger.info(f"Duplicate event ignored: {event_id}")
        return {"status": "ok", "action": "duplicate_ignored"}

    event_type = route_event(payload)
    logger.info(f"Event received: type={event_type}, id={event_id}")

    if event_type == "comment":
        background_tasks.add_task(process_comment_flow, payload)
    elif event_type == "checkbox":
        background_tasks.add_task(process_checkbox_flow, payload)
    else:
        logger.info(f"Unknown event type, ignoring: {event_type}")

    return {"status": "ok"}


async def process_comment_flow(payload: dict):
    try:
        result = await handle_comment_event(payload, notion_client)
        logger.info(f"Comment flow result: {result}")
    except Exception as e:
        logger.error(f"Comment flow error: {e}", exc_info=True)


async def process_checkbox_flow(payload: dict):
    try:
        result = await handle_checkbox_event(payload, notion_client)
        logger.info(f"Checkbox flow result: {result}")
    except Exception as e:
        logger.error(f"Checkbox flow error: {e}", exc_info=True)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=SERVER_PORT, reload=True)
