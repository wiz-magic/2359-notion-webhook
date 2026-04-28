import logging
from contextlib import asynccontextmanager
from collections import deque

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from config import (
    NOTION_API_TOKEN,
    NOTION_API_VERSION,
    SERVER_PORT,
)
from middleware import limiter, RequestLoggingMiddleware
from webhook_handler import verify_notion_signature, is_duplicate, route_event, extract_verification_token, store_verification_token, get_verification_token
from notion_client import NotionClient
from llm_client import LLMClient
from flows.comment_trigger import handle_comment_event
from flows.checkbox_trigger import handle_checkbox_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
webhook_debug_events = deque(maxlen=30)

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


@app.get("/debug/connection")
async def debug_connection():
    result = {
        "status": "ok",
        "notion_api_token_configured": bool(NOTION_API_TOKEN),
        "notion_api_connected": False,
    }
    try:
        me = await notion_client.get_me()
        result["notion_api_connected"] = True
        result["notion_bot_id"] = me.get("bot", {}).get("workspace_name", "")
    except Exception as e:
        logger.error("Notion connection check failed: %s", e, exc_info=True)
        result["status"] = "error"
        result["error"] = str(e)
    return result


@app.get("/debug/webhook-events")
async def debug_webhook_events():
    return {
        "count": len(webhook_debug_events),
        "events": list(webhook_debug_events),
    }


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
        webhook_debug_events.appendleft({
            "stage": "signature_failed",
            "signature_present": bool(signature),
            "token_stored": bool(get_verification_token()),
            "body_bytes": len(body),
        })
        logger.warning(
            "Invalid webhook signature: signature_present=%s token_stored=%s",
            bool(signature),
            bool(get_verification_token()),
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
        webhook_debug_events.appendleft({
            "stage": "verification_complete",
            "token_prefix": verification_token[:8],
        })
        return {"verification_token": verification_token}

    if event_id and is_duplicate(event_id):
        logger.info(f"Duplicate event ignored: {event_id}")
        return {"status": "ok", "action": "duplicate_ignored"}

    event_type = route_event(payload)
    logger.info(f"Event received: type={event_type}, id={event_id}")
    webhook_debug_events.appendleft({
        "stage": "event_routed",
        "id": event_id,
        "event_type": event_type,
    })

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
        webhook_debug_events.appendleft({
            "stage": "comment_flow_result",
            "result": result,
        })
        logger.info(f"Comment flow result: {result}")
    except Exception as e:
        webhook_debug_events.appendleft({
            "stage": "comment_flow_error",
            "error": str(e),
        })
        logger.error(f"Comment flow error: {e}", exc_info=True)


async def process_checkbox_flow(payload: dict):
    try:
        result = await handle_checkbox_event(payload, notion_client)
        webhook_debug_events.appendleft({
            "stage": "checkbox_flow_result",
            "result": result,
        })
        logger.info(f"Checkbox flow result: {result}")
    except Exception as e:
        webhook_debug_events.appendleft({
            "stage": "checkbox_flow_error",
            "error": str(e),
        })
        logger.error(f"Checkbox flow error: {e}", exc_info=True)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=SERVER_PORT, reload=True)
