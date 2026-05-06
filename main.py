import logging
import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException

from config import (
    FLOW_TASK_TIMEOUT_SECONDS,
    NOTION_API_TOKEN,
    NOTION_API_MAX_CONCURRENCY,
    NOTION_API_MAX_RETRIES,
    NOTION_API_MAX_RPS,
    NOTION_API_VERSION,
    RECONCILE_CHECKED_SETTINGS_COOLDOWN_SECONDS,
    RECONCILE_CHECKED_SETTINGS_ON_WEBHOOK,
    SERVER_PORT,
    WEBHOOK_MAX_BODY_BYTES,
)
from middleware import RequestLoggingMiddleware
from webhook_handler import (
    get_dedup_cache_size,
    get_verification_token,
    verify_notion_signature,
    is_duplicate,
    route_event,
    extract_verification_token,
    store_verification_token,
)
from notion_client import NotionClient
from llm_client import LLMClient
from flows.comment_trigger import handle_comment_event
from flows.checkbox_trigger import handle_checkbox_event, reconcile_checked_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

notion_client = NotionClient(
    NOTION_API_TOKEN,
    NOTION_API_VERSION,
    max_rps=NOTION_API_MAX_RPS,
    max_concurrency=NOTION_API_MAX_CONCURRENCY,
    max_retries=NOTION_API_MAX_RETRIES,
)
llm_client = LLMClient()
_reconcile_lock = asyncio.Lock()
_last_reconcile_started_at = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Server starting on port {SERVER_PORT}")
    yield
    logger.info("Server shutting down")
    await notion_client.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLoggingMiddleware)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "webhook_verification_token_configured": bool(get_verification_token()),
        "dedup_cache_size": get_dedup_cache_size(),
    }


@app.get("/")
async def root():
    return await health()


@app.post("/")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > WEBHOOK_MAX_BODY_BYTES:
                raise HTTPException(status_code=413, detail="Body too large")
        except ValueError:
            logger.warning("Invalid content-length header: %s", content_length)

    body = await request.body()
    if len(body) > WEBHOOK_MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Body too large")

    logger.info(
        "Webhook received: method=%s path=%s body_bytes=%s content_type=%s ua=%s",
        request.method,
        request.url.path,
        len(body),
        request.headers.get("content-type", ""),
        request.headers.get("user-agent", ""),
    )

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    verification_token = extract_verification_token(payload)
    if verification_token:
        store_verification_token(verification_token)
        logger.info("Webhook verification request received, token stored")
        return {"verification_token": verification_token}

    signature = request.headers.get("X-Notion-Signature", "")
    if not verify_notion_signature(body, signature):
        logger.warning(
            "Invalid webhook signature: signature_present=%s",
            bool(signature),
        )
        raise HTTPException(status_code=401, detail="Invalid signature")

    event_id = payload.get("id", "")
    payload_type = payload.get("type", "")
    logger.info(
        "Webhook payload parsed: id=%s type=%s",
        event_id,
        payload_type,
    )

    if event_id and is_duplicate(event_id):
        logger.info(f"Duplicate event ignored: {event_id}")
        return {"status": "ok", "action": "duplicate_ignored"}

    event_type = route_event(payload)
    logger.info(f"Event received: type={event_type}, id={event_id}")

    if event_type == "comment":
        background_tasks.add_task(process_comment_flow, payload)
    elif event_type == "checkbox":
        background_tasks.add_task(process_checkbox_flow, payload)
    elif payload_type == "page.properties_updated":
        maybe_schedule_reconcile(background_tasks, payload)
    else:
        logger.info(f"Unknown event type, ignoring: {event_type}")

    return {"status": "ok"}


async def process_comment_flow(payload: dict):
    try:
        result = await asyncio.wait_for(
            handle_comment_event(payload, notion_client),
            timeout=FLOW_TASK_TIMEOUT_SECONDS,
        )
        logger.info(f"Comment flow result: {result}")
    except asyncio.TimeoutError:
        logger.error(
            "Comment flow timed out after %ss: event_id=%s",
            FLOW_TASK_TIMEOUT_SECONDS,
            payload.get("id", ""),
        )
    except Exception as e:
        logger.error(f"Comment flow error: {e}", exc_info=True)


async def process_checkbox_flow(payload: dict):
    try:
        result = await asyncio.wait_for(
            handle_checkbox_event(payload, notion_client, llm_client),
            timeout=FLOW_TASK_TIMEOUT_SECONDS,
        )
        logger.info(f"Checkbox flow result: {result}")
    except asyncio.TimeoutError:
        logger.error(
            "Checkbox flow timed out after %ss: event_id=%s",
            FLOW_TASK_TIMEOUT_SECONDS,
            payload.get("id", ""),
        )
    except Exception as e:
        logger.error(f"Checkbox flow error: {e}", exc_info=True)


def maybe_schedule_reconcile(background_tasks: BackgroundTasks, payload: dict):
    if not RECONCILE_CHECKED_SETTINGS_ON_WEBHOOK:
        logger.info("Reconcile skipped: disabled by configuration")
        return
    logger.info(
        "Scheduling checked setting reconciliation after %s event id=%s",
        payload.get("type", ""),
        payload.get("id", ""),
    )
    background_tasks.add_task(process_reconcile_flow, payload)


async def process_reconcile_flow(payload: dict | None = None):
    global _last_reconcile_started_at
    now = asyncio.get_running_loop().time()

    if now - _last_reconcile_started_at < RECONCILE_CHECKED_SETTINGS_COOLDOWN_SECONDS:
        logger.info(
            "Reconcile skipped: cooldown active (event_id=%s)",
            (payload or {}).get("id", ""),
        )
        return

    if _reconcile_lock.locked():
        logger.info(
            "Reconcile skipped: already running (event_id=%s)",
            (payload or {}).get("id", ""),
        )
        return

    async with _reconcile_lock:
        _last_reconcile_started_at = asyncio.get_running_loop().time()
        try:
            result = await asyncio.wait_for(
                reconcile_checked_settings(notion_client, llm_client),
                timeout=FLOW_TASK_TIMEOUT_SECONDS,
            )
            logger.info(f"Reconcile flow result: {result}")
        except asyncio.TimeoutError:
            logger.error(
                "Reconcile flow timed out after %ss: event_id=%s",
                FLOW_TASK_TIMEOUT_SECONDS,
                (payload or {}).get("id", ""),
            )
        except Exception as e:
            logger.error(f"Reconcile flow error: {e}", exc_info=True)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=SERVER_PORT, reload=True)
