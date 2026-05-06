import hmac
import hashlib
import logging
import time

from config import (
    SETTING_LIST_DB_ID,
    WEBHOOK_DEDUP_MAX_CACHE_SIZE,
    WEBHOOK_DEDUP_TTL_SECONDS,
    WEBHOOK_VERIFICATION_TOKEN,
)

logger = logging.getLogger(__name__)

_processed_events: dict[str, float] = {}

_webhook_verification_token = WEBHOOK_VERIFICATION_TOKEN


def _normalize_notion_id(value: str | None) -> str:
    return (value or "").replace("-", "")


def get_verification_token() -> str | None:
    return _webhook_verification_token or None


def store_verification_token(token: str):
    global _webhook_verification_token
    if not _webhook_verification_token:
        _webhook_verification_token = token
        logger.warning(
            "Verification token stored in memory only. Set WEBHOOK_VERIFICATION_TOKEN "
            "in Railway so signature checks survive restarts."
        )


def verify_notion_signature(payload_body: bytes, signature_header: str) -> bool:
    token = _webhook_verification_token
    if not token:
        logger.warning(
            "No webhook verification token configured; accepting request only so "
            "initial Notion verification can complete."
        )
        return True
    computed = hmac.new(token.encode("utf-8"), payload_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={computed}", signature_header)


def is_duplicate(event_id: str) -> bool:
    now = time.time()
    if event_id in _processed_events:
        if now - _processed_events[event_id] < WEBHOOK_DEDUP_TTL_SECONDS:
            return True
    _processed_events[event_id] = now
    if len(_processed_events) > WEBHOOK_DEDUP_MAX_CACHE_SIZE:
        cutoff = now - WEBHOOK_DEDUP_TTL_SECONDS
        expired = [eid for eid, ts in _processed_events.items() if ts < cutoff]
        for eid in expired:
            del _processed_events[eid]
        if len(_processed_events) > WEBHOOK_DEDUP_MAX_CACHE_SIZE:
            oldest = sorted(_processed_events.items(), key=lambda item: item[1])
            for eid, _ in oldest[: len(_processed_events) - WEBHOOK_DEDUP_MAX_CACHE_SIZE]:
                del _processed_events[eid]
    return False


def get_dedup_cache_size() -> int:
    return len(_processed_events)


def get_parent_data_source_id(payload: dict) -> str:
    parent = payload.get("data", {}).get("parent", {})
    return _normalize_notion_id(parent.get("data_source_id"))


def route_event(payload: dict) -> str:
    event_type = payload.get("type", "")
    if event_type.startswith("comment."):
        return "comment"
    if event_type == "page.properties_updated":
        parent_ds_id = get_parent_data_source_id(payload)
        if parent_ds_id and parent_ds_id != _normalize_notion_id(SETTING_LIST_DB_ID):
            logger.info(
                "Skipping page.properties_updated outside setting list: parent_ds_id=%s",
                parent_ds_id,
            )
            return "unknown"
        return "checkbox"
    return "unknown"


def extract_verification_token(payload: dict) -> str | None:
    return payload.get("verification_token")
