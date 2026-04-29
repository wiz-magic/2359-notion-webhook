import hmac
import hashlib
import logging
import os
import time

logger = logging.getLogger(__name__)

MAX_CACHE_SIZE = 10000
DEDUP_TTL_SECONDS = 3600
_processed_events: dict[str, float] = {}

_webhook_signing_secret = os.environ.get("WEBHOOK_SIGNING_SECRET", "")


def get_verification_token() -> str | None:
    return _webhook_signing_secret or None


def store_verification_token(token: str):
    global _webhook_signing_secret
    if not _webhook_signing_secret:
        _webhook_signing_secret = token
        logger.info("Verification token stored from Notion")


def verify_notion_signature(payload_body: bytes, signature_header: str) -> bool:
    secret = _webhook_signing_secret
    if not secret:
        logger.warning("No signing secret configured; skipping signature check")
        return True
    computed = hmac.new(secret.encode("utf-8"), payload_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={computed}", signature_header)


def is_duplicate(event_id: str) -> bool:
    now = time.time()
    if event_id in _processed_events:
        if now - _processed_events[event_id] < DEDUP_TTL_SECONDS:
            return True
    _processed_events[event_id] = now
    if len(_processed_events) > MAX_CACHE_SIZE:
        cutoff = now - DEDUP_TTL_SECONDS
        expired = [eid for eid, ts in _processed_events.items() if ts < cutoff]
        for eid in expired:
            del _processed_events[eid]
    return False


def route_event(payload: dict) -> str:
    event_type = payload.get("type", "")
    if "comment" in event_type:
        return "comment"
    if "page" in event_type:
        return "checkbox"
    return "unknown"


def extract_verification_token(payload: dict) -> str | None:
    return payload.get("verification_token")
