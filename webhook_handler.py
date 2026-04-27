import hmac
import hashlib
import logging
import time

logger = logging.getLogger(__name__)

MAX_CACHE_SIZE = 10000
DEDUP_TTL_SECONDS = 3600
_processed_events: dict[str, float] = {}


def verify_notion_signature(payload_body: bytes, signature_header: str, secret: str) -> bool:
    if not secret:
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


def is_verification_request(payload: dict) -> bool:
    return "verification_token" in payload
