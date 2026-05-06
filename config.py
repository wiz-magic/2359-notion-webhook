import os
from dotenv import load_dotenv

load_dotenv()

# --- Notion API ---
NOTION_API_TOKEN = os.environ.get("NOTION_API_TOKEN", "")
NOTION_API_VERSION = os.environ.get("NOTION_API_VERSION", "2025-09-03")
NOTION_BASE_URL = "https://api.notion.com/v1"

# --- Webhook ---
WEBHOOK_PATH_SECRET = os.environ.get("WEBHOOK_PATH_SECRET", "change-me")
WEBHOOK_VERIFICATION_TOKEN = (
    os.environ.get("WEBHOOK_VERIFICATION_TOKEN")
    or os.environ.get("WEBHOOK_SIGNING_SECRET")
    or ""
)
ENABLE_IP_WHITELIST = os.environ.get("ENABLE_IP_WHITELIST", "false").lower() == "true"
WEBHOOK_MAX_BODY_BYTES = int(os.environ.get("WEBHOOK_MAX_BODY_BYTES", "262144"))
WEBHOOK_DEDUP_TTL_SECONDS = int(os.environ.get("WEBHOOK_DEDUP_TTL_SECONDS", "90000"))
WEBHOOK_DEDUP_MAX_CACHE_SIZE = int(os.environ.get("WEBHOOK_DEDUP_MAX_CACHE_SIZE", "50000"))
FLOW_TASK_TIMEOUT_SECONDS = int(os.environ.get("FLOW_TASK_TIMEOUT_SECONDS", "180"))

# --- Server ---
SERVER_PORT = int(os.environ.get("PORT", os.environ.get("SERVER_PORT", "8000")))

# --- Notion API Safety ---
NOTION_API_MAX_RPS = float(os.environ.get("NOTION_API_MAX_RPS", "2.5"))
NOTION_API_MAX_CONCURRENCY = int(os.environ.get("NOTION_API_MAX_CONCURRENCY", "3"))
NOTION_API_MAX_RETRIES = int(os.environ.get("NOTION_API_MAX_RETRIES", "5"))

# --- LLM ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# --- Brand ↔ Datasource Mapping ---
BRAND_DS_ID_MAP = {
    "셀라딕스":    "2ff7aa4b7b0f8193b0cb000b117a0118",
    "웰릿":       "2ff7aa4b7b0f8183940a000bb5ae5ed9",
    "클리너리":    "2ff7aa4b7b0f817e80de000bc2e620bb",
    "메디온":      "2ff7aa4b7b0f811db748000b5810fb19",
    "큐리셀":      "2ff7aa4b7b0f817ba57e000ba531d597",
    "비피젠":      "2ff7aa4b7b0f8171b8ed000bf5933f6e",
    "프로뉴트리션": "2ff7aa4b7b0f815480bc000b2ddc1408",
    "하우스윗":    "2ff7aa4b7b0f81159616000b1419e9ab",
    "히든":       "2ff7aa4b7b0f81efb648000b427e5265",
}

DS_ID_TO_BRAND_MAP = {v: k for k, v in BRAND_DS_ID_MAP.items()}

# --- Databases ---
SETTING_LIST_DB_ID = "34f7aa4b7b0f81588d6a000bb908893b"

# --- Flow 1 Constants ---
TRIGGER_KEYWORDS = ["수정 완료", "완료", "업로드"]
VALID_STATUSES = ["수정 중", "미통과", "신규 발견"]

# --- Flow 2 Constants ---
TARGET_STATUS = "진행 중"
QUERY_STATUSES = ["수정 중", "미통과"]
