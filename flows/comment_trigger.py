import logging

from config import (
    SETTING_LIST_DB_ID,
    DS_ID_TO_BRAND_MAP,
    TRIGGER_KEYWORDS,
    VALID_STATUSES,
)
from ad_name_parser import get_title
from notion_client import NotionClient

logger = logging.getLogger(__name__)


def _get_property_text(properties: dict, prop_name: str) -> str:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "title":
        return "".join(rt.get("plain_text", "") for rt in prop.get("title", []))
    if prop.get("type") == "rich_text":
        return "".join(rt.get("plain_text", "") for rt in prop.get("rich_text", []))
    return ""


def _get_property_select(properties: dict, prop_name: str) -> str:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if prop.get("type") == "status":
        st = prop.get("status")
        return st["name"] if st else ""
    return ""


def _has_trigger_keyword(text: str) -> bool:
    return any(kw in text for kw in TRIGGER_KEYWORDS)


async def handle_comment_event(payload: dict, notion: NotionClient) -> dict:
    try:
        # Step 1: comment_id, page_id 추출
        comment_data = payload.get("comment", {})
        comment_id = comment_data.get("id", "")
        page_id = payload.get("entity", {}).get("id", "")

        if not comment_id or not page_id:
            logger.info("Flow 1: comment_id or page_id missing, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 2: 댓글 텍스트 조회
        comment = await notion.get_comment(comment_id)
        rich_text = comment.get("rich_text", [])
        comment_text = "".join(rt.get("plain_text", "") for rt in rich_text)

        logger.info(f"Flow 1: comment on page {page_id}: \"{comment_text[:50]}\"")

        # Step 3: 키워드 필터링
        if not _has_trigger_keyword(comment_text):
            logger.info("Flow 1: no trigger keyword, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 4: 소재 페이지 조회
        page = await notion.get_page(page_id)
        properties = page.get("properties", {})
        ad_name = get_title(page)

        if not ad_name:
            logger.info(f"Flow 1: ad_name is empty for page {page_id}, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 5: 상태 조건 검증
        status = _get_property_select(properties, "상태")
        if status not in VALID_STATUSES:
            logger.info(f"Flow 1: status '{status}' not in {VALID_STATUSES}, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 6: 브랜드 결정
        parent = page.get("parent", {})
        parent_ds_id = (parent.get("data_source_id") or parent.get("database_id") or "").replace("-", "")
        brand = DS_ID_TO_BRAND_MAP.get(parent_ds_id, "")

        # Step 7: 중복 확인 (원본 페이지 링크 기준)
        page_url = page.get("url", "")
        if page_url:
            existing_results, _ = await notion.query_database(
                SETTING_LIST_DB_ID,
                {"filter": {"property": "원본 페이지 링크", "url": {"equals": page_url}}},
            )
            if existing_results:
                logger.info(f"Flow 1: duplicate entry exists for {page_url}, skipping")
                return {"status": "ok", "action": "skipped"}

        # Step 8: 세팅 리스트에 페이지 추가
        ad_group = _get_property_text(properties, "광고그룹명")
        campaign_name = _get_property_text(properties, "캠페인명")

        new_properties = {
            "이름": {"title": [{"text": {"content": ad_name}}]},
            "광고그룹명": {"rich_text": [{"text": {"content": ad_group}}]},
            "캠페인명": {"rich_text": [{"text": {"content": campaign_name}}]},
        }

        if brand:
            new_properties["브랜드"] = {"select": {"name": brand}}

        if page_url:
            new_properties["원본 페이지 링크"] = {"url": page_url}

        result = await notion.create_page(
            parent={"data_source_id": SETTING_LIST_DB_ID},
            properties=new_properties,
        )
        logger.info(f"Flow 1: created setting list entry for '{ad_name}' (brand: {brand})")
        return {"status": "ok", "action": "setting_list_entry_created", "page_id": result.get("id")}

    except Exception as e:
        logger.error(f"Flow 1 error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}
