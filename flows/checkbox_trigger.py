import asyncio
import logging

from config import (
    SETTING_LIST_DB_ID,
    BRAND_DS_ID_MAP,
    TARGET_STATUS,
    QUERY_STATUSES,
)
from ad_name_parser import get_title, extract_material_name, filter_exact_material
from notion_client import NotionClient

logger = logging.getLogger(__name__)


def _get_property_checkbox(properties: dict, prop_name: str) -> bool:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "checkbox":
        return prop.get("checkbox", False)
    return False


def _get_property_select(properties: dict, prop_name: str) -> str:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    return ""


async def handle_checkbox_event(payload: dict, notion: NotionClient) -> dict:
    try:
        page_id = payload.get("entity", {}).get("id", "")

        if not page_id:
            logger.info("Flow 2: page_id missing, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 2: 페이지가 세팅 리스트 DB 소속인지 확인
        page = await notion.get_page(page_id)
        parent = page.get("parent", {})
        parent_ds_id = (parent.get("data_source_id") or parent.get("database_id") or "").replace("-", "")

        if parent_ds_id != SETTING_LIST_DB_ID:
            logger.info(f"Flow 2: page {page_id} not in setting list DB, skipping")
            return {"status": "ok", "action": "skipped"}

        properties = page.get("properties", {})

        # Step 3: "진행중" 체크박스 확인
        if not _get_property_checkbox(properties, "진행중"):
            logger.info(f"Flow 2: checkbox not checked for page {page_id}, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 3.5: 브랜드 값 확인
        brand = _get_property_select(properties, "브랜드")
        if not brand:
            logger.info(f"Flow 2: no brand for page {page_id}, skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 4: 소재이름 추출
        ad_name = get_title(page)
        if not ad_name:
            logger.info(f"Flow 2: ad_name empty for page {page_id}, skipping")
            return {"status": "ok", "action": "skipped"}

        material_name = extract_material_name(ad_name)
        if not material_name:
            logger.info(f"Flow 2: could not extract material name from '{ad_name}', skipping")
            return {"status": "ok", "action": "skipped"}

        logger.info(f"Flow 2: material_name='{material_name}', brand='{brand}'")

        # Step 5: 대상 데이터소스 결정
        datasource_id = BRAND_DS_ID_MAP.get(brand)
        if not datasource_id:
            logger.info(f"Flow 2: no datasource for brand '{brand}', skipping")
            return {"status": "ok", "action": "skipped"}

        # Step 6: 소재이름 기반 1차 조회 (contains)
        filter_body = {
            "filter": {
                "and": [
                    {"property": "광고명", "title": {"contains": material_name}},
                    {
                        "or": [
                            {"property": "상태", "status": {"equals": s}}
                            for s in QUERY_STATUSES
                        ]
                    },
                ]
            }
        }

        candidates = await notion.query_database_all(datasource_id, filter_body)
        logger.info(f"Flow 2: {len(candidates)} candidates from contains query")

        # Step 6.5: 정밀 필터링
        exact_matches = filter_exact_material(candidates, material_name)
        logger.info(f"Flow 2: {len(exact_matches)} exact matches after filtering")

        if not exact_matches:
            return {"status": "ok", "action": "no_matches", "updated_count": 0}

        # Step 7: 상태 일괄 변경
        updated_count = 0
        for match_page in exact_matches:
            match_id = match_page["id"]
            match_name = get_title(match_page)
            try:
                await notion.update_page(match_id, {"상태": {"status": {"name": TARGET_STATUS}}})
                updated_count += 1
                logger.info(f"Flow 2: updated '{match_name}' → {TARGET_STATUS}")
                await asyncio.sleep(0.4)
            except Exception as e:
                logger.error(f"Flow 2: failed to update page {match_id}: {e}")

        logger.info(f"Flow 2: done. {updated_count}/{len(exact_matches)} pages updated")
        return {"status": "ok", "action": "status_updated", "updated_count": updated_count}

    except Exception as e:
        logger.error(f"Flow 2 error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}
