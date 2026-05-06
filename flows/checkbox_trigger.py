import asyncio
import logging
import re
from typing import Any

from config import (
    SETTING_LIST_DB_ID,
    BRAND_DS_ID_MAP,
    RECONCILE_CHECKED_SETTINGS_LIMIT,
    TARGET_STATUS,
    QUERY_STATUSES,
)
from ad_name_parser import get_title, extract_material_name, filter_exact_material
from llm_client import LLMClient
from notion_client import NotionClient

logger = logging.getLogger(__name__)

LLM_CONFIDENCE_THRESHOLD = 0.7
NOTION_PAGE_ID_PATTERN = re.compile(r"([0-9a-fA-F]{32})(?:\?|$)")


def _get_property_checkbox(properties: dict, prop_name: str) -> bool:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "checkbox":
        return prop.get("checkbox", False)
    return False


def _normalize_notion_id(value: str | None) -> str:
    return (value or "").replace("-", "")


def _format_notion_id(value: str) -> str:
    raw = _normalize_notion_id(value)
    if len(raw) != 32:
        return value
    return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"


def _extract_page_id_from_url(url: str) -> str:
    match = NOTION_PAGE_ID_PATTERN.search(url or "")
    if not match:
        return ""
    return _format_notion_id(match.group(1))


def _get_property_select(properties: dict, prop_name: str) -> str:
    prop = properties.get(prop_name, {})
    if prop.get("type") == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    return ""


def _get_property_text(properties: dict, prop_name: str) -> str:
    prop = properties.get(prop_name, {})
    prop_type = prop.get("type")
    if prop_type == "title":
        return "".join(rt.get("plain_text", "") for rt in prop.get("title", []))
    if prop_type == "rich_text":
        return "".join(rt.get("plain_text", "") for rt in prop.get("rich_text", []))
    if prop_type == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if prop_type == "status":
        status = prop.get("status")
        return status.get("name", "") if status else ""
    if prop_type == "multi_select":
        return ", ".join(item.get("name", "") for item in prop.get("multi_select", []))
    if prop_type == "url":
        return prop.get("url") or ""
    if prop_type == "number":
        value = prop.get("number")
        return "" if value is None else str(value)
    return ""


def _build_setting_context(
    page_id: str,
    page: dict,
    brand: str,
    ad_name: str,
    material_name: str,
) -> dict:
    properties = page.get("properties", {})
    return {
        "page_id": page_id,
        "url": page.get("url", ""),
        "brand": brand,
        "setting_ad_name": ad_name,
        "material_name": material_name,
        "campaign_name": _get_property_text(properties, "캠페인명"),
        "ad_group_name": _get_property_text(properties, "광고그룹명"),
        "ad_id_list": _get_property_text(properties, "광고 ID 목록"),
    }


def _build_candidate_context(page: dict) -> dict:
    properties = page.get("properties", {})
    return {
        "page_id": page.get("id", ""),
        "url": page.get("url", ""),
        "ad_name": get_title(page),
        "material_name": extract_material_name(get_title(page)),
        "campaign_name": _get_property_text(properties, "캠페인명"),
        "ad_group_name": _get_property_text(properties, "광고그룹명"),
        "status": _get_property_text(properties, "상태"),
        "ad_id_list": _get_property_text(properties, "광고 ID 목록"),
    }


def _validate_selected_pages(selection: dict[str, Any], pages: list[dict]) -> list[dict]:
    page_by_id = {page.get("id", ""): page for page in pages}
    selected_ids = selection.get("selected_page_ids", [])
    return [
        page_by_id[page_id]
        for page_id in selected_ids
        if page_id in page_by_id
    ]


async def _select_matches_with_llm(
    llm: LLMClient | None,
    setting_context: dict,
    match_pages: list[dict],
) -> tuple[list[dict], dict]:
    if not llm or not llm.enabled:
        return [], {
            "model": "",
            "confidence": 0.0,
            "needs_fallback": False,
            "reason": "Gemini disabled",
        }

    candidate_contexts = [_build_candidate_context(page) for page in match_pages]
    lite_selection = await llm.select_ad_candidates(
        setting_context,
        candidate_contexts,
        use_flash=False,
    )
    lite_matches = _validate_selected_pages(lite_selection, match_pages)

    if (
        lite_matches
        and not lite_selection.get("needs_fallback")
        and lite_selection.get("confidence", 0.0) >= LLM_CONFIDENCE_THRESHOLD
    ):
        return lite_matches, lite_selection

    flash_selection = await llm.select_ad_candidates(
        setting_context,
        candidate_contexts,
        use_flash=True,
    )
    flash_matches = _validate_selected_pages(flash_selection, match_pages)
    return flash_matches, flash_selection


async def handle_checkbox_event(
    payload: dict,
    notion: NotionClient,
    llm: LLMClient | None = None,
) -> dict:
    try:
        page_id = payload.get("entity", {}).get("id", "")

        if not page_id:
            logger.info("Flow 2: page_id missing, skipping")
            return {"status": "ok", "action": "skipped"}

        parent_ds_id = _normalize_notion_id(
            payload.get("data", {}).get("parent", {}).get("data_source_id")
        )
        if parent_ds_id and parent_ds_id != _normalize_notion_id(SETTING_LIST_DB_ID):
            logger.info(
                "Flow 2: parent data source is not setting list, skipping "
                "(parent_ds_id=%s)",
                parent_ds_id,
            )
            return {"status": "ok", "action": "skipped"}

        return await process_setting_page(page_id, notion, llm)

    except Exception as e:
        logger.error(f"Flow 2 error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}


async def process_setting_page(
    page_id: str,
    notion: NotionClient,
    llm: LLMClient | None = None,
    page: dict | None = None,
) -> dict:
    # Step 2: 페이지가 세팅 리스트 DB 소속인지 확인
    page = page or await notion.get_page(page_id)
    parent = page.get("parent", {})
    parent_ds_id = _normalize_notion_id(
        parent.get("data_source_id") or parent.get("database_id")
    )

    if parent_ds_id != _normalize_notion_id(SETTING_LIST_DB_ID):
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

    source_result = await _update_source_page_from_setting_link(
        notion,
        properties,
        datasource_id,
        ad_name,
    )
    if source_result:
        return source_result

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

    selected_matches = [match for match in exact_matches if get_title(match) == ad_name]
    llm_selection = None

    if selected_matches:
        logger.info(
            "Flow 2: selected %s exact ad-name matches before LLM",
            len(selected_matches),
        )
    else:
        setting_context = _build_setting_context(
            page_id,
            page,
            brand,
            ad_name,
            material_name,
        )
        selected_matches, llm_selection = await _select_matches_with_llm(
            llm,
            setting_context,
            exact_matches,
        )
        logger.info(
            "Flow 2: LLM selection model=%s confidence=%s selected=%s reason=%s",
            llm_selection.get("model", "") if llm_selection else "",
            llm_selection.get("confidence", 0.0) if llm_selection else 0.0,
            len(selected_matches),
            llm_selection.get("reason", "") if llm_selection else "",
        )
        if not selected_matches:
            return {
                "status": "ok",
                "action": "ambiguous_no_selection",
                "updated_count": 0,
                "candidate_count": len(exact_matches),
                "llm_selection": llm_selection,
            }

    # Step 7: 상태 일괄 변경
    updated_count = 0
    for match_page in selected_matches:
        match_id = match_page["id"]
        match_name = get_title(match_page)
        try:
            await notion.update_page(match_id, {"상태": {"status": {"name": TARGET_STATUS}}})
            updated_count += 1
            logger.info(f"Flow 2: updated '{match_name}' -> {TARGET_STATUS}")
            await asyncio.sleep(0.4)
        except Exception as e:
            logger.error(f"Flow 2: failed to update page {match_id}: {e}")

    logger.info(f"Flow 2: done. {updated_count}/{len(selected_matches)} pages updated")
    return {
        "status": "ok",
        "action": "status_updated",
        "updated_count": updated_count,
        "candidate_count": len(exact_matches),
        "selected_count": len(selected_matches),
        "llm_selection": llm_selection,
    }


async def _update_source_page_from_setting_link(
    notion: NotionClient,
    setting_properties: dict,
    datasource_id: str,
    ad_name: str,
) -> dict | None:
    source_page_id = _extract_page_id_from_url(
        _get_property_text(setting_properties, "원본 페이지 링크")
    )
    if not source_page_id:
        return None

    try:
        source_page = await notion.get_page(source_page_id)
    except Exception as e:
        logger.warning(
            "Flow 2: source page lookup failed; falling back to search "
            "(source_page_id=%s): %s",
            source_page_id,
            e,
        )
        return None

    source_parent = source_page.get("parent", {})
    source_parent_ds_id = _normalize_notion_id(
        source_parent.get("data_source_id") or source_parent.get("database_id")
    )
    if source_parent_ds_id != _normalize_notion_id(datasource_id):
        logger.info(
            "Flow 2: source page parent mismatch; falling back to search "
            "(source_parent_ds_id=%s, expected=%s)",
            source_parent_ds_id,
            datasource_id,
        )
        return None

    source_title = get_title(source_page)
    if source_title != ad_name:
        logger.info(
            "Flow 2: source page title mismatch; falling back to search "
            "(source_title=%s, setting_title=%s)",
            source_title,
            ad_name,
        )
        return None

    source_status = _get_property_text(source_page.get("properties", {}), "상태")
    if source_status == TARGET_STATUS:
        logger.info("Flow 2: source page already %s: %s", TARGET_STATUS, source_title)
        return {
            "status": "ok",
            "action": "already_updated",
            "updated_count": 0,
            "candidate_count": 1,
            "selected_count": 1,
            "llm_selection": None,
        }
    if source_status not in QUERY_STATUSES:
        logger.info(
            "Flow 2: source page status '%s' is not actionable for %s",
            source_status,
            source_title,
        )
        return {
            "status": "ok",
            "action": "source_status_not_actionable",
            "updated_count": 0,
            "candidate_count": 1,
            "selected_count": 0,
            "llm_selection": None,
        }

    await notion.update_page(source_page_id, {"상태": {"status": {"name": TARGET_STATUS}}})
    logger.info("Flow 2: updated source page '%s' -> %s", source_title, TARGET_STATUS)
    return {
        "status": "ok",
        "action": "status_updated",
        "updated_count": 1,
        "candidate_count": 1,
        "selected_count": 1,
        "llm_selection": None,
    }


async def reconcile_checked_settings(
    notion: NotionClient,
    llm: LLMClient | None = None,
    limit: int = RECONCILE_CHECKED_SETTINGS_LIMIT,
) -> dict:
    checked_pages = await notion.query_database_all(
        SETTING_LIST_DB_ID,
        {
            "filter": {"property": "진행중", "checkbox": {"equals": True}},
            "page_size": max(1, min(limit, 100)),
        },
    )
    checked_pages = checked_pages[:limit]
    logger.info("Flow 2 reconcile: %s checked setting rows found", len(checked_pages))

    results = []
    updated_count = 0
    for page in checked_pages:
        result = await process_setting_page(
            page.get("id", ""),
            notion,
            llm,
            page=page,
        )
        results.append(
            {
                "page_id": page.get("id", ""),
                "title": get_title(page),
                "action": result.get("action"),
                "updated_count": result.get("updated_count", 0),
                "selected_count": result.get("selected_count", 0),
            }
        )
        updated_count += int(result.get("updated_count", 0) or 0)

    return {
        "status": "ok",
        "action": "reconciled_checked_settings",
        "checked_count": len(checked_pages),
        "updated_count": updated_count,
        "results": results,
    }
