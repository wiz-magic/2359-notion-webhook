import logging
import os
import json

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

INTENT_PROMPT = """다음 댓글이 구글 애즈 비승인 소재의 "수정 완료"를 알리는 의도인지 판단해줘.
정말로 수정 작업이 완료되었음을 의미하는 댓글이면 "yes", 아니면 "no"로만 답해줘.

댓글: "{comment_text}"
"""

FALLBACK_PARSER_PROMPT = """다음 구글 애즈 광고명에서 소재이름을 추출해줘.
광고명은 _ 로 구분된 여러 파트로 이루어져 있고, 두 번째 파트의 끝에 있는 숫자(인트로번호)를 제거한 것이 소재이름이다.
소재이름만 출력하고, 다른 설명은 하지 마.

예시:
입력: v9204_설구절절성(프)(유)1_Msj9_Afr(CYE)_26.02.09
출력: 설구절절성(프)(유)

입력: v1620_(살)애덤헬창1_Mdy3_Afr(KMB)_26.04.20
출력: (살)애덤헬창

광고명: "{ad_name}"
"""

AD_SELECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_page_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "진행 중으로 변경해야 하는 Notion 후보 page_id 목록",
        },
        "confidence": {
            "type": "number",
            "description": "판단 신뢰도. 0.0은 전혀 확신 없음, 1.0은 매우 확신.",
        },
        "needs_fallback": {
            "type": "boolean",
            "description": "더 강한 모델의 재판정이 필요하면 true",
        },
        "reason": {
            "type": "string",
            "description": "선택 또는 보류 이유를 한 문장으로 설명",
        },
    },
    "required": ["selected_page_ids", "confidence", "needs_fallback", "reason"],
}

AD_SELECTION_PROMPT = """너는 구글 애즈 비승인 소재 운영 자동화의 후보 선택 검수자다.

목표:
- 세팅 리스트에서 "진행중" 체크박스가 켜진 항목을 보고, 후보 광고 중 어떤 Notion 페이지의 상태를 "진행 중"으로 바꿔야 하는지 고른다.
- 후보가 여러 광고그룹/캠페인에 걸쳐 있으면 세팅 리스트의 캠페인명/광고그룹명/소재명과 가장 같은 업로드 맥락의 후보만 고른다.

판단 규칙:
- selected_page_ids에는 반드시 후보 목록에 있는 page_id만 넣어라.
- 소재명이 다르거나 캠페인/광고그룹 맥락이 다른 후보는 넣지 마라.
- 같은 업로드 맥락의 후보가 여러 개라면 모두 고를 수 있다.
- 판단이 애매하면 selected_page_ids를 빈 배열로 두고 confidence를 낮게, needs_fallback을 true로 둔다.
- 설명은 짧게 작성하라.

세팅 리스트 항목:
{setting_context}

후보 광고 목록:
{candidates_context}
"""


class LLMClient:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY", "")
        self.flash_lite_model = os.environ.get(
            "GEMINI_FLASH_LITE_MODEL",
            "gemini-3.1-flash-lite-preview",
        )
        self.flash_model = os.environ.get(
            "GEMINI_FLASH_MODEL",
            "gemini-3-flash-preview",
        )
        if api_key:
            self.client = genai.Client(api_key=api_key)
            self.enabled = True
            logger.info("LLM client initialized (Gemini)")
        else:
            self.enabled = False
            self.client = None
            logger.warning("GEMINI_API_KEY not set, LLM features disabled")

    async def classify_comment_intent(self, comment_text: str) -> bool:
        """댓글이 '수정 완료' 의도인지 판단. True/False 반환."""
        if not self.enabled:
            return False
        try:
            prompt = INTENT_PROMPT.format(comment_text=comment_text)
            response = await self.client.aio.models.generate_content(
                model=self.flash_lite_model,
                contents=prompt,
            )
            answer = response.text.strip().lower()
            return answer == "yes"
        except Exception as e:
            logger.error(f"LLM classify error: {e}")
            return False

    async def extract_material_name_fallback(self, ad_name: str) -> str:
        """규칙 기반 파싱 실패 시 LLM으로 소재이름 추출."""
        if not self.enabled:
            return ""
        try:
            prompt = FALLBACK_PARSER_PROMPT.format(ad_name=ad_name)
            response = await self.client.aio.models.generate_content(
                model=self.flash_model,
                contents=prompt,
            )
            return response.text.strip()
        except Exception as e:
            logger.error(f"LLM extract_material_name error: {e}")
            return ""

    async def select_ad_candidates(
        self,
        setting_context: dict,
        candidates: list[dict],
        use_flash: bool = False,
    ) -> dict:
        """후보 광고 중 상태 변경 대상을 고른다."""
        if not self.enabled:
            return {
                "selected_page_ids": [],
                "confidence": 0.0,
                "needs_fallback": False,
                "reason": "LLM features disabled",
                "model": "",
            }

        model = self.flash_model if use_flash else self.flash_lite_model
        try:
            prompt = AD_SELECTION_PROMPT.format(
                setting_context=json.dumps(setting_context, ensure_ascii=False, indent=2),
                candidates_context=json.dumps(candidates, ensure_ascii=False, indent=2),
            )
            response = await self.client.aio.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    response_mime_type="application/json",
                    response_schema=AD_SELECTION_SCHEMA,
                ),
            )
            result = json.loads(response.text)
            selected_page_ids = result.get("selected_page_ids", [])
            if not isinstance(selected_page_ids, list):
                selected_page_ids = []
            result["selected_page_ids"] = [
                str(page_id) for page_id in selected_page_ids
            ]
            try:
                result["confidence"] = float(result.get("confidence", 0.0))
            except (TypeError, ValueError):
                result["confidence"] = 0.0
            result["needs_fallback"] = bool(result.get("needs_fallback", False))
            result["reason"] = str(result.get("reason", ""))
            result["model"] = model
            return result
        except Exception as e:
            logger.error(f"LLM select_ad_candidates error ({model}): {e}")
            return {
                "selected_page_ids": [],
                "confidence": 0.0,
                "needs_fallback": True,
                "reason": str(e),
                "model": model,
            }
