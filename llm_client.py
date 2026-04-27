import logging
import os

from google import genai

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


class LLMClient:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY", "")
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
                model="gemini-2.0-flash-lite",
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
                model="gemini-2.0-flash",
                contents=prompt,
            )
            return response.text.strip()
        except Exception as e:
            logger.error(f"LLM extract_material_name error: {e}")
            return ""
