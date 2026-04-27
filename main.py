from fastapi import FastAPI, Request
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.post("/")
async def handle_notion(request: Request):
    payload = await request.json()

    # Handle Notion's webhook verification challenge
    if "challenge" in payload:
        verification_token = payload["challenge"]
        logger.info(f"Notion verification_token (challenge): {verification_token}")
        print(f"[VERIFICATION] challenge token: {verification_token}")
        return {"challenge": verification_token}

    # General event handling for future webhook events
    logger.info(f"Received Notion webhook event: {payload}")
    print(payload)
    return {"status": "ok"}
