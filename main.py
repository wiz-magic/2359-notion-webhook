from fastapi import FastAPI, Request
app = FastAPI()

async def _handle_notion_payload(request: Request):
    payload = await request.json()
    # 여기서 이벤트 처리
    print(payload)
    return {"status": "ok"}


@app.post("/")
async def handle_notion_root(request: Request):
    return await _handle_notion_payload(request)


@app.post("/notion-webhook")
async def handle_notion(request: Request):
    return await _handle_notion_payload(request)
