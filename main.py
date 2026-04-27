from fastapi import FastAPI, Request
app = FastAPI()

@app.post("/notion-webhook")
async def handle_notion(request: Request):
    payload = await request.json()
    # 여기서 이벤트 처리
    print(payload)
    return {"status": "ok"}
