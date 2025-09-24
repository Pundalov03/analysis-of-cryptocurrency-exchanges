from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from .routes.ws import router as ws_router
from .routes.test import router as test_router

app = FastAPI(title='CryptoCurrency Exchanges API')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:8000'],
)

app.include_router(ws_router)
app.include_router(test_router)

@app.get("/tests")
async def test_endpoint():
    return {
        "message": "FastAPI is working",
        "endpoints_updated": True,
        'date': datetime.now(),
    }