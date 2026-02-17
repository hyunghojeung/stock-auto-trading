"""FastAPI 메인 앱"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic
from contextlib import asynccontextmanager
from app.core.config import config
from app.core.scheduler import setup_scheduler
from app.api import stock_routes, trade_routes, portfolio_routes, watchlist_routes, strategy_routes
from app.utils.kr_holiday import get_market_status
from datetime import datetime

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_scheduler()
    print("[서버] 10억 만들기 자동매매 서버 시작")
    yield
    print("[서버] 서버 종료")

app = FastAPI(title="10억 만들기 - 주식 자동매매", version="1.0.0", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# API 라우터 등록
app.include_router(stock_routes.router)
app.include_router(trade_routes.router)
app.include_router(portfolio_routes.router)
app.include_router(watchlist_routes.router)
app.include_router(strategy_routes.router)

@app.get("/")
async def root():
    return {"name": "10억 만들기", "status": "running", "market": get_market_status()}

@app.post("/api/auth")
async def authenticate(password: str):
    if password == config.SITE_PASSWORD:
        return {"authenticated": True}
    raise HTTPException(403, "비밀번호가 틀렸습니다")

@app.get("/api/system/status")
async def system_status():
    now = datetime.now()
    return {
        "datetime": now.isoformat(),
        "date_kr": now.strftime("%Y년 %m월 %d일 (%a)"),
        "time_kr": now.strftime("%H:%M:%S"),
        "market_status": get_market_status(now),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORT)
