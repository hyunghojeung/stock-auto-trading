"""시세 조회 API"""
from fastapi import APIRouter
from app.services.kis_stock import get_current_price, get_daily_candles, get_minute_candles
from app.utils.kr_holiday import get_market_status

router = APIRouter(prefix="/api/stock", tags=["시세"])

@router.get("/price/{code}")
async def stock_price(code: str):
    return get_current_price(code) or {"error": "조회 실패"}

@router.get("/daily/{code}")
async def daily_chart(code: str, period: int = 30):
    return get_daily_candles(code, period)

@router.get("/minute/{code}")
async def minute_chart(code: str, count: int = 30):
    return get_minute_candles(code, count)

@router.get("/market-status")
async def market_status():
    return {"status": get_market_status()}
