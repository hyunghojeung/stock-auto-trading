"""매매 기록 API"""
from fastapi import APIRouter, Query
from app.core.database import db
from datetime import date

router = APIRouter(prefix="/api/trades", tags=["매매"])

@router.get("/")
async def get_trades(strategy_id: int = None, start_date: str = None, end_date: str = None, limit: int = 50):
    q = db.table("trades").select("*").order("traded_at", desc=True).limit(limit)
    if strategy_id: q = q.eq("strategy_id", strategy_id)
    if start_date: q = q.gte("traded_at", f"{start_date}T00:00:00")
    if end_date: q = q.lte("traded_at", f"{end_date}T23:59:59")
    return q.execute().data or []

@router.get("/today")
async def today_trades(strategy_id: int = None):
    today = date.today().isoformat()
    q = db.table("trades").select("*").gte("traded_at", f"{today}T00:00:00").order("traded_at", desc=True)
    if strategy_id: q = q.eq("strategy_id", strategy_id)
    return q.execute().data or []
