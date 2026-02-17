"""감시종목 API"""
from fastapi import APIRouter
from app.core.database import db
from datetime import date

router = APIRouter(prefix="/api/watchlist", tags=["감시종목"])

@router.get("/")
async def get_watchlist():
    today = date.today().isoformat()
    return db.table("watchlist").select("*").eq("scan_date", today).order("score", desc=True).execute().data or []

@router.get("/blocked")
async def get_blocked():
    from datetime import datetime
    now = datetime.now().isoformat()
    return db.table("blocked_stocks").select("*").gt("unblock_at", now).execute().data or []
