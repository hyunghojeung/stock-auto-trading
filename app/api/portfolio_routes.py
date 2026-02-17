"""보유종목 / 수익 API"""
from fastapi import APIRouter
from app.core.database import db
from datetime import date

router = APIRouter(prefix="/api/portfolio", tags=["포트폴리오"])

@router.get("/holdings")
async def get_holdings(strategy_id: int = None):
    q = db.table("holdings").select("*")
    if strategy_id: q = q.eq("strategy_id", strategy_id)
    return q.execute().data or []

@router.get("/daily-report")
async def daily_report(strategy_id: int = None, days: int = 30):
    q = db.table("daily_reports").select("*").order("report_date", desc=True).limit(days)
    if strategy_id: q = q.eq("strategy_id", strategy_id)
    return q.execute().data or []

@router.get("/asset-history")
async def asset_history(strategy_id: int = None):
    q = db.table("asset_history").select("*").order("record_date", desc=True)
    if strategy_id: q = q.eq("strategy_id", strategy_id)
    return q.execute().data or []

@router.get("/summary")
async def portfolio_summary(strategy_id: int = None):
    holdings = db.table("holdings").select("*")
    if strategy_id: holdings = holdings.eq("strategy_id", strategy_id)
    holdings = holdings.execute().data or []

    total_invested = sum(h["buy_price"] * h["quantity"] for h in holdings)
    total_current = sum((h.get("current_price") or h["buy_price"]) * h["quantity"] for h in holdings)
    unrealized = total_current - total_invested

    sells = db.table("trades").select("net_profit").eq("trade_type", "sell")
    if strategy_id: sells = sells.eq("strategy_id", strategy_id)
    sells = sells.execute().data or []
    realized = sum(s.get("net_profit", 0) for s in sells)

    return {
        "holdings_count": len(holdings),
        "total_invested": total_invested,
        "total_current": total_current,
        "unrealized_profit": unrealized,
        "realized_profit": realized,
        "total_profit": realized + unrealized,
    }
