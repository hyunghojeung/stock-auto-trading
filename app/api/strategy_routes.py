"""전략 관리 API"""
from fastapi import APIRouter, HTTPException
from app.core.database import db
from app.core.config import config

router = APIRouter(prefix="/api/strategy", tags=["전략"])

@router.get("/")
async def get_strategies():
    return db.table("strategies").select("*").order("id").execute().data or []

@router.get("/{sid}")
async def get_strategy(sid: int):
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")
    return r.data[0]

@router.post("/{sid}/toggle-live")
async def toggle_live(sid: int, password: str = ""):
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("is_live").eq("id", sid).execute()
    if not r.data: raise HTTPException(404)
    current = r.data[0]["is_live"]
    if not current:
        # 다른 실전 전략 확인
        live = db.table("strategies").select("id").eq("is_live", True).execute()
        if live.data:
            raise HTTPException(400, "이미 실전 매매 중인 전략이 있습니다")
    db.table("strategies").update({"is_live": not current}).eq("id", sid).execute()
    return {"is_live": not current}

@router.get("/compare/all")
async def compare_strategies():
    strategies = db.table("strategies").select("*").eq("is_active", True).execute().data or []
    result = []
    for s in strategies:
        sid = s["id"]
        asset = db.table("asset_history").select("*").eq("strategy_id", sid).order("record_date", desc=True).limit(1).execute()
        daily = db.table("daily_reports").select("*").eq("strategy_id", sid).order("report_date", desc=True).limit(30).execute()
        sells = db.table("trades").select("net_profit").eq("strategy_id", sid).eq("trade_type", "sell").execute()

        total_trades = len(sells.data) if sells.data else 0
        wins = len([t for t in (sells.data or []) if (t.get("net_profit") or 0) > 0])
        total_profit = sum(t.get("net_profit", 0) for t in (sells.data or []))

        result.append({
            "strategy": s,
            "total_asset": asset.data[0]["total_asset"] if asset.data else s["initial_capital"],
            "total_profit": total_profit,
            "total_trades": total_trades,
            "win_rate": round(wins/total_trades*100, 2) if total_trades > 0 else 0,
            "daily_reports": daily.data or [],
        })
    return result
