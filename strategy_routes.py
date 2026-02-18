"""전략 관리 API (눌림목 + 갭상승전략 통합)"""
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
        live = db.table("strategies").select("id").eq("is_live", True).execute()
        if live.data:
            raise HTTPException(400, "이미 실전 매매 중인 전략이 있습니다")
    db.table("strategies").update({"is_live": not current}).eq("id", sid).execute()
    return {"is_live": not current}

@router.get("/compare/all")
async def compare_strategies():
    strategies = db.table("strategies").select("*").execute().data or []
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
            "total_asset": asset.data[0]["total_asset"] if asset.data else s.get("initial_capital", 1000000),
            "total_profit": total_profit,
            "total_trades": total_trades,
            "win_rate": round(wins/total_trades*100, 2) if total_trades > 0 else 0,
            "daily_reports": daily.data or [],
        })
    return result

# ============================================================
# 갭상승전략 실시간 상태 API (신규)
# ============================================================

@router.get("/gap/status")
async def get_gap_status():
    """갭상승전략 현재 상태 조회"""
    try:
        from app.engine.gap_scheduler import gap_phase, gap_filtered_stocks, gap_holdings
        return {
            "phase": gap_phase,
            "filtered_count": len(gap_filtered_stocks),
            "holdings_count": len(gap_holdings),
            "filtered_stocks": [
                {
                    "code": s.get("code", ""),
                    "name": s.get("name", ""),
                    "gap_pct": s.get("gap_pct", 0),
                    "gap_type": s.get("gap_type", ""),
                }
                for s in gap_filtered_stocks
            ],
            "holdings": [
                {
                    "code": h.get("code", ""),
                    "name": h.get("name", ""),
                    "strategy": h.get("strategy", ""),
                    "entry_price": h.get("entry_price", 0),
                    "quantity": h.get("quantity", 0),
                }
                for h in gap_holdings.values()
            ],
        }
    except Exception as e:
        return {
            "phase": "idle",
            "filtered_count": 0,
            "holdings_count": 0,
            "filtered_stocks": [],
            "holdings": [],
            "error": str(e),
        }
