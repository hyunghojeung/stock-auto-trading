"""전략 관리 API (눌림목 + 갭상승전략 통합 + 실전/모의 매매 제어)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v2: 전략 CRUD + 매매 시작/중지 + 활성화 토글 추가
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.core.database import db
from app.core.config import config

router = APIRouter(prefix="/api/strategy", tags=["전략"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class StrategyCreate(BaseModel):
    name: str
    is_live: bool = False           # False=모의투자, True=실전투자
    initial_capital: int = 1_000_000
    stop_loss_pct: float = -3.0
    atr_multiplier: float = 2.0
    description: str = ""

class StrategyUpdate(BaseModel):
    name: Optional[str] = None
    initial_capital: Optional[int] = None
    stop_loss_pct: Optional[float] = None
    atr_multiplier: Optional[float] = None
    description: Optional[str] = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/")
async def get_strategies():
    return db.table("strategies").select("*").order("id").execute().data or []

@router.get("/{sid}")
async def get_strategy(sid: int):
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")
    return r.data[0]

@router.post("/")
async def create_strategy(req: StrategyCreate, password: str = ""):
    """전략 생성 (모의투자 or 실전투자)"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    # 실전투자 전략 생성 시 기존 실전 전략 확인
    if req.is_live:
        live = db.table("strategies").select("id,name").eq("is_live", True).execute()
        if live.data:
            raise HTTPException(400, f"이미 실전 매매 중인 전략이 있습니다: {live.data[0]['name']}")

    data = {
        "name": req.name,
        "is_active": False,  # 생성 시 비활성 (수동으로 시작)
        "is_live": req.is_live,
        "initial_capital": req.initial_capital,
        "stop_loss_pct": req.stop_loss_pct,
        "atr_multiplier": req.atr_multiplier,
        "description": req.description,
    }
    resp = db.table("strategies").insert(data).execute()
    if not resp.data:
        raise HTTPException(500, "전략 생성 실패")
    return resp.data[0]

@router.put("/{sid}")
async def update_strategy(sid: int, req: StrategyUpdate, password: str = ""):
    """전략 수정"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")

    update_data = {k: v for k, v in req.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(400, "수정할 내용이 없습니다")

    db.table("strategies").update(update_data).eq("id", sid).execute()
    updated = db.table("strategies").select("*").eq("id", sid).execute()
    return updated.data[0]

@router.delete("/{sid}")
async def delete_strategy(sid: int, password: str = ""):
    """전략 삭제 (보유종목이 있으면 삭제 불가)"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")

    # 보유종목 확인
    holdings = db.table("holdings").select("id").eq("strategy_id", sid).execute()
    if holdings.data:
        raise HTTPException(400, f"보유종목 {len(holdings.data)}개가 있어 삭제할 수 없습니다. 먼저 전량 매도하세요.")

    db.table("strategies").delete().eq("id", sid).execute()
    return {"message": f"전략 {sid} 삭제 완료"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 매매 시작/중지 (is_active 토글)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/{sid}/start")
async def start_trading(sid: int, password: str = ""):
    """매매 시작 (is_active=True) — 스케줄러가 이 전략을 매매 대상에 포함"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")
    strategy = r.data[0]

    if strategy.get("is_active"):
        return {"message": "이미 매매 중입니다", "is_active": True, "is_live": strategy["is_live"]}

    # 실전투자 시작 시 KIS API Key 확인
    if strategy.get("is_live"):
        if not config.KIS_LIVE_APP_KEY or not config.KIS_LIVE_APP_SECRET:
            raise HTTPException(400, "실전투자 API Key가 설정되지 않았습니다. 환경변수 KIS_LIVE_APP_KEY/SECRET을 확인하세요.")
        if not config.KIS_CANO:
            raise HTTPException(400, "계좌번호(KIS_CANO)가 설정되지 않았습니다.")
    else:
        if not config.KIS_APP_KEY or not config.KIS_APP_SECRET:
            raise HTTPException(400, "모의투자 API Key가 설정되지 않았습니다. 환경변수 KIS_APP_KEY/SECRET을 확인하세요.")

    db.table("strategies").update({"is_active": True}).eq("id", sid).execute()

    mode = "실전" if strategy["is_live"] else "모의"
    return {
        "message": f"{strategy['name']} {mode}투자 매매 시작",
        "is_active": True,
        "is_live": strategy["is_live"],
    }

@router.post("/{sid}/stop")
async def stop_trading(sid: int, password: str = ""):
    """매매 중지 (is_active=False) — 보유종목은 유지, 신규매매만 중단"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404, "전략을 찾을 수 없습니다")
    strategy = r.data[0]

    if not strategy.get("is_active"):
        return {"message": "이미 중지 상태입니다", "is_active": False}

    db.table("strategies").update({"is_active": False}).eq("id", sid).execute()

    holdings = db.table("holdings").select("id").eq("strategy_id", sid).execute()
    holdings_count = len(holdings.data) if holdings.data else 0

    return {
        "message": f"{strategy['name']} 매매 중지 (보유종목 {holdings_count}개 유지)",
        "is_active": False,
        "holdings_count": holdings_count,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 실전/모의 전환
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/{sid}/toggle-live")
async def toggle_live(sid: int, password: str = ""):
    """실전↔모의 전환 (매매 중지 상태에서만 가능)"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    r = db.table("strategies").select("*").eq("id", sid).execute()
    if not r.data: raise HTTPException(404)
    strategy = r.data[0]

    # 매매 중일 때는 전환 불가
    if strategy.get("is_active"):
        raise HTTPException(400, "매매 진행 중에는 모드를 전환할 수 없습니다. 먼저 매매를 중지하세요.")

    current = strategy["is_live"]
    if not current:
        live = db.table("strategies").select("id,name").eq("is_live", True).execute()
        if live.data:
            raise HTTPException(400, f"이미 실전 매매 전략이 있습니다: {live.data[0]['name']}")

    db.table("strategies").update({"is_live": not current}).eq("id", sid).execute()
    new_mode = "실전투자" if not current else "모의투자"
    return {"is_live": not current, "message": f"{new_mode}로 전환 완료"}

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
