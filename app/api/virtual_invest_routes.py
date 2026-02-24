"""
가상투자 시뮬레이터 API 라우트 / Virtual Investment API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/virtual_invest_routes.py

엔드포인트:
  POST /api/virtual-invest/compare     — 5가지 전략 동시 비교 (백테스트)
  POST /api/virtual-invest/start       — 실시간 모의투자 시작
  GET  /api/virtual-invest/status      — 실시간 모의투자 현황
  POST /api/virtual-invest/update      — 실시간 포지션 업데이트 (장 마감 후)
  GET  /api/virtual-invest/presets     — 프리셋 목록 조회
"""

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Optional
import logging

from app.services.virtual_invest import (
    run_comparison,
    start_realtime,
    update_realtime,
    get_realtime_status,
    STRATEGY_PRESETS,
    DEFAULT_CAPITAL,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/virtual-invest", tags=["virtual-invest"])

# Supabase 연결 (선택적)
try:
    from app.core.config import config
    from supabase import create_client
    supabase = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
except Exception:
    supabase = None
    logger.warning("[가상투자] Supabase 연결 실패 — DB 없이 동작")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Request / Response 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class StockInput(BaseModel):
    code: str
    name: str = ""
    buy_price: float = 0
    signal_date: str = ""


class CustomParams(BaseModel):
    take_profit_pct: float = 7.0
    stop_loss_pct: float = 3.0
    max_hold_days: int = 10


class CompareRequest(BaseModel):
    stocks: List[StockInput]
    capital: float = DEFAULT_CAPITAL
    custom_params: Optional[CustomParams] = None


class RealtimeStartRequest(BaseModel):
    stocks: List[StockInput]
    capital: float = DEFAULT_CAPITAL
    take_profit_pct: float = 7.0
    stop_loss_pct: float = 3.0
    max_hold_days: int = 10


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 비교 실행 상태 관리 (메모리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
compare_state = {
    "running": False,
    "progress": 0,
    "message": "",
    "result": None,
    "error": None,
}


async def _run_compare_task(stocks, capital, custom_params):
    """백그라운드에서 비교 실행 / Run comparison in background"""
    global compare_state
    try:
        compare_state["running"] = True
        compare_state["progress"] = 10
        compare_state["message"] = "일봉 데이터 수집 중..."

        stocks_list = [s.dict() if hasattr(s, 'dict') else s for s in stocks]
        cp = custom_params.dict() if custom_params and hasattr(custom_params, 'dict') else custom_params

        compare_state["progress"] = 30
        compare_state["message"] = "5가지 전략 시뮬레이션 중..."

        result = await run_comparison(
            stocks=stocks_list,
            capital=capital,
            custom_params=cp,
        )

        compare_state["progress"] = 100
        compare_state["message"] = "완료"
        compare_state["result"] = result
        compare_state["error"] = None

        # DB 저장 (선택적)
        if supabase and "rankings" in result:
            try:
                for r in result["rankings"]:
                    supabase.table("virtual_compare_result").insert({
                        "session_id": result["session_id"],
                        "mode": "backtest",
                        "strategy": r["strategy"],
                        "total_return_pct": r["total_return_pct"],
                        "total_return_won": r["total_return_won"],
                        "win_rate": r["win_rate"],
                        "win_count": r["win_count"],
                        "loss_count": r["loss_count"],
                        "total_trades": r["total_trades"],
                        "mdd_pct": r["mdd_pct"],
                        "risk_reward_ratio": r["risk_reward_ratio"],
                        "score": r["score"],
                        "ranking": r["ranking"],
                        "best_strategy": r["ranking"] == 1,
                        "params": {
                            "take_profit_pct": r["take_profit_pct"],
                            "stop_loss_pct": r["stop_loss_pct"],
                            "max_hold_days": r["max_hold_days"],
                        },
                    }).execute()
            except Exception as e:
                logger.warning(f"[가상투자] DB 저장 실패 (무시): {e}")

    except Exception as e:
        logger.error(f"[가상투자] 비교 실행 오류: {e}")
        compare_state["error"] = str(e)
        compare_state["message"] = f"오류: {e}"
    finally:
        compare_state["running"] = False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/presets")
async def get_presets():
    """프리셋 목록 조회 / Get strategy presets"""
    presets = []
    for key, val in STRATEGY_PRESETS.items():
        presets.append({
            "key": key,
            "name": val["name"],
            "name_en": val["name_en"],
            "take_profit_pct": val["take_profit_pct"],
            "stop_loss_pct": val["stop_loss_pct"],
            "max_hold_days": val["max_hold_days"],
            "color": val["color"],
        })
    return {"presets": presets}


@router.post("/compare")
async def compare_strategies(req: CompareRequest, bg: BackgroundTasks):
    """
    5가지 전략 동시 비교 실행 (백테스트)
    Run comparison of 5 strategies simultaneously
    """
    global compare_state

    if compare_state["running"]:
        return {"status": "already_running", "message": "이미 비교 실행 중입니다."}

    # 상태 초기화
    compare_state = {
        "running": True,
        "progress": 0,
        "message": "시작 중...",
        "result": None,
        "error": None,
    }

    # 백그라운드 실행
    bg.add_task(_run_compare_task, req.stocks, req.capital, req.custom_params)

    return {
        "status": "started",
        "message": f"{len(req.stocks)}개 종목 × 5가지 전략 비교 시작",
        "stocks_count": len(req.stocks),
    }


@router.get("/compare/progress")
async def compare_progress():
    """비교 실행 진행 상태 / Comparison progress"""
    return {
        "running": compare_state["running"],
        "progress": compare_state["progress"],
        "message": compare_state["message"],
        "error": compare_state["error"],
        "has_result": compare_state["result"] is not None,
    }


@router.get("/compare/result")
async def compare_result():
    """비교 실행 결과 조회 / Get comparison result"""
    if compare_state["result"]:
        return compare_state["result"]
    elif compare_state["error"]:
        return {"error": compare_state["error"]}
    elif compare_state["running"]:
        return {"status": "running", "message": "아직 실행 중..."}
    else:
        return {"status": "no_result", "message": "실행 결과가 없습니다."}


@router.post("/realtime/start")
async def realtime_start(req: RealtimeStartRequest):
    """
    실시간 모의투자 시작
    Start realtime virtual trading
    """
    stocks_list = [s.dict() for s in req.stocks]
    result = await start_realtime(
        stocks=stocks_list,
        capital=req.capital,
        take_profit_pct=req.take_profit_pct,
        stop_loss_pct=req.stop_loss_pct,
        max_hold_days=req.max_hold_days,
        supabase=supabase,
    )
    return result


@router.get("/realtime/status/{session_id}")
async def realtime_status(session_id: str):
    """실시간 모의투자 현황 조회 / Get realtime status"""
    return await get_realtime_status(session_id, supabase=supabase)


@router.post("/realtime/update/{session_id}")
async def realtime_update(session_id: str):
    """
    실시간 포지션 업데이트 (장 마감 후 호출)
    Update positions after market close
    """
    return await update_realtime(session_id, supabase=supabase)
