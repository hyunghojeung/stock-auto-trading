"""
가상투자 시뮬레이터 API 라우트 / Virtual Investment API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/virtual_invest_routes.py

엔드포인트:
  POST /api/virtual-invest/compare           — 5가지 전략 동시 비교 (백테스트)
  GET  /api/virtual-invest/compare/progress  — 비교 실행 진행 상태
  GET  /api/virtual-invest/compare/result    — 비교 결과 조회
  POST /api/virtual-invest/realtime/start    — 실시간 모의투자 시작 (포트폴리오+포지션 생성)
  GET  /api/virtual-invest/realtime/sessions — 세션(포트폴리오) 목록 조회
  GET  /api/virtual-invest/realtime/status   — 실시간 모의투자 현황
  POST /api/virtual-invest/realtime/update   — 실시간 포지션 업데이트 (장 마감 후)
  GET  /api/virtual-invest/presets           — 프리셋 목록 조회
  GET  /api/virtual-invest/candles/{code}    — 종목 일봉 데이터 조회
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

# Supabase 연결 — app.core.database 사용 (다른 라우트와 동일)
try:
    from app.core.database import db as supabase
    logger.info("[가상투자] DB 연결 성공 (app.core.database)")
except Exception:
    supabase = None
    logger.warning("[가상투자] DB 연결 실패 — DB 없이 동작")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Request / Response 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class StockInput(BaseModel):
    code: str
    name: str = ""
    buy_price: float = 0
    current_price: float = 0
    signal_date: str = ""
    similarity: float = 0
    signal: str = ""
    pattern_id: Optional[str] = None
    pattern_name: Optional[str] = None


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
    title: str = ""
    preset: str = "standard"
    take_profit_pct: float = 7.0
    stop_loss_pct: float = 3.0
    max_hold_days: int = 10
    trailing_stop_pct: float = 0.0
    grace_days: int = 0


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
    실시간 모의투자 시작 → virtual_portfolios + virtual_positions(FK) 생성
    Start realtime virtual trading → create portfolio & positions in DB
    ※ 레거시 start_realtime() 미사용 — FK 기반 테이블 직접 INSERT
    """
    from datetime import datetime
    import uuid

    stocks_list = [s.dict() for s in req.stocks]

    if not supabase:
        return {"error": "DB 미연결", "session_id": None}

    portfolio_id = None
    try:
        per_stock = req.capital / max(len(stocks_list), 1)

        # ── 1) virtual_portfolios INSERT ──
        pf_data = {
            "name": req.title or datetime.now().strftime("%Y-%m-%d %H:%M"),
            "capital": req.capital,
            "strategy": req.preset,
            "status": "active",
            "stock_count": len(stocks_list),
            "current_value": req.capital,
        }
        pf_res = supabase.table("virtual_portfolios").insert(pf_data).execute()
        if pf_res.data and len(pf_res.data) > 0:
            portfolio_id = pf_res.data[0]["id"]
            logger.info(f"[가상투자] 포트폴리오 생성: id={portfolio_id}, name={pf_data['name']}")

            # ── 2) virtual_positions INSERT (FK 버전) ──
            positions = []
            for s in stocks_list:
                buy_price = s.get("buy_price", 0)
                qty = int(per_stock / buy_price) if buy_price > 0 else 0
                invest = qty * buy_price

                pos_data = {
                    "portfolio_id": portfolio_id,
                    "code": s.get("code", ""),
                    "name": s.get("name", ""),
                    "buy_price": buy_price,
                    "current_price": buy_price,
                    "quantity": qty,
                    "invest_amount": invest,
                    "status": "holding",
                    "peak_price": buy_price,
                    "similarity": s.get("similarity", 0),
                    "signal": s.get("signal", ""),
                }
                # ★ 패턴 라이브러리 연동
                logger.info(f"[가상투자] 종목 {s.get('name')} pattern_id={s.get('pattern_id')}, pattern_name={s.get('pattern_name')}")
                if s.get("pattern_id"):
                    pos_data["pattern_id"] = s["pattern_id"]
                if s.get("pattern_name"):
                    pos_data["pattern_name"] = s["pattern_name"]
                positions.append(pos_data)

            if positions:
                supabase.table("virtual_positions").insert(positions).execute()
                logger.info(f"[가상투자] {len(positions)}개 포지션 생성 완료")

            return {
                "session_id": str(portfolio_id),
                "portfolio_id": portfolio_id,
                "status": "active",
                "message": f"{len(stocks_list)}종목 가상투자 등록 완료",
            }
        else:
            return {"error": "포트폴리오 생성 실패", "session_id": None}

    except Exception as e:
        logger.error(f"[가상투자] 포트폴리오 생성 실패: {e}")
        return {"error": str(e), "session_id": None}


@router.get("/realtime/sessions")
async def realtime_sessions():
    """
    실시간 모의투자 세션 목록 조회 (virtual_portfolios 기반)
    Get list of all realtime trading sessions from virtual_portfolios
    """
    if not supabase:
        return {"sessions": [], "error": "DB 미연결"}

    try:
        # ── 포트폴리오 목록 조회 ──
        res = supabase.table("virtual_portfolios") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(50) \
            .execute()

        portfolios = res.data or []
        sessions = []

        for pf in portfolios:
            pid = pf.get("id")

            # ── 각 포트폴리오의 포지션 조회 ──
            positions = []
            if pid:
                try:
                    pos_res = supabase.table("virtual_positions") \
                        .select("*") \
                        .eq("portfolio_id", pid) \
                        .execute()
                    positions = pos_res.data or []
                except Exception as e:
                    logger.warning(f"[가상투자] 포지션 조회 실패 (pf={pid}): {e}")

            # ── 수익 계산 ──
            capital = pf.get("capital", 1000000)
            total_invest = sum(p.get("invest_amount", 0) for p in positions)
            # 현재가 0원이면 매수가로 대체 (수익률 0% 처리)
            total_current = 0
            for p in positions:
                if p.get("status") != "holding":
                    continue
                cp = p.get("current_price", 0)
                bp = p.get("buy_price", 0)
                qty = p.get("quantity", 0)
                price = cp if cp > 0 else bp
                total_current += price * qty
            # 매도 완료 종목 손익
            realized = sum(p.get("profit_won", 0) for p in positions if p.get("status") != "holding")
            cash = capital - total_invest + realized
            holding_value = total_current
            total_asset = cash + holding_value
            profit = total_asset - capital
            profit_pct = (profit / capital * 100) if capital > 0 else 0

            sessions.append({
                "session_id": str(pid),
                "id": pid,
                "title": pf.get("name", ""),
                "preset": pf.get("strategy", "standard"),
                "capital": capital,
                "status": pf.get("status", "active"),
                "stock_count": pf.get("stock_count", len(positions)),
                "stock_names": [p.get("name", p.get("code", "")) for p in positions],
                "stocks": [p.get("name", p.get("code", "")) for p in positions],
                "total_profit": round(profit),
                "total_profit_pct": round(profit_pct, 2),
                "total_asset": round(total_asset),
                "cash": round(cash),
                "holding_value": round(holding_value),
                "holding_count": len([p for p in positions if p.get("status") == "holding"]),
                "win_count": pf.get("win_count", 0),
                "loss_count": pf.get("loss_count", 0),
                "created_at": pf.get("created_at", ""),
                "updated_at": pf.get("updated_at", ""),
                "positions": [{
                    "stock_code": p.get("code", ""),
                    "stock_name": p.get("name", ""),
                    "buy_price": p.get("buy_price", 0),
                    "current_price": p.get("current_price", 0),
                    "quantity": p.get("quantity", 0),
                    "status": p.get("status", "holding"),
                    "profit_pct": p.get("profit_pct", 0),
                    "profit_won": p.get("profit_won", 0),
                    "hold_days": p.get("hold_days", 0),
                    "peak_price": p.get("peak_price", 0),
                    "similarity": p.get("similarity", 0),
                } for p in positions],
            })

        return {"sessions": sessions}

    except Exception as e:
        logger.error(f"[가상투자] 세션 목록 조회 오류: {e}")
        return {"sessions": [], "error": str(e)}


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


@router.get("/candles/{code}")
async def get_candles(code: str, count: int = 120):
    """
    종목 일봉 데이터 조회 (봉차트용)
    Fetch daily candles for chart display
    """
    import asyncio
    try:
        from app.services.naver_stock import get_daily_candles_naver
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None, lambda: get_daily_candles_naver(code, count=count)
        )
        return {"code": code, "candles": candles, "count": len(candles)}
    except Exception as e:
        logger.error(f"[가상투자] 일봉 조회 오류: {e}")
        return {"code": code, "candles": [], "error": str(e)}
