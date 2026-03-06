"""
한국투자증권 KIS 모의투자 / 실제투자 API 라우트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/kis_trading_routes.py

엔드포인트:
  GET  /api/kis-trading/balance            — 예수금(주문가능금액) 조회
  GET  /api/kis-trading/holdings           — 보유종목 조회
  GET  /api/kis-trading/orders             — 당일 주문내역 조회
  POST /api/kis-trading/buy                — 매수 주문
  POST /api/kis-trading/sell               — 매도 주문
  GET  /api/kis-trading/price/{code}       — 현재가 조회
  POST /api/kis-trading/auto/start         — 자동매매 시작
  POST /api/kis-trading/auto/stop          — 자동매매 중지
  GET  /api/kis-trading/auto/status        — 자동매매 상태
  GET  /api/kis-trading/status             — 모의/실제 연결 상태

모든 엔드포인트에 ?mode=mock (모의) 또는 ?mode=live (실제) 파라미터 사용
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta, timezone
import logging
import asyncio

from app.services.kis_auth import get_kis
from app.services.kis_order import buy_stock, sell_stock
from app.services.kis_stock import get_current_price
from app.services.kis_account import get_account_balance, get_holdings, get_order_history
from app.core.config import config

KST = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/kis-trading", tags=["kis-trading"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Request 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class OrderRequest(BaseModel):
    code: str
    quantity: int
    price: int = 0  # 0이면 시장가


class AutoTradingStock(BaseModel):
    code: str
    name: str = ""
    buy_price: float = 0
    take_profit_pct: float = 5.0
    stop_loss_pct: float = 3.0


class AutoTradingRequest(BaseModel):
    stocks: List[AutoTradingStock]
    capital: float = 1000000
    take_profit_pct: float = 5.0
    stop_loss_pct: float = 3.0
    max_hold_days: int = 10


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 자동매매 상태
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_auto_state = {
    "mock": {
        "running": False,
        "stocks": [],
        "capital": 0,
        "params": {},
        "started_at": None,
        "orders": [],
        "message": "",
    },
    "live": {
        "running": False,
        "stocks": [],
        "capital": 0,
        "params": {},
        "started_at": None,
        "orders": [],
        "message": "",
    },
}


def _is_live(mode: str) -> bool:
    return mode == "live"


def _validate_mode(mode: str):
    if mode not in ("mock", "live"):
        raise HTTPException(400, "mode는 'mock' 또는 'live'만 가능합니다")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 연결 상태 확인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/status")
async def trading_status(mode: str = Query("mock", description="mock 또는 live")):
    """모의/실제 투자 연결 상태 확인"""
    _validate_mode(mode)
    is_live = _is_live(mode)

    try:
        auth = get_kis(is_live)
        token = auth.get_token()
        connected = bool(token)
    except Exception as e:
        connected = False
        logger.warning(f"[KIS {mode}] 연결 실패: {e}")

    return {
        "mode": mode,
        "mode_label": "실제투자" if is_live else "모의투자",
        "connected": connected,
        "base_url": config.KIS_LIVE_BASE_URL if is_live else config.KIS_BASE_URL,
        "has_key": bool(config.KIS_LIVE_APP_KEY if is_live else config.KIS_APP_KEY),
        "account": config.KIS_CANO,
        "auto_trading": _auto_state[mode]["running"],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 예수금 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/balance")
async def balance(mode: str = Query("mock", description="mock 또는 live")):
    """예수금(주문가능금액) 조회"""
    _validate_mode(mode)
    result = get_account_balance(is_live=_is_live(mode))
    return {"mode": mode, **result}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 보유종목 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/holdings")
async def holdings(mode: str = Query("mock", description="mock 또는 live")):
    """보유종목 조회"""
    _validate_mode(mode)
    result = get_holdings(is_live=_is_live(mode))
    return {"mode": mode, **result}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 당일 주문내역
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/orders")
async def orders(mode: str = Query("mock", description="mock 또는 live")):
    """당일 주문내역 조회"""
    _validate_mode(mode)
    result = get_order_history(is_live=_is_live(mode))
    return {"mode": mode, **result}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 현재가 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/price/{code}")
async def price(code: str, mode: str = Query("mock", description="mock 또는 live")):
    """종목 현재가 조회"""
    _validate_mode(mode)
    result = get_current_price(code, is_live=_is_live(mode))
    if result:
        return {"mode": mode, **result}
    return {"mode": mode, "error": f"{code} 현재가 조회 실패"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 매수 주문
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.post("/buy")
async def buy(req: OrderRequest, mode: str = Query("mock", description="mock 또는 live")):
    """매수 주문 (price=0이면 시장가)"""
    _validate_mode(mode)
    is_live = _is_live(mode)

    if is_live:
        logger.warning(f"[실제투자] 매수 주문: {req.code} x {req.quantity} @ {req.price}")

    result = buy_stock(req.code, req.quantity, req.price, is_live=is_live)
    return {
        "mode": mode,
        "action": "buy",
        "code": req.code,
        "quantity": req.quantity,
        "price": req.price,
        "order_type": "지정가" if req.price > 0 else "시장가",
        **result,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 매도 주문
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.post("/sell")
async def sell(req: OrderRequest, mode: str = Query("mock", description="mock 또는 live")):
    """매도 주문 (price=0이면 시장가)"""
    _validate_mode(mode)
    is_live = _is_live(mode)

    if is_live:
        logger.warning(f"[실제투자] 매도 주문: {req.code} x {req.quantity} @ {req.price}")

    result = sell_stock(req.code, req.quantity, req.price, is_live=is_live)
    return {
        "mode": mode,
        "action": "sell",
        "code": req.code,
        "quantity": req.quantity,
        "price": req.price,
        "order_type": "지정가" if req.price > 0 else "시장가",
        **result,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. 자동매매 시작
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.post("/auto/start")
async def auto_start(
    req: AutoTradingRequest,
    bg: BackgroundTasks,
    mode: str = Query("mock", description="mock 또는 live"),
):
    """자동매매 시작 — 종목 리스트 + 익절/손절 기준으로 자동 매매"""
    _validate_mode(mode)
    is_live = _is_live(mode)
    state = _auto_state[mode]

    if state["running"]:
        return {"status": "already_running", "message": f"{mode} 자동매매가 이미 실행 중입니다"}

    if is_live and not config.KIS_LIVE_APP_KEY:
        return {"error": "실제투자 API 키가 설정되지 않았습니다"}

    state["running"] = True
    state["stocks"] = [s.dict() for s in req.stocks]
    state["capital"] = req.capital
    state["params"] = {
        "take_profit_pct": req.take_profit_pct,
        "stop_loss_pct": req.stop_loss_pct,
        "max_hold_days": req.max_hold_days,
    }
    state["started_at"] = datetime.now(KST).isoformat()
    state["orders"] = []
    state["message"] = "자동매매 시작"

    bg.add_task(_run_auto_trading, mode, is_live, req)

    return {
        "status": "started",
        "mode": mode,
        "mode_label": "실제투자" if is_live else "모의투자",
        "message": f"{len(req.stocks)}종목 자동매매 시작",
        "stocks_count": len(req.stocks),
        "params": state["params"],
    }


async def _run_auto_trading(mode: str, is_live: bool, req: AutoTradingRequest):
    """자동매매 백그라운드 태스크"""
    state = _auto_state[mode]
    try:
        per_stock = req.capital / max(len(req.stocks), 1)

        # 1단계: 매수 주문
        for stock in req.stocks:
            if not state["running"]:
                break

            code = stock.code
            price_info = get_current_price(code, is_live=is_live)
            if not price_info:
                state["orders"].append({
                    "code": code, "name": stock.name,
                    "action": "매수실패", "reason": "현재가 조회 실패",
                    "time": datetime.now(KST).isoformat(),
                })
                continue

            current_price = price_info["price"]
            qty = int(per_stock / current_price)
            if qty <= 0:
                state["orders"].append({
                    "code": code, "name": stock.name,
                    "action": "매수실패", "reason": f"수량 부족 (가격={current_price:,}, 배정={per_stock:,.0f})",
                    "time": datetime.now(KST).isoformat(),
                })
                continue

            result = buy_stock(code, qty, price=0, is_live=is_live)
            state["orders"].append({
                "code": code, "name": stock.name,
                "action": "매수",
                "quantity": qty,
                "price": current_price,
                "success": result.get("success", False),
                "time": datetime.now(KST).isoformat(),
            })
            state["message"] = f"{stock.name}({code}) {qty}주 매수 {'성공' if result.get('success') else '실패'}"
            logger.info(f"[자동매매 {mode}] {state['message']}")

            await asyncio.sleep(0.5)  # API 속도 제한

        state["message"] = f"매수 완료 — 매도 모니터링 중 (익절 {req.take_profit_pct}% / 손절 {req.stop_loss_pct}%)"

        # 2단계: 매도 모니터링 (장중에만)
        while state["running"]:
            now = datetime.now(KST)
            hour = now.hour
            minute = now.minute

            # 장중 시간 체크 (09:00 ~ 15:20)
            if hour < 9 or (hour >= 15 and minute >= 20):
                state["message"] = "장 마감 — 다음 거래일 대기 중"
                await asyncio.sleep(60)
                continue

            # 보유종목 체크
            holdings_data = get_holdings(is_live=is_live)
            for h in holdings_data.get("holdings", []):
                if not state["running"]:
                    break

                code = h["code"]
                buy_price = h["buy_avg_price"]
                current = h["current_price"]
                qty = h["sellable_qty"]

                if buy_price <= 0 or qty <= 0:
                    continue

                profit_pct = (current - buy_price) / buy_price * 100

                # 익절
                if profit_pct >= req.take_profit_pct:
                    result = sell_stock(code, qty, price=0, is_live=is_live)
                    state["orders"].append({
                        "code": code, "name": h["name"],
                        "action": "익절매도",
                        "quantity": qty,
                        "buy_price": buy_price,
                        "sell_price": current,
                        "profit_pct": round(profit_pct, 2),
                        "success": result.get("success", False),
                        "time": datetime.now(KST).isoformat(),
                    })
                    state["message"] = f"익절! {h['name']} +{profit_pct:.1f}%"
                    logger.info(f"[자동매매 {mode}] 익절: {h['name']} +{profit_pct:.1f}%")

                # 손절
                elif profit_pct <= -req.stop_loss_pct:
                    result = sell_stock(code, qty, price=0, is_live=is_live)
                    state["orders"].append({
                        "code": code, "name": h["name"],
                        "action": "손절매도",
                        "quantity": qty,
                        "buy_price": buy_price,
                        "sell_price": current,
                        "profit_pct": round(profit_pct, 2),
                        "success": result.get("success", False),
                        "time": datetime.now(KST).isoformat(),
                    })
                    state["message"] = f"손절! {h['name']} {profit_pct:.1f}%"
                    logger.info(f"[자동매매 {mode}] 손절: {h['name']} {profit_pct:.1f}%")

            await asyncio.sleep(30)  # 30초마다 체크

    except Exception as e:
        logger.error(f"[자동매매 {mode}] 오류: {e}")
        state["message"] = f"오류 발생: {e}"
    finally:
        state["running"] = False
        state["message"] = "자동매매 종료"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 9. 자동매매 중지
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.post("/auto/stop")
async def auto_stop(mode: str = Query("mock", description="mock 또는 live")):
    """자동매매 중지"""
    _validate_mode(mode)
    state = _auto_state[mode]

    if not state["running"]:
        return {"status": "not_running", "message": f"{mode} 자동매매가 실행 중이 아닙니다"}

    state["running"] = False
    return {
        "status": "stopping",
        "mode": mode,
        "message": "자동매매 중지 요청됨",
        "orders": state["orders"],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 10. 자동매매 상태
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/auto/status")
async def auto_status(mode: str = Query("mock", description="mock 또는 live")):
    """자동매매 현재 상태 조회"""
    _validate_mode(mode)
    state = _auto_state[mode]

    # 보유종목 현황도 함께 조회
    holdings_data = {}
    if state["running"]:
        try:
            holdings_data = get_holdings(is_live=_is_live(mode))
        except Exception:
            pass

    return {
        "mode": mode,
        "mode_label": "실제투자" if mode == "live" else "모의투자",
        "running": state["running"],
        "started_at": state["started_at"],
        "message": state["message"],
        "stocks": state["stocks"],
        "params": state["params"],
        "orders": state["orders"],
        "order_count": len(state["orders"]),
        "holdings": holdings_data.get("holdings", []),
        "summary": holdings_data.get("summary", {}),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 11. 대시보드 요약 (모의+실제 한눈에)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.get("/dashboard")
async def dashboard():
    """모의투자 + 실제투자 대시보드 요약"""
    result = {}
    for mode in ("mock", "live"):
        is_live = _is_live(mode)
        state = _auto_state[mode]
        data = {
            "mode_label": "실제투자" if is_live else "모의투자",
            "auto_running": state["running"],
            "auto_message": state["message"],
            "order_count": len(state["orders"]),
            "connected": False,
            "holdings": [],
            "summary": {},
        }
        try:
            auth = get_kis(is_live)
            token = auth.get_token()
            data["connected"] = bool(token)
            if data["connected"]:
                h = get_holdings(is_live=is_live)
                data["holdings"] = h.get("holdings", [])
                data["summary"] = h.get("summary", {})
        except Exception as e:
            data["error"] = str(e)
        result[mode] = data

    return result
