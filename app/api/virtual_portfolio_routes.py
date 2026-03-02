"""
실시간 가상투자 포트폴리오 API
Virtual Portfolio Tracking API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/virtual_portfolio_routes.py

기능:
  - 매수추천 종목 → 가상투자 포트폴리오 등록
  - 포트폴리오 목록/상세 조회
  - 일별 가격 갱신 + 자동 청산 (스마트형 전략)
  - 포트폴리오 수동 청산
"""

import json
import math
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.core.database import db
from app.utils.kr_holiday import is_market_open_day, get_market_status
from app.services.naver_stock import get_daily_candles_naver

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/virtual-portfolio", tags=["virtual-portfolio"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청/응답 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegisterRequest(BaseModel):
    name: str = ""
    capital: float = 1000000
    strategy: str = "smart"
    stocks: List[Dict]  # [{code, name, current_price, similarity, signal}]


class UpdatePriceRequest(BaseModel):
    portfolio_id: int


# ★ 복리 그룹 요청 모델
class CompoundCreateRequest(BaseModel):
    name: str = "10억 도전"
    seed_money: float = 100000
    goal_amount: float = 1000000000
    strategy: str = "smart"
    stocks: List[Dict] = []


class CompoundNextRoundRequest(BaseModel):
    stocks: List[Dict]
    preset: str = "smart"
    take_profit_pct: float = 15.0
    stop_loss_pct: float = 12.0
    max_hold_days: int = 30
    trailing_stop_pct: float = 5.0
    grace_days: int = 7


COMMISSION_RATE = 0.00015  # 0.015%


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 파라미터
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STRATEGY_PARAMS = {
    "smart": {
        "stop_loss_pct": 12.0,
        "trailing_stop_pct": 5.0,
        "profit_activation_pct": 15.0,
        "grace_days": 7,
        "max_hold_days": 30,
    },
    "aggressive": {
        "take_profit_pct": 10.0,
        "stop_loss_pct": 5.0,
        "max_hold_days": 5,
    },
    "balanced": {
        "take_profit_pct": 7.0,
        "stop_loss_pct": 3.0,
        "max_hold_days": 10,
    },
}

COMMISSION_RATE = 0.00015  # 매매 수수료
SELL_TAX_RATE = 0.0018     # 매도세


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 포트폴리오 등록
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/register")
async def register_portfolio(req: RegisterRequest):
    """매수추천 종목으로 가상투자 포트폴리오 등록"""
    if not req.stocks or len(req.stocks) == 0:
        raise HTTPException(400, "종목을 선택해주세요")

    # ★ 장 운영일 체크 — 주말/공휴일에는 포트폴리오 등록 불가
    today = datetime.now().date()
    if not is_market_open_day(today):
        status = get_market_status()
        raise HTTPException(400, f"현재 {status} — 장 운영일에만 포트폴리오를 등록할 수 있습니다")

    now = datetime.now().isoformat()
    name = req.name or f"포트폴리오 {now[:10]}"

    try:
        # 포트폴리오 생성
        pf_resp = db.table("virtual_portfolios").insert({
            "name": name,
            "capital": req.capital,
            "strategy": req.strategy,
            "status": "active",
            "stock_count": len(req.stocks),
            "current_value": req.capital,
            "created_at": now,
            "updated_at": now,
        }).execute()

        portfolio = pf_resp.data[0]
        portfolio_id = portfolio["id"]

        # 종목별 포지션 생성
        per_stock = req.capital / len(req.stocks)
        positions = []

        for stock in req.stocks:
            code = stock["code"]

            # ★ 서버에서 네이버 종가를 직접 조회하여 buy_price 결정
            try:
                candles = get_daily_candles_naver(code, count=3)
                if candles and len(candles) > 0:
                    buy_price = candles[-1].get("close", 0)
                else:
                    buy_price = stock.get("current_price", 0)
            except Exception:
                buy_price = stock.get("current_price", 0)

            if buy_price <= 0:
                continue

            # 수수료 차감
            commission = per_stock * COMMISSION_RATE
            actual_invest = per_stock - commission
            quantity = actual_invest / buy_price

            positions.append({
                "portfolio_id": portfolio_id,
                "code": stock["code"],
                "name": stock.get("name", stock["code"]),
                "buy_price": buy_price,
                "current_price": buy_price,
                "quantity": round(quantity, 4),
                "invest_amount": round(per_stock),
                "status": "holding",
                "peak_price": buy_price,
                "similarity": stock.get("similarity", 0),
                "signal": stock.get("signal", ""),
                "price_history": json.dumps([{
                    "date": now[:10],
                    "close": buy_price,
                }]),
                "buy_date": now,
                "updated_at": now,
            })

        if positions:
            db.table("virtual_positions").insert(positions).execute()

        logger.info(f"[가상포트] 등록 완료: {name}, {len(positions)}종목, {req.capital:,.0f}원")

        return {
            "success": True,
            "portfolio_id": portfolio_id,
            "name": name,
            "stock_count": len(positions),
            "message": f"'{name}' 포트폴리오 등록 완료 ({len(positions)}종목)",
        }

    except Exception as e:
        logger.error(f"[가상포트] 등록 실패: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"등록 실패: {str(e)}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 포트폴리오 목록 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/list")
async def list_portfolios():
    """등록된 포트폴리오 목록 (최신순)"""
    try:
        resp = db.table("virtual_portfolios") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()

        portfolios = resp.data or []

        # 각 포트폴리오에 종목 요약 추가
        for pf in portfolios:
            pos_resp = db.table("virtual_positions") \
                .select("code, name, status, profit_pct") \
                .eq("portfolio_id", pf["id"]) \
                .execute()
            pf["positions_summary"] = pos_resp.data or []

        return {"portfolios": portfolios}

    except Exception as e:
        logger.error(f"[가상포트] 목록 조회 실패: {e}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 포트폴리오 상세 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/detail/{portfolio_id}")
async def get_portfolio_detail(portfolio_id: int):
    """포트폴리오 상세 + 종목별 포지션"""
    try:
        pf_resp = db.table("virtual_portfolios") \
            .select("*") \
            .eq("id", portfolio_id) \
            .execute()

        if not pf_resp.data:
            raise HTTPException(404, "포트폴리오를 찾을 수 없습니다")

        portfolio = pf_resp.data[0]

        pos_resp = db.table("virtual_positions") \
            .select("*") \
            .eq("portfolio_id", portfolio_id) \
            .order("profit_pct", desc=True) \
            .execute()

        positions = pos_resp.data or []

        # price_history JSON 파싱
        for pos in positions:
            if isinstance(pos.get("price_history"), str):
                try:
                    pos["price_history"] = json.loads(pos["price_history"])
                except Exception:
                    pos["price_history"] = []

        return {
            "portfolio": portfolio,
            "positions": positions,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[가상포트] 상세 조회 실패: {e}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 가격 갱신 + 자동 청산 체크
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/update-prices/{portfolio_id}")
async def update_prices(portfolio_id: int):
    """네이버에서 최신 가격 가져와 포지션 업데이트 + 자동 청산 체크"""
    from app.services.naver_stock import get_daily_candles_naver

    try:
        pf_resp = db.table("virtual_portfolios") \
            .select("*") \
            .eq("id", portfolio_id) \
            .execute()

        if not pf_resp.data:
            raise HTTPException(404, "포트폴리오를 찾을 수 없습니다")

        portfolio = pf_resp.data[0]
        strategy = portfolio.get("strategy", "smart")
        params = STRATEGY_PARAMS.get(strategy, STRATEGY_PARAMS["smart"])

        pos_resp = db.table("virtual_positions") \
            .select("*") \
            .eq("portfolio_id", portfolio_id) \
            .eq("status", "holding") \
            .execute()

        positions = pos_resp.data or []
        if not positions:
            return {"message": "보유 중인 종목이 없습니다", "updated": 0}

        now = datetime.now().isoformat()
        today = now[:10]
        updated_count = 0
        closed_count = 0
        total_value = 0
        total_profit_won = 0
        win = 0
        loss = 0

        for pos in positions:
            code = pos["code"]
            try:
                candles = get_daily_candles_naver(code, count=3)
                if not candles:
                    continue

                latest = candles[-1]
                current_price = latest.get("close", 0)
                high_price = latest.get("high", current_price)
                low_price = latest.get("low", current_price)

                if current_price <= 0:
                    continue

                buy_price = pos["buy_price"]
                peak_price = max(pos.get("peak_price", buy_price), current_price)
                buy_date = pos.get("buy_date", now)

                # 보유일 계산
                try:
                    bd = datetime.fromisoformat(str(buy_date).replace("Z", "+00:00"))
                    hold_days = (datetime.now(bd.tzinfo) - bd).days if bd.tzinfo else (datetime.now() - bd).days
                except Exception:
                    hold_days = pos.get("hold_days", 0) + 1

                # price_history 업데이트
                history = pos.get("price_history", [])
                if isinstance(history, str):
                    try:
                        history = json.loads(history)
                    except Exception:
                        history = []

                # 같은 날짜 중복 방지
                if not history or history[-1].get("date") != today:
                    history.append({
                        "date": today,
                        "close": current_price,
                        "high": high_price,
                        "low": low_price,
                    })

                # 수익률 계산
                gross_pct = ((current_price - buy_price) / buy_price) * 100
                sell_amount = pos["quantity"] * current_price
                sell_comm = sell_amount * COMMISSION_RATE
                sell_tax = sell_amount * SELL_TAX_RATE
                buy_amount = pos["quantity"] * buy_price
                buy_comm = buy_amount * COMMISSION_RATE
                profit_won = round((sell_amount - sell_comm - sell_tax) - (buy_amount + buy_comm))
                net_pct = round((profit_won / pos["invest_amount"]) * 100, 2) if pos["invest_amount"] else 0

                # ── 자동 청산 체크 (스마트형) ──
                sell_reason = None

                if strategy == "smart":
                    grace_days = params.get("grace_days", 7)
                    stop_loss = params.get("stop_loss_pct", 12.0)
                    trailing = params.get("trailing_stop_pct", 5.0)
                    activation = params.get("profit_activation_pct", 15.0)
                    max_hold = params.get("max_hold_days", 30)

                    # 수익 활성화 체크
                    peak_pct = ((peak_price - buy_price) / buy_price) * 100

                    if hold_days > grace_days:
                        # 추적손절 (수익 활성화 후)
                        if peak_pct >= activation:
                            drop = ((current_price - peak_price) / peak_price) * 100
                            if drop <= -trailing:
                                sell_reason = "trailing"

                        # 손절
                        if net_pct <= -stop_loss:
                            sell_reason = "loss"

                    # 만기
                    if hold_days >= max_hold:
                        sell_reason = "timeout"

                else:
                    # 일반 전략
                    tp = params.get("take_profit_pct", 7.0)
                    sl = params.get("stop_loss_pct", 3.0)
                    mhd = params.get("max_hold_days", 10)

                    if gross_pct >= tp:
                        sell_reason = "profit"
                    elif gross_pct <= -sl:
                        sell_reason = "loss"
                    elif hold_days >= mhd:
                        sell_reason = "timeout"

                # ── 포지션 업데이트 ──
                update_data = {
                    "current_price": current_price,
                    "peak_price": peak_price,
                    "profit_pct": net_pct,
                    "profit_won": profit_won,
                    "hold_days": hold_days,
                    "price_history": json.dumps(history),
                    "updated_at": now,
                }

                if sell_reason:
                    status_map = {
                        "profit": "profit", "trailing": "trailing",
                        "loss": "loss", "timeout": "timeout",
                    }
                    update_data["status"] = status_map.get(sell_reason, "timeout")
                    update_data["sell_price"] = current_price
                    update_data["sell_date"] = now
                    update_data["sell_reason"] = sell_reason
                    closed_count += 1

                    if profit_won > 0:
                        win += 1
                    else:
                        loss += 1

                db.table("virtual_positions") \
                    .update(update_data) \
                    .eq("id", pos["id"]) \
                    .execute()

                # 자산 합산
                if not sell_reason:
                    total_value += pos["quantity"] * current_price
                else:
                    total_value += pos["invest_amount"] + profit_won

                total_profit_won += profit_won
                updated_count += 1

            except Exception as e:
                logger.warning(f"[가상포트] {code} 가격 갱신 실패: {e}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ★ 수익 재투자 로직 (Compound Reinvest)
        # 청산된 종목의 수익금을 남은 보유종목에 균등 재분배
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if closed_count > 0:
            try:
                # 이번 갱신에서 청산된 종목들의 회수금 합산
                closed_resp = db.table("virtual_positions") \
                    .select("id, invest_amount, profit_won, status") \
                    .eq("portfolio_id", portfolio_id) \
                    .neq("status", "holding") \
                    .execute()

                # 아직 보유중인 종목 조회
                holding_resp = db.table("virtual_positions") \
                    .select("id, code, current_price, quantity, invest_amount") \
                    .eq("portfolio_id", portfolio_id) \
                    .eq("status", "holding") \
                    .execute()

                holding_positions = holding_resp.data or []

                if holding_positions:
                    # 청산 종목들의 총 회수금 계산
                    total_recovered = 0
                    for cp in (closed_resp.data or []):
                        recovered = (cp.get("invest_amount", 0) or 0) + (cp.get("profit_won", 0) or 0)
                        total_recovered += recovered

                    # 회수금 중 원래 투자금 제외 = 순수익
                    # → 순수익을 남은 보유종목에 균등 분배
                    # (원래 투자금은 이미 invest_amount에 포함되어 있으므로 순수익만 재투자)
                    total_closed_invest = sum((cp.get("invest_amount", 0) or 0) for cp in (closed_resp.data or []))
                    pure_profit = total_recovered - total_closed_invest

                    if pure_profit > 0:
                        reinvest_per_stock = pure_profit / len(holding_positions)
                        logger.info(f"[가상포트] ★ 수익 재투자: 순수익 {pure_profit:,.0f}원 → "
                                   f"{len(holding_positions)}종목 × {reinvest_per_stock:,.0f}원")

                        for hp in holding_positions:
                            cp = hp["current_price"]
                            if cp <= 0:
                                continue
                            # 추가 매수 수량 계산
                            add_qty = reinvest_per_stock / cp
                            new_qty = round(hp["quantity"] + add_qty, 4)
                            new_invest = round(hp["invest_amount"] + reinvest_per_stock)

                            db.table("virtual_positions").update({
                                "quantity": new_qty,
                                "invest_amount": new_invest,
                                "updated_at": now,
                            }).eq("id", hp["id"]).execute()

                            logger.info(f"[가상포트]   {hp['code']}: "
                                       f"수량 {hp['quantity']:.2f}→{new_qty:.2f}, "
                                       f"투자금 {hp['invest_amount']:,.0f}→{new_invest:,.0f}")

            except Exception as e:
                logger.warning(f"[가상포트] 수익 재투자 실패 (무시): {e}")

        # ── 포트폴리오 합산 업데이트 ──
        # 미보유 포지션(이미 청산됨)의 수익도 합산
        all_pos_resp = db.table("virtual_positions") \
            .select("status, profit_won, invest_amount, quantity, current_price") \
            .eq("portfolio_id", portfolio_id) \
            .execute()

        all_positions = all_pos_resp.data or []
        capital = portfolio["capital"]
        pf_total_value = 0
        pf_win = 0
        pf_loss = 0

        for p in all_positions:
            if p["status"] == "holding":
                pf_total_value += p["quantity"] * p["current_price"]
            else:
                pf_total_value += p["invest_amount"] + (p["profit_won"] or 0)

            pw = p.get("profit_won", 0)
            if p["status"] != "holding":
                if pw > 0:
                    pf_win += 1
                elif pw < 0:
                    pf_loss += 1

        pf_return_won = round(pf_total_value - capital)
        pf_return_pct = round((pf_return_won / capital) * 100, 2) if capital > 0 else 0

        # 모든 포지션 청산 시 포트폴리오 종료
        holding_count = sum(1 for p in all_positions if p["status"] == "holding")
        pf_status = "active" if holding_count > 0 else "closed"

        db.table("virtual_portfolios").update({
            "current_value": round(pf_total_value),
            "total_return_pct": pf_return_pct,
            "total_return_won": pf_return_won,
            "win_count": pf_win,
            "loss_count": pf_loss,
            "status": pf_status,
            "closed_at": now if pf_status == "closed" else None,
            "updated_at": now,
        }).eq("id", portfolio_id).execute()

        logger.info(f"[가상포트] #{portfolio_id} 갱신: {updated_count}종목, 청산:{closed_count}, 수익:{pf_return_pct}%")

        return {
            "success": True,
            "updated": updated_count,
            "closed": closed_count,
            "total_return_pct": pf_return_pct,
            "total_return_won": pf_return_won,
            "status": pf_status,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[가상포트] 가격 갱신 실패: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 전체 활성 포트폴리오 일괄 갱신 (스케줄러용)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def update_all_active_portfolios():
    """매일 18:35 스케줄러에서 호출 — 모든 활성 포트폴리오 가격 갱신"""
    import requests
    import time

    # ★ 장 운영일이 아니면 스킵
    today = datetime.now().date()
    if not is_market_open_day(today):
        logger.info(f"[가상포트] 장 운영일이 아님 ({get_market_status()}) — 일괄 갱신 스킵")
        return

    try:
        resp = db.table("virtual_portfolios") \
            .select("id") \
            .eq("status", "active") \
            .execute()

        portfolios = resp.data or []
        logger.info(f"[가상포트] 일괄 갱신 시작: {len(portfolios)}개 포트폴리오")

        for pf in portfolios:
            try:
                # 내부 API 호출 대신 직접 로직 실행
                _sync_update_prices(pf["id"])
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"[가상포트] #{pf['id']} 갱신 실패: {e}")

        logger.info(f"[가상포트] 일괄 갱신 완료")

    except Exception as e:
        logger.error(f"[가상포트] 일괄 갱신 실패: {e}")


def _sync_update_prices(portfolio_id: int):
    """동기 방식 가격 갱신 (스케줄러용)"""
    from app.services.naver_stock import get_daily_candles_naver
    import time

    pf_resp = db.table("virtual_portfolios").select("*").eq("id", portfolio_id).execute()
    if not pf_resp.data:
        return

    portfolio = pf_resp.data[0]
    strategy = portfolio.get("strategy", "smart")
    params = STRATEGY_PARAMS.get(strategy, STRATEGY_PARAMS["smart"])
    now = datetime.now().isoformat()
    today = now[:10]

    pos_resp = db.table("virtual_positions") \
        .select("*") \
        .eq("portfolio_id", portfolio_id) \
        .eq("status", "holding") \
        .execute()

    positions = pos_resp.data or []

    for pos in positions:
        code = pos["code"]
        try:
            candles = get_daily_candles_naver(code, count=3)
            if not candles:
                continue

            latest = candles[-1]
            current_price = latest.get("close", 0)
            if current_price <= 0:
                continue

            buy_price = pos["buy_price"]
            peak_price = max(pos.get("peak_price", buy_price), current_price)

            try:
                bd = datetime.fromisoformat(str(pos["buy_date"]).replace("Z", "+00:00"))
                hold_days = (datetime.now(bd.tzinfo) - bd).days if bd.tzinfo else (datetime.now() - bd).days
            except Exception:
                hold_days = pos.get("hold_days", 0) + 1

            history = pos.get("price_history", [])
            if isinstance(history, str):
                try:
                    history = json.loads(history)
                except Exception:
                    history = []

            if not history or history[-1].get("date") != today:
                history.append({
                    "date": today,
                    "close": current_price,
                    "high": latest.get("high", current_price),
                    "low": latest.get("low", current_price),
                })

            # 수익 계산
            sell_amount = pos["quantity"] * current_price
            sell_comm = sell_amount * COMMISSION_RATE
            sell_tax = sell_amount * SELL_TAX_RATE
            buy_amount = pos["quantity"] * buy_price
            buy_comm = buy_amount * COMMISSION_RATE
            profit_won = round((sell_amount - sell_comm - sell_tax) - (buy_amount + buy_comm))
            net_pct = round((profit_won / pos["invest_amount"]) * 100, 2) if pos["invest_amount"] else 0

            # 청산 체크
            sell_reason = None
            if strategy == "smart":
                grace_days = params.get("grace_days", 7)
                peak_pct = ((peak_price - buy_price) / buy_price) * 100

                if hold_days > grace_days:
                    if peak_pct >= params.get("profit_activation_pct", 15.0):
                        drop = ((current_price - peak_price) / peak_price) * 100
                        if drop <= -params.get("trailing_stop_pct", 5.0):
                            sell_reason = "trailing"
                    if net_pct <= -params.get("stop_loss_pct", 12.0):
                        sell_reason = "loss"
                if hold_days >= params.get("max_hold_days", 30):
                    sell_reason = "timeout"
            else:
                tp = params.get("take_profit_pct", 7.0)
                sl = params.get("stop_loss_pct", 3.0)
                gross_pct = ((current_price - buy_price) / buy_price) * 100
                if gross_pct >= tp:
                    sell_reason = "profit"
                elif gross_pct <= -sl:
                    sell_reason = "loss"
                elif hold_days >= params.get("max_hold_days", 10):
                    sell_reason = "timeout"

            update_data = {
                "current_price": current_price,
                "peak_price": peak_price,
                "profit_pct": net_pct,
                "profit_won": profit_won,
                "hold_days": hold_days,
                "price_history": json.dumps(history),
                "updated_at": now,
            }

            if sell_reason:
                update_data["status"] = sell_reason
                update_data["sell_price"] = current_price
                update_data["sell_date"] = now
                update_data["sell_reason"] = sell_reason

            db.table("virtual_positions").update(update_data).eq("id", pos["id"]).execute()
            time.sleep(0.15)

        except Exception as e:
            logger.warning(f"[가상포트] {code} 갱신 실패: {e}")

    # 포트폴리오 합산
    all_pos = db.table("virtual_positions").select("*").eq("portfolio_id", portfolio_id).execute().data or []
    capital = portfolio["capital"]
    total_value = 0
    w, l = 0, 0
    for p in all_pos:
        if p["status"] == "holding":
            total_value += p["quantity"] * p["current_price"]
        else:
            total_value += p["invest_amount"] + (p["profit_won"] or 0)
            if (p["profit_won"] or 0) > 0:
                w += 1
            elif (p["profit_won"] or 0) < 0:
                l += 1

    holding_count = sum(1 for p in all_pos if p["status"] == "holding")
    pf_status = "active" if holding_count > 0 else "closed"
    pf_return_won = round(total_value - capital)
    pf_return_pct = round((pf_return_won / capital) * 100, 2) if capital else 0

    db.table("virtual_portfolios").update({
        "current_value": round(total_value),
        "total_return_pct": pf_return_pct,
        "total_return_won": pf_return_won,
        "win_count": w,
        "loss_count": l,
        "status": pf_status,
        "closed_at": now if pf_status == "closed" else None,
        "updated_at": now,
    }).eq("id", portfolio_id).execute()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 포트폴리오 수동 전체 청산
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/close/{portfolio_id}")
async def close_portfolio(portfolio_id: int):
    """포트폴리오 전체 수동 청산"""
    now = datetime.now().isoformat()

    try:
        # 보유 중인 포지션 현재가 기준 청산
        pos_resp = db.table("virtual_positions") \
            .select("*") \
            .eq("portfolio_id", portfolio_id) \
            .eq("status", "holding") \
            .execute()

        for pos in (pos_resp.data or []):
            db.table("virtual_positions").update({
                "status": "timeout",
                "sell_price": pos["current_price"],
                "sell_date": now,
                "sell_reason": "manual_close",
                "updated_at": now,
            }).eq("id", pos["id"]).execute()

        # 포트폴리오 종료
        db.table("virtual_portfolios").update({
            "status": "closed",
            "closed_at": now,
            "updated_at": now,
        }).eq("id", portfolio_id).execute()

        # ★ 복리 그룹 연동: 수익 이월
        _check_compound_reinvest(portfolio_id)

        return {"success": True, "message": "포트폴리오가 청산되었습니다"}

    except Exception as e:
        logger.error(f"[가상포트] 청산 실패: {e}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6-1. 포트폴리오 제목 수정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.put("/rename/{portfolio_id}")
async def rename_portfolio(portfolio_id: int, req: dict):
    """포트폴리오 제목 수정"""
    try:
        new_name = req.get("name", "").strip()
        if not new_name:
            raise HTTPException(400, "제목을 입력해주세요")

        db.table("virtual_portfolios").update({
            "name": new_name,
            "updated_at": datetime.now().isoformat(),
        }).eq("id", portfolio_id).execute()

        logger.info(f"[가상포트] #{portfolio_id} 제목 변경: {new_name}")
        return {"success": True, "message": f"제목이 '{new_name}'으로 변경되었습니다"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[가상포트] 제목 변경 실패: {e}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 포트폴리오 삭제
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.delete("/delete/{portfolio_id}")
async def delete_portfolio(portfolio_id: int):
    """포트폴리오 영구 삭제 (CASCADE로 포지션도 삭제)"""
    try:
        db.table("virtual_portfolios").delete().eq("id", portfolio_id).execute()
        return {"success": True, "message": "삭제되었습니다"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. 종목 캔들 데이터 조회 (차트용)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/candles/{code}")
async def get_candles(code: str, days: int = 120):
    """네이버에서 일봉 캔들 데이터 조회"""
    import asyncio
    from app.services.naver_stock import get_daily_candles_naver

    try:
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None, lambda: get_daily_candles_naver(code, count=days)
        )
        if not candles:
            return {"candles": [], "name": code}

        result = []
        for c in candles:
            try:
                close = c.get("close", 0)
                if close <= 0:
                    continue
                # ★ open/high/low가 0이면 close로 보정 (거래 희박 종목 대응)
                op = c.get("open", 0) or close
                hi = c.get("high", 0) or close
                lo = c.get("low", 0) or close
                result.append({
                    "date": c.get("date", ""),
                    "open": op,
                    "high": max(hi, op, close),
                    "low": min(lo, op, close) if min(lo, op, close) > 0 else close,
                    "close": close,
                    "volume": c.get("volume", 0),
                })
            except Exception:
                continue

        result.sort(key=lambda x: x["date"])

        return {"candles": result, "code": code}

    except Exception as e:
        logger.error(f"[가상포트] 캔들 조회 실패 {code}: {e}")
        return {"candles": [], "code": code, "error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 기존 포지션 buy_price 교정 (네이버 종가 기준)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/fix-buy-prices")
async def fix_buy_prices():
    """기존 포지션의 buy_price를 네이버 종가로 교정"""
    import time

    try:
        # 모든 활성 포지션 조회
        pos_resp = db.table("virtual_positions") \
            .select("id, code, name, buy_price, portfolio_id") \
            .eq("status", "holding") \
            .execute()

        positions = pos_resp.data or []
        fixed = []
        errors = []

        for pos in positions:
            code = pos["code"]
            old_price = pos["buy_price"]

            try:
                candles = get_daily_candles_naver(code, count=3)
                if not candles:
                    errors.append(f"{code}: 캔들 데이터 없음")
                    continue

                new_price = candles[-1].get("close", 0)
                if new_price <= 0:
                    errors.append(f"{code}: 종가 0")
                    continue

                if old_price == new_price:
                    continue  # 이미 동일

                # buy_price, current_price, peak_price 교정 + 수익률 재계산
                quantity = pos.get("quantity", 0)
                invest_amount = pos.get("invest_amount", 0)

                # 수량 재계산 (투자금 / 새 매수가)
                if invest_amount > 0 and new_price > 0:
                    commission = invest_amount * COMMISSION_RATE
                    actual_invest = invest_amount - commission
                    new_quantity = round(actual_invest / new_price, 4)
                else:
                    new_quantity = quantity

                db.table("virtual_positions").update({
                    "buy_price": new_price,
                    "current_price": new_price,
                    "peak_price": new_price,
                    "quantity": new_quantity,
                    "profit_pct": 0,
                    "profit_won": 0,
                    "updated_at": datetime.now().isoformat(),
                }).eq("id", pos["id"]).execute()

                fixed.append({
                    "code": code,
                    "name": pos["name"],
                    "old_price": old_price,
                    "new_price": new_price,
                    "old_qty": quantity,
                    "new_qty": new_quantity,
                })
                time.sleep(0.3)

            except Exception as e:
                errors.append(f"{code}: {str(e)}")

        # 포트폴리오 합산 값도 리셋
        pf_ids = set(pos["portfolio_id"] for pos in positions)
        for pf_id in pf_ids:
            all_pos = db.table("virtual_positions") \
                .select("invest_amount, profit_won, status") \
                .eq("portfolio_id", pf_id) \
                .execute()

            total_value = sum(
                (p.get("invest_amount", 0) + (p.get("profit_won", 0) or 0))
                for p in (all_pos.data or [])
            )
            total_profit = sum(
                (p.get("profit_won", 0) or 0) for p in (all_pos.data or [])
            )

            db.table("virtual_portfolios").update({
                "current_value": round(total_value),
                "total_return_won": round(total_profit),
                "total_return_pct": 0,
                "updated_at": datetime.now().isoformat(),
            }).eq("id", pf_id).execute()

        logger.info(f"[가상포트] buy_price 교정 완료: {len(fixed)}건 수정, {len(errors)}건 오류")

        return {
            "success": True,
            "fixed_count": len(fixed),
            "fixed": fixed,
            "errors": errors,
        }

    except Exception as e:
        logger.error(f"[가상포트] buy_price 교정 실패: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 복리 그룹 헬퍼 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _calc_goal_progress(seed: float, current: float, goal: float) -> float:
    """목표 진행률 (로그 스케일)"""
    if seed <= 0 or goal <= seed or current <= 0:
        return 0.0
    try:
        progress = math.log(current / seed) / math.log(goal / seed) * 100
        return round(min(max(progress, 0), 100), 2)
    except (ValueError, ZeroDivisionError):
        return 0.0


def _check_compound_reinvest(portfolio_id: int):
    """포트폴리오 종료 시 복리 그룹 업데이트 (수익 이월)"""
    try:
        pf = db.table("virtual_portfolios") \
            .select("compound_group_id, round_number, capital, current_value") \
            .eq("id", portfolio_id) \
            .execute()

        if not pf.data or not pf.data[0].get("compound_group_id"):
            return  # 복리 그룹 아님

        pf_data = pf.data[0]
        group_id = pf_data["compound_group_id"]
        round_num = pf_data.get("round_number", 1)
        final_value = pf_data.get("current_value", pf_data.get("capital", 0))
        start_capital = pf_data.get("capital", 0)

        round_return_pct = ((final_value / start_capital) - 1) * 100 if start_capital > 0 else 0

        # 그룹 정보 조회
        grp = db.table("compound_groups").select("*").eq("id", group_id).execute()
        if not grp.data:
            return

        group = grp.data[0]
        seed = group["seed_money"]
        goal = group["goal_amount"]
        now = datetime.now().isoformat()

        # 누적 통계
        total_rounds = group.get("total_rounds", 0) + 1
        win_rounds = group.get("win_rounds", 0) + (1 if round_return_pct > 0 else 0)
        loss_rounds = group.get("loss_rounds", 0) + (1 if round_return_pct < 0 else 0)
        best = max(group.get("best_round_pct", 0), round_return_pct)
        worst = min(group.get("worst_round_pct", 0), round_return_pct)
        total_return = ((final_value / seed) - 1) * 100 if seed > 0 else 0
        status = "goal_reached" if final_value >= goal else "active"

        db.table("compound_groups").update({
            "current_capital": round(final_value),
            "current_round": round_num + 1,
            "total_return_pct": round(total_return, 2),
            "total_rounds": total_rounds,
            "win_rounds": win_rounds,
            "loss_rounds": loss_rounds,
            "best_round_pct": round(best, 2),
            "worst_round_pct": round(worst, 2),
            "status": status,
            "updated_at": now,
        }).eq("id", group_id).execute()

        if status == "goal_reached":
            logger.info(f"[복리] 🎉 그룹 #{group_id} 목표 달성! {seed:,.0f} → {final_value:,.0f}")
        else:
            logger.info(f"[복리] 그룹 #{group_id} {round_num}회차 종료: "
                       f"{start_capital:,.0f} → {final_value:,.0f} ({round_return_pct:+.1f}%)")

    except Exception as e:
        logger.warning(f"[복리] 재투자 체크 오류 (무시): {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 복리 그룹 API 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/compound/create")
async def create_compound_group(req: CompoundCreateRequest):
    """복리 그룹 생성 + 1회차 포트폴리오 (종목 있으면 즉시 등록)"""
    now = datetime.now().isoformat()

    try:
        grp_resp = db.table("compound_groups").insert({
            "name": req.name,
            "seed_money": req.seed_money,
            "goal_amount": req.goal_amount,
            "current_capital": req.seed_money,
            "current_round": 1,
            "strategy": req.strategy,
            "status": "active",
            "created_at": now,
            "updated_at": now,
        }).execute()

        group = grp_resp.data[0]
        group_id = group["id"]
        result = {
            "success": True,
            "group_id": group_id,
            "name": req.name,
            "seed_money": req.seed_money,
            "goal_amount": req.goal_amount,
            "portfolio_id": None,
        }

        # 종목이 있으면 1회차 포트폴리오 즉시 생성
        if req.stocks:
            pf_name = f"{req.name} - 1회차"
            pf_resp = db.table("virtual_portfolios").insert({
                "name": pf_name,
                "capital": req.seed_money,
                "strategy": req.strategy,
                "status": "active",
                "stock_count": len(req.stocks),
                "current_value": req.seed_money,
                "compound_group_id": group_id,
                "round_number": 1,
                "created_at": now,
                "updated_at": now,
            }).execute()

            portfolio_id = pf_resp.data[0]["id"]

            per_stock = req.seed_money / len(req.stocks)
            positions = []
            for stock in req.stocks:
                code = stock["code"]
                # ★ 서버에서 네이버 종가 직접 조회
                try:
                    candles = get_daily_candles_naver(code, count=3)
                    if candles and len(candles) > 0:
                        buy_price = candles[-1].get("close", 0)
                    else:
                        buy_price = stock.get("current_price", stock.get("buy_price", 0))
                except Exception:
                    buy_price = stock.get("current_price", stock.get("buy_price", 0))

                if buy_price <= 0:
                    continue
                commission = per_stock * COMMISSION_RATE
                actual_invest = per_stock - commission
                quantity = actual_invest / buy_price

                positions.append({
                    "portfolio_id": portfolio_id,
                    "code": code,
                    "name": stock.get("name", code),
                    "buy_price": buy_price,
                    "current_price": buy_price,
                    "quantity": round(quantity, 4),
                    "invest_amount": round(per_stock),
                    "status": "holding",
                    "peak_price": buy_price,
                    "similarity": stock.get("similarity", 0),
                    "signal": stock.get("signal", ""),
                })

            if positions:
                db.table("virtual_positions").insert(positions).execute()

            result["portfolio_id"] = portfolio_id
            result["stock_count"] = len(positions)
            result["message"] = f"복리 그룹 '{req.name}' + 1회차 등록 ({len(positions)}종목)"
        else:
            result["message"] = f"복리 그룹 '{req.name}' 생성 (종목 대기)"

        logger.info(f"[복리] 그룹 생성: {req.name}, 시드={req.seed_money:,.0f}, 목표={req.goal_amount:,.0f}")
        return result

    except Exception as e:
        logger.error(f"[복리] 그룹 생성 실패: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"복리 그룹 생성 실패: {e}")


@router.get("/compound/list")
async def list_compound_groups():
    """복리 그룹 목록 조회"""
    try:
        resp = db.table("compound_groups") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()

        groups = resp.data or []

        for g in groups:
            g["goal_progress"] = _calc_goal_progress(
                g["seed_money"], g["current_capital"], g["goal_amount"]
            )
            g["growth_multiple"] = round(g["current_capital"] / g["seed_money"], 2) if g["seed_money"] > 0 else 1

            # 해당 그룹의 포트폴리오 수
            pf_resp = db.table("virtual_portfolios") \
                .select("id, status") \
                .eq("compound_group_id", g["id"]) \
                .execute()
            g["portfolio_count"] = len(pf_resp.data or [])

        return {"success": True, "groups": groups, "total": len(groups)}

    except Exception as e:
        logger.error(f"[복리] 목록 조회 실패: {e}")
        raise HTTPException(500, str(e))


@router.get("/compound/{group_id}")
async def get_compound_detail(group_id: int):
    """복리 그룹 상세 (회차별 이력 포함)"""
    try:
        grp = db.table("compound_groups").select("*").eq("id", group_id).execute()
        if not grp.data:
            raise HTTPException(404, f"복리 그룹 #{group_id}를 찾을 수 없습니다")

        group = grp.data[0]
        group["goal_progress"] = _calc_goal_progress(
            group["seed_money"], group["current_capital"], group["goal_amount"]
        )
        group["growth_multiple"] = round(group["current_capital"] / group["seed_money"], 2) if group["seed_money"] > 0 else 1

        # 회차별 포트폴리오
        pf_resp = db.table("virtual_portfolios") \
            .select("id, name, status, round_number, capital, current_value, "
                    "total_return_pct, total_return_won, win_count, loss_count, "
                    "stock_count, created_at, closed_at") \
            .eq("compound_group_id", group_id) \
            .order("round_number") \
            .execute()

        rounds = pf_resp.data or []

        # 회차별 누적 자본 추적
        cumulative = group["seed_money"]
        for r in rounds:
            r["start_capital"] = round(cumulative)
            end_val = r.get("current_value", cumulative)
            r["end_capital"] = round(end_val)
            r["round_return_pct"] = round(((end_val / cumulative) - 1) * 100, 2) if cumulative > 0 else 0
            if r["status"] != "active":
                cumulative = end_val

        return {"success": True, "group": group, "rounds": rounds}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[복리] 상세 조회 실패: {e}")
        raise HTTPException(500, str(e))


@router.post("/compound/{group_id}/next-round")
async def start_next_round(group_id: int, req: CompoundNextRoundRequest):
    """다음 회차 포트폴리오 생성 (종목 선택 후 호출)"""
    now = datetime.now().isoformat()

    try:
        grp = db.table("compound_groups").select("*").eq("id", group_id).execute()
        if not grp.data:
            raise HTTPException(404, f"복리 그룹 #{group_id}를 찾을 수 없습니다")

        group = grp.data[0]

        if group["status"] == "goal_reached":
            raise HTTPException(400, "이미 목표 달성한 그룹입니다")
        if group["status"] == "stopped":
            raise HTTPException(400, "중단된 그룹입니다")

        # 현재 활성 포트폴리오 체크
        active_check = db.table("virtual_portfolios") \
            .select("id") \
            .eq("compound_group_id", group_id) \
            .eq("status", "active") \
            .execute()

        if active_check.data:
            raise HTTPException(400, f"아직 활성 포트폴리오(#{active_check.data[0]['id']})가 있습니다. 먼저 청산해주세요.")

        if not req.stocks:
            raise HTTPException(400, "종목을 선택해주세요")

        round_num = group["current_round"]
        capital = group["current_capital"]

        # 포트폴리오 생성
        pf_name = f"{group['name']} - {round_num}회차"
        pf_resp = db.table("virtual_portfolios").insert({
            "name": pf_name,
            "capital": capital,
            "strategy": req.preset,
            "status": "active",
            "stock_count": len(req.stocks),
            "current_value": capital,
            "compound_group_id": group_id,
            "round_number": round_num,
            "created_at": now,
            "updated_at": now,
        }).execute()

        portfolio_id = pf_resp.data[0]["id"]

        per_stock = capital / len(req.stocks)
        positions = []
        for stock in req.stocks:
            code = stock["code"]
            # ★ 서버에서 네이버 종가 직접 조회
            try:
                candles = get_daily_candles_naver(code, count=3)
                if candles and len(candles) > 0:
                    buy_price = candles[-1].get("close", 0)
                else:
                    buy_price = stock.get("current_price", stock.get("buy_price", 0))
            except Exception:
                buy_price = stock.get("current_price", stock.get("buy_price", 0))

            if buy_price <= 0:
                continue
            commission = per_stock * COMMISSION_RATE
            actual_invest = per_stock - commission
            quantity = actual_invest / buy_price

            positions.append({
                "portfolio_id": portfolio_id,
                "code": code,
                "name": stock.get("name", code),
                "buy_price": buy_price,
                "current_price": buy_price,
                "quantity": round(quantity, 4),
                "invest_amount": round(per_stock),
                "status": "holding",
                "peak_price": buy_price,
                "similarity": stock.get("similarity", 0),
                "signal": stock.get("signal", ""),
            })

        if positions:
            db.table("virtual_positions").insert(positions).execute()

        logger.info(f"[복리] 그룹 #{group_id} {round_num}회차 시작: 원금 {capital:,.0f}원, {len(positions)}종목")

        return {
            "success": True,
            "group_id": group_id,
            "portfolio_id": portfolio_id,
            "round_number": round_num,
            "capital": capital,
            "stock_count": len(positions),
            "message": f"{round_num}회차 시작 ({len(positions)}종목, {capital:,.0f}원)",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[복리] 다음 회차 생성 실패: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, str(e))


@router.put("/compound/{group_id}/stop")
async def stop_compound_group(group_id: int):
    """복리 그룹 중단"""
    try:
        now = datetime.now().isoformat()
        db.table("compound_groups").update({
            "status": "stopped",
            "updated_at": now,
        }).eq("id", group_id).execute()

        return {"success": True, "message": f"복리 그룹 #{group_id} 중단됨"}
    except Exception as e:
        raise HTTPException(500, str(e))


@router.put("/compound/{group_id}/edit")
async def edit_compound_group(group_id: int, req: dict):
    """복리 그룹 수정 (이름, 목표금액, 전략)"""
    try:
        grp = db.table("compound_groups").select("*").eq("id", group_id).execute()
        if not grp.data:
            raise HTTPException(404, f"복리 그룹 #{group_id}를 찾을 수 없습니다")

        update_data = {"updated_at": datetime.now().isoformat()}

        if "name" in req and req["name"]:
            update_data["name"] = req["name"]
        if "goal_amount" in req and req["goal_amount"] > 0:
            update_data["goal_amount"] = req["goal_amount"]
        if "strategy" in req and req["strategy"]:
            update_data["strategy"] = req["strategy"]

        # 시드머니는 아직 1회차 시작 전이면 수정 가능
        group = grp.data[0]
        if "seed_money" in req and req["seed_money"] > 0:
            if group.get("total_rounds", 0) == 0 and group.get("current_round", 1) == 1:
                update_data["seed_money"] = req["seed_money"]
                update_data["current_capital"] = req["seed_money"]
            else:
                logger.warning(f"[복리] 그룹 #{group_id} 이미 회차 진행 중 → 시드머니 수정 불가")

        db.table("compound_groups").update(update_data).eq("id", group_id).execute()

        logger.info(f"[복리] 그룹 #{group_id} 수정: {update_data}")
        return {"success": True, "message": f"복리 그룹 '{update_data.get('name', group['name'])}' 수정 완료"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[복리] 그룹 수정 실패: {e}")
        raise HTTPException(500, str(e))


@router.delete("/compound/{group_id}")
async def delete_compound_group(group_id: int):
    """복리 그룹 삭제 (연결된 포트폴리오는 compound_group_id만 해제)"""
    try:
        grp = db.table("compound_groups").select("id, name").eq("id", group_id).execute()
        if not grp.data:
            raise HTTPException(404, f"복리 그룹 #{group_id}를 찾을 수 없습니다")

        name = grp.data[0]["name"]

        # 연결된 포트폴리오의 compound_group_id 해제 (포트폴리오 자체는 보존)
        db.table("virtual_portfolios").update({
            "compound_group_id": None,
            "updated_at": datetime.now().isoformat(),
        }).eq("compound_group_id", group_id).execute()

        # 그룹 삭제
        db.table("compound_groups").delete().eq("id", group_id).execute()

        logger.info(f"[복리] 그룹 #{group_id} '{name}' 삭제")
        return {"success": True, "message": f"복리 그룹 '{name}' 삭제 완료"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[복리] 그룹 삭제 실패: {e}")
        raise HTTPException(500, str(e))
