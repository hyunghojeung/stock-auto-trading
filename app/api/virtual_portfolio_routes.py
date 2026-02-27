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
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.core.database import db

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

        return {"success": True, "message": "포트폴리오가 청산되었습니다"}

    except Exception as e:
        logger.error(f"[가상포트] 청산 실패: {e}")
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
