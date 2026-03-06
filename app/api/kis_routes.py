"""한국투자증권(KIS) 계좌 조회 & 연결 관리 API
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
계좌 잔고, 체결내역, 연결 상태 확인, 매수/매도 수동 주문

GET  /api/kis/status         — KIS API 연결 상태 확인
GET  /api/kis/balance        — 계좌 잔고 조회 (예수금 + 보유종목)
GET  /api/kis/orders/today   — 당일 체결내역 조회
POST /api/kis/order/buy      — 수동 매수 주문
POST /api/kis/order/sell     — 수동 매도 주문
GET  /api/kis/config         — 현재 KIS 설정 상태 (Key 유무)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
import logging
import traceback

from app.core.config import config
from app.services.kis_auth import get_kis
from app.services.kis_order import buy_stock, sell_stock

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/kis", tags=["KIS 증권"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 요청 모델
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class OrderRequest(BaseModel):
    code: str               # 종목코드 (6자리)
    quantity: int           # 수량
    price: int = 0          # 0 = 시장가, >0 = 지정가
    is_live: bool = False   # False=모의, True=실전
    password: str = ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KIS 설정 상태 확인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/config")
async def get_kis_config():
    """현재 KIS API Key 설정 상태 확인 (Key 값 자체는 노출하지 않음)"""
    return {
        "paper": {
            "app_key_set": bool(config.KIS_APP_KEY),
            "app_secret_set": bool(config.KIS_APP_SECRET),
            "base_url": config.KIS_BASE_URL,
        },
        "live": {
            "app_key_set": bool(config.KIS_LIVE_APP_KEY),
            "app_secret_set": bool(config.KIS_LIVE_APP_SECRET),
            "base_url": config.KIS_LIVE_BASE_URL,
        },
        "account": {
            "cano_set": bool(config.KIS_CANO),
            "cano_masked": f"****{config.KIS_CANO[-4:]}" if len(config.KIS_CANO) >= 4 else "",
            "acnt_prdt_cd": config.KIS_ACNT_PRDT_CD,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KIS 연결 테스트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/status")
async def check_kis_status(is_live: bool = False):
    """KIS API 연결 상태 확인 (토큰 발급 테스트)"""
    mode = "실전" if is_live else "모의"

    try:
        auth = get_kis(is_live)

        # API Key 미설정 체크
        if not auth.app_key or not auth.app_secret:
            return {
                "connected": False,
                "mode": mode,
                "error": f"{mode}투자 API Key가 설정되지 않았습니다",
            }

        # 토큰 발급 시도
        token = auth.get_token()
        if not token:
            return {
                "connected": False,
                "mode": mode,
                "error": "토큰 발급 실패",
            }

        return {
            "connected": True,
            "mode": mode,
            "token_expires_at": auth.token_expired_at.isoformat() if auth.token_expired_at else None,
            "base_url": auth.base_url,
            "account": f"****{config.KIS_CANO[-4:]}" if len(config.KIS_CANO) >= 4 else "미설정",
        }

    except Exception as e:
        logger.error(f"KIS 연결 테스트 실패 ({mode}): {e}")
        return {
            "connected": False,
            "mode": mode,
            "error": str(e),
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 계좌 잔고 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/balance")
async def get_balance(is_live: bool = False):
    """계좌 잔고 조회 (예수금 + 보유종목)
    - 모의: VTTC8434R
    - 실전: TTTC8434R
    """
    mode = "실전" if is_live else "모의"

    try:
        auth = get_kis(is_live)
        if not auth.app_key:
            raise HTTPException(400, f"{mode}투자 API Key가 설정되지 않았습니다")

        headers = auth.get_headers()
        headers["tr_id"] = "TTTC8434R" if is_live else "VTTC8434R"

        params = {
            "CANO": config.KIS_CANO,
            "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        resp = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=headers, params=params, timeout=10
        )
        data = resp.json()

        if data.get("rt_cd") != "0":
            return {
                "success": False,
                "mode": mode,
                "error": data.get("msg1", "조회 실패"),
            }

        # 보유종목 파싱
        holdings = []
        for item in data.get("output1", []):
            if int(item.get("hldg_qty", 0)) > 0:
                buy_price = float(item.get("pchs_avg_pric", 0))
                current_price = float(item.get("prpr", 0))
                quantity = int(item.get("hldg_qty", 0))
                profit_pct = float(item.get("evlu_pfls_rt", 0))
                profit_amt = float(item.get("evlu_pfls_amt", 0))

                holdings.append({
                    "code": item.get("pdno", ""),
                    "name": item.get("prdt_name", ""),
                    "quantity": quantity,
                    "buy_price": buy_price,
                    "current_price": current_price,
                    "profit_pct": profit_pct,
                    "profit_amt": profit_amt,
                    "eval_amt": float(item.get("evlu_amt", 0)),
                })

        # 계좌 요약
        summary = data.get("output2", [{}])
        account_summary = summary[0] if summary else {}

        return {
            "success": True,
            "mode": mode,
            "account": f"****{config.KIS_CANO[-4:]}" if len(config.KIS_CANO) >= 4 else "",
            "holdings": holdings,
            "holdings_count": len(holdings),
            "summary": {
                "total_eval": float(account_summary.get("tot_evlu_amt", 0)),
                "total_buy": float(account_summary.get("pchs_amt_smtl_amt", 0)),
                "total_profit": float(account_summary.get("evlu_pfls_smtl_amt", 0)),
                "total_profit_pct": float(account_summary.get("tot_evlu_pfls_rt", 0)),
                "deposit": float(account_summary.get("dnca_tot_amt", 0)),
                "available_order": float(account_summary.get("nass_amt", 0)),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"잔고 조회 실패 ({mode}): {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"잔고 조회 실패: {str(e)}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 당일 체결내역 조회
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/orders/today")
async def get_today_orders(is_live: bool = False):
    """당일 주문/체결내역 조회
    - 모의: VTTC8001R
    - 실전: TTTC8001R
    """
    mode = "실전" if is_live else "모의"

    try:
        auth = get_kis(is_live)
        if not auth.app_key:
            raise HTTPException(400, f"{mode}투자 API Key가 설정되지 않았습니다")

        headers = auth.get_headers()
        headers["tr_id"] = "TTTC8001R" if is_live else "VTTC8001R"

        from datetime import datetime
        from app.core.config import KST
        today = datetime.now(KST).strftime("%Y%m%d")

        params = {
            "CANO": config.KIS_CANO,
            "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",  # 00=전체, 01=매도, 02=매수
            "INQR_DVSN": "01",
            "PDNO": "",
            "CCLD_DVSN": "01",        # 01=체결, 02=미체결
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        resp = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            headers=headers, params=params, timeout=10
        )
        data = resp.json()

        if data.get("rt_cd") != "0":
            return {
                "success": False,
                "mode": mode,
                "error": data.get("msg1", "조회 실패"),
            }

        orders = []
        for item in data.get("output1", []):
            orders.append({
                "order_no": item.get("odno", ""),
                "code": item.get("pdno", ""),
                "name": item.get("prdt_name", ""),
                "order_type": "매수" if item.get("sll_buy_dvsn_cd") == "02" else "매도",
                "order_qty": int(item.get("ord_qty", 0)),
                "filled_qty": int(item.get("tot_ccld_qty", 0)),
                "order_price": float(item.get("ord_unpr", 0)),
                "filled_price": float(item.get("avg_prvs", 0)),
                "order_time": item.get("ord_tmd", ""),
                "status": "체결" if int(item.get("tot_ccld_qty", 0)) > 0 else "미체결",
            })

        return {
            "success": True,
            "mode": mode,
            "date": today,
            "orders": orders,
            "total_count": len(orders),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"체결내역 조회 실패 ({mode}): {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"체결내역 조회 실패: {str(e)}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 수동 매수/매도 주문
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/order/buy")
async def manual_buy(req: OrderRequest):
    """수동 매수 주문"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    if len(req.code) != 6:
        raise HTTPException(400, "종목코드는 6자리여야 합니다")
    if req.quantity <= 0:
        raise HTTPException(400, "수량은 1 이상이어야 합니다")

    mode = "실전" if req.is_live else "모의"
    result = buy_stock(req.code, req.quantity, req.price, is_live=req.is_live)

    order_type = f"지정가 {req.price:,}원" if req.price > 0 else "시장가"
    logger.info(f"수동 매수 ({mode}): {req.code} {req.quantity}주 {order_type} → {result}")

    return {
        "mode": mode,
        "action": "매수",
        "code": req.code,
        "quantity": req.quantity,
        "order_type": order_type,
        **result,
    }

@router.post("/order/sell")
async def manual_sell(req: OrderRequest):
    """수동 매도 주문"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    if len(req.code) != 6:
        raise HTTPException(400, "종목코드는 6자리여야 합니다")
    if req.quantity <= 0:
        raise HTTPException(400, "수량은 1 이상이어야 합니다")

    mode = "실전" if req.is_live else "모의"
    result = sell_stock(req.code, req.quantity, req.price, is_live=req.is_live)

    order_type = f"지정가 {req.price:,}원" if req.price > 0 else "시장가"
    logger.info(f"수동 매도 ({mode}): {req.code} {req.quantity}주 {order_type} → {result}")

    return {
        "mode": mode,
        "action": "매도",
        "code": req.code,
        "quantity": req.quantity,
        "order_type": order_type,
        **result,
    }
