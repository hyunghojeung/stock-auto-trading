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
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
from datetime import timedelta
import asyncio
import requests
import logging
import traceback
import json
import time

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

class LiveKeyRequest(BaseModel):
    app_key: str
    app_secret: str
    cano: str = ""
    acnt_prdt_cd: str = "01"
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


@router.post("/config/live")
async def set_live_config(req: LiveKeyRequest):
    """실전투자 API 키 동적 설정 (서버 재시작 없이 즉시 적용)"""
    if req.password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")

    from app.services.kis_auth import kis_live

    # config 객체에 반영
    config.KIS_LIVE_APP_KEY = req.app_key
    config.KIS_LIVE_APP_SECRET = req.app_secret
    if req.cano:
        config.KIS_LIVE_CANO = req.cano
    if req.acnt_prdt_cd:
        config.KIS_ACNT_PRDT_CD = req.acnt_prdt_cd

    # kis_live 인스턴스에도 반영 + 기존 토큰 초기화
    kis_live.app_key = req.app_key
    kis_live.app_secret = req.app_secret
    kis_live.access_token = None
    kis_live.token_expired_at = None
    kis_live.websocket_approval_key = None

    logger.info(f"[KIS] 실전 API 키 동적 설정 완료 (cano: {'****' + req.cano[-4:] if len(req.cano) >= 4 else '미설정'})")
    return {
        "success": True,
        "message": "실전투자 키가 적용되었습니다. 토큰은 다음 API 호출 시 자동 발급됩니다.",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 토큰 관리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/token/revoke")
async def revoke_token(is_live: bool = False, password: str = ""):
    """접근토큰 폐기 — 토큰을 더 이상 사용하지 않을 때"""
    if password != config.SITE_PASSWORD:
        raise HTTPException(403, "비밀번호가 틀렸습니다")
    auth = get_kis(is_live)
    result = auth.revoke_token()
    return {"mode": "실전" if is_live else "모의", **result}

@router.get("/token/info")
async def get_token_info(is_live: bool = False):
    """현재 토큰 상태 확인 (발급 여부, 만료 시간)"""
    auth = get_kis(is_live)
    mode = "실전" if is_live else "모의"
    return {
        "mode": mode,
        "has_token": bool(auth.access_token),
        "expires_at": auth.token_expired_at.isoformat() if auth.token_expired_at else None,
        "has_ws_key": bool(auth.websocket_approval_key),
    }

@router.post("/websocket/key")
async def get_websocket_key(is_live: bool = False):
    """실시간 웹소켓 접속키 발급"""
    auth = get_kis(is_live)
    key = auth.get_websocket_approval_key()
    if not key:
        raise HTTPException(500, "웹소켓 접속키 발급 실패")
    return {
        "mode": "실전" if is_live else "모의",
        "approval_key": key,
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KIS 시세 조회 API (프론트 대시보드용)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/price/{code}")
async def get_stock_price(code: str, is_live: bool = False):
    """종목 현재가 시세 조회 (KIS API)"""
    from app.services.kis_stock import get_current_price
    result = get_current_price(code, is_live=is_live)
    if not result:
        raise HTTPException(404, f"시세 조회 실패: {code}")
    return result

@router.get("/orderbook/{code}")
async def get_stock_orderbook(code: str, is_live: bool = False):
    """종목 호가창 조회 (KIS API)"""
    from app.services.kis_stock import get_orderbook
    result = get_orderbook(code, is_live=is_live)
    if not result:
        raise HTTPException(404, f"호가 조회 실패: {code}")
    return result

@router.get("/candles/{code}")
async def get_stock_candles(code: str, period: int = 30, period_code: str = "D", is_live: bool = False):
    """종목 기간별 시세 조회 (D:일봉, W:주봉, M:월봉)"""
    from app.services.kis_stock import get_period_candles
    from datetime import datetime
    from app.core.config import KST
    end_date = datetime.now(KST).strftime("%Y%m%d")
    days = {"D": period * 2, "W": period * 10, "M": period * 35, "Y": period * 400}
    start_date = (datetime.now(KST) - timedelta(days=days.get(period_code, period * 2))).strftime("%Y%m%d")
    result = get_period_candles(code, start_date, end_date, period_code, is_live=is_live)
    return result

@router.get("/minutes/{code}")
async def get_stock_minutes(code: str, count: int = 30, is_live: bool = False):
    """종목 분봉 조회 (실전계좌 전용, 모의투자는 현재가 대체)"""
    from app.services.kis_stock import get_minute_candles
    result = get_minute_candles(code, count=count, is_live=is_live)
    return {"code": code, "count": len(result), "candles": result, "is_live": is_live}


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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SSE 실시간 시세 스트리밍 (MCP 스타일)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/sse/price")
async def sse_price_stream(codes: str, is_live: bool = False, interval: int = 3):
    """실시간 시세 SSE 스트림 (MCP 호환 형식)

    codes: 쉼표로 구분된 종목코드 (예: 005930,000660,035720)
    interval: 갱신 주기 초 (기본 3초, 최소 2초)
    """
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if not code_list:
        raise HTTPException(400, "종목코드를 입력하세요")
    if len(code_list) > 10:
        raise HTTPException(400, "최대 10종목까지 가능합니다")
    interval = max(2, min(interval, 30))

    async def event_generator():
        from app.services.kis_stock import get_current_price
        yield f"event: connected\ndata: {json.dumps({'codes': code_list, 'interval': interval})}\n\n"

        while True:
            prices = []
            for code in code_list:
                try:
                    p = get_current_price(code, is_live=is_live)
                    if p:
                        prices.append(p)
                except Exception:
                    pass

            payload = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "prices": prices,
            }
            yield f"event: price\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
            await asyncio.sleep(interval)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/sse/portfolio")
async def sse_portfolio_stream(is_live: bool = False, interval: int = 10):
    """계좌 잔고 SSE 실시간 스트림 — 포트폴리오 자동 갱신"""
    interval = max(5, min(interval, 60))

    async def event_generator():
        yield f"event: connected\ndata: {json.dumps({'mode': '실전' if is_live else '모의', 'interval': interval})}\n\n"

        while True:
            try:
                auth = get_kis(is_live)
                headers = auth.get_headers()
                tr_id = "TTTC8434R" if is_live else "VTTC8434R"
                headers["tr_id"] = tr_id

                params = {
                    "CANO": config.KIS_LIVE_CANO if is_live else config.KIS_CANO,
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

                if data.get("rt_cd") == "0":
                    holdings = []
                    for item in data.get("output1", []):
                        qty = int(item.get("hldg_qty", 0))
                        if qty <= 0:
                            continue
                        holdings.append({
                            "code": item.get("pdno", ""),
                            "name": item.get("prdt_name", ""),
                            "quantity": qty,
                            "avg_price": float(item.get("pchs_avg_pric", 0)),
                            "current_price": int(item.get("prpr", 0)),
                            "profit_amt": int(item.get("evlu_pfls_amt", 0)),
                            "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                        })

                    output2 = data.get("output2", [{}])
                    summary_data = output2[0] if isinstance(output2, list) and output2 else output2
                    summary = {
                        "total_eval": int(summary_data.get("tot_evlu_amt", 0)),
                        "deposit": int(summary_data.get("dnca_tot_amt", 0)),
                        "total_profit": int(summary_data.get("evlu_pfls_smtl_amt", 0)),
                        "total_purchase": int(summary_data.get("pchs_amt_smtl_amt", 0)),
                    }
                    total_purchase = summary["total_purchase"]
                    summary["total_profit_pct"] = round(
                        summary["total_profit"] / total_purchase * 100, 2
                    ) if total_purchase > 0 else 0

                    payload = {
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "summary": summary,
                        "holdings": holdings,
                    }
                    yield f"event: portfolio\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                else:
                    yield f"event: error\ndata: {json.dumps({'error': data.get('msg1', '조회 실패')})}\n\n"

            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"

            await asyncio.sleep(interval)
