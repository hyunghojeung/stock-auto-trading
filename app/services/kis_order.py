"""KIS 주문 모듈 — 한국투자증권 매수/매도 주문
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v2: hashkey 보안 적용 + 상세 응답 처리

매수: TTTC0802U(실전) / VTTC0802U(모의)
매도: TTTC0801U(실전) / VTTC0801U(모의)
URL: /uapi/domestic-stock/v1/trading/order-cash

ORD_DVSN (주문구분):
  00: 지정가
  01: 시장가
  02: 조건부지정가
  03: 최유리지정가
  04: 최우선지정가
  05: 장전 시간외
  06: 장후 시간외
  07: 시간외 단일가
"""
import requests
import logging
from app.services.kis_auth import get_kis
from app.core.config import config

logger = logging.getLogger(__name__)


def buy_stock(code, quantity, price=0, is_live=False):
    """매수 주문
    price=0: 시장가(01), price>0: 지정가(00)
    ★ hashkey 보안 헤더 자동 포함
    """
    auth = get_kis(is_live)
    body = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00" if price > 0 else "01",  # 00:지정가, 01:시장가
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }
    # ★ hashkey 포함 헤더
    h = auth.get_order_headers(body)
    h["tr_id"] = "TTTC0802U" if is_live else "VTTC0802U"

    mode = "실전" if is_live else "모의"
    try:
        r = requests.post(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/order-cash",
            headers=h, json=body, timeout=10
        )
        d = r.json()
        success = d.get("rt_cd") == "0"
        if success:
            output = d.get("output", {})
            logger.info(f"[매수 성공] ({mode}) {code} {quantity}주 | 주문번호: {output.get('ODNO', '')}")
        else:
            logger.warning(f"[매수 실패] ({mode}) {code}: {d.get('msg1', '')}")
        return {
            "success": success,
            "data": d,
            "order_no": d.get("output", {}).get("ODNO", "") if success else "",
            "message": d.get("msg1", ""),
        }
    except Exception as e:
        logger.error(f"[매수 오류] ({mode}) {code}: {e}")
        return {"success": False, "error": str(e)}


def sell_stock(code, quantity, price=0, is_live=False):
    """매도 주문
    price=0: 시장가(01), price>0: 지정가(00)
    ★ hashkey 보안 헤더 자동 포함
    """
    auth = get_kis(is_live)
    body = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00" if price > 0 else "01",
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }
    h = auth.get_order_headers(body)
    h["tr_id"] = "TTTC0801U" if is_live else "VTTC0801U"

    mode = "실전" if is_live else "모의"
    try:
        r = requests.post(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/order-cash",
            headers=h, json=body, timeout=10
        )
        d = r.json()
        success = d.get("rt_cd") == "0"
        if success:
            output = d.get("output", {})
            logger.info(f"[매도 성공] ({mode}) {code} {quantity}주 | 주문번호: {output.get('ODNO', '')}")
        else:
            logger.warning(f"[매도 실패] ({mode}) {code}: {d.get('msg1', '')}")
        return {
            "success": success,
            "data": d,
            "order_no": d.get("output", {}).get("ODNO", "") if success else "",
            "message": d.get("msg1", ""),
        }
    except Exception as e:
        logger.error(f"[매도 오류] ({mode}) {code}: {e}")
        return {"success": False, "error": str(e)}
