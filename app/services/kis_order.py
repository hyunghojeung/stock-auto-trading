"""KIS 주문 모듈"""
import requests
from app.services.kis_auth import get_kis
from app.core.config import config

def buy_stock(code, quantity, price=0, is_live=False):
    """매수 주문 (price=0이면 시장가)"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC0802U" if is_live else "VTTC0802U"
    body = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01" if price > 0 else "06",
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }
    try:
        r = requests.post(f"{auth.base_url}/uapi/domestic-stock/v1/trading/order-cash", headers=h, json=body, timeout=10)
        d = r.json()
        return {"success": d.get("rt_cd") == "0", "data": d}
    except Exception as e:
        return {"success": False, "error": str(e)}

def sell_stock(code, quantity, price=0, is_live=False):
    """매도 주문"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC0801U" if is_live else "VTTC0801U"
    body = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01" if price > 0 else "06",
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }
    try:
        r = requests.post(f"{auth.base_url}/uapi/domestic-stock/v1/trading/order-cash", headers=h, json=body, timeout=10)
        d = r.json()
        return {"success": d.get("rt_cd") == "0", "data": d}
    except Exception as e:
        return {"success": False, "error": str(e)}
