"""KIS 계좌 조회 모듈 (잔고, 보유종목, 주문내역)"""
import requests
import logging
from app.services.kis_auth import get_kis
from app.core.config import config

logger = logging.getLogger(__name__)


def get_account_balance(is_live=False):
    """예수금(주문가능금액) 조회"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC8908R" if is_live else "VTTC8908R"
    p = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "DNCA_TOTL_AMT": "",
        "INQUS_DVSN_CD": "02",
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "00",
        "PRCS_DVSN_CD": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            headers=h, params=p, timeout=10,
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d.get("output", {})
            return {
                "ord_psbl_cash": int(o.get("ord_psbl_cash", 0)),
                "nrcvb_buy_amt": int(o.get("nrcvb_buy_amt", 0)),
                "tot_ord_psbl_amt": int(o.get("tot_ord_psbl_amt", 0)),
            }
        else:
            logger.warning(f"[잔고조회] 실패: {d.get('msg1', '')}")
            return {"error": d.get("msg1", "조회 실패")}
    except Exception as e:
        logger.error(f"[잔고조회] 오류: {e}")
        return {"error": str(e)}


def get_holdings(is_live=False):
    """보유종목 조회"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC8434R" if is_live else "VTTC8434R"
    p = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN_CD": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=h, params=p, timeout=10,
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            output1 = d.get("output1", [])
            output2 = d.get("output2", [{}])
            holdings = []
            for item in output1:
                qty = int(item.get("hldg_qty", 0))
                if qty <= 0:
                    continue
                holdings.append({
                    "code": item.get("pdno", ""),
                    "name": item.get("prdt_name", ""),
                    "quantity": qty,
                    "buy_avg_price": int(float(item.get("pchs_avg_pric", 0))),
                    "current_price": int(item.get("prpr", 0)),
                    "eval_amount": int(item.get("evlu_amt", 0)),
                    "profit_loss": int(item.get("evlu_pfls_amt", 0)),
                    "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                    "sellable_qty": int(item.get("ord_psbl_qty", 0)),
                })
            summary = output2[0] if output2 else {}
            return {
                "holdings": holdings,
                "summary": {
                    "total_eval": int(summary.get("tot_evlu_amt", 0)),
                    "total_buy": int(summary.get("pchs_amt_smtl_amt", 0)),
                    "total_profit": int(summary.get("evlu_pfls_smtl_amt", 0)),
                    "total_profit_pct": float(summary.get("tot_evlu_pfls_rt", 0)) if summary.get("tot_evlu_pfls_rt") else 0,
                    "deposit": int(summary.get("dnca_tot_amt", 0)),
                    "total_asset": int(summary.get("tot_evlu_amt", 0)) + int(summary.get("dnca_tot_amt", 0)),
                },
            }
        else:
            logger.warning(f"[보유종목] 실패: {d.get('msg1', '')}")
            return {"holdings": [], "summary": {}, "error": d.get("msg1", "조회 실패")}
    except Exception as e:
        logger.error(f"[보유종목] 오류: {e}")
        return {"holdings": [], "summary": {}, "error": str(e)}


def get_order_history(is_live=False):
    """당일 주문내역 조회"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC8001R" if is_live else "VTTC8001R"
    from datetime import datetime
    today = datetime.now().strftime("%Y%m%d")
    p = {
        "CANO": config.KIS_CANO,
        "ACNT_PRDT_CD": config.KIS_ACNT_PRDT_CD,
        "INQR_STRT_DT": today,
        "INQR_END_DT": today,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": "",
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            headers=h, params=p, timeout=10,
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            orders = []
            for item in d.get("output1", []):
                orders.append({
                    "order_no": item.get("odno", ""),
                    "code": item.get("pdno", ""),
                    "name": item.get("prdt_name", ""),
                    "side": "매수" if item.get("sll_buy_dvsn_cd") == "02" else "매도",
                    "order_qty": int(item.get("ord_qty", 0)),
                    "exec_qty": int(item.get("tot_ccld_qty", 0)),
                    "order_price": int(item.get("ord_unpr", 0)),
                    "exec_price": int(float(item.get("avg_prvs", 0))) if item.get("avg_prvs") else 0,
                    "status": "체결" if int(item.get("tot_ccld_qty", 0)) > 0 else "미체결",
                    "order_time": item.get("ord_tmd", ""),
                })
            return {"orders": orders}
        else:
            return {"orders": [], "error": d.get("msg1", "조회 실패")}
    except Exception as e:
        logger.error(f"[주문내역] 오류: {e}")
        return {"orders": [], "error": str(e)}
