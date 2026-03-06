"""KIS 시세 조회"""
import requests
from datetime import datetime, timedelta
from app.services.kis_auth import get_kis


def get_current_price(code, is_live=False):
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010100"
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-price", headers=h, params=p, timeout=10)
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d["output"]
            return {"code": code, "price": int(o["stck_prpr"]), "change": int(o["prdy_vrss"]),
                    "change_pct": float(o["prdy_ctrt"]), "volume": int(o["acml_vol"]),
                    "high": int(o["stck_hgpr"]), "low": int(o["stck_lwpr"]), "open": int(o["stck_oprc"])}
    except Exception as e:
        print(f"[현재가 오류] {code}: {e}")
    return None


def get_daily_candles(code, period=30, is_live=False):
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010400"
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=period*2)).strftime("%Y%m%d")
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
         "FID_INPUT_DATE_1": start, "FID_INPUT_DATE_2": end,
         "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"}
    try:
        r = requests.get(f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price", headers=h, params=p, timeout=10)
        d = r.json()
        if d.get("rt_cd") == "0":
            return [{"date": i["stck_bsop_date"], "open": int(i["stck_oprc"]), "high": int(i["stck_hgpr"]),
                     "low": int(i["stck_lwpr"]), "close": int(i["stck_clpr"]), "volume": int(i["acml_vol"])}
                    for i in d.get("output", [])[:period]]
    except Exception as e:
        print(f"[일봉 오류] {code}: {e}")
    return []


def get_minute_candles(code, count=30, is_live=False):
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010200"
    p = {"FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
         "FID_INPUT_HOUR_1": datetime.now().strftime("%H%M%S"), "FID_PW_DATA_INCU_YN": "Y"}
    try:
        r = requests.get(f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice", headers=h, params=p, timeout=10)
        d = r.json()
        if d.get("rt_cd") == "0":
            output2 = d.get("output2", [])
            if isinstance(output2, dict):
                output2 = [output2]
            if not isinstance(output2, list):
                return []
            return [{"time": i.get("stck_cntg_hour",""), "open": int(i.get("stck_oprc",0)),
                     "high": int(i.get("stck_hgpr",0)), "low": int(i.get("stck_lwpr",0)),
                     "close": int(i.get("stck_prpr",0)), "volume": int(i.get("cntg_vol",0))}
                    for i in output2[:count]]
    except Exception as e:
        print(f"[분봉 오류] {code}: {e}")
    return []


def get_account_balance(is_live=False):
    """계좌 잔고 조회"""
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "TTTC8434R" if is_live else "VTTC8434R"
    from app.core.config import config
    p = {
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
    try:
        r = requests.get(f"{auth.base_url}/uapi/domestic-stock/v1/trading/inquire-balance", headers=h, params=p, timeout=10)
        d = r.json()
        if d.get("rt_cd") == "0":
            holdings = []
            for item in d.get("output1", []):
                if int(item.get("hldg_qty", 0)) > 0:
                    holdings.append({
                        "code": item.get("pdno", ""),
                        "name": item.get("prdt_name", ""),
                        "quantity": int(item.get("hldg_qty", 0)),
                        "buy_price": float(item.get("pchs_avg_pric", 0)),
                        "current_price": int(item.get("prpr", 0)),
                        "profit_pct": float(item.get("evlu_pfls_rt", 0)),
                        "profit_won": int(item.get("evlu_pfls_amt", 0)),
                        "eval_amount": int(item.get("evlu_amt", 0)),
                    })
            output2 = d.get("output2", [{}])
            summary = output2[0] if isinstance(output2, list) and output2 else output2
            return {
                "holdings": holdings,
                "total_eval": int(summary.get("tot_evlu_amt", 0)),
                "total_profit": int(summary.get("evlu_pfls_smtl_amt", 0)),
                "total_deposit": int(summary.get("dnca_tot_amt", 0)),
                "total_buy_amount": int(summary.get("pchs_amt_smtl_amt", 0)),
            }
    except Exception as e:
        print(f"[계좌잔고 오류] {e}")
    return None


def get_orderbook(code, is_live=False):
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010200"
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn", headers=h, params=p, timeout=10)
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d["output1"]
            ask = sum(int(o.get(f"askp_rsqn{i}",0)) for i in range(1,11))
            bid = sum(int(o.get(f"bidp_rsqn{i}",0)) for i in range(1,11))
            return {"total_ask": ask, "total_bid": bid, "bid_ratio": bid/ask if ask > 0 else 0}
    except Exception as e:
        print(f"[호가 오류] {code}: {e}")
    return None
