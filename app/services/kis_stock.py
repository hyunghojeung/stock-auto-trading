"""KIS 시세 조회 — 한국투자증권 Open API 기반
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v2: API 문서 기반 TR_ID 정확히 반영
  - 주식현재가 시세 (FHKST01010100) — 현재가 + 상세 시세
  - 주식현재가 일자별 (FHKST01010400) — 일봉 데이터
  - 주식현재가 호가/예상체결 (FHKST01010200) — 호가창 10단계
  - 국내주식기간별시세 (FHKST03010100) — 일/주/월/년 봉
  - 주식일별분봉조회 (FHKST03010230) — 분봉 (실전계좌 전용)
"""
import requests
import logging
from datetime import datetime, timedelta
from app.services.kis_auth import get_kis
from app.core.config import KST

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 주식현재가 시세 (FHKST01010100)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_current_price(code, is_live=False):
    """현재가 + 상세 시세 조회
    TR_ID: FHKST01010100 (실전/모의 동일)
    URL: /uapi/domestic-stock/v1/quotations/inquire-price
    """
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010100"
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d["output"]
            return {
                "code": code,
                "name": o.get("hts_kor_isnm", ""),           # HTS 한글 종목명
                "price": int(o.get("stck_prpr", 0)),          # 주식 현재가
                "change": int(o.get("prdy_vrss", 0)),         # 전일 대비
                "change_sign": o.get("prdy_vrss_sign", ""),   # 전일 대비 부호 (1:상한,2:상승,3:보합,4:하한,5:하락)
                "change_pct": float(o.get("prdy_ctrt", 0)),   # 전일 대비율
                "volume": int(o.get("acml_vol", 0)),          # 누적 거래량
                "trade_amount": int(o.get("acml_tr_pbmn", 0)),  # 누적 거래 대금
                "high": int(o.get("stck_hgpr", 0)),           # 주식 최고가
                "low": int(o.get("stck_lwpr", 0)),            # 주식 최저가
                "open": int(o.get("stck_oprc", 0)),           # 주식 시가2
                "upper_limit": int(o.get("stck_mxpr", 0)),    # 상한가
                "lower_limit": int(o.get("stck_llam", 0)),    # 하한가
                "prev_close": int(o.get("stck_sdpr", 0)),     # 주식 기준가
                "per": float(o.get("per", 0)),                # PER
                "pbr": float(o.get("pbr", 0)),                # PBR
                "eps": float(o.get("eps", 0)),                # EPS
                "market_cap": int(o.get("hts_avls", 0)),      # HTS 시가총액
                "volume_ratio": float(o.get("vol_tnrt", 0)),  # 거래량 회전율
                "w52_high": int(o.get("w52_hgpr", 0)),        # 52주 최고가
                "w52_low": int(o.get("w52_lwpr", 0)),         # 52주 최저가
                "listed_shares": int(o.get("lstn_stcn", 0)),  # 상장 주수
            }
    except Exception as e:
        logger.debug(f"[현재가 오류] {code}: {e}")
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 주식현재가 일자별 (FHKST01010400)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_daily_candles(code, period=30, is_live=False):
    """일자별 주가 조회 (최근 N일)
    TR_ID: FHKST01010400 (실전/모의 동일)
    URL: /uapi/domestic-stock/v1/quotations/inquire-daily-price
    """
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010400"
    end = datetime.now(KST).strftime("%Y%m%d")
    start = (datetime.now(KST) - timedelta(days=period * 2)).strftime("%Y%m%d")
    p = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": start,
        "FID_INPUT_DATE_2": end,
        "FID_PERIOD_DIV_CODE": "D",  # D:일, W:주, M:월, Y:년
        "FID_ORG_ADJ_PRC": "0",      # 0:수정주가 반영
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            return [
                {
                    "date": i["stck_bsop_date"],
                    "open": int(i.get("stck_oprc", 0)),
                    "high": int(i.get("stck_hgpr", 0)),
                    "low": int(i.get("stck_lwpr", 0)),
                    "close": int(i.get("stck_clpr", 0)),
                    "volume": int(i.get("acml_vol", 0)),
                    "change": int(i.get("prdy_vrss", 0)),
                    "change_pct": float(i.get("prdy_ctrt", 0)) if i.get("prdy_ctrt") else 0,
                }
                for i in d.get("output", [])[:period]
                if i.get("stck_bsop_date")
            ]
    except Exception as e:
        logger.debug(f"[일봉 오류] {code}: {e}")
    return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 국내주식기간별시세 (FHKST03010100) — 일/주/월/년
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_period_candles(code, start_date, end_date, period_code="D", is_live=False):
    """기간별 시세 조회 (일/주/월/년)
    TR_ID: FHKST03010100 (실전/모의 동일)
    URL: /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice

    period_code: D=일봉, W=주봉, M=월봉, Y=년봉
    start_date, end_date: 'YYYYMMDD' 형식
    최대 100건/회
    """
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST03010100"
    p = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": period_code,
        "FID_ORG_ADJ_PRC": "0",
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            # output1 = 기본 시세, output2 = 기간별 캔들 배열
            meta = d.get("output1", {})
            candles = []
            for i in d.get("output2", []):
                if not i.get("stck_bsop_date"):
                    continue
                candles.append({
                    "date": i["stck_bsop_date"],
                    "open": int(i.get("stck_oprc", 0)),
                    "high": int(i.get("stck_hgpr", 0)),
                    "low": int(i.get("stck_lwpr", 0)),
                    "close": int(i.get("stck_clpr", 0)),
                    "volume": int(i.get("acml_vol", 0)),
                    "trade_amount": int(i.get("acml_tr_pbmn", 0)),
                    "change": int(i.get("prdy_vrss", 0)),
                    "change_pct": float(i.get("prdy_ctrt", 0)) if i.get("prdy_ctrt") else 0,
                    "change_sign": i.get("prdy_vrss_sign", ""),
                    "flng_cls_code": i.get("flng_cls_code", ""),  # 락 구분 (01:권리락 등)
                })
            return {
                "meta": {
                    "name": meta.get("hts_kor_isnm", ""),
                    "price": int(meta.get("stck_prpr", 0)),
                    "change_pct": float(meta.get("prdy_ctrt", 0)) if meta.get("prdy_ctrt") else 0,
                    "per": float(meta.get("per", 0)) if meta.get("per") else 0,
                    "pbr": float(meta.get("pbr", 0)) if meta.get("pbr") else 0,
                    "eps": float(meta.get("eps", 0)) if meta.get("eps") else 0,
                },
                "candles": candles,
            }
    except Exception as e:
        logger.debug(f"[기간별시세 오류] {code}: {e}")
    return {"meta": {}, "candles": []}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 주식현재가 호가/예상체결 (FHKST01010200)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_orderbook(code, is_live=False):
    """호가창 10단계 조회
    TR_ID: FHKST01010200 (실전/모의 동일)
    URL: /uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn
    """
    auth = get_kis(is_live)
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010200"
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            o1 = d.get("output1", {})
            o2 = d.get("output2", {})

            # 매도호가 (askp1~10), 매도잔량 (askp_rsqn1~10)
            asks = []
            for i in range(1, 11):
                price = int(o1.get(f"askp{i}", 0))
                qty = int(o1.get(f"askp_rsqn{i}", 0))
                if price > 0:
                    asks.append({"price": price, "quantity": qty})

            # 매수호가 (bidp1~10), 매수잔량 (bidp_rsqn1~10)
            bids = []
            for i in range(1, 11):
                price = int(o1.get(f"bidp{i}", 0))
                qty = int(o1.get(f"bidp_rsqn{i}", 0))
                if price > 0:
                    bids.append({"price": price, "quantity": qty})

            total_ask = int(o1.get("total_askp_rsqn", 0))
            total_bid = int(o1.get("total_bidp_rsqn", 0))

            return {
                "asks": asks,                              # 매도호가 목록
                "bids": bids,                              # 매수호가 목록
                "total_ask": total_ask,                    # 총 매도잔량
                "total_bid": total_bid,                    # 총 매수잔량
                "total_ask_icdc": int(o1.get("ovtm_total_askp_rsqn", 0)),  # 시간외 매도잔량
                "total_bid_icdc": int(o1.get("ovtm_total_bidp_rsqn", 0)),  # 시간외 매수잔량
                "bid_ratio": round(total_bid / total_ask, 4) if total_ask > 0 else 0,
                # output2: 예상 체결 정보
                "expected_price": int(o2.get("antc_cnpr", 0)) if o2 else 0,     # 예상 체결가
                "expected_volume": int(o2.get("antc_cntg_vrss", 0)) if o2 else 0,
                "market_status": o2.get("new_mkop_cls_code", "") if o2 else "",  # 장운영구분
            }
    except Exception as e:
        logger.debug(f"[호가 오류] {code}: {e}")
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 주식일별분봉조회 (FHKST03010230) — ★ 실전계좌 전용
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_minute_candles(code, count=30, is_live=False):
    """분봉 데이터 조회
    TR_ID: FHKST03010230 (실전만 지원, 모의투자 미지원)
    URL: /uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice
    ★ 모의투자에서는 현재가 시세로 대체

    한 번의 호출에 최대 120건, FID_INPUT_HOUR_1 이용하여 과거 분봉 조회
    """
    auth = get_kis(is_live)

    # ★ 모의투자는 분봉 API 미지원 → 현재가로 대체
    if not is_live:
        return _get_minute_candles_fallback(code, count, auth)

    h = auth.get_headers()
    h["tr_id"] = "FHKST03010230"

    now = datetime.now(KST)
    today = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")

    p = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": today,
        "FID_INPUT_HOUR_1": time_str,
        "FID_PW_DATA_INCU_YN": "Y",    # 과거 데이터 포함 여부
        "FID_FAKE_TICK_INCU_YN": "N",   # 허봉 포함 여부
    }
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            output2 = d.get("output2", [])
            if isinstance(output2, dict):
                output2 = [output2]
            if not isinstance(output2, list):
                return []
            return [
                {
                    "date": i.get("stck_bsop_date", ""),
                    "time": i.get("stck_cntg_hour", ""),
                    "open": int(i.get("stck_oprc", 0)),
                    "high": int(i.get("stck_hgpr", 0)),
                    "low": int(i.get("stck_lwpr", 0)),
                    "close": int(i.get("stck_prpr", 0)),
                    "volume": int(i.get("cntg_vol", 0)),
                    "trade_amount": int(i.get("acml_tr_pbmn", 0)),
                }
                for i in output2[:count]
                if i.get("stck_cntg_hour")
            ]
    except Exception as e:
        logger.debug(f"[분봉 오류] {code}: {e}")
    return []


def _get_minute_candles_fallback(code, count, auth):
    """모의투자용 분봉 대체 — 현재가 시세 API 활용
    모의투자에서는 분봉 API(FHKST03010230)가 미지원이므로
    현재가 시세로 단일 데이터 포인트를 반환합니다.
    """
    h = auth.get_headers()
    h["tr_id"] = "FHKST01010100"
    p = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(
            f"{auth.base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=h, params=p, timeout=10
        )
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d["output"]
            now = datetime.now(KST)
            return [{
                "date": now.strftime("%Y%m%d"),
                "time": now.strftime("%H%M%S"),
                "open": int(o.get("stck_oprc", 0)),
                "high": int(o.get("stck_hgpr", 0)),
                "low": int(o.get("stck_lwpr", 0)),
                "close": int(o.get("stck_prpr", 0)),
                "volume": int(o.get("acml_vol", 0)),
                "trade_amount": int(o.get("acml_tr_pbmn", 0)),
            }]
    except Exception as e:
        logger.debug(f"[분봉 폴백 오류] {code}: {e}")
    return []
