"""전종목 스캔 모듈 (KRX 데이터)"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
from app.core.database import db

async def scan_all_stocks():
    """KRX에서 전종목 데이터 가져오기"""
    print(f"[스캐너] 전종목 스캔 시작")
    stocks = []
    try:
        # KOSPI
        kospi = _fetch_krx_stocks("STK")
        stocks.extend(kospi)
        # KOSDAQ
        kosdaq = _fetch_krx_stocks("KSQ")
        stocks.extend(kosdaq)
        print(f"[스캐너] 총 {len(stocks)}개 종목 수집")
    except Exception as e:
        print(f"[스캐너 오류] {e}")
    return stocks

def _fetch_krx_stocks(market="STK"):
    """KRX에서 종목 데이터 크롤링"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded"}
    # 마지막 거래일 찾기 (오늘 장 전이면 이전 거래일 사용)
    from app.utils.kr_holiday import is_market_open_day
    from datetime import timedelta
    check_date = datetime.now().date()
    for _ in range(10):
        if is_market_open_day(check_date):
            now = datetime.now()
            if check_date == now.date() and now.hour < 16:
                check_date -= timedelta(days=1)
                continue
            break
        check_date -= timedelta(days=1)
    today = check_date.strftime("%Y%m%d")
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": market,
        "trdDd": today,
        "share": "1", "money": "1",
        "csvxls_isNo": "false",
    }
    try:
        r = requests.post(url, headers=headers, data=data, timeout=30)
        items = r.json().get("OutBlock_1", [])
        stocks = []
        for item in items:
            try:
                stocks.append({
                    "code": item.get("ISU_SRT_CD", ""),
                    "name": item.get("ISU_ABBRV", ""),
                    "market": "kospi" if market == "STK" else "kosdaq",
                    "price": int(item.get("TDD_CLSPRC", "0").replace(",", "")),
                    "change_pct": float(item.get("FLUC_RT", "0").replace(",", "")),
                    "volume": int(item.get("ACC_TRDVOL", "0").replace(",", "")),
                    "market_cap": int(item.get("MKTCAP", "0").replace(",", "")),
                })
            except:
                continue
        return stocks
    except Exception as e:
        print(f"[KRX 크롤링 오류] {market}: {e}")
        return []

async def refine_watchlist():
    """장전 최종 감시종목 확정"""
    from app.engine.scorer import score_and_select
    try:
        # 전날 선별된 후보 가져오기
        result = db.table("watchlist").select("*").order("score", desc=True).limit(30).execute()
        candidates = result.data if result.data else []
        if candidates:
            # 상위 10개만 최종 확정
            for item in candidates[:10]:
                db.table("watchlist").update({"status": "감시중"}).eq("id", item["id"]).execute()
            print(f"[스캐너] 최종 감시종목 {min(10, len(candidates))}개 확정")
    except Exception as e:
        print(f"[감시종목 확정 오류] {e}")
