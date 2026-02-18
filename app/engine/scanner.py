"""전종목 스캔 모듈 (KRX 데이터) / Full Market Scanner"""
import requests
from datetime import datetime, date, timedelta
from app.core.database import db


async def scan_all_stocks():
    """KRX에서 전종목 데이터 가져오기 / Fetch all stocks from KRX"""
    print(f"[스캐너] 전종목 스캔 시작")
    stocks = []
    try:
        kospi = _fetch_krx_stocks("STK")
        stocks.extend(kospi)
        kosdaq = _fetch_krx_stocks("KSQ")
        stocks.extend(kosdaq)
        print(f"[스캐너] 총 {len(stocks)}개 종목 수집 (코스피 {len(kospi)}, 코스닥 {len(kosdaq)})")
    except Exception as e:
        print(f"[스캐너 오류] {e}")
    return stocks


def _get_last_trading_date():
    """마지막 거래일 찾기 / Find last trading date with KRX data"""
    from app.utils.kr_holiday import is_market_open_day

    now = datetime.now()
    check_date = now.date()

    # 오늘이 거래일이고 16시 이후면 → 오늘 데이터 사용
    if is_market_open_day(check_date) and now.hour >= 16:
        return check_date

    # 그 외: 이전 거래일 찾기 (오늘 포함하지 않음)
    check_date -= timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date -= timedelta(days=1)

    # fallback: 못 찾으면 오늘
    return now.date()


def _get_next_trading_date():
    """다음 거래일 찾기 / Find next trading date (for night scan)"""
    from app.utils.kr_holiday import is_market_open_day

    check_date = datetime.now().date() + timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date += timedelta(days=1)
    return check_date


def _fetch_krx_stocks(market="STK"):
    """KRX에서 종목 데이터 크롤링 / Crawl stock data from KRX"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    trading_date = _get_last_trading_date()
    today = trading_date.strftime("%Y%m%d")
    print(f"[스캐너] KRX 요청 날짜: {today} ({market})")

    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": market,
        "trdDd": today,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    try:
        r = requests.post(url, headers=headers, data=data, timeout=30)
        items = r.json().get("OutBlock_1", [])
        stocks = []
        for item in items:
            try:
                code = item.get("ISU_SRT_CD", "")
                name = item.get("ISU_ABBRV", "")
                price = int(item.get("TDD_CLSPRC", "0").replace(",", ""))
                volume = int(item.get("ACC_TRDVOL", "0").replace(",", ""))

                # 기본 필터: 가격 0원, 거래량 0 제외
                if price <= 0 or volume <= 0:
                    continue
                # ETF, ETN, 리츠 등 제외 (코드가 숫자 6자리가 아닌 것)
                if not code.isdigit() or len(code) != 6:
                    continue
                # 관리종목/정리매매 제외 (이름에 특수문자 포함)
                if any(x in name for x in ["스팩", "SPAC"]):
                    continue

                stocks.append({
                    "code": code,
                    "name": name,
                    "market": "kospi" if market == "STK" else "kosdaq",
                    "price": price,
                    "change_pct": float(item.get("FLUC_RT", "0").replace(",", "")),
                    "volume": volume,
                    "market_cap": int(item.get("MKTCAP", "0").replace(",", "")),
                })
            except:
                continue
        print(f"[스캐너] {market} 필터 후 {len(stocks)}개 종목")
        return stocks
    except Exception as e:
        print(f"[KRX 크롤링 오류] {market}: {e}")
        return []


async def refine_watchlist():
    """장전 최종 감시종목 확정 / Pre-market final watchlist confirmation"""
    try:
        # 최근 3일 이내 스캔 결과만 조회 (오래된 데이터 제외)
        recent_date = (date.today() - timedelta(days=3)).isoformat()
        result = (
            db.table("watchlist")
            .select("*")
            .gte("scan_date", recent_date)
            .order("score", desc=True)
            .limit(30)
            .execute()
        )
        candidates = result.data if result.data else []

        if not candidates:
            print("[스캐너] 최근 감시 후보가 없습니다. 야간스캔 결과를 확인하세요.")
            return

        # 기존 "감시중" 상태 초기화
        try:
            db.table("watchlist").update({"status": "대기"}).eq("status", "감시중").execute()
        except:
            pass

        # 상위 10개만 최종 확정
        confirmed = min(10, len(candidates))
        for item in candidates[:confirmed]:
            db.table("watchlist").update({"status": "감시중"}).eq("id", item["id"]).execute()

        print(f"[스캐너] 최종 감시종목 {confirmed}개 확정 (최근 {recent_date} 이후 데이터)")
    except Exception as e:
        print(f"[감시종목 확정 오류] {e}")
