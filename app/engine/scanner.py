"""전종목 스캔 모듈 (KRX 데이터) / Full Market Scanner"""
import requests
from datetime import datetime, date, timedelta
from app.core.database import db
from app.core.config import KST


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

    now = datetime.now(KST)
    check_date = now.date()

    # 오늘이 거래일이고 16시 이후면 → 오늘 데이터 사용
    if is_market_open_day(check_date) and now.hour >= 16:
        return check_date

    # 장중(9시~16시)이면 → 전일 데이터 사용 (당일 데이터는 장 마감 후 확정)
    # 장 전(~9시)이면 → 전일 데이터 사용
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

    check_date = datetime.now(KST).date() + timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date += timedelta(days=1)
    return check_date


def _fetch_krx_stocks(market="STK"):
    """KRX에서 종목 데이터 크롤링 / Crawl stock data from KRX
    
    ★ Replit 작동 코드(kiwoom-api.ts fetchKRXStocksByMarket)와 100% 동일하게 맞춤
    - Referer: menuId=MDC0201020101 (전종목 시세 페이지)
    - X-Requested-With 헤더 없음
    - locale 파라미터 없음
    """
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    # ★ Replit 작동 코드와 동일한 헤더 (순서까지 맞춤)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "http://data.krx.co.kr",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    }

    trading_date = _get_last_trading_date()
    trd_dd = trading_date.strftime("%Y%m%d")
    print(f"[스캐너] KRX 요청 날짜: {trd_dd} ({market})")

    # ★ Replit 작동 코드와 동일한 파라미터 (locale 없음)
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": market,
        "trdDd": trd_dd,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }

    # 최대 3번 재시도 (날짜를 하루씩 앞당기며)
    for attempt in range(3):
        try:
            print(f"[KRX] 시도 {attempt+1}/3 — URL: {url}, 날짜: {data['trdDd']}, 시장: {market}")
            
            r = requests.post(url, headers=headers, data=data, timeout=30)

            # ★ 디버그: 응답 상태 + 헤더 + 본문 앞부분
            print(f"[KRX] HTTP {r.status_code} — Content-Type: {r.headers.get('Content-Type', 'N/A')}")
            print(f"[KRX] 응답 크기: {len(r.text)} bytes, 처음 300자: {r.text[:300]}")

            # 응답 상태 확인
            if r.status_code != 200:
                print(f"[KRX] HTTP {r.status_code} — 재시도 {attempt+1}/3")
                # 날짜 하루 앞당기기
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                continue

            # JSON 파싱 시도
            try:
                json_data = r.json()
            except Exception as json_err:
                # JSON이 아닌 응답 (HTML 등) — 처음 200자 로그
                print(f"[KRX] JSON 파싱 실패 ({json_err}) — 응답 처음 200자: {r.text[:200]}")
                # 날짜 하루 앞당기고 재시도
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                print(f"[KRX] 날짜 변경하여 재시도: {data['trdDd']}")
                continue

            items = json_data.get("OutBlock_1", [])
            print(f"[KRX] JSON 키: {list(json_data.keys())}, OutBlock_1 항목 수: {len(items)}")
            
            if not items:
                print(f"[KRX] OutBlock_1 비어있음 (날짜: {data['trdDd']}) — 재시도 {attempt+1}/3")
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                continue

            # ★ 첫 번째 항목의 키 구조 로그 (디버그용)
            if items:
                print(f"[KRX] 첫 항목 키: {list(items[0].keys())}")

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
                    # 관리종목/정리매매 제외
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

            print(f"[스캐너] {market} 날짜 {data['trdDd']} — 필터 후 {len(stocks)}개 종목")
            return stocks

        except requests.exceptions.Timeout:
            print(f"[KRX] 타임아웃 — 재시도 {attempt+1}/3")
        except Exception as e:
            print(f"[KRX 크롤링 오류] {market}: {e}")
            import traceback
            traceback.print_exc()

    print(f"[KRX] {market} 3회 재시도 모두 실패")
    return []


def _is_weekday(d):
    """주말이 아닌지 확인 (간단 체크)"""
    return d.weekday() < 5


async def refine_watchlist():
    """장전 최종 감시종목 확정 / Pre-market final watchlist confirmation"""
    try:
        # 최근 3일 이내 스캔 결과만 조회 (오래된 데이터 제외)
        now = datetime.now(KST)
        recent_date = (now.date() - timedelta(days=3)).isoformat()
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
