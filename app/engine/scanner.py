"""전종목 스캔 모듈 (KRX 데이터) / Full Market Scanner

3단계 폴백:
1차: 네이버 모바일 JSON API (해외 IP OK, 순수 JSON)
2차: KRX OTP + CSV 다운로드
3차: KRX JSON API (Railway에서 HTTP 400 확인됨)
"""
import requests
import csv
import io
import re
from datetime import datetime, date, timedelta
from app.core.database import db
from app.core.config import KST


async def scan_all_stocks():
    """전종목 데이터 가져오기 / Fetch all stocks"""
    print(f"[스캐너] 전종목 스캔 시작")
    stocks = []
    try:
        kospi = _fetch_stocks_with_fallback("STK")
        stocks.extend(kospi)
        kosdaq = _fetch_stocks_with_fallback("KSQ")
        stocks.extend(kosdaq)
        print(f"[스캐너] 총 {len(stocks)}개 종목 수집 (코스피 {len(kospi)}, 코스닥 {len(kosdaq)})")
    except Exception as e:
        print(f"[스캐너 오류] {e}")
        import traceback
        traceback.print_exc()
    return stocks


def _fetch_stocks_with_fallback(market="STK"):
    """3단계 폴백으로 종목 데이터 가져오기"""

    # 1차: 네이버 모바일 JSON API
    print(f"[스캐너] 1차 시도: 네이버 JSON API ({market})")
    try:
        stocks = _fetch_naver_json_api(market)
        if stocks:
            print(f"[스캐너] ✅ 네이버 JSON API 성공: {len(stocks)}개 ({market})")
            return stocks
        print(f"[스캐너] 1차 결과 0개")
    except Exception as e:
        print(f"[스캐너] 1차 실패: {e}")
        import traceback
        traceback.print_exc()

    # 2차: KRX OTP + CSV
    print(f"[스캐너] 2차 시도: KRX OTP+CSV ({market})")
    try:
        stocks = _fetch_krx_otp_csv(market)
        if stocks:
            print(f"[스캐너] ✅ KRX OTP+CSV 성공: {len(stocks)}개 ({market})")
            return stocks
        print(f"[스캐너] 2차 결과 0개")
    except Exception as e:
        print(f"[스캐너] 2차 실패: {e}")

    # 3차: KRX JSON (Railway에서 400)
    print(f"[스캐너] 3차 시도: KRX JSON ({market})")
    try:
        stocks = _fetch_krx_json(market)
        if stocks:
            print(f"[스캐너] ✅ KRX JSON 성공: {len(stocks)}개 ({market})")
            return stocks
    except Exception as e:
        print(f"[스캐너] 3차 실패: {e}")

    print(f"[스캐너] ❌ 모든 방식 실패 ({market})")
    return []


# ═══════════════════════════════════════════════════
# 방법 1: 네이버 모바일 JSON API (★ 메인)
# ═══════════════════════════════════════════════════
def _fetch_naver_json_api(market="STK"):
    """네이버 모바일 증권 JSON API
    
    엔드포인트:
    - KOSPI:  https://m.stock.naver.com/api/stocks/marketValue/KOSPI?page=1&pageSize=100
    - KOSDAQ: https://m.stock.naver.com/api/stocks/marketValue/KOSDAQ?page=1&pageSize=100
    
    해외 IP에서도 작동하며 순수 JSON 반환 (HTML 파싱 불필요)
    """
    market_name = "KOSPI" if market == "STK" else "KOSDAQ"
    all_stocks = []
    page = 1
    page_size = 100

    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        "Accept": "application/json",
        "Referer": f"https://m.stock.naver.com/domestic/stock/list/{market_name}",
    }

    while True:
        url = f"https://m.stock.naver.com/api/stocks/marketValue/{market_name}?page={page}&pageSize={page_size}"

        try:
            resp = requests.get(url, headers=headers, timeout=15)

            if page == 1:
                print(f"[네이버API] {market_name} HTTP {resp.status_code}, 크기: {len(resp.text)} bytes")
                # 첫 응답 구조 로그
                print(f"[네이버API] 응답 첫 500자: {resp.text[:500]}")

            if resp.status_code != 200:
                print(f"[네이버API] HTTP {resp.status_code} (page {page})")
                break

            data = resp.json()

            # 응답 구조 파악 (첫 페이지만)
            if page == 1:
                if isinstance(data, dict):
                    print(f"[네이버API] JSON 키: {list(data.keys())}")
                elif isinstance(data, list):
                    print(f"[네이버API] JSON 배열, 길이: {len(data)}")
                    if len(data) > 0:
                        print(f"[네이버API] 첫 항목 키: {list(data[0].keys()) if isinstance(data[0], dict) else type(data[0])}")

            # 종목 리스트 추출 (다양한 응답 구조 대응)
            stocks_list = _extract_stocks_list(data)

            if not stocks_list:
                if page == 1:
                    print(f"[네이버API] 종목 리스트 추출 실패")
                break

            if len(stocks_list) == 0:
                break

            for item in stocks_list:
                stock = _parse_naver_item(item, market)
                if stock:
                    all_stocks.append(stock)

            # 마지막 페이지
            if len(stocks_list) < page_size:
                break

            page += 1
            if page > 30:  # 안전장치 (최대 3000종목)
                break

        except Exception as e:
            print(f"[네이버API] 오류 (page {page}): {e}")
            if page == 1:
                import traceback
                traceback.print_exc()
            break

    return all_stocks


def _extract_stocks_list(data):
    """다양한 네이버 API 응답 구조에서 종목 리스트 추출"""
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        # 가능한 키들 시도
        for key in ["stocks", "datas", "result", "items", "data", "list", "stockList", "results"]:
            if key in data and isinstance(data[key], list):
                return data[key]

        # 중첩 구조: data.stocks, result.stocks 등
        for outer_key in ["data", "result", "body"]:
            if outer_key in data and isinstance(data[outer_key], dict):
                inner = data[outer_key]
                for inner_key in ["stocks", "list", "items", "datas"]:
                    if inner_key in inner and isinstance(inner[inner_key], list):
                        return inner[inner_key]

    return None


def _parse_naver_item(item, market="STK"):
    """네이버 API 종목 아이템 → dict 변환"""
    if not isinstance(item, dict):
        return None

    try:
        # ETF, ETN 등 제외 — 보통주만
        end_type = item.get("stockEndType", "stock")
        if end_type in ("etf", "etn", "elw"):
            return None

        # 종목코드 (다양한 필드명)
        code = str(
            item.get("itemCode") or item.get("stockCode") or
            item.get("code") or item.get("cd") or
            item.get("symbolCode") or item.get("reutersCode", "")
        ).strip()

        # 6자리 숫자 코드만 허용 (0126Z0 같은 비숫자 코드 제외)
        # reutersCode 형식: "005930.KS" → "005930"
        if "." in code:
            code = code.split(".")[0]
        if len(code) != 6 or not code.isdigit():
            return None

        # 종목명
        name = str(
            item.get("stockName") or item.get("itemName") or
            item.get("name") or item.get("nm") or
            item.get("stockNameKor") or ""
        ).strip()

        if not name:
            return None

        # 스팩 제외
        if any(x in name for x in ["스팩", "SPAC"]):
            return None

        # 현재가
        price = _safe_int(
            item.get("closePrice") or item.get("nowVal") or
            item.get("price") or item.get("currentPrice") or
            item.get("nv") or item.get("close") or 0
        )
        if price <= 0:
            return None

        # 등락률
        change_pct = _safe_float(
            item.get("fluctuationsRatio") or item.get("changeRate") or
            item.get("cr") or item.get("changePct") or
            item.get("fluctuation") or 0
        )

        # 거래량
        volume = _safe_int(
            item.get("accumulatedTradingVolume") or item.get("dealCnt") or
            item.get("volume") or item.get("tv") or
            item.get("tradeVolume") or 0
        )
        if volume <= 0:
            return None

        # 시가총액
        mktcap = _safe_int(
            item.get("marketValue") or item.get("marketCap") or
            item.get("mv") or item.get("mktCap") or 0
        )

        return {
            "code": code,
            "name": name,
            "market": "kospi" if market == "STK" else "kosdaq",
            "price": price,
            "change_pct": change_pct,
            "volume": volume,
            "market_cap": mktcap,
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════
# 방법 2: KRX OTP + CSV 다운로드
# ═══════════════════════════════════════════════════
def _fetch_krx_otp_csv(market="STK"):
    """KRX OTP를 발급받고 CSV로 전종목 다운로드"""
    trading_date = _get_last_trading_date()
    trd_dd = trading_date.strftime("%Y%m%d")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    }

    # OTP 발급
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_params = {
        "locale": "ko_KR",
        "mktId": market,
        "trdDd": trd_dd,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01501",
    }

    print(f"[KRX-OTP] OTP 요청: 날짜={trd_dd}, 시장={market}")
    otp_resp = requests.get(otp_url, params=otp_params, headers=headers, timeout=15)

    if otp_resp.status_code != 200:
        print(f"[KRX-OTP] OTP 발급 실패: HTTP {otp_resp.status_code}")
        return []

    otp = otp_resp.text.strip()
    if not otp or len(otp) < 10 or "LOGOUT" in otp.upper():
        print(f"[KRX-OTP] OTP 값 비정상: '{otp[:100]}'")
        return []

    print(f"[KRX-OTP] OTP 발급 성공 (길이: {len(otp)})")

    # CSV 다운로드
    download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    csv_resp = requests.post(download_url, data={"code": otp}, headers=headers, timeout=30)

    if csv_resp.status_code != 200:
        print(f"[KRX-OTP] CSV 다운로드 실패: HTTP {csv_resp.status_code}")
        return []

    try:
        content = csv_resp.content.decode("euc-kr", errors="replace")
    except Exception:
        content = csv_resp.content.decode("utf-8", errors="replace")

    print(f"[KRX-OTP] CSV 수신: {len(content)} bytes")

    reader = csv.DictReader(io.StringIO(content))
    print(f"[KRX-OTP] CSV 컬럼: {reader.fieldnames}")

    stocks = []
    for row in reader:
        try:
            code = row.get("종목코드", "").strip()
            name = row.get("종목명", "").strip()
            price = _parse_int(row.get("종가", "0"))
            volume = _parse_int(row.get("거래량", "0"))

            if price <= 0 or volume <= 0:
                continue
            if not code.isdigit() or len(code) != 6:
                continue
            if any(x in name for x in ["스팩", "SPAC"]):
                continue

            stocks.append({
                "code": code,
                "name": name,
                "market": "kospi" if market == "STK" else "kosdaq",
                "price": price,
                "change_pct": _parse_float(row.get("등락률", "0")),
                "volume": volume,
                "market_cap": _parse_int(row.get("시가총액", "0")),
            })
        except Exception:
            continue

    return stocks


# ═══════════════════════════════════════════════════
# 방법 3: KRX JSON API (기존 — Railway에서 400)
# ═══════════════════════════════════════════════════
def _fetch_krx_json(market="STK"):
    """KRX JSON API — Railway IP에서 HTTP 400 확인됨"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Origin": "http://data.krx.co.kr",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    }

    trading_date = _get_last_trading_date()
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": market,
        "trdDd": trading_date.strftime("%Y%m%d"),
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }

    for attempt in range(3):
        try:
            r = requests.post(url, headers=headers, data=data, timeout=30)
            print(f"[KRX-JSON] HTTP {r.status_code}, 크기: {len(r.text)} bytes")

            if r.status_code != 200:
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                continue

            try:
                json_data = r.json()
            except Exception:
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                continue

            items = json_data.get("OutBlock_1", [])
            if not items:
                trading_date -= timedelta(days=1)
                while not _is_weekday(trading_date):
                    trading_date -= timedelta(days=1)
                data["trdDd"] = trading_date.strftime("%Y%m%d")
                continue

            stocks = []
            for item in items:
                try:
                    code = item.get("ISU_SRT_CD", "")
                    name = item.get("ISU_ABBRV", "")
                    price = _parse_int(item.get("TDD_CLSPRC", "0"))
                    volume = _parse_int(item.get("ACC_TRDVOL", "0"))

                    if price <= 0 or volume <= 0:
                        continue
                    if not code.isdigit() or len(code) != 6:
                        continue
                    if any(x in name for x in ["스팩", "SPAC"]):
                        continue

                    stocks.append({
                        "code": code,
                        "name": name,
                        "market": "kospi" if market == "STK" else "kosdaq",
                        "price": price,
                        "change_pct": _parse_float(item.get("FLUC_RT", "0")),
                        "volume": volume,
                        "market_cap": _parse_int(item.get("MKTCAP", "0")),
                    })
                except Exception:
                    continue

            return stocks

        except requests.exceptions.Timeout:
            print(f"[KRX-JSON] 타임아웃 — 재시도 {attempt+1}/3")
        except Exception as e:
            print(f"[KRX-JSON] 오류: {e}")

    return []


# ═══════════════════════════════════════════════════
# 공통 유틸리티
# ═══════════════════════════════════════════════════
def _safe_int(val):
    """안전하게 int 변환 (문자열, 숫자 모두 대응)"""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).replace(",", "").replace("+", "").replace(" ", "").strip()
    try:
        return int(float(s)) if s else 0
    except (ValueError, TypeError):
        return 0


def _safe_float(val):
    """안전하게 float 변환"""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").replace("+", "").replace("%", "").replace(" ", "").strip()
    try:
        return float(s) if s else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_int(val):
    """쉼표 포함 문자열 → int"""
    return _safe_int(val)


def _parse_float(val):
    """쉼표 포함 문자열 → float"""
    return _safe_float(val)


def _is_weekday(d):
    """주말이 아닌지 확인"""
    return d.weekday() < 5


def _get_last_trading_date():
    """마지막 거래일 찾기"""
    from app.utils.kr_holiday import is_market_open_day

    now = datetime.now(KST)
    check_date = now.date()

    if is_market_open_day(check_date) and now.hour >= 16:
        return check_date

    check_date -= timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date -= timedelta(days=1)

    return now.date()


def _get_next_trading_date():
    """다음 거래일 찾기"""
    from app.utils.kr_holiday import is_market_open_day

    check_date = datetime.now(KST).date() + timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date += timedelta(days=1)
    return check_date


async def refine_watchlist():
    """장전 최종 감시종목 확정"""
    try:
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
            print("[스캐너] 최근 감시 후보가 없습니다.")
            return

        try:
            db.table("watchlist").update({"status": "대기"}).eq("status", "감시중").execute()
        except Exception:
            pass

        confirmed = min(10, len(candidates))
        for item in candidates[:confirmed]:
            db.table("watchlist").update({"status": "감시중"}).eq("id", item["id"]).execute()

        print(f"[스캐너] 최종 감시종목 {confirmed}개 확정")
    except Exception as e:
        print(f"[감시종목 확정 오류] {e}")
