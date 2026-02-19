"""전종목 스캔 모듈 (KRX 데이터) / Full Market Scanner

3단계 폴백:
1차: KRX OTP + CSV 다운로드 (다른 엔드포인트)
2차: 네이버 금융 API (해외 IP에서도 안정적)
3차: KRX JSON API (기존 방식 — Railway에서 HTTP 400)
"""
import requests
import csv
import io
import re
from datetime import datetime, date, timedelta
from app.core.database import db
from app.core.config import KST


async def scan_all_stocks():
    """KRX에서 전종목 데이터 가져오기 / Fetch all stocks from KRX"""
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

    # 1차: KRX OTP + CSV
    print(f"[스캐너] 1차 시도: KRX OTP+CSV ({market})")
    try:
        stocks = _fetch_krx_otp_csv(market)
        if stocks:
            print(f"[스캐너] ✅ KRX OTP+CSV 성공: {len(stocks)}개 ({market})")
            return stocks
    except Exception as e:
        print(f"[스캐너] 1차 실패: {e}")

    # 2차: 네이버 금융
    print(f"[스캐너] 2차 시도: 네이버 금융 ({market})")
    try:
        stocks = _fetch_naver_stocks(market)
        if stocks:
            print(f"[스캐너] ✅ 네이버 금융 성공: {len(stocks)}개 ({market})")
            return stocks
    except Exception as e:
        print(f"[스캐너] 2차 실패: {e}")

    # 3차: KRX JSON (기존 — Railway에서 400 가능)
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
# 방법 1: KRX OTP + CSV 다운로드
# ═══════════════════════════════════════════════════
def _fetch_krx_otp_csv(market="STK"):
    """KRX OTP를 발급받고 CSV로 전종목 다운로드"""
    trading_date = _get_last_trading_date()
    trd_dd = trading_date.strftime("%Y%m%d")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    }

    # Step 1: OTP 발급
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
        print(f"[KRX-OTP] OTP 발급 실패: HTTP {otp_resp.status_code}, 응답: {otp_resp.text[:200]}")
        return []

    otp = otp_resp.text.strip()
    if not otp or len(otp) < 10:
        print(f"[KRX-OTP] OTP 값 비정상: '{otp[:100]}'")
        return []

    print(f"[KRX-OTP] OTP 발급 성공 (길이: {len(otp)})")

    # Step 2: CSV 다운로드
    download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    csv_resp = requests.post(
        download_url,
        data={"code": otp},
        headers=headers,
        timeout=30,
    )

    if csv_resp.status_code != 200:
        print(f"[KRX-OTP] CSV 다운로드 실패: HTTP {csv_resp.status_code}")
        return []

    # CSV 파싱 (EUC-KR → UTF-8)
    try:
        content = csv_resp.content.decode("euc-kr", errors="replace")
    except Exception:
        content = csv_resp.content.decode("utf-8", errors="replace")

    print(f"[KRX-OTP] CSV 수신: {len(content)} bytes")

    reader = csv.DictReader(io.StringIO(content))
    fieldnames = reader.fieldnames or []
    print(f"[KRX-OTP] CSV 컬럼: {fieldnames}")

    stocks = []
    for row in reader:
        try:
            code = row.get("종목코드", "").strip()
            name = row.get("종목명", "").strip()
            price = _parse_int(row.get("종가", "0"))
            volume = _parse_int(row.get("거래량", "0"))
            change_pct = _parse_float(row.get("등락률", "0"))
            mktcap = _parse_int(row.get("시가총액", "0"))

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
                "change_pct": change_pct,
                "volume": volume,
                "market_cap": mktcap,
            })
        except Exception:
            continue

    return stocks


# ═══════════════════════════════════════════════════
# 방법 2: 네이버 금융 API
# ═══════════════════════════════════════════════════
def _fetch_naver_stocks(market="STK"):
    """네이버 금융 시가총액 페이지에서 전종목 크롤링"""
    sosok = "0" if market == "STK" else "1"
    all_stocks = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    for page in range(1, 50):
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"

        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"[네이버] HTTP {resp.status_code} (page {page})")
            break

        resp.encoding = "euc-kr"
        html = resp.text

        stocks = _parse_naver_page(html, market)
        if not stocks:
            break

        all_stocks.extend(stocks)

        if page % 10 == 0:
            print(f"[네이버] {market} 페이지 {page} — 누적 {len(all_stocks)}개")

    return all_stocks


def _parse_naver_page(html, market="STK"):
    """네이버 금융 시가총액 HTML에서 종목 추출"""
    stocks = []

    # 테이블 찾기
    tbody_match = re.search(r'<table[^>]*class="type_2"[^>]*>(.*?)</table>', html, re.DOTALL)
    if not tbody_match:
        return []

    table_html = tbody_match.group(1)

    # 각 행(tr) 분리
    row_blocks = re.split(r'<tr[^>]*>', table_html)

    for block in row_blocks:
        # 종목 코드+이름 추출
        code_match = re.search(
            r'href="/item/main\.naver\?code=(\d{6})"[^>]*>\s*([^<]+?)\s*</a>',
            block
        )
        if not code_match:
            continue

        code = code_match.group(1)
        name = code_match.group(2).strip()

        # 숫자 데이터 (td class="number")
        numbers = re.findall(r'<td\s+class="number">\s*([^<]*?)\s*</td>', block)

        if len(numbers) < 9:
            continue

        try:
            # 0:현재가, 1:전일비, 2:등락률, 3:액면가, 4:시가총액, 5:상장주식수, 6:외국인비율, 7:거래량, 8:PER
            price = _parse_int(numbers[0])
            change_pct = _parse_float(numbers[2].replace("%", ""))
            mktcap = _parse_int(numbers[4])
            volume = _parse_int(numbers[7])

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
                "change_pct": change_pct,
                "volume": volume,
                "market_cap": mktcap,
            })
        except Exception:
            continue

    return stocks


# ═══════════════════════════════════════════════════
# 방법 3: KRX JSON API (기존)
# ═══════════════════════════════════════════════════
def _fetch_krx_json(market="STK"):
    """KRX JSON API — Railway에서 HTTP 400 가능"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
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
                print(f"[KRX-JSON] JSON 파싱 실패: {r.text[:200]}")
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
def _parse_int(val):
    """쉼표 포함 문자열 → int"""
    if not val:
        return 0
    return int(str(val).replace(",", "").replace("+", "").replace(" ", "").strip() or "0")


def _parse_float(val):
    """쉼표 포함 문자열 → float"""
    if not val:
        return 0.0
    return float(str(val).replace(",", "").replace("+", "").replace("%", "").replace(" ", "").strip() or "0")


def _is_weekday(d):
    """주말이 아닌지 확인"""
    return d.weekday() < 5


def _get_last_trading_date():
    """마지막 거래일 찾기 / Find last trading date"""
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
    """다음 거래일 찾기 / Find next trading date (for night scan)"""
    from app.utils.kr_holiday import is_market_open_day

    check_date = datetime.now(KST).date() + timedelta(days=1)
    for _ in range(10):
        if is_market_open_day(check_date):
            return check_date
        check_date += timedelta(days=1)
    return check_date


async def refine_watchlist():
    """장전 최종 감시종목 확정 / Pre-market final watchlist confirmation"""
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
            print("[스캐너] 최근 감시 후보가 없습니다. 야간스캔 결과를 확인하세요.")
            return

        try:
            db.table("watchlist").update({"status": "대기"}).eq("status", "감시중").execute()
        except Exception:
            pass

        confirmed = min(10, len(candidates))
        for item in candidates[:confirmed]:
            db.table("watchlist").update({"status": "감시중"}).eq("id", item["id"]).execute()

        print(f"[스캐너] 최종 감시종목 {confirmed}개 확정 (최근 {recent_date} 이후 데이터)")
    except Exception as e:
        print(f"[감시종목 확정 오류] {e}")
