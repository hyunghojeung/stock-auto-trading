"""
전종목 수집 서비스 / Stock List Fetcher Service
네이버 금융에서 전종목 데이터 수집 (KRX는 Railway IP 차단)

파일 위치: app/services/stock_fetcher.py
"""

import requests
import time
import traceback
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from app.core.database import db


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 네이버 금융 전종목 수집 / Fetch All Stocks from Naver Finance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_krx_all_stocks() -> List[Dict]:
    """
    네이버 금융에서 코스피 + 코스닥 전종목 데이터 수집
    (함수명은 기존 호환성을 위해 유지)
    """
    all_stocks = []

    # 코스피 수집 (sosok=0)
    print("[전종목 수집] 코스피(KOSPI) 종목 수집 시작...")
    kospi = _fetch_naver_market(0)
    print(f"[전종목 수집] 코스피 {len(kospi)}개 종목 수집 완료")
    all_stocks.extend(kospi)

    time.sleep(1)

    # 코스닥 수집 (sosok=1)
    print("[전종목 수집] 코스닥(KOSDAQ) 종목 수집 시작...")
    kosdaq = _fetch_naver_market(1)
    print(f"[전종목 수집] 코스닥 {len(kosdaq)}개 종목 수집 완료")
    all_stocks.extend(kosdaq)

    print(f"[전종목 수집] 총 {len(all_stocks)}개 종목 수집 완료")
    return all_stocks


def _fetch_naver_market(sosok: int) -> List[Dict]:
    """
    네이버 금융 시가총액 페이지에서 전종목 수집
    sosok: 0=코스피, 1=코스닥
    """
    market_name = "kospi" if sosok == 0 else "kosdaq"
    base_url = "https://finance.naver.com/sise/sise_market_sum.naver"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    all_stocks = []
    seen_codes = set()

    # 총 페이지 수 확인
    total_pages = _get_total_pages(base_url, sosok, headers)
    print(f"[전종목 수집] {market_name} 총 {total_pages}페이지")

    for page in range(1, total_pages + 1):
        try:
            params = {"sosok": sosok, "page": page}
            resp = requests.get(base_url, params=params, headers=headers, timeout=15)
            resp.encoding = "euc-kr"

            if resp.status_code != 200:
                print(f"[전종목 수집] {market_name} page {page} HTTP {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # 종목 테이블 파싱
            table = soup.select_one("table.type_2")
            if not table:
                continue

            rows = table.select("tr")

            for row in rows:
                try:
                    cols = row.select("td")
                    if len(cols) < 10:
                        continue

                    # 종목명 + 코드
                    name_tag = cols[1].select_one("a")
                    if not name_tag:
                        continue

                    name = name_tag.text.strip()
                    href = name_tag.get("href", "")

                    # 코드 추출: /item/main.naver?code=005930
                    code_match = re.search(r"code=(\d{6})", href)
                    if not code_match:
                        continue
                    code = code_match.group(1)

                    # 중복 체크
                    if code in seen_codes:
                        continue
                    seen_codes.add(code)

                    # 현재가
                    price = _parse_td_int(cols[2])
                    # 등락률
                    change_pct = _parse_change_pct(cols[3], cols[4])
                    # 시가총액 (억원 → 원)
                    market_cap = _parse_td_int(cols[6]) * 100000000
                    # 거래량
                    volume = _parse_td_int(cols[9])

                    # ETF / 우선주 판별
                    is_etf = _is_etf(code, name)
                    is_preferred = code[-1] != "0" and not is_etf

                    all_stocks.append({
                        "code": code,
                        "name": name,
                        "market": market_name,
                        "sector": "",
                        "market_cap": market_cap,
                        "price": price,
                        "volume": volume,
                        "change_pct": round(change_pct, 2),
                        "is_active": True,
                        "is_etf": is_etf,
                        "is_preferred": is_preferred,
                        "listed_shares": 0,
                    })

                except Exception:
                    continue

            if page % 10 == 0 or page == total_pages:
                print(f"[전종목 수집] {market_name} {page}/{total_pages}페이지 (누적 {len(all_stocks)}개)")

            # 네이버 부하 방지
            time.sleep(0.3)

        except Exception as e:
            print(f"[전종목 수집] {market_name} page {page} 오류: {e}")
            time.sleep(1)
            continue

    return all_stocks


def _get_total_pages(base_url: str, sosok: int, headers: dict) -> int:
    """네이버 금융 시가총액 페이지의 총 페이지 수 확인"""
    try:
        resp = requests.get(
            base_url,
            params={"sosok": sosok, "page": 1},
            headers=headers,
            timeout=15
        )
        resp.encoding = "euc-kr"
        soup = BeautifulSoup(resp.text, "lxml")

        # 맨끝 페이지 링크 찾기
        page_nav = soup.select("td.pgRR a")
        if page_nav:
            href = page_nav[0].get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                return int(match.group(1))

        # fallback
        page_links = soup.select("table.Nnavi td a")
        max_page = 1
        for link in page_links:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                max_page = max(max_page, int(match.group(1)))

        return max(max_page, 40)

    except Exception as e:
        print(f"[전종목 수집] 페이지 수 확인 실패: {e}")
        return 45


def _parse_td_int(td) -> int:
    """td 셀에서 정수 파싱"""
    try:
        text = td.text.strip().replace(",", "").replace("\n", "").replace("\t", "")
        num = re.sub(r"[^\d]", "", text)
        return int(num) if num else 0
    except Exception:
        return 0


def _parse_change_pct(change_td, pct_td) -> float:
    """등락률 파싱 (상승/하락 판별)"""
    try:
        pct_text = pct_td.text.strip().replace("%", "").replace(",", "")
        pct = float(re.sub(r"[^\d.\-]", "", pct_text)) if pct_text else 0.0

        # 하락 판별
        img = change_td.select_one("img")
        if img:
            alt = img.get("alt", "")
            if "하락" in alt or "down" in alt.lower():
                pct = -abs(pct)
            elif "상승" in alt or "up" in alt.lower():
                pct = abs(pct)
        else:
            change_text = change_td.text.strip()
            if "-" in change_text:
                pct = -abs(pct)

        return pct
    except Exception:
        return 0.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. DB 저장 / Save to Supabase
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def save_stocks_to_db(stocks: List[Dict]) -> Dict:
    """수집된 종목 데이터를 Supabase stock_list 테이블에 저장 (UPSERT)"""
    if not stocks:
        return {"inserted": 0, "updated": 0, "total": 0, "errors": 0}

    inserted = 0
    updated = 0
    errors = 0
    batch_size = 100

    print(f"[DB 저장] {len(stocks)}개 종목 저장 시작 (배치 크기: {batch_size})")

    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        try:
            result = db.table("stock_list").upsert(
                batch,
                on_conflict="code"
            ).execute()

            batch_count = len(result.data) if result.data else len(batch)
            inserted += batch_count

            pct = min(100, int((i + len(batch)) / len(stocks) * 100))
            print(f"[DB 저장] 진행: {pct}% ({i + len(batch)}/{len(stocks)})")

        except Exception as e:
            errors += len(batch)
            print(f"[DB 저장] 배치 {i // batch_size + 1} 오류: {e}")

    deactivated = await _deactivate_delisted(stocks)

    total = inserted + updated
    print(f"[DB 저장] 완료: 저장 {total}개, 오류 {errors}개, 비활성화 {deactivated}개")

    return {
        "inserted": inserted,
        "updated": updated,
        "total": total,
        "errors": errors,
        "deactivated": deactivated,
    }


async def _deactivate_delisted(current_stocks: List[Dict]) -> int:
    """현재 목록에 없는 기존 DB 종목을 비활성화"""
    try:
        current_codes = {s["code"] for s in current_stocks}

        existing = db.table("stock_list").select("code").eq(
            "is_active", True
        ).execute().data

        if not existing:
            return 0

        delisted_codes = [
            s["code"] for s in existing
            if s["code"] not in current_codes
        ]

        if not delisted_codes:
            return 0

        for i in range(0, len(delisted_codes), 50):
            batch = delisted_codes[i:i + 50]
            db.table("stock_list").update(
                {"is_active": False}
            ).in_("code", batch).execute()

        print(f"[DB 저장] {len(delisted_codes)}개 종목 비활성화 (상장폐지 추정)")
        return len(delisted_codes)

    except Exception as e:
        print(f"[DB 저장] 비활성화 처리 오류: {e}")
        return 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. 통합 실행 / Run Full Update
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def update_stock_list() -> Dict:
    """전체 프로세스: 네이버 수집 → DB 저장 → 상장폐지 처리"""
    print(f"\n{'=' * 50}")
    print(f"[전종목 업데이트] 시작: {datetime.now()}")
    print(f"{'=' * 50}")

    start = time.time()

    stocks = fetch_krx_all_stocks()
    if not stocks:
        return {
            "success": False,
            "error": "네이버 금융에서 종목 수집 실패",
            "elapsed": 0,
        }

    result = await save_stocks_to_db(stocks)

    elapsed = round(time.time() - start, 1)
    print(f"[전종목 업데이트] 완료: {elapsed}초 소요")
    print(f"{'=' * 50}\n")

    return {
        "success": True,
        "total_fetched": len(stocks),
        "kospi": len([s for s in stocks if s["market"] == "kospi"]),
        "kosdaq": len([s for s in stocks if s["market"] == "kosdaq"]),
        "db_result": result,
        "elapsed": elapsed,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. DB 검색 / Search from DB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def search_stocks_from_db(
    query: str,
    market: Optional[str] = None,
    limit: int = 20,
    active_only: bool = True,
    exclude_etf: bool = False,
    exclude_preferred: bool = False,
) -> List[Dict]:
    """stock_list 테이블에서 종목 검색"""
    try:
        q = query.strip()
        if not q:
            return []

        builder = db.table("stock_list").select(
            "code, name, market, sector, market_cap, price, volume, change_pct, is_etf, is_preferred"
        )

        if active_only:
            builder = builder.eq("is_active", True)
        if market:
            builder = builder.eq("market", market)
        if exclude_etf:
            builder = builder.eq("is_etf", False)
        if exclude_preferred:
            builder = builder.eq("is_preferred", False)

        if q.isdigit():
            builder = builder.like("code", f"{q}%")
        else:
            builder = builder.ilike("name", f"%{q}%")

        builder = builder.order("market_cap", desc=True).limit(limit)

        result = builder.execute()
        return result.data if result.data else []

    except Exception as e:
        print(f"[종목 검색] DB 검색 오류: {e}")
        return []


def get_stock_by_code(code: str) -> Optional[Dict]:
    """종목코드로 단일 종목 조회"""
    try:
        result = db.table("stock_list").select("*").eq("code", code).single().execute()
        return result.data if result.data else None
    except Exception:
        return None


def get_all_active_stocks(
    market: Optional[str] = None,
    exclude_etf: bool = True,
    exclude_preferred: bool = True,
    min_price: int = 0,
    min_volume: int = 0,
) -> List[Dict]:
    """전체 활성 종목 조회"""
    try:
        builder = db.table("stock_list").select(
            "code, name, market, sector, market_cap, price, volume, change_pct"
        ).eq("is_active", True)

        if market:
            builder = builder.eq("market", market)
        if exclude_etf:
            builder = builder.eq("is_etf", False)
        if exclude_preferred:
            builder = builder.eq("is_preferred", False)
        if min_price > 0:
            builder = builder.gte("price", min_price)
        if min_volume > 0:
            builder = builder.gte("volume", min_volume)

        all_data = []
        page_size = 1000
        offset = 0

        while True:
            result = builder.order("market_cap", desc=True).range(
                offset, offset + page_size - 1
            ).execute()

            if not result.data:
                break
            all_data.extend(result.data)
            if len(result.data) < page_size:
                break
            offset += page_size

        return all_data

    except Exception as e:
        print(f"[종목 조회] 전체 조회 오류: {e}")
        return []


def get_stock_stats() -> Dict:
    """stock_list 테이블 통계"""
    try:
        total = db.table("stock_list").select("code", count="exact").execute()
        active = db.table("stock_list").select("code", count="exact").eq("is_active", True).execute()
        kospi = db.table("stock_list").select("code", count="exact").eq("market", "kospi").eq("is_active", True).execute()
        kosdaq = db.table("stock_list").select("code", count="exact").eq("market", "kosdaq").eq("is_active", True).execute()
        etf = db.table("stock_list").select("code", count="exact").eq("is_etf", True).eq("is_active", True).execute()

        return {
            "total": total.count or 0,
            "active": active.count or 0,
            "kospi": kospi.count or 0,
            "kosdaq": kosdaq.count or 0,
            "etf": etf.count or 0,
        }
    except Exception as e:
        print(f"[종목 통계] 오류: {e}")
        return {"total": 0, "active": 0, "kospi": 0, "kosdaq": 0, "etf": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 유틸리티 / Utility Functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_int(val) -> int:
    try:
        return int(str(val).replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return 0


def _parse_float(val) -> float:
    try:
        return float(str(val).replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return 0.0


def _is_etf(code: str, name: str) -> bool:
    """ETF 여부 판별"""
    etf_keywords = [
        "ETF", "ETN", "KODEX", "TIGER", "KBSTAR", "ARIRANG",
        "SOL", "HANARO", "ACE", "KOSEF", "KINDEX", "FOCUS",
        "파워", "인버스", "레버리지", "선물", "합성",
    ]
    name_upper = name.upper()
    for keyword in etf_keywords:
        if keyword.upper() in name_upper:
            return True
    return False
