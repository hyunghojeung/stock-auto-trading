"""
전종목 수집 서비스 / Stock List Fetcher Service
requests.Session으로 KRX 세션/쿠키 자동 처리 (추가 라이브러리 불필요)

파일 위치: app/services/stock_fetcher.py
"""

import requests
import time
import traceback
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

from app.core.database import db


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. KRX 전종목 수집 / Fetch All Stocks from KRX
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_krx_all_stocks() -> List[Dict]:
    """
    KRX(한국거래소)에서 코스피 + 코스닥 전종목 데이터 수집
    requests.Session 사용으로 세션/쿠키 자동 처리
    """
    trade_date = _get_last_trading_date()
    print(f"[전종목 수집] 기준일: {trade_date}")

    all_stocks = []

    # 세션 생성 (쿠키 자동 처리)
    session = _create_krx_session()

    # 코스피 수집
    print("[전종목 수집] 코스피(KOSPI) 종목 수집 시작...")
    kospi = _fetch_krx_market(session, "STK", trade_date)
    print(f"[전종목 수집] 코스피 {len(kospi)}개 종목 수집 완료")
    all_stocks.extend(kospi)

    time.sleep(2)

    # 코스닥 수집
    print("[전종목 수집] 코스닥(KOSDAQ) 종목 수집 시작...")
    kosdaq = _fetch_krx_market(session, "KSQ", trade_date)
    print(f"[전종목 수집] 코스닥 {len(kosdaq)}개 종목 수집 완료")
    all_stocks.extend(kosdaq)

    print(f"[전종목 수집] 총 {len(all_stocks)}개 종목 수집 완료")
    return all_stocks


def _create_krx_session() -> requests.Session:
    """KRX 세션 생성 — 먼저 메인 페이지 방문하여 쿠키 획득"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "X-Requested-With": "XMLHttpRequest",
    })

    try:
        # KRX 메인 페이지 방문하여 세션 쿠키 획득
        session.get(
            "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
            timeout=15
        )
        print("[전종목 수집] KRX 세션 쿠키 획득 완료")
    except Exception as e:
        print(f"[전종목 수집] KRX 세션 생성 경고: {e} (계속 진행)")

    return session


def _fetch_krx_market(session: requests.Session, market_code: str, trade_date: str) -> List[Dict]:
    """
    KRX DATA에서 특정 시장의 전종목 데이터 가져오기
    market_code: "STK" (코스피) 또는 "KSQ" (코스닥)
    """
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "locale": "ko_KR",
        "mktId": market_code,
        "trdDd": trade_date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }

    # 최대 3회 재시도 (날짜를 하루씩 뒤로)
    retry_date = trade_date
    for attempt in range(3):
        try:
            data["trdDd"] = retry_date
            print(f"[전종목 수집] KRX 요청: market={market_code}, date={retry_date}, 시도={attempt + 1}")

            resp = session.post(
                url,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
                },
                timeout=30
            )

            print(f"[전종목 수집] KRX 응답: status={resp.status_code}, length={len(resp.text)}")

            if resp.status_code == 200:
                result = resp.json()
                items = result.get("OutBlock_1", [])

                if items:
                    stocks = _parse_krx_items(items, market_code)
                    return stocks
                else:
                    print(f"[전종목 수집] {market_code} date={retry_date} → 데이터 0건, 이전 날짜로 재시도")
            else:
                print(f"[전종목 수집] {market_code} HTTP {resp.status_code}, 이전 날짜로 재시도")

        except Exception as e:
            print(f"[전종목 수집] KRX {market_code} 요청 오류: {e}")

        # 하루 전으로 이동
        dt = datetime.strptime(retry_date, "%Y%m%d").date() - timedelta(days=1)
        # 주말 건너뛰기
        while dt.weekday() >= 5:
            dt -= timedelta(days=1)
        retry_date = dt.strftime("%Y%m%d")
        time.sleep(1)

    print(f"[전종목 수집] {market_code} 3회 시도 모두 실패")
    return []


def _parse_krx_items(items: list, market_code: str) -> List[Dict]:
    """KRX 응답 데이터를 파싱하여 종목 리스트로 변환"""
    stocks = []
    market_name = "kospi" if market_code == "STK" else "kosdaq"

    for item in items:
        try:
            code = item.get("ISU_SRT_CD", "").strip()
            name = item.get("ISU_ABBRV", "").strip()

            if not code or not name:
                continue
            if len(code) != 6 or not code.isdigit():
                continue

            price = _parse_int(item.get("TDD_CLSPRC", "0"))
            volume = _parse_int(item.get("ACC_TRDVOL", "0"))
            change_pct = _parse_float(item.get("FLUC_RT", "0"))
            market_cap = _parse_int(item.get("MKTCAP", "0"))
            listed_shares = _parse_int(item.get("LIST_SHRS", "0"))

            is_etf = _is_etf(code, name)
            is_preferred = code[-1] != "0" and not is_etf
            sector = item.get("IDX_IND_NM", "").strip()

            stocks.append({
                "code": code,
                "name": name,
                "market": market_name,
                "sector": sector,
                "market_cap": market_cap,
                "price": price,
                "volume": volume,
                "change_pct": round(change_pct, 2),
                "is_active": True,
                "is_etf": is_etf,
                "is_preferred": is_preferred,
                "listed_shares": listed_shares,
            })
        except Exception:
            continue

    return stocks


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
    """현재 KRX 목록에 없는 기존 DB 종목을 비활성화"""
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
    """전체 프로세스: KRX 수집 → DB 저장 → 상장폐지 처리"""
    print(f"\n{'=' * 50}")
    print(f"[전종목 업데이트] 시작: {datetime.now()}")
    print(f"{'=' * 50}")

    start = time.time()

    stocks = fetch_krx_all_stocks()
    if not stocks:
        return {
            "success": False,
            "error": "KRX에서 종목 수집 실패",
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
    """stock_list 테이블에서 종목 검색 (종목명 또는 코드)"""
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

def _get_last_trading_date() -> str:
    """최근 거래일 반환 (장중이면 직전 거래일, 장 마감 후면 오늘)"""
    now = datetime.now()
    today = now.date()

    # 16시 이전이면 오늘 데이터 미확정 → 직전 거래일
    if now.hour < 16:
        today = today - timedelta(days=1)

    # 주말이면 이전 평일로
    for _ in range(10):
        if today.weekday() < 5:
            break
        today = today - timedelta(days=1)

    return today.strftime("%Y%m%d")


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
