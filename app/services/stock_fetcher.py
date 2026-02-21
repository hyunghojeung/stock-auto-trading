"""
전종목 수집 서비스 / Stock List Fetcher Service
KRX(한국거래소)에서 전체 상장종목을 가져와 Supabase DB에 저장

파일 위치: app/services/stock_fetcher.py
"""

import requests
import json
import time
import traceback
from datetime import datetime, date
from typing import List, Dict, Optional

from app.core.database import db


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. KRX 전종목 수집 / Fetch All Stocks from KRX
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_krx_all_stocks() -> List[Dict]:
    """
    KRX(한국거래소)에서 코스피 + 코스닥 전종목 데이터 수집
    약 2500개 종목의 코드, 이름, 시장, 시가총액, 가격 등을 가져옴

    Returns: [{"code": "005930", "name": "삼성전자", "market": "kospi", ...}, ...]
    """
    all_stocks = []

    # 코스피 수집
    print("[전종목 수집] 코스피(KOSPI) 종목 수집 시작...")
    kospi = _fetch_krx_market("STK")
    print(f"[전종목 수집] 코스피 {len(kospi)}개 종목 수집 완료")
    all_stocks.extend(kospi)

    # API 부하 방지 대기
    time.sleep(1)

    # 코스닥 수집
    print("[전종목 수집] 코스닥(KOSDAQ) 종목 수집 시작...")
    kosdaq = _fetch_krx_market("KSQ")
    print(f"[전종목 수집] 코스닥 {len(kosdaq)}개 종목 수집 완료")
    all_stocks.extend(kosdaq)

    print(f"[전종목 수집] 총 {len(all_stocks)}개 종목 수집 완료")
    return all_stocks


def _fetch_krx_market(market_code: str) -> List[Dict]:
    """
    KRX DATA에서 특정 시장의 전종목 데이터 가져오기
    market_code: "STK" (코스피) 또는 "KSQ" (코스닥)
    """
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    }

    # 오늘 날짜 (주말이면 금요일로 조정)
    today = _get_last_trading_date()

    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "locale": "ko_KR",
        "mktId": market_code,
        "trdDd": today,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }

    try:
        resp = requests.post(url, headers=headers, data=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        items = result.get("OutBlock_1", [])

        stocks = []
        market_name = "kospi" if market_code == "STK" else "kosdaq"

        for item in items:
            try:
                code = item.get("ISU_SRT_CD", "").strip()
                name = item.get("ISU_ABBRV", "").strip()

                # 빈 코드/이름 건너뛰기
                if not code or not name:
                    continue

                # 6자리 숫자 코드만 (ETN, 워런트 등 제외)
                if len(code) != 6 or not code.isdigit():
                    continue

                # 가격, 거래량 파싱
                price = _parse_int(item.get("TDD_CLSPRC", "0"))
                volume = _parse_int(item.get("ACC_TRDVOL", "0"))
                change_pct = _parse_float(item.get("FLUC_RT", "0"))
                market_cap = _parse_int(item.get("MKTCAP", "0"))
                listed_shares = _parse_int(item.get("LIST_SHRS", "0"))

                # ETF 판별 (종목코드 패턴 + 이름 패턴)
                is_etf = _is_etf(code, name)

                # 우선주 판별 (코드 끝자리가 0이 아닌 경우)
                is_preferred = code[-1] != "0" and not is_etf

                # 업종 (섹터)
                sector = item.get("IDX_IND_NM", "").strip()

                stocks.append({
                    "code": code,
                    "name": name,
                    "market": market_name,
                    "sector": sector,
                    "market_cap": market_cap,
                    "price": price,
                    "volume": volume,
                    "change_pct": change_pct,
                    "is_active": True,
                    "is_etf": is_etf,
                    "is_preferred": is_preferred,
                    "listed_shares": listed_shares,
                })
            except Exception as e:
                continue

        return stocks

    except Exception as e:
        print(f"[전종목 수집] KRX {market_code} 수집 실패: {e}")
        traceback.print_exc()
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. DB 저장 / Save to Supabase
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def save_stocks_to_db(stocks: List[Dict]) -> Dict:
    """
    수집된 종목 데이터를 Supabase stock_list 테이블에 저장
    UPSERT: 이미 있는 종목은 업데이트, 없는 종목은 신규 삽입
    
    Returns: {"inserted": n, "updated": n, "total": n, "errors": n}
    """
    if not stocks:
        return {"inserted": 0, "updated": 0, "total": 0, "errors": 0}

    inserted = 0
    updated = 0
    errors = 0
    batch_size = 100  # 한 번에 100개씩 처리

    print(f"[DB 저장] {len(stocks)}개 종목 저장 시작 (배치 크기: {batch_size})")

    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        try:
            # upsert: code 기준으로 중복이면 UPDATE, 없으면 INSERT
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
            print(f"[DB 저장] 배치 {i//batch_size + 1} 오류: {e}")

    # 상장폐지 종목 비활성화 처리
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
    """
    현재 KRX 목록에 없는 기존 DB 종목을 비활성화 (상장폐지 처리)
    """
    try:
        current_codes = {s["code"] for s in current_stocks}

        # DB에서 현재 활성 종목 조회
        existing = db.table("stock_list").select("code").eq(
            "is_active", True
        ).execute().data

        if not existing:
            return 0

        # KRX에 없는 종목 = 상장폐지 후보
        delisted_codes = [
            s["code"] for s in existing
            if s["code"] not in current_codes
        ]

        if not delisted_codes:
            return 0

        # 배치 비활성화
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
    """
    전체 프로세스 실행:
    1) KRX에서 전종목 수집
    2) Supabase DB에 저장 (upsert)
    3) 상장폐지 종목 비활성화

    스케줄러에서 매일 18:00에 호출됨
    """
    print(f"\n{'='*50}")
    print(f"[전종목 업데이트] 시작: {datetime.now()}")
    print(f"{'='*50}")

    start = time.time()

    # Step 1: KRX 수집
    stocks = fetch_krx_all_stocks()
    if not stocks:
        return {
            "success": False,
            "error": "KRX에서 종목 수집 실패",
            "elapsed": 0,
        }

    # Step 2: DB 저장
    result = await save_stocks_to_db(stocks)

    elapsed = round(time.time() - start, 1)
    print(f"[전종목 업데이트] 완료: {elapsed}초 소요")
    print(f"{'='*50}\n")

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
    """
    stock_list 테이블에서 종목 검색 (종목명 또는 코드)
    패턴탐지기, 스윙백테스트 등 모든 검색 기능에서 공통으로 사용

    Args:
        query: 검색어 (종목명 또는 코드)
        market: "kospi" 또는 "kosdaq" (None이면 전체)
        limit: 최대 결과 수
        active_only: 활성 종목만
        exclude_etf: ETF 제외
        exclude_preferred: 우선주 제외

    Returns: [{"code": "005930", "name": "삼성전자", "market": "kospi", ...}, ...]
    """
    try:
        q = query.strip()
        if not q:
            return []

        # 쿼리 빌더 시작
        builder = db.table("stock_list").select(
            "code, name, market, sector, market_cap, price, volume, change_pct, is_etf, is_preferred"
        )

        # 필터 적용
        if active_only:
            builder = builder.eq("is_active", True)
        if market:
            builder = builder.eq("market", market)
        if exclude_etf:
            builder = builder.eq("is_etf", False)
        if exclude_preferred:
            builder = builder.eq("is_preferred", False)

        # 검색: 코드 또는 이름
        if q.isdigit():
            # 숫자만 입력 → 코드로 검색 (부분 일치)
            builder = builder.like("code", f"{q}%")
        else:
            # 한글/영문 → 이름으로 검색 (부분 일치)
            builder = builder.ilike("name", f"%{q}%")

        # 시가총액 순 정렬 + 제한
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
    """
    전체 활성 종목 조회 (패턴분석, 스윙발굴 등에서 사용)
    
    Args:
        market: "kospi"/"kosdaq"/None(전체)
        exclude_etf: ETF 제외 (기본 True)
        exclude_preferred: 우선주 제외 (기본 True)
        min_price: 최소 가격 필터
        min_volume: 최소 거래량 필터
    """
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

        # Supabase 기본 limit이 1000이므로, 전체 가져오려면 페이지네이션
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
    """최근 거래일 반환 (주말이면 금요일로 조정)"""
    today = date.today()
    weekday = today.weekday()  # 월=0, 일=6
    if weekday == 5:  # 토요일
        today = today.replace(day=today.day - 1)
    elif weekday == 6:  # 일요일
        today = today.replace(day=today.day - 2)
    return today.strftime("%Y%m%d")


def _parse_int(val) -> int:
    """쉼표 포함 숫자 문자열 → int 변환"""
    try:
        return int(str(val).replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return 0


def _parse_float(val) -> float:
    """쉼표 포함 소수 문자열 → float 변환"""
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
