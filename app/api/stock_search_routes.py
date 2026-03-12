"""KIS 종목코드 마스터파일 기반 종목 검색 API.

한국투자증권이 공식 제공하는 KOSPI/KOSDAQ 종목코드 마스터파일을 다운로드·파싱하여
종목명 또는 종목코드로 검색할 수 있는 REST API를 제공합니다.

마스터파일 출처:
  - KOSPI: https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip
  - KOSDAQ: https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip
  - 참고: https://github.com/koreainvestment/open-trading-api/tree/main/stocks_info
"""

import os
import ssl
import time
import zipfile
import tempfile
import urllib.request
from typing import List, Optional
from fastapi import APIRouter, Query

router = APIRouter(prefix="/api", tags=["종목검색"])

# ── 캐시 ──
_stock_cache: List[dict] = []
_cache_time: float = 0
_CACHE_TTL = 24 * 60 * 60  # 24시간


# ──────────────────────────────────────────────
# 마스터파일 다운로드 & 파싱
# ──────────────────────────────────────────────

def _download_and_extract(url: str, tmp_dir: str, filename: str) -> str:
    """ZIP 다운로드 → 압축 해제 → .mst 파일 경로 반환."""
    ssl._create_default_https_context = ssl._create_unverified_context
    zip_path = os.path.join(tmp_dir, f"{filename}.zip")
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmp_dir)
    os.remove(zip_path)
    return os.path.join(tmp_dir, f"{filename}.mst")


def _parse_kospi(mst_path: str) -> List[dict]:
    """KOSPI 마스터파일 파싱. 뒤 228바이트가 추가정보."""
    results = []
    try:
        with open(mst_path, mode="r", encoding="cp949") as f:
            for row in f:
                # 기본정보 = row[:-228], 추가정보 = row[-228:]
                basic = row[: len(row) - 228]
                code = basic[0:9].strip()
                # std_code = basic[9:21].strip()
                name = basic[21:].strip()
                if code and name:
                    results.append({"code": code, "name": name, "market": "KOSPI"})
    except Exception as e:
        print(f"[종목검색] KOSPI 파싱 오류: {e}")
    return results


def _parse_kosdaq(mst_path: str) -> List[dict]:
    """KOSDAQ 마스터파일 파싱. 뒤 222바이트가 추가정보."""
    results = []
    try:
        with open(mst_path, mode="r", encoding="cp949") as f:
            for row in f:
                basic = row[: len(row) - 222]
                code = basic[0:9].strip()
                name = basic[21:].strip()
                if code and name:
                    results.append({"code": code, "name": name, "market": "KOSDAQ"})
    except Exception as e:
        print(f"[종목검색] KOSDAQ 파싱 오류: {e}")
    return results


def _load_master_files() -> List[dict]:
    """KOSPI + KOSDAQ 마스터파일을 다운로드·파싱하여 종목 리스트 반환."""
    tmp_dir = tempfile.mkdtemp(prefix="kis_master_")
    all_stocks: List[dict] = []
    try:
        # KOSPI
        try:
            kospi_path = _download_and_extract(
                "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
                tmp_dir, "kospi_code",
            )
            kospi = _parse_kospi(kospi_path)
            all_stocks.extend(kospi)
            print(f"[종목검색] KOSPI {len(kospi)}개 종목 로드")
        except Exception as e:
            print(f"[종목검색] KOSPI 다운로드/파싱 실패: {e}")

        # KOSDAQ
        try:
            kosdaq_path = _download_and_extract(
                "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
                tmp_dir, "kosdaq_code",
            )
            kosdaq = _parse_kosdaq(kosdaq_path)
            all_stocks.extend(kosdaq)
            print(f"[종목검색] KOSDAQ {len(kosdaq)}개 종목 로드")
        except Exception as e:
            print(f"[종목검색] KOSDAQ 다운로드/파싱 실패: {e}")

    finally:
        # 임시파일 정리
        import shutil
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    print(f"[종목검색] 총 {len(all_stocks)}개 종목 캐시 완료")
    return all_stocks


def _get_stocks() -> List[dict]:
    """캐시된 종목 리스트 반환. TTL 만료 시 재로드."""
    global _stock_cache, _cache_time
    if _stock_cache and (time.time() - _cache_time < _CACHE_TTL):
        return _stock_cache
    _stock_cache = _load_master_files()
    _cache_time = time.time()
    return _stock_cache


# ──────────────────────────────────────────────
# 검색 로직
# ──────────────────────────────────────────────

def _search_stocks(keyword: str, stocks: List[dict], limit: int = 20) -> List[dict]:
    """종목 검색. 우선순위: 정확매칭 > 시작매칭 > 포함매칭."""
    kw = keyword.strip().lower()
    if not kw:
        return []

    exact = []      # 코드 또는 이름 정확 일치
    starts = []     # 코드가 시작하거나, 이름이 시작
    contains = []   # 코드 또는 이름에 포함

    for s in stocks:
        code = s["code"]
        name_lower = s["name"].lower()

        if code == kw or name_lower == kw:
            exact.append(s)
        elif code.startswith(kw) or name_lower.startswith(kw):
            starts.append(s)
        elif kw in code or kw in name_lower:
            contains.append(s)

    # 각 그룹 내에서 이름 순 정렬
    exact.sort(key=lambda x: x["name"])
    starts.sort(key=lambda x: x["name"])
    contains.sort(key=lambda x: x["name"])

    merged = exact + starts + contains
    return merged[:limit]


# ──────────────────────────────────────────────
# API 엔드포인트
# ──────────────────────────────────────────────

@router.get("/stock-search")
async def stock_search(
    keyword: str = Query("", description="종목코드 또는 종목명"),
    limit: int = Query(20, ge=1, le=100, description="최대 결과 수"),
    market: Optional[str] = Query(None, description="시장 필터 (KOSPI / KOSDAQ)"),
):
    """종목코드/종목명으로 검색.

    - 로컬 KIS 마스터파일 캐시에서 검색 (24시간 TTL)
    - 우선순위: 정확매칭 > 시작매칭 > 포함매칭
    """
    keyword = keyword.strip()
    if not keyword:
        return {"success": True, "results": [], "total": 0}

    stocks = _get_stocks()

    # 시장 필터
    if market and market.upper() in ("KOSPI", "KOSDAQ"):
        stocks = [s for s in stocks if s["market"] == market.upper()]

    results = _search_stocks(keyword, stocks, limit)
    return {
        "success": True,
        "results": results,
        "total": len(results),
        "cached_total": len(_stock_cache),
    }


@router.get("/stock-search/status")
async def stock_search_status():
    """종목검색 캐시 상태 확인."""
    return {
        "cached_count": len(_stock_cache),
        "cache_age_seconds": int(time.time() - _cache_time) if _cache_time > 0 else -1,
        "cache_ttl_seconds": _CACHE_TTL,
        "is_cached": len(_stock_cache) > 0,
    }


@router.post("/stock-search/refresh")
async def stock_search_refresh():
    """종목검색 캐시 강제 갱신."""
    global _stock_cache, _cache_time
    _cache_time = 0  # TTL 만료 강제
    stocks = _get_stocks()
    return {
        "success": True,
        "message": f"캐시 갱신 완료: {len(stocks)}개 종목",
        "cached_count": len(stocks),
    }
