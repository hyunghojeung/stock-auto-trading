"""네이버 금융 일봉 데이터 수집기 / Naver Finance Daily Candle Collector
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KIS API 일봉 대신 네이버 금융 차트 API를 사용합니다.
- API 키 불필요 (무료)
- 호출 제한 거의 없음
- 최대 600거래일(약 2.5년) 데이터 수집 가능
- 모든 KOSPI/KOSDAQ 종목 지원
- ★ 종목명도 함께 반환 (XML chartdata 태그의 name 속성)
- ★ OHLC 보정: 거래 희박 종목의 open/high/low=0 자동 보정
- ★ v2: 3단계 재시도 + 의심 데이터 감지 추가

파일 경로: app/services/naver_stock.py
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import time
import logging

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 재시도 설정 / Retry Configuration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RETRY_DELAYS = [2, 5, 10]  # 3단계 재시도 간격(초) / 3-step retry delays (seconds)
MAX_RETRIES = len(RETRY_DELAYS)


def get_daily_candles_naver(code: str, count: int = 250) -> List[Dict]:
    """
    네이버 금융에서 일봉 데이터를 가져옵니다 (캔들만 반환).
    Fetches daily candle data from Naver Finance (candles only).
    """
    candles, _ = _fetch_naver_chart(code, count)
    return candles


def get_daily_candles_with_name(code: str, count: int = 250) -> Tuple[List[Dict], str]:
    """
    ★ 일봉 데이터 + 종목명을 함께 가져옵니다.
    Fetches daily candles AND stock name together (single API call).

    Returns:
        (candles, stock_name)
        예: ([{...}, ...], "삼성전자")
    """
    return _fetch_naver_chart(code, count)


def get_daily_candles_naver_safe(code: str, count: int = 3) -> Dict:
    """
    ★ 안전한 일봉 조회 (스케줄러용) — 재시도 + 의심 데이터 감지
    Safe daily candle fetch (for scheduler) — with retry + suspicious data detection

    Returns:
        {
            "candles": [...],
            "name": "삼성전자",
            "suspicious": False,     # 데이터 의심 여부
            "suspicious_reason": "",  # 의심 사유
            "success": True,
            "retries": 0,            # 재시도 횟수
        }
    """
    result = {
        "candles": [],
        "name": code,
        "suspicious": False,
        "suspicious_reason": "",
        "success": False,
        "retries": 0,
    }

    # 3단계 재시도 / 3-step retry
    for attempt in range(MAX_RETRIES + 1):
        try:
            candles, name = _fetch_naver_chart(code, count)
            result["name"] = name

            if candles and len(candles) > 0:
                result["candles"] = candles
                result["success"] = True
                result["retries"] = attempt

                # ★ 의심 데이터 감지 / Suspicious data detection
                suspicious, reason = _check_suspicious(candles)
                result["suspicious"] = suspicious
                result["suspicious_reason"] = reason

                return result

        except Exception as e:
            logger.warning(f"[네이버 안전조회] {code} 시도 {attempt + 1}/{MAX_RETRIES + 1} 실패: {e}")

        # 재시도 대기 (마지막 시도 후에는 대기 없음)
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAYS[attempt]
            logger.info(f"[네이버 안전조회] {code} {delay}초 후 재시도...")
            time.sleep(delay)
            result["retries"] = attempt + 1

    # 모든 재시도 실패
    logger.error(f"[네이버 안전조회] {code}: {MAX_RETRIES + 1}회 시도 모두 실패")
    return result


def _check_suspicious(candles: List[Dict]) -> Tuple[bool, str]:
    """
    ★ 데이터 의심 여부 판단 / Check if data seems suspicious
    - 최근 3일 종가가 모두 동일 → 의심
    - OHLC 전체가 동일 → 의심 (거래 정지 가능성)

    Returns:
        (is_suspicious, reason)
    """
    if len(candles) < 3:
        return False, ""

    recent = candles[-3:]

    # 최근 3일 종가 모두 동일 체크 / Check if last 3 days close prices are identical
    closes = [c["close"] for c in recent]
    if len(set(closes)) == 1:
        return True, f"최근 3일 종가 동일({closes[0]}원) — 거래정지/데이터오류 의심"

    # 최근 1일 OHLC 모두 동일 체크 / Check if latest day OHLC are all the same
    latest = recent[-1]
    if (latest["open"] == latest["high"] == latest["low"] == latest["close"]):
        vol = latest.get("volume", 0)
        if vol == 0:
            return True, f"최근일 OHLC 동일 + 거래량 0 — 거래정지 의심"

    return False, ""


def _fetch_naver_chart(code: str, count: int) -> Tuple[List[Dict], str]:
    """
    네이버 차트 API 호출 (내부 공통 함수)
    ★ XML의 chartdata 태그에서 종목명(name) 추출
    ★ OHLC 보정: open/high/low가 0이면 close로 채움 (거래 희박 종목 대응)
    """
    url = (
        f"https://fchart.stock.naver.com/sise.nhn"
        f"?symbol={code}&timeframe=day&count={count}&requestType=0"
    )

    stock_name = code  # 기본값: 코드

    try:
        res = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        res.encoding = "euc-kr"

        if res.status_code != 200:
            logger.warning(f"[네이버 일봉] {code}: HTTP {res.status_code}")
            return [], code

        # XML 파싱 / Parse XML
        root = ET.fromstring(res.text)

        # ★ 종목명 추출: <chartdata ... name="삼성전자" ...>
        chartdata = root.find(".//chartdata")
        if chartdata is not None:
            name_attr = chartdata.get("name", "")
            if name_attr:
                stock_name = name_attr

        items = root.findall(".//item")

        if not items:
            logger.info(f"[네이버 일봉] {code}: 데이터 없음")
            return [], stock_name

        candles = []
        for item in items:
            data = item.get("data", "")
            parts = data.split("|")
            if len(parts) < 6:
                continue

            try:
                close_val = int(parts[4].strip())
                if close_val <= 0:
                    continue

                open_val = int(parts[1].strip())
                high_val = int(parts[2].strip())
                low_val = int(parts[3].strip())
                vol_val = int(parts[5].strip())

                # ★ OHLC 보정: open/high/low가 0이면 close로 채움
                # 거래가 극히 적은 종목에서 발생 (예: 선도전기, 에스디생명공학 등)
                if open_val <= 0:
                    open_val = close_val
                if high_val <= 0:
                    high_val = close_val
                if low_val <= 0:
                    low_val = close_val

                # 정합성 보장: high >= max(open, close), low <= min(open, close)
                high_val = max(high_val, open_val, close_val)
                low_val = min(low_val, open_val, close_val) if min(low_val, open_val, close_val) > 0 else close_val

                candle = {
                    "date": parts[0].strip(),        # "20260219"
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                    "volume": max(vol_val, 0),
                }
                candles.append(candle)
            except (ValueError, IndexError):
                continue

        # 날짜 오름차순 정렬 (과거 → 최근)
        candles.sort(key=lambda x: x["date"])

        if candles:
            logger.debug(f"[네이버 일봉] {code}({stock_name}): {len(candles)}개 수집 "
                  f"({candles[0]['date']} ~ {candles[-1]['date']})")

        return candles, stock_name

    except requests.exceptions.Timeout:
        logger.warning(f"[네이버 일봉] {code}: 타임아웃")
        return [], code
    except ET.ParseError as e:
        logger.warning(f"[네이버 일봉] {code}: XML 파싱 오류 - {e}")
        return [], code
    except Exception as e:
        logger.warning(f"[네이버 일봉] {code}: 오류 - {e}")
        return [], code


def get_daily_candles_naver_batch(
    codes: List[str],
    count: int = 250,
    delay: float = 0.1
) -> Dict[str, List[Dict]]:
    """
    여러 종목의 일봉 데이터를 배치로 수집합니다.
    Batch collect daily candles for multiple stocks.

    Args:
        codes: 종목코드 리스트
        count: 종목당 일봉 개수
        delay: API 호출 간격 (초) - 네이버 서버 부하 방지

    Returns:
        {"005930": [candles...], "000660": [candles...], ...}
    """
    result = {}
    success = 0
    fail = 0

    logger.info(f"[네이버 배치] {len(codes)}개 종목 일봉 수집 시작 (count={count})")

    for i, code in enumerate(codes):
        candles = get_daily_candles_naver(code, count)

        if candles and len(candles) >= 60:  # 최소 60일 필요
            result[code] = candles
            success += 1
        else:
            fail += 1

        # 진행률 로그 (50개마다)
        if (i + 1) % 50 == 0:
            logger.info(f"[네이버 배치] 진행: {i + 1}/{len(codes)} "
                  f"(성공: {success}, 실패: {fail})")

        # 속도 제한 방지
        if delay > 0:
            time.sleep(delay)

    logger.info(f"[네이버 배치] 완료: {success}개 성공, {fail}개 실패")
    return result


def get_stock_info_naver(code: str) -> Optional[Dict]:
    """
    네이버 금융에서 종목 기본 정보를 가져옵니다.
    Fetches basic stock info from Naver Finance.

    Returns:
        {"code": "005930", "name": "삼성전자", "price": 58000,
         "market_cap": 3460000, ...}
    """
    url = f"https://finance.naver.com/item/main.naver?code={code}"

    try:
        res = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        res.encoding = "euc-kr"

        if res.status_code != 200:
            return None

        # 간단한 텍스트 파싱 (BeautifulSoup 없이)
        text = res.text

        # 종목명 추출
        name = ""
        name_start = text.find('<title>')
        name_end = text.find('</title>')
        if name_start >= 0 and name_end >= 0:
            title = text[name_start + 7:name_end]
            # "삼성전자 : 네이버 금융" 형태
            name = title.split(":")[0].strip()

        return {
            "code": code,
            "name": name,
        }

    except Exception as e:
        logger.warning(f"[네이버 종목정보] {code}: {e}")
        return None
