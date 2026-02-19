"""네이버 금융 일봉 데이터 수집기 / Naver Finance Daily Candle Collector
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KIS API 일봉 대신 네이버 금융 차트 API를 사용합니다.
- API 키 불필요 (무료)
- 호출 제한 거의 없음
- 최대 600거래일(약 2.5년) 데이터 수집 가능
- 모든 KOSPI/KOSDAQ 종목 지원
- ★ 종목명도 함께 반환 (XML chartdata 태그의 name 속성)

파일 경로: app/services/naver_stock.py
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import time


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


def _fetch_naver_chart(code: str, count: int) -> Tuple[List[Dict], str]:
    """
    네이버 차트 API 호출 (내부 공통 함수)
    ★ XML의 chartdata 태그에서 종목명(name) 추출
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
            print(f"[네이버 일봉] {code}: HTTP {res.status_code}")
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
            print(f"[네이버 일봉] {code}: 데이터 없음")
            return [], stock_name

        candles = []
        for item in items:
            data = item.get("data", "")
            parts = data.split("|")
            if len(parts) < 6:
                continue

            try:
                candle = {
                    "date": parts[0].strip(),        # "20260219"
                    "open": int(parts[1].strip()),
                    "high": int(parts[2].strip()),
                    "low": int(parts[3].strip()),
                    "close": int(parts[4].strip()),
                    "volume": int(parts[5].strip()),
                }
                if candle["close"] > 0 and candle["volume"] >= 0:
                    candles.append(candle)
            except (ValueError, IndexError):
                continue

        # 날짜 오름차순 정렬 (과거 → 최근)
        candles.sort(key=lambda x: x["date"])

        if candles:
            print(f"[네이버 일봉] {code}({stock_name}): {len(candles)}개 수집 "
                  f"({candles[0]['date']} ~ {candles[-1]['date']})")

        return candles, stock_name

    except requests.exceptions.Timeout:
        print(f"[네이버 일봉] {code}: 타임아웃")
        return [], code
    except ET.ParseError as e:
        print(f"[네이버 일봉] {code}: XML 파싱 오류 - {e}")
        return [], code
    except Exception as e:
        print(f"[네이버 일봉] {code}: 오류 - {e}")
        return [], code


def get_daily_candles_naver_batch(
    codes: List[str],
    count: int = 250,
    delay: float = 0.1
) -> Dict[str, List[Dict]]:
    """
    여러 종목의 일봉 데이터를 배치로 수집합니다.
    Batch collect daily candles for multiple stocks.
    """
    result = {}
    success = 0
    fail = 0

    print(f"[네이버 배치] {len(codes)}개 종목 일봉 수집 시작 (count={count})")

    for i, code in enumerate(codes):
        candles = get_daily_candles_naver(code, count)

        if candles and len(candles) >= 60:
            result[code] = candles
            success += 1
        else:
            fail += 1

        if (i + 1) % 50 == 0:
            print(f"[네이버 배치] 진행: {i + 1}/{len(codes)} "
                  f"(성공: {success}, 실패: {fail})")

        if delay > 0:
            time.sleep(delay)

    print(f"[네이버 배치] 완료: {success}개 성공, {fail}개 실패")
    return result
