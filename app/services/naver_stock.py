"""네이버 금융 일봉 데이터 수집기 / Naver Finance Daily Candle Collector
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KIS API 일봉 대신 네이버 금융 차트 API를 사용합니다.
- API 키 불필요 (무료)
- 호출 제한 거의 없음
- 최대 600거래일(약 2.5년) 데이터 수집 가능
- 모든 KOSPI/KOSDAQ 종목 지원

파일 경로: app/services/naver_stock.py
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time


def get_daily_candles_naver(code: str, count: int = 250) -> List[Dict]:
    """
    네이버 금융에서 일봉 데이터를 가져옵니다.
    Fetches daily candle data from Naver Finance.

    Args:
        code: 종목코드 (예: "005930")
        count: 가져올 일봉 개수 (최대 ~600)

    Returns:
        [{"date": "20260219", "open": 58000, "high": 59000,
          "low": 57500, "close": 58500, "volume": 12345678}, ...]
        날짜 오름차순 (과거 → 최근) 정렬
    """
    url = (
        f"https://fchart.stock.naver.com/sise.nhn"
        f"?symbol={code}&timeframe=day&count={count}&requestType=0"
    )

    try:
        res = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        res.encoding = "euc-kr"

        if res.status_code != 200:
            print(f"[네이버 일봉] {code}: HTTP {res.status_code}")
            return []

        # XML 파싱 / Parse XML
        root = ET.fromstring(res.text)
        items = root.findall(".//item")

        if not items:
            print(f"[네이버 일봉] {code}: 데이터 없음")
            return []

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
                # 유효성 검증 / Validation
                if candle["close"] > 0 and candle["volume"] >= 0:
                    candles.append(candle)
            except (ValueError, IndexError):
                continue

        # 날짜 오름차순 정렬 (과거 → 최근)
        # Sort ascending by date (oldest → newest)
        candles.sort(key=lambda x: x["date"])

        if candles:
            print(f"[네이버 일봉] {code}: {len(candles)}개 수집 "
                  f"({candles[0]['date']} ~ {candles[-1]['date']})")

        return candles

    except requests.exceptions.Timeout:
        print(f"[네이버 일봉] {code}: 타임아웃")
        return []
    except ET.ParseError as e:
        print(f"[네이버 일봉] {code}: XML 파싱 오류 - {e}")
        return []
    except Exception as e:
        print(f"[네이버 일봉] {code}: 오류 - {e}")
        return []


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

    print(f"[네이버 배치] {len(codes)}개 종목 일봉 수집 시작 (count={count})")

    for i, code in enumerate(codes):
        candles = get_daily_candles_naver(code, count)

        if candles and len(candles) >= 60:  # 최소 60일 필요
            result[code] = candles
            success += 1
        else:
            fail += 1

        # 진행률 로그 (50개마다)
        if (i + 1) % 50 == 0:
            print(f"[네이버 배치] 진행: {i + 1}/{len(codes)} "
                  f"(성공: {success}, 실패: {fail})")

        # 속도 제한 방지
        if delay > 0:
            time.sleep(delay)

    print(f"[네이버 배치] 완료: {success}개 성공, {fail}개 실패")
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
        print(f"[네이버 종목정보] {code}: {e}")
        return None
