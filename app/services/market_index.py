"""시장 지수 수집 및 상태 판정 모듈 / Market Index Fetcher & Status Evaluator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
네이버 금융에서 KOSPI/KOSDAQ 지수 일봉을 수집하고,
시장 상태(매수 허용 여부)를 판정합니다.

매수 금지 조건 (3가지 중 2가지 이상 충족 시 차단):
  1. MA5 < MA20 (단기 이평이 중기 아래)
  2. 지수 3일 연속 하락
  3. 당일 지수 등락률 -2% 이하

파일경로: app/services/market_index.py
"""

import requests
import xml.etree.ElementTree as ET
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 지수 심볼 매핑 / Index symbol mapping
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INDEX_SYMBOLS = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}

# 매수 금지 임계값 / Buy block thresholds
BLOCK_THRESHOLDS = {
    "consecutive_decline_days": 3,   # 연속 하락 일수
    "daily_drop_pct": -2.0,          # 당일 등락률 기준 (%)
    "conditions_to_block": 2,        # 이 개수 이상 충족 시 차단
}


def get_index_candles(index_name: str = "KOSPI", count: int = 30) -> List[Dict]:
    """
    네이버 금융에서 지수 일봉 데이터를 가져옵니다.
    Fetches index daily candle data from Naver Finance.

    Args:
        index_name: "KOSPI" 또는 "KOSDAQ"
        count: 가져올 일봉 개수 (기본 30일)

    Returns:
        [{"date": "20260228", "open": 2650.0, "high": 2680.0,
          "low": 2640.0, "close": 2670.0, "volume": 450000000}, ...]
    """
    symbol = INDEX_SYMBOLS.get(index_name.upper(), index_name)
    url = (
        f"https://fchart.stock.naver.com/sise.nhn"
        f"?symbol={symbol}&timeframe=day&count={count}&requestType=0"
    )

    try:
        res = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        res.encoding = "euc-kr"

        if res.status_code != 200:
            logger.warning(f"[시장지수] {index_name}: HTTP {res.status_code}")
            return []

        root = ET.fromstring(res.text)
        items = root.findall(".//item")

        if not items:
            logger.info(f"[시장지수] {index_name}: 데이터 없음")
            return []

        candles = []
        for item in items:
            data = item.get("data", "")
            parts = data.split("|")
            if len(parts) < 6:
                continue
            try:
                candle = {
                    "date": parts[0].strip(),
                    "open": float(parts[1].strip()),
                    "high": float(parts[2].strip()),
                    "low": float(parts[3].strip()),
                    "close": float(parts[4].strip()),
                    "volume": int(parts[5].strip()),
                }
                if candle["close"] > 0:
                    candles.append(candle)
            except (ValueError, IndexError):
                continue

        candles.sort(key=lambda x: x["date"])

        if candles:
            logger.info(f"[시장지수] {index_name}: {len(candles)}일 수집 "
                       f"({candles[0]['date']} ~ {candles[-1]['date']}) "
                       f"최근종가: {candles[-1]['close']}")

        return candles

    except requests.exceptions.Timeout:
        logger.warning(f"[시장지수] {index_name}: 타임아웃")
        return []
    except ET.ParseError as e:
        logger.warning(f"[시장지수] {index_name}: XML 파싱 오류 - {e}")
        return []
    except Exception as e:
        logger.warning(f"[시장지수] {index_name}: 오류 - {e}")
        return []


def _calc_ma(candles: List[Dict], period: int) -> float:
    """최근 N일 이동평균 계산 / Calculate N-day moving average"""
    if len(candles) < period:
        return 0.0
    recent = candles[-period:]
    return sum(c["close"] for c in recent) / period


def _count_consecutive_declines(candles: List[Dict]) -> int:
    """최근 연속 하락 일수 계산 / Count consecutive decline days from latest"""
    if len(candles) < 2:
        return 0
    count = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] < candles[i - 1]["close"]:
            count += 1
        else:
            break
    return count


def _calc_change_pct(candles: List[Dict]) -> float:
    """당일 등락률 계산 / Calculate today's change percentage"""
    if len(candles) < 2:
        return 0.0
    prev_close = candles[-2]["close"]
    curr_close = candles[-1]["close"]
    if prev_close <= 0:
        return 0.0
    return round((curr_close - prev_close) / prev_close * 100, 2)


def evaluate_index(candles: List[Dict], index_name: str = "") -> Dict:
    """
    개별 지수의 상태를 평가합니다.
    Evaluate a single index's status.

    Returns:
        {
            "close": 2670.0,
            "change_pct": -1.5,
            "ma5": 2680.0,
            "ma20": 2690.0,
            "ma5_below_ma20": True,
            "consecutive_declines": 3,
            "daily_drop_severe": False,
            "block_conditions_met": 2,
            "trend": "하락"
        }
    """
    if not candles or len(candles) < 20:
        logger.warning(f"[시장지수] {index_name}: 데이터 부족 ({len(candles) if candles else 0}일)")
        return {
            "close": 0, "change_pct": 0, "ma5": 0, "ma20": 0,
            "ma5_below_ma20": False, "consecutive_declines": 0,
            "daily_drop_severe": False, "block_conditions_met": 0,
            "trend": "판단불가",
        }

    close = candles[-1]["close"]
    ma5 = round(_calc_ma(candles, 5), 2)
    ma20 = round(_calc_ma(candles, 20), 2)
    change_pct = _calc_change_pct(candles)
    consecutive = _count_consecutive_declines(candles)

    # 3가지 매수 금지 조건 체크
    cond1_ma_cross = ma5 < ma20
    cond2_consecutive = consecutive >= BLOCK_THRESHOLDS["consecutive_decline_days"]
    cond3_daily_drop = change_pct <= BLOCK_THRESHOLDS["daily_drop_pct"]

    conditions_met = sum([cond1_ma_cross, cond2_consecutive, cond3_daily_drop])

    # 추세 판정
    if ma5 > ma20 and change_pct > 0:
        trend = "상승"
    elif ma5 < ma20 and consecutive >= 2:
        trend = "하락"
    else:
        trend = "횡보"

    return {
        "close": close,
        "change_pct": change_pct,
        "ma5": ma5,
        "ma20": ma20,
        "ma5_below_ma20": cond1_ma_cross,
        "consecutive_declines": consecutive,
        "daily_drop_severe": cond3_daily_drop,
        "block_conditions_met": conditions_met,
        "trend": trend,
    }


def get_market_status() -> Dict:
    """
    ★ KOSPI + KOSDAQ 종합 시장 상태 판정
    Overall market status evaluation combining KOSPI and KOSDAQ.

    Returns:
        {
            "can_buy": True/False,
            "reason": "정상" 또는 차단 사유,
            "kospi": { ... evaluate_index 결과 ... },
            "kosdaq": { ... evaluate_index 결과 ... },
            "checked_at": "2026-02-28T18:35:00"
        }
    """
    kospi_candles = get_index_candles("KOSPI", count=30)
    kosdaq_candles = get_index_candles("KOSDAQ", count=30)

    kospi_eval = evaluate_index(kospi_candles, "KOSPI")
    kosdaq_eval = evaluate_index(kosdaq_candles, "KOSDAQ")

    # 매수 허용 판정: 어느 한쪽이라도 2개 이상 조건 충족 시 차단
    threshold = BLOCK_THRESHOLDS["conditions_to_block"]
    kospi_blocked = kospi_eval["block_conditions_met"] >= threshold
    kosdaq_blocked = kosdaq_eval["block_conditions_met"] >= threshold

    can_buy = True
    reasons = []

    if kospi_blocked:
        can_buy = False
        reasons.append(f"KOSPI 위험 (조건 {kospi_eval['block_conditions_met']}개 충족: "
                       f"MA5<MA20={kospi_eval['ma5_below_ma20']}, "
                       f"연속하락={kospi_eval['consecutive_declines']}일, "
                       f"등락={kospi_eval['change_pct']}%)")

    if kosdaq_blocked:
        can_buy = False
        reasons.append(f"KOSDAQ 위험 (조건 {kosdaq_eval['block_conditions_met']}개 충족: "
                       f"MA5<MA20={kosdaq_eval['ma5_below_ma20']}, "
                       f"연속하락={kosdaq_eval['consecutive_declines']}일, "
                       f"등락={kosdaq_eval['change_pct']}%)")

    reason = " / ".join(reasons) if reasons else "정상"

    result = {
        "can_buy": can_buy,
        "reason": reason,
        "kospi": kospi_eval,
        "kosdaq": kosdaq_eval,
        "checked_at": datetime.now().isoformat(),
    }

    status_emoji = "🟢" if can_buy else "🔴"
    logger.info(f"[시장상태] {status_emoji} can_buy={can_buy} | "
               f"KOSPI {kospi_eval['trend']}({kospi_eval['change_pct']}%) | "
               f"KOSDAQ {kosdaq_eval['trend']}({kosdaq_eval['change_pct']}%)")

    return result


def save_market_status_to_db(status: Dict = None) -> bool:
    """
    시장 상태를 DB에 저장합니다 (매일 1회, 야간 스캔 시)
    Save market status to database (once daily, during night scan).

    Args:
        status: get_market_status() 결과. None이면 새로 조회.
    """
    try:
        from app.core.database import db
        from datetime import date

        if status is None:
            status = get_market_status()

        kospi = status.get("kospi", {})
        kosdaq = status.get("kosdaq", {})
        today = date.today().isoformat()

        db.table("market_status").upsert({
            "date": today,
            "kospi_close": kospi.get("close", 0),
            "kospi_change_pct": kospi.get("change_pct", 0),
            "kospi_ma5": kospi.get("ma5", 0),
            "kospi_ma20": kospi.get("ma20", 0),
            "kosdaq_close": kosdaq.get("close", 0),
            "kosdaq_change_pct": kosdaq.get("change_pct", 0),
            "kosdaq_ma5": kosdaq.get("ma5", 0),
            "kosdaq_ma20": kosdaq.get("ma20", 0),
            "can_buy": status.get("can_buy", True),
            "block_reason": status.get("reason", ""),
        }, on_conflict="date").execute()

        logger.info(f"[시장상태] DB 저장 완료: {today}, can_buy={status['can_buy']}")
        return True

    except Exception as e:
        logger.error(f"[시장상태] DB 저장 실패: {e}")
        return False


def get_cached_market_status() -> Dict:
    """
    ★ DB에서 오늘의 시장 상태를 조회합니다 (장중 매매 시 사용).
    Fetch today's cached market status from DB (used during trading hours).
    API 호출 없이 DB에서만 읽으므로 빠릅니다.

    Returns:
        {"can_buy": True/False, "reason": "..."} 또는 없으면 API 직접 조회
    """
    try:
        from app.core.database import db
        from datetime import date

        today = date.today().isoformat()
        resp = db.table("market_status") \
            .select("can_buy, block_reason") \
            .eq("date", today) \
            .execute()

        if resp.data:
            row = resp.data[0]
            return {
                "can_buy": row["can_buy"],
                "reason": row.get("block_reason", ""),
                "source": "cache",
            }

        # DB에 오늘 데이터 없으면 실시간 조회 후 저장
        logger.info("[시장상태] 캐시 없음 → 실시간 조회")
        status = get_market_status()
        save_market_status_to_db(status)
        return {
            "can_buy": status["can_buy"],
            "reason": status["reason"],
            "source": "live",
        }

    except Exception as e:
        logger.error(f"[시장상태] 캐시 조회 실패: {e}")
        # 실패 시 안전하게 매수 허용 (보수적으로 차단하려면 False)
        return {"can_buy": True, "reason": "조회실패-기본허용", "source": "fallback"}
