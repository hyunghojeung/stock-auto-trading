"""
전종목 패턴 벡터 사전 수집 서비스
Stock Pattern Vector Pre-collector Service
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/services/stock_pattern_collector.py

매일 18:00 장 마감 후 실행:
  → stock_list에서 일반 주식 ~2,500개 로드
  → 각 종목의 최근 50일 캔들을 네이버에서 수집
  → 30일 등락률 + 거래량비율 벡터 계산
  → stock_patterns 테이블에 upsert
"""

import time
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from app.core.database import db
from app.services.naver_stock import get_daily_candles_naver

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETF/ETN/스팩/우선주 필터 (일반 주식만 통과)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ETF_KEYWORDS = [
    "KODEX", "TIGER", "RISE", "SOL", "ACE", "HANARO", "KBSTAR",
    "ARIRANG", "KOSEF", "PLUS", "BNK", "TREX", "WOORI", "파워",
    "마이티", "마이다스", "히어로", "에셋플러스",
]

EXCLUDE_KEYWORDS = [
    "ETN", "스팩", "리츠", "인프라", "선물", "인버스", "레버리지",
    "채권", "국고", "통안", "CD금리", "머니마켓", "단기자금",
]


def is_regular_stock(stock: Dict) -> bool:
    """일반 주식 여부 판별 / Filter: regular stocks only"""
    name = stock.get("name", "")
    code = stock.get("code", "")

    for kw in ETF_KEYWORDS:
        if kw in name:
            return False

    for kw in EXCLUDE_KEYWORDS:
        if kw in name:
            return False

    # 우선주 필터 (종목코드 끝자리 5,7,8,9)
    if code and len(code) == 6 and code[-1] in ("5", "7", "8", "9"):
        return False

    return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 벡터 계산
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compute_vectors(candles: List[Dict], vector_days: int = 30) -> Tuple[List[float], List[float]]:
    """
    캔들 데이터에서 등락률 + 거래량비율 벡터 계산
    Compute return flow + volume ratio vectors

    Args:
        candles: 날짜 오름차순 정렬된 캔들 리스트
        vector_days: 벡터 길이 (기본 30일)
    Returns:
        (returns_vector, volumes_vector)
    """
    if len(candles) < vector_days + 20:
        return [], []

    recent = candles[-vector_days:]

    # 등락률 벡터
    returns = []
    for k in range(len(recent)):
        if k == 0:
            idx_in_full = len(candles) - vector_days
            prev_close = candles[idx_in_full - 1].get("close", 0) if idx_in_full > 0 else recent[0].get("open", 0)
        else:
            prev_close = recent[k - 1].get("close", 0)

        close = recent[k].get("close", 0)
        ret = ((close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
        returns.append(round(ret, 4))

    # 거래량비율 벡터 (20일 평균 대비)
    volumes = []
    for k in range(len(recent)):
        abs_idx = len(candles) - vector_days + k
        vol_start = max(0, abs_idx - 20)
        vol_slice = candles[vol_start:abs_idx]
        avg_vol = sum(c.get("volume", 0) for c in vol_slice) / len(vol_slice) if vol_slice else 1
        vol = recent[k].get("volume", 0)
        ratio = round(vol / avg_vol, 4) if avg_vol > 0 else 1.0
        volumes.append(ratio)

    return returns, volumes


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인 수집 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_pattern_collection(vector_days: int = 30):
    """
    전종목 패턴 벡터 수집 메인 함수
    Main collection function — called by scheduler at 18:00 daily

    Flow:
      1. stock_list에서 일반 주식만 로드
      2. 각 종목 네이버 캔들 수집 (50일)
      3. 30일 벡터 계산
      4. stock_patterns에 upsert
    """
    start_time = time.time()
    logger.info("[패턴수집] ━━━ 전종목 패턴 벡터 수집 시작 ━━━")

    # ── 1. stock_list 로드 ──
    try:
        resp = db.table("stock_list").select("code, name, market").execute()
        all_stocks = resp.data or []
    except Exception as e:
        logger.error(f"[패턴수집] stock_list 조회 실패: {e}")
        return {"error": str(e)}

    # 일반 주식만 필터링
    stocks = [s for s in all_stocks if is_regular_stock(s)]
    logger.info(f"[패턴수집] 전체 {len(all_stocks)}개 → 일반주식 {len(stocks)}개")

    # ── 2~3. 캔들 수집 + 벡터 계산 ──
    fetch_days = vector_days + 25  # 벡터 30일 + MA20 여유
    success_count = 0
    fail_count = 0
    skip_count = 0
    batch = []
    batch_size = 50  # 50개씩 일괄 upsert

    for idx, stock in enumerate(stocks):
        code = stock["code"]
        name = stock.get("name", code)
        market = stock.get("market", "")

        try:
            candles = get_daily_candles_naver(code, count=fetch_days)

            if not candles or len(candles) < vector_days + 20:
                skip_count += 1
                continue

            # 벡터 계산
            returns_vec, volumes_vec = compute_vectors(candles, vector_days)

            if not returns_vec:
                skip_count += 1
                continue

            last_candle = candles[-1]
            batch.append({
                "code": code,
                "name": name,
                "market": market,
                "returns_30d": json.dumps(returns_vec),
                "volumes_30d": json.dumps(volumes_vec),
                "last_close": last_candle.get("close", 0),
                "last_date": str(last_candle.get("date", "")),
                "candle_count": len(candles),
                "updated_at": datetime.now().isoformat(),
            })
            success_count += 1

        except Exception as e:
            fail_count += 1
            if fail_count <= 5:
                logger.warning(f"[패턴수집] {code}({name}) 실패: {e}")

        # 배치 upsert
        if len(batch) >= batch_size:
            _upsert_batch(batch)
            batch = []

        # API 부하 방지 (네이버 차단 방지)
        time.sleep(0.15)

        # 진행률 로그 (200개마다)
        if (idx + 1) % 200 == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[패턴수집] 진행: {idx+1}/{len(stocks)} "
                f"(성공:{success_count} 실패:{fail_count} 스킵:{skip_count}) "
                f"경과:{elapsed:.0f}초"
            )

    # 남은 배치 처리
    if batch:
        _upsert_batch(batch)

    elapsed = time.time() - start_time
    result = {
        "total": len(stocks),
        "success": success_count,
        "fail": fail_count,
        "skip": skip_count,
        "elapsed_sec": round(elapsed, 1),
    }

    logger.info(
        f"[패턴수집] ━━━ 완료 ━━━ "
        f"총 {len(stocks)}개 중 성공:{success_count} 실패:{fail_count} 스킵:{skip_count} "
        f"소요:{elapsed:.0f}초"
    )

    return result


def _upsert_batch(batch: List[Dict]):
    """배치 upsert — stock_patterns 테이블에 저장"""
    try:
        db.table("stock_patterns").upsert(
            batch,
            on_conflict="code",
        ).execute()
    except Exception as e:
        logger.error(f"[패턴수집] upsert 실패 ({len(batch)}건): {e}")
        # 개별 insert 시도 (fallback)
        for row in batch:
            try:
                db.table("stock_patterns").upsert(
                    row,
                    on_conflict="code",
                ).execute()
            except Exception:
                pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 직접 실행용 (테스트)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_pattern_collection()
    print(f"수집 결과: {result}")
