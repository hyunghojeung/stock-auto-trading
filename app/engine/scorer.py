"""점수제 종목 선별 (100점 만점) / Score-based Stock Selection
개선사항:
- 절대 거래량 → 회전율(상대 거래량) 기반 점수
- 눌림목 후보 가점 (소폭 하락 + 고거래량)
- 가격대/시가총액 필터링
- 야간스캔 시 scan_date를 다음 거래일로 저장
"""
from datetime import datetime, date, timedelta
from app.core.database import db


async def score_and_select(stocks, top_n=10):
    """전종목 점수 계산 후 상위 N개 선별 / Score all stocks and select top N"""
    if not stocks:
        print("[선별] 입력 종목 0개 — 스캔 결과를 확인하세요")
        return []

    scored = []
    filtered_count = 0

    for stock in stocks:
        # 사전 필터: 투자 부적합 종목 제거
        if not _passes_filter(stock):
            filtered_count += 1
            continue

        score = calculate_score(stock)
        if score > 30:
            stock["score"] = score
            scored.append(stock)

    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[:top_n]

    print(f"[선별] 전체 {len(stocks)}개 → 필터 제외 {filtered_count}개 → "
          f"30점 이상 {len(scored)}개 → 상위 {len(top)}개 선별")

    # DB 저장 — 야간(18시 이후)이면 다음 거래일로 저장
    scan_date = _get_scan_date()
    for s in top:
        try:
            db.table("watchlist").upsert({
                "stock_code": s["code"],
                "stock_name": s["name"],
                "score": s["score"],
                "volume_score": s.get("volume_score", 0),
                "trend_score": s.get("trend_score", 0),
                "theme_score": s.get("theme_score", 0),
                "technical_score": s.get("technical_score", 0),
                "supply_score": s.get("supply_score", 0),
                "status": "후보",
                "scan_date": scan_date,
            }, on_conflict="stock_code,scan_date").execute()
        except Exception as e:
            # upsert 실패 시 insert 시도
            try:
                db.table("watchlist").insert({
                    "stock_code": s["code"],
                    "stock_name": s["name"],
                    "score": s["score"],
                    "volume_score": s.get("volume_score", 0),
                    "trend_score": s.get("trend_score", 0),
                    "theme_score": s.get("theme_score", 0),
                    "technical_score": s.get("technical_score", 0),
                    "supply_score": s.get("supply_score", 0),
                    "status": "후보",
                    "scan_date": scan_date,
                }).execute()
            except Exception as e2:
                print(f"[DB 저장 오류] {s['name']}: {e2}")

    print(f"[선별] 상위 {len(top)}개 종목 DB 저장 완료 (scan_date: {scan_date})")
    return top


def _get_scan_date():
    """스캔 날짜 결정 / Determine scan date for DB storage
    야간스캔(16시 이후): 다음 거래일로 저장
    장중스캔(09~16시): 오늘 날짜로 저장
    """
    from app.utils.kr_holiday import is_market_open_day

    now = datetime.now()

    if now.hour >= 16:
        # 야간: 다음 거래일
        check = now.date() + timedelta(days=1)
        for _ in range(10):
            if is_market_open_day(check):
                return check.isoformat()
            check += timedelta(days=1)

    return date.today().isoformat()


def _passes_filter(stock):
    """사전 필터: 투자 부적합 종목 제거 / Pre-filter unsuitable stocks"""
    price = stock.get("price", 0)
    volume = stock.get("volume", 0)
    market_cap = stock.get("market_cap", 0)
    name = stock.get("name", "")

    # 가격 필터: 1,000원 미만 (동전주) 또는 50만원 초과 (고가주) 제외
    if price < 1000 or price > 500000:
        return False

    # 거래량 필터: 최소 5만주 이상
    if volume < 50000:
        return False

    # 시가총액 필터: 500억 미만 제외 (극소형주 리스크)
    if market_cap < 50000000000:
        return False

    # 이름 필터: 우선주, 스팩 등 제외
    exclude_keywords = ["우B", "우C", "스팩", "SPAC", "리츠", "인프라"]
    for kw in exclude_keywords:
        if kw in name:
            return False

    return True


def calculate_score(stock):
    """개별 종목 점수 계산 (100점 만점) / Calculate individual stock score"""
    total = 0

    # 1. 거래량 활성도 점수 (25점) — 회전율 기반
    vol_score = _volume_score(stock)
    stock["volume_score"] = vol_score
    total += vol_score

    # 2. 눌림목 후보 점수 (30점) — 핵심!
    trend_score = _trend_score(stock)
    stock["trend_score"] = trend_score
    total += trend_score

    # 3. 거래대금 활성도 점수 (15점)
    theme_score = _theme_score(stock)
    stock["theme_score"] = theme_score
    total += theme_score

    # 4. 기술적 신호 점수 (20점) — 변동률 + 거래량 복합
    tech_score = _technical_score(stock)
    stock["technical_score"] = tech_score
    total += tech_score

    # 5. 시가총액/유동성 점수 (10점)
    supply_score = _supply_score(stock)
    stock["supply_score"] = supply_score
    total += supply_score

    return round(total, 2)


def _volume_score(stock):
    """거래량 활성도 점수 (25점) — 회전율(거래대금/시총) 기반
    회전율이 높을수록 시장 관심이 높은 종목
    """
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    if market_cap <= 0 or price <= 0:
        return 0

    # 회전율 = 거래대금 / 시가총액 (%)
    trade_value = volume * price
    turnover = trade_value / market_cap * 100

    # 회전율 기반 점수
    if turnover > 5.0:
        return 25  # 매우 활발 (시총 5% 이상 거래)
    elif turnover > 2.0:
        return 20
    elif turnover > 1.0:
        return 16
    elif turnover > 0.5:
        return 12
    elif turnover > 0.2:
        return 8
    elif turnover > 0.1:
        return 5
    return 2


def _trend_score(stock):
    """눌림목 후보 점수 (30점) — 핵심 점수!
    눌림목 전략에 적합한 종목: 소폭 하락(-1%~-5%) + 높은 거래량
    급등 종목보다 '적절히 눌린' 종목에 높은 점수
    """
    change = stock.get("change_pct", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    score = 0

    # 핵심: 소폭 하락 + 거래량 동반 = 눌림목 후보
    if -5.0 <= change <= -1.0:
        # 눌림목 최적 구간!
        score = 22
        # 거래량 동반 시 가점
        if market_cap > 0:
            turnover = (volume * price) / market_cap * 100
            if turnover > 1.0:
                score = 30  # 거래량 동반 눌림 = 최고점
            elif turnover > 0.5:
                score = 26

    elif -1.0 < change <= 0:
        # 보합~소폭 하락 (관망 구간)
        score = 12

    elif 0 < change <= 2.0:
        # 소폭 상승 (반등 초기 가능)
        score = 15

    elif 2.0 < change <= 5.0:
        # 중폭 상승 (추세 확인 필요)
        score = 10

    elif change > 5.0:
        # 급등 (추격 매수 위험)
        score = 5

    elif change < -5.0:
        # 급락 (낙폭 과대, 위험)
        score = 3

    return score


def _theme_score(stock):
    """거래대금 활성도 점수 (15점) / Trading value activity score"""
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    trade_value = volume * price  # 거래대금

    # 거래대금 기준 (원)
    if trade_value > 100_000_000_000:    # 1000억 이상
        return 15
    elif trade_value > 50_000_000_000:   # 500억
        return 12
    elif trade_value > 10_000_000_000:   # 100억
        return 9
    elif trade_value > 5_000_000_000:    # 50억
        return 6
    elif trade_value > 1_000_000_000:    # 10억
        return 3
    return 1


def _technical_score(stock):
    """기술적 복합 점수 (20점) — 변동률 + 거래량 + 가격대 복합 판단
    단순 change_pct > 0 대신, 복합 조건으로 판단
    """
    change = stock.get("change_pct", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    score = 0

    # 조건 1: 거래량 동반 여부 (기본 +5)
    if volume > 500000:
        score += 5
    elif volume > 200000:
        score += 3

    # 조건 2: 적정 변동 범위 (-3% ~ +3%) 내이면 안정적 (+5)
    if -3.0 <= change <= 3.0:
        score += 5
    elif -5.0 <= change <= 5.0:
        score += 2

    # 조건 3: 적정 가격대 (5,000 ~ 100,000원) 매매 활발 구간 (+5)
    if 5000 <= price <= 100000:
        score += 5
    elif 3000 <= price <= 200000:
        score += 3

    # 조건 4: 시가총액 적정 규모 (1000억~5조) (+5)
    if 100_000_000_000 <= market_cap <= 5_000_000_000_000:
        score += 5
    elif 50_000_000_000 <= market_cap <= 10_000_000_000_000:
        score += 3

    return min(score, 20)  # 최대 20점


def _supply_score(stock):
    """시가총액/유동성 점수 (10점) / Market cap & liquidity score"""
    market_cap = stock.get("market_cap", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)

    score = 0

    # 시가총액 규모 (안정성)
    if market_cap > 1_000_000_000_000:    # 1조 이상
        score += 5
    elif market_cap > 500_000_000_000:    # 5000억
        score += 4
    elif market_cap > 100_000_000_000:    # 1000억
        score += 3
    else:
        score += 1

    # 유동성 (일평균 거래대금)
    trade_value = volume * price
    if trade_value > 10_000_000_000:     # 100억 이상
        score += 5
    elif trade_value > 5_000_000_000:    # 50억
        score += 4
    elif trade_value > 1_000_000_000:    # 10억
        score += 3
    else:
        score += 1

    return min(score, 10)  # 최대 10점
