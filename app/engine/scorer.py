"""점수제 종목 선별 (100점 만점) / Score-based Stock Selection
개선사항:
- 절대 거래량 → 회전율(상대 거래량) 기반 점수
- 눌림목 후보 가점 (소폭 하락 + 고거래량)
- 가격대/시가총액 필터링
- 야간스캔 시 scan_date를 다음 거래일로 저장
"""
from datetime import datetime, date, timedelta
from app.core.database import db
from app.core.config import KST


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
               "current_price": s.get("price", 0),
"change_pct": s.get("change_pct", 0),
"status": "감시중",
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
"current_price": s.get("price", 0),
"change_pct": s.get("change_pct", 0),
"status": "감시중",
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

    now = datetime.now(KST)

    if now.hour >= 16:
        # 야간: 다음 거래일
        check = now.date() + timedelta(days=1)
        for _ in range(10):
            if is_market_open_day(check):
                return check.isoformat()
            check += timedelta(days=1)

    return now.date().isoformat()


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
    """거래량 활성도 점수 (25점) — 회전율(거래대금/시총) 기반"""
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    if market_cap <= 0 or price <= 0:
        return 0

    trade_value = volume * price
    turnover = trade_value / market_cap * 100

    if turnover > 5.0:
        return 25
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
    """눌림목 후보 점수 (30점) — 핵심 점수!"""
    change = stock.get("change_pct", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    score = 0

    if -5.0 <= change <= -1.0:
        score = 22
        if market_cap > 0:
            turnover = (volume * price) / market_cap * 100
            if turnover > 1.0:
                score = 30
            elif turnover > 0.5:
                score = 26
    elif -1.0 < change <= 0:
        score = 12
    elif 0 < change <= 2.0:
        score = 15
    elif 2.0 < change <= 5.0:
        score = 10
    elif change > 5.0:
        score = 5
    elif change < -5.0:
        score = 3

    return score


def _theme_score(stock):
    """거래대금 활성도 점수 (15점)"""
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    trade_value = volume * price

    if trade_value > 100_000_000_000:
        return 15
    elif trade_value > 50_000_000_000:
        return 12
    elif trade_value > 10_000_000_000:
        return 9
    elif trade_value > 5_000_000_000:
        return 6
    elif trade_value > 1_000_000_000:
        return 3
    return 1


def _technical_score(stock):
    """기술적 복합 점수 (20점)"""
    change = stock.get("change_pct", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)
    market_cap = stock.get("market_cap", 0)

    score = 0

    if volume > 500000:
        score += 5
    elif volume > 200000:
        score += 3

    if -3.0 <= change <= 3.0:
        score += 5
    elif -5.0 <= change <= 5.0:
        score += 2

    if 5000 <= price <= 100000:
        score += 5
    elif 3000 <= price <= 200000:
        score += 3

    if 100_000_000_000 <= market_cap <= 5_000_000_000_000:
        score += 5
    elif 50_000_000_000 <= market_cap <= 10_000_000_000_000:
        score += 3

    return min(score, 20)


def _supply_score(stock):
    """시가총액/유동성 점수 (10점)"""
    market_cap = stock.get("market_cap", 0)
    volume = stock.get("volume", 0)
    price = stock.get("price", 0)

    score = 0

    if market_cap > 1_000_000_000_000:
        score += 5
    elif market_cap > 500_000_000_000:
        score += 4
    elif market_cap > 100_000_000_000:
        score += 3
    else:
        score += 1

    trade_value = volume * price
    if trade_value > 10_000_000_000:
        score += 5
    elif trade_value > 5_000_000_000:
        score += 4
    elif trade_value > 1_000_000_000:
        score += 3
    else:
        score += 1

    return min(score, 10)
