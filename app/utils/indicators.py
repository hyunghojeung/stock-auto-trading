"""기술적 지표 (RSI, MACD, ATR, VWAP, MA)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
★ v4: DTW 강화를 위한 함수 추가
  - z_normalize(): Z-Score 정규화 (평균0, 표준편차1)
  - ma_distance_ratio(): MA20 이격도 계산

파일경로: app/utils/indicators.py
"""

def sma(prices, period):
    result = [None]*len(prices)
    for i in range(period-1, len(prices)):
        result[i] = sum(prices[i-period+1:i+1]) / period
    return result

def ema(prices, period):
    result = [None]*len(prices)
    if len(prices) < period: return result
    result[period-1] = sum(prices[:period]) / period
    m = 2/(period+1)
    for i in range(period, len(prices)):
        result[i] = (prices[i]-result[i-1])*m + result[i-1]
    return result

def rsi(prices, period=14):
    result = [None]*len(prices)
    if len(prices) < period+1: return result
    gains, losses = [], []
    for i in range(1, len(prices)):
        c = prices[i]-prices[i-1]
        gains.append(max(0,c)); losses.append(max(0,-c))
    ag = sum(gains[:period])/period
    al = sum(losses[:period])/period
    result[period] = 100-(100/(1+ag/al)) if al > 0 else 100
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
        result[i+1] = 100-(100/(1+ag/al)) if al > 0 else 100
    return result

def atr(highs, lows, closes, period=14):
    result = [None]*len(highs)
    if len(highs) < 2: return result
    trs = [highs[0]-lows[0]]
    for i in range(1, len(highs)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < period: return result
    result[period-1] = sum(trs[:period])/period
    for i in range(period, len(trs)):
        result[i] = (result[i-1]*(period-1)+trs[i])/period
    return result

def vwap(highs, lows, closes, volumes):
    result = [None]*len(closes)
    ctv, cv = 0.0, 0
    for i in range(len(closes)):
        tp = (highs[i]+lows[i]+closes[i])/3
        ctv += tp*volumes[i]; cv += volumes[i]
        if cv > 0: result[i] = ctv/cv
    return result

def macd(prices, fast=12, slow=26, signal=9):
    ef = ema(prices, fast); es = ema(prices, slow)
    ml = [None]*len(prices)
    for i in range(len(prices)):
        if ef[i] is not None and es[i] is not None: ml[i] = ef[i]-es[i]
    vals = [v for v in ml if v is not None]
    sl = ema(vals, signal) if len(vals) >= signal else []
    sig = [None]*len(prices); hist = [None]*len(prices)
    si = next((i for i,v in enumerate(ml) if v is not None), len(prices))
    for i,v in enumerate(sl):
        idx = si+i
        if idx < len(prices) and v is not None:
            sig[idx] = v
            if ml[idx] is not None: hist[idx] = ml[idx]-v
    return {"macd": ml, "signal": sig, "histogram": hist}

def volume_ratio(volumes, period=20):
    result = [None]*len(volumes)
    for i in range(period, len(volumes)):
        avg = sum(volumes[i-period:i])/period
        if avg > 0: result[i] = volumes[i]/avg
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ v4: DTW 강화용 추가 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def z_normalize(series):
    """
    Z-Score 정규화: 평균 0, 표준편차 1로 변환
    DTW 비교 전 스케일 통일에 사용

    Args:
        series: 숫자 리스트 [1.2, -0.5, 3.1, ...]

    Returns:
        정규화된 리스트. 표준편차 0이면 모두 0.0 반환.
    """
    if not series or len(series) < 2:
        return series if series else []

    n = len(series)
    mean = sum(series) / n
    variance = sum((x - mean) ** 2 for x in series) / n
    std = variance ** 0.5

    if std < 1e-10:
        return [0.0] * n

    return [round((x - mean) / std, 6) for x in series]


def ma_distance_ratio(closes, period=20):
    """
    MA 이격도: (종가 - MA) / MA × 100
    종가가 이동평균선에서 얼마나 벗어났는지 비율(%)

    Args:
        closes: 종가 리스트
        period: 이동평균 기간 (기본 20)

    Returns:
        이격도 리스트 (앞부분은 None)
        양수 = MA 위, 음수 = MA 아래
    """
    result = [None] * len(closes)
    ma = sma(closes, period)

    for i in range(len(closes)):
        if ma[i] is not None and ma[i] > 0:
            result[i] = round((closes[i] - ma[i]) / ma[i] * 100, 4)

    return result


def rsi_series(prices, period=14):
    """
    RSI 시계열 반환 (0~100 스케일을 0~1 스케일로 정규화)
    DTW 비교에 사용하기 위해 0~1 범위로 변환

    Args:
        prices: 종가 리스트
        period: RSI 기간

    Returns:
        RSI/100 리스트 (앞부분은 None, 유효값은 0.0~1.0)
    """
    raw = rsi(prices, period)
    return [round(v / 100, 4) if v is not None else None for v in raw]
