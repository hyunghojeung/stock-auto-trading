"""기술적 지표 (RSI, MACD, ATR, VWAP, MA)"""

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
