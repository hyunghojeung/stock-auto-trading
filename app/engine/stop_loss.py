"""손절 로직 (3단계 안전장치)"""
from app.utils.indicators import sma, vwap, atr

def check_stop_loss(buy_price, current_price, candles_5m, atr_value, vwap_value, absolute_pct=-3.0):
    """손절 조건 확인"""
    closes = [c["close"] for c in candles_5m]

    # 1차: VWAP 이탈
    if vwap_value and atr_value:
        vwap_stop = vwap_value - (atr_value * 0.5)
        if current_price < vwap_stop:
            return {"should_stop": True, "reason": "VWAP이탈",
                    "stop_price": vwap_stop, "loss_pct": round((current_price-buy_price)/buy_price*100, 2)}

    # 2차: 5분봉 20MA 2봉 연속 이탈
    ma20 = sma(closes, 20)
    if len(ma20) >= 2 and ma20[-1] and ma20[-2]:
        if closes[-1] < ma20[-1] and closes[-2] < ma20[-2]:
            return {"should_stop": True, "reason": "20MA이탈(2봉연속)",
                    "stop_price": ma20[-1], "loss_pct": round((current_price-buy_price)/buy_price*100, 2)}

    # 3차: 절대 손절 (-3%)
    loss_pct = (current_price - buy_price) / buy_price * 100
    if loss_pct <= absolute_pct:
        return {"should_stop": True, "reason": f"절대손절({absolute_pct}%)",
                "stop_price": buy_price * (1 + absolute_pct/100),
                "loss_pct": round(loss_pct, 2)}

    return {"should_stop": False, "reason": None, "loss_pct": round(loss_pct, 2)}
