"""Conservative, closed-candle 1m entry confirmation for scanner signals."""
from __future__ import annotations


def confirm_one_minute_entry(rows: list[list], reference_price: float) -> tuple[bool, str, dict[str, float]]:
    """Evaluate the last *closed* 1m candle; the final Binance row is still forming.

    Requires a bullish candle, a non-weak close, no excessive upper wick, and price
    holding near the 5m signal reference. Returns (accepted, reason, metrics).
    """
    if not rows or len(rows) < 3:
        return False, "INSUFFICIENT_1M_CANDLES", {}
    if reference_price <= 0:
        return False, "INVALID_REFERENCE_PRICE", {}

    # Binance includes the currently-forming candle as the last row.
    previous, candle = rows[-3], rows[-2]
    try:
        open_price, high, low, close = (float(candle[i]) for i in (1, 2, 3, 4))
        previous_close = float(previous[4])
    except (TypeError, ValueError, IndexError):
        return False, "INVALID_1M_CANDLE", {}
    if min(open_price, high, low, close, previous_close) <= 0 or high < low:
        return False, "INVALID_1M_CANDLE", {}

    candle_range = high - low
    close_location = (close - low) / candle_range if candle_range else 0.5
    upper_wick_ratio = (high - max(open_price, close)) / candle_range if candle_range else 0.0
    pullback_pct = (close / reference_price - 1.0) * 100.0
    metrics = {
        "close_location": close_location,
        "upper_wick_ratio": upper_wick_ratio,
        "pullback_pct": pullback_pct,
        "closed_1m_change_pct": (close / previous_close - 1.0) * 100.0,
    }
    if close < open_price:
        return False, "BEARISH_1M_CANDLE", metrics
    if close < previous_close:
        return False, "LOWER_THAN_PREVIOUS_1M_CLOSE", metrics
    if close_location < 0.60:
        return False, "WEAK_1M_CLOSE", metrics
    if upper_wick_ratio > 0.45:
        return False, "LONG_UPPER_WICK_1M", metrics
    if pullback_pct < -0.30:
        return False, "PRICE_LOST_SIGNAL_LEVEL", metrics
    if pullback_pct > 1.20:
        return False, "CHASED_1M_MOVE", metrics
    return True, "CONFIRMED", metrics
