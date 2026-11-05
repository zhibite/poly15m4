"""liquidity.py — Polymarket order-book liquidity helpers.

Extracted from the removed end-window strategy so the main strategy and the
factor model can keep using the same OBI / depth confidence calculation.
"""

from __future__ import annotations


# OBI → confidence mapping (the orderbook passed here is the orderbook of the
# token being traded, so positive OBI is bullish for that token).
OBI_BULL_THRESH = 0.10
OBI_BEAR_THRESH = -0.10


def _obi_to_confidence(obi: float, depth_factor: float) -> float:
    """Combine OBI and depth into a 0..100 confidence score."""
    depth_multiplier = 0.5 + 0.5 * min(depth_factor, 1.0)
    base = 50.0 + 50.0 * max(-1.0, min(1.0, obi))
    return max(0.0, min(100.0, base * depth_multiplier))


def compute_liquidity_confidence(
    orderbook: dict | None,
    direction: str,
    *,
    min_depth_tokens: float = 10.0,
    min_levels: int = 3,
) -> tuple[float, float, float, bool]:
    """
    Compute (D5, obi, depth_factor, depth_ok) from an orderbook.

    orderbook must be the book of the token being traded:
      {"bids": [{"price": "0.46", "size": "150"}, ...],
       "asks": [{"price": "0.47", "size": "120"}, ...]}

    direction is kept for call-site compatibility; OBI is already interpreted
    relative to the traded token.
    """
    if orderbook is None:
        return 50.0, 0.0, 0.0, False

    bids_raw = orderbook.get("bids") or []
    asks_raw = orderbook.get("asks") or []

    def parse_levels(levels: list) -> list[tuple[float, float]]:
        result = []
        for lv in levels:
            price = float(lv.get("price", 0))
            size = float(lv.get("size", 0))
            if price > 0 and size >= min_depth_tokens:
                result.append((price, size))
        return result

    bids = parse_levels(bids_raw)
    asks = parse_levels(asks_raw)

    if len(bids) < min_levels or len(asks) < min_levels:
        return 50.0, 0.0, 0.0, False

    bid_vol = sum(s for _, s in bids)
    ask_vol = sum(s for _, s in asks)
    total_vol = bid_vol + ask_vol

    if total_vol < 1e-9:
        return 50.0, 0.0, 0.0, False

    obi = (bid_vol - ask_vol) / total_vol

    min_expected = min_depth_tokens * min_levels * 2
    depth_factor = total_vol / min_expected if min_expected > 0 else 1.0
    depth_ok = depth_factor >= 1.0

    if obi < OBI_BEAR_THRESH:
        depth_ok = False

    D5 = _obi_to_confidence(obi, depth_factor)

    return D5, obi, depth_factor, depth_ok
