"""Polymarket fee arithmetic.

Crypto-category taker fee (v2):

    fee = shares * fee_rate * price * (1 - price)

``fee_rate`` defaults to 0.07 for the crypto Up/Down markets; makers pay 0.
The formula is largest at price 0.5, so a 50c entry/exit round trip is much
more expensive than a 30 bps flat-rate estimate would suggest.
"""

from __future__ import annotations


DEFAULT_TAKER_FEE_RATE = 0.07


def taker_fee(shares: float, price: float, fee_rate: float = DEFAULT_TAKER_FEE_RATE) -> float:
    """Fee in USDC for ``shares`` executed at ``price``."""
    s = max(0.0, float(shares or 0.0))
    p = min(1.0, max(0.0, float(price or 0.0)))
    return s * max(0.0, float(fee_rate or 0.0)) * p * (1.0 - p)
