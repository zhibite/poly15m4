"""
live_trader.py — Polymarket CLOB v2 trading interface, powered by py_clob_client_v2.

Key design (v2):
  1. Uses py_clob_client_v2.ClobClient for all CLOB operations.
     - create_and_post_market_order() + MarketOrderArgs for FOK (buy) / FAK (sell).
     - get_balance_allowance() + BalanceAllowanceParams for balance queries.
     - OrderType.FOK for market buys, OrderType.FAK for market sells.
  2. Decimal amounts throughout — CLOB stores micro-units (×1e6).
  3. Signature types: 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE.
  4. Collateral: pUSD (wrapped from USDC.e via Collateral Onramp).
  5. Fee: protocol-determined at match time; feeRateBps removed from orders.
  6. On-chain redemption via CTF / NegRiskAdapter on Polygon.
"""

import asyncio
import json as _json
import os
import threading
import warnings
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Optional

# Suppress RuntimeWarnings raised by py_clob_client_v2 internals.
# The SDK may create unawaited coroutines during __init__ or at GC time.
warnings.simplefilter("ignore", RuntimeWarning)

import httpx
from eth_account import Account
from eth_utils import to_checksum_address
from loguru import logger

from py_clob_client_v2 import (
    BalanceAllowanceParams,
    ClobClient,
    MarketOrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)
from py_clob_client_v2.clob_types import OrderArgsV2

from app.config import settings

# ── Contract addresses (Polygon mainnet, V2) ────────────────────────────────────
# Consolidated — shared with polygon_usdc.py
POLYGON_PUSD      = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_ADDRESS       = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_ADAPTER  = "0xe2222d279d744050d28e00520010520000310F59"


def _to_micro(amount: Decimal) -> int:
    return int((amount * Decimal("1_000_000")).quantize(Decimal("1"), rounding=ROUND_DOWN))


def _from_micro(micro: int) -> Decimal:
    return Decimal(micro) / Decimal("1_000_000")


def _q(value: Decimal | float, prec: Decimal) -> float:
    """
    Quantize a value to the given precision and return a clean float.

    Conversion via str() avoids float64 binary representation noise
    (e.g. 0.420000000001) before rounding for the CLOB API.
    """
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    if not isinstance(prec, Decimal):
        prec = Decimal(str(prec))
    return float(value.quantize(prec, rounding=ROUND_DOWN))


def _prec_for_fak_size(tick_size: Decimal) -> Decimal:
    """FAK size precision: the coarser of tick_size and 0.0001."""
    return max(Decimal("0.0001"), tick_size)


@dataclass
class FillResult:
    order_id: str
    tokens: float       # number of tokens bought/sold
    usdc: float         # USDC spent (buy) or received (sell), gross of fees
    fill_price: float   # effective fill price per token
    success: bool
    error: str = ""
    estimated_price: bool = False  # CRITICAL FIX (R1): true when fill price was estimated (no averageFillPrice from CLOB)
    fee_usdc: float = 0.0          # taker/maker fee reported by CLOB fills


class CLOBError(Exception):
    def __init__(self, message: str, *, status_code: int = 0, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _api_creds_complete(api_key: str, api_secret: str, api_passphrase: str) -> bool:
    return bool(api_key and api_secret and api_passphrase)


class LiveTrader:
    def __init__(
        self,
        *,
        host: str,
        private_key: str,
        chain_id: int = 137,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        slippage_pct: float = 2.0,
        signature_type: int = 1,
        signer_address: str = "",
        funder_address: str = "",
        pm_client=None,
    ):
        self._host = host
        self._private_key = private_key
        self._chain_id = chain_id
        self.slippage_pct = slippage_pct
        self._signature_type = signature_type
        self._signer_address = signer_address
        self._funder = funder_address or os.getenv("FUNDER_ADDRESS", "").strip()
        self._pm_client = pm_client
        self._http_lock = threading.Lock()
        self._clob_client: Optional[ClobClient] = None
        self._account_addr: Optional[str] = None
        self._w3 = None
        self._tick_size_cache: dict[str, str] = {}
        # R8 fix: _min_exit_price is updated every time _sync_config runs.
        # Additionally initialise from settings so it is always valid even if _sync_config
        # has not yet been called (e.g. set_live_trader before the first loop tick).
        self._min_exit_price: float = float(settings.min_exit_price)
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._balance_lock: asyncio.Lock = asyncio.Lock()
        # Serialises all on-chain redeem transactions for this wallet so concurrent
        # close events (BTC + ETH settle simultaneously) cannot produce nonce conflicts.
        self._redeem_lock: threading.RLock = threading.RLock()
        self._balance_cache_stale: bool = False
        # Token balance cache: maps token_id → (balance_human, timestamp_sec).
        # TTL of 0.3s keeps balance fresh during 1s polling cycles while
        # still avoiding redundant CLOB API calls from multiple close events.
        self._token_balance_cache: dict[str, tuple[float, float]] = {}
        self._TOKEN_BALANCE_CACHE_TTL: float = 0.3
        self._init_sync(api_key, api_secret, api_passphrase)

    def _init_sync(self, api_key: str, api_secret: str, api_passphrase: str):
        # Capture the current running loop before any async work — _init_sync is
        # called from __init__ (sync context), so there is no running loop here.
        # We must use run_until_complete for all async operations.
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        try:
            self._account_addr = to_checksum_address(
                Account.from_key(self._private_key).address
            )
        except Exception as exc:
            logger.error("Failed to derive EOA address from private key: {}", exc)
            raise

        if not _api_creds_complete(api_key, api_secret, api_passphrase):
            logger.warning("L2 API credentials not provided — LiveTrader is read-only (balance unavailable).")
            self._clob_client = None
            return

        try:
            from py_clob_client_v2 import ApiCreds
            api_creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
        except Exception as exc:
            logger.error("Failed to build ApiCreds: {}", exc)
            self._clob_client = None
            return

        try:
            self._clob_client = ClobClient(
                host=self._host,
                chain_id=self._chain_id,
                key=self._private_key,
                creds=api_creds,
                signature_type=self._signature_type,
                funder=self._funder or self._account_addr,
            )
            logger.info(
                "LiveTrader v2 ready: address={} sig_type={} funder={}",
                LiveTrader._addr_short(self._account_addr), self._signature_type,
                LiveTrader._addr_short(self._funder) if self._funder else "(EOA)",
            )
        except Exception as exc:
            logger.error("Failed to init ClobClient v2: {}", exc)
            self._clob_client = None

        # EOA mode (sig_type=0): pUSD must be approved for the CLOB Exchange V2
        if self._clob_client is not None and self._signature_type == 0:
            self._ensure_pusd_approved()
        # POLY_GNOSIS_SAFE (sig_type=2): pUSD approval is managed via Safe Transaction
        # (multi-sig) which cannot be executed by a plain eth_sendTransaction.
        if self._clob_client is not None and self._signature_type == 2:
            logger.warning(
                "LiveTrader sig_type=2 (POLY_GNOSIS_SAFE): pUSD approval must be performed "
                "via a Safe Transaction before live trading. "
                "If pUSD allowance is insufficient, orders will fail with 'balance' error. "
                "Address: {}",
                LiveTrader._addr_short(self._account_addr),
            )

    def _ensure_client(self):
        if self._clob_client is None:
            raise CLOBError("ClobClient v2 not initialised — check credentials")

    @staticmethod
    def _addr_short(addr: str) -> str:
        if not addr:
            return ""
        a = addr.strip()
        return f"{a[:6]}…{a[-4:]}" if len(a) >= 12 else a

    @staticmethod
    async def _run(fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    @staticmethod
    def _apply_fee_fallback(result: Optional[FillResult], fee_rate_bps: int) -> Optional[FillResult]:
        """Estimate a missing CLOB fee from the token's fee rate.

        Normal fills include an explicit per-fill ``fee``.  Some FOK responses
        report ``matched`` without fills[] (and therefore without a fee), so we
        fall back to ``notional * fee_bps / 10_000`` to avoid recording a
        zero-fee trade.
        """
        if result is None or not result.success:
            return result
        if result.fee_usdc > 0 or fee_rate_bps <= 0:
            return result
        fee = (Decimal(str(result.usdc)) * Decimal(int(fee_rate_bps)) / Decimal(10000)).quantize(
            Decimal("0.000001"), rounding=ROUND_DOWN,
        )
        result.fee_usdc = float(fee)
        return result

    async def _get_tick_size(self, token_id: str) -> str:
        """Return the correct tick_size for a token, cached per token_id."""
        if token_id in self._tick_size_cache:
            return self._tick_size_cache[token_id]
        if self._pm_client is not None:
            try:
                ts = await self._pm_client.get_tick_size(token_id)
                self._tick_size_cache[token_id] = ts
                return ts
            except Exception as exc:
                logger.warning("get_tick_size failed for {}: {}", token_id[:12], exc)
        # Fallback
        self._tick_size_cache[token_id] = "0.0001"
        return "0.0001"

    # ── Market buy (FOK primary, FAK opt-in) ──────────────────────────────────────
    async def buy_token(
        self,
        token_id: str,
        size_usdc: float,
        midpoint: float,
        fee_rate_bps: int = 0,
        *,
        user_usdc_balance: float = 0.0,
        neg_risk: bool = False,
        slippage_pct: Optional[float] = None,
        use_fak: bool = False,
        max_entry_price: Optional[float] = None,
    ) -> Optional[FillResult]:
        """
        BUY via FOK (default) or FAK. Amount = USDC value.

        max_entry_price: if provided, the FAK fill is validated against this price.
        If fill_price > max_entry_price the fill is rejected so callers can retry
        at a better price (e.g. waiting for the market to move lower).
        """
        if self._clob_client is None:
            return FillResult("", 0, 0, 0, False, "ClobClient not initialised")
        if size_usdc < 1.0:
            return FillResult(
                "", 0, 0, midpoint, False,
                f"order amount {size_usdc} below CLOB minimum $1",
            )

        tick_size = await self._get_tick_size(token_id)
        tick_size_d = Decimal(tick_size)

        if midpoint < 0.001 or midpoint > 0.999:
            return FillResult(
                "", 0, 0, midpoint, False,
                f"midpoint {midpoint} out of safe range [0.001, 0.999]",
            )

        # ── Compute raw Decimal values (no quantization until API call) ─────────
        slip = Decimal(str(slippage_pct if slippage_pct is not None else self.slippage_pct))
        slip_dec = slip / Decimal("100")

        # Layer 1: slippage buffer
        slip_buffer = Decimal(str(midpoint)) * slip_dec
        # Layer 2: minimum absolute buffer (3% or 5 ticks, whichever is larger)
        abs_buffer = max(
            Decimal(str(midpoint)) * Decimal("0.03"),
            Decimal(str(tick_size)) * Decimal("5"),
        )
        limit_price_dec = Decimal(str(midpoint)) + max(slip_buffer, abs_buffer)
        limit_price_dec = max(Decimal("0.0001"), min(Decimal("0.9999"), limit_price_dec))

        # Raw token amount (no quantize yet)
        tokens_d = Decimal(str(size_usdc)) / limit_price_dec

        # Fallback price for _parse_fill (reference value, not sent to API)
        limit_price_ref = float(limit_price_dec)
        tokens_ref = float(tokens_d)

        logger.debug(
            "buy_token token={} usdc={} mid={} limit={:.4f} tokens={:.4f} tick={} neg_risk={}",
            token_id[:12], size_usdc, midpoint, limit_price_ref, tokens_ref, tick_size, neg_risk,
        )

        # ── Market order (FAK or FOK) ──────────────────────────────────────────────
        amount_clean = _q(Decimal(str(size_usdc)), Decimal("0.01"))

        async def _run_market(order_type: OrderType) -> dict:
            return await self._run(
                self._clob_client.create_and_post_market_order,
                order_args=MarketOrderArgs(
                    token_id=token_id,
                    amount=amount_clean,
                    side=Side.BUY,
                    order_type=order_type,
                    user_usdc_balance=float(user_usdc_balance) if user_usdc_balance else 0.0,
                ),
                options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
                order_type=order_type,
            )

        try:
            resp = await _run_market(OrderType.FAK if use_fak else OrderType.FOK)
        except Exception as exc:
            exc_str = str(exc)

            # 400 FOK: insufficient liquidity at current price — retry once with FAK.
            # FOK is fill-or-killed; it fails when the order cannot be fully matched
            # immediately (e.g. stale midpoint, spread moved).  Falling back to FAK
            # lets the CLOB match whatever is available at or below the limit price.
            if "status=400" in exc_str and any(
                kw in exc_str.lower() for kw in ("fok", "couldn't be fully filled", "fully filled or killed")
            ):
                logger.warning(
                    "buy_token FOK 400 (no liquidity): {} — retrying with FAK",
                    exc_str[:300],
                )
                try:
                    fak_resp = await _run_market(OrderType.FAK)
                    fills_fak = fak_resp.get("fills") or []
                    result_fak = self._parse_fill(fak_resp, tokens_ref, "BUY", tick_size, midpoint)
                    # Validate: reject if FAK fill price exceeds the trigger price.
                    if max_entry_price is not None and result_fak.success:
                        if result_fak.fill_price > max_entry_price:
                            logger.warning(
                                "buy_token FAK fill rejected: fill_price={:.4f} > max_entry_price={:.4f} "
                                "(trigger price exceeded) — order will retry",
                                result_fak.fill_price, max_entry_price,
                            )
                            return FillResult(
                                order_id=result_fak.order_id or "",
                                tokens=0,
                                usdc=0,
                                fill_price=result_fak.fill_price,
                                success=False,
                                error=f"price_too_bad: fill={result_fak.fill_price:.4f} limit={max_entry_price:.4f}",
                            )
                    if fills_fak and result_fak.success:
                        self.invalidate_token_balance_cache(token_id)
                        return self._apply_fee_fallback(result_fak, fee_rate_bps)
                    logger.warning(
                        "buy_token FAK fallback also returned no fills for {}",
                        token_id[:12],
                    )
                except Exception as exc_fak:
                    logger.warning("buy_token FAK fallback failed: {}", exc_fak)
                return FillResult("", 0, 0, midpoint, False, f"FOK+FALLBACK_FAILED: {exc_str[:200]}")

            # 400 balance:人工确保余额充足，不需要重试
            raise

        # FAK path: check fills inline (taker market order returns fills immediately).
        if use_fak:
            fills = resp.get("fills") or []
            result = self._parse_fill(resp, tokens_ref, "BUY", tick_size, midpoint)
            if not fills or not result.success:
                logger.warning("buy_token FAK no fills for {}", token_id[:12])
            elif max_entry_price is not None and result.fill_price > max_entry_price:
                logger.warning(
                    "buy_token FAK fill rejected: fill_price={:.4f} > max_entry_price={:.4f}",
                    result.fill_price, max_entry_price,
                )
                result = FillResult(
                    order_id=result.order_id or "",
                    tokens=0,
                    usdc=0,
                    fill_price=result.fill_price,
                    success=False,
                    error=f"price_too_bad: fill={result.fill_price:.4f} limit={max_entry_price:.4f}",
                )
            if result.success:
                self.invalidate_token_balance_cache(token_id)
            return self._apply_fee_fallback(result, fee_rate_bps)

        # FOK: poll until fills confirmed.
        resp = await self._poll_fills_until_ready(
            resp, limit_price_ref, tokens_ref, token_id=token_id, side="BUY", midpoint=midpoint,
        )
        result = self._parse_fill(resp, tokens_ref, "BUY", tick_size, midpoint)
        if result.success:
            self.invalidate_token_balance_cache(token_id)
        return self._apply_fee_fallback(result, fee_rate_bps)

    # ── Market sell (FAK) ──────────────────────────────────────────────────────
    async def sell_token(
        self,
        token_id: str,
        tokens: float,
        midpoint: float,
        fee_rate_bps: int = 0,
        *,
        neg_risk: bool = False,
        user_usdc_balance: Optional[float] = None,
        slippage_pct: Optional[float] = None,
        max_price: Optional[float] = None,
    ) -> Optional[FillResult]:
        """
        Market SELL via FAK. Amount = number of tokens.
        All intermediate calculations use raw Decimal; quantization happens once
        just before the API call.

        max_price: optional price ceiling (R13). If provided and the computed
        limit_price exceeds this ceiling, the order is rejected so callers can
        wait for a better price rather than selling at an unfavorably high price.
        """
        if self._clob_client is None:
            return FillResult("", 0, 0, 0, False, "ClobClient not initialised")

        tick_size = await self._get_tick_size(token_id)
        tick_size_d = Decimal(tick_size)

        if midpoint < 0.001 or midpoint > 0.999:
            return FillResult(
                "", 0, 0, midpoint, False,
                f"midpoint {midpoint} out of safe range [0.001, 0.999]",
            )

        slip = Decimal(str(slippage_pct if slippage_pct is not None else self.slippage_pct))
        limit_price_dec = Decimal(str(midpoint)) * (Decimal("1") - slip / Decimal("100"))
        limit_price_dec = max(Decimal("0.0001"), min(Decimal("0.9999"), limit_price_dec))

        # R13: sell price upper bound check — reject if limit exceeds max_price.
        # This is symmetric with buy's max_entry_price guard.
        if max_price is not None and float(limit_price_dec) > max_price:
            return FillResult(
                "", 0, 0, float(limit_price_dec), False,
                f"sell limit {float(limit_price_dec):.4f} > max_price {max_price:.4f}",
            )

        min_price = float(getattr(self, "_min_exit_price", 0.0) or 0.0)
        if min_price > 0 and float(limit_price_dec) < min_price:
            return FillResult(
                "", 0, 0, float(limit_price_dec), False,
                f"sell limit {float(limit_price_dec):.4f} < min_exit_price {min_price:.4f}",
            )

        tick_size_f = float(tick_size)
        if tokens < tick_size_f:
            logger.info(
                "sell_token skipped: tokens={:.8f} < tick_size={} — dust discarded, treating as closed",
                tokens, tick_size_f,
            )
            return FillResult("", 0, 0, float(limit_price_dec), False, "dust_skipped")

        # Proactively cap the sell amount to the authoritative live balance, but ONLY
        # when CLOB shows less than we track (normal partial sell / cache lag).
        # If CLOB shows MORE than we track, something is wrong (external token injection,
        # stale cache from a prior oversell, etc.) — do NOT sell more than we tracked.
        # Invalidate cache FIRST so get_token_balance always reads fresh CLOB data.
        self.invalidate_token_balance_cache(token_id)
        live_bal = await self.get_token_balance(token_id)
        if live_bal is not None:
            safe_tokens = max(live_bal, 0.0)
            if safe_tokens < tick_size_f:
                logger.info(
                    "sell_token skipped: live balance {:.8f} < tick_size={} — dust discarded, treating as closed",
                    safe_tokens, tick_size_f,
                )
                return FillResult("", 0, 0, float(limit_price_dec), False, "dust_skipped")
            # Guard: never sell more than we tracked, even if CLOB balance is higher.
            # The CLOB balance may be inflated by a stale cache after a prior partial fill,
            # or by external token injections we didn't account for.
            tokens = min(tokens, safe_tokens)
            logger.debug(
                "sell_token: tokens={:.4f} (tracked), live_balance={:.4f} (CLOB)",
                tokens, safe_tokens,
            )
        else:
            logger.debug(
                "sell_token: tokens={:.4f} (tracked), live_balance=unknown (CLOB query failed)",
                tokens,
            )

        # CRITICAL: After capping tokens, invalidate cache and re-query live balance so that
        # bal_before_sell and tokens_ref are always consistent with each other and with the
        # actual amount being sold.  Without this, if safe_tokens < tokens (CLOB shows less
        # than we tracked), bal_before_sell would still hold the pre-adjustment stale value,
        # causing balance-diff verification to compute an incorrect measured_sold.
        self.invalidate_token_balance_cache(token_id)
        fresh_live_bal = await self.get_token_balance(token_id)

        # Record live balance BEFORE the sell so we can verify actual fills via balance diff.
        bal_before_sell = fresh_live_bal if fresh_live_bal is not None else live_bal

        # Reference values (not sent to API)
        limit_price_ref = float(limit_price_dec)
        tokens_ref = tokens

        logger.debug(
            "sell_token token={} tokens={} mid={} limit={:.4f} tick={} neg_risk={}",
            token_id[:12], tokens, midpoint, limit_price_ref, tick_size, neg_risk,
        )

        # Clean values only at API call time
        size_prec = _prec_for_fak_size(tick_size_d)
        tokens_clean = _q(Decimal(str(tokens)), size_prec)

        resp: Optional[dict] = None
        try:
            resp = await self._run(
                self._clob_client.create_and_post_market_order,
                order_args=MarketOrderArgs(
                    token_id=token_id,
                    amount=tokens_clean,
                    side=Side.SELL,
                    order_type=OrderType.FAK,
                    user_usdc_balance=float(user_usdc_balance) if user_usdc_balance else 0.0,
                ),
                options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
                order_type=OrderType.FAK,
            )
        except Exception as exc:
            exc_str = str(exc)

            # ── Balance-diff check (always, regardless of exception type) ─────────
            # ANY exception from create_and_post_market_order means we don't know the
            # order outcome.  The most important thing is to detect whether the order
            # actually filled on-chain despite the exception (e.g. CLOB returned an
            # error response but the transaction was mined).  We do this by comparing
            # token balance before vs after.
            self.invalidate_token_balance_cache(token_id)
            await asyncio.sleep(2.0)  # let CLOB settle before re-querying
            bal_pre_retry = await self.get_token_balance(token_id)
            if bal_pre_retry is not None and bal_before_sell is not None:
                measured_pre = max(0.0, bal_before_sell - bal_pre_retry)
                if measured_pre > 1e-9:
                    logger.info(
                        "sell_token {} EXCEPTION but fill confirmed via balance diff: "
                        "measured={:.4f} tokens exc={} — returning success",
                        exc_str[:200], measured_pre, exc_str[:100],
                    )
                    measured_usdc_pre = measured_pre * float(limit_price_dec)
                    self.invalidate_balance_cache()
                    return self._apply_fee_fallback(FillResult(
                        order_id="",
                        tokens=_q(Decimal(str(measured_pre)), size_prec),
                        usdc=_q(Decimal(str(measured_usdc_pre)), Decimal("0.01")),
                        fill_price=limit_price_ref,
                        success=True,
                        error="",
                    ), fee_rate_bps)

            # ── Balance-diff shows no fill: classify error and retry ──────────────
            is_balance_error = "status=400" in exc_str and "balance" in exc_str.lower()
            is_fak_no_match = (
                "status=400" in exc_str
                and "no orders found to match with fak order" in exc_str.lower()
            )
            if is_fak_no_match:
                logger.warning(
                    "sell_token FAK no-match (stale order?): {} — retrying",
                    exc_str[:300],
                )
            elif is_balance_error:
                logger.warning(
                    "sell_token balance error: {} — attempting auto-reduce",
                    exc_str[:300],
                )
            else:
                logger.warning(
                    "sell_token unexpected exception (no fill via balance diff): {} — "
                    "attempting auto-reduce",
                    exc_str[:300],
                )

            self.invalidate_token_balance_cache(token_id)
            live_bal = await self.get_token_balance(token_id)
            if live_bal is not None and live_bal > 1e-9:
                bal_tokens_clean = _q(Decimal(str(live_bal)), size_prec)
                safe_tokens = _q(min(Decimal(str(tokens)), Decimal(str(live_bal))), size_prec)
                logger.info(
                    "sell_token auto-reduced: tokens={:.4f} -> {:.4f} (CLOB balance={:.4f})",
                    tokens_clean, safe_tokens, bal_tokens_clean,
                )
                if safe_tokens >= tick_size_f:
                    # Re-query balance after capping to ensure consistency.
                    # tokens may have been reduced from the original tracked amount.
                    self.invalidate_token_balance_cache(token_id)
                    retry_bal = await self.get_token_balance(token_id)
                    try:
                        resp_retry = await self._run(
                            self._clob_client.create_and_post_market_order,
                            order_args=MarketOrderArgs(
                                token_id=token_id,
                                amount=safe_tokens,
                                side=Side.SELL,
                                order_type=OrderType.FAK,
                                user_usdc_balance=float(user_usdc_balance) if user_usdc_balance else 0.0,
                            ),
                            options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
                            order_type=OrderType.FAK,
                        )
                        resp_retry = await self._poll_fills_until_ready(
                            resp_retry, limit_price_ref, float(safe_tokens),
                            token_id=token_id, side="SELL", midpoint=midpoint,
                        )
                        result = self._parse_fill(resp_retry, float(safe_tokens), "SELL", tick_size, midpoint)
                        measured_sold_retry: Optional[float] = None
                        measured_from_poll_retry = resp_retry.get("_measured_sold") if resp_retry is not None else None
                        if measured_from_poll_retry is not None:
                            measured_sold_retry = measured_from_poll_retry
                        if result.success and measured_sold_retry is not None and measured_sold_retry > 1e-9:
                            logger.info(
                                "sell_token retry fill verified: measured={:.4f} tokens (API={:.4f})",
                                measured_sold_retry, result.tokens,
                            )
                            result = FillResult(
                                order_id=result.order_id or "",
                                tokens=_q(Decimal(str(measured_sold_retry)), size_prec),
                                usdc=_q(Decimal(str(measured_sold_retry * float(limit_price_dec))), Decimal("0.01")),
                                fill_price=limit_price_ref,
                                success=True,
                                error="",
                            )
                        if result.success:
                            self.invalidate_token_balance_cache(token_id)
                            self.invalidate_balance_cache()
                        resp = resp_retry
                        return self._apply_fee_fallback(result, fee_rate_bps)
                    except Exception as exc_retry:
                        # Balance-diff check before re-raising: the order may have filled on-chain
                        # despite the exception (same race as the primary path above).
                        self.invalidate_token_balance_cache(token_id)
                        bal_after_retry = await self.get_token_balance(token_id)
                        if retry_bal is not None and bal_after_retry is not None:
                            diff = max(0.0, retry_bal - bal_after_retry)
                            if diff > 1e-9:
                                logger.info(
                                    "sell_token retry {} actually filled (balance diff {:.4f} tokens) "
                                    "despite exception — returning success",
                                    exc_str[:200], diff,
                                )
                                self.invalidate_balance_cache()
                                return self._apply_fee_fallback(FillResult(
                                    order_id="",
                                    tokens=_q(Decimal(str(diff)), size_prec),
                                    usdc=_q(Decimal(str(diff * float(limit_price_dec))), Decimal("0.01")),
                                    fill_price=limit_price_ref,
                                    success=True,
                                    error="",
                                ), fee_rate_bps)
                        logger.warning("sell_token retry after reduce also failed: {}", exc_retry)
                        return FillResult("", 0, 0, midpoint, False, str(exc_retry))
                else:
                    logger.info(
                        "sell_token retry skipped: CLOB balance {:.6f} < tick_size {} — dust discarded",
                        safe_tokens, tick_size_f,
                    )
                    return FillResult("", 0, 0, limit_price_ref, False, "dust_skipped")
            return FillResult(
                "", 0, 0, limit_price_ref, False,
                f"balance/retry exhausted: {exc_str[:200]}",
            )

        resp = await self._poll_fills_until_ready(
            resp, limit_price_ref, tokens_clean, token_id=token_id, side="SELL", midpoint=midpoint,
        )
        result = self._parse_fill(resp, tokens_clean, "SELL", tick_size, midpoint)

        # ── Verify actual fills via balance diff ─────────────────────────────────
        # _poll_fills_until_ready already queries live token balance when SELL polling
        # times out; reuse that result via _measured_sold to avoid a redundant query.
        # For normal (fills-confirmed) cases, fall back to balance diff query.
        measured_sold: Optional[float] = None
        measured_from_poll = resp.get("_measured_sold") if resp is not None else None
        if measured_from_poll is not None:
            measured_sold = measured_from_poll
            logger.debug(
                "sell_token measured_sold={:.4f} from _poll_fills_until_ready (avoided extra balance query)",
                measured_sold,
            )
        elif result.success and bal_before_sell is not None:
            self.invalidate_token_balance_cache(token_id)
            bal_after = await self.get_token_balance(token_id)
            if bal_after is not None:
                measured_sold = max(0.0, bal_before_sell - bal_after)

        if result.success and measured_sold is not None and measured_sold > 1e-9:
            # Use the authoritative measured fill qty to rebuild the result.
            if abs(measured_sold - result.tokens) > 1e-6:
                logger.info(
                    "sell_token fill verified via balance diff: measured={:.4f} tokens "
                    "(API claimed={:.4f}) — correcting result",
                    measured_sold, result.tokens,
                )
            else:
                logger.debug(
                    "sell_token fill confirmed via balance diff: {:.4f} tokens",
                    measured_sold,
                )
            measured_usdc = measured_sold * float(limit_price_dec)
            result = FillResult(
                order_id=result.order_id or "",
                tokens=_q(Decimal(str(measured_sold)), size_prec),
                usdc=_q(Decimal(str(measured_usdc)), Decimal("0.01")),
                fill_price=limit_price_ref,
                success=True,
                error="",
            )
            self.invalidate_token_balance_cache(token_id)
            self.invalidate_balance_cache()  # also refresh pUSD collateral balance for next entry

        if result.success:
            self.invalidate_token_balance_cache(token_id)
            self.invalidate_balance_cache()  # also refresh pUSD collateral balance for next entry
        return self._apply_fee_fallback(result, fee_rate_bps)

    async def _poll_fills_until_ready(
        self,
        resp: dict,
        fallback_price: float,
        fallback_tokens: float,
        token_id: str = "",
        side: str = "BUY",
        midpoint: float = 0.5,
    ) -> dict:
        """
        Resolve fills for a market order.

        BUY  (FAK, primary): fills are returned directly in the response; no polling needed.
        BUY  (FOK, fallback): status=matched is definitive — the fill is on-chain immediately.
                              Accept without polling to save ~4.5s.

        SELL (FAK): Always poll (up to 3x) since partial fills are possible.
                    If polling returns no fills, query CLOB balance and cap tokens
                    to the real available amount.
        """
        if resp is None:
            logger.warning(
                "gave up polling: resp is None — side={}", side,
            )
            return resp

        fills = resp.get("fills") or []

        if fills:
            return resp

        order_id = resp.get("orderID", "")
        if not order_id:
            return resp

        status = (resp.get("status") or "").lower()

        # BUY (FOK fallback): matched = fill confirmed on-chain.
        # CRITICAL FIX (R2/R3): poll get_order for up to 5s before falling back to midpoint.
        # Do NOT use resp["price"] (the limit price with slippage padding).
        if side.upper() == "BUY" and status == "matched":
            ts_str = await self._get_tick_size(token_id) if token_id else "0.0001"
            ts = Decimal(ts_str)
            avg_fill = resp.get("averageFillPrice")
            if avg_fill is None:
                best_guess = midpoint
                for attempt in range(1, 6):  # poll up to 5 times, ~1s apart
                    await asyncio.sleep(1.0)
                    try:
                        order_resp = await self._run(self._clob_client.get_order, order_id)
                        if order_resp:
                            avg_fill = order_resp.get("averageFillPrice")
                            if avg_fill is not None:
                                best_guess = float(
                                    Decimal(str(avg_fill)).quantize(ts, rounding=ROUND_DOWN)
                                )
                                logger.info(
                                    "FOK BUY {} fill price resolved after {}s: {:.4f}",
                                    order_id[:12], attempt, best_guess,
                                )
                                break
                    except Exception as exc_g:
                        logger.debug(
                            "get_order {} poll {}/5 failed: {}",
                            order_id[:12], attempt, exc_g,
                        )
                if avg_fill is None:
                    avg_fill = best_guess
                    logger.warning(
                        "buy fill accepted on matched (no averageFillPrice after 5s) — "
                        "using {:.4f} as estimated fill price. orderID={}",
                        avg_fill, order_id[:12],
                    )
                else:
                    avg_fill = float(Decimal(str(avg_fill)).quantize(ts, rounding=ROUND_DOWN))
            else:
                avg_fill = float(Decimal(str(avg_fill)).quantize(ts, rounding=ROUND_DOWN))
            logger.info(
                "buy fill accepted on matched (no polling): orderID={} tokens={} avg_price={} (midpoint={})",
                order_id[:12], fallback_tokens, avg_fill, midpoint,
            )
            return {
                "orderID": order_id,
                "status": "FILLED",
                "fills": [{"size": fallback_tokens, "price": avg_fill}],
            }

        # BUY (FOK fallback) non-matched / pending states:
        # Re-query once after a brief wait to avoid false negatives.
        # If the order is still pending, return failure so the caller retries.
        # If it becomes matched, return success (avoiding a duplicate order from the caller).
        BUY_STATUSES = {"matched"}
        if side.upper() == "BUY" and status not in BUY_STATUSES:
            logger.warning(
                "FOK BUY order {} in status={} — re-querying after 2s",
                order_id[:12], status,
            )
            await asyncio.sleep(2.0)
            try:
                recheck = await self._run(self._clob_client.get_order, order_id)
                if recheck:
                    re_status = (recheck.get("status") or "").lower()
                    if re_status == "matched":
                        avg_fill = recheck.get("averageFillPrice")
                        if avg_fill is None:
                            avg_fill = midpoint
                            logger.warning(
                                "FOK BUY {} became matched but no averageFillPrice — "
                                "using midpoint {:.4f}",
                                order_id[:12], avg_fill,
                            )
                        else:
                            try:
                                ts_str = await self._get_tick_size(token_id) if token_id else "0.0001"
                                ts = Decimal(ts_str)
                                avg_fill = float(Decimal(str(avg_fill)).quantize(ts, rounding=ROUND_DOWN))
                            except Exception:
                                avg_fill = midpoint
                        logger.info(
                            "FOK BUY {} became matched on re-query: avg_price={}",
                            order_id[:12], avg_fill,
                        )
                        return {
                            "orderID": order_id,
                            "status": "FILLED",
                            "fills": [{"size": fallback_tokens, "price": avg_fill}],
                        }
                    elif re_status in {"cancelled", "expired", "failed"}:
                        logger.info(
                            "FOK BUY {} is {} after re-query — returning failure so caller retries",
                            order_id[:12], re_status,
                        )
                    else:
                        logger.warning(
                            "FOK BUY {} still in status={} after re-query — returning failure",
                            order_id[:12], re_status,
                        )
            except Exception as exc:
                logger.warning("FOK BUY re-query failed: {}", exc)
            return resp

        # SELL (FAK): poll for fills; handle partial or stale fill data.
        terminal_statuses = {"filled", "cancelled", "expired", "failed"}

        for attempt in range(1, 4):  # max 3 polls
            try:
                order_resp = await self._run(self._clob_client.get_order, order_id)
            except Exception as exc:
                logger.warning(
                    "poll get_order {}/3 failed: {}", attempt, exc,
                )
                if attempt == 3:
                    break
                continue

            if order_resp:
                order_fills = order_resp.get("fills") or []
                if order_fills:
                    logger.info(
                        "fill confirmed via get_order {}/3: orderID={}",
                        attempt, order_id[:12],
                    )
                    return order_resp

                order_status = (order_resp.get("status") or "").lower()
                if order_status in terminal_statuses:
                    logger.warning(
                        "poll {}/3 terminal status={} for orderID={}",
                        attempt, order_status, order_id[:12],
                    )
                    return order_resp

                # SELL FAK: matched = order is on-chain, use balance diff immediately.
                # CLOB matched orders may stay in "matched" status without returning fills[]
                # in the poll response, but the token balance has already been updated.
                if side.upper() == "SELL" and order_status == "matched":
                    logger.info(
                        "poll {}/3 SELL FAK status=matched for orderID={} — "
                        "using balance diff instead of waiting for fills[]",
                        attempt, order_id[:12],
                    )
                    measured_sold = None
                    final_fill_price = fallback_price
                    try:
                        self.invalidate_token_balance_cache(token_id)
                        live_bal = await self.get_token_balance(token_id)
                        if live_bal is not None:
                            measured_sold = max(0.0, fallback_tokens - live_bal)
                            logger.info(
                                "SELL FAK matched balance diff: measured_sold={:.4f} "
                                "(fallback_tokens={:.4f}, live_balance={:.4f})",
                                measured_sold, fallback_tokens, live_bal,
                            )
                    except Exception as exc_bal:
                        logger.debug(
                            "SELL FAK matched balance query failed: {} — "
                            "falling back to fallback_tokens",
                            exc_bal,
                        )
                    # Try one last get_order for averageFillPrice
                    try:
                        final_resp = await self._run(self._clob_client.get_order, order_id)
                        if final_resp:
                            api_avg = final_resp.get("averageFillPrice")
                            if api_avg is not None:
                                ts_str = await self._get_tick_size(token_id) if token_id else "0.0001"
                                ts = Decimal(ts_str)
                                final_fill_price = float(
                                    Decimal(str(api_avg)).quantize(ts, rounding=ROUND_DOWN)
                                )
                                logger.info(
                                    "SELL FAK matched fill price resolved: {:.4f}",
                                    final_fill_price,
                                )
                    except Exception:
                        pass
                    actual_tokens = (
                        measured_sold
                        if measured_sold is not None and measured_sold > 1e-9
                        else fallback_tokens
                    )
                    return {
                        "orderID": order_id,
                        "status": "FILLED",
                        "fills": [{"size": actual_tokens, "price": final_fill_price}],
                        "_measured_sold": actual_tokens if measured_sold is not None else None,
                    }

                if attempt < 3:
                    logger.debug(
                        "poll {}/3 no fills yet for orderID={} (status={})",
                        attempt, order_id[:12], order_status,
                    )

        # No fills after polling — SELL: use live token balance to determine the actual
        # filled amount via authoritative balance diff, instead of blindly assuming all
        # fallback_tokens were filled (which causes double-sell when CLOB is still
        # processing the order but status=matched with no fills[] yet).
        if side.upper() == "SELL":
            final_fill_price = fallback_price
            # Query live token balance BEFORE the constructed response to get the
            # authoritative fill amount while balances are still fresh.
            measured_sold = None
            try:
                self.invalidate_token_balance_cache(token_id)
                live_bal = await self.get_token_balance(token_id)
                if live_bal is not None:
                    measured_sold = max(0.0, fallback_tokens - live_bal)
                    logger.info(
                        "sell polling timeout: measured_sold={:.4f} via balance diff "
                        "(fallback_tokens={:.4f}, live_balance={:.4f})",
                        measured_sold, fallback_tokens, live_bal,
                    )
            except Exception as exc_bal:
                logger.debug(
                    "sell polling timeout: live balance query failed ({}) — "
                    "falling back to fallback_tokens",
                    exc_bal,
                )
            try:
                final_resp = await self._run(self._clob_client.get_order, order_id)
                if final_resp:
                    api_avg = final_resp.get("averageFillPrice")
                    if api_avg is not None:
                        ts_str = await self._get_tick_size(token_id) if token_id else "0.0001"
                        ts = Decimal(ts_str)
                        final_fill_price = float(
                            Decimal(str(api_avg)).quantize(ts, rounding=ROUND_DOWN)
                        )
                        logger.info(
                            "sell fill accepted (averageFillPrice={}) "
                            "orderID={}",
                            final_fill_price, order_id[:12],
                        )
            except Exception:
                pass
            actual_tokens = measured_sold if measured_sold is not None and measured_sold > 1e-9 else fallback_tokens
            constructed = {
                "orderID": order_id,
                "status": "FILLED",
                "fills": [{"size": actual_tokens, "price": final_fill_price}],
                # Inject measured_sold so sell_token can skip redundant balance query.
                "_measured_sold": actual_tokens if measured_sold is not None else None,
            }
            return constructed

        logger.warning(
            "gave up polling get_order for {} after 3 attempts — orderID={} status={}",
            order_id[:12], order_id[:12], status,
        )
        return resp

    # ── Wrap USDC.e → pUSD via Collateral Onramp ────────────────────────────────
    def wrap_usdc_e(self, usdc_e_amount: float) -> dict:
        """
        Wrap USDC.e into pUSD so the CLOB balance can be used for trading.

        Returns:
            dict with keys: success (bool), tx_hash (str), error (str)
        """
        from app.polygon_usdc import wrap_usdc_e_to_pusd as _wrap_fn
        return _wrap_fn(
            private_key=self._private_key,
            rpc_url=settings.polygon_rpc_url or "",
            usdc_e_amount=usdc_e_amount,
        )

    # ── Cancel order ───────────────────────────────────────────────────────────
    async def cancel_order(self, order_id: str) -> bool:
        if self._clob_client is None:
            return False
        try:
            from py_clob_client_v2 import OrderPayload
            payload = OrderPayload(orderID=order_id)
            resp = await self._run(self._clob_client.cancel_order, payload)
            success = resp.get("success", False)
            if not success:
                logger.warning("cancel_order {} → {}", order_id[:12], resp)
            return success
        except Exception as exc:
            logger.warning("cancel_order {} failed: {}", order_id[:12], exc)
            return False

    # ── Parse fill response ────────────────────────────────────────────────────
    @staticmethod
    def _parse_fill(
        resp,
        fallback_tokens: float,
        side: str,
        tick_size: Optional[Decimal] = None,
        midpoint: float = 0.0,
    ) -> FillResult:
        """
        Parse create_and_post_market_order response (v2 shape).
        Response: {orderID, status, fills: [{size, price, fee}], errorMsg}

        tick_size: price precision to quantize avg_price to (default Decimal("0.0001")).
        midpoint: the mid-market price at time of order submission. Used as fallback
            fill_price for FOK orders when fills[] is empty (i.e. market matched
            without returning actual fill prices in the response body).
        """
        price_tick = tick_size if tick_size is not None else Decimal("0.0001")
        if resp is None:
            return FillResult("", 0, 0, midpoint, False, "empty response")

        if isinstance(resp, str):
            try:
                resp = _json.loads(resp)
            except Exception:
                return FillResult("", 0, 0, midpoint, False, f"invalid JSON: {resp}")

        error_msg = resp.get("errorMsg") or resp.get("error") or ""
        if error_msg:
            return FillResult(resp.get("orderID", ""), 0, 0, midpoint, False, str(error_msg))

        status = (resp.get("status") or "").lower()
        fills = resp.get("fills") or []

        if not fills:
            # FOK orders: "status=matched" means the fill already happened on-chain
            # (Fill-Or-Kill returns immediately matched) but the fills[] array may be
            # absent from the HTTP response body even though the position was opened.
            # Treat matched-without-fills as a confirmed fill so Trade records are saved.
            # For any other status (live, unknown), return failure so the caller retries.
            if status == "matched" and resp.get("orderID"):
                # Use the CLOB's averageFillPrice if available; it reflects the actual
                # on-chain fill price.
                # CRITICAL FIX (R3): do NOT fall back to resp.get("price") — that is the
                # limit price (which includes slippage padding and is systematically too
                # high for buys / too low for sells). Fall back to midpoint instead, which
                # is a much better estimate than the padded limit price.
                avg_fill = resp.get("averageFillPrice")
                estimated = False
                if avg_fill is None:
                    # CRITICAL FIX (R2): midpoint is the best available estimate when
                    # the CLOB did not return averageFillPrice. Using limit price here
                    # would systematically inflate entry costs and distort PnL / VWAP.
                    avg_fill = midpoint
                    estimated = True
                    logger.warning(
                        "FOK matched without averageFillPrice — "
                        "using estimated midpoint {:.4f} as fill price for orderID={}",
                        avg_fill, resp["orderID"][:12],
                    )
                else:
                    try:
                        avg_fill = float(Decimal(str(avg_fill)).quantize(
                            Decimal(str(price_tick)), rounding=ROUND_DOWN,
                        ))
                    except Exception:
                        avg_fill = midpoint
                return FillResult(
                    order_id=resp["orderID"],
                    tokens=_q(fallback_tokens, Decimal("0.0001")),
                    usdc=_q(Decimal(str(avg_fill)) * Decimal(str(fallback_tokens)), Decimal("0.01")),
                    fill_price=avg_fill,
                    success=True,
                    error="",
                    estimated_price=estimated,
                )
            return FillResult(
                resp.get("orderID", ""), 0, 0, midpoint, False,
                f"no fills, status={status}",
            )

        total_tokens = Decimal("0")
        total_usdc = Decimal("0")
        total_fee = Decimal("0")
        for f in fills:
            size = Decimal(str(float(f.get("size", 0))))
            price = Decimal(str(float(f.get("price", midpoint))))
            fee = Decimal(str(float(f.get("fee", 0) or 0)))
            total_tokens += size
            total_usdc += size * price
            total_fee += fee

        if total_tokens == 0:
            return FillResult(resp.get("orderID", ""), 0, 0, midpoint, False, "zero fill size")

        avg_price = _q(total_usdc / total_tokens, price_tick)
        tokens_f  = _q(total_tokens, Decimal("0.0001"))
        usdc_f    = _q(total_usdc, Decimal("0.01"))
        fee_f     = _q(total_fee, Decimal("0.000001"))

        return FillResult(
            order_id=resp.get("orderID", ""),
            tokens=tokens_f,
            usdc=usdc_f,
            fill_price=avg_price,
            success=True,
            error="",
            fee_usdc=fee_f,
        )

    # ── Balance / allowance ─────────────────────────────────────────────────────
    async def get_balance_and_allowance(
        self,
        asset_type: str = "COLLATERAL",
    ) -> tuple[int, int]:
        """
        Return (balance_raw, allowance_raw) in micro-units.
        For POLY_PROXY (sig_type=1) and POLY_GNOSIS_SAFE (sig_type=2):
          allowance is N/A — funds live in the funder/proxy address on-chain.
          We check the funder's on-chain pUSD balance as the effective "balance",
          and return a large dummy allowance so the balance check always passes.
        For EOA (sig_type=0):
          Uses ClobClient.get_balance_allowance() with BalanceAllowanceParams (v2).
        """
        if self._clob_client is None:
            return 0, 0

        # POLY_PROXY / POLY_GNOSIS_SAFE: use CLOB get_balance_allowance (same API as EOA).
        # The ClobClient is initialized identically for all signature types.
        # On-chain pUSD balance (polygon_pusd_balance_micro) was stale because it includes
        # pUSD locked in the CLOB exchange and is not synced after each trade.
        if self._signature_type in (1, 2):
            result = await self._proxy_balance()
            # Emit warning if CLOB returned zero balance — operator must investigate.
            bal_val = result[0]
            if bal_val == 0:
                logger.warning(
                    "CLOB balance returns 0 for sig_type={} (proxy/safe mode). "
                    "If this is unexpected, the CLOB cache may be stale or funds may be depleted. "
                    "Current wallet: {}",
                    self._signature_type,
                    LiveTrader._addr_short(self._account_addr or self._funder or ""),
                )
            return result

        # EOA mode: use CLOB balance/allowance
        try:
            from py_clob_client_v2 import AssetType as SDKAssetType
            sdk_asset = SDKAssetType.COLLATERAL
            resp = await self._run(
                self._clob_client.get_balance_allowance,
                BalanceAllowanceParams(asset_type=sdk_asset),
            )
            bal = int(resp.get("balance", 0) or 0)
            alw = int(resp.get("allowance", 0) or 0)
            return bal, alw
        except Exception as exc:
            logger.warning("get_balance_and_allowance failed: {}", exc)
            return 0, 0

    async def _proxy_balance(self) -> tuple[int, int]:
        """
        POLY_PROXY / POLY_GNOSIS_SAFE: use CLOB get_balance_allowance (same API as EOA).
        On-chain pUSD balance (polygon_pusd_balance_micro) was stale because:
          1. It includes pUSD locked in the CLOB exchange which is NOT spendable for new orders.
          2. It is not synced immediately after each trade.
        The CLOB balance endpoint reflects the internally-consistent balance used for order
        amounts, so token accumulation and this balance always agree.

        A _balance_lock ensures that if a cache refresh is in progress (after a trade),
        this method blocks until it completes before issuing the balance query, so the
        result always reflects the latest trade.

        R13: when CLOB returns 0, cross-verify against on-chain balance to avoid
        false-zero readings caused by stale cache.
        """
        async with self._balance_lock:
            # If the cache was invalidated since the last call (e.g. after a sell),
            # reset the flag and force a fresh CLOB query.
            stale = self._balance_cache_stale
            self._balance_cache_stale = False
            try:
                from py_clob_client_v2 import AssetType as SDKAssetType
                resp = await self._run(
                    self._clob_client.get_balance_allowance,
                    BalanceAllowanceParams(asset_type=SDKAssetType.COLLATERAL),
                )
                bal = int(resp.get("balance", 0) or 0)
                alw = int(resp.get("allowance", 0) or 0)

                # R13: CLOB balance is 0 — cross-verify with on-chain pUSD balance.
                # A zero CLOB balance could mean: (a) funds exhausted, or (b) cache lag.
                # If the on-chain balance is significantly higher, trust the chain value.
                if bal == 0 and alw == 0 and self._signature_type in (1, 2):
                    try:
                        chain_bal_raw = await self._chain_pusd_balance()
                        if chain_bal_raw is not None and chain_bal_raw > 0:
                            logger.info(
                                "proxy_balance: CLOB=0 but on-chain pUSD={:.4f} — "
                                "CLOB cache may be stale, using chain balance",
                                chain_bal_raw / 1_000_000,
                            )
                            bal = chain_bal_raw
                    except Exception as exc_chain:
                        logger.debug("on-chain balance cross-check failed: {}", exc_chain)

                # Dummy allowance: proxy/safe have no separate approval step;
                # the dummy allows balance checks to pass without on-chain tx.
                allowance = alw if alw > 0 else int(10_000_000 * 1_000_000)
                logger.debug(
                    "proxy_balance (CLOB): balance={} (dummy_allowance={})",
                    bal / 1_000_000,
                    allowance / 1_000_000,
                )
                return bal, allowance
            except Exception as exc:
                logger.warning("_proxy_balance (CLOB) failed: {}", exc)
                return 0, 0

    async def _chain_pusd_balance(self) -> Optional[int]:
        """Return on-chain pUSD balance in micro-units, or None on error."""
        try:
            from app.polygon_usdc import polygon_pusd_balance_micro
            wallet = self._account_addr or self._funder or ""
            if not wallet:
                return None
            raw = polygon_pusd_balance_micro(
                holder=wallet,
                rpc_url=settings.polygon_rpc_url or "",
            )
            return raw
        except Exception:
            return None

    async def get_usdc_balance_micro(self) -> int:
        bal, _ = await self.get_balance_and_allowance("COLLATERAL")
        return bal

    @staticmethod
    def _extract_balance_from_error(exc_str: str) -> Optional[int]:
        """
        Extract CLOB micro-balance from a 400 'not enough balance' error message.

        Error format: "status=400 url=... body={\"error\":\"...balance: 53635098, order amount: 55170000\"}"
        Returns balance in micro-USDC (int), or None if parsing fails.
        """
        import re
        # Match "balance" followed by any separators (literal ":", JSON-escaped "\u003a",
        # comma, space, etc.) and capture the integer micro-balance.
        m = re.search(r"balance[^\d]*(\d+)", exc_str)
        if m:
            return int(m.group(1))
        return None

    async def _refresh_balance_cache(self, amount_hint: Optional[float] = None):
        """
        Force-refresh the CLOB server's balance/allowance cache after a trade.

        CLOB uses a polling cache for balance — it does NOT update in real-time after each
        order. Without this call, get_balance_and_allowance() returns a stale balance that
        does not reflect the cost of tokens just bought, causing subsequent sells to fail
        with "not enough balance" even when funds are sufficient.

        Uses sync_clob_balance_cache (not the SDK's update_balance_allowance) because:
          - It passes the `amount` hint which the CLOB server uses to more reliably update
            the cached balance after a trade.
          - It uses L2 HMAC auth identical to the ClobClient, and works correctly for
            POLY_PROXY (sig_type=1) where the SDK's update_balance_allowance may return
            an empty response body and silently fail to refresh the cache.

        Args:
            amount_hint: Deprecated. Kept for backward compatibility but has no effect.

        Raises:
            RuntimeError: after 2 consecutive failures — signals that orders should pause.
        """
        from app.polygon_usdc import sync_clob_balance_cache

        for attempt in range(1, 3):  # max 2 attempts
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: sync_clob_balance_cache(
                        private_key=self._private_key,
                        host=self._host,
                        api_key=self._api_key,
                        api_secret=self._api_secret,
                        api_passphrase=self._api_passphrase,
                        signature_type=self._signature_type,
                        amount=amount_hint if amount_hint is not None else 0.0,
                    ),
                )
            except Exception:
                logger.warning("_refresh_balance_cache attempt {}/2 failed (exception)", attempt)
                if attempt == 2:
                    # CRITICAL (R7): after 2 failures, pause orders and raise.
                    # Without this, stale balance can cause "not enough balance" errors
                    # that appear as insufficient funds but are actually a cache bug.
                    logger.error(
                        "CLOB balance cache refresh exhausted after 2 attempts — "
                        "orders will be PAUSED until next successful refresh. "
                        "Manual intervention required if orders repeatedly fail."
                    )
                    self._balance_cache_stale = True
                    raise RuntimeError(
                        "CLOB balance cache refresh failed 2 consecutive times — "
                        "orders paused to prevent stale-balance errors"
                    )
                continue

            if result.get("ok"):
                self._balance_cache_stale = False
                logger.debug("CLOB balance cache refreshed: hint={}", amount_hint)
                return
            else:
                logger.debug(
                    "CLOB balance cache refresh attempt {}/2 failed: {}",
                    attempt, result.get("error", "unknown"),
                )
                if attempt == 2:
                    logger.error(
                        "CLOB balance cache refresh exhausted — "
                        "orders will be PAUSED until next successful refresh"
                    )
                    self._balance_cache_stale = True
                    raise RuntimeError(
                        "CLOB balance cache refresh failed 2 consecutive times — "
                        "orders paused to prevent stale-balance errors"
                    )

    @property
    def balance_cache_stale(self) -> bool:
        """True when balance cache has failed to refresh 2+ times consecutively."""
        return getattr(self, "_balance_cache_stale", False)

    def invalidate_token_balance_cache(self, token_id: Optional[str] = None):
        """
        Invalidate the token balance cache.

        Call this after a trade so the next get_token_balance() always hits the
        live CLOB instead of returning a stale cached value.

        Args:
            token_id: If provided, only that token's cache entry is removed.
                      If None, the entire cache is cleared.
        """
        if token_id is not None:
            self._token_balance_cache.pop(token_id, None)
        else:
            self._token_balance_cache.clear()

    def invalidate_balance_cache(self):
        """
        Mark the pUSD/USDC collateral balance as stale so the next
        get_balance_and_allowance() call always fetches fresh data from CLOB.

        This is critical after a sell: the balance query that returned before the
        sell executed may still be completing.  By setting _balance_cache_stale,
        the next _proxy_balance call bypasses any in-flight result and re-queries.
        """
        self._balance_cache_stale = True

    # ── Token balance query ─────────────────────────────────────────────────────
    async def get_token_balance(self, token_id: str) -> Optional[float]:
        """
        Return the live token balance for the current wallet on CLOB (in human units).
        Uses ClobClient.get_balance_allowance() with AssetType.CONDITIONAL + token_id.
        """
        if self._clob_client is None:
            return None

        try:
            from py_clob_client_v2 import AssetType as SDKAssetType
            resp = await self._run(
                self._clob_client.get_balance_allowance,
                BalanceAllowanceParams(
                    asset_type=SDKAssetType.CONDITIONAL,
                    token_id=token_id,
                ),
            )
            raw = int(resp.get("balance", 0) or 0)
            # Bug-8 fix: quantize to the market's actual tick_size (not a fixed 0.00000001).
            # Polymarket tokens have tick_size=0.0001; using 8-decimal quantization
            # introduced rounding errors at high token counts (>1000 tokens → ~0.0001 error).
            ts_str = await self._get_tick_size(token_id)
            ts_prec = Decimal(ts_str)
            balance = float(
                (Decimal(raw) / Decimal("1_000_000")).quantize(ts_prec, rounding=ROUND_DOWN)
            )
            return balance
        except Exception as exc:
            logger.warning("get_token_balance {} failed: {}", token_id[:12], exc)
            return None

    # ── On-chain redemption ─────────────────────────────────────────────────────
    def redeem_open_positions(self, size_threshold: float = 0.01, condition_id: Optional[str] = None) -> dict:
        """
        Synchronous entry point — creates a fresh event loop to run _redeem_sync (pure web3).
        Wrapping in asyncio ensures the web3 blocking calls don't pollute the main event loop.
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self._run(self._redeem_sync, size_threshold, condition_id))
            finally:
                loop.close()
        except Exception as exc:
            logger.error("redeem_open_positions unexpected error: {}", exc)
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": [str(exc)]}

    async def redeem_open_positions_async(self, size_threshold: float = 0.01, condition_id: Optional[str] = None) -> dict:
        """Async entry point — runs _redeem_sync via thread executor so web3 blocking calls don't block the event loop."""
        # Bug-5 fix: reject None condition_id to prevent accidentally redeeming all windows.
        # All callers (skip_record path, close_position, _close_loop dust path) must pass
        # the specific condition_id to scope redemption to the current window only.
        if condition_id is None:
            logger.error(
                "redeem_open_positions_async called with condition_id=None — "
                "rejecting to prevent cross-window redemption sweep. "
                "Callers must always pass the target condition_id.",
            )
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": ["condition_id is required"]}
        return await self._run(self._redeem_sync, size_threshold, condition_id)

    def _redeem_sync(self, size_threshold: float, condition_id: Optional[str] = None) -> dict:
        from app.polygon_usdc import _erc20_balance_raw, POLYGON_PUSD as _PUSD
        from web3 import Web3
        from web3.contract import Contract

        rpc_url = settings.polygon_rpc_url
        if not rpc_url:
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": ["no RPC URL"]}

        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": ["Web3 not connected"]}

        wallet = to_checksum_address(self._account_addr or self._funder or "")
        if not wallet:
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": ["no wallet address"]}

        # Pass condition_id so only the current window's tokens are redeemed.
        # Without this filter, redeem would also sweep tokens from previous windows.
        positions = self._fetch_redeemable_positions(wallet, condition_id=condition_id)
        if not positions:
            return {"checked": 0, "redeemed": 0, "total_usdc": 0.0, "errors": []}

        checked = 0
        redeemed = 0
        total_usdc = 0.0
        errors = []

        for pos in positions:
            checked += 1
            cond_id  = pos.get("conditionId") or pos.get("condition_id") or ""
            up_id    = pos.get("upTokenId")   or pos.get("up_token_id")   or ""
            down_id  = pos.get("downTokenId") or pos.get("down_token_id") or ""
            neg_risk = bool(pos.get("neg_risk") or pos.get("negRisk", False))
            size = float(pos.get("size") or pos.get("balance") or 0)

            if size < size_threshold:
                continue

            try:
                with self._redeem_lock:
                    usdc = self._redeem_one_position(w3, wallet, cond_id, up_id, down_id, neg_risk)
                if usdc > 0:
                    redeemed += 1
                    total_usdc += usdc
            except Exception as exc:
                errors.append(f"redeem {cond_id[:12]}: {exc}")

        return {
            "checked": checked, "redeemed": redeemed,
            "total_usdc": round(total_usdc, 2), "errors": errors,
        }

    def _fetch_redeemable_positions(
        self, wallet: str, condition_id: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch redeemable positions from Polymarket data API.

        Args:
            wallet: wallet address to query
            condition_id: if provided, only positions matching this condition_id are returned.
                          This prevents redeeming tokens from a different (previous) window.
        """
        for status in ("all", "open"):
            try:
                resp = httpx.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": wallet.lower(), "status": status},
                    timeout=15.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    positions = data if isinstance(data, list) else data.get("positions", [])
                    if positions:
                        if condition_id:
                            positions = [
                                p for p in positions
                                if (p.get("conditionId") or p.get("condition_id") or "") == condition_id
                            ]
                        if positions:
                            return positions
            except Exception as exc:
                logger.debug("_fetch_redeemable_positions (status={}) failed: {}", status, exc)
        return []

    def _redeem_one_position(
        self, w3, wallet: str, condition_id: str,
        up_token_id: str, down_token_id: str, neg_risk: bool,
    ) -> float:
        from web3.contract import Contract
        from app.polygon_usdc import POLYGON_PUSD as _PUSD, _erc20_balance_raw

        redeem_abi = {
            "name": "redeemPositions",
            "type": "function",
            "inputs": [
                {"name": "conditionId", "type": "bytes32"},
                {"name": "tokenIds",    "type": "uint256[]"},
                {"name": "amounts",     "type": "uint256[]"},
            ],
            "outputs": [],
        }

        ctf_addr = to_checksum_address(
            NEG_RISK_ADAPTER if neg_risk else CTF_ADDRESS
        )
        contract: Contract = w3.eth.contract(address=ctf_addr, abi=[redeem_abi])

        bal_before = _erc20_balance_raw(wallet, _PUSD, settings.polygon_rpc_url) or 0

        # Validate condition_id before hex decoding to avoid ValueError on invalid input
        hex_str = condition_id.replace("0x", "").replace("0X", "")
        if not all(c in "0123456789abcdefABCDEF" for c in hex_str):
            raise ValueError(
                f"Invalid condition_id format (non-hex characters): {condition_id[:20]!r}"
            )
        if len(hex_str) != 64:
            raise ValueError(
                f"Invalid condition_id length: expected 64 hex chars, got {len(hex_str)} "
                f"for {condition_id[:20]!r}"
            )
        cond_bytes = bytes.fromhex(hex_str)
        # 1 << 255 is the conventional sentinel value used by the CTF / NegRiskAdapter
        #redeemPositions to indicate "redeem all tokens".  While mathematically it is
        # type(int256).min rather than type(uint256).max, it is the value hard-coded
        # in the CLOB SDK and on-chain examples — do not change without confirming
        # compatibility with the deployed contracts.
        MAX_AMOUNT = 1 << 255
        fn = contract.functions.redeemPositions(
            cond_bytes,
            [up_token_id, down_token_id],
            [MAX_AMOUNT, MAX_AMOUNT],
        )

        # All on-chain operations that affect nonce must be serialised so concurrent
        # BTC + ETH settlements cannot produce overlapping nonces.
        with self._redeem_lock:
            # Build base tx for gas estimation (requires 'to' + data for estimate_gas)
            base_tx = {
                "from": to_checksum_address(wallet),
                "nonce": w3.eth.get_transaction_count(to_checksum_address(wallet)),
                "chainId": 137,
            }
            try:
                estimated_gas = int(w3.eth.estimate_gas(fn.build_transaction(base_tx)) * 1.3)
            except Exception as exc:
                logger.warning(
                    "redeem gas estimate failed: {} — using adaptive fallback gas",
                    exc,
                )
                # redeemPositions for 2 token outcomes on Polygon: ~300-450k typical.
                # NegRisk tokens may need more. Use dynamic fallback based on token count.
                num_tokens = 2  # up + down token
                base_gas = 450_000
                per_token_gas = 50_000
                estimated_gas = base_gas + (num_tokens - 1) * per_token_gas
                # Hard upper bound to prevent runaway gas
                estimated_gas = min(estimated_gas, 800_000)

            latest_block = w3.eth.get_block("latest")
            base_fee = latest_block.get("baseFeePerGas", w3.eth.gas_price)
            priority_fee = w3.eth.max_priority_fee
            # EIP-1559: max_fee = base_fee + priority_fee.
            # The actual fee is min(max_fee, base_fee + actual_priority_paid).
            max_fee = int(base_fee + priority_fee)

        tx = fn.build_transaction({
            "from":          to_checksum_address(wallet),
            "gas":           estimated_gas,
            "maxFeePerGas":  max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "nonce":         base_tx["nonce"],
            "chainId":       137,
        })

        signed = w3.eth.account.sign_transaction(tx, self._private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt.status == 0:
            raise RuntimeError(f"redeem tx reverted: {tx_hash.hex()}")

        bal_after = _erc20_balance_raw(wallet, _PUSD, settings.polygon_rpc_url) or 0
        return (bal_after - bal_before) / 1_000_000

    def _ensure_pusd_approved(self):
        """EOA mode: approve CLOB Exchange V2 to spend pUSD if not already approved."""
        from app.polygon_usdc import _allowance_raw, approve_pusd_to_exchange, POLYGON_PUSD, CLOB_EXCHANGE_V2
        if not settings.polygon_rpc_url:
            return
        wallet = self._account_addr or self._funder
        if not wallet:
            return
        try:
            current_allowance = _allowance_raw(
                wallet, CLOB_EXCHANGE_V2, POLYGON_PUSD, settings.polygon_rpc_url,
            )
            # If allowance is already set to a large value, skip
            if current_allowance >= int(1e9):  # ≥ 1000 USDC
                logger.debug("pUSD already approved for exchange (allowance={})", current_allowance)
                self._sync_cache_after_approval()
                return
            logger.info("EOA mode: approving pUSD for CLOB Exchange V2 ...")
            result = approve_pusd_to_exchange(
                private_key=self._private_key,
                rpc_url=settings.polygon_rpc_url,
                amount=0.0,  # 0 means approve for unlimited
            )
            if result.get("success"):
                logger.info("pUSD approval tx confirmed: {}", result.get("tx_hash", ""))
                self._sync_cache_after_approval()
            else:
                logger.warning("pUSD approval failed: {}", result.get("error", "unknown"))
        except Exception as exc:
            logger.warning("pUSD approval check failed: {}", exc)

    def _sync_cache_after_approval(self):
        """After on-chain approval, refresh the CLOB balance cache so allowance reflects the new on-chain state."""
        from app.polygon_usdc import sync_clob_balance_cache
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                sync_clob_balance_cache(
                    private_key=self._private_key,
                    host=self._host,
                    api_key=self._api_key,
                    api_secret=self._api_secret,
                    api_passphrase=self._api_passphrase,
                    signature_type=self._signature_type,
                    amount=0.0,
                )
                logger.debug("CLOB cache synced after pUSD approval")
            finally:
                loop.close()
        except Exception as exc:
            logger.warning("CLOB cache sync after approval failed: {}", exc)

    def set_pm_client(self, pm):
        self._pm_client = pm

    def close(self):
        if self._pm_client is not None:
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._pm_client.close())
                finally:
                    loop.close()
            except Exception as exc:
                logger.debug("pm_client.close() failed: {}", exc)
        logger.info("LiveTrader v2 closed.")
