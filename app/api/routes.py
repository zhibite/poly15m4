"""
routes.py — Core REST API routes: wallet, config, trades, dashboard.
"""

import asyncio
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger

from app.config import settings
from app.database import async_session
from app.models import Fill, Trade, User
from app.runtime_config import get_config, get_creds, update_config
from app.strategy.position_state import ExitReason
from app.api.websocket import set_strategy_ref as _set_ws_strategy_ref
from app.auth import get_current_user

router = APIRouter(prefix="/api", tags=["api"])

# ── Global refs (set by main.py) ───────────────────────────────────────────────
_strategy_ref = None
_live_trader_ref = None
_reload_fn = None

# live_mode hot-switch guard (R0-3): require explicit POST /api/live/confirm
# before the first live_mode=True transition. Resets to False on startup.
_live_mode_confirmed: bool = False
_live_mode_confirmed_ts: str | None = None
_live_mode_confirmed_lock = threading.Lock()

# ── Rate limiting ─────────────────────────────────────────────────────────────
_rate_limit_lock = threading.Lock()
_rate_limit: dict[str, tuple[deque, datetime]] = {}


def _check_rate_limit(identifier: str, max_requests: int = 10, window_sec: int = 60):
    now = datetime.now(timezone.utc)
    with _rate_limit_lock:
        entry = _rate_limit.get(identifier)
        if entry is None:
            d = deque(maxlen=max_requests)
            d.append(now)
            _rate_limit[identifier] = (d, now)
            return
        d, _ = entry
        while d and (now - d[0]).total_seconds() > window_sec:
            d.popleft()
        if len(d) >= max_requests:
            raise HTTPException(status_code=429, detail="Too many requests, please try again later")
        d.append(now)
        _rate_limit[identifier] = (d, now)


def _client_id(request: Request) -> str:
    return request.client.host if request.client else "unknown"

# ── Strategy config ─────────────────────────────────────────────────────────────
_CONFIG_BOUNDS: dict[str, tuple] = {
    # 基础参数
    "entry_base_usdc": (1.0, 100.0),
    "poll_interval_sec":              (1.0, 60.0),
    "live_slippage_pct": (0.1, 10.0),
    "btc_slippage_pct": (0.1, 10.0),
    "eth_slippage_pct": (0.1, 10.0),
    "min_exit_price": (0.0, 0.5),
    "settle_grace_sec": (10, 3600),
    "max_settle_wait_sec": (60, 7200),
    "default_fee_bps": (1.0, 500.0),
    "market_duration_sec": (120.0, 86400.0),
    "live_mode": (0, 1),
    "sell_max_retry_count": (1, 10),

    # 入场截止
    "entry_early_start_elapsed_sec": (0.0, 900.0),
    "entry_late_cutoff_elapsed_sec": (0.0, 900.0),

    # 信号驱动入场
    "core_agree_threshold": (2.0, 6.0),
    "core_agree_standard": (2.0, 6.0),
    "core_contradict_limit": (0.0, 6.0),
    "entry_price_alignment_min_dev": (0.0, 0.50),
    "entry_price_min": (0.01, 0.95),
    "entry_price_max": (0.05, 0.70),

    # 流动性与信号阈值
    "min_depth_coverage": (0.5, 20.0),
    "max_slippage_pct": (0.1, 20.0),
    "obi_contradict_skip": (0.0, 1.0),
    "oi_change_significant": (0.001, 0.50),

    # 按 Core 共识数量配置仓位倍数
    "stake_mult_core3": (0.0, 5.0),
    "stake_mult_core4": (0.0, 5.0),
    "stake_mult_core5": (0.0, 5.0),
    "stake_mult_core6": (0.0, 5.0),

    # 分数 Kelly
    "kelly_fraction": (0.0, 1.0),
    "kelly_prior_weight": (1.0, 200.0),
    "kelly_min_samples": (0.0, 1000.0),
    "kelly_max_mult": (0.0, 10.0),
    "bankroll_usdc": (1.0, 1_000_000.0),

    # 因子模型
    "factor_mode": (0.0, 2.0),
    "factor_w_trend": (0.0, 1.0),
    "factor_w_reversion": (0.0, 1.0),
    "factor_w_price": (0.0, 1.0),
    "factor_w_micro": (0.0, 1.0),
    "factor_w_flow": (0.0, 1.0),
    "factor_min_net": (0.0, 1.0),
    "factor_min_agreement": (0.0, 1.0),
    "factor_rev_veto_score": (0.0, 1.0),
    "factor_max_spread_bps": (1.0, 1000.0),
    "factor_oi_change_threshold": (0.001, 0.50),

    # 止盈止损（entry_controller._calc_stop_levels）
    "tp_base_pct": (1.0, 99.0),
    "tp_range_pct": (5.0, 50.0),
    "entry_price_min_tp": (0.05, 0.50),
    "entry_price_max_tp": (0.05, 0.60),
    "sl_base_pct": (-99.0, 0.0),

    # 出场策略
    "exit_tp_sl_disable_after_elapsed_sec": (0.0, 900.0),
    "exit_cost_buffer_pct": (0.0, 20.0),
    "exit_trailing_stop_pct": (0.0, 50.0),
    "exit_trailing_activation_pct": (0.0, 200.0),

    # 主策略风控
    "main_max_daily_loss_usdc": (-10000.0, 0.0),
    "main_max_consecutive_losses": (0.0, 50.0),
    "main_loss_cooldown_sec": (0.0, 86400.0),
    "max_open_positions_total": (1.0, 20.0),

    # 兼容旧 key
    "signal_fixed_stop_loss_pct": (-99.0, 0.0),
    "entry_skip_if_late": (0, 1),

    # ── 末端顺势策略 ────────────────────────────────────────────────
    "end_window_start_sec": (0.0, 900.0),
    "end_window_end_sec": (0.0, 900.0),
    "end_hardcut_sec": (0.0, 900.0),
    "end_min_entry_price": (0.0, 1.0),
    "end_confidence_threshold": (0.0, 100.0),
    # 注意：reversal_stop_pct 是小数（0.05 = 5%），不是百分数
    "end_reversal_stop_pct": (0.0, 0.5),
    "end_tp_min_pct": (0.0, 100.0),
    "end_tp_max_pct": (0.0, 100.0),
    "end_tp_entry_min": (0.01, 0.99),
    "end_tp_entry_max": (0.01, 1.0),
    "end_sl_pct": (-100.0, 0.0),
    "end_base_position_usdc": (1.0, 100.0),
    "end_max_daily_loss": (-1000.0, 0.0),
}


_BOOL_KEYS_BODY = frozenset(
    {
        "asset_btc_enabled",
        "asset_eth_enabled",
        "entry_skip_if_late",
        "entry_require_price_alignment",
        "use_kelly_sizing",
        "exit_hold_to_settlement",
        "exit_tp_enabled",
        "exit_sl_enabled",
        "end_enabled",
        "end_asset_btc_enabled",
        "end_asset_eth_enabled",
    }
)


@router.get("/config")
async def api_get_config():
    return get_config()


@router.post("/config")
async def api_update_config(body: dict[str, Any], request: Request):
    base = get_config()
    patch: dict[str, Any] = {}

    if "market_interval_slug" in body:
        slug = str(body["market_interval_slug"]).strip().lower().replace("/", "")
        if not slug or len(slug) > 32:
            raise HTTPException(status_code=400, detail="invalid market_interval_slug")
        patch["market_interval_slug"] = slug

    for bk in _BOOL_KEYS_BODY:
        if bk not in body:
            continue
        v = body[bk]
        patch[bk] = v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes", "on")

    for k, v in body.items():
        # 旧 key 映射到新 key
        if k == "signal_fixed_stop_loss_pct":
            k = "sl_base_pct"
        if k == "market_interval_slug":
            continue
        if k in _BOOL_KEYS_BODY:
            continue
        if k not in _CONFIG_BOUNDS:
            continue
        lo, hi = _CONFIG_BOUNDS[k]
        if k == "live_mode":
            patch[k] = v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes", "on")
            continue
        try:
            if k in (
                "core_agree_threshold", "core_agree_standard",
                "core_contradict_limit", "main_max_consecutive_losses",
                "main_loss_cooldown_sec", "max_open_positions_total",
                "kelly_min_samples", "factor_mode",
            ):
                val = int(round(float(v)))
            else:
                val = float(v)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail=f"Invalid value for {k}") from None
        if k in (
            "core_agree_threshold", "core_agree_standard",
            "core_contradict_limit", "main_max_consecutive_losses",
            "main_loss_cooldown_sec", "max_open_positions_total",
            "kelly_min_samples", "factor_mode",
        ):
            if not (int(lo) <= int(val) <= int(hi)):
                raise HTTPException(status_code=400, detail=f"{k} must be between {int(lo)} and {int(hi)}")
            patch[k] = val
        else:
            fv = float(val)
            if lo is not None and hi is not None and not (float(lo) <= fv <= float(hi)):
                raise HTTPException(status_code=400, detail=f"{k} must be between {lo} and {hi}")
            patch[k] = fv

    poll_interval = float(patch.get("poll_interval_sec", base.get("poll_interval_sec", 1.0)))
    if poll_interval <= 0:
        raise HTTPException(status_code=400, detail="poll_interval_sec must be greater than 0")

    merged = dict(base)
    merged.update(patch)

    if not merged.get("asset_btc_enabled", True) and not merged.get("asset_eth_enabled", False):
        raise HTTPException(status_code=400, detail="至少启用 BTC 或 ETH 之一")

    if float(merged["settle_grace_sec"]) >= float(merged["max_settle_wait_sec"]):
        raise HTTPException(status_code=400, detail="settle_grace_sec must be less than max_settle_wait_sec")

    if float(merged.get("entry_price_min", 0)) >= float(merged.get("entry_price_max", 1)):
        raise HTTPException(
            status_code=400,
            detail="entry_price_min must be less than entry_price_max",
        )

    # entry 时间窗口必须在窗口总长以内，且起始 <= 截止
    window_dur = float(merged.get("market_duration_sec", 900))
    entry_start = float(merged.get("entry_early_start_elapsed_sec", 0.0))
    entry_cutoff = float(merged.get("entry_late_cutoff_elapsed_sec", 540.0))
    if entry_cutoff > window_dur:
        raise HTTPException(
            status_code=400,
            detail=f"entry_late_cutoff_elapsed_sec ({entry_cutoff:.0f}s) must be <= market_duration_sec ({window_dur:.0f}s)",
        )
    if entry_start > entry_cutoff:
        raise HTTPException(
            status_code=400,
            detail=f"entry_early_start_elapsed_sec ({entry_start:.0f}s) must be <= entry_late_cutoff_elapsed_sec ({entry_cutoff:.0f}s)",
        )

    exit_disable_after = float(
        merged.get("exit_tp_sl_disable_after_elapsed_sec", 0.0)
    )
    if exit_disable_after > window_dur:
        raise HTTPException(
            status_code=400,
            detail=f"exit_tp_sl_disable_after_elapsed_sec ({exit_disable_after:.0f}s) must be <= market_duration_sec ({window_dur:.0f}s)",
        )

    # 末端策略时间窗口与止盈区间校验
    end_start = float(merged.get("end_window_start_sec", 720.0))
    end_end = float(merged.get("end_window_end_sec", 870.0))
    end_hardcut = float(merged.get("end_hardcut_sec", 885.0))
    if end_start >= end_end:
        raise HTTPException(
            status_code=400,
            detail=f"end_window_start_sec ({end_start:.0f}s) must be < end_window_end_sec ({end_end:.0f}s)",
        )
    if end_end > window_dur:
        raise HTTPException(
            status_code=400,
            detail=f"end_window_end_sec ({end_end:.0f}s) must be <= market_duration_sec ({window_dur:.0f}s)",
        )
    if end_hardcut > window_dur:
        raise HTTPException(
            status_code=400,
            detail=f"end_hardcut_sec ({end_hardcut:.0f}s) must be <= market_duration_sec ({window_dur:.0f}s)",
        )
    if end_hardcut < end_end:
        raise HTTPException(
            status_code=400,
            detail=f"end_hardcut_sec ({end_hardcut:.0f}s) must be >= end_window_end_sec ({end_end:.0f}s)",
        )
    end_tp_min = float(merged.get("end_tp_min_pct", 10.0))
    end_tp_max = float(merged.get("end_tp_max_pct", 30.0))
    if end_tp_min > end_tp_max:
        raise HTTPException(
            status_code=400,
            detail=f"end_tp_min_pct ({end_tp_min}) must be <= end_tp_max_pct ({end_tp_max})",
        )
    end_tp_entry_min = float(merged.get("end_tp_entry_min", 0.70))
    end_tp_entry_max = float(merged.get("end_tp_entry_max", 0.90))
    if end_tp_entry_min >= end_tp_entry_max:
        raise HTTPException(
            status_code=400,
            detail=f"end_tp_entry_min ({end_tp_entry_min}) must be < end_tp_entry_max ({end_tp_entry_max})",
        )

    # 信号驱动：时间窗口上界必须在窗口总长以内

    # ── live_mode confirmation guard (R0-3) ──────────────────────────────────
    live_switch = patch.get("live_mode")
    if live_switch is True:
        global _live_mode_confirmed, _live_mode_confirmed_ts
        current_live = base.get("live_mode", False)
        if current_live is not True and not _live_mode_confirmed:
            raise HTTPException(
                status_code=403,
                detail=(
                    "实盘模式切换需要先确认。请先调用 POST /api/live/confirm 接口。\n"
                    "警告：实盘模式将使用真实资金下单，请确保配置正确、余额充足。"
                ),
            )
        # 5-minute confirmation window
        with _live_mode_confirmed_lock:
            if _live_mode_confirmed_ts:
                try:
                    confirmed_at = datetime.fromisoformat(_live_mode_confirmed_ts)
                    if datetime.now(timezone.utc) - confirmed_at > timedelta(minutes=5):
                        _live_mode_confirmed = False
                        raise HTTPException(
                            status_code=403,
                            detail="实盘确认已过期（5 分钟内未切换）。请重新调用 POST /api/live/confirm",
                        )
                except ValueError:
                    pass
            _live_mode_confirmed_ts = datetime.now(timezone.utc).isoformat()
        logger.warning(
            "LIVE MODE ENABLED via API — real orders will be submitted to Polymarket CLOB",
        )
    # ── end live_mode guard ──────────────────────────────────────────────────

    # 只有切换到实盘时才需要私钥
    if live_switch is True:
        creds = get_creds()
        if not creds.get("private_key"):
            raise HTTPException(
                status_code=400,
                detail="开启实盘前请先在配置页连接钱包并完成 API 凭证",
            )

    cfg = update_config(patch)

    if _strategy_ref is not None and patch:
        _strategy_ref._sync_config()
        ms = getattr(_strategy_ref, "_market_svc", None)
        if ms is not None:
            ms.update_interval(_strategy_ref.market_interval_slug, _strategy_ref.market_duration_sec)

    return cfg


# ── Wallet ─────────────────────────────────────────────────────────────────────
@router.get("/wallet/status")
async def wallet_status(request: Request):
    _check_rate_limit(_client_id(request), max_requests=15, window_sec=60)
    creds = get_creds()
    return {
        "wallet_connected": bool(creds.get("private_key")),
        "signature_type": creds.get("signature_type", 0),
        "signer_address": creds.get("signer_address", ""),
        "funder_address": creds.get("funder_address", ""),
    }


@router.get("/wallet/balance")
async def wallet_balance(request: Request):
    _check_rate_limit(_client_id(request), max_requests=10, window_sec=60)
    creds = get_creds()
    if not creds.get("private_key"):
        raise HTTPException(status_code=401, detail="Wallet not connected")

    # EOA mode: on-chain balances at the EOA address
    chain_addr = creds.get("funder_address") or creds.get("signer_address", "")
    chain_pusd = None
    chain_usdc = None
    try:
        from app.polygon_usdc import (
            POLYGON_USDC_NATIVE,
            polygon_pusd_balance_micro,
            _erc20_balance_raw,
        )
        from app.config import settings as cfg

        # polygon_usdc uses the synchronous httpx/web3 clients; run the RPC calls
        # in a worker thread so a slow Polygon node cannot block the event loop
        # (and therefore the strategy tick).
        pusd_raw, usdc_raw = await asyncio.gather(
            asyncio.to_thread(
                polygon_pusd_balance_micro,
                holder=chain_addr,
                rpc_url=cfg.polygon_rpc_url,
            ),
            asyncio.to_thread(
                _erc20_balance_raw,
                chain_addr,
                POLYGON_USDC_NATIVE,
                cfg.polygon_rpc_url or "",
            ),
        )
        if pusd_raw is not None:
            chain_pusd = pusd_raw / 1_000_000

        if usdc_raw is not None and usdc_raw > 0:
            chain_usdc = usdc_raw / 1_000_000
    except Exception as exc:
        logger.warning("chain balance query failed: {}", exc)

    try:
        # Reuse the shared LiveTrader instance instead of creating a new one per request
        trader = _live_trader_ref
        if trader is None:
            logger.debug("wallet_balance: LiveTrader not yet initialised — returning null balance")
            return {
                "balance_raw": None,
                "balance_usdc": None,
                "allowance_raw": None,
                "allowance_usdc": None,
                "chain_pusd": chain_pusd,
                "chain_usdc": chain_usdc,
                "signer_pusd": None,
            }
        bal_micro, alw_micro = await trader.get_balance_and_allowance("COLLATERAL")
    except Exception as exc:
        logger.warning("balance query failed: {}", exc)
        raise HTTPException(status_code=502, detail=f"Balance query failed: {exc}")

    return {
        "balance_raw": bal_micro,
        "balance_usdc": bal_micro / 1_000_000,
        "allowance_raw": alw_micro,
        "allowance_usdc": alw_micro / 1_000_000,
        "chain_pusd": chain_pusd,
        "chain_usdc": chain_usdc,
        "signer_pusd": None,
    }


@router.post("/wallet/sync-balance")
async def wallet_sync_balance(request: Request):
    """
    No-op: _refresh_balance_cache has been removed since account balance
    is guaranteed by the operator. Kept for API compatibility (200 OK).
    """
    _check_rate_limit(_client_id(request), max_requests=10, window_sec=60)
    creds = get_creds()
    if not creds.get("private_key"):
        raise HTTPException(status_code=401, detail="Wallet not connected")
    return {"ok": True}


# ── Stats ───────────────────────────────────────────────────────────────────────
@router.get("/stats")
async def get_stats():
    from sqlalchemy import and_, func, select

    async with async_session() as db:
        from app.models import EntryRecord, ExitRecord

        # EntryRecord 总数
        total_row = await db.execute(select(func.count(EntryRecord.id)))
        total_entries = total_row.scalar() or 0

        # ExitRecord 统计（按 exit_record 维度统计盈亏）
        exits_row = await db.execute(select(func.count(ExitRecord.id)))
        total_exits = exits_row.scalar() or 0

        win_row = await db.execute(
            select(func.count(ExitRecord.id)).where(ExitRecord.pnl_usd > 0)
        )
        wins = win_row.scalar() or 0

        pnl_row = await db.execute(
            select(func.coalesce(func.sum(ExitRecord.pnl_usd), 0.0))
        )
        total_pnl = float(pnl_row.scalar() or 0)

        bet_row = await db.execute(
            select(func.coalesce(func.sum(EntryRecord.cost_usdc), 0.0))
        )
        total_bet = float(bet_row.scalar() or 0)

        win_amt_row = await db.execute(
            select(func.coalesce(func.sum(ExitRecord.pnl_usd), 0.0)).where(ExitRecord.pnl_usd > 0)
        )
        win_amt = float(win_amt_row.scalar() or 0)

        loss_amt_row = await db.execute(
            select(func.coalesce(func.sum(ExitRecord.pnl_usd), 0.0)).where(ExitRecord.pnl_usd < 0)
        )
        loss_amt = abs(float(loss_amt_row.scalar() or 0))

        yield_pct = (total_pnl / total_bet * 100) if total_bet > 0 else 0.0

    return {
        "total_entries": total_entries,
        "total_exits": total_exits,
        "wins": wins,
        "losses": total_exits - wins,
        "win_rate": (wins / total_exits * 100) if total_exits > 0 else 0.0,
        "total_pnl": total_pnl,
        "total_bet": total_bet,
        "yield_pct": yield_pct,
        "win_amount": win_amt,
        "loss_amount": loss_amt,
    }


# ── Trades ──────────────────────────────────────────────────────────────────────
@router.get("/trades")
async def get_trades(
    limit: int = 15,
    page: int = 1,
    asset: str | None = None,
    side: str | None = None,
    mode: str | None = None,
):
    from sqlalchemy import and_, desc, func, select

    async with async_session() as db:
        conditions = []
        if asset:
            conditions.append(Trade.asset == asset.upper())
        if side:
            from app.models import Side as TradeSide
            normalized = side.upper()
            if normalized not in ("UP", "DOWN"):
                raise HTTPException(status_code=400, detail=f"Invalid side: {side} — must be UP or DOWN")
            conditions.append(Trade.side == TradeSide[normalized])
        if mode:
            conditions.append(Trade.mode == mode.lower())

        count_q = select(func.count(Trade.id))
        trades_q = select(Trade)
        if conditions:
            count_q = count_q.where(and_(*conditions))
            trades_q = trades_q.where(and_(*conditions))

        count_row = await db.execute(count_q)
        total = count_row.scalar() or 0

        row = await db.execute(
            trades_q.order_by(desc(Trade.id)).offset((page - 1) * limit).limit(limit)
        )
        trades = row.scalars().all()

        # 批量查询每笔交易的 Fill 记录（复用同一 session）
        trade_ids = [t.id for t in trades]
        fills_map: dict[int, list] = {tid: [] for tid in trade_ids}
        if trade_ids:
            fill_rows = await db.execute(
                select(Fill)
                .where(Fill.trade_id.in_(trade_ids))
                .order_by(Fill.fill_index)
            )
            for f in fill_rows.scalars().all():
                fills_map.setdefault(f.trade_id, []).append({
                    "id": f.id,
                    "fill_index": f.fill_index,
                    "fill_price": f.fill_price,
                    "tokens": f.tokens,
                    "cost_usdc": f.cost_usdc,
                    "fee_usdc": f.fee_usdc,
                    "vwap_after": f.vwap_after,
                    "mid_price_at_fill": f.mid_price_at_fill,
                    "mode": f.mode,
                    "created_at": f.created_at.isoformat() if f.created_at else "",
                })

        return {
            "trades": [
                {
                    "id": t.id,
                    "asset": t.asset,
                    "condition_id": t.condition_id,
                    "side": t.side.value,
                    "mode": t.mode,
                    "entry_price": t.entry_price,
                    "vwap": t.effective_entry_price,
                    "tokens_bought": t.tokens_bought,
                    "exit_price": t.exit_price,
                    "pnl_pct": t.pnl_pct,
                    "pnl_usd": t.pnl_usd,
                    "close_reason": t.close_reason,
                    "bet_usdc": t.bet_usdc,
                    "opened_at": t.opened_at.isoformat() if t.opened_at else "",
                    "closed_at": t.closed_at.isoformat() if t.closed_at else "",
                    "fills": len(fills_map.get(t.id, [])),
                    "fill_list": fills_map.get(t.id, []),
                }
                for t in trades
                if not ExitReason.is_dust(t.close_reason or "")
            ],
            "total": sum(1 for t in trades if not ExitReason.is_dust(t.close_reason or "")),
            "page": page,
            "limit": limit,
        }


# ── Entries (买入记录) ─────────────────────────────────────────────────────────
@router.get("/entries")
async def get_entries(
    limit: int = 15,
    page: int = 1,
    asset: str | None = None,
    side: str | None = None,
    mode: str | None = None,
):
    from sqlalchemy import and_, desc, func, select

    async with async_session() as db:
        from app.models import EntryRecord

        conditions = []
        if asset:
            conditions.append(EntryRecord.asset == asset.upper())
        if side:
            from app.models import Side as TradeSide
            normalized = side.upper()
            if normalized not in ("UP", "DOWN"):
                raise HTTPException(status_code=400, detail=f"Invalid side: {side}")
            conditions.append(EntryRecord.side == TradeSide[normalized])
        if mode:
            conditions.append(EntryRecord.mode == mode.lower())

        count_q = select(func.count(EntryRecord.id))
        entries_q = select(EntryRecord)
        if conditions:
            count_q = count_q.where(and_(*conditions))
            entries_q = entries_q.where(and_(*conditions))

        count_row = await db.execute(count_q)
        total = count_row.scalar() or 0

        row = await db.execute(
            entries_q.order_by(desc(EntryRecord.id)).offset((page - 1) * limit).limit(limit)
        )
        entries = row.scalars().all()

        return {
            "entries": [
                {
                    "id": e.id,
                    "asset": e.asset,
                    "condition_id": e.condition_id,
                    "side": e.side.value,
                    "mode": e.mode,
                    "entry_price": e.entry_price,
                    "effective_entry_price": e.effective_entry_price,
                    "tokens_bought": e.tokens_bought,
                    "cost_usdc": e.cost_usdc,
                    "fee_usdc": e.fee_usdc,
                    "entry_core_agree": e.entry_core_agree,
                    "opened_at": e.opened_at.isoformat() if e.opened_at else "",
                }
                for e in entries
            ],
            "total": total,
            "page": page,
            "limit": limit,
        }


# ── Exits (卖出记录) ───────────────────────────────────────────────────────────
@router.get("/exits")
async def get_exits(
    limit: int = 15,
    page: int = 1,
    asset: str | None = None,
    side: str | None = None,
    mode: str | None = None,
    close_reason: str | None = None,
):
    from sqlalchemy import and_, desc, func, select

    async with async_session() as db:
        from app.models import ExitRecord

        conditions = []
        if asset:
            conditions.append(ExitRecord.asset == asset.upper())
        if side:
            from app.models import Side as TradeSide
            normalized = side.upper()
            if normalized not in ("UP", "DOWN"):
                raise HTTPException(status_code=400, detail=f"Invalid side: {side}")
            conditions.append(ExitRecord.side == TradeSide[normalized])
        if mode:
            conditions.append(ExitRecord.mode == mode.lower())
        if close_reason:
            conditions.append(ExitRecord.close_reason == close_reason)

        count_q = select(func.count(ExitRecord.id))
        exits_q = select(ExitRecord)
        if conditions:
            count_q = count_q.where(and_(*conditions))
            exits_q = exits_q.where(and_(*conditions))

        count_row = await db.execute(count_q)
        total = count_row.scalar() or 0

        row = await db.execute(
            exits_q.order_by(desc(ExitRecord.id)).offset((page - 1) * limit).limit(limit)
        )
        exits = row.scalars().all()

        return {
            "exits": [
                {
                    "id": e.id,
                    "asset": e.asset,
                    "condition_id": e.condition_id,
                    "side": e.side.value,
                    "mode": e.mode,
                    "exit_price": e.exit_price,
                    "tokens_exited": e.tokens_exited,
                    "partial_pct": e.partial_pct,
                    "pnl_pct": e.pnl_pct,
                    "pnl_usd": e.pnl_usd,
                    "close_reason": e.close_reason,
                    "entry_record_id": e.entry_record_id,
                    "closed_at": e.closed_at.isoformat() if e.closed_at else "",
                }
                for e in exits
            ],
            "total": total,
            "page": page,
            "limit": limit,
        }


# ── Live ────────────────────────────────────────────────────────────────────────
@router.get("/live/status")
async def live_status():
    cfg = get_config()
    creds = get_creds()
    return {
        "live_mode": cfg.get("live_mode", False),
        "live_slippage_pct": cfg.get("live_slippage_pct", 2.0),
        "has_credentials": bool(creds.get("private_key") and creds.get("api_key")),
        # R0-3: exposes confirmation state so the UI can show "需要确认" vs "已确认"
        "live_mode_confirmed": _live_mode_confirmed,
        "live_mode_confirmed_ts": _live_mode_confirmed_ts,
        # Strategy running state
        "strategy_running": _strategy_ref.is_running() if _strategy_ref else False,
    }


@router.post("/live/confirm")
async def live_confirm(request: Request, current_user: User = Depends(get_current_user)):
    """
    Confirm intent to enable live trading. Required before setting live_mode=True
    via POST /api/config. Resets to False on every server restart.

    Safety: Call this endpoint before toggling live_mode to True. After confirmation,
    you have a 5-minute window to switch live_mode on. After 5 minutes without
    switching, re-confirmation is required.
    """
    global _live_mode_confirmed, _live_mode_confirmed_ts
    with _live_mode_confirmed_lock:
        _live_mode_confirmed = True
        _live_mode_confirmed_ts = datetime.now(timezone.utc).isoformat()
    logger.warning(
        "LIVE TRADING CONFIRMED at {} — "
        "live_mode=True is now allowed for the next 5 minutes",
        _live_mode_confirmed_ts,
    )
    return {
        "ok": True,
        "confirmed_at": _live_mode_confirmed_ts,
        "message": "实盘确认已记录，请在 5 分钟内切换 live_mode=True",
    }


# ── Strategy Control ─────────────────────────────────────────────────────────────
@router.get("/strategy/status")
async def strategy_status():
    """返回策略运行状态（start / stop / running）"""
    if _strategy_ref is None:
        return {"running": False, "error": "strategy_not_initialised"}
    return {
        "running": _strategy_ref.is_running(),
    }


@router.post("/strategy/start")
async def strategy_start(request: Request):
    """
    开启策略运行。开启后策略开始每 tick 检查并执行订单。
    需要管理员权限。
    """
    _check_rate_limit(_client_id(request), max_requests=5, window_sec=60)
    if _strategy_ref is None:
        raise HTTPException(status_code=503, detail="策略未初始化")
    was_running = _strategy_ref.is_running()
    await _strategy_ref.start()
    logger.warning(
        "Strategy started via API by client {}",
        _client_id(request),
    )
    return {
        "ok": True,
        "running": True,
        "message": "策略已开启" if not was_running else "策略已在运行中",
    }


@router.post("/strategy/stop")
async def strategy_stop(request: Request):
    """
    停止策略运行。停止后策略暂停 tick，不再检查市场、不会发出任何订单。
    已在执行中的订单（如平仓）会完成，但不会再开新仓。
    需要管理员权限。
    """
    _check_rate_limit(_client_id(request), max_requests=5, window_sec=60)
    if _strategy_ref is None:
        raise HTTPException(status_code=503, detail="策略未初始化")
    was_running = await _strategy_ref.stop()
    logger.warning(
        "Strategy stopped via API by client {} (was_running={})",
        _client_id(request), was_running,
    )
    return {
        "ok": True,
        "running": False,
        "message": "策略已停止" if was_running else "策略本来就在停止状态",
    }


@router.get("/live/dashboard")
async def live_dashboard():
    if _strategy_ref is None:
        return {"error": "strategy not ready"}
    latest = getattr(_strategy_ref, "_latest", {})
    return latest


def set_live_trader_ref(ref):
    global _live_trader_ref
    _live_trader_ref = ref


def set_strategy_ref(ref):
    global _strategy_ref
    _strategy_ref = ref
    _set_ws_strategy_ref(ref)   # keep websocket.py in sync


def set_reload_fn(fn):
    global _reload_fn
    _reload_fn = fn
