"""
analysis_routes.py — 策略表现分析接口

用途：
  - /api/analysis/performance：按 strategy / Core 共识数 / 入场价格区间 / 平仓原因
    统计历史 Trade 的胜率、PnL、profit factor，用于校准信号和仓位。
  - /api/analysis/decisions：查看决策快照（Core 6 / Advanced 5 完整详情）。
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query
from sqlalchemy import desc, select

from app.database import async_session
from app.models import DecisionSnapshot, Trade

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


def _price_bucket(price: float) -> str:
    if price <= 0:
        return "unknown"
    if price < 0.30:
        return "<0.30"
    if price < 0.40:
        return "0.30-0.40"
    if price < 0.50:
        return "0.40-0.50"
    if price < 0.60:
        return "0.50-0.60"
    return ">=0.60"


def _elapsed_bucket(sec: float) -> str:
    if sec < 120:
        return "0-2min"
    if sec < 240:
        return "2-4min"
    if sec < 360:
        return "4-6min"
    if sec < 540:
        return "6-9min"
    return ">=9min"


def _agg(rows: list[dict]) -> dict:
    n = len(rows)
    if n == 0:
        return {
            "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "total_pnl_usd": 0.0, "avg_pnl_usd": 0.0,
            "total_bet_usdc": 0.0, "return_pct": 0.0,
            "profit_factor": None,
        }
    wins = [r for r in rows if float(r.get("pnl_usd") or 0.0) > 0]
    losses = [r for r in rows if float(r.get("pnl_usd") or 0.0) < 0]
    total_pnl = sum(float(r.get("pnl_usd") or 0.0) for r in rows)
    total_bet = sum(float(r.get("bet_usdc") or 0.0) for r in rows)
    win_sum = sum(float(r.get("pnl_usd") or 0.0) for r in wins)
    loss_sum = abs(sum(float(r.get("pnl_usd") or 0.0) for r in losses))
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / n * 100.0, 2),
        "total_pnl_usd": round(total_pnl, 4),
        "avg_pnl_usd": round(total_pnl / n, 4),
        "total_bet_usdc": round(total_bet, 4),
        "return_pct": round(total_pnl / total_bet * 100.0, 2) if total_bet > 0 else 0.0,
        "profit_factor": round(win_sum / loss_sum, 3) if loss_sum > 0 else None,
    }


@router.get("/performance")
async def performance(
    days: int = Query(30, ge=1, le=3650),
    strategy: str = Query("all"),
):
    """按多个维度统计历史交易表现。"""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with async_session() as db:
        result = await db.execute(
            select(Trade)
            .where(Trade.closed_at >= cutoff)
            .order_by(desc(Trade.closed_at))
        )
        trades = result.scalars().all()

    rows = []
    for t in trades:
        strat = (t.strategy or "main").lower()
        if strategy.lower() != "all" and strat != strategy.lower():
            continue
        rows.append({
            "strategy": strat,
            "core_agree": int(t.entry_core_agree or 0),
            "price_bucket": _price_bucket(float(t.entry_price or 0.0)),
            "close_reason": t.close_reason or "",
            "pnl_usd": float(t.pnl_usd or 0.0),
            "pnl_pct": float(t.pnl_pct or 0.0),
            "bet_usdc": float(t.bet_usdc or 0.0),
        })

    def group(key_fn) -> dict:
        groups: dict[str, list[dict]] = {}
        for r in rows:
            groups.setdefault(str(key_fn(r)), []).append(r)
        return {k: _agg(v) for k, v in sorted(groups.items())}

    return {
        "days": days,
        "strategy_filter": strategy,
        "overall": _agg(rows),
        "by_strategy": group(lambda r: r["strategy"]),
        "by_core_agree": group(lambda r: f"{r['core_agree']}/6"),
        "by_price_bucket": group(lambda r: r["price_bucket"]),
        "by_close_reason": group(lambda r: r["close_reason"] or "unknown"),
    }


@router.get("/signal-outcomes")
async def signal_outcomes(
    days: int = Query(30, ge=1, le=3650),
):
    """
    只统计真正入场（entered=1）的决策快照，按信号特征统计实际胜率。

    用于回答：哪个 Advanced filter / 方向 / Core 共识桶 / 价格桶真正赚钱。
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with async_session() as db:
        result = await db.execute(
            select(DecisionSnapshot, Trade)
            .join(
                Trade,
                (Trade.condition_id == DecisionSnapshot.condition_id)
                & (Trade.strategy == "main"),
            )
            .where(
                DecisionSnapshot.entered == 1,
                Trade.closed_at >= cutoff,
            )
        )
        pairs = result.all()

    rows = []
    for snap, trade in pairs:
        rows.append({
            "advanced_filter": snap.advanced_filter or "neutral",
            "direction": snap.direction or "NONE",
            "core_agree": int(snap.core_agree or 0),
            "price_bucket": _price_bucket(float(snap.decision_price or 0.0)),
            "elapsed_bucket": _elapsed_bucket(float(snap.elapsed_sec or 0.0)),
            "pnl_usd": float(trade.pnl_usd or 0.0),
            "pnl_pct": float(trade.pnl_pct or 0.0),
            "bet_usdc": float(trade.bet_usdc or 0.0),
        })

    def group(key_fn) -> dict:
        groups: dict[str, list[dict]] = {}
        for r in rows:
            groups.setdefault(str(key_fn(r)), []).append(r)
        return {k: _agg(v) for k, v in sorted(groups.items())}

    return {
        "days": days,
        "overall": _agg(rows),
        "by_advanced_filter": group(lambda r: r["advanced_filter"]),
        "by_direction": group(lambda r: r["direction"]),
        "by_core_agree": group(lambda r: f"{r['core_agree']}/6"),
        "by_price_bucket": group(lambda r: r["price_bucket"]),
        "by_elapsed_bucket": group(lambda r: r["elapsed_bucket"]),
    }


CORE_NAMES = ("rsi", "macd", "vwap", "ema", "pivot", "bbands")
# 趋势组：与 EntryController.core_trend_min_agree 保持一致
TREND_NAMES = ("macd", "vwap", "ema")


def _core_flags(core: dict, direction: str) -> dict[str, bool]:
    """单条决策快照中，各 Core 指标是否与最终方向同向且已确认。"""
    out: dict[str, bool] = {}
    dir_up = str(direction or "").upper()
    for name in CORE_NAMES:
        item = core.get(name) or {}
        out[name] = (
            bool(item.get("agree"))
            and str(item.get("direction", "")).upper() == dir_up
        )
    return out


@router.get("/attribution")
async def attribution(
    days: int = Query(30, ge=1, le=3650),
    trend_quorum: int = Query(2, ge=0, le=3),
):
    """
    已入场决策的单指标归因 + 规则回放。

    只统计 entered=1 的决策快照，并用 condition_id 关联真实 Trade PnL。
    返回：
      - by_indicator：每个 Core 指标同向/不同向时的胜率与 PnL
      - by_core_agree / by_advanced_filter / by_price_bucket / by_trend_agree
      - what_if：跳过 Core 4/6、跳过 Advanced REDUCE、趋势组法定人数、
        高价减仓等候选规则的假设结果（基于历史 PnL 线性缩放）
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with async_session() as db:
        result = await db.execute(
            select(DecisionSnapshot, Trade)
            .join(
                Trade,
                (Trade.condition_id == DecisionSnapshot.condition_id)
                & (Trade.strategy == "main"),
            )
            .where(
                DecisionSnapshot.entered == 1,
                Trade.closed_at >= cutoff,
            )
        )
        pairs = result.all()

    rows: list[dict] = []
    seen: set[str] = set()
    for snap, trade in pairs:
        cid = snap.condition_id or ""
        # 同一 condition 只允许一笔主策略交易；去重防止 join 重复
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        try:
            core = json.loads(snap.core_json) if snap.core_json else {}
        except Exception:
            core = {}
        flags = _core_flags(core, snap.direction)
        trend_agree = sum(1 for n in TREND_NAMES if flags.get(n))
        price = float(snap.decision_price or 0.0)
        rows.append({
            "core_agree": int(snap.core_agree or 0),
            "advanced_filter": str(snap.advanced_filter or "neutral"),
            "price": price,
            "price_bucket": _price_bucket(price),
            "direction": snap.direction or "NONE",
            "trend_agree": trend_agree,
            "flags": flags,
            "pnl_usd": float(trade.pnl_usd or 0.0),
            "pnl_pct": float(trade.pnl_pct or 0.0),
            "bet_usdc": float(trade.bet_usdc or 0.0),
        })

    def group(key_fn) -> dict:
        groups: dict[str, list[dict]] = {}
        for r in rows:
            groups.setdefault(str(key_fn(r)), []).append(r)
        return {k: _agg(v) for k, v in sorted(groups.items())}

    def agg_where(pred) -> dict:
        return _agg([r for r in rows if pred(r)])

    by_indicator = {
        name: {
            "agree": agg_where(lambda r, n=name: r["flags"].get(n, False)),
            "disagree": agg_where(lambda r, n=name: not r["flags"].get(n, False)),
        }
        for name in CORE_NAMES
    }

    # 规则回放：候选入场规则在历史已入场样本上的表现
    def _recommended_rows(scale_high_price: bool) -> list[dict]:
        out = []
        for r in rows:
            if r["core_agree"] == 4:
                continue
            if r["advanced_filter"] == "reduce":
                continue
            if r["trend_agree"] < trend_quorum:
                continue
            scale = 0.5 if (scale_high_price and r["price"] >= 0.50) else 1.0
            out.append({
                **r,
                "pnl_usd": r["pnl_usd"] * scale,
                "bet_usdc": r["bet_usdc"] * scale,
            })
        return out

    what_if = {
        "baseline": _agg(rows),
        "skip_core4": agg_where(lambda r: r["core_agree"] != 4),
        "skip_advanced_reduce": agg_where(lambda r: r["advanced_filter"] != "reduce"),
        "skip_core4_and_reduce": agg_where(
            lambda r: r["core_agree"] != 4 and r["advanced_filter"] != "reduce"
        ),
        f"trend_quorum_{trend_quorum}": agg_where(
            lambda r: r["trend_agree"] >= trend_quorum
        ),
        "price_le_050": agg_where(lambda r: 0 < r["price"] <= 0.50),
        "recommended": _agg(_recommended_rows(scale_high_price=False)),
        "recommended_high_price_half": _agg(_recommended_rows(scale_high_price=True)),
    }

    return {
        "days": days,
        "entries": len(rows),
        "trend_quorum": trend_quorum,
        "baseline": _agg(rows),
        "by_indicator": by_indicator,
        "by_core_agree": group(lambda r: f"{r['core_agree']}/6"),
        "by_advanced_filter": group(lambda r: r["advanced_filter"]),
        "by_price_bucket": group(lambda r: r["price_bucket"]),
        "by_trend_agree": group(lambda r: f"{r['trend_agree']}/3"),
        "what_if": what_if,
    }


FACTOR_NAMES = ("trend", "reversion", "price", "micro", "flow")


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = min(len(xs), len(ys))
    if n < 3:
        return None
    x = xs[:n]
    y = ys[:n]
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    dx = math.sqrt(sum((a - mx) ** 2 for a in x))
    dy = math.sqrt(sum((b - my) ** 2 for b in y))
    if dx <= 1e-12 or dy <= 1e-12:
        return None
    return round(num / (dx * dy), 4)


@router.get("/factors")
async def factor_analysis(
    days: int = Query(30, ge=1, le=3650),
    only_entered: bool = Query(False),
):
    """
    因子相关性 / IC 分析。

    - correlation: 因子分数的两两相关矩阵（用于发现冗余）
    - ic: 因子分数与主策略 Trade PnL 的相关性（样本 >= 3）
    - win_avg / loss_avg: 盈利/亏损交易的平均因子分数
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with async_session() as db:
        stmt = select(DecisionSnapshot).where(
            DecisionSnapshot.created_at >= cutoff,
            DecisionSnapshot.factors_json != "",
        )
        if only_entered:
            stmt = stmt.where(DecisionSnapshot.entered == 1)
        result = await db.execute(stmt)
        snaps = result.scalars().all()

        condition_ids = list({s.condition_id for s in snaps if s.condition_id})
        pnl_by_cid: dict[str, float] = {}
        if condition_ids:
            trade_rows = await db.execute(
                select(Trade.condition_id, Trade.pnl_usd).where(
                    Trade.strategy == "main",
                    Trade.condition_id.in_(condition_ids),
                )
            )
            for cid, pnl in trade_rows.all():
                pnl_by_cid[cid] = float(pnl or 0.0)

    rows: list[tuple[dict, float | None, bool]] = []
    for s in snaps:
        try:
            data = json.loads(s.factors_json) if s.factors_json else {}
        except Exception:
            continue
        scores = {
            name: float(data.get(name, {}).get("score", 0.0) or 0.0)
            for name in FACTOR_NAMES
        }
        rows.append((scores, pnl_by_cid.get(s.condition_id), bool(s.entered)))

    # 相关矩阵
    corr: dict[str, dict[str, float | None]] = {}
    for a in FACTOR_NAMES:
        corr[a] = {}
        for b in FACTOR_NAMES:
            xs = [r[0][a] for r in rows]
            ys = [r[0][b] for r in rows]
            corr[a][b] = _pearson(xs, ys)

    # IC + 盈/亏平均分
    # IC/盈亏损只使用真正入场的快照，避免同一窗口多份快照共享同一个 PnL
    outcome_rows = [(r[0], r[1]) for r in rows if r[1] is not None and r[2]]
    ic: dict[str, float | None] = {}
    win_avg: dict[str, float | None] = {}
    loss_avg: dict[str, float | None] = {}
    for name in FACTOR_NAMES:
        xs = [float(r[0][name]) for r in outcome_rows]
        ys = [float(r[1]) for r in outcome_rows]
        ic[name] = _pearson(xs, ys)
        wins = [float(r[0][name]) for r in outcome_rows if (r[1] or 0.0) > 0]
        losses = [float(r[0][name]) for r in outcome_rows if (r[1] or 0.0) < 0]
        win_avg[name] = round(sum(wins) / len(wins), 4) if wins else None
        loss_avg[name] = round(sum(losses) / len(losses), 4) if losses else None

    return {
        "days": days,
        "snapshots": len(rows),
        "with_outcome": len(outcome_rows),
        "correlation": corr,
        "ic": ic,
        "win_avg_score": win_avg,
        "loss_avg_score": loss_avg,
    }


@router.get("/decisions")
async def decisions(
    limit: int = Query(100, ge=1, le=1000),
    asset: str | None = Query(None),
    only_entered: bool = Query(False),
):
    """查看最近的决策快照。"""
    async with async_session() as db:
        stmt = select(DecisionSnapshot)
        if asset:
            stmt = stmt.where(DecisionSnapshot.asset == asset.upper())
        if only_entered:
            stmt = stmt.where(DecisionSnapshot.entered == 1)
        stmt = stmt.order_by(desc(DecisionSnapshot.id)).limit(limit)
        result = await db.execute(stmt)
        snaps = result.scalars().all()

    def _load(raw: str) -> dict:
        try:
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    return {
        "count": len(snaps),
        "decisions": [
            {
                "id": s.id,
                "created_at": s.created_at.isoformat() if s.created_at else "",
                "asset": s.asset,
                "condition_id": s.condition_id,
                "bar_ts": s.bar_ts,
                "window_start": s.window_start.isoformat() if s.window_start else "",
                "elapsed_sec": s.elapsed_sec,
                "mode": s.mode,
                "up_mid": s.up_mid,
                "down_mid": s.down_mid,
                "decision_price": s.decision_price,
                "direction": s.direction,
                "core_agree": s.core_agree,
                "core_contradict": s.core_contradict,
                "strength": s.strength,
                "advanced_filter": s.advanced_filter,
                "suggested_action": s.suggested_action,
                "skip_reason": s.skip_reason,
                "entered": bool(s.entered),
                "entry_record_id": s.entry_record_id,
                "core": _load(s.core_json),
                "advanced": _load(s.advanced_json),
                "factor_mode": s.factor_mode,
                "factor_net": s.factor_net,
                "factor_agreement": s.factor_agreement,
                "factor_conflict": s.factor_conflict,
                "factor_direction": s.factor_direction,
                "factors": _load(s.factors_json),
            }
            for s in snaps
        ],
    }
