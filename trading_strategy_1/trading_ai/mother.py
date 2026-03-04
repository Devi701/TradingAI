from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Callable

from .child import ChildRuntime
from .config import MotherSpec, Settings
from .indicators import max_drawdown, sharpe_ratio
from .ledger import Ledger
from .models import ChildDNA, OrderIntent, VirtualFill


class MotherRuntime:
    def __init__(
        self,
        settings: Settings,
        spec: MotherSpec,
        ledger: Ledger,
        bus,
        forward_intent: Callable[[OrderIntent], object],
        frozen_getter: Callable[[], bool],
        macro_mode_getter: Callable[[], str],
    ) -> None:
        self.settings = settings
        self.spec = spec
        self.ledger = ledger
        self.bus = bus
        self.forward_intent = forward_intent
        self.frozen_getter = frozen_getter
        self.macro_mode_getter = macro_mode_getter

        self.allowance: float = 0.0
        self.children: dict[str, ChildRuntime] = {}
        self.child_tasks: dict[str, asyncio.Task] = {}
        self.running = False
        self.last_heartbeat = datetime.now(timezone.utc)
        self.dynamic_min_confidence: float = 0.0
        self.dynamic_risk_scale: float = 1.0
        self.aggressive_whitelist: set[str] = set()
        self.aggressive_blacklist: set[str] = set()

        self._report_task: asyncio.Task | None = None
        self._dna_snapshot_task: asyncio.Task | None = None

    async def start(self, restored_children: list[tuple[str, ChildDNA, float, float, float]] | None = None) -> None:
        self.running = True
        if restored_children:
            for child_id, dna, pnl, qty, entry in restored_children:
                await self._spawn_child(dna, child_id=child_id, existing_pnl=pnl, position_qty=qty, position_entry=entry)
        else:
            # Try to assign unique symbols to initial children to increase strategy diversity
            used_symbols: set[str] = set()
            max_children = max(0, int(self.settings.mother_children_per_strategy))
            symbol_pool = list(self.spec.symbols)
            for _ in range(max_children):
                dna = self._random_dna()
                # If we have a pool of symbols, prefer unused ones to avoid many children targeting same ticker
                attempts = 0
                while dna.ticker in used_symbols and attempts < 10 and len(used_symbols) < len(symbol_pool):
                    dna = self._random_dna()
                    attempts += 1
                used_symbols.add(dna.ticker)
                await self._spawn_child(dna)

        self._report_task = asyncio.create_task(self._hourly_report_loop(), name=f"mother-report-{self.spec.id}")
        self._dna_snapshot_task = asyncio.create_task(self._dna_snapshot_loop(), name=f"mother-dna-{self.spec.id}")

    async def stop(self) -> None:
        self.running = False
        for child in self.children.values():
            child.stop()
        for task in self.child_tasks.values():
            task.cancel()
        if self._report_task:
            self._report_task.cancel()
        if self._dna_snapshot_task:
            self._dna_snapshot_task.cancel()
        await asyncio.gather(*self.child_tasks.values(), return_exceptions=True)

    async def submit_intent(self, intent: OrderIntent) -> bool:
        if not self.running:
            return False
        self.last_heartbeat = datetime.now(timezone.utc)

        if self.frozen_getter():
            await self.ledger.persist_intent(intent, status="rejected_frozen")
            return False

        if not self._constraint_mask(intent):
            await self.ledger.persist_intent(intent, status="rejected_by_mother")
            return False

        await self.ledger.persist_intent(intent, status="approved_by_mother")
        await self.forward_intent(intent)
        # Emit a concise console log that a child requested grandmother to trade.
        intent_logger = logging.getLogger("trading_ai.intent")
        intent_logger.info(
            "intent_submitted child_id=%s mother_id=%s symbol=%s side=%s qty=%.6f confidence=%.3f reason=%s",
            intent.child_id,
            intent.mother_id,
            intent.symbol,
            intent.side,
            intent.qty,
            intent.confidence,
            intent.reason,
        )
        return True

    def heartbeat(self) -> bool:
        self.last_heartbeat = datetime.now(timezone.utc)
        if not self.running:
            return False
        dead_tasks = [task for task in self.child_tasks.values() if task.done()]
        return len(dead_tasks) <= max(1, int(len(self.child_tasks) * 0.2))

    def portfolio_metrics(self) -> dict[str, float]:
        pnls = [child.performance.pnl for child in self.children.values()]
        returns: list[float] = []
        wins = 0
        trades = 0
        curves: list[float] = []
        for child in self.children.values():
            returns.extend(child.performance.returns)
            wins += child.performance.wins
            trades += child.performance.trade_count
            if child.performance.equity_curve:
                curves.extend(child.performance.equity_curve)

        pnl_total = sum(pnls)
        sharpe = sharpe_ratio(returns)
        win_rate = (wins / trades) if trades > 0 else 0.0
        drawdown = max_drawdown(curves)
        return {
            "pnl": pnl_total,
            "sharpe": sharpe,
            "win_rate": win_rate,
            "drawdown": drawdown,
            "trades": float(trades),
        }

    def portfolio_score(self) -> float:
        metrics = self.portfolio_metrics()
        pnl = metrics["pnl"]
        sharpe = metrics["sharpe"]
        drawdown = metrics["drawdown"]

        if self.spec.fitness_objective == "net_profit":
            return pnl + (sharpe * 100) - (drawdown * 150)
        return (sharpe * 200) + (pnl * 0.1) - (drawdown * 200)

    def pnl_total(self) -> float:
        return sum(child.performance.pnl for child in self.children.values())

    def get_allowance(self) -> float:
        return self.allowance * self.dynamic_risk_scale

    def set_allowance(self, value: float) -> None:
        self.allowance = max(value, 0.0)

    def set_dynamic_controls(self, min_confidence: float, risk_scale: float) -> None:
        self.dynamic_min_confidence = max(0.0, min(1.0, min_confidence))
        self.dynamic_risk_scale = max(0.1, min(1.0, risk_scale))

    def set_aggressive_guidance(self, whitelist: set[str], blacklist: set[str]) -> None:
        if self.spec.id not in {"speedy", "chaos"}:
            self.aggressive_whitelist = set()
            self.aggressive_blacklist = set()
            return
        self.aggressive_whitelist = set(whitelist)
        self.aggressive_blacklist = set(blacklist)

    async def evolve(self) -> None:
        if not self.children:
            return

        target_size = self.settings.mother_children_per_strategy
        ranked = sorted(self.children.values(), key=self._child_score, reverse=True)
        kill_n = min(self.settings.mother_kill_count_per_cycle, max(len(ranked) - 2, 0))

        losers = ranked[-kill_n:] if kill_n > 0 else []
        for loser in losers:
            await self._remove_child(loser.id)

        survivors = sorted(self.children.values(), key=self._child_score, reverse=True)
        parents = survivors[: min(4, len(survivors))]

        while len(self.children) < target_size and len(parents) >= 2:
            p1, p2 = self._tournament_pair(parents)
            child_dna = self._crossover_mutate(p1.dna, p2.dna)
            await self._spawn_child(child_dna)

        while len(self.children) > target_size:
            worst = min(self.children.values(), key=self._child_score)
            await self._remove_child(worst.id)

        await self.ledger.log_event(
            "INFO",
            f"mother.{self.spec.id}",
            "evolution_cycle_complete",
            {
                "children": len(self.children),
                "killed": kill_n,
                "fitness": self.spec.fitness_objective,
                "score": self.portfolio_score(),
            },
        )

    async def _spawn_child(
        self,
        dna: ChildDNA,
        child_id: str | None = None,
        existing_pnl: float = 0.0,
        position_qty: float = 0.0,
        position_entry: float = 0.0,
    ) -> None:
        child = ChildRuntime(
            settings=self.settings,
            mother_id=self.spec.id,
            dna=dna,
            submit_intent=self.submit_intent,
            allowance_getter=self.get_allowance,
            child_id=child_id,
        )
        child.performance.pnl = existing_pnl
        child.position.qty = position_qty
        child.position.entry_price = position_entry

        self.children[child.id] = child
        self.child_tasks[child.id] = asyncio.create_task(
            child.run(self.bus, frozen_getter=self.frozen_getter),
            name=f"child-{child.id}",
        )
        await self.ledger.upsert_child(
            child.id,
            self.spec.id,
            child.dna,
            "alive",
            child.performance.pnl,
            child.position.qty,
            child.position.entry_price,
        )

    async def _remove_child(self, child_id: str) -> None:
        child = self.children.pop(child_id, None)
        task = self.child_tasks.pop(child_id, None)
        if child:
            child.stop()
            await self.ledger.upsert_child(
                child.id,
                self.spec.id,
                child.dna,
                "pruned",
                child.performance.pnl,
                child.position.qty,
                child.position.entry_price,
            )
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _constraint_mask(self, intent: OrderIntent) -> bool:
        if intent.symbol not in self.spec.symbols:
            return False

        open_positions = sum(1 for c in self.children.values() if c.position.qty > 0)
        if intent.side == "buy" and open_positions >= self.settings.child_max_open_positions:
            return False

        if intent.side == "buy" and intent.confidence < self.dynamic_min_confidence:
            return False

        indicators = intent.indicators
        if self.spec.id == "bargain" and intent.side == "buy":
            rsi_val = indicators.get("rsi")
            if rsi_val is not None and rsi_val > 40:
                return False

        if self.spec.id == "techie" and intent.side == "buy":
            ema_fast = indicators.get("ema_fast")
            ema_slow = indicators.get("ema_slow")
            trend = indicators.get("trend")
            if ema_fast is not None and ema_slow is not None and ema_fast <= ema_slow:
                return False
            if trend is not None and trend <= 0:
                return False

        if self.spec.id == "chaos" and intent.side == "buy":
            volume_spike = indicators.get("volume_spike", 0.0)
            breakout = indicators.get("breakout_strength", 0.0)
            if volume_spike < 1.4 and breakout <= 0:
                return False

        if self.spec.id == "speedy" and intent.confidence < 0.2:
            return False

        if (
            self.spec.id in {"speedy", "chaos"}
            and intent.side == "buy"
            and intent.symbol in self.aggressive_blacklist
        ):
            return False

        macro_mode = self.macro_mode_getter()
        if macro_mode == "risk_off" and self.spec.id in {"speedy", "chaos"} and intent.side == "buy":
            return intent.confidence >= 0.65

        return True

    def _child_score(self, child: ChildRuntime) -> float:
        perf = child.performance
        sharpe = sharpe_ratio(perf.returns)
        drawdown = max_drawdown(perf.equity_curve)
        win_rate = perf.wins / perf.trade_count if perf.trade_count > 0 else 0.0

        if self.spec.fitness_objective == "net_profit":
            return perf.pnl + sharpe * 35 + win_rate * 20 - drawdown * 120
        return sharpe * 120 + win_rate * 30 + perf.pnl * 0.15 - drawdown * 150

    def _style_signal_pool(self) -> list[str]:
        if self.spec.id == "speedy":
            return ["HYBRID", "BREAKOUT", "MACD"]
        if self.spec.id == "techie":
            return ["EMA_CROSS", "MACD", "HYBRID"]
        if self.spec.id == "bargain":
            return ["RSI", "ZSCORE_REVERT", "EMA_CROSS"]
        if self.spec.id == "chaos":
            return ["VOLUME_SPIKE", "BREAKOUT", "HYBRID"]
        return ["EMA_CROSS", "RSI", "ZSCORE_REVERT"]

    def _random_dna(self) -> ChildDNA:
        signal = random.choice(self._style_signal_pool())
        symbol = random.choice(self.spec.symbols)

        if signal == "EMA_CROSS":
            params = [random.randint(7, 15), random.randint(20, 40)]
        elif signal == "RSI":
            params = [14, random.randint(20, 35), random.randint(65, 80)]
        elif signal == "MACD":
            params = [12, 26, 9]
        elif signal == "VOLUME_SPIKE":
            params = [random.randint(15, 30)]
        elif signal == "ZSCORE_REVERT":
            params = [random.randint(15, 30), round(random.uniform(1.2, 2.0), 2)]
        elif signal == "BREAKOUT":
            params = [random.randint(15, 35)]
        else:
            params = [12, 26, 14]

        # Style-specific risk envelope.
        if self.spec.id in {"speedy", "chaos"}:
            stop_loss = random.uniform(0.01, 0.03)
            take_profit = random.uniform(0.03, 0.08)
            max_position = min(self.settings.child_max_position_pct, random.uniform(0.04, 0.10))
            risk_bias = random.uniform(0.9, 1.3)
        elif self.spec.id == "bargain":
            stop_loss = random.uniform(0.02, 0.05)
            take_profit = random.uniform(0.03, 0.09)
            max_position = min(self.settings.child_max_position_pct, random.uniform(0.03, 0.08))
            risk_bias = random.uniform(0.7, 1.0)
        else:
            stop_loss = random.uniform(0.015, 0.04)
            take_profit = random.uniform(0.03, 0.07)
            max_position = min(self.settings.child_max_position_pct, random.uniform(0.02, 0.07))
            risk_bias = random.uniform(0.6, 1.0)

        return ChildDNA(
            ticker=symbol,
            entry_signal=signal,
            params=[float(p) for p in params],
            stop_loss=float(round(stop_loss, 4)),
            take_profit=float(round(take_profit, 4)),
            max_position_pct=float(round(max_position, 4)),
            risk_bias=float(round(risk_bias, 3)),
        )

    def _tournament_pair(self, parents: list[ChildRuntime]) -> tuple[ChildRuntime, ChildRuntime]:
        sample_size = min(3, len(parents))
        a_pool = random.sample(parents, sample_size)
        b_pool = random.sample(parents, sample_size)
        a = max(a_pool, key=self._child_score)
        b = max(b_pool, key=self._child_score)
        return a, b

    def _crossover_mutate(self, a: ChildDNA, b: ChildDNA) -> ChildDNA:
        signal = random.choice([a.entry_signal, b.entry_signal])
        symbol = random.choice(self.spec.symbols)

        max_len = max(len(a.params), len(b.params))
        params: list[float] = []
        for i in range(max_len):
            av = a.params[i] if i < len(a.params) else b.params[i]
            bv = b.params[i] if i < len(b.params) else a.params[i]
            base = random.choice([av, bv, (av + bv) / 2])
            if random.random() < self.settings.mother_mutation_rate:
                base *= 1 + random.uniform(-self.settings.mother_mutation_step, self.settings.mother_mutation_step)
            params.append(max(0.001, float(base)))

        stop_loss = (a.stop_loss + b.stop_loss) / 2
        take_profit = (a.take_profit + b.take_profit) / 2
        max_position = (a.max_position_pct + b.max_position_pct) / 2
        risk_bias = (a.risk_bias + b.risk_bias) / 2

        if random.random() < self.settings.mother_mutation_rate:
            stop_loss *= 1 + random.uniform(-0.2, 0.2)
        if random.random() < self.settings.mother_mutation_rate:
            take_profit *= 1 + random.uniform(-0.2, 0.2)
        if random.random() < self.settings.mother_mutation_rate:
            max_position *= 1 + random.uniform(-0.25, 0.25)
        if random.random() < self.settings.mother_mutation_rate:
            risk_bias *= 1 + random.uniform(-0.2, 0.2)

        return ChildDNA(
            ticker=symbol,
            entry_signal=signal,
            params=[round(x, 4) for x in params],
            stop_loss=max(0.005, round(stop_loss, 4)),
            take_profit=max(0.01, round(take_profit, 4)),
            max_position_pct=min(self.settings.child_max_position_pct, max(0.01, round(max_position, 4))),
            risk_bias=max(0.3, min(1.8, round(risk_bias, 4))),
        )

    async def _hourly_report_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.mother_report_interval_min * 60)
            metrics = self.portfolio_metrics()
            await self.ledger.log_event(
                "INFO",
                f"mother.{self.spec.id}",
                "hourly_pnl_report",
                {
                    "pnl": metrics["pnl"],
                    "sharpe": metrics["sharpe"],
                    "drawdown": metrics["drawdown"],
                    "win_rate": metrics["win_rate"],
                    "allowance": self.allowance,
                },
            )

    async def _dna_snapshot_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.ledger_dna_snapshot_interval_min * 60)
            for child in self.children.values():
                await self.ledger.upsert_child(
                    child.id,
                    self.spec.id,
                    child.dna,
                    "alive",
                    child.performance.pnl,
                    child.position.qty,
                    child.position.entry_price,
                )

    async def apply_virtual_fill(self, child_id: str, fill: VirtualFill) -> None:
        child = self.children.get(child_id)
        if not child:
            return
        child.on_virtual_fill(fill)
        await self.ledger.upsert_child(
            child.id,
            self.spec.id,
            child.dna,
            "alive",
            child.performance.pnl,
            child.position.qty,
            child.position.entry_price,
        )
