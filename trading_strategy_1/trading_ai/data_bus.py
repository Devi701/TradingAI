from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict

from .models import Snapshot


@dataclass
class SnapshotEnvelope:
    version: int
    snapshots: Dict[str, Snapshot]


class MemoryDataBus:
    def __init__(self) -> None:
        self._snapshots: dict[str, Snapshot] = {}
        self._version = 0
        self._cond = asyncio.Condition()

    async def publish(self, updates: dict[str, Snapshot]) -> int:
        async with self._cond:
            self._snapshots.update(updates)
            self._version += 1
            self._cond.notify_all()
            return self._version

    async def wait_for_update(self, last_version: int, timeout: float | None = None) -> SnapshotEnvelope:
        async with self._cond:
            if self._version <= last_version:
                if timeout is None:
                    await self._cond.wait()
                else:
                    await asyncio.wait_for(self._cond.wait(), timeout=timeout)
            return SnapshotEnvelope(version=self._version, snapshots=dict(self._snapshots))

    def get(self, symbol: str) -> Snapshot | None:
        return self._snapshots.get(symbol)

    @property
    def version(self) -> int:
        return self._version
