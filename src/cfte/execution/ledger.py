from __future__ import annotations

from dataclasses import dataclass, field

from cfte.execution.models import FillFact


@dataclass(slots=True)
class FillLedger:
    _fills: list[FillFact] = field(default_factory=list)
    _seen_fill_ids: set[str] = field(default_factory=set)

    def append_fill(self, fill: FillFact) -> None:
        if fill.fill_id in self._seen_fill_ids:
            raise ValueError(f"Duplicate fill_id: {fill.fill_id}")
        self._fills.append(fill)
        self._seen_fill_ids.add(fill.fill_id)

    def all_fills(self) -> list[FillFact]:
        return list(self._fills)

    def fills_for_order(self, order_id: str) -> list[FillFact]:
        return [fill for fill in self._fills if fill.order_id == order_id]
