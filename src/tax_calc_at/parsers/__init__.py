"""Parser subpackage. Each module exposes ``parse(path) -> (txns, ParseReport)``."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from ..model import ParseReport, Transaction
from . import ibkr_flex, scalable, trade_republic, trading212
from .base import apply_steuereinfach_cutoff

if TYPE_CHECKING:
    from ..engine.rules import BrokerConfig

ParserFn = Callable[[Path], tuple[list[Transaction], ParseReport]]

REGISTRY: dict[str, ParserFn] = {
    "scalable": scalable.parse,
    "trade_republic": trade_republic.parse,
    "trading212": trading212.parse,
    "ibkr_flex": ibkr_flex.parse,
}


def get_parser(name: str) -> ParserFn:
    if name not in REGISTRY:
        raise KeyError(f"Unknown parser {name!r}; known: {sorted(REGISTRY)}")
    return REGISTRY[name]


def parse_file(
    cfg: BrokerConfig, path: Path, *, strict_cutoff: bool = False
) -> tuple[list[Transaction], ParseReport]:
    """Parse one export with the broker's parser and apply its cutoff."""
    txns, report = get_parser(cfg.parser)(path)
    apply_steuereinfach_cutoff(txns, report, cfg.steuereinfach_from, strict=strict_cutoff)
    return txns, report
