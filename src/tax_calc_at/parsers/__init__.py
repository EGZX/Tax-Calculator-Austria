"""Parser subpackage. Each module exposes ``parse(path) -> tuple[list[Transaction], ParseReport]``."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from ..model import ParseReport, Transaction
from . import ibkr_flex, scalable, trade_republic, trading212

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
