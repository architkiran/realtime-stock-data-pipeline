"""Symbol universe and runtime settings (environment variables, never hardcoded)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Universe:
    equities: tuple[str, ...]
    crypto: tuple[str, ...]
    sources: dict[str, str] = field(default_factory=dict)

    @property
    def all_symbols(self) -> frozenset[str]:
        return frozenset(self.equities) | frozenset(self.crypto)

    def asset_class(self, symbol: str) -> str | None:
        if symbol in self.equities:
            return "equity"
        if symbol in self.crypto:
            return "crypto"
        return None

    def symbols_for(self, source: str) -> tuple[str, ...]:
        group = self.sources.get(source)
        return tuple(getattr(self, group)) if group else ()


def _default_path() -> Path:
    env = os.environ.get("SYMBOLS_CONFIG")
    if env:
        return Path(env)
    here = Path(__file__).resolve()
    for candidate in (here.parent.parent / "config" / "symbols.yaml", here.parents[3] / "config" / "symbols.yaml"):
        if candidate.exists():
            return candidate
    raise FileNotFoundError("symbols.yaml not found; set SYMBOLS_CONFIG")


@lru_cache(maxsize=4)
def load_universe(path: str | None = None) -> Universe:
    data = yaml.safe_load(Path(path or _default_path()).read_text())
    return Universe(
        equities=tuple(data.get("equities", [])),
        crypto=tuple(data.get("crypto", [])),
        sources=dict(data.get("sources", {})),
    )


@dataclass(frozen=True)
class Settings:
    region: str = os.environ.get("AWS_REGION", "us-east-1")
    stream_name: str = os.environ.get("STREAM_NAME", "stockstream-trades")
    bucket: str = os.environ.get("DATA_BUCKET", "stockstream-data-local")
    late_watermark_minutes: int = int(os.environ.get("LATE_WATERMARK_MINUTES", "120"))
    future_tolerance_ms: int = int(os.environ.get("FUTURE_TOLERANCE_MS", "5000"))
    suspect_jump_pct: float = float(os.environ.get("SUSPECT_JUMP_PCT", "0.20"))


def settings() -> Settings:
    return Settings()
