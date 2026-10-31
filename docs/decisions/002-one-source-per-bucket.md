# ADR 002: Stats use one source per symbol and bucket

**Status:** accepted, 2026-10-30 (from BUG-07)

**Context.** Equities can arrive from Finnhub (no trade ids) and Alpaca (IEX only, with trade ids).
The same trade can't be matched across feeds, and the feeds don't cover the same market.

**Decision.** For each symbol and hour (or trading day), stats use only the highest-priority
source present: Finnhub, then Alpaca. Gold rows record the `source` they came from.

**Consequences.** If Finnhub drops out for an hour, that hour falls back to Alpaca, which is
IEX only and shows lower volume. The `source` column makes this visible instead of silent.
