# core.py

## Summary
Defines core backtest enumeration types and lightweight data containers used throughout the backtesting framework: `TradeInMethod` and `MarketModel` enums for strategy configuration, `ValuationFixingType` enum for data tagging, `TimeWindow` NamedTuple for time-of-day ranges, `ValuationMethod` NamedTuple combining a fixing type with an optional time window, and a `Backtest` class that extends the generated target `Backtest` with a `get_results` method calling the GS Backtest API.

## Dependencies
- Internal: `gs_quant.base` (`EnumBase`), `gs_quant.target.backtests` (`Backtest` as `__Backtest`, `BacktestResult`), `gs_quant.api.gs.backtests` (`GsBacktestApi` -- imported lazily inside `get_results`)
- External: `enum` (`Enum`), `typing` (`Tuple`, `NamedTuple`, `Union`, `Optional`), `datetime` (`dt.time`, `dt.datetime`, `dt.date`)

## Type Definitions

### TradeInMethod (enum)
Inherits: `EnumBase`, `Enum`

| Value | Raw | Description |
|-------|-----|-------------|
| FixedRoll | `"fixedRoll"` | Fixed roll trade-in method |

### Backtest (class)
Inherits: `__Backtest` (from `gs_quant.target.backtests`)

No additional fields beyond those inherited from target `Backtest`.

### MarketModel (enum)
Inherits: `EnumBase`, `Enum`

| Value | Raw | Description |
|-------|-----|-------------|
| STICKY_FIXED_STRIKE | `"SFK"` | Sticky fixed-strike market model |
| STICKY_DELTA | `"SD"` | Sticky delta market model |

### TimeWindow (NamedTuple)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| start | `Union[dt.time, dt.datetime]` | `None` | Start of valuation window |
| end | `Union[dt.time, dt.datetime]` | `None` | End of valuation window |

### ValuationFixingType (enum)
Inherits: `EnumBase`, `Enum`

| Value | Raw | Description |
|-------|-----|-------------|
| PRICE | `"price"` | Use price-based fixing for valuation |

### ValuationMethod (NamedTuple)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_tag | `ValuationFixingType` | `ValuationFixingType.PRICE` | Which fixing type to use for valuation |
| window | `Optional[TimeWindow]` | `None` | Optional time window for intraday averaging; `None` means use daily fixing |

## Enums and Constants
See Type Definitions above -- `TradeInMethod`, `MarketModel`, and `ValuationFixingType` are all enums.

### Module Constants
None.

## Functions/Methods

### Backtest.get_results(self) -> Tuple[BacktestResult, ...]
Purpose: Fetch backtest results from the GS Backtest API using this backtest's ID.

**Algorithm:**
1. Lazily import `GsBacktestApi` from `gs_quant.api.gs.backtests`
2. Call `GsBacktestApi.get_results(backtest_id=self.id)` and return the result

**Raises:** Any API-level exceptions from `GsBacktestApi.get_results` (network errors, auth errors, etc.)

## State Mutation
- No state is mutated by any function in this module.
- `Backtest.get_results` is a pure read operation (API call).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| *(API errors)* | `Backtest.get_results` | When the GS API returns an error (authentication, not found, etc.) |

## Edge Cases
- `Backtest.get_results` performs a lazy import on every call; repeated calls re-import each time (though Python caches modules).
- `self.id` on `Backtest` must be set (inherited from target `__Backtest`) before calling `get_results`, otherwise a `None` or missing ID would be passed to the API.
- `TimeWindow` and `ValuationMethod` are immutable `NamedTuple` instances.

## Bugs Found
None.

## Coverage Notes
- Branch count: 2 (the `get_results` call itself is a single path; the lazy import adds 1 implicit branch on first call vs. cached).
- In practice the only testable branch is the API call in `get_results`, which requires mocking `GsBacktestApi`.
- Pragmas: none
