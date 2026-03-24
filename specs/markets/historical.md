# historical.py

## Summary
Provides `HistoricalPricingContext` and `BackToTheFuturePricingContext` subclasses of `PricingContext` for producing instrument valuations over multiple dates. `HistoricalPricingContext` calculates over a historical date range using close market data. `BackToTheFuturePricingContext` extends this to also handle future dates by applying a `RollFwd` scenario.

## Dependencies
- Internal: `gs_quant.base` (InstrumentBase, RiskKey), `gs_quant.common` (RiskMeasure), `gs_quant.datetime.date` (date_range), `gs_quant.risk` (RollFwd, MarketDataScenario), `gs_quant.risk.results` (HistoricalPricingFuture, PricingFuture), `gs_quant.markets.core` (PricingContext), `gs_quant.markets.markets` (CloseMarket), `gs_quant.api.risk` (GenericRiskApi)
- External: `datetime` (dt), `typing` (Iterable, Optional, Tuple, Union, Type)

## Type Definitions

### HistoricalPricingContext (class)
Inherits: `PricingContext`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __date_range | `Tuple[dt.date, ...]` | computed | Tuple of business dates for valuation |

All other fields are inherited from `PricingContext` (is_async, is_batch, use_cache, visible_to_gs, request_priority, csa_term, market_data_location, timeout, show_progress, use_server_cache, provider).

### BackToTheFuturePricingContext (class)
Inherits: `HistoricalPricingContext`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __date_range | `Tuple[dt.date, ...]` | computed | Tuple of business dates (duplicated from parent's logic) |
| _roll_to_fwds | `bool` | `True` | Whether to realise forward curve for future dates |
| name | `str` | `None` | Optional name passed to `RollFwd` scenario |

## Enums and Constants
None.

## Functions/Methods

### HistoricalPricingContext.__init__(self, start: Optional[Union[int, dt.date]] = None, end: Optional[Union[int, dt.date]] = None, calendars: Union[str, Tuple] = (), dates: Optional[Iterable[dt.date]] = None, is_async: bool = None, is_batch: bool = None, use_cache: bool = None, visible_to_gs: bool = None, request_priority: Optional[int] = None, csa_term: str = None, market_data_location: Optional[str] = None, timeout: Optional[int] = None, show_progress: Optional[bool] = None, use_server_cache: Optional[bool] = None, provider: Optional[Type[GenericRiskApi]] = None)
Purpose: Initialize a historical pricing context with a date range.

**Algorithm:**
1. Call `super().__init__` with all PricingContext kwargs plus `use_historical_diddles_only=True`
2. Branch: `start is not None` ->
   a. Branch: `dates is not None` -> raise `ValueError('Must supply start or dates, not both')`
   b. Branch: `end is None` -> default `end = dt.date.today()`
   c. Compute `self.__date_range = tuple(date_range(start, end, calendars=calendars))`
3. Branch: `start is None and dates is not None` ->
   a. `self.__date_range = tuple(dates)`
4. Branch: `start is None and dates is None` ->
   a. Raise `ValueError('Must supply start or dates')`

**Raises:**
- `ValueError` when both `start` and `dates` are supplied
- `ValueError` when neither `start` nor `dates` is supplied

### HistoricalPricingContext._market(self, date: dt.date, location: str) -> CloseMarket
Purpose: Create a CloseMarket for a specific date and location.

**Algorithm:**
1. Return `CloseMarket(location=location, date=date, check=True)`

### HistoricalPricingContext.calc(self, instrument: InstrumentBase, risk_measure: RiskMeasure) -> PricingFuture
Purpose: Calculate a risk measure for each date in the date range.

**Algorithm:**
1. Resolve `provider`: Branch: `self.provider is None` -> use `instrument.provider`; else use `self.provider`
2. Get `scenario`, `parameters`, `location` from current context/market
3. For each `date` in `self.__date_range`:
   a. Create `RiskKey` with the date's market from `self._market(date, location)`
   b. Append `self._calc(instrument, risk_key)` to futures
4. Return `HistoricalPricingFuture(futures)`

### HistoricalPricingContext.date_range (property) -> Tuple[dt.date, ...]
Purpose: Get the date range tuple.

### BackToTheFuturePricingContext.__init__(self, start, end, calendars, dates, roll_to_fwds: bool = True, is_async, is_batch, use_cache, visible_to_gs, csa_term, market_data_location, timeout, show_progress, name: Optional[str] = None, provider)
Purpose: Initialize a context that handles both historical and future dates.

**Algorithm:**
1. Call `super().__init__` (HistoricalPricingContext) with all common kwargs
2. Store `self._roll_to_fwds = roll_to_fwds`
3. Store `self.name = name`
4. Branch: `start is not None` ->
   a. Branch: `dates is not None` -> raise `ValueError('Must supply start or dates, not both')`
   b. Branch: `end is None` -> default `end = dt.date.today()`
   c. Compute `self.__date_range = tuple(date_range(start, end, calendars=calendars))`
5. Branch: `start is None and dates is not None` ->
   a. `self.__date_range = tuple(dates)`
6. Branch: `start is None and dates is None` ->
   a. Raise `ValueError('Must supply start or dates')`

**Raises:**
- `ValueError` when both `start` and `dates` are supplied
- `ValueError` when neither `start` nor `dates` is supplied

Note: The date range validation logic is duplicated from `HistoricalPricingContext.__init__`. The parent's `__init__` also performs the same checks, so supplying both `start` and `dates` will raise from the parent first. However, `BackToTheFuturePricingContext` re-computes `__date_range` in its own name-mangled field.

### BackToTheFuturePricingContext.calc(self, instrument: InstrumentBase, risk_measure: RiskMeasure) -> PricingFuture
Purpose: Calculate a risk measure for each date, using RollFwd scenarios for future dates.

**Algorithm:**
1. Resolve `provider`: Branch: `self.provider is None` -> use `instrument.provider`; else use `self.provider`
2. Get `base_scenario`, `parameters`, `location`, `base_market` from current context
3. For each `date` in `self.__date_range`:
   a. Branch: `date > self.pricing_date` (future date) ->
      i. Create `MarketDataScenario(RollFwd(date=date, realise_fwd=self._roll_to_fwds, name=self.name))`
      ii. Create `RiskKey` with `base_market` and the roll-forward scenario
   b. Branch: else (historical or current date) ->
      i. Create `RiskKey` with `self._market(date, location)` and `base_scenario`
   c. Append `self._calc(instrument, risk_key)` to futures
4. Return `HistoricalPricingFuture(futures)`

## State Mutation
- `self.__date_range`: Set once during `__init__`, immutable thereafter (tuple)
- `self._roll_to_fwds`: Set once during `__init__`, not mutated
- `self.name`: Public attribute, set during `__init__`, could be mutated externally
- All other state is managed by parent `PricingContext`
- Thread safety: inherits PricingContext's context-management pattern (context-local)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `HistoricalPricingContext.__init__` | When both `start` and `dates` are provided |
| `ValueError` | `HistoricalPricingContext.__init__` | When neither `start` nor `dates` is provided |
| `ValueError` | `BackToTheFuturePricingContext.__init__` | When both `start` and `dates` are provided |
| `ValueError` | `BackToTheFuturePricingContext.__init__` | When neither `start` nor `dates` is provided |

## Edge Cases
- `start` can be an `int` (number of business days back) or a `dt.date`; the `date_range` function handles both
- `end` defaults to `dt.date.today()` if not provided when `start` is given
- `calendars` defaults to empty tuple, meaning no holiday filtering
- `BackToTheFuturePricingContext` duplicates parent's date range logic due to Python name mangling (`__date_range` becomes `_ClassName__date_range`); each class has its own private copy
- Both `start` and `dates` being `None` raises immediately; they are mutually exclusive inputs
- `BackToTheFuturePricingContext.calc` splits behavior at `self.pricing_date`: dates equal to pricing_date use the historical path (not the future path)
- The parent `HistoricalPricingContext.__init__` will raise `ValueError` before `BackToTheFuturePricingContext.__init__` reaches its own validation when both `start` and `dates` are provided

## Coverage Notes
- Branch count: ~14
- Key branches: `start` provided vs not (2), `dates` provided vs not (2), `end` default (2), `start+dates` conflict (2) -- these appear in both `__init__` methods (doubled). `calc` provider resolution (2 each class), `BackToTheFuturePricingContext.calc` date > pricing_date (2)
- Pragmas: none
