# core.py

## Summary
Provides the pricing infrastructure for gs_quant: a weakref-based `PricingCache`, the central `PricingContext` (context manager that batches and dispatches risk requests to providers), and `PositionContext` (context manager for portfolio position dates). PricingContext handles request grouping, async/sync dispatch via thread pools, parameter inheritance from parent contexts, and integration with the tracing system.

## Dependencies
- Internal: `gs_quant.base` (InstrumentBase, RiskKey, Scenario, get_enum_value), `gs_quant.common` (PricingLocation, RiskMeasure, PricingDateAndMarketDataAsOf), `gs_quant.context_base` (ContextBaseWithDefault), `gs_quant.datetime.date` (business_day_offset, today), `gs_quant.risk` (CompositeScenario, DataFrameWithInfo, ErrorValue, FloatWithInfo, MarketDataScenario, StringWithInfo), `gs_quant.risk.results` (PricingFuture), `gs_quant.session` (GsSession), `gs_quant.target.risk` (RiskPosition, RiskRequest, RiskRequestParameters), `gs_quant.tracing` (Tracer), `gs_quant.markets.markets` (CloseMarket, LiveMarket, Market, close_market_date, OverlayMarket, RelativeMarket), `gs_quant.api.risk` (GenericRiskApi)
- External: `asyncio`, `datetime` (dt), `logging`, `sys`, `weakref`, `abc` (ABCMeta), `concurrent.futures` (ThreadPoolExecutor), `inspect` (signature), `itertools` (zip_longest, takewhile), `typing` (Optional, Union, Type), `tqdm` (tqdm)

## Type Definitions

### TypeAlias
```python
CacheResult = Union[DataFrameWithInfo, FloatWithInfo, StringWithInfo]
```

### PricingCache (class, metaclass=ABCMeta)
Inherits: object (with ABCMeta)

Class-level weakref cache for instrument calculation results. Uses a `WeakKeyDictionary` keyed by instrument, with values being dicts of `RiskKey -> CacheResult`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __cache | `weakref.WeakKeyDictionary` | `WeakKeyDictionary()` | Class-level cache mapping instruments to {RiskKey: CacheResult} |

### PricingContext (class)
Inherits: ContextBaseWithDefault

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __pricing_date | `Optional[dt.date]` | `None` | The pricing date |
| __market_data_location | `Optional[Union[PricingLocation, str]]` | `None` | Market data sourcing location |
| __is_async | `bool` | `None` | Whether to return futures immediately |
| __is_batch | `bool` | `None` | Use batch mode for long-running calcs |
| __use_cache | `bool` | `None` | Whether to use PricingCache |
| __visible_to_gs | `Optional[bool]` | `None` | Whether risk requests are visible to GS |
| __request_priority | `Optional[int]` | `None` | Priority of risk requests |
| __csa_term | `Optional[str]` | `None` | CSA term for calculations |
| __timeout | `Optional[int]` | `None` | Timeout for batch operations |
| __market | `Optional[Market]` | `None` | A Market object |
| __show_progress | `Optional[bool]` | `None` | Whether to show tqdm progress bar |
| __use_server_cache | `Optional[bool]` | `None` | Cache on GS servers |
| __market_behaviour | `Optional[str]` | `'ContraintsBased'` | Curve building behaviour |
| __set_parameters_only | `bool` | `False` | If true, don't block nested contexts |
| __use_historical_diddles_only | `Optional[bool]` | `None` | Only use historical diddles |
| __provider | `Optional[Type[GenericRiskApi]]` | `None` | Risk API implementation |
| __max_per_batch | `Optional[int]` | `None` | Max instruments per batch (default inherited: 1000) |
| __max_concurrent | `Optional[int]` | `None` | Max concurrent requests (default inherited: 1000) |
| __dates_per_batch | `Optional[int]` | `None` | Dates per batch (default inherited: 1) |
| __pending | `dict` | `{}` | Pending `(RiskKey, InstrumentBase) -> PricingFuture` map |
| _group_by_date | `bool` | `True` | Whether to group requests by date |
| __attrs_on_entry | `dict` | `{}` | Saved attribute state on context entry |

### PositionContext (class)
Inherits: ContextBaseWithDefault

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __position_date | `dt.date` | `business_day_offset(today(), 0, roll='preceding')` | The position date |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### PricingCache.clear(cls)  [classmethod]
Purpose: Intended to clear the cache (but has a bug).

**Algorithm:**
1. Creates a new local variable `__cache = WeakKeyDictionary()` (does NOT reset `cls.__cache`)

**Note:** This is a bug -- the local variable shadows the class variable, so the cache is never actually cleared.

### PricingCache.get(cls, risk_key: RiskKey, instrument: InstrumentBase) -> Optional[CacheResult]  [classmethod]
Purpose: Retrieve a cached result for a given risk key and instrument.

**Algorithm:**
1. Return `cls.__cache.get(instrument, {}).get(risk_key)`
2. Branch: instrument not in cache -> returns `None`
3. Branch: risk_key not in instrument's sub-dict -> returns `None`

### PricingCache.put(cls, risk_key: RiskKey, instrument: InstrumentBase, result: CacheResult)  [classmethod]
Purpose: Store a calculation result in the cache.

**Algorithm:**
1. Branch: if result is `ErrorValue` -> do not cache (return)
2. Branch: if `risk_key.market` is `LiveMarket` -> do not cache (return)
3. Otherwise, `cls.__cache.setdefault(instrument, {})[risk_key] = result`

### PricingCache.drop(cls, instrument: InstrumentBase)  [classmethod]
Purpose: Remove all cached results for an instrument.

**Algorithm:**
1. Branch: if instrument in `cls.__cache` -> pop it
2. Branch: else -> no-op

### PricingContext.__init__(self, pricing_date, market_data_location, is_async, is_batch, use_cache, visible_to_gs, request_priority, csa_term, timeout, market, show_progress, use_server_cache, market_behaviour, set_parameters_only, use_historical_diddles_only, provider)
Purpose: Initialize the pricing context with all configuration parameters.

**Algorithm:**
1. Call `super().__init__()`
2. Branch: if `market` and `market_data_location` and `market.location != get_enum_value(PricingLocation, market_data_location)` -> raise `ValueError`
3. Branch: if not `market` and `pricing_date` and `pricing_date > today + 5 days` -> raise `ValueError` (no future pricing without market)
4. Branch: if `market`:
   a. Branch: if `isinstance(market, (OverlayMarket, CloseMarket))` -> extract `market_date` from `market.date` or `market.market.date`
   b. Branch: if `isinstance(market, RelativeMarket)` -> compute `market_date` from from_market/to_market dates
   c. Branch: if `market_date` and `market_date > today` -> raise `ValueError`
5. Branch: if not `market_data_location` and `market` -> use `market.location`
6. Convert `market_data_location` via `get_enum_value`
7. Store all parameters as private fields, initialize `__pending = {}`, `_group_by_date = True`, `__attrs_on_entry = {}`

**Raises:**
- `ValueError` when market.location conflicts with market_data_location
- `ValueError` when pricing_date is more than 5 days in the future without a market
- `ValueError` when market date is in the future

### PricingContext.__save_attrs_to(self, attr_dict)
Purpose: Save current attribute values to a dictionary for later restoration.

**Algorithm:**
1. Copy all 17 tracked attributes into `attr_dict`

### PricingContext._inherited_val(self, parameter, default=None, from_active=False)
Purpose: Resolve a parameter value by inheriting from parent contexts or returning a default.

**Algorithm:**
1. Branch: if `from_active` and `self != self.active_context` and active context has non-None value -> return active context's value
2. Branch: if not entered and no prior (or self is not prior):
   a. Branch: if `PricingContext.current` is not self and current has non-None value -> return current's value
3. Branch: else (entered):
   a. Branch: if prior exists and prior is not self and prior has non-None value -> return prior's value
4. Return `default`

### PricingContext._on_enter(self)
Purpose: Called when entering the context manager; saves state and resolves inherited values.

**Algorithm:**
1. Save current attrs to `__attrs_on_entry`
2. Resolve all properties (which trigger inheritance) and store resolved values back to private fields

### PricingContext.__reset_atts(self)
Purpose: Restore attributes from saved state on context exit.

**Algorithm:**
1. Restore all attributes from `__attrs_on_entry` dict
2. Clear `__attrs_on_entry`

### PricingContext._on_exit(self, exc_type, exc_val, exc_tb)
Purpose: Called when exiting the context manager; dispatches calculations or propagates exceptions.

**Algorithm:**
1. Branch: if `exc_val` is truthy -> re-raise the exception (skip calc)
2. Branch: else -> call `self.__calc()`
3. Finally: always call `self.__reset_atts()`

### PricingContext.__calc(self)
Purpose: Core method that groups pending risk requests and dispatches them to providers.

**Algorithm:**
1. Define inner function `run_requests` that optionally creates a new event loop and calls `provider.populate_pending_futures`
2. Group `self.__pending` by provider, then by (params, scenario), then by instrument -> (dates_markets, measures)
3. Re-group into `(params, scenario, dates_markets_tuple, risk_measures_tuple) -> [instruments]`
4. Branch: if `requests_by_provider` is not empty:
   a. Get current session and determine `request_visible_to_gs`
   b. For each provider, chunk instruments by `_max_per_batch` and dates by `_dates_per_batch` (or `_max_per_batch` if not grouping by date, or all dates if provider doesn't batch dates)
   c. Build `RiskRequest` objects for each chunk
5. Determine `show_status`: Branch: if `show_progress` and more than 1 provider or more than 1 request
6. Create `ThreadPoolExecutor` if more than 1 provider or is_async; else None
7. Create tqdm progress bar if `show_status`
8. Save attrs for async dispatch
9. Branch: if `is_async` and active span is recording -> create sub-span for dispatch tracking
10. Define `handle_fut_res` callback that finishes span when all futures complete
11. For each provider/requests:
    a. Branch: if `request_pool` -> submit to thread pool
       - Branch: if `is_async` -> add done callback
       - Branch: else -> append to completion_futures list
    b. Branch: else -> call `run_requests` directly
12. Branch: if `request_pool` -> shutdown(False), wait on completion_futures

### PricingContext.__risk_key(self, risk_measure: RiskMeasure, provider: type) -> RiskKey
Purpose: Construct a RiskKey from current context state.

**Algorithm:**
1. Return `RiskKey(provider, self.__pricing_date, self.__market, self._parameters, self._scenario, risk_measure)`

### PricingContext._parameters (property) -> RiskRequestParameters
Purpose: Build risk request parameters from current context.

**Algorithm:**
1. Return `RiskRequestParameters(csa_term, raw_results=True, market_behaviour, use_historical_diddles_only)`

### PricingContext._scenario (property) -> Optional[MarketDataScenario]
Purpose: Build scenario from the current Scenario path stack.

**Algorithm:**
1. Get `Scenario.path`
2. Branch: if empty -> return `None`
3. Branch: if single scenario -> wrap in `MarketDataScenario`
4. Branch: if multiple -> wrap in `CompositeScenario(scenarios=tuple(reversed(scenarios)))` then `MarketDataScenario`

### PricingContext.active_context (property)
Purpose: Find the active (entered, non-set-parameters-only) context above self on the stack.

**Algorithm:**
1. Walk the path stack in reverse, stopping at self
2. Return first context that `is_entered` and not `set_parameters_only`
3. Branch: if none found -> return self

### PricingContext.is_current (property) -> bool
Purpose: Check if this context is the current default.

### PricingContext._max_concurrent (property) -> int
Purpose: Return max concurrent value, inheriting from parent if not set (default 1000).

### PricingContext._max_per_batch (property) -> int
Purpose: Return max per batch value, inheriting from parent if not set (default 1000).

### PricingContext._dates_per_batch (property) -> int
Purpose: Return dates per batch value, inheriting from parent if not set (default 1).

### PricingContext.is_async (property) -> bool
Purpose: Return is_async, inheriting from parent if not set (default False).

**Algorithm:**
1. Branch: if `self.__is_async is not None` -> return it
2. Else -> inherit (default False)

### PricingContext.is_batch (property) -> bool
Purpose: Return is_batch, inheriting from parent if falsy (default False).

### PricingContext.market (property) -> Market
Purpose: Return market, or construct a default CloseMarket.

**Algorithm:**
1. Branch: if `self.__market` is truthy -> return it
2. Branch: else -> construct `CloseMarket(date=close_market_date(...), location=self.market_data_location)`

### PricingContext.market_data_location (property) -> PricingLocation
Purpose: Return market data location, inheriting from active context or parent (default PricingLocation.LDN).

### PricingContext.csa_term (property) -> str
Purpose: Return CSA term, inheriting if not set.

### PricingContext.show_progress (property) -> bool
Purpose: Return show_progress (default False).

### PricingContext.timeout (property) -> int
Purpose: Return timeout, inheriting if not set.

### PricingContext.request_priority (property) -> int
Purpose: Return request_priority, inheriting if not set.

### PricingContext.use_server_cache (property) -> bool
Purpose: Return use_server_cache (default False). Uses `is not None` check for inheritance.

### PricingContext.provider (property) -> Type[GenericRiskApi]
Purpose: Return provider, inheriting if not set.

### PricingContext.market_behaviour (property) -> str
Purpose: Return market_behaviour (default 'ContraintsBased').

### PricingContext.pricing_date (property) -> dt.date
Purpose: Return pricing date, computing default from business_day_offset if not set.

**Algorithm:**
1. Branch: if `self.__pricing_date is not None` -> return it
2. Compute `default_pricing_date = business_day_offset(today(self.market_data_location), 0, roll='preceding')`
3. Inherit or use default

### PricingContext.use_cache (property) -> bool
Purpose: Return use_cache (default False).

### PricingContext.visible_to_gs (property) -> Optional[bool]
Purpose: Return visible_to_gs, inheriting if not set.

### PricingContext.set_parameters_only (property) -> bool
Purpose: Return set_parameters_only flag.

### PricingContext.use_historical_diddles_only (property) -> bool
Purpose: Return use_historical_diddles_only (default False). Uses `is not None` check.

### PricingContext.clone(self, **kwargs) -> PricingContext
Purpose: Create a new PricingContext with the same parameters, optionally overriding some.

**Algorithm:**
1. Extract all `__init__` parameter names via `inspect.signature`
2. Get current values of each from self
3. Override with `kwargs`
4. Return `self.__class__(**clone_kwargs)`

### PricingContext._calc(self, instrument: InstrumentBase, risk_key: RiskKey) -> PricingFuture
Purpose: Register an instrument for calculation, returning a future for the result.

**Algorithm:**
1. Get `pending` from `self.active_context.__pending`
2. Branch: if instrument is `DummyInstrument` -> return future with `StringWithInfo(value=instrument.dummy_result)`
3. Branch: if `(risk_key, instrument)` already in pending -> return existing future
4. Branch: else -> create new `PricingFuture()`
   a. Branch: if `use_cache` and cached result exists -> set result immediately
   b. Branch: else -> add to pending dict
5. Return future

### PricingContext.calc(self, instrument: InstrumentBase, risk_measure: RiskMeasure) -> PricingFuture
Purpose: Public method to calculate a risk measure for an instrument.

**Algorithm:**
1. Determine provider: Branch: if `self.provider is None` -> use `instrument.provider`; else -> use `self.provider`
2. Call `self._calc(instrument, self.__risk_key(risk_measure, provider))`

### PositionContext.__init__(self, position_date: Optional[dt.date] = None)
Purpose: Initialize position context with a date.

**Algorithm:**
1. Call `super().__init__()`
2. Branch: if `position_date` and `position_date > today` -> raise `ValueError`
3. Branch: if `position_date` -> use it; else -> `business_day_offset(today, 0, roll='preceding')`

**Raises:** `ValueError` when position_date is in the future

### PositionContext.position_date (property) -> dt.date
Purpose: Return the position date.

### PositionContext.default_value(cls) -> PositionContext  [classmethod]
Purpose: Return a fresh default PositionContext.

### PositionContext.clone(self, **kwargs) -> PositionContext
Purpose: Clone with optional overrides, same pattern as PricingContext.clone.

## State Mutation
- `PricingCache.__cache`: Class-level WeakKeyDictionary, modified by `put()` and `drop()`. `clear()` is buggy (no-op).
- `self.__pending`: Populated by `_calc()`, consumed/read by `__calc()`. Accumulates during context body, dispatched on exit.
- `self.__attrs_on_entry`: Set on `_on_enter`, cleared on `__reset_atts` (called from `_on_exit`)
- All `self.__*` private fields: Saved on enter, resolved from inheritance, restored on exit
- Thread safety: `__calc()` uses `ThreadPoolExecutor` for multi-provider dispatch. `__pending` dict is shared between main thread (which populates it in `_calc`) and worker threads (which read it in `run_requests`). This is safe because population happens before dispatch (inside the `with` block body), and dispatch happens on exit.
- The progress bar (`tqdm`) and pending dict are shared across threads during async dispatch.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `PricingContext.__init__` | When `market.location` and `market_data_location` conflict |
| `ValueError` | `PricingContext.__init__` | When `pricing_date` is >5 days in the future without a market |
| `ValueError` | `PricingContext.__init__` | When market date is in the future |
| `ValueError` | `PositionContext.__init__` | When `position_date` is in the future |

## Edge Cases
- `PricingCache.clear()` is a no-op due to local variable assignment bug (line 63)
- `PricingCache.put()` silently skips `ErrorValue` results and `LiveMarket` risk keys
- `_inherited_val` with `from_active=True` only used by `market_data_location`; active context search stops at self to prevent infinite recursion
- `__calc()` creates a new event loop on worker threads (`create_event_loop=True`) when using thread pool
- When `is_async=True`, the active tracing span may have closed by the time worker threads execute, so a sub-span is created
- `market_behaviour` default has a typo: `'ContraintsBased'` (missing 's' in 'Constraints')
- `_on_exit` re-raises exceptions from the `with` block body before calling `__calc()`, but always resets attrs in `finally`
- Properties use different inheritance patterns: some check `is not None`, others check truthiness -- this means `False` and `0` behave differently across properties
- `DummyInstrument` is imported lazily inside `_calc` to avoid circular imports

## Bugs Found
- Line 63: `PricingCache.clear()` assigns to local `__cache` instead of `cls.__cache`, so the cache is never cleared (OPEN)
- Line 99: `market_behaviour` default value `'ContraintsBased'` is misspelled (likely intended: `'ConstraintsBased'`) (OPEN)

## Coverage Notes
- Branch count: ~55
- Key branches in `__init__`: market/market_data_location conflict (3 conditions), future pricing_date check, market type dispatch (OverlayMarket/CloseMarket vs RelativeMarket), future market date check
- Key branches in `__calc()`: request grouping, provider batching (batch_dates vs not, group_by_date vs not), thread pool vs direct call, async vs sync, progress bar display, tracing sub-span creation
- Key branches in `_inherited_val`: from_active path, entered-vs-not-entered path, prior-exists path
- Key branches in `_calc`: DummyInstrument shortcut, existing pending future, cache hit, cache miss
- Properties have consistent pattern: check private field -> inherit -> default
