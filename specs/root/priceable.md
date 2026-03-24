# priceable.py

## Summary
Provides `PriceableImpl`, the concrete implementation of the abstract `Priceable` base class (from `gs_quant.base`). It wires pricing operations (`dollar_price`, `price`, `market`) to the current `PricingContext` via `self.calc()`. The module also handles the context-manager-aware decision of whether to return immediate results or futures, and includes a nested helper for transforming raw market data results into `OverlayMarket` objects. A module-level registry dict `__asset_class_and_type_to_instrument` is defined but populated elsewhere.

## Dependencies
- Internal:
  - `gs_quant.base` (Priceable)
  - `gs_quant.context_base` (nullcontext)
  - `gs_quant.markets` (MarketDataCoordinate, PricingContext, CloseMarket, OverlayMarket)
  - `gs_quant.risk` (DataFrameWithInfo, DollarPrice, FloatWithInfo, Price, SeriesWithInfo, MarketData)
  - `gs_quant.risk.results` (PricingFuture, PortfolioRiskResult, ErrorValue)
- External:
  - `logging` (getLogger)
  - `abc` (ABC)
  - `typing` (Union, Optional)

## Type Definitions

### __asset_class_and_type_to_instrument (module-level)
```python
__asset_class_and_type_to_instrument: Dict = {}
```
Module-private dict. Serves as a registry mapping asset class/type tuples to instrument classes. Defined here but populated by other modules.

### _logger (module-level)
```python
_logger: logging.Logger = logging.getLogger(__name__)
```

### PriceableImpl (class, ABC)
Inherits: `Priceable` (from `gs_quant.base`), `ABC`

No `__init__` defined -- inherits from `Priceable` which inherits from `Base`. This is an abstract class that provides concrete implementations of the pricing methods declared abstract in `Priceable`.

| Field / Property | Type | Default | Description |
|------------------|------|---------|-------------|
| `_pricing_context` | `PricingContext \| nullcontext` | computed (property) | Returns the current PricingContext or a nullcontext if it is entered/async |
| `_return_future` | `bool` | computed (property) | Whether pricing calls should return futures instead of resolved values |

Note: `PriceableImpl` inherits `calc()`, `resolve()`, `dollar_price()`, `price()` from `Priceable`/`Base`. The `calc()` method is the core dispatch -- it is declared as `raise NotImplementedError` in `Priceable` but is implemented by the pricing framework (likely mixed in or overridden downstream).

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `__asset_class_and_type_to_instrument` | `dict` | `{}` | Registry mapping asset class/type to instrument class |
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### PriceableImpl._pricing_context (property) -> PricingContext | nullcontext
Purpose: Get the appropriate pricing context for calculations. Returns a `nullcontext` when the current context is already entered or async (to avoid double-entering).

**Algorithm:**
1. Get `pricing_context = PricingContext.current` (uses ContextMeta.current, may raise MqUninitialisedError)
2. Branch: if `pricing_context.is_entered` is `True` OR `pricing_context.is_async` is `True` -> return `nullcontext()`
3. Else -> return `pricing_context` as-is

**Rationale:** When a PricingContext is already entered (inside a `with` block) or is async, the calc call should NOT re-enter the context. The `nullcontext()` acts as a no-op wrapper.

### PriceableImpl._return_future (property) -> bool
Purpose: Determine whether pricing methods should return futures (deferred results) or resolved values.

**Algorithm:**
1. Get `pricing_context = self._pricing_context`
2. Branch: if `pricing_context` is NOT an instance of `PricingContext` (i.e., it's a `nullcontext`) -> return `True`
3. Branch: if `pricing_context.is_async` is `True` OR `pricing_context.is_entered` is `True` -> return `True`
4. Else -> return `False`

**Note:** When `_pricing_context` returns a `nullcontext`, the `isinstance` check fails, so `_return_future` returns `True`. This is the expected behavior -- if we're already inside a context, results are futures.

### PriceableImpl.dollar_price(self) -> Union[FloatWithInfo, PortfolioRiskResult, PricingFuture, SeriesWithInfo]
Purpose: Calculate the present value of the instrument in USD.

**Algorithm:**
1. Return `self.calc(DollarPrice)`

Delegates entirely to `calc()` with the `DollarPrice` risk measure.

### PriceableImpl.price(self, currency=None) -> Union[FloatWithInfo, PortfolioRiskResult, PricingFuture, SeriesWithInfo]
Purpose: Calculate the present value of the instrument in local currency (or a specified currency).

**Algorithm:**
1. Branch: if `currency` is truthy -> return `self.calc(Price(currency=currency))`
2. Else -> return `self.calc(Price)` (uses Price class itself, not an instance)

**Note:** When `currency` is None/falsy, `Price` (the class) is passed directly, not `Price()` (an instance). This is a meaningful distinction -- the risk framework handles both class and instance risk measures.

### PriceableImpl.market(self) -> Union[OverlayMarket, PricingFuture]
Purpose: Retrieve the market data (coordinates and values) used for pricing this instrument.

**Algorithm:**
1. Define inner function `handle_result(result)`
2. Return `self.calc(MarketData, fn=handle_result)`

#### handle_result(result: Optional[Union[DataFrameWithInfo, ErrorValue, PricingFuture]]) -> Union[OverlayMarket, dict]
Purpose: Post-process the raw market data result into an `OverlayMarket` or a dict of date -> `OverlayMarket`.

**Algorithm:**
1. Branch: if `result` is an `ErrorValue` instance -> return `result` as-is (pass through the error)
2. Get `properties = MarketDataCoordinate.properties()` -- list of coordinate property names
3. Determine `is_historical = result.index.name == 'date'`
4. Get `location = PricingContext.current.market_data_location`
5. Define inner function `extract_market_data(this_result)` (see below)
6. Branch: if `is_historical` is `True`:
   a. Iterate over unique dates in `result.index`
   b. For each date, create `OverlayMarket(base_market=CloseMarket(date=date, location=location), market_data=extract_market_data(result.loc[date]))`
   c. Return dict of `{date: OverlayMarket}`
7. Else (not historical):
   a. Return `OverlayMarket(base_market=result.risk_key.market, market_data=extract_market_data(result))`

#### extract_market_data(this_result: DataFrameWithInfo) -> dict
Purpose: Convert DataFrame rows into a dict mapping `MarketDataCoordinate` to values.

**Algorithm:**
1. Initialize `market_data = {}`
2. For each row in `this_result.iterrows()`:
   a. Build `coordinate_values = {p: row.get(p) for p in properties}`
   b. Get `mkt_point = coordinate_values.get('mkt_point')`
   c. Branch: if `mkt_point is not None` -> split by `;` and convert to tuple: `coordinate_values['mkt_point'] = tuple(mkt_point.split(';'))`
   d. Create key: `MarketDataCoordinate.from_dict(coordinate_values)`
   e. Branch: if `row['permissions'] == 'Granted'` -> value is `row['value']`; else -> value is the string `'redacted'`
   f. Store in `market_data`
3. Return `market_data`

## State Mutation
- `__asset_class_and_type_to_instrument`: Module-level dict. Defined as empty `{}` here; populated externally by instrument registration code. Not modified within this module.
- `_logger`: Module-level logger, stateless (standard logging).
- No instance state is mutated by any method in `PriceableImpl`. All methods are read-only computations that delegate to `self.calc()`.
- `PricingContext.current` is accessed (read-only) via ContextMeta properties.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqUninitialisedError` | `_pricing_context` (via `PricingContext.current`) | When PricingContext has not been initialized and has no default |
| (inherited) | `calc()` | Various errors from the pricing framework |

Note: `PriceableImpl` itself does not explicitly raise exceptions, but its properties access `PricingContext.current` which can raise `MqUninitialisedError`.

## Edge Cases
- **nullcontext as pricing_context**: When `_pricing_context` returns `nullcontext()`, `_return_future` always returns `True` because `isinstance(nullcontext(), PricingContext)` is `False`. This is intentional -- inside an entered context, all results are futures.
- **Price class vs instance**: `price()` passes `Price` (the class) when no currency is given, vs `Price(currency=currency)` (an instance) when currency is specified. The `calc()` framework must handle both.
- **ErrorValue passthrough**: `handle_result` checks for `ErrorValue` first and returns it unchanged. This means the return type from `market()` can actually be an `ErrorValue` (not reflected in the type annotation).
- **Redacted coordinates**: Market data coordinates with `permissions != 'Granted'` get the string `'redacted'` as their value instead of the actual numeric value.
- **mkt_point semicolon splitting**: The `mkt_point` field is stored as a semicolon-delimited string in the DataFrame but converted to a tuple of strings for the `MarketDataCoordinate`.
- **Historical vs spot**: The `is_historical` branch is determined by `result.index.name == 'date'`. Historical results produce a dict keyed by date; spot results produce a single `OverlayMarket`.
- **PricingContext dependency**: Both `_pricing_context` and `handle_result` access `PricingContext.current`, which depends on the context_base thread-local stack being properly initialized.

## Elixir Porting Notes
- **ABC + Priceable base class**: In Elixir, `PriceableImpl` maps to a module with a behaviour (`@behaviour Priceable`) or a set of default implementations via `defoverridable` in a `__using__` macro.
- **_pricing_context / nullcontext pattern**: The "return nullcontext if already entered" pattern maps to checking the process dictionary for an active context. In Elixir, if a context is already active, skip wrapping and return a pass-through.
- **Closures (handle_result, extract_market_data)**: The nested function definitions inside `market()` map to private functions or anonymous functions in Elixir. `extract_market_data` captures `properties` and `location` from the outer scope.
- **Union return types**: Elixir uses tagged tuples: `{:ok, %OverlayMarket{}}` or `{:error, %ErrorValue{}}` or `{:future, ref}`.
- **DataFrame operations**: `iterrows()`, `index.name`, `.loc[date]` require a data frame library (e.g., Explorer) or custom struct in Elixir.

## Coverage Notes
- Branch count: 10
- Key branches: `_pricing_context` (is_entered or is_async), `_return_future` (isinstance check, is_async, is_entered), `price` (currency truthy vs falsy), `handle_result` (ErrorValue check, is_historical), `extract_market_data` (mkt_point None check, permissions check)
- Pragmas: None in this file
- The `nullcontext` import from `context_base` is always available (polyfill exists), so no import branching here
