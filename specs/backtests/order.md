# order.py

## Summary
Order types for the event-driven backtest engine. `OrderBase` is the abstract base class (ABCMeta) defining the order interface. Concrete subclasses implement specific execution strategies: `OrderTWAP` (time-weighted average price), `OrderMarketOnClose` (end-of-day close price), `OrderCost` (zero-price cost/fee orders), `OrderAtMarket` (market price at a specific datetime), and `OrderTwapBTIC` (BTIC TWAP + underlying close).

## Dependencies
- Internal: `gs_quant.instrument` (Instrument, Cash)
- Internal: `gs_quant.backtests.core` (TimeWindow, ValuationFixingType)
- Internal: `gs_quant.backtests.data_handler` (DataHandler)
- External: `abc` (ABCMeta)
- External: `numpy` (np)
- External: `datetime` (dt)

## Type Definitions

### OrderBase (class, ABCMeta)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| instrument | `Instrument` | required | Instrument to be traded |
| quantity | `float` | required | Quantity to trade |
| generation_time | `dt.datetime` | required | When the order was generated |
| source | `str` | required | Name of the entity that generated this order |
| executed_price | `Optional[float]` | `None` | Cached execution price (lazy computed) |

### OrderTWAP (class)
Inherits: `OrderBase`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| window | `TimeWindow` | required | TWAP execution window (start, end) |

### OrderMarketOnClose (class)
Inherits: `OrderBase`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| execution_date | `dt.date` | required | Date for market-on-close execution |

### OrderCost (class)
Inherits: `OrderBase`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| execution_time | `dt.datetime` | required | Time when the cost order executes |

Note: `instrument` is always `Cash(currency)` -- set in `__init__` via `super().__init__(Cash(currency), ...)`.

### OrderAtMarket (class)
Inherits: `OrderBase`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| execution_datetime | `dt.datetime` | required | Exact datetime for market execution |

### OrderTwapBTIC (class)
Inherits: `OrderTWAP`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| btic_instrument | `Instrument` | required | BTIC instrument for spread pricing |
| future_underlying | `Any` | required | Underlying future instrument for close price |

## Enums and Constants
None.

## Functions/Methods

### OrderBase.__init__(self, instrument: Instrument, quantity: float, generation_time: dt.datetime, source: str) -> None
Purpose: Initialize base order fields.

### OrderBase.execution_end_time(self) -> dt.datetime
Purpose: Abstract; raises `RuntimeError('The method execution_end_time is not implemented on OrderBase')`.

### OrderBase._execution_price(self, data_handler: DataHandler) -> float
Purpose: Abstract; raises `RuntimeError('The method execution_price is not implemented on OrderBase')`.

### OrderBase.execution_price(self, data_handler: DataHandler) -> float
Purpose: Compute execution price, raising if NaN.

**Algorithm:**
1. Call `price = self._execution_price(data_handler)`
2. Branch: `np.isnan(price)` -> raise `RuntimeError('can not compute the execution price')`
3. Branch: else -> return `price`

### OrderBase.execution_quantity(self) -> float
Purpose: Abstract; raises `RuntimeError('The method execution_quantity is not implemented on OrderBase')`.

### OrderBase.execution_notional(self, data_handler: DataHandler) -> float
Purpose: Return `execution_price(data_handler) * execution_quantity()`.

### OrderBase._short_name(self) -> str
Purpose: Abstract; raises `RuntimeError`.

### OrderBase.to_dict(self, data_handler: DataHandler) -> dict
Purpose: Return dict with Instrument (via `instrument.ric`), Type, Price, Quantity.

### OrderTWAP.__init__(self, instrument, quantity, generation_time, source, window: TimeWindow) -> None
Purpose: Initialize TWAP order with execution window.

### OrderTWAP.execution_end_time(self) -> dt.datetime
Purpose: Return `self.window.end`.

### OrderTWAP._execution_price(self, data_handler: DataHandler) -> float
Purpose: Lazily compute mean of prices over the TWAP window.

**Algorithm:**
1. Branch: `self.executed_price is None` -> get data range over `[window.start, window.end]` for `ValuationFixingType.PRICE`, compute `np.mean`, cache in `self.executed_price`
2. Return `self.executed_price`

### OrderTWAP.execution_quantity(self) -> float
Purpose: Return `self.quantity`.

### OrderTWAP._short_name(self) -> str
Purpose: Return `'TWAP {start}:{end}'`.

### OrderMarketOnClose.__init__(self, instrument, quantity, generation_time, execution_date: dt.date, source) -> None
Purpose: Initialize market-on-close order.

### OrderMarketOnClose.execution_end_time(self) -> dt.datetime
Purpose: Return `dt.datetime.combine(self.execution_date, dt.time(23, 0, 0))`.

### OrderMarketOnClose._execution_price(self, data_handler: DataHandler) -> float
Purpose: Lazily get closing price at `execution_date`.

**Algorithm:**
1. Branch: `self.executed_price is None` -> `data_handler.get_data(self.execution_date, self.instrument, PRICE)`, cache
2. Return `self.executed_price`

### OrderMarketOnClose.execution_quantity(self) -> float
Purpose: Return `self.quantity`.

### OrderCost.__init__(self, currency: str, quantity: float, source: str, execution_time: dt.datetime) -> None
Purpose: Initialize cost order with `Cash(currency)` instrument.

### OrderCost.execution_end_time(self) -> dt.datetime
Purpose: Return `self.execution_time`.

### OrderCost._execution_price(self, data_handler: DataHandler) -> float
Purpose: Lazily return 0.

**Algorithm:**
1. Branch: `self.executed_price is None` -> set `self.executed_price = 0`
2. Return `self.executed_price`

### OrderCost.execution_quantity(self) -> float
Purpose: Return `self.quantity`.

### OrderCost._short_name(self) -> str
Purpose: Return `'Cost'`.

### OrderCost.to_dict(self, data_handler: DataHandler) -> dict
Purpose: Return dict using `instrument.currency` instead of `instrument.ric`.

### OrderAtMarket.__init__(self, instrument, quantity, generation_time, execution_datetime: dt.datetime, source) -> None
Purpose: Initialize market order at specific datetime.

### OrderAtMarket.execution_end_time(self) -> dt.datetime
Purpose: Return `self.execution_datetime`.

### OrderAtMarket._execution_price(self, data_handler: DataHandler) -> float
Purpose: Lazily get market price at `execution_datetime`.

**Algorithm:**
1. Branch: `self.executed_price is None` -> `data_handler.get_data(self.execution_datetime, self.instrument, PRICE)`, cache
2. Return `self.executed_price`

### OrderAtMarket.execution_quantity(self) -> float
Purpose: Return `self.quantity`.

### OrderTwapBTIC.__init__(self, instrument, quantity, generation_time, source, window, btic_instrument: Instrument, future_underlying) -> None
Purpose: Initialize BTIC TWAP order.

### OrderTwapBTIC._execution_price(self, data_handler: DataHandler) -> float
Purpose: Lazily compute BTIC TWAP + underlying close.

**Algorithm:**
1. Branch: `self.executed_price is None`:
   a. Get BTIC fixings over `[window.start, window.end]` for `btic_instrument`
   b. Compute `btic_twap = np.mean(btic_fixings)`
   c. Get `close = data_handler.get_data(self.window.end.date(), self.future_underlying)`
   d. Set `self.executed_price = close + btic_twap`
2. Return `self.executed_price`

### OrderTwapBTIC._short_name(self) -> str
Purpose: Return `'TwapBTIC'`.

## State Mutation
- `self.executed_price`: `None` initially; lazily computed and cached on first call to `_execution_price()`. All subclasses follow this pattern.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `OrderBase.execution_end_time` | Always (abstract) |
| `RuntimeError` | `OrderBase._execution_price` | Always (abstract) |
| `RuntimeError` | `OrderBase.execution_price` | When computed price is `NaN` |
| `RuntimeError` | `OrderBase.execution_quantity` | Always (abstract) |
| `RuntimeError` | `OrderBase._short_name` | Always (abstract) |

## Edge Cases
- All `_execution_price` methods are lazy: computed once, then cached in `executed_price`
- `NaN` price from any subclass triggers `RuntimeError` in `execution_price()`
- `OrderCost` always returns price 0 (used for fee/cost tracking)
- `OrderCost.to_dict` uses `instrument.currency` instead of `instrument.ric` since instrument is `Cash`
- `OrderTwapBTIC` calls `data_handler.get_data` without explicit `ValuationFixingType` for the underlying close price

## Bugs Found
None.

## Coverage Notes
- Branch count: ~16
- Key branches: Each `_execution_price` has a lazy-init branch (executed_price is None vs cached), `execution_price` has NaN check
- OrderBase abstract methods need not be tested directly but their RuntimeError raises should be verified
