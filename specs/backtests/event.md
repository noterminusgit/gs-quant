# event.py

## Summary
Event classes for the backtest engine's event-driven architecture. Defines a base `Event` class and four concrete event types: `MarketEvent`, `ValuationEvent`, `OrderEvent`, and `FillEvent`. Each stores a `type` string tag and relevant payload data. Used by the `PredefinedAssetEngine` event loop.

## Dependencies
- Internal: `gs_quant.backtests.order` (OrderBase)
- External: none

## Type Definitions

### Event (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | | | Empty base class |

### MarketEvent (class)
Inherits: `Event`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type | `str` | `'Market'` | Event type tag |

### ValuationEvent (class)
Inherits: `Event`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type | `str` | `'Valuation'` | Event type tag |

### OrderEvent (class)
Inherits: `Event`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type | `str` | `'Order'` | Event type tag |
| order | `OrderBase` | required | The order to submit |

### FillEvent (class)
Inherits: `Event`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type | `str` | `'Fill'` | Event type tag |
| order | `OrderBase` | required | The filled order |
| filled_units | `float` | required | Number of units filled |
| filled_price | `float` | required | Price at which order was filled |

## Enums and Constants
None.

## Functions/Methods

### Event.__init__(self) -> None
Purpose: Base class; no-op (inherits from `object`).

### MarketEvent.__init__(self) -> None
Purpose: Create a market event tagged `'Market'`.

### ValuationEvent.__init__(self) -> None
Purpose: Create a valuation event tagged `'Valuation'`.

### OrderEvent.__init__(self, order: OrderBase) -> None
Purpose: Create an order event wrapping the given order.

### FillEvent.__init__(self, order: OrderBase, filled_units: float, filled_price: float) -> None
Purpose: Create a fill event recording execution details.

## State Mutation
- All fields are set once during `__init__` and not subsequently modified.

## Error Handling
None.

## Edge Cases
- These are pure data classes with no branching logic
- Event type is identified by string comparison (`event.type == 'Market'`), not by `isinstance`

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- Pure data classes with no conditional logic
