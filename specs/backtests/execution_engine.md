# execution_engine.py

## Summary
Provides `ExecutionEngine` (empty base class) and `SimulatedExecutionEngine` for the event-driven backtest. `SimulatedExecutionEngine` manages a sorted queue of pending orders, filling them when the simulation clock reaches their execution end time.

## Dependencies
- Internal: `gs_quant.backtests.data_handler` (DataHandler)
- Internal: `gs_quant.backtests.event` (OrderEvent, FillEvent)
- Internal: `gs_quant.backtests.order` (OrderBase)
- External: `datetime` (dt.datetime)

## Type Definitions

### ExecutionEngine (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | | | Empty base class |

### SimulatedExecutionEngine (class)
Inherits: `ExecutionEngine`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_handler | `DataHandler` | required | Data handler for price lookups during fill |
| orders | `List[OrderEvent]` | `[]` | Pending orders sorted by execution_end_time |

## Enums and Constants
None.

## Functions/Methods

### SimulatedExecutionEngine.__init__(self, data_handler: DataHandler) -> None
Purpose: Initialize with a data handler and empty order queue.

**Algorithm:**
1. Store `self.data_handler = data_handler`
2. Initialize `self.orders = []`

### SimulatedExecutionEngine.submit_order(self, order: OrderEvent) -> None
Purpose: Add an order to the queue and maintain sorted order by execution end time.

**Algorithm:**
1. Append `order` to `self.orders`
2. Sort `self.orders` by `e.order.execution_end_time()` (ascending)

### SimulatedExecutionEngine.ping(self, state: dt.datetime) -> List[FillEvent]
Purpose: Check if any pending orders should be filled at the current time.

**Algorithm:**
1. Initialize `fill_events = []`
2. While `self.orders` is non-empty:
   a. Get `order = self.orders[0].order`
   b. Get `end_time = order.execution_end_time()`
   c. Branch: `end_time > state` -> `break` (not yet due)
   d. Branch: else (end_time <= state) -> create `FillEvent` with `order`, `order.execution_price(self.data_handler)`, and `order.execution_quantity()`; append to `fill_events`; pop `self.orders[0]`
3. Return `fill_events`

## State Mutation
- `self.orders`: Appended to by `submit_order()`, re-sorted after each append; elements popped from front by `ping()`
- Thread safety: Not thread-safe; designed for single-threaded event loop

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `order.execution_price` (via OrderBase) | When computed price is NaN |
| `RuntimeError` | `order.execution_quantity` (via OrderBase) | When called on OrderBase directly |

## Edge Cases
- Empty orders list: `ping()` returns empty `fill_events` immediately
- Multiple orders with the same `end_time`: all are filled in the same `ping()` call since `end_time <= state` for all
- Orders are sorted by `execution_end_time` so earliest-expiring are processed first

## Bugs Found
None.

## Coverage Notes
- Branch count: 4
- Key branches: while-loop entry (orders non-empty vs empty), end_time > state (break vs fill)
- `submit_order` takes `OrderEvent` but the sort lambda accesses `.order.execution_end_time()`
