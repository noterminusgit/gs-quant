# execution_engine.py

## Summary
ExecutionEngine (empty base) and SimulatedExecutionEngine. Manages order submission, sorting by execution end time, and filling orders when time arrives.

## Classes

### SimulatedExecutionEngine
- submit_order(order): appends and sorts by execution_end_time
- ping(state):
  1. While orders exist:
     a. If first order's end_time > state → break (not yet)
     b. Else → create FillEvent, pop order
  2. Return fill_events list

## Edge Cases
- Empty orders list → returns empty fills
- Multiple orders with same end_time → all filled

## Bugs Found
None.

## Coverage Notes
- ~4 branches
