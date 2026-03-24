# backtest_engine.py

## Summary
Defines `BacktestBaseEngine`, an abstract base class with a single abstract method `get_action_handler`. This serves as the interface contract that all backtest engine implementations must satisfy, mapping action types to their corresponding handler implementations.

## Dependencies
- Internal: `gs_quant.backtests.action_handler` (`TActionHandler`), `gs_quant.backtests.actions` (`TAction`)
- External: `abc` (`abstractmethod`)

## Type Definitions

### BacktestBaseEngine (class)
Inherits: object (implicitly)

No fields. Pure abstract engine interface.

## Enums and Constants
None.

## Functions/Methods

### BacktestBaseEngine.get_action_handler(self, action: TAction) -> TActionHandler
Purpose: Abstract method that engine subclasses implement to return the appropriate action handler for a given action.

**Algorithm:**
1. Abstract -- `pass`. Must be overridden by subclasses.

## State Mutation
None. No instance state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | Python runtime | Same caveat as `action_handler.py` -- uses `@abstractmethod` without `ABC` base, so not enforced at instantiation |

## Edge Cases
- Like `ActionHandler`, `BacktestBaseEngine` uses `@abstractmethod` but does not inherit from `ABC`, so direct instantiation will not raise `TypeError`.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No conditional logic; purely abstract.
- Pragmas: none
