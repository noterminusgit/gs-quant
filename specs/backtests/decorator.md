# decorator.py

## Summary
Provides a single decorator factory `plot_backtest()` that marks a method with a `_plot_backtest = True` attribute. Used to indicate that a class method should be exported to the plottool as a member function / pseudo-measure of a backtest.

## Dependencies
- Internal: none
- External: none

## Type Definitions
None.

## Enums and Constants
None.

## Functions/Methods

### plot_backtest() -> Callable
Purpose: Return a decorator that sets `_plot_backtest = True` on the decorated object.

**Algorithm:**
1. Define inner `decorator(obj)`:
   a. Set `obj._plot_backtest = True`
   b. Return `obj`
2. Return `decorator`

## State Mutation
- Adds attribute `_plot_backtest = True` to the decorated object

## Error Handling
None.

## Edge Cases
- Calling `plot_backtest()` (with parens) returns the decorator; it is a decorator factory, not a direct decorator
- Works on any object (functions, methods, classes) since it just sets an attribute

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No conditional logic
