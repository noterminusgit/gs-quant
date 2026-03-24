# action_handler.py

## Summary
Defines the `ActionHandler` base class that pairs an action with an abstract `apply_action` method, a `TypeVar` alias `TActionHandler`, and `ActionHandlerBaseFactory` which provides an abstract factory method for constructing action handlers. This module is the bridge between `actions.py` (what to do) and engine-specific handler implementations (how to do it).

## Dependencies
- Internal: `gs_quant.backtests.actions` (`TAction`), `gs_quant.backtests.backtest_objects` (`TBaseBacktest`)
- External: `datetime` (`dt.date`), `abc` (`abstractmethod`), `typing` (`Union`, `Iterable`, `Any`, `TypeVar`)

## Type Definitions

### ActionHandler (class)
Inherits: object (implicitly)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _action | `TAction` | *(required)* | The action instance this handler is responsible for applying |

### TActionHandler (TypeVar)
```
TActionHandler = TypeVar('TActionHandler', bound='ActionHandler')
```

### ActionHandlerBaseFactory (class)
Inherits: object (implicitly)

No fields. Pure abstract factory.

## Enums and Constants
None.

## Functions/Methods

### ActionHandler.__init__(self, action: TAction) -> None
Purpose: Store the action reference for later use by `apply_action`.

**Algorithm:**
1. Set `self._action = action`

### ActionHandler.action (property) -> TAction
Purpose: Read-only accessor for the stored action.

**Algorithm:**
1. Return `self._action`

### ActionHandler.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: TBaseBacktest, trigger_info: Any) -> Any
Purpose: Abstract method that subclasses must implement to apply the action to the backtest state on a given date or dates.

**Algorithm:**
1. Abstract -- `pass`. Must be overridden by subclasses.

### ActionHandlerBaseFactory.get_action_handler(self, action: TAction) -> TActionHandler
Purpose: Abstract factory method returning the appropriate handler for a given action type.

**Algorithm:**
1. Abstract -- `pass`. Must be overridden by subclasses.

## State Mutation
- `self._action`: Set once during `__init__`, never modified afterward.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | Python runtime | If a subclass fails to implement `apply_action` or `get_action_handler` and is instantiated (though `@abstractmethod` is used without `ABC` base, so Python will not enforce this at instantiation -- see Edge Cases) |

## Edge Cases
- `ActionHandler` and `ActionHandlerBaseFactory` use `@abstractmethod` but do **not** inherit from `ABC`. This means Python will not raise `TypeError` when instantiating them directly -- the abstract contract is advisory, not enforced at runtime.
- `apply_action` accepts both a single `dt.date` and an `Iterable[dt.date]` via `Union`, so implementations must handle both cases.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No conditional logic in this module; it is purely abstract/structural.
- Pragmas: none
