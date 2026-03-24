# generic_backtest_datatypes.py

## Summary
Defines the `Strategy` base class used in generic backtest requests. This is a minimal placeholder dataclass that serves as a base type for strategy objects on which backtests can be run.

## Dependencies
- Internal: none
- External: `dataclasses` (dataclass), `dataclasses_json` (dataclass_json)

## Type Definitions

### Strategy (dataclass, dataclass_json)
Inherits: `object`

An empty dataclass with no fields. Serves as a base type / marker for strategy definitions.

```
@dataclass_json
@dataclass
class Strategy(object):
    pass
```

Note: Unlike other dataclasses in this package, `Strategy` does not use `LetterCase.CAMEL` -- it uses the default `@dataclass_json` without letter case configuration.

## Elixir Porting Notes
- This maps to `defstruct []` -- an empty struct acting as a base type.
- Since Elixir does not have inheritance, if subtypes of `Strategy` are needed, use a protocol or a tagged union (`{:strategy, type, data}`).
- The lack of `LetterCase.CAMEL` means JSON field names match Python field names directly (no transformation). Since there are no fields, this is moot for this specific class but should be noted for any subclasses.

## Edge Cases
- The class has no fields, so `Strategy.from_dict({})` produces an empty instance.
- `Strategy.to_dict()` returns `{}` and `Strategy.to_json()` returns `"{}"`.
