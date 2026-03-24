# common.py

## Summary
Provides domain-specific enums and risk measure classes that extend and customize auto-generated types from `gs_quant.target.common`. This module re-exports everything from the target common module via wildcard import, then layers on custom `PositionType`, `DateLimit`, and `PayReceive` enums as well as `RiskMeasure` and `ParameterisedRiskMeasure` classes with comparison, representation, and parameter handling logic. It is the primary public interface for these types throughout the gs_quant library.

## Dependencies
- Internal:
  - `gs_quant.target.common` -- wildcard re-export of all names (`from gs_quant.target.common import *`), plus explicit imports:
    - `PayReceive` as `_PayReceive` (the auto-generated version, used for conversion in the custom `PayReceive._missing_`)
    - `RiskMeasure` as `__RiskMeasure` (the auto-generated base dataclass, parent of the custom `RiskMeasure`)
    - `RiskMeasureType` (enum of risk measure types)
    - `AssetClass` (enum of asset classes)
    - `RiskMeasureUnit` (enum of risk measure units)
  - `gs_quant.base` -- `EnumBase` (mixin for case-insensitive enum lookup, comparison, and string representation), `RiskMeasureParameter` (ABC for risk measure parameters with `as_dict()` and `parameter_type`)
  - `gs_quant.markets` -- `PricingContext` (lazy import inside `RiskMeasure.pricing_context` property)
- External:
  - `enum` -- `Enum`
  - `typing` -- `Union`
  - `datetime` as `dt` -- `dt.date`

## Type Definitions

### PositionType (Enum)
Inherits: `Enum`

Standard Python `Enum` (does NOT use `EnumBase`). Represents position types for portfolios or indices.

No instance fields beyond the enum value.

### DateLimit (Enum)
Inherits: `Enum`

Standard Python `Enum` (does NOT use `EnumBase`). Holds date boundary constants.

No instance fields beyond the enum value.

### PayReceive (EnumBase, Enum)
Inherits: `EnumBase`, `Enum` (multiple inheritance, in that order)

Custom pay/receive enum that extends the auto-generated `_PayReceive` from `gs_quant.target.common` by:
1. Reducing members to just `Pay`, `Receive`, `Straddle` (vs. the target's six members: Pay, Payer, Receive, Receiver, Straddle, Rec).
2. Mapping the raw value of `Receive` to `'Rec'` (not `'Receive'`).
3. Adding a `_missing_` classmethod for flexible lookup.

Because it inherits `EnumBase`, it gets:
- Case-insensitive lookup via `EnumBase._missing_`
- `__lt__` comparison by raw value
- `__repr__` and `__str__` returning the raw value string

### RiskMeasure (class)
Inherits: `__RiskMeasure` (which is `gs_quant.target.common.RiskMeasure`, a `@dataclass` inheriting `Base`)

The parent `__RiskMeasure` is a dataclass with these fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| asset_class | `Optional[AssetClass]` | `None` | Asset class for the risk measure |
| measure_type | `Optional[RiskMeasureType]` | `None` | Type of risk measure (e.g., Delta, Vega) |
| unit | `Optional[RiskMeasureUnit]` | `None` | Unit of the risk measure (Percent, Dollar, BPS, Pips) |
| parameters | `Optional[RiskMeasureParameter]` | `None` | Parameters for the risk measure |
| value | `Optional[Union[float, str]]` | `None` | Numeric or string value |
| name | `Optional[str]` | `None` | Human-readable name |

This subclass adds no new fields but overrides `__lt__`, `__repr__`, and adds a `pricing_context` property.

### ParameterisedRiskMeasure (class)
Inherits: `RiskMeasure` (the custom one defined in this module)

Same fields as `RiskMeasure` plus custom `__init__` that validates and assigns `parameters`. No additional dataclass fields are declared -- the `parameters` field is inherited from the grandparent `__RiskMeasure` dataclass.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| asset_class | `Union[AssetClass, str]` | `None` | Asset class (inherited) |
| measure_type | `Union[RiskMeasureType, str]` | `None` | Risk measure type (inherited) |
| unit | `Union[RiskMeasureUnit, str]` | `None` | Unit (inherited) |
| value | `Union[float, str]` | `None` | Value (inherited) |
| parameters | `RiskMeasureParameter` | `None` | Must be a `RiskMeasureParameter` instance if provided |
| name | `str` | `None` | Name (inherited) |

## Enums and Constants

### PositionType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| OPEN | `"open"` | Open positions (corporate action adjusted) |
| CLOSE | `"close"` | Close positions (reflect trading activity on the close) |
| ANY | `"any"` | Any position type |

### DateLimit(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| LOW_LIMIT | `datetime.date(1952, 1, 1)` | Earliest allowed date boundary. The raw value is a `datetime.date` object, not a string. |

### PayReceive(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Pay | `"Pay"` | Pay fixed leg |
| Receive | `"Rec"` | Receive fixed leg. NOTE: raw value is `"Rec"`, not `"Receive"` |
| Straddle | `"Straddle"` | Straddle (both pay and receive) |

### Module Constants
None declared at module level (beyond the re-exports from `gs_quant.target.common`).

## Functions/Methods

### PayReceive._missing_(cls, key) -> Optional[PayReceive]
Purpose: Classmethod hook called by Python's Enum machinery when a value lookup does not match any member directly. Provides flexible matching for pay/receive lookups.

**Algorithm:**
1. Branch: if `key` is an instance of `_PayReceive` (the auto-generated `gs_quant.target.common.PayReceive` enum) -> replace `key` with `key.value` (extract the string value)
2. After potential conversion, `key` is expected to be a string.
3. Branch: if `key.lower()` is in `('receive', 'receiver')` -> return `cls.Receive`
4. Branch: otherwise -> delegate to `super()._missing_(key)`, which is `EnumBase._missing_`. That performs case-insensitive matching against all members' `.value` strings. For example, `'pay'` would match `Pay` (value `'Pay'`), `'straddle'` would match `Straddle`, `'rec'` would match `Receive` (value `'Rec'`).
5. If `EnumBase._missing_` finds no match, it returns `None`, which causes Python to raise a `ValueError`.

**Key behaviors:**
- `PayReceive('Receive')` -> `PayReceive.Receive` (via the `'receive'` lowered check)
- `PayReceive('Receiver')` -> `PayReceive.Receive` (via the `'receiver'` lowered check)
- `PayReceive('Rec')` -> `PayReceive.Receive` (via direct member value match, before `_missing_` is called)
- `PayReceive('rec')` -> `PayReceive.Receive` (via `EnumBase._missing_` case-insensitive match on `'Rec'`)
- `PayReceive(_PayReceive.Receive)` -> `PayReceive.Receive` (extracts `_PayReceive.Receive.value` = `'Receive'`, then lowered matches `'receive'`)
- `PayReceive(_PayReceive.Pay)` -> `PayReceive.Pay` (extracts value `'Pay'`, lowered is `'pay'`, not in receive set, delegates to `EnumBase._missing_` which matches `'Pay'` case-insensitively)

### RiskMeasure.__lt__(self, other) -> bool
Purpose: Define less-than ordering for `RiskMeasure` instances, enabling sorting of risk measures.

**Algorithm:**
1. Branch: if `self.name != other.name` -> return `self.name < other.name` (lexicographic comparison on name)
2. Branch: elif `self.parameters is not None`:
   a. Branch: if `other.parameters is None` -> return `False` (self has params, other doesn't; self is not less)
   b. Branch: if `not isinstance(other.parameters, type(self.parameters))` -> return `self.parameters.parameter_type < other.parameters.parameter_type` (different parameter types, compare by parameter_type)
   c. Branch: else (same parameter type) -> return `repr(self.parameters) < repr(other.parameters)` (same type, compare string representations)
3. Branch: elif `other.parameters is not None` -> return `True` (self has no params, other does; self is less)
4. Return `False` (both names equal, both parameters None; they are equal, so not less-than)

**Branch summary (6 branches):**
| # | Condition | Result |
|---|-----------|--------|
| 1 | names differ | `self.name < other.name` |
| 2 | self has params, other has no params | `False` |
| 3 | self has params, other has params, different types | compare `parameter_type` |
| 4 | self has params, other has params, same type | compare `repr()` |
| 5 | self has no params, other has params | `True` |
| 6 | both names equal, both params None | `False` |

### RiskMeasure.__repr__(self) -> str
Purpose: Return a concise string representation using the name or measure type name.

**Algorithm:**
1. Return `self.name` if it is truthy, otherwise return `self.measure_type.name`

Note: Uses Python's short-circuit `or` operator. If `self.name` is `None` or empty string, falls through to `self.measure_type.name`. If `self.measure_type` is also `None`, this will raise `AttributeError`.

### RiskMeasure.pricing_context (property) -> PricingContext
Purpose: Lazy accessor for the current `PricingContext`. Uses a deferred import to avoid circular dependency.

**Algorithm:**
1. Import `PricingContext` from `gs_quant.markets` (deferred/lazy import inside the property body)
2. Return `PricingContext.current` (a class property/context variable that returns the active pricing context)

### ParameterisedRiskMeasure.__init__(self, asset_class=None, measure_type=None, unit=None, value=None, parameters=None, name=None)
Purpose: Initialize a risk measure with validated parameters.

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| asset_class | `Union[AssetClass, str]` | `None` | Asset class |
| measure_type | `Union[RiskMeasureType, str]` | `None` | Measure type |
| unit | `Union[RiskMeasureUnit, str]` | `None` | Unit |
| value | `Union[float, str]` | `None` | Value |
| parameters | `RiskMeasureParameter` | `None` | Risk measure parameters |
| name | `str` | `None` | Human-readable name |

**Algorithm:**
1. Call `super().__init__(asset_class=asset_class, measure_type=measure_type, unit=unit, value=value, name=name)`. Note: `parameters` is NOT passed to super.
2. Branch: if `parameters` is truthy:
   a. Branch: if `isinstance(parameters, RiskMeasureParameter)` -> set `self.parameters = parameters`
   b. Branch: else -> raise `TypeError(f"Unsupported parameter {parameters}")`
3. If `parameters` is falsy (None, or any falsy value), `self.parameters` retains whatever the parent set (which defaults to `None` from the dataclass field).

**Raises:** `TypeError` when `parameters` is provided but is not an instance of `RiskMeasureParameter`.

### ParameterisedRiskMeasure.__repr__(self) -> str
Purpose: Produce a detailed string representation including parameter key-value pairs.

**Algorithm:**
1. Set `name = self.name or self.measure_type.name` (same fallback logic as parent `__repr__`)
2. Set `params = None`
3. Branch: if `self.parameters` is truthy:
   a. Call `self.parameters.as_dict()` to get a dictionary representation
   b. Remove the `'parameter_type'` key from the dict (via `params.pop('parameter_type', None)`)
   c. Sort the remaining keys case-insensitively: `sorted(params.keys(), key=lambda x: x.lower())`
   d. For each key in sorted order, format as `'{key}:{value}'` where:
      - Branch: if `params[k]` is an instance of `EnumBase` -> use `params[k].value` (the raw enum value)
      - Branch: else -> use `params[k]` directly
   e. Join all formatted pairs with `', '`
4. Branch: if `params` is truthy (non-empty string after joining) -> return `name + '(' + params + ')'`
5. Branch: if `params` is falsy (None because no parameters, or empty string) -> return just `name`

**Output format examples:**
- With parameters: `"DeltaLocalCcy(currency:USD, aggregation_level:Type)"`
- Without parameters: `"Delta"`

### ParameterisedRiskMeasure.parameter_is_empty(self) -> bool
Purpose: Check whether this risk measure has parameters set.

**Algorithm:**
1. Return `self.parameters is None`

Returns `True` if parameters is `None`, `False` otherwise.

## State Mutation
- `ParameterisedRiskMeasure.__init__` sets `self.parameters` on the instance if a valid `RiskMeasureParameter` is provided.
- `RiskMeasure.pricing_context` is a read-only property; no mutation occurs.
- The wildcard import `from gs_quant.target.common import *` populates this module's namespace with all public names from the target module at import time.
- No module-level mutable state is declared in this file.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | `ParameterisedRiskMeasure.__init__` | When `parameters` is truthy but not an instance of `RiskMeasureParameter` |
| `ValueError` (implicit) | `PayReceive._missing_` | When no member matches the given key (Python Enum raises this when `_missing_` returns `None`) |
| `AttributeError` (implicit) | `RiskMeasure.__repr__` | If both `self.name` is falsy and `self.measure_type` is `None` |
| `AttributeError` (implicit) | `PayReceive._missing_` | If `key` is not a `_PayReceive` instance and not a string (calling `.lower()` on it would fail) |

## Edge Cases
- `PayReceive._missing_` receives a `_PayReceive` enum member: it extracts `.value` before comparison. This is important because the auto-generated `_PayReceive` enum has different members (including `Payer`, `Receiver`, `Rec`) that must map correctly to the custom `PayReceive` enum.
- `PayReceive('Rec')` does NOT go through `_missing_` at all -- it matches the raw value of `PayReceive.Receive` directly since `Receive = 'Rec'`.
- `RiskMeasure.__lt__` with `self.name == other.name == None`: the first branch `self.name != other.name` evaluates `None != None` which is `False`, so it falls through. If both parameters are also `None`, returns `False`.
- `RiskMeasure.__lt__` does not handle the case where `other` is not a `RiskMeasure` -- no type checking is performed.
- `ParameterisedRiskMeasure.__init__` passes `name` to super but does NOT pass `parameters`. The parent dataclass `__init__` sets `parameters=None` by default, and `ParameterisedRiskMeasure` conditionally overwrites it.
- `ParameterisedRiskMeasure.__repr__` calls `params.pop('parameter_type', None)` which mutates the dict returned by `as_dict()`. This is safe because `as_dict()` returns a new dict each time.
- `DateLimit.LOW_LIMIT.value` is a `datetime.date` object, not a string. This is atypical for enums and must be handled specially in Elixir (likely as a module attribute or a function returning a `Date` struct).
- The `ParameterisedRiskMeasure.__repr__` method produces `name + '(' + params + ')'` even if `params` is an empty string `''` -- however, this case should not arise in practice because `as_dict()` on a non-None `RiskMeasureParameter` should always have at least one key besides `parameter_type`.

## Coverage Notes
- Branch count: approximately 17
  - `PayReceive._missing_`: 4 branches (isinstance check true/false, lower in receive set true/false)
  - `RiskMeasure.__lt__`: 6 branches (see table above)
  - `RiskMeasure.__repr__`: 2 branches (name truthy/falsy)
  - `ParameterisedRiskMeasure.__init__`: 3 branches (parameters falsy, parameters is RiskMeasureParameter, parameters is wrong type)
  - `ParameterisedRiskMeasure.__repr__`: 4 branches (parameters truthy/falsy, name truthy/falsy from `or`, EnumBase check per param value)
  - `ParameterisedRiskMeasure.parameter_is_empty`: 2 branches (None vs not None) -- trivial
- Missing branches: none expected; all branches are exercisable
- Pragmas: none in this file
