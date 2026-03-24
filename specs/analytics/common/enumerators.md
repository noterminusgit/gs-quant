# enumerators.py

## Summary
Defines the `ScaleShape` enum with three values representing visual shapes used by scale processors in the analytics/datagrid system.

## Dependencies
- Internal: None
- External: `enum` (`Enum`)

## Type Definitions
None beyond the enum defined below.

## Enums and Constants

### ScaleShape(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `DIAMOND` | `"diamond"` | Diamond shape for scale visualization |
| `PIPE` | `"pipe"` | Pipe shape for scale visualization |
| `BAR` | `"bar"` | Bar shape for scale visualization |

### Module Constants
None.

## Functions/Methods
None. This module contains no functions or methods.

## State Mutation
None. Enum members are immutable once defined.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `ScaleShape(value)` | Built-in Enum behavior when `value` is not one of `"diamond"`, `"pipe"`, `"bar"` |

## Edge Cases
- Enum values are lowercase strings. Constructing `ScaleShape("Diamond")` (capitalized) will raise `ValueError`.
- `ScaleShape` is referenced in `processor.py`'s `PARSABLE_OBJECT_MAP` under key `'scaleShape'`.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No branches to cover; this is a pure enum definition module.
- Pragmas: none
