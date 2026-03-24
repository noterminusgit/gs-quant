# processor_result.py

## Summary
Defines `ProcessorResult`, a simple dataclass that serves as the universal return type for all processor computations in the analytics system. Carries a success flag and the resulting data (which may be a string error message, a pandas Series, or a dictionary).

## Dependencies
- Internal: None
- External:
  - `dataclasses` (`dataclass`)
  - `typing` (`Union`, `Dict`)
  - `pandas` (as `pd`: `pd.Series`)

## Type Definitions

### ProcessorResult (dataclass)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `success` | `bool` | (required) | Whether the processor computation succeeded |
| `data` | `Union[str, pd.Series, Dict]` | (required) | The result data: error message string on failure, `pd.Series` for time series data on success, or `Dict` for structured results |

**Notes:**
- Both fields are positional (no defaults), so both must be provided at construction time.
- The `data` field is mutable. Callers (e.g., `BaseProcessor.post_process()` and `BaseProcessor.__handle_date_range()`) may mutate `data` in place when it is a `pd.Series`.
- The dataclass auto-generates `__init__`, `__repr__`, and `__eq__`.

## Enums and Constants
None.

## Functions/Methods
None beyond the auto-generated dataclass methods (`__init__`, `__repr__`, `__eq__`).

### Auto-generated: ProcessorResult.__init__(self, success: bool, data: Union[str, pd.Series, Dict]) -> None
Purpose: Construct a ProcessorResult with success status and data.

### Auto-generated: ProcessorResult.__eq__(self, other) -> bool
Purpose: Structural equality comparison of two ProcessorResult instances (compares `success` and `data` fields).

### Auto-generated: ProcessorResult.__repr__(self) -> str
Purpose: String representation showing field values.

## State Mutation
- `self.data`: Mutable after creation. `BaseProcessor.post_process()` replaces `self.data` with `self.data.iloc[-1:]`. `BaseProcessor.__handle_date_range()` replaces `self.data` with a filtered subset.
- `self.success`: Not mutated after creation by any known caller.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | `__init__` (auto) | If required arguments not provided |

## Edge Cases
- `data` can be an empty `pd.Series` (success=True with no data points)
- `data` can be a string even when `success=True` (no enforcement of string-only-on-failure)
- Equality comparison with `pd.Series` in `data` field may raise a ValueError from pandas ("The truth value of a Series is ambiguous") if compared naively; the auto-generated `__eq__` uses `==` which returns element-wise for Series

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No branches to cover; this is a pure dataclass definition.
- Pragmas: none
