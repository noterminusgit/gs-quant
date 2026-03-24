# utils.py

## Summary
Utility module for DataGrid providing sort/filter configuration dataclasses, their associated enums, and a UTC timestamp helper function. Defines `SortType`, `SortOrder`, `FilterOperation`, `FilterCondition` enums and `DataGridSort`, `DataGridFilter` dataclasses with auto-coercion in `__post_init__`.

## Dependencies
- Internal: None
- External:
  - `dataclasses` (dataclass, fields)
  - `datetime` (dt.datetime)
  - `enum` (Enum)
  - `typing` (Union, List)

## Type Definitions

### DataGridSort (dataclass)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| columnName | `str` | required | Name of column to sort by |
| sortType | `SortType` | `SortType.VALUE` | Type of sort (value or absolute value) |
| order | `SortOrder` | `SortOrder.ASCENDING` | Sort direction |

### DataGridFilter (dataclass)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| columnName | `str` | required | Name of column to filter on |
| operation | `FilterOperation` | required | Filter operation type |
| value | `Union[float, str, List[float], List[str]]` | required | Filter threshold/comparison value |
| condition | `FilterCondition` | `FilterCondition.AND` | How to combine with other filters |

## Enums and Constants

### SortType(str, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| VALUE | `'value'` | Sort by the column's actual value |
| ABSOLUTE_VALUE | `'absoluteValue'` | Sort by the absolute value of the column |

### SortOrder(str, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ASCENDING | `'ascending'` | Ascending sort order |
| DESCENDING | `'descending'` | Descending sort order |

### FilterOperation(str, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| TOP | `'top'` | Top N rows by value |
| BOTTOM | `'bottom'` | Bottom N rows by value |
| ABSOLUTE_TOP | `'absoluteTop'` | Top N rows by absolute value |
| ABSOLUTE_BOTTOM | `'absoluteBottom'` | Bottom N rows by absolute value |
| EQUALS | `'equals'` | Rows equal to value or list of values |
| NOT_EQUALS | `'notEquals'` | Rows not equal to value or list of values |
| GREATER_THAN | `'greaterThan'` | Rows greater than value |
| LESS_THAN | `'lessThan'` | Rows less than value |
| LESS_THAN_EQUALS | `'lessThanEquals'` | Rows less than or equal to value |
| GREATER_THAN_EQUALS | `'greaterThanEquals'` | Rows greater than or equal to value |

### FilterCondition(str, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| AND | `'and'` | Intersect with previous filter results |
| OR | `'or'` | Union with previous filter results |

### Module Constants
None.

## Functions/Methods

### get_utc_now() -> str
Purpose: Return the current UTC time as an ISO-8601 string with millisecond precision and 'Z' suffix.

**Algorithm:**
1. Call `dt.datetime.utcnow()`
2. Format as `"%Y-%m-%dT%H:%M:%S.%f"` (microseconds)
3. Truncate last 3 chars (microseconds to milliseconds): `[:-3]`
4. Append `'Z'`
5. Return the string

**Example output:** `'2024-01-15T10:30:45.123Z'`

### DataGridSort.__post_init__(self) -> None
Purpose: Coerce string field values to their enum types.

**Algorithm:**
1. `self.sortType = SortType(self.sortType)` -- coerces string to enum if needed
2. `self.order = SortOrder(self.order)` -- coerces string to enum if needed

**Raises:** `ValueError` when sortType or order string is not a valid enum member.

### DataGridSort.from_dict(cls, dict_: dict) -> DataGridSort
Purpose: Construct DataGridSort from dict, filtering to valid field names.

**Algorithm:**
1. Get `class_fields` as set of field names via `fields(cls)`
2. Filter `dict_` keys to those in `class_fields`
3. Return `DataGridSort(**filtered_dict)` (triggers `__post_init__`)

### DataGridFilter.__post_init__(self) -> None
Purpose: Coerce string field values to their enum types.

**Algorithm:**
1. `self.operation = FilterOperation(self.operation)` -- coerces string to enum
2. `self.condition = FilterCondition(self.condition)` -- coerces string to enum

**Raises:** `ValueError` when operation or condition string is not a valid enum member.

### DataGridFilter.from_dict(cls, dict_: dict) -> DataGridFilter
Purpose: Construct DataGridFilter from dict, filtering to valid field names.

**Algorithm:**
1. Get `class_fields` as set of field names via `fields(cls)`
2. Filter `dict_` keys to those in `class_fields`
3. Return `DataGridFilter(**filtered_dict)` (triggers `__post_init__`)

## State Mutation
- `DataGridSort.__post_init__`: Mutates `self.sortType` and `self.order` in-place (coercion from str to Enum)
- `DataGridFilter.__post_init__`: Mutates `self.operation` and `self.condition` in-place

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `DataGridSort.__post_init__` | Invalid string for `SortType` or `SortOrder` |
| `ValueError` | `DataGridFilter.__post_init__` | Invalid string for `FilterOperation` or `FilterCondition` |

## Edge Cases
- `from_dict` silently drops unknown keys -- no error on extra fields
- `from_dict` does not supply defaults for missing keys -- relies on dataclass defaults; missing required fields (columnName) will raise `TypeError`
- Passing an already-valid Enum to `__post_init__` is a no-op (Enum constructor returns same value)
- `get_utc_now` uses `datetime.utcnow()` which is deprecated in Python 3.12+ (should use `datetime.now(timezone.utc)`)

## Bugs Found
None.

## Coverage Notes
- Branch count: ~6
  - `get_utc_now`: 0 branches (no conditionals)
  - `DataGridSort.__post_init__`: 0 branches (always coerces)
  - `DataGridSort.from_dict`: 0 branches
  - `DataGridFilter.__post_init__`: 0 branches
  - `DataGridFilter.from_dict`: 0 branches
  - Total unique branches from conditionals: 0 (all paths are linear)
- Note: The branch count in the original spec (~10) likely counted enum value paths; the actual code has no if/else branching
- No pragmas
