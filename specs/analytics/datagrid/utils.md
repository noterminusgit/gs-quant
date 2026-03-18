# utils.py

## Summary
Utility classes and enums for DataGrid: sort/filter configuration and UTC timestamp helper.

## Dependencies
- External: datetime, enum, dataclasses

## Functions

### get_utc_now()
Returns UTC now as ISO string with millisecond precision + 'Z'.

## Classes

### SortType(str, Enum)
VALUE, ABSOLUTE_VALUE

### SortOrder(str, Enum)
ASCENDING, DESCENDING

### FilterOperation(str, Enum)
TOP, BOTTOM, ABSOLUTE_TOP, ABSOLUTE_BOTTOM, EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_EQUALS, GREATER_THAN_EQUALS

### FilterCondition(str, Enum)
AND, OR

### DataGridSort (dataclass)
- __post_init__: coerces sortType and order to their enum types
- from_dict: filters dict keys to class fields

### DataGridFilter (dataclass)
- __post_init__: coerces operation and condition to their enum types
- from_dict: filters dict keys to class fields

## Edge Cases
- from_dict silently drops unknown keys

## Bugs Found
None.
