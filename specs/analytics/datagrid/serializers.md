# serializers.py

## Summary
Single-function module providing `row_from_dict`, which dispatches row deserialization to either `RowSeparator.from_dict` or `DataRow.from_dict` based on the row's `type` field. Acts as a factory/router for DataGrid row deserialization.

## Dependencies
- Internal:
  - `gs_quant.analytics.datagrid.data_row` (RowSeparator, DataRow, ROW_SEPARATOR)
- External:
  - `typing` (List, Dict)

## Type Definitions
None (no classes or type aliases defined).

## Enums and Constants
None (uses `ROW_SEPARATOR` imported from `data_row`).

## Functions/Methods

### row_from_dict(row: Dict, reference_list: List) -> Union[RowSeparator, DataRow]
Purpose: Dispatch row deserialization based on the row type field.

**Algorithm:**
1. Branch: `row.get('type') == ROW_SEPARATOR`
   - True: return `RowSeparator.from_dict(row)`
   - False: return `DataRow.from_dict(row, reference_list)`

## State Mutation
- `reference_list`: May be mutated by `DataRow.from_dict` (appends reference entry) when the else branch is taken.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None raised directly | - | Exceptions propagate from RowSeparator.from_dict or DataRow.from_dict |

## Edge Cases
- Row dict without `type` key: `row.get('type')` returns `None`, which is not equal to `ROW_SEPARATOR`, so it falls through to `DataRow.from_dict`
- Row with unknown type value: also falls through to `DataRow.from_dict`

## Bugs Found
None.

## Coverage Notes
- Branch count: 2
  - `row.get('type') == ROW_SEPARATOR`: True path, False path
- No pragmas
