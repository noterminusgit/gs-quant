# serializers.py (lightweight)

## Summary
Single function `row_from_dict` that dispatches to `RowSeparator.from_dict` or `DataRow.from_dict` based on row type.

## Branches
1. row.get('type') == ROW_SEPARATOR → RowSeparator.from_dict
2. else → DataRow.from_dict
