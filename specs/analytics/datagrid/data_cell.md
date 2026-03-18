# data_cell.py

## Summary
`DataCell` represents a single cell in a DataGrid. Holds a processor, entity, and value. Builds cell graph and updates value from processor results.

## Dependencies
- Internal: BaseProcessor, DataQueryInfo, MeasureQueryInfo, ProcessorResult, Override, get_utc_now, Entity

## Class: DataCell

### __init__
Deep-copies processor. Defaults value to ProcessorResult(False, DATA_CELL_NOT_CALCULATED).

### build_cell_graph(all_queries, rdate_entity_map)
1. If self.processor exists:
   a. Set processor.parent = self
   b. Call processor.build_graph(...)
   c. Store cell_queries, extend all_queries

### update(result)
1. If result.data is pd.Series:
   a. If empty → value = ProcessorResult(False, 'Empty series...')
   b. Else → value = ProcessorResult(True, result.data.iloc[-1])
2. Else → value = ProcessorResult(True, result.data)
3. Set updated_time

## Edge Cases
- Empty series → failure result
- Non-series data (dict, str) → success with raw data

## Bugs Found
None.

## Coverage Notes
- ~6 branches
