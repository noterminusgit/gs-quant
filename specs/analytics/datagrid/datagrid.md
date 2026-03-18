# datagrid.py

## Summary
DataGrid: main analytics grid class. Handles initialization (building cell graphs), polling (resolving queries, fetching data), and post-processing (sorting, filtering). Supports CRUD via DataGrid API.

## Dependencies
- Internal: analytics.common, core, datagrid, processors, entities, session
- External: asyncio, datetime, json, logging, webbrowser, collections, dataclasses, numbers, numpy, pandas

## Class: DataGrid

### __init__
- Stores rows, columns, sorts, filters, multiColumnGroups
- polling_time setter: None→0, 0 ok, <5000→MqValueError
- Prints DATAGRID_HELP_MSG on construction

### initialize()
1. For each row:
   a. RowSeparator → set current_row_group, continue
   b. Entity → add to entity_map; string → entity_map['']
   c. For each column:
      - Get overrides via _get_overrides
      - Create DataCell
      - Branch on processor override / value override / EntityProcessor / CoordinateProcessor / measure_processor / normal
      - Normal path: build_cell_graph
2. Store cells, queries, entity_cells

### poll()
1. _resolve_rdates → _resolve_queries → _process_special_cells → _fetch_queries

### _process_special_cells()
- Entity cells: call processor.process(entity) with try/except
- Coord processor cells: call processor.process() with try/except

### _resolve_rdates(rule_cache)
1. Determine calendar based on session type (internal vs OAuth2)
2. For each entity → each rule: resolve RelativeDate, cache result

### _resolve_queries(availability_cache)
1. For each query:
   a. Skip if entity is string or MeasureQueryInfo
   b. Resolve start/end RelativeDates from rule_cache
   c. If entity_dimension not in coord.dimensions:
      - If coord has dataset_id → set dimensions directly
      - Else → fetch availability, get_data_coordinate

### _fetch_queries()
1. Aggregate queries by dataset_id
2. For each aggregated query: fetch_query → match dimensions → assign data
3. For each query_info: calculate with appropriate ProcessorResult

### _post_process() → DataFrame
1. Build results dict from cells
2. Round numeric values by column format precision
3. Group by rowGroup → apply filters → apply sorts → concat
4. Set multi-index

### __handle_sorts(df)
- For each sort: ascending/descending, value/absolute_value

### __handle_filters(df) → DataFrame
- For each filter:
  1. Skip if value is None
  2. OR condition → reset to starting_df; AND → use running_df
  3. Dispatch by FilterOperation (TOP/BOTTOM/ABS_TOP/ABS_BOTTOM/EQUALS/NOT_EQUALS/GT/LT/LTE/GTE)
  4. EQUALS/NOT_EQUALS: list handling, string vs float tolerance
  5. OR condition → merge outer; AND → replace running_df

### to_frame()
- If not initialized → log, return empty
- Else → _post_process()

### save/create/delete/open
- CRUD via GsSession API
- open: domain transformation, webbrowser.open

### from_dict(cls, obj, reference_list)
1. Parse rows via row_from_dict
2. Parse columns via DataColumn.from_dict
3. Parse sorts, filters, multi_column_groups
4. Optionally resolve entities
5. Construct DataGrid

### as_dict()
Serializes to dict for API

### _get_overrides (module function)
1. If no overrides → return empty
2. For each override matching column_name: dispatch by type (Dimensions/Value/Processor)

## Edge Cases
- polling_time between 1-4999 → MqValueError (but 0 is OK)
- Entity is string (fetch failed) → skipped in _resolve_queries
- Empty df from fetch → valid_dimensions fails → empty series assigned
- Filter with None value → skipped
- EQUALS filter with float values → uses np.isclose tolerance

## Bugs Found
None in this file (bugs were in processor.py).

## Coverage Notes
- 170 branches
- Heavy GsSession mocking required
- asyncio.get_event_loop() used in _fetch_queries
- webbrowser.open needs mock
