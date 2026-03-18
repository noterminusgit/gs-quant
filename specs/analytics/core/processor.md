# processor.py

## Summary
Core processor framework: BaseProcessor (abstract), DataQueryInfo, MeasureQueryInfo, and the DataCoordinateOrProcessor union type. BaseProcessor provides graph building, update/calculate lifecycle, serialization/deserialization, and date range handling.

## Dependencies
- Internal: analytics.common (constants), helpers, ProcessorResult, DataCoordinate, DataQuery, Entity, RelativeDate, Window, Returns, Currency, ScaleShape
- External: asyncio, datetime, functools, logging, uuid, abc, concurrent.futures, dataclasses, enum, typing, numpy, pandas, pydash

## Dataclasses

### DataQueryInfo
Fields: attr, processor, query, entity, data (default None)

### MeasureQueryInfo
Fields: attr, processor, entity

## Class: BaseProcessor (ABCMeta)

### __init__(**kwargs)
- Generates UUID-based id
- Default value: ProcessorResult(False, 'Value not set')
- Optional kwargs: last_value, measure_processor

### process() [abstract]
Subclasses implement computation logic.

### post_process()
1. If last_value AND value is ProcessorResult AND success AND data is Series AND not empty:
   → Truncate to last element only

### __handle_date_range(result, rdate_entity_map)
1. If result not ProcessorResult or not success → return
2. Get start, end from self
3. If neither start nor end → return
4. Get entity from data_cell, resolve entity_id
5. Three branches:
   a. start AND end: resolve rdates if needed → mask with >= start AND <= end
   b. start only: resolve rdate → mask with >= start
   c. end only: resolve rdate → mask with <= end (BUG 1 FIXED: was >= end)

### update(attribute, result, rdate_entity_map, pool, query_info)
1. If not measure_processor → apply date range filter
2. Store result in children_data
3. If result is ProcessorResult AND success:
   a. If pool: run in executor (measure_processor → process(entity), else process())
   b. Else: direct call
   c. Call post_process()
   d. On exception → failure ProcessorResult
4. Else: propagate failure

### get_plot_expression() [abstract]

### __add_required_rdates(entity, rdate_entity_map)
1. Check start/end for RelativeDate instances → add to rdate_entity_map

### build_graph(entity, cell, queries, rdate_entity_map, overrides)
1. Set data_cell
2. Add required rdates
3. If measure_processor → append MeasureQueryInfo
4. For each child:
   a. DataCoordinate → apply overrides if any, create DataQuery, append to queries
   b. BaseProcessor → set parent/parent_attr, recurse build_graph
   c. DataQueryInfo → set parent/parent_attr/processor, append

### calculate(attribute, result, rdate_entity_map, pool, query_info)
1. Call update()
2. If parent exists:
   a. If value successful:
      - Parent is BaseProcessor → recurse calculate on parent
      - Else (DataCell) → parent.update(value)
   b. Else → put error on data_cell

### as_dict()
Complex serialization:
1. Build base dict with TYPE, PROCESSOR_NAME, PARAMETERS
2. For each type-hinted __init__ parameter:
   a. DataCoordinate/Processor types → recursive as_dict()
   b. Entity → ENTITY dict with id/type
   c. date/datetime → formatted string
   d. Enum → .value
   e. list → [item.as_dict() for item]
   f. builtin → raw value
   g. Other → .as_dict()

### from_dict(cls, obj, reference_list) [classmethod]
Complex deserialization:
1. Dynamic import of processor class by name
2. For each parameter in parameters dict:
   a. DATA_COORDINATE → DataCoordinate.from_dict
   b. PROCESSOR → recursive BaseProcessor.from_dict
   c. ENTITY → defer to reference_list for later resolution
   d. DATE/DATETIME/RELATIVE_DATE → parse to appropriate type
   e. PARSABLE_OBJECT_MAP types → Enum or .from_dict
   f. LIST → recursive for dicts
   g. Other → raw value
3. Instantiate processor with arguments
4. Link references

## Edge Cases
- entity is string → entity_id = ''
- pool is None → synchronous execution
- Unknown processor name → processor = None → from_dict returns None
- PARSABLE_OBJECT_MAP: only window, returns, currency, scaleShape
- Override with coordinate_id: first matching coordinate_id wins; if none match and first override has no id, use first

## Bugs Found
- Bug 1 (line 143): FIXED — end-only date range used >= instead of <=

## Coverage Notes
- 112 branches
- Requires async test infrastructure (asyncio.get_running_loop)
- ProcessPoolExecutor path needs special handling
