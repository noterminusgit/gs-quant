# query_helpers.py

## Summary
Helper functions for aggregating, fetching, and building DataGrid data queries. Provides the bridge between the processor graph and the GS data service API: groups queries by dataset, fetches data via `GsSession`, builds query strings for dimension filtering, and validates dimension presence in DataFrames.

## Dependencies
- Internal:
  - `gs_quant.analytics.core.processor` (`MeasureQueryInfo`)
  - `gs_quant.analytics.core.processor_result` (`ProcessorResult`)
  - `gs_quant.data` (`DataFrequency`)
  - `gs_quant.session` (`GsSession`)
- External:
  - `datetime` (as `dt`: `dt.date`, `dt.datetime`)
  - `asyncio` (`get_event_loop`, `run_until_complete`)
  - `logging` (`getLogger`)
  - `collections` (`defaultdict`)
  - `typing` (`Dict`, `Tuple`, `Union`)
  - `pandas` (as `pd`: `pd.DataFrame`, `pd.to_datetime`)

## Type Definitions
None. This module defines no classes or dataclasses.

### Module-Level Variables
| Name | Type | Description |
|------|------|-------------|
| `_logger` | `logging.Logger` | Module-level logger via `logging.getLogger(__name__)` |

## Enums and Constants
None.

## Functions/Methods

### aggregate_queries(query_infos: List) -> Dict
Purpose: Groups a list of `DataQueryInfo`/`MeasureQueryInfo` objects by dataset ID and query range, aggregating dimensions for batched fetching.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `query_infos` | list of `DataQueryInfo` or `MeasureQueryInfo` | (required) | The query info objects to aggregate |

**Return type:** `defaultdict(dict)` -- nested mapping of `dataset_id -> query_key -> query_map`

**Algorithm:**
1. Create `mappings = defaultdict(dict)`
2. For each `query_info` in `query_infos`:
   a. Branch: if `query_info` is `MeasureQueryInfo` -> `continue` (skip)
   b. Extract `query = query_info.query`, `coordinate = query.coordinate`, `dataset_id = coordinate.dataset_id`
   c. Get `dataset_mappings = mappings[dataset_id]`
   d. Get `query_key = query.get_range_string()`
   e. Branch: if `dataset_id is None`:
      - Create `ProcessorResult(False, f'No dataset resolved for measure={coordinate.measure} with dimensions={coordinate.dimensions}')`
      - Call `asyncio.get_event_loop().run_until_complete(query_info.processor.calculate(query_info.attr, series, None))`
      - `continue`
   f. `setdefault` on `dataset_mappings[query_key]` with initial structure:
      ```
      {
        'datasetId': dataset_id,
        'parameters': {},
        'queries': defaultdict(list),
        'range': {},
        'realTime': True if coordinate.frequency == DataFrequency.REAL_TIME else False,
        'measures': set()
      }
      ```
   g. Get `query_map = dataset_mappings[query_key]`
   h. Branch: if `query_map['range']` is empty (falsy):
      - Branch: if `query.start` is `dt.date` -> set `query_map['range']['startDate']`
      - Branch: elif `query.start` is `dt.datetime` -> set `query_map['range']['startTime']`
      - Branch: if `query.end` is `dt.date` -> set `query_map['range']['endDate']`
      - Branch: elif `query.end` is `dt.datetime` -> set `query_map['range']['endTime']`
   i. Append `query_info` to `queries[coordinate.get_dimensions()]`
   j. Add `coordinate.measure` to `query_map['measures']`
   k. For each `(dimension, value)` in `coordinate.dimensions.items()`:
      - `parameters.setdefault(dimension, set())`
      - `parameters[dimension].add(value)`
3. Return `mappings`

### fetch_query(query_info: Dict) -> pd.DataFrame
Purpose: Executes a data query against the GS data service API and returns the result as a DataFrame.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `query_info` | `Dict` | (required) | A query map dict with keys: `'parameters'`, `'range'`, `'datasetId'`, `'realTime'` |

**Return type:** `pd.DataFrame`

**Algorithm:**
1. Build `where` clause from `query_info['parameters']`:
   a. For each `(key, value)` in parameters:
      - Convert `value` (set) to list
      - Branch: if first element is `bool`:
        - Branch: if only 1 value -> `where[key] = value_list[0]`
        - `continue` (skip if both True and False present)
      - Else -> `where[key] = list(value)`
2. Build `query = {'where': where, **query_info['range'], 'useFieldAlias': True, 'remapSchemaToAlias': True}`
3. Try:
   a. Branch: if `query_info['realTime']` AND `query_info['range']` is empty (falsy):
      - POST to `/data/{datasetId}/last/query` with payload=query
   b. Else:
      - POST to `/data/{datasetId}/query` with payload=query
4. Except `Exception as e`:
   - Log error
   - Return empty `pd.DataFrame()`
5. Create `df = pd.DataFrame(response.get('data', {}))`
6. Branch: if `df` is empty -> return `df`
7. Branch: if `'date'` in `df.columns` -> set index to `'date'`; else -> set index to `'time'`
8. Convert index to datetime, remove timezone info (`tz_localize(None)`)
9. Return `df`

### build_query_string(dimensions: iterable) -> str
Purpose: Builds a pandas-style query string from a sequence of (key, value) dimension tuples.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dimensions` | iterable of `(str, Any)` tuples | (required) | Key-value pairs for dimensions |

**Return type:** `str`

**Algorithm:**
1. Initialize `output = ''`
2. For each `(count, dimension)` in `enumerate(dimensions)`:
   a. Get `value = dimension[1]`
   b. Branch: if `value` is a `str` -> wrap in double quotes: `f'"{value}"'`
   c. Branch: if `count == 0` -> `output += f'{dimension[0]} == {value}'` (no prefix)
   d. Else -> `output += f' & {dimension[0]} == {value}'` (with `' & '` prefix)
3. Return `output`

### valid_dimensions(query_dimensions: Tuple[str, Union[str, float, bool]], df: pd.DataFrame) -> bool
Purpose: Checks whether all dimension keys exist as columns in the DataFrame.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `query_dimensions` | `Tuple[str, Union[str, float, bool]]` | (required) | Tuple of (dimension_name, value) pairs |
| `df` | `pd.DataFrame` | (required) | The DataFrame to check columns against |

**Return type:** `bool`

**Algorithm:**
1. Get `columns = df.columns`
2. For each `query_dimension` in `query_dimensions`:
   a. Get `dimension = query_dimension[0]`
   b. Branch: if `dimension not in columns` -> return `False`
3. Return `True`

## State Mutation
- `mappings`: Local to `aggregate_queries`, built up and returned
- `query_info.processor`: `calculate()` called on it in `aggregate_queries` when `dataset_id is None` (triggers async state changes on the processor)
- `GsSession.current`: Read-only access for HTTP calls in `fetch_query`
- `_logger`: Module-level logger; read-only after creation
- Thread safety: `fetch_query` uses `GsSession.current` which is thread-local. `aggregate_queries` uses `asyncio.get_event_loop().run_until_complete()` which must be called from a non-async context.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` (generic) | `fetch_query` (caught) | Any exception during GsSession POST; caught, logged, returns empty DataFrame |
| (indirect) `ProcessorResult(False, ...)` | `aggregate_queries` | When `dataset_id is None`, a failure result is created and propagated via `calculate()` |

## Edge Cases
- `dataset_id=None` -> immediate failure ProcessorResult, no API call
- Bool dimension with both `True` and `False` values -> skipped entirely (not added to `where`)
- Bool dimension with single value -> set directly (not as list)
- Empty DataFrame from API -> returned as-is (no index manipulation)
- `'date'` vs `'time'` column detection: if neither exists, `set_index` will raise `KeyError`
- `query_info['range']` empty dict is falsy -> triggers `/last/query` endpoint for real-time
- `build_query_string` with empty dimensions -> returns `''`
- `valid_dimensions` with empty `query_dimensions` -> returns `True` (vacuously)
- `aggregate_queries` uses `asyncio.get_event_loop()` (deprecated in Python 3.10+) not `asyncio.get_running_loop()`

## Bugs Found
None.

## Coverage Notes
- Branch count: ~38
- `GsSession` calls need mocking in tests
- `asyncio.get_event_loop().run_until_complete()` in `aggregate_queries` needs event loop setup
- `fetch_query` exception path needs mock to raise during POST
- Bool dimension branches require specific test data (single bool vs both bools)
- `'date'` vs `'time'` index branch requires DataFrames with different column names
- Pragmas: none marked
