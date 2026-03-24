# utilities.py

## Summary
Provides data extraction and transformation utilities for the GS Data Platform. Contains the `Utilities` class with methods for parallel dataset querying, identifier mapping, batch processing, CSV export, and coverage retrieval. Also contains `SecmasterXrefFormatter`, which implements a sweep-line algorithm to merge overlapping cross-reference (xref) time periods into consolidated, non-overlapping periods with combined identifiers.

## Dependencies
- Internal: `gs_quant.target.assets` (FieldFilterMap, EntityQuery)
- Internal: `gs_quant.session` (GsSession)
- External: `os` (getcwd, path.exists, access, makedirs, W_OK)
- External: `dataclasses` (dataclass)
- External: `enum` (Enum)
- External: `itertools` (groupby)
- External: `functools` (partial)
- External: `concurrent.futures` (ThreadPoolExecutor)
- External: `math` (ceil)
- External: `pandas` (DataFrame, concat)
- External: `datetime` (datetime, timedelta, date)
- External: `typing` (Dict, List, Any, Union, Tuple)

## Type Definitions

### Utilities (class)
Inherits: none

A static utility class with no instance state. All methods are `@staticmethod` or `@classmethod`. Contains a nested `AssetApi` class.

### Utilities.AssetApi (nested class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (no instance fields) | | | All methods are classmethods |

#### Type Alias
```
IdList = Union[Tuple[str, ...], List]
```

### SecmasterXrefFormatter (class)
Inherits: none

A static utility class for converting raw cross-reference records into consolidated time periods. Contains nested `EventType` enum and `Event` dataclass.

### SecmasterXrefFormatter.EventType (Enum)
See Enums section below.

### SecmasterXrefFormatter.Event (dataclass)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `date` | `str` | required | Date string in `YYYY-MM-DD` format for the event |
| `event_type` | `SecmasterXrefFormatter.EventType` | required | Whether this is a START or END event |
| `record` | `Dict[str, Any]` | required | The original xref record this event was created from |
| `priority` | `int` | computed in `__post_init__` | Sort priority: `1` for END events, `0` for START events; ensures END events are processed after START events on the same date |

## Enums and Constants

### SecmasterXrefFormatter.EventType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| START | `"start"` | Marks the beginning of a cross-reference record's active period |
| END | `"end"` | Marks the end of a cross-reference record's active period |

### SecmasterXrefFormatter Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `INFINITY_DATE` | `str` | `"9999-12-31"` | Canonical representation of an unbounded end date |
| `INFINITY_MARKER` | `str` | `"9999-99-99"` | Sentinel marker in input data indicating infinity; normalized to `INFINITY_DATE` during processing |

## Functions/Methods

### Utilities.AssetApi.__create_query(cls, fields: Union[List, Tuple] = None, as_of: dt.datetime = None, limit: int = None, scroll: str = None, scroll_id: str = None, order_by: List[str] = None, **kwargs) -> EntityQuery
Purpose: Build a validated `EntityQuery` for the asset API, rejecting any kwargs not recognized by `FieldFilterMap`.

**Algorithm:**
1. Extract `keys = set(kwargs.keys())`
2. Compute `valid = keys.intersection(FieldFilterMap.properties())`
3. Compute `invalid = keys.difference(valid)`
4. Branch: `invalid` is truthy (non-empty) -> format bad args as `"key=value"` strings, raise `KeyError` with message listing all invalid arguments
5. Branch: `invalid` is falsy -> proceed
6. Construct and return `EntityQuery` with:
   - `where=FieldFilterMap(**kwargs)`
   - `fields=fields`
   - `asOfTime=as_of or dt.datetime.utcnow()` (Branch: `as_of` is truthy -> use it; else -> use `utcnow()`)
   - `limit=limit`
   - `scroll=scroll`
   - `scroll_id=scroll_id`
   - `order_by=order_by`

**Raises:** `KeyError` when any kwargs key is not a valid `FieldFilterMap` property

### Utilities.AssetApi.get_many_assets_data(cls, fields: IdList = None, as_of: dt.datetime = None, limit: int = None, **kwargs) -> dict
Purpose: Query the asset data API and return results.

**Algorithm:**
1. Call `cls.__create_query(fields, as_of, limit, **kwargs)` to build the query
2. POST to `'/assets/data/query'` via `GsSession.current.sync.post` with the query as payload
3. Return `response['results']`

### Utilities.target_folder() -> Union[str, int, None]
Purpose: Create (or locate) a timestamped target directory for CSV export in the current working directory.

**Algorithm:**
1. Get `get_cwd = os.getcwd()`
2. Build `target_dir` name as `"data_extract_" + datetime.now().strftime("%d_%m_%Y_%H_%M_%S")`
3. Branch: `os.path.exists(get_cwd + "\\" + target_dir)` -> return `get_cwd + "\\" + target_dir`
4. Branch (elif): `os.access(get_cwd, os.W_OK)` ->
   - Try: `os.makedirs(target_dir)`, return `get_cwd + "\\" + target_dir`
   - Except `Exception`: print the exception, return `1` (error sentinel)
5. Implicit return: `None` if directory does not exist and cwd is not writable

Note: Uses Windows-style backslash path separators (`"\\"`) which will fail on Linux/Mac.

### Utilities.pre_checks(final_end, original_start, time_field, datetime_delta_override, request_batch_size, write_to_csv) -> Tuple
Purpose: Validate all input parameters before data extraction begins.

**Algorithm:**
1. Branch: `write_to_csv` is truthy ->
   - Call `Utilities.target_folder()`
   - Branch: result `== 1` -> raise `ValueError("Current working directory doesn't have write permissions...")`
   - Branch: result != 1 -> `target_dir_result = result`
2. Branch: `write_to_csv` is falsy -> `target_dir_result = None`
3. Branch: `request_batch_size is None or not (0 < request_batch_size < 5)` -> raise `ValueError("Enter request batch size beteen 1-5")`
4. Branch: `datetime_delta_override is not None` ->
   - Branch: `not isinstance(datetime_delta_override, int)` -> raise `ValueError("Time delta override must be greater than 0 and 1 - 5 for intraday dataset")`
   - Branch (elif): `isinstance(datetime_delta_override, int) and datetime_delta_override < 0` -> raise `ValueError("Time delta override must be greater than 0 and 1 - 5 for intraday dataset")`
   - Branch (elif): `time_field == "time" and datetime_delta_override > 5` -> raise `ValueError("Time delta override must be greater than 0 and 1 - 5 for intraday dataset")`
5. Branch: `final_end is not None` ->
   - Branch: `not isinstance(final_end, dt.datetime)` -> raise `ValueError("End date must of datetime.datetime format...")`
   - Branch (elif): `not isinstance(original_start, dt.datetime)` -> raise `ValueError("Start date must be of datetime.datetime format...")`
   - Branch (elif): `original_start > final_end` -> raise `ValueError("Start date cannot be greater than end date...")`
   - Branch (elif): `time_field == "time" and (final_end - original_start).total_seconds() / 3600 > 5` -> raise `ValueError("For intraday datasets diff between start & end date should be <= 5 hrs")`
6. Return `(final_end, target_dir_result)`

**Raises:** `ValueError` for multiple validation failures (see algorithm branches above)

### Utilities.batch(iterable, n=1) -> generator
Purpose: Yield successive slices of `iterable` of size `n`.

**Algorithm:**
1. Compute `iter_len = len(iterable)`
2. For `ndx` in `range(0, iter_len, n)`:
   - Yield `iterable[ndx : min(ndx + n, iter_len)]`

### Utilities.fetch_data(dataset, symbols, start=dt.datetime.now(), end=dt.datetime.now(), dimension="assetId", auth=None) -> pd.DataFrame
Purpose: Fetch data from a dataset for a batch of symbols, optionally authenticating first.

**Algorithm:**
1. Branch: `auth is not None` -> call `auth()` (invoke the authentication callable)
2. Try: return `dataset.get_data(start, end, **{dimension: symbols})`
3. Except `Exception as ex`: print the exception, return `pd.DataFrame()` (empty DataFrame)

Note: Default parameter values `start=dt.datetime.now()` and `end=dt.datetime.now()` are evaluated at module import time, not at call time. This is a known Python gotcha.

### Utilities.execute_parallel_query(dataset, coverage, start, end, symbol_dimension, parallel_factor, batch_size, authenticate, retry=0) -> pd.DataFrame
Purpose: Execute parallel data fetches across symbol batches using a thread pool, with retry logic.

**Algorithm:**
1. Create `bound_get_data` as a `partial` of `Utilities.fetch_data` with `dataset`, `start`, `end`, `dimension=symbol_dimension`, `auth=authenticate`
2. Print status: number of symbols and expected batch count
3. Open `ThreadPoolExecutor(max_workers=parallel_factor)` context manager (variable name `e` shadows the outer exception `e` in the except block -- see note)
4. Try:
   - `df = pd.concat(e.map(bound_get_data, Utilities.batch(coverage, n=batch_size)))`
   - Print success message with row count
5. Except `Exception as e`:
   - Print failure message
   - Branch: `retry > 3` -> raise `Exception("retry failure")`
   - Branch: else -> increment `retry`, recursively call `Utilities.execute_parallel_query(...)` with incremented retry
6. Return `df`

Note: The `except` clause uses `e` as the exception variable, which shadows the `ThreadPoolExecutor` context variable also named `e`. The `df` variable may be unbound if the exception path is taken and retry <= 3, since the recursive call assigns to a local `df` inside the recursive frame. The returned `df` from recursion is assigned to the local `df` in the except block.

**Raises:** `Exception("retry failure")` when retry count exceeds 3

### Utilities.get_dataset_parameter(dataset) -> List
Purpose: Extract key parameters from a dataset definition for use in data extraction.

**Algorithm:**
1. Get `dimensions = dataset.provider.symbol_dimensions(dataset.id)`
2. Extract `symbol_dimension = dimensions[0]`
3. Get `dataset_definition = dataset.provider.get_definition(dataset.id)`
4. Extract `history_time = dataset_definition.parameters.history_date`
5. Extract `time_field = dataset_definition.dimensions.time_field`
6. Branch: `time_field == "date"` -> `timedelta = dt.timedelta(days=180)`
7. Branch: else -> `timedelta = dt.timedelta(hours=1)`
8. Return `[time_field, history_time, symbol_dimension, timedelta]`

### Utilities.write_consolidated_results(data_frame, target_dir_result, dataset, batch_number, handler, write_to_csv, coverage_length, symbols_per_csv)
Purpose: Write a batch of results to CSV or pass to a handler function.

**Algorithm:**
1. Branch: `data_frame.shape[0] > 0` (DataFrame has rows) ->
   - Branch: `write_to_csv` is truthy -> write to CSV at `"{target_dir_result}\\{dataset.id}-batch {batch_number}.csv"`
   - Branch: else -> call `handler(data_frame)`
2. Print batch status message

Note: Uses Windows backslash path separator.

### Utilities.iterate_over_series(dataset, coverage_batch, original_start, original_end, datetime_delta_override, symbol_dimension, request_batch_size, authenticate, final_end, write_to_csv, target_dir_result, batch_number, coverage_length, symbols_per_csv, handler, parallel_factor=5)
Purpose: Iterate through time windows, executing parallel queries and accumulating results until the end date is reached.

**Algorithm:**
1. Set `start = original_start`, `end = original_end`
2. Initialize `data_frame = pd.DataFrame()`
3. Enter infinite `while True` loop:
   - Call `Utilities.execute_parallel_query(...)` to get `batch_frame`
   - Concatenate: `data_frame = pd.concat([data_frame, batch_frame], axis=0)`
   - Advance: `start += datetime_delta_override`, `end += datetime_delta_override`
   - Branch: `end > final_end` -> call `Utilities.write_consolidated_results(...)`, `break`
   - Branch: else -> continue loop
4. `del data_frame` (explicit cleanup)
5. Return `None`

### Utilities.extract_xref(assets, out_type) -> str
Purpose: From a list of asset records, return the value of `out_type` from the highest-ranked asset.

**Algorithm:**
1. Sort `assets` by `x.get("rank", 0)` in descending order
2. Return `sorted_list[0].get(out_type, "")` (first element's `out_type` value, or empty string)

### Utilities.map_identifiers(input_type: str, output_type: str, ids, as_of=dt.datetime.now()) -> dict
Purpose: Map a list of identifiers from one type to another using the asset API, processing in batches of 1000.

**Algorithm:**
1. Split `ids` into batches of 1000 via `Utilities.batch(ids, n=1000)`
2. Initialize `all_assets = []`
3. For each `asset_batch`:
   - Call `Utilities.AssetApi.get_many_assets_data` with `input_type` as kwarg key, `asset_batch` as value, `limit=min(5000, 5 * len(asset_batch))`, `as_of=as_of`, requested fields, and `asset_classifications_is_primary=[True]`
   - Sort returned assets by `input_type` key
   - Append to `all_assets`
4. Group `all_assets` by `input_type` using `itertools.groupby`
5. Return dict comprehension: `{inp_id: Utilities.extract_xref(grouped_assets, output_type) for ...}`

Note: Default parameter `as_of=dt.datetime.now()` is evaluated at module import time.

### Utilities.get_dataset_coverage(identifier, symbol_dimension, dataset) -> list
Purpose: Get coverage list for a dataset, with special handling depending on the symbol dimension type.

**Algorithm:**
1. Branch: `symbol_dimension == "assetId"` ->
   - Get coverage with `dataset.get_coverage(fields=[identifier])`
   - Filter to non-null rows: `cov[cov[identifier].notna()][identifier].tolist()`
   - Return filtered list
2. Branch (elif): `symbol_dimension == "gsid"` ->
   - Get full coverage, extract `symbol_dimension` column as list
   - Map identifiers via `Utilities.map_identifiers(symbol_dimension, identifier, coverage)`
   - Return `list(mapped_values.values())`
3. Branch (else): ->
   - Get full coverage, extract `symbol_dimension` column as list
   - Return the list directly

### SecmasterXrefFormatter.Event.__post_init__(self)
Purpose: Set the sort priority field based on event type.

**Algorithm:**
1. Branch: `self.event_type == SecmasterXrefFormatter.EventType.END` -> `self.priority = 1`
2. Branch: else -> `self.priority = 0`

### SecmasterXrefFormatter.convert(data: Dict[str, Any]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]
Purpose: Convert raw xref data (keyed by entity) into consolidated time periods per entity.

**Algorithm:**
1. Initialize `results = {}`
2. For each `(entity_key, records)` in `data.items()`:
   - Call `SecmasterXrefFormatter._convert_entity_records(records)` to get `xrefs`
   - Set `results[entity_key] = {"xrefs": xrefs}`
3. Return `results`

### SecmasterXrefFormatter._convert_entity_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]
Purpose: Convert a list of xref records for a single entity into consolidated, non-overlapping time periods using a sweep-line algorithm.

**Algorithm:**
1. Branch: `not records` (empty list) -> return `[]`
2. Normalize records: copy each record; if `endDate == INFINITY_MARKER` ("9999-99-99"), replace with `INFINITY_DATE` ("9999-12-31")
3. Branch: `not normalized_records` (empty after normalization) -> return `[]`
   Note: This branch is unreachable if step 1 passed, since normalization does not filter records.
4. Call `_create_events(normalized_records)` to generate events
5. Sort events by `(date_sort_key, priority)` -- dates sorted chronologically, END events after START events on the same date
6. Call `_process_events(events)` and return result

### SecmasterXrefFormatter._create_events(records: List[Dict[str, Any]]) -> List[Event]
Purpose: Create START and END events from xref records for the sweep-line algorithm.

**Algorithm:**
1. Initialize `events = []`
2. For each `record`:
   - Append a START event with `date=record['startDate']`
   - Branch: `record['endDate'] != INFINITY_DATE` ->
     - Call `_add_one_day(record['endDate'])` to get `next_day`
     - Branch: `next_day` is truthy (not None) -> append END event with `date=next_day`
     - Branch: `next_day` is None -> skip (no END event for infinity-adjacent dates)
   - Branch: `record['endDate'] == INFINITY_DATE` -> no END event created (period extends to infinity)
3. Return `events`

### SecmasterXrefFormatter._process_events(events: List[Event]) -> List[Dict[str, Any]]
Purpose: Process sorted events using sweep-line to generate non-overlapping time periods with merged identifiers.

**Algorithm:**
1. Initialize `periods = []`, `active_identifiers = {}` (type -> record mapping), `current_period_start = None`
2. Set `i = 0`
3. While `i < len(events)`:
   - Record `current_date = events[i].date`
   - Collect all events for `current_date` into `current_date_events` (advance `i`)
   - Branch: `active_identifiers` is truthy AND `current_period_start is not None` ->
     - Compute `period_end = _subtract_one_day(current_date)`
     - Append period: `{"startDate": current_period_start, "endDate": period_end, "identifiers": {type: value for active}}`
   - Separate events into `end_events` and `start_events`
   - Process END events: for each, remove `identifier_type` from `active_identifiers` if present
   - Process START events: for each, add/overwrite `identifier_type` in `active_identifiers`
   - Branch: `active_identifiers` is truthy -> `current_period_start = current_date`
4. After loop -- handle final period:
   - Branch: `active_identifiers` is truthy AND `current_period_start is not None` ->
     - Branch: any active record has `endDate == INFINITY_DATE` -> `period_end = INFINITY_DATE`
     - Branch: else -> `period_end = max(record['endDate'] for active records)`
     - Append final period
5. Return `periods`

### SecmasterXrefFormatter._date_sort_key(date_str: str) -> dt.datetime
Purpose: Convert a date string to a datetime for sorting purposes.

**Algorithm:**
1. Branch: `date_str == INFINITY_DATE` -> return `dt.datetime(9999, 12, 31)`
2. Branch: else -> return `dt.datetime.strptime(date_str, '%Y-%m-%d')`

### SecmasterXrefFormatter._add_one_day(date_str: str) -> Optional[str]
Purpose: Add one day to a date string, returning None for overflow/infinity cases.

**Algorithm:**
1. Try:
   - Branch: `date_str == INFINITY_DATE` -> return `None`
   - Parse `date_obj = dt.datetime.strptime(date_str, '%Y-%m-%d')`
   - Branch: `date_obj.year == 9999 and date_obj.month == 12 and date_obj.day == 31` -> return `None`
   - Compute `next_day = date_obj + dt.timedelta(days=1)`
   - Return `next_day.strftime('%Y-%m-%d')`
2. Except `(ValueError, OverflowError)` -> return `None`

Note: The second `9999-12-31` check is redundant with the first `INFINITY_DATE` check since `INFINITY_DATE == "9999-12-31"`.

### SecmasterXrefFormatter._subtract_one_day(date_str: str) -> str
Purpose: Subtract one day from a date string, returning the original string on error.

**Algorithm:**
1. Try:
   - Parse `date_obj = dt.datetime.strptime(date_str, '%Y-%m-%d')`
   - Compute `prev_day = date_obj - dt.timedelta(days=1)`
   - Return `prev_day.strftime('%Y-%m-%d')`
2. Except `(ValueError, OverflowError)` -> return `date_str` unchanged

## State Mutation
- No instance state in `Utilities` -- all methods are static/classmethod
- No instance state in `SecmasterXrefFormatter` -- all methods are static
- `SecmasterXrefFormatter.Event.priority`: Set in `__post_init__`, not mutated afterward
- `active_identifiers` dict in `_process_events`: mutated throughout the sweep-line loop (keys added/removed)
- Side effects: `Utilities.target_folder()` creates directories on the filesystem via `os.makedirs`
- Side effects: `Utilities.write_consolidated_results()` writes CSV files to disk
- Side effects: `Utilities.fetch_data()` may invoke `auth()` callable
- Side effects: `Utilities.execute_parallel_query()` prints status messages and may recursively retry
- Side effects: Multiple methods call `print()` for logging
- Thread safety: `execute_parallel_query` uses `ThreadPoolExecutor` for parallel I/O; `fetch_data` is called from multiple threads -- each invocation is independent (no shared mutable state)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `AssetApi.__create_query` | When kwargs contain keys not in `FieldFilterMap.properties()` |
| `ValueError` | `pre_checks` | When `write_to_csv` is true and `target_folder()` returns `1` (write permission failure) |
| `ValueError` | `pre_checks` | When `request_batch_size` is None or not in range (0, 5) exclusive |
| `ValueError` | `pre_checks` | When `datetime_delta_override` is not None and: not an int, or negative, or > 5 for intraday |
| `ValueError` | `pre_checks` | When `final_end` is not None and: not a datetime, or start not a datetime, or start > end, or intraday diff > 5 hours |
| `Exception` | `execute_parallel_query` | When retry count exceeds 3 ("retry failure") |
| `Exception` (caught) | `fetch_data` | Any exception from `dataset.get_data` is caught, printed, and returns empty DataFrame |
| `Exception` (caught) | `target_folder` | Any exception from `os.makedirs` is caught, printed, and returns `1` |

## Edge Cases
- `target_folder` uses Windows-style path separators (`"\\"`) and will produce incorrect paths on Unix/Linux/Mac systems
- `write_consolidated_results` also uses Windows backslash in the CSV path
- `fetch_data` default parameters `start=dt.datetime.now()` and `end=dt.datetime.now()` are evaluated once at import time, not per call -- they will always reflect the module load time
- `map_identifiers` default parameter `as_of=dt.datetime.now()` has the same import-time evaluation issue
- `execute_parallel_query` variable shadowing: the `ThreadPoolExecutor` context manager variable `e` is shadowed by the exception variable `e` in the except clause
- `execute_parallel_query`: if the except branch is taken with `retry <= 3`, the `df` variable is assigned from the recursive call; if the try branch succeeds, `df` is set from `pd.concat`. If try fails and retry > 3, `df` is never assigned and the function raises before reaching `return df`
- `pre_checks` has a typo in error message: "beteen" instead of "between"
- `pre_checks`: the `datetime_delta_override` check `isinstance(datetime_delta_override, int) and (datetime_delta_override < 0)` is redundant since the previous branch already excluded non-int types; also, `datetime_delta_override == 0` passes all checks but is likely invalid
- `extract_xref` will raise `IndexError` if `assets` is an empty list (accessing `[0]` on empty sorted list)
- `_convert_entity_records`: the second `not normalized_records` check is unreachable -- if `records` was non-empty, `normalized_records` will also be non-empty since normalization copies all records
- `_add_one_day`: the `9999-12-31` year/month/day check after `strptime` is redundant with the earlier `INFINITY_DATE` string comparison
- `_subtract_one_day`: subtracting one day from `"0001-01-01"` would underflow -- caught by the `OverflowError` handler, returning the original string
- `batch` with `n=0` will cause `range(0, iter_len, 0)` which raises `ValueError`
- `batch` with an empty iterable returns immediately (no yields)
- `iterate_over_series`: if `end` never exceeds `final_end` (e.g., `datetime_delta_override` is zero or negative), the loop runs infinitely

## Coverage Notes
- Branch count: 48
- Key branches in `pre_checks`: 12 distinct validation branches across `write_to_csv`, `request_batch_size`, `datetime_delta_override`, and `final_end` checks
- Key branches in `_process_events`: `active_identifiers` truthiness (x2: mid-loop and post-loop), `current_period_start is not None` (x2), `has_infinity` for final period
- Key branches in `_create_events`: `endDate != INFINITY_DATE`, `next_day` truthiness
- Key branches in `_add_one_day`: `date_str == INFINITY_DATE`, `9999-12-31` year/month/day check, try/except
- Key branches in `get_dataset_coverage`: 3-way branch on `symbol_dimension` ("assetId", "gsid", else)
- Key branches in `execute_parallel_query`: try/except, `retry > 3`
- Key branches in `target_folder`: `os.path.exists`, `os.access`, try/except
- Key branches in `write_consolidated_results`: `data_frame.shape[0] > 0`, `write_to_csv`
- Key branches in `fetch_data`: `auth is not None`, try/except
- Pragmas: none marked
