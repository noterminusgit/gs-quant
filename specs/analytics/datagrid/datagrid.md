# datagrid.py

## Summary
`DataGrid` is the main analytics grid class. It orchestrates initialization (building cell processor graphs), polling (resolving relative dates, resolving data queries, fetching data), and post-processing (sorting, filtering, formatting). Supports CRUD operations via the DataGrid API and serialization/deserialization. Also contains the module-level helper function `_get_overrides`.

## Dependencies
- Internal:
  - `gs_quant.analytics.common` (DATAGRID_HELP_MSG)
  - `gs_quant.analytics.common.helpers` (resolve_entities, get_entity_rdate_key, get_entity_rdate_key_from_rdate, get_rdate_cache_key)
  - `gs_quant.analytics.core.processor` (DataQueryInfo, MeasureQueryInfo)
  - `gs_quant.analytics.core.processor_result` (ProcessorResult)
  - `gs_quant.analytics.core.query_helpers` (aggregate_queries, fetch_query, build_query_string, valid_dimensions)
  - `gs_quant.analytics.datagrid.data_cell` (DataCell)
  - `gs_quant.analytics.datagrid.data_column` (DataColumn, ColumnFormat, MultiColumnGroup)
  - `gs_quant.analytics.datagrid.data_row` (DataRow, DimensionsOverride, ProcessorOverride, Override, ValueOverride, RowSeparator)
  - `gs_quant.analytics.datagrid.serializers` (row_from_dict)
  - `gs_quant.analytics.datagrid.utils` (DataGridSort, SortOrder, SortType, DataGridFilter, FilterOperation, FilterCondition, get_utc_now)
  - `gs_quant.analytics.processors` (CoordinateProcessor, EntityProcessor)
  - `gs_quant.common` (Entitlements as Entitlements_)
  - `gs_quant.datetime.relative_date` (RelativeDate)
  - `gs_quant.entities.entitlements` (Entitlements)
  - `gs_quant.entities.entity` (Entity)
  - `gs_quant.errors` (MqValueError)
  - `gs_quant.session` (GsSession, OAuth2Session)
- External:
  - `asyncio` (get_event_loop)
  - `datetime` (dt.datetime, dt.date)
  - `json` (dumps)
  - `logging` (getLogger)
  - `webbrowser` (open)
  - `collections` (defaultdict)
  - `dataclasses` (asdict)
  - `numbers` (Number)
  - `typing` (List, Dict, Optional, Tuple, Union, Set)
  - `numpy` (np.nan, np.isclose)
  - `pandas` (pd.DataFrame, pd.Series, pd.concat)

## Type Definitions

### DataGrid (class)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id_ | `str` | `None` | Unique identifier (set after persistence) |
| entitlements | `Union[Entitlements, Entitlements_]` | `None` | Access control entitlements |
| name | `str` | required | Name of the DataGrid |
| rows | `List[Union[DataRow, RowSeparator]]` | required | Row definitions |
| columns | `List[DataColumn]` | required | Column definitions |
| sorts | `List[DataGridSort]` | `[]` | Sort configurations |
| filters | `List[DataGridFilter]` | `[]` | Filter configurations |
| multiColumnGroups | `Optional[List[MultiColumnGroup]]` | `None` | Column grouping for heatmaps |
| polling_time | `int` | `0` (via property) | Polling interval in ms |
| _primary_column_index | `int` | `0` (from kwargs) | Index of primary expanding column |
| _cells | `List[DataCell]` | `[]` | All cells in the grid |
| _data_queries | `List[Union[DataQueryInfo, MeasureQueryInfo]]` | `[]` | All data queries |
| _entity_cells | `List[DataCell]` | `[]` | Cells using EntityProcessor |
| _coord_processor_cells | `List[DataCell]` | `[]` | Cells using CoordinateProcessor |
| _value_cells | `List[DataCell]` | `[]` | Cells with value overrides |
| entity_map | `Dict[str, Entity]` | `{}` | Map of entity ID to Entity |
| rdate_entity_map | `Dict[str, Set[Tuple]]` | `defaultdict(set)` | Map of entity to rdate rules |
| rule_cache | `Dict[str, dt.date]` | `{}` | Cached resolved rdate values |
| results | `List[List[DataCell]]` | `[]` | 2D grid of cells (rows x columns) |
| is_initialized | `bool` | `False` | Whether initialize() has been called |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| API | `str` | `'/data/grids'` | Base API path for DataGrid CRUD |
| DATAGRID_HEADERS | `Dict[str, str]` | `{'Content-Type': 'application/json;charset=utf-8'}` | HTTP headers for API calls |
| _logger | `logging.Logger` | `logging.getLogger(__name__)` | Module logger |

## Functions/Methods

### DataGrid.__init__(self, name: str, rows: List[Union[DataRow, RowSeparator]], columns: List[DataColumn], *, id_: str = None, entitlements: Union[Entitlements, Entitlements_] = None, polling_time: int = None, sorts: Optional[List[DataGridSort]] = None, filters: Optional[List[DataGridFilter]] = None, multiColumnGroups: Optional[List[MultiColumnGroup]] = None, **kwargs) -> None
Purpose: Initialize the DataGrid, store configuration, set up internal state, print help message.

**Algorithm:**
1. Store `id_`, `entitlements`, `name`, `rows`, `columns`
2. `self.sorts = sorts or []`
3. `self.filters = filters or []`
4. `self.multiColumnGroups = multiColumnGroups`
5. `self.polling_time = polling_time or 0` (triggers property setter)
6. Initialize internal collections: `_primary_column_index` from kwargs (default 0), `_cells`, `_data_queries`, `_entity_cells`, `_coord_processor_cells`, `_value_cells` as empty lists; `entity_map` as empty dict
7. Initialize `rdate_entity_map = defaultdict(set)`, `rule_cache = {}`
8. Initialize `results = []`, `is_initialized = False`
9. Call `print(DATAGRID_HELP_MSG)`

### DataGrid.polling_time (property getter) -> int
Purpose: Return the polling time value.

### DataGrid.polling_time (property setter, value: int) -> None
Purpose: Validate and set polling time.

**Algorithm:**
1. Branch: `value is None` -> set `self.__polling_time = 0`
2. Branch: `value != 0 and value < 5000` -> raise `MqValueError('polling_time must be >= than 10000ms.')`
3. Set `self.__polling_time = value`

### DataGrid.get_id(self) -> Optional[str]
Purpose: Return the unique DataGrid identifier.

**Algorithm:**
1. Return `self.id_`

### DataGrid.initialize(self) -> None
Purpose: Build processor graphs for all cells across all rows and columns.

**Algorithm:**
1. Initialize `all_queries = []`, `entity_cells = []`, `current_row_group = None`
2. For each `(row_index, row)` in `enumerate(self.rows)`:
   a. Branch: `isinstance(row, RowSeparator)` -> set `current_row_group = row.name`, continue
   b. Get `entity = row.entity`
   c. Branch: `isinstance(entity, Entity)` -> `self.entity_map[entity.get_marquee_id()] = entity`
   d. Else -> `self.entity_map[''] = entity`
   e. Initialize `cells = []`, get `row_overrides = row.overrides`
   f. For each `(column_index, column)` in `enumerate(self.columns)`:
      i. Get `column_name`, `column_processor`
      ii. Call `_get_overrides(row_overrides, column_name)` -> `data_overrides, value_override, processor_override`
      iii. Create `DataCell(column_name, column_processor, entity, data_overrides, column_index, row_index, current_row_group)`
      iv. Branch: `processor_override` -> set `cell.processor = processor_override`
      v. Branch: `value_override` -> set `cell.value = ProcessorResult(True, value_override.value)`, set `cell.updated_time`
      vi. Elif `isinstance(column_processor, EntityProcessor)` -> append to `entity_cells`
      vii. Elif `isinstance(column_processor, CoordinateProcessor)`:
           - Branch: `len(data_overrides)` > 0 -> set dimensions from last override
           - Append to `self._coord_processor_cells`
      viii. Elif `column_processor.measure_processor` -> append `MeasureQueryInfo` to `all_queries`
      ix. Else -> call `cell.build_cell_graph(all_queries, self.rdate_entity_map)`
      x. Append cell to `cells`
   g. Extend `self._cells` with `cells`, append cells to `self.results`
3. Store `self._data_queries = all_queries`, `self._entity_cells = entity_cells`
4. Set `self.is_initialized = True`

### DataGrid.poll(self) -> None
Purpose: Execute the full data fetch pipeline.

**Algorithm:**
1. Call `self._resolve_rdates()`
2. Call `self._resolve_queries()`
3. Call `self._process_special_cells()`
4. Call `self._fetch_queries()`

### DataGrid._process_special_cells(self) -> None
Purpose: Process EntityProcessor and CoordinateProcessor cells.

**Algorithm:**
1. For each `cell` in `self._entity_cells`:
   a. Try: `cell.value = cell.processor.process(cell.entity)`
   b. Except `Exception as e`: set `cell.value` to error string with processor name, entity ID, and exception
   c. Set `cell.updated_time = get_utc_now()`
2. For each `cell` in `self._coord_processor_cells`:
   a. Try: `cell.value = cell.processor.process()`
   b. Except `Exception as e`: set `cell.value` to error string
   c. Set `cell.updated_time = get_utc_now()`

### DataGrid._resolve_rdates(self, rule_cache: Dict = None) -> None
Purpose: Resolve RelativeDate rules to concrete dates for all entities.

**Algorithm:**
1. `rule_cache = rule_cache or {}`
2. Determine `calendar`: Branch: `not GsSession.current.is_internal() and isinstance(GsSession.current, OAuth2Session)` -> `calendar = []`; else -> `calendar = None`
3. For each `(entity_id, rules)` in `self.rdate_entity_map.items()`:
   a. Get entity from `self.entity_map.get(entity_id)`
   b. Initialize `currencies = None`, `exchanges = None`
   c. Branch: `isinstance(entity, Entity)`:
      - Get `entity_dict = entity.get_entity()`
      - Get `currency = entity_dict.get("currency")`; if truthy -> `currencies = [currency]`
      - Get `exchange = entity_dict.get("exchange")`; if truthy -> `exchanges = [exchange]`
   d. For each `rule_base_date_tuple` in `rules`:
      i. Extract `rule` and `base_date`
      ii. Compute `cache_key` via `get_rdate_cache_key`
      iii. Check `date_value = rule_cache.get(cache_key)`
      iv. Branch: `date_value is None`:
          - Branch: `base_date` truthy -> parse to `dt.date` via `strptime`
          - Apply `RelativeDate(rule, base_date).apply_rule(currencies, exchanges, holiday_calendar=calendar)`
          - Store in `rule_cache[cache_key]`
      v. Store in `self.rule_cache[get_entity_rdate_key(...)]`

### DataGrid._resolve_queries(self, availability_cache: Dict = None) -> None
Purpose: Resolve dataset IDs and coordinates for each data query.

**Algorithm:**
1. `availability_cache = availability_cache or {}`
2. For each `query` in `self._data_queries`:
   a. Get `entity = query.entity`
   b. Branch: `isinstance(entity, str) or isinstance(query, MeasureQueryInfo)` -> continue (skip)
   c. Reassign `query = query.query` (DataQuery object)
   d. Get `coord`, `entity_dimension`, `entity_id`
   e. Branch: `isinstance(query_start, RelativeDate)` -> resolve from `self.rule_cache`
   f. Branch: `isinstance(query_end, RelativeDate)` -> resolve from `self.rule_cache`
   g. Branch: `entity_dimension not in coord.dimensions`:
      - Branch: `coord.dataset_id` truthy -> set dimensions directly, update coordinate
      - Else -> try fetching availability:
        - Check `availability_cache.get(entity_id)`
        - Branch: `raw_availability is None` -> fetch via API, cache
        - Call `entity.get_data_coordinate(...)` to resolve coordinate
        - On `Exception` -> log info message

### DataGrid._fetch_queries(self) -> None
Purpose: Aggregate and fetch data queries, then calculate processor results.

**Algorithm:**
1. Call `aggregate_queries(self._data_queries)` -> `query_aggregations`
2. For each `(dataset_id, query_map)` in `query_aggregations.items()`:
   a. For each `query` in `query_map.values()`:
      - `df = fetch_query(query)`
      - For each `(query_dimensions, query_infos)` in `query['queries'].items()`:
        - Branch: `valid_dimensions(query_dimensions, df)`:
          - True: query df, assign measure column to each `query_info.data`
          - False: assign empty `pd.Series(dtype=float)` to each `query_info.data`
3. For each `query_info` in `self._data_queries`:
   a. Branch: `isinstance(query_info, MeasureQueryInfo)` -> `asyncio.get_event_loop().run_until_complete(calculate(...))`
   b. Elif `query_info.data is None or len(query_info.data) == 0` -> calculate with failure ProcessorResult
   c. Else -> calculate with success ProcessorResult containing the data

### DataGrid._post_process(self) -> pd.DataFrame
Purpose: Build DataFrame from cell results with rounding, filtering, and sorting.

**Algorithm:**
1. Build `results` dict from `self.results`:
   a. For each row: append `rowGroup` (from first cell, or `''`)
   b. For each cell: Branch `column_value.success`:
      - True + isinstance Number -> round to `format_.precision`, append
      - True + not Number -> append raw data
      - False -> append `np.nan`
2. Create `df = pd.DataFrame.from_dict(results)`
3. Get unique `row_groups` from df
4. For each row_group: apply `__handle_filters`, then `__handle_sorts`, collect sub_dfs
5. `pd.concat(sub_dfs)`
6. Set multi-index `['rowGroup', df.index]`, rename axis to `['', '']`
7. Return df

### DataGrid.__handle_sorts(self, df) -> pd.DataFrame
Purpose: Apply sort configurations to DataFrame.

**Algorithm:**
1. For each `sort` in `self.sorts`:
   a. `ascending = True if sort.order == SortOrder.ASCENDING else False`
   b. Branch: `sort.sortType == SortType.ABSOLUTE_VALUE` -> reindex by abs().sort_values
   c. Else -> `df.sort_values(by=sort.columnName, ascending=ascending, na_position='last')`
2. Return df

### DataGrid.__handle_filters(self, df) -> pd.DataFrame
Purpose: Apply filter configurations to DataFrame.

**Algorithm:**
1. Branch: `not len(df)` -> return df (empty)
2. Copy `starting_df = df.copy()`, set `running_df = df`
3. For each `filter_` in `self.filters`:
   a. Branch: `filter_value is None` -> continue
   b. Branch: `filter_condition == FilterCondition.OR` -> reset `df = starting_df`; else -> `df = running_df`
   c. Dispatch by `operation`:
      - `TOP`: sort descending, head(filter_value)
      - `BOTTOM`: sort ascending, head(filter_value)
      - `ABSOLUTE_TOP`: reindex by abs descending, head
      - `ABSOLUTE_BOTTOM`: reindex by abs ascending, head
      - `EQUALS`:
        - Wrap non-list to list
        - Branch: first element is str -> `df.loc[df[col].isin(filter_value)]`
        - Else -> `np.isclose` with atol=1e-10
      - `NOT_EQUALS`:
        - Wrap non-list to list
        - Branch: first element is str -> `~df[col].isin(filter_value)`
        - Else -> `~np.isclose` with atol=1e-10
      - `GREATER_THAN`: `df[col] > filter_value`
      - `LESS_THAN`: `df[col] < filter_value`
      - `LESS_THAN_EQUALS`: `df[col] <= filter_value`
      - `GREATER_THAN_EQUALS`: `df[col] >= filter_value`
      - else -> raise `MqValueError`
   d. Branch: `filter_.condition == FilterCondition.OR` -> `running_df = running_df.merge(df, how='outer')`
   e. Else -> `running_df = df`
4. Return `running_df`

### DataGrid.to_frame(self) -> pd.DataFrame
Purpose: Return results as DataFrame.

**Algorithm:**
1. Branch: `not self.is_initialized` -> log info, return empty `pd.DataFrame()`
2. Else -> return `self._post_process()`

### DataGrid.save(self) -> str
Purpose: Save or update DataGrid via API.

**Algorithm:**
1. `datagrid_json = self.__as_json()`
2. Branch: `self.id_` truthy -> PUT to `{API}/{self.id_}`
3. Else -> POST to `{API}`, set `self.id_ = response['id']`
4. Return `DataGrid.from_dict(response).id_`

### DataGrid.create(self) -> str
Purpose: Create a new DataGrid via API (always POST).

**Algorithm:**
1. `datagrid_json = self.__as_json()`
2. POST to `{API}`
3. Set `self.id_ = response['id']`
4. Return `response['id']`

### DataGrid.delete(self) -> None
Purpose: Delete persisted DataGrid.

**Algorithm:**
1. Branch: `self.id_` truthy -> DELETE `{API}/{self.id_}`
2. Else -> raise `MqValueError('DataGrid has not been persisted.')`

### DataGrid.open(self) -> None
Purpose: Open DataGrid in browser.

**Algorithm:**
1. Branch: `self.id_ is None` -> raise `MqValueError('DataGrid must be created or saved before opening.')`
2. Transform domain: replace ".web" with "", if result is `'https://api.gs.com'` -> use `'https://marquee.gs.com'`
3. Construct URL and call `webbrowser.open(url)`

### DataGrid.set_primary_column_index(self, index: int) -> None
Purpose: Set the primary column index.

**Algorithm:**
1. Set `self._primary_column_index = index`

### DataGrid.set_sorts(self, sorts: List[DataGridSort]) -> None
Purpose: Replace the sorts list.

### DataGrid.add_sort(self, sort: DataGridSort, index: int = None) -> None
Purpose: Add a sort to the list.

**Algorithm:**
1. Branch: `index` truthy -> `self.sorts.insert(index, sort)`
2. Else -> `self.sorts.append(sort)`

Note: `index=0` is falsy, so inserting at position 0 will actually append instead.

### DataGrid.set_filters(self, filters: List[DataGridFilter]) -> None
Purpose: Replace the filters list.

### DataGrid.add_filter(self, filter_: DataGridFilter, index: int = None) -> None
Purpose: Add a filter to the list.

**Algorithm:**
1. Branch: `index` truthy -> `self.filters.insert(index, filter_)`
2. Else -> `self.filters.append(filter_)`

Note: Same `index=0` bug as `add_sort`.

### DataGrid.from_dict(cls, obj, reference_list: Optional[List] = None) -> DataGrid
Purpose: Deserialize DataGrid from API response dict.

**Algorithm:**
1. Extract `id_`, `name`, `parameters`, `entitlements`
2. Branch: `reference_list is not None` -> `should_resolve_entities = False`; else -> `True`, init empty list
3. Parse rows via `row_from_dict`, columns via `DataColumn.from_dict`, sorts, filters, multi_column_groups
4. Branch: `should_resolve_entities` -> call `resolve_entities(reference_list)`
5. Construct and return `DataGrid(...)`

### DataGrid.as_dict(self) -> dict
Purpose: Serialize DataGrid to dict for API.

**Algorithm:**
1. Build base dict with `name`, `parameters` (rows, columns, primaryColumnIndex, pollingTime)
2. Branch: `self.entitlements` truthy:
   a. Branch: `isinstance(self.entitlements, Entitlements_)` -> `as_dict()`
   b. Elif `isinstance(self.entitlements, Entitlements)` -> `to_dict()`
   c. Else -> use raw value
3. Branch: `len(self.sorts)` > 0 -> add sorts
4. Branch: `len(self.filters)` > 0 -> add filters
5. Branch: `self.multiColumnGroups` truthy -> add multiColumnGroups
6. Return dict

### DataGrid.__as_json(self) -> str
Purpose: Serialize to JSON string.

**Algorithm:**
1. Return `json.dumps(self.as_dict())`

### DataGrid.aggregate_queries(query_infos) [staticmethod]
Purpose: Aggregate queries by dataset_id (appears unused/legacy).

**Algorithm:**
1. Build `mappings = defaultdict(dict)` grouping by dataset_id and query range
2. For each query_info: accumulate dimensions into parameter sets

### _get_overrides(row_overrides: List[Override], column_name: str) -> Tuple[List[DimensionsOverride], Optional[ValueOverride], Optional[ProcessorOverride]]
Purpose: Extract matching overrides for a given column name from row overrides.

**Algorithm:**
1. Branch: `not row_overrides` -> return `([], None, None)`
2. Initialize `dimensions_overrides = [], value_override = None, processor_override = None`
3. For each override in `row_overrides`:
   a. Branch: `column_name in override.column_names`:
      - Branch: `isinstance(override, DimensionsOverride)` -> append
      - Elif `isinstance(override, ValueOverride)` -> assign
      - Elif `isinstance(override, ProcessorOverride)` -> assign `override.processor`
4. Return tuple

## State Mutation
- `self.entity_map`: Populated during `initialize()`
- `self._cells`, `self._data_queries`, `self._entity_cells`, `self._coord_processor_cells`: Set during `initialize()`
- `self.results`: Populated during `initialize()`
- `self.is_initialized`: Set to `True` after `initialize()`
- `self.rule_cache`: Populated during `_resolve_rdates()`
- `self.rdate_entity_map`: Populated during `build_cell_graph()` calls in `initialize()`
- `self.id_`: Set during `save()`, `create()`, `from_dict()`
- Cell values: Updated during `_process_special_cells()`, `_fetch_queries()` via processor calculate
- `print(DATAGRID_HELP_MSG)`: Side effect in `__init__`
- Thread safety: `asyncio.get_event_loop().run_until_complete()` used in `_fetch_queries()` -- not thread-safe

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `polling_time.setter` | When `value != 0 and value < 5000` |
| `MqValueError` | `delete` | When `self.id_` is falsy |
| `MqValueError` | `open` | When `self.id_ is None` |
| `MqValueError` | `__handle_filters` | When `operation` is unknown |
| `Exception` (caught) | `_process_special_cells` | Any exception from processor.process() -- caught, stored as error string |
| `Exception` (caught) | `_resolve_queries` | Exception from availability fetch -- caught, logged as info |

## Edge Cases
- `polling_time` between 1-4999: raises `MqValueError` (but error message says ">= 10000ms" which is misleading)
- `polling_time = 0`: allowed (no polling)
- `polling_time = None`: coerced to 0
- Entity is string (fetch failed): skipped in `_resolve_queries` via isinstance check
- Empty df from fetch: `valid_dimensions` returns False -> empty series assigned
- Filter with `None` value: skipped entirely
- `EQUALS`/`NOT_EQUALS` filter with float values: uses `np.isclose` with tolerance `atol=1e-10`
- `EQUALS`/`NOT_EQUALS` filter with string values: uses `isin`/`~isin`
- `add_sort(sort, index=0)`: `index=0` is falsy, so appends instead of inserting at position 0
- `add_filter(filter_, index=0)`: same bug as `add_sort`
- `to_frame()` before `initialize()`: returns empty DataFrame with info log
- `open()`: domain `'https://api.gs.com'` is special-cased to `'https://marquee.gs.com'`
- `as_dict()` entitlements: three-way dispatch on type (Entitlements_, Entitlements, or raw)

## Bugs Found
- `add_sort` / `add_filter`: `if index:` is falsy for `index=0`, so inserting at the beginning of the list does not work. Should be `if index is not None:`.
- `polling_time` setter error message says ">= 10000ms" but the actual check is `< 5000`.

## Coverage Notes
- Branch count: ~170
- Heavy GsSession mocking required for save/create/delete/open and _resolve_queries
- `asyncio.get_event_loop()` used in `_fetch_queries` -- needs asyncio test setup
- `webbrowser.open` needs mock in `open()`
- `print(DATAGRID_HELP_MSG)` side effect in `__init__`
- `__handle_filters` has ~22 branches (10 filter operations + OR/AND + type checks)
- `initialize` has ~12 branches (RowSeparator, Entity isinstance, 6-way cell type dispatch)
- `_resolve_rdates` has ~6 branches
- `_resolve_queries` has ~8 branches
- `as_dict` has ~6 branches (entitlements type, sorts, filters, multiColumnGroups)
