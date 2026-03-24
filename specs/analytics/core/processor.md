# processor.py

## Summary
Core processor framework for the analytics/datagrid system. Defines `BaseProcessor` (abstract base class with metaclass `ABCMeta`) that provides graph building, async update/calculate lifecycle, serialization/deserialization, and date range handling. Also defines `DataQueryInfo` and `MeasureQueryInfo` dataclasses for tracking queries through the processor graph, and the `DataCoordinateOrProcessor` union type alias.

## Dependencies
- Internal:
  - `gs_quant.analytics.common` (`TYPE`, `PROCESSOR`, `PARAMETERS`, `DATA_COORDINATE`, `ENTITY`, `VALUE`, `DATE`, `DATETIME`, `PROCESSOR_NAME`, `ENTITY_ID`, `ENTITY_TYPE`, `PARAMETER`, `REFERENCE`, `RELATIVE_DATE`, `LIST`)
  - `gs_quant.analytics.common.enumerators` (`ScaleShape`)
  - `gs_quant.analytics.common.helpers` (`is_of_builtin_type`, `get_entity_rdate_key_from_rdate`)
  - `gs_quant.analytics.core.processor_result` (`ProcessorResult`)
  - `gs_quant.common` (`Currency`)
  - `gs_quant.data` (`DataCoordinate`, `DataFrequency`)
  - `gs_quant.data.coordinate` (`DateOrDatetime`)
  - `gs_quant.data.query` (`DataQuery`, `DataQueryType`)
  - `gs_quant.entities.entity` (`Entity`)
  - `gs_quant.timeseries` (`Window`, `Returns`, `RelativeDate`)
- External:
  - `asyncio` (`get_running_loop`)
  - `datetime` (as `dt`: `dt.date`, `dt.datetime`)
  - `functools` (`partial`)
  - `logging` (`getLogger`)
  - `uuid` (`uuid4`)
  - `abc` (`ABCMeta`, `abstractmethod`)
  - `concurrent.futures.process` (`ProcessPoolExecutor`)
  - `dataclasses` (`dataclass`)
  - `enum` (`Enum`, `EnumMeta`)
  - `typing` (`List`, `Optional`, `Union`, `Dict`, `get_type_hints`, `Set`, `Tuple`)
  - `numpy` (as `np`: `datetime64`)
  - `pandas` (as `pd`: `Series`)
  - `pydash` (`decapitalize`, `get`)

## Type Definitions

### DataQueryInfo (dataclass)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `attr` | `str` | (required) | Attribute name on the parent processor that this query feeds |
| `processor` | `BaseProcessor` | (required) | The processor that owns this query |
| `query` | `DataQuery` | (required) | The data query to execute |
| `entity` | `Entity` | (required) | The entity this query is for |
| `data` | `pd.Series` | `None` | The fetched data series, populated after query execution |

### MeasureQueryInfo (dataclass)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `attr` | `str` | (required) | Attribute name (always `'a'` when created in `build_graph`) |
| `processor` | `BaseProcessor` | (required) | The measure processor |
| `entity` | `Entity` | (required) | The entity for the measure query |

### DateOrDatetimeOrRDate
```
DateOrDatetimeOrRDate = Union[DateOrDatetime, RelativeDate]
```
Union of `dt.date`, `dt.datetime`, or `RelativeDate`. Used for start/end parameters on processors.

### DataCoordinateOrProcessor
```
DataCoordinateOrProcessor = Union[DataCoordinate, BaseProcessor]
```
Union type for processor children that can be either a raw data coordinate or a nested processor.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `PARSABLE_OBJECT_MAP` | `dict` | `{'window': Window, 'returns': Returns, 'currency': Currency, 'scaleShape': ScaleShape}` | Maps serialized type strings to their Python classes for deserialization |
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### BaseProcessor.__init__(self, **kwargs) -> None
Purpose: Initializes a processor with a unique ID, default result, and optional keyword arguments.

**Parameters (via kwargs):**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `last_value` | `bool` | `False` | If True, post_process truncates result to last element |
| `measure_processor` | `bool` | `False` | If True, this processor takes an entity argument in process() |

**Instance attributes set:**
| Attribute | Type | Initial Value | Description |
|-----------|------|---------------|-------------|
| `self.id` | `str` | `f'{ClassName}-{uuid4()}'` | Unique processor identifier |
| `self.value` | `ProcessorResult` | `ProcessorResult(False, 'Value not set')` | Current computed value |
| `self.parent` | `Optional[BaseProcessor]` | `None` | Parent processor in the graph |
| `self.parent_attr` | `Optional[str]` | `None` | Attribute name on parent that this processor feeds |
| `self.children` | `Dict[str, Union[DataCoordinateOrProcessor, DataQueryInfo]]` | `{}` | Map of child attribute names to child processors/coordinates |
| `self.children_data` | `Dict[str, ProcessorResult]` | `{}` | Map of child attribute names to their latest results |
| `self.data_cell` | `None` | `None` | Reference to the DataCell this processor belongs to |
| `self.last_value` | `bool` | `False` | Whether to truncate to last value in post_process |
| `self.measure_processor` | `bool` | `False` | Whether this is a measure processor |

### BaseProcessor.process(self, *args) [abstract]
Purpose: Subclasses implement the actual computation logic.

**Return type:** Typically sets `self.value` to a `ProcessorResult`.

### BaseProcessor.post_process(self) -> None
Purpose: Truncates the result to the last element if `last_value` is True and conditions are met.

**Algorithm:**
1. If `self.last_value` is truthy:
   a. If `self.value` is a `ProcessorResult` AND `self.value.success` is True AND `self.value.data` is a `pd.Series` AND `self.value.data` is not empty:
      - Set `self.value.data = self.value.data.iloc[-1:]` (keep only last element)

### BaseProcessor.__handle_date_range(self, result: ProcessorResult, rdate_entity_map: Dict[str, dt.date]) -> None
Purpose: Applies a date/datetime mask on the result using start/end parameters on the processor.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `result` | `ProcessorResult` | (required) | The result to filter |
| `rdate_entity_map` | `Dict[str, dt.date]` | (required) | Map of entity rdate keys to resolved date values |

**Algorithm:**
1. If `result` is not a `ProcessorResult` or `result.success` is False -> return (no-op)
2. Get `start = pydash.get(self, 'start')` and `end = pydash.get(self, 'end')`
3. If neither `start` nor `end` is truthy -> return
4. Get `entity` from `self.data_cell.entity`
5. Branch: if entity is a string -> `entity_id = ''`; else -> `entity_id = entity.get_marquee_id()`
6. Branch: start AND end both present:
   a. If `start` is `RelativeDate` -> resolve via `rdate_entity_map[get_entity_rdate_key_from_rdate(entity_id, start)]`
   b. If `end` is `RelativeDate` -> resolve similarly
   c. Create mask: `result.data.index >= np.datetime64(start) AND result.data.index <= np.datetime64(end)`
7. Branch: start only (no end):
   a. If `start` is `RelativeDate` -> resolve via rdate_entity_map
   b. Create mask: `result.data.index >= np.datetime64(start)`
8. Branch: end only (no start):
   a. If `end` is `RelativeDate` -> resolve via rdate_entity_map
   b. Create mask: `result.data.index <= np.datetime64(end)`
9. Apply mask: `result.data = result.data[mask]`

### BaseProcessor.update(self, attribute: str, result: ProcessorResult, rdate_entity_map: Dict[str, dt.date], pool: ProcessPoolExecutor = None, query_info: Union[DataQueryInfo, MeasureQueryInfo] = None) -> None [async]
Purpose: Handles the update of a single coordinate result and recalculates the processor value.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` | (required) | Attribute name aligning to data coordinate |
| `result` | `ProcessorResult` | (required) | Result from data query |
| `rdate_entity_map` | `Dict[str, dt.date]` | (required) | Resolved relative date map |
| `pool` | `ProcessPoolExecutor` | `None` | Optional process pool for parallel execution |
| `query_info` | `Union[DataQueryInfo, MeasureQueryInfo]` | `None` | Query info, needed for measure processors |

**Algorithm:**
1. If not `self.measure_processor` -> call `self.__handle_date_range(result, rdate_entity_map)`
2. Store `self.children_data[attribute] = result`
3. Branch: if `result` is a `ProcessorResult`:
   a. If `result.success` is True:
      - Try:
        - Branch: if `pool` is not None:
          - Branch: if `self.measure_processor` -> `value = await loop.run_in_executor(pool, partial(self.process, query_info.entity))`
          - Else -> `value = await loop.run_in_executor(pool, self.process)`
          - Set `self.value = value`
        - Else (no pool):
          - Branch: if `self.measure_processor` -> `self.process(query_info.entity)`
          - Else -> `self.process()`
        - Call `self.post_process()`
      - Except `Exception as e` -> `self.value = ProcessorResult(False, f'Error Calculating processor {class_name} due to {e}')`
   b. Else (not success) -> `self.value = result` (propagate failure)

### BaseProcessor.get_plot_expression(self) [abstract]
Purpose: Returns a plot expression used to go from grid to plottool. Must be implemented by subclasses.

### BaseProcessor.__add_required_rdates(self, entity: Entity, rdate_entity_map: Dict[str, Set[Tuple]]) -> None
Purpose: Inspects start/end attributes for RelativeDate instances and adds them to the rdate map for later resolution.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `entity` | `Entity` | (required) | The entity to associate with the relative dates |
| `rdate_entity_map` | `Dict[str, Set[Tuple]]` | (required) | Map of entity_id -> set of (rule, base_date) tuples |

**Algorithm:**
1. Get `start` and `end` from `self` via `pydash.get`
2. Determine `entity_id`: if `entity` is an `Entity` instance -> `entity.get_marquee_id()`; else -> `''`
3. If `start` is `RelativeDate`:
   a. `base_date = str(start.base_date)` if `start.base_date_passed_in` else `None`
   b. Add `(start.rule, base_date)` to `rdate_entity_map[entity_id]`
4. If `end` is `RelativeDate`:
   a. `base_date = str(end.base_date)` if `end.base_date_passed_in` else `None`
   b. Add `(end.rule, base_date)` to `rdate_entity_map[entity_id]`

### BaseProcessor.build_graph(self, entity: Entity, cell: Any, queries: List[Union[DataQueryInfo, MeasureQueryInfo]], rdate_entity_map: Dict[str, Set[Tuple]], overrides: Optional[List]) -> None
Purpose: Generates the nested cell graph and accumulates leaf data queries.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `entity` | `Entity` | (required) | The entity for this graph segment |
| `cell` | (DataCell) | (required) | The data cell this processor belongs to |
| `queries` | `List[Union[DataQueryInfo, MeasureQueryInfo]]` | (required) | Accumulated list of queries (mutated) |
| `rdate_entity_map` | `Dict[str, Set[Tuple]]` | (required) | Accumulated rdate map (mutated) |
| `overrides` | `Optional[List]` | (required) | Optional list of dimension overrides |

**Algorithm:**
1. Set `self.data_cell = cell`
2. Call `self.__add_required_rdates(entity, rdate_entity_map)`
3. If `self.measure_processor` -> append `MeasureQueryInfo(attr='a', processor=self, entity=entity)` to `queries`
4. For each `(attr_name, child)` in `self.children.items()`:
   a. Branch: `child` is `DataCoordinate`:
      - If `overrides` is not None:
        - Filter overrides where `override.coordinate == child`
        - If any match:
          - `use_default = True`
          - For each override: if `override.coordinate_id == child.id` -> `child.set_dimensions(override.dimensions)`, `use_default = False`, break
          - If `use_default` and `overrides[0].coordinate_id is None` -> `child.set_dimensions(overrides[0].dimensions)`
      - Branch: if `child.frequency == DataFrequency.DAILY` -> create `DataQuery(coordinate=child, start=start, end=end)`
      - Else -> create `DataQuery(coordinate=child, query_type=DataQueryType.LAST)`
      - Append `DataQueryInfo(attr=attr_name, processor=self, query=query, entity=entity)` to `queries`
   b. Branch: `child` is `BaseProcessor`:
      - Set `child.parent = self`, `child.parent_attr = attr_name`
      - Recurse: `child.build_graph(entity, cell, queries, rdate_entity_map, overrides)`
   c. Branch: `child` is `DataQueryInfo`:
      - Set `child.parent = self`, `child.parent_attr = attr_name`, `child.processor = self`
      - Append `child` to `queries`

### BaseProcessor.calculate(self, attribute: str, result: ProcessorResult, rdate_entity_map: Dict[str, dt.date], pool: ProcessPoolExecutor = None, query_info: Union[DataQueryInfo, MeasureQueryInfo] = None) -> None [async]
Purpose: Sets the result on the processor and recursively calls parent to propagate and recalculate.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `attribute` | `str` | (required) | Attribute name for the result |
| `result` | `ProcessorResult` | (required) | Result to set |
| `rdate_entity_map` | `Dict[str, dt.date]` | (required) | Resolved relative date map |
| `pool` | `ProcessPoolExecutor` | `None` | Optional process pool |
| `query_info` | `Union[DataQueryInfo, MeasureQueryInfo]` | `None` | Query info for measure processors |

**Algorithm:**
1. `await self.update(attribute, result, rdate_entity_map, pool, query_info)`
2. If `self.parent` is not None:
   a. Get `value = self.value`
   b. Branch: if `value` is `ProcessorResult`:
      - Branch: if `value.success`:
        - Branch: if `self.parent` is `BaseProcessor` -> `await self.parent.calculate(self.parent_attr, value, rdate_entity_map, pool)`
        - Else (parent is DataCell) -> `self.parent.update(value)`
      - Else (not success):
        - Set `self.data_cell.value = value`
        - Set `self.data_cell.updated_time` to UTC timestamp string `'YYYY-MM-DDTHH:MM:SS.mmmZ'`

### BaseProcessor.as_dict(self) -> Dict
Purpose: Creates a dictionary representation of the processor for JSON/API serialization. Supports nested processors.

**Algorithm:**
1. Build base dict: `{TYPE: PROCESSOR, PARAMETERS: self.get_default_params()}`
2. If `self` is `BaseProcessor` -> set `processor[PROCESSOR_NAME] = self.__class__.__name__`
3. For each `(parameter, alias)` from `get_type_hints(self.__init__)`:
   a. Branch: if alias is one of `DataCoordinateOrProcessor`, `DataCoordinate`, `Union[DataCoordinateOrProcessor, None]`, `BaseProcessor`:
      - Get `attribute = self.children[parameter]`
      - If `attribute is None` -> continue (skip)
      - Branch: if attribute is `BaseProcessor` -> set `TYPE: PROCESSOR`, `PROCESSOR_NAME`
      - Branch: if attribute is `DataCoordinate` -> set `TYPE: DATA_COORDINATE`
      - Else -> continue
      - Merge `attribute.as_dict()` into the parameter dict
   b. Else (non-coordinate/processor types):
      - Get `attribute = getattr(self, parameter)`
      - If `attribute is not None`:
        - Branch: if `isinstance(attribute, list)` -> `value = [item.as_dict() for item in attribute]`
        - Branch: if `is_of_builtin_type(attribute)` -> `value = attribute`
        - Branch: if `isinstance(attribute, Enum)` -> `value = attribute.value`
        - Branch: if `isinstance(attribute, Entity)` -> store `{TYPE: ENTITY, ENTITY_ID, ENTITY_TYPE}`, continue
        - Branch: if `isinstance(attribute, (dt.date, dt.datetime))`:
          - Sub-branch: if `dt.date` (not datetime) -> `value = str(attribute)`
          - Sub-branch: if `dt.datetime` -> `value = formatted string 'YYYY-MM-DDTHH:MM:SS.mmmZ'`
        - Else -> `value = attribute.as_dict()`
        - Store `{TYPE: decapitalize(type(attribute).__name__), VALUE: value}`
4. Return `processor` dict

### BaseProcessor.get_default_params(self) -> Dict
Purpose: Builds the default parameters dict for kwargs-based parameters shared by all processors.

**Algorithm:**
1. Create empty `default_params` dict
2. If `self.last_value` is truthy -> `default_params['last_value'] = dict(type='bool', value=True)`
3. Return `default_params`

### BaseProcessor.from_dict(cls, obj: Dict, reference_list: List) -> Optional[BaseProcessor] [classmethod]
Purpose: Deserializes a processor from its dictionary representation. Recursively handles nested processors, coordinates, entities, dates, and mapped object types.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `obj` | `Dict` | (required) | The serialized processor dictionary |
| `reference_list` | `List` | (required) | Accumulated list of entity references for later resolution (mutated) |

**Return type:** `Optional[BaseProcessor]` (returns `None` if processor class not found)

**Algorithm:**
1. Get `processor_name` from `obj.get(PROCESSOR_NAME, 'None')`
2. Dynamic import: `getattr(__import__('gs_quant.analytics.processors', fromlist=['']), processor_name, None)`
3. Get `parameters = obj.get(PARAMETERS, {})`
4. Create empty `local_reference_list` and `arguments` dict
5. For each `(parameter, parameters_dict)` in `parameters.items()`:
   a. Get `parameter_type = parameters_dict.get(TYPE)`
   b. Branch: `parameter_type == DATA_COORDINATE` -> `arguments[parameter] = DataCoordinate.from_dict(parameters_dict)`
   c. Branch: `parameter_type == PROCESSOR` -> `arguments[parameter] = BaseProcessor.from_dict(parameters_dict, reference_list)` (recursive)
   d. Branch: `parameter_type == ENTITY` -> add to `local_reference_list` with `{TYPE: PROCESSOR, ENTITY_ID, ENTITY_TYPE, PARAMETER}`, set `arguments[parameter] = None`
   e. Branch: `parameter_type in (DATE, DATETIME, RELATIVE_DATE)`:
      - Sub-branch: `DATE` -> parse `'%Y-%m-%d'` to `dt.date`
      - Sub-branch: `RELATIVE_DATE` -> extract `rule` and optional `baseDate`, create `RelativeDate`
      - Sub-branch: `DATETIME` -> parse `'%Y-%m-%dT%H:%M:%S.%f'` (strip trailing `Z`) to `dt.datetime`
   f. Branch: `parameter_type in PARSABLE_OBJECT_MAP`:
      - Get class from map
      - Sub-branch: if class is `Enum`/`EnumMeta` -> construct from value
      - Else -> call `.from_dict()` on the value
   g. Branch: `parameter_type == LIST` -> list comprehension: `BaseProcessor.from_dict(item, reference_list)` for dicts, else raw item
   h. Else (default) -> `arguments[parameter] = parameters_dict.get(VALUE)` (raw built-in value)
6. Instantiate: `processor = processor(**arguments)` if processor class found, else `None`
7. Link references: for each in `local_reference_list`, set `reference[REFERENCE] = processor`
8. Extend `reference_list` with `local_reference_list`
9. Return `processor`

## State Mutation
- `self.id`: Set in `__init__`, never changed
- `self.value`: Set in `__init__`, updated by `update()`, `post_process()`, and `calculate()`
- `self.parent` / `self.parent_attr`: Set by parent's `build_graph()`
- `self.children`: Expected to be set by subclass `__init__`
- `self.children_data`: Updated by `update()` on each attribute result
- `self.data_cell`: Set by `build_graph()`; also accessed in `calculate()` to set error values
- `self.last_value` / `self.measure_processor`: Set in `__init__` from kwargs
- `result.data`: Mutated in-place by `__handle_date_range()` (index-filtered)
- `self.data_cell.value`: Set by `calculate()` on failure
- `self.data_cell.updated_time`: Set by `calculate()` on failure
- `queries` list: Mutated (appended to) by `build_graph()`
- `rdate_entity_map`: Mutated by `__add_required_rdates()`
- `reference_list`: Mutated (extended) by `from_dict()`
- Thread safety: `update()` and `calculate()` are async and may run in a `ProcessPoolExecutor`. The `process()` call is offloaded to the pool but state mutations (`self.value`, `self.children_data`) happen in the async context.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` (generic) | `update` (caught) | Any exception during `self.process()` or executor call; caught and stored as `ProcessorResult(False, ...)` |
| `KeyError` | `as_dict` | If `parameter` not found in `self.children` (for coordinate/processor types) |
| `AttributeError` | `from_dict` | If dynamically imported processor class is `None` and `processor(**arguments)` is called |
| `ImportError` | `from_dict` | If `gs_quant.analytics.processors` module cannot be imported |

## Edge Cases
- Entity is a string (not resolved) -> `entity_id = ''` in date range handling
- `pool` is `None` -> synchronous execution (no executor)
- Unknown processor name in `from_dict` -> `processor = None` -> returns `None`
- `PARSABLE_OBJECT_MAP` only handles: `window` (Window), `returns` (Returns), `currency` (Currency), `scaleShape` (ScaleShape)
- Override with `coordinate_id`: first matching `coordinate_id` wins; if none match and `overrides[0].coordinate_id is None`, uses first override's dimensions
- `as_dict` checks `isinstance(self, BaseProcessor)` which is always True (since method is on BaseProcessor), so `PROCESSOR_NAME` is always set
- `from_dict` with `RELATIVE_DATE`: `baseDate` can be `None`, creating a RelativeDate with `base_date=None`
- `post_process` only truncates if all conditions met (last_value, ProcessorResult, success, Series, non-empty)

## Bugs Found
- Bug 1 (line 143): FIXED -- end-only date range used `>=` instead of `<=`. Now correctly uses `<=`.

## Coverage Notes
- Branch count: ~112
- Requires async test infrastructure (`asyncio.get_running_loop`)
- `ProcessPoolExecutor` path needs special handling in tests
- `from_dict` has many branches for each parameter type
- `as_dict` has many branches for each attribute type
- `build_graph` override logic has nested branches
- Pragmas: none marked
