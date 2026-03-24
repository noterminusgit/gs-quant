# special_processors.py

## Summary
Special-purpose processors that retrieve data from entities and coordinates rather than computing on time series. `EntityProcessor` fetches a field value from an entity object (including nested fields and identifiers). `CoordinateProcessor` extracts a dimension value from a `DataCoordinate`. `MeasureProcessor` is a base class for measure-based processors that delegates its `process()` to subclasses.

## Dependencies
- Internal: `gs_quant.analytics.core.processor` (BaseProcessor)
- Internal: `gs_quant.analytics.core.processor_result` (ProcessorResult)
- Internal: `gs_quant.data` (DataDimension, DataCoordinate)
- Internal: `gs_quant.entities.entity` (Entity)
- External: `enum` (Enum)
- External: `typing` (Union)
- External: `pydash` (get)

## Type Definitions

### EntityProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| field | `str` | (required) | The entity property to retrieve. Supports nested fields via dot notation (e.g., `'xref.bbid'`) |

Inherited from BaseProcessor:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id | `str` | auto-generated | `"{ClassName}-{uuid4()}"` |
| value | `ProcessorResult` | `ProcessorResult(False, 'Value not set')` | Current computed result |
| parent | `Optional[BaseProcessor]` | `None` | Parent processor reference |
| parent_attr | `Optional[str]` | `None` | Attribute name in parent |
| children | `Dict[str, ...]` | `{}` | Child processors/coordinates (not used by EntityProcessor) |
| children_data | `Dict[str, ProcessorResult]` | `{}` | Resolved child data (not used by EntityProcessor) |
| data_cell | `Any` | `None` | Data cell reference |
| last_value | `bool` | `False` | Whether to keep only last value |
| measure_processor | `bool` | `False` | Whether this is a measure processor |

### CoordinateProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinate` | (required) | The coordinate to extract a dimension from |
| dimension | `Union[DataDimension, str]` | (required) | The dimension name or enum to look up in the coordinate |

### MeasureProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none added) | | | No additional fields; `measure_processor=True` is passed to BaseProcessor via kwargs |

Note: `MeasureProcessor.__init__` passes `measure_processor=True` to `super().__init__(**kwargs)`, so `self.measure_processor` is always `True`.

## Enums and Constants
None defined in this module. Uses `Enum` from stdlib for isinstance checking on `dimension` in `CoordinateProcessor`.

## Functions/Methods

### EntityProcessor.__init__(self, field: str) -> None
Purpose: Store the field path to retrieve from entity objects.

**Algorithm:**
1. Call `super().__init__()`
2. Set `self.field = field`

### EntityProcessor.process(self, entity: Entity) -> ProcessorResult
Purpose: Fetch a field value from the given entity object.

**Algorithm:**
1. Branch: `isinstance(entity, str)` -> return `ProcessorResult(False, f"Unable to resolve Entity {entity}")` (entity fetch failed, got string error instead of Entity object)
2. Try block:
   a. Call `entity_dict = entity.get_entity()` to get the entity as a dict
   b. Call `data = get(entity_dict, self.field)` using pydash deep get with dot notation
   c. Branch: `data` is truthy -> return `ProcessorResult(True, data)`
   d. Search identifiers: `identifier = next(iter(filter(lambda x: x['type'] == self.field, entity_dict.get('identifiers', []))), None)`
   e. Branch: `identifier` is truthy -> return `ProcessorResult(True, identifier['value'])`
   f. Return `ProcessorResult(False, f'Unable to find {self.field} in identifiers for entity {entity.get_marquee_id()}')`
3. Except `ValueError` -> return `ProcessorResult(False, "Could not get field on entity")`

**Raises:** No exceptions raised; all errors returned as `ProcessorResult(False, ...)`.

### EntityProcessor.update(self, attribute: str, result: ProcessorResult) -> None
Purpose: No-op. Entity processors do not use the standard child-data update mechanism.

**Algorithm:**
1. `pass` (does nothing)

Note: This overrides the async `BaseProcessor.update()` with a synchronous no-op signature `(self, attribute: str, result: ProcessorResult) -> None`. The base class `update` is async and has a different signature.

### EntityProcessor.get_plot_expression(self) -> None
Purpose: No-op. Not applicable for entity processors.

### CoordinateProcessor.__init__(self, a: DataCoordinate, dimension: Union[DataDimension, str]) -> None
Purpose: Store the coordinate and dimension to extract.

**Algorithm:**
1. Call `super().__init__()`
2. Set `self.children['a'] = a` (the coordinate)
3. Set `self.dimension = dimension`

### CoordinateProcessor.process(self) -> ProcessorResult
Purpose: Extract a dimension value from the stored coordinate.

**Algorithm:**
1. Determine key: Branch `isinstance(self.dimension, Enum)` -> `key = self.dimension.value`; else `key = self.dimension`
2. Get `coordinate = self.children.get('a')` (note: reads from `self.children`, NOT `self.children_data`)
3. Branch: `coordinate` is truthy -> `dimension_value = coordinate.dimensions.get(key)`; else `dimension_value = None`
4. Branch: `dimension_value` is truthy -> return `ProcessorResult(True, dimension_value)`
5. Else -> return `ProcessorResult(False, f'Dimension {key} not in given coordinate')`

### CoordinateProcessor.update(self, attribute: str, result: ProcessorResult) -> None
Purpose: No-op override.

**Algorithm:**
1. `pass`

### CoordinateProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### MeasureProcessor.__init__(self, **kwargs) -> None
Purpose: Initialize a measure processor with `measure_processor=True`.

**Algorithm:**
1. Call `super().__init__(**kwargs, measure_processor=True)`

### MeasureProcessor.process(self, *args) -> None
Purpose: No-op base implementation. Subclasses should override.

**Algorithm:**
1. `pass`

### MeasureProcessor.get_plot_expression(self) -> None
Purpose: No-op.

## State Mutation
- `EntityProcessor`: Does NOT mutate `self.value`. Returns `ProcessorResult` directly from `process()` without storing it. This differs from other processors that set `self.value` before returning.
- `CoordinateProcessor`: Does NOT mutate `self.value`. Returns `ProcessorResult` directly from `process()`.
- `CoordinateProcessor.children['a']`: Set during `__init__`. Read directly in `process()` (NOT via `children_data`). This means the coordinate is accessed directly from `children` rather than through the usual `children_data` update mechanism.
- `MeasureProcessor`: No state mutation.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised) | | All failure paths return `ProcessorResult(False, message)` |

Internal exception handling:
| Exception | Caught By | Condition | Result |
|-----------|-----------|-----------|--------|
| `ValueError` | `EntityProcessor.process` | When `entity.get_entity()` or `get()` raises ValueError | Returns `ProcessorResult(False, "Could not get field on entity")` |

## Edge Cases
- `EntityProcessor.process`: When `entity` is a `str` (failed API fetch returning error string), immediately returns failure. The error message includes the string entity value.
- `EntityProcessor.process`: `data = get(entity_dict, self.field)` returns `None` for missing fields. `None` is falsy, so it falls through to identifier search.
- `EntityProcessor.process`: If field value is `0`, `""`, `[]`, or other falsy values, `if data:` will be `False` and the code falls through to identifier search even though the field was found.
- `CoordinateProcessor.process`: Reads `self.children.get('a')` directly, not `self.children_data.get('a')`. This means it does not use the resolved child data mechanism. The coordinate must be the original `DataCoordinate` object stored in `children`.
- `CoordinateProcessor.process`: If `dimension_value` is `0`, `""`, or other falsy value, returns failure even though the dimension was found.
- `EntityProcessor.update` and `CoordinateProcessor.update`: Override the base class async update with a synchronous no-op. This prevents child data updates from being processed.

## Bugs Found
- `EntityProcessor.process` line 46: `if data:` uses truthiness check, so falsy values like `0`, `""`, `False`, `[]` are treated as missing even when the field exists. Should use `if data is not None:`. (OPEN)
- `CoordinateProcessor.process` line 88: `if dimension_value:` has the same falsy-value problem. A dimension value of `0` or `""` would be treated as missing. (OPEN)
- `EntityProcessor` does not set `self.value` -- it returns ProcessorResult directly without storing it. Other processors set `self.value` before returning. This inconsistency means `post_process()` and parent update logic that reads `self.value` will still see the initial `ProcessorResult(False, 'Value not set')`. (OPEN)
- `CoordinateProcessor` has the same `self.value` storage issue as `EntityProcessor`. (OPEN)

## Coverage Notes
- Branch count: ~12
- Key branches: EntityProcessor entity-is-string check (1), try/except ValueError (1), data truthy (1), identifier found (1), CoordinateProcessor dimension-is-Enum (1), coordinate truthy (1), dimension_value truthy (1), EntityProcessor isinstance checks (1)
- Pragmas: none
