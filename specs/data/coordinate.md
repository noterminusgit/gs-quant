# coordinate.py

## Summary
Defines data coordinate classes that uniquely identify a single timeseries on the GS Data Platform. A coordinate combines a dataset identifier, a measure (numerical field), dimension values (keys/filters), and a frequency. Coordinates are used throughout the system to reference, query, and compare timeseries data.

## Dependencies
- Internal: `gs_quant.data.core` (DataContext, DataFrequency, DataAggregationOperator)
- Internal: `gs_quant.data.dataset` (Dataset)
- Internal: `gs_quant.data.fields` (DataMeasure, DataDimension)
- External: `datetime` (date, datetime)
- External: `json` (dumps)
- External: `uuid` (uuid4)
- External: `abc` (ABCMeta)
- External: `enum` (Enum)
- External: `typing` (Union, Dict, Tuple, Optional, List)
- External: `pandas` (Series)

## Type Definitions

### Type Aliases
```
DataDimensions = Dict[Union[DataDimension, str], Union[str, float]]
DateOrDatetime = Union[dt.date, dt.datetime]
```

### BaseDataCoordinate (abstract class, metaclass=ABCMeta)
Inherits: none (uses ABCMeta metaclass)

Uses `__slots__` for memory efficiency.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__measure` | `Union[DataMeasure, str]` | required | The data measure (numerical field) this coordinate references |
| `__dimensions` | `tuple` | `tuple()` | Sorted tuple of (key, value) pairs representing dimension filters; keys are normalized to string values if they are Enum members |

### DataCoordinate (class)
Inherits: `BaseDataCoordinate`

Uses `__slots__` for memory efficiency.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__dataset_id` | `Optional[str]` | `None` | Unique identifier for the dataset on the GS Data Platform |
| `__frequency` | `Optional[DataFrequency]` | `None` | Data frequency (DAILY, REAL_TIME, ANY) |
| `__id` | `str` | `str(uuid.uuid4())` | Auto-generated UUID string; uniquely identifies this coordinate instance |

## Enums and Constants

No enums or constants are defined in this module. The module uses `DataMeasure`, `DataDimension`, `DataFrequency`, and `DataAggregationOperator` from other modules.

## Functions/Methods

### BaseDataCoordinate.__init__(self, measure: Union[DataMeasure, str], dimensions: Optional[DataDimensions] = None)
Purpose: Initialize base coordinate with a measure and optional dimensions, normalizing dimension keys from Enum to string values and sorting for order-independent equality.

**Algorithm:**
1. Store `measure` as `self.__measure`
2. Branch: `dimensions is None` -> set `self.__dimensions = tuple()`
3. Branch: `dimensions is not None` -> iterate all key-value pairs; for each key, if `isinstance(key, Enum)` use `key.value`, else use `key` as-is; collect into a dict (deduplicating Enum vs string keys); sort by key; convert to tuple of pairs and store as `self.__dimensions`

### BaseDataCoordinate.measure (property) -> DataMeasure
Purpose: Return the measure field.

**Algorithm:**
1. Return `self.__measure`

### BaseDataCoordinate.dimensions (property) -> Dict
Purpose: Return dimensions as a mutable dict (copy of the internal tuple).

**Algorithm:**
1. Convert `self.__dimensions` tuple back to dict via `dict()` and return

### BaseDataCoordinate.get_series(self, start: Optional[DateOrDatetime] = None, end: Optional[DateOrDatetime] = None)
Purpose: Base method stub for getting series data. Returns None (no-op in base class).

**Algorithm:**
1. `pass` (returns `None` implicitly)

### BaseDataCoordinate.set_dimensions(self, dimensions: DataDimensions)
Purpose: Add or overwrite dimensions after initialization. Used when dimension values (e.g. asset_id) are not known at construction time (e.g. in datagrids).

**Algorithm:**
1. Get a mutable copy of current dimensions via `self.dimensions` (property returns a dict)
2. For each `(key, v)` in `dimensions.items()`:
   - Branch: `isinstance(key, Enum)` -> use `key.value` as the dict key
   - Branch: else -> use `key` as-is
3. Re-sort the combined dict items and store as a tuple in `self.__dimensions`

Note: Accesses the name-mangled `__dimensions` attribute of `BaseDataCoordinate` via `_BaseDataCoordinate__dimensions`.

### DataCoordinate.__init__(self, measure: Union[DataMeasure, str], dataset_id: Optional[str] = None, dimensions: Optional[DataDimensions] = None, frequency: Optional[DataFrequency] = None)
Purpose: Initialize a fully-specified data coordinate with dataset ID, measure, dimensions, and frequency.

**Algorithm:**
1. Call `super().__init__(measure, dimensions)` to initialize base class
2. Store `dataset_id` as `self.__dataset_id`
3. Store `frequency` as `self.__frequency`
4. Generate a new UUID string via `str(uuid.uuid4())` and store as `self.__id`

### DataCoordinate.dataset_id (property) -> str
Purpose: Return the dataset identifier.

**Algorithm:**
1. Return `self.__dataset_id`

### DataCoordinate.frequency (property) -> DataFrequency
Purpose: Return the data frequency.

**Algorithm:**
1. Return `self.__frequency`

### DataCoordinate.id (property) -> str
Purpose: Return the unique coordinate instance identifier.

**Algorithm:**
1. Return `self.__id`

### DataCoordinate.id (setter)
Purpose: Allow overwriting the coordinate's ID (used by `from_dict` to restore a serialized ID).

**Algorithm:**
1. Set `self.__id = value`

### DataCoordinate.__eq__(self, other) -> bool
Purpose: Determine equality of two coordinates based on dataset_id, measure, and dimensions. Normalizes measure values from Enum to string for comparison.

**Algorithm:**
1. Branch: `isinstance(self.measure, str)` -> `measure = self.measure`
2. Branch: else -> `measure = self.measure.value`
3. Branch: `isinstance(other.measure, str)` -> `other_measure = other.measure`
4. Branch: else -> `other_measure = other.measure.value`
5. Return `(self.dataset_id, measure, self.dimensions) == (other.dataset_id, other_measure, other.dimensions)`

Note: Does not check `isinstance(other, DataCoordinate)` -- will raise `AttributeError` if `other` lacks `.measure`, `.dataset_id`, or `.dimensions`.

### DataCoordinate.get_dimensions(self) -> Tuple
Purpose: Return dimensions as a tuple of (key, value) pairs.

**Algorithm:**
1. Return `tuple(self.dimensions.items())`

### DataCoordinate.__hash__(self) -> int
Purpose: Make DataCoordinate hashable (for use in sets/dicts). Hash is based on dataset_id, measure, and dimensions tuple.

**Algorithm:**
1. Return `hash((self.dataset_id, self.measure, tuple(self.dimensions)))`

Note: `self.dimensions` returns a dict, so `tuple(self.dimensions)` yields only the keys, not key-value pairs. This means two coordinates with same keys but different values could collide (though `__eq__` would still distinguish them).

### DataCoordinate.__str__(self) -> str
Purpose: Return a human-readable string representation of the coordinate.

**Algorithm:**
1. Branch: `isinstance(self.measure, str)` -> use `self.measure` directly
2. Branch: else -> use `self.measure.value`
3. Format string: `"Dataset Id: ({dataset_id}) Measure: ({measure}) Dimensions: ({json.dumps(dimensions)})"`

### DataCoordinate.get_range(self, start: Optional[DateOrDatetime] = None, end: Optional[DateOrDatetime] = None) -> Tuple[Optional[DateOrDatetime], Optional[DateOrDatetime]]
Purpose: Resolve start/end range, falling back to the current DataContext if not provided. Uses time-based defaults for REAL_TIME frequency and date-based defaults for others.

**Algorithm:**
1. Branch: `start is None`:
   - Branch: `self.frequency is DataFrequency.REAL_TIME` -> `start = DataContext.current.start_time`
   - Branch: else -> `start = DataContext.current.start_date`
2. Branch: `end is None`:
   - Branch: `self.frequency is DataFrequency.REAL_TIME` -> `end = DataContext.current.end_time`
   - Branch: else -> `end = DataContext.current.end_date`
3. Return `(start, end)`

### DataCoordinate.get_series(self, start: Optional[DateOrDatetime] = None, end: Optional[DateOrDatetime] = None, dates: List[dt.date] = None, operator: DataAggregationOperator = None) -> Union[pd.Series, None]
Purpose: Query the dataset and return a timeseries for this coordinate's measure and dimensions.

**Algorithm:**
1. Branch: `not self.dataset_id` -> return `None`
2. Create `Dataset(self.dataset_id)`
3. Call `self.get_range(start, end)` to resolve start/end
4. Set `measure = self.measure`
5. Branch: `operator` is truthy -> `measure = f'{operator}({measure})'`
6. Return `dataset.get_data_series(measure, start=start, end=end, dates=dates, **self.dimensions)`

### DataCoordinate.last_value(self, before: Optional[DateOrDatetime] = None) -> Union[float, None]
Purpose: Return the last available data value before a given date/time, or before the current DataContext end if not specified.

**Algorithm:**
1. Branch: `not self.dataset_id` -> return `None`
2. Call `self.get_range(None, before)` to get `(start, end)`
3. Create `Dataset(self.dataset_id)`
4. Branch: `isinstance(self.measure, Enum)` -> `measure = self.measure.value`
5. Branch: else -> `measure = self.measure`
6. Call `dataset.get_data_last(end, fields=[measure], **self.dimensions)` and call `.get(measure, default=None)` on the result
7. Return the result

### DataCoordinate.as_dict(self) -> dict
Purpose: Serialize the coordinate to a plain dictionary for JSON serialization.

**Algorithm:**
1. Initialize empty `dimensions` dict
2. For each `(key, value)` in `self.dimensions.items()`:
   - Branch: `isinstance(key, Enum)` -> `dimensions[key.value] = value`
   - Branch: else -> `dimensions[key] = value`
3. Build `coordinate` dict with keys:
   - `'measure'`: Branch: `isinstance(self.measure, Enum)` -> `self.measure.value`, else `self.measure`
   - `'frequency'`: `self.frequency.value`
   - `'id'`: `self.id`
4. Branch: `self.dataset_id` is truthy -> add `'datasetId': self.dataset_id`
5. Branch: `dimensions` is truthy (non-empty) -> add `'dimensions': dimensions`
6. Return `coordinate`

### DataCoordinate.from_dict(cls, obj) -> DataCoordinate (classmethod)
Purpose: Deserialize a coordinate from a plain dictionary.

**Algorithm:**
1. Extract `measure = obj.get('measure')`
2. Extract `dimensions = obj.get('dimensions', {})`
3. Extract `frequency = obj.get('frequency')`
4. Extract `dataset_id = obj.get('datasetId')`
5. Extract `id_ = obj.get('id')`
6. Branch: `measure in DataMeasure._value2member_map_` -> `measure = DataMeasure(measure)` (convert to enum)
7. Branch: else -> keep as string
8. Initialize `parsed_dimensions = {}`; get `data_dimension_map = DataDimension._value2member_map_`
9. For each `(key, value)` in `dimensions.items()`:
   - Branch: `key in data_dimension_map` -> `parsed_dimensions[DataDimension(key)] = value`
   - Branch: else -> `parsed_dimensions[key] = value`
10. Branch: `dataset_id` is truthy -> construct `DataCoordinate(dataset_id=dataset_id, measure=measure, dimensions=parsed_dimensions, frequency=DataFrequency(frequency))`
11. Branch: else -> construct `DataCoordinate(measure=measure, dimensions=parsed_dimensions, frequency=DataFrequency(frequency))`
12. Branch: `id_` is truthy -> `coordinate.id = id_`
13. Return `coordinate`

Note: Both branches in step 10/11 construct the same object (with `dataset_id=None` when falsy), but the code explicitly omits the kwarg.

## State Mutation
- `self.__measure` (`BaseDataCoordinate`): Set during `__init__`, never mutated afterward
- `self.__dimensions` (`BaseDataCoordinate`): Set during `__init__`, can be mutated by `set_dimensions()`
- `self.__dataset_id` (`DataCoordinate`): Set during `__init__`, never mutated afterward
- `self.__frequency` (`DataCoordinate`): Set during `__init__`, never mutated afterward
- `self.__id` (`DataCoordinate`): Set during `__init__` with auto-generated UUID, can be overwritten via `id` setter (used by `from_dict`)
- Thread safety: No locking. `set_dimensions` mutates internal state and is not thread-safe. The `DataContext.current` access in `get_range` depends on thread-local or context-based state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `AttributeError` | `__eq__` | When `other` does not have `measure`, `dataset_id`, or `dimensions` attributes |
| `ValueError` | `DataFrequency(frequency)` in `from_dict` | When `frequency` string is not a valid `DataFrequency` value |
| (various) | `Dataset.get_data_series`, `Dataset.get_data_last` | Network/API errors propagated from dataset queries in `get_series` and `last_value` |

## Edge Cases
- Dimensions with Enum keys vs string keys that have the same `.value` are normalized to the same string key in `__init__`, effectively deduplicating them (last one wins due to dict construction)
- `dimensions=None` produces an empty tuple, while `dimensions={}` also produces an empty tuple -- both are equivalent
- `__hash__` uses `tuple(self.dimensions)` which only includes keys (not values), so coordinates with identical keys but different values will hash the same but compare as not-equal via `__eq__`
- `__eq__` does not check type of `other`; comparing a `DataCoordinate` with a non-coordinate object will raise `AttributeError` rather than returning `False`
- `get_series` and `last_value` return `None` early if `dataset_id` is falsy (None or empty string)
- `as_dict` always accesses `self.frequency.value` without a None check -- will raise `AttributeError` if `frequency` is `None`
- `from_dict` always calls `DataFrequency(frequency)` -- will raise `ValueError` if frequency is `None` or invalid
- `from_dict` with `dataset_id` truthy vs falsy creates identical `DataCoordinate` objects (the falsy branch just omits the keyword argument, defaulting to `None`)
- `set_dimensions` on `BaseDataCoordinate` accesses the name-mangled `_BaseDataCoordinate__dimensions` attribute through `self.__dimensions`, which works correctly within the class but subclasses cannot directly access this attribute

## Coverage Notes
- Branch count: 30
- Key branches: `dimensions is None` in `__init__`, `isinstance(key, Enum)` in dimension normalization (appears in `__init__`, `set_dimensions`, `as_dict`), `isinstance(self.measure, str)` in `__eq__`/`__str__`, `self.frequency is DataFrequency.REAL_TIME` (x2 in `get_range`), `not self.dataset_id` (in `get_series`, `last_value`), `operator` truthiness in `get_series`, `isinstance(self.measure, Enum)` in `last_value`/`as_dict`, `self.dataset_id` truthiness in `as_dict`, `dimensions` truthiness in `as_dict`, `measure in DataMeasure._value2member_map_` in `from_dict`, `key in data_dimension_map` in `from_dict`, `dataset_id` truthiness in `from_dict`, `id_` truthiness in `from_dict`
- `get_series` on `BaseDataCoordinate` is a no-op stub (`pass`) -- no meaningful branch
