# data_sources.py

## Summary
Data source classes for the backtest framework. `DataSource` is the abstract base with a subclass registry pattern. `GsDataSource` loads data lazily from the GS Dataset API. `GenericDataSource` holds an in-memory pandas Series with configurable missing-data strategies. `DataManager` routes data requests to registered sources by (frequency, instrument_name, valuation_type) key.

## Dependencies
- Internal: `gs_quant.backtests.core` (ValuationFixingType)
- Internal: `gs_quant.base` (field_metadata, static_field)
- Internal: `gs_quant.data` (DataFrequency, Dataset)
- Internal: `gs_quant.instrument` (Instrument)
- Internal: `gs_quant.json_convertors` (decode_pandas_series, encode_pandas_series)
- External: `dataclasses` (dataclass, field)
- External: `dataclasses_json` (dataclass_json, config)
- External: `datetime` (dt)
- External: `enum` (Enum)
- External: `numpy` (np)
- External: `pandas` (pd)
- External: `typing` (Union, Iterable, ClassVar, List)

## Type Definitions

### DataSource (dataclass)
Inherits: none (base class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __sub_classes | `ClassVar[List[type]]` | `[]` | Registry of all DataSource subclasses |

### GsDataSource (dataclass)
Inherits: `DataSource`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_set | `str` | required | GS Dataset identifier |
| asset_id | `str` | required | Asset identifier for queries |
| min_date | `dt.date` | `None` | Minimum date for data loading |
| max_date | `dt.date` | `None` | Maximum date for data loading |
| value_header | `str` | `'rate'` | Column name to extract values from |
| class_type | `str` | `'gs_data_source'` | Serialization discriminator |
| loaded_data | `Optional[pd.DataFrame]` | `None` | Lazily loaded cached data (set in __post_init__) |

### GenericDataSource (dataclass)
Inherits: `DataSource`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_set | `pd.Series` | `None` | Pandas series indexed by date or datetime |
| missing_data_strategy | `MissingDataStrategy` | `MissingDataStrategy.fail` | Strategy for handling missing data points |
| class_type | `str` | `'generic_data_source'` | Serialization discriminator |
| _tz_aware | `bool` | computed | Whether the data_set index is tz-aware (set in __post_init__) |

### DataManager (dataclass)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _data_sources | `Dict[Tuple, DataSource]` | `{}` | Registry of data sources keyed by (freq, name, valuation_type) |

## Enums and Constants

### MissingDataStrategy(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| fill_forward | `"fill_forward"` | Forward-fill missing values |
| interpolate | `"interpolate"` | Interpolate missing values |
| fail | `"fail"` | Raise KeyError on missing data (default) |

## Functions/Methods

### DataSource.__init_subclass__(cls, **kwargs) -> None
Purpose: Register each DataSource subclass in `__sub_classes`.

**Algorithm:**
1. Call `super().__init_subclass__(**kwargs)`
2. Append `cls` to `DataSource.__sub_classes`

### DataSource.sub_classes() -> Tuple[type, ...]
Purpose: Return tuple of all registered DataSource subclasses.

### DataSource.get_data(self, state, **kwargs) -> NoReturn
Purpose: Abstract method; raises `RuntimeError`.

### DataSource.get_data_range(self, start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime, int], **kwargs) -> NoReturn
Purpose: Abstract method; raises `RuntimeError`.

### GsDataSource.__post_init__(self) -> None
Purpose: Initialize `loaded_data` cache to `None`.

### GsDataSource.get_data(self, state: Union[dt.date, dt.datetime] = None, **kwargs) -> Any
Purpose: Lazily load data from GS Dataset API and return value at `state`.

**Algorithm:**
1. Branch: `self.loaded_data is None`:
   a. Create `ds = Dataset(self.data_set)`
   b. Branch: `self.min_date` is truthy -> load range `[min_date, max_date]`, store in `self.loaded_data`
   c. Branch: `state is not None` -> return `ds.get_data(state, state, ...)[value_header]` directly (no caching)
   d. Branch: else -> return `ds.get_data(dt.datetime(2000, 1, 1), **kwargs)[value_header]` (no caching)
2. Return `self.loaded_data[value_header].at[pd.to_datetime(state)]`

### GsDataSource.get_data_range(self, start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime, int], **kwargs) -> pd.DataFrame
Purpose: Lazily load data and return a filtered range.

**Algorithm:**
1. Branch: `self.loaded_data is None`:
   a. Create `ds = Dataset(self.data_set)`
   b. Branch: `self.asset_id is not None` -> add to kwargs
   c. Branch: `self.min_date` is truthy -> load `[min_date, max_date]`
   d. Branch: else -> load `[start, max_date]`
2. Branch: `isinstance(end, int)` -> return `loaded_data` rows before `start`, tail `end`
3. Branch: else -> return rows where `start < index <= end`

### GenericDataSource.__eq__(self, other) -> bool
Purpose: Compare by `missing_data_strategy` and `data_set.equals()`.

**Algorithm:**
1. Branch: `not isinstance(other, GenericDataSource)` -> return `False`
2. Return `self.missing_data_strategy == other.missing_data_strategy and self.data_set.equals(other.data_set)`

### GenericDataSource.__post_init__(self) -> None
Purpose: Detect whether the data_set index is tz-aware.

**Algorithm:**
1. Set `self._tz_aware = isinstance(self.data_set.index[0], dt.datetime) and self.data_set.index[0].tzinfo is not None`

### GenericDataSource.get_data(self, state: Union[dt.date, dt.datetime, Iterable]) -> Any
Purpose: Get the value at a given state, handling missing data according to strategy.

**Algorithm:**
1. Branch: `state is None` -> return entire `self.data_set`
2. Branch: `isinstance(state, Iterable)` -> return `[self.get_data(i) for i in state]` (recursive)
3. Branch: `self._tz_aware and state is tz-naive` -> replace state tzinfo with UTC
4. Branch: `pd.Timestamp(state) in self.data_set` -> return `self.data_set[pd.Timestamp(state)]`
5. Branch: `state in self.data_set or self.missing_data_strategy == fail` -> return `self.data_set[state]` (may raise KeyError)
6. Branch: else (missing data, non-fail strategy):
   a. Branch: `isinstance(self.data_set.index, pd.DatetimeIndex)` -> insert NaN at `pd.to_datetime(state)`, sort
   b. Branch: else -> insert NaN at `state`, sort
   c. Branch: `strategy == interpolate` -> `self.data_set = self.data_set.interpolate()`
   d. Branch: `strategy == fill_forward` -> `self.data_set = self.data_set.ffill()`
   e. Branch: else -> raise `RuntimeError` (unrecognised strategy)
   f. Branch: DatetimeIndex -> return `self.data_set[pd.to_datetime(state)]`; else -> return `self.data_set[state]`

### GenericDataSource.get_data_range(self, start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime, int]) -> pd.Series
Purpose: Get a range of values from the dataset.

**Algorithm:**
1. Branch: `isinstance(end, int)` -> return `data_set` rows before `start`, tail `end`
2. Branch: else -> return rows where `start < index <= end`

### DataManager.__post_init__(self) -> None
Purpose: Initialize the `_data_sources` dictionary.

### DataManager.add_data_source(self, series: Union[pd.Series, DataSource], data_freq: DataFrequency, instrument: Instrument, valuation_type: ValuationFixingType) -> None
Purpose: Register a data source for a given instrument and valuation type.

**Algorithm:**
1. Branch: `not isinstance(series, DataSource) and not len(series)` -> return (skip empty series)
2. Branch: `instrument.name is None` -> raise `RuntimeError`
3. Build `key = (data_freq, instrument.name, valuation_type)`
4. Branch: `key in self._data_sources` -> raise `RuntimeError` (duplicate)
5. Branch: `isinstance(series, pd.Series)` -> wrap in `GenericDataSource(series)`; else -> use as-is
6. Store `self._data_sources[key] = source`

**Raises:** `RuntimeError` when instrument has no name or duplicate key exists

### DataManager.get_data(self, state: Union[dt.date, dt.datetime], instrument: Instrument, valuation_type: ValuationFixingType) -> Any
Purpose: Route a data query to the appropriate source.

**Algorithm:**
1. Build key: `freq = REAL_TIME if isinstance(state, dt.datetime) else DAILY`
2. Extract instrument name: `instrument.name.split('_')[-1]`
3. Return `self._data_sources[key].get_data(state)`

### DataManager.get_data_range(self, start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime], instrument: Instrument, valuation_type: ValuationFixingType) -> Any
Purpose: Route a range query to the appropriate source.

**Algorithm:**
1. Build key: `freq = REAL_TIME if isinstance(start, dt.datetime) else DAILY`
2. Extract instrument name: `instrument.name.split('_')[-1]`
3. Return `self._data_sources[key].get_data_range(start, end)`

## State Mutation
- `GsDataSource.loaded_data`: `None` initially; set on first `get_data()` or `get_data_range()` call when `min_date` is truthy (lazy caching). Subsequent calls use cached data.
- `GenericDataSource.data_set`: Modified **in-place** during `get_data()` when data is missing and strategy is `interpolate` or `fill_forward`. NaN is inserted, index is sorted, then interpolation/fill is applied.
- `DataManager._data_sources`: Populated via `add_data_source()`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `DataSource.get_data` | Always (abstract) |
| `RuntimeError` | `DataSource.get_data_range` | Always (abstract) |
| `RuntimeError` | `GenericDataSource.get_data` | Unrecognised `missing_data_strategy` |
| `RuntimeError` | `DataManager.add_data_source` | `instrument.name is None` |
| `RuntimeError` | `DataManager.add_data_source` | Duplicate key |
| `KeyError` | `GenericDataSource.get_data` | State not in data_set when strategy is `fail` |

## Edge Cases
- `GsDataSource` lazy loads with caching; first call with `min_date` set caches the full range
- `GsDataSource.get_data` with `state=None` and no `min_date` loads from year 2000
- `GenericDataSource.get_data` modifies `data_set` in-place during missing data handling, which affects future calls
- `GenericDataSource.__eq__` returns `False` for non-GenericDataSource comparisons (not `NotImplemented`)
- `DataManager` freq detection: `dt.datetime` -> `REAL_TIME`, `dt.date` -> `DAILY`
- `instrument.name.split('_')[-1]` strips prefix from instrument names (e.g., `"ActionName_InstrName_Date"` -> `"Date"`)
- `GsDataSource.get_data_range` with `int` end returns tail rows before start (lookback window)
- Empty `pd.Series` (non-DataSource) in `add_data_source` is silently skipped

## Bugs Found
None.

## Coverage Notes
- Branch count: ~40
- `GsDataSource` methods need `Dataset` mock to avoid API calls
- `GenericDataSource.get_data` has the most complex branching (~12 paths)
- `DataManager.get_data`/`get_data_range` have 2 branches each (datetime vs date frequency)
