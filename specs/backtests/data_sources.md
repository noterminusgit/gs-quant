# data_sources.py

## Summary
Data source classes: DataSource (base), GsDataSource (GS Dataset API), GenericDataSource (in-memory pandas), DataManager (routes data requests to sources).

## Classes

### DataSource
- Sub-class registry pattern via __init_subclass__
- get_data/get_data_range: raise RuntimeError

### GsDataSource
- get_data(state):
  1. If loaded_data is None:
     a. If min_date → load range
     b. If state not None → load single date range
     c. Else → load from year 2000
  2. Return loaded_data[value_header].at[state]
- get_data_range(start, end):
  1. If loaded_data is None:
     a. If asset_id → add to kwargs
     b. If min_date → load from min to max; else from start to max
  2. If end is int → tail(end) before start
  3. Else → filter between start and end

### GenericDataSource
- __eq__: checks missing_data_strategy and data_set.equals
- __post_init__: detects tz-aware index
- get_data(state):
  1. None → return full dataset
  2. Iterable → recursive for each
  3. tz-aware but state is naive → add UTC
  4. Timestamp in data → return direct
  5. state in data or strategy==fail → direct (may raise KeyError)
  6. Else (missing): insert NaN, sort, apply strategy (interpolate/ffill/else RuntimeError)
- get_data_range(start, end):
  1. int end → tail before start
  2. Else → filter between start and end

### DataManager
- add_data_source:
  1. Not DataSource and empty series → return (skip)
  2. instrument.name is None → RuntimeError
  3. Duplicate key → RuntimeError
  4. pd.Series → wrap in GenericDataSource
- get_data: key = (freq based on type, instrument.name.split('_')[-1], valuation_type)
- get_data_range: same key logic

## Edge Cases
- GsDataSource lazy loading with caching
- GenericDataSource modifies data_set in-place during missing data handling
- DataManager freq detection: datetime → REAL_TIME, date → DAILY
- instrument.name.split('_')[-1] removes prefix

## Bugs Found
None.

## Coverage Notes
- ~40 branches
- GsDataSource needs Dataset mock
