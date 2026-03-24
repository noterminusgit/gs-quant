# data.py

## Summary
Abstract base class defining the interface for data API operations (query, last, schema introspection) and a static `build_query` factory method that constructs either `DataQuery` or `MDAPIDataQuery` objects from flexible keyword arguments. This is the central data-fetching contract inherited by concrete implementations (GS, FRED, etc.).

## Dependencies
- Internal: `gs_quant.api.api_session` (ApiWithCustomSession), `gs_quant.api.fred.fred_query` (FredQuery), `gs_quant.base` (Base), `gs_quant.target.coordinates` (MDAPIDataQuery), `gs_quant.target.data` (DataQuery)
- External: `datetime` (date, datetime), `logging`, `abc` (ABCMeta), `typing` (Optional, Union, List), `inflection` (underscore), `pandas` (DataFrame, Series)

## Type Definitions

### DataApi (ABCMeta)
Inherits: ApiWithCustomSession

Abstract base class. All concrete data APIs must subclass this and implement the abstract `@classmethod` methods.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger for debug messages |

## Functions/Methods

### DataApi.query_data(cls, query: Union[DataQuery, FredQuery], dataset_id: str = None) -> Union[list, tuple]
Purpose: Abstract method. Query a dataset for data matching the given query.

**Algorithm:**
1. Raises `NotImplementedError` unconditionally.

### DataApi.last_data(cls, query: DataQuery, dataset_id: str = None) -> Union[list, tuple]
Purpose: Abstract method. Get the last data point(s) for a dataset.

**Algorithm:**
1. Raises `NotImplementedError` unconditionally.

### DataApi.symbol_dimensions(cls, dataset_id: str) -> tuple
Purpose: Abstract method. Return the symbol dimensions for a dataset.

**Algorithm:**
1. Raises `NotImplementedError` unconditionally.

### DataApi.time_field(cls, dataset_id: str) -> str
Purpose: Abstract method. Return the time field name for a dataset.

**Algorithm:**
1. Raises `NotImplementedError` unconditionally.

### DataApi.construct_dataframe_with_types(cls, dataset_id: str, data: Union[Base, list, tuple, pd.Series], schema_varies=False, standard_fields=False) -> pd.DataFrame
Purpose: Abstract method. Build a typed DataFrame from raw data.

**Algorithm:**
1. Raises `NotImplementedError` unconditionally.

### DataApi.build_query(start, end, as_of, since, restrict_fields, format, dates, empty_intervals, **kwargs) -> Union[DataQuery, MDAPIDataQuery]
Purpose: Static factory that constructs either an `MDAPIDataQuery` (when `market_data_coordinates` is in kwargs) or a `DataQuery`, routing keyword arguments to either query properties or the `where` dict.

**Algorithm:**
1. Determine `end_is_time` = `isinstance(end, dt.datetime)`, `start_is_time` = `isinstance(start, dt.datetime)`
2. Branch: if `kwargs` contains `market_data_coordinates`:
   a. Compute `real_time` = both start and end are either `None` or `datetime`
   b. Construct `MDAPIDataQuery` with time or date fields set based on `real_time` flag
3. Branch: else (no `market_data_coordinates`):
   a. Branch: if `start_is_time` and `end is not None` and `not end_is_time` -> raise `ValueError`
   b. Branch: if `start` is `date` and `end is not None` and `end` is not `date` -> raise `ValueError`
   c. Construct `DataQuery` with start/end routed to date or time fields based on type, plus `as_of`, `since`, `format`, `dates`, `empty_intervals`
4. Get `query_properties` from constructed query
5. Initialize `query.where = dict()`
6. For each `(field, value)` in `kwargs`:
   a. Convert `field` to `snake_case_field` via `inflection.underscore`
   b. Branch: if `snake_case_field` is in `query_properties` AND is not `'name'` -> `setattr(query, snake_case_field, value)`
   c. Branch: else -> `query.where[field] = value`
7. Branch: if `query.fields` is not None:
   a. Try to set `query.restrict_fields = restrict_fields`
   b. Branch: on `AttributeError` -> log debug message, swallow exception
8. Return `query`

**Raises:** `ValueError` when start/end type mismatch occurs (datetime start with date end, or date start with non-date end)

## State Mutation
- No instance state. `build_query` is `@staticmethod`.
- `build_query` mutates the constructed query object's `where` dict and may set arbitrary properties via `setattr`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `query_data`, `last_data`, `symbol_dimensions`, `time_field`, `construct_dataframe_with_types` | Always (abstract methods) |
| `ValueError` | `build_query` | When `start` is `datetime` but `end` is not `datetime` (and `end` is not None) |
| `ValueError` | `build_query` | When `start` is `date` but `end` is not `date` (and `end` is not None) |

## Edge Cases
- `build_query` with `start=None, end=None` produces a query with all date/time fields as `None`
- `build_query` with a kwarg named `name` is always routed to `query.where` (explicitly excluded from property-setting even though it may be a query property)
- `build_query` with `market_data_coordinates` in kwargs: `real_time` is `True` when both start and end are `None`, causing all date/time fields to be `None` on the MDAPI query
- `build_query` with `query.fields = None` (the common case) skips the `restrict_fields` assignment entirely
- The `AttributeError` catch on `restrict_fields` handles query types that do not have this attribute
- `start` being a `datetime` (which is a subclass of `date`) means `isinstance(start, dt.date)` is also true; the second validation check on line 87 fires for `date` instances that are not `datetime`, but because `datetime` is a subclass of `date`, a `datetime` start would also pass the `isinstance(start, dt.date)` check -- however the first validation on line 84 takes precedence since `start_is_time` is checked first

## Bugs Found
- Line 55: `construct_dataframe_with_types` error message says `'Must implement time_field'` instead of `'Must implement construct_dataframe_with_types'` (copy-paste error, cosmetic)

## Coverage Notes
- Branch count: 18
- Key branches: `market_data_coordinates` presence (2 paths), `real_time` computation (4 sub-conditions), start/end type validation (2 raises), `snake_case_field in query_properties` routing, `fields is not None` guard, `AttributeError` catch
- Pragmas: none
