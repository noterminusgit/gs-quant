# fred/data.py

## Summary
Concrete implementation of `DataApi` for querying the Federal Reserve Economic Data (FRED) API. Provides methods to build FRED-specific queries, fetch observation data, parse JSON responses into pandas Series, and construct typed DataFrames. Requires a valid FRED API key passed at construction time.

## Dependencies
- Internal: `gs_quant.api.utils` (handle_proxy), `gs_quant.api.data` (DataApi), `gs_quant.api.fred.fred_query` (FredQuery)
- External: `typing` (Iterable, Optional, Union), `pandas` (DataFrame, Series), `datetime` (date, datetime), `textwrap` (dedent), `requests.exceptions` (HTTPError), `dataclasses` (asdict, replace)

## Type Definitions

### FredDataApi (class)
Inherits: DataApi

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `api_key` | `str` | (required) | FRED API key, must be provided at construction |

## Enums and Constants

### Module Constants (class-level on FredDataApi)
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `earliest_realtime_start` | `str` | `'1776-07-04'` | FRED's earliest allowed realtime start date |
| `latest_realtime_end` | `str` | `'9999-12-31'` | FRED's latest allowed realtime end date |
| `root_url` | `str` | `'https://api.stlouisfed.org/fred/series/observations'` | Base URL for FRED observations endpoint |

## Functions/Methods

### FredDataApi.__init__(self, api_key=None)
Purpose: Initialize the FRED API client with a required API key.

**Algorithm:**
1. Branch: if `api_key is not None` -> set `self.api_key = api_key`
2. Branch: else -> raise `ValueError` with message directing user to sign up for a FRED API key

**Raises:** `ValueError` when `api_key` is `None`

### FredDataApi.build_query(self, start, end, as_of, since, fields, **kwargs) -> FredQuery
Purpose: Build a `FredQuery` dataclass from date/time parameters. Overrides the parent `DataApi.build_query` static method as an instance method.

**Algorithm:**
1. Branch: if `start is not None` and `end is not None`:
   a. Branch: if `type(start) is not type(end)` -> raise `ValueError('Start and end types must match!')`
2. Construct and return `FredQuery(observation_start=start, observation_end=end, realtime_end=as_of, realtime_start=since)`

**Raises:** `ValueError` when start and end are both provided but have different types

### FredDataApi.query_data(self, query: FredQuery, dataset_id: str, asset_id_type: str = None) -> pd.Series
Purpose: Execute a FRED query for a given series ID and return parsed observation data as a pandas Series.

**Algorithm:**
1. Create `request` by replacing `api_key` and `series_id` on the query using `dataclasses.replace`
2. Call `handle_proxy(self.root_url, asdict(request))` to make the HTTP request
3. Call `self.__handle_response(response)` to parse the response
4. Set `handled.name = dataset_id`
5. Return the named Series

### FredDataApi.last_data(self, query: FredQuery, dataset_id: str) -> pd.Series
Purpose: Get the last data point for a FRED series.

**Algorithm:**
1. Call `self.query_data(query, dataset_id)` to get full data
2. Return `data.last('1D')` (last 1-day window)

### FredDataApi.__handle_response(response) -> pd.Series [staticmethod, private]
Purpose: Parse a FRED API HTTP response into a cleaned pandas Series of float values indexed by date.

**Algorithm:**
1. Try: call `response.raise_for_status()`, then parse `json_data = response.json()`
2. Branch: on `HTTPError` -> raise `ValueError(response.json()['error_message'])`
3. Branch: if `len(json_data['observations'])` is 0 (empty) -> raise `ValueError` with "No data exists" message
4. Extract `['date', 'value']` columns from observations into a DataFrame
5. Filter out rows where `value == '.'` (FRED's missing-data sentinel)
6. Convert `date` column to `pd.to_datetime`
7. Convert `value` column to `float`
8. Set `date` as index, extract `value` Series
9. Sort by index
10. Return sorted Series

**Raises:** `ValueError` when HTTP error occurs or when no observations are returned

### FredDataApi.construct_dataframe_with_types(self, dataset_id: str, data: pd.Series, schema_varies=False, standard_fields=False) -> pd.DataFrame
Purpose: Convert a Series to a DataFrame, or return empty DataFrame if data is empty or wrong type.

**Algorithm:**
1. Branch: if `len(data)` is truthy AND `data` is a `pd.Series` -> return `data.to_frame()`
2. Branch: else -> return empty `pd.DataFrame({})`

### FredDataApi.symbol_dimensions(self, dataset_id: str) -> tuple
Purpose: Get the shape of data for a given FRED series.

**Algorithm:**
1. Create a default `FredQuery()`
2. Query data via `self.query_data(query, dataset_id)`
3. Return `data.shape`

### FredDataApi.time_field(self, dataset_id: str) -> str
Purpose: Return the time field name for a FRED dataset. Currently unimplemented.

**Algorithm:**
1. Returns `None` implicitly (pass)

## State Mutation
- `self.api_key`: Set once during `__init__`, never modified afterwards
- No other mutable instance state
- Thread safety: Instance is safe for concurrent reads after construction. `query_data` and other query methods are stateless beyond `self.api_key`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `__init__` | When `api_key` is `None` |
| `ValueError` | `build_query` | When both `start` and `end` are provided but have different types |
| `ValueError` | `__handle_response` | When HTTP response is an error (wraps the FRED error message) |
| `ValueError` | `__handle_response` | When response contains zero observations |
| `RuntimeError` | `query_data` (via `handle_proxy`) | When on internal network without `gs_quant_auth` |

## Edge Cases
- `__handle_response` filters rows where `value == '.'` -- this is FRED's convention for missing/unavailable data points; the resulting Series may be shorter than the raw observations
- `__handle_response` uses `id` (built-in) in the "No data exists" error message instead of `dataset_id` -- this produces `"No data exists for <class 'builtin_function_or_method'>"` instead of the intended series ID
- `construct_dataframe_with_types` with a non-empty object that is not a `pd.Series` returns empty DataFrame (the `isinstance` check fails)
- `construct_dataframe_with_types` with an empty Series (`len(data) == 0`) returns empty DataFrame even though it is a valid Series
- `last_data` using `data.last('1D')` relies on the Series having a DatetimeIndex; if the index is not datetime-like, this raises a `TypeError`
- `build_query` uses `type(start) is not type(end)` (exact type comparison) rather than `isinstance`, so a `datetime` start and `date` end would fail even though `datetime` is a subclass of `date`
- `time_field` returns `None` implicitly -- callers expecting a string will receive `None`
- `symbol_dimensions` creates a default FredQuery with empty `api_key` and `series_id`, which may fail when `query_data` calls `replace` to set these fields

## Bugs Found
- Line 125: `__handle_response` error message uses Python's built-in `id` function instead of the `dataset_id` parameter (which is not in scope for the static method). The format string `format(id)` produces a useless string like `"No data exists for <built-in function id>"`. (OPEN)
- Line 111: `data.last('1D')` is deprecated in newer pandas versions; should use `data.iloc[-1:]` or similar. (OPEN)

## Coverage Notes
- Branch count: 10
- Key branches: `api_key is not None` in `__init__`, `start/end not None` + type check in `build_query`, `HTTPError` catch in `__handle_response`, empty observations check, `len(data) and isinstance` in `construct_dataframe_with_types`
- Pragmas: none
