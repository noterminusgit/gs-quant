# helper.py

## Summary
Core helper/utility module for the timeseries package providing window management (`Window`, `normalize_window`, `apply_ramp`), decorator infrastructure for the Marquee Plot Service (`plot_function`, `plot_method`, `plot_measure`, `plot_measure_entity`, `plot_session_function`, `requires_session`), pandas version compatibility constants, rolling window computation (`rolling_offset`, `rolling_apply`), enum/constant factories (`_create_enum`, `_create_int_enum`), tenor/date conversion utilities (`_to_offset`, `_tenor_to_month`, `_month_to_tenor`), data-fetching helpers (`get_df_with_retries`, `get_dataset_data_with_retries`, `get_dataset_with_many_assets`), and miscellaneous utilities (`_split_where_conditions`, `check_forward_looking`, `log_return`).

## Dependencies
- Internal: `gs_quant.api.gs.data` (QueryType), `gs_quant.api.utils` (ThreadPoolManager), `gs_quant.data` (DataContext, Dataset), `gs_quant.datetime.relative_date` (RelativeDate), `gs_quant.entities.entity` (EntityType), `gs_quant.errors` (MqValueError, MqRequestError), `gs_quant.timeseries.measure_registry` (register_measure)
- External: `datetime` (dt), `inspect`, `logging`, `os`, `re`, `enum` (Enum, IntEnum), `functools` (wraps, partial), `typing` (Optional, Union, List, Iterable, Callable), `numpy` (np), `pandas` (pd)
- Optional: `quant_extensions.timeseries.rolling` (rolling_apply) -- falls back to pure-Python implementation if unavailable

## Type Definitions

### Window (class)
Represents a rolling window with a size and ramp-up period.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| w | `Union[int, str, pd.DateOffset, None]` | `None` | Window size (number of observations or date offset) |
| r | `Union[int, str, pd.DateOffset, None]` | same as `w` | Ramp-up value; defaults to `w` if `r` is not provided |

### Entitlement (Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| INTERNAL | `"internal"` | Internal entitlement level |

### Interpolate (Enum, dynamic)
Created via `_create_enum('Interpolate', ['intersect', 'step', 'nan', 'zero', 'time'])`.

| Value | Raw |
|-------|-----|
| INTERSECT | `"intersect"` |
| STEP | `"step"` |
| NAN | `"nan"` |
| ZERO | `"zero"` |
| TIME | `"time"` |

### Returns (Enum, dynamic)
Created via `_create_enum('Returns', ['simple', 'logarithmic', 'absolute'])`.

| Value | Raw |
|-------|-----|
| SIMPLE | `"simple"` |
| LOGARITHMIC | `"logarithmic"` |
| ABSOLUTE | `"absolute"` |

### SeriesType (Enum, dynamic)
Created via `_create_enum('SeriesType', ['prices', 'returns'])`.

| Value | Raw |
|-------|-----|
| PRICES | `"prices"` |
| RETURNS | `"returns"` |

### CurveType (Enum, dynamic)
Created via `_create_enum('CurveType', ['prices', 'excess_returns'])`.

| Value | Raw |
|-------|-----|
| PRICES | `"prices"` |
| EXCESS_RETURNS | `"excess_returns"` |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_PD_VERSION` | `tuple[int, int]` | Parsed from `pd.__version__` | Major.minor pandas version tuple |
| `FREQ_MONTH_END` | `str` | `"ME"` or `"M"` | Month-end frequency string (pandas >= 2.2 vs older) |
| `FREQ_QUARTER_END` | `str` | `"QE"` or `"Q"` | Quarter-end frequency string |
| `FREQ_YEAR_END` | `str` | `"YE"` or `"Y"` | Year-end frequency string |
| `FREQ_HOUR` | `str` | `"h"` or `"H"` | Hour frequency string |
| `FREQ_SECOND` | `str` | `"s"` or `"S"` | Second frequency string |
| `FREQ_BUSINESS_MONTH_END` | `str` | `"BME"` or `"BM"` | Business month-end frequency string |
| `FREQ_BUSINESS_QUARTER_END` | `str` | `"BQE"` or `"BQ"` | Business quarter-end frequency string |
| `FREQ_BUSINESS_YEAR_END` | `str` | `"BYE"` or `"BY"` | Business year-end frequency string |
| `ENABLE_DISPLAY_NAME` | `str` | `"GSQ_ENABLE_MEASURE_DISPLAY_NAME"` | Environment variable name for enabling display names |
| `USE_DISPLAY_NAME` | `bool` | `os.environ.get(ENABLE_DISPLAY_NAME) == "1"` | Whether display names are enabled at module load |
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### rolling_apply(s: pd.Series, offset: pd.DateOffset, function: Callable[[np.ndarray], float]) -> pd.Series
Purpose: Fallback pure-Python rolling window apply when C extension is unavailable. For each index position, applies `function` to the window of values within the offset.

**Algorithm:**
1. Branch: If `s.index` is a `pd.DatetimeIndex`:
   a. For each `idx` in `s.index`: apply `function` to `s.loc[(s.index > (idx - offset)) & (s.index <= idx)]`.
2. Otherwise (date index, not datetime):
   a. For each `idx`: apply `function` to `s.loc[(s.index > (idx - offset).date()) & (s.index <= idx)]`.
3. Return `pd.Series(values, index=s.index, dtype=np.double)`.

Note: Only defined if `quant_extensions.timeseries.rolling.rolling_apply` cannot be imported.

### _create_enum(name: str, members: list) -> Enum
Purpose: Dynamically create an Enum class with uppercased member names and lowercased values.

**Algorithm:**
1. Return `Enum(name, {n.upper(): n.lower() for n in members}, module=__name__)`.

### _create_int_enum(name: str, mappings: dict) -> IntEnum
Purpose: Dynamically create an IntEnum class from a dict mapping names to integer values.

**Algorithm:**
1. Return `IntEnum(name, {k.upper(): v for k, v in mappings.items()})`.

### _to_offset(tenor: str) -> pd.DateOffset
Purpose: Parse a tenor string (e.g., "1m", "5d", "2y") into a pandas DateOffset.

**Algorithm:**
1. Match `tenor` against regex `(\d+)([hdwmy])`.
2. Branch: No match -> raise `MqValueError('invalid tenor ' + tenor)`.
3. Map letter to keyword: `h`->`hours`, `d`->`days`, `w`->`weeks`, `m`->`months`, `y`->`years`.
4. Return `pd.DateOffset(**{name: int(magnitude)})`.

**Raises:** `MqValueError` when tenor format is invalid.

### _tenor_to_month(relative_date: str) -> int
Purpose: Convert a relative date tenor (months or years) to a number of months.

**Algorithm:**
1. Match `relative_date` against regex `([1-9]\d*)([my])`.
2. Branch: Match found:
   a. If unit is `m` -> return magnitude.
   b. If unit is `y` -> return magnitude * 12.
3. Branch: No match -> raise `MqValueError`.

**Raises:** `MqValueError` when input is not in months or years format.

### _month_to_tenor(months: int) -> str
Purpose: Convert a number of months to a tenor string.

**Algorithm:**
1. Branch: If `months % 12 == 0` -> return `f'{months // 12}y'`.
2. Otherwise -> return `f'{months}m'`.

### Window.__init__(self, w: Union[int, str, None] = None, r: Union[int, str, None] = None) -> None
Purpose: Create a Window object with size `w` and ramp `r`.

**Algorithm:**
1. Set `self.w = w`.
2. Branch: If `r` is None -> set `self.r = w`. Otherwise -> set `self.r = r`.

### Window.as_dict(self) -> dict
Purpose: Serialize window to dictionary.

**Algorithm:**
1. Return `{'w': self.w, 'r': self.r}`.

### Window.from_dict(cls, obj: dict) -> Window
Purpose: Class method to deserialize a Window from a dictionary.

**Algorithm:**
1. Return `Window(w=obj.get('w'), r=obj.get('r'))`.

### _check_window(series_length: int, window: Window) -> None
Purpose: Validate window parameters against series length.

**Algorithm:**
1. Branch: If `series_length > 0` AND `window.w` is int AND `window.r` is int:
   a. Branch: If `window.w <= 0` -> raise `MqValueError('Window value must be greater than zero.')`.
   b. Branch: If `window.r > series_length` OR `window.r < 0` -> raise `MqValueError('Ramp value must be less than the length of the series and greater than zero.')`.

**Raises:** `MqValueError` for invalid window size or ramp value.

### apply_ramp(x: pd.Series, window: Window) -> pd.Series
Purpose: Apply ramp-up to a series by trimming the initial portion.

**Algorithm:**
1. Call `_check_window(len(x), window)`.
2. Branch: If `window.w` is int AND `window.w > len(x)` -> return empty `pd.Series(dtype=float)`.
3. Branch: If `window.r` is a `pd.DateOffset`:
   a. Branch: If index is a date subtype -> return `x.loc[(x.index[0] + window.r).date():]`.
   b. Otherwise -> return `x.loc[(x.index[0] + window.r).to_pydatetime():]`.
4. Otherwise (int ramp) -> return `x[window.r:]`.

### normalize_window(x: Union[pd.Series, pd.DataFrame], window: Union[Window, int, str, None], default_window: int = None) -> Window
Purpose: Normalize various window representations into a canonical Window object.

**Algorithm:**
1. If `default_window` is None -> set to `len(x)`.
2. Branch: If `window` is int -> return `Window(window, window)`.
3. Branch: If `window` is str -> parse with `_to_offset`; return `Window(offset, offset)`.
4. Branch: If `window` is None -> return `Window(default_window, 0)`.
5. Otherwise (Window object):
   a. If `window.w` is str -> convert to offset.
   b. If `window.r` is str -> convert to offset.
   c. If `window.w` is None -> replace with `default_window`.
6. Call `_check_window(default_window, window)`.
7. Return `window`.

### plot_function(fn) -> fn
Purpose: Decorator that marks a function for export to plottool as a pure function.

**Algorithm:**
1. Set `fn.plot_function = True`.
2. Return `fn`.

### plot_session_function(fn) -> fn
Purpose: Decorator that marks a function for plottool export and requires a session.

**Algorithm:**
1. Set `fn.plot_function = True`.
2. Set `fn.requires_session = True`.
3. Return `fn`.

### check_forward_looking(pricing_date, source, name: str = "function") -> None
Purpose: Validate that a forward-looking date range is set when called from plottool.

**Algorithm:**
1. Branch: If `pricing_date is not None` OR `source != 'plottool'` -> return (no check needed).
2. Branch: If `DataContext.current.end_date <= dt.date.today()` -> raise `MqValueError` with message about requiring forward-looking date range.

**Raises:** `MqValueError` when plottool call does not have a forward-looking date range.

### plot_measure(asset_class: tuple, asset_type: Optional[tuple] = None, dependencies: Optional[List[QueryType]] = tuple(), asset_type_excluded: Optional[tuple] = None, display_name: Optional[str] = None, entitlements: Optional[List[Entitlement]] = []) -> Callable
Purpose: Decorator factory that marks a function as a plottool measure with asset class/type restrictions.

**Algorithm:**
1. Return inner `decorator(fn)`:
   a. Assert `asset_class` is tuple with >= 1 element.
   b. Assert `asset_type` is None or tuple.
   c. Assert `asset_type_excluded` is None or tuple.
   d. Assert `asset_type` and `asset_type_excluded` are not both set.
   e. Set attributes on `fn`: `plot_measure`, `entity_type`, `asset_class`, `asset_type`, `asset_type_excluded`, `dependencies`, `entitlements`.
   f. Branch: If `USE_DISPLAY_NAME`:
      - Set `fn.display_name = display_name`.
      - Call `register_measure(fn)` -> `multi_measure`.
      - Set `multi_measure.entity_type = EntityType.ASSET`.
      - Return `multi_measure`.
   g. Otherwise -> return `fn`.

### plot_measure_entity(entity_type: EntityType, dependencies: Optional[Iterable[QueryType]] = tuple()) -> Callable
Purpose: Decorator factory for entity-type measures (non-asset).

**Algorithm:**
1. Return inner `decorator(fn)`:
   a. Assert `entity_type` is `EntityType`.
   b. If dependencies is not None, assert all are `QueryType`.
   c. Set `fn.plot_measure_entity = True`, `fn.entity_type`, `fn.dependencies`.
   d. Return `fn`.

### requires_session(fn) -> fn
Purpose: Decorator that marks a function as requiring an active session.

**Algorithm:**
1. Set `fn.requires_session = True`.
2. Return `fn`.

### plot_method(fn) -> Callable
Purpose: Decorator that marks a function for plottool export as a method, stripping unexpected keyword arguments.

**Algorithm:**
1. Set `fn.plot_method = True`.
2. Return wrapper `ignore_extra_argument(*args, **kwargs)`:
   a. For each of `'real_time'`, `'interval'`, `'time_filter'`:
      - If arg not in `fn`'s signature parameters -> pop from kwargs.
   b. Call and return `fn(*args, **kwargs)`.

### log_return(logger: logging.Logger, message: str) -> Callable
Purpose: Decorator factory that logs the return value of a function.

**Algorithm:**
1. Return `outer(fn)` -> `inner(*args, **kwargs)`:
   a. Call `response = fn(*args, **kwargs)`.
   b. `logger.debug('%s: %s', message, response)`.
   c. Return `response`.

### get_df_with_retries(fetcher: Callable, start_date, end_date, exchange, retries: int = 1) -> pd.DataFrame
Purpose: Call a data-fetching function with retry logic, shifting the date range backwards on empty results.

**Algorithm:**
1. `retries = max(retries, 0)`.
2. While `retries > -1`:
   a. Call `fetcher()` within `DataContext(start_date, end_date)`.
   b. Branch: If result is not empty -> break.
   c. Compute new `end_date` as 1 business day before `start_date` using `RelativeDate`.
   d. Set `start_date = end_date`.
   e. Decrement retries.
3. Return result.

### get_dataset_data_with_retries(dataset: Dataset, *, start: dt.date, end: dt.date, count: int = 0, max_retries: int = 5, **kwargs) -> pd.DataFrame
Purpose: Fetch dataset data, splitting the date range in half on `MqRequestError` and retrying recursively.

**Algorithm:**
1. Try `dataset.get_data(start=start, end=end, **kwargs)`.
2. Branch: `MqRequestError` caught:
   a. Branch: If `count < max_retries`:
      - Compute `mid = start + (end - start) / 2`.
      - Increment `count`.
      - Create two partial calls for `[start, mid]` and `[mid+1, end]`.
      - Run both async via `ThreadPoolManager.run_async`.
      - Concatenate and sort results.
   b. Otherwise -> re-raise the error.
3. Return data.

**Raises:** `MqRequestError` after exhausting retries.

### get_dataset_with_many_assets(ds: Dataset, *, assets: List[str], start: dt.date, end: dt.date, batch_limit: int = 100, **kwargs) -> pd.DataFrame
Purpose: Fetch dataset data for many assets by batching into groups of `batch_limit`.

**Algorithm:**
1. Chunk `assets` into batches of `batch_limit`.
2. Create partial tasks calling `ds.get_data(assetId=batch, ...)` for each batch.
3. Run all tasks async via `ThreadPoolManager.run_async`.
4. Concatenate and return results.

### _split_where_conditions(where: dict) -> list[dict]
Purpose: Expand a where-clause dict with list values into a list of dicts with single values (Cartesian product).

**Algorithm:**
1. Initialize `la = [dict()]`.
2. For each `(k, v)` in `where.items()`:
   a. Initialize `lb = []`.
   b. While `la` has elements, pop each `temp`:
      - Branch: If `v` is a list -> for each element `cv`, clone `temp`, add `{k: [cv]}`, append to `lb`.
      - Otherwise -> append `dict(**temp, **{k: v})` to `lb`.
   c. Set `la = lb`.
3. Return `la`.

### _pandas_roll(s: pd.Series, window_str: str, method_name: str) -> pd.Series
Purpose: Helper to call a named rolling method on a series.

**Algorithm:**
1. Return `getattr(s.rolling(window_str), method_name)()`.

### rolling_offset(s: pd.Series, offset: pd.DateOffset, function: Callable[[np.ndarray], float], method_name: str = None) -> pd.Series
Purpose: Perform rolling window calculations with optional fast-path using pandas native rolling for fixed-frequency offsets.

**Algorithm:**
1. Define `fixed = {'hour': 'h', 'hours': 'h', 'day': 'D', 'days': 'D'}`.
2. Branch: If `method_name` is provided AND `offset.kwds` has exactly 1 entry:
   a. Extract `(freq, count)` from `offset.kwds`.
   b. Branch: If `freq` is in `fixed`:
      - Build `window_str = f'{count}{fixed[freq]}'`.
      - Branch: If index is `np.datetime64` subtype -> return `_pandas_roll(s, window_str, method_name)`.
      - Otherwise -> copy series, convert index to datetime, call `_pandas_roll`, re-attach original index.
3. Fall back to `rolling_apply(s, offset, function)`.

## State Mutation
- `USE_DISPLAY_NAME`: Module-level bool set at import time from environment variable; read-only thereafter.
- `_PD_VERSION`: Module-level tuple set at import time; read-only thereafter.
- Frequency constants (`FREQ_MONTH_END`, etc.): Module-level strings set at import time; read-only thereafter.
- `plot_measure` decorator modifies function attributes (`plot_measure`, `entity_type`, `asset_class`, etc.) at decoration time.
- `plot_method` wraps the decorated function, modifying its behavior to strip extra kwargs.
- Thread safety: All utilities are stateless or operate on local data. `ThreadPoolManager.run_async` handles its own thread safety.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_to_offset` | Tenor string does not match `(\d+)([hdwmy])` pattern |
| `MqValueError` | `_tenor_to_month` | Relative date not in months or years format |
| `MqValueError` | `_check_window` | `window.w <= 0` or `window.r` out of bounds |
| `MqValueError` | `check_forward_looking` | Plottool call without forward-looking date range |
| `MqRequestError` | `get_dataset_data_with_retries` | Request fails after max retries |

## Edge Cases
- `_to_offset` only supports single-letter units (`h`, `d`, `w`, `m`, `y`); multi-character suffixes like "bd" (business days) are not supported.
- `_tenor_to_month` only accepts months and years (not days, weeks, or hours).
- `_check_window` only validates when both `w` and `r` are ints AND `series_length > 0`; DateOffset windows bypass validation.
- `apply_ramp` with `window.w` as a DateOffset does not check if window exceeds series length.
- `normalize_window` with `window=None` sets ramp to 0 (no ramp), while `window` as int sets ramp equal to window size.
- `rolling_offset` destructively pops from `offset.kwds` via `popitem()`, which mutates the offset object. This could cause issues if the offset is reused.
- `get_df_with_retries` sets both `start_date` and `end_date` to the same (earlier) date on retry, querying a single-day range.
- `get_dataset_data_with_retries` uses binary splitting, so a single failing date could cause O(log(n)) retries.
- `_split_where_conditions` wraps scalar values as-is but wraps list elements in `[cv]` (single-element lists), maintaining the list type.
- `rolling_apply` fallback uses exclusive lower bound (`index > idx - offset`) so the window does not include the exact boundary point.
- The `plot_measure` decorator has an assertion (`assert asset_type is None or asset_type_excluded is None`) that would fail in production if both are provided, rather than raising a proper error.

## Bugs Found
- Line 419 (`rolling_offset`): `offset.kwds.popitem()` mutates the `pd.DateOffset` object's internal `kwds` dict, which means the offset becomes empty after the first call. If the same offset object is passed to `rolling_offset` again, the fast path will not be taken and behavior changes. (OPEN)

## Coverage Notes
- Branch count: ~50
- Key branches: pandas version check (line 40-57); `rolling_apply` index type check (line 74-77); `_to_offset` letter dispatch (lines 97-107); `_check_window` validation branches (lines 165-169); `apply_ramp` window type checks (lines 174-182); `normalize_window` multi-branch dispatch (lines 191-206); `plot_measure` USE_DISPLAY_NAME branch (lines 258-265); `plot_method` argument stripping (lines 298-301); `get_df_with_retries` retry loop (lines 333-342); `get_dataset_data_with_retries` error handling (lines 349-362); `rolling_offset` fast-path conditions (lines 418-429).
- Pragmas: line 49 `else: # pragma: no cover` -- older pandas version branch for frequency constants.
