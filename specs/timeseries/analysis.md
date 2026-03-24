# analysis.py

## Summary
Timeseries analysis library providing functions for analyzing properties of time series, including spike smoothing, repetition/forward-fill, first/last value extraction, counting, differencing, comparison, and lagging. All public functions are decorated with `@plot_function` for chart service integration.

## Dependencies
- Internal: `gs_quant.datetime` (relative_date_add), `gs_quant.timeseries` (align), `gs_quant.timeseries.helper` (plot_function, Window, Interpolate), `gs_quant.errors` (MqValueError)
- External: `re`, `enum` (Enum), `numbers` (Real), `typing` (Union), `pandas`

## Type Definitions
None (no dataclasses or type aliases).

## Enums and Constants

### ThresholdType(str, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| percentage | `"percentage"` | Threshold is a percentage multiplier (1 + threshold) |
| absolute | `"absolute"` | Threshold is an absolute value difference |

### LagMode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| TRUNCATE | `"truncate"` | Truncate lagged series at original end date |
| EXTEND | `"extend"` | Extend series index into the future for lagged values |

## Functions/Methods

### smooth_spikes(x: pd.Series, threshold: float, threshold_type: ThresholdType = ThresholdType.percentage) -> pd.Series
Purpose: Replace spike values that exceed threshold relative to both neighbors with the average of those neighbors.

**Algorithm:**
1. Define inner `check_percentage(previous, current, next_, multiplier)` -> bool: checks if current is larger than both neighbors * multiplier, or smaller than both neighbors * multiplier
2. Define inner `check_absolute(previous, current, next_, absolute)` -> bool: checks if current exceeds both neighbors by more than the absolute threshold
3. Branch: if `len(x) < 3` -> return empty `pd.Series(dtype=float)`
4. Branch: if `threshold_type == ThresholdType.absolute` -> use `(threshold, check_absolute)`
5. Branch: else (percentage) -> use `(1 + threshold, check_percentage)`
6. Copy input to float result series
7. Iterate from index 1 to len-2, checking each point against its neighbors
8. Branch: if `check_spike(previous, current, next_, threshold_value)` -> replace `result.iloc[i]` with average of neighbors
9. Return `result[1:-1]` (drops first and last points)

### repeat(x: pd.Series, n: int = 1) -> pd.Series
Purpose: Forward-fill missing values and optionally downsample to every n days.

**Algorithm:**
1. Branch: if `n` not in range `(0, 367)` exclusive -> raise `MqValueError`
2. Branch: if `x.empty` -> return `x` unchanged
3. Create date range from first to last index with frequency `{n}D`
4. Reindex with forward-fill method
5. Return result

**Raises:** `MqValueError` when n <= 0 or n >= 367.

### first(x: pd.Series) -> pd.Series
Purpose: Return a series filled with the first value of the input series for all dates.

**Algorithm:**
1. Return `pd.Series(x.iloc[0], x.index)`

### last(x: pd.Series) -> pd.Series
Purpose: Return a series filled with the last non-NaN value of the input series for all dates.

**Algorithm:**
1. Drop NaN values from x
2. Return `pd.Series(x.dropna().iloc[-1], x.index)` using original full index

### last_value(x: pd.Series) -> Union[int, float]
Purpose: Return the last non-NaN value as a scalar.

**Algorithm:**
1. Branch: if `x.empty` -> raise `MqValueError("cannot get last value of an empty series")`
2. Return `x.dropna().iloc[-1]`

**Raises:** `MqValueError` when series is empty.

### count(x: pd.Series) -> pd.Series
Purpose: Cumulative count of non-NaN observations.

**Algorithm:**
1. Apply rolling window of size `x.size` with min_periods=0, calling `.count()`
2. Return result (cumulative count of valid observations)

### diff(x: pd.Series, obs: Union[Window, int, str] = 1) -> pd.Series
Purpose: Compute difference of series values over a given lag.

**Algorithm:**
1. Compute `x - lag(x, obs, LagMode.TRUNCATE)`
2. Return result

### compare(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Compare two series or scalars, returning 1 (x > y), 0 (x == y), or -1 (x < y).

**Algorithm:**
1. Align x and y using the specified interpolation method via `align(x, y, method)`
2. Compute `(x > y) * 1.0 + (x < y) * -1.0`
3. Return result

### lag(x: pd.Series, obs: Union[Window, int, str] = 1, mode: LagMode = LagMode.EXTEND) -> pd.Series
Purpose: Lag a timeseries by a number of observations or a relative date string.

**Algorithm:**
1. Branch: if `obs` is a string:
   - Branch: if `obs` contains 'b' or 'B' (business day pattern) -> raise `MqValueError` with message about unsupported business day offset
   - Save `end = pd.Timestamp(x.index[-1])`
   - Copy `x` to avoid mutation
   - Branch: if `obs` matches pattern `(\d+)y` (year offset):
     - Shift index by `DateOffset(years=N)`
     - Group by index and take first (dedup)
     - Branch: if index has `as_unit` method -> convert to nanoseconds
   - Branch: else (other relative date):
     - Compute new index using `relative_date_add(obs)` for each date
     - Branch: if new index has `as_unit` method -> convert to nanoseconds
   - Branch: if `mode == LagMode.EXTEND` -> return full shifted series
   - Branch: else (TRUNCATE) -> return `y[:end]` (truncate at original end)
2. Branch: else (obs is int or Window):
   - Extract `.w` attribute if obs is a Window object: `obs = getattr(obs, 'w', obs)`
   - Branch: if `mode == LagMode.EXTEND`:
     - Branch: if `x.empty` -> return `x`
     - Branch: if `x.index.resolution != 'day'` -> raise `MqValueError`
     - Build date range to extend index by `abs(obs) + 1` days
     - Branch: if `obs > 0` -> extend from end (`start=x.index[-1]`)
     - Branch: else (`obs <= 0`) -> extend from beginning (`end=x.index[0]`)
     - Reindex with union of original and extended dates
     - Branch: if index has `as_unit` method -> convert to nanoseconds
   - Return `x.shift(obs)`

**Raises:** `MqValueError` for business day offsets in string mode, or non-day resolution index in EXTEND mode.

## State Mutation
- No module-level mutable state.
- `lag` copies the input series (`x.copy()`) in string-obs mode to avoid mutating the caller's series.
- All functions return new series/values; no in-place mutation of inputs.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `repeat` | `n <= 0` or `n >= 367` |
| `MqValueError` | `last_value` | Input series is empty |
| `MqValueError` | `lag` | Business day pattern in string obs (e.g., `'5b'`, `'1B'`) |
| `MqValueError` | `lag` | `mode == EXTEND` with non-day resolution index |

## Edge Cases
- `smooth_spikes` with fewer than 3 data points returns an empty series.
- `smooth_spikes` always drops the first and last points of the input series (returns `result[1:-1]`).
- `first` will raise `IndexError` if the series is empty (no guard).
- `last` will raise `IndexError` if the series has only NaN values after `dropna()`.
- `last_value` guards against empty series but not all-NaN series (will raise `IndexError` from `iloc[-1]` on empty dropna result).
- `lag` with `obs=0` (integer) and `mode=EXTEND` extends the index by 1 day (`abs(0)+1=1`), which may be unexpected.
- `lag` with a year string like `'2y'` groups by index to deduplicate, but other relative date strings do not.
- `lag` string mode checks for business day pattern using regex `[bB]` which would also match strings like `'feb1'` that contain 'b'.
- `diff` delegates to `lag` with `LagMode.TRUNCATE`, so the result series may be shorter than the input.
- `compare` with two Real scalars returns a Real scalar (not a series).

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~22
- Key branches: ThresholdType selection in smooth_spikes, len < 3 guard, repeat empty/bounds checks, last_value empty check, string vs int obs in lag, year vs relative date in lag string mode, EXTEND vs TRUNCATE mode, obs positive vs negative in EXTEND, as_unit availability checks
- Inner functions `check_percentage` and `check_absolute` each have 2 compound boolean branches (current_higher, current_lower)
