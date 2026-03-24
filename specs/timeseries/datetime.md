# datetime.py

## Summary
Date and time manipulation for timeseries, including date/time shifting, calendar operations, curve alignment, interpolation, sampling, aggregation (bucketizing), and day-counting utilities. All public functions are decorated with `@plot_function` to integrate with the Chart Service. The module exposes two dynamic enums (`AggregateFunction`, `AggregatePeriod`) and a private helper for step-interpolation.

## Dependencies
- Internal: `gs_quant.timeseries.helper` (`_create_enum`, `Interpolate`, `plot_function`, `requires_session`, `FREQ_MONTH_END`, `FREQ_QUARTER_END`, `FREQ_YEAR_END`), `gs_quant.datetime` (`GsCalendar`), `gs_quant.datetime.date` (`DayCountConvention`, `PaymentFrequency`, `day_count_fraction`, `date_range` aliased as `_date_range`), `gs_quant.errors` (`MqValueError`, `MqTypeError`)
- External: `datetime` (stdlib, aliased `dt`), `enum` (`Enum`), `numbers` (`Real`), `typing` (`Any`, `Union`, `List`), `numpy` (`np`), `pandas` (`pd`)

## Type Definitions

### TypeAlias
```
AggregateFunction = Enum('AggregateFunction', {'MAX': 'max', 'MIN': 'min', 'MEAN': 'mean', 'SUM': 'sum', 'FIRST': 'first', 'LAST': 'last'})
```
Dynamic enum created via `_create_enum`. Upper-case member names map to lower-case string values. Used in `bucketize()` to select the pandas aggregation function applied to each resampled bucket.

```
AggregatePeriod = Enum('AggregatePeriod', {'WEEK': 'week', 'MONTH': 'month', 'QUARTER': 'quarter', 'YEAR': 'year'})
```
Dynamic enum created via `_create_enum`. Determines the resampling frequency in `bucketize()`.

```
Interpolate = Enum('Interpolate', {'INTERSECT': 'intersect', 'STEP': 'step', 'NAN': 'nan', 'ZERO': 'zero', 'TIME': 'time'})
```
Imported from `helper`. Controls how missing dates are handled in `align()`, `interpolate()`, and `value()`.

## Enums and Constants

### AggregateFunction(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| MAX | `"max"` | Maximum value in each bucket |
| MIN | `"min"` | Minimum value in each bucket |
| MEAN | `"mean"` | Arithmetic mean of each bucket |
| SUM | `"sum"` | Sum of values in each bucket |
| FIRST | `"first"` | First value in each bucket |
| LAST | `"last"` | Last value in each bucket |

### AggregatePeriod(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| WEEK | `"week"` | Weekly buckets |
| MONTH | `"month"` | Monthly buckets |
| QUARTER | `"quarter"` | Quarterly buckets |
| YEAR | `"year"` | Yearly buckets |

### Module Constants
None defined at module level beyond the two dynamic enums above. The frequency constants `FREQ_MONTH_END`, `FREQ_QUARTER_END`, `FREQ_YEAR_END` are imported from `helper` and used inside `bucketize()`. Their values are version-dependent: `'ME'`/`'QE'`/`'YE'` on newer pandas, `'M'`/`'Q'`/`'Y'` on older pandas.

## Functions/Methods

### __interpolate_step(x: pd.Series, dates: pd.Series = None) -> pd.Series
Purpose: Private helper that performs step (forward-fill / backward-fill) interpolation, mapping values from series `x` onto the index of `dates`.

**Algorithm:**
1. Branch: if `x` is empty -> raise `MqValueError('Cannot perform step interpolation on an empty series')`.
2. Determine `first_date`: if the first element of `x.index` is a `pd.Timestamp`, use `pd.Timestamp(dates.index[0])`; otherwise use `dates.index[0]` directly.
3. Locate previous valid date: Branch: if `first_date < x.index[0]` -> use `x.index[0]` as `prev`; else -> use `x.index[x.index.get_indexer([first_date], method='pad')]` to find the nearest preceding index entry.
4. Set `current = x[prev]` (the starting fill value).
5. Right-align `x` with `dates` using `x.align(dates, 'right')`, keeping only the values from `x` (index `[0]`).
6. Iterate over each `(key, val)` in the aligned curve:
   - Branch: if `val` is NaN -> replace with `current` (forward-fill).
   - Branch: else -> update `current = val` (advance the fill value).
7. Return the filled curve.

**Raises:** `MqValueError` when `x` is empty.

---

### align(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.INTERSECT) -> Union[List[pd.Series], List[Real]]
Purpose: Align dates of two series (or scalars) using one of five interpolation strategies.

**Algorithm:**
1. Branch: if both `x` and `y` are `Real` -> return `[x, y]` unchanged.
2. Branch: if only `x` is `Real` -> broadcast `x` into a `pd.Series` with `y`'s index, return `[pd.Series(x, index=y.index), y]`.
3. Branch: if only `y` is `Real` -> broadcast `y` into a `pd.Series` with `x`'s index, return `[x, pd.Series(y, index=x.index)]`.
4. Both are `pd.Series`; dispatch on `method`:
   - Branch: `INTERSECT` -> `x.align(y, 'inner')` (intersection of indices).
   - Branch: `NAN` -> `x.align(y, 'outer')` (union, missing values become NaN).
   - Branch: `ZERO` -> `x.align(y, 'outer', fill_value=0)` (union, missing values become 0).
   - Branch: `TIME` -> `x.align(y, 'outer')`, then `interpolate('time', limit_area='inside')` on both. Requires `DateTimeIndex`.
   - Branch: `STEP` -> `x.align(y, 'outer')`, then `ffill().bfill()` on both.
   - Branch: else -> raise `MqValueError('Unknown intersection type: ' + method)`.
5. Return list of two aligned series.

**Raises:** `MqValueError` for unknown interpolation method.

---

### interpolate(x: pd.Series, dates: Union[List[dt.date], List[dt.time], pd.Series] = None, method: Interpolate = Interpolate.INTERSECT) -> pd.Series
Purpose: Interpolate series `x` over specified dates/times using the given method.

**Algorithm:**
1. Branch: if `dates` is `None` -> set `dates = x` (interpolate over own dates, effectively a no-op for INTERSECT).
2. Branch: if `dates` is a `pd.Series` -> use it directly as `align_series`.
3. Branch: else (list of dates/times) -> create `align_series = pd.Series(np.nan, dates)`.
4. Dispatch on `method`:
   - Branch: `INTERSECT` -> `x.align(align_series, 'inner')[0]`.
   - Branch: `NAN` -> `x.align(align_series, 'right')[0]` (requested dates only; NaN where `x` has no value).
   - Branch: `ZERO` -> create `align_series = pd.Series(0.0, dates)`, then `x.align(align_series, 'right', fill_value=0)[0]`.
   - Branch: `STEP` -> delegate to `__interpolate_step(x, align_series)`.
   - Branch: else -> raise `MqValueError('Unknown intersection type: ' + method)`.
5. Return the resulting single series.

**Raises:** `MqValueError` for unknown interpolation method.

---

### value(x: pd.Series, date: Union[dt.date, dt.time], method: Interpolate = Interpolate.STEP) -> pd.Series
Purpose: Return the value of series `x` at a single specified date or time, using the given interpolation method.

**Algorithm:**
1. Call `interpolate(x, [date], method)` to get a one-element series.
2. Branch: if the result is empty -> return `None`.
3. Branch: else -> return `values.iloc[0]` (the scalar value).

**Raises:** Nothing directly; delegates to `interpolate()` which may raise `MqValueError`.

---

### day(x: pd.Series) -> pd.Series
Purpose: Extract the day-of-month (1-31) for each observation in the series.

**Algorithm:**
1. Convert `x.index` to a `pd.Series`, then to `pd.Timestamp` via `pd.to_datetime`.
2. Extract `.dt.day` and cast to `np.int64`.
3. Return the resulting `pd.Series`.

---

### month(x: pd.Series) -> pd.Series
Purpose: Extract the month (1-12) for each observation in the series.

**Algorithm:**
1. Convert `x.index` to a `pd.Series`, then to `pd.Timestamp` via `pd.to_datetime`.
2. Extract `.dt.month` and cast to `np.int64`.
3. Return the resulting `pd.Series`.

---

### year(x: pd.Series) -> pd.Series
Purpose: Extract the year (e.g. 2019, 2020) for each observation in the series.

**Algorithm:**
1. Convert `x.index` to a `pd.Series`, then to `pd.Timestamp` via `pd.to_datetime`.
2. Extract `.dt.year` and cast to `np.int64`.
3. Return the resulting `pd.Series`.

---

### quarter(x: pd.Series) -> pd.Series
Purpose: Extract the quarter (1-4) for each observation in the series.

**Algorithm:**
1. Convert `x.index` to a `pd.Series`, then to `pd.Timestamp` via `pd.to_datetime`.
2. Extract `.dt.quarter` and cast to `np.int64`.
3. Return the resulting `pd.Series`.

---

### weekday(x: pd.Series) -> pd.Series
Purpose: Extract the weekday (0=Monday through 6=Sunday) for each observation in the series.

**Algorithm:**
1. Convert `x.index` to a `pd.Series`, then to `pd.Timestamp` via `pd.to_datetime`.
2. Extract `.dt.weekday` and cast to `np.int64`.
3. Return the resulting `pd.Series`.

---

### day_count_fractions(dates: Union[List[dt.date], pd.Series], convention: DayCountConvention = DayCountConvention.ACTUAL_360, frequency: PaymentFrequency = PaymentFrequency.MONTHLY) -> pd.Series
Purpose: Compute the day count fraction between consecutive dates using a financial day-count convention.

**Algorithm:**
1. Branch: if `dates` is a `pd.Series` -> extract `date_list = list(dates.index)`.
2. Branch: else -> `date_list = dates`.
3. Branch: if `len(date_list) < 2` -> return an empty `pd.Series(dtype=float)`.
4. Create `start_dates = date_list[0:-1]` and `end_dates = date_list[1:]`.
5. Map `day_count_fraction(a, b, convention, frequency)` pairwise over `(start_dates, end_dates)`.
6. Return `pd.Series(data=[np.nan] + list(dcfs), index=date_list)`. The first element is `NaN` because there is no preceding date for the first entry.

---

### date_range(x: pd.Series, start_date: Union[dt.date, int], end_date: Union[dt.date, int], weekdays_only: bool = False) -> pd.Series
Purpose: Slice a timeseries to a sub-range defined by start/end dates (or integer offsets from the boundaries), optionally filtering to weekdays only.

**Algorithm:**
1. Branch: if `weekdays_only` is not a `bool` -> raise `MqTypeError('expected a boolean value for "weekdays_only"')`.
2. Branch: if `x.index` is not `DatetimeIndex` and not all elements are `dt.date` -> raise `MqValueError('input is not a time series')`.
3. Branch: if `start_date` is `int` -> resolve `start_date = x.index[start_date]` (offset from beginning).
4. Branch: if `end_date` is `int` -> resolve `end_date = x.index[-(1 + end_date)]` (offset from end).
5. Try to convert `start_date` and `end_date` to plain `date` via `.date()`:
   - Branch: if `AttributeError` (already a `dt.date`) -> pass.
6. Branch: if `weekdays_only`:
   - Set `week_mask = None` (default Mon-Fri).
   - Compute weekday of `start_date`; if Saturday or Sunday (`wd > 4`), advance start to the next Monday: `start_date += timedelta(days=7 - wd)`.
7. Branch: else -> set `week_mask = (True, True, True, True, True, True, True)` (all seven days).
8. Generate `date_list` from `_date_range(start_date, end_date, week_mask=week_mask)`.
9. Branch: if `x.index` is `DatetimeIndex` -> convert each date to `pd.Timestamp`.
10. Return `x.loc[x.index.isin(date_list)]`.

**Raises:** `MqTypeError` when `weekdays_only` is not boolean. `MqValueError` when input index is not dates.

---

### append(series: List[pd.Series]) -> pd.Series
Purpose: Concatenate multiple timeseries sequentially; each successive series contributes only dates after the last date of the accumulated result.

**Algorithm:**
1. Branch: if `len(series) == 0` -> return an empty `pd.Series(dtype='float64')`.
2. Copy the first series into `res`.
3. For each subsequent series `cur` (index 1..n-1):
   - Determine `start = res.index[-1]`.
   - Concatenate `res` with the portion of `cur` where `cur.index > start`.
4. Return `res`.

---

### prepend(x: List[pd.Series]) -> pd.Series
Purpose: Prepend lower-quality/longer-history series in front of higher-quality/shorter-history series. Each series contributes only dates before the start of the next series.

**Algorithm:**
1. Branch: if `x` is empty -> return an empty `pd.Series(dtype='float64')`.
2. Branch: if `len(x) == 1` -> return `x[0].copy()`.
3. For each series `x[i]` (index 0..n-1):
   - Branch: if `i` is the last index -> append the entire series to `parts`.
   - Branch: else -> find `end = x[i+1].index[0]` and append only `this.loc[this.index < end]`.
4. Return `pd.concat(parts)`.

---

### union(x: List[pd.Series]) -> pd.Series
Purpose: Combine multiple series by filling in missing dates from successive series (first series has priority).

**Algorithm:**
1. Branch: if `len(x) > 0`:
   - Start with `res = pd.Series(dtype='float64', index=x[0].index)`.
   - For each `series` in `x`, call `res = res.combine_first(series)`.
2. Branch: else -> `res = pd.Series(dtype='float64')`.
3. Return `res`.

---

### bucketize(series: pd.Series, aggregate_function: AggregateFunction, period: AggregatePeriod) -> pd.Series
Purpose: Resample a timeseries into fixed-period buckets and apply an aggregation function to each bucket.

**Algorithm:**
1. Convert `series.index` to `DatetimeIndex` via `pd.to_datetime` (in-place mutation of the input series).
2. Extract the first character of the period's value and uppercase it to get `period_char` (e.g. `'week'` -> `'W'`).
3. Map `period_char` to a pandas frequency string via `frequency_map`:
   - `'W'` -> `'W'`
   - `'M'` -> `FREQ_MONTH_END` (version-dependent: `'ME'` or `'M'`)
   - `'Q'` -> `FREQ_QUARTER_END` (version-dependent: `'QE'` or `'Q'`)
   - `'Y'` -> `FREQ_YEAR_END` (version-dependent: `'YE'` or `'Y'`)
4. Extract `agg = aggregate_function.value` (the string name, e.g. `'mean'`).
5. Resample `series` at the determined frequency and apply `agg`.
6. Branch: if result is empty -> return it immediately.
7. Otherwise, adjust the last index timestamp to be `min(series.index[-1], result.index[-1])` so the final bucket's label reflects actual data range:
   - Build a new index from `result.index[:-1]` plus the adjusted last timestamp.
   - Replace `result.index` with the new index.
8. Return `result`.

---

### day_count(first: dt.date, second: dt.date) -> int
Purpose: Count the number of business days (Mon-Fri) between two dates.

**Algorithm:**
1. Branch: if either `first` or `second` is not `dt.date` -> raise `MqValueError('inputs must be dates')`.
2. Return `np.busday_count(first, second)`.

**Raises:** `MqValueError` when inputs are not dates.

---

### day_countdown(end_date: dt.date, start_date: dt.date = None, business_days: bool = False) -> pd.Series
Purpose: Generate a series counting down the number of days from each date to `end_date`.

**Algorithm:**
1. Branch: if `start_date is None` -> default to `dt.date.today()`.
2. Branch: if `start_date` is not `dt.date` -> raise `MqValueError('start_date must be a date')`.
3. Branch: if `end_date` is not `dt.date` -> raise `MqValueError('end_date must be a date')`.
4. Branch: if `start_date > end_date` -> return empty `pd.Series(dtype=np.int64)`.
5. Branch: if `business_days`:
   - Generate `idx = pd.bdate_range(start=start_date, end=end_date)` (business days only).
   - Branch: if `idx` is empty -> return empty `pd.Series(dtype=np.int64)`.
   - Convert index and `end_date` to `datetime64[D]`.
   - Compute `values = np.busday_count(start_days, end_day)` cast to `np.int64`.
6. Branch: else (calendar days):
   - Generate `idx = pd.date_range(start=start_date, end=end_date, freq='D')`.
   - Compute `values = (end_day - start_days)` cast to `np.int64`.
7. Return `pd.Series(values, index=idx)`.

**Raises:** `MqValueError` when `start_date` or `end_date` is not a `dt.date`.

---

### align_calendar(series: pd.Series, calendar: str) -> pd.Series
Purpose: Filter a timeseries to retain only dates that fall on business days according to the given GS calendar (removing holidays and weekends).

**Decorator:** `@requires_session` (requires an active GS API session).

**Algorithm:**
1. Fetch the calendar via `GsCalendar.get(calendar)`.
2. Create a `CustomBusinessDay` offset backed by the calendar's `business_day_calendar()`.
3. Generate all business dates from the series's first valid index to its last valid index at the custom business day frequency.
4. Filter `series` to only include dates present in that generated business-day range.
5. Return the filtered series.

## State Mutation
- `bucketize()`: Mutates the input `series.index` in-place by calling `series.index = pd.to_datetime(series.index)`. Callers passing a series with a non-DatetimeIndex will see their original series's index permanently changed.
- No global state is modified.
- No thread-safety concerns beyond the shared-nothing pandas Series pattern.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `__interpolate_step` | Input series `x` is empty |
| `MqValueError` | `align` | Unknown `method` not in the Interpolate enum |
| `MqValueError` | `interpolate` | Unknown `method` not in the Interpolate enum |
| `MqValueError` | `date_range` | Input index is not a timeseries (not DatetimeIndex and not all dt.date) |
| `MqTypeError` | `date_range` | `weekdays_only` parameter is not a bool |
| `MqValueError` | `day_count` | Either `first` or `second` is not a `dt.date` |
| `MqValueError` | `day_countdown` | `start_date` is not a `dt.date` |
| `MqValueError` | `day_countdown` | `end_date` is not a `dt.date` |

## Edge Cases
- `align()` with two `Real` scalars: returns them unchanged as a list; no pandas operations performed.
- `align()` with one `Real` and one `pd.Series`: the scalar is broadcast across the series's index.
- `interpolate()` with `dates=None`: defaults to using `x` itself as both data and target dates.
- `interpolate()` with `method=ZERO` rebuilds `align_series` as `pd.Series(0.0, dates)` regardless of the previously constructed NaN-based align_series, to ensure zero-fill semantics.
- `value()` returns `None` (not NaN) when the interpolated result is empty.
- `day_count_fractions()` with fewer than 2 dates: returns an empty series (no fractions possible).
- `day_count_fractions()` first element is always `NaN` because no preceding date pair exists.
- `date_range()` with integer `start_date`/`end_date`: interprets as offsets from beginning/end of the series respectively.
- `date_range()` with `weekdays_only=True` and `start_date` on Saturday/Sunday: advances start to the next Monday.
- `append()` and `prepend()` with empty list: return an empty float64 series.
- `prepend()` with a single-element list: returns a copy of that series.
- `union()` with empty list: returns an empty float64 series.
- `union()` starts with an all-NaN series (matching first series index) so that `combine_first` fills from each successive series in priority order.
- `bucketize()` with an empty resample result: returns immediately without adjusting index.
- `bucketize()` last bucket index is clamped to `min(series.index[-1], result.index[-1])` to avoid a label beyond actual data.
- `day_countdown()` with `start_date > end_date`: returns an empty int64 series.
- `day_countdown()` with `business_days=True` and resulting empty bdate_range: returns an empty int64 series.
- `align_calendar()` requires an active session (`@requires_session`); will fail if called without one.

## Bugs Found
- Line 683 (`bucketize`): `series.index = pd.to_datetime(series.index)` mutates the caller's series index in place. This is a side effect that violates the expectation that timeseries functions return new data without modifying inputs. (OPEN)
- Line 57 (`__interpolate_step`): When `first_date >= x.index[0]`, the `get_indexer` call with `method='pad'` returns an array of integer positions, which is then used to index `x.index`. The result is an `Index` object (not a single value), and `x[prev]` returns a sub-series rather than a scalar. This works incidentally because the subsequent iteration treats `current` as the last valid scalar through the loop, but the types are loosely handled. (OPEN)

## Coverage Notes
- Branch count: 46 (across all functions)
- Key branches: `align` has 8 branches (3 scalar checks + 5 method dispatches + 1 else); `interpolate` has 7 branches (1 dates-None + 1 isinstance + 4 method dispatches + 1 else); `date_range` has 8 branches (1 bool check + 1 index check + 2 int checks + 1 try/except + 1 weekdays_only + 1 DatetimeIndex check + filtering); `__interpolate_step` has 5 branches; `day_countdown` has 7 branches; `day_count_fractions` has 3 branches; `append` has 2 branches; `prepend` has 3 branches; `union` has 2 branches; `bucketize` has 2 branches.
- Pragmas: None marked.
