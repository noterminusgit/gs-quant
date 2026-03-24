# date.py

## Summary
Provides business day calendar utilities (offset, count, range, previous business day), day count fraction calculations for financial instruments, a `today()` helper with timezone awareness, and a leap-day detection helper. All business-day functions delegate to `numpy` bus-day routines backed by `GsCalendar`.

## Dependencies
- Internal: `gs_quant.common` (`PricingLocation`), `gs_quant.datetime.gscalendar` (`GsCalendar`)
- External: `calendar` (stdlib, aliased `cal`), `datetime` (stdlib, aliased `dt`), `zoneinfo` (stdlib), `enum` (`Enum`, `IntEnum`), `typing` (`Iterable`, `Optional`, `Tuple`, `Union`), `numpy` (`np`)

## Type Definitions

### TypeAlias
```
DateOrDates = Union[dt.date, Iterable[dt.date]]
```
Used as the parameter/return type for most business-day functions. When a single `dt.date` is passed the functions return a scalar; when an iterable is passed they return a `tuple`.

## Enums and Constants

### PaymentFrequency(IntEnum)
Represents the number of payment periods per year. Used by `day_count_fraction` for ACTUAL_365L convention.

| Value | Raw | Description |
|-------|-----|-------------|
| DAILY | `252` | Daily (trading days) |
| WEEKLY | `52` | Weekly |
| SEMI_MONTHLY | `26` | Twice a month |
| MONTHLY | `12` | Monthly |
| SEMI_QUARTERLY | `6` | Twice a quarter |
| QUARTERLY | `4` | Quarterly |
| TRI_ANNUALLY | `3` | Three times a year |
| SEMI_ANNUALLY | `2` | Twice a year |
| ANNUALLY | `1` | Once a year |

### DayCountConvention(Enum)
Day count conventions that determine how interest accrues over payment periods.

| Value | Raw | Description |
|-------|-----|-------------|
| ACTUAL_360 | `"ACTUAL_360"` | Actual days / 360 |
| ACTUAL_364 | `"ACTUAL_364"` | Actual days / 364 |
| ACTUAL_365_25 | `"ACTUAL_365_25"` | Actual days / 365.25 |
| ACTUAL_365F | `"ACTUAL_365F"` | Actual days / 365 (fixed) |
| ACTUAL_365L | `"ACTUAL_365L"` | Actual days / 365 or 366 (leap year aware) |
| ONE_ONE | `"ONE_ONE"` | Always returns 1 |

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `location_to_tz_mapping` | `dict[PricingLocation, ZoneInfo]` | See below | Maps pricing locations to IANA timezones |

```python
location_to_tz_mapping = {
    PricingLocation.NYC: ZoneInfo("America/New_York"),
    PricingLocation.LDN: ZoneInfo("Europe/London"),
    PricingLocation.HKG: ZoneInfo("Asia/Hong_Kong"),
    PricingLocation.TKO: ZoneInfo("Asia/Tokyo"),
}
```

## Functions/Methods

### is_business_day(dates: DateOrDates, calendars: Union[str, Tuple[str, ...]] = (), week_mask: Optional[str] = None) -> Union[bool, Tuple[bool, ...]]
Purpose: Determine whether each date is a business day.

**Algorithm:**
1. Fetch `GsCalendar` via `GsCalendar.get(calendars)`.
2. Call `np.is_busday(dates, busdaycal=calendar.business_day_calendar(week_mask))`.
3. Branch: if result is `np.ndarray` -> return `tuple(res)`.
4. Branch: else (scalar `np.bool_`) -> return `res` directly (truthy bool).

**Raises:** Nothing explicitly. `GsCalendar.get` may raise on invalid calendars. `np.is_busday` may raise on invalid dates.

---

### business_day_offset(dates: DateOrDates, offsets: Union[int, Iterable[int]], roll: str = 'raise', calendars: Union[str, Tuple[str, ...]] = (), week_mask: Optional[str] = None) -> DateOrDates
Purpose: Apply business-day offsets to dates and roll to nearest business day.

**Algorithm:**
1. Fetch `GsCalendar` via `GsCalendar.get(calendars)`.
2. Call `np.busday_offset(dates, offsets, roll, busdaycal=calendar.business_day_calendar(week_mask))`.
3. Cast result `.astype(dt.date)`.
4. Branch: if result is `np.ndarray` -> return `tuple(res)`.
5. Branch: else (scalar) -> return `res` directly.

**Parameters - `roll`:**
- `'raise'` (default): raise on non-business day inputs
- `'forward'`: roll forward to next business day
- `'backward'` / `'preceding'`: roll backward to previous business day
- `'modifiedfollowing'`: roll forward, but if that crosses month boundary, roll backward
- `'modifiedpreceding'`: roll backward, but if that crosses month boundary, roll forward

---

### prev_business_date(dates: DateOrDates = dt.date.today(), calendars: Union[str, Tuple[str, ...]] = (), week_mask: Optional[str] = None) -> DateOrDates
Purpose: Return the previous business date relative to the given date(s).

**Algorithm:**
1. Delegate to `business_day_offset(dates, -1, roll='forward', calendars=calendars, week_mask=week_mask)`.

**Note:** Default `dates` is evaluated once at module import time (`dt.date.today()`). This is a known Python mutable-default-argument pattern -- the default is evaluated at function definition time, not at call time.

---

### business_day_count(begin_dates: DateOrDates, end_dates: DateOrDates, calendars: Union[str, Tuple[str, ...]] = (), week_mask: Optional[str] = None) -> Union[int, Tuple[int, ...]]
Purpose: Count business days between begin and end dates.

**Algorithm:**
1. Fetch `GsCalendar` via `GsCalendar.get(calendars)`.
2. Call `np.busday_count(begin_dates, end_dates, busdaycal=calendar.business_day_calendar(week_mask))`.
3. Branch: if result is `np.ndarray` -> return `tuple(res)`.
4. Branch: else (scalar) -> return `res` directly.

**Note:** `np.busday_count` counts the number of valid business days in the half-open interval `[begin, end)`.

---

### date_range(begin: Union[int, dt.date], end: Union[int, dt.date], calendars: Union[str, Tuple[str, ...]] = (), week_mask: Optional[str] = None) -> Iterable[dt.date]
Purpose: Construct a range (generator) of business dates.

**Algorithm:**
1. Branch: `begin` is `dt.date`:
   a. Branch: `end` is `dt.date`:
      - Define inner generator `f()`.
      - If `begin > end` -> raise `ValueError('begin must be <= end')`.
      - Yield `begin`, then repeatedly call `business_day_offset(prev, 1, ...)` until `prev > end`.
      - Return a generator wrapping `f()`.
   b. Branch: `end` is `int`:
      - Return generator expression: `(business_day_offset(begin, i, ...) for i in range(end))`.
   c. Branch: else -> raise `ValueError('end must be a date or int')`.
2. Branch: `begin` is `int`:
   a. Branch: `end` is `dt.date`:
      - Return generator expression: `(business_day_offset(end, -i, roll='preceding', ...) for i in range(begin))`.
      - Note: dates are yielded in reverse chronological order (end, end-1, end-2, ...).
   b. Branch: else -> raise `ValueError('end must be a date if begin is an int')`.
3. Branch: else -> raise `ValueError('begin must be a date or int')`.

**Raises:**
- `ValueError` when `begin > end` (both dates)
- `ValueError` when types are invalid

---

### today(location: Optional[PricingLocation] = None) -> dt.date
Purpose: Return today's date, optionally in a specific timezone.

**Algorithm:**
1. Branch: `location` is falsy (None) -> return `dt.date.today()`.
2. Look up `tz` from `location_to_tz_mapping.get(location, None)`.
3. Branch: `tz is None` -> raise `ValueError(f'Unrecognized timezone {location}')`.
4. Return `dt.datetime.now(tz).date()`.

**Raises:** `ValueError` when location is not in `location_to_tz_mapping`.

---

### has_feb_29(start: dt.date, end: dt.date) -> bool
Purpose: Determine if the date range (start exclusive, end inclusive) contains a leap day (Feb 29).

**Algorithm:**
1. Initialize `feb_29 = False`.
2. Loop `x` from 1 to `(end - start).days` inclusive:
   a. Compute `date = start + timedelta(days=x)`.
   b. Bitwise-OR `feb_29` with `(date.month == 2 and date.day == 29)`.
3. Return `feb_29`.

**Note:** Start date is exclusive; end date is inclusive. If `start >= end`, the range is empty and returns `False`. Uses bitwise OR (`|`) rather than logical OR -- both work identically for booleans, but the `|` prevents short-circuit evaluation.

**Elixir porting note:** This is an O(n) scan over every day in the range. An Elixir port could optimize by checking if any year in the range is a leap year and whether Feb 29 of that year falls in `(start, end]`.

---

### day_count_fraction(start: dt.date, end: dt.date, convention: DayCountConvention = DayCountConvention.ACTUAL_360, frequency: PaymentFrequency = PaymentFrequency.MONTHLY) -> Union[float, int]
Purpose: Compute the day count fraction between two dates per a given convention.

**Algorithm:**
1. Compute `delta = (end - start).days`.
2. Branch by `convention`:
   a. `ACTUAL_360` -> return `delta / 360`.
   b. `ACTUAL_364` -> return `delta / 364`.
   c. `ACTUAL_365F` -> return `delta / 365`.
   d. `ACTUAL_365L`:
      - Sub-branch: `frequency == PaymentFrequency.ANNUALLY`:
        - `days_in_year = 366` if `has_feb_29(start, end)` else `365`.
      - Sub-branch: else (any other frequency):
        - `days_in_year = 366` if `cal.isleap(end.year)` else `365`.
      - Return `delta / days_in_year`.
   e. `ACTUAL_365_25` -> return `delta / 365.25`.
   f. `ONE_ONE` -> return `1` (integer, not float).
   g. else -> raise `ValueError('Unknown day count convention: ' + convention.value)`.

**Raises:** `ValueError` for unknown convention (should be unreachable given the enum, but guards against future enum additions).

## State Mutation
- `location_to_tz_mapping`: Module-level dict, never mutated at runtime.
- No mutable global state. All functions are pure (given calendar state).
- Thread safety: `GsCalendar.get()` may have internal caching; `np` functions are thread-safe for read operations.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `date_range` | `begin > end` when both are dates |
| `ValueError` | `date_range` | Invalid type for `begin` or `end` |
| `ValueError` | `today` | `location` not in `location_to_tz_mapping` |
| `ValueError` | `day_count_fraction` | Unknown `DayCountConvention` value |

## Edge Cases
- `prev_business_date` default argument `dates=dt.date.today()` is evaluated at import time, not call time. In long-running processes, calling `prev_business_date()` without arguments may return stale results.
- `has_feb_29` returns `False` when `start >= end` (empty range).
- `date_range(0, some_date)` returns an empty generator (range(0) is empty).
- `date_range(some_date, 0)` returns an empty generator (range(0) is empty).
- `business_day_offset` with `roll='raise'` will raise `ValueError` if the input date is not a business day.
- `day_count_fraction` with `start == end` returns `0` for all conventions except `ONE_ONE` (returns `1`).
- `is_business_day` / `business_day_count` / `business_day_offset` accept both single dates and iterables, polymorphically returning scalar or tuple.

## Coverage Notes
- Branch count: ~20
- Key branches: `isinstance` checks on `np.ndarray` in `is_business_day`/`business_day_offset`/`business_day_count` (2 branches each), `date_range` type dispatch (6 branches), `today` location checks (3 branches), `day_count_fraction` convention dispatch (7 branches), `has_feb_29` loop body condition (2 branches), `ACTUAL_365L` sub-branch on frequency (2 branches).
- All enum members of `DayCountConvention` have explicit branches. The `else` branch in `day_count_fraction` is technically unreachable with the current enum but should be tested.
