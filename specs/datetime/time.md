# time.py

## Summary
Provides time-related constants (durations expressed in seconds, minutes, hours, etc.), a context-manager `Timer` class for measuring and logging execution duration, and two utility functions for datetime formatting: `to_zulu_string` (ISO 8601 Zulu format) and `time_difference_as_string` (human-readable elapsed-time string).

## Dependencies
- Internal: `gs_quant.errors` (MqValueError)
- External: `datetime` (datetime, now), `logging`, `numpy` (timedelta64)

## Type Definitions

### Timer (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__print_on_exit` | `bool` | `True` | Whether to log elapsed time on context exit |
| `__label` | `str` | `'Execution'` | Label prefix for the log message |
| `__threshold` | `int` | `None` | Minimum elapsed seconds required before logging (None means always log) |
| `__start` | `dt.datetime` | *(set on __enter__)* | Timestamp captured when entering the context |
| `__elapsed` | `dt.timedelta` | *(set on __exit__)* | Computed elapsed duration |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `DAYS_IN_YEAR` | `float` | `365.25` | Average days per year (accounts for leap years) |
| `DAYS_IN_WEEK` | `int` | `7` | Days per week |
| `HOURS_IN_DAY` | `int` | `24` | Hours per day |
| `MINS_IN_HOUR` | `int` | `60` | Minutes per hour |
| `SECS_IN_MIN` | `int` | `60` | Seconds per minute |
| `SECS_IN_HOUR` | `int` | `3600` | Computed: `SECS_IN_MIN * MINS_IN_HOUR` |
| `MINS_IN_DAY` | `int` | `1440` | Computed: `MINS_IN_HOUR * HOURS_IN_DAY` |
| `SECS_IN_DAY` | `int` | `86400` | Computed: `SECS_IN_MIN * MINS_IN_DAY` |
| `HOURS_IN_WEEK` | `int` | `168` | Computed: `HOURS_IN_DAY * DAYS_IN_WEEK` |
| `MINS_IN_WEEK` | `int` | `10080` | Computed: `HOURS_IN_WEEK * MINS_IN_HOUR` |
| `SECS_IN_WEEK` | `int` | `36288000` | Computed: `MINS_IN_WEEK * SECS_IN_HOUR` (Note: this equals 10080 * 3600 = 36,288,000, not 604,800 which is the actual seconds in a week) |
| `SECS_IN_YEAR` | `float` | `31557600.0` | Computed: `SECS_IN_MIN * MINS_IN_HOUR * HOURS_IN_DAY * DAYS_IN_YEAR` |

## Functions/Methods

### Timer.__init__(self, print_on_exit: bool = True, label: str = 'Execution', threshold: int = None)
Purpose: Configure the timer with logging preferences.

**Algorithm:**
1. Store `self.__print_on_exit = print_on_exit`.
2. Store `self.__label = label`.
3. Store `self.__threshold = threshold`.

### Timer.__enter__(self)
Purpose: Record the start time when entering the `with` block.

**Algorithm:**
1. Set `self.__start = dt.datetime.now()`.
2. Returns `None` (no explicit return).

### Timer.__exit__(self, *args)
Purpose: Compute elapsed time and optionally log it.

**Algorithm:**
1. Compute `self.__elapsed = dt.datetime.now() - self.__start`.
2. Branch: `self.__print_on_exit` is True:
   - Branch: `self.__threshold is None` OR `self.__elapsed.seconds > self.__threshold`:
     - Log warning: `'{label} took {seconds + microseconds/1000000} seconds'`.
   - Else (threshold set and elapsed <= threshold): do nothing.
3. Else (`print_on_exit` is False): do nothing.

### to_zulu_string(time: dt.datetime) -> str
Purpose: Convert a datetime to ISO 8601 Zulu-time string (truncating microseconds to milliseconds).

**Algorithm:**
1. Call `time.isoformat()` which yields e.g. `'2024-01-15T10:30:00.123456'`.
2. Slice off last 3 characters (`[:-3]`), removing the last 3 digits of microseconds.
3. Append `'Z'`.
4. Return the result.

### time_difference_as_string(time_delta: np.timedelta64, resolution: str = 'Second') -> str
Purpose: Convert a numpy timedelta to a human-readable string like `"2 Hours 30 Minutes"`, stopping at the specified resolution.

**Algorithm:**
1. Define `times = [SECS_IN_YEAR, SECS_IN_WEEK, SECS_IN_DAY, SECS_IN_HOUR, SECS_IN_MIN, 1]`.
2. Define `time_strings = ['Year', 'Week', 'Day', 'Hour', 'Minute', 'Second']`.
3. Branch: `resolution not in time_strings` -> raise `MqValueError('incorrect resolution passed in "s"' % resolution)`. **Note:** The format string uses `"s"` with `%` operator but `resolution` is not substituted because `"s"` has no `%s` placeholder; the literal string `'incorrect resolution passed in "s"'` is always produced.
4. Zip `times` and `time_strings` into `times_mapped`.
5. Compute `diff = abs(time_delta / np.timedelta64(1, 's'))` to get total seconds as int.
6. Initialize `result = ''`.
7. For each `(time, time_string)` in `times_mapped`:
   - Compute `m = diff // time` (integer division).
   - Branch: `m > 0`:
     - Set `added = time_string`.
     - Branch: `m != 1` -> append `'s'` to `added` (pluralize).
     - Append `'{m} {added} '` to `result`.
     - Subtract `m * time` from `diff`.
   - Branch: `time_string == resolution` -> break out of loop.
8. Return `result.strip()`.

**Raises:** `MqValueError` when `resolution` is not one of `['Year', 'Week', 'Day', 'Hour', 'Minute', 'Second']`.

## State Mutation
- `self.__start`: Set on `__enter__`.
- `self.__elapsed`: Set on `__exit__`.
- No module-level mutable state beyond the logger.
- Thread safety: `Timer` instances are not thread-safe; each should be used in a single thread.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `time_difference_as_string` | When `resolution` is not a valid time unit string |

## Edge Cases
- `to_zulu_string` with a datetime that has no microseconds (e.g., `datetime(2024, 1, 1, 0, 0, 0)`): `isoformat()` returns `'2024-01-01T00:00:00'`, and `[:-3]` chops the last 3 chars of the seconds, producing `'2024-01-01T00:0Z'` -- malformed output. This is a latent bug when microseconds are zero.
- `to_zulu_string` with timezone-aware datetimes: `isoformat()` appends timezone info (e.g., `+00:00`), and the `[:-3] + 'Z'` slicing would corrupt the output.
- `time_difference_as_string` with `resolution='Year'`: only the Year component is computed; all remaining time is discarded.
- `time_difference_as_string` with a zero timedelta: `diff` is 0, no `m > 0` branch triggers, returns `''` (empty string).
- `time_difference_as_string` error message format bug: `'incorrect resolution passed in "s"' % resolution` -- the `%` operator treats the string as a format string, but `"s"` contains no format specifier, so `resolution` is ignored. The error message will not include the invalid resolution value.
- `SECS_IN_WEEK` is computed as `MINS_IN_WEEK * SECS_IN_HOUR` = 10080 * 3600 = 36,288,000. The actual seconds in a week is 604,800 (= 7 * 86400). The formula should be `SECS_IN_DAY * DAYS_IN_WEEK` or `MINS_IN_WEEK * SECS_IN_MIN`. This is a computation bug: `SECS_IN_WEEK` is 60x too large.
- `Timer.__exit__` uses `self.__elapsed.seconds` which is the `.seconds` component of a `timedelta`, not total seconds. For durations over 1 day, `.seconds` resets (e.g., 1 day 30 seconds has `.seconds == 30`). This means the threshold comparison is only against the sub-day seconds component. Also, `.seconds` is always >= 0, so negative thresholds always trigger logging.
- `Timer.__enter__` returns `None`, so `with Timer() as t:` binds `t = None`.

## Bugs Found
- Line 38: `SECS_IN_WEEK = MINS_IN_WEEK * SECS_IN_HOUR` computes 36,288,000 instead of 604,800. The multiplication should use `SECS_IN_MIN` instead of `SECS_IN_HOUR`. (OPEN)
- Line 71: `'incorrect resolution passed in "s"' % resolution` -- the `%` operator does not substitute `resolution` because the format string has no `%s` placeholder. The `"s"` in the string is a literal, not a format specifier. Should be `'incorrect resolution passed in "%s"' % resolution`. (OPEN)
- Line 63: `to_zulu_string` will produce malformed output when `time.isoformat()` does not include microseconds (6 digits), since it unconditionally slices off the last 3 characters. (OPEN)

## Coverage Notes
- Branch count: ~10
- Key branches: `Timer.__exit__` has 3 branches (print_on_exit false, threshold is None, elapsed > threshold). `time_difference_as_string` has 4 branches (invalid resolution, m > 0, m != 1, time_string == resolution). `to_zulu_string` is branchless.
- Pragmas: none observed.
