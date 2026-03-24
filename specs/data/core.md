# data/core.py

## Summary
Defines fundamental data-related types used throughout the gs_quant library: the `DataFrequency` and `IntervalFrequency` enums for time-frequency classification, the `DataAggregationOperator` constants class, and the `DataContext` context manager that carries start/end/interval parameters for data queries. `DataContext` inherits from `ContextBaseWithDefault`, making it a thread-local, nestable context with an auto-constructed default instance.

## Dependencies
- Internal: `gs_quant.context_base` (`ContextBaseWithDefault`)
- Internal: `gs_quant.errors` (`MqTypeError`, `MqValueError`)
- External: `datetime` (date, datetime, time, timedelta, timezone), `re` (fullmatch), `enum` (Enum)

## Type Definitions

### DataFrequency (Enum)
Inherits: `enum.Enum`

Enumeration of data subscription frequencies.

| Member | Value (str) | Description |
|--------|-------------|-------------|
| DAILY | `"daily"` | Series updating daily |
| REAL_TIME | `"realTime"` | Real-time or intraday series |
| ANY | `"any"` | Either real-time or daily |

### DataAggregationOperator (class -- plain class, NOT Enum)
Inherits: `object`

A namespace of string constants for aggregation operations. Note: this is a plain class, not an Enum. Members are bare class attributes.

| Attribute | Type | Value | Description |
|-----------|------|-------|-------------|
| MIN | `str` | `"min"` | Minimum aggregation |
| MAX | `str` | `"max"` | Maximum aggregation |
| FIRST | `str` | `"first"` | First-value aggregation |
| LAST | `str` | `"last"` | Last-value aggregation |

### IntervalFrequency (Enum)
Inherits: `enum.Enum`

| Member | Value (str) | Description |
|--------|-------------|-------------|
| DAILY | `"daily"` | Daily interval |
| WEEKLY | `"weekly"` | Weekly interval |
| MONTHLY | `"monthly"` | Monthly interval |
| YEARLY | `"yearly"` | Yearly interval |

### DataContext (class)
Inherits: `ContextBaseWithDefault` -> `ContextBase` (metaclass `ContextMeta`)

A thread-local, nestable context manager that holds date/time range parameters for data queries. Because it inherits `ContextBaseWithDefault`, calling `DataContext.current` when no context is active auto-constructs a default `DataContext()` (with all-`None` fields).

| Field | Type | Default | Visibility | Description |
|-------|------|---------|------------|-------------|
| `__start` | `Union[None, dt.date, dt.datetime, str]` | `None` | Private (name-mangled) | Start of the date/time range |
| `__end` | `Union[None, dt.date, dt.datetime, str]` | `None` | Private (name-mangled) | End of the date/time range |
| `__interval` | `Optional[str]` | `None` | Private (name-mangled) | Interval string matching pattern `[1-9]\d{0,2}[a-z]` (e.g. `"1m"`, `"2h"`, `"3d"`) |

**Properties exposed (read-only):**

| Property | Return Type | Description |
|----------|-------------|-------------|
| `start_date` | `dt.date` | Start as a date; defaults to today minus 30 days |
| `end_date` | `dt.date` | End as a date; defaults to today |
| `start_time` | `dt.datetime` | Start as a UTC datetime; defaults to now minus 1 day |
| `end_time` | `dt.datetime` | End as a UTC datetime; defaults to now |
| `interval` | `Optional[str]` | The interval string, or None |

## Enums and Constants

See `DataFrequency`, `DataAggregationOperator`, and `IntervalFrequency` in Type Definitions above.

### Module-level function used as helper
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_now` | `Callable[[], dt.datetime]` | Returns `dt.datetime.now(dt.timezone.utc)` | UTC-aware "now" helper; used by `start_time` / `end_time` defaults |

## Functions/Methods

### _now() -> dt.datetime
Purpose: Returns the current UTC-aware datetime.

**Algorithm:**
1. Return `dt.datetime.now(dt.timezone.utc)`.

No branches.

---

### DataContext.__init__(self, start=None, end=None, interval=None) -> None
Purpose: Initialize data context with optional start, end, and interval. Validates the interval format.

**Algorithm:**
1. Call `super().__init__()` (ContextBaseWithDefault).
2. Store `start` into `self.__start`.
3. Store `end` into `self.__end`.
4. Branch: `interval is None` -> set `self.__interval = None` and return early.
5. Branch: `not isinstance(interval, str)` -> raise `MqTypeError('interval must be a str')`.
6. Branch: regex `re.fullmatch('[1-9]\\d{0,2}[a-z]', interval)` fails -> raise `MqValueError('interval must be a valid str e.g. 1m, 2h, 3d')`.
7. Store `interval` into `self.__interval`.

**Raises:**
- `MqTypeError` when `interval` is not None and not a string.
- `MqValueError` when `interval` is a string but does not match the pattern `[1-9]\d{0,2}[a-z]`.

---

### DataContext._get_date(o, default) -> dt.date  [staticmethod]
Purpose: Convert a value to `dt.date`, falling back to `default` when value is None.

**Algorithm:**
1. Branch: `o is None` -> return `default`.
2. Branch: `isinstance(o, dt.datetime)` -> return `o.date()`. (Note: `datetime` is a subclass of `date`, so this branch is checked first.)
3. Branch: `isinstance(o, dt.date)` -> return `o` as-is.
4. Branch: `isinstance(o, str)` ->
   a. Find `'T'` in the string (`o.find('T')`).
   b. Sub-branch: `loc != -1` -> slice to `o[:loc]`; else use full string.
   c. Parse date string with `strptime(ds, '%Y-%m-%d')` and return `.date()`.
5. Else -> raise `ValueError(f'{o} is not a valid date')`.

**Raises:** `ValueError` when `o` is not None/datetime/date/str.

---

### DataContext._get_datetime(o, default) -> dt.datetime  [staticmethod]
Purpose: Convert a value to `dt.datetime`, falling back to `default` when value is None.

**Algorithm:**
1. Branch: `o is None` -> return `default`.
2. Branch: `isinstance(o, dt.datetime)` -> return `o` as-is.
3. Branch: `isinstance(o, dt.date)` -> return `dt.datetime.combine(o, dt.time(tzinfo=dt.timezone.utc))`.
4. Branch: `isinstance(o, str)` -> parse with `strptime(o, '%Y-%m-%dT%H:%M:%SZ')`, replace tzinfo with `dt.timezone.utc`, return.
5. Else -> raise `ValueError(f'{o} is not a valid date')`.

**Raises:** `ValueError` when `o` is not None/datetime/date/str.

---

### DataContext.start_date (property) -> dt.date
Purpose: Return start as a date, defaulting to 30 days ago.

**Algorithm:**
1. Delegate to `self._get_date(self.__start, dt.date.today() - dt.timedelta(days=30))`.

---

### DataContext.end_date (property) -> dt.date
Purpose: Return end as a date, defaulting to today.

**Algorithm:**
1. Delegate to `self._get_date(self.__end, dt.date.today())`.

---

### DataContext.start_time (property) -> dt.datetime
Purpose: Return start as a UTC datetime, defaulting to 1 day ago.

**Algorithm:**
1. Delegate to `self._get_datetime(self.__start, _now() - dt.timedelta(days=1))`.

---

### DataContext.end_time (property) -> dt.datetime
Purpose: Return end as a UTC datetime, defaulting to now.

**Algorithm:**
1. Delegate to `self._get_datetime(self.__end, _now())`.

---

### DataContext.interval (property) -> Optional[str]
Purpose: Return the interval string.

**Algorithm:**
1. Return `self.__interval`.

## State Mutation
- `self.__start`, `self.__end`, `self.__interval`: Set during `__init__` only; never modified after construction.
- Thread-local context stack: Managed by inherited `ContextBase.__enter__` / `__exit__` which call `ContextMeta.push` / `pop`. The context stack is stored on `threading.local()` under keys derived from the class name.
- Thread safety: Each thread gets its own context stack via `threading.local()`. Safe for concurrent use across threads; not designed for async concurrency within a single thread.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqTypeError` | `__init__` | `interval` is not None and not a `str` |
| `MqValueError` | `__init__` | `interval` is a `str` but does not match `[1-9]\d{0,2}[a-z]` |
| `ValueError` | `_get_date` | `o` is not None, datetime, date, or str |
| `ValueError` | `_get_datetime` | `o` is not None, datetime, date, or str |

## Edge Cases
- `_get_date` with a `datetime` input: Because `datetime` is a subclass of `date`, the `isinstance(o, dt.datetime)` check must come before `isinstance(o, dt.date)`. The code handles this correctly.
- `_get_date` with a string containing `'T'`: Strips the time portion before parsing. A string without `'T'` is parsed as-is.
- `_get_datetime` with a string: Expects the exact format `'%Y-%m-%dT%H:%M:%SZ'`. Other formats (e.g., no timezone, different separators) will raise `ValueError` from `strptime`.
- Default `DataContext()` (no arguments): All fields are `None`, so property accessors compute dynamic defaults based on `dt.date.today()` / `_now()`.
- `interval` validation regex: Allows 1-3 digit number (first digit non-zero) followed by exactly one lowercase letter. Examples of valid: `"1m"`, `"12h"`, `"999d"`. Invalid: `"0m"`, `"1000d"`, `"1M"`, `"1"`, `""`.
- The `if __name__ == '__main__'` block at the end is demo/test code; not part of the module's public API.

## Coverage Notes
- Branch count: 14
  - `__init__`: 4 branches (interval is None / not str / invalid regex / valid)
  - `_get_date`: 5 branches (None / datetime / date / str with T / str without T / else)
  - `_get_datetime`: 5 branches (None / datetime / date / str / else)
- The `if __name__ == '__main__'` block (lines 126-132) is only executed when run as a script; typically excluded from coverage.
- The `_get_date` str branch has an internal sub-branch on whether `'T'` is found in the string (loc == -1 vs loc != -1).
