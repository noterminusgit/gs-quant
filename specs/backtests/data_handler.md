# data_handler.py

## Summary
Provides time-tracking (`Clock`) and data-access (`DataHandler`) classes for backtesting. `Clock` enforces monotonically increasing time to prevent lookahead bias. `DataHandler` wraps a `DataManager`, converting timezone-naive datetimes to UTC before querying and delegating all data access through the clock's time check.

## Dependencies
- Internal: `gs_quant.backtests.data_sources` (DataManager)
- External: `datetime` (dt.datetime, dt.date, dt.timezone)
- External: `typing` (Union)

## Type Definitions

### Clock (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _time | `dt.datetime` | `dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)` | Current clock time, always tz-aware UTC |

### DataHandler (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _data_mgr | `DataManager` | required | Underlying data source manager |
| _clock | `Clock` | `Clock()` | Clock instance for lookahead protection |
| _tz | `dt.timezone` | required | Timezone used for naive-to-UTC conversion |

## Enums and Constants
None.

## Functions/Methods

### Clock.__init__(self) -> None
Purpose: Initialize the clock with a `_time` field and reset to the epoch.

**Algorithm:**
1. Set `self._time = None`
2. Call `self.reset()`

### Clock.update(self, time: dt.datetime) -> None
Purpose: Advance the clock to `time`, raising if time goes backwards.

**Algorithm:**
1. Branch: `time.tzinfo is None or time.tzinfo.utcoffset(time) is None` (tz-naive) -> `compare_time = self._time.replace(tzinfo=None)`
2. Branch: else (tz-aware) -> `compare_time = self._time`
3. Branch: `time < compare_time` -> raise `RuntimeError` with message about running backwards
4. Set `self._time = time`

**Raises:** `RuntimeError` when `time < compare_time` (clock running backwards)

### Clock.reset(self) -> None
Purpose: Reset the clock to 1900-01-01 00:00:00 UTC.

**Algorithm:**
1. Set `self._time = dt.datetime(1900, 1, 1).replace(tzinfo=dt.timezone.utc)`

### Clock.time_check(self, state: Union[dt.date, dt.datetime]) -> None
Purpose: Check if `state` is in the future relative to the current clock time; raise on lookahead.

**Algorithm:**
1. Branch: `isinstance(state, dt.datetime)`:
   a. Branch: `state.tzinfo is None or state.tzinfo.utcoffset(state) is None` (tz-naive) -> `lookahead = state > self._time.replace(tzinfo=None)`
   b. Branch: else (tz-aware) -> `lookahead = state > self._time`
2. Branch: else (state is `dt.date`) -> `lookahead = state > self._time.date()`
3. Branch: `lookahead is True` -> raise `RuntimeError` with message about accessing future data

**Raises:** `RuntimeError` when state is ahead of current clock time

### DataHandler.__init__(self, data_mgr: DataManager, tz: dt.timezone) -> None
Purpose: Initialize with data manager, clock, and timezone.

**Algorithm:**
1. Store `self._data_mgr = data_mgr`
2. Create `self._clock = Clock()`
3. Store `self._tz = tz`

### DataHandler.reset_clock(self) -> None
Purpose: Reset the internal clock to the epoch.

**Algorithm:**
1. Call `self._clock.reset()`

### DataHandler.update(self, state: dt.datetime) -> None
Purpose: Advance the internal clock to `state`.

**Algorithm:**
1. Call `self._clock.update(state)`

### DataHandler._utc_time(self, state: Union[dt.date, dt.datetime]) -> Union[dt.date, dt.datetime]
Purpose: Convert tz-naive datetimes to UTC using `self._tz`; return tz-aware datetimes and dates unchanged.

**Algorithm:**
1. Branch: `isinstance(state, dt.datetime) and (state.tzinfo is None or state.tzinfo.utcoffset(state) is None)` -> replace tzinfo with `self._tz`, convert to UTC, strip tzinfo, return
2. Branch: else -> return `state` as-is

### DataHandler.get_data(self, state: Union[dt.date, dt.datetime], *key) -> Any
Purpose: Time-check then retrieve a single data point.

**Algorithm:**
1. Call `self._clock.time_check(state)`
2. Return `self._data_mgr.get_data(self._utc_time(state), *key)`

### DataHandler.get_data_range(self, start: Union[dt.date, dt.datetime], end: Union[dt.date, dt.datetime], *key) -> Any
Purpose: Time-check then retrieve a range of data, requiring matching types for start/end.

**Algorithm:**
1. Call `self._clock.time_check(end)`
2. Branch: `type(start) is not type(end)` -> raise `RuntimeError` about mismatched types
3. Return `self._data_mgr.get_data_range(self._utc_time(start), self._utc_time(end), *key)`

**Raises:** `RuntimeError` when start and end have different types

## State Mutation
- `Clock._time`: Set during `reset()` to 1900-01-01 UTC; updated monotonically by `update()`
- `DataHandler._clock._time`: Updated via `DataHandler.update()` and reset via `DataHandler.reset_clock()`

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `Clock.update` | When `time < compare_time` (backwards clock) |
| `RuntimeError` | `Clock.time_check` | When `state` is ahead of current clock time (lookahead) |
| `RuntimeError` | `DataHandler.get_data_range` | When `type(start) is not type(end)` |

## Edge Cases
- tz-naive datetimes throughout: Clock strips tzinfo from `_time` for comparison; DataHandler converts to UTC
- Clock starts at 1900-01-01 UTC so any real date should pass the `update()` check
- `get_data_range` with mixed `dt.date`/`dt.datetime` types raises an error
- `_utc_time` only converts `dt.datetime` instances; `dt.date` values pass through unchanged
- `time_check` uses `isinstance(state, dt.datetime)` which is True for `dt.datetime` subclasses but also note `dt.datetime` is a subclass of `dt.date`, so the isinstance check must be done in datetime-first order (which it is)

## Bugs Found
None.

## Coverage Notes
- Branch count: 14
- Key branches: Clock.update (2: tz-naive vs tz-aware), Clock.time_check (3: datetime tz-naive, datetime tz-aware, date), DataHandler._utc_time (2: tz-naive datetime, else), DataHandler.get_data_range (1: type mismatch)
- All branches are testable without mocking external APIs
