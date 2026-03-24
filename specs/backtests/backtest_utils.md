# backtest_utils.py

## Summary
Utility functions and types for the backtesting framework: `make_list` for normalizing values to lists, `get_final_date` for computing trade expiration dates from various duration specifications (with caching), `scale_trade` for scaling instruments, `map_ccy_name_to_ccy` for currency name-to-code mapping, and `interpolate_signal` for expanding sparse date-keyed signals to daily time series. Also defines `CalcType` enum and `CustomDuration` dataclass.

## Dependencies
- Internal: `gs_quant.common` (`CurrencyName`), `gs_quant.datetime.relative_date` (`RelativeDate`), `gs_quant.instrument` (`Instrument`), `gs_quant.timeseries` (`interpolate`, `Interpolate`)
- External: `datetime` (`dt.date`, `dt.datetime`, `dt.timedelta`), `pandas` (`pd.Series`), `enum` (`Enum`), `dataclasses` (`dataclass`), `dataclasses_json` (`dataclass_json`), `typing` (`Callable`, `Tuple`, `Union`)

## Type Definitions

### CalcType (Enum)

| Value | Raw | Description |
|-------|-----|-------------|
| simple | `"simple"` | Simple calculation, no path dependency |
| semi_path_dependent | `"semi_path_dependent"` | Semi-path-dependent (e.g., hedging) |
| path_dependent | `"path_dependent"` | Fully path-dependent (e.g., rebalancing) |

### CustomDuration (dataclass, dataclass_json)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| durations | `Tuple[Union[str, dt.date, dt.timedelta], ...]` | *(required)* | Tuple of sub-durations to be combined |
| function | `Callable[[Tuple[Union[str, dt.date, dt.timedelta], ...]], Union[str, dt.date, dt.timedelta]]` | *(required)* | Function that combines resolved sub-durations into a final duration |

### Duration (type alias, defined in actions.py but uses CustomDuration)
```
Duration = Union[str, dt.date, dt.timedelta, CustomDuration]
```

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| final_date_cache | `dict` | `{}` | Module-level mutable cache mapping `(inst, create_date, duration, holiday_calendar)` tuples to resolved final dates |

## Functions/Methods

### make_list(thing: Any) -> list
Purpose: Normalize a value into a list -- handles `None`, strings, iterables, and non-iterable scalars.

**Algorithm:**
1. Branch: `thing is None` -> return `[]`
2. Branch: `isinstance(thing, str)` -> return `[thing]`
3. Try `iter(thing)`:
   a. Branch: `TypeError` raised -> return `[thing]` (non-iterable scalar)
   b. Branch: success -> return `list(thing)` (convert iterable to list)

### get_final_date(inst: Any, create_date: dt.date, duration: Any, holiday_calendar: Optional[Iterable[dt.date]] = None, trigger_info: Any = None) -> dt.date
Purpose: Compute the final/expiration date for a trade given various duration specifications, with result caching.

**Algorithm:**
1. Build `cache_key = (inst, create_date, duration, holiday_calendar)`
2. Branch: `cache_key in final_date_cache` -> return cached value
3. Branch: `duration is None` -> cache and return `dt.date.max`
4. Branch: `isinstance(duration, (dt.datetime, dt.date))` -> cache and return `duration` as-is
5. Branch: `hasattr(inst, str(duration))` -> cache `getattr(inst, str(duration))` and return it
6. Branch: `str(duration).lower() == 'next schedule'`:
   a. Branch: `hasattr(trigger_info, 'next_schedule')` -> return `trigger_info.next_schedule or dt.date.max`
   b. Branch: else -> raise `RuntimeError('Next schedule not supported by action')`
7. Branch: `isinstance(duration, CustomDuration)` -> recursively call `get_final_date` for each sub-duration, then apply `duration.function(*resolved_sub_durations)` and return result
8. Fallback: construct `RelativeDate(duration, create_date).apply_rule(holiday_calendar=holiday_calendar)`, cache, and return

**Raises:** `RuntimeError` when duration is `'next schedule'` but `trigger_info` lacks `next_schedule` attribute.

### CustomDuration.__hash__(self) -> int
Purpose: Make `CustomDuration` hashable so it can be used as part of the `final_date_cache` key.

**Algorithm:**
1. Return `hash((self.durations, self.function))`

### scale_trade(inst: Instrument, ratio: float) -> Instrument
Purpose: Scale an instrument by a ratio.

**Algorithm:**
1. Call `inst.scale(ratio)` and return the result.

### map_ccy_name_to_ccy(currency_name: Union[str, CurrencyName]) -> Optional[str]
Purpose: Convert a currency full name (or `CurrencyName` enum) to its 3-letter ISO code.

**Algorithm:**
1. Define static mapping dict of full names to 3-letter codes (17 currencies)
2. Branch: `isinstance(currency_name, CurrencyName)` -> use `currency_name.value` for lookup
3. Branch: else -> use `currency_name` directly as string for lookup
4. Return `map.get(...)` (returns `None` if not found)

### interpolate_signal(signal: dict[dt.date, float], method: Interpolate = Interpolate.STEP) -> pd.Series
Purpose: Expand a sparse date-to-float dictionary into a daily `pd.Series` using interpolation.

**Algorithm:**
1. Find `min_date` and `max_date` from signal keys
2. Generate `all_dates` as daily range from min to max (inclusive)
3. Call `interpolate(pd.Series(signal).sort_index(), all_dates, method=method)` and return result

## State Mutation
- `final_date_cache`: Module-level global dict. Modified by `get_final_date()` on every cache miss. Grows unboundedly over the lifetime of the process. Never cleared automatically.
- Thread safety: `final_date_cache` is a plain `dict` with no synchronization. Concurrent calls to `get_final_date` could cause race conditions, though in practice backtests run single-threaded.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `get_final_date` | When `duration` is `'next schedule'` but `trigger_info` has no `next_schedule` attribute |

## Edge Cases
- `make_list(None)` returns `[]`; `make_list("")` returns `[""]` (empty string is still a string).
- `make_list` with a non-iterable, non-string value (e.g., `make_list(42)`) returns `[42]`.
- `get_final_date` with `duration='next schedule'` and `trigger_info.next_schedule = None` returns `dt.date.max` (via the `or` fallback).
- `get_final_date` with `CustomDuration` does NOT cache the result (only the recursive sub-calls are cached).
- `get_final_date` with `'next schedule'` does NOT cache the result either.
- `map_ccy_name_to_ccy` with an unknown currency string returns `None`.
- `map_ccy_name_to_ccy` maps `'Yuan Renminbi (Onshore)'` to `'CHY'` (note: not standard ISO `'CNY'`).
- `interpolate_signal` with an empty dict will raise `ValueError` from `min()`/`max()` on empty keys.
- `final_date_cache` is module-level mutable global state; it persists across tests unless explicitly cleared.

## Bugs Found
None.

## Coverage Notes
- Branch count: ~20
- `make_list`: 4 branches (None, str, iterable, non-iterable)
- `get_final_date`: ~10 branches (cache hit, None, date/datetime, hasattr, next schedule with/without attr, next_schedule truthy/falsy, CustomDuration, fallback)
- `map_ccy_name_to_ccy`: 2 branches (CurrencyName instance vs. string)
- `interpolate_signal`: 0 conditional branches
- `scale_trade`: 0 conditional branches
- `CustomDuration.__hash__`: 0 conditional branches
- Mocking notes: `get_final_date` needs mocked instruments with dynamic attributes and `RelativeDate`; `interpolate_signal` needs the `gs_quant.timeseries.interpolate` function; `final_date_cache` should be reset between tests.
- Pragmas: none
