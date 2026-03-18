# data_handler.py

## Summary
Clock (time tracking with lookahead protection) and DataHandler (data access with timezone conversion).

## Classes

### Clock
- update(time):
  1. If time is tz-naive → compare against _time without tz
  2. Else → compare directly
  3. If time < compare_time → RuntimeError (backwards)
- reset(): sets to 1900-01-01 UTC
- time_check(state):
  1. If datetime:
     a. tz-naive → compare without tz
     b. tz-aware → compare directly
  2. If date → compare against _time.date()
  3. If lookahead → RuntimeError

### DataHandler
- _utc_time(state):
  1. If datetime AND tz-naive → convert via self._tz to UTC, strip tz
  2. Else → return as-is
- get_data(state, *key): time_check then data_mgr.get_data
- get_data_range(start, end, *key):
  1. time_check on end
  2. If type(start) != type(end) → RuntimeError
  3. Convert both to UTC, get_data_range

## Edge Cases
- tz-naive datetime handling throughout
- Clock starts at 1900 → any real date should pass
- get_data_range with mixed date/datetime types → error

## Bugs Found
None.

## Coverage Notes
- ~14 branches
