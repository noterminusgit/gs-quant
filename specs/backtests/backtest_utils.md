# backtest_utils.py

## Summary
Utility functions: make_list, get_final_date, scale_trade, map_ccy_name_to_ccy, interpolate_signal. Plus CalcType enum and CustomDuration dataclass.

## Functions

### make_list(thing)
1. None → []
2. str → [thing]
3. try iter(thing):
   a. TypeError → [thing] (not iterable)
   b. success → list(thing)

### get_final_date(inst, create_date, duration, holiday_calendar, trigger_info)
1. Check cache → return if found
2. duration is None → dt.date.max
3. duration is date/datetime → return as-is
4. hasattr(inst, str(duration)) → getattr
5. str(duration).lower() == 'next schedule' → trigger_info.next_schedule or date.max; no attr → RuntimeError
6. isinstance(duration, CustomDuration) → recursive call for each sub-duration
7. Fallback: RelativeDate(duration, create_date).apply_rule

### scale_trade(inst, ratio)
Returns inst.scale(ratio)

### map_ccy_name_to_ccy(currency_name)
Maps CurrencyName to 3-letter code. Branch: isinstance CurrencyName → .value, else use string directly.

### interpolate_signal(signal, method)
Interpolates sparse dict[date, float] to daily pd.Series.

## Edge Cases
- make_list with non-iterable non-string → wrapped in list
- get_final_date with 'next schedule' but no next_schedule attr → RuntimeError
- map_ccy_name_to_ccy with unknown currency → returns None
- final_date_cache is module-level mutable global

## Bugs Found
None.

## Coverage Notes
- ~20 branches
