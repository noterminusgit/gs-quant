# rules.py

## Summary
Defines a family of date-manipulation "rule" classes used by the RDate (relative-date) system. Each rule encodes a single calendar operation (e.g., add business days, jump to nth weekday of month, advance by months/years with holiday-aware rolling). The abstract base class `RDateRule` provides shared infrastructure for holiday lookup, business-day offset, nth-day-of-month computation, and weekend detection. Twenty-eight concrete subclasses each implement a `handle()` method that performs one specific date transformation.

## Dependencies
- Internal: `gs_quant.datetime.gscalendar` (GsCalendar), `gs_quant.markets.securities` (ExchangeCode), `gs_quant.common` (Currency)
- External: `datetime` (date, timedelta), `calendar` (monthrange, MONDAY..SUNDAY constants), `logging`, `abc` (ABC, abstractmethod), `typing` (List, Union, Optional), `dateutil.relativedelta` (relativedelta, MO, TU, WE, TH, FR, SA, SU), `numpy` (busday_offset), `pandas` (to_datetime)

## Type Definitions

### RDateRule (ABC)
Inherits: `ABC`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| result | `dt.date` | *(required)* | The working date that rules mutate/transform |
| number | `int` | `None` | Numeric parameter controlling magnitude of offset or ordinal selector |
| week_mask | `str` | `None` | 7-char string of `'0'`/`'1'` indicating valid weekdays (Mon-Sun), passed to `numpy.busday_offset` |
| currencies | `List[Union[Currency, str]]` | `None` | Currency codes for holiday calendar lookup |
| exchanges | `List[Union[ExchangeCode, str]]` | `None` | Exchange codes for holiday calendar lookup |
| holiday_calendar | `List[dt.date]` | `None` | Pre-supplied list of holiday dates (bypasses GsCalendar fetch) |
| usd_calendar | `List[dt.date]` | `None` | Supplemental USD holiday calendar merged with `holiday_calendar` |
| roll | `str` | `None` | Roll convention override (`'forward'`, `'backward'`, `'preceding'`, etc.) |
| sign | `Optional[str]` | `None` | Sign string (`"+"` or `"-"`) used by `uRule` to determine roll direction |

All fields are set via `__init__(result, **params)` using `params.get(...)` with implicit `None` defaults.

### Concrete Rule Classes

Each inherits `RDateRule` and implements `handle() -> dt.date`. No additional fields are declared on any subclass.

| Class | Letter | Category |
|-------|--------|----------|
| `ARule` | A | Absolute year |
| `bRule` | b | Business day offset |
| `dRule` | d | Calendar day offset |
| `eRule` | e | End of current month |
| `FRule` | F | Nth Friday of month |
| `gRule` | g | Week offset + business-day snap |
| `NRule` | N | Next/nth Monday |
| `GRule` | G | Next/nth Friday |
| `IRule` | I | Next/nth Saturday |
| `JRule` | J | First day of month |
| `kRule` | k | Year offset + weekday skip + business-day snap |
| `mRule` | m | Month offset + business-day snap |
| `MRule` | M | Nth Monday of month |
| `PRule` | P | Next/nth Sunday |
| `rRule` | r | Year-end + year offset |
| `RRule` | R | Nth Thursday of month |
| `SRule` | S | Next/nth Thursday |
| `TRule` | T | Nth Tuesday of month |
| `uRule` | u | Business day offset with sign-aware roll |
| `URule` | U | Next/nth Tuesday |
| `vRule` | v | Month offset + end-of-month + business-day snap |
| `VRule` | V | Nth Saturday of month |
| `WRule` | W | Nth Wednesday of month |
| `wRule` | w | Week offset + directional business-day snap |
| `xRule` | x | End of month + business-day snap |
| `XRule` | X | Next/nth Wednesday |
| `yRule` | y | Year offset + weekday skip + business-day snap |
| `ZRule` | Z | Nth Sunday of month |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### RDateRule.__init__(self, result: dt.date, **params)
Purpose: Initialize rule instance from a base date and keyword parameters.

**Algorithm:**
1. Set `self.result = result`.
2. Extract each named parameter from `params` via `.get()`: `number`, `week_mask`, `currencies`, `exchanges`, `holiday_calendar`, `usd_calendar`, `roll`, `sign`.
3. Call `super().__init__()` (ABC init).

### RDateRule.handle(self) -> dt.date [abstract]
Purpose: Compute and return the transformed date. Must be implemented by every subclass.

### RDateRule._get_holidays(self) -> List[dt.date]
Purpose: Obtain the list of holiday dates, either from a pre-supplied calendar or by fetching from GsCalendar.

**Algorithm:**
1. Branch: `self.holiday_calendar is not None` ->
   - Branch: `self.usd_calendar is None` -> return `self.holiday_calendar` directly.
   - Else -> return union of `self.holiday_calendar` and `self.usd_calendar` as a list (via `set().union()`).
2. Else -> try to build a `GsCalendar`:
   - Branch: `self.currencies is None` -> use `[]`.
   - Branch: `self.currencies` is a `str` -> wrap in `[self.currencies]`.
   - Else -> use `self.currencies` as-is.
   - Same logic for `self.exchanges`.
   - Create `GsCalendar(exchanges + currencies)` and return `cal.holidays`.
3. Branch: exception during GsCalendar fetch -> log warning, return `[]`.

### RDateRule._apply_business_days_logic(self, holidays: List[dt.date], offset: int = None, roll: str = 'preceding') -> dt.date
Purpose: Apply numpy business-day offset to `self.result` respecting holidays and week mask.

**Algorithm:**
1. Branch: `offset is not None` -> use `offset`.
2. Else -> Branch: `self.number` is truthy -> use `self.number`; else use `0`.
3. Call `np.busday_offset(self.result, offset_to_use, roll, holidays=holidays, weekmask=self.week_mask)`.
4. Convert result via `pd.to_datetime(...).date()` and return.

### RDateRule._get_nth_day_of_month(self, calendar_day: int) -> dt.date
Purpose: Find the Nth occurrence of a given weekday within the current month of `self.result`.

**Algorithm:**
1. Set `temp` to first of the month (`self.result.replace(day=1)`).
2. Compute `adj = (calendar_day - temp.weekday()) % 7` to find offset to first occurrence.
3. Add `adj` days to `temp`.
4. Add `(self.number - 1)` weeks to `temp`.
5. Return `temp`.

### RDateRule.add_years(self, holidays: List[dt.date]) -> dt.date
Purpose: Add `self.number` years to `self.result`, snap off weekends, then apply zero-offset business-day logic.

**Algorithm:**
1. Add `self.number` years via `relativedelta`.
2. Branch: `self.result.isoweekday()` is 6 (Sat) or 7 (Sun) -> add `isoweekday() % 5` days (Sat->1 day forward to Sun? Note: this maps Sat(6)->6%5=1, Sun(7)->7%5=2).
3. Return `self._apply_business_days_logic(holidays, offset=0)`.

### RDateRule.is_weekend(d: dt.date) -> bool [staticmethod]
Purpose: Return whether a date falls on Saturday or Sunday.

**Algorithm:**
1. Branch: `d.weekday() < 5` -> return `False`.
2. Else -> return `True`.

### RDateRule.roll_convention(self, default=None) -> str
Purpose: Return the roll convention, preferring `self.roll` if set, otherwise the provided default.

**Algorithm:**
1. Return `self.roll or default`.

---

### ARule.handle(self) -> dt.date
Purpose: Set date to January 1st of the year specified by `self.number`.

**Algorithm:**
1. Set result to Jan 1 of current year: `self.result.replace(month=1, day=1)`.
2. Add `relativedelta(year=self.number)` (note: `year=` sets absolute year).
3. Return the result.

### bRule.handle(self) -> dt.date
Purpose: Offset by `self.number` business days with appropriate roll convention.

**Algorithm:**
1. Fetch holidays via `_get_holidays()`.
2. Branch: `self.number <= 0` -> roll = `'forward'`; else -> roll = `'preceding'`. Override by `self.roll` if set.
3. Apply business-day offset with `offset=self.number` and computed roll.

### dRule.handle(self) -> dt.date
Purpose: Add `self.number` calendar days.

**Algorithm:**
1. Return `self.result + relativedelta(days=self.number)`.

### eRule.handle(self) -> dt.date
Purpose: Move to last day of the current month.

**Algorithm:**
1. Get month range via `calendar.monthrange(year, month)`.
2. Return `self.result.replace(day=month_range[1])`.

### FRule.handle(self) -> dt.date
Purpose: Find the Nth Friday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.FRIDAY)`.

### gRule.handle(self) -> dt.date
Purpose: Add `self.number` weeks, then snap to nearest business day (backward roll default).

**Algorithm:**
1. Add `self.number` weeks to `self.result`.
2. Fetch holidays.
3. Apply zero-offset business-day logic with roll = `self.roll or 'backward'`.

### NRule.handle(self) -> dt.date
Purpose: Find the next/nth Monday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=MO(self.number))`.

### GRule.handle(self) -> dt.date
Purpose: Find the next/nth Friday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=FR(self.number))`.

### IRule.handle(self) -> dt.date
Purpose: Find the next/nth Saturday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=SA(self.number))`.

### JRule.handle(self) -> dt.date
Purpose: Move to the first day of the current month.

**Algorithm:**
1. Return `self.result.replace(day=1)`.

### kRule.handle(self) -> dt.date
Purpose: Add `self.number` years, skip forward past non-business weekdays (per `week_mask`), then snap to business day.

**Algorithm:**
1. Add `self.number` years to `self.result`.
2. Loop: while `self.week_mask[self.result.isoweekday() - 1] == '0'`, add 1 day.
3. Fetch holidays.
4. Apply zero-offset business-day logic with roll = `self.roll or 'backward'`.

### mRule.handle(self) -> dt.date
Purpose: Add `self.number` months, then snap to business day (forward roll default).

**Algorithm:**
1. Add `self.number` months to `self.result`.
2. Apply zero-offset business-day logic with roll = `self.roll or 'forward'`.

### MRule.handle(self) -> dt.date
Purpose: Find the Nth Monday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.MONDAY)`.

### PRule.handle(self) -> dt.date
Purpose: Find the next/nth Sunday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=SU(self.number))`.

### rRule.handle(self) -> dt.date
Purpose: Move to December 31st, then offset by `self.number` years.

**Algorithm:**
1. Return `self.result.replace(month=12, day=31) + relativedelta(years=self.number)`.

### RRule.handle(self) -> dt.date
Purpose: Find the Nth Thursday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.THURSDAY)`.

### SRule.handle(self) -> dt.date
Purpose: Find the next/nth Thursday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=TH(self.number))`.

### TRule.handle(self) -> dt.date
Purpose: Find the Nth Tuesday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.TUESDAY)`.

### uRule.handle(self) -> dt.date
Purpose: Offset by `self.number` business days with sign-aware roll logic.

**Algorithm:**
1. Fetch holidays.
2. Branch: `self.sign == "-"` AND `self.number == 0` -> roll = `'preceding'`.
3. Else Branch: `self.number <= 0` -> roll = `'forward'`.
4. Else -> roll = `'preceding'`.
5. Apply business-day offset with `offset=self.number` and computed roll.

### URule.handle(self) -> dt.date
Purpose: Find the next/nth Tuesday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=TU(self.number))`.

### vRule.handle(self) -> dt.date
Purpose: Optionally add months, move to end of month, then snap to business day (backward roll default).

**Algorithm:**
1. Branch: `self.number` is truthy -> add `self.number` months. Else -> keep `self.result` unchanged.
2. Get last day of month via `calendar.monthrange`.
3. Replace day with last day.
4. Fetch holidays.
5. Apply zero-offset business-day logic with roll = `self.roll or 'backward'`.

### VRule.handle(self) -> dt.date
Purpose: Find the Nth Saturday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.SATURDAY)`.

### WRule.handle(self) -> dt.date
Purpose: Find the Nth Wednesday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.WEDNESDAY)`.

### wRule.handle(self) -> dt.date
Purpose: Add `self.number` weeks, then snap to business day with direction-dependent roll.

**Algorithm:**
1. Add `self.number` weeks to `self.result`.
2. Fetch holidays.
3. Branch: `self.number >= 0` -> default roll = `'forward'`. Else -> default roll = `'backward'`.
4. Apply zero-offset business-day logic with computed roll (overridable by `self.roll`).

### xRule.handle(self) -> dt.date
Purpose: Move to end of current month, then snap to business day (backward roll default).

**Algorithm:**
1. Get last day of month via `calendar.monthrange`.
2. Replace day with last day.
3. Fetch holidays.
4. Apply zero-offset business-day logic with roll = `self.roll or 'backward'`.

### XRule.handle(self) -> dt.date
Purpose: Find the next/nth Wednesday relative to `self.result`.

**Algorithm:**
1. Return `self.result + relativedelta(weekday=WE(self.number))`.

### yRule.handle(self) -> dt.date
Purpose: Add `self.number` years, skip forward past non-business weekdays (per `week_mask`), then snap to business day.

**Algorithm:**
1. Add `self.number` years to `self.result`.
2. Loop: while `self.week_mask[self.result.isoweekday() - 1] == '0'`, add 1 day.
3. Fetch holidays.
4. Apply zero-offset business-day logic with roll = `self.roll or 'backward'`.

**Note:** `yRule` and `kRule` have identical implementations.

### ZRule.handle(self) -> dt.date
Purpose: Find the Nth Sunday of the current month.

**Algorithm:**
1. Return `self._get_nth_day_of_month(calendar.SUNDAY)`.

## State Mutation
- `self.result`: Mutated in-place by `gRule`, `kRule`, `mRule`, `vRule`, `wRule`, `xRule`, `yRule` before calling `_apply_business_days_logic`. Also mutated by `add_years`. This means the rule instance is not reusable after `handle()` is called for these classes.
- `self.__business_day_calendars` (on GsCalendar, called indirectly): Populated lazily.
- Thread safety: No thread-safety mechanisms. Rule instances should not be shared across threads.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| Generic `Exception` (caught) | `_get_holidays` | GsCalendar construction or holiday fetch fails; logged as warning, returns `[]` |

## Edge Cases
- `_get_holidays` with `holiday_calendar` set but `usd_calendar` is `None`: returns `holiday_calendar` directly without creating a set union.
- `_get_holidays` with `holiday_calendar` set and `usd_calendar` set: creates union via `set().union()`, removing duplicates but losing order.
- `_get_holidays` with currencies/exchanges as a bare string instead of list: wraps in list before passing to GsCalendar.
- `_apply_business_days_logic` when `self.number` is `0` (falsy): uses `0` as offset, which is correct but exercises the `else` branch of the `self.number` truthiness check.
- `_apply_business_days_logic` when `self.number` is `None` (falsy): also uses `0`, preventing `None` from reaching numpy.
- `add_years` weekend adjustment: Saturday (isoweekday=6) -> adds 1 day (to Sunday), Sunday (isoweekday=7) -> adds 2 days (to Tuesday). The Saturday case moves to Sunday which is still a weekend; the subsequent `_apply_business_days_logic(offset=0)` with default `'preceding'` roll will snap to the previous Friday. This may be intentional or a subtle bug.
- `vRule` with `self.number` equal to `0` (falsy): skips the month addition, proceeding directly to end-of-month logic.
- `uRule` special case: `sign="-"` with `number=0` uses `'preceding'` roll, while unsigned `number=0` uses `'forward'` roll. This distinguishes `-0b` from `0b` in RDate expressions.
- `kRule`/`yRule` while loop: if `week_mask` is all zeros, this is an infinite loop. No guard exists.
- `ARule`: uses `relativedelta(year=self.number)` (absolute year set, not offset). The preceding `replace(month=1, day=1)` sets Jan 1, then `year=` overrides the year.
- `_get_nth_day_of_month` with `self.number` of 5+: may return a date in the next month if the month has fewer than 5 occurrences of that weekday.

## Coverage Notes
- Branch count: ~45 (including all handle methods, _get_holidays branches, _apply_business_days_logic branches, add_years weekend check, uRule triple branch, vRule number truthiness, wRule direction branch)
- Key branches: `_get_holidays` has 7 branches (holiday_calendar present/absent, usd_calendar present/absent, currencies is None/str/list, exchanges is None/str/list, exception). `_apply_business_days_logic` has 3 branches (offset not None, number truthy, number falsy). `uRule.handle` has 3 branches (sign=="-" and number==0, number<=0, else). `bRule.handle` has 2 branches (number<=0, else). `wRule.handle` has 2 branches (number>=0, else).
- Pragmas: none observed.
