# point.py

## Summary
Provides tenor/market-data-coordinate point parsing and sort-order calculation. The core function `point_sort_order` converts a wide variety of financial tenor string formats (e.g. `"3m"`, `"Dec20"`, `"1y"`, `"QE1-2024"`, `"3x6"`, `"O/N"`) into a numeric day-count value suitable for sorting. The helper `relative_date_add` converts date-rule strings (e.g. `"1d"`, `"1y"`, `"-1w"`) into day counts. This module is critical for ordering market data points by maturity/expiry.

## Dependencies
- Internal: `gs_quant.errors` (`MqValueError`)
- External: `datetime` (stdlib, aliased `dt`), `functools` (`lru_cache`), `re`, `string` (`capwords`), `typing` (`Optional`)

## Type Definitions
No classes are defined in this module. All logic is in module-level functions.

## Enums and Constants

### ConstPoints
Lookup table for well-known constant point strings (case-insensitive via `.upper()`).

| Key | Value | Description |
|-----|-------|-------------|
| `"O/N"` | `0` | Overnight |
| `"T/N"` | `0.1` | Tomorrow/next |
| `"OIS FIX"` | `1` | OIS fixing |
| `"CASH STUB"` | `1.1` | Cash stub |
| `"CASHSTUB"` | `1.1` | Cash stub (no space) |
| `"DEFAULT"` | `0` | Default point |
| `"IN"` | `0.1` | In point |
| `"OUT"` | `0.2` | Out point |

### DictDayRule
Maps date-rule letter suffixes and word forms to day-count multipliers.

| Key | Value | Description |
|-----|-------|-------------|
| `'Month'` / `'MONTH'` / `'m'` / `'M'` / `'f'` / `'F'` | `30` | Month (30 calendar days) |
| `'Week'` / `'WEEK'` / `'w'` / `'W'` | `7` | Week (7 days) |
| `'Year'` / `'YEAR'` / `'y'` / `'Y'` | `365` | Year (365 days) |
| `'Day'` / `'DAY'` / `'d'` / `'D'` / `'b'` / `'B'` | `1` | Day (1 day); `b`/`B` is business day (treated as 1 calendar day for sort order) |

### Regular Expression Constants
All compiled as raw strings, used in `point_sort_order` for pattern matching.

| Name | Pattern | Matches | Example |
|------|---------|---------|---------|
| `EuroOrFraReg` | `^(Jan\|Feb\|...\|DEC)+([0-9][0-9])$` | Euro-dollar / FRA futures: `MMMdd` | `"Dec20"`, `"MAR25"` |
| `NumberReg` | `^([0-9]*)$` | Pure numeric strings | `"100"`, `"0"` |
| `MMMYYYYReg` | `^([a-zA-Z]{3}[0-9]{4})$` | Month-year: `MMMyyyy` | `"Jan2024"` |
| `DDMMMYYYYReg` | `^([1-3]*[0-9]{1}[a-zA-Z]{3}[0-9]{4})$` | Day-month-year: `ddMMMyyyy` | `"15Jan2024"` |
| `SpikeQEReg` | `^(QE[0-9])-([0-9]{4})$` | Quarter-end spike | `"QE1-2024"` |
| `FRAxReg` | `^([0-9]+)x([0-9]+)$` | FRA notation | `"3x6"` |
| `RDatePartReg` | `^([-]*[0-9]+[mydwbfMYDWBF])([-]*[0-9]+[mydwbfMYDWBF])?$` | Relative date parts | `"3m"`, `"-1y"`, `"1y6m"` |
| `CashFXReg` | `^([-]*[0-9]+[mydwbfMYDWBF])([-]*[0-9]+[mydwbfMYDWBF])? XC$` | Cash FX tenor with ` XC` suffix | `"3m XC"` |
| `PricerCoordRegI` | `^(No )([0-9]*)$` | Pricer coordinate format I | `"No 5"` |
| `PricerCoordRegII` | `^(Pricer )([0-9]*)$` | Pricer coordinate format II | `"Pricer 10"` |
| `PricerBFReg` | `^([-]*[0-9]+[mydwbfMYDWBFM])([-]*[0-9]+[mydwbfMYDWBF])([-]*[0-9]+[mydwbfMYDWBF])?$` | Pricer butterfly | `"1y3m6m"` |
| `PricerBondSpreadReg` | `^[0-9][SQHT]([0-9]{2})[/][0-9][SQHT]([0-9]{2})` | Bond spread | `"2S25/5S25"` |
| `SeasonalFrontReg` | `(Front\|Back)` | Seasonal front/back | `"Front"`, `"Back"` |
| `infl_volReg` | `(Caplet\|ZCCap\|Swaption\|ZCSwo)` | Inflation vol types | `"Caplet"`, `"ZCCap"` |
| `MMMReg` | `^(Jan\|Feb\|...\|Dec)$` | Three-letter month name (exact) | `"Jan"` |
| `MMMYYReg` | `^(JAN\|FEB\|...\|DEC) ([0-9]{2})` | Upper-case month + 2-digit year | `"JAN 24"` |
| `DatePairReg` | `^([0-9]{8})/([0-9]{8})$` | Date pair separated by `/` | `"20240101/20240701"` |
| `DatePairReg2` | `^([0-9]{8}) ([0-9]{8})$` | Date pair separated by space | `"20240101 20240701"` |
| `FXVolAddonParmsReg` | `(Spread Addon\|Spread Init\|...\|Addon HL)` | FX vol add-on parameters | `"Spread Addon"` |
| `CopulaReg` | `(Rho$\|Rho Rate\|...\|K0=S)` | Copula parameters | `"Rho"`, `"Alpha"` |
| `BondCoordReg` | `^[0-9]* ([0-9.]*) ([0-9]{2}/[0-9]{2}/[0-9]{4})$` | Bond coordinate | `"5 3.25 01/15/2030"` |
| `BondFutReg` | `^[A-Z]{3}([FGHJKMNQUVXZ])([0-9])$` | Bond future | `"TUSU4"` (note: 3-letter prefix + month-code + digit) |
| `FFFutReg` | `^FF([FGHJKMNQUVXZ])([0-9])$` | Fed Funds future | `"FFZ4"` |
| `RepoGCReg` | `^(ON\|SN\|TN\|[0-9]+) (\|Month \|Week \|Year \|Day )GC$` | Repo GC | `"ON GC"`, `"3 Month GC"` |
| `FloatingYear` | `^([0-9]*\.[0-9])[yY]$` | Floating-point year tenor | `"1.5y"`, `"2.5Y"` |
| `RelativeReg` | `^([0-9]+) (day\|week\|month\|year\|DAY\|WEEK\|MONTH\|YEAR)$` | Relative duration with word | `"3 month"` |
| `LYYReg` | `([FGHJKMNQUVXZ])([0-9]{2})` | Futures month-letter + 2-digit year | `"Z24"`, `"H25"` |
| `DateRuleReg` | `^([-]*[0-9]+[mydwbfMYDWBFM])+$` | One or more date-rule parts | `"1y"`, `"-3m"`, `"1y6m"` |
| `DDMMMYYReg` | `^([0-3]*[0-9]{1}[a-zA-Z]{3}[0-9]{2})$` | Day-month-2-digit-year | `"15Jan24"` |
| `FutMonth` | `"FGHJKMNQUVXZ"` | Futures month code string; index+1 = month number | `F=Jan, G=Feb, ..., Z=Dec` |

## Functions/Methods

### relative_date_add(date_rule: str, strict: bool = False) -> float
Purpose: Convert a date-rule string (e.g. `"1d"`, `"1y"`, `"-1w"`) to a float number of days.

**Algorithm:**
1. Attempt `re.search(DateRuleReg, date_rule)`.
2. Branch: match found:
   a. Extract first capture group `date_str = res.group(1)`.
   b. Branch: `date_str[0] == '-'`:
      - `num = float(date_str[1:-1])`, set `days = '-'` (string prefix for negation).
   c. Branch: else:
      - `num = float(date_str[:-1])`.
   d. Extract `rule = date_str[-1:]` (the letter suffix).
   e. Branch: `rule in DictDayRule`:
      - `scale = DictDayRule[rule]`.
      - Compute `days = days + str(num * scale)` (string concatenation, e.g. `"-"` + `"365.0"` = `"-365.0"`).
      - Return `float(days)`.
   f. Branch: `rule not in DictDayRule`:
      - Raise `MqValueError('There are no valid day rule for the point provided.')`.
3. Branch: no match:
   a. Branch: `strict` is `True` -> raise `MqValueError(f'invalid date rule {date_rule}')`.
   b. Branch: `strict` is `False` -> return `0`.

**Raises:**
- `MqValueError` when rule letter not in `DictDayRule`.
- `MqValueError` when `strict=True` and pattern does not match.

**Elixir porting note:** The negation is handled via string concatenation (`days = '-'` prefix), not arithmetic negation. The Elixir port should use `num * scale * -1` instead.

---

### point_sort_order(point: str, ref_date: Optional[dt.date] = None) -> Optional[float]
Purpose: Convert a market data coordinate point string into a numeric value (days from reference date) for sorting.

**Decorator:** `@functools.lru_cache(maxsize=None)` -- unbounded memoization. The function is cached on `(point, ref_date)` tuples. Since `ref_date` defaults to `dt.date.today()`, cache entries become stale across days.

**Algorithm:**
1. Set `ref_date = dt.date.today()` if `ref_date is None`.
2. Branch: `not point or not isinstance(point, str)` -> return `0`.
3. Look up `point.upper()` in `ConstPoints`. Branch: found -> return the constant value.
4. Split `point` by `';'`, strip parts.
5. Branch: multiple parts (semicolon-separated compound point):
   a. Recursively call `point_sort_order(parts[0])` (no ref_date -- uses today).
   b. Branch: `first` is falsy (0 or None) -> return `0`.
   c. Return `first + (0.1 * sum(point_sort_order(p, ref_date) for p in parts[1:]) / first)`.
6. Initialize `days = None`.
7. Match `point` against patterns in priority order (first match wins):

| Priority | Pattern / Condition | Computation | Result |
|----------|-------------------|-------------|--------|
| 1 | `point == 'o/n'` | -- | `days = 0` |
| 2 | `point == 't/n'` | -- | `days = 0.1` |
| 3 | `point == 'Cash Stub'` | -- | `days = 1.1` |
| 4 | `point == 'CashStub'` | -- | `days = 1.1` |
| 5 | `point == 'Default'` | -- | `days = 0` |
| 6 | `point == 'In'` | -- | `days = 0.1` |
| 7 | `point == 'Out'` | -- | `days = 0.2` |
| 8 | `infl_volReg` | Sub-branch on group(1): `Caplet`->0, `ZCCap`->1, `Swaption`->2, `ZCSwo`->3 | `days = 0..3` |
| 9 | `CopulaReg` | `pass` (days remains None) | `days = None` |
| 10 | `SeasonalFrontReg` | `Front`->0, else->1 | `days = 0 or 1` |
| 11 | `MMMReg` | Parse `"1" + month + "2000"` with `%d%b%Y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 12 | `EuroOrFraReg` | Parse `"15" + month + yy` with `%d%b%y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 13 | `RDatePartReg` | `relative_date_add(group(1))` | `days = float` |
| 14 | `CashFXReg` | `relative_date_add(group(1))` | `days = float` |
| 15 | `PricerBFReg` | `relative_date_add(group(1))` | `days = float` |
| 16 | `FRAxReg` | `relative_date_add(group(1) + 'm')` | `days = float` (months converted) |
| 17 | `SpikeQEReg` | Map QE1->Mar, QE2->Jun, QE3->Sep, QE4->Dec (else also Dec), parse `"1" + month + year` with `%d%b%Y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 18 | `MMMYYYYReg` | Parse `"1" + mmmyyyy` with `%d%b%Y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 19 | `DDMMMYYYYReg` | Parse `ddmmmyyyy` with `%d%b%Y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 20 | `NumberReg` | `float(group(1))` | `days = float` |
| 21 | `FloatingYear` | `365 * float(group(1))` | `days = float` |
| 22 | `PricerCoordRegI` | `float(group(2))` | `days = float` |
| 23 | `PricerCoordRegII` | `float(group(2))` | `days = float` |
| 24 | `PricerBondSpreadReg` | `pass` (days remains None) | `days = None` |
| 25 | `LYYReg` | `month = FutMonth.find(group(1)) + 1`, parse `yy-month-1` with `%y-%m-%d`, diff from ref_date | `days = (parsed - ref_date).days` |
| 26 | `DatePairReg` | Parse second group (end date) `%Y%m%d`, diff from ref_date | `days = (parsed - ref_date).days` |
| 27 | `DatePairReg2` | Parse second group (end date) `%Y%m%d`, diff from ref_date | `days = (parsed - ref_date).days` |
| 28 | `MMMYYReg` | Parse `"1" + MMM + YY` with `%d%b%y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 29 | `FXVolAddonParmsReg` | `pass` (days remains None) | `days = None` |
| 30 | `BondCoordReg` | Parse date group(2) with `%d/%m/%Y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 31 | `BondFutReg` | `month = FutMonth.find(group(1)) + 1`, parse `ref_year-month-1` with `%Y-%m-%d`, diff from ref_date | `days = (parsed - ref_date).days` |
| 32 | `FFFutReg` | Same as BondFutReg | `days = (parsed - ref_date).days` |
| 33 | `RepoGCReg` | Sub-branch: `ON GC`->0, `TN GC`->1, `SN GC`->2, else: look up group(2).strip() in DictDayRule, compute `num * scale` | `days = float` |
| 34 | `RelativeReg` | `string.capwords(group(2))` then look up in DictDayRule, compute `num * scale` | `days = float` |
| 35 | `DDMMMYYReg` | Parse `ddMMMYY` with `%d%b%y`, diff from ref_date | `days = (parsed - ref_date).days` |
| 36 | else (no match) | -- | `days = 0` |

8. Return `days`.

**Return type notes:** Returns `None` when `days` is never assigned (stays `None`) -- this happens for `CopulaReg`, `PricerBondSpreadReg`, `FXVolAddonParmsReg` matches, and for `RepoGCReg`/`RelativeReg` when the rule lookup fails. Returns `Optional[float]` as declared.

**Raises:** Nothing explicitly. `relative_date_add` may raise `MqValueError`. `datetime.strptime` may raise `ValueError` on malformed date strings.

**Elixir porting notes:**
- The `lru_cache` means results are memoized globally. In Elixir, consider an ETS table or process dictionary for caching, or simply recompute (the function is pure given a ref_date).
- The regex matching order matters: first match wins, and some patterns overlap (e.g. `LYYReg` is a substring match, placed deliberately after more specific patterns).
- `FutMonth.find(letter) + 1` maps `F`->1 (Jan), `G`->2 (Feb), ..., `Z`->12 (Dec).
- Compound semicolon-separated points use a weighted average formula: `first + 0.1 * sum(rest) / first`.

## State Mutation
- `point_sort_order` is decorated with `@functools.lru_cache(maxsize=None)`, creating a global, unbounded, module-level cache. Cache entries are keyed on `(point, ref_date)`.
- No other mutable state.
- Thread safety: `lru_cache` is thread-safe in CPython (GIL-protected). In Elixir, equivalent caching needs explicit concurrency handling.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `relative_date_add` | Rule letter not found in `DictDayRule` |
| `MqValueError` | `relative_date_add` | `strict=True` and date_rule doesn't match `DateRuleReg` |
| `ValueError` (implicit) | `point_sort_order` | `datetime.strptime` fails on malformed date in any date-parsing branch |

## Edge Cases
- `point_sort_order(None)` -> returns `0` (guarded by `not point`).
- `point_sort_order("")` -> returns `0` (guarded by `not point`).
- `point_sort_order(123)` -> returns `0` (guarded by `not isinstance(point, str)`).
- Semicolon-separated points where first part resolves to `0` -> returns `0` (avoids division by zero).
- `CopulaReg`, `PricerBondSpreadReg`, `FXVolAddonParmsReg` matches return `None` (not `0`), meaning these points have no defined sort order.
- `RepoGCReg` matches where `group(2).strip()` is not in `DictDayRule` (e.g. empty string for `"5 GC"`) -> `days` stays `None`, returns `None`.
- `RelativeReg` matches where `capwords(group(2))` not in `DictDayRule` -> `days` stays `None` (should not happen given the regex only matches `day|week|month|year`).
- `relative_date_add` with negative values: negation is done via string prefix `"-"`, which works but is fragile.
- `MMMReg` uses hardcoded year 2000 (`"1" + month + "2000"`), so "Jan" always maps to 2000-01-01 regardless of ref_date's year.
- `EuroOrFraReg` group(1) uses `+` quantifier, so `"JanJan20"` would technically match (month repeated). In practice, the 2-digit year constraint limits this.
- `BondFutReg` and `FFFutReg` use `ref_date.year` for the year, so the same future code yields different results across calendar years.
- `NumberReg` matches empty string `""` since pattern is `[0-9]*` -- but this is guarded by the earlier `not point` check.

## Coverage Notes
- Branch count: ~55+ (high due to the long `elif` chain in `point_sort_order`).
- Critical branches to test: every regex branch in `point_sort_order`, the semicolon split logic, `relative_date_add` negative/positive/strict/non-strict paths.
- `infl_volReg` sub-branches: 4 (Caplet, ZCCap, Swaption, ZCSwo).
- `SpikeQEReg` sub-branches: 5 (QE1, QE2, QE3, QE4, else/default).
- `SeasonalFrontReg` sub-branches: 2 (Front, else).
- `RepoGCReg` sub-branches: 4 (ON, TN, SN, numeric+rule).
- `CopulaReg`, `PricerBondSpreadReg`, `FXVolAddonParmsReg` branches use `pass` -- return `None`.
- `lru_cache` makes repeated calls with same arguments return cached values (does not re-execute).
