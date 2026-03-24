# relative_date.py

## Summary
Provides `RelativeDate` and `RelativeDateSchedule` classes for computing dates from relative date-rule strings (e.g. `"-1d"`, `"3m"`, `"1b"`, `"0b"`, `"1m2b"`). Rules are parsed into individual components and dispatched to rule-handler classes in `gs_quant.datetime.rules`. Supports business-day-aware date arithmetic with configurable holiday calendars, exchange calendars, and week masks.

## Dependencies
- Internal: `gs_quant.datetime.rules` (entire module -- rule classes accessed via `getattr`), `gs_quant.common` (`Currency`), `gs_quant.errors` (`MqValueError`), `gs_quant.markets` (`PricingContext`), `gs_quant.markets.securities` (`ExchangeCode`)
- External: `datetime` (stdlib, aliased `dt`), `logging`, `copy` (`copy`), `typing` (`Union`, `Optional`, `List`), `pandas` (`pd`)

## Type Definitions

### RelativeDate (class)
Inherits: `object`

Parses a rule string into individual rule components and applies them sequentially to a base date.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rule` | `str` | (required) | The rule string, e.g. `"-1d"`, `"3m"`, `"1y2b"` |
| `base_date` | `dt.date` | Computed (see init) | The starting date for rule application |
| `base_date_passed_in` | `bool` | `False` | Whether the base_date was explicitly provided by the caller |

### RelativeDateSchedule (class)
Inherits: `object`

Wraps a `RelativeDate` to generate a schedule of dates from a base date to an end date by repeatedly applying the rule at increasing multiples.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rule` | `str` | (required) | The rule string, e.g. `"1m"`, `"1w"` |
| `base_date` | `dt.date` | Computed (see init) | The starting date |
| `base_date_passed_in` | `bool` | `False` | Whether the base_date was explicitly provided |
| `end_date` | `Optional[dt.date]` | `None` | Upper bound; schedule stops when next date exceeds this |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### RelativeDate.__init__(self, rule: str, base_date: Optional[dt.date] = None)
Purpose: Initialize RelativeDate with a rule string and determine the base date.

**Algorithm:**
1. Set `self.rule = rule`.
2. Set `self.base_date_passed_in = False`.
3. Branch: `base_date` is truthy:
   a. Set `self.base_date = base_date`.
   b. Set `self.base_date_passed_in = True`.
4. Branch: `PricingContext.current.is_entered` is truthy:
   a. Set `self.base_date = PricingContext.current.pricing_date`.
5. Branch: else (no base_date, no active PricingContext):
   a. Set `self.base_date = dt.date.today()`.
6. Normalize `self.base_date`: if it is a `dt.datetime` or `pd.Timestamp`, call `.date()` to extract the date portion. Otherwise keep as-is.

**Elixir porting note:** The `PricingContext.current.is_entered` check is GS-internal infrastructure for pricing context management. An Elixir port may use a process dictionary or explicit parameter passing instead.

---

### RelativeDate.apply_rule(self, currencies: List[Union[Currency, str]] = None, exchanges: List[Union[ExchangeCode, str]] = None, holiday_calendar: List[dt.date] = None, week_mask: str = '1111100', **kwargs) -> dt.date
Purpose: Apply the parsed rule components sequentially to produce a final date.

**Algorithm:**
1. `result = copy(self.base_date)` (shallow copy of date).
2. Iterate over `self._get_rules()`:
   a. For each rule string, call `self.__handle_rule(rule, result, week_mask, currencies=..., exchanges=..., holiday_calendar=..., **kwargs)`.
   b. Update `result` with the return value.
3. Return `result`.

**Parameters:**
- `currencies`: Holiday calendars by currency (GS internal).
- `exchanges`: Holiday calendars by exchange code.
- `holiday_calendar`: Explicit list of holiday dates (takes precedence over currencies/exchanges).
- `week_mask`: 7-character string; `'1'` = valid day, `'0'` = weekend. Default `'1111100'` (Mon-Fri valid, Sat-Sun weekend).
- `**kwargs`: Supports `roll_convention` (passed to rule handlers) and `usd_calendar`.

---

### RelativeDate._get_rules(self) -> List[str]
Purpose: Parse `self.rule` into individual rule components.

**Algorithm:**
1. Initialize `rule_list = []`, `current_rule = ''`.
2. Branch: `len(self.rule) == 0` -> raise `MqValueError('Invalid Rule ""')`.
3. Set `current_alpha = self.rule[0].isalpha()`.
4. Iterate character by character over `self.rule`:
   a. Compute `is_alpha = c.isalpha()`.
   b. Branch: `current_alpha and not is_alpha` (transition from alpha to non-alpha):
      - Branch: `current_rule.startswith('+')` -> append `current_rule[1:]` (strip leading `+`).
      - Branch: else -> append `current_rule` as-is.
      - Reset `current_rule = ''`, `current_alpha = False`.
   c. Branch: `is_alpha` -> set `current_alpha = True`.
   d. Append `c` to `current_rule`.
5. After loop, handle final `current_rule`:
   a. Branch: `current_rule.startswith('+')` -> append `current_rule[1:]`.
   b. Branch: else -> append `current_rule`.
6. Return `rule_list`.

**Parsing examples:**
- `"-1d"` -> `["-1d"]`
- `"3m"` -> `["3m"]`
- `"1y2b"` -> `["1y", "2b"]` (alpha `y` followed by digit `2` triggers split)
- `"1m+2b"` -> `["1m", "2b"]` (the `+` is stripped from `"+2b"`)
- `"0b"` -> `["0b"]`
- `"A1024"` -> `["A", "1024"]` (letter `A` then digits)

**Raises:** `MqValueError` when `self.rule` is an empty string.

---

### RelativeDate.__handle_rule(self, rule: str, result: dt.date, week_mask: str, currencies=None, exchanges=None, holiday_calendar=None, **kwargs) -> dt.date
Purpose: Parse a single rule component into a number and rule-letter, then dispatch to the appropriate rule handler class.

**Algorithm:**
1. Set `sign = "+"`.
2. Branch: `rule.startswith('-')` (negative rule):
   a. Walk `index` from 1 while `rule[index].isdigit()`.
   b. Branch: `index < len(rule)`:
      - `number = int(rule[1:index]) * -1`.
      - `rule_str = rule[index]` (the letter after the digits).
   c. Branch: `index == len(rule)` (all digits after `-`, e.g. `"-123"`):
      - `number = 0`.
      - `rule_str = rule[index]` -- **Bug note**: this would be `rule[len(rule)]` which is an IndexError. In practice, this branch is only reachable for strings like `"-123"` which won't match `DateRuleReg` in `_get_rules`, so it should not occur.
   d. Set `sign = "-"`.
3. Branch: rule does NOT start with `-`:
   a. Set `index = 0`.
   b. Branch: `rule[0]` is not a digit (starts with letter):
      - `rule_str = rule` (entire string is the rule, e.g. `"A"`, `"J"`).
      - `number = 0`.
   c. Branch: `rule[0]` is a digit:
      - Walk `index` while `rule[index].isdigit()`.
      - Branch: `index < len(rule)`:
        - `number = int(rule[0:index])`.
        - `rule_str = rule[index]` (letter after digits).
      - Branch: `index == len(rule)` (all digits, no letter):
        - `rule_str = rule` (entire string).
        - `number = 0`.
4. Branch: `not rule_str` -> raise `MqValueError(f'Invalid rule "{rule}"')`.
5. Get `roll = kwargs.get('roll_convention')`.
6. Try:
   a. Look up `rule_class = getattr(rules, f'{rule_str}Rule')`.
   b. Instantiate with all parameters and call `.handle()`.
   c. Return result.
7. Except `AttributeError`:
   a. Raise `NotImplementedError(f'Rule {rule} not implemented')`.

**Dispatched rule classes** (from `gs_quant.datetime.rules`):

| Rule Letter | Class | Behavior |
|-------------|-------|----------|
| `A` | `ARule` | Set to Jan 1 of year `number` |
| `b` | `bRule` | Business day offset with configurable roll |
| `d` | `dRule` | Calendar day offset |
| `e` | `eRule` | End of current month |
| `F` | `FRule` | Nth Friday of month |
| `g` | `gRule` | Week offset + business day adjust (backward) |
| `N` | `NRule` | Next/Nth Monday |
| `G` | `GRule` | Next/Nth Friday |
| `I` | `IRule` | Next/Nth Saturday |
| `J` | `JRule` | First day of current month |
| `k` | `kRule` | Year offset + skip weekends + business day adjust |
| `m` | `mRule` | Month offset + business day adjust (forward) |
| `M` | `MRule` | Nth Monday of month |
| `P` | `PRule` | Next/Nth Sunday |
| `r` | `rRule` | Dec 31 + year offset |
| `R` | `RRule` | Nth Thursday of month |
| `S` | `SRule` | Next/Nth Thursday |
| `T` | `TRule` | Nth Tuesday of month |
| `u` | `uRule` | Business day offset with sign-aware roll |
| `U` | `URule` | Next/Nth Tuesday |
| `v` | `vRule` | Month offset + end of month + business day adjust |
| `V` | `VRule` | Nth Saturday of month |
| `W` | `WRule` | Nth Wednesday of month |
| `w` | `wRule` | Week offset + business day adjust (direction depends on sign) |
| `x` | `xRule` | End of month + business day adjust (backward) |
| `X` | `XRule` | Next/Nth Wednesday |
| `y` | `yRule` | Year offset + skip weekends + business day adjust |
| `Z` | `ZRule` | Nth Sunday of month |

**Raises:**
- `MqValueError` when `rule_str` is empty.
- `NotImplementedError` when rule letter has no corresponding class.

---

### RelativeDate.as_dict(self) -> dict
Purpose: Serialize the RelativeDate to a dictionary.

**Algorithm:**
1. Initialize `rdate_dict = {'rule': self.rule}`.
2. Branch: `self.base_date_passed_in` is True:
   a. Add `'baseDate': str(self.base_date)`.
3. Return `rdate_dict`.

---

### RelativeDateSchedule.__init__(self, rule: str, base_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None)
Purpose: Initialize a schedule generator with a rule, base date, and optional end date.

**Algorithm:**
1. Set `self.rule = rule`.
2. Set `self.base_date_passed_in = False`.
3. Branch: `base_date` is truthy:
   a. Set `self.base_date = base_date`.
   b. Set `self.base_date_passed_in = True`.
4. Branch: `PricingContext.current.is_entered` is truthy:
   a. Get `pricing_date = PricingContext.current.pricing_date`.
   b. Normalize: if `pricing_date` is `dt.datetime` or `pd.Timestamp`, call `.date()`. Otherwise keep as-is.
   c. Set `self.base_date = pricing_date` (normalized).
5. Branch: else:
   a. Set `self.base_date = dt.date.today()`.
6. Set `self.end_date = end_date`.

**Difference from RelativeDate.__init__:** The datetime/Timestamp normalization happens inside the PricingContext branch here, whereas in `RelativeDate.__init__` it happens unconditionally after all branches.

---

### RelativeDateSchedule.apply_rule(self, currencies: List[Union[Currency, str]] = None, exchanges: List[Union[ExchangeCode, str]] = None, holiday_calendar: List[dt.date] = None, week_mask: str = '1111100', **kwargs) -> List[dt.date]
Purpose: Generate a schedule of dates by repeatedly applying the rule at increasing multiples.

**Algorithm:**
1. Initialize `i = 1`, `schedule = [self.base_date]`.
2. Loop (infinite `while True`):
   a. Construct scaled rule: `rule = f'{int(self.rule[:-1]) * i}{self.rule[-1]}'`.
      - E.g. for `self.rule = "1m"` and `i = 3`: `rule = "3m"`.
   b. Create `RelativeDate(rule, self.base_date)` and call `.apply_rule(currencies, exchanges, holiday_calendar, week_mask, **kwargs)`.
   c. Branch: `self.end_date is None or result > self.end_date`:
      - `break` (stop generating).
   d. Increment `i += 1`.
   e. Append `result` to `schedule`.
3. Return `schedule`.

**Key behavior:**
- Schedule always starts with `self.base_date`.
- Each subsequent date is computed from `self.base_date` (not from the previous date), using a multiplied rule.
- If `end_date is None`, the loop breaks on the first iteration (since `self.end_date is None` is always True), returning only `[self.base_date]`.
- The rule string is assumed to be of the form `<number><letter>` (e.g. `"1m"`, `"2w"`). `self.rule[:-1]` extracts the number, `self.rule[-1]` extracts the letter.

**Raises:** May raise if `int(self.rule[:-1])` fails (non-numeric prefix) or if the composed rule string is invalid.

---

### RelativeDateSchedule.as_dict(self) -> dict
Purpose: Serialize the schedule to a dictionary.

**Algorithm:**
1. Initialize `rdate_dict = {'rule': self.rule}`.
2. Branch: `self.base_date_passed_in` is True:
   a. Add `'baseDate': str(self.base_date)`.
3. Always add `'endDate': str(self.end_date)`.
4. Return `rdate_dict`.

**Note:** Unlike `RelativeDate.as_dict`, `endDate` is always included (even if `None`, producing `'None'`).

## State Mutation
- `self.base_date`: Set in `__init__`, never modified after construction (for both classes).
- `self.base_date_passed_in`: Set in `__init__`, never modified after construction.
- `self.rule`: Set in `__init__`, never modified after construction.
- `self.end_date`: Set in `__init__` (RelativeDateSchedule only), never modified.
- `result` in `apply_rule`: Local variable, mutated by each rule handler call sequentially.
- `result` in `__handle_rule`: Passed to the rule class, which may mutate its own copy (rule classes receive it as a parameter and may modify `self.result`).
- Thread safety: Depends on `PricingContext.current` thread-local behavior. Rule classes may fetch holiday calendars which involve network calls.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_get_rules` | `self.rule` is empty string |
| `MqValueError` | `__handle_rule` | `rule_str` is empty after parsing |
| `NotImplementedError` | `__handle_rule` | No rule class found for the rule letter (via `getattr` raising `AttributeError`) |
| `ValueError` (implicit) | `__handle_rule` | `int()` conversion fails on non-numeric portion of rule |

## Edge Cases
- Empty rule string `""` raises `MqValueError` in `_get_rules`.
- Rule `"0b"` is valid: means "offset 0 business days" (effectively snap to nearest business day).
- Rule `"0d"` is valid: means "offset 0 calendar days" (no-op, returns base_date).
- Rule with `+` prefix like `"+1d"` has the `+` stripped by `_get_rules`.
- `base_date` as `pd.Timestamp` or `dt.datetime` is normalized to `dt.date` in `__init__`.
- `PricingContext.current.is_entered` may be False outside of a `with PricingContext(...)` block, causing fallback to `dt.date.today()`.
- `PricingContext.current.pricing_date` may be a `dt.datetime` -- the normalization in `RelativeDate.__init__` handles this at the end (line 71-73), while `RelativeDateSchedule.__init__` handles it inline (line 227-229).
- `RelativeDateSchedule.apply_rule` with `end_date=None` returns `[self.base_date]` (single-element list) because the break condition `self.end_date is None` is immediately True.
- `RelativeDateSchedule.as_dict()` always includes `'endDate'` key, even when `end_date` is `None` (serialized as string `"None"`).
- Compound rules like `"1y2m3b"` are split into `["1y", "2m", "3b"]` and applied sequentially -- each rule operates on the result of the previous one.
- Case sensitivity matters: `"1b"` dispatches to `bRule`, `"1B"` dispatches to `BRule` (which does not exist, raising `NotImplementedError`). Only the exact letters listed in the rule dispatch table are valid.
- The `__handle_rule` method is name-mangled (`_RelativeDate__handle_rule`) due to double-underscore prefix, making it effectively private.

## Coverage Notes
- Branch count: ~25
- Key branches in `__init__`: 3 (explicit base_date, PricingContext, today fallback) + 1 (datetime/Timestamp normalization).
- Key branches in `_get_rules`: empty string check, alpha-to-non-alpha transition, `+` prefix stripping (x2: mid-loop and end-of-loop).
- Key branches in `__handle_rule`: starts with `-` vs not, digit-leading vs alpha-leading, `index < len(rule)` vs `index == len(rule)`, empty `rule_str` check, `AttributeError` catch.
- `RelativeDateSchedule.apply_rule`: `end_date is None` branch, `result > end_date` branch, normal append branch.
- Rule dispatch covers 29 rule classes (A, b, d, e, F, g, G, I, J, k, m, M, N, P, r, R, S, T, u, U, v, V, W, w, x, X, y, Z, plus the error case).
