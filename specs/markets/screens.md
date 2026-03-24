# screens.py

## Summary
Provides a credit bond screening framework with filter classes (`RangeFilter`, `CheckboxFilter`, `ScreenFilters`) and the main `Screen` class for constructing, calculating, saving, and deleting credit asset screens via the GS Screens and Asset Screener APIs. Includes enumerations for filter option values (sector, seniority, direction, currency, checkbox type).

## Dependencies
- Internal: `gs_quant.api.gs.screens` (GsScreenApi), `gs_quant.errors` (MqValueError), `gs_quant.target.assets_screener` (AssetScreenerCreditRequestFilters, AssetScreenerRequest, AssetScreenerRequestFilterLimits, AssetScreenerRequestStringOptions), `gs_quant.target.screens` (Screen as TargetScreen, ScreenParameters as TargetScreenParameters), `gs_quant.common` (Currency as CurrencyImport)
- External: `datetime` (dt), `logging`, `enum` (Enum, unique), `typing` (Union, Tuple), `pandas` (pd), `pydash` (set_, get)

**Note:** The import `ScreenParameters as TargetScreenParameters` from `gs_quant.target.screens` does not exist in the current target module. This will raise an `ImportError` at import time unless the class exists elsewhere or has been removed. This is a latent bug.

## Type Definitions

### RangeFilter (class)
Inherits: none

Represents asset filters that are numeric/string ranges with min and max bounds.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __min | `Union[float, str]` | `None` | Minimum bound of the range |
| __max | `Union[float, str]` | `None` | Maximum bound of the range |

### CheckboxFilter (class)
Inherits: none

Represents asset filters with multiple enumerated option selections.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __selections | `Tuple[Enum, ...]` | `None` | Selected enum values |
| __checkbox_type | `CheckboxType` | `None` | Include or Exclude mode |

### ScreenFilters (class)
Inherits: none

Container for all credit screen filter parameters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __face_value | `float` | `1000000` | Face value of the bond |
| __direction | `str` | `"Buy"` | Buy or Sell direction |
| __liquidity_score | `RangeFilter` | `RangeFilter()` | Liquidity score range (validated: 1-6) |
| __gs_charge_bps | `RangeFilter` | `RangeFilter()` | GS charge in bps (validated: 0-10) |
| __gs_charge_dollars | `RangeFilter` | `RangeFilter()` | GS charge in dollars (validated: 0-2) |
| __duration | `RangeFilter` | `RangeFilter()` | Bond duration (validated: 0-20) |
| __yield_ | `RangeFilter` | `RangeFilter()` | Yield (validated: 0-10) |
| __spread | `RangeFilter` | `RangeFilter()` | Spread to benchmark (validated: 0-1000) |
| __z_spread | `RangeFilter` | `RangeFilter()` | Zero volatility spread (no validation) |
| __g_spread | `RangeFilter` | `RangeFilter()` | G-spread (no validation) |
| __mid_price | `RangeFilter` | `RangeFilter()` | Mid price (validated: 0-200) |
| __maturity | `RangeFilter` | `RangeFilter()` | Maturity (validated: 0-40) |
| __amount_outstanding | `RangeFilter` | `RangeFilter()` | Amount outstanding (validated: 0-1000000000) |
| __rating | `RangeFilter` | `RangeFilter()` | S&P letter rating (no validation) |
| __seniority | `CheckboxFilter` | `CheckboxFilter()` | Seniority filter |
| __currency | `CheckboxFilter` | `CheckboxFilter()` | Currency filter |
| __sector | `CheckboxFilter` | `CheckboxFilter()` | Sector filter |

### Screen (class)
Inherits: none

Main class for creating and managing credit bond screens.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __filters | `ScreenFilters` | `ScreenFilters()` | Active screen filter configuration |
| __id | `str` | `None` | Screen ID (set on create or get) |
| __name | `str` | `f"Screen {date}"` | Screen name (defaults to "Screen DD-Mon-YYYY") |

## Enums and Constants

### CheckboxType(Enum) - @unique
| Value | Raw | Description |
|-------|-----|-------------|
| INCLUDE | `"Include"` | Include matching assets |
| EXCLUDE | `"Exclude"` | Exclude matching assets |

### Sector(Enum) - @unique
| Value | Raw | Description |
|-------|-----|-------------|
| COMMUNICATION_SERVICES | `"Communication Services"` | Communication services sector |
| CONSUMER_DISCRETIONARY | `"Consumer Discretionary"` | Consumer discretionary sector |
| CONSUMER_STAPLES | `"Consumer Staples"` | Consumer staples sector |
| ENERGY | `"Energy"` | Energy sector |
| FINANCIALS | `"Financials"` | Financials sector |
| HEALTH_CARE | `"Health Care"` | Health care sector |
| INDUSTRIALS | `"Industrials"` | Industrials sector |
| INFORMATION_TECHNOLOGY | `"Information Technology"` | Information technology sector |
| MATERIALS | `"Materials"` | Materials sector |
| REAL_ESTATE | `"Real Estate"` | Real estate sector |
| UTILITIES | `"Utilities"` | Utilities sector |

### Seniority(Enum) - @unique
| Value | Raw | Description |
|-------|-----|-------------|
| JUNIOR_SUBORDINATE | `"Junior Subordinate"` | Junior subordinate seniority |
| SENIOR | `"Senior"` | Senior seniority |
| SENIOR_SUBORDINATE | `"Senior Subordinate"` | Senior subordinate seniority |
| SUBORDINATE | `"Subordinate"` | Subordinate seniority |

### Direction(Enum) - @unique
| Value | Raw | Description |
|-------|-----|-------------|
| BUY | `"Buy"` | Buy direction |
| SELL | `"Sell"` | Sell direction |

### Currency(CurrencyImport, Enum) - @unique
Inherits all members from `gs_quant.common.Currency` (which itself inherits from `EnumBase`). This is a pass-through subclass with no additional members.

### Module-level Configuration
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `logging.root.setLevel('INFO')` | side effect | N/A | Sets root logger to INFO level at module import time |

## Functions/Methods

### RangeFilter.__init__(self, min_: Union[float, str] = None, max_: Union[float, str] = None)
Purpose: Initialize a range filter with optional min/max bounds.

**Algorithm:**
1. Set `self.__min = min_`.
2. Set `self.__max = max_`.

### RangeFilter.__str__(self) -> str
Purpose: Return string representation of the range.

**Algorithm:**
1. Return `f'{{Min: {self.min}, Max: {self.max}}}'`.

### RangeFilter.min (property, getter/setter)
Purpose: Get/set the minimum bound.

### RangeFilter.max (property, getter/setter)
Purpose: Get/set the maximum bound.

### CheckboxFilter.__init__(self, checkbox_type: CheckboxType = None, selections: Tuple[Enum, ...] = None)
Purpose: Initialize a checkbox filter with type and selections.

**Algorithm:**
1. Set `self.__selections = selections`.
2. Set `self.__checkbox_type = checkbox_type`.

### CheckboxFilter.__str__(self) -> str
Purpose: Return string representation.

**Algorithm:**
1. Return `f'{{Type: {self.checkbox_type}, Selections: {self.selections}}}'`.

### CheckboxFilter.checkbox_type (property, getter/setter)
Purpose: Get/set the checkbox type (Include/Exclude).

### CheckboxFilter.selections (property, getter/setter)
Purpose: Get/set the selected enum values.

### CheckboxFilter.add(self, new_selections: Tuple[Enum, ...])
Purpose: Add new selections to the existing selections set.

**Algorithm:**
1. Convert `new_selections` to a `set`.
2. Convert `self.selections` to a `set`.
3. Set `self.selections = tuple(set(new_selections).union(set(old_selections)))`.

Note: Re-converts `new_selections` and `old_selections` to sets redundantly (they were already converted in steps 1-2). The `old_selections` variable from step 2 is not used; instead `set(self.selections)` effectively re-reads via the property.

### CheckboxFilter.remove(self, remove_selections: Tuple[Enum, ...])
Purpose: Remove selections from the existing selections set.

**Algorithm:**
1. Convert `remove_selections` to a `set`.
2. Convert `self.selections` to a `set`.
3. Set `self.selections = tuple(old_selections.difference(remove_selections))`.

Note: Same redundancy -- `remove_selections` is converted to set in step 1 but `.difference()` on step 3 uses the original `remove_selections` variable (which is already a set from step 1).

### ScreenFilters.__init__(self, face_value: float = 1000000, direction: str = "Buy", liquidity_score: RangeFilter = RangeFilter(), gs_charge_bps: RangeFilter = RangeFilter(), gs_charge_dollars: RangeFilter = RangeFilter(), duration: RangeFilter = RangeFilter(), yield_: RangeFilter = RangeFilter(), spread: RangeFilter = RangeFilter(), z_spread: RangeFilter = RangeFilter(), g_spread: RangeFilter = RangeFilter(), mid_price: RangeFilter = RangeFilter(), maturity: RangeFilter = RangeFilter(), amount_outstanding: RangeFilter = RangeFilter(), letter_rating: RangeFilter = RangeFilter(), seniority: CheckboxFilter = CheckboxFilter(), currency: CheckboxFilter = CheckboxFilter(), sector: CheckboxFilter = CheckboxFilter())
Purpose: Initialize all screen filter parameters with defaults.

**Algorithm:**
1. Assign all 17 parameters to corresponding private fields.

**Note:** Uses mutable default arguments (`RangeFilter()`, `CheckboxFilter()`). Since these are class instances (not lists/dicts), each call creates a new instance -- but if caller passes no argument, Python evaluates the default once at function definition time. In practice this is safe because `RangeFilter()` and `CheckboxFilter()` return new objects each time.

### ScreenFilters.__str__(self) -> str
Purpose: Return string representation of all non-falsy filters.

**Algorithm:**
1. Initialize empty dict `to_return`.
2. Iterate all keys in `self.__dict__`.
3. Branch: if `self.__dict__[name]` is truthy -> add `name: str(filter)` to `to_return`.
4. Return `str(to_return)`.

### ScreenFilters property getters/setters

Each of the 17 filter fields has a property getter and setter. The following setters include validation:

| Property | Setter Validation | Range |
|----------|-------------------|-------|
| liquidity_score | `__validate_range_settings(min_=1, max_=6, ...)` | 1-6 |
| gs_charge_bps | `__validate_range_settings(min_=0, max_=10, ...)` | 0-10 |
| gs_charge_dollars | `__validate_range_settings(min_=0, max_=2, ...)` | 0-2 |
| duration | `__validate_range_settings(min_=0, max_=20, ...)` | 0-20 |
| yield_ | `__validate_range_settings(min_=0, max_=10, ...)` | 0-10 |
| spread | `__validate_range_settings(min_=0, max_=1000, ...)` | 0-1000 |
| mid_price | `__validate_range_settings(min_=0, max_=200, ...)` | 0-200 |
| maturity | `__validate_range_settings(min_=0, max_=40, ...)` | 0-40 |
| amount_outstanding | `__validate_range_settings(min_=0, max_=1000000000, ...)` | 0-1B |

The following setters have NO validation: `face_value`, `direction`, `z_spread`, `g_spread`, `rating`, `seniority`, `currency`, `sector`.

**Bug in validated setters:** The validation call passes `value=self.__<field>` (the OLD value before assignment) rather than `value=value` (the NEW value being set). This means validation is applied to the *current* stored value, not the incoming value. The new value is then unconditionally assigned regardless of validation outcome.

### ScreenFilters.__validate_range_settings(min_: int, max_: int, value: RangeFilter) -> None (static)
Purpose: Validate that a RangeFilter's min/max fall within allowed bounds.

**Algorithm:**
1. Branch: if `value.min is None and value.max is None` -> return (skip validation).
2. Branch: if `value.min < min_` or `value.max > max_` -> raise `MqValueError`.

**Bug:** The error message uses `{min}` and `{max}` (Python builtins) instead of `{min_}` and `{max_}` (the function parameters). This will display `<built-in function min>` and `<built-in function max>` in the error message.

**Raises:** `MqValueError` when range bounds are violated (with buggy message).

**Edge case:** If only one of `min`/`max` is `None`, step 2 will attempt `None < min_` which raises `TypeError` in Python 3.

### Screen.__init__(self, filters: ScreenFilters = None, screen_id: str = None, name: str = None)
Purpose: Initialize a Screen with filters, optional ID, and name.

**Algorithm:**
1. Branch: if `not filters` (falsy/None) -> create new `ScreenFilters()`.
2. Branch: else -> use provided `filters`.
3. Set `self.__id = screen_id`.
4. Branch: if `name is not None` -> use provided name.
5. Branch: else -> generate name as `f"Screen {dt.date.today().strftime('%d-%b-%Y')}"`.

### Screen.id (property, read-only)
Purpose: Get the screen ID.

### Screen.name (property, getter/setter)
Purpose: Get/set the screen name.

### Screen.filters (property, getter/setter)
Purpose: Get/set the screen filters.

### Screen.get(cls, screen_id: str) -> Screen (classmethod)
Purpose: Retrieve a screen from the API by ID.

**Algorithm:**
1. Call `GsScreenApi.get_screen(screen_id=screen_id)` to get a target `Screen` object.
2. Call `Screen.__from_target(screen)` to convert to the local `Screen` class.
3. Return result.

### Screen.calculate(self, format_: str = None) -> Union[pd.DataFrame, str]
Purpose: Apply screen filters and return matching assets.

**Algorithm:**
1. Call `self.__to_target_filters()` to convert filters to API format.
2. Create `AssetScreenerRequest(filters=filters)`.
3. Call `GsScreenApi.calculate(payload)`.
4. Wrap result in `pd.DataFrame(assets)`.
5. Branch: if `format_ == 'json'` -> return `dataframe['results'].to_json(indent=4)`.
6. Branch: if `format_ == 'csv'` -> return `dataframe.to_csv()`.
7. Branch: else (default, including `format_=None`) -> return the DataFrame.

### Screen.save(self)
Purpose: Create or update a screen via the API.

**Algorithm:**
1. Call `self.__to_target_parameters()` to convert filters to target parameters.
2. Create `TargetScreen(name=self.name, parameters=parameters)`.
3. Branch: if `self.id` is truthy (screen already exists):
   a. Set `target_screen.id = self.id`.
   b. Call `GsScreenApi.update_screen(target_screen)`.
4. Branch: else (new screen):
   a. Call `GsScreenApi.create_screen(target_screen)`.
   b. Set `self.__id = screen.id` from the response.
   c. Log info with new screen ID.

**Note:** `save()` references `TargetScreenParameters` which does not exist in the target module. This method will fail at runtime.

### Screen.delete(self)
Purpose: Delete the screen via the API.

**Algorithm:**
1. Call `GsScreenApi.delete_screen(self.id)`.

### Screen.__from_target(cls, screen) -> Screen (classmethod, private)
Purpose: Convert a target API Screen object to a local Screen instance.

**Algorithm:**
1. Return `Screen(filters=screen.parameters, screen_id=screen.id, name=screen.name)`.

**Note:** Passes `screen.parameters` directly as `filters`, which would be a `TargetScreenParameters` object, not a `ScreenFilters` instance. This is a type mismatch that may cause runtime issues.

### Screen.__to_target_filters(self) -> AssetScreenerCreditRequestFilters
Purpose: Convert local ScreenFilters to API filter format.

**Algorithm:**
1. Initialize empty `payload` dict.
2. Call `self.__set_up_filters()` to build filter dict.
3. For each filter name:
   a. Branch: if `name == 'face_value'` or `name == 'direction'` -> copy value directly.
   b. Branch: elif value is `RangeFilter` -> convert to `AssetScreenerRequestFilterLimits(min_=..., max_=...)`.
   c. Branch: elif value is `CheckboxFilter`:
      - Branch: if `selections` and `checkbox_type` are both truthy -> convert to `AssetScreenerRequestStringOptions(options=selections, type_=checkbox_type)`.
      - Branch: else -> skip (checkbox without selections or type is omitted).
4. Return `AssetScreenerCreditRequestFilters(**payload)`.

### Screen.__set_up_filters(self) -> dict
Purpose: Extract filter values matching `AssetScreenerCreditRequestFilters` properties.

**Algorithm:**
1. Initialize empty `filters` dict.
2. For each property name from `AssetScreenerCreditRequestFilters.properties()`:
   a. Call `pydash.set_(filters, prop, pydash.get(self.__filters, prop))`.
3. Return `filters`.

**Note:** Uses `pydash.get` on a `ScreenFilters` object, which will attempt attribute access. The filter property names from the API class must match the attribute names accessible via `pydash.get` on `ScreenFilters`.

### Screen.__to_target_parameters(self) -> TargetScreenParameters
Purpose: Convert local ScreenFilters to target screen parameters for save.

**Algorithm:**
1. Initialize empty `payload` dict.
2. Call `self.__set_up_parameters()` to build parameter dict.
3. For each parameter name:
   a. Branch: if `name == 'face_value'` or `name == 'direction'` -> copy value directly.
   b. Branch: elif value is `RangeFilter` -> convert to `AssetScreenerRequestFilterLimits(min_=..., max_=...)`.
   c. Branch: elif value is `CheckboxFilter`:
      - Branch: if `selections` and `checkbox_type` are both truthy -> set `payload[name] = selections` (tuple of Enum values, NOT `AssetScreenerRequestStringOptions`).
      - Branch: else -> skip.
4. Return `TargetScreenParameters(**payload)`.

**Note:** Unlike `__to_target_filters`, this method puts raw `selections` tuples into the payload for checkbox filters rather than wrapping in `AssetScreenerRequestStringOptions`.

### Screen.__set_up_parameters(self) -> dict
Purpose: Map API parameter names to local filter properties and extract values.

**Algorithm:**
1. Define `filter_to_parameter` dict mapping 17 API property names to local `ScreenFilters` property names. Key mappings include:
   - `'gs_liquidity_score'` -> `'liquidity_score'`
   - `'modified_duration'` -> `'duration'`
   - `'yield_to_convention'` -> `'yield_'`
   - `'spread_to_benchmark'` -> `'spread'`
   - `'bval_mid_price'` -> `'mid_price'`
   - `'rating_standard_and_poors'` -> `'rating'`
   - `'issue_date'` -> `'issue_date'` (note: `ScreenFilters` has no `issue_date` property)
2. Initialize empty `parameters` dict.
3. For each property name from `TargetScreenParameters.properties()`:
   a. Look up the local filter name via `filter_to_parameter[prop]`.
   b. Call `pydash.set_(parameters, prop, pydash.get(self.__filters, filter_to_parameter[prop]))`.
4. Return `parameters`.

**Bug:** `'issue_date'` is mapped to itself but `ScreenFilters` has no `issue_date` attribute, so `pydash.get` will return `None` for this key.

## State Mutation
- `RangeFilter.__min`, `RangeFilter.__max`: Mutable via property setters.
- `CheckboxFilter.__selections`, `CheckboxFilter.__checkbox_type`: Mutable via property setters and `add()`/`remove()` methods.
- `ScreenFilters` fields: All 17 fields mutable via property setters, with some setters calling `__validate_range_settings`.
- `Screen.__id`: Set in `__init__`, updated by `save()` when creating a new screen.
- `Screen.__name`: Mutable via property setter.
- `Screen.__filters`: Mutable via property setter.
- Module-level side effect: `logging.root.setLevel('INFO')` is executed at import time, modifying global logging configuration.
- Thread safety: No locks or synchronization; not thread-safe for concurrent mutation.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `ScreenFilters.__validate_range_settings` | When `value.min < min_` or `value.max > max_` |
| `TypeError` | `ScreenFilters.__validate_range_settings` | When only one of min/max is `None` (implicit, comparing `None < int`) |
| `ImportError` | module import | `ScreenParameters` does not exist in `gs_quant.target.screens` |
| `KeyError` | `Screen.__set_up_parameters` | If `TargetScreenParameters.properties()` returns a key not in `filter_to_parameter` |

## Edge Cases
- `RangeFilter` default constructor creates a filter with both `min=None` and `max=None`, which passes validation (early return).
- `CheckboxFilter.add()` and `remove()` will raise `TypeError` if `self.selections` is `None` (converting `None` to a set fails).
- `ScreenFilters` property setters validate the OLD value, not the NEW value being set. This means:
  - First assignment always "validates" the default `RangeFilter()` (which has None/None, so validation returns early).
  - The invalid value is always stored regardless of what the old value was.
- `ScreenFilters.__validate_range_settings` error message uses `{min}` and `{max}` (builtins) instead of `{min_}` and `{max_}` (parameters) -- produces unhelpful error text.
- `Screen.__init__` with `filters=None` creates a default `ScreenFilters()`. With `filters=` any falsy value (e.g., `0`, `False`, `[]`) also creates defaults.
- `Screen.__from_target` passes `screen.parameters` as `filters`, but `parameters` is a `TargetScreenParameters` (or potentially `ScreenerQueryBuilder.filters`), not a `ScreenFilters` -- type mismatch.
- `Screen.calculate()` checks `format_` with string equality; any value other than `'json'` or `'csv'` returns the raw DataFrame.
- `Screen.delete()` does not clear `self.__id` after deletion; the screen object retains the stale ID.
- `Screen.save()` for update does not capture or return the API response.
- Module sets `logging.root.setLevel('INFO')` at import time, which is a global side effect that affects all loggers in the application.

## Bugs Found
- Line 362: `__validate_range_settings` error message uses `{min}` and `{max}` (Python builtins) instead of `{min_}` and `{max_}` (function parameters). (OPEN)
- Lines 217, 227, 237, 246, 257, 268, 297, 307, 318: All validated setters call `__validate_range_settings` with the OLD value (`self.__field`) instead of the incoming `value` parameter. (OPEN)
- Line 359-361: `__validate_range_settings` will raise `TypeError` if only one of `min`/`max` is `None` due to `None < int` comparison in Python 3. (OPEN)
- Line 33: Import of `ScreenParameters as TargetScreenParameters` from `gs_quant.target.screens` -- class does not exist in that module. (OPEN)
- Line 489: `'issue_date'` maps to itself in `filter_to_parameter` but `ScreenFilters` has no `issue_date` property. (OPEN)

## Coverage Notes
- Branch count: ~35
- Key branches in `Screen.__init__`: filters falsy/truthy (2), name None/provided (2).
- Key branches in `Screen.calculate`: format_ == 'json' (2), format_ == 'csv' (2), default (1).
- Key branches in `Screen.save`: self.id truthy/falsy (2).
- Key branches in `__to_target_filters`: per-filter type dispatch face_value/direction (2), RangeFilter (2), CheckboxFilter with valid selections (2), CheckboxFilter without (2).
- Key branches in `__to_target_parameters`: same structure as `__to_target_filters` (same count).
- Key branches in `__validate_range_settings`: both None (2), out of range (2).
- Key branches in `ScreenFilters.__str__`: per-filter truthy check (2 per filter x 17 filters).
- Pragmas: none.
