# json_convertors.py

## Summary
Core JSON serialization/deserialization module providing encoder and decoder functions for date, datetime, time, float, instrument, portfolio, pandas Series, quote report, and polymorphic dataclass types. These functions are used as `dataclasses-json` field-level `config()` encoders/decoders throughout the codebase, and form the foundation of all wire-format conversions.

## Dependencies
- Internal: `gs_quant.instrument` (Instrument) -- lazy import; `gs_quant.markets.portfolio` (Portfolio) -- lazy import; `gs_quant.quote_reports.core` (quote_report_from_dict, quote_reports_from_dicts, custom_comment_from_dict, custom_comments_from_dicts, hedge_type_from_dict, hedge_type_from_dicts) -- lazy imports
- External: `datetime` (date, datetime, time, timedelta), `re`, `dataclasses` (MISSING, fields), `typing`, `pandas` (Series), `dataclasses_json` (config), `dateutil.parser` (isoparse)

## Type Definitions

### DateOrDateTime (TypeAlias)
```
DateOrDateTime = Union[dt.date, dt.datetime]
```

### Module-level config objects
```python
optional_datetime_config = config(encoder=optional_to_isodatetime, decoder=optional_from_isodatetime)
optional_date_config = config(encoder=encode_date_or_str, decoder=decode_optional_date)
```
These are `dataclasses-json` field config objects intended to be used in `field(metadata=...)` declarations on dataclass fields.

## Enums and Constants

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `__valid_date_formats` | `tuple[str, ...]` | `('%Y-%m-%d', '%d%b%y', '%d%b%Y', '%d-%b-%y', '%d/%m/%Y')` | Ordered list of date string formats tried during parsing |

**Date format details (critical for Elixir port):**

| Format | Example | strptime Pattern | Notes |
|--------|---------|------------------|-------|
| `'%Y-%m-%d'` | `'2020-07-28'` | ISO 8601 date | Most common format |
| `'%d%b%y'` | `'28Jul20'` | Day + abbreviated month + 2-digit year | No separators |
| `'%d%b%Y'` | `'28Jul2020'` | Day + abbreviated month + 4-digit year | No separators |
| `'%d-%b-%y'` | `'28-Jul-20'` | Day-abbreviated month-2-digit year | Dash-separated |
| `'%d/%m/%Y'` | `'28/07/2020'` | Day/month/4-digit year | Slash-separated |

## Functions/Methods

### encode_date_or_str(value: Optional[Union[str, dt.date]]) -> Optional[str]
Purpose: Encode a date or string value for JSON serialization. If it is a `dt.date` (or `dt.datetime`, since datetime is a subclass of date), call `.isoformat()`; otherwise pass through the string (or None) unchanged.

**Algorithm:**
1. Branch: `isinstance(value, dt.date)` is True (covers both `date` and `datetime`) -> return `value.isoformat()`
2. Branch: value is a `str` or `None` -> return value as-is

**Elixir note:** `dt.datetime` is a subclass of `dt.date`, so `isinstance(value, dt.date)` catches both. In Elixir, pattern match on `%DateTime{}` before `%Date{}` or use a single `Date.to_iso8601/1` / `DateTime.to_iso8601/1`.

---

### decode_optional_date(value: Optional[str]) -> Optional[dt.date]
Purpose: Decode a JSON value to an optional `dt.date`.

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Branch: `isinstance(value, dt.date)` -> return value (already decoded; compatibility with dataclasses-json >=0.6.5)
3. Branch: `isinstance(value, str)` -> call `__try_decode_valid_date_formats(value)`
   - Sub-branch: result is not None -> return the parsed date
   - Sub-branch: result is None -> fall through to raise
4. Raise `ValueError(f'Cannot convert {value} to date')` -- reached when value is a non-str/non-date/non-None type or when no format matches a string

**Raises:** `ValueError` when no format matches or unsupported type.

---

### decode_optional_time(value: Optional[str]) -> Optional[dt.time]
Purpose: Decode a JSON value to an optional `dt.time`.

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Branch: `isinstance(value, dt.time)` -> return value (already decoded; compatibility)
3. Branch: `isinstance(value, str)` -> return `dt.time.fromisoformat(value)`
4. Raise `ValueError(f'Cannot convert {value} to date')` -- note: error message says "date" but this is a time decoder (likely a copy-paste bug in the original)

**Raises:** `ValueError` when unsupported type.

---

### encode_optional_time(value: Optional[Union[str, dt.time]]) -> Optional[str]
Purpose: Encode a `dt.time` or passthrough a string.

**Algorithm:**
1. Branch: `isinstance(value, dt.time)` -> return `value.isoformat()`
2. Branch: else (str or None) -> return value as-is

---

### decode_date_tuple(blob: Tuple[str, ...]) -> Optional[Tuple[dt.date, ...]]
Purpose: Decode a tuple/list of date strings into a tuple of dates.

**Algorithm:**
1. Branch: `isinstance(blob, (tuple, list))` -> return `tuple(decode_optional_date(s) for s in blob)`
2. Branch: else -> return `None`

---

### encode_date_tuple(values: Tuple[Optional[Union[str, dt.date]], ...]) -> Optional[Tuple]
Purpose: Encode a tuple of date/string values to a tuple of ISO strings.

**Algorithm:**
1. Branch: `values is not None` -> map each element:
   - Sub-branch: `isinstance(value, (str, dt.date))` -> `encode_date_or_str(value)`
   - Sub-branch: else -> `None`
2. Branch: `values is None` -> return `None`

---

### decode_iso_date_or_datetime(value: Any) -> Union[Tuple[DateOrDateTime, ...], DateOrDateTime]
Purpose: Decode a value that could be a date string, datetime string, or a list/tuple thereof.

**Algorithm:**
1. Branch: `isinstance(value, (tuple, list))` -> recursively decode each element, return as tuple
2. Branch: `isinstance(value, (dt.date, dt.datetime))` -> return value as-is
3. Branch: `isinstance(value, str)`:
   - Sub-branch: `len(value) == 10` -> call `decode_optional_date(value)` (assumes date-only ISO format)
   - Sub-branch: else -> call `optional_from_isodatetime(value)` (assumes datetime)
4. Raise `TypeError` for any other type

**Raises:** `TypeError` when value is not str, date, datetime, list, or tuple.

**Elixir note:** The length-10 heuristic distinguishes `"2020-07-28"` (date) from `"2020-07-28T12:00:00Z"` (datetime). In Elixir, use pattern matching or `String.length/1`.

---

### optional_from_isodatetime(datetime: Union[str, dt.datetime, None]) -> Optional[dt.datetime]
Purpose: Parse an ISO datetime string, stripping trailing 'Z'.

**Algorithm:**
1. Branch: `datetime is None` -> return `None`
2. Branch: `isinstance(datetime, dt.datetime)` -> return as-is
3. Branch: str -> `dt.datetime.fromisoformat(datetime.replace('Z', ''))` -- strips the 'Z' UTC indicator before parsing

**Elixir note:** Python's `fromisoformat` does not handle 'Z' suffix (prior to 3.11), hence the replace. Elixir's `DateTime.from_iso8601/1` handles 'Z' natively.

---

### optional_to_isodatetime(datetime: Optional[dt.datetime]) -> Optional[str]
Purpose: Encode a datetime to ISO 8601 with seconds precision and trailing 'Z'.

**Algorithm:**
1. Branch: `datetime is not None` -> return `f'{dt.datetime.isoformat(datetime, timespec="seconds")}Z'`
2. Branch: `datetime is None` -> return `None`

**Output format:** `"2020-07-28T12:00:00Z"` -- always seconds precision, always 'Z' suffix.

---

### decode_dict_date_key(value) -> Optional[Dict[dt.date, Any]]
Purpose: Decode a dict whose keys are ISO date strings into a dict with `dt.date` keys.

**Algorithm:**
1. Branch: `value is not None` -> `{dt.date.fromisoformat(d): v for d, v in value.items()}`
2. Branch: `value is None` -> return `None`

---

### decode_dict_date_key_or_float(value) -> Optional[Union[Dict[dt.date, Any], float, str]]
Purpose: Decode a value that is either a dict with date keys or a float/string.

**Algorithm:**
1. Branch: `value is not None`:
   - Sub-branch: `isinstance(value, dict)` -> `decode_dict_date_key(value)`
   - Sub-branch: else -> `decode_float_or_str(value)`
2. Branch: `value is None` -> return `None`

---

### decode_dict_dict_date_key(value) -> Optional[Dict[str, Optional[Dict[dt.date, Any]]]]
Purpose: Decode a nested dict where inner dicts have date-string keys.

**Algorithm:**
1. Branch: `value is not None` -> for each `(k, val)` in `value.items()`:
   - Sub-branch: `val is not None` -> `{dt.date.fromisoformat(d): v for d, v in val.items()}`
   - Sub-branch: `val is None` -> `None`
2. Branch: `value is None` -> return `None`

---

### decode_dict_date_value(value) -> Optional[Dict[str, dt.date]]
Purpose: Decode a dict whose values are ISO date strings into `dt.date` values.

**Algorithm:**
1. Branch: `value is not None` -> `{k: dt.date.fromisoformat(d) for k, d in value.items()}`
2. Branch: `value is None` -> return `None`

---

### decode_datetime_tuple(blob: Tuple[str, ...]) -> Optional[Tuple[dt.datetime, ...]]
Purpose: Decode a tuple of ISO datetime strings to datetimes.

**Algorithm:**
1. Branch: `isinstance(blob, (tuple, list))` -> `tuple(optional_from_isodatetime(s) for s in blob)`
2. Branch: else -> return `None`

---

### __try_decode_valid_date_formats(value: str) -> Optional[dt.date]
Purpose: Private helper. Attempt to parse a string against each format in `__valid_date_formats`.

**Algorithm:**
1. Iterate over `__valid_date_formats` in order
2. For each format: try `dt.datetime.strptime(value, fmt).date()`
   - Branch: success -> return the date
   - Branch: `ValueError` -> continue to next format
3. If no format matches -> return `None`

**Elixir note:** Implement as a list of `{format, parser_fn}` tuples and try each. The `strptime` patterns map to: Timex format strings or custom parsers with NimbleParsec/regex.

---

### decode_date_or_str(value: Union[dt.date, float, str]) -> Optional[Union[dt.date, str]]
Purpose: Decode a value that could be a date, Excel serial date number, or string (tenor like 'ATM', '3m').

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Branch: `isinstance(value, dt.date)` -> return value
3. Branch: `isinstance(value, float)` -> Excel serial date conversion:
   - Sub-branch: `value > 59` -> `value -= 1` (Excel leap year bug: 1900 is not a leap year, Excel thinks it is)
   - Compute: `(dt.datetime(1899, 12, 31) + dt.timedelta(days=value)).date()`
   - Return the computed date
4. Branch: `isinstance(value, str)` -> try `__try_decode_valid_date_formats(value)`
   - Sub-branch: parsed successfully -> return the date
   - Sub-branch: not parsed -> return the original string (assumed to be a tenor like 'ATM', '1y', '3m')
5. Raise `TypeError` for any other type

**Raises:** `TypeError` when unsupported type.

**Elixir note:** The Excel date epoch is December 31, 1899. The leap year bug at day 60 (Feb 29, 1900) must be replicated: if serial > 59, subtract 1 before adding to epoch.

---

### encode_datetime(value: Optional[dt.datetime]) -> Optional[str]
Purpose: Encode a datetime to ISO 8601 with millisecond precision.

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Try: `value.isoformat(timespec='milliseconds')`
   - Branch: success -> `iso_formatted` is the result
   - Branch: `TypeError` -> Pandas `Timestamp` objects do not accept `timespec`, call `value.isoformat()` without it
3. Branch: `value.tzinfo` is truthy -> return `iso_formatted` as-is (already has timezone info)
4. Branch: `value.tzinfo` is falsy -> append `'Z'` and return

**Output format examples:**
- Naive datetime: `"2020-07-28T12:00:00.000Z"`
- Timezone-aware datetime: `"2020-07-28T12:00:00.000+00:00"` (no 'Z' appended)
- Pandas Timestamp: `"2020-07-28T12:00:00Z"` (no milliseconds if TypeError)

---

### decode_datetime(value: Optional[Union[int, str]]) -> Optional[dt.datetime]
Purpose: Decode a datetime from an integer (epoch millis) or ISO string.

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Branch: `isinstance(value, dt.datetime)` -> return as-is
3. Branch: `isinstance(value, int)` -> `dt.datetime.fromtimestamp(value / 1000)` (epoch milliseconds to local datetime)
4. Branch: `isinstance(value, str)`:
   - Regex search: `\\.([0-9]*)Z$` (sub-second digits before trailing Z)
   - Branch: match found AND `len(sub_seconds) > 6` -> truncate sub-seconds to 6 digits (microsecond precision max), replace in string
   - Branch: no match or <=6 digits -> use string as-is
   - Call `isoparse(value)` (from `dateutil.parser`)
5. Raise `TypeError` for any other type

**Raises:** `TypeError` when unsupported type.

**Elixir note:** The sub-second truncation handles nanosecond-precision timestamps from some APIs. Elixir's `DateTime` supports microseconds natively. For epoch millis, use `DateTime.from_unix(value, :millisecond)`.

---

### decode_float_or_str(value: Optional[Union[float, int, str]]) -> Optional[Union[float, str]]
Purpose: Coerce a value to float if possible, otherwise keep as string.

**Algorithm:**
1. Branch: `value is None` -> return `None`
2. Branch: `isinstance(value, float)` -> return value
3. Branch: `isinstance(value, int)` -> return `float(value)`
4. Branch: `isinstance(value, str)`:
   - Try: `float(value)` -> return the float
   - Except `ValueError`: return the original string (assumed to be a tenor/label like 'ATM')
5. Raise `TypeError` for any other type

**Raises:** `TypeError` when unsupported type.

---

### decode_instrument(value: Optional[Dict]) -> Optional[Instrument]
Purpose: Decode a dict to an Instrument instance.

**Algorithm:**
1. Branch: `value` is truthy -> `Instrument.from_dict(value)` (lazy import)
2. Branch: `value` is falsy (None, empty dict, etc.) -> return `None`

---

### decode_named_instrument(value: Optional[Union[Iterable[Dict], dict]]) -> Optional[Union[Tuple, Instrument, Portfolio]]
Purpose: Decode a value that could be a single instrument dict, a portfolio dict (has `portfolio_name` key), or a list/tuple of such dicts. Recursive.

**Algorithm:**
1. Branch: `isinstance(value, (list, tuple))` -> recursively decode each element, return as tuple
2. Branch: `isinstance(value, dict) and 'portfolio_name' in value.keys()` -> `decode_named_portfolio(value)`
3. Branch: `value` is truthy -> `Instrument.from_dict(value)` (lazy import)
4. Branch: `value` is falsy -> return `None`

---

### decode_named_portfolio(value) -> Portfolio
Purpose: Decode a portfolio dict to a Portfolio object.

**Algorithm:**
1. Decode each element in `value['instruments']` via `decode_named_instrument(v)` (recursive)
2. Create `Portfolio(instruments, name=value['portfolio_name'])`

**Expected dict structure:** `{'portfolio_name': str, 'instruments': [...]}`

---

### encode_named_instrument(obj) -> Union[Tuple, dict]
Purpose: Encode an instrument or portfolio to a dict. Recursive.

**Algorithm:**
1. Branch: `isinstance(obj, (list, tuple))` -> recursively encode each element, return as tuple
2. Branch: `isinstance(obj, Portfolio)` -> `encode_named_portfolio(obj)` (lazy import)
3. Branch: else -> `obj.as_dict()`

---

### encode_named_portfolio(obj) -> dict
Purpose: Encode a Portfolio to a dict.

**Algorithm:**
1. Return `{'portfolio_name': obj.name, 'instruments': tuple(encode_named_instrument(o) for o in obj.all_instruments)}`

---

### encode_pandas_series(obj) -> dict
Purpose: Encode a `pd.Series` to a dict, converting date/datetime keys to ISO strings.

**Algorithm:**
1. Convert series to dict via `pd.Series.to_dict(obj)`
2. Check the first key: `isinstance(next(iter(series_dict)), (dt.date, dt.datetime))`
   - Branch: True -> re-key dict with `.isoformat()` on each key
   - Branch: False -> use dict as-is
3. Return the dict

**Elixir note:** In Elixir, this maps to encoding a map. Check key types and convert `Date`/`DateTime` keys to ISO strings.

---

### decode_pandas_series(value: dict) -> pd.Series
Purpose: Decode a dict to a `pd.Series`, parsing keys as dates/datetimes.

**Algorithm:**
1. For each `(k, v)` in `value.items()`, decode key via `decode_iso_date_or_datetime(k)`
2. Construct `pd.Series(dated_dict)`

**Elixir note:** This maps to a map with date/datetime keys. No direct Series equivalent; use a list of `{date, value}` tuples or a map.

---

### decode_quote_report(value: Optional[dict]) -> Optional[QuoteReport]
Purpose: Decode a dict to a quote report. Lazy import from `gs_quant.quote_reports.core`.

**Algorithm:**
1. Branch: `value` is truthy -> `quote_report_from_dict(value)`
2. Branch: falsy -> return `None`

---

### decode_quote_reports(value: Optional[Iterable[Dict]]) -> Optional[list]
Purpose: Decode a list of dicts to quote reports.

**Algorithm:**
1. Branch: `value` is truthy -> `quote_reports_from_dicts(value)`
2. Branch: falsy -> return `None`

---

### decode_custom_comment(value: Optional[dict]) -> Optional
Purpose: Decode a single custom comment dict.

**Algorithm:**
1. Branch: truthy -> `custom_comment_from_dict(value)`
2. Branch: falsy -> return `None`

---

### decode_custom_comments(value: Optional[Iterable[Dict]]) -> Optional
Purpose: Decode a list of custom comment dicts.

**Algorithm:**
1. Branch: truthy -> `custom_comments_from_dicts(value)`
2. Branch: falsy -> return `None`

---

### decode_hedge_type(value: Optional[dict]) -> Optional
Purpose: Decode a single hedge type dict.

**Algorithm:**
1. Branch: truthy -> `hedge_type_from_dict(value)`
2. Branch: falsy -> return `None`

---

### decode_hedge_types(value: Optional[Iterable[Dict]]) -> Optional
Purpose: Decode a list of hedge type dicts.

**Algorithm:**
1. Branch: truthy -> `hedge_type_from_dicts(value)`
2. Branch: falsy -> return `None`

---

### encode_dictable(o) -> Optional[dict]
Purpose: Encode an object that has a `.to_dict()` method, or return None.

**Algorithm:**
1. Branch: `o is None` -> return `None`
2. Branch: else -> return `o.to_dict()`

---

### encode_named_dictable(o) -> Optional[dict]
Purpose: Encode an object via `.to_dict()` and inject its class name as `'type'`.

**Algorithm:**
1. Call `encode_dictable(o)` -> `d`
2. Branch: `d is not None` -> set `d['type'] = type(o).__name__`
3. Return `d`

**Elixir note:** This is polymorphic serialization. The `type` field is used for deserialization dispatch. In Elixir, store module name in the map under `:type` key.

---

### _get_dc_type(cls, name_field: str, allow_missing: bool) -> Optional[str]
Purpose: Extract the default value of a dataclass field named `name_field` (or `name_field_`) from a class. Used to build the class_type -> class mapping for polymorphic decoding.

**Algorithm:**
1. Filter `fields(cls)` to find field with name `name_field` or `f'{name_field}_'`
2. Branch: no matching field found:
   - Sub-branch: `allow_missing` is True -> return `None`
   - Sub-branch: `allow_missing` is False -> raise `ValueError`
3. Get `def_value = type_field[0].default`
4. Branch: `def_value == MISSING or def_value is None` -> raise `ValueError('No default value for "class_type" field on class')`
5. Return `def_value`

**Raises:** `ValueError` when field not found (and not allow_missing) or when default is MISSING/None.

---

### _value_decoder(type_to_cls_map, explicit_cls=None, str_mapper=None) -> Callable
Purpose: Factory that creates a polymorphic decoder function. Returns a closure `decode_value`.

**Algorithm (returned `decode_value` closure):**
1. Branch: `value is None or value == 'null'` -> return `None`
2. Branch: `isinstance(value, (list, tuple))` -> recursively decode each element, return as tuple
3. Branch: `isinstance(value, str)`:
   - Sub-branch: `str_mapper is not None` -> return `str_mapper(value)`
   - Sub-branch: `str_mapper is None` -> return value as-is
4. Branch: `isinstance(value, (float, int, dt.date))` -> return value as-is (primitive passthrough)
5. Branch: `not isinstance(value, dict)` -> raise `TypeError`
6. Branch: `explicit_cls is not None` -> return `explicit_cls.from_dict(value)`
7. Branch: `explicit_cls is None` (polymorphic dispatch):
   - Branch: `'class_type' not in value` -> raise `ValueError`
   - Get `obj_type = value['class_type']`
   - Branch: `obj_type not in type_to_cls_map` -> raise `ValueError`
   - Try: `type_to_cls_map[obj_type].from_dict(value)`
   - Except any `Exception` -> raise `ValueError` wrapping original exception (chained with `from e`)

**Raises:** `TypeError` for non-dict non-primitive types; `ValueError` for missing class_type, unknown class_type, or deserialization failure.

---

### dc_decode(*classes, name_field='class_type', allow_missing=False) -> Callable
Purpose: Build a polymorphic decoder for a set of dataclass types. Each class must have a field (default: `class_type`) with a default value that serves as its discriminator.

**Algorithm:**
1. For each class in `classes`: call `_get_dc_type(cls, name_field, allow_missing)` to get the discriminator value
2. Filter out `None` mappings (from `allow_missing=True`)
3. Build `type_to_cls_map` dict: `{discriminator_value: class}`
4. Return `_value_decoder(type_to_cls_map, None)`

## State Mutation
- `__valid_date_formats`: Module-level constant tuple; never mutated.
- `optional_datetime_config` / `optional_date_config`: Module-level config objects; never mutated after creation.
- No global mutable state.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `decode_optional_date` | No date format matches the input string, or unsupported type |
| `ValueError` | `decode_optional_time` | Unsupported type (message says "date" -- copy-paste bug) |
| `TypeError` | `decode_iso_date_or_datetime` | Value is not str, date, datetime, list, or tuple |
| `TypeError` | `decode_date_or_str` | Unsupported type |
| `TypeError` | `encode_datetime` | Pandas Timestamp's `isoformat` rejects `timespec` kwarg (caught internally) |
| `TypeError` | `decode_datetime` | Unsupported type |
| `TypeError` | `decode_float_or_str` | Unsupported type |
| `ValueError` | `_get_dc_type` | Field not found (when allow_missing=False) or default is MISSING/None |
| `TypeError` | `_value_decoder` (decode_value) | Value is not dict and not a recognized primitive |
| `ValueError` | `_value_decoder` (decode_value) | Missing `class_type` key, unknown class_type, or from_dict failure |

## Edge Cases
- `encode_date_or_str` with a `dt.datetime` value: returns `datetime.isoformat()` (not just date part) because `datetime` is a subclass of `date`. The ISO output will include time components.
- `decode_optional_date` with a value that is already a `dt.date` instance: returns it directly (compatibility with dataclasses-json >= 0.6.5 which applies global config to Optional[T]).
- `decode_optional_date` with a string that matches none of the 5 formats: raises `ValueError`.
- `decode_date_or_str` with a float value <= 59: no leap-year adjustment; value of exactly 60 would map to 1900-03-01 (same as Excel's incorrect Feb 29, 1900).
- `decode_date_or_str` with a string like `'ATM'`, `'1y'`, `'3m'`: returns the string unchanged (treated as a tenor).
- `decode_datetime` with nanosecond-precision strings (sub-seconds > 6 digits): truncates to 6 digits (microseconds).
- `encode_datetime` with timezone-aware datetime: does NOT append 'Z' (trusts existing tzinfo).
- `encode_datetime` with naive datetime: always appends 'Z' (assumes UTC).
- `encode_datetime` with pandas Timestamp: falls back to `.isoformat()` without `timespec` (no millisecond precision guarantee).
- `optional_from_isodatetime` replaces ALL occurrences of 'Z' in the string, not just trailing 'Z' (uses `str.replace`). In practice only trailing 'Z' appears in ISO datetimes.
- `_value_decoder`: the string `'null'` is treated as `None`.
- `_value_decoder`: `dt.date` instances pass through without decoding (alongside float/int).
- `decode_named_instrument`: distinguishes portfolios from instruments by the presence of `'portfolio_name'` key.
- `encode_pandas_series`: only checks the FIRST key's type to decide whether to convert all keys. If a series has mixed key types, behavior may be inconsistent.
- `decode_pandas_series`: always attempts to parse ALL keys as date/datetime via `decode_iso_date_or_datetime`, which will raise `TypeError` for non-date string keys.

## Bugs Found
- Line 62: `decode_optional_time` error message says "Cannot convert {value} to date" but should say "to time". (OPEN -- cosmetic)

## Coverage Notes
- Branch count: ~55 distinct branches across all functions
- Key branch clusters: `decode_date_or_str` (5 type branches + Excel leap year sub-branch), `decode_datetime` (4 type branches + regex sub-branches), `_value_decoder.decode_value` (7+ branches)
- All lazy imports (`gs_quant.instrument`, `gs_quant.markets.portfolio`, `gs_quant.quote_reports.core`) are inside function bodies to avoid circular imports
