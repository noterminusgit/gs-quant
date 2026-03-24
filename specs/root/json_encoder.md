# json_encoder.py

## Summary
Custom JSON encoder module that extends Python's `json.JSONEncoder` to handle Goldman Sachs quant domain types: `datetime`, `date`, `time`, `Enum`, `Base` (gs_quant base class), `Market`, and `pandas.DataFrame`. The `encode_default` function provides the type-dispatch logic, and `JSONEncoder` is the drop-in replacement for `json.JSONEncoder` used throughout the codebase for `json.dumps()` calls.

## Dependencies
- Internal: `gs_quant.base` (Base, Market), `gs_quant.json_convertors` (encode_date_or_str, encode_datetime)
- External: `datetime` (datetime, date, time), `enum` (Enum), `json` (JSONEncoder), `pandas` (DataFrame)

## Type Definitions

### JSONEncoder (class)
Inherits: `json.JSONEncoder`

No additional fields. Overrides only the `default` method.

## Enums and Constants
None.

## Functions/Methods

### encode_default(o) -> Optional[Union[str, dict]]
Purpose: Top-level type dispatcher that converts non-JSON-serializable Python objects to JSON-compatible representations. Returns `None` if the type is not recognized (allowing the caller to handle further).

**Algorithm -- type dispatch chain (ORDER MATTERS):**

1. **`dt.datetime` check** (line 27): `isinstance(o, dt.datetime)`
   - Branch: True -> return `encode_datetime(o)` (from `json_convertors`)
   - Output: ISO 8601 string with millisecond precision, trailing 'Z' for naive datetimes
   - **CRITICAL:** This check MUST come before the `dt.date` check because `datetime` is a subclass of `date`. If reversed, datetimes would be encoded as dates (losing time information).

2. **`dt.date` check** (line 29): `isinstance(o, dt.date)`
   - Branch: True -> return `encode_date_or_str(o)` (from `json_convertors`)
   - Output: ISO 8601 date string, e.g., `"2020-07-28"`

3. **`dt.time` check** (line 31): `isinstance(o, dt.time)`
   - Branch: True -> return `o.isoformat(timespec='milliseconds')`
   - Output: ISO 8601 time string with milliseconds, e.g., `"12:30:45.000"`
   - **Note:** Unlike `encode_optional_time` in `json_convertors` (which uses default `isoformat()`), this explicitly uses `timespec='milliseconds'`.

4. **`Enum` check** (line 33): `isinstance(o, Enum)`
   - Branch: True -> return `o.value`
   - Output: The raw value of the enum member (typically a string like `"member_name"`)

5. **`Base` or `Market` check** (line 35): `isinstance(o, (Base, Market))`
   - Branch: True -> return `o.to_dict()`
   - Output: dict representation of the gs_quant object
   - `Base` is the ABC for all gs_quant target/model classes
   - `Market` is the ABC for market data classes

6. **`pd.DataFrame` check** (line 37): `isinstance(o, pd.DataFrame)`
   - Branch: True -> return `o.to_json()`
   - Output: JSON string (note: returns a STRING, not a dict -- this means the DataFrame is double-encoded when the outer `json.dumps` serializes it)

7. **No match** (implicit): function returns `None` (falls off the end)

**Complete type dispatch table (Elixir port reference):**

| Python Type | Encoder | Elixir Equivalent | Output Format |
|-------------|---------|-------------------|---------------|
| `dt.datetime` | `encode_datetime(o)` | Jason.Encoder for DateTime | `"2020-07-28T12:00:00.000Z"` |
| `dt.date` | `encode_date_or_str(o)` | Jason.Encoder for Date | `"2020-07-28"` |
| `dt.time` | `o.isoformat(timespec='milliseconds')` | Jason.Encoder for Time | `"12:30:45.000"` |
| `Enum` | `o.value` | Jason.Encoder for custom enums | raw value (usually string) |
| `Base` / `Market` | `o.to_dict()` | Jason.Encoder for Base/Market structs | nested dict |
| `pd.DataFrame` | `o.to_json()` | Jason.Encoder for DataFrame equivalent | JSON string (double-encoded) |

---

### JSONEncoder.default(self, o) -> Any
Purpose: Override of `json.JSONEncoder.default()`. Called by `json.dumps()` for objects that are not natively JSON-serializable.

**Algorithm:**
1. Call `encode_default(o)` -> `ret`
2. Branch: `ret is None` -> call `super().default(o)` (which raises `TypeError` for unserializable types)
3. Branch: `ret is not None` -> return `ret`

**Raises:** `TypeError` (via `super().default(o)`) when the object type is not handled by `encode_default`.

**Usage pattern:**
```python
json.dumps(data, cls=JSONEncoder)
```

## State Mutation
- No mutable state. All functions are pure.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | `JSONEncoder.default` (via `super().default(o)`) | When `encode_default` returns `None` (object type not recognized) |
| `TypeError` | `encode_datetime` (called by `encode_default`) | Pandas Timestamp `isoformat` rejects `timespec` kwarg -- caught internally by `encode_datetime` |

## Edge Cases
- **datetime before date ordering:** The `isinstance(o, dt.datetime)` check at line 27 MUST precede the `isinstance(o, dt.date)` check at line 29. Since `datetime` is a subclass of `date`, reversing the order would cause datetimes to be encoded as date-only strings, silently dropping time information. The current code is correct.
- **pd.DataFrame double-encoding:** `o.to_json()` returns a JSON string, not a dict. When this string is returned from `default()`, `json.dumps()` will serialize it as a JSON string (with escaped quotes). The consumer must parse the resulting string value as JSON to recover the DataFrame data. This is likely intentional for embedding DataFrames as opaque JSON blobs.
- **Enum with non-string values:** If an Enum's `.value` is itself a non-serializable type (e.g., a tuple or custom object), this will cause a subsequent `TypeError` during serialization of the enum value.
- **Base/Market subclass check order:** `Base` and `Market` are ABCs. Any object inheriting from both `Enum` and `Base` (unlikely but possible) would be caught by the `Enum` check first due to ordering.
- **`encode_default` returning `None`:** When called standalone (not via `JSONEncoder`), the caller is responsible for handling the `None` return. Inside `JSONEncoder.default`, the `None` return triggers `super().default(o)` which raises `TypeError`.
- **`dt.time` with milliseconds=0:** `isoformat(timespec='milliseconds')` still outputs `"12:30:45.000"` (trailing zeros preserved).
- **Naive vs aware datetimes:** Delegated to `encode_datetime` in `json_convertors` -- naive datetimes get 'Z' suffix, aware ones keep their timezone.

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 8 (6 type checks in `encode_default` + 2 branches in `JSONEncoder.default`)
- The `encode_default` function has a linear type dispatch chain -- each `isinstance` check is a branch. The implicit `None` return at the end is the 7th path.
- `JSONEncoder.default` has 2 branches: `ret is None` vs `ret is not None`.
- Testing requires objects of each supported type plus at least one unsupported type to exercise the `super().default()` path.
