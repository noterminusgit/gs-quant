# risk/core.py

## Summary

Core risk result types and aggregation utilities for the gs_quant pricing pipeline. This module defines the `ResultInfo` abstract base class and its concrete subclasses (`ErrorValue`, `UnsupportedValue`, `FloatWithInfo`, `StringWithInfo`, `DictWithInfo`, `SeriesWithInfo`, `DataFrameWithInfo`) which wrap raw computation results with metadata (risk key, unit, error, request ID). It also provides functions for aggregating, subtracting, sorting, and composing risk results. The MQVS validator dataclasses support validation configuration. In an Elixir port, the class hierarchy maps to structs implementing a shared `ResultInfo` behaviour, the `compose` static methods map to protocol dispatch, and `aggregate_risk`/`aggregate_results` map to `Enum.reduce`-style pipelines.

## Dependencies

- Internal:
  - `gs_quant.base` (`RiskKey` -- namedtuple with fields: provider, date, market, params, scenario, risk_measure)
  - `gs_quant.config` (`DisplayOptions` -- has `show_na` boolean property)
  - `gs_quant.config.display_options` (module-level global `DisplayOptions` instance, accessed as fallback)
  - `gs_quant.datetime` (`point_sort_order` -- converts point strings to sortable floats)
  - `gs_quant.markets.markets` (`historical_risk_key` -- lazy import inside `composition_info`)
  - `gs_quant.markets` (`MarketDataCoordinate` -- lazy import inside `filter_by_coord`)
- External:
  - `datetime` (dt.date)
  - `itertools` (chain.from_iterable)
  - `abc` (ABCMeta, abstractmethod)
  - `concurrent.futures` (Future)
  - `copy` (copy)
  - `dataclasses` (dataclass, fields)
  - `typing` (Iterable, Optional, Union, Tuple, Dict, Callable, List)
  - `pandas` (pd.Series, pd.DataFrame, pd.DatetimeIndex, pd.concat)
  - `dataclasses_json` (dataclass_json)

### Elixir Mapping Notes

- `pd.Series` -> Consider a custom `%SeriesWithInfo{}` struct wrapping `Explorer.Series` or a `{index, data}` tuple.
- `pd.DataFrame` -> `Explorer.DataFrame` or custom struct.
- `concurrent.futures.Future` -> `Task` or `GenStage` demand-based producer.
- `ABCMeta` / `abstractmethod` -> Elixir `@behaviour` with `@callback` declarations.
- `dataclass_json` -> `Jason.Encoder` protocol implementation.
- `copy.copy` -> Struct update syntax `%{struct | field: value}`.
- Multiple inheritance (e.g., `FloatWithInfo(ScalarWithInfo, float)`) -> Elixir struct + protocol implementations.

## Type Definitions

### ResultInfo (ABC, metaclass=ABCMeta)

Abstract base class for all risk result wrappers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__risk_key` | `RiskKey` | (required) | Identifies the provider/date/market/params/scenario/measure |
| `__unit` | `Optional[dict]` | `None` | Unit metadata, e.g. `{"USD": 1}` |
| `__error` | `Optional[Union[str, dict]]` | `None` | Error string or structured error dict |
| `__request_id` | `Optional[str]` | `None` | Request ID for tracing |

Properties (read-only): `risk_key`, `unit`, `error`, `request_id`.

Abstract property: `raw_value` -- must be implemented by all subclasses.

**Elixir mapping:** Define a `@behaviour ResultInfo` with `@callback raw_value(t()) :: any()`. Each concrete type is a struct that implements this behaviour. The private fields become regular struct fields (Elixir has no field visibility).

### ErrorValue (ResultInfo)

Represents a failed computation result.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherited from ResultInfo) | | | `error` is required, `unit` is always `None` |

### UnsupportedValue (ResultInfo)

Represents an unsupported computation (not an error, but no meaningful numeric value).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherited from ResultInfo) | | | No extra fields |

### ScalarWithInfo (ResultInfo, ABC)

Abstract base for scalar result types. Calls both `ResultInfo.__init__` and `float.__init__`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherited from ResultInfo) | | | Plus `value` passed to the built-in type constructor |

### FloatWithInfo (ScalarWithInfo, float)

Concrete class: a `float` that also carries `ResultInfo` metadata. Uses `__new__` to construct the float value.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `risk_key` | `RiskKey` | (required) | From ResultInfo |
| `value` | `Union[float, str]` | (required) | The float value (also the float base) |
| `unit` | `dict` | `None` | Unit dict |
| `error` | `Optional[str]` | `None` | Error string |
| `request_id` | `Optional[str]` | `None` | Request ID |

**Elixir mapping:** `%FloatWithInfo{value: float(), risk_key: ..., unit: ..., error: ..., request_id: ...}`. Implement `Kernel` arithmetic via protocol or explicit functions.

### StringWithInfo (ScalarWithInfo, str)

Concrete class: a `str` that also carries `ResultInfo` metadata.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `risk_key` | `RiskKey` | (required) | From ResultInfo |
| `value` | `Union[float, str]` | (required) | The string value |
| `unit` | `Optional[dict]` | `None` | Unit dict |
| `error` | `Optional[str]` | `None` | Error string |
| `request_id` | `Optional[str]` | `None` | Request ID |

### DictWithInfo (ScalarWithInfo, dict)

Concrete class: a `dict` that also carries `ResultInfo` metadata. Has both `__new__` and `__init__`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `risk_key` | `RiskKey` | (required) | From ResultInfo |
| `value` | `Union[float, str, dict]` | (required) | The dict value |
| `unit` | `Optional[dict]` | `None` | Unit dict |
| `error` | `Optional[Union[str, dict]]` | `None` | Error |
| `request_id` | `Optional[str]` | `None` | Request ID |

### SeriesWithInfo (pd.Series, ResultInfo)

A pandas `Series` with `ResultInfo` metadata. Uses `_internal_names` and `_internal_names_set` to prevent pandas from treating ResultInfo fields as data columns.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `*args` | any | | Forwarded to `pd.Series.__init__` |
| `risk_key` | `Optional[RiskKey]` | `None` | From ResultInfo |
| `unit` | `Optional[dict]` | `None` | Unit dict |
| `error` | `Optional[Union[str, dict]]` | `None` | Error |
| `request_id` | `Optional[str]` | `None` | Request ID |
| `**kwargs` | any | | Forwarded to `pd.Series.__init__` |

Class attributes:
- `_internal_names`: `pd.DataFrame._internal_names` + mangled ResultInfo property names
- `_internal_names_set`: `set(_internal_names)`

Constructor properties:
- `_constructor` -> `SeriesWithInfo`
- `_constructor_expanddim` -> `DataFrameWithInfo`

### DataFrameWithInfo (pd.DataFrame, ResultInfo)

A pandas `DataFrame` with `ResultInfo` metadata. Same `_internal_names` trick as `SeriesWithInfo`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `*args` | any | | Forwarded to `pd.DataFrame.__init__` |
| `risk_key` | `Optional[RiskKey]` | `None` | From ResultInfo |
| `unit` | `Optional[dict]` | `None` | Unit dict |
| `error` | `Optional[Union[str, dict]]` | `None` | Error |
| `request_id` | `Optional[str]` | `None` | Request ID |
| `**kwargs` | any | | Forwarded to `pd.DataFrame.__init__` |

Constructor properties:
- `_constructor` -> `DataFrameWithInfo`
- `_constructor_sliced` -> `SeriesWithInfo`

### MQVSValidationTarget (dataclass, dataclass_json)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `env` | `Optional[str]` | `None` | Environment identifier |
| `operator` | `Optional[str]` | `None` | Operator string |
| `mqGroups` | `Optional[Tuple[str, ...]]` | `None` | MQ group names |
| `users` | `Optional[Tuple[str, ...]]` | `None` | User identifiers |
| `assetClasses` | `Optional[Tuple[str, ...]]` | `None` | Asset class filters |
| `assets` | `Optional[Tuple[str, ...]]` | `None` | Asset identifiers |
| `legTypes` | `Optional[Tuple[str, ...]]` | `None` | Leg type filters |
| `legFields` | `Optional[Dict[str, str]]` | `None` | Leg field key-value pairs |

### MQVSValidatorDefn (dataclass, dataclass_json)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `validatorType` | `str` | (required) | Type of validator |
| `targets` | `Tuple[MQVSValidationTarget, ...]` | (required) | Validation targets |
| `args` | `Dict[str, str]` | (required) | Validator arguments |
| `groupId` | `Optional[str]` | `None` | Group identifier |
| `groupIndex` | `Optional[int]` | `None` | Index within group |
| `groupMethod` | `Optional[str]` | `None` | Grouping method |

### MQVSValidatorDefnsWithInfo (ResultInfo)

Wraps one or more `MQVSValidatorDefn` instances with `ResultInfo` metadata.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `validators` | `Tuple[MQVSValidatorDefn, ...]` | (set in `__init__`) | The validator definitions |

### Type Aliases

```python
ResultType = Union[None, dict, tuple, DataFrameWithInfo, FloatWithInfo, SeriesWithInfo]
```

## Enums and Constants

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `__column_sort_fns` | `dict` | `{'label1': point_sort_order, 'mkt_point': point_sort_order, 'point': point_sort_order}` | Maps column names to sort-key functions |
| `__risk_columns` | `tuple` | `('date', 'time', 'mkt_type', 'mkt_asset', 'mkt_class', 'mkt_point')` | Default columns for risk sorting |

Note: Both are module-private (name-mangled with `__` prefix, accessible as `_core__column_sort_fns` etc.).

## Functions/Methods

### ResultInfo.__init__(self, risk_key: RiskKey, unit: Optional[dict] = None, error: Optional[Union[str, dict]] = None, request_id: Optional[str] = None)

Purpose: Initialize the base result metadata.

**Algorithm:**
1. Store `risk_key` as `self.__risk_key`
2. Store `unit` as `self.__unit`
3. Store `error` as `self.__error`
4. Store `request_id` as `self.__request_id`

---

### ResultInfo.raw_value (abstract property)

Purpose: Return the unwrapped raw value. Must be implemented by all subclasses.

---

### ResultInfo.composition_info(components: Iterable) -> Tuple[list, list, dict, RiskKey, dict]

Purpose: Extract dates, values, errors, a unified risk_key, and unit from an iterable of result components. Used by all `compose()` static methods.

**Algorithm:**
1. Lazy-import `historical_risk_key` from `gs_quant.markets.markets`
2. Initialize `dates = []`, `values = []`, `errors = {}`, `risk_key = None`, `unit = None`
3. For each `component` in `components`:
   a. Extract `date` from `component.risk_key.date`
   b. **Branch:** If `risk_key is None` -> set `risk_key = historical_risk_key(component.risk_key)` (strips date, replaces market with LocationOnlyMarket)
   c. **Branch:** If `risk_key.market.location != component.risk_key.market.location` -> raise `ValueError('Cannot compose results with different markets')`
   d. **Branch:** If `component` is `ErrorValue` or `Exception` -> store in `errors[date]`
   e. **Branch:** If `component` is `UnsupportedValue` -> append to `values`, append `date` to `dates`, set `unit = None`
   f. **Branch:** Otherwise (normal result) -> append `component.raw_value` to `values`, append `date` to `dates`, set `unit = unit or component.unit`
4. Return `(dates, values, errors, risk_key, unit)`

**Elixir mapping:** `Enum.reduce/3` over components accumulating `{dates, values, errors, risk_key, unit}` tuple. Pattern-match on component type.

---

### ErrorValue.__init__(self, risk_key: RiskKey, error: Union[str, dict], request_id: Optional[str] = None)

Purpose: Construct an error result.

**Algorithm:**
1. Call `super().__init__(risk_key, error=error, request_id=request_id)` -- `unit` is always `None`

---

### ErrorValue.__repr__(self) -> str

Purpose: Return the error string.

**Algorithm:**
1. Return `self.error`

---

### ErrorValue.__getattr__(self, item)

Purpose: Raise `AttributeError` for any missing attribute access, including the error message for diagnostics.

**Algorithm:**
1. Raise `AttributeError(f'ErrorValue object has no attribute {item}. Error was {self.error}')`

Note: Only called when normal attribute lookup fails.

---

### ErrorValue.raw_value (property) -> None

Purpose: Return `None` (no valid value for errors).

---

### ErrorValue._to_records(self, extra_dict, display_options: DisplayOptions = None) -> list

Purpose: Convert to record list for tabular display.

**Algorithm:**
1. Return `[{**extra_dict, 'value': self}]`

---

### UnsupportedValue.__init__(self, risk_key: RiskKey, request_id: Optional[str] = None)

Purpose: Construct an unsupported-value result.

**Algorithm:**
1. Call `super().__init__(risk_key, request_id=request_id)` -- `unit` and `error` are `None`

---

### UnsupportedValue.__repr__(self) -> str

Purpose: Return `'Unsupported Value'`.

---

### UnsupportedValue.raw_value (property) -> str

Purpose: Return `'Unsupported Value'` string.

---

### UnsupportedValue.compose(components: Iterable) -> SeriesWithInfo (static)

Purpose: Compose multiple unsupported values into a time series.

**Algorithm:**
1. Call `ResultInfo.composition_info(components)` to get `dates, values, errors, risk_key, unit`
2. Return `SeriesWithInfo(pd.Series(index=pd.DatetimeIndex(dates).date, data=values), risk_key=risk_key, unit=unit, error=errors)`

---

### UnsupportedValue._to_records(self, extra_dict, display_options: DisplayOptions = None) -> list

Purpose: Convert to record list, respecting `show_na` display option.

**Algorithm:**
1. **Branch:** If `display_options is not None` and not `isinstance(display_options, DisplayOptions)` -> raise `TypeError("display_options must be of type DisplayOptions")`
2. **Branch:** If `display_options is not None` -> use it; else -> use `gs_quant.config.display_options` (global default)
3. Read `show_na` from options
4. **Branch:** If `show_na` is truthy -> return `[{**extra_dict, 'value': self}]`
5. **Branch:** If `show_na` is falsy -> return `[]`

---

### ScalarWithInfo.__init__(self, risk_key, value, unit=None, error=None, request_id=None)

Purpose: Initialize both `ResultInfo` and `float` base.

**Algorithm:**
1. Call `ResultInfo.__init__(self, risk_key, unit=unit, error=error, request_id=request_id)`
2. Call `float.__init__(value)` -- note: this is essentially a no-op since float is immutable and created in `__new__`

---

### ScalarWithInfo.__reduce__(self) -> tuple

Purpose: Support pickling/serialization.

**Algorithm:**
1. Return `(self.__class__, (self.risk_key, self.raw_value, self.unit, self.error, self.request_id))`

---

### ScalarWithInfo.compose(components: Iterable) -> SeriesWithInfo (static)

Purpose: Compose scalar results into a historical time series.

**Algorithm:**
1. Call `ResultInfo.composition_info(components)` to get `dates, values, errors, risk_key, unit`
2. Return `SeriesWithInfo(pd.Series(index=pd.DatetimeIndex(dates).date, data=values), risk_key=risk_key, unit=unit, error=errors)`

---

### ScalarWithInfo._to_records(self, extra_dict, display_options=None) -> list

Purpose: Convert scalar to record list.

**Algorithm:**
1. Return `[{**extra_dict, 'value': self}]`

---

### FloatWithInfo.__new__(cls, risk_key, value, unit=None, error=None, request_id=None) -> FloatWithInfo

Purpose: Create the float instance (immutable type requires `__new__`).

**Algorithm:**
1. Return `float.__new__(cls, value)`

---

### FloatWithInfo.raw_value (property) -> float

Purpose: Return `float(self)`.

---

### FloatWithInfo.__str__(self) -> str

Purpose: Return `float.__repr__(self)`.

---

### FloatWithInfo.__repr__(self) -> str

Purpose: Human-readable representation with optional unit string.

**Algorithm:**
1. **Branch:** If `self.error` is truthy -> return `self.error`
2. Else: compute `res = float.__repr__(self)`
3. **Branch:** If `self.unit` is truthy AND `isinstance(self.unit, dict)`:
   a. Initialize `numerator = []`, `denominator = []`
   b. For each `(unit_name, power)` in `self.unit.items()`:
      - **Branch:** `power > 0`:
        - **Branch:** `power == 1` -> append `unit_name` to numerator
        - **Branch:** `power != 1` -> append `f"{unit_name}^{power}"` to numerator
      - **Branch:** `power < 0`:
        - **Branch:** `power == -1` -> append `unit_name` to denominator
        - **Branch:** `power != -1` -> append `f"{unit_name}^{abs(power)}"` to denominator
      - **Branch:** `power == 0` -> neither list (implicit skip)
   c. **Branch:** Both `numerator` and `denominator` non-empty -> `unit_str = "num1*num2/den1*den2"`
   d. **Branch:** Only `numerator` non-empty -> `unit_str = "num1*num2"`
   e. **Branch:** Only `denominator` non-empty -> `unit_str = "1/den1*den2"`
   f. **Branch:** Both empty -> `unit_str = ""`
   g. **Branch:** `unit_str` non-empty -> return `f"{res} ({unit_str})"`
   h. **Branch:** `unit_str` empty -> return `res`
4. **Branch:** `self.unit` falsy or not dict -> return `res`

Total branch points in `__repr__`: 12.

---

### FloatWithInfo.__add__(self, other) -> FloatWithInfo | float

Purpose: Add two FloatWithInfo values (with unit checking) or fall back to float addition.

**Algorithm:**
1. **Branch:** If `isinstance(other, FloatWithInfo)`:
   a. **Branch:** If `self.unit == other.unit` -> return `FloatWithInfo(combine_risk_key(self.risk_key, other.risk_key), self.raw_value + other.raw_value, self.unit)`
   b. **Branch:** Else -> raise `ValueError('FloatWithInfo unit mismatch')`
2. **Branch:** Else -> return `super().__add__(other)` (plain float addition)

---

### FloatWithInfo.__mul__(self, other) -> FloatWithInfo

Purpose: Multiply, returning FloatWithInfo with combined or original risk key.

**Algorithm:**
1. **Branch:** If `isinstance(other, FloatWithInfo)` -> return `FloatWithInfo(combine_risk_key(self.risk_key, other.risk_key), self.raw_value * other.raw_value, self.unit)`
2. **Branch:** Else -> return `FloatWithInfo(self.risk_key, self.raw_value * other, self.unit)`

---

### FloatWithInfo.to_frame(self) -> self

Purpose: No-op for API compatibility (DataFrameWithInfo also has this).

---

### StringWithInfo.__new__(cls, risk_key, value, unit=None, error=None, request_id=None) -> StringWithInfo

Purpose: Create the str instance.

**Algorithm:**
1. Return `str.__new__(cls, value)`

---

### StringWithInfo.raw_value (property) -> str

Purpose: Return `str(self)`.

---

### StringWithInfo.__repr__(self) -> str

Purpose: Return error if present, else the string repr.

**Algorithm:**
1. **Branch:** If `self.error` -> return `self.error`
2. **Branch:** Else -> return `str.__repr__(self)`

---

### DictWithInfo.__new__(cls, risk_key, value, unit=None, error=None, request_id=None) -> DictWithInfo

Purpose: Create the dict instance.

**Algorithm:**
1. Return `dict.__new__(cls, value)`

---

### DictWithInfo.__init__(self, risk_key, value, unit=None, error=None, request_id=None)

Purpose: Initialize both dict content and ResultInfo metadata.

**Algorithm:**
1. `dict.__init__(self, value)`
2. `ResultInfo.__init__(self, risk_key, unit=unit, error=error, request_id=request_id)`

Note: `DictWithInfo` has explicit `__init__` because `dict` is mutable and needs initialization, unlike `float`/`str`.

---

### DictWithInfo.raw_value (property) -> dict

Purpose: Return `dict(self)` (plain dict copy).

---

### DictWithInfo.__repr__(self) -> str

**Algorithm:**
1. **Branch:** If `self.error` -> return `self.error`
2. **Branch:** Else -> return `dict.__repr__(self)`

---

### SeriesWithInfo.__init__(self, *args, risk_key=None, unit=None, error=None, request_id=None, **kwargs)

Purpose: Initialize both pd.Series and ResultInfo.

**Algorithm:**
1. `pd.Series.__init__(self, *args, **kwargs)`
2. `ResultInfo.__init__(self, risk_key, unit=unit, error=error, request_id=request_id)`

---

### SeriesWithInfo.__repr__(self) -> str

**Algorithm:**
1. **Branch:** If `self.error` -> return `pd.Series.__repr__(self) + "\nErrors: " + str(self.error)`
2. **Branch:** Else -> return `pd.Series.__repr__(self)`

---

### SeriesWithInfo.raw_value (property) -> pd.Series

Purpose: Return plain `pd.Series(self)`.

---

### SeriesWithInfo.compose(components: Iterable) -> SeriesWithInfo (static)

Purpose: Compose multiple series results into a single historical series.

**Algorithm:**
1. Call `ResultInfo.composition_info(components)`
2. Return `SeriesWithInfo(pd.Series(index=pd.DatetimeIndex(dates).date, data=values), risk_key=risk_key, unit=unit, error=errors)`

---

### SeriesWithInfo._to_records(self, extra_dict, display_options=None) -> list

Purpose: Convert series to list of dicts for tabular output.

**Algorithm:**
1. Create DataFrame from self, reset index
2. Rename columns to `['dates', 'value']`
3. Convert to records, merge each with `extra_dict`
4. Return list of merged dicts

---

### SeriesWithInfo.__mul__(self, other) -> SeriesWithInfo

Purpose: Multiply series, preserving ResultInfo metadata.

**Algorithm:**
1. `new_result = pd.Series.__mul__(self, other)` -- standard pandas multiply
2. Re-initialize ResultInfo on `new_result` with original metadata
3. Return `new_result`

Note: This mutates `new_result` in-place via `ResultInfo.__init__` after pandas creates it.

---

### SeriesWithInfo.copy_with_resultinfo(self, deep=True) -> SeriesWithInfo

Purpose: Deep copy that preserves ResultInfo metadata (unlike pandas `.copy()` which loses it).

**Algorithm:**
1. Return `SeriesWithInfo(self.raw_value.copy(deep=deep), risk_key=..., unit=..., error=..., request_id=...)`

---

### DataFrameWithInfo.__init__(self, *args, risk_key=None, unit=None, error=None, request_id=None, **kwargs)

Purpose: Initialize both pd.DataFrame and ResultInfo.

**Algorithm:**
1. `pd.DataFrame.__init__(self, *args, **kwargs)`
2. `ResultInfo.__init__(self, risk_key, unit=unit, error=error, request_id=request_id)`

---

### DataFrameWithInfo.__repr__(self) -> str

**Algorithm:**
1. **Branch:** If `self.error` -> return `pd.DataFrame.__repr__(self) + "\nErrors: " + str(self.errors)`
2. **Branch:** Else -> return `pd.DataFrame.__repr__(self)`

**BUG (line 389):** Uses `self.errors` (plural) instead of `self.error` (singular). This will likely raise `AttributeError` at runtime because `pd.DataFrame` has an `.errors` attribute only in some contexts (it may accidentally work if pandas defines it, but the intent is clearly `self.error`).

---

### DataFrameWithInfo.raw_value (property) -> pd.DataFrame

Purpose: Return plain DataFrame, with optional date index normalization.

**Algorithm:**
1. **Branch:** If `self.empty` -> return `pd.DataFrame(self)` (empty copy)
2. Copy self: `df = self.copy()`
3. **Branch:** If `isinstance(self.index.values[0], dt.date)`:
   a. Set `df.index.name = 'dates'`
   b. Reset index (moves dates from index to column)
4. Return `pd.DataFrame(df)`

---

### DataFrameWithInfo.compose(components: Iterable) -> DataFrameWithInfo (static)

Purpose: Compose multiple DataFrameWithInfo results into a single DataFrame keyed by date.

**Algorithm:**
1. Call `ResultInfo.composition_info(components)`
2. Concatenate all DataFrames, assigning a `date` column from the matching date: `pd.concat(v.assign(date=d) for d, v in zip(dates, values)).set_index('date')`
3. Return `DataFrameWithInfo(df, risk_key=risk_key, unit=unit, error=errors)`

---

### DataFrameWithInfo.to_frame(self) -> self

Purpose: No-op for API compatibility.

---

### DataFrameWithInfo._to_records(self, extra_dict, display_options=None) -> list

Purpose: Convert DataFrame to list of record dicts.

**Algorithm:**
1. **Branch:** If `self.empty`:
   a. **Branch:** If `display_options is not None` and not `isinstance(display_options, DisplayOptions)` -> raise `TypeError`
   b. **Branch:** If `display_options is not None` -> use it; else use `gs_quant.config.display_options`
   c. Read `show_na`
   d. **Branch:** If `show_na` -> return `[{**extra_dict, 'value': None}]`
   e. **Branch:** If not `show_na` -> return `[]`
2. Else (not empty): convert `self.raw_value` to dict records, merge each with `extra_dict`, return list

---

### DataFrameWithInfo.copy_with_resultinfo(self, deep=True) -> DataFrameWithInfo

Purpose: Deep copy preserving ResultInfo metadata.

**Algorithm:**
1. Return `DataFrameWithInfo(self.raw_value.copy(deep=deep), risk_key=..., unit=..., error=..., request_id=...)`

---

### DataFrameWithInfo.filter_by_coord(self, coordinate) -> DataFrameWithInfo

Purpose: Filter rows matching a `MarketDataCoordinate`'s non-None fields.

**Algorithm:**
1. Lazy-import `MarketDataCoordinate` from `gs_quant.markets`
2. `df = self.copy_with_resultinfo()`
3. For each field name `att` in `MarketDataCoordinate` dataclass fields:
   a. **Branch:** If `getattr(coordinate, att) is not None`:
      - **Branch:** If `isinstance(getattr(coordinate, att), str)` -> filter `df` where column `att` equals the string value
      - **Branch:** Else (iterable) -> filter `df` where column `att` is in the iterable value
4. Return filtered `df`

**Elixir mapping:** `Enum.reduce/3` over coordinate fields, filtering the DataFrame at each step.

---

### MQVSValidatorDefnsWithInfo.__init__(self, risk_key, value, unit=None, error=None, request_id=None)

Purpose: Wrap validator definitions with ResultInfo.

**Algorithm:**
1. Call `ResultInfo.__init__`
2. **Branch:** If `value` is truthy AND `isinstance(value, tuple)` -> set `self.validators = value`
3. **Branch:** Elif `value` is truthy AND `isinstance(value, MQVSValidatorDefn)` -> set `self.validators = tuple([value])`
4. **Branch:** (Implicit) If `value` is falsy -> `self.validators` is NOT set (could cause `AttributeError` later)

---

### MQVSValidatorDefnsWithInfo.raw_value (property)

Purpose: Return `self.validators`.

---

### aggregate_risk(results, threshold=None, allow_heterogeneous_types=False) -> pd.DataFrame

Purpose: Combine results from multiple `InstrumentBase.calc()` calls into a single aggregated DataFrame.

**Algorithm:**
1. Define inner function `get_df(result_obj)`:
   a. **Branch:** If `isinstance(result_obj, Future)` -> resolve it: `result_obj = result_obj.result()`
   b. **Branch:** If `isinstance(result_obj, pd.Series)` AND `allow_heterogeneous_types` -> return `pd.DataFrame(result_obj.raw_value).T`
   c. Otherwise -> return `result_obj.raw_value`
2. Build list of DataFrames: `dfs = [get_df(r) for r in results]`
3. Concatenate all and fill NaN with 0: `pd.concat(dfs).fillna(0)`
4. Group by all non-value columns and sum
5. **Branch:** If `threshold is not None` -> filter rows where `abs(value) > threshold`
6. Call `sort_risk(result)` and return

**Elixir mapping:** This is a reduce/concat/group_by pipeline. Futures become `Task.await/1`. The inner `get_df` maps to pattern matching on the result type.

---

### aggregate_results(results, allow_mismatch_risk_keys=False, allow_heterogeneous_types=False) -> ResultType

Purpose: Aggregate multiple typed results into a single result of the same type.

**Algorithm:**
1. Set `unit = None`, `risk_key = None`
2. Materialize: `results = tuple(results)`
3. **Branch:** If `len(results) == 0` -> return `None`
4. Validation loop over each `result`:
   a. **Branch:** If `isinstance(result, Exception)` -> raise `Exception` (bare, no message)
   b. **Branch:** If `result.error` is truthy -> raise `ValueError('Cannot aggregate results in error')`
   c. **Branch:** If not `allow_heterogeneous_types` AND `type(result) != type(results[0])` -> raise `ValueError` (heterogeneous types)
   d. **Branch:** If `result.unit` is truthy:
      - **Branch:** If `unit` is already set AND `unit != result.unit` -> raise `ValueError` (different units)
      - Set `unit = unit or result.unit`
   e. **Branch:** If not `allow_mismatch_risk_keys` AND `risk_key` is set AND `risk_key.ex_historical_diddle != result.risk_key.ex_historical_diddle` -> raise `ValueError` (different pricing keys)
   f. Set `risk_key = risk_key or result.risk_key`
5. `inst = next(iter(results))`
6. Dispatch on type of `inst`:
   a. **Branch:** `isinstance(inst, dict)` -> recursively aggregate each key: `{k: aggregate_results([r[k] for r in results]) for k in inst.keys()}`
   b. **Branch:** `isinstance(inst, tuple)` -> flatten and deduplicate: `tuple(set(itertools.chain.from_iterable(results)))`
   c. **Branch:** `isinstance(inst, FloatWithInfo)` -> `FloatWithInfo(risk_key, sum(results), unit=unit)`
   d. **Branch:** `isinstance(inst, SeriesWithInfo)` -> `SeriesWithInfo(sum(results), risk_key=risk_key, unit=unit)`
   e. **Branch:** `isinstance(inst, DataFrameWithInfo)` -> `DataFrameWithInfo(aggregate_risk(results, allow_heterogeneous_types=...), risk_key=risk_key, unit=unit)`
   f. **Branch:** (Implicit) None of the above -> returns `None` (falls through without explicit return)

Total branch points: ~13.

**Elixir mapping:** Pattern-match on the first element's type, then reduce. The recursive dict case maps naturally to `Map.new/2` with recursive calls.

---

### subtract_risk(left: DataFrameWithInfo, right: DataFrameWithInfo) -> pd.DataFrame

Purpose: Subtract bucketed risk by negating the right side and aggregating.

**Algorithm:**
1. Assert `left.columns.names == right.columns.names`
2. Assert `'value' in left.columns.names`
3. `right_negated = copy(right)` -- shallow copy
4. `right_negated.value *= -1`
5. Return `aggregate_risk((left, right_negated))`

---

### sort_values(data: Iterable, columns: Tuple[str, ...], by: Tuple[str, ...]) -> Iterable

Purpose: Sort rows of data using column-specific sort functions.

**Algorithm:**
1. Compute `indices`: positions of `by` columns within `columns` (skip columns not in `columns`)
2. Initialize `fns` list of length `len(columns)` filled with `None`
3. For each index in `indices`: look up sort function from `__column_sort_fns` (may be `None`)
4. Define `cmp(row)` closure: builds a tuple of sort keys:
   a. For each index `i` in `indices`:
      - **Branch:** If `fns[i]` exists -> apply it to `row[i]`, using `0` if result is `None`
      - **Branch:** Else -> use `row[i]` directly
5. Return `sorted(data, key=cmp)`

---

### sort_risk(df: pd.DataFrame, by: Tuple[str, ...] = __risk_columns) -> pd.DataFrame

Purpose: Sort a risk DataFrame by the standard risk columns.

**Algorithm:**
1. Extract `columns = tuple(df.columns)`
2. Call `sort_values(df.values, columns, by)` to get sorted data
3. Build `df_fields`: start with `by` columns that exist in `columns`, then append remaining columns
4. Reconstruct DataFrame from sorted records with reordered columns
5. **Branch:** If `'date'` in result columns -> set as index
6. Return result

---

### combine_risk_key(key_1: RiskKey, key_2: RiskKey) -> RiskKey

Purpose: Merge two risk keys, keeping field values only where they agree.

**Algorithm:**
1. Define inner `get_field_value(field_name)`:
   - **Branch:** If `getattr(key_1, field_name) == getattr(key_2, field_name)` -> return the shared value
   - **Branch:** Else -> return `None`
2. Return `RiskKey(get_field_value("provider"), get_field_value("date"), get_field_value("market"), get_field_value("params"), get_field_value("scenario"), get_field_value("risk_measure"))`

**Elixir mapping:** Zip the two RiskKey tuples, compare element-wise, keep matches or `nil`.

## State Mutation

- **Module-level globals `__column_sort_fns` and `__risk_columns`:** Read-only after module load. No mutation.
- **`gs_quant.config.display_options`:** Read (never written) as a fallback in `UnsupportedValue._to_records` and `DataFrameWithInfo._to_records`.
- **`ResultInfo` private fields (`__risk_key`, `__unit`, `__error`, `__request_id`):** Set once in `__init__`, never mutated afterward. However, `SeriesWithInfo.__mul__` calls `ResultInfo.__init__` on a *new* object to reinitialize these fields.
- **`subtract_risk` mutates `right_negated.value`:** The shallow copy via `copy(right)` means the DataFrame structure is new but underlying data may share references. The `*= -1` on the `value` column mutates the copy in place.
- **Thread safety:** No explicit locking. The `Future.result()` call in `aggregate_risk` blocks the calling thread. In Elixir, replace with `Task.await/1` or `GenStage` demand.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `ResultInfo.composition_info` | Components have different market locations |
| `AttributeError` | `ErrorValue.__getattr__` | Any attribute access on ErrorValue that does not exist |
| `TypeError` | `UnsupportedValue._to_records` | `display_options` is not `None` and not `DisplayOptions` |
| `TypeError` | `DataFrameWithInfo._to_records` | `display_options` is not `None` and not `DisplayOptions` |
| `Exception` | `aggregate_results` | Any result `isinstance(result, Exception)` -- raises bare `Exception` |
| `ValueError` | `aggregate_results` | Any result has truthy `.error` |
| `ValueError` | `aggregate_results` | Heterogeneous types when `allow_heterogeneous_types=False` |
| `ValueError` | `aggregate_results` | Different units across results |
| `ValueError` | `aggregate_results` | Different `ex_historical_diddle` keys when `allow_mismatch_risk_keys=False` |
| `ValueError` | `FloatWithInfo.__add__` | Unit mismatch between two `FloatWithInfo` operands |
| `AssertionError` | `subtract_risk` | Column names mismatch or `'value'` not in columns |

## Edge Cases

- **Empty results to `aggregate_results`:** Returns `None` (explicit check for empty tuple).
- **Empty DataFrame in `DataFrameWithInfo.raw_value`:** Returns `pd.DataFrame(self)` immediately, skipping date index logic.
- **Empty DataFrame in `DataFrameWithInfo._to_records`:** Respects `show_na` setting to decide between `[{..., 'value': None}]` and `[]`.
- **`MQVSValidatorDefnsWithInfo` with falsy `value`:** `self.validators` is never assigned; subsequent `raw_value` access will raise `AttributeError`.
- **`FloatWithInfo.__repr__` with `unit` containing power=0:** The unit entry is silently skipped (neither numerator nor denominator), which could yield an empty `unit_str` even though `self.unit` is truthy.
- **`FloatWithInfo.__repr__` with `unit` that is truthy but not a dict:** Falls through to return bare `res` without unit annotation.
- **`aggregate_results` with mixed types:** The `isinstance(inst, dict)` check fires for `DictWithInfo` too (since it inherits from `dict`), so dict-typed results take the recursive path rather than the `DataFrameWithInfo` path.
- **`aggregate_results` implicit fall-through:** If `inst` is not dict, tuple, FloatWithInfo, SeriesWithInfo, or DataFrameWithInfo, the function returns `None` implicitly.
- **`composition_info` with only error components:** Returns empty `dates`/`values` lists and non-empty `errors` dict; `risk_key` is set from first component.
- **`sort_values` with columns not in `by`:** Those columns are skipped in `indices` computation, so sort is only by available matching columns.
- **`sort_risk` with no `'date'` column:** Skips `set_index('date')`.
- **`DataFrameWithInfo.__repr__` bug:** Uses `self.errors` (plural) instead of `self.error` (singular) on line 389 -- likely runtime error when `self.error` is truthy.

## Bugs Found

- Line 389: `DataFrameWithInfo.__repr__` uses `str(self.errors)` instead of `str(self.error)`. The property defined on `ResultInfo` is `error` (singular). `self.errors` will resolve to `pd.DataFrame.errors` if it exists or raise `AttributeError`. This is almost certainly a typo. (OPEN)
- Line 566: `aggregate_results` raises bare `Exception` (no message) when a result is an Exception instance. Should likely re-raise the original exception or provide context. (OPEN)

## Coverage Notes

- **Branch count:** ~58 distinct branch points across the module
- **Key high-branch methods:** `FloatWithInfo.__repr__` (12 branches), `aggregate_results` (~13 branches), `ResultInfo.composition_info` (~7 branches), `DataFrameWithInfo._to_records` (~5 branches)
- **Hard-to-reach branches:**
  - `FloatWithInfo.__repr__` denominator-only path (unit dict with all negative powers)
  - `FloatWithInfo.__repr__` both-empty path (unit dict with all zero powers)
  - `aggregate_results` bare `Exception` raise path
  - `aggregate_results` tuple-type aggregation path
  - `MQVSValidatorDefnsWithInfo` falsy-value path (validators never set)
  - `DataFrameWithInfo.__repr__` error path (triggers the `self.errors` bug)
- **Lines needing `pragma: no cover`:** None currently marked.
- **Elixir test mapping:** Each branch should map to an ExUnit test case. The `compose` static methods and `aggregate_results` type dispatch are the highest-value test targets. The `__repr__` unit-formatting logic should be exhaustively tested with property-based tests (StreamData) for all power sign combinations.
