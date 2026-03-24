# json_convertors_common.py

## Summary
Specialized JSON serialization/deserialization module for `RiskMeasure` and `ParameterisedRiskMeasure` types. Provides encoding of risk measures to camelCase dicts and decoding from dicts back to typed risk measure objects, with support for matching against pre-defined risk measure constants in the `gs_quant.risk` module and handling parameterised variants. This is the wire-format layer for risk measure objects sent to/from the pricing API.

## Dependencies
- Internal: `gs_quant.base` (RiskMeasureParameter), `gs_quant.common` (RiskMeasure, ParameterisedRiskMeasure), `gs_quant.common` (used as module for `getattr` lookup of parameter classes), `gs_quant.risk` (used as module for `getattr` lookup of named risk measures)
- External: `copy` (copy), `enum` (Enum), `typing` (Tuple, Dict, Optional, Union)

## Type Definitions
No new classes or type aliases defined in this module. All types are imported:

- `RiskMeasure` -- from `gs_quant.common`, base risk measure dataclass with fields: `name`, `asset_class`, `measure_type`, etc.
- `ParameterisedRiskMeasure` -- from `gs_quant.common`, extends `RiskMeasure` with a `parameters` field
- `RiskMeasureParameter` -- from `gs_quant.base`, ABC base for parameter dataclasses (e.g., `RiskMeasureWithCurrencyParameter`, `RiskMeasureWithDoubleParameter`, etc.)

## Enums and Constants
No enums or constants defined in this module.

**Implicit type mappings (critical for Elixir port):**

The module relies on a naming convention for parameter classes. The `parameterType` field in a dict (e.g., `"Currency"`) is appended with `"Parameter"` to form a class name (e.g., `"CurrencyParameter"`), which is then looked up via `getattr(common, cls_name)`. Known parameter classes in `gs_quant.target.measures`:

| parameterType Value | Resolved Class Name | Module |
|---------------------|---------------------|--------|
| `"Currency"` | `CurrencyParameter` | `gs_quant.common` (re-exported from `gs_quant.target.measures`) |
| `"Double"` | `DoubleParameter` | same |
| `"ListOfNumber"` | `ListOfNumberParameter` | same |
| `"ListOfString"` | `ListOfStringParameter` | same |
| `"Map"` | `MapParameter` | same |
| `"String"` | `StringParameter` | same |
| `"FiniteDifference"` | `FiniteDifferenceParameter` | same |

The corresponding `ParameterisedRiskMeasure` subclasses are:
- `RiskMeasureWithCurrencyParameter`
- `RiskMeasureWithDoubleParameter`
- `RiskMeasureWithListOfNumberParameter`
- `RiskMeasureWithListOfStringParameter`
- `RiskMeasureWithMapParameter`
- `RiskMeasureWithStringParameter`
- `RiskMeasureWithFiniteDifferenceParameter`

**Risk measure constants:** Named risk measures are attributes of `gs_quant.risk` (e.g., `risk.DollarPrice`, `risk.IRDelta`, etc.). The full set is dynamic and defined in that module.

## Functions/Methods

### gsq_rm_for_name(name: str) -> Optional[RiskMeasure]
Purpose: Look up a pre-defined risk measure constant by name from the `gs_quant.risk` module.

**Algorithm:**
1. Branch: `name is None` -> return `None`
2. Branch: `name not in dir(risk)` -> return `None` (attribute does not exist on module)
3. Branch: name exists -> return `getattr(risk, name)`

**Elixir note:** This is a module-level attribute lookup. In Elixir, maintain a map of `%{"DollarPrice" => %RiskMeasure{...}, ...}` or use a function head per known name.

---

### encode_risk_measure(rm: RiskMeasure) -> Dict
Purpose: Serialize a RiskMeasure to a camelCase dict for API consumption.

**Algorithm:**
1. Call `rm.as_dict(as_camel_case=True)` -> `result`
2. Branch: `rm.parameters is not None` -> set `result['parameters'] = rm.parameters.as_dict(as_camel_case=True)`
3. Branch: `rm.parameters is None` -> no modification
4. Return `result`

**Elixir note:** The `as_camel_case=True` converts snake_case field names to camelCase. In Elixir, use a helper like `Recase.to_camel/1` or build the mapping explicitly.

---

### encode_risk_measure_tuple(blob: Tuple[RiskMeasure, ...]) -> Tuple[Dict, ...]
Purpose: Encode a tuple of risk measures to a tuple of dicts.

**Algorithm:**
1. Map `encode_risk_measure` over each element in `blob`
2. Return as tuple

---

### _decode_param(data: dict) -> Optional[RiskMeasureParameter]
Purpose: Extract and decode the `parameters` sub-dict from a risk measure dict into a typed `RiskMeasureParameter` instance.

**Algorithm:**
1. Get `params = data.get('parameters', None)`
2. Branch: `params is not None AND isinstance(params, dict) AND 'parameterType' in params`:
   - Compute `cls_name = params['parameterType'] + 'Parameter'` (e.g., `"Currency"` -> `"CurrencyParameter"`)
   - Look up class: `parameter_cls = getattr(common, cls_name)`
   - Instantiate: `parameter_cls(**{k: v for k, v in params.items() if k != 'parameterType'})` (all fields except `parameterType` are passed as kwargs)
   - Return the parameter instance
3. Branch: any of the conditions fail -> return `None`

**Elixir note:** The `parameterType` field is the discriminator. In Elixir, pattern match on it and call the appropriate struct constructor. The `parameterType` key itself is excluded from the constructor kwargs.

---

### _decode_gsq_risk_measure(data: dict) -> Optional[RiskMeasure]
Purpose: Attempt to decode a dict as a known (pre-defined) risk measure from `gs_quant.risk`, verifying that the asset class and measure type match.

**Algorithm:**
1. Get `name = data.get('name', None)`
2. Call `gsq_rm_for_name(name)` -> `gsq_rm`
3. Branch: `gsq_rm is None` -> return `None` (not a known risk measure)
4. Get `asset_class = data.get('assetClass', None)` and `measure_type = data.get('measureType', None)`
5. Call `_enum_or_str_equal(asset_class, gsq_rm.asset_class)` AND `_enum_or_str_equal(measure_type, gsq_rm.measure_type)`
   - Branch: both match -> proceed
   - Branch: mismatch -> return `None`
6. `result = copy.copy(gsq_rm)` (shallow copy the pre-defined risk measure)
7. Call `_decode_param(data)` -> `param`
8. Branch: `param` is truthy -> `result.parameters = param`
9. Return `result`

**Inner function: `_enum_or_str_equal(a, b)`:**
- Branch: `a is None and b is None` -> return `True`
- Branch: else -> compare `str(a).lower() == str(b).lower()` (handles Enum vs string comparison by converting both to lowercase strings)

**Elixir note:** The `copy.copy` creates a shallow clone of the pre-defined risk measure constant so mutations (adding parameters) do not affect the original. In Elixir, structs are immutable so this is not needed -- just update the struct.

---

### decode_risk_measure(data: Dict) -> RiskMeasure
Purpose: Decode a dict to a RiskMeasure, first trying to match a known risk measure, then falling back to generic deserialization.

**Algorithm:**
1. Call `_decode_gsq_risk_measure(data)` -> `result`
2. Branch: `result is not None` -> return `result` (matched a known risk measure)
3. Branch: `result is None` (fallback to generic):
   - Branch: `'parameters' in data` -> deserialize as `ParameterisedRiskMeasure.from_dict(data)`, then set `result.parameters = _decode_param(data)`
   - Branch: `'parameters' not in data` -> `RiskMeasure.from_dict(data)`
4. Return `result`

**Elixir note:** This is a three-tier decoding strategy: (1) match known constant, (2) parameterised generic, (3) plain generic. In Elixir, implement as pattern matching clauses.

---

### decode_risk_measure_tuple(blob: Tuple[Dict, ...]) -> Optional[Tuple[RiskMeasure, ...]]
Purpose: Decode a tuple/list of risk measure dicts.

**Algorithm:**
1. Branch: `isinstance(blob, (tuple, list))` -> map `decode_risk_measure` over each element, return as tuple
2. Branch: else -> return `None`

## State Mutation
- No module-level mutable state.
- `_decode_gsq_risk_measure` uses `copy.copy` to avoid mutating pre-defined risk measure constants in `gs_quant.risk`. The copy is shallow -- nested objects (if any) share references.
- `result.parameters = param` mutates the copied risk measure object (line 70).

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `_get_dc_type` (called indirectly) | If a parameter class cannot be found -- but this module does not call `_get_dc_type` |
| `AttributeError` | `_decode_param` | If `getattr(common, cls_name)` fails because the computed class name does not exist on `gs_quant.common` (not caught) |
| `TypeError` | `_decode_param` | If the parameter class constructor rejects the provided kwargs (not caught) |

**Note:** This module does not explicitly raise exceptions. Errors propagate from `getattr`, `from_dict`, or constructor calls. The `gsq_rm_for_name` function defensively returns `None` rather than raising on missing names.

## Edge Cases
- `gsq_rm_for_name` with `name=None`: returns `None` (first branch).
- `gsq_rm_for_name` with a name that exists in `dir(risk)` but is not a `RiskMeasure` (e.g., a function or import): returns whatever `getattr` yields. No type check is performed.
- `_enum_or_str_equal` with one `None` and one non-`None`: `str(None)` is `"none"`, so `"none" == str(other).lower()` -- this could match if the other value's string form is `"None"` or `"none"`.
- `_enum_or_str_equal` with Enum values: uses `str(enum_val)` which typically returns `"EnumClass.MEMBER"`, not just the value. This means comparison works only if both sides use the same string representation. For example, `str(AssetClass.Equity)` might be `"AssetClass.Equity"` in which case it would NOT match the string `"Equity"` from the dict.
- `_decode_param` with `params` that is a dict but missing `'parameterType'`: returns `None`.
- `_decode_param` with `params` that is not a dict (e.g., a string): returns `None`.
- `decode_risk_measure` always returns a result (never `None`) -- it is the only function in this module guaranteed to return a non-None value for non-None input.
- `decode_risk_measure_tuple` with a non-list/non-tuple input (e.g., a single dict): returns `None` rather than raising.
- `encode_risk_measure`: the `parameters` sub-dict is only included if `rm.parameters is not None`. The base `as_dict` call may already include a `parameters` key (set to `None`); the explicit assignment overwrites it.

## Bugs Found
None identified.

## Coverage Notes
- Branch count: ~20 distinct branches
- Key branch clusters: `_decode_gsq_risk_measure` (4 branches), `decode_risk_measure` (3 branches), `_decode_param` (2 branches), `_enum_or_str_equal` (2 branches)
- The `_enum_or_str_equal` comparison via `str()` on Enum values may have subtle behavior differences across Python versions (Python 3.11+ changed `str()` on some Enum subclasses)
