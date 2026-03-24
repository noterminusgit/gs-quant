# risk_result_datatypes.py

## Summary
Defines the polymorphic `RiskResultWithData` base class and its concrete subtypes (`FloatWithData`, `StringWithData`, `VectorWithData`, `MatrixWithData`, `DefnValuesWithData`, `DictsWithData`). Each subtype wraps a different result data type (float, string, pd.Series, pd.DataFrame, instrument definition, dict) along with an optional unit string. The base class implements arithmetic operators for aggregation.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.generic_datatype_encoders` (decode_inst), `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.risk_result_datatype_encoders` (encode_series_result, decode_series_result, encode_dataframe_result, decode_dataframe_result), `gs_quant.priceable` (PriceableImpl)
- External: `dataclasses` (dataclass, field), `dataclasses_json` (dataclass_json, LetterCase, config), `pandas` (pd), `typing` (Optional)

## Type Definitions

### RiskResultWithData (dataclass, dataclass_json, base class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| unit | `Optional[str]` | `None` | Unit of measurement (e.g. currency code) |

### FloatWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| result | `Optional[float]` | `None` | Scalar numeric result |
| type | `str` | `'float'` | Discriminator tag |

### StringWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| result | `Optional[str]` | `None` | String result |
| type | `str` | `'string'` | Discriminator tag |

### VectorWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Encoder/Decoder | Description |
|-------|------|---------|-----------------|-------------|
| result | `Optional[pd.Series]` | `None` | encoder=`encode_series_result`, decoder=`decode_series_result` | Series result |
| type | `str` | `'vector'` | | Discriminator tag |

### MatrixWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Encoder/Decoder | Description |
|-------|------|---------|-----------------|-------------|
| result | `pd.DataFrame` | `None` | encoder=`encode_dataframe_result`, decoder=`decode_dataframe_result` | DataFrame result |
| type | `str` | `'matrix'` | | Discriminator tag |

### DefnValuesWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| result | `Optional[PriceableImpl]` | `None` | `decode_inst` | Instrument definition result |
| type | `str` | `'defn'` | | Discriminator tag |

### DictsWithData (dataclass, dataclass_json)
Inherits: `RiskResultWithData`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| result | `Optional[dict]` | `None` | Raw dictionary result |
| type | `str` | `'dict'` | Discriminator tag |

## Functions/Methods

### RiskResultWithData.check_can_aggregate(self, other)
Purpose: Validate that two results can be combined via arithmetic.

**Algorithm:**
1. If `other` is a `RiskResultWithData`:
   - If `self.unit != other.unit`, raise `ValueError` with both units.
2. Else (raw operand):
   - If `type(other)` does not match `type(self.result)`, raise `TypeError`.

### RiskResultWithData.__add__(self, other) -> RiskResultWithData
Purpose: Add two risk results or a risk result and a raw value.

**Algorithm:**
1. Call `self.check_can_aggregate(other)`.
2. Extract `other_operand` as `other.result` if `other` is `RiskResultWithData`, else `other` directly.
3. Return `type(self)(unit=self.unit, result=self.result + other_operand)`.

### RiskResultWithData.__radd__(self, other) -> RiskResultWithData
Purpose: Right-add (for `raw_value + risk_result`).

**Algorithm:** Same as `__add__` but computes `other_operand + self.result` (preserving operand order).

### RiskResultWithData.__sub__(self, other) -> RiskResultWithData
Purpose: Subtract. Same pattern: `self.result - other_operand`.

### RiskResultWithData.__mul__(self, other) -> RiskResultWithData
Purpose: Multiply. Same pattern: `self.result * other_operand`.

### RiskResultWithData.__rmul__(self, other) -> RiskResultWithData
Purpose: Right-multiply. Same pattern: `other_operand * self.result`.

### RiskResultWithData.__truediv__(self, other) -> RiskResultWithData
Purpose: Divide. Same pattern: `self.result / other_operand`.

## Elixir Porting Notes
- The `RiskResultWithData` hierarchy maps to a tagged union or a protocol. The `type` discriminator field can be used for pattern matching: `%FloatWithData{}`, `%VectorWithData{}`, etc.
- Each concrete type becomes its own Elixir struct module implementing a shared protocol (e.g. `RiskResult.Arithmetic`).
- Arithmetic operators become protocol functions: `RiskResult.add/2`, `RiskResult.subtract/2`, etc. Elixir does not have operator overloading outside of `Kernel` guards, so use named functions or define via `defimpl` for a `Calculable` protocol.
- `pd.Series` maps to a list of `{index, value}` tuples or a custom `Series` struct. `pd.DataFrame` maps to a list of rows or a custom `DataFrame` struct. Consider using `Explorer.DataFrame` if the Elixir project uses the Explorer library.
- The `type(self)(...)` constructor call (polymorphic instantiation) maps to calling the specific struct module's `new/1` function determined at compile time or via a dispatch map.
- `check_can_aggregate` becomes a guard or pattern-match validation before arithmetic.

## Edge Cases
- Arithmetic on `RiskResultWithData` instances with different `unit` values raises `ValueError`.
- Arithmetic with a raw value checks `type(other)` against `type(self.result)`, meaning `FloatWithData(result=1.0) + "abc"` would raise `TypeError`.
- `__radd__` enables `0 + FloatWithData(result=5.0)` to work (useful for `sum()` which starts with `0`).
- All `result` fields default to `None`; arithmetic on `None` results would raise at the Python operator level.
