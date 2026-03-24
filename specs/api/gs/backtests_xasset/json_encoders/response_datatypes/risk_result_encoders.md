# risk_result_encoders.py

## Summary
Provides decoder functions for polymorphic risk result deserialization: mapping raw result data to typed `RiskResultWithData` subtypes via a type discriminator, mapping raw Python values to their corresponding datatype classes, and decoding `RiskResults` (by-date or error) containers.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result` (RiskResultsByDate, RefType, RiskResultsError, RiskResults), `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result_datatypes` (FloatWithData, StringWithData, VectorWithData, MatrixWithData, RiskResultWithData, DefnValuesWithData, DictsWithData), `gs_quant.priceable` (PriceableImpl)
- External: `datetime`, `pandas` (pd)

## Module Constants

### _type_to_datatype_map
| Key | Value | Description |
|-----|-------|-------------|
| `'float'` | `FloatWithData` | Scalar numeric results |
| `'string'` | `StringWithData` | String results |
| `'vector'` | `VectorWithData` | Series results |
| `'matrix'` | `MatrixWithData` | DataFrame results |
| `'defn'` | `DefnValuesWithData` | Instrument definition results |
| `'dict'` | `DictsWithData` | Raw dict results |

## Functions/Methods

### map_result_to_datatype(data: Any) -> Type[RiskResultWithData]
Purpose: Given a raw Python value, return the corresponding `RiskResultWithData` subclass.

**Algorithm:**
1. If `data` is `float` or `int`, return `FloatWithData`.
2. If `data` is `str`, return `StringWithData`.
3. If `data` is `pd.Series`, return `VectorWithData`.
4. If `data` is `pd.DataFrame`, return `MatrixWithData`.
5. If `data` is `PriceableImpl`, return `DefnValuesWithData`.
6. If `data` is `dict`, return `DictsWithData`.
7. Otherwise raise `ValueError('Cannot assign result type to data')`.

### decode_risk_result_with_data(r: dict) -> RiskResultWithData
Purpose: Decode a single risk result dict using the `type` discriminator field.

**Algorithm:**
1. Look up `r['type']` in `_type_to_datatype_map`.
2. Call `.from_dict(r)` on the matched class.
3. Return the decoded instance.

### decode_risk_result(d: dict) -> RiskResults
Purpose: Decode a risk result container dict, discriminating between success (by-date) and error results.

**Algorithm:**
1. Parse `d['refs']` into `{RefType(k): v}` dict.
2. If `'result'` key exists in `d`:
   a. For each `{k: v}` in `d['result']`: parse `k` as `dt.date.fromisoformat`, decode `v` via `decode_risk_result_with_data`.
   b. Return `RiskResultsByDate(refs, result_dict)`.
3. Else (error case):
   a. Return `RiskResultsError(refs, d['error'], d['trace_id'])`.

## Elixir Porting Notes
- `_type_to_datatype_map` becomes a module attribute map: `@type_map %{"float" => FloatWithData, ...}`.
- `map_result_to_datatype` maps to multi-clause pattern matching using guards: `def map_result_to_datatype(data) when is_float(data)`, etc. For pandas types, match on the struct type.
- `decode_risk_result_with_data` maps to `@type_map[type_string].from_map(r)`.
- `decode_risk_result` discrimination between success/error maps to pattern matching on `Map.has_key?(d, "result")`.
- `RefType` string-to-enum conversion in `decode_risk_result` maps to atom construction: `String.to_existing_atom/1` or a lookup map.

## Edge Cases
- `map_result_to_datatype` treats `int` as `FloatWithData` (integers are mapped to the float type).
- `decode_risk_result` requires either `'result'` or `'error'`+`'trace_id'` keys; missing both would raise a `KeyError`.
- Unknown `type` values in `decode_risk_result_with_data` would raise a `KeyError` from the map lookup.
- `RefType` construction from string keys in `decode_risk_result` would raise `ValueError` for unknown ref type strings.
