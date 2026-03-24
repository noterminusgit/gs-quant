# risk_result.py

## Summary
Defines the risk result container types: `RiskResults` (base with reference keys), `RiskResultsByDate` (date-keyed results), and `RiskResultsError` (error responses). Also defines the `RefType` enum for reference key discrimination.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result_datatypes` (RiskResultWithData)
- External: `datetime`, `dataclasses` (dataclass), `enum` (Enum), `dataclasses_json` (dataclass_json, LetterCase)

## Enums and Constants

### RefType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| LEG_ID | `'legId'` | Reference to a leg identifier |
| RISK_MEASURE | `'riskMeasure'` | Reference to a risk measure identifier |

## Type Definitions

### RiskResults (dataclass, dataclass_json)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| refs | `Dict[RefType, str]` | required | Map of reference type to reference ID string |

### RiskResultsByDate (dataclass, dataclass_json)
Inherits: `RiskResults`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| refs | `Dict[RefType, str]` | (inherited) | Reference keys |
| result | `Dict[dt.date, RiskResultWithData]` | required | Date-keyed risk results |

### RiskResultsError (dataclass, dataclass_json)
Inherits: `RiskResults`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| refs | `Dict[RefType, str]` | (inherited) | Reference keys |
| error | `Optional[str]` | `None` | Error message |
| trace_id | `Optional[str]` | `None` | Trace ID for debugging |

## Elixir Porting Notes
- `RiskResults` becomes a base struct or a shared set of fields embedded in each variant.
- The two subtypes (`RiskResultsByDate`, `RiskResultsError`) map to a tagged union: `{:results_by_date, refs, result}` or `{:results_error, refs, error, trace_id}`. Alternatively, two separate struct modules both containing a `refs` field.
- `RefType` enum becomes atoms: `:leg_id`, `:risk_measure` with JSON mapping to `"legId"` and `"riskMeasure"`.
- `Dict[RefType, str]` in Elixir is `%{ref_type() => String.t()}`. The decoder must convert string keys from JSON (`"legId"`) to the enum/atom form.
- `Dict[dt.date, RiskResultWithData]` needs ISO date string keys parsed during deserialization.

## Edge Cases
- `RiskResultsError` may have both `error` and `trace_id` as `None` (all-optional fields).
- The `refs` dict key type is `RefType` (an enum), not a string; deserialization must convert string keys to enum values.
- Discriminating between `RiskResultsByDate` and `RiskResultsError` during decoding is done by checking for `'result'` vs `'error'` keys in the dict (handled in `risk_result_encoders.py`).
