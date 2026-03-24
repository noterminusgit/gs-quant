# response.py

## Summary
Defines response dataclasses for the cross-asset backtesting API: `RiskResponse`, `BasicBacktestResponse`, and an empty `GenericBacktestResponse`. These wrap deserialized API results including risk results by leg/measure, daily portfolios, transactions, and additional hedge data.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.generic_datatype_encoders` (decode_daily_portfolio), `gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders` (decode_leg_refs, decode_risk_measure_refs, decode_result_tuple, decode_basic_bt_measure_dict, decode_basic_bt_transactions), `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (Transaction, AdditionalResults), `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result` (RiskResults), `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result_datatypes` (RiskResultWithData), `gs_quant.instrument` (Instrument), `gs_quant.priceable` (PriceableImpl), `gs_quant.common` (RiskMeasure), `gs_quant.target.backtests` (FlowVolBacktestMeasure)
- External: `datetime`, `dataclasses`, `dataclasses_json` (dataclass_json, LetterCase, config)

## Type Definitions

### RiskResponse (dataclass, dataclass_json)

| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| legRefs | `Dict[str, PriceableImpl]` | `None` | `decode_leg_refs` | Map of leg ID to instrument |
| riskMeasureRefs | `Dict[str, RiskMeasure]` | `None` | `decode_risk_measure_refs` | Map of measure ID to RiskMeasure |
| results | `Tuple[RiskResults, ...]` | `None` | `decode_result_tuple` | Ordered risk result entries |

Note: Field names `legRefs` and `riskMeasureRefs` use camelCase directly (not snake_case) despite the `LetterCase.CAMEL` decorator. This is effectively a no-op transformation for these already-camelCase names.

### BasicBacktestResponse (dataclass, dataclass_json)

| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| measures | `Dict[FlowVolBacktestMeasure, Dict[dt.date, RiskResultWithData]]` | `None` | `decode_basic_bt_measure_dict` | Results by measure and date |
| portfolio | `Dict[dt.date, Tuple[Instrument, ...]]` | `None` | `decode_daily_portfolio` | Daily portfolio snapshots |
| transactions | `Dict[dt.date, Tuple[Transaction, ...]]` | `None` | `decode_basic_bt_transactions` | Daily transaction records |
| additional_results | `Optional[AdditionalResults]` | `None` | (default dataclass_json) | Extra results (hedges, PnL, etc.) |

### GenericBacktestResponse (dataclass, dataclass_json)
Empty placeholder dataclass with no fields.

## Functions/Methods

### BasicBacktestResponse.from_dict_custom(cls, data: Any, decode_instruments: bool = True) -> BasicBacktestResponse
Purpose: Alternative constructor that optionally skips full instrument deserialization.

**Algorithm:**
1. If `decode_instruments` is `True`, delegate to `cls.from_dict(data)` (standard dataclass_json).
2. Otherwise, manually construct a `BasicBacktestResponse`:
   - `measures`: decoded via `decode_basic_bt_measure_dict(data['measures'])`
   - `portfolio`: decoded via `decode_daily_portfolio(data['portfolio'], decode_instruments)` (passes `False`)
   - `transactions`: decoded via `decode_basic_bt_transactions(data['transactions'], decode_instruments)` (passes `False`)
   - `additional_results`: if `data['additional_results']` is not `None`, decoded via `AdditionalResults.from_dict_custom(data['additional_results'], decode_instruments)`; otherwise `None`.
3. Return the constructed instance.

## Elixir Porting Notes
- Each response dataclass becomes an Elixir struct.
- The `from_dict`/`from_dict_custom` pattern maps to `decode/1` and `decode/2` functions (the second accepting a `decode_instruments` boolean option).
- Dict keys that are enum values (`FlowVolBacktestMeasure`) or dates (`dt.date`) need custom map-key decoding; Elixir maps naturally support any term as key.
- `RiskResponse` field names are already camelCase in Python; in Elixir use snake_case struct fields with a JSON key mapping layer.
- `GenericBacktestResponse` is an empty struct placeholder; port as `defstruct []`.

## Edge Cases
- `from_dict_custom` with `decode_instruments=False` leaves portfolio/transaction instrument data as raw dicts/lists instead of `Instrument` structs.
- `additional_results` can be `None` in the response JSON; the `from_dict_custom` path explicitly checks for this.
- All fields on `RiskResponse` default to `None`, so a completely empty response is structurally valid.
