# request.py

## Summary
Defines the request dataclasses for the cross-asset backtesting API: `RiskRequest`, `BasicBacktestRequest`, and `GenericBacktestRequest`. Each is a `@dataclass_json` with camelCase serialization and custom field-level encoders/decoders for dates, instruments, enums, and risk measures.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders` (legs_encoder, legs_decoder, enum_decode), `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (DateConfig, Trade, Configuration, TransactionCostConfig, StrategyHedge), `gs_quant.api.gs.backtests_xasset.response_datatypes.generic_backtest_datatypes` (Strategy), `gs_quant.base` (EnumBase), `gs_quant.common` (RiskMeasure), `gs_quant.json_convertors` (decode_optional_date, decode_date_tuple, encode_date_tuple), `gs_quant.json_convertors_common` (encode_risk_measure_tuple, decode_risk_measure_tuple), `gs_quant.priceable` (PriceableImpl), `gs_quant.target.backtests` (FlowVolBacktestMeasure)
- External: `datetime`, `dataclasses`, `dataclasses_json` (dataclass_json, LetterCase, config), `enum`

## Enums and Constants

### RiskProviderEnum(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Default | `"Default"` | Default risk provider |
| DataSetProvider | `"DataSetProvider"` | Dataset-based risk provider |

## Type Definitions

### RiskRequest (dataclass, dataclass_json, unsafe_hash=True, repr=False)

| Field | Type | Default | Encoder/Decoder | Description |
|-------|------|---------|-----------------|-------------|
| start_date | `Optional[dt.date]` | `None` | decoder=`decode_optional_date` | Start date for risk calc |
| end_date | `Optional[dt.date]` | `None` | decoder=`decode_optional_date` | End date for risk calc |
| additional_dates | `Optional[Tuple[dt.date, ...]]` | `None` | encoder=`encode_date_tuple`, decoder=`decode_date_tuple` | Extra specific dates |
| legs | `Optional[Tuple[PriceableImpl, ...]]` | `None` | encoder=`legs_encoder`, decoder=`legs_decoder` | Instrument legs |
| measures | `Optional[Tuple[RiskMeasure, ...]]` | `None` | encoder=`encode_risk_measure_tuple`, decoder=`decode_risk_measure_tuple` | Risk measures to compute |
| risk_provider | `Optional[RiskProviderEnum]` | `None` | decoder=`enum_decode(RiskProviderEnum)` | Risk calculation provider |

### BasicBacktestRequest (dataclass, dataclass_json, unsafe_hash=True, repr=False)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| dates | `DateConfig` | required | Date range and frequency config |
| trades | `Tuple[Trade, ...]` | required | Trade definitions |
| measures | `Tuple[FlowVolBacktestMeasure, ...]` | required | Backtest measures |
| delta_hedge_frequency | `Optional[str]` | `None` | Hedging frequency string |
| transaction_costs | `Optional[TransactionCostConfig]` | `None` | Transaction cost configuration |
| configuration | `Optional[Configuration]` | `None` | Market/model configuration |
| hedge | `Optional[StrategyHedge]` | `None` | Hedge specification |
| risk_provider | `Optional[RiskProviderEnum]` | `None` | Risk provider (custom decoder) |

### GenericBacktestRequest (dataclass, dataclass_json, unsafe_hash=True, repr=False)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| strategy | `Strategy` | required | Strategy to backtest |
| dates | `Union[DateConfig, Tuple[dt.date, ...]]` | required | Date config or explicit date list |
| configuration | `Optional[Configuration]` | `None` | Market/model configuration |

## Elixir Porting Notes
- Each dataclass maps to an Elixir struct with `@enforce_keys` for required fields.
- `@dataclass_json(letter_case=LetterCase.CAMEL)` means JSON keys are camelCase; implement `Jason.Encoder` with key transformation or use a shared `camelize_keys/1` helper.
- Custom field encoders/decoders become `encode/1` and `decode/1` functions composed into the struct's serialization module.
- `RiskProviderEnum` becomes a simple module with string-backed atoms or a dedicated enum type.
- `unsafe_hash=True` is not needed in Elixir since structs are value types.
- `Union[DateConfig, Tuple[dt.date, ...]]` for `GenericBacktestRequest.dates` needs a tagged-union or protocol-based approach in Elixir: pattern match on the shape during deserialization.
- `PriceableImpl` legs serialization relies on `Instrument.from_dict/to_dict`; the Elixir port needs the equivalent instrument codec.

## Edge Cases
- All fields on `RiskRequest` are optional; a fully-nil request is structurally valid.
- `enum_decode` returns `None` for `null` string literal `"null"` as well as actual `None`.
- `GenericBacktestRequest.dates` is a union type: deserialization must distinguish between a dict (DateConfig) and a list of date strings.
