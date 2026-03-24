# backtest_datatypes.py

## Summary
Defines the core data types used in backtest requests and responses: date configuration, trade definitions, transaction records, cost models (with algebraic addition), and market configuration. This is the largest type-definition file in the backtests_xasset package.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders` (legs_decoder), `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.generic_datatype_encoders` (decode_daily_portfolio), `gs_quant.instrument` (Instrument), `gs_quant.interfaces.algebra` (AlgebraicType), `gs_quant.json_convertors` (decode_optional_date, encode_date_tuple, decode_date_tuple, decode_dict_date_key_or_float), `gs_quant.target.backtests` (BacktestTradingQuantityType, EquityMarketModel), `gs_quant.common` (Currency, CurrencyName, PricingLocation)
- External: `dataclasses`, `datetime`, `enum`, `dataclasses_json` (dataclass_json, LetterCase, config), `abc` (abstractmethod)

## Enums and Constants

### TransactionCostModel(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Fixed | `'Fixed'` | Fixed cost model type |

### TransactionDirection(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Entry | `'Entry'` | Trade entry |
| Exit | `'Exit'` | Trade exit |

### RollDateMode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| OTC | `'OTC'` | Over-the-counter roll dates |
| Listed | `'Listed'` | Listed/exchange roll dates |

Has a custom `_missing_` classmethod:
1. If value is `None`, return `None`.
2. Otherwise, case-insensitive lookup: compare `value.lower()` against each member's value.
3. Return matching member or `None`.

### TransactionCostScalingType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Quantity | `'Quantity'` | Scale by quantity |
| Notional | `'Notional'` | Scale by notional |
| Delta | `'Delta'` | Scale by delta |
| Vega | `'Vega'` | Scale by vega |

### CostAggregationType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Sum | `'Sum'` | Sum of sub-model costs |
| Max | `'Max'` | Maximum of sub-model costs |
| Min | `'Min'` | Minimum of sub-model costs |

### HedgeRiskMeasure(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Delta | `'Delta'` | Delta hedging |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _type_to_basic_model_map | `dict` | `{'fixed_cost_model': FixedCostModel, 'scaled_cost_model': ScaledCostModel, 'FixedCostModel': FixedCostModel, 'ScaledCostModel': ScaledCostModel}` | Maps type strings (both snake_case and PascalCase) to basic cost model classes |

## Type Definitions

### Transaction (dataclass, dataclass_json)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| portfolio | `Tuple[Instrument, ...]` | required | Instruments in the transaction |
| portfolio_price | `Optional[float]` | `None` | Total portfolio price |
| cost | `Optional[float]` | `None` | Transaction cost |
| currency | `Optional[Union[Currency, CurrencyName, str]]` | `None` | Currency of the transaction |
| direction | `Optional[TransactionDirection]` | `None` | Entry or Exit |
| quantity | `Optional[float]` | `None` | Trade quantity |

### TradeEvent (dataclass, dataclass_json)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| direction | `TransactionDirection` | required | Entry or Exit |
| price | `float` | required | Event price |
| trade_id | `Optional[str]` | `None` | Optional trade identifier |

### AdditionalResults (dataclass, dataclass_json)
| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| hedges | `Optional[Dict[dt.date, Tuple[Instrument, ...]]]` | `None` | `decode_daily_portfolio` | Daily hedge instruments |
| hedge_pnl | `Optional[Dict[dt.date, float]]` | `None` | (default) | Daily hedge PnL |
| no_of_calculations | `Optional[int]` | `None` | (default) | Total calculation count |
| trade_events | `Optional[Dict[dt.date, Tuple[TradeEvent, ...]]]` | `None` | `decode_trade_event_tuple_dict` | Daily trade events |
| hedge_events | `Optional[Dict[dt.date, Tuple[TradeEvent, ...]]]` | `None` | `decode_trade_event_tuple_dict` | Daily hedge events |

### DateConfig (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| start_date | `dt.date` | `None` | `decode_optional_date` | Backtest start date |
| end_date | `dt.date` | `None` | `decode_optional_date` | Backtest end date |
| frequency | `str` | `'1b'` | (default) | Date frequency (business days) |
| holiday_calendar | `Optional[str]` | `None` | (default) | Holiday calendar name |

### Trade (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Encoder/Decoder | Description |
|-------|------|---------|-----------------|-------------|
| legs | `Optional[Tuple[Instrument, ...]]` | `None` | decoder=`legs_decoder` | Instrument legs |
| buy_frequency | `str` | `None` | (default) | Buy frequency string |
| buy_dates | `Optional[Tuple[dt.date, ...]]` | `None` | encoder=`encode_date_tuple`, decoder=`decode_date_tuple` | Specific buy dates |
| holding_period | `str` | `None` | (default) | Holding period string |
| exit_dates | `Optional[Tuple[dt.date, ...]]` | `None` | encoder=`encode_date_tuple`, decoder=`decode_date_tuple` | Specific exit dates |
| quantity | `Optional[Union[float, dict[dt.date, float]]]` | `None` | decoder=`decode_dict_date_key_or_float` | Quantity (scalar or date-keyed dict) |
| quantity_type | `BacktestTradingQuantityType` | `BacktestTradingQuantityType.quantity` | (default) | Quantity interpretation |

### Model (dataclass, dataclass_json, unsafe_hash=True, repr=False, ABC)
Inherits: `AlgebraicType`

Abstract base for cost models. Defines algebraic operations (`__add__`, `__sub__`, `__mul__`, `__div__`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | | | Empty body; `pass` |

### FixedCostModel (dataclass, dataclass_json, unsafe_hash=True, repr=False)
Inherits: `Model`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| cost | `float` | `0.0` | Fixed cost amount |
| type | `str` | `'FixedCostModel'` | Discriminator for polymorphic decoding |

### ScaledCostModel (dataclass, dataclass_json, unsafe_hash=True, repr=False)
Inherits: `Model`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| scaling_level | `float` | `0.0` | Scaling factor |
| scaling_quantity_type | `TransactionCostScalingType` | `TransactionCostScalingType.Quantity` | What to scale by |
| type | `str` | `'ScaledCostModel'` | Discriminator |

### AggregateCostModel (dataclass, dataclass_json, unsafe_hash=True, repr=False)
Inherits: `Model`

| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| models | `Tuple[Union[FixedCostModel, ScaledCostModel], ...]` | required | `basic_tc_tuple_decoder` | Sub-models |
| aggregation_type | `CostAggregationType` | required | (default) | How to combine sub-model results |
| type | `str` | `'AggregateCostModel'` | (default) | Discriminator |

### TradingCosts (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Decoder | Description |
|-------|------|---------|---------|-------------|
| entry | `Union[FixedCostModel, ScaledCostModel, AggregateCostModel]` | `FixedCostModel(0)` | `tcm_decoder` | Entry cost model |
| exit | `Optional[Union[FixedCostModel, ScaledCostModel, AggregateCostModel]]` | `None` | `tcm_decoder` | Exit cost model |

### TransactionCostConfig (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trade_cost_model | `TradingCosts` | required | Cost model for trades |
| hedge_cost_model | `Optional[TradingCosts]` | `None` | Cost model for hedges |

### Configuration (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| market_data_location | `Optional[PricingLocation]` | `None` | Pricing location |
| market_model | `Optional[EquityMarketModel]` | `None` | Market model |
| cash_accrual | `bool` | `False` | Whether to accrue cash |
| roll_date_mode | `Optional[RollDateMode]` | `None` | Roll date mode (OTC/Listed) |
| combine_roll_signal_entries | `bool` | `False` | Combine roll and signal entries |

### StrategyHedge (dataclass, dataclass_json, unsafe_hash=True, repr=False)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| risk | `HedgeRiskMeasure` | `HedgeRiskMeasure.Delta` | Risk measure to hedge |
| frequency | `str` | `'1b'` | Hedge frequency |
| risk_percentage | `float` | `100` | Percentage of risk to hedge |

## Functions/Methods

### decode_trade_event_tuple_dict(results: dict) -> Dict[dt.date, Tuple[TradeEvent, ...]]
Purpose: Decode a dict of ISO date strings to lists of TradeEvent dicts into typed form.

**Algorithm:**
1. For each key-value in `results`: parse key via `dt.date.fromisoformat`, decode each element via `TradeEvent.from_dict`.
2. Return dict of `{date: tuple_of_TradeEvents}`.

### AdditionalResults.from_dict_custom(cls, data: Any, decode_instruments: bool = True) -> AdditionalResults
Purpose: Alternative constructor that optionally skips instrument deserialization.

**Algorithm:**
1. If `decode_instruments` is `True`, delegate to `cls.from_dict(data)`.
2. Otherwise, construct `AdditionalResults` manually:
   - `hedges`: via `decode_daily_portfolio(data['hedges'], decode_instruments)`
   - `hedge_pnl`: raw `data['hedge_pnl']`
   - `no_of_calculations`: raw `data['no_of_calculations']`
   - `trade_events`: via `decode_trade_event_tuple_dict(data['trade_events'])`
   - `hedge_events`: via `decode_trade_event_tuple_dict(data['hedge_events'])`

### basic_tc_tuple_decoder(data: Optional[Tuple[dict, ...]]) -> Optional[Union[FixedCostModel, ScaledCostModel]]
Purpose: Decode a tuple of cost model dicts using the `type` discriminator field.

**Algorithm:**
1. If `data` is `None`, return `None`.
2. For each dict `m` in `data`, look up `m['type']` in `_type_to_basic_model_map` and call `.from_dict(m)`.
3. Return as tuple.

### tcm_decoder(data: Optional[dict]) -> Optional[Union[FixedCostModel, ScaledCostModel, AggregateCostModel]]
Purpose: Decode a single cost model dict, including `AggregateCostModel`.

**Algorithm:**
1. Build `full_type_map` by merging `_type_to_basic_model_map` with aggregate entries (`'aggregate_cost_model'` and `'AggregateCostModel'`).
2. If `data` is not `None`, look up `data['type']` and call `.from_dict(data)`.
3. Otherwise return `None`.

### Model.__add__(self, other) -> Model
Purpose: Add two cost models together.

**Algorithm:**
1. If `other` is not a `Model`, raise `TypeError`.
2. If `other` is an `AggregateCostModel`, delegate to `other + self`.
3. Check if same type and all fields equal except the scaling property; if so, create a copy and add scaling properties.
4. Otherwise, wrap both in `AggregateCostModel` with `CostAggregationType.Sum`.

### Model.__sub__, __mul__, __div__
All raise `NotImplementedError`.

### AggregateCostModel.__add__(self, other)
Purpose: Add aggregate cost models.

**Algorithm:**
1. If `other` is an `AggregateCostModel` with the same `aggregation_type`, concatenate models tuples.
2. Otherwise raise `TypeError`.

### FixedCostModel.__eq__, ScaledCostModel.__eq__, AggregateCostModel.__eq__
Custom equality: compare only the relevant data fields (not `type` string).

## Elixir Porting Notes
- Enums become modules with `@type t :: :otc | :listed` etc., or string-backed atoms.
- `RollDateMode._missing_` (case-insensitive lookup) maps to a `parse/1` function that downcases the input before matching.
- The `Model` ABC with `AlgebraicType` and `__add__` maps to a protocol (e.g. `CostModel.Algebra`) with `add/2`, `scale/2` etc. Elixir structs implementing the protocol provide their own clauses.
- Polymorphic deserialization via `_type_to_basic_model_map` / `tcm_decoder` maps to a `decode/1` function that pattern-matches on the `"type"` key.
- `dataclasses.replace` in `Model.__add__` maps to `struct(self, %{field => new_value})` or `Map.put`.
- `unsafe_hash=True` is irrelevant in Elixir (structs are value-comparable by default).

## Edge Cases
- `RollDateMode._missing_` returns `None` for `None` input (not an error).
- `basic_tc_tuple_decoder` accepts both snake_case (`fixed_cost_model`) and PascalCase (`FixedCostModel`) type strings.
- `Model.__sub__` error message incorrectly says "Multiplication" instead of "Subtraction" -- this is a bug in the Python source.
- `Trade.quantity` is a union of `float` and `dict[dt.date, float]`; deserialization uses `decode_dict_date_key_or_float` which must disambiguate.

## Bugs Found
- Line 189: `__sub__` error message says "Multiplication not implemented" -- should say "Subtraction not implemented" (OPEN).
