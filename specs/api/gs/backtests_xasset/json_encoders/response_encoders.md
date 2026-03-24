# response_encoders.py

## Summary
Provides encoder and decoder functions for response-level serialization: encoding response objects (risk measures, Series, DataFrames), decoding leg references, risk measure references, risk result tuples, basic backtest measure dicts, and basic backtest transactions (with currency type resolution).

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.generic_datatype_encoders` (decode_inst_tuple, decode_inst), `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.risk_result_datatype_encoders` (encode_series_result, encode_dataframe_result), `gs_quant.api.gs.backtests_xasset.json_encoders.response_datatypes.risk_result_encoders` (decode_risk_result, decode_risk_result_with_data), `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (Transaction, TransactionDirection), `gs_quant.api.gs.backtests_xasset.response_datatypes.risk_result_datatypes` (RiskResultWithData), `gs_quant.common` (Currency, CurrencyName, RiskMeasure), `gs_quant.json_convertors_common` (encode_risk_measure, decode_risk_measure), `gs_quant.priceable` (PriceableImpl), `gs_quant.target.backtests` (FlowVolBacktestMeasure)
- External: `datetime`, `pandas` (pd)

## Functions/Methods

### encode_response_obj(data: Any) -> Dict
Purpose: Encode a response object to a JSON-serializable dict based on its type.

**Algorithm:**
1. If `data` is a `RiskMeasure`, return `encode_risk_measure(data)`.
2. If `data` is a `pd.Series`, return `encode_series_result(data)`.
3. If `data` is a `pd.DataFrame`, return `encode_dataframe_result(data)`.
4. Otherwise, return `data.to_dict()`.

### decode_leg_refs(d: dict) -> Dict[str, PriceableImpl]
Purpose: Decode a dict of leg ID to instrument dict into typed form.

**Algorithm:**
1. For each `{k: v}` in `d`, decode `v` via `decode_inst(v)`.
2. Return `{k: instrument}` dict.

### decode_risk_measure_refs(d: dict) -> Dict[str, RiskMeasure]
Purpose: Decode a dict of measure ID to risk measure dict into typed form.

**Algorithm:**
1. For each `{k: v}` in `d`, decode `v` via `decode_risk_measure(v)`.
2. Return `{k: risk_measure}` dict.

### decode_result_tuple(results: tuple) -> Tuple[RiskResults, ...]
Purpose: Decode a tuple of raw risk result dicts into `RiskResults` objects.

**Algorithm:**
1. For each `r` in `results`, decode via `decode_risk_result(r)`.
2. Return as tuple.

### decode_basic_bt_measure_dict(results: dict) -> Dict[FlowVolBacktestMeasure, Dict[dt.date, RiskResultWithData]]
Purpose: Decode the nested measures dict from a basic backtest response.

**Algorithm:**
1. For each top-level key `k` (measure name string), value `v` (date-keyed results):
   a. Convert `k` to `FlowVolBacktestMeasure(k)`.
   b. For each `{d: r}` in `v`: parse `d` via `dt.date.fromisoformat`, decode `r` via `decode_risk_result_with_data`.
2. Return nested dict.

### decode_basic_bt_transactions(results: dict, decode_instruments: bool = True) -> Dict[dt.date, Tuple[Transaction, ...]]
Purpose: Decode the transactions dict, manually constructing `Transaction` objects with currency type resolution.

**Algorithm:**
1. Define inner function `to_ccy(s: str)`:
   a. If `s` is in `Currency` values, return `Currency(s)`.
   b. Else if `s` is in `CurrencyName` values, return `CurrencyName(s)`.
   c. Else return raw string `s`.
2. For each `{k: v}` in `results` (k = ISO date string, v = list of transaction dicts):
   a. Parse `k` via `dt.date.fromisoformat`.
   b. For each transaction dict `t`:
      - `portfolio`: if `decode_instruments`, decode via `decode_inst_tuple(t['portfolio'])`; otherwise use raw `t['portfolio']`.
      - `portfolio_price`: `t.get('portfolio_price')`.
      - `cost`: `t.get('cost')`.
      - `currency`: if `t.get('currency')` is truthy, convert via `to_ccy`; else `None`.
      - `direction`: if `t.get('direction')` is truthy, convert via `TransactionDirection(t['direction'])`; else `None`.
      - `quantity`: `t.get('quantity')`.
3. Return dict of `{date: tuple_of_Transactions}`.

## Elixir Porting Notes
- `encode_response_obj` maps to multi-clause pattern matching on struct type.
- `decode_leg_refs` / `decode_risk_measure_refs` are simple map transformations: `Map.new(d, fn {k, v} -> {k, decode(v)} end)`.
- `decode_basic_bt_transactions` is the most complex function; the `to_ccy` inner function maps to a multi-clause helper that tries `Currency.parse/1`, then `CurrencyName.parse/1`, then returns the raw string.
- The `decode_instruments` boolean parameter threads through multiple decoder functions; in Elixir, consider an options keyword list.
- `FlowVolBacktestMeasure(k)` enum construction from string needs a parse function in the Elixir enum module.

## Edge Cases
- `encode_response_obj` falls through to `data.to_dict()` for any unrecognized type; if `data` has no `to_dict`, it will raise `AttributeError`.
- `to_ccy` does a linear scan of `Currency` and `CurrencyName` enum values; order matters (Currency is tried first).
- `decode_basic_bt_transactions` uses `.get()` for optional fields, returning `None` if absent; the `currency` and `direction` fields are only decoded if truthy (not just non-None).
- `decode_instruments=False` leaves portfolio data as raw dicts/lists.
