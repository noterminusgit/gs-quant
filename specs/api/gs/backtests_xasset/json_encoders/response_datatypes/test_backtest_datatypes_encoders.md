# test_backtest_datatypes_encoders.py

## Summary
Contains a single test function that validates the round-trip encoding/decoding of `TransactionCostConfig` and its nested cost model types. Tests deserialization from JSON dicts, equality assertions, and re-serialization fidelity across both snake_case and PascalCase type discriminators.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (TransactionCostConfig, TradingCosts, FixedCostModel, ScaledCostModel, TransactionCostScalingType, AggregateCostModel, CostAggregationType)
- External: `json`

## Functions/Methods

### test_transaction_cost_config_encoding()
Purpose: Validate that `TransactionCostConfig` correctly deserializes from various JSON shapes and round-trips through `to_json()`/`from_dict()`.

**Algorithm (three test cases):**

**Case 1 -- Simple fixed cost model:**
1. Deserialize from `{"tradeCostModel": {"entry": {"cost": 5.0, "type": "fixed_cost_model"}}, "hedgeCostModel": None}`.
2. Assert equals `TransactionCostConfig(TradingCosts(entry=FixedCostModel(cost=5.0), exit=None), hedge_cost_model=None)`.

**Case 2 -- Mixed models with snake_case type strings:**
1. Deserialize from a dict with:
   - `tradeCostModel.entry`: `ScaledCostModel` with `scalingLevel=5.0`, `scalingQuantityType="Vega"`, `type="scaled_cost_model"`.
   - `tradeCostModel.exit`: `AggregateCostModel` with two sub-models (ScaledCostModel + FixedCostModel), `type="aggregate_cost_model"`.
   - `hedgeCostModel.entry`: `FixedCostModel` with `cost=10`, `type="fixed_cost_model"`.
2. Assert equals the expected constructed object.
3. Assert round-trip: `TransactionCostConfig.from_dict(json.loads(tc.to_json()))` equals the original.

**Case 3 -- Same structure with PascalCase type strings:**
1. Same as Case 2 but type strings are `"ScaledCostModel"`, `"FixedCostModel"`, `"AggregateCostModel"` instead of snake_case.
2. Assert equals expected (with `TransactionCostScalingType.Quantity` instead of `.Vega`).
3. Assert round-trip fidelity.

## Elixir Porting Notes
- This test file maps to an ExUnit test module (e.g. `TransactionCostConfigEncodingTest`).
- Each test case becomes a `test "description" do ... end` block.
- `from_dict(dict)` maps to `TransactionCostConfig.from_map(map)`.
- `json.loads(tc.to_json())` maps to `tc |> TransactionCostConfig.to_map() |> Jason.encode!() |> Jason.decode!()`.
- Equality assertions use `assert expected == actual`.
- The test validates that both snake_case and PascalCase type discriminators are accepted; the Elixir decoder must handle both variants.

## Edge Cases
- `hedgeCostModel: None` in Case 1 tests null/nil handling.
- The `type` field accepts both `"fixed_cost_model"` (snake_case) and `"FixedCostModel"` (PascalCase) -- this dual-format support must be preserved.
- Round-trip test (`from_dict(json.loads(to_json()))`) verifies that serialization output can be re-deserialized to an equal object.
