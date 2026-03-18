# equity_vol_engine.py

## Summary
Implements the `EquityVolEngine` backtest engine for equity volatility strategies, along with helper functions for mapping trading quantity types and detecting synthetic forwards, a `BacktestResult` wrapper for extracting measure series/portfolio/trade history from backtest results, and a `TenorParser` utility for parsing expiration date strings with optional mode suffixes (e.g., `"3m@listed"`).

## Functions/Classes

### `get_backtest_trading_quantity_type(scaling_type, risk)` (lines 59-70)
Maps a `ScalingActionType` and risk measure to a `BacktestTradingQuantityType`.
1. If `scaling_type == ScalingActionType.size` -> return `quantity`
2. If `scaling_type == ScalingActionType.NAV` -> return `NAV`
3. If `risk == EqSpot` -> return `notional`
4. If `risk == EqGamma` -> return `gamma`
5. If `risk == EqVega` -> return `vega`
6. Else -> raise `ValueError`

Branch points: 6 (5 early returns + 1 error raise)

### `is_synthetic_forward(priceable)` (lines 73-93)
Checks whether a priceable is a synthetic forward (Portfolio of 2 EqOptions forming a long call / short put pair).
1. Check `isinstance(priceable, Portfolio)` -> if False, return False
2. Check `len(priceable) == 2` -> if False, return False (via `is_syn_fwd`)
3. Check both elements are `EqOption` -> if False, return False (via `&=`)
4. Check matching underlier, expiration_date, and strike_price -> if any mismatch, `is_syn_fwd` becomes False
5. Check call/buy and put/sell membership in the set of option_type/buy_sell pairs -> **BUGGY** (see Bugs Found)

Branch points: 5 (Portfolio check, size check, EqOption check, attribute match check, option type/buy_sell check)

### `BacktestResult` (lines 96-143)
Wraps raw backtest results and exposes convenience methods.

#### `__init__(self, results)` (line 97)
Stores `results` in `self._results`.

#### `get_measure_series(self, measure)` (lines 100-107)
1. Uses `next(iter(...))` to find the first risk entry whose `name == measure.value`, defaulting to empty tuple
2. Constructs a DataFrame from the timeseries records
3. If DataFrame is empty -> return empty DataFrame (branch point)
4. Else -> convert `date` column to datetime, set as index, return `value` series

Branch points: 2 (empty vs non-empty DataFrame)

#### `get_portfolio_history(self)` (lines 109-120)
1. Iterates `self._results.portfolio`
2. For each item, maps positions to dicts merging `date`, `quantity`, and instrument fields
3. Returns a DataFrame of all accumulated position records

Branch points: 1 (empty vs non-empty portfolio)

#### `get_trade_history(self)` (lines 122-143)
1. Iterates `self._results.portfolio`
2. For each item, iterates `transactions`
3. For each transaction, maps trades to dicts including date, quantity, transactionType, price, and cost
4. Cost is set to `transaction.get('cost')` only if `len(transaction['trades']) == 1`, else `None`
5. Returns a DataFrame of all accumulated trade records

Branch points: 2 (empty portfolio, cost conditional on single-trade transactions)

### `EquityVolEngine` (lines 146-434)

#### `check_strategy(cls, strategy)` (lines 148-292)
Validates a strategy's triggers and actions. Returns a list of error strings (empty means valid).
1. If `len(strategy.initial_portfolio) > 0` -> append error
2. If `len(strategy.triggers) > 3` -> append error
3. If any trigger is not `AggregateTrigger` or `PeriodicTrigger` -> append error
4. For each `AggregateTrigger`:
   a. If not exactly 2 sub-triggers -> append error
   b. If not exactly 1 `DateTriggerRequirements` -> append error
   c. If not exactly 1 `PortfolioTriggerRequirements` -> append error
   d. If portfolio trigger's `data_source != 'len'` or `trigger_level != 0` -> append error
5. Collect all actions from all triggers; warn if `ExitPositionAction` present (DeprecationWarning)
6. Warn if `EnterPositionQuantityScaledAction` present (DeprecationWarning)
7. If any action is not one of the 6 supported types -> append error
8. If duplicate action types -> append error
9. For each child trigger (skipping `PortfolioTrigger`):
   a. If trigger has != 1 action -> append error
   b. For each action:
      - If Enter/AddTrade/AddScaled:
        i.   If `PeriodicTrigger` and frequency != trade_duration -> append error
        ii.  If any priceable not EqOption/EqVarianceSwap -> append error
        iii. If `EnterPositionQuantityScaledAction` with None quantity/type -> append error
        iv.  If `AddScaledTradeAction` with None scaling_level/type -> append error
        v.   If mixed expiry date modes -> append error
        vi.  If invalid expiry date mode (not 'otc'/'listed') -> append error
        vii. If any priceable size != 1 -> append error
      - If `HedgeAction`:
        i.   If not synthetic forward -> append error
        ii.  If frequency != trade_duration -> append error
        iii. If risk != EqDelta -> append error
      - If `ExitPositionAction`/`ExitTradeAction` -> continue (no checks)
      - Else -> append unsupported action error

Branch points: ~20+ distinct conditional branches

#### `supports_strategy(cls, strategy)` (lines 294-300)
1. Calls `check_strategy`
2. If any errors -> return `False`
3. Else -> return `True`

Branch points: 2

#### `run_backtest(cls, strategy, start, end, market_model, cash_accrual)` (lines 302-401)
1. Calls `check_strategy`; if errors -> raise `RuntimeError`
2. Iterates triggers:
   a. If `AggregateTrigger`:
      - Extracts date signal from `DateTriggerRequirements`
      - Checks portfolio trigger direction: if `EQUAL` and level 0 -> `trade_in_signals`, else `trade_out_signals`
   b. Gets first action from trigger:
      - If `EnterPositionQuantityScaledAction` -> sets underlier_list, roll_frequency, trade_quantity/type, expiry_date_mode, transaction_cost
      - If `AddTradeAction` -> same but quantity=1, type=quantity
      - If `AddScaledTradeAction` -> same but uses scaling_level and calls `get_backtest_trading_quantity_type`
      - If `HedgeAction` -> creates `DeltaHedgeParameters`, sets hedge transaction cost
3. Builds `TransactionCostConfig` (None if no costs set)
4. Constructs `StrategySystematic` and calls `backtest(start, end)`
5. Returns `BacktestResult(result)`

Branch points: ~8 (error check, AggregateTrigger, trade_in vs trade_out, 4 action types, transaction_cost_config None check)

#### `__get_underlier_list(cls, priceables)` (lines 403-415)
1. Deep-copies priceables
2. For each priceable:
   a. Parses expiration_date via `TenorParser`, replaces with date part
   b. If priceable `hasattr` `trade_as`:
      - Gets enum value from mode; sets `trade_as` to existing value or expiry_date_mode (if `TradeAs`), else `None`
      - Prints `priceable.trade_as` (debug leftover)

Branch points: 3 (loop iteration, hasattr check, isinstance TradeAs check)

#### `__map_tc_model(cls, model)` (lines 417-434)
1. If `ConstantTransactionModel` -> return `FixedCostModel(cost=model.cost)`
2. If `ScaledTransactionModel`:
   a. If `model.scaling_type == EqVega` -> use `TransactionCostScalingType.Vega`
   b. Else -> call `get_enum_value`; if result not `TransactionCostScalingType` -> raise `RuntimeError`
   c. Return `ScaledCostModel`
3. If `AggregateTransactionModel` -> recursively map sub-models, return `AggregateCostModel`
4. Else (including `None`) -> return `None`

Branch points: 5 (ConstantTxn, ScaledTxn with EqVega, ScaledTxn with other valid type, ScaledTxn with invalid type raising RuntimeError, AggregateTxn, fallthrough None)

### `TenorParser` (lines 437-462)
Parses expiration date strings in `"tenor@mode"` format (e.g., `"3m@listed"`).

#### `__init__(self, expiry)` (line 441)
Stores `expiry` (str or `dt.date`).

#### `get_date(self)` (lines 444-452)
1. If `expiry` is `dt.date` -> return it directly
2. Regex match on `"(.*)@(.*)"`; if match -> return group 1 (tenor part)
3. Else -> return `expiry` as-is

Branch points: 3

#### `get_mode(self)` (lines 454-462)
1. If `expiry` is `dt.date` -> return `None`
2. Regex match; if match -> return group 2 (mode part)
3. Else -> return `None`

Branch points: 3

## Edge Cases
- `get_backtest_trading_quantity_type`: passing an unrecognized `scaling_type` + `risk` combination raises `ValueError`
- `is_synthetic_forward`: passing a non-Portfolio priceable returns False; passing a Portfolio with != 2 elements returns False; passing a Portfolio of 2 non-EqOption instruments returns False
- `BacktestResult.get_measure_series`: when no risk entry matches the measure name, `next()` returns `()` and produces an empty DataFrame
- `BacktestResult.get_trade_history`: cost field is `None` when a transaction has more than one trade
- `EquityVolEngine.check_strategy`: accessing `portfolio_triggers[0]` (line 173) will raise `IndexError` if the earlier check at line 171 already appended an error but no `PortfolioTriggerRequirements` exists (execution continues past the check)
- `EquityVolEngine.__get_underlier_list`: contains a `print(priceable.trade_as)` statement on line 414 that appears to be a debug leftover
- `TenorParser.get_date` / `get_mode`: passing `dt.date` always returns the date / `None` respectively, never attempts regex

## Bugs Found
- **Line 88**: `is_syn_fwd &= (OptionType.Call, BuySell.Buy) and (OptionType.Put, BuySell.Sell) in {...}` -- Python's `and` operator evaluates `(OptionType.Call, BuySell.Buy)` as truthy (non-empty tuple), then evaluates `(OptionType.Put, BuySell.Sell) in {set}`. The result is that only the Put/Sell membership is checked; the Call/Buy membership is never verified. A portfolio with two puts (one sell, one buy) where underlier/expiry/strike match would incorrectly be identified as a synthetic forward. Fix: use two separate `in` checks combined with `and`.
- **Line 173**: After appending an error that no `PortfolioTriggerRequirements` found (line 172), execution falls through to line 173 which accesses `portfolio_triggers[0]`, causing an `IndexError` if the list is empty. This should be guarded with an `else`/`continue`.
- **Line 414**: `print(priceable.trade_as)` is a debug statement left in production code.

## Coverage Notes
**Estimated branch count**: ~50+ branches across all functions and methods.

**Mocking notes**:
- `StrategySystematic` and its `backtest()` method must be mocked in `run_backtest` tests to avoid real API calls
- `warnings.warn` calls in `check_strategy` should be tested with `pytest.warns(DeprecationWarning)`
- `copy.deepcopy` in `__get_underlier_list` means priceables are not mutated; tests should verify original is unchanged
- `get_enum_value` (from `gs_quant.base`) is used in `__get_underlier_list` and `__map_tc_model`; may need mocking if enum resolution has side effects
- `BacktestResult` methods depend on the shape of `self._results`; create mock result objects with `.risks`, `.portfolio` attributes containing appropriate nested structures
- Private methods `__get_underlier_list` and `__map_tc_model` are name-mangled to `_EquityVolEngine__get_underlier_list` / `_EquityVolEngine__map_tc_model`; test via `run_backtest` integration or access directly using the mangled names
- `is_synthetic_forward` has 5 nesting levels of conditionals; need separate test cases for each early-exit path
- `TenorParser` needs test cases for `dt.date` input, plain string input (no `@`), and `"tenor@mode"` format
