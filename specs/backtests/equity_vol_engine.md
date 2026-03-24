# equity_vol_engine.py

## Summary
Implements the `EquityVolEngine` backtest engine for equity volatility strategies, along with module-level helpers `get_backtest_trading_quantity_type` (maps scaling types to trading quantity types) and `is_synthetic_forward` (detects synthetic forward portfolios). Also provides a `BacktestResult` wrapper for extracting measure series, portfolio history, and trade history from raw backtest results, and a `TenorParser` utility for parsing expiration date strings with optional mode suffixes (e.g., `"3m@listed"`).

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (`TransactionCostConfig`, `TradingCosts`, `FixedCostModel`, `ScaledCostModel`, `TransactionCostScalingType`, `AggregateCostModel`, `CostAggregationType`), `gs_quant.backtests.actions` (aliased as `a`: `ScalingActionType`, `BacktestTradingQuantityType`, `EnterPositionQuantityScaledAction`, `HedgeAction`, `ExitPositionAction`, `ExitTradeAction`, `AddTradeAction`, `AddScaledTradeAction`), `gs_quant.backtests.triggers` (aliased as `t`: `AggregateTrigger`, `PeriodicTrigger`, `DateTriggerRequirements`, `PortfolioTriggerRequirements`, `PortfolioTrigger`, `AggregateTriggerRequirements`, `TriggerDirection`), `gs_quant.backtests.backtest_objects` (`TransactionModel`, `ConstantTransactionModel`, `ScaledTransactionModel`, `AggregateTransactionModel`), `gs_quant.backtests.strategy_systematic` (`StrategySystematic`, `DeltaHedgeParameters`, `TradeInMethod`), `gs_quant.base` (`get_enum_value`), `gs_quant.common` (`OptionType`, `BuySell`, `TradeAs`), `gs_quant.instrument` (`EqOption`, `EqVarianceSwap`), `gs_quant.markets.portfolio` (`Portfolio`), `gs_quant.risk` (`EqDelta`, `EqSpot`, `EqGamma`, `EqVega`), `gs_quant.target.backtests` (`BacktestSignalSeriesItem`, `BacktestTradingQuantityType`, `EquityMarketModel`, `FlowVolBacktestMeasure`)
- External: `copy` (`deepcopy`), `re` (`search`), `warnings` (`warn`), `functools` (`reduce`), `typing` (`Union`), `datetime` (`dt.date`), `pandas` (`pd.DataFrame`, `pd.to_datetime`)

## Type Definitions

### BacktestResult (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _results | `object` | *(required)* | Raw backtest result object with `.risks` and `.portfolio` attributes |

### EquityVolEngine (class)
Inherits: `object`

No instance fields -- all methods are `@classmethod`.

### TenorParser (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| expiry | `Union[str, dt.date]` | *(required)* | The expiration date string or date object to parse |

### Class Constants

#### TenorParser
| Name | Type | Value | Description |
|------|------|-------|-------------|
| expiry_regex | `str` | `'(.*)@(.*)'` | Regex pattern for parsing `"tenor@mode"` format |

## Enums and Constants

None defined in this module (enums are imported from other modules).

## Functions/Methods

### get_backtest_trading_quantity_type(scaling_type, risk) -> BacktestTradingQuantityType
Purpose: Map a `ScalingActionType` and optional risk measure to the corresponding `BacktestTradingQuantityType`.

**Algorithm:**
1. Branch: `scaling_type == ScalingActionType.size` -> return `BacktestTradingQuantityType.quantity`
2. Branch: `scaling_type == ScalingActionType.NAV` -> return `BacktestTradingQuantityType.NAV`
3. Branch: `risk == EqSpot` -> return `BacktestTradingQuantityType.notional`
4. Branch: `risk == EqGamma` -> return `BacktestTradingQuantityType.gamma`
5. Branch: `risk == EqVega` -> return `BacktestTradingQuantityType.vega`
6. Branch: else -> raise `ValueError(f"unable to translate {scaling_type} and {risk}")`

**Raises:** `ValueError` when no mapping exists for the given combination

### is_synthetic_forward(priceable) -> bool
Purpose: Check whether a priceable is a synthetic forward (Portfolio of 2 EqOptions forming a long call / short put pair with matching underlier, expiry, and strike).

**Algorithm:**
1. `is_portfolio = isinstance(priceable, Portfolio)`; `is_syn_fwd = is_portfolio`
2. Branch: `is_portfolio` is False -> skip to step 7
3. `is_size_two = len(priceable) == 2`; `is_syn_fwd = is_size_two`
4. Branch: `is_size_two` is False -> skip to step 7
5. `has_two_eq_options = isinstance(priceable[0], EqOption) and isinstance(priceable[1], EqOption)`; `is_syn_fwd &= has_two_eq_options`
6. Branch: `has_two_eq_options` is True ->
   a. Check underlier, expiration_date, strike_price all match between `priceable[0]` and `priceable[1]`; `is_syn_fwd &= result`
   b. Build `option_set` of `(option_type, buy_sell)` tuples for both options
   c. `is_syn_fwd &= (OptionType.Call, BuySell.Buy) in option_set and (OptionType.Put, BuySell.Sell) in option_set`
7. Return `is_syn_fwd`

### BacktestResult.__init__(self, results) -> None
Purpose: Store raw backtest results.

**Algorithm:**
1. Set `self._results = results`

### BacktestResult.get_measure_series(self, measure: FlowVolBacktestMeasure) -> Union[pd.Series, pd.DataFrame]
Purpose: Extract a time series for a specific measure from the backtest results.

**Algorithm:**
1. Use `next(iter(r.timeseries for r in self._results.risks if r.name == measure.value), ())` to find matching risk entry
2. Construct `df = pd.DataFrame.from_records(data)`
3. Branch: `len(df) == 0` -> return empty DataFrame
4. Branch: else -> convert `date` column to datetime, set as index, return `value` series

### BacktestResult.get_portfolio_history(self) -> pd.DataFrame
Purpose: Extract portfolio position history as a flat DataFrame.

**Algorithm:**
1. Initialize `data = []`
2. For each `item` in `self._results.portfolio`:
   a. Map each position in `item['positions']` to a dict merging `{'date': item['date'], 'quantity': x['quantity']}` with `x['instrument']`
   b. Extend `data` with the resulting list
3. Return `pd.DataFrame(data)`

### BacktestResult.get_trade_history(self) -> pd.DataFrame
Purpose: Extract trade transaction history as a flat DataFrame.

**Algorithm:**
1. Initialize `data = []`
2. For each `item` in `self._results.portfolio`:
   a. For each `transaction` in `item['transactions']`:
      - Map each trade in `transaction['trades']` to a dict with keys: `date`, `quantity`, `transactionType`, `price`, `cost`
      - Branch: `len(transaction['trades']) == 1` -> set `cost = transaction.get('cost')`
      - Branch: else -> set `cost = None`
      - Extend `data` with the resulting list
3. Return `pd.DataFrame(data)`

### EquityVolEngine.check_strategy(cls, strategy) -> list[str]
Purpose: Validate a strategy's triggers and actions for compatibility with the equity vol engine. Returns a list of error strings (empty means valid).

**Algorithm:**
1. Initialize `check_results = []`
2. Branch: `len(strategy.initial_portfolio) > 0` -> append error
3. Branch: `len(strategy.triggers) > 3` -> append error
4. Branch: any trigger not `AggregateTrigger` or `PeriodicTrigger` -> append error
5. For each `AggregateTrigger` (`at`):
   a. Branch: `len(at.trigger_requirements.triggers) != 2` -> append error
   b. Branch: count of `DateTriggerRequirements` != 1 -> append error
   c. Collect `portfolio_triggers` (list of `PortfolioTriggerRequirements`)
   d. Branch: `len(portfolio_triggers) != 1` -> append error
   e. Branch: `portfolio_triggers[0].data_source != 'len'` or `portfolio_triggers[0].trigger_level != 0` -> append error
6. Collect all actions via `reduce` over `strategy.triggers`
7. Branch: any action is `ExitPositionAction` -> emit `DeprecationWarning`
8. Branch: any action is `EnterPositionQuantityScaledAction` -> emit `DeprecationWarning`
9. Branch: any action not one of the 6 supported types -> append error
10. Branch: duplicate action types (set length != list length) -> append error
11. Flatten all child triggers (unwrapping `AggregateTriggerRequirements`)
12. For each child trigger:
    a. Branch: `isinstance(trigger, PortfolioTrigger)` -> `continue`
    b. Branch: `len(trigger.actions) != 1` -> append error
    c. For each action:
       - Branch: `isinstance(action, (EnterPositionQuantityScaledAction, AddTradeAction, AddScaledTradeAction))` ->
         i.   Branch: `isinstance(trigger, PeriodicTrigger) and frequency != trade_duration` -> append error
         ii.  Branch: any priceable not `EqOption` or `EqVarianceSwap` -> append error
         iii. Branch: `EnterPositionQuantityScaledAction` with `None` quantity or type -> append error
         iv.  Branch: `AddScaledTradeAction` with `None` scaling_level or type -> append error
         v.   Branch: mixed expiry date modes (more than 1 unique mode) -> append error
         vi.  Branch: expiry date mode not in `[None, 'otc', 'listed']` -> append error
         vii. Branch: any priceable size != 1 -> append error
       - Branch: `isinstance(action, HedgeAction)` ->
         i.   Branch: `not is_synthetic_forward(action.priceable)` -> append error
         ii.  Branch: `frequency != trade_duration` -> append error
         iii. Branch: `action.risk != EqDelta` -> append error
       - Branch: `isinstance(action, (ExitPositionAction, ExitTradeAction))` -> `continue`
       - Branch: else -> append unsupported action error
13. Return `check_results`

### EquityVolEngine.supports_strategy(cls, strategy) -> bool
Purpose: Return whether the strategy passes validation.

**Algorithm:**
1. Call `cls.check_strategy(strategy)` -> `check_result`
2. Branch: `len(check_result) > 0` -> return `False`
3. Branch: else -> return `True`

### EquityVolEngine.run_backtest(cls, strategy, start, end, market_model=EquityMarketModel.SFK, cash_accrual=True) -> BacktestResult
Purpose: Validate, configure, and execute an equity vol backtest, returning a `BacktestResult`.

**Algorithm:**
1. Call `cls.check_strategy(strategy)` -> `check_result`
2. Branch: `len(check_result) > 0` -> raise `RuntimeError(check_result)`
3. Initialize local variables: `underlier_list`, `roll_frequency`, `trade_quantity`, `trade_quantity_type`, `trade_in_signals`, `trade_out_signals`, `hedge` (all `None`); `transaction_cost = TransactionCostConfig(None)`
4. For each `trigger` in `strategy.triggers`:
   a. Branch: `isinstance(trigger, AggregateTrigger)` ->
      - Extract `date_trigger` (first `DateTriggerRequirements`)
      - Build `date_signal` list of `BacktestSignalSeriesItem`
      - Extract `portfolio_trigger` (first `PortfolioTriggerRequirements`)
      - Branch: `direction == EQUAL and trigger_level == 0` -> `is_trade_in = True`
      - Branch: else -> `is_trade_in = False`
      - Branch: `is_trade_in` -> set `trade_in_signals = date_signal`
      - Branch: else -> set `trade_out_signals = date_signal`
   b. Get `action = trigger.actions[0]`
   c. Branch: `isinstance(action, EnterPositionQuantityScaledAction)` -> set underlier_list, roll_frequency, trade_quantity (from action), trade_quantity_type (from action), expiry_date_mode, transaction_cost.trade_cost_model
   d. Branch: `isinstance(action, AddTradeAction)` -> same but `trade_quantity=1`, `trade_quantity_type=BacktestTradingQuantityType.quantity`
   e. Branch: `isinstance(action, AddScaledTradeAction)` -> same but `trade_quantity=action.scaling_level`, `trade_quantity_type=get_backtest_trading_quantity_type(...)`
   f. Branch: `isinstance(action, HedgeAction)` -> create `DeltaHedgeParameters`, set `transaction_cost.hedge_cost_model`
5. Branch: `transaction_cost.trade_cost_model or transaction_cost.hedge_cost_model` is truthy -> use `transaction_cost`; else -> `None`
6. Construct `StrategySystematic` with all collected parameters
7. Call `strategy.backtest(start, end)` -> `result`
8. Return `BacktestResult(result)`

### EquityVolEngine.__get_underlier_list(cls, priceables) -> list
Purpose: Deep-copy priceables, parse expiration dates, and set `trade_as` attribute if applicable.

**Algorithm:**
1. `priceables_copy = copy.deepcopy(priceables)`
2. For each `priceable` in `priceables_copy`:
   a. Parse `priceable.expiration_date` via `TenorParser` -> replace with `edp.get_date()`
   b. Branch: `hasattr(priceable, 'trade_as')` ->
      - `expiry_date_mode = get_enum_value(TradeAs, edp.get_mode())`
      - Branch: `isinstance(expiry_date_mode, TradeAs)` -> `priceable.trade_as = priceable.trade_as or expiry_date_mode`
      - Branch: else -> `priceable.trade_as = None`
      - `print(priceable.trade_as)` (debug leftover)
3. Return `priceables_copy`

### EquityVolEngine.__map_tc_model(cls, model: TransactionModel) -> Union[FixedCostModel, ScaledCostModel, AggregateCostModel, None]
Purpose: Convert internal `TransactionModel` types to xasset API cost model types.

**Algorithm:**
1. Branch: `isinstance(model, ConstantTransactionModel)` -> return `FixedCostModel(cost=model.cost)`
2. Branch: `isinstance(model, ScaledTransactionModel)` ->
   a. Branch: `model.scaling_type == EqVega` -> `scaling_quantity_type = TransactionCostScalingType.Vega`
   b. Branch: else -> `scaling_quantity_type = get_enum_value(TransactionCostScalingType, model.scaling_type)`
      - Branch: `not isinstance(scaling_quantity_type, TransactionCostScalingType)` -> raise `RuntimeError`
   c. Return `ScaledCostModel(scaling_quantity_type, scaling_level=model.scaling_level)`
3. Branch: `isinstance(model, AggregateTransactionModel)` -> recursively map sub-models, return `AggregateCostModel`
4. Branch: else (including `None`) -> return `None`

### TenorParser.__init__(self, expiry: Union[str, dt.date]) -> None
Purpose: Store the expiry value for parsing.

**Algorithm:**
1. Set `self.expiry = expiry`

### TenorParser.get_date(self) -> Union[str, dt.date]
Purpose: Extract the date/tenor portion from the expiry, stripping any mode suffix.

**Algorithm:**
1. Branch: `isinstance(self.expiry, dt.date)` -> return `self.expiry`
2. `parts = re.search(self.expiry_regex, self.expiry)`
3. Branch: `parts` is truthy -> return `parts.group(1)`
4. Branch: else -> return `self.expiry` as-is

### TenorParser.get_mode(self) -> Optional[str]
Purpose: Extract the mode suffix (e.g., `"listed"`, `"otc"`) from the expiry string.

**Algorithm:**
1. Branch: `isinstance(self.expiry, dt.date)` -> return `None`
2. `parts = re.search(self.expiry_regex, self.expiry)`
3. Branch: `parts` is truthy -> return `parts.group(2)`
4. Branch: else -> return `None`

## State Mutation
- `BacktestResult._results`: Set once in `__init__`, never modified; all methods are read-only accessors
- `EquityVolEngine`: No instance state; all methods are classmethods operating on input parameters
- `TenorParser.expiry`: Set once in `__init__`, never modified
- `__get_underlier_list` deep-copies priceables, so original instruments are not mutated; `priceable.expiration_date` and `priceable.trade_as` are modified on the copies only
- Thread safety: All functions are stateless or operate on local/copied data; no shared mutable state

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_backtest_trading_quantity_type` | When no mapping exists for the `scaling_type`/`risk` combination |
| `RuntimeError` | `EquityVolEngine.run_backtest` | When `check_strategy` returns errors |
| `RuntimeError` | `EquityVolEngine.__map_tc_model` | When `get_enum_value` returns a value that is not a `TransactionCostScalingType` instance |
| `DeprecationWarning` | `EquityVolEngine.check_strategy` | When `ExitPositionAction` is used (deprecated in favor of `ExitTradeAction`) |
| `DeprecationWarning` | `EquityVolEngine.check_strategy` | When `EnterPositionQuantityScaledAction` is used (deprecated in favor of `AddScaledTradeAction`) |
| `IndexError` (potential) | `EquityVolEngine.check_strategy` | When `portfolio_triggers` list is empty but `portfolio_triggers[0]` is accessed on line 177 |

## Edge Cases
- `get_backtest_trading_quantity_type`: passing an unrecognized `scaling_type` + `risk` combination raises `ValueError`
- `is_synthetic_forward`: passing a non-`Portfolio` returns `False`; a `Portfolio` with != 2 elements returns `False`; a `Portfolio` of 2 non-`EqOption` instruments returns `False`; matching attributes but wrong call/put + buy/sell combination returns `False`
- `BacktestResult.get_measure_series`: when no risk entry matches the measure name, `next()` returns `()` and produces an empty DataFrame
- `BacktestResult.get_trade_history`: cost field is `None` when a transaction has more than one trade
- `BacktestResult.get_portfolio_history`: if `self._results.portfolio` is empty, returns empty DataFrame
- `EquityVolEngine.check_strategy` line 177: accessing `portfolio_triggers[0]` will raise `IndexError` if the earlier check at line 175 already appended an error but no `PortfolioTriggerRequirements` exists (execution continues past the check without short-circuiting)
- `EquityVolEngine.__get_underlier_list` line 418: contains `print(priceable.trade_as)` -- a debug statement left in production code
- `TenorParser.get_date` / `get_mode`: passing `dt.date` always returns the date / `None` respectively, never attempts regex
- `TenorParser` regex `(.*)@(.*)` is greedy, so `"3m@listed@extra"` would match group 1 as `"3m@listed"` and group 2 as `"extra"`

## Bugs Found
- Line 92-94: `is_syn_fwd &= (OptionType.Call, BuySell.Buy) in option_set and (OptionType.Put, BuySell.Sell) in option_set` -- due to Python's `in` operator binding tighter than `and`, and the `&=` combining both checks, this actually works correctly. Both membership checks are evaluated and combined with `and`. Earlier spec versions incorrectly flagged this as a bug where only the Put/Sell check was evaluated, but the `in` operator has higher precedence than `and`, so both sides are properly checked. (CORRECTED from earlier analysis)
- Line 177: After appending an error that no `PortfolioTriggerRequirements` found (line 176), execution falls through to line 177 which accesses `portfolio_triggers[0]`, causing an `IndexError` if the list is empty. Should be guarded with `else`/`continue`. (OPEN)
- Line 418: `print(priceable.trade_as)` is a debug statement left in production code. (OPEN)

## Coverage Notes
- Branch count: ~55
- `get_backtest_trading_quantity_type`: 6 branches (5 early returns + 1 raise)
- `is_synthetic_forward`: 5 branches (Portfolio check, size check, EqOption check, attribute match, option_set membership)
- `BacktestResult.get_measure_series`: 2 branches (empty vs non-empty DataFrame)
- `BacktestResult.get_portfolio_history`: 1 branch (loop body; empty portfolio produces empty data)
- `BacktestResult.get_trade_history`: 2 branches (nested loops + cost conditional on single-trade transactions)
- `EquityVolEngine.check_strategy`: ~22 branches (initial_portfolio, trigger count, trigger types, aggregate trigger sub-checks, action deprecation warnings, action type check, duplicate check, per-child-trigger action validation)
- `EquityVolEngine.supports_strategy`: 2 branches (errors present / absent)
- `EquityVolEngine.run_backtest`: ~8 branches (check_result, AggregateTrigger, trade_in vs trade_out, 4 action type branches, transaction_cost_config None check)
- `EquityVolEngine.__get_underlier_list`: 3 branches (loop, hasattr, isinstance TradeAs)
- `EquityVolEngine.__map_tc_model`: 5 branches (ConstantTxn, ScaledTxn+EqVega, ScaledTxn+valid, ScaledTxn+invalid raising RuntimeError, AggregateTxn, fallthrough None)
- `TenorParser.get_date`: 3 branches (dt.date, regex match, no match)
- `TenorParser.get_mode`: 3 branches (dt.date, regex match, no match)
- Mocking notes: `StrategySystematic` and its `backtest()` method must be mocked in `run_backtest` tests to avoid real API calls. `warnings.warn` calls in `check_strategy` should be tested with `pytest.warns(DeprecationWarning)`. `copy.deepcopy` in `__get_underlier_list` means priceables are not mutated; tests should verify original is unchanged. `get_enum_value` may need mocking. `BacktestResult` methods depend on the shape of `self._results` (`.risks`, `.portfolio` attributes). Private methods are name-mangled to `_EquityVolEngine__get_underlier_list` / `_EquityVolEngine__map_tc_model`.
- Pragmas: none
