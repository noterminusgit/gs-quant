# backtest_objects.py

## Summary
Core backtest state containers, transaction cost models, cash payment/accrual models, and PnL attribution logic. Defines the `BackTest` class (generic engine state), `PredefinedAssetBacktest` (FIFO-based asset backtest), several `TransactionModel` variants (constant, scaled, aggregate), `CashPayment`, `Hedge`, `ScalingPortfolio`, and cash accrual models (constant, data-driven, OIS fixing).

## Classes/Functions

### TransactionAggType (Enum)
Values: SUM, MAX, MIN. No branching.

### PnlAttribute (dataclass)
- `get_risks()`: returns list of [attribute_metric, market_data_metric]. No branching.

### PnlDefinition (dataclass)
- `get_risks()`: flattens all attribute risks. No branching beyond iteration.

### BackTest(BaseBacktest) (dataclass)

#### `__post_init__`
1. Initializes defaultdicts for portfolio, hedges, cash_payments, transaction_costs, transaction_cost_entries, results, trade_exit_risk_results
2. Deepcopies strategy
3. Wraps self.risks via make_list
4. Sets _risk_summary_dict = None, _calc_calls = 0, _calculations = 0

#### `add_results(date, results, replace=False)`
1. **B1**: date in self._results AND len(self._results[date]) AND not replace → append (+=)
2. **B2**: else → overwrite

#### `get_risk_summary_df(zero_on_empty_dates=False)`
1. **B1**: _risk_summary_dict is not None → use cached dict (skip recomputation)
2. **B2**: _risk_summary_dict is None:
   a. **B3**: not self._results → return empty DataFrame
   b. Filter dates with non-empty results
   c. For each (date, results), for each risk_measure:
      - **B4**: try aggregate → value
      - **B5**: except TypeError → ErrorValue
   d. Cache into _risk_summary_dict
3. **B6**: zero_on_empty_dates is True → fill cash-only dates with 0 for each risk
4. Build DataFrame from dict, sort, return

#### `result_summary` (property)
1. Calls get_risk_summary_df()
2. Builds cash_summary from _cash_dict (ccy → date → value)
3. **B1**: len(cash_summary) > 1 → raise RuntimeError (multiple currencies)
4. **B2**: len(cash_summary) == 1 → pd.concat cash series
5. **B3**: len(cash_summary) == 0 → empty DataFrame
6. Builds transaction_costs series, cumsum
7. Joins summary + cash + transaction_costs, ffill, fillna(0)
8. Computes Total column, slices to last state

#### `trade_ledger()`
1. For each date (sorted), for each CashPayment:
   a. **B1**: direction == 0 → create closed-at-zero entry
   b. **B2**: name in names AND len(cash_paid) > 0 → update existing entry (close date, close value, PnL, status=closed)
   c. **B3**: name in names AND len(cash_paid) == 0 → no update (falls through elif without entering body)
   d. **B4**: else (new name) → create new open entry
2. Return DataFrame

#### `strategy_as_time_series()`
1. Builds cp_table from cash_payments via CashPayment.to_frame()
2. Builds risk_measure_table from results
3. Joins risk_measure_table with cp_table (outer)
4. Builds static_inst_info, deduplicates on index
5. Joins static_inst_info with risk+cp table (outer), sort
No conditional branches; will raise if cash_payments or results are empty (concat on empty).

#### `pnl_explain()`
1. **B1**: pnl_explain_def is None → return None
2. Merge and sort dates from results and exit_risk_results
3. For each attribute, iterate date pairs (idx=1..len-1):
   a. **B2**: prev_date not in risk_results → store cum_total, continue
   b. For each instrument on prev_date:
      - **B3**: prev_date_risk == 0 → skip (continue)
      - **B4**: cur_date in risk_results AND instrument in cur_date portfolio → use risk_results
      - **B5**: else → use exit_risk_results
      - **B6**: attribute.second_order → 0.5 * scaling * risk * delta^2
      - **B7**: else → scaling * risk * delta (first order)
   c. Accumulate cum_total
4. Return dict of attribute_name → date→cumulative results

### ScalingPortfolio
Constructor stores trade, dates, risk, csa_term, risk_transformation, risk_percentage, results=None. No branching.

### TransactionModel (base)
- `get_unit_cost()`: returns None (pass). No branching.

### ConstantTransactionModel(TransactionModel)
- `get_unit_cost()`: returns self.cost. No branching.

### ScaledTransactionModel(TransactionModel)
#### `get_unit_cost(state, info, instrument)`
1. **B1**: isinstance(scaling_type, str) → try getattr(instrument, scaling_type)
   a. **B2**: AttributeError → raise RuntimeError
2. **B3**: state > dt.date.today() → return np.nan
3. **B4**: else → calc risk with PricingContext(state), return future

### AggregateTransactionModel(TransactionModel)
#### `get_unit_cost(state, info, instrument)`
1. **B1**: not self.transaction_models (empty) → return 0
2. **B2**: aggregate_type == SUM → sum of sub-model costs
3. **B3**: aggregate_type == MAX → max of sub-model costs
4. **B4**: aggregate_type == MIN → min of sub-model costs
5. **B5**: else → raise RuntimeError (**BUG**: references self.aggregation_type, should be self.aggregate_type)

### TransactionCostEntry
#### `all_instruments` (property)
1. **B1**: _instrument is Portfolio → return .all_instruments
2. **B2**: else → return (_instrument,)

#### `all_transaction_models` (property)
1. **B1**: _transaction_model is AggregateTransactionModel → return .transaction_models
2. **B2**: else → return (_transaction_model,)

#### `cost_aggregation_func` (property)
1. **B1**: _transaction_model is AggregateTransactionModel:
   a. **B2**: aggregate_type is SUM → return sum
   b. **B3**: aggregate_type is MAX → return max
   c. **B4**: aggregate_type is MIN → return min
2. **B5**: fallthrough (not aggregate, or unrecognized type) → return sum

#### `no_of_risk_calcs` (property)
Counts ScaledTransactionModel instances where scaling_type is RiskMeasure.
1. **B1**: isinstance(m, ScaledTransactionModel) AND isinstance(m.scaling_type, RiskMeasure)

#### `calculate_unit_cost()`
Iterates all_transaction_models x all_instruments, calls get_unit_cost. No branching.

#### `__resolved_cost(cost)` (static)
1. **B1**: isinstance PortfolioRiskResult → .aggregate()
2. **B2**: isinstance PricingFuture → .result()
3. **B3**: else → return cost as-is

#### `get_final_cost()`
1. For each model, sum resolved costs across instruments
2. **B1**: isinstance ScaledTransactionModel → apply scaling_level * abs(cost * additional_scaling)
3. **B2**: final_costs non-empty → apply cost_aggregation_func
4. **B3**: final_costs empty → return 0

#### `get_cost_by_component()`
1. For each model, sum resolved costs, split into fixed_costs or scaled_costs lists
   a. **B1**: isinstance ScaledTransactionModel → scaled_costs
   b. **B2**: else → fixed_costs
2. Apply cost_aggregation_func if list non-empty, else None
3. **B3**: scaled_cost is None → return (fixed_cost, 0)
4. **B4**: fixed_cost is None → return (0, scaled_cost)
5. **B5**: cost_aggregation_func is sum → return (fixed_cost, scaled_cost)
6. **B6**: else (min/max):
   a. **B7**: aggregated == fixed_cost → return (fixed_cost, 0)
   b. **B8**: aggregated == scaled_cost → return (0, scaled_cost)
   c. **B9**: else → raise ValueError

### CashPayment
- `to_frame()`: builds DataFrame from cash_paid dict. No branching.

### Hedge
Constructor only. No branching.

### PredefinedAssetBacktest(BaseBacktest) (dataclass)

#### `__post_init__`
Initializes performance (Series), cash_asset (Cash('USD')), holdings (defaultdict), historical_holdings, historical_weights, orders list, results dict.

#### `set_start_date(start)`
Sets initial performance value and cash holdings. No branching.

#### `record_orders(orders)`
Extends self.orders. No branching.

#### `update_fill(fill)`
Adjusts cash and instrument holdings. No branching.

#### `trade_ledger()`
1. Build instrument_queues: for each order:
   a. **B1**: instrument not in queues → create (longs, shorts) FIFO pair
   b. **B2**: quantity < 0 → put in shorts
   c. **B3**: else → put in longs
2. Match pairs: while both non-empty, compare execution_end_time:
   a. **B4**: long.execution_end_time < short → open=long, close=short
   b. **B5**: else → open=short, close=long
3. Handle unmatched: while either non-empty:
   a. **B6**: longs not empty → get from longs
   b. **B7**: else → get from shorts
4. Build trade_df: for each pair:
   a. **B8**: close_order exists → status=closed, PnL calculated
   b. **B9**: close_order is None → status=open, end_dt/end_value=None, PnL=None

#### `mark_to_market(state, valuation_method)`
1. For each (instrument, units) in holdings:
   a. **B1**: abs(units) > epsilon:
      - **B2**: isinstance(instrument, Cash) → fixing = 1
      - **B3**: else:
        - **B4**: window is truthy → get_data_range, mean if non-empty, else nan
        - **B5**: window is falsy → get_data (daily fixing)
      - Compute notional, accumulate mtm
   b. (implicit: abs(units) <= epsilon → skip)
2. Set performance[date] = mtm
3. Normalize weights by mtm (potential ZeroDivisionError if mtm=0)

#### `get_level(date)`
Returns performance[date]. No branching (KeyError if missing).

#### `get_costs()`
1. For each order:
   a. **B1**: isinstance OrderCost → accumulate cost by date
2. Return Series

#### `get_orders_for_date(date)`
Filters orders by execution_end_time date. No branching beyond filter.

### CashAccrualModel (base dataclass)
- `get_accrued_value()`: returns None (pass). No branching.

### ConstantCashAccrualModel
#### `get_accrued_value(current_value, to_state)`
1. Computes days = (to_state - from_state).days
2. For each currency:
   - **B1**: self.annual → divide rate by 365
   - **B2**: else → divide by 1
3. Compound: value * (1 + adjusted_rate) ** days

### DataCashAccrualModel
#### `get_accrued_value(current_value, to_state)`
Same structure as ConstantCashAccrualModel but fetches rate from data_source.get_data(from_state).
1. **B1**: annual → /365, else /1

### OisFixingCashAccrualModel
#### `get_accrued_value(current_value, to_state)`
1. For each currency in current_value:
   a. **B1**: currency not in global ois_fixings → load OIS fixings:
      - **B2**: start_date is dt.date → use as-is
      - **B3**: start_date is str → RelativeDate.apply_rule()
      - **B4**: end_date is dt.date → use as-is
      - **B5**: end_date is str → RelativeDate.apply_rule()
      - Construct IRSwap, calc Cashflows, filter 'Flt', build GenericDataSource, cache
   b. Delegate to DataCashAccrualModel.get_accrued_value()
   c. **NOTE**: returns on first currency iteration (only processes first currency)

## Edge Cases
- `BackTest.add_results`: date exists but results list is empty (len==0) → takes else branch (overwrites)
- `BackTest.result_summary`: >1 currency in cash_dict → RuntimeError
- `BackTest.trade_ledger`: direction==0 creates a closed-at-zero entry; name already in names but cash_paid empty → elif body not entered (no update)
- `BackTest.pnl_explain`: pnl_explain_def is None → returns None; only one date in dates → loop body never executes (range(1,1) is empty)
- `BackTest.strategy_as_time_series`: empty cash_payments or empty results → will raise on pd.concat with empty input
- `ScaledTransactionModel.get_unit_cost`: state > today() → np.nan; scaling_type string not found on instrument → RuntimeError
- `AggregateTransactionModel.get_unit_cost`: empty transaction_models → returns 0; unrecognized aggregate_type → RuntimeError (but crashes with AttributeError due to bug)
- `TransactionCostEntry.get_cost_by_component`: min/max aggregation where both fixed and scaled costs exist and func([f,s]) equals neither → ValueError
- `PredefinedAssetBacktest.mark_to_market`: all holdings below epsilon → mtm=0, then division by zero in weight normalization
- `PredefinedAssetBacktest.mark_to_market`: window with no fixings (empty range) → np.nan fixing → nan notional
- `OisFixingCashAccrualModel.get_accrued_value`: returns inside the for loop on first currency, so multi-currency dicts only process the first currency

## Bugs Found
- **Line 439**: `AggregateTransactionModel.get_unit_cost` else branch references `self.aggregation_type` but the field is named `self.aggregate_type`. This will raise `AttributeError` instead of the intended `RuntimeError` if an unrecognized `TransactionAggType` value is somehow passed.
- **Line 798** (OisFixingCashAccrualModel.get_accrued_value): The `return` statement is inside the `for currency` loop body, so only the first currency is ever processed. If `current_value[0]` has multiple currencies, subsequent ones are silently ignored.
- **Line 698** (PredefinedAssetBacktest.mark_to_market): If all instrument holdings are below epsilon (or holdings is empty), `mtm` remains 0, causing `ZeroDivisionError` in the weight normalization loop `notional / mtm`.

## Coverage Notes
- Approximately 85-90 distinct branch points across all classes.
- `BackTest`: ~25 branches (add_results: 2, get_risk_summary_df: 6, result_summary: 3, trade_ledger: 4, pnl_explain: 7, properties: trivial).
- `TransactionModel` hierarchy: ~12 branches (ScaledTransactionModel: 4, AggregateTransactionModel: 5, ConstantTransactionModel: 0, base: 0).
- `TransactionCostEntry`: ~20 branches across 7 methods (all_instruments: 2, all_transaction_models: 2, cost_aggregation_func: 5, __resolved_cost: 3, get_final_cost: 3, get_cost_by_component: 9).
- `PredefinedAssetBacktest`: ~15 branches (trade_ledger: 9, mark_to_market: 5, get_costs: 1).
- Cash accrual models: ~8 branches (ConstantCashAccrualModel: 1, DataCashAccrualModel: 1, OisFixingCashAccrualModel: 5).
- Mocking notes:
  - `BackTest` methods need mocked `PortfolioRiskResult` objects with `.aggregate()`, `.risk_measures`, `.__getitem__()`, `.portfolio`.
  - `ScaledTransactionModel.get_unit_cost` needs mocked instruments with dynamic attributes and `PricingContext`.
  - `TransactionCostEntry` needs mocked `PricingFuture.result()` and `PortfolioRiskResult.aggregate()`.
  - `PredefinedAssetBacktest.mark_to_market` needs mocked `DataHandler` with `get_data` and `get_data_range`.
  - `OisFixingCashAccrualModel` needs mocked `PricingContext`, `IRSwap.calc`, `Cashflows` result, and the global `ois_fixings` dict should be reset between tests.
  - `BackTest.get_risk_summary_df` caches `_risk_summary_dict`; tests should verify both cached and uncached paths.
