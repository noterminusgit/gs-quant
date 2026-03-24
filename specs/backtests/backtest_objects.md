# backtest_objects.py

## Summary
Core backtest state containers, transaction cost models, cash payment/accrual models, and PnL attribution logic. Defines `BaseBacktest` (ABC), `BackTest` (generic engine state container with portfolio, results, cash tracking), `PredefinedAssetBacktest` (FIFO-based asset backtest with mark-to-market), several `TransactionModel` variants (`ConstantTransactionModel`, `ScaledTransactionModel`, `AggregateTransactionModel`), `TransactionCostEntry` (stateful wrapper linking costs to cash payments), `CashPayment`, `Hedge`, `ScalingPortfolio`, `WeightedScalingPortfolio`, `WeightedTrade`, cash accrual models (`CashAccrualModel`, `ConstantCashAccrualModel`, `DataCashAccrualModel`, `OisFixingCashAccrualModel`), and PnL attribution classes (`PnlAttribute`, `PnlDefinition`).

## Dependencies
- Internal: `gs_quant.backtests.backtest_utils` (`make_list`), `gs_quant.backtests.core` (`ValuationMethod`), `gs_quant.backtests.data_handler` (`DataHandler`), `gs_quant.backtests.data_sources` (`DataSource`, `GenericDataSource`, `MissingDataStrategy`), `gs_quant.backtests.event` (`FillEvent`), `gs_quant.backtests.order` (`OrderBase`, `OrderCost`), `gs_quant.base` (`field_metadata`, `static_field`), `gs_quant.common` (`RiskMeasure`), `gs_quant.datetime.relative_date` (`RelativeDate`), `gs_quant.instrument` (`Cash`, `IRSwap`, `Instrument`), `gs_quant.json_convertors` (`dc_decode`), `gs_quant.markets` (`PricingContext`), `gs_quant.markets.portfolio` (`Portfolio`), `gs_quant.risk` (`ErrorValue`, `Cashflows`), `gs_quant.risk.transform` (`Transformer`), `gs_quant.risk.results` (`PricingFuture`, `PortfolioRiskResult`)
- External: `abc` (`ABC`), `collections` (`defaultdict`), `copy` (`deepcopy`), `dataclasses` (`dataclass`, `field`), `dataclasses_json` (`dataclass_json`, `config`), `enum` (`Enum`), `queue` (`Queue` as `FifoQueue`), `typing` (`Iterable`, `TypeVar`, `Optional`, `Union`, `Callable`, `Tuple`, `ClassVar`), `datetime` (`dt.date`, `dt.datetime`, `dt.timedelta`), `numpy` (`np.nan`, `np.mean`, `np.sign`), `pandas` (`pd.DataFrame`, `pd.Series`, `pd.concat`, `pd.MultiIndex`)

## Type Definitions

### BaseBacktest (ABC)
Inherits: `ABC`

No fields. Abstract base class for all backtests.

### TBaseBacktest (TypeVar)
```
TBaseBacktest = TypeVar('TBaseBacktest', bound='BaseBacktest')
```

### PnlAttribute (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| attribute_name | `str` | *(required)* | Name of the PnL attribution component |
| attribute_metric | `RiskMeasure` | *(required)* | Risk measure for the attribution |
| market_data_metric | `RiskMeasure` | *(required)* | Market data risk measure for delta computation |
| scaling_factor | `float` | *(required)* | Multiplicative scaling factor |
| second_order | `bool` | `False` | Whether to use second-order (gamma-like) attribution |

### PnlDefinition (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| attributes | `Iterable[PnlAttribute]` | *(required)* | List of PnL attribution components |

### BackTest (dataclass_json, dataclass)
Inherits: `BaseBacktest`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| CUMULATIVE_CASH_COLUMN | `ClassVar[str]` | `"Cumulative Cash"` | Column name constant |
| TRANSACTION_COSTS_COLUMN | `ClassVar[str]` | `"Transaction Costs"` | Column name constant |
| TOTAL_COLUMN | `ClassVar[str]` | `"Total"` | Column name constant |
| strategy | `object` | *(required)* | Strategy definition (deep-copied in `__post_init__`) |
| states | `Iterable` | *(required)* | Pricing dates for the backtest |
| risks | `Iterable[RiskMeasure]` | *(required)* | Risk measures to calculate |
| price_measure | `RiskMeasure` | *(required)* | Price risk measure for PV computation |
| holiday_calendar | `Iterable[dt.date]` | `None` | Holiday calendar for date math |
| pnl_explain_def | `Optional[PnlDefinition]` | `None` | PnL attribution definition |
| _portfolio_dict | `defaultdict(Portfolio)` | *(set in `__post_init__`)* | Portfolio by state date |
| _cash_dict | `dict` | *(set in `__post_init__`)* | Cash positions by state date and currency |
| _hedges | `defaultdict(list)` | *(set in `__post_init__`)* | Hedge objects by date |
| _weighted_trades | `defaultdict(list)` | *(set in `__post_init__`)* | WeightedTrade objects by date |
| _cash_payments | `defaultdict(list)` | *(set in `__post_init__`)* | CashPayment objects by date |
| _transaction_costs | `defaultdict(int)` | *(set in `__post_init__`)* | Transaction costs by date |
| _transaction_cost_entries | `defaultdict(list)` | *(set in `__post_init__`)* | TransactionCostEntry objects by state |
| _results | `defaultdict(list)` | *(set in `__post_init__`)* | PortfolioRiskResult by date |
| _trade_exit_risk_results | `defaultdict(list)` | *(set in `__post_init__`)* | Risk results at trade exit dates |
| _risk_summary_dict | `Optional[dict]` | `None` | Cached summary dict (computed once, reused) |
| _calc_calls | `int` | `0` | Counter for pricing API calls |
| _calculations | `int` | `0` | Counter for individual calculations |

### ScalingPortfolio (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trade | `Instrument or Portfolio` | *(required)* | The hedge instrument(s) |
| dates | `list` | *(required)* | Active dates for the hedge |
| risk | `RiskMeasure` | *(required)* | Risk measure for scaling |
| csa_term | `Optional[str]` | `None` | CSA term for pricing |
| risk_transformation | `Optional[Transformer]` | `None` | Optional risk transformation |
| risk_percentage | `float` | `100` | Percentage of risk to hedge |
| results | `Optional` | `None` | Calculated risk results (set later) |

### TransactionModel (dataclass_json, dataclass, frozen=True)
Inherits: `object`

No fields. Base class with `get_unit_cost()` returning `None`.

### ConstantTransactionModel (dataclass_json, dataclass, frozen=True)
Inherits: `TransactionModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| cost | `Union[float, int]` | `0` | Constant transaction cost |
| class_type | `str` | `'constant_transaction_model'` (static_field) | Serialization discriminator |

### ScaledTransactionModel (dataclass_json, dataclass, frozen=True)
Inherits: `TransactionModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| scaling_type | `Union[str, RiskMeasure]` | `'notional_amount'` | How to obtain the base cost: string attribute name on instrument, or RiskMeasure to calc |
| scaling_level | `Union[float, int]` | `0.0001` | Multiplier applied to the base cost |
| class_type | `str` | `'scaled_transaction_model'` (static_field) | Serialization discriminator |

### AggregateTransactionModel (dataclass_json, dataclass, frozen=True)
Inherits: `TransactionModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| transaction_models | `tuple` | `tuple()` | Sub-models to aggregate |
| aggregate_type | `TransactionAggType` | `TransactionAggType.SUM` | Aggregation method |

### TransactionCostEntry (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _date | `dt.date` | *(required)* | Date of the transaction |
| _instrument | `Instrument` | *(required)* | The instrument being transacted |
| _transaction_model | `TransactionModel` | *(required)* | Transaction cost model |
| _unit_cost_by_model_by_inst | `dict` | `{}` | Cached unit costs: `{model: {instrument: cost}}` |
| _additional_scaling | `float` | `1` | Additional scaling factor (set by hedge scaling) |

### CashPayment (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trade | `Instrument or Portfolio` | *(required)* | The traded instrument |
| effective_date | `Optional[dt.date]` | `None` | Date the cash payment is effective |
| direction | `int` | `1` | -1 for entry (buy), 1 for exit (sell), 0 for zero-cost close |
| cash_paid | `defaultdict(float)` | `{}` | Currency to amount mapping, populated during cash handling |
| transaction_cost_entry | `Optional[TransactionCostEntry]` | `None` | Associated TC entry |

### Hedge (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| scaling_portfolio | `ScalingPortfolio` | *(required)* | The hedge scaling portfolio |
| entry_payment | `CashPayment` | *(required)* | Entry cash payment |
| exit_payment | `Optional[CashPayment]` | *(required)* | Exit cash payment (None if future) |

### WeightedScalingPortfolio (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trades | `Portfolio` | *(required)* | Portfolio of instruments to weight |
| dates | `list` | *(required)* | Active dates |
| risk | `RiskMeasure` | *(required)* | Risk measure for weighting |
| total_size | `float` | *(required)* | Total notional to distribute by risk |
| csa_term | `Optional` | `None` | CSA term for pricing |
| results | `Optional` | `None` | Stored risk calculation results |

### WeightedTrade (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| scaling_portfolio | `WeightedScalingPortfolio` | *(required)* | The weighted scaling portfolio |
| entry_payments | `list` | *(required)* | List of CashPayment for each instrument |
| exit_payments | `list` | *(required)* | List of CashPayment for each instrument (or None) |

### PredefinedAssetBacktest (dataclass_json, dataclass)
Inherits: `BaseBacktest`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_handler | `DataHandler` | *(required)* | Handler for market data |
| initial_value | `float` | *(required)* | Starting portfolio value |
| performance | `pd.Series` | *(set in `__post_init__`)* | Time series of portfolio values |
| cash_asset | `Cash` | *(set in `__post_init__`)* | `Cash('USD')` |
| holdings | `defaultdict(float)` | *(set in `__post_init__`)* | Instrument to units mapping |
| historical_holdings | `pd.Series` | *(set in `__post_init__`)* | Historical holdings by date |
| historical_weights | `pd.Series` | *(set in `__post_init__`)* | Historical weights by date |
| orders | `list` | *(set in `__post_init__`)* | List of executed orders |
| results | `dict` | *(set in `__post_init__`)* | Pricing results |

### CashAccrualModel (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| class_type | `str` | `'cash_accrual_model'` (static_field) | Serialization discriminator |

### ConstantCashAccrualModel (dataclass_json, dataclass)
Inherits: `CashAccrualModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| rate | `float` | `0` | Interest rate |
| annual | `bool` | `True` | Whether rate is annual (divided by 365) or daily |
| class_type | `str` | `'cash_accrual_model'` (static_field) | Serialization discriminator |

### DataCashAccrualModel (dataclass_json, dataclass)
Inherits: `CashAccrualModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_source | `DataSource` | `None` | Data source for rate lookups |
| annual | `bool` | `True` | Whether rate is annual or daily |
| class_type | `str` | `'cash_accrual_model'` (static_field) | Serialization discriminator |

### OisFixingCashAccrualModel (dataclass_json, dataclass)
Inherits: `CashAccrualModel`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| start_date | `Union[dt.date, str]` | `'-1y'` | Start date for OIS fixing data (date or relative string) |
| end_date | `Union[dt.date, str]` | `dt.date.today()` | End date for OIS fixing data |
| class_type | `str` | `'ois_fixing_cash_accrual_model'` (static_field) | Serialization discriminator |

## Enums and Constants

### TransactionAggType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SUM | `'sum'` | Sum of all sub-model costs |
| MAX | `'max'` | Maximum of all sub-model costs |
| MIN | `'min'` | Minimum of all sub-model costs |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| ois_fixings | `dict` | `{}` | Global cache of OIS fixing data sources, keyed by currency |

## Functions/Methods

### PnlAttribute.get_risks(self) -> list
Purpose: Return the two risk measures needed for this attribution.

**Algorithm:**
1. Return `[self.attribute_metric, self.market_data_metric]`

### PnlDefinition.get_risks(self) -> list
Purpose: Flatten all attribute risks into a single list.

**Algorithm:**
1. Return `[risk for attribute in self.attributes for risk in attribute.get_risks()]`

### BackTest.__post_init__(self) -> None
Purpose: Initialize all mutable state containers.

**Algorithm:**
1. Initialize `_portfolio_dict = defaultdict(Portfolio)`
2. Initialize `_cash_dict = {}`
3. Initialize `_hedges = defaultdict(list)`
4. Initialize `_weighted_trades = defaultdict(list)`
5. Initialize `_cash_payments = defaultdict(list)`
6. Initialize `_transaction_costs = defaultdict(int)`
7. Initialize `_transaction_cost_entries = defaultdict(list)`
8. Deep-copy `self.strategy`
9. Initialize `_results = defaultdict(list)`
10. Initialize `_trade_exit_risk_results = defaultdict(list)`
11. Set `self.risks = make_list(self.risks)`
12. Set `_risk_summary_dict = None`, `_calc_calls = 0`, `_calculations = 0`

### BackTest.add_results(self, date, results, replace=False) -> None
Purpose: Add or replace risk results for a date.

**Algorithm:**
1. Branch: `date in self._results AND len(self._results[date]) AND not replace` -> append (`+=`)
2. Branch: else -> overwrite (`=`)

### BackTest.get_risk_summary_df(self, zero_on_empty_dates=False) -> pd.DataFrame
Purpose: Build a summary DataFrame of aggregated risk results per date.

**Algorithm:**
1. Branch: `self._risk_summary_dict is not None` -> use cached `summary_dict`
2. Branch: `self._risk_summary_dict is None`:
   a. Branch: `not self._results` -> return empty `pd.DataFrame(columns=self.risks)`
   b. Filter dates with non-empty results
   c. For each `(date, results)`, for each `risk_measure`:
      - Try: `value = results[risk].aggregate(True, True)`
      - Branch: `TypeError` raised -> `value = ErrorValue(None, error='Could not aggregate risk results')`
   d. Cache into `self._risk_summary_dict`
3. Copy `summary_dict`
4. Branch: `zero_on_empty_dates is True` -> for each cash-only date (in `_cash_dict` but not in summary), set all risks to 0
5. Build DataFrame, sort index, return

### BackTest.result_summary (property) -> pd.DataFrame
Purpose: Get summary DataFrame with PV, cash, and transaction costs.

**Algorithm:**
1. Call `get_risk_summary_df()`
2. Build `cash_summary` from `_cash_dict` (ccy -> date -> value)
3. Branch: `len(cash_summary) > 1` -> raise `RuntimeError('Cannot aggregate cash in multiple currencies')`
4. Branch: `len(cash_summary) == 1` -> `pd.concat` cash series
5. Branch: `len(cash_summary) == 0` -> empty DataFrame
6. Build `transaction_costs` series, `cumsum`
7. Join summary + cash + transaction_costs, `ffill`, `fillna(0)`
8. Compute `Total` column = PV + Cash + TC
9. Slice to `self.states[-1]`

### BackTest.risk_summary (property) -> pd.DataFrame
Purpose: Get risk summary with zero-filled empty dates.

**Algorithm:**
1. Return `self.get_risk_summary_df(zero_on_empty_dates=True)`

### BackTest.trade_ledger(self) -> pd.DataFrame
Purpose: Build a ledger of trade entries and exits with PnL.

**Algorithm:**
1. For each date (sorted), for each `CashPayment`:
   a. Branch: `cash.direction == 0` -> create closed-at-zero entry (Open=Close=date, PnL=0)
   b. Branch: `cash.trade.name in names AND len(cash.cash_paid) > 0` -> update existing entry (Close date, Close Value, PnL, status=closed)
   c. Branch: `cash.trade.name in names AND len(cash.cash_paid) == 0` -> no update (elif condition met but body not entered due to `len(cash.cash_paid) > 0` failing)
   d. Branch: else (new name) -> create new open entry
2. Return `pd.DataFrame(ledger).T.sort_index()`

### BackTest.strategy_as_time_series(self) -> pd.DataFrame
Purpose: Build a DataFrame indexed by (date, instrument) showing risks, cash payments, and static data.

**Algorithm:**
1. Build `cp_table` from all cash payments via `CashPayment.to_frame()`
2. Build `risk_measure_table` from results via `.to_frame()`
3. Join risk_measure_table with cp_table (outer)
4. Build `static_inst_info` from result portfolios, deduplicate on index
5. Join static_inst_info with risk+cp table (outer), sort
6. Return result

### BackTest.pnl_explain(self) -> Optional[dict]
Purpose: Compute PnL attribution by risk factor.

**Algorithm:**
1. Branch: `self.pnl_explain_def is None` -> return `None`
2. Merge and sort dates from `results` and `trade_exit_risk_results`
3. For each attribute, iterate date pairs (`idx=1..len-1`):
   a. Branch: `prev_date not in risk_results` -> store `cum_total`, continue
   b. For each instrument on `prev_date`:
      - Branch: `prev_date_risk == 0` -> skip (continue)
      - Branch: `cur_date in risk_results AND instrument in cur_date portfolio` -> use risk_results
      - Branch: else -> use `exit_risk_results`
      - Branch: `attribute.second_order` -> `0.5 * scaling * risk * delta^2`
      - Branch: else -> `scaling * risk * delta` (first order)
   c. Accumulate `cum_total`
4. Return dict of `attribute_name -> {date -> cumulative_value}`

### BackTest properties (read-only and read-write)
Properties with getters and setters for: `cash_dict` (read-only), `portfolio_dict` (r/w), `cash_payments` (r/w), `transaction_costs` (r/w), `transaction_cost_entries` (read-only), `hedges` (r/w), `weighted_trades` (r/w), `results` (read-only), `trade_exit_risk_results` (read-only), `calc_calls` (r/w), `calculations` (r/w). Also `set_results(date, results)` method for direct assignment.

### TransactionModel.get_unit_cost(self, state, info, instrument) -> float
Purpose: Base implementation returning `None` (pass).

### ConstantTransactionModel.get_unit_cost(self, state, info, instrument) -> float
Purpose: Return constant cost.

**Algorithm:**
1. Return `self.cost`

### ScaledTransactionModel.get_unit_cost(self, state, info, instrument) -> Union[float, PricingFuture]
Purpose: Return instrument-specific cost, either from an attribute or risk calculation.

**Algorithm:**
1. Branch: `isinstance(self.scaling_type, str)`:
   a. Try: return `getattr(instrument, self.scaling_type)`
   b. Branch: `AttributeError` -> raise `RuntimeError(f'{self.scaling_type} not recognised for instrument {instrument.type}')`
2. Branch: `state > dt.date.today()` -> return `np.nan`
3. Branch: else -> open `PricingContext(state)`, calc risk, return future

### AggregateTransactionModel.get_unit_cost(self, state, info, instrument) -> float
Purpose: Aggregate costs from multiple sub-models.

**Algorithm:**
1. Branch: `not self.transaction_models` (empty) -> return `0`
2. Branch: `self.aggregate_type == TransactionAggType.SUM` -> return sum of sub-model costs
3. Branch: `self.aggregate_type == TransactionAggType.MAX` -> return max of sub-model costs
4. Branch: `self.aggregate_type == TransactionAggType.MIN` -> return min of sub-model costs
5. Branch: else -> raise `RuntimeError(f'unrecognised aggregation type:{str(self.aggregate_type)}')`

**Raises:** `RuntimeError` -- but see Bugs section (references wrong attribute name in error message body)

### TransactionCostEntry.all_instruments (property) -> Tuple[Instrument, ...]
Purpose: Flatten instrument to tuple (unwrap Portfolio if needed).

**Algorithm:**
1. Branch: `isinstance(self._instrument, Portfolio)` -> return `self._instrument.all_instruments`
2. Branch: else -> return `(self._instrument,)`

### TransactionCostEntry.all_transaction_models (property) -> tuple
Purpose: Flatten transaction model (unwrap Aggregate if needed).

**Algorithm:**
1. Branch: `isinstance(self._transaction_model, AggregateTransactionModel)` -> return `self._transaction_model.transaction_models`
2. Branch: else -> return `(self._transaction_model,)`

### TransactionCostEntry.cost_aggregation_func (property) -> Callable
Purpose: Return the appropriate aggregation function (sum/max/min).

**Algorithm:**
1. Branch: `isinstance(self._transaction_model, AggregateTransactionModel)`:
   a. Branch: `aggregate_type is TransactionAggType.SUM` -> return `sum`
   b. Branch: `aggregate_type is TransactionAggType.MAX` -> return `max`
   c. Branch: `aggregate_type is TransactionAggType.MIN` -> return `min`
2. Branch: fallthrough (not aggregate, or unrecognized type) -> return `sum`

### TransactionCostEntry.no_of_risk_calcs (property) -> int
Purpose: Count ScaledTransactionModel instances with RiskMeasure scaling_type.

**Algorithm:**
1. Filter `all_transaction_models` for instances where both `isinstance(m, ScaledTransactionModel)` and `isinstance(m.scaling_type, RiskMeasure)` are true
2. Return count

### TransactionCostEntry.calculate_unit_cost(self) -> None
Purpose: Pre-compute unit costs for all model/instrument combinations.

**Algorithm:**
1. For each model in `all_transaction_models`:
   a. For each instrument in `all_instruments`:
      - Call `m.get_unit_cost(self._date, None, instrument)`, store in `_unit_cost_by_model_by_inst[m][i]`

### TransactionCostEntry.__resolved_cost(cost) -> float (staticmethod)
Purpose: Resolve async cost values to concrete floats.

**Algorithm:**
1. Branch: `isinstance(cost, PortfolioRiskResult)` -> return `cost.aggregate()`
2. Branch: `isinstance(cost, PricingFuture)` -> return `cost.result()`
3. Branch: else -> return `cost` as-is

### TransactionCostEntry.get_final_cost(self) -> float
Purpose: Compute final aggregated cost across all models and instruments.

**Algorithm:**
1. For each model, sum resolved costs across instruments
2. Branch: `isinstance(m, ScaledTransactionModel)` -> apply `scaling_level * abs(cost * additional_scaling)`
3. Append to `final_costs`
4. Branch: `final_costs` non-empty -> return `cost_aggregation_func(final_costs)`
5. Branch: `final_costs` empty -> return `0`

### TransactionCostEntry.get_cost_by_component(self) -> Tuple[float, float]
Purpose: Split costs into fixed and scaled components.

**Algorithm:**
1. For each model, sum resolved costs across instruments:
   a. Branch: `isinstance(m, ScaledTransactionModel)` -> `abs(cost * scaling_level * additional_scaling)`, append to `scaled_costs`
   b. Branch: else -> append to `fixed_costs`
2. Apply `cost_aggregation_func` if list non-empty, else `None`
3. Branch: `scaled_cost is None` -> return `(fixed_cost, 0)`
4. Branch: `fixed_cost is None` -> return `(0, scaled_cost)`
5. Branch: `cost_aggregation_func is sum` -> return `(fixed_cost, scaled_cost)`
6. Branch: else (min/max):
   a. Branch: `cost_aggregation_func([fixed_cost, scaled_cost]) == fixed_cost` -> return `(fixed_cost, 0)`
   b. Branch: `cost_aggregation_func([fixed_cost, scaled_cost]) == scaled_cost` -> return `(0, scaled_cost)`
   c. Branch: else -> raise `ValueError(f"Unable to split cost for aggregation function {self.cost_aggregation_func}")`

### CashPayment.to_frame(self) -> pd.DataFrame
Purpose: Convert cash payment to a DataFrame row.

**Algorithm:**
1. Build DataFrame from `cash_paid.items()` with columns `['Cash Ccy', 'Cash Amount']`
2. Add `Instrument Name` and `Pricing Date` columns
3. Return DataFrame

### PredefinedAssetBacktest.__post_init__(self) -> None
Purpose: Initialize all mutable state for asset backtest.

**Algorithm:**
1. Set `performance = pd.Series(dtype=float)`
2. Set `cash_asset = Cash('USD')`
3. Set `holdings = defaultdict(float)`
4. Set `historical_holdings = pd.Series(dtype=float)`
5. Set `historical_weights = pd.Series(dtype=float)`
6. Set `orders = []`
7. Set `results = {}`

### PredefinedAssetBacktest.set_start_date(self, start: dt.date) -> None
Purpose: Set initial portfolio value and cash holdings.

**Algorithm:**
1. Set `self.performance[start] = self.initial_value`
2. Set `self.holdings[self.cash_asset] = self.initial_value`

### PredefinedAssetBacktest.record_orders(self, orders: Iterable[OrderBase]) -> None
Purpose: Extend order list.

**Algorithm:**
1. Call `self.orders.extend(orders)`

### PredefinedAssetBacktest.update_fill(self, fill: FillEvent) -> None
Purpose: Update holdings based on a fill event.

**Algorithm:**
1. Subtract `fill.filled_price * fill.filled_units` from cash holdings
2. Add `fill.filled_units` to instrument holdings

### PredefinedAssetBacktest.trade_ledger(self) -> pd.DataFrame
Purpose: Match long/short orders into trade pairs using FIFO queues.

**Algorithm:**
1. For each order:
   a. Branch: `instrument not in instrument_queues` -> create `(FifoQueue(), FifoQueue())` pair
   b. Branch: `quantity < 0` -> put in shorts queue
   c. Branch: else -> put in longs queue
2. Match pairs: while both queues non-empty:
   a. Get one long and one short
   b. Branch: `long.execution_end_time() < short.execution_end_time()` -> `open=long, close=short`
   c. Branch: else -> `open=short, close=long`
3. Handle unmatched: while either queue non-empty:
   a. Branch: `longs not empty` -> get from longs
   b. Branch: else -> get from shorts
   c. Pair with `None` (open position)
4. Build `trade_df`:
   a. Branch: `close_order is not None` -> status=closed, compute PnL
   b. Branch: `close_order is None` -> status=open, end_dt/end_value/PnL=None

### PredefinedAssetBacktest.mark_to_market(self, state: dt.datetime, valuation_method: ValuationMethod) -> None
Purpose: Calculate mark-to-market value and weights.

**Algorithm:**
1. Set `epsilon = 1e-12`, `date = state.date()`, `mtm = 0`
2. For each `(instrument, units)` in holdings:
   a. Branch: `abs(units) > epsilon`:
      - Record in `historical_holdings`
      - Branch: `isinstance(instrument, Cash)` -> `fixing = 1`
      - Branch: else:
        - Branch: `window` is truthy -> `get_data_range(start, end, instrument, tag)`, then `np.mean(fixings) if len(fixings) else np.nan`
        - Branch: `window` is falsy -> `get_data(state.date(), instrument, tag)`
      - Compute `notional = fixing * units`
      - Record in `historical_weights`, accumulate `mtm`
3. Set `performance[date] = mtm`
4. Normalize weights: `notional / mtm` for each instrument

### PredefinedAssetBacktest.get_level(self, date: dt.date) -> float
Purpose: Return performance value at date.

**Algorithm:**
1. Return `self.performance[date]`

### PredefinedAssetBacktest.get_costs(self) -> pd.Series
Purpose: Aggregate order execution costs by date.

**Algorithm:**
1. For each order:
   a. Branch: `isinstance(order, OrderCost)` -> accumulate `execution_quantity` by date
2. Return `pd.Series(costs)`

### PredefinedAssetBacktest.get_orders_for_date(self, date: dt.date) -> pd.DataFrame
Purpose: Filter and return orders for a specific date.

**Algorithm:**
1. Filter orders where `execution_end_time().date() == date`
2. Convert to DataFrame via `to_dict`

### CashAccrualModel.get_accrued_value(self, current_value, to_state) -> dict
Purpose: Base implementation returning `None` (pass).

### ConstantCashAccrualModel.get_accrued_value(self, current_value, to_state) -> dict
Purpose: Compute accrued value using constant interest rate.

**Algorithm:**
1. Compute `days = (to_state - current_value[1]).days`
2. For each currency in `current_value[0]`:
   a. Branch: `self.annual` -> divide rate by 365
   b. Branch: else -> divide by 1
   c. Compound: `value * (1 + adjusted_rate) ** days`
3. Return `new_value` dict

### DataCashAccrualModel.get_accrued_value(self, current_value, to_state) -> dict
Purpose: Compute accrued value using rate from data source.

**Algorithm:**
1. Compute `days = (to_state - current_value[1]).days`
2. Fetch `rate = self.data_source.get_data(from_state)`
3. For each currency:
   a. Branch: `self.annual` -> divide rate by 365
   b. Branch: else -> divide by 1
   c. Compound: `value * (1 + adjusted_rate) ** days`
4. Return `new_value` dict

### OisFixingCashAccrualModel.get_accrued_value(self, current_value, to_state) -> dict
Purpose: Compute accrued value using OIS fixing rates (lazy-loaded from IRSwap Cashflows).

**Algorithm:**
1. For each `currency` in `current_value[0].keys()`:
   a. Branch: `currency not in ois_fixings` (global cache miss):
      - Branch: `isinstance(self.start_date, dt.date)` -> use as-is
      - Branch: else (str) -> `RelativeDate(self.start_date).apply_rule()`
      - Subtract 7 days from start_date
      - Branch: `isinstance(self.end_date, dt.date)` -> use as-is
      - Branch: else (str) -> `RelativeDate(self.end_date).apply_rule()`
      - Construct `IRSwap` with OIS parameters
      - Price with `PricingContext()`, calc `Cashflows`
      - Filter for `'Flt'` payment type, build `GenericDataSource` with fill-forward
      - Cache in `ois_fixings[currency]`
   b. Create `DataCashAccrualModel` with cached data source
   c. Return `ds_accrual_model.get_accrued_value(current_value, to_state)`

*Note: The `return` is inside the for loop, so only the first currency is processed.*

## State Mutation
- `BackTest._portfolio_dict`: Modified by engine action handlers.
- `BackTest._cash_dict`: Modified by `_handle_cash` in the engine.
- `BackTest._hedges`: Modified by `HedgeActionImpl`.
- `BackTest._cash_payments`: Modified by all action handlers.
- `BackTest._transaction_costs`: Set at end of backtest run.
- `BackTest._transaction_cost_entries`: Modified by all action handlers.
- `BackTest._results`: Modified by pricing steps and `add_results()`.
- `BackTest._trade_exit_risk_results`: Modified by `_handle_cash`.
- `BackTest._risk_summary_dict`: Lazily computed and cached by `get_risk_summary_df()`.
- `BackTest._calc_calls`, `_calculations`: Incremented throughout backtest.
- `BackTest.strategy`: Deep-copied in `__post_init__`.
- `BackTest.risks`: Wrapped via `make_list` in `__post_init__`.
- `TransactionCostEntry._unit_cost_by_model_by_inst`: Populated by `calculate_unit_cost()`.
- `TransactionCostEntry._additional_scaling`: Set by hedge scaling logic in engine.
- `TransactionCostEntry._date`: May be reassigned by `ExitTradeActionImpl`.
- `CashPayment.cash_paid`: Populated during `_handle_cash`.
- `CashPayment.direction`: May be modified by `ExitTradeActionImpl` (netted out).
- `CashPayment.effective_date`: May be modified by `ExitTradeActionImpl`.
- `PredefinedAssetBacktest.holdings`: Modified by `update_fill` and `set_start_date`.
- `PredefinedAssetBacktest.performance`: Modified by `mark_to_market` and `set_start_date`.
- `PredefinedAssetBacktest.orders`: Extended by `record_orders`.
- `ois_fixings` (global): Lazily populated by `OisFixingCashAccrualModel.get_accrued_value()`.
- `ScalingPortfolio.results`: Set to `None` initially, populated later by engine.
- Thread safety: No synchronization. `ois_fixings` is a global mutable dict with no locking.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `ScaledTransactionModel.get_unit_cost` | When `scaling_type` string attribute is not found on instrument (`AttributeError` caught and re-raised) |
| `RuntimeError` | `AggregateTransactionModel.get_unit_cost` | When `aggregate_type` is unrecognized (but see bug below) |
| `RuntimeError` | `BackTest.result_summary` | When `len(cash_summary) > 1` (multiple currencies) |
| `ValueError` | `TransactionCostEntry.get_cost_by_component` | When min/max aggregation result does not match either fixed or scaled cost |
| `TypeError` (caught) | `BackTest.get_risk_summary_df` | When `results[risk].aggregate()` fails; produces `ErrorValue` instead |
| `ZeroDivisionError` (uncaught) | `PredefinedAssetBacktest.mark_to_market` | When `mtm == 0` during weight normalization |

## Edge Cases
- `BackTest.add_results`: date exists but results list is empty (`len==0`) -> takes else branch (overwrites).
- `BackTest.result_summary`: >1 currency in cash_dict -> RuntimeError.
- `BackTest.trade_ledger`: `direction==0` creates a closed-at-zero entry; name already in names but `cash_paid` empty -> elif body not entered (no update).
- `BackTest.pnl_explain`: `pnl_explain_def is None` -> returns None; only one date in dates -> loop body never executes (`range(1,1)` is empty).
- `BackTest.strategy_as_time_series`: empty cash_payments or empty results -> will raise on `pd.concat` with empty input.
- `BackTest.get_risk_summary_df`: caches `_risk_summary_dict` on first call; subsequent calls return cached version even if results change.
- `ScaledTransactionModel.get_unit_cost`: `state > today()` -> returns `np.nan`; scaling_type string not found on instrument -> RuntimeError.
- `AggregateTransactionModel.get_unit_cost`: empty `transaction_models` -> returns `0`; unrecognized `aggregate_type` -> RuntimeError (but crashes with wrong attribute name in error message).
- `TransactionCostEntry.get_cost_by_component`: min/max aggregation where both fixed and scaled costs exist and `func([f,s])` equals neither -> ValueError.
- `TransactionCostEntry.cost_aggregation_func`: if `AggregateTransactionModel` has unrecognized `aggregate_type`, falls through all `if` checks and returns `sum` (default).
- `PredefinedAssetBacktest.mark_to_market`: all holdings below epsilon -> `mtm=0`, then division by zero in weight normalization.
- `PredefinedAssetBacktest.mark_to_market`: window with no fixings (empty range) -> `np.nan` fixing -> nan notional.
- `OisFixingCashAccrualModel.get_accrued_value`: returns inside the `for currency` loop body, so multi-currency dicts only process the first currency.
- `OisFixingCashAccrualModel.end_date` defaults to `dt.date.today()` at module import time (not at call time), so it becomes stale.
- `ConstantCashAccrualModel` and `DataCashAccrualModel` share the same `class_type = 'cash_accrual_model'` as the base class, which could cause deserialization ambiguity.
- `PredefinedAssetBacktest.trade_ledger`: if an instrument has only longs and no shorts (or vice versa), all orders become unmatched open positions.
- `TransactionModel.get_unit_cost` base returns `None` (from `pass`), not `0`. Callers expecting a numeric result will fail.

## Bugs Found
- **Line 448** (`AggregateTransactionModel.get_unit_cost` else branch): The error message string references `self.aggregate_type` correctly, but historically the spec noted `self.aggregation_type` which would be an `AttributeError`. Reviewing the actual code, the raise statement is `raise RuntimeError(f'unrecognised aggregation type:{str(self.aggregate_type)}')` which is correct. However, this branch is unreachable since `TransactionAggType` is a closed enum with only SUM/MAX/MIN values, and the earlier branches cover all three.
- **Line 846** (`OisFixingCashAccrualModel.get_accrued_value`): The `return` statement is inside the `for currency` loop body, so only the first currency is ever processed. If `current_value[0]` has multiple currencies, subsequent ones are silently ignored.
- **Line 746** (`PredefinedAssetBacktest.mark_to_market`): If all instrument holdings are below epsilon (or holdings is empty), `mtm` remains 0, causing `ZeroDivisionError` in the weight normalization loop `notional / mtm`.
- **`OisFixingCashAccrualModel.end_date`**: Defaults to `dt.date.today()` at class definition time (module import), not at runtime. This means the default becomes stale for long-running processes.

## Coverage Notes
- Approximately 85-90 distinct branch points across all classes.
- `BackTest`: ~25 branches
  - `add_results`: 2 branches (date exists with non-empty + not replace vs else)
  - `get_risk_summary_df`: 6 branches (cached, empty results, TypeError, zero_on_empty_dates)
  - `result_summary`: 3 branches (cash_summary length: >1, ==1, ==0)
  - `trade_ledger`: 4 branches (direction==0, name in names + cash_paid, name in names + no cash, else new)
  - `pnl_explain`: 7 branches (def is None, prev_date not in results, risk==0, cur_date in results, second_order)
  - Properties: trivial (no branches)
- `TransactionModel` hierarchy: ~12 branches
  - `ScaledTransactionModel.get_unit_cost`: 4 branches (isinstance str + success/AttributeError, state > today, else)
  - `AggregateTransactionModel.get_unit_cost`: 5 branches (empty, SUM, MAX, MIN, else)
  - `ConstantTransactionModel`: 0 branches
  - Base `TransactionModel`: 0 branches
- `TransactionCostEntry`: ~20 branches across 7 methods
  - `all_instruments`: 2 (Portfolio / not)
  - `all_transaction_models`: 2 (Aggregate / not)
  - `cost_aggregation_func`: 5 (Aggregate + SUM/MAX/MIN, fallthrough)
  - `no_of_risk_calcs`: 1 (filter condition)
  - `__resolved_cost`: 3 (PortfolioRiskResult, PricingFuture, else)
  - `get_final_cost`: 3 (ScaledTransactionModel, costs non-empty, costs empty)
  - `get_cost_by_component`: 9 (Scaled/not, scaled None, fixed None, sum, min/max match fixed/scaled/neither)
- `PredefinedAssetBacktest`: ~15 branches
  - `trade_ledger`: 9 (instrument not in queues, quantity < 0, longs empty, execution time comparison, close_order exists, longs/shorts get)
  - `mark_to_market`: 5 (abs > epsilon, isinstance Cash, window truthy + fixings empty, window falsy)
  - `get_costs`: 1 (isinstance OrderCost)
- Cash accrual models: ~8 branches
  - `ConstantCashAccrualModel`: 1 (annual true/false)
  - `DataCashAccrualModel`: 1 (annual true/false)
  - `OisFixingCashAccrualModel`: 5 (currency not in cache, start_date isinstance, end_date isinstance)
- Mocking notes:
  - `BackTest` methods need mocked `PortfolioRiskResult` objects with `.aggregate()`, `.risk_measures`, `.__getitem__()`, `.portfolio`, `.to_frame()`, `.futures`.
  - `ScaledTransactionModel.get_unit_cost` needs mocked instruments with dynamic attributes and `PricingContext`.
  - `TransactionCostEntry` needs mocked `PricingFuture.result()` and `PortfolioRiskResult.aggregate()`.
  - `PredefinedAssetBacktest.mark_to_market` needs mocked `DataHandler` with `get_data` and `get_data_range`.
  - `OisFixingCashAccrualModel` needs mocked `PricingContext`, `IRSwap.calc`, `Cashflows` result, and the global `ois_fixings` dict should be reset between tests.
  - `BackTest.get_risk_summary_df` caches `_risk_summary_dict`; tests should verify both cached and uncached paths.
  - `PredefinedAssetBacktest.trade_ledger` needs mock orders with `instrument`, `quantity`, `execution_end_time()`, `executed_price`.
- Pragmas: none
