# generic_engine.py

## Summary
Implements the `GenericEngine` backtest runner and its action handler implementations (`AddTradeActionImpl`, `AddScaledTradeActionImpl`, `HedgeActionImpl`, `ExitTradeActionImpl`, `RebalanceActionImpl`). Each action handler translates strategy trigger events into portfolio mutations: order creation, scaling, hedging, exit, and rebalance -- with transaction cost tracking and cash payment bookkeeping. The engine orchestrates the full backtest lifecycle: date scheduling, trigger evaluation, portfolio pricing, path-dependent processing, hedge scaling, and cash handling. Also includes `GenericEngineActionFactory` for mapping action types to their handler implementations.

## Dependencies
- Internal: `gs_quant.backtests.action_handler` (`ActionHandlerBaseFactory`, `ActionHandler`), `gs_quant.backtests.actions` (`Action`, `AddTradeAction`, `HedgeAction`, `AddTradeActionInfo`, `HedgeActionInfo`, `ExitTradeAction`, `ExitTradeActionInfo`, `RebalanceAction`, `RebalanceActionInfo`, `ExitAllPositionsAction`, `AddScaledTradeAction`, `ScalingActionType`, `AddScaledTradeActionInfo`), `gs_quant.backtests.backtest_engine` (`BacktestBaseEngine`), `gs_quant.backtests.backtest_objects` (`BackTest`, `ScalingPortfolio`, `CashPayment`, `Hedge`, `PnlDefinition`, `TransactionCostEntry`), `gs_quant.backtests.backtest_utils` (`make_list`, `CalcType`, `get_final_date`, `map_ccy_name_to_ccy`, `interpolate_signal`), `gs_quant.backtests.strategy` (`Strategy`), `gs_quant.common` (`Currency`, `ParameterisedRiskMeasure`, `RiskMeasure`), `gs_quant.context_base` (`nullcontext`), `gs_quant.datetime.relative_date` (`RelativeDateSchedule`), `gs_quant.instrument` (`Instrument`), `gs_quant.markets` (`PricingContext`, `HistoricalPricingContext`), `gs_quant.markets.portfolio` (`Portfolio`), `gs_quant.risk` (`Price`), `gs_quant.risk.results` (`PortfolioRiskResult`), `gs_quant.target.measures` (`ResolvedInstrumentValues`), `gs_quant.tracing` (`Tracer`)
- External: `copy` (`deepcopy`), `datetime` (`dt.date`, `dt.datetime`), `logging`, `abc` (`ABCMeta`), `collections` (`defaultdict`, `namedtuple`), `functools` (`reduce`), `itertools` (`zip_longest`), `typing` (`Union`, `Iterable`, `Optional`, `Dict`, `Collection`)

## Type Definitions

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| DEFAULT_REQUEST_PRIORITY | `int` | `5` | Default priority for pricing API requests (range 1-10) |
| logger | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

### OrderBasedActionImpl (class, abstract)
Inherits: `ActionHandler` (via `ABCMeta`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _order_valuations | `list` | `[ResolvedInstrumentValues]` | Risk measures to request when creating orders |

### AddTradeActionImpl (class)
Inherits: `OrderBasedActionImpl`

No additional fields beyond inherited.

### AddScaledTradeActionImpl (class)
Inherits: `OrderBasedActionImpl`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _scaling_level_signal | `Optional[dict]` | `None` or interpolated signal | Interpolated scaling signal when `action.scaling_level` is a dict; `None` when it is a scalar |

### HedgeActionImpl (class)
Inherits: `OrderBasedActionImpl`

No additional fields beyond inherited.

### ExitTradeActionImpl (class)
Inherits: `ActionHandler`

No additional fields.

### RebalanceActionImpl (class)
Inherits: `ActionHandler`

No additional fields.

### GenericEngineActionFactory (class)
Inherits: `ActionHandlerBaseFactory`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| action_impl_map | `dict` | Default map (see below) | Maps Action types to their handler implementation classes |

### GenericEngine (class)
Inherits: `BacktestBaseEngine`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| action_impl_map | `dict` | `{}` | User-provided action implementation overrides |
| price_measure | `RiskMeasure` | `Price` | Default risk measure for pricing |
| _pricing_context_params | `Optional[dict]` | `None` | Stored pricing context parameters from `run_backtest` |
| _initial_pricing_context | `Optional[PricingContext]` | `None` | Unused (initialized but never read) |
| _tracing_enabled | `bool` | `False` | Whether OpenTracing is active |

## Enums and Constants

No enums defined in this module. The module uses `ScalingActionType` and `CalcType` from other modules.

## Functions/Methods

### raiser(ex) -> NoReturn
Purpose: Raise a RuntimeError; used as an expression-level raise (e.g. inside list comprehensions).

**Algorithm:**
1. Raise `RuntimeError(ex)` unconditionally.

### OrderBasedActionImpl.__init__(self, action: Action) -> None
Purpose: Initialize with order valuations list and delegate to parent.

**Algorithm:**
1. Set `self._order_valuations = [ResolvedInstrumentValues]`
2. Call `super().__init__(action)`

### OrderBasedActionImpl.get_base_orders_for_states(self, states: Collection[dt.date], **kwargs) -> dict
Purpose: Resolve instruments at each state date using PricingContext.

**Algorithm:**
1. Initialize `orders = {}`
2. Get `dated_priceables` from `getattr(self.action, 'dated_priceables', {})`
3. Open outer `PricingContext()`
4. For each state `s`:
   a. Branch: `dated_priceables.get(s)` is truthy -> use it as `active_portfolio`
   b. Branch: else -> use `self.action.priceables` as `active_portfolio`
   c. Open inner `PricingContext(pricing_date=s)`, calc `Portfolio(active_portfolio).calc(tuple(self._order_valuations))`
   d. Store result in `orders[s]`
5. Return `orders`

### OrderBasedActionImpl.get_instrument_final_date(self, inst: Instrument, order_date: dt.date, info: namedtuple) -> dt.date
Purpose: Delegate to `get_final_date` utility for trade duration computation.

**Algorithm:**
1. Return `get_final_date(inst, order_date, self.action.trade_duration, self.action.holiday_calendar, info)`

### AddTradeActionImpl.__init__(self, action: AddTradeAction) -> None
Purpose: Delegate to parent.

### AddTradeActionImpl._raise_order(self, state, trigger_info) -> dict
Purpose: Create orders by resolving instruments, cloning with date suffix, and applying scaling.

**Algorithm:**
1. Convert `state` to list via `make_list`
2. Branch: `trigger_info is None or isinstance(trigger_info, AddTradeActionInfo)` -> replicate for all states
3. Branch: else (trigger_info is already a list) -> use as-is
4. Build `ti_by_state` dict via `zip_longest`
5. Call `get_base_orders_for_states(state_list)`
6. For each `(d, p)` in orders:
   a. Clone each trade with `_{d}` name suffix
   b. Branch: `ti is None` -> scale with `None`
   c. Branch: else -> scale with `ti.scaling`
7. Return `{date: (scaled_portfolio, trigger_info)}` dict

### AddTradeActionImpl.apply_action(self, state, backtest: BackTest, trigger_info=None) -> BackTest
Purpose: Execute add-trade action: create entry/exit cash payments, TCs, and add instruments to portfolio.

**Algorithm:**
1. Call `_raise_order(state, trigger_info)` to get orders
2. For each `(create_date, (portfolio, info))`:
   a. For each instrument:
      - Create entry `TransactionCostEntry` and `CashPayment(direction=-1)`
      - Compute `final_date` via `get_instrument_final_date`
      - Create exit `TransactionCostEntry` and `CashPayment(direction=1)`
      - Append instrument to `backtest.portfolio_dict[s]` for all states in `[create_date, final_date)`
3. Open `PricingContext(is_async=True)`:
   a. Branch: `any(tce.no_of_risk_calcs > 0 for tce in current_tc_entries)` -> increment `backtest.calc_calls`
   b. Calculate unit costs for all TC entries
4. Return `backtest`

### AddScaledTradeActionImpl.__init__(self, action: AddScaledTradeAction) -> None
Purpose: Initialize scaling signal if scaling_level is a dict.

**Algorithm:**
1. Call `super().__init__(action)`
2. Branch: `isinstance(self.action.scaling_level, dict)` -> set `self._scaling_level_signal = interpolate_signal(self.action.scaling_level)`
3. Branch: else -> set `self._scaling_level_signal = None`

### AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash(portfolio, available_cash, cur_day, unscaled_prices_by_day, unscaled_entry_tces_by_day) -> float (staticmethod)
Purpose: Compute portfolio scaling factor that fits within available cash, accounting for transaction costs.

**Algorithm:**
1. First pass: sum `fixed_tcs` and `scaling_based_tcs` from unscaled TC entries via `get_cost_by_component()`
2. Compute `first_scale_factor = (available_cash - fixed_tcs) / (aggregate_price + scaling_based_tcs)`
3. Branch: `first_scale_factor == 0` -> return `0` (early exit)
4. Second pass: set `additional_scaling = first_scale_factor` on each TCE, re-sum fixed and scaling TCs
5. Compute `second_scale_factor = max(available_cash - fixed_tcs, 0) / (aggregate_price * first_scale_factor + scaling_based_tcs)`
6. Return `first_scale_factor * second_scale_factor`

### AddScaledTradeActionImpl._nav_scale_orders(self, orders, price_measure, trigger_infos) -> None
Purpose: Scale orders by NAV: invest available cash, then reinvest unwind proceeds.

**Algorithm:**
1. Sort order days
2. Build `unscaled_entry_tces_by_day` and `unscaled_unwind_tces_by_day` dicts (instrument -> TCE per day)
3. Send all unscaled prices and TC calcs in a single async PricingContext:
   a. For each order day: calc `price_measure` on portfolio
   b. For each unwind day:
      - Branch: `unwind_day <= dt.date.today()` -> calc unwind price
      - Branch: else -> skip (future date)
   c. Calculate unit costs for all entry and exit TCEs
4. Set initial `available_cash = self.action.scaling_level`
5. For each sorted order day (`idx, cur_day`):
   a. Call `__portfolio_scaling_for_available_cash` to get `scale_factor`
   b. Record `scaling_factors_by_day[cur_day]` and per-instrument
   c. Reset `available_cash = 0`
   d. Branch: `idx + 1 < len(sorted_order_days)` -> compute `next_day`
   e. Branch: `idx + 1 >= len` -> break
   f. For each final day `d` between `cur_day` and `next_day`:
      - Accumulate `available_cash` from unwind proceeds minus TC costs
   g. Floor `available_cash = max(available_cash, 0)`
6. Apply scaling to orders:
   a. Branch: `scaling_factors_by_day[day] == 0` -> delete order day from dict
   b. Branch: nonzero -> `portfolio.scale(factor)` in place

### AddScaledTradeActionImpl._scaling_level_for_date(self, d: dt.date) -> float
Purpose: Return scaling level for a given date, using interpolated signal if available.

**Algorithm:**
1. Branch: `self._scaling_level_signal is not None`:
   a. Branch: `d in self._scaling_level_signal` -> return `self._scaling_level_signal[d]`
   b. Branch: `d not in signal` -> return `0`
2. Branch: `self._scaling_level_signal is None` -> return `self.action.scaling_level`

### AddScaledTradeActionImpl._scale_order(self, orders, daily_risk, price_measure, trigger_infos) -> None
Purpose: Dispatch to appropriate scaling method based on scaling_type.

**Algorithm:**
1. Branch: `scaling_type == ScalingActionType.size` -> scale each portfolio by `_scaling_level_for_date(day)`
2. Branch: `scaling_type == ScalingActionType.NAV` -> delegate to `_nav_scale_orders`
3. Branch: `scaling_type == ScalingActionType.risk_measure` -> scale by `scaling_level / daily_risk[day]`
4. Branch: else -> raise `RuntimeError(f'Scaling Type {self.action.scaling_type} not supported by engine')`

### AddScaledTradeActionImpl._raise_order(self, state_list, price_measure, trigger_infos) -> dict
Purpose: Create scaled orders based on scaling type.

**Algorithm:**
1. Branch: `scaling_type == ScalingActionType.risk_measure` -> append `action.scaling_risk` to `_order_valuations`
2. Call `get_base_orders_for_states(state_list)`
3. For each `(d, res)`:
   a. For each instrument in `self.action.priceables`:
      - Get resolved instrument from result
      - Branch: `len(self._order_valuations) > 1` -> extract `ResolvedInstrumentValues` sub-result
      - Branch: single valuation -> use result directly
      - Rename with date suffix
4. Branch: `scaling_type == ScalingActionType.risk_measure` -> build `daily_risk = {d: res[scaling_risk].aggregate()}`
5. Branch: else -> `daily_risk = None`
6. Call `_scale_order(final_orders, daily_risk, price_measure, trigger_infos)`
7. Return final_orders

### AddScaledTradeActionImpl.apply_action(self, state, backtest, trigger_info=None) -> BackTest
Purpose: Execute scaled-trade action (same structure as AddTradeActionImpl but with price_measure and trigger_infos).

**Algorithm:**
1. Convert state to list, normalize trigger_info:
   a. Branch: `trigger_info is None or isinstance(trigger_info, AddScaledTradeActionInfo)` -> replicate for all states
   b. Branch: else -> use as-is
2. Call `_raise_order(state_list, backtest.price_measure, trigger_infos)`
3. For each `(create_date, portfolio)`:
   a. Create entry/exit TCs and CashPayments (same pattern as AddTradeActionImpl)
   b. Populate `portfolio_dict` for active dates
4. Async TC calculation
5. Return `backtest`

### HedgeActionImpl.__init__(self, action: HedgeAction) -> None
Purpose: Delegate to parent.

### HedgeActionImpl.get_base_orders_for_states(self, states, **kwargs) -> dict
Purpose: Resolve hedge portfolio using HistoricalPricingContext.

**Algorithm:**
1. Open `HistoricalPricingContext(dates=states, csa_term=self.action.csa_term)`
2. Resolve `Portfolio(self.action.priceable)` out of place
3. Return resolved result

### HedgeActionImpl.apply_action(self, state, backtest, trigger_info=None) -> BackTest
Purpose: Create hedge entries with ScalingPortfolio for later risk-based scaling.

**Algorithm:**
1. Convert state/trigger_info to lists (same pattern as AddTradeActionImpl)
2. Increment `backtest.calc_calls` and `calculations`
3. Call `get_base_orders_for_states`
4. For each `(create_date, portfolio)`:
   a. Get `hedge_trade = portfolio.priceables[0]`
   b. Rename with date suffix
   c. Branch: `isinstance(hedge_trade, Portfolio)` -> rename each sub-instrument
   d. Compute `final_date`
   e. Filter `active_dates` = states in `[create_date, final_date)`
   f. Branch: `len(active_dates) > 0`:
      - Create `ScalingPortfolio` with trade, dates, risk, csa_term, etc.
      - Create entry `TransactionCostEntry` and `CashPayment(direction=-1)`
      - Create exit `TransactionCostEntry`
      - Branch: `final_date <= dt.date.today()` -> create exit `CashPayment`
      - Branch: else -> `exit_payment = None`
      - Create `Hedge` object, append to `backtest.hedges[create_date]`
   g. Branch: `len(active_dates) == 0` -> skip (no hedge created)
5. Async TC calculation
6. Return `backtest`

### ExitTradeActionImpl.__init__(self, action: ExitTradeAction) -> None
Purpose: Delegate to parent.

### ExitTradeActionImpl.apply_action(self, state, backtest, trigger_info=None) -> BackTest
Purpose: Remove specified trades from portfolio and rearrange cash payments.

**Algorithm:**
1. For each state `s` in `make_list(state)`:
   a. Initialize `trades_to_remove = []`
   b. Branch: `self.action.priceable_names is None` -> collect `current_trade_names` from portfolio at `s`
   c. Filter future dates `>= s`
   d. For each future `port_date`:
      - Get position list and results list
      - Branch: `self.action.priceable_names` is truthy -> filter by name suffix matching (TradeDate <= s AND TradeName in priceable_names)
      - Branch: `priceable_names is None` -> filter by name in `current_trade_names`
      - Remove matched trades from position list and results (reverse-index deletion)
      - Branch: `result_indexes_to_remove` non-empty -> rebuild `PortfolioRiskResult`
   e. Move future cash payments to exit date `s`:
      - For each `cp_date > s` with matching trades:
        - Branch: trade already has a cash payment at `s` (`prev_pos` found) -> net out direction
        - Branch: no existing payment -> move payment to `s`
        - Transfer TC entries from `cp_date` to `s`
        - Branch: `backtest.cash_payments[cp_date]` is now empty -> delete key
   f. For trades with no cash payment at `s`:
      - Branch: `isinstance(trade, Portfolio)` -> `set(t.to_dict() for t in trade.all_instruments)`
      - Branch: single instrument -> `{trade.to_dict()}`
      - Find matching TCE, create `CashPayment` at `s`
2. Return `backtest`

### RebalanceActionImpl.__init__(self, action: RebalanceAction) -> None
Purpose: Delegate to parent.

### RebalanceActionImpl.apply_action(self, state, backtest, trigger_info=None) -> BackTest
Purpose: Rebalance a position to a new size determined by the action's method.

**Algorithm:**
1. Call `new_size = self.action.method(state, backtest, trigger_info)`
2. Sum `current_size` from matching trades in `backtest.portfolio_dict[state]`
3. Branch: `new_size - current_size == 0` -> return `backtest` (no-op)
4. Clone position with delta size
5. Create entry `TransactionCostEntry` and `CashPayment(direction=-1)`
6. Search `cash_payments` in reverse date order for matching unwind payment (`direction == 1`):
   a. Branch: found -> create exit `TransactionCostEntry` and unwind `CashPayment`, append to `backtest.cash_payments[d]` and `backtest.transaction_cost_entries[d]`
   b. Break after finding first match
7. Branch: `unwind_payment is None` -> raise `ValueError("Found no final cash payment to rebalance for trade.")`
8. Add position to `portfolio_dict` for states in `[state, unwind_date)`
9. Async TC calculation
10. Return `backtest`

### GenericEngineActionFactory.__init__(self, action_impl_map=None) -> None
Purpose: Build default action-to-handler map, merge with user overrides.

**Algorithm:**
1. Build default map: `{AddTradeAction: AddTradeActionImpl, HedgeAction: HedgeActionImpl, ExitTradeAction: ExitTradeActionImpl, ExitAllPositionsAction: ExitTradeActionImpl, RebalanceAction: RebalanceActionImpl, AddScaledTradeAction: AddScaledTradeActionImpl}`
2. Merge with `action_impl_map or {}`

### GenericEngineActionFactory.get_action_handler(self, action: Action) -> ActionHandler
Purpose: Look up and instantiate the handler for an action type.

**Algorithm:**
1. Branch: `type(action) in self.action_impl_map` -> return `self.action_impl_map[type(action)](action)`
2. Branch: not in map -> raise `RuntimeError(f'Action {type(action)} not supported by engine')`

### GenericEngine.__init__(self, action_impl_map=None, price_measure=Price) -> None
Purpose: Initialize engine with optional action overrides and price measure.

**Algorithm:**
1. Branch: `action_impl_map is None` -> set `self.action_impl_map = {}`
2. Branch: else -> use provided map
3. Store `price_measure`, initialize `_pricing_context_params`, `_initial_pricing_context`, `_tracing_enabled`

### GenericEngine.get_action_handler(self, action: Action) -> ActionHandler
Purpose: Create factory and delegate.

### GenericEngine.supports_strategy(self, strategy: Strategy) -> bool
Purpose: Check if all actions in strategy are supported.

**Algorithm:**
1. Flatten all actions from all triggers
2. Try `get_action_handler` for each:
   a. Branch: `RuntimeError` raised -> return `False`
   b. Branch: no error -> return `True`

### GenericEngine.new_pricing_context(self) -> PricingContext
Purpose: Create a PricingContext with stored params and batch settings.

**Algorithm:**
1. Read params with defaults (`show_progress=False`, `request_priority=5`, `is_batch=True`)
2. Create `PricingContext(set_parameters_only=True, use_historical_diddles_only=True, ...)`
3. Set `_max_concurrent=1500`, `_dates_per_batch=200`
4. Return context

### GenericEngine.run_backtest(self, strategy, start, end, frequency, states, risks, show_progress, csa_term, visible_to_gs, initial_value, result_ccy, holiday_calendar, market_data_location, is_batch, calc_risk_at_trade_exits, pnl_explain) -> BackTest
Purpose: Entry point -- configure pricing context and delegate to `__run`.

**Algorithm:**
1. Set `_tracing_enabled` from `Tracer.active_span()`
2. Store pricing context params
3. Open `new_pricing_context()`, call `__run(...)`
4. Return result

### GenericEngine._trace(self, label: str) -> context manager
Purpose: Return tracing context if enabled.

**Algorithm:**
1. Branch: `self._tracing_enabled` -> return `Tracer(label)`
2. Branch: not enabled -> return `nullcontext()`

### GenericEngine.__run(self, strategy, start, end, frequency, states, risks, initial_value, result_ccy, holiday_calendar, calc_risk_at_trade_exits, pnl_explain) -> BackTest
Purpose: Core backtest execution loop.

**Algorithm:**
1. Branch: `states is None` -> build dates from `RelativeDateSchedule(frequency, start, end).apply_rule()`
2. Branch: else -> use provided `states`
3. Sort dates, add trigger times within `[start, end]`
4. Deduplicate and re-sort
5. Branch: `pnl_explain is not None` -> force `calc_risk_at_trade_exits=True`, extract `pnl_risks`
6. Branch: else -> `pnl_risks = []`
7. Build risks list (union of input risks, strategy risks, pnl_risks, price_measure)
8. Branch: `result_ccy is not None`:
   a. For each risk:
      - Branch: `isinstance(r, ParameterisedRiskMeasure)` -> parameterize with currency
      - Branch: else -> call `raiser()` (raises RuntimeError)
   b. Branch: `isinstance(self.price_measure, ParameterisedRiskMeasure)` -> parameterize
   c. Branch: else -> call `raiser()`
9. Branch: `result_ccy is None` -> `price_risk = self.price_measure`
10. Create `BackTest` object
11. Call `_resolve_initial_portfolio`
12. Call `_build_simple_and_semi_triggers_and_actions`
13. Filter `portfolio_dict` and `hedges` to `[start, end]` range
14. Call `_price_semi_det_triggers`
15. For each date: call `_process_triggers_and_actions_for_date`
    - Branch: tracing `scope` is truthy -> set tags and log
16. Call `_calc_new_trades`
17. Call `_handle_cash`
18. Populate `transaction_costs` dict from TC entries
19. Return `backtest`

### GenericEngine._resolve_initial_portfolio(self, initial_portfolio, backtest, strategy_start_date, strategy_pricing_dates, holiday_calendar, duration=None) -> None
Purpose: Recursively resolve initial portfolio positions into the backtest.

**Algorithm:**
1. Branch: `isinstance(initial_portfolio, dict)`:
   a. Sort dates from dict keys
   b. For each `(i, d)`:
      - Branch: `i + 1 < len(sorted_dates)` -> `end_date = sorted_dates[i+1]`
      - Branch: else -> `end_date = strategy_pricing_dates[-1]`
      - Recurse with sub-portfolio and end_date as duration
2. Branch: not dict (list):
   a. Branch: `len(initial_portfolio) > 0`:
      - Clone/rename each instrument with date suffix
      - Create entry `CashPayment(direction=-1)` and exit `CashPayment`
      - Resolve portfolio at start date
      - For each pricing date `d`:
        - Branch: `duration is None` -> add to all dates `>= start`
        - Branch: `duration is not None` -> add if `d >= start and (d < duration or duration == last date)`
   b. Branch: `len(initial_portfolio) == 0` -> no-op

### GenericEngine._build_simple_and_semi_triggers_and_actions(self, strategy, backtest, strategy_pricing_dates) -> None
Purpose: Evaluate non-path-dependent triggers and apply their non-path-dependent actions in batch.

**Algorithm:**
1. For each trigger:
   a. Branch: `trigger.calc_type != CalcType.path_dependent`:
      - Evaluate trigger on all dates, collect `triggered_dates` and `trigger_infos`
      - For each action:
        - Branch: `action.calc_type != CalcType.path_dependent`:
          - Branch: `type(action) in trigger_infos` -> use direct match
          - Branch: `isinstance(action, mapped_action_type)` found -> use that
          - Branch: no match -> `trigger_info = None`
          - Call `apply_action(triggered_dates, backtest, trigger_info)`
   b. Branch: `trigger.calc_type == CalcType.path_dependent` -> skip

### GenericEngine._price_semi_det_triggers(backtest, risks) -> None (staticmethod)
Purpose: Price all portfolio positions and hedge scaling portfolios.

**Algorithm:**
1. Open `PricingContext()`, increment `calc_calls`
2. For each `(day, portfolio)` in `portfolio_dict`:
   a. Branch: `isinstance(day, dt.date)` -> price portfolio with `PricingContext(day)`
3. For each hedge list: price `scaling_portfolio` on its dates
   a. Branch: `isinstance(p.trade, Portfolio)` -> use directly
   b. Branch: else -> wrap in `Portfolio([p.trade])`

### GenericEngine.__ensure_risk_results(self, dates, backtest, risks) -> None
Purpose: Calculate risk results for trades not yet priced.

**Algorithm:**
1. For each date: find trades not in results:
   a. Branch: `not backtest.results[d]` -> all trades are new
   b. Branch: results exist -> filter trades whose name is not in results portfolio
2. Branch: `len(port_by_date) > 0` -> price missing trades in batch, add results
3. Branch: empty -> no-op

### GenericEngine._process_triggers_and_actions_for_date(self, d, strategy, backtest, risks) -> None
Purpose: Process path-dependent triggers/actions for a single date, then scale hedges.

**Algorithm:**
1. For each trigger:
   a. Branch: `trigger.calc_type == CalcType.path_dependent`:
      - Branch: `trigger.has_triggered(d, backtest)` -> for each action: ensure risk results, apply action
   b. Branch: not path_dependent:
      - For each action:
        - Branch: `action.calc_type == CalcType.path_dependent`:
          - Branch: `trigger.has_triggered(d, backtest)` -> ensure risk results, apply action
2. Branch: `d not in backtest.hedges` -> return early
3. For each hedge at `d`:
   a. Branch: `sp.results is None` -> price with `HistoricalPricingContext`
      - Branch: `isinstance(sp.trade, Portfolio)` -> use directly
      - Branch: else -> wrap in `Portfolio`
4. Branch: `backtest.hedges[d]` is non-empty:
   a. Call `__ensure_risk_results`
   b. Branch: `d not in backtest.results` -> return early (no risk to hedge)
   c. For each hedge:
      - Compute `current_risk` and `hedge_risk`
      - Branch: `hedge_risk == 0` -> skip (`continue`)
      - Branch: `current_risk.unit != hedge_risk.unit` -> raise `RuntimeError('cannot hedge in a different currency')`
      - Compute `scaling_factor`
      - Set `additional_scaling` on entry/exit TC entries
      - Branch: `hedge.exit_payment is not None` -> set additional_scaling on exit TC too
      - Branch: `isinstance(p.trade, Portfolio)`:
        - Deep-copy, scale, rename with `Scaled_` prefix
        - Add to `portfolio_dict` for all hedge dates
        - Update entry/exit payment trades
      - Branch: not Portfolio -> raise `RuntimeError('Hedge trade instrument must be a Portfolio')`
      - Append entry payment to cash_payments
      - Branch: `hedge.exit_payment is not None` -> append exit payment

### GenericEngine._calc_new_trades(self, backtest, risks) -> None
Purpose: Calculate risk for newly added portfolio positions not yet in results.

**Algorithm:**
1. Open `PricingContext()`, increment `calc_calls`
2. For each `(day, portfolio)`:
   a. Branch: `not portfolio` -> continue (empty)
   b. Get `trades_for_date` from results (Branch: `isinstance(results, PortfolioRiskResult)` -> use `.portfolio`, else `[]`)
   c. Find `leaves` not in `trades_for_date`
   d. Branch: `len(leaves) > 0` -> price them
3. Add leaf results to backtest

### GenericEngine._handle_cash(self, backtest, risks, price_risk, strategy_pricing_dates, strategy_end_date, initial_value, calc_risk_at_trade_exits, cash_accrual) -> None
Purpose: Calculate cash payments, track currency-based cash positions.

**Algorithm:**
1. Collect cash trades needing pricing:
   a. Branch: `isinstance(cp.trade, Portfolio)` -> expand to `all_instruments`
   b. Branch: else -> single trade
   c. Branch: `effective_date <= strategy_end_date and trade not in backtest.results` -> schedule for calc
   d. Branch: `calc_risk_at_trade_exits and cp.direction == 1` -> add to `exited_cash_trades`
2. Price cash trades in batch:
   a. Branch: `calc_risk_at_trade_exits and cash_date in exited_map` -> calc exit risks
3. Iterate sorted union of pricing dates and cash payment dates:
   a. Branch: `d <= strategy_end_date`:
      - Branch: `current_value is not None`:
        - Branch: `cash_accrual is None` -> use raw value
        - Branch: else -> `cash_accrual.get_accrued_value(current_value, d)`
      - Branch: `d in backtest.cash_payments`:
        - For each payment/trade:
          - Try `cash_results` lookup, fall back to `backtest.results`
          - Branch: `value == {}` (not found in either) -> raise `RuntimeError`
          - Branch: `value is not float` -> raise `RuntimeError`
          - Branch: `d not in cash_dict` -> initialize with `{ccy: initial_value}`
          - Branch: `ccy not in cash_dict[d]` -> initialize to `0`
          - Accumulate `cash_paid` per currency
        - Set `current_value = (cash_dict[d], d)`
      - Deep-copy `current_value`

## State Mutation
- `backtest.portfolio_dict`: Modified by all action handlers; instruments appended for active dates, removed by ExitTradeActionImpl.
- `backtest.cash_payments`: Modified by all action handlers; CashPayments appended at entry/exit dates, moved by ExitTradeActionImpl.
- `backtest.transaction_cost_entries`: Modified by all action handlers; TCEs appended at entry/exit dates.
- `backtest.results`: Modified by `_price_semi_det_triggers`, `__ensure_risk_results`, `_calc_new_trades`; results added/replaced.
- `backtest.hedges`: Modified by HedgeActionImpl; Hedge objects appended per date.
- `backtest.cash_dict`: Modified by `_handle_cash`; cash positions tracked per date/currency.
- `backtest.calc_calls`, `backtest.calculations`: Incremented throughout for tracking.
- `backtest.trade_exit_risk_results`: Modified by `_handle_cash` when `calc_risk_at_trade_exits=True`.
- `backtest.transaction_costs`: Set at end of `__run` from TC entries.
- `self._tracing_enabled`: Set in `run_backtest`.
- `self._pricing_context_params`: Set in `run_backtest`.
- `AddScaledTradeActionImpl._order_valuations`: May be mutated in `_raise_order` by appending `scaling_risk` (accumulates across calls).
- Thread safety: No synchronization; the engine is designed for single-threaded execution within PricingContext.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `raiser()` | Called from list comprehension when risk is not `ParameterisedRiskMeasure` and `result_ccy` is set |
| `RuntimeError` | `GenericEngineActionFactory.get_action_handler` | When action type is not in the action_impl_map |
| `RuntimeError` | `AddScaledTradeActionImpl._scale_order` | When scaling_type is not `size`, `NAV`, or `risk_measure` |
| `RuntimeError` | `_process_triggers_and_actions_for_date` | When `current_risk.unit != hedge_risk.unit` |
| `RuntimeError` | `_process_triggers_and_actions_for_date` | When hedge trade is not a Portfolio |
| `RuntimeError` | `_handle_cash` | When cash value lookup fails (empty dict `{}`) |
| `RuntimeError` | `_handle_cash` | When resolved cash value is not a float |
| `ValueError` | `RebalanceActionImpl.apply_action` | When no matching unwind payment is found |

## Edge Cases
- `result_ccy is not None` with unparameterised risk measures -> `raiser()` called, RuntimeError for each non-ParameterisedRiskMeasure.
- Empty `initial_portfolio` (`len == 0`) -> no-op, nothing added.
- `initial_portfolio` is a dict with multiple start dates -> recursive resolution with cascading end dates.
- `ExitTradeAction` with `priceable_names=None` and no trades in portfolio -> empty removal lists, no-op.
- `ExitTradeAction` moving cash payments: existing payment at exit date -> nets direction instead of appending.
- `ExitTradeAction` when `backtest.cash_payments[cp_date]` becomes empty after removal -> key is deleted.
- `RebalanceAction` where `new_size == current_size` -> early return (no-op).
- `RebalanceAction` with no matching unwind payment found -> raises ValueError.
- `HedgeAction` where `hedge_risk == 0` -> skips hedge (continue).
- `HedgeAction` where trade is not Portfolio -> raises RuntimeError.
- Hedge `exit_payment is None` (final_date > today) -> no exit payment appended.
- `AddScaledTradeAction` with `scaling_level` as dict -> interpolated signal; date not in signal -> returns 0.
- `AddScaledTradeAction` NAV scaling where `first_scale_factor == 0` -> returns 0 immediately.
- `AddScaledTradeAction` NAV scaling where `scale_factor == 0` for a day -> order deleted from dict.
- Unsupported scaling type -> RuntimeError.
- `_process_triggers_and_actions_for_date`: date not in `backtest.hedges` -> early return (avoids populating defaultdict).
- `_process_triggers_and_actions_for_date`: date not in `backtest.results` after ensure -> early return (no risk to hedge).
- `_calc_new_trades`: empty portfolio for a date -> continue.
- `_handle_cash`: cash value lookup fails (`value == {}`) -> RuntimeError with trade name and date.
- `_handle_cash`: value is not float -> RuntimeError.
- `_handle_cash`: `cash_accrual is None` -> raw value used (no interest accrual).
- `AddScaledTradeActionImpl._order_valuations` is mutated (appended to) in `_raise_order` when scaling_type is `risk_measure`. This means repeated calls accumulate duplicates in the list. This is a potential issue if the same handler is reused.
- `GenericEngine._resolve_initial_portfolio` with `duration=None` adds instruments to all pricing dates >= start, with no end bound (relies on caller providing correct date range).
- `_build_simple_and_semi_triggers_and_actions` uses `isinstance` fallback for trigger_info matching, which means subclasses of action types will match their parent's trigger_info.

## Bugs Found
- **Line 591 area** (`RebalanceActionImpl.apply_action`): The original spec noted that `backtest.transaction_cost_entries[d].append(exit)` uses Python's builtin `exit` instead of `tc_exit`. Reviewing the actual code at line 591, the code correctly uses `tc_exit` (the variable name). The spec note about the builtin `exit` appears to be outdated or based on a different version.
- **`AddScaledTradeActionImpl._raise_order`** (line 310): `self._order_valuations.append(self.action.scaling_risk)` mutates the instance list on every call. If the same handler instance is used for multiple `_raise_order` calls, `scaling_risk` will be appended multiple times, causing incorrect result extraction logic (the `len > 1` branch).

## Coverage Notes

**Branch counts (estimated):**
- `raiser`: 0 branches (always raises)
- `OrderBasedActionImpl.get_base_orders_for_states`: 2 branches (dated_priceables hit/miss)
- `AddTradeActionImpl._raise_order`: 3 branches (trigger_info None/single vs list, ti is None for scaling)
- `AddTradeActionImpl.apply_action`: 1 branch (any tc risk_calcs > 0)
- `AddScaledTradeActionImpl.__init__`: 2 branches (scaling_level dict vs not)
- `AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash`: 2 branches (first_scale_factor == 0 early return, normal path)
- `AddScaledTradeActionImpl._nav_scale_orders`: ~6 branches (unwind_day <= today, idx+1 < len, scale_factor == 0, available_cash floor)
- `AddScaledTradeActionImpl._scaling_level_for_date`: 3 branches (signal not None + d in, signal not None + d not in, signal None)
- `AddScaledTradeActionImpl._scale_order`: 4 branches (size, NAV, risk_measure, unsupported)
- `AddScaledTradeActionImpl._raise_order`: 3 branches (risk_measure type, len > 1 valuations, daily_risk None vs dict)
- `AddScaledTradeActionImpl.apply_action`: 2 branches (trigger_info None/single vs list)
- `HedgeActionImpl.get_base_orders_for_states`: 1 path
- `HedgeActionImpl.apply_action`: ~5 branches (trigger_info type, hedge_trade is Portfolio, len(active_dates) > 0, final_date <= today)
- `ExitTradeActionImpl.apply_action`: ~10 branches (priceable_names None vs provided, results exist, result_indexes non-empty, cash payment netting vs append, cash_payments empty, trade not in cash payments + Portfolio vs single)
- `RebalanceActionImpl.apply_action`: ~4 branches (delta == 0, unwind found vs not, unwind_payment is None)
- `GenericEngineActionFactory.get_action_handler`: 2 branches (known vs unknown action)
- `GenericEngine.__init__`: 1 branch (action_impl_map is None)
- `GenericEngine.supports_strategy`: 2 branches (RuntimeError vs success)
- `GenericEngine._trace`: 2 branches (tracing enabled / disabled)
- `GenericEngine.__run`: ~10 branches (states None, pnl_explain None, result_ccy None x2, tracing scope x2)
- `GenericEngine._resolve_initial_portfolio`: ~5 branches (dict vs list, len > 0, duration None, i+1 < len, d >= start with duration)
- `GenericEngine._build_simple_and_semi_triggers_and_actions`: ~6 branches (calc_type, action calc_type, trigger_info match by type vs isinstance vs None, scope truthy)
- `GenericEngine._price_semi_det_triggers`: 2 branches (day is dt.date, trade is Portfolio)
- `GenericEngine.__ensure_risk_results`: 3 branches (no results for date, trade not in results, port_by_date non-empty)
- `GenericEngine._process_triggers_and_actions_for_date`: ~10 branches (path_dependent trigger/action, has_triggered, hedge results None, trade is Portfolio, hedges non-empty, d not in results, hedge_risk == 0, unit mismatch, exit_payment is not None x2)
- `GenericEngine._calc_new_trades`: 3 branches (portfolio empty, isinstance PortfolioRiskResult, leaves found)
- `GenericEngine._handle_cash`: ~12 branches (trade is Portfolio, effective_date conditions, calc_risk_at_trade_exits x2, current_value None, cash_accrual None, value == {}, value not float, d not in cash_dict, ccy not in cash_dict)

**Total estimated branches: ~90**

**Mocking notes:**
- `PricingContext`, `HistoricalPricingContext`: Must be mocked; all pricing calls go through these context managers. Mock `__enter__`/`__exit__` and ensure `Portfolio.calc` / `Portfolio.resolve` return appropriate mock results.
- `Portfolio`: Mock `.calc()` to return `PortfolioRiskResult`-like objects with subscript access by risk measure and instrument name. Mock `.resolve()`, `.scale()`, `.all_instruments`, `.__iter__`.
- `Tracer` / `nullcontext`: Mock `Tracer.active_span()` to test tracing-enabled vs disabled paths.
- `RelativeDateSchedule`: Mock `.apply_rule()` to return date lists for `states=None` path.
- `Strategy`: Mock with triggers list, each trigger having `calc_type`, `has_triggered()`, `get_trigger_times()`, `actions`.
- `BackTest`: Can use real object but needs mock results dict, portfolio_dict, cash_payments, hedges, transaction_cost_entries.
- `TransactionCostEntry`: Mock or use real; need `.no_of_risk_calcs`, `.calculate_unit_cost()`, `.get_cost_by_component()`, `.get_final_cost()`, `.additional_scaling`.
- `CashPayment`: Mock or use real; need `.trade`, `.effective_date`, `.direction`, `.cash_paid`, `.transaction_cost_entry`.
- `get_final_date`, `interpolate_signal`, `make_list`, `map_ccy_name_to_ccy`: Utility functions that may need mocking for isolation.
- `Instrument.clone`, `Instrument.name`, `Instrument.to_dict`: Mock instrument behavior for trade creation and name matching.
- Pragmas: none
