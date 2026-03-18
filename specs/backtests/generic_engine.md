# generic_engine.py

## Summary
Implements the GenericEngine backtest runner and its action handlers (AddTrade, AddScaledTrade, Hedge, ExitTrade, Rebalance). Each action handler translates strategy triggers into portfolio mutations (order creation, scaling, hedging, exit, rebalance) with transaction cost tracking and cash payment bookkeeping. The engine orchestrates the full backtest lifecycle: date scheduling, trigger evaluation, portfolio pricing, path-dependent processing, and cash handling.

## Classes/Functions

### raiser(ex) (line 74)
1. Raises RuntimeError(ex) unconditionally.
   - Used as an expression-level raise (e.g. inside list comprehensions).

### OrderBasedActionImpl (line 82, abstract)
Base for order-based action handlers. Extends ActionHandler via ABCMeta.

**__init__(action):**
1. Sets `_order_valuations = [ResolvedInstrumentValues]`.
2. Calls super().__init__(action).

**get_base_orders_for_states(states, **kwargs):**
1. Gets `dated_priceables` from action (may be empty dict).
2. Opens outer PricingContext.
3. For each state s:
   - Branch 1: dated_priceables has entry for s -> use it.
   - Branch 2: no entry -> fall back to action.priceables.
4. Opens inner PricingContext(pricing_date=s), calculates ResolvedInstrumentValues.
5. Returns dict {date: result}.

**get_instrument_final_date(inst, order_date, info):**
1. Delegates to `get_final_date` utility.

### AddTradeActionImpl (line 101)

**_raise_order(state, trigger_info):**
1. Converts state to list.
2. Branch 1: trigger_info is None or single AddTradeActionInfo -> replicate for all states.
   Branch 2: trigger_info is already a list -> use as-is.
3. Calls get_base_orders_for_states.
4. For each date/portfolio pair:
   - Clones each trade with `_<date>` suffix.
   - Branch: ti is None -> scale(None). Else -> scale(ti.scaling).
5. Returns {date: (scaled_portfolio, trigger_info)}.

**apply_action(state, backtest, trigger_info):**
1. Calls _raise_order to get orders.
2. For each (create_date, portfolio):
   - For each instrument:
     a. Creates entry TransactionCostEntry + CashPayment(direction=-1).
     b. Calculates final_date via get_instrument_final_date.
     c. Creates exit TransactionCostEntry + CashPayment(direction=1 implicit).
     d. Appends instrument to portfolio_dict for all states in [create_date, final_date).
3. Opens async PricingContext:
   - Branch: any tc_entry has risk_calcs > 0 -> increment calc_calls.
   - Calculates unit costs for all tc entries.
4. Returns backtest.

### AddScaledTradeActionImpl (line 165)

**__init__(action):**
1. Branch: scaling_level is dict -> interpolate_signal to create _scaling_level_signal.
   Branch: not dict -> _scaling_level_signal = None.

**__portfolio_scaling_for_available_cash (static, line 172):**
1. First pass: sums fixed_tcs and scaling_based_tcs from unscaled entries.
2. Computes first_scale_factor = (available_cash - fixed_tcs) / (aggregate_price + scaling_based_tcs).
   - Branch: first_scale_factor == 0 -> return 0 early.
3. Second pass: sets additional_scaling on each TCE, re-computes components.
4. Computes second_scale_factor with max(available_cash - fixed_tcs, 0) floor.
5. Returns first_scale_factor * second_scale_factor.

**_nav_scale_orders(orders, price_measure, trigger_infos):**
1. Sorts order days.
2. Populates unscaled_entry_tces_by_day and unscaled_unwind_tces_by_day dicts.
3. Sends all unscaled prices and TC calcs in a single async PricingContext.
   - Branch: unwind_day <= today -> calc unwind price. Else -> skip.
4. Iterates sorted order days:
   - Calls __portfolio_scaling_for_available_cash for each day.
   - Records scaling_factor per instrument and per day.
   - Branch: idx+1 < len -> compute available_cash from unwinds between cur_day and next_day.
   - Branch: idx+1 >= len -> break.
   - Floors available_cash to 0.
5. Applies scaling to orders:
   - Branch: scaling_factor == 0 -> delete order day from dict.
   - Branch: nonzero -> scale portfolio in place.

**_scaling_level_for_date(d):**
1. Branch: _scaling_level_signal is not None:
   - Branch: d in signal -> return signal[d].
   - Branch: d not in signal -> return 0.
2. Branch: signal is None -> return action.scaling_level.

**_scale_order(orders, daily_risk, price_measure, trigger_infos):**
1. Branch: scaling_type == size -> scale each portfolio by _scaling_level_for_date(day).
2. Branch: scaling_type == NAV -> delegate to _nav_scale_orders.
3. Branch: scaling_type == risk_measure -> scale by scaling_level / daily_risk[day].
4. Branch: else -> raise RuntimeError.

**_raise_order(state_list, price_measure, trigger_infos):**
1. Branch: scaling_type == risk_measure -> append scaling_risk to _order_valuations.
2. Calls get_base_orders_for_states.
3. For each date/result:
   - Branch: len(_order_valuations) > 1 -> extract ResolvedInstrumentValues sub-result.
   - Branch: single valuation -> use result directly.
   - Renames instruments with date suffix.
4. Branch: scaling_type == risk_measure -> build daily_risk dict. Else -> daily_risk = None.
5. Calls _scale_order.

**apply_action(state, backtest, trigger_info):**
1. Same structure as AddTradeActionImpl.apply_action but calls _raise_order with price_measure and trigger_infos dict.
2. Creates entry/exit TCs and CashPayments, populates portfolio_dict.
3. Async TC calculation.

### HedgeActionImpl (line 378)

**get_base_orders_for_states(states, **kwargs):**
1. Uses HistoricalPricingContext(dates=states, csa_term=...).
2. Resolves Portfolio(action.priceable) out of place.
3. Returns resolved result.

**apply_action(state, backtest, trigger_info):**
1. Converts state/trigger_info to lists.
2. Calls get_base_orders_for_states.
3. For each create_date:
   - Renames hedge trade with date suffix.
   - Branch: hedge_trade is Portfolio -> rename each sub-instrument.
   - Calculates final_date.
   - Filters active_dates = states in [create_date, final_date).
   - Branch: len(active_dates) > 0:
     a. Creates ScalingPortfolio.
     b. Creates entry TC + CashPayment.
     c. Branch: final_date <= today -> create exit CashPayment. Else -> exit_payment = None.
     d. Creates Hedge object, appends to backtest.hedges.
   - Branch: no active dates -> skip (no hedge created).
4. Async TC calculation.

### ExitTradeActionImpl (line 450)

**apply_action(state, backtest, trigger_info):**
1. For each state s:
   - Branch: priceable_names is None -> collect current_trade_names from portfolio at s.
   - Filters future dates >= s.
   - For each future date:
     - Branch: priceable_names provided -> filter by name suffix matching + date <= s.
     - Branch: priceable_names is None -> filter by name in current_trade_names.
     - Removes matched trades from portfolio and results (reverse-index deletion).
     - Branch: result_indexes_to_remove non-empty -> rebuild PortfolioRiskResult.
   - Moves future cash payments to exit date s:
     - Branch: trade already has a cash payment at s -> net out direction.
     - Branch: no existing payment -> move payment to s.
     - Transfers TC entries from future date to s.
     - Branch: cash_payments[cp_date] is now empty -> delete key.
   - For trades with no cash payment at s:
     - Branch: trade is Portfolio -> set of dicts from all_instruments.
     - Branch: single instrument -> singleton set.
     - Finds matching TCE, creates CashPayment at s.

### RebalanceActionImpl (line 552)

**apply_action(state, backtest, trigger_info):**
1. Calls action.method(state, backtest, trigger_info) to get new_size.
2. Sums current_size from matching trades in portfolio_dict[state].
3. Branch: new_size - current_size == 0 -> return early (no-op).
4. Creates cloned position with delta size.
5. Creates entry TC + CashPayment.
6. Searches cash_payments in reverse date order for matching unwind payment (direction == 1).
   - Branch: found -> creates exit TC, unwind CashPayment.
   - **BUG line 591**: appends `exit` (Python builtin) instead of `tc_exit`.
7. Branch: unwind_payment is None -> raises ValueError.
8. Adds position to portfolio_dict for states in [state, unwind_date).
9. Async TC calculation.

### GenericEngineActionFactory (line 613)

**__init__(action_impl_map=None):**
1. Builds default map: {AddTradeAction: AddTradeActionImpl, HedgeAction: HedgeActionImpl, ExitTradeAction: ExitTradeActionImpl, ExitAllPositionsAction: ExitTradeActionImpl, RebalanceAction: RebalanceActionImpl, AddScaledTradeAction: AddScaledTradeActionImpl}.
2. Merges with user-provided action_impl_map if any.

**get_action_handler(action):**
1. Branch: type(action) in map -> return impl(action).
2. Branch: not in map -> raise RuntimeError.

### GenericEngine (line 631)

**__init__(action_impl_map=None, price_measure=Price):**
1. Branch: action_impl_map is None -> empty dict. Else -> use provided.
2. Stores price_measure, initializes _pricing_context_params, _initial_pricing_context, _tracing_enabled.

**supports_strategy(strategy):**
1. Flattens all actions from all triggers.
2. Tries get_action_handler for each.
   - Branch: RuntimeError raised -> return False.
   - Branch: no error -> return True.

**new_pricing_context():**
1. Reads stored params with defaults (show_progress=False, request_priority=5, is_batch=True).
2. Creates PricingContext with set_parameters_only=True, use_historical_diddles_only=True.
3. Sets _max_concurrent=1500, _dates_per_batch=200.

**run_backtest(strategy, start, end, frequency, states, risks, ...):**
1. Sets _tracing_enabled from active span.
2. Stores pricing context params.
3. Opens new_pricing_context, delegates to __run.

**_trace(label):**
1. Branch: _tracing_enabled -> return Tracer(label).
2. Branch: not enabled -> return nullcontext().

**__run(strategy, start, end, frequency, states, risks, initial_value, result_ccy, holiday_calendar, calc_risk_at_trade_exits, pnl_explain):**
1. Branch: states is None -> build dates from RelativeDateSchedule. Else -> use states.
2. Sorts dates, adds trigger times within [start, end].
3. Deduplicates and re-sorts.
4. Branch: pnl_explain is not None -> force calc_risk_at_trade_exits=True, extract pnl_risks. Else -> pnl_risks=[].
5. Builds risks list (union of input risks, strategy risks, pnl_risks, price_measure).
6. Branch: result_ccy is not None:
   - For each risk: Branch: ParameterisedRiskMeasure -> parameterize with currency. Else -> calls raiser() (raises).
   - Branch: price_measure is ParameterisedRiskMeasure -> parameterize. Else -> calls raiser().
7. Branch: result_ccy is None -> price_risk = self.price_measure.
8. Creates BackTest object.
9. Calls _resolve_initial_portfolio.
10. Calls _build_simple_and_semi_triggers_and_actions.
11. Filters portfolio_dict and hedges to [start, end] range.
12. Calls _price_semi_det_triggers.
13. For each date: calls _process_triggers_and_actions_for_date.
    - Branch: scope (tracing) -> set tags and log.
14. Calls _calc_new_trades.
15. Calls _handle_cash.
16. Populates transaction_costs dict.
17. Returns backtest.

**_resolve_initial_portfolio(initial_portfolio, backtest, strategy_start_date, strategy_pricing_dates, holiday_calendar, duration=None):**
1. Branch: initial_portfolio is dict -> iterate sorted dates, recurse with sub-portfolio and end_date.
   - Branch: i+1 < len(sorted_dates) -> end_date = next date. Else -> end_date = last pricing date.
2. Branch: not dict (list):
   - Branch: len > 0:
     a. Clones/renames each instrument with date suffix.
     b. Creates entry CashPayment(direction=-1) and exit CashPayment.
     c. Resolves portfolio at start date.
     d. For each pricing date:
        - Branch: duration is None -> add to all dates >= start.
        - Branch: duration set -> add if d >= start and (d < duration or duration == last date).
   - Branch: len == 0 -> no-op.

**_build_simple_and_semi_triggers_and_actions(strategy, backtest, strategy_pricing_dates):**
1. For each trigger:
   - Branch: trigger.calc_type != path_dependent:
     a. Evaluates trigger on all dates, collects triggered_dates and trigger_infos.
     b. For each action:
        - Branch: action.calc_type != path_dependent:
          - Branch: action type in trigger_infos -> use direct match.
          - Branch: isinstance match found -> use that.
          - Branch: no match -> trigger_info = None.
          - Calls apply_action with all triggered_dates at once.
   - Branch: trigger.calc_type == path_dependent -> skip (handled later).

**_price_semi_det_triggers(backtest, risks) (static):**
1. Opens PricingContext, increments calc_calls.
2. For each day in portfolio_dict:
   - Branch: day is dt.date -> price portfolio.
3. For each hedge: prices scaling_portfolio on its dates.
   - Branch: trade is Portfolio -> use directly. Else -> wrap in Portfolio.

**__ensure_risk_results(dates, backtest, risks):**
1. For each date: finds trades not yet in results.
   - Branch: no results for date -> all trades are new.
   - Branch: results exist -> only trades not in portfolio.
2. Branch: port_by_date non-empty -> prices missing trades, adds results.
3. Branch: empty -> no-op.

**_process_triggers_and_actions_for_date(d, strategy, backtest, risks):**
1. For each trigger:
   - Branch: trigger is path_dependent:
     - Branch: has_triggered(d) -> for each action: ensure risk results, apply action.
   - Branch: trigger is not path_dependent:
     - For each action:
       - Branch: action is path_dependent:
         - Branch: has_triggered(d) -> ensure risk results, apply action.
2. Branch: d not in backtest.hedges -> return early.
3. For each hedge at d:
   - Branch: sp.results is None -> price with HistoricalPricingContext.
4. Branch: backtest.hedges[d] is non-empty:
   - Calls __ensure_risk_results.
   - Branch: d not in backtest.results -> return early (no risk to hedge).
   - For each hedge:
     - Computes scaling_factor = current_risk / hedge_risk * risk_percentage / 100.
     - Branch: hedge_risk == 0 -> skip (continue).
     - Branch: current_risk.unit != hedge_risk.unit -> raise RuntimeError.
     - Branch: trade is Portfolio -> deep-copy, scale, add to portfolio_dict for all dates.
     - Branch: trade is not Portfolio -> raise RuntimeError.
     - Appends entry payment. Branch: exit_payment is not None -> append exit payment.

**_calc_new_trades(backtest, risks):**
1. Opens PricingContext, increments calc_calls.
2. For each day/portfolio:
   - Branch: portfolio empty -> continue.
   - Finds trades not yet in results (by name).
   - Branch: new leaves found -> prices them.
3. Adds leaf results to backtest.

**_handle_cash(backtest, risks, price_risk, strategy_pricing_dates, strategy_end_date, initial_value, calc_risk_at_trade_exits, cash_accrual):**
1. Collects cash trades needing pricing:
   - Branch: trade is Portfolio -> expand to all_instruments. Else -> single trade.
   - Branch: effective_date <= strategy_end_date and trade not in results -> schedule calc.
   - Branch: calc_risk_at_trade_exits and direction == 1 -> add to exited_cash_trades.
2. Prices cash trades:
   - Branch: calc_risk_at_trade_exits and date in exited map -> calc exit risks.
3. Iterates sorted union of pricing dates and cash payment dates:
   - Branch: d <= strategy_end_date:
     - Branch: current_value is not None:
       - Branch: cash_accrual is None -> use raw value. Else -> get_accrued_value.
     - Branch: d in cash_payments:
       - For each payment/trade:
         - Tries cash_results, falls back to backtest.results.
         - Branch: value == {} (not found in either) -> raise RuntimeError.
         - Branch: value is not float -> raise RuntimeError.
         - Branch: d not in cash_dict -> initialize with initial_value.
         - Branch: ccy not in cash_dict[d] -> initialize to 0.
       - Updates cash_dict with cash_paid per currency.
       - Sets current_value = (cash_dict[d], d).
     - Deep-copies current_value.

## Edge Cases
- result_ccy is not None with unparameterised risk measures -> raiser() called, RuntimeError for each non-ParameterisedRiskMeasure.
- Empty initial_portfolio (len == 0) -> no-op, nothing added.
- initial_portfolio is a dict with multiple start dates -> recursive resolution.
- ExitTradeAction with priceable_names=None and no trades in portfolio -> empty removal lists.
- ExitTradeAction moving cash payments: existing payment at exit date -> nets direction instead of appending.
- RebalanceAction where new_size == current_size -> early return.
- RebalanceAction with no matching unwind payment found -> raises ValueError.
- HedgeAction where hedge_risk == 0 -> skips hedge (continue).
- HedgeAction where trade is not Portfolio -> raises RuntimeError.
- HedgeAction where current_risk.unit != hedge_risk.unit -> raises RuntimeError.
- Hedge exit_payment is None (final_date > today) -> no exit payment appended.
- AddScaledTradeAction with scaling_level as dict -> interpolated signal; date not in signal -> returns 0.
- AddScaledTradeAction NAV scaling where first_scale_factor == 0 -> returns 0 immediately.
- AddScaledTradeAction NAV scaling where scale_factor == 0 for a day -> order deleted from dict.
- Unsupported scaling type -> RuntimeError.
- Unknown action type in factory -> RuntimeError.
- _process_triggers_and_actions_for_date: date not in backtest.hedges -> early return.
- _process_triggers_and_actions_for_date: date not in backtest.results after ensure -> early return.
- _calc_new_trades: empty portfolio for a date -> continue.
- _handle_cash: cash value lookup fails -> RuntimeError with trade name and date.
- _handle_cash: value is not float -> RuntimeError.

## Bugs Found
- **Line 591**: `backtest.transaction_cost_entries[d].append(exit)` uses Python's builtin `exit` function instead of the local variable `tc_exit`. This causes the wrong object (the builtin `exit` callable) to be appended to the transaction cost entries list. Should be `backtest.transaction_cost_entries[d].append(tc_exit)`.

## Coverage Notes

**Branch counts (estimated):**
- OrderBasedActionImpl.get_base_orders_for_states: 2 branches (dated_priceables hit/miss)
- AddTradeActionImpl._raise_order: 2 branches (trigger_info None/single vs list)
- AddTradeActionImpl.apply_action: 2 branches (tc risk_calcs > 0 or not)
- AddScaledTradeActionImpl.__init__: 2 branches (scaling_level dict vs not)
- AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash: 2 branches (first_scale_factor == 0 early return, normal path)
- AddScaledTradeActionImpl._nav_scale_orders: ~6 branches (unwind_day <= today, idx+1 < len, scale_factor == 0, available_cash floor)
- AddScaledTradeActionImpl._scaling_level_for_date: 3 branches (signal not None + d in, signal not None + d not in, signal None)
- AddScaledTradeActionImpl._scale_order: 4 branches (size, NAV, risk_measure, unsupported)
- AddScaledTradeActionImpl._raise_order: 3 branches (risk_measure type, len > 1 valuations, daily_risk None vs dict)
- AddScaledTradeActionImpl.apply_action: 2 branches (trigger_info None/single vs list)
- HedgeActionImpl.get_base_orders_for_states: 1 path
- HedgeActionImpl.apply_action: ~5 branches (trigger_info type, hedge_trade is Portfolio, len(active_dates) > 0, final_date <= today)
- ExitTradeActionImpl.apply_action: ~10 branches (priceable_names None vs provided, results exist, result_indexes non-empty, cash payment netting vs append, cash_payments empty, trade not in cash payments + Portfolio vs single)
- RebalanceActionImpl.apply_action: ~4 branches (delta == 0, unwind found vs not, unwind_payment is None)
- GenericEngineActionFactory.get_action_handler: 2 branches (known vs unknown action)
- GenericEngine.supports_strategy: 2 branches (RuntimeError vs success)
- GenericEngine.__run: ~10 branches (states None, pnl_explain None, result_ccy None, tracing scope)
- GenericEngine._resolve_initial_portfolio: ~4 branches (dict vs list, len > 0, duration None, i+1 < len)
- GenericEngine._build_simple_and_semi_triggers_and_actions: ~5 branches (calc_type, action calc_type, trigger_info match by type vs isinstance vs None)
- GenericEngine._price_semi_det_triggers: 2 branches (day is dt.date, trade is Portfolio)
- GenericEngine.__ensure_risk_results: 3 branches (no results for date, trade not in results, port_by_date non-empty)
- GenericEngine._process_triggers_and_actions_for_date: ~8 branches (path_dependent trigger/action, has_triggered, hedge results None, hedges non-empty, d not in results, hedge_risk == 0, unit mismatch, trade is Portfolio, exit_payment None)
- GenericEngine._calc_new_trades: 2 branches (portfolio empty, leaves found)
- GenericEngine._handle_cash: ~10 branches (trade is Portfolio, effective_date conditions, calc_risk_at_trade_exits, current_value None, cash_accrual None, value == {}, value not float, d not in cash_dict, ccy not in cash_dict)

**Total estimated branches: ~90**

**Mocking notes:**
- PricingContext, HistoricalPricingContext: must be mocked; all pricing calls go through these context managers. Mock `__enter__`/`__exit__` and ensure `Portfolio.calc` / `Portfolio.resolve` return appropriate mock results.
- Portfolio: mock `.calc()` to return PortfolioRiskResult-like objects with subscript access by risk measure and instrument name. Mock `.resolve()`, `.scale()`, `.all_instruments`, `.__iter__`.
- Tracer / nullcontext: mock Tracer.active_span() to test tracing-enabled vs disabled paths.
- RelativeDateSchedule: mock `.apply_rule()` to return date lists for states=None path.
- Strategy: mock with triggers list, each trigger having calc_type, has_triggered(), get_trigger_times(), actions.
- BackTest: can use real object but needs mock results dict, portfolio_dict, cash_payments, hedges, transaction_cost_entries.
- TransactionCostEntry: mock or use real; need `.no_of_risk_calcs`, `.calculate_unit_cost()`, `.get_cost_by_component()`, `.get_final_cost()`, `.additional_scaling`.
- CashPayment: mock or use real; need `.trade`, `.effective_date`, `.direction`, `.cash_paid`, `.transaction_cost_entry`.
- get_final_date, interpolate_signal, make_list, map_ccy_name_to_ccy: utility functions that may need mocking for isolation.
- Instrument.clone, Instrument.name, Instrument.to_dict: mock instrument behavior for trade creation and name matching.
- The line 591 bug means the RebalanceActionImpl unwind path cannot be fully tested without fixing the bug first (appending builtin `exit` instead of `tc_exit` will not raise but produces wrong data).
