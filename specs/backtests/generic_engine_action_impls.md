# generic_engine_action_impls.py

## Summary
Implements the concrete action handler classes for the generic backtest engine. Each class translates a specific action type (AddTrade, AddScaledTrade, Hedge, ExitTrade, Rebalance, AddWeightedTrade) into portfolio mutations -- order creation, scaling, hedging, early exit, rebalancing, and weighted trade allocation -- with full transaction cost tracking and cash payment bookkeeping. All order-based handlers share a common base (`OrderBasedActionImpl`) that resolves instruments and computes final dates.

## Dependencies
- Internal: `gs_quant.backtests.actions` (Action, AddTradeAction, HedgeAction, AddTradeActionInfo, HedgeActionInfo, ExitTradeAction, ExitTradeActionInfo, RebalanceAction, RebalanceActionInfo, AddScaledTradeAction, ScalingActionType, AddScaledTradeActionInfo, AddWeightedTradeAction, AddWeightedTradeActionInfo)
- Internal: `gs_quant.backtests.action_handler` (ActionHandler)
- Internal: `gs_quant.backtests.backtest_objects` (BackTest, ScalingPortfolio, CashPayment, Hedge, TransactionCostEntry, WeightedScalingPortfolio, WeightedTrade)
- Internal: `gs_quant.backtests.backtest_utils` (make_list, get_final_date, interpolate_signal)
- Internal: `gs_quant.common` (RiskMeasure)
- Internal: `gs_quant.instrument` (Instrument)
- Internal: `gs_quant.markets` (PricingContext, HistoricalPricingContext)
- Internal: `gs_quant.markets.portfolio` (Portfolio)
- Internal: `gs_quant.risk.results` (PortfolioRiskResult)
- Internal: `gs_quant.target.measures` (ResolvedInstrumentValues)
- External: `abc` (ABCMeta)
- External: `datetime` (dt.date, dt.datetime)
- External: `collections` (defaultdict, namedtuple)
- External: `itertools` (zip_longest)
- External: `typing` (Union, Iterable, Optional, Dict, Collection)

## Type Definitions

### OrderBasedActionImpl (abstract class, ABCMeta)
Inherits: ActionHandler

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _order_valuations | `list` | `[ResolvedInstrumentValues]` | List of risk measure types used when resolving orders |

### AddTradeActionImpl (class)
Inherits: OrderBasedActionImpl

No additional fields beyond inherited.

### AddScaledTradeActionImpl (class)
Inherits: OrderBasedActionImpl

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _scaling_level_signal | `Optional[dict]` | computed | If `action.scaling_level` is a dict, the interpolated signal; otherwise `None` |

### HedgeActionImpl (class)
Inherits: OrderBasedActionImpl

No additional fields beyond inherited.

### ExitTradeActionImpl (class)
Inherits: ActionHandler

No additional fields beyond inherited.

### RebalanceActionImpl (class)
Inherits: ActionHandler

No additional fields beyond inherited.

### AddWeightedTradeActionImpl (class)
Inherits: OrderBasedActionImpl

No additional fields beyond inherited.

## Enums and Constants

None defined in this module. `ScalingActionType` is imported from `gs_quant.backtests.actions`.

## Functions/Methods

### OrderBasedActionImpl.__init__(self, action: Action)
Purpose: Initialize order valuations list and delegate to parent.

**Algorithm:**
1. Set `self._order_valuations = [ResolvedInstrumentValues]` (list containing the class itself, not an instance).
2. Call `super().__init__(action)` which stores `self._action = action`.

---

### OrderBasedActionImpl.get_base_orders_for_states(self, states: Collection[dt.date], **kwargs) -> dict
Purpose: Resolve instrument definitions for each state date by pricing them.

**Algorithm:**
1. Initialize empty `orders` dict.
2. Retrieve `dated_priceables` from `self.action` via `getattr` with default `{}`. Apply `or {}` to handle `None`.
3. Open outer `PricingContext()` (no arguments).
4. For each state `s` in `states`:
   a. Branch: `dated_priceables.get(s)` is truthy -> use it as `active_portfolio`.
   b. Branch: `dated_priceables.get(s)` is falsy -> fall back to `self.action.priceables`.
   c. Open inner `PricingContext(pricing_date=s)`.
   d. Calculate `Portfolio(active_portfolio).calc(tuple(self._order_valuations))`, store in `orders[s]`.
5. Return `orders`.

---

### OrderBasedActionImpl.get_instrument_final_date(self, inst: Instrument, order_date: dt.date, info: namedtuple) -> dt.date
Purpose: Compute the final (expiry/unwind) date for an instrument.

**Algorithm:**
1. Delegate to `get_final_date(inst, order_date, self.action.trade_duration, self.action.holiday_calendar, info)`.
2. Return the result.

---

### AddTradeActionImpl.__init__(self, action: AddTradeAction)
Purpose: Initialize with an AddTradeAction.

**Algorithm:**
1. Call `super().__init__(action)`.

---

### AddTradeActionImpl._raise_order(self, state: Union[dt.date, Iterable[dt.date]], trigger_info: Optional[Union[AddTradeActionInfo, Iterable[AddTradeActionInfo]]] = None) -> dict
Purpose: Create resolved, scaled trade orders for one or more dates.

**Algorithm:**
1. Convert `state` to a list via `make_list(state)`.
2. Branch: `trigger_info is None` OR `isinstance(trigger_info, AddTradeActionInfo)` -> replicate the single value for all states: `[trigger_info for _ in range(len(state_list))]`.
3. Branch: `trigger_info` is already an iterable -> use as-is (implicit from else path).
4. Build `ti_by_state` dict by zipping `state_list` with `trigger_info` using `zip_longest`.
5. Call `self.get_base_orders_for_states(state_list, trigger_infos=ti_by_state)` to get raw orders.
6. For each `(d, p)` in `orders.items()`:
   a. Clone each trade in `p.result()` with name suffixed by `_{d}`, wrap in `Portfolio`.
   b. Retrieve `ti = ti_by_state[d]`.
   c. Branch: `ti is None` -> call `new_port.scale(None, in_place=False)`.
   d. Branch: `ti is not None` -> call `new_port.scale(ti.scaling, in_place=False)`.
   e. Store `final_orders[d] = (scaled_portfolio, ti)`.
7. Return `final_orders`.

---

### AddTradeActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[AddTradeActionInfo, Iterable[AddTradeActionInfo]]] = None) -> BackTest
Purpose: Apply add-trade orders to the backtest: create transaction cost entries, cash payments, and populate portfolio_dict.

**Algorithm:**
1. Call `self._raise_order(state, trigger_info)` to get `orders`.
2. Initialize `current_tc_entries = []`.
3. For each `(create_date, (portfolio, info))` in `orders.items()`:
   a. For each `inst` in `portfolio.all_instruments`:
      i. Create entry `TransactionCostEntry(create_date, inst, self.action.transaction_cost)`.
      ii. Append to `current_tc_entries`.
      iii. Append `CashPayment(inst, effective_date=create_date, direction=-1, transaction_cost_entry=tc_enter)` to `backtest.cash_payments[create_date]`.
      iv. Append `tc_enter` to `backtest.transaction_cost_entries[create_date]`.
      v. Compute `final_date` via `self.get_instrument_final_date(inst, create_date, info)`.
      vi. Create exit `TransactionCostEntry(final_date, inst, self.action.transaction_cost_exit)`.
      vii. Append to `current_tc_entries`.
      viii. Append `CashPayment(inst, effective_date=final_date, transaction_cost_entry=tc_exit)` to `backtest.cash_payments[final_date]` (direction defaults to 1).
      ix. Append `tc_exit` to `backtest.transaction_cost_entries[final_date]`.
      x. Generate `backtest_states` as a generator of states `s` where `final_date > s >= create_date`.
      xi. For each such state `s`, append `inst` to `backtest.portfolio_dict[s]`.
4. Open `PricingContext(is_async=True)`:
   a. Branch: `any(tce.no_of_risk_calcs > 0 for tce in current_tc_entries)` is True -> increment `backtest.calc_calls` by 1.
   b. Branch: all `no_of_risk_calcs` are 0 -> do not increment.
   c. For each `tce` in `current_tc_entries`:
      - Add `tce.no_of_risk_calcs` to `backtest.calculations`.
      - Call `tce.calculate_unit_cost()`.
5. Return `backtest`.

---

### AddScaledTradeActionImpl.__init__(self, action: AddScaledTradeAction)
Purpose: Initialize with scaling level signal interpolation.

**Algorithm:**
1. Call `super().__init__(action)`.
2. Branch: `isinstance(self.action.scaling_level, dict)` is True -> set `self._scaling_level_signal = interpolate_signal(self.action.scaling_level)`.
3. Branch: `scaling_level` is not a dict -> set `self._scaling_level_signal = None`.

---

### AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash(portfolio, available_cash, cur_day, unscaled_prices_by_day, unscaled_entry_tces_by_day) -> float [staticmethod]
Purpose: Solve for the portfolio scaling factor such that instrument prices plus transaction costs equal available cash.

**Algorithm:**
1. Initialize `fixed_tcs = 0`, `scaling_based_tcs = 0`.
2. **First pass:** For each `inst` in `portfolio`:
   a. Call `unscaled_entry_tces_by_day[cur_day][inst].get_cost_by_component()` returning `(insed_fixed_tc, inst_scaling_tc)`.
   b. Accumulate `fixed_tcs += insed_fixed_tc`.
   c. Accumulate `scaling_based_tcs += inst_scaling_tc`.
3. Compute `first_scale_factor = (available_cash - fixed_tcs) / (unscaled_prices_by_day[cur_day].aggregate() + scaling_based_tcs)`.
4. Branch: `first_scale_factor == 0` -> return `0` immediately.
5. **Second pass:** Reset `fixed_tcs = 0`, `scaling_based_tcs = 0`.
6. For each `inst` in `portfolio`:
   a. Set `unscaled_entry_tces_by_day[cur_day][inst].additional_scaling = first_scale_factor`.
   b. Call `get_cost_by_component()` again.
   c. Re-accumulate `fixed_tcs` and `scaling_based_tcs`.
7. Compute `second_scale_factor = max(available_cash - fixed_tcs, 0) / (unscaled_prices_by_day[cur_day].aggregate() * first_scale_factor + scaling_based_tcs)`.
8. Return `first_scale_factor * second_scale_factor`.

---

### AddScaledTradeActionImpl._nav_scale_orders(self, orders, price_measure, trigger_infos) -> None
Purpose: Scale orders using NAV-based logic where available cash flows from instrument unwinds.

**Algorithm:**
1. Sort order days: `sorted_order_days = sorted(make_list(orders.keys()))`.
2. Initialize `final_days_orders = {}`, `unscaled_entry_tces_by_day = defaultdict(dict)`, `unscaled_unwind_tces_by_day = defaultdict(dict)`.
3. **Populate TCE dicts:** For each `(create_date, portfolio)` in `orders.items()`:
   a. Retrieve `info = trigger_infos[create_date]`.
   b. For each `inst` in `portfolio.all_instruments`:
      i. Create entry `TransactionCostEntry(create_date, inst, self.action.transaction_cost)`, store in `unscaled_entry_tces_by_day[create_date][inst]`.
      ii. Compute final date `d = self.get_instrument_final_date(inst, create_date, info)`.
      iii. Create exit `TransactionCostEntry(d, inst, self.action.transaction_cost_exit)`, store in `unscaled_unwind_tces_by_day[d][inst]`.
      iv. Branch: `d not in final_days_orders.keys()` -> initialize `final_days_orders[d] = []`.
      v. Append `inst` to `final_days_orders[d]`.
4. Initialize `unscaled_prices_by_day = {}`, `unscaled_unwind_prices_by_day = {}`.
5. **Async pricing block** `PricingContext(is_async=True)`:
   a. For each `(day, portfolio)` in `orders.items()`:
      - Open `PricingContext(pricing_date=day)`, calc `portfolio.calc(price_measure)`, store in `unscaled_prices_by_day[day]`.
   b. For each `(unwind_day, unwind_instruments)` in `final_days_orders.items()`:
      - Branch: `unwind_day <= dt.date.today()` -> open `PricingContext(pricing_date=unwind_day)`, calc `Portfolio(unwind_instruments).calc(price_measure)`, store in `unscaled_unwind_prices_by_day[unwind_day]`.
      - Branch: `unwind_day > dt.date.today()` -> skip (no pricing for future unwinds).
   c. For each `(day, inst_tce_map)` in `unscaled_entry_tces_by_day.items()`:
      - For each `tce`: call `tce.calculate_unit_cost()`.
   d. For each `(day, inst_tce_map)` in `unscaled_unwind_tces_by_day.items()`:
      - For each `tce`: call `tce.calculate_unit_cost()`.
6. Set `available_cash = self.action.scaling_level`.
7. Initialize `scaling_factors_by_inst = {}`, `scaling_factors_by_day = {}`.
8. **Iterate sorted order days** with index `idx`:
   a. Get `portfolio = orders[cur_day]`.
   b. Compute `scale_factor = self.__portfolio_scaling_for_available_cash(portfolio, available_cash, cur_day, unscaled_prices_by_day, unscaled_entry_tces_by_day)`.
   c. Store `scaling_factors_by_day[cur_day] = scale_factor`.
   d. For each `inst` in `portfolio`: store `scaling_factors_by_inst[inst] = scale_factor`.
   e. Set `available_cash = 0`.
   f. Branch: `idx + 1 < len(sorted_order_days)` -> `next_day = sorted_order_days[idx + 1]`.
   g. Branch: `idx + 1 >= len(sorted_order_days)` -> `break` (last order day, no more cash to compute).
   h. **Cash from unwinds:** For each `(d, p)` in `final_days_orders.items()`:
      - Branch: `cur_day < d <= next_day` -> for each `inst` in `p`:
        - `available_cash += unscaled_unwind_prices_by_day[d][inst] * scaling_factors_by_inst[inst]`.
        - Set `tce.additional_scaling = scaling_factors_by_inst[inst]`.
        - `available_cash -= unscaled_unwind_tces_by_day[d][inst].get_final_cost()`.
      - Branch: `d` outside range -> skip.
   i. Floor: `available_cash = max(available_cash, 0)`.
9. **Apply scaling:** For each `day` in `sorted_order_days`:
   a. Branch: `scaling_factors_by_day[day] == 0` -> `del orders[day]` (remove zero-scaled order).
   b. Branch: nonzero -> `orders[day].scale(scaling_factors_by_day[day])` (in-place).

---

### AddScaledTradeActionImpl._scaling_level_for_date(self, d: dt.date) -> float
Purpose: Return the scaling level for a given date, using interpolated signal if available.

**Algorithm:**
1. Branch: `self._scaling_level_signal is not None`:
   a. Branch: `d in self._scaling_level_signal` -> return `self._scaling_level_signal[d]`.
   b. Branch: `d not in self._scaling_level_signal` -> return `0`.
2. Branch: `self._scaling_level_signal is None` -> return `self.action.scaling_level`.

---

### AddScaledTradeActionImpl._scale_order(self, orders, daily_risk, price_measure, trigger_infos) -> None
Purpose: Dispatch to the appropriate scaling strategy based on scaling_type.

**Algorithm:**
1. Branch: `self.action.scaling_type == ScalingActionType.size` -> for each `(day, portfolio)` in `orders.items()`, call `portfolio.scale(self._scaling_level_for_date(day))`.
2. Branch: `self.action.scaling_type == ScalingActionType.NAV` -> call `self._nav_scale_orders(orders, price_measure, trigger_infos)`.
3. Branch: `self.action.scaling_type == ScalingActionType.risk_measure` -> for each `(day, portfolio)`, compute `scaling_factor = self._scaling_level_for_date(day) / daily_risk[day]`, call `portfolio.scale(scaling_factor)`.
4. Branch: none of the above -> raise `RuntimeError(f'Scaling Type {self.action.scaling_type} not supported by engine')`.

**Raises:** `RuntimeError` when `scaling_type` is not one of `size`, `NAV`, `risk_measure`.

---

### AddScaledTradeActionImpl._raise_order(self, state_list: Collection[dt.date], price_measure: RiskMeasure, trigger_infos: Dict[dt.date, Optional[Union[AddScaledTradeActionInfo, Iterable[AddScaledTradeActionInfo]]]]) -> dict
Purpose: Build resolved and scaled orders for all state dates.

**Algorithm:**
1. Branch: `self.action.scaling_type == ScalingActionType.risk_measure` -> append `self.action.scaling_risk` to `self._order_valuations`.
2. Call `self.get_base_orders_for_states(state_list, trigger_infos=trigger_infos)` to get raw `orders`.
3. For each `(d, res)` in `orders.items()`:
   a. Initialize `new_port = []`.
   b. Retrieve `dated_priceables` from `self.action` via `getattr` with default `{}`, apply `or {}`.
   c. Branch: `dated_priceables.get(d)` is truthy -> use as `instruments`.
   d. Branch: falsy -> use `self.action.priceables` as `instruments`.
   e. For each `inst` in `instruments`:
      - Get `new_inst = res[inst]`.
      - Branch: `len(self._order_valuations) > 1` -> extract sub-result: `new_inst = new_inst[ResolvedInstrumentValues]`.
      - Branch: single valuation -> use `new_inst` directly.
      - Rename: `new_inst.name = f'{new_inst.name}_{d}'`.
      - Append to `new_port`.
   f. Store `final_orders[d] = Portfolio(new_port)`.
4. Branch: `self.action.scaling_type == ScalingActionType.risk_measure` -> build `daily_risk = {d: res[self.action.scaling_risk].aggregate() for d, res in orders.items()}`.
5. Branch: other scaling type -> `daily_risk = None`.
6. Call `self._scale_order(final_orders, daily_risk, price_measure, trigger_infos)`.
7. Return `final_orders`.

---

### AddScaledTradeActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[AddScaledTradeActionInfo, Iterable[AddScaledTradeActionInfo]]] = None) -> BackTest
Purpose: Apply scaled trade orders to the backtest with full TC/cash payment bookkeeping.

**Algorithm:**
1. Convert `state` to list via `make_list(state)`.
2. Branch: `trigger_info is None` OR `isinstance(trigger_info, AddScaledTradeActionInfo)` -> replicate for all states.
3. Branch: trigger_info is iterable -> use as-is (implicit else).
4. Build `trigger_infos = dict(zip_longest(state_list, trigger_info))`.
5. Call `self._raise_order(state_list, backtest.price_measure, trigger_infos)` to get `orders`.
6. Initialize `current_tc_entries = []`.
7. For each `(create_date, portfolio)` in `orders.items()`:
   a. Retrieve `info = trigger_infos[create_date]`.
   b. For each `inst` in `portfolio.all_instruments`:
      i. Create entry `TransactionCostEntry(create_date, inst, self.action.transaction_cost)`.
      ii. Append to `current_tc_entries`.
      iii. Append `CashPayment(inst, effective_date=create_date, direction=-1, transaction_cost_entry=tc_enter)` to `backtest.cash_payments[create_date]`.
      iv. Append `tc_enter` to `backtest.transaction_cost_entries[create_date]`.
      v. Compute `final_date = self.get_instrument_final_date(inst, create_date, info)`.
      vi. Create exit `TransactionCostEntry(final_date, inst, self.action.transaction_cost_exit)`.
      vii. Append to `current_tc_entries`.
      viii. Append `CashPayment(inst, effective_date=final_date, transaction_cost_entry=tc_exit)` to `backtest.cash_payments[final_date]`.
      ix. Append `tc_exit` to `backtest.transaction_cost_entries[final_date]`.
      x. Generate `backtest_states` for states `s` where `final_date > s >= create_date`.
      xi. For each such state, append `inst` to `backtest.portfolio_dict[s]`.
8. Open `PricingContext(is_async=True)`:
   a. Branch: `any(tce.no_of_risk_calcs > 0 ...)` -> increment `backtest.calc_calls`.
   b. For each `tce`: add `no_of_risk_calcs` to `backtest.calculations`, call `tce.calculate_unit_cost()`.
9. Return `backtest`.

---

### HedgeActionImpl.__init__(self, action: HedgeAction)
Purpose: Initialize with a HedgeAction.

**Algorithm:**
1. Call `super().__init__(action)`.

---

### HedgeActionImpl.get_base_orders_for_states(self, states: Collection[dt.date], **kwargs) -> dict
Purpose: Resolve the hedge priceable across all state dates using historical pricing.

**Algorithm:**
1. Open `HistoricalPricingContext(dates=states, csa_term=self.action.csa_term)`.
2. Call `Portfolio(self.action.priceable).resolve(in_place=False)`.
3. Return `f.result()` (dict of date -> resolved portfolio).

Note: This overrides the parent `get_base_orders_for_states` entirely -- does not use `dated_priceables` or `_order_valuations`.

---

### HedgeActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[HedgeActionInfo, Iterable[HedgeActionInfo]]] = None) -> BackTest
Purpose: Create hedging portfolios with scaling, entry/exit payments, and TC tracking.

**Algorithm:**
1. Convert `state` to list via `make_list(state)`.
2. Branch: `trigger_info is None` OR `isinstance(trigger_info, HedgeActionInfo)` -> replicate for all states.
3. Branch: trigger_info is iterable -> use as-is.
4. Build `trigger_infos = dict(zip_longest(state_list, trigger_info))`.
5. Increment `backtest.calc_calls += 1`.
6. Increment `backtest.calculations += len(state_list)`.
7. Call `self.get_base_orders_for_states(state_list, trigger_infos=trigger_infos)` to get `orders`.
8. Initialize `current_tc_entries = []`.
9. For each `(create_date, portfolio)` in `orders.items()`:
   a. Retrieve `info = trigger_infos[create_date]`.
   b. Get `hedge_trade = portfolio.priceables[0]`.
   c. Rename: `hedge_trade.name = f'{hedge_trade.name}_{create_date.strftime("%Y-%m-%d")}'`.
   d. Branch: `isinstance(hedge_trade, Portfolio)` -> for each sub-instrument, rename: `f'{hedge_trade.name}_{instrument.name}'`.
   e. Branch: `hedge_trade` is not a Portfolio -> no sub-instrument renaming.
   f. Compute `final_date = self.get_instrument_final_date(hedge_trade, create_date, info)`.
   g. Filter `active_dates = [s for s in backtest.states if create_date <= s < final_date]`.
   h. Branch: `len(active_dates) > 0`:
      i. Create `ScalingPortfolio(trade=hedge_trade, dates=active_dates, risk=self.action.risk, csa_term=self.action.csa_term, risk_transformation=self.action.risk_transformation, risk_percentage=self.action.risk_percentage)`.
      ii. Create entry `TransactionCostEntry(create_date, hedge_trade, self.action.transaction_cost)`.
      iii. Append to `current_tc_entries`.
      iv. Create `entry_payment = CashPayment(trade=hedge_trade, effective_date=create_date, direction=-1, transaction_cost_entry=tc_enter)`.
      v. Append `tc_enter` to `backtest.transaction_cost_entries[create_date]`.
      vi. Create exit `TransactionCostEntry(final_date, hedge_trade, self.action.transaction_cost_exit)`.
      vii. Append to `current_tc_entries`.
      viii. Branch: `final_date <= dt.date.today()` -> create `exit_payment = CashPayment(trade=hedge_trade, effective_date=final_date, transaction_cost_entry=tc_exit)`.
      ix. Branch: `final_date > dt.date.today()` -> `exit_payment = None`.
      x. Append `tc_exit` to `backtest.transaction_cost_entries[final_date]`.
      xi. Create `Hedge(scaling_portfolio=scaling_portfolio, entry_payment=entry_payment, exit_payment=exit_payment)`.
      xii. Append hedge to `backtest.hedges[create_date]`.
   i. Branch: `len(active_dates) == 0` -> skip (no hedge created for this date).
10. Open `PricingContext(is_async=True)`:
    a. Branch: `any(tce.no_of_risk_calcs > 0 ...)` -> increment `backtest.calc_calls`.
    b. For each `tce`: add `no_of_risk_calcs` to `backtest.calculations`, call `tce.calculate_unit_cost()`.
11. Return `backtest`.

---

### ExitTradeActionImpl.__init__(self, action: ExitTradeAction)
Purpose: Initialize with an ExitTradeAction.

**Algorithm:**
1. Call `super().__init__(action)`.

---

### ExitTradeActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[ExitTradeActionInfo, Iterable[ExitTradeActionInfo]]] = None) -> BackTest
Purpose: Remove trades from the backtest portfolio at future dates and relocate cash payments to the exit date.

**Algorithm:**
1. For each `s` in `make_list(state)`:
   a. Initialize `trades_to_remove = []`.
   b. Branch: `self.action.priceable_names is None` -> collect `current_trade_names` as names of all instruments in `backtest.portfolio_dict[s]`.
   c. Branch: `self.action.priceable_names` is not None -> `current_trade_names` is not set (not needed).
   d. Filter `fut_dates` = dates `d` in `backtest.states` where `d >= s` and `type(d) is dt.date`.
   e. **Remove trades from future dates:** For each `port_date` in `fut_dates`:
      i. Initialize `res_fut = []`, `res_futures = []`.
      ii. Get `pos_fut` = list of all instruments in `backtest.portfolio_dict[port_date]`.
      iii. Branch: `backtest.results[port_date]` is truthy -> set `res_fut` = list of result portfolio instruments, `res_futures` = list of result futures.
      iv. Branch: `backtest.results[port_date]` is falsy -> `res_fut` and `res_futures` remain empty.
      v. Branch: `self.action.priceable_names` is truthy:
         - Build `port_indexes_to_remove`: indices where instrument name's last `_`-segment parses as a date `<= s` AND second-to-last segment is in `self.action.priceable_names`.
         - Build `result_indexes_to_remove` with same logic on `res_fut`.
      vi. Branch: `self.action.priceable_names` is falsy:
         - Build `port_indexes_to_remove`: indices where `x.name in current_trade_names`.
         - Build `result_indexes_to_remove` with same logic on `res_fut`.
      vii. For each index in `port_indexes_to_remove` (sorted descending):
         - Branch: `pos_fut[index].name not in trades_to_remove` (by name) -> append the instrument to `trades_to_remove`.
         - Delete `pos_fut[index]`.
      viii. For each index in `result_indexes_to_remove` (sorted descending):
         - Delete `res_fut[index]` and `res_futures[index]`.
      ix. Rebuild: `backtest.portfolio_dict[port_date] = Portfolio(tuple(pos_fut))`.
      x. Branch: `result_indexes_to_remove` is non-empty -> rebuild `backtest.results[port_date] = PortfolioRiskResult(Portfolio(res_fut), backtest.results[port_date].risk_measures, res_futures)`.
      xi. Branch: `result_indexes_to_remove` is empty -> no result modification.
   f. **Relocate cash payments:** For each `(cp_date, cp_list)` in `list(backtest.cash_payments.items())`:
      i. Branch: `cp_date > s`:
         - Build `indexes_to_remove`: indices where `cp.trade.name` is in the names of `trades_to_remove`.
         - For each index (sorted descending):
           - Get `cp = cp_list[index]`.
           - Find `prev_pos` = indices of existing cash payments at date `s` where `cp.trade.name == x.trade.name`.
           - Branch: `prev_pos` is non-empty -> net out: `backtest.cash_payments[s][prev_pos[0]].direction += cp.direction`.
           - Branch: `prev_pos` is empty -> set `cp.effective_date = s`, append `cp` to `backtest.cash_payments[s]`.
           - Move TC entry: append `cp.transaction_cost_entry` to `backtest.transaction_cost_entries[s]`.
           - Remove from original: `backtest.transaction_cost_entries[cp_date].remove(cp.transaction_cost_entry)`.
           - Update date: `cp.transaction_cost_entry.date = s`.
           - Delete `backtest.cash_payments[cp_date][index]`.
      ii. Branch: `cp_date <= s` -> skip.
      iii. Branch: `not backtest.cash_payments[cp_date]` (list now empty) -> `del backtest.cash_payments[cp_date]`.
   g. **Create missing exit payments:** For each `trade` in `trades_to_remove`:
      i. Branch: `trade.name not in [x.trade.name for x in backtest.cash_payments[s]]` (no payment at exit date):
         - Branch: `isinstance(trade, Portfolio)` -> `trade_instruments = set(t.to_dict() for t in trade.all_instruments)`.
         - Branch: not Portfolio -> `trade_instruments = {trade.to_dict()}`.
         - Find matching `trade_tce` in `backtest.transaction_cost_entries[s]` where the set of `i.to_dict()` for instruments matches `trade_instruments`.
         - Set `tce = trade_tce[0] if trade_tce else None`.
         - Append `CashPayment(trade, effective_date=s, transaction_cost_entry=tce)` to `backtest.cash_payments[s]`.
      ii. Branch: trade already has a payment at `s` -> skip.
2. Return `backtest`.

---

### RebalanceActionImpl.__init__(self, action: RebalanceAction)
Purpose: Initialize with a RebalanceAction.

**Algorithm:**
1. Call `super().__init__(action)`.

---

### RebalanceActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[RebalanceActionInfo, Iterable[RebalanceActionInfo]]] = None) -> BackTest
Purpose: Rebalance a specific trade to a new size, creating a delta position with entry/exit payments.

**Algorithm:**
1. Call `self.action.method(state, backtest, trigger_info)` to get `new_size`.
2. Initialize `current_size = 0`.
3. For each `trade` in `backtest.portfolio_dict[state]`:
   a. Branch: `self.action.priceable.name.split('_')[-1] in trade.name` -> accumulate `current_size += getattr(trade, self.action.size_parameter)`.
   b. Branch: name not matched -> skip.
4. Branch: `new_size - current_size == 0` -> return `backtest` early (no rebalancing needed).
5. Clone: `pos = self.action.priceable.clone(**{self.action.size_parameter: new_size - current_size, 'name': f'{self.action.priceable.name}_{state}'})`.
6. Initialize `current_tc_entries = []`.
7. Create entry `TransactionCostEntry(state, pos, self.action.transaction_cost)`. Append to `current_tc_entries`.
8. Append `CashPayment(pos, effective_date=state, direction=-1, transaction_cost_entry=tc_enter)` to `backtest.cash_payments[state]`.
9. Append `tc_enter` to `backtest.transaction_cost_entries[state]`.
10. Set `unwind_payment = None`.
11. Get `cash_payment_dates = backtest.cash_payments.keys()`.
12. **Search for matching unwind payment (reverse chronological):** For each `d` in `reversed(sorted(cash_payment_dates))`:
    a. For each `cp` in `backtest.cash_payments[d]`:
       - Branch: `self.action.priceable.name.split('_')[-1] in cp.trade.name` AND `cp.direction == 1`:
         - Create exit `TransactionCostEntry(d, pos, self.action.transaction_cost_exit)`. Append to `current_tc_entries`.
         - Create `unwind_payment = CashPayment(pos, effective_date=d, transaction_cost_entry=tc_exit)`.
         - Append `unwind_payment` to `backtest.cash_payments[d]`.
         - Append `tc_exit` to `backtest.transaction_cost_entries[d]`.
         - `break` inner loop.
    b. Branch: `unwind_payment` is truthy -> `break` outer loop.
13. Branch: `unwind_payment is None` -> raise `ValueError("Found no final cash payment to rebalance for trade.")`.
14. For each `s` in `backtest.states`:
    a. Branch: `unwind_payment.effective_date > s >= state` -> append `pos` to `backtest.portfolio_dict[s]`.
15. Open `PricingContext(is_async=True)`:
    a. Branch: `any(tce.no_of_risk_calcs > 0 ...)` -> increment `backtest.calc_calls`.
    b. For each `tce`: add `no_of_risk_calcs` to `backtest.calculations`, call `tce.calculate_unit_cost()`.
16. Return `backtest`.

**Raises:** `ValueError` when no matching unwind payment (direction == 1) is found.

---

### AddWeightedTradeActionImpl.__init__(self, action: AddWeightedTradeAction)
Purpose: Initialize with an AddWeightedTradeAction.

**Algorithm:**
1. Call `super().__init__(action)`.

---

### AddWeightedTradeActionImpl.get_base_orders_for_states(self, states: Collection[dt.date], **kwargs) -> dict
Purpose: Resolve priceables across all state dates using historical pricing.

**Algorithm:**
1. Open `HistoricalPricingContext(dates=states)` (no csa_term).
2. Call `Portfolio(self.action.priceables).resolve(in_place=False)`.
3. Return `f.result()`.

Note: Overrides parent -- does not use `dated_priceables` or `_order_valuations`.

---

### AddWeightedTradeActionImpl.apply_action(self, state: Union[dt.date, Iterable[dt.date]], backtest: BackTest, trigger_info: Optional[Union[AddWeightedTradeActionInfo, Iterable[AddWeightedTradeActionInfo]]] = None) -> BackTest
Purpose: Create weighted trade portfolios with risk-based scaling, entry/exit payments, and TC tracking.

**Algorithm:**
1. Convert `state` to list via `make_list(state)`.
2. Branch: `trigger_info is None` OR `isinstance(trigger_info, AddWeightedTradeActionInfo)` -> replicate for all states.
3. Branch: iterable -> use as-is.
4. Build `trigger_infos = dict(zip_longest(state_list, trigger_info))`.
5. Increment `backtest.calc_calls += 1`.
6. Increment `backtest.calculations += len(state_list)`.
7. Call `self.get_base_orders_for_states(state_list, trigger_infos=trigger_infos)` to get `orders`.
8. Initialize `current_tc_entries = []`.
9. For each `(create_date, portfolio)` in `orders.items()`:
   a. Retrieve `info = trigger_infos[create_date]`.
   b. Get `instruments = portfolio.priceables`.
   c. Branch: `not instruments` (empty) -> `continue` to next date.
   d. Branch: instruments non-empty:
      - Clone and rename each instrument with date suffix `_{create_date.strftime("%Y-%m-%d")}`.
      - Wrap in `weighted_portfolio = Portfolio(renamed_instruments)`.
      - Compute `final_date = self.get_instrument_final_date(renamed_instruments[0], create_date, info)`.
      - Filter `active_dates = [s for s in backtest.states if create_date <= s < final_date]`.
      - Branch: `len(active_dates) > 0`:
        i. Create `WeightedScalingPortfolio(trades=weighted_portfolio, dates=active_dates, risk=self.action.scaling_risk, total_size=self.action.total_size)`.
        ii. For each `inst` in `renamed_instruments`:
           - Create entry `TransactionCostEntry(create_date, inst, self.action.transaction_cost)`. Append to `current_tc_entries`.
           - Create `entry_payment = CashPayment(trade=inst, effective_date=create_date, direction=-1, transaction_cost_entry=tc_enter)`. Append to `entry_payments`.
           - Append `tc_enter` to `backtest.transaction_cost_entries[create_date]`.
           - Create exit `TransactionCostEntry(final_date, inst, self.action.transaction_cost_exit)`. Append to `current_tc_entries`.
           - Branch: `final_date <= dt.date.today()` -> `exit_payment = CashPayment(trade=inst, effective_date=final_date, transaction_cost_entry=tc_exit)`.
           - Branch: `final_date > dt.date.today()` -> `exit_payment = None`.
           - Append `exit_payment` to `exit_payments`.
           - Append `tc_exit` to `backtest.transaction_cost_entries[final_date]`.
        iii. Create `WeightedTrade(scaling_portfolio=scaling_portfolio, entry_payments=entry_payments, exit_payments=exit_payments)`.
        iv. Append to `backtest.weighted_trades[create_date]`.
      - Branch: `len(active_dates) == 0` -> skip (no weighted trade created).
10. Open `PricingContext(is_async=True)`:
    a. Branch: `any(tce.no_of_risk_calcs > 0 ...)` -> increment `backtest.calc_calls`.
    b. For each `tce`: add `no_of_risk_calcs` to `backtest.calculations`, call `tce.calculate_unit_cost()`.
11. Return `backtest`.

## State Mutation

- `self._order_valuations`: Initialized in `OrderBasedActionImpl.__init__`. Mutated in `AddScaledTradeActionImpl._raise_order` when `scaling_type == risk_measure` (appends `scaling_risk`). This is cumulative and not reset, so calling `_raise_order` multiple times appends duplicates.
- `self._scaling_level_signal`: Set once in `AddScaledTradeActionImpl.__init__`, never mutated afterward.
- `backtest.cash_payments`: Mutated by all `apply_action` methods -- entries appended per create_date and final_date. `ExitTradeActionImpl` also deletes, moves, and nets out payments.
- `backtest.transaction_cost_entries`: Mutated by all `apply_action` methods -- entries appended per date. `ExitTradeActionImpl` also moves entries between dates.
- `backtest.portfolio_dict`: Mutated by `AddTradeActionImpl`, `AddScaledTradeActionImpl`, `RebalanceActionImpl` (instruments appended for active dates). `ExitTradeActionImpl` rebuilds portfolios at future dates.
- `backtest.results`: Mutated only by `ExitTradeActionImpl` -- rebuilds `PortfolioRiskResult` after removing exited instruments.
- `backtest.hedges`: Mutated by `HedgeActionImpl` -- appends `Hedge` objects per create_date.
- `backtest.weighted_trades`: Mutated by `AddWeightedTradeActionImpl` -- appends `WeightedTrade` objects per create_date.
- `backtest.calc_calls`: Incremented by all `apply_action` methods when any TCE has risk calcs. Also unconditionally incremented by `HedgeActionImpl` and `AddWeightedTradeActionImpl`.
- `backtest.calculations`: Incremented by all `apply_action` methods.
- `orders` dict: Mutated in-place by `_nav_scale_orders` (deletes zero-scaled entries, calls `scale` on portfolios).
- `unscaled_entry_tces_by_day` TCE objects: `additional_scaling` field mutated by `__portfolio_scaling_for_available_cash`.
- `unscaled_unwind_tces_by_day` TCE objects: `additional_scaling` field mutated in the unwind cash calculation loop.
- `TransactionCostEntry.date`: Mutated by `ExitTradeActionImpl` when relocating TC entries.
- `CashPayment.effective_date`: Mutated by `ExitTradeActionImpl` when moving payments to exit date.
- `CashPayment.direction`: Mutated by `ExitTradeActionImpl` when netting out existing payments.
- Thread safety: No thread safety mechanisms. All operations assume single-threaded execution within the backtest loop.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `AddScaledTradeActionImpl._scale_order` | When `scaling_type` is not `size`, `NAV`, or `risk_measure` |
| `ValueError` | `RebalanceActionImpl.apply_action` | When no matching unwind cash payment with `direction == 1` is found |

## Edge Cases

- `AddTradeActionImpl._raise_order` with `trigger_info=None`: All states get `None` as trigger info, causing `scale(None)` which relies on Portfolio.scale handling None.
- `AddScaledTradeActionImpl.__init__` with `scaling_level` as a dict: Interpolated signal may not contain all backtest dates; missing dates return `0` from `_scaling_level_for_date`.
- `AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash` with `first_scale_factor == 0`: Returns 0 early, avoiding division by zero in the second pass.
- `AddScaledTradeActionImpl._nav_scale_orders` where `scale_factor == 0` for a day: Order for that day is deleted from the dict, producing no trade.
- `AddScaledTradeActionImpl._nav_scale_orders` where `unwind_day > dt.date.today()`: Unwind prices are not calculated; if referenced later in available_cash computation, will cause a `KeyError`.
- `AddScaledTradeActionImpl._raise_order` appends to `_order_valuations` without checking for duplicates: Repeated calls for `risk_measure` scaling type will keep appending `scaling_risk`.
- `HedgeActionImpl.apply_action` with `hedge_trade` that is a `Portfolio`: Sub-instruments get renamed with the parent hedge name prefix.
- `HedgeActionImpl.apply_action` with `final_date > dt.date.today()`: Exit payment is `None`, so the Hedge has no exit payment.
- `HedgeActionImpl.apply_action` where no dates fall in `[create_date, final_date)`: No hedge is created for that order (silently skipped).
- `ExitTradeActionImpl.apply_action` with `priceable_names=None`: Collects all current trade names and removes them from all future dates.
- `ExitTradeActionImpl.apply_action` trade name parsing: Assumes names follow `<prefix>_<name>_<YYYY-MM-DD>` format. Malformed names will raise `ValueError` from `strptime`.
- `ExitTradeActionImpl.apply_action` netting cash payments: When an existing payment at exit date matches by trade name, directions are summed (netted), potentially producing direction=0.
- `ExitTradeActionImpl.apply_action` when `backtest.cash_payments[cp_date]` becomes empty after removals: The key is deleted from the dict.
- `ExitTradeActionImpl.apply_action` with trade not in `cash_payments[s]`: Creates a new CashPayment using matched TCE, or `None` if no TCE found.
- `ExitTradeActionImpl.apply_action` with trade being a `Portfolio`: Uses `all_instruments` to get component dicts for TCE matching.
- `RebalanceActionImpl.apply_action` where `new_size == current_size`: Returns early with no modification.
- `RebalanceActionImpl.apply_action` with no matching trade name in portfolio: `current_size` stays 0, full `new_size` delta is used.
- `AddWeightedTradeActionImpl.apply_action` with empty `instruments` list: Continues to next date, no trade created.
- `AddWeightedTradeActionImpl.apply_action` with no active dates: No weighted trade created for that order date.
- All `apply_action` methods with empty `current_tc_entries`: The `any(...)` check is False, so `calc_calls` is not incremented in the async block.
- `OrderBasedActionImpl.get_base_orders_for_states` with `dated_priceables` attribute missing from action: `getattr` returns `{}`, then `or {}` also yields `{}`, so all states use `action.priceables`.
- `zip_longest` usage: If `state_list` and `trigger_info` list have different lengths, `zip_longest` pads with `None` fill values.

## Coverage Notes
- **Branch count (estimated): ~55**
  - `OrderBasedActionImpl.get_base_orders_for_states`: 2 branches (dated_priceables hit/miss)
  - `AddTradeActionImpl._raise_order`: 3 branches (trigger_info None, single, list; ti None vs not for scaling)
  - `AddTradeActionImpl.apply_action`: 2 branches (tc risk_calcs > 0 or not)
  - `AddScaledTradeActionImpl.__init__`: 2 branches (scaling_level dict vs not)
  - `AddScaledTradeActionImpl.__portfolio_scaling_for_available_cash`: 2 branches (first_scale_factor == 0 early return, normal)
  - `AddScaledTradeActionImpl._nav_scale_orders`: ~8 branches (unwind_day <= today, idx+1 < len, cur_day < d <= next_day, scale_factor == 0, available_cash floor, d not in final_days_orders)
  - `AddScaledTradeActionImpl._scaling_level_for_date`: 3 branches (signal not None + d in, signal not None + d not in, signal None)
  - `AddScaledTradeActionImpl._scale_order`: 4 branches (size, NAV, risk_measure, unsupported)
  - `AddScaledTradeActionImpl._raise_order`: 4 branches (risk_measure type, dated_priceables hit/miss, len > 1 valuations, daily_risk None vs dict)
  - `AddScaledTradeActionImpl.apply_action`: 2 branches (trigger_info None/single vs list)
  - `HedgeActionImpl.apply_action`: ~5 branches (trigger_info type, hedge_trade is Portfolio, len(active_dates) > 0, final_date <= today, tc risk calcs)
  - `ExitTradeActionImpl.apply_action`: ~12 branches (priceable_names None vs provided, results truthy, priceable_names filter logic, name not in trades_to_remove, result_indexes non-empty, cp_date > s, prev_pos non-empty, cash_payments empty after removal, trade not in cash_payments[s], trade is Portfolio)
  - `RebalanceActionImpl.apply_action`: ~5 branches (name match in portfolio, delta == 0, unwind found inner/outer, unwind_payment None, tc risk calcs)
  - `AddWeightedTradeActionImpl.apply_action`: ~5 branches (trigger_info type, instruments empty, active_dates > 0, final_date <= today, tc risk calcs)
- **Mocking notes:**
  - `PricingContext` / `HistoricalPricingContext`: Must mock `__enter__`/`__exit__`; all pricing via these context managers.
  - `Portfolio`: Mock `.calc()`, `.resolve()`, `.scale()`, `.all_instruments`, `.priceables`, `.__iter__`.
  - `TransactionCostEntry`: Mock `.no_of_risk_calcs`, `.calculate_unit_cost()`, `.get_cost_by_component()`, `.get_final_cost()`, `.additional_scaling`.
  - `CashPayment`: Mock/use real with `.trade`, `.effective_date`, `.direction`, `.transaction_cost_entry`.
  - `Instrument`: Mock `.clone()`, `.name`, `.to_dict()`.
  - `get_final_date`, `make_list`, `interpolate_signal`: Utility functions that should be mocked for unit isolation.
  - `PortfolioRiskResult`: Constructor needed for `ExitTradeActionImpl` result rebuilding.
  - `dt.date.today()`: Must be patched to control `final_date <= today` branches in `HedgeActionImpl` and `AddWeightedTradeActionImpl`.
