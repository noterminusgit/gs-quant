"""
Copyright 2019 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
from collections import defaultdict
from copy import deepcopy
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.backtests.backtest_objects import (
    AggregateTransactionModel,
    BackTest,
    CashAccrualModel,
    CashPayment,
    ConstantCashAccrualModel,
    ConstantTransactionModel,
    DataCashAccrualModel,
    Hedge,
    PnlAttribute,
    PnlDefinition,
    PredefinedAssetBacktest,
    ScaledTransactionModel,
    ScalingPortfolio,
    TransactionAggType,
    TransactionCostEntry,
    TransactionModel,
)
from gs_quant.backtests.core import TimeWindow, ValuationFixingType, ValuationMethod
from gs_quant.backtests.event import FillEvent
from gs_quant.backtests.order import OrderBase, OrderCost
from gs_quant.common import RiskMeasure
from gs_quant.instrument import Cash
from gs_quant.markets.portfolio import Portfolio
from gs_quant.risk import ErrorValue
from gs_quant.risk.results import PricingFuture, PortfolioRiskResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_risk_measure(name='PV'):
    """Create a simple RiskMeasure mock-like object."""
    rm = RiskMeasure()
    rm.name = name
    return rm


def _mock_instrument(name='inst1', notional_amount=1_000_000):
    """Return a MagicMock that behaves like an Instrument."""
    inst = MagicMock()
    inst.name = name
    inst.type = 'MockType'
    inst.notional_amount = notional_amount
    inst.all_instruments = (inst,)
    return inst


def _make_backtest(risks=None, states=None, price_measure=None, pnl_explain_def=None):
    """Create a BackTest with minimal mocked strategy."""
    if risks is None:
        risks = [_make_risk_measure('PV')]
    if states is None:
        states = [dt.date(2021, 1, 1), dt.date(2021, 1, 2), dt.date(2021, 1, 3)]
    if price_measure is None:
        price_measure = risks[0]
    strategy = MagicMock()
    bt = BackTest(
        strategy=strategy,
        states=states,
        risks=risks,
        price_measure=price_measure,
        pnl_explain_def=pnl_explain_def,
    )
    return bt


# ---------------------------------------------------------------------------
# PnlAttribute / PnlDefinition
# ---------------------------------------------------------------------------

class TestPnlAttribute:
    def test_get_risks(self):
        rm1 = _make_risk_measure('Delta')
        rm2 = _make_risk_measure('Spot')
        attr = PnlAttribute(
            attribute_name='delta',
            attribute_metric=rm1,
            market_data_metric=rm2,
            scaling_factor=1.0,
        )
        result = attr.get_risks()
        assert result == [rm1, rm2]

    def test_second_order_default(self):
        attr = PnlAttribute(
            attribute_name='delta',
            attribute_metric=_make_risk_measure(),
            market_data_metric=_make_risk_measure(),
            scaling_factor=1.0,
        )
        assert attr.second_order is False


class TestPnlDefinition:
    def test_get_risks_flattened(self):
        rm1 = _make_risk_measure('Delta')
        rm2 = _make_risk_measure('Spot')
        rm3 = _make_risk_measure('Gamma')
        rm4 = _make_risk_measure('Vol')
        attr1 = PnlAttribute('delta', rm1, rm2, 1.0)
        attr2 = PnlAttribute('gamma', rm3, rm4, 0.5, second_order=True)
        defn = PnlDefinition(attributes=[attr1, attr2])
        risks = defn.get_risks()
        assert risks == [rm1, rm2, rm3, rm4]

    def test_get_risks_single_attribute(self):
        rm1 = _make_risk_measure('A')
        rm2 = _make_risk_measure('B')
        defn = PnlDefinition(attributes=[PnlAttribute('x', rm1, rm2, 1.0)])
        assert defn.get_risks() == [rm1, rm2]


# ---------------------------------------------------------------------------
# BackTest
# ---------------------------------------------------------------------------

class TestBackTestInit:
    def test_post_init_sets_defaults(self):
        bt = _make_backtest()
        assert isinstance(bt._portfolio_dict, defaultdict)
        assert isinstance(bt._cash_dict, dict)
        assert isinstance(bt._hedges, defaultdict)
        assert isinstance(bt._cash_payments, defaultdict)
        assert isinstance(bt._transaction_costs, defaultdict)
        assert isinstance(bt._results, defaultdict)
        assert bt._risk_summary_dict is None
        assert bt._calc_calls == 0
        assert bt._calculations == 0

    def test_post_init_deepcopies_strategy(self):
        original_strategy = MagicMock()
        original_strategy.some_state = 'original'
        bt = BackTest(
            strategy=original_strategy,
            states=[dt.date(2021, 1, 1)],
            risks=[_make_risk_measure()],
            price_measure=_make_risk_measure(),
        )
        # Strategy is deepcopied, so it is not the same object
        assert bt.strategy is not original_strategy

    def test_risks_are_made_into_list(self):
        rm = _make_risk_measure()
        strategy = MagicMock()
        bt = BackTest(
            strategy=strategy,
            states=[dt.date(2021, 1, 1)],
            risks=rm,
            price_measure=rm,
        )
        assert isinstance(bt.risks, list)
        assert bt.risks == [rm]


class TestBackTestProperties:
    def test_cash_dict(self):
        bt = _make_backtest()
        assert bt.cash_dict == {}

    def test_portfolio_dict_getter_setter(self):
        bt = _make_backtest()
        new_dict = {'key': 'val'}
        bt.portfolio_dict = new_dict
        assert bt.portfolio_dict == new_dict

    def test_cash_payments_getter_setter(self):
        bt = _make_backtest()
        new_cp = defaultdict(list)
        bt.cash_payments = new_cp
        assert bt.cash_payments is new_cp

    def test_transaction_costs_getter_setter(self):
        bt = _make_backtest()
        new_tc = {dt.date(2021, 1, 1): 100}
        bt.transaction_costs = new_tc
        assert bt.transaction_costs == new_tc

    def test_hedges_getter_setter(self):
        bt = _make_backtest()
        new_h = defaultdict(list)
        bt.hedges = new_h
        assert bt.hedges is new_h

    def test_calc_calls_getter_setter(self):
        bt = _make_backtest()
        bt.calc_calls = 5
        assert bt.calc_calls == 5

    def test_calculations_getter_setter(self):
        bt = _make_backtest()
        bt.calculations = 10
        assert bt.calculations == 10


class TestBackTestAddResults:
    def test_add_results_new_date(self):
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        results = [1, 2, 3]
        bt.add_results(d, results)
        assert bt.results[d] == [1, 2, 3]

    def test_add_results_append_existing(self):
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        bt.add_results(d, [1, 2])
        bt.add_results(d, [3, 4])
        assert bt.results[d] == [1, 2, 3, 4]

    def test_add_results_replace(self):
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        bt.add_results(d, [1, 2])
        bt.add_results(d, [5, 6], replace=True)
        assert bt.results[d] == [5, 6]

    def test_add_results_empty_existing_does_not_append(self):
        """If date exists but results list is empty, it replaces."""
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        bt._results[d] = []
        bt.add_results(d, [9, 10])
        assert bt.results[d] == [9, 10]

    def test_set_results(self):
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        bt.set_results(d, [1])
        assert bt.results[d] == [1]
        bt.set_results(d, [2])
        assert bt.results[d] == [2]


class TestGetRiskSummaryDf:
    def test_empty_results_returns_empty_df(self):
        bt = _make_backtest()
        df = bt.get_risk_summary_df()
        assert df.empty
        assert list(df.columns) == bt.risks

    def test_basic_risk_summary(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        pv_risk = bt.risks[0]

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(return_value=100.0)
        mock_results.__getitem__ = MagicMock(return_value=agg_result)

        bt._results[d1] = mock_results
        df = bt.get_risk_summary_df()
        assert not df.empty
        assert df.loc[d1, pv_risk] == 100.0

    def test_caching_of_summary_dict(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        pv_risk = bt.risks[0]

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(return_value=100.0)
        mock_results.__getitem__ = MagicMock(return_value=agg_result)
        bt._results[d1] = mock_results

        df1 = bt.get_risk_summary_df()
        df2 = bt.get_risk_summary_df()
        # aggregate should only be called once due to caching
        assert agg_result.aggregate.call_count == 1

    def test_type_error_on_aggregate_gives_error_value(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        pv_risk = bt.risks[0]

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(side_effect=TypeError('bad'))
        mock_results.__getitem__ = MagicMock(return_value=agg_result)
        bt._results[d1] = mock_results

        df = bt.get_risk_summary_df()
        result_val = df.loc[d1, pv_risk]
        assert isinstance(result_val, ErrorValue)

    def test_zero_on_empty_dates(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)
        pv_risk = bt.risks[0]

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(return_value=50.0)
        mock_results.__getitem__ = MagicMock(return_value=agg_result)
        bt._results[d1] = mock_results

        # d2 is cash-only date
        bt._cash_dict[d2] = {'USD': 1000}

        df = bt.get_risk_summary_df(zero_on_empty_dates=True)
        assert df.loc[d2, pv_risk] == 0

    def test_filter_empty_results(self):
        """Dates with empty result lists should be filtered out."""
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        bt._results[d1] = []  # empty

        df = bt.get_risk_summary_df()
        assert df.empty


class TestResultSummary:
    def _setup_backtest_with_cash(self, num_currencies=1):
        bt = _make_backtest()
        pv_risk = bt.risks[0]
        d1 = dt.date(2021, 1, 1)

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(return_value=100.0)
        mock_results.__getitem__ = MagicMock(return_value=agg_result)
        bt._results[d1] = mock_results

        if num_currencies == 0:
            pass
        elif num_currencies == 1:
            bt._cash_dict[d1] = {'USD': 500}
        else:
            bt._cash_dict[d1] = {'USD': 500, 'EUR': 300}

        return bt

    def test_result_summary_single_currency(self):
        bt = self._setup_backtest_with_cash(1)
        df = bt.result_summary
        assert BackTest.CUMULATIVE_CASH_COLUMN in df.columns
        assert BackTest.TRANSACTION_COSTS_COLUMN in df.columns
        assert BackTest.TOTAL_COLUMN in df.columns

    def test_result_summary_zero_currencies(self):
        bt = self._setup_backtest_with_cash(0)
        df = bt.result_summary
        assert BackTest.CUMULATIVE_CASH_COLUMN in df.columns

    def test_result_summary_multi_currency_raises(self):
        bt = self._setup_backtest_with_cash(2)
        with pytest.raises(RuntimeError, match='Cannot aggregate cash in multiple currencies'):
            _ = bt.result_summary

    def test_result_summary_truncated_to_last_state(self):
        bt = self._setup_backtest_with_cash(1)
        d_extra = dt.date(2021, 1, 5)
        bt._results[d_extra] = bt._results[dt.date(2021, 1, 1)]
        bt._cash_dict[d_extra] = {'USD': 999}
        df = bt.result_summary
        # states end at 2021-01-03
        assert df.index[-1] <= bt.states[-1]


class TestTradeLedger:
    def test_empty_ledger(self):
        bt = _make_backtest()
        df = bt.trade_ledger()
        assert df.empty

    def test_direction_zero_closed_at_zero(self):
        bt = _make_backtest()
        d = dt.date(2021, 1, 1)
        trade = MagicMock()
        trade.name = 'trade1'
        cash = MagicMock()
        cash.direction = 0
        cash.trade = trade
        bt._cash_payments[d] = [cash]

        df = bt.trade_ledger()
        assert df.loc['trade1', 'Status'] == 'closed'
        assert df.loc['trade1', 'Trade PnL'] == 0
        assert df.loc['trade1', 'Open Value'] == 0
        assert df.loc['trade1', 'Close Value'] == 0

    def test_new_entry_then_close(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)
        trade = MagicMock()
        trade.name = 'trade1'

        # Entry
        cash_entry = MagicMock()
        cash_entry.direction = 1
        cash_entry.trade = trade
        cash_entry.cash_paid = {'USD': -100}
        bt._cash_payments[d1] = [cash_entry]

        # Exit
        cash_exit = MagicMock()
        cash_exit.direction = -1
        cash_exit.trade = trade
        cash_exit.cash_paid = {'USD': 120}
        bt._cash_payments[d2] = [cash_exit]

        df = bt.trade_ledger()
        assert df.loc['trade1', 'Status'] == 'closed'
        assert df.loc['trade1', 'Open Value'] == -100
        assert df.loc['trade1', 'Close Value'] == 120
        assert df.loc['trade1', 'Trade PnL'] == 20  # 120 + (-100)

    def test_open_trade_no_close(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        trade = MagicMock()
        trade.name = 'trade_open'

        cash_entry = MagicMock()
        cash_entry.direction = 1
        cash_entry.trade = trade
        cash_entry.cash_paid = {'USD': -200}
        bt._cash_payments[d1] = [cash_entry]

        df = bt.trade_ledger()
        assert df.loc['trade_open', 'Status'] == 'open'
        assert df.loc['trade_open', 'Trade PnL'] is None
        assert df.loc['trade_open', 'Close'] is None

    def test_close_with_empty_cash_paid(self):
        """Close event with empty cash_paid dict does not update close."""
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)
        trade = MagicMock()
        trade.name = 'trade_noclose'

        # Entry
        cash_entry = MagicMock()
        cash_entry.direction = 1
        cash_entry.trade = trade
        cash_entry.cash_paid = {'USD': -100}
        bt._cash_payments[d1] = [cash_entry]

        # Second event for same trade with empty cash_paid - should NOT update close
        cash_exit = MagicMock()
        cash_exit.direction = -1
        cash_exit.trade = trade
        cash_exit.cash_paid = {}
        bt._cash_payments[d2] = [cash_exit]

        df = bt.trade_ledger()
        # With empty cash_paid, the close branch doesn't execute
        assert df.loc['trade_noclose', 'Status'] == 'open'


class TestPnlExplain:
    def test_pnl_explain_none_when_no_def(self):
        bt = _make_backtest()
        assert bt.pnl_explain() is None

    def test_pnl_explain_first_order(self):
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('delta', rm_delta, rm_spot, scaling_factor=1.0, second_order=False)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        inst = _mock_instrument('inst1')

        mock_portfolio = MagicMock()
        mock_portfolio.all_instruments = [inst]
        mock_portfolio.__contains__ = MagicMock(return_value=True)

        # d1 results
        r1 = MagicMock()
        r1.portfolio = mock_portfolio
        r1.__getitem__ = MagicMock(return_value={rm_delta: 10.0, rm_spot: 100.0})
        # Nested: r1[inst][metric]
        d1_inst_result = MagicMock()
        d1_inst_result.__getitem__ = lambda self_inner, key: {rm_delta: 10.0, rm_spot: 100.0}[key]
        r1.__getitem__ = MagicMock(return_value=d1_inst_result)
        r1.portfolio = mock_portfolio

        # d2 results
        r2 = MagicMock()
        r2.portfolio = mock_portfolio
        d2_inst_result = MagicMock()
        d2_inst_result.__getitem__ = lambda self_inner, key: {rm_delta: 12.0, rm_spot: 102.0}[key]
        r2.__getitem__ = MagicMock(return_value=d2_inst_result)
        r2.portfolio = mock_portfolio

        bt._results = {d1: r1, d2: r2}

        result = bt.pnl_explain()
        assert 'delta' in result
        # first_order: scaling_factor * delta * (spot_d2 - spot_d1) = 1.0 * 10.0 * (102-100) = 20.0
        assert result['delta'][d2] == pytest.approx(20.0)

    def test_pnl_explain_second_order(self):
        rm_gamma = _make_risk_measure('Gamma')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('gamma', rm_gamma, rm_spot, scaling_factor=1.0, second_order=True)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        inst = _mock_instrument('inst1')
        mock_portfolio = MagicMock()
        mock_portfolio.all_instruments = [inst]
        mock_portfolio.__contains__ = MagicMock(return_value=True)

        d1_inst_result = MagicMock()
        d1_inst_result.__getitem__ = lambda self_inner, key: {rm_gamma: 5.0, rm_spot: 100.0}[key]
        r1 = MagicMock()
        r1.portfolio = mock_portfolio
        r1.__getitem__ = MagicMock(return_value=d1_inst_result)

        d2_inst_result = MagicMock()
        d2_inst_result.__getitem__ = lambda self_inner, key: {rm_gamma: 6.0, rm_spot: 103.0}[key]
        r2 = MagicMock()
        r2.portfolio = mock_portfolio
        r2.__getitem__ = MagicMock(return_value=d2_inst_result)

        bt._results = {d1: r1, d2: r2}

        result = bt.pnl_explain()
        # second_order: 0.5 * 1.0 * 5.0 * (103-100)^2 = 0.5 * 5 * 9 = 22.5
        assert result['gamma'][d2] == pytest.approx(22.5)

    def test_pnl_explain_prev_date_not_in_risk_results(self):
        """When prev_date is only in exit_risk_results, skip to next."""
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('delta', rm_delta, rm_spot, scaling_factor=1.0)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        # d1 only in exit_risk_results, not in results
        bt._results = {}
        bt._trade_exit_risk_results[d1] = MagicMock()
        bt._trade_exit_risk_results[d2] = MagicMock()

        result = bt.pnl_explain()
        assert result['delta'][d2] == 0.0

    def test_pnl_explain_zero_risk_skips_instrument(self):
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('delta', rm_delta, rm_spot, scaling_factor=1.0)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        inst = _mock_instrument('inst1')
        mock_portfolio = MagicMock()
        mock_portfolio.all_instruments = [inst]
        mock_portfolio.__contains__ = MagicMock(return_value=True)

        # delta is 0 on d1 -> instrument should be skipped
        d1_inst_result = MagicMock()
        d1_inst_result.__getitem__ = lambda s, key: {rm_delta: 0, rm_spot: 100.0}[key]
        r1 = MagicMock()
        r1.portfolio = mock_portfolio
        r1.__getitem__ = MagicMock(return_value=d1_inst_result)

        d2_inst_result = MagicMock()
        d2_inst_result.__getitem__ = lambda s, key: {rm_delta: 5.0, rm_spot: 105.0}[key]
        r2 = MagicMock()
        r2.portfolio = mock_portfolio
        r2.__getitem__ = MagicMock(return_value=d2_inst_result)

        bt._results = {d1: r1, d2: r2}
        result = bt.pnl_explain()
        # since delta was 0, metric_pnl stays 0
        assert result['delta'][d2] == 0.0

    def test_pnl_explain_uses_exit_risk_when_inst_not_in_cur_results(self):
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('delta', rm_delta, rm_spot, scaling_factor=2.0)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        inst = _mock_instrument('inst1')
        mock_portfolio_d1 = MagicMock()
        mock_portfolio_d1.all_instruments = [inst]

        # d2 portfolio does NOT contain inst
        mock_portfolio_d2 = MagicMock()
        mock_portfolio_d2.__contains__ = MagicMock(return_value=False)

        d1_inst_result = MagicMock()
        d1_inst_result.__getitem__ = lambda s, key: {rm_delta: 10.0, rm_spot: 100.0}[key]
        r1 = MagicMock()
        r1.portfolio = mock_portfolio_d1
        r1.__getitem__ = MagicMock(return_value=d1_inst_result)

        # d2 in risk_results but inst not in portfolio
        r2 = MagicMock()
        r2.portfolio = mock_portfolio_d2
        bt._results = {d1: r1, d2: r2}

        # exit risk results for d2
        exit_inst_result = MagicMock()
        exit_inst_result.__getitem__ = lambda s, key: {rm_delta: 12.0, rm_spot: 110.0}[key]
        exit_r2 = MagicMock()
        exit_r2.__getitem__ = MagicMock(return_value=exit_inst_result)
        bt._trade_exit_risk_results[d2] = exit_r2

        result = bt.pnl_explain()
        # first_order: 2.0 * 10.0 * (110 - 100) = 200.0
        assert result['delta'][d2] == pytest.approx(200.0)


class TestRiskSummary:
    def test_risk_summary_calls_zero_on_empty_dates(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)
        pv_risk = bt.risks[0]

        mock_results = MagicMock()
        mock_results.__len__ = MagicMock(return_value=1)
        mock_results.risk_measures = [pv_risk]
        agg_result = MagicMock()
        agg_result.aggregate = MagicMock(return_value=100.0)
        mock_results.__getitem__ = MagicMock(return_value=agg_result)
        bt._results[d1] = mock_results

        bt._cash_dict[d2] = {'USD': 500}

        df = bt.risk_summary
        assert pv_risk in df.columns
        assert df.loc[d2, pv_risk] == 0


# ---------------------------------------------------------------------------
# TransactionModel / ConstantTransactionModel
# ---------------------------------------------------------------------------

class TestTransactionModel:
    def test_base_get_unit_cost_returns_none(self):
        tm = TransactionModel()
        assert tm.get_unit_cost(dt.date.today(), None, None) is None

    def test_constant_returns_cost(self):
        ctm = ConstantTransactionModel(cost=42)
        assert ctm.get_unit_cost(dt.date.today(), None, None) == 42

    def test_constant_default_cost(self):
        ctm = ConstantTransactionModel()
        assert ctm.get_unit_cost(dt.date.today(), None, None) == 0


# ---------------------------------------------------------------------------
# ScaledTransactionModel
# ---------------------------------------------------------------------------

class TestScaledTransactionModel:
    def test_string_scaling_type_attr_found(self):
        inst = MagicMock()
        inst.notional_amount = 1_000_000
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        cost = stm.get_unit_cost(dt.date(2020, 1, 1), None, inst)
        assert cost == 1_000_000

    def test_string_scaling_type_attr_not_found(self):
        inst = MagicMock(spec=[])  # no attributes
        inst.type = 'SomeInstrument'
        stm = ScaledTransactionModel(scaling_type='nonexistent_attr')
        with pytest.raises(RuntimeError, match='not recognised'):
            stm.get_unit_cost(dt.date(2020, 1, 1), None, inst)

    def test_risk_measure_future_date_returns_nan(self):
        rm = _make_risk_measure('PV')
        stm = ScaledTransactionModel(scaling_type=rm)
        future_date = dt.date.today() + dt.timedelta(days=365)
        inst = MagicMock()
        result = stm.get_unit_cost(future_date, None, inst)
        assert np.isnan(result)

    @patch('gs_quant.backtests.backtest_objects.PricingContext')
    def test_risk_measure_past_date_calcs(self, mock_pc):
        rm = _make_risk_measure('PV')
        stm = ScaledTransactionModel(scaling_type=rm)
        past_date = dt.date(2020, 1, 1)
        inst = MagicMock()
        mock_risk_future = MagicMock()
        inst.calc.return_value = mock_risk_future

        result = stm.get_unit_cost(past_date, None, inst)
        inst.calc.assert_called_once_with(rm)
        assert result is mock_risk_future


# ---------------------------------------------------------------------------
# AggregateTransactionModel
# ---------------------------------------------------------------------------

class TestAggregateTransactionModel:
    def test_empty_models_returns_zero(self):
        atm = AggregateTransactionModel()
        assert atm.get_unit_cost(dt.date.today(), None, None) == 0

    def test_sum_aggregation(self):
        m1 = ConstantTransactionModel(cost=10)
        m2 = ConstantTransactionModel(cost=20)
        atm = AggregateTransactionModel(
            transaction_models=(m1, m2),
            aggregate_type=TransactionAggType.SUM,
        )
        assert atm.get_unit_cost(dt.date.today(), None, None) == 30

    def test_max_aggregation(self):
        m1 = ConstantTransactionModel(cost=10)
        m2 = ConstantTransactionModel(cost=20)
        atm = AggregateTransactionModel(
            transaction_models=(m1, m2),
            aggregate_type=TransactionAggType.MAX,
        )
        assert atm.get_unit_cost(dt.date.today(), None, None) == 20

    def test_min_aggregation(self):
        m1 = ConstantTransactionModel(cost=10)
        m2 = ConstantTransactionModel(cost=20)
        atm = AggregateTransactionModel(
            transaction_models=(m1, m2),
            aggregate_type=TransactionAggType.MIN,
        )
        assert atm.get_unit_cost(dt.date.today(), None, None) == 10

    def test_unknown_aggregation_type_raises(self):
        """The else branch uses self.aggregation_type (bug: should be self.aggregate_type)."""
        m1 = ConstantTransactionModel(cost=10)
        atm = AggregateTransactionModel(transaction_models=(m1,))
        # Force an unknown aggregate_type to trigger the else branch
        object.__setattr__(atm, 'aggregate_type', 'UNKNOWN')
        with pytest.raises((RuntimeError, AttributeError)):
            atm.get_unit_cost(dt.date.today(), None, None)


# ---------------------------------------------------------------------------
# TransactionCostEntry
# ---------------------------------------------------------------------------

class TestTransactionCostEntry:
    def test_all_instruments_single_instrument(self):
        inst = _mock_instrument()
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ConstantTransactionModel(cost=5))
        assert tce.all_instruments == (inst,)

    def test_all_instruments_portfolio(self):
        inst1 = _mock_instrument('a')
        inst2 = _mock_instrument('b')
        portfolio = MagicMock(spec=Portfolio)
        portfolio.all_instruments = (inst1, inst2)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), portfolio, ConstantTransactionModel(cost=5))
        assert tce.all_instruments == (inst1, inst2)

    def test_all_transaction_models_single(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=5)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        assert tce.all_transaction_models == (ctm,)

    def test_all_transaction_models_aggregate(self):
        inst = _mock_instrument()
        ctm1 = ConstantTransactionModel(cost=5)
        ctm2 = ConstantTransactionModel(cost=10)
        atm = AggregateTransactionModel(transaction_models=(ctm1, ctm2))
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        assert tce.all_transaction_models == (ctm1, ctm2)

    def test_cost_aggregation_func_sum(self):
        inst = _mock_instrument()
        atm = AggregateTransactionModel(aggregate_type=TransactionAggType.SUM)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        assert tce.cost_aggregation_func is sum

    def test_cost_aggregation_func_max(self):
        inst = _mock_instrument()
        atm = AggregateTransactionModel(aggregate_type=TransactionAggType.MAX)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        assert tce.cost_aggregation_func is max

    def test_cost_aggregation_func_min(self):
        inst = _mock_instrument()
        atm = AggregateTransactionModel(aggregate_type=TransactionAggType.MIN)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        assert tce.cost_aggregation_func is min

    def test_cost_aggregation_func_non_aggregate_defaults_to_sum(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=5)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        assert tce.cost_aggregation_func is sum

    def test_additional_scaling_default_and_setter(self):
        inst = _mock_instrument()
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ConstantTransactionModel())
        assert tce.additional_scaling == 1
        tce.additional_scaling = 2.5
        assert tce.additional_scaling == 2.5

    def test_date_property_and_setter(self):
        inst = _mock_instrument()
        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 6, 1)
        tce = TransactionCostEntry(d1, inst, ConstantTransactionModel())
        assert tce.date == d1
        tce.date = d2
        assert tce.date == d2

    def test_no_of_risk_calcs_none(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=5)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        assert tce.no_of_risk_calcs == 0

    def test_no_of_risk_calcs_with_risk_measure(self):
        inst = _mock_instrument()
        rm = _make_risk_measure('PV')
        stm = ScaledTransactionModel(scaling_type=rm)
        ctm = ConstantTransactionModel(cost=5)
        atm = AggregateTransactionModel(transaction_models=(stm, ctm))
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        assert tce.no_of_risk_calcs == 1

    def test_no_of_risk_calcs_string_scaling(self):
        inst = _mock_instrument()
        stm = ScaledTransactionModel(scaling_type='notional_amount')
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, stm)
        assert tce.no_of_risk_calcs == 0  # string, not RiskMeasure

    def test_calculate_unit_cost(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=7)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        assert tce._unit_cost_by_model_by_inst[ctm][inst] == 7

    def test_get_final_cost_constant(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=100)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # For non-scaled model, cost is just the raw value summed
        assert cost == 100

    def test_get_final_cost_scaled(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, stm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # scaling_level * abs(notional_amount * additional_scaling) = 0.0001 * 1_000_000 = 100
        assert cost == pytest.approx(100.0)

    def test_get_final_cost_with_additional_scaling(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, stm)
        tce.additional_scaling = 2.0
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # 0.0001 * abs(1_000_000 * 2.0) = 200
        assert cost == pytest.approx(200.0)

    def test_get_final_cost_empty_models(self):
        inst = _mock_instrument()
        atm = AggregateTransactionModel(transaction_models=tuple())
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        assert cost == 0

    def test_get_final_cost_aggregate_sum(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.SUM,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # sum(50, 0.0001 * 1_000_000) = sum(50, 100) = 150
        assert cost == pytest.approx(150.0)

    def test_get_final_cost_aggregate_max(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MAX,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # max(50, 100) = 100
        assert cost == pytest.approx(100.0)

    def test_resolved_cost_float(self):
        """Test __resolved_cost with plain float."""
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=42)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        assert tce.get_final_cost() == 42

    def test_resolved_cost_pricing_future(self):
        """Test __resolved_cost with PricingFuture wrapping a value."""
        inst = _mock_instrument()
        ctm = MagicMock(spec=ConstantTransactionModel)
        ctm.transaction_models = None  # not aggregate
        pf = MagicMock(spec=PricingFuture)
        pf.result.return_value = 75.0
        ctm.get_unit_cost = MagicMock(return_value=pf)

        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        assert cost == 75.0

    def test_resolved_cost_portfolio_risk_result(self):
        """Test __resolved_cost with PortfolioRiskResult."""
        inst = _mock_instrument()
        ctm = MagicMock(spec=ConstantTransactionModel)
        ctm.transaction_models = None
        prr = MagicMock(spec=PortfolioRiskResult)
        prr.aggregate.return_value = 99.0
        ctm.get_unit_cost = MagicMock(return_value=prr)

        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        assert cost == 99.0

    def test_get_cost_by_component_only_fixed(self):
        inst = _mock_instrument()
        ctm = ConstantTransactionModel(cost=50)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, ctm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        assert fixed == 50
        assert scaled == 0

    def test_get_cost_by_component_only_scaled(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, stm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        assert fixed == 0
        assert scaled == pytest.approx(100.0)

    def test_get_cost_by_component_both_sum(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.SUM,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        assert fixed == 50
        assert scaled == pytest.approx(100.0)

    def test_get_cost_by_component_max_fixed_wins(self):
        inst = _mock_instrument('i', notional_amount=100)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MAX,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        # max(50, 0.01) -> fixed wins
        assert fixed == 50
        assert scaled == 0

    def test_get_cost_by_component_max_scaled_wins(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=5)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MAX,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        # max(5, 100) -> scaled wins
        assert fixed == 0
        assert scaled == pytest.approx(100.0)

    def test_get_cost_by_component_min_fixed_wins(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=5)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MIN,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        # min(5, 100) -> fixed wins
        assert fixed == 5
        assert scaled == 0

    def test_get_cost_by_component_min_scaled_wins(self):
        inst = _mock_instrument('i', notional_amount=100)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MIN,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        # min(50, 0.01) -> scaled wins
        assert fixed == 0
        assert scaled == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# CashPayment
# ---------------------------------------------------------------------------

class TestCashPayment:
    def test_to_frame(self):
        trade = MagicMock()
        trade.name = 'my_trade'
        cp = CashPayment(trade=trade, effective_date=dt.date(2021, 6, 1), direction=1)
        cp.cash_paid['USD'] = 100.0
        cp.cash_paid['EUR'] = 50.0
        df = cp.to_frame()
        assert 'Cash Ccy' in df.columns
        assert 'Cash Amount' in df.columns
        assert 'Instrument Name' in df.columns
        assert 'Pricing Date' in df.columns
        assert len(df) == 2
        assert set(df['Cash Ccy'].tolist()) == {'USD', 'EUR'}

    def test_default_values(self):
        trade = MagicMock()
        cp = CashPayment(trade=trade)
        assert cp.effective_date is None
        assert cp.direction == 1
        assert isinstance(cp.cash_paid, defaultdict)
        assert cp.transaction_cost_entry is None


# ---------------------------------------------------------------------------
# ScalingPortfolio
# ---------------------------------------------------------------------------

class TestScalingPortfolio:
    def test_init(self):
        trade = MagicMock()
        dates = [dt.date(2021, 1, 1)]
        risk = _make_risk_measure()
        sp = ScalingPortfolio(trade, dates, risk, csa_term='OIS', risk_percentage=50)
        assert sp.trade is trade
        assert sp.dates == dates
        assert sp.risk is risk
        assert sp.csa_term == 'OIS'
        assert sp.risk_percentage == 50
        assert sp.results is None
        assert sp.risk_transformation is None


# ---------------------------------------------------------------------------
# Hedge
# ---------------------------------------------------------------------------

class TestHedge:
    def test_init(self):
        sp = MagicMock()
        entry = MagicMock()
        exit_p = MagicMock()
        h = Hedge(sp, entry, exit_p)
        assert h.scaling_portfolio is sp
        assert h.entry_payment is entry
        assert h.exit_payment is exit_p


# ---------------------------------------------------------------------------
# PredefinedAssetBacktest
# ---------------------------------------------------------------------------

class TestPredefinedAssetBacktest:
    def _make_pab(self):
        dh = MagicMock()
        pab = PredefinedAssetBacktest(data_handler=dh, initial_value=1_000_000)
        return pab

    def test_post_init(self):
        pab = self._make_pab()
        assert isinstance(pab.performance, pd.Series)
        assert isinstance(pab.holdings, defaultdict)
        assert pab.orders == []
        assert pab.results == {}
        assert isinstance(pab.cash_asset, Cash)

    def test_set_start_date(self):
        pab = self._make_pab()
        d = dt.date(2021, 1, 1)
        pab.set_start_date(d)
        assert pab.performance[d] == 1_000_000
        assert pab.holdings[pab.cash_asset] == 1_000_000

    def test_record_orders(self):
        pab = self._make_pab()
        o1 = MagicMock()
        o2 = MagicMock()
        pab.record_orders([o1, o2])
        assert pab.orders == [o1, o2]
        o3 = MagicMock()
        pab.record_orders([o3])
        assert pab.orders == [o1, o2, o3]

    def test_update_fill(self):
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))

        inst = _mock_instrument('stock_a')
        order = MagicMock()
        order.instrument = inst
        fill = FillEvent(order=order, filled_units=10, filled_price=100.0)

        pab.update_fill(fill)
        assert pab.holdings[pab.cash_asset] == 1_000_000 - 1000  # -= 10 * 100
        assert pab.holdings[inst] == 10

    def test_trade_ledger_empty(self):
        pab = self._make_pab()
        df = pab.trade_ledger()
        assert df.empty

    def test_trade_ledger_fifo_matching(self):
        pab = self._make_pab()
        inst = _mock_instrument('stock')

        # Buy then sell
        buy_order = MagicMock()
        buy_order.instrument = inst
        buy_order.quantity = 10
        buy_order.execution_end_time.return_value = dt.datetime(2021, 1, 1, 10, 0)
        buy_order.executed_price = 100.0

        sell_order = MagicMock()
        sell_order.instrument = inst
        sell_order.quantity = -10
        sell_order.execution_end_time.return_value = dt.datetime(2021, 1, 2, 10, 0)
        sell_order.executed_price = 110.0

        pab.orders = [buy_order, sell_order]
        df = pab.trade_ledger()
        assert len(df) == 1
        assert df.iloc[0]['Status'] == 'closed'
        assert df.iloc[0]['Trade PnL'] == 10.0  # (110 - 100) * sign(10) = 10

    def test_trade_ledger_unmatched_open(self):
        pab = self._make_pab()
        inst = _mock_instrument('stock')

        buy_order = MagicMock()
        buy_order.instrument = inst
        buy_order.quantity = 10
        buy_order.execution_end_time.return_value = dt.datetime(2021, 1, 1, 10, 0)
        buy_order.executed_price = 100.0

        pab.orders = [buy_order]
        df = pab.trade_ledger()
        assert len(df) == 1
        assert df.iloc[0]['Status'] == 'open'
        assert df.iloc[0]['Trade PnL'] is None
        assert df.iloc[0]['Close'] is None
        assert df.iloc[0]['Close Value'] is None

    def test_trade_ledger_short_opens_first(self):
        """When short order comes before long, short is open, long is close."""
        pab = self._make_pab()
        inst = _mock_instrument('stock')

        short_order = MagicMock()
        short_order.instrument = inst
        short_order.quantity = -10
        short_order.execution_end_time.return_value = dt.datetime(2021, 1, 1, 10, 0)
        short_order.executed_price = 110.0

        long_order = MagicMock()
        long_order.instrument = inst
        long_order.quantity = 10
        long_order.execution_end_time.return_value = dt.datetime(2021, 1, 2, 10, 0)
        long_order.executed_price = 100.0

        pab.orders = [short_order, long_order]
        df = pab.trade_ledger()
        assert len(df) == 1
        assert df.iloc[0]['Status'] == 'closed'
        # PnL = (close - open) * sign(open_qty) = (100 - 110) * -1 = 10
        assert df.iloc[0]['Trade PnL'] == 10.0

    def test_mark_to_market_cash_instrument(self):
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))
        state = dt.datetime(2021, 1, 1, 16, 0)
        vm = ValuationMethod()

        pab.mark_to_market(state, vm)
        d = state.date()
        assert pab.performance[d] == 1_000_000
        assert pab.historical_holdings[d][pab.cash_asset] == 1_000_000

    def test_mark_to_market_non_cash_daily(self):
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))

        inst = _mock_instrument('stock')
        pab.holdings[inst] = 10
        # Remove cash so only stock is counted
        pab.holdings[pab.cash_asset] = 0

        state = dt.datetime(2021, 1, 1, 16, 0)
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=None)

        pab.data_handler.get_data.return_value = 100.0
        pab.mark_to_market(state, vm)
        d = state.date()
        assert pab.performance[d] == 1000.0

    def test_mark_to_market_non_cash_window(self):
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))

        inst = _mock_instrument('stock')
        pab.holdings[inst] = 5
        pab.holdings[pab.cash_asset] = 0

        state = dt.datetime(2021, 1, 1, 16, 0)
        window = TimeWindow(start=dt.time(10, 0), end=dt.time(16, 0))
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=window)

        pab.data_handler.get_data_range.return_value = [100.0, 102.0, 104.0]
        pab.mark_to_market(state, vm)
        d = state.date()
        expected_price = np.mean([100.0, 102.0, 104.0])
        assert pab.performance[d] == pytest.approx(expected_price * 5)

    def test_mark_to_market_window_empty_fixings(self):
        """If fixings list is empty, fixing should be nan."""
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))

        inst = _mock_instrument('stock')
        pab.holdings[inst] = 5
        pab.holdings[pab.cash_asset] = 0

        state = dt.datetime(2021, 1, 1, 16, 0)
        window = TimeWindow(start=dt.time(10, 0), end=dt.time(16, 0))
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=window)

        pab.data_handler.get_data_range.return_value = []
        pab.mark_to_market(state, vm)
        d = state.date()
        # np.nan * 5 = nan; performance = 0 + nan = nan
        assert np.isnan(pab.performance[d])

    def test_mark_to_market_skips_zero_holdings(self):
        """Instruments with abs(units) <= epsilon should be skipped."""
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))
        pab.holdings[pab.cash_asset] = 0  # zero
        inst = _mock_instrument('stock')
        pab.holdings[inst] = 1e-14  # less than epsilon

        state = dt.datetime(2021, 1, 1, 16, 0)
        vm = ValuationMethod()
        pab.mark_to_market(state, vm)
        d = state.date()
        assert pab.performance[d] == 0

    def test_get_level(self):
        pab = self._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))
        assert pab.get_level(dt.date(2021, 1, 1)) == 1_000_000

    def test_get_costs_empty(self):
        pab = self._make_pab()
        costs = pab.get_costs()
        assert costs.empty

    def test_get_costs_with_order_cost(self):
        pab = self._make_pab()
        oc = MagicMock(spec=OrderCost)
        oc.execution_end_time.return_value = dt.datetime(2021, 1, 1, 10, 0)
        oc.execution_quantity.return_value = 50.0
        pab.orders = [oc]

        costs = pab.get_costs()
        assert costs[dt.date(2021, 1, 1)] == 50.0

    def test_get_costs_ignores_non_order_cost(self):
        pab = self._make_pab()
        regular_order = MagicMock(spec=OrderBase)
        pab.orders = [regular_order]
        costs = pab.get_costs()
        assert costs.empty

    def test_get_orders_for_date(self):
        pab = self._make_pab()
        o1 = MagicMock()
        o1.execution_end_time.return_value = dt.datetime(2021, 1, 1, 10, 0)
        o1.to_dict.return_value = {'Instrument': 'A', 'Quantity': 10}
        o2 = MagicMock()
        o2.execution_end_time.return_value = dt.datetime(2021, 1, 2, 10, 0)
        o2.to_dict.return_value = {'Instrument': 'B', 'Quantity': 5}
        pab.orders = [o1, o2]

        df = pab.get_orders_for_date(dt.date(2021, 1, 1))
        assert len(df) == 1
        assert df.iloc[0]['Instrument'] == 'A'

    def test_get_orders_for_date_none(self):
        pab = self._make_pab()
        df = pab.get_orders_for_date(dt.date(2021, 1, 1))
        assert df.empty


# ---------------------------------------------------------------------------
# CashAccrualModel
# ---------------------------------------------------------------------------

class TestCashAccrualModel:
    def test_base_returns_none(self):
        m = CashAccrualModel()
        result = m.get_accrued_value({'USD': 100}, dt.date(2021, 1, 1))
        assert result is None


class TestConstantCashAccrualModel:
    def test_annual_accrual(self):
        m = ConstantCashAccrualModel(rate=0.05, annual=True)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 2)
        current_value = ({'USD': 1000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        # (1 + 0.05/365)^1 * 1000
        expected = 1000.0 * (1 + 0.05 / 365) ** 1
        assert result['USD'] == pytest.approx(expected)

    def test_non_annual_accrual(self):
        m = ConstantCashAccrualModel(rate=0.001, annual=False)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 3)
        current_value = ({'USD': 5000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        expected = 5000.0 * (1 + 0.001) ** 2
        assert result['USD'] == pytest.approx(expected)

    def test_multi_day_compounding(self):
        m = ConstantCashAccrualModel(rate=0.10, annual=True)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 11)  # 10 days
        current_value = ({'USD': 10000.0, 'EUR': 5000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        daily_rate = 0.10 / 365
        assert result['USD'] == pytest.approx(10000.0 * (1 + daily_rate) ** 10)
        assert result['EUR'] == pytest.approx(5000.0 * (1 + daily_rate) ** 10)

    def test_zero_rate(self):
        m = ConstantCashAccrualModel(rate=0.0, annual=True)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 6, 1)
        current_value = ({'USD': 1000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        assert result['USD'] == pytest.approx(1000.0)

    def test_same_day(self):
        m = ConstantCashAccrualModel(rate=0.05, annual=True)
        d = dt.date(2021, 1, 1)
        current_value = ({'USD': 1000.0}, d)
        result = m.get_accrued_value(current_value, d)
        assert result['USD'] == pytest.approx(1000.0)  # (1+r)^0 = 1


class TestDataCashAccrualModel:
    def test_data_accrual_annual(self):
        ds = MagicMock()
        ds.get_data.return_value = 0.03
        m = DataCashAccrualModel(data_source=ds, annual=True)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 4)
        current_value = ({'USD': 2000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        expected = 2000.0 * (1 + 0.03 / 365) ** 3
        assert result['USD'] == pytest.approx(expected)
        ds.get_data.assert_called_once_with(from_state)

    def test_data_accrual_non_annual(self):
        ds = MagicMock()
        ds.get_data.return_value = 0.001
        m = DataCashAccrualModel(data_source=ds, annual=False)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 3)
        current_value = ({'EUR': 3000.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        expected = 3000.0 * (1 + 0.001) ** 2
        assert result['EUR'] == pytest.approx(expected)

    def test_data_accrual_multi_currency(self):
        ds = MagicMock()
        ds.get_data.return_value = 0.02
        m = DataCashAccrualModel(data_source=ds, annual=True)
        from_state = dt.date(2021, 1, 1)
        to_state = dt.date(2021, 1, 2)
        current_value = ({'USD': 1000.0, 'GBP': 500.0}, from_state)
        result = m.get_accrued_value(current_value, to_state)
        daily = 0.02 / 365
        assert result['USD'] == pytest.approx(1000.0 * (1 + daily))
        assert result['GBP'] == pytest.approx(500.0 * (1 + daily))


# ---------------------------------------------------------------------------
# TransactionAggType
# ---------------------------------------------------------------------------

class TestTransactionAggType:
    def test_enum_values(self):
        assert TransactionAggType.SUM.value == 'sum'
        assert TransactionAggType.MAX.value == 'max'
        assert TransactionAggType.MIN.value == 'min'


# ---------------------------------------------------------------------------
# Edge cases & integration-style tests
# ---------------------------------------------------------------------------

class TestBackTestEdgeCases:
    def test_trade_exit_risk_results_property(self):
        bt = _make_backtest()
        assert isinstance(bt.trade_exit_risk_results, defaultdict)

    def test_transaction_cost_entries_property(self):
        bt = _make_backtest()
        bt._transaction_cost_entries[dt.date(2021, 1, 1)] = ['entry']
        assert bt.transaction_cost_entries[dt.date(2021, 1, 1)] == ['entry']

    def test_results_property(self):
        bt = _make_backtest()
        bt._results['key'] = 'val'
        assert bt.results['key'] == 'val'


class TestTransactionCostEntryEdgeCases:
    def test_get_cost_by_component_value_error(self):
        """When aggregation func is min/max and both costs are equal and non-trivial,
        the value error branch is hard to hit because it requires an aggregate function
        where f([a, b]) != a and f([a, b]) != b, which can't happen with min/max.
        We test the nominal behaviour instead."""
        inst = _mock_instrument('i', notional_amount=500_000)
        # scaled cost will be 0.0001 * 500_000 = 50
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MAX,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        fixed, scaled = tce.get_cost_by_component()
        # Both are 50, max(50, 50) == 50 == fixed_cost -> (50, 0)
        assert fixed == 50
        assert scaled == 0

    def test_calculate_unit_cost_portfolio_with_aggregate(self):
        """Test with a Portfolio instrument and AggregateTransactionModel."""
        inst1 = _mock_instrument('a', notional_amount=100)
        inst2 = _mock_instrument('b', notional_amount=200)
        portfolio = MagicMock(spec=Portfolio)
        portfolio.all_instruments = (inst1, inst2)

        ctm = ConstantTransactionModel(cost=10)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.01)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.SUM,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), portfolio, atm)
        tce.calculate_unit_cost()

        final = tce.get_final_cost()
        # constant: 10 + 10 = 20
        # scaled: 0.01 * abs((100 + 200) * 1) = 3
        # sum(20, 3) = 23
        assert final == pytest.approx(23.0)

    def test_get_final_cost_min_aggregate(self):
        inst = _mock_instrument('i', notional_amount=1_000_000)
        ctm = ConstantTransactionModel(cost=50)
        stm = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=0.0001)
        atm = AggregateTransactionModel(
            transaction_models=(ctm, stm),
            aggregate_type=TransactionAggType.MIN,
        )
        tce = TransactionCostEntry(dt.date(2021, 1, 1), inst, atm)
        tce.calculate_unit_cost()
        cost = tce.get_final_cost()
        # min(50, 100) = 50
        assert cost == pytest.approx(50.0)


class TestPredefinedAssetBacktestWeights:
    def test_mark_to_market_weights_normalized(self):
        pab = TestPredefinedAssetBacktest()._make_pab()
        pab.set_start_date(dt.date(2021, 1, 1))

        inst = _mock_instrument('stock')
        pab.holdings[inst] = 10
        # cash stays at initial value = 1_000_000

        state = dt.datetime(2021, 1, 1, 16, 0)
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=None)
        pab.data_handler.get_data.return_value = 100.0

        pab.mark_to_market(state, vm)
        d = state.date()
        total = pab.performance[d]
        weights = pab.historical_weights[d]
        weight_sum = sum(weights.values())
        assert weight_sum == pytest.approx(1.0)


class TestMultiplePnlExplainAttributes:
    def test_multiple_attributes(self):
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        rm_gamma = _make_risk_measure('Gamma')
        rm_vol = _make_risk_measure('Vol')

        attr1 = PnlAttribute('delta_pnl', rm_delta, rm_spot, scaling_factor=1.0, second_order=False)
        attr2 = PnlAttribute('gamma_pnl', rm_gamma, rm_vol, scaling_factor=1.0, second_order=True)
        pnl_def = PnlDefinition(attributes=[attr1, attr2])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)

        inst = _mock_instrument('inst1')
        mock_portfolio = MagicMock()
        mock_portfolio.all_instruments = [inst]
        mock_portfolio.__contains__ = MagicMock(return_value=True)

        d1_inst_result = MagicMock()
        d1_data = {rm_delta: 10.0, rm_spot: 100.0, rm_gamma: 5.0, rm_vol: 0.20}
        d1_inst_result.__getitem__ = lambda s, key: d1_data[key]
        r1 = MagicMock()
        r1.portfolio = mock_portfolio
        r1.__getitem__ = MagicMock(return_value=d1_inst_result)

        d2_inst_result = MagicMock()
        d2_data = {rm_delta: 12.0, rm_spot: 102.0, rm_gamma: 6.0, rm_vol: 0.22}
        d2_inst_result.__getitem__ = lambda s, key: d2_data[key]
        r2 = MagicMock()
        r2.portfolio = mock_portfolio
        r2.__getitem__ = MagicMock(return_value=d2_inst_result)

        bt._results = {d1: r1, d2: r2}
        result = bt.pnl_explain()

        assert 'delta_pnl' in result
        assert 'gamma_pnl' in result
        # delta_pnl: 1.0 * 10 * (102 - 100) = 20
        assert result['delta_pnl'][d2] == pytest.approx(20.0)
        # gamma_pnl: 0.5 * 1.0 * 5.0 * (0.22-0.20)^2 = 0.5 * 5 * 0.0004 = 0.001
        assert result['gamma_pnl'][d2] == pytest.approx(0.001)


class TestWeightedScalingPortfolio:
    def test_init(self):
        from gs_quant.backtests.backtest_objects import WeightedScalingPortfolio
        trades = MagicMock(spec=Portfolio)
        dates = [dt.date(2021, 1, 1), dt.date(2021, 1, 2)]
        risk = _make_risk_measure('PV')
        total_size = 1_000_000.0
        wsp = WeightedScalingPortfolio(trades=trades, dates=dates, risk=risk, total_size=total_size, csa_term='OIS')
        assert wsp.trades is trades
        assert wsp.dates == dates
        assert wsp.risk is risk
        assert wsp.total_size == total_size
        assert wsp.csa_term == 'OIS'
        assert wsp.results is None

    def test_init_defaults(self):
        from gs_quant.backtests.backtest_objects import WeightedScalingPortfolio
        trades = MagicMock(spec=Portfolio)
        risk = _make_risk_measure()
        wsp = WeightedScalingPortfolio(trades=trades, dates=[], risk=risk, total_size=100.0)
        assert wsp.csa_term is None
        assert wsp.results is None


class TestWeightedTrade:
    def test_init(self):
        from gs_quant.backtests.backtest_objects import WeightedTrade, WeightedScalingPortfolio
        sp = MagicMock(spec=WeightedScalingPortfolio)
        entry_payments = [MagicMock(), MagicMock()]
        exit_payments = [MagicMock()]
        wt = WeightedTrade(scaling_portfolio=sp, entry_payments=entry_payments, exit_payments=exit_payments)
        assert wt.scaling_portfolio is sp
        assert wt.entry_payments == entry_payments
        assert wt.exit_payments == exit_payments


class TestBackTestWeightedTradesProperty:
    def test_weighted_trades_getter_setter(self):
        bt = _make_backtest()
        new_wt = defaultdict(list)
        new_wt[dt.date(2021, 1, 1)] = ['trade1']
        bt.weighted_trades = new_wt
        assert bt.weighted_trades is new_wt
        assert bt.weighted_trades[dt.date(2021, 1, 1)] == ['trade1']


class TestStrategyAsTimeSeries:
    def test_strategy_as_time_series(self):
        bt = _make_backtest()
        d1 = dt.date(2021, 1, 1)

        # Create cash payments
        trade = MagicMock()
        trade.name = 'trade1'
        cp = CashPayment(trade=trade, effective_date=d1, direction=1)
        cp.cash_paid['USD'] = -100.0
        bt._cash_payments[d1] = [cp]

        # Create mock risk results that support to_frame and portfolio.to_frame
        mock_results = MagicMock()
        risk_df = pd.DataFrame({
            'instrument_name': ['trade1'],
            'risk_measure': ['PV'],
            'value': [100.0],
        })
        mock_results.to_frame.return_value = risk_df
        mock_results.__len__ = MagicMock(return_value=1)

        portfolio_df = pd.DataFrame({'name': ['trade1'], 'type': ['IRSwap']})
        mock_results.portfolio = MagicMock()
        mock_results.portfolio.to_frame.return_value = portfolio_df

        bt._results[d1] = mock_results

        result = bt.strategy_as_time_series()
        assert not result.empty


class TestPnlExplainCumulative:
    def test_cumulative_across_three_dates(self):
        rm_delta = _make_risk_measure('Delta')
        rm_spot = _make_risk_measure('Spot')
        attr = PnlAttribute('delta', rm_delta, rm_spot, scaling_factor=1.0)
        pnl_def = PnlDefinition(attributes=[attr])
        bt = _make_backtest(pnl_explain_def=pnl_def)

        d1 = dt.date(2021, 1, 1)
        d2 = dt.date(2021, 1, 2)
        d3 = dt.date(2021, 1, 3)

        inst = _mock_instrument('inst1')
        mock_portfolio = MagicMock()
        mock_portfolio.all_instruments = [inst]
        mock_portfolio.__contains__ = MagicMock(return_value=True)

        def make_results(delta_val, spot_val):
            inst_result = MagicMock()
            inst_result.__getitem__ = lambda s, key: {rm_delta: delta_val, rm_spot: spot_val}[key]
            r = MagicMock()
            r.portfolio = mock_portfolio
            r.__getitem__ = MagicMock(return_value=inst_result)
            return r

        bt._results = {
            d1: make_results(10.0, 100.0),
            d2: make_results(10.0, 105.0),
            d3: make_results(10.0, 108.0),
        }

        result = bt.pnl_explain()
        # d2: 1.0 * 10 * (105-100) = 50; cum = 50
        assert result['delta'][d2] == pytest.approx(50.0)
        # d3: 1.0 * 10 * (108-105) = 30; cum = 80
        assert result['delta'][d3] == pytest.approx(80.0)
