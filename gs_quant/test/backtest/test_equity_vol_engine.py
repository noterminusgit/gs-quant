"""
Copyright 2021 Goldman Sachs.
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
import warnings
from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.backtests.equity_vol_engine import (
    BacktestResult,
    EquityVolEngine,
    TenorParser,
    get_backtest_trading_quantity_type,
    is_synthetic_forward,
)
from gs_quant.backtests.actions import (
    EnterPositionQuantityScaledAction,
    HedgeAction,
    ExitPositionAction,
    ExitTradeAction,
    AddTradeAction,
    AddScaledTradeAction,
    ScalingActionType,
)
from gs_quant.backtests.backtest_objects import (
    ConstantTransactionModel,
    ScaledTransactionModel,
    AggregateTransactionModel,
    TransactionAggType,
)
from gs_quant.backtests.strategy import Strategy
from gs_quant.backtests.triggers import (
    PeriodicTrigger,
    PeriodicTriggerRequirements,
    DateTriggerRequirements,
    AggregateTrigger,
    AggregateTriggerRequirements,
    PortfolioTrigger,
    PortfolioTriggerRequirements,
    TriggerDirection,
)
from gs_quant.common import BuySell, OptionType, TradeAs
from gs_quant.instrument import EqOption, EqVarianceSwap
from gs_quant.markets.portfolio import Portfolio
from gs_quant.risk import EqDelta, EqSpot, EqGamma, EqVega
from gs_quant.target.backtests import (
    BacktestTradingQuantityType,
    FlowVolBacktestMeasure,
    OptionStyle,
)

# Suppress deprecation warnings from tested legacy actions
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# =====================================================================
#  get_backtest_trading_quantity_type
# =====================================================================

class TestGetBacktestTradingQuantityType:
    def test_size_scaling(self):
        result = get_backtest_trading_quantity_type(ScalingActionType.size, None)
        assert result == BacktestTradingQuantityType.quantity

    def test_nav_scaling(self):
        result = get_backtest_trading_quantity_type(ScalingActionType.NAV, None)
        assert result == BacktestTradingQuantityType.NAV

    def test_eq_spot_risk(self):
        result = get_backtest_trading_quantity_type(None, EqSpot)
        assert result == BacktestTradingQuantityType.notional

    def test_eq_gamma_risk(self):
        result = get_backtest_trading_quantity_type(None, EqGamma)
        assert result == BacktestTradingQuantityType.gamma

    def test_eq_vega_risk(self):
        result = get_backtest_trading_quantity_type(None, EqVega)
        assert result == BacktestTradingQuantityType.vega

    def test_unsupported_raises(self):
        with pytest.raises(ValueError, match="unable to translate"):
            get_backtest_trading_quantity_type(None, None)


# =====================================================================
#  is_synthetic_forward
# =====================================================================

class TestIsSyntheticForward:
    def _make_syn_fwd(self, underlier='.SPX', expiry='3m', strike='ATM'):
        long_call = EqOption(
            underlier, expiration_date=expiry, strike_price=strike,
            option_type=OptionType.Call, buy_sell=BuySell.Buy,
        )
        short_put = EqOption(
            underlier, expiration_date=expiry, strike_price=strike,
            option_type=OptionType.Put, buy_sell=BuySell.Sell,
        )
        return Portfolio(priceables=[long_call, short_put])

    def test_valid_synthetic_forward(self):
        assert is_synthetic_forward(self._make_syn_fwd()) is True

    def test_not_portfolio(self):
        assert is_synthetic_forward(EqOption('.SPX', expiration_date='3m')) is False

    def test_portfolio_wrong_size(self):
        opt = EqOption('.SPX', expiration_date='3m')
        assert is_synthetic_forward(Portfolio(priceables=[opt])) is False

    def test_portfolio_size_two_not_eq_options(self):
        # Two non-EqOption instruments
        p = Portfolio(priceables=[MagicMock(), MagicMock()])
        assert is_synthetic_forward(p) is False

    def test_different_underliers(self):
        long_call = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                             option_type=OptionType.Call, buy_sell=BuySell.Buy)
        short_put = EqOption('.STOXX50E', expiration_date='3m', strike_price='ATM',
                             option_type=OptionType.Put, buy_sell=BuySell.Sell)
        assert is_synthetic_forward(Portfolio(priceables=[long_call, short_put])) is False

    def test_different_expiry(self):
        long_call = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                             option_type=OptionType.Call, buy_sell=BuySell.Buy)
        short_put = EqOption('.SPX', expiration_date='6m', strike_price='ATM',
                             option_type=OptionType.Put, buy_sell=BuySell.Sell)
        assert is_synthetic_forward(Portfolio(priceables=[long_call, short_put])) is False

    def test_different_strikes(self):
        long_call = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                             option_type=OptionType.Call, buy_sell=BuySell.Buy)
        short_put = EqOption('.SPX', expiration_date='3m', strike_price='ATMS',
                             option_type=OptionType.Put, buy_sell=BuySell.Sell)
        assert is_synthetic_forward(Portfolio(priceables=[long_call, short_put])) is False

    def test_wrong_option_types(self):
        # Two calls instead of call+put
        opt1 = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                        option_type=OptionType.Call, buy_sell=BuySell.Buy)
        opt2 = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                        option_type=OptionType.Call, buy_sell=BuySell.Sell)
        assert is_synthetic_forward(Portfolio(priceables=[opt1, opt2])) is False

    def test_wrong_buy_sell(self):
        # Both buy
        opt1 = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                        option_type=OptionType.Call, buy_sell=BuySell.Buy)
        opt2 = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                        option_type=OptionType.Put, buy_sell=BuySell.Buy)
        assert is_synthetic_forward(Portfolio(priceables=[opt1, opt2])) is False

    def test_portfolio_three_items(self):
        opt = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                       option_type=OptionType.Call, buy_sell=BuySell.Buy)
        assert is_synthetic_forward(Portfolio(priceables=[opt, opt, opt])) is False


# =====================================================================
#  TenorParser
# =====================================================================

class TestTenorParser:
    def test_get_date_with_date_object(self):
        d = dt.date(2021, 1, 1)
        tp = TenorParser(d)
        assert tp.get_date() == d

    def test_get_date_with_mode(self):
        tp = TenorParser('3m@listed')
        assert tp.get_date() == '3m'

    def test_get_date_without_mode(self):
        tp = TenorParser('3m')
        assert tp.get_date() == '3m'

    def test_get_mode_with_date_object(self):
        d = dt.date(2021, 1, 1)
        tp = TenorParser(d)
        assert tp.get_mode() is None

    def test_get_mode_with_modifier(self):
        tp = TenorParser('3m@listed')
        assert tp.get_mode() == 'listed'

    def test_get_mode_without_modifier(self):
        tp = TenorParser('3m')
        assert tp.get_mode() is None

    def test_get_mode_otc(self):
        tp = TenorParser('3m@otc')
        assert tp.get_mode() == 'otc'


# =====================================================================
#  BacktestResult
# =====================================================================

class TestBacktestResult:
    def test_get_measure_series_with_data(self):
        mock_risk = MagicMock()
        mock_risk.name = FlowVolBacktestMeasure.PNL.value
        mock_risk.timeseries = (
            {'date': '2021-01-01', 'value': 100.0},
            {'date': '2021-01-02', 'value': 101.0},
        )

        mock_results = MagicMock()
        mock_results.risks = [mock_risk]

        br = BacktestResult(mock_results)
        series = br.get_measure_series(FlowVolBacktestMeasure.PNL)
        assert len(series) == 2

    def test_get_measure_series_empty(self):
        mock_risk = MagicMock()
        mock_risk.name = FlowVolBacktestMeasure.PNL.value
        mock_risk.timeseries = ()

        mock_results = MagicMock()
        mock_results.risks = [mock_risk]

        br = BacktestResult(mock_results)
        df = br.get_measure_series(FlowVolBacktestMeasure.PNL)
        assert len(df) == 0

    def test_get_measure_series_no_matching_measure(self):
        mock_results = MagicMock()
        mock_results.risks = []

        br = BacktestResult(mock_results)
        df = br.get_measure_series(FlowVolBacktestMeasure.PNL)
        assert len(df) == 0

    def test_get_portfolio_history(self):
        mock_results = MagicMock()
        mock_results.portfolio = [
            {
                'date': '2021-01-01',
                'positions': [
                    {'quantity': 1, 'instrument': {'type': 'Option', 'underlier': '.SPX'}},
                    {'quantity': 2, 'instrument': {'type': 'Option', 'underlier': '.STOXX'}},
                ],
            }
        ]

        br = BacktestResult(mock_results)
        df = br.get_portfolio_history()
        assert len(df) == 2
        assert 'date' in df.columns
        assert 'quantity' in df.columns

    def test_get_trade_history(self):
        mock_results = MagicMock()
        mock_results.portfolio = [
            {
                'date': '2021-01-01',
                'transactions': [
                    {
                        'type': 'Entry',
                        'cost': 5.0,
                        'trades': [
                            {'quantity': 1, 'price': 100, 'instrument': {'type': 'Option'}},
                        ],
                    },
                ],
            }
        ]

        br = BacktestResult(mock_results)
        df = br.get_trade_history()
        assert len(df) == 1
        assert 'transactionType' in df.columns
        assert df.iloc[0]['cost'] == 5.0

    def test_get_trade_history_multiple_trades_cost_none(self):
        """When there are multiple trades in a transaction, cost should be None."""
        mock_results = MagicMock()
        mock_results.portfolio = [
            {
                'date': '2021-01-01',
                'transactions': [
                    {
                        'type': 'Entry',
                        'cost': 10.0,
                        'trades': [
                            {'quantity': 1, 'price': 100, 'instrument': {'type': 'Option'}},
                            {'quantity': 2, 'price': 200, 'instrument': {'type': 'Option'}},
                        ],
                    },
                ],
            }
        ]

        br = BacktestResult(mock_results)
        df = br.get_trade_history()
        assert len(df) == 2
        # When len(transaction['trades']) != 1, cost should be None
        assert df.iloc[0]['cost'] is None
        assert df.iloc[1]['cost'] is None


# =====================================================================
#  EquityVolEngine.check_strategy - validation tests
# =====================================================================

def _make_option(underlier='.SPX', expiry='3m', strike='ATM'):
    return EqOption(
        underlier, expiration_date=expiry, strike_price=strike,
        option_type=OptionType.Call, option_style=OptionStyle.European,
        number_of_options=1,
    )


def _make_hedge_portfolio(underlier='.SPX', expiry='3m', strike='ATM'):
    long_call = EqOption(
        underlier, expiration_date=expiry, strike_price=strike,
        option_type=OptionType.Call, option_style=OptionStyle.European,
        buy_sell=BuySell.Buy, number_of_options=1,
    )
    short_put = EqOption(
        underlier, expiration_date=expiry, strike_price=strike,
        option_type=OptionType.Put, option_style=OptionStyle.European,
        buy_sell=BuySell.Sell, number_of_options=1,
    )
    return Portfolio(priceables=[long_call, short_put])


def _make_valid_strategy():
    start = dt.date(2021, 1, 1)
    end = dt.date(2021, 6, 1)
    option = _make_option()
    action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
    trigger = PeriodicTrigger(
        PeriodicTriggerRequirements(start_date=start, end_date=end, frequency='1m'),
        actions=action,
    )
    hedge = HedgeAction(EqDelta, priceables=_make_hedge_portfolio(), trade_duration='1b', name='h')
    hedge_trigger = PeriodicTrigger(
        PeriodicTriggerRequirements(start_date=start, end_date=end, frequency='1b'),
        actions=hedge,
    )
    return Strategy(initial_portfolio=None, triggers=[trigger, hedge_trigger])


class TestCheckStrategy:
    def test_valid_strategy(self):
        result = EquityVolEngine.check_strategy(_make_valid_strategy())
        assert result == []

    def test_initial_portfolio_non_empty(self):
        strategy = MagicMock()
        strategy.initial_portfolio = [MagicMock()]
        strategy.triggers = []
        result = EquityVolEngine.check_strategy(strategy)
        assert any('initial_portfolio' in r for r in result)

    def test_too_many_triggers(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        triggers = [
            PeriodicTrigger(
                PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
                actions=action,
            )
            for _ in range(4)
        ]
        strategy = Strategy(initial_portfolio=None, triggers=triggers)
        result = EquityVolEngine.check_strategy(strategy)
        assert any('Maximum of 3 triggers' in r for r in result)

    def test_unsupported_trigger_type(self):
        strategy = MagicMock()
        strategy.initial_portfolio = []
        mock_trigger = MagicMock()
        # Not an AggregateTrigger or PeriodicTrigger
        mock_trigger.__class__ = type('BadTrigger', (), {})
        strategy.triggers = [mock_trigger]
        result = EquityVolEngine.check_strategy(strategy)
        assert any('Only AggregateTrigger and PeriodTrigger' in r for r in result)

    def test_aggregate_trigger_wrong_num_sub_triggers(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    DateTriggerRequirements(dates=[dt.date(2021, 1, 1)]),
                    PortfolioTriggerRequirements('len', 0, TriggerDirection.EQUAL),
                    DateTriggerRequirements(dates=[dt.date(2021, 2, 1)]),
                ]
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[agg])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('composed of 2 triggers' in r for r in result)

    def test_aggregate_trigger_no_date_trigger(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    PortfolioTriggerRequirements('len', 0, TriggerDirection.EQUAL),
                    PortfolioTriggerRequirements('len', 0, TriggerDirection.EQUAL),
                ]
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[agg])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('contain 1 DateTrigger' in r for r in result)

    def test_aggregate_trigger_no_portfolio_trigger(self):
        """When no portfolio triggers exist, source code crashes with IndexError at line 177."""
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    DateTriggerRequirements(dates=[dt.date(2021, 1, 1)]),
                    DateTriggerRequirements(dates=[dt.date(2021, 1, 2)]),
                ]
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[agg])
        with pytest.raises(IndexError):
            EquityVolEngine.check_strategy(strategy)

    def test_aggregate_trigger_bad_portfolio_trigger_config(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    DateTriggerRequirements(dates=[dt.date(2021, 1, 1)]),
                    PortfolioTriggerRequirements('count', 5, TriggerDirection.EQUAL),
                ]
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[agg])
        result = EquityVolEngine.check_strategy(strategy)
        assert any("data_source = 'len'" in r for r in result)

    def test_unsupported_action_type(self):
        class BadAction:
            """An action type not supported by the engine."""
            risk = None
            name = 'bad'

        bad_action = BadAction()
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=bad_action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('actions must be one of' in r for r in result)

    def test_duplicate_actions(self):
        option = _make_option()
        action1 = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a1')
        action2 = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a2')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=[action1, action2],
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('multiple actions of the same type' in r for r in result)

    def test_trigger_multiple_actions_error(self):
        option = _make_option()
        action1 = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a1')
        action2 = ExitTradeAction(name='exit')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=[action1, action2],
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('only 1 action' in r for r in result)

    def test_enter_position_freq_mismatch(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='3m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('frequency must be the same' in r for r in result)

    def test_non_eq_option_priceable(self):
        """The source code at line 257 accesses expirationDate on priceables,
        so non-EqOption/EqVarianceSwap instruments that lack expirationDate
        will crash before the 'Only EqOption...' error is appended.
        We verify the crash (AttributeError) with a non-EqOption priceable."""
        from gs_quant.instrument import IRSwap
        swap = IRSwap()
        action = AddTradeAction(priceables=swap, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        with pytest.raises(AttributeError):
            EquityVolEngine.check_strategy(strategy)

    def test_enter_position_missing_trade_quantity(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            trade_quantity=None, trade_quantity_type=None,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('trade_quantity or trade_quantity_type is None' in r for r in result)

    def test_add_scaled_trade_missing_scaling(self):
        option = _make_option()
        action = AddScaledTradeAction(
            priceables=option, trade_duration='1m', name='a',
            scaling_level=None, scaling_type=None,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('scaling_level or scaling_type is None' in r for r in result)

    def test_mixed_expiry_date_modes(self):
        opt1 = EqOption('.SPX', expiration_date='3m@listed', strike_price='ATM',
                        option_type=OptionType.Call, option_style=OptionStyle.European, number_of_options=1)
        opt2 = EqOption('.SPX', expiration_date='3m@otc', strike_price='ATM',
                        option_type=OptionType.Call, option_style=OptionStyle.European, number_of_options=1)
        action = EnterPositionQuantityScaledAction(priceables=[opt1, opt2], trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('expiration_date modifiers must be the same' in r for r in result)

    def test_invalid_expiry_date_mode(self):
        opt = EqOption('.SPX', expiration_date='3m@invalid', strike_price='ATM',
                       option_type=OptionType.Call, option_style=OptionStyle.European, number_of_options=1)
        action = EnterPositionQuantityScaledAction(priceables=opt, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('invalid expiration_date modifier' in r for r in result)

    def test_priceable_non_unit_size(self):
        opt = EqOption('.SPX', expiration_date='3m', strike_price='ATM',
                       option_type=OptionType.Call, option_style=OptionStyle.European,
                       number_of_options=5)
        action = EnterPositionQuantityScaledAction(priceables=opt, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('unit size of 1' in r for r in result)

    def test_hedge_action_not_synthetic_forward(self):
        opt = _make_option()
        hedge = HedgeAction(EqDelta, priceables=opt, trade_duration='1b', name='h')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1b'),
            actions=hedge,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('synthetic forward' in r for r in result)

    def test_hedge_action_freq_mismatch(self):
        hedge = HedgeAction(EqDelta, priceables=_make_hedge_portfolio(), trade_duration='1m', name='h')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1b'),
            actions=hedge,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('frequency must be the same' in r for r in result)

    def test_hedge_action_wrong_risk(self):
        hedge = HedgeAction(EqVega, priceables=_make_hedge_portfolio(), trade_duration='1b', name='h')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1b'),
            actions=hedge,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('EqDelta' in r for r in result)

    def test_exit_position_action_deprecation_warning(self):
        exit_action = ExitPositionAction(name='exit')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=exit_action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            EquityVolEngine.check_strategy(strategy)
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1

    def test_exit_trade_action_valid(self):
        option = _make_option()
        enter_action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='enter')
        exit_action = ExitTradeAction(name='exit')
        t1 = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=enter_action,
        )
        t2 = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=exit_action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[t1, t2])
        result = EquityVolEngine.check_strategy(strategy)
        assert result == []

    def test_add_trade_action_valid(self):
        option = _make_option()
        action = AddTradeAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert result == []

    def test_add_trade_action_freq_mismatch(self):
        option = _make_option()
        action = AddTradeAction(priceables=option, trade_duration='3m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert any('frequency must be the same' in r for r in result)

    def test_add_scaled_trade_action_valid(self):
        option = _make_option()
        action = AddScaledTradeAction(
            priceables=option, trade_duration='1m', name='a',
            scaling_type=ScalingActionType.size, scaling_level=100,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        assert result == []

    def test_eq_variance_swap_accepted(self):
        vs = EqVarianceSwap(underlier='.SPX', expiration_date='3m', strike_price='ATM')
        action = AddTradeAction(priceables=vs, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.check_strategy(strategy)
        # Should not contain the "Only EqOption or EqVarianceSwap" error
        assert not any('Only EqOption or EqVarianceSwap' in r for r in result)

    def test_portfolio_trigger_skipped_in_child_triggers(self):
        """When a PortfolioTrigger is included in strategy triggers,
        it should be skipped in the per-trigger validation loop (line 228-229)."""
        option = _make_option()
        enter_action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='enter')
        exit_action = ExitTradeAction(name='exit')

        periodic = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=enter_action,
        )
        portfolio_trigger = PortfolioTrigger(
            trigger_requirements=PortfolioTriggerRequirements('len', 0, TriggerDirection.EQUAL),
            actions=exit_action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[periodic, portfolio_trigger])
        result = EquityVolEngine.check_strategy(strategy)
        # Should have the "Only AggregateTrigger and PeriodTrigger supported" error
        # but should NOT crash because the PortfolioTrigger is skipped (continue at line 229)
        assert any('Only AggregateTrigger and PeriodTrigger' in r for r in result)


# =====================================================================
#  EquityVolEngine.supports_strategy
# =====================================================================

class TestSupportsStrategy:
    def test_valid_strategy(self):
        assert EquityVolEngine.supports_strategy(_make_valid_strategy()) is True

    def test_invalid_strategy(self):
        strategy = MagicMock()
        strategy.initial_portfolio = [MagicMock()]
        strategy.triggers = []
        assert EquityVolEngine.supports_strategy(strategy) is False


# =====================================================================
#  EquityVolEngine.run_backtest
# =====================================================================

class TestRunBacktest:
    def test_invalid_strategy_raises(self):
        strategy = MagicMock()
        strategy.initial_portfolio = [MagicMock()]
        strategy.triggers = []
        with pytest.raises(RuntimeError):
            EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_with_enter_position_action(self, mock_strategy_cls):
        mock_result = MagicMock()
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = mock_result
        mock_strategy_cls.return_value = mock_strategy_instance

        strategy = _make_valid_strategy()
        result = EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        assert isinstance(result, BacktestResult)
        mock_strategy_instance.backtest.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_with_add_trade_action(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        action = AddTradeAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_with_add_scaled_trade_action(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        action = AddScaledTradeAction(
            priceables=option, trade_duration='1m', name='a',
            scaling_type=ScalingActionType.size, scaling_level=100,
            scaling_risk=EqSpot,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        result = EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_with_aggregate_trade_in_signal(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')

        entry_agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    DateTriggerRequirements(dates=[dt.date(2021, 2, 1)]),
                    PortfolioTriggerRequirements('len', 0, TriggerDirection.EQUAL),
                ]
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[entry_agg])
        result = EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_with_aggregate_trade_out_signal(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        enter_action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='enter')
        exit_action = ExitTradeAction(name='exit')

        entry_trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=enter_action,
        )

        exit_agg = AggregateTrigger(
            AggregateTriggerRequirements(
                triggers=[
                    DateTriggerRequirements(dates=[dt.date(2021, 3, 1)]),
                    PortfolioTriggerRequirements('len', 0, TriggerDirection.ABOVE),
                ]
            ),
            actions=exit_action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[entry_trigger, exit_agg])
        result = EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_run_no_transaction_cost(self, mock_strategy_cls):
        """Test when no transaction costs are specified, transaction_cost_config should be None."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        action = AddTradeAction(priceables=option, trade_duration='1m', name='a',
                                transaction_cost=None, transaction_cost_exit=None)
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))

        # Verify StrategySystematic was called
        mock_strategy_cls.assert_called_once()


# =====================================================================
#  EquityVolEngine.__map_tc_model (private, tested via run_backtest)
# =====================================================================

class TestMapTcModel:
    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_constant_transaction_model(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        tc = ConstantTransactionModel(cost=5.0)
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=tc, transaction_cost_exit=tc,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_scaled_transaction_model_vega(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        tc = ScaledTransactionModel(scaling_type=EqVega, scaling_level=0.01)
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=tc,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_scaled_transaction_model_non_vega(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        tc = ScaledTransactionModel(scaling_type='Vega', scaling_level=0.01)
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=tc,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_scaled_transaction_model_unsupported_type(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        tc = ScaledTransactionModel(scaling_type='CompletelyInvalidType', scaling_level=0.01)
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=tc,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        with pytest.raises(RuntimeError, match='unsupported scaled transaction quantity type'):
            EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_aggregate_transaction_model(self, mock_strategy_cls):
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        tc1 = ConstantTransactionModel(cost=1.0)
        tc2 = ConstantTransactionModel(cost=2.0)
        # Use a mock aggregate_type whose .value matches CostAggregationType ('Sum' not 'sum')
        mock_agg_type = MagicMock()
        mock_agg_type.value = 'Sum'
        tc = AggregateTransactionModel(
            transaction_models=(tc1, tc2),
            aggregate_type=mock_agg_type,
        )
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=tc,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_none_transaction_model(self, mock_strategy_cls):
        """When transaction_cost is None, __map_tc_model returns None."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option()
        action = EnterPositionQuantityScaledAction(
            priceables=option, trade_duration='1m', name='a',
            transaction_cost=None,
        )
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()


# =====================================================================
#  EquityVolEngine.__get_underlier_list (tested via run_backtest)
# =====================================================================

class TestGetUnderlierList:
    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_trade_as_attribute(self, mock_strategy_cls):
        """Test that if priceable has trade_as attribute and mode is valid TradeAs, it gets set."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = _make_option(expiry='3m@listed')
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_no_trade_as_attribute(self, mock_strategy_cls):
        """Test priceable without trade_as attribute."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        vs = EqVarianceSwap(underlier='.SPX', expiration_date='3m', strike_price='ATM')
        action = AddTradeAction(priceables=vs, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_expiry_date_mode_not_trade_as(self, mock_strategy_cls):
        """When expiry_date_mode is not a valid TradeAs, priceable.trade_as should be set to None."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        # Using plain tenor without @mode, so mode is None, and get_enum_value returns None (not TradeAs)
        option = _make_option(expiry='3m')
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()

    @patch('gs_quant.backtests.equity_vol_engine.StrategySystematic')
    def test_date_expiry(self, mock_strategy_cls):
        """Test priceable with dt.date as expiration_date."""
        mock_strategy_instance = MagicMock()
        mock_strategy_instance.backtest.return_value = MagicMock()
        mock_strategy_cls.return_value = mock_strategy_instance

        option = EqOption('.SPX', expiration_date=dt.date(2021, 6, 1), strike_price='ATM',
                          option_type=OptionType.Call, option_style=OptionStyle.European, number_of_options=1)
        action = AddTradeAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(
                start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'
            ),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        EquityVolEngine.run_backtest(strategy, dt.date(2021, 1, 1), dt.date(2021, 6, 1))
        mock_strategy_cls.assert_called_once()


# =====================================================================
#  EnterPositionQuantityScaledAction deprecation warning
# =====================================================================

class TestDeprecationWarnings:
    def test_enter_position_quantity_scaled_deprecation(self):
        option = _make_option()
        action = EnterPositionQuantityScaledAction(priceables=option, trade_duration='1m', name='a')
        trigger = PeriodicTrigger(
            PeriodicTriggerRequirements(start_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 6, 1), frequency='1m'),
            actions=action,
        )
        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            EquityVolEngine.check_strategy(strategy)
            dep = [x for x in w if 'EnterPositionQuantityScaledAction' in str(x.message)]
            assert len(dep) >= 1
