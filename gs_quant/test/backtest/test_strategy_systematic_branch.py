"""
Branch coverage tests for gs_quant/backtests/strategy_systematic.py

Targets uncovered lines: 119-120, 126, 140-145, 173->175, 176-178, 202-266, 269-273, 284, 305-308
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.backtests.strategy_systematic import StrategySystematic
from gs_quant.common import AssetClass, Currency
from gs_quant.errors import MqValueError
from gs_quant.instrument import EqOption, EqVarianceSwap
from gs_quant.target.backtests import (
    BacktestRisk,
    BacktestResult,
    BacktestSignalSeriesItem,
    BacktestTradingQuantityType,
    DeltaHedgeParameters,
    FlowVolBacktestMeasure,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_eq_option(**kwargs):
    defaults = dict(
        underlier="MA4B66MW5E27U8P32SB",
        expiration_date="3m",
        strike_price=3000,
        option_type='Call',
        option_style='European',
    )
    defaults.update(kwargs)
    return EqOption(**defaults)


def _make_strategy(**kwargs):
    defaults = dict(
        underliers=_make_eq_option(),
        quantity=1,
        quantity_type=BacktestTradingQuantityType.notional,
        roll_frequency='1m',
        use_xasset_backtesting_service=False,
    )
    defaults.update(kwargs)
    return StrategySystematic(**defaults)


# ---------------------------------------------------------------------------
# __init__ branches
# ---------------------------------------------------------------------------

class TestStrategySystematicInit:
    def test_single_underlier(self):
        """Single (non-iterable) underlier follows the else branch (lines 140-145)."""
        strategy = _make_strategy(underliers=_make_eq_option())
        assert strategy is not None

    def test_iterable_underlier_as_list(self):
        """Iterable underliers follow the for loop branch."""
        strategy = _make_strategy(underliers=[_make_eq_option()])
        assert strategy is not None

    def test_iterable_underlier_as_tuple_pair(self):
        """Iterable underlier with tuple (instrument, notional_percentage) (lines 119-120)."""
        eq = _make_eq_option(number_of_options=100, buy_sell='Buy')
        strategy = _make_strategy(underliers=[(eq, 75)])
        assert strategy is not None

    def test_unsupported_eq_instrument_iterable_raises(self):
        """Unsupported Eq instrument in iterable raises MqValueError (line 126)."""
        # Create a mock that starts with 'Eq' but is not EqOption or EqVarianceSwap
        mock_inst = MagicMock()
        mock_inst.__class__.__name__ = 'EqForward'
        mock_inst.scale = MagicMock(return_value=mock_inst)

        with pytest.raises(MqValueError, match='The format of the backtest asset is incorrect'):
            _make_strategy(underliers=[mock_inst])

    def test_unsupported_eq_instrument_single_raises(self):
        """Unsupported Eq instrument as single underlier raises MqValueError (line 142)."""
        # Need a non-iterable object that starts with 'Eq' and is not a supported type
        class EqForward:
            pass

        mock_inst = EqForward()

        with pytest.raises(MqValueError, match='The format of the backtest asset is incorrect'):
            _make_strategy(underliers=mock_inst)

    def test_delta_hedge_daily_frequency(self):
        """Delta hedge with 'Daily' frequency maps to '1b' (line 174)."""
        hedge = DeltaHedgeParameters(frequency='Daily')
        strategy = _make_strategy(delta_hedge=hedge)
        assert strategy is not None

    def test_delta_hedge_non_daily_frequency(self):
        """Delta hedge with non-Daily frequency is passed through (line 174 else)."""
        hedge = DeltaHedgeParameters(frequency='Weekly')
        strategy = _make_strategy(delta_hedge=hedge)
        assert strategy is not None

    def test_delta_hedge_with_notional(self):
        """Delta hedge with notional sets risk_percentage (line 176)."""
        hedge = DeltaHedgeParameters(frequency='Daily', notional=50.0)
        strategy = _make_strategy(delta_hedge=hedge)
        assert strategy is not None

    def test_no_delta_hedge(self):
        """No delta hedge sets hedge_params to None (line 178)."""
        strategy = _make_strategy(delta_hedge=None)
        assert strategy is not None

    def test_trade_in_signals(self):
        """trade_in_signals are processed for trade_buy_dates."""
        signals = (
            BacktestSignalSeriesItem(date=dt.date(2021, 1, 4), value=1.0),
            BacktestSignalSeriesItem(date=dt.date(2021, 1, 5), value=0.0),
        )
        strategy = _make_strategy(trade_in_signals=signals)
        assert strategy is not None

    def test_trade_out_signals(self):
        """trade_out_signals are processed for trade_exit_dates."""
        signals = (
            BacktestSignalSeriesItem(date=dt.date(2021, 1, 6), value=1.0),
        )
        strategy = _make_strategy(trade_out_signals=signals)
        assert strategy is not None

    def test_eq_variance_swap_underlier(self):
        """EqVarianceSwap is a supported instrument."""
        eq_vs = EqVarianceSwap(
            underlier="MA4B66MW5E27U8P32SB",
            expiration_date="3m",
            strike_price=100,
            buy_sell='Buy',
            quantity=1,
        )
        strategy = _make_strategy(underliers=eq_vs)
        assert strategy is not None


# ---------------------------------------------------------------------------
# __run_service_based_backtest
# ---------------------------------------------------------------------------

class TestRunServiceBasedBacktest:
    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_full(self, mock_xasset_api):
        """Full test of __run_service_based_backtest with portfolio, transactions, events."""
        mock_measure_result = MagicMock()
        mock_measure_result.result = 100.0

        mock_measure_key = MagicMock()
        mock_measure_key.value = 'PNL'

        mock_response = MagicMock()
        mock_response.measures = {mock_measure_key: {dt.date(2021, 1, 4): mock_measure_result}}

        # Set up additional_results with trade_events and hedge_events
        mock_response.additional_results = MagicMock()
        mock_response.additional_results.trade_events = {dt.date(2021, 1, 4): 'trade_event_1'}
        mock_response.additional_results.hedge_events = {dt.date(2021, 1, 4): 'hedge_event_1'}

        # Set up portfolio
        mock_response.portfolio = {
            dt.date(2021, 1, 4): [
                {'assetClass': 'Equity', 'buySell': 'Buy', 'type': 'Option', 'numberOfOptions': 100}
            ]
        }

        # Set up transactions
        mock_txn = MagicMock()
        mock_txn.direction.value = 'Buy'
        mock_txn.portfolio_price = 10.0
        mock_txn.quantity = 5
        mock_txn.cost = 1.0
        mock_txn.portfolio = [{'assetClass': 'Equity', 'type': 'Option'}]
        mock_response.transactions = {dt.date(2021, 1, 4): [mock_txn]}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
            measures=(FlowVolBacktestMeasure.PNL,),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_no_additional_results(self, mock_xasset_api):
        """Test when additional_results is None."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {}
        mock_response.transactions = {}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_no_trade_events(self, mock_xasset_api):
        """Test when additional_results exists but trade_events is None."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = MagicMock()
        mock_response.additional_results.trade_events = None
        mock_response.additional_results.hedge_events = None
        mock_response.portfolio = {}
        mock_response.transactions = {}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_empty_measures(self, mock_xasset_api):
        """When measures is empty, defaults to PNL."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {}
        mock_response.transactions = {}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
            measures=(),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_transaction_no_portfolio(self, mock_xasset_api):
        """Test when transaction has portfolio=None."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {}

        mock_txn = MagicMock()
        mock_txn.direction.value = 'Sell'
        mock_txn.portfolio_price = 5.0
        mock_txn.quantity = None
        mock_txn.cost = 0.5
        mock_txn.portfolio = None
        mock_response.transactions = {dt.date(2021, 1, 4): [mock_txn]}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_date_with_no_portfolio(self, mock_xasset_api):
        """Test when a date is in transactions but not in portfolio (lines 247-248)."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {dt.date(2021, 1, 4): []}

        mock_txn = MagicMock()
        mock_txn.direction.value = 'Buy'
        mock_txn.portfolio_price = 10.0
        mock_txn.quantity = 1
        mock_txn.cost = 0.0
        mock_txn.portfolio = [{'assetClass': 'Equity', 'type': 'Option'}]
        mock_response.transactions = {dt.date(2021, 1, 5): [mock_txn]}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_run_service_based_backtest_instrument_none_in_transaction(self, mock_xasset_api):
        """Test when instrument in transaction portfolio is None (gets replaced with {})."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {}

        mock_txn = MagicMock()
        mock_txn.direction.value = 'Buy'
        mock_txn.portfolio_price = 10.0
        mock_txn.quantity = 1
        mock_txn.cost = 0.0
        mock_txn.portfolio = [None]  # instrument is None in txn portfolio
        mock_response.transactions = {dt.date(2021, 1, 4): [mock_txn]}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)


# ---------------------------------------------------------------------------
# __position_quantity
# ---------------------------------------------------------------------------

class TestPositionQuantity:
    def test_equity_option_buy(self):
        """Equity Option Buy uses numberOfOptions."""
        strategy = _make_strategy()
        inst = {'assetClass': 'Equity', 'buySell': 'Buy', 'type': 'Option', 'numberOfOptions': 100}
        result = strategy._StrategySystematic__position_quantity(inst)
        assert result == 100

    def test_equity_option_sell(self):
        """Equity Option Sell uses negative numberOfOptions."""
        strategy = _make_strategy()
        inst = {'assetClass': 'Equity', 'buySell': 'Sell', 'type': 'Option', 'numberOfOptions': 50}
        result = strategy._StrategySystematic__position_quantity(inst)
        assert result == -50

    def test_equity_non_option(self):
        """Equity non-Option uses quantity field."""
        strategy = _make_strategy()
        inst = {'assetClass': 'Equity', 'buySell': 'Buy', 'type': 'VarianceSwap', 'quantity': 200}
        result = strategy._StrategySystematic__position_quantity(inst)
        assert result == 200

    def test_non_equity_returns_none(self):
        """Non-equity returns None (line 273)."""
        strategy = _make_strategy()
        inst = {'assetClass': 'Rates', 'type': 'Swap'}
        result = strategy._StrategySystematic__position_quantity(inst)
        assert result is None


# ---------------------------------------------------------------------------
# backtest method
# ---------------------------------------------------------------------------

class TestBacktestMethod:
    @patch('gs_quant.backtests.strategy_systematic.GsBacktestApi')
    def test_backtest_non_xasset_sync(self, mock_bt_api):
        """backtest with use_xasset_backtesting_service=False runs on-the-fly."""
        mock_bt_api.run_backtest.return_value = MagicMock(spec=BacktestResult)

        strategy = _make_strategy(use_xasset_backtesting_service=False)
        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        mock_bt_api.run_backtest.assert_called_once()

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestApi')
    def test_backtest_non_xasset_async(self, mock_bt_api):
        """backtest with is_async=True creates and schedules the backtest (lines 305-308)."""
        mock_response = MagicMock()
        mock_response.id = 'bt-123'
        mock_bt_api.create_backtest.return_value = mock_response

        strategy = _make_strategy(use_xasset_backtesting_service=False)
        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
            is_async=True,
        )
        mock_bt_api.create_backtest.assert_called_once()
        mock_bt_api.schedule_backtest.assert_called_once_with(backtest_id='bt-123')

    @patch('gs_quant.backtests.strategy_systematic.GsBacktestXassetApi')
    def test_backtest_xasset_service(self, mock_xasset_api):
        """backtest with use_xasset_backtesting_service=True calls service-based backtest (line 284)."""
        mock_response = MagicMock()
        mock_response.measures = {}
        mock_response.additional_results = None
        mock_response.portfolio = {}
        mock_response.transactions = {}

        mock_xasset_api.calculate_basic_backtest.return_value = mock_response

        strategy = StrategySystematic(
            underliers=_make_eq_option(),
            quantity=1,
            roll_frequency='1m',
            use_xasset_backtesting_service=True,
        )

        result = strategy.backtest(
            start=dt.date(2021, 1, 4),
            end=dt.date(2021, 1, 8),
        )
        assert isinstance(result, BacktestResult)
