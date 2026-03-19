"""
Branch coverage tests for gs_quant/backtests/predefined_asset_engine.py

Targets uncovered lines: 63, 100, 109-115, 136, 162, 186
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.backtests.actions import Action, AddTradeAction
from gs_quant.backtests.backtest_objects import PredefinedAssetBacktest
from gs_quant.backtests.core import ValuationFixingType, ValuationMethod, TimeWindow
from gs_quant.backtests.predefined_asset_engine import (
    AddTradeActionImpl,
    PredefinedAssetEngine,
    PredefinedAssetEngineActionFactory,
    SubmitOrderActionImpl,
)
from gs_quant.backtests.strategy import Strategy
from gs_quant.backtests.data_sources import DataManager


def _make_priceable(name='TestInstrument', instrument_quantity=1):
    """Create a proper mock that can survive AddTradeAction.__post_init__."""
    p = MagicMock()
    p.name = name
    p.instrument_quantity = instrument_quantity
    p.clone.return_value = p
    return p


def _make_add_trade_action(trade_duration=None, name='TestAction', instrument_quantity=1):
    """Create an AddTradeAction, bypassing __post_init__ side effects via mocking."""
    priceable = _make_priceable(name='TestAction_Inst', instrument_quantity=instrument_quantity)
    action = MagicMock(spec=AddTradeAction)
    action.priceables = [priceable]
    action.trade_duration = trade_duration
    action.name = name
    return action


# ---------------------------------------------------------------------------
# AddTradeActionImpl
# ---------------------------------------------------------------------------

class TestAddTradeActionImpl:
    def test_generate_orders_with_trade_duration_timedelta(self):
        """When trade_duration is a timedelta, close orders should be generated (line 63)."""
        action = _make_add_trade_action(trade_duration=dt.timedelta(days=1), instrument_quantity=5)
        impl = AddTradeActionImpl.__new__(AddTradeActionImpl)
        impl._action = action

        state = dt.datetime(2021, 1, 5, 10, 0)
        bt = MagicMock(spec=PredefinedAssetBacktest)

        orders = impl.generate_orders(state, bt, info=None)
        # Should have 2 orders: open and close
        assert len(orders) == 2
        # Close order should have negative quantity
        assert orders[1].quantity == -5
        # Close order execution datetime should be state + timedelta
        assert orders[1].execution_datetime == state + dt.timedelta(days=1)

    def test_generate_orders_without_trade_duration(self):
        """When trade_duration is None, only open orders should be generated."""
        action = _make_add_trade_action(trade_duration=None, instrument_quantity=3)
        impl = AddTradeActionImpl.__new__(AddTradeActionImpl)
        impl._action = action

        state = dt.datetime(2021, 1, 5, 10, 0)
        bt = MagicMock(spec=PredefinedAssetBacktest)

        orders = impl.generate_orders(state, bt, info=None)
        assert len(orders) == 1

    def test_generate_orders_with_info_scaling(self):
        """When info provides scaling, it should be used for quantity."""
        action = _make_add_trade_action(instrument_quantity=2)
        impl = AddTradeActionImpl.__new__(AddTradeActionImpl)
        impl._action = action

        state = dt.datetime(2021, 1, 5, 10, 0)
        bt = MagicMock(spec=PredefinedAssetBacktest)
        info = MagicMock()
        info.scaling = 3.0

        orders = impl.generate_orders(state, bt, info)
        assert len(orders) == 1
        assert orders[0].quantity == 3.0

    def test_apply_action(self):
        """apply_action delegates to generate_orders."""
        action = _make_add_trade_action(instrument_quantity=1)
        impl = AddTradeActionImpl.__new__(AddTradeActionImpl)
        impl._action = action

        state = dt.datetime(2021, 1, 5, 10, 0)
        bt = MagicMock(spec=PredefinedAssetBacktest)

        orders = impl.apply_action(state, bt, info=None)
        assert len(orders) == 1


# ---------------------------------------------------------------------------
# SubmitOrderActionImpl
# ---------------------------------------------------------------------------

class TestSubmitOrderActionImpl:
    def test_apply_action_returns_info(self):
        """SubmitOrderActionImpl.apply_action simply returns the info argument."""
        action = MagicMock(spec=Action)
        impl = SubmitOrderActionImpl(action)
        info = [MagicMock(), MagicMock()]

        result = impl.apply_action(dt.datetime(2021, 1, 5, 10, 0), MagicMock(), info)
        assert result is info


# ---------------------------------------------------------------------------
# PredefinedAssetEngineActionFactory
# ---------------------------------------------------------------------------

class TestPredefinedAssetEngineActionFactory:
    def test_default_action_impl_map(self):
        """Factory default includes AddTradeAction mapping."""
        factory = PredefinedAssetEngineActionFactory()
        assert AddTradeAction in factory.action_impl_map

    def test_custom_action_impl_map(self):
        """Factory accepts custom action_impl_map."""
        custom_map = {Action: SubmitOrderActionImpl}
        factory = PredefinedAssetEngineActionFactory(action_impl_map=custom_map)
        assert Action in factory.action_impl_map
        assert AddTradeAction in factory.action_impl_map

    def test_get_action_handler_unknown_raises(self):
        """get_action_handler raises RuntimeError for unsupported action type (line 100)."""
        factory = PredefinedAssetEngineActionFactory()
        unknown_action = MagicMock()
        with pytest.raises(RuntimeError, match='not supported by engine'):
            factory.get_action_handler(unknown_action)


# ---------------------------------------------------------------------------
# PredefinedAssetEngine
# ---------------------------------------------------------------------------

class TestPredefinedAssetEngine:
    def test_init_defaults(self):
        """Engine has correct default settings."""
        engine = PredefinedAssetEngine()
        assert engine.calendars is None
        assert engine.tz == dt.timezone.utc

    def test_supports_strategy_true(self):
        """supports_strategy returns True when all actions are supported (line 109-115)."""
        # Action type is in the default action_impl_map via __init__
        action = Action.__new__(Action)
        trigger = MagicMock()
        trigger.actions = [action]

        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        # Default action_impl_map maps Action -> SubmitOrderActionImpl
        engine = PredefinedAssetEngine()
        assert engine.supports_strategy(strategy) is True

    def test_supports_strategy_false(self):
        """supports_strategy returns False when an action is not supported (line 113-114)."""
        # Use a type that's not in any action_impl_map
        class UnsupportedAction:
            pass

        unsupported_action = UnsupportedAction()
        trigger = MagicMock()
        trigger.actions = [unsupported_action]

        strategy = Strategy(initial_portfolio=None, triggers=[trigger])
        engine = PredefinedAssetEngine()
        assert engine.supports_strategy(strategy) is False

    def test_eod_valuation_time_with_window(self):
        """_eod_valuation_time returns window.end when window is set (line 136)."""
        window = TimeWindow(start=dt.time(10, 0), end=dt.time(16, 0))
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=window)
        engine = PredefinedAssetEngine(valuation_method=vm)
        assert engine._eod_valuation_time() == dt.time(16, 0)

    def test_eod_valuation_time_without_window(self):
        """_eod_valuation_time returns dt.time(23) when no window is set."""
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=None)
        engine = PredefinedAssetEngine(valuation_method=vm)
        assert engine._eod_valuation_time() == dt.time(23)

    @patch('gs_quant.backtests.predefined_asset_engine.is_business_day')
    def test_adjust_date_with_calendar_non_business_day(self, mock_is_bday):
        """_adjust_date with calendars and non-business day adjusts to previous business day (line 186)."""
        mock_is_bday.return_value = False
        engine = PredefinedAssetEngine(calendars='NYSE')

        with patch('gs_quant.backtests.predefined_asset_engine.prev_business_date') as mock_prev:
            mock_prev.return_value = dt.date(2021, 1, 4)
            result = engine._adjust_date(dt.date(2021, 1, 5))
            assert result == dt.date(2021, 1, 4)

    @patch('gs_quant.backtests.predefined_asset_engine.is_business_day')
    def test_adjust_date_weekend_calendar(self, mock_is_bday):
        """_adjust_date with 'weekend' calendar returns the date directly (line 183)."""
        engine = PredefinedAssetEngine(calendars='weekend')
        # 'weekend' calendar means None is passed to is_business_day
        result = engine._adjust_date(dt.date(2021, 1, 4))
        # Should return the weekday-adjusted date
        assert isinstance(result, dt.date)

    def test_adjust_date_no_calendar(self):
        """_adjust_date with no calendar adjusts to weekday."""
        engine = PredefinedAssetEngine()
        result = engine._adjust_date(dt.date(2021, 1, 4))
        assert result == dt.date(2021, 1, 4)

    def test_timer_with_datetime_trigger_times(self):
        """_timer handles triggers that return datetime objects (line 155)."""
        engine = PredefinedAssetEngine()

        mock_trigger = MagicMock()
        mock_trigger.get_trigger_times.return_value = [
            dt.datetime(2021, 1, 5, 14, 0)  # datetime, not time
        ]

        strategy = MagicMock()
        strategy.triggers = [mock_trigger]

        start = dt.date(2021, 1, 4)
        end = dt.date(2021, 1, 6)

        timer = engine._timer(strategy, start, end, 'B')
        assert dt.datetime(2021, 1, 5, 14, 0) in timer

    @patch('gs_quant.backtests.predefined_asset_engine.is_business_day')
    def test_timer_with_calendar_filter(self, mock_is_bday):
        """_timer with calendars filters non-business days (line 162)."""
        engine = PredefinedAssetEngine(calendars='weekend')

        strategy = MagicMock()
        strategy.triggers = []

        start = dt.date(2021, 1, 4)
        end = dt.date(2021, 1, 8)

        mock_is_bday.return_value = [True, True, True, True, True]

        timer = engine._timer(strategy, start, end, 'B')
        assert len(timer) > 0

    def test_timer_with_states(self):
        """_timer with explicitly provided states uses them."""
        engine = PredefinedAssetEngine()
        strategy = MagicMock()
        strategy.triggers = []

        states = [dt.date(2021, 1, 4), dt.date(2021, 1, 5)]
        timer = engine._timer(strategy, dt.date(2021, 1, 4), dt.date(2021, 1, 5), 'B', states=states)
        assert len(timer) == 2

    def test_timer_no_get_trigger_times(self):
        """_timer skips triggers that don't have get_trigger_times."""
        engine = PredefinedAssetEngine()

        mock_trigger = MagicMock(spec=[])  # no attributes
        strategy = MagicMock()
        strategy.triggers = [mock_trigger]

        start = dt.date(2021, 1, 4)
        end = dt.date(2021, 1, 6)

        timer = engine._timer(strategy, start, end, 'B')
        assert len(timer) > 0

    @patch('gs_quant.backtests.predefined_asset_engine.is_business_day')
    @patch('gs_quant.backtests.predefined_asset_engine.business_day_offset')
    def test_run_backtest_with_calendar(self, mock_bday_offset, mock_is_bday):
        """run_backtest with calendars adjusts start date."""
        # is_business_day is called with a list of dates and returns a list of bools
        mock_is_bday.side_effect = lambda dates, cal=None: (
            [True] * len(dates) if isinstance(dates, list) else True
        )
        mock_bday_offset.return_value = dt.date(2021, 1, 5)

        engine = PredefinedAssetEngine(calendars='weekend')
        strategy = MagicMock()
        strategy.triggers = []

        with patch.object(engine, '_run') as mock_run:
            mock_run.return_value = MagicMock()
            backtest = engine.run_backtest(strategy, dt.date(2021, 1, 4), dt.date(2021, 1, 8))
            mock_run.assert_called_once()

    def test_run_backtest_with_states(self):
        """run_backtest with states uses them directly."""
        engine = PredefinedAssetEngine()
        strategy = MagicMock()
        strategy.triggers = []
        states = [dt.datetime(2021, 1, 4, 10, 0), dt.datetime(2021, 1, 5, 10, 0)]

        with patch.object(engine, '_run') as mock_run:
            mock_run.return_value = MagicMock()
            backtest = engine.run_backtest(strategy, dt.date(2021, 1, 4), dt.date(2021, 1, 5), states=states)
            mock_run.assert_called_once()
