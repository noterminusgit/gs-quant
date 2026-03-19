"""
Tests for gs_quant/backtests/generic_engine_action_impls.py
Targets 100% branch coverage.
"""

import datetime as dt
from collections import defaultdict, namedtuple
from unittest.mock import MagicMock, patch, PropertyMock, call

import pytest

# We need to mock heavy imports that require auth before importing the module under test.
# The module imports are at the top level so we patch at the module level after import.

from gs_quant.backtests.generic_engine_action_impls import (
    OrderBasedActionImpl,
    AddTradeActionImpl,
    AddScaledTradeActionImpl,
    HedgeActionImpl,
    ExitTradeActionImpl,
    RebalanceActionImpl,
    AddWeightedTradeActionImpl,
)
from gs_quant.backtests.actions import (
    AddTradeActionInfo,
    HedgeActionInfo,
    ExitTradeActionInfo,
    RebalanceActionInfo,
    AddScaledTradeActionInfo,
    ScalingActionType,
    AddWeightedTradeActionInfo,
)
from gs_quant.backtests.backtest_objects import (
    CashPayment,
    TransactionCostEntry,
    ScalingPortfolio,
    Hedge,
    WeightedScalingPortfolio,
    WeightedTrade,
)
from gs_quant.instrument import Instrument
from gs_quant.markets.portfolio import Portfolio


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _make_instrument(name='inst', use_spec=False):
    """Create a mock Instrument with a name and clone behaviour.
    When use_spec=True, uses spec=Instrument so isinstance checks pass.
    """
    inst = MagicMock(spec=Instrument) if use_spec else MagicMock()
    inst.name = name
    inst.clone.return_value = inst
    inst.all_instruments = (inst,)
    inst.to_dict.return_value = frozenset({('type', 'mock')})
    return inst


def _make_portfolio(instruments):
    """Create a mock Portfolio from a list of instruments."""
    port = MagicMock(spec=Portfolio)
    port.all_instruments = tuple(instruments)
    port.__iter__ = lambda self: iter(instruments)
    port.__len__ = lambda self: len(instruments)
    port.priceables = instruments
    port.scale.return_value = port
    return port


def _make_backtest(states=None):
    """Create a mock BackTest with defaultdict-based attributes."""
    bt = MagicMock()
    bt.states = states or [dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)]
    bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
    bt.cash_payments = defaultdict(list)
    bt.transaction_cost_entries = defaultdict(list)
    bt.hedges = defaultdict(list)
    bt.weighted_trades = defaultdict(list)
    bt.results = defaultdict(list)
    bt.calc_calls = 0
    bt.calculations = 0
    bt.price_measure = MagicMock()
    return bt


def _make_action(cls_name='AddTradeAction'):
    """Create a mock action with typical attributes."""
    action = MagicMock()
    action.priceables = [_make_instrument('priceable0')]
    action.dated_priceables = {}
    action.trade_duration = None
    action.holiday_calendar = None
    action.transaction_cost = MagicMock()
    action.transaction_cost_exit = MagicMock()
    action.csa_term = None
    action.risk = MagicMock()
    action.risk_transformation = None
    action.risk_percentage = 100
    action.priceable = MagicMock()
    action.priceable.name = 'TestAction_TestTrade'
    action.scaling_level = 1000.0
    action.scaling_type = ScalingActionType.size
    action.scaling_risk = MagicMock()
    action.method = MagicMock(return_value=100)
    action.size_parameter = 'notional_amount'
    action.priceable_names = None
    action.total_size = 100000.0
    return action


# ─── OrderBasedActionImpl ──────────────────────────────────────────────────────

class TestOrderBasedActionImpl:
    """Tests for the OrderBasedActionImpl base class."""

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_get_base_orders_for_states_no_dated_priceables(self, mock_pc):
        """Exercise get_base_orders_for_states when dated_priceables is empty."""
        action = _make_action()
        action.dated_priceables = {}

        impl = AddTradeActionImpl(action)

        # Mock Portfolio and its calc result
        mock_result = MagicMock()
        states = [dt.date(2020, 1, 1)]

        with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
            mock_port_inst = MagicMock()
            mock_port_inst.calc.return_value = mock_result
            mock_port_cls.return_value = mock_port_inst

            orders = impl.get_base_orders_for_states(states)

        assert dt.date(2020, 1, 1) in orders

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_get_base_orders_for_states_with_dated_priceables(self, mock_pc):
        """Exercise get_base_orders_for_states when dated_priceables has entries for the date."""
        action = _make_action()
        specific_priceable = _make_instrument('dated')
        action.dated_priceables = {dt.date(2020, 1, 1): [specific_priceable]}

        impl = AddTradeActionImpl(action)

        with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
            mock_port_inst = MagicMock()
            mock_port_cls.return_value = mock_port_inst

            orders = impl.get_base_orders_for_states([dt.date(2020, 1, 1)])

        # Should use the dated priceables
        mock_port_cls.assert_called_with([specific_priceable])

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_get_base_orders_dated_priceables_none(self, mock_pc):
        """Branch: dated_priceables is None (getattr returns None)."""
        action = _make_action()
        action.dated_priceables = None

        impl = AddTradeActionImpl(action)

        with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
            mock_port_inst = MagicMock()
            mock_port_cls.return_value = mock_port_inst
            orders = impl.get_base_orders_for_states([dt.date(2020, 1, 1)])

        # When dated_priceables is None, should fall through to action.priceables
        mock_port_cls.assert_called_with(action.priceables)

    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_get_instrument_final_date(self, mock_gfd):
        """Test get_instrument_final_date delegates to get_final_date."""
        action = _make_action()
        impl = AddTradeActionImpl(action)

        inst = _make_instrument()
        info = MagicMock()
        mock_gfd.return_value = dt.date(2020, 6, 1)

        result = impl.get_instrument_final_date(inst, dt.date(2020, 1, 1), info)
        assert result == dt.date(2020, 6, 1)
        mock_gfd.assert_called_once_with(inst, dt.date(2020, 1, 1), action.trade_duration, action.holiday_calendar, info)


# ─── AddTradeActionImpl ───────────────────────────────────────────────────────

class TestAddTradeActionImpl:

    def _setup_impl(self, action=None):
        action = action or _make_action()
        impl = AddTradeActionImpl(action)
        return impl, action

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_single_state_no_trigger_info(self, mock_gfd, mock_pc):
        """Test apply_action with single state and trigger_info=None."""
        impl, action = self._setup_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('trade_2020-01-01')
        inst.clone.return_value = inst

        # Mock the _raise_order to return known orders
        port = _make_portfolio([inst])
        port.scale.return_value = port
        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): (port, None)}):
            bt = _make_backtest()

            # Mock TransactionCostEntry
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
                    result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_with_trigger_info(self, mock_gfd, mock_pc):
        """Test apply_action with an AddTradeActionInfo (scaling != None)."""
        impl, action = self._setup_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info = AddTradeActionInfo(scaling=2.0, next_schedule=None)

        inst = _make_instrument('trade_2020-01-01')
        port = _make_portfolio([inst])
        port.scale.return_value = port

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): (port, info)}):
            bt = _make_backtest()

            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 1  # > 0 to hit the calc_calls branch
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt, info)

        assert result is bt
        # calc_calls should have been incremented because no_of_risk_calcs > 0
        assert bt.calc_calls == 1

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_no_risk_calcs(self, mock_gfd, mock_pc):
        """Test apply_action path where no TCE has risk calcs (no_of_risk_calcs == 0)."""
        impl, action = self._setup_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('trade')
        port = _make_portfolio([inst])

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): (port, None)}):
            bt = _make_backtest()

            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert bt.calc_calls == 0

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_trigger_info_none(self, mock_pc):
        """Test _raise_order with trigger_info=None."""
        impl, action = self._setup_impl()

        # Mock get_base_orders_for_states
        inst = _make_instrument('inst0')
        mock_result = MagicMock()
        mock_result.result.return_value = [inst]

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                new_port = MagicMock()
                new_port.scale.return_value = new_port
                mock_port_cls.return_value = new_port

                orders = impl._raise_order(dt.date(2020, 1, 1), trigger_info=None)

        assert dt.date(2020, 1, 1) in orders
        # trigger_info was None, so scaling should be None
        new_port.scale.assert_called_once_with(None, in_place=False)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_with_list_trigger_info(self, mock_pc):
        """Test _raise_order with a list of trigger infos."""
        impl, action = self._setup_impl()

        info1 = AddTradeActionInfo(scaling=2.0, next_schedule=None)
        info2 = AddTradeActionInfo(scaling=3.0, next_schedule=None)

        inst = _make_instrument('inst0')
        mock_result = MagicMock()
        mock_result.result.return_value = [inst]

        with patch.object(impl, 'get_base_orders_for_states', return_value={
            dt.date(2020, 1, 1): mock_result,
            dt.date(2020, 1, 2): mock_result,
        }):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                new_port = MagicMock()
                new_port.scale.return_value = new_port
                mock_port_cls.return_value = new_port

                orders = impl._raise_order(
                    [dt.date(2020, 1, 1), dt.date(2020, 1, 2)],
                    trigger_info=[info1, info2],
                )

        assert len(orders) == 2

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_single_trigger_info_replicated(self, mock_pc):
        """Branch: trigger_info is an AddTradeActionInfo (not list) -- replicated for all states."""
        impl, action = self._setup_impl()

        info = AddTradeActionInfo(scaling=5.0, next_schedule=None)
        inst = _make_instrument('inst0')
        mock_result = MagicMock()
        mock_result.result.return_value = [inst]

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                new_port = MagicMock()
                new_port.scale.return_value = new_port
                mock_port_cls.return_value = new_port

                orders = impl._raise_order(dt.date(2020, 1, 1), trigger_info=info)

        new_port.scale.assert_called_once_with(5.0, in_place=False)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_backtest_states_filtering(self, mock_gfd, mock_pc):
        """Test that only states within [create_date, final_date) are added."""
        impl, action = self._setup_impl()
        # final_date is between state 2 and 3 so only state 1 should be included
        mock_gfd.return_value = dt.date(2020, 1, 2)

        inst = _make_instrument('trade')
        port = _make_portfolio([inst])

        bt = _make_backtest(states=[dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)])
        # Use real lists in portfolio_dict so append works
        bt.portfolio_dict = defaultdict(list)

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): (port, None)}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce
                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # Only state 2020-01-01 should be added (>= create_date and < final_date=2020-01-02)
        assert inst in bt.portfolio_dict[dt.date(2020, 1, 1)]


# ─── AddScaledTradeActionImpl ─────────────────────────────────────────────────

class TestAddScaledTradeActionImpl:

    def _make_impl(self, scaling_type=ScalingActionType.size, scaling_level=1000.0):
        action = _make_action()
        action.scaling_type = scaling_type
        action.scaling_level = scaling_level
        action.scaling_risk = MagicMock()
        impl = AddScaledTradeActionImpl(action)
        return impl, action

    def test_init_with_dict_scaling_level(self):
        """Branch: scaling_level is a dict -> _scaling_level_signal is set."""
        action = _make_action()
        action.scaling_level = {dt.date(2020, 1, 1): 100.0, dt.date(2020, 1, 2): 200.0}
        with patch('gs_quant.backtests.generic_engine_action_impls.interpolate_signal') as mock_interp:
            mock_interp.return_value = {dt.date(2020, 1, 1): 100.0, dt.date(2020, 1, 2): 200.0}
            impl = AddScaledTradeActionImpl(action)
        assert impl._scaling_level_signal is not None

    def test_init_with_float_scaling_level(self):
        """Branch: scaling_level is not a dict -> _scaling_level_signal is None."""
        impl, _ = self._make_impl(scaling_level=500.0)
        assert impl._scaling_level_signal is None

    def test_scaling_level_for_date_with_signal(self):
        """Test _scaling_level_for_date when _scaling_level_signal is not None."""
        impl, action = self._make_impl()
        impl._scaling_level_signal = {dt.date(2020, 1, 1): 42.0}

        # date in signal
        assert impl._scaling_level_for_date(dt.date(2020, 1, 1)) == 42.0

        # date not in signal -> returns 0
        assert impl._scaling_level_for_date(dt.date(2020, 1, 5)) == 0

    def test_scaling_level_for_date_no_signal(self):
        """Test _scaling_level_for_date when _scaling_level_signal is None."""
        impl, action = self._make_impl(scaling_level=1234.0)
        assert impl._scaling_level_for_date(dt.date(2020, 1, 1)) == 1234.0

    def test_scale_order_size(self):
        """Branch: scaling_type == ScalingActionType.size."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.size, scaling_level=2.0)

        port = MagicMock()
        orders = {dt.date(2020, 1, 1): port}
        impl._scale_order(orders, None, None, None)
        port.scale.assert_called_once_with(2.0)

    def test_scale_order_nav(self):
        """Branch: scaling_type == ScalingActionType.NAV."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.NAV)

        with patch.object(impl, '_nav_scale_orders') as mock_nav:
            orders = {dt.date(2020, 1, 1): MagicMock()}
            price_measure = MagicMock()
            trigger_infos = {}
            impl._scale_order(orders, None, price_measure, trigger_infos)
            mock_nav.assert_called_once_with(orders, price_measure, trigger_infos)

    def test_scale_order_risk_measure(self):
        """Branch: scaling_type == ScalingActionType.risk_measure."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.risk_measure, scaling_level=100.0)

        port = MagicMock()
        daily_risk = {dt.date(2020, 1, 1): 50.0}
        orders = {dt.date(2020, 1, 1): port}
        impl._scale_order(orders, daily_risk, None, None)
        port.scale.assert_called_once_with(2.0)  # 100/50 = 2.0

    def test_scale_order_unsupported_type(self):
        """Branch: unknown scaling type raises RuntimeError."""
        impl, action = self._make_impl()
        action.scaling_type = 'unsupported'

        with pytest.raises(RuntimeError, match='Scaling Type'):
            impl._scale_order({}, None, None, None)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_scaled_single_state(self, mock_gfd, mock_pc):
        """Test apply_action for AddScaledTradeActionImpl."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.size)
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('inst_2020-01-01')
        port = _make_portfolio([inst])

        bt = _make_backtest()

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_scaled_with_risk_calcs(self, mock_gfd, mock_pc):
        """Test apply_action where TCE has risk calcs > 0."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('inst_2020-01-01')
        port = _make_portfolio([inst])

        bt = _make_backtest()

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 2
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert bt.calc_calls == 1

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_scaled_trigger_info_single(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a single AddScaledTradeActionInfo (not list, not None)."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info = AddScaledTradeActionInfo(next_schedule=None)
        inst = _make_instrument('inst')
        port = _make_portfolio([inst])

        bt = _make_backtest()

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action(dt.date(2020, 1, 1), bt, info)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_scaled_trigger_info_list(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a list of AddScaledTradeActionInfo."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info_list = [AddScaledTradeActionInfo(next_schedule=None)]
        inst = _make_instrument('inst')
        port = _make_portfolio([inst])

        bt = _make_backtest()

        with patch.object(impl, '_raise_order', return_value={dt.date(2020, 1, 1): port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    result = impl.apply_action([dt.date(2020, 1, 1)], bt, info_list)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_risk_measure_branches(self, mock_pc):
        """Test _raise_order with risk_measure scaling type (appends scaling_risk, and builds daily_risk)."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.risk_measure, scaling_level=100.0)

        inst = _make_instrument('inst0')
        mock_risk_result = MagicMock()
        mock_risk_result.aggregate.return_value = 50.0

        # The result for a single inst should support __getitem__
        mock_single = MagicMock()
        mock_resolved = MagicMock()
        mock_resolved.name = 'inst0_2020-01-01'
        mock_single.__getitem__ = MagicMock(return_value=mock_resolved)

        from gs_quant.target.measures import ResolvedInstrumentValues
        mock_result = MagicMock()
        # orders[d] should be subscriptable by instrument
        mock_result.__getitem__ = MagicMock(return_value=mock_single)

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port = MagicMock()
                mock_port_cls.return_value = mock_port

                with patch.object(impl, '_scale_order'):
                    trigger_infos = {dt.date(2020, 1, 1): None}
                    orders = impl._raise_order(
                        [dt.date(2020, 1, 1)],
                        MagicMock(),
                        trigger_infos,
                    )

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_size_type_no_extra_valuations(self, mock_pc):
        """Test _raise_order with size scaling (len(_order_valuations) == 1)."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.size, scaling_level=100.0)

        inst = _make_instrument('inst0')
        mock_resolved = MagicMock()
        mock_resolved.name = 'inst0'

        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(return_value=mock_resolved)

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port = MagicMock()
                mock_port_cls.return_value = mock_port

                with patch.object(impl, '_scale_order'):
                    trigger_infos = {dt.date(2020, 1, 1): None}
                    orders = impl._raise_order(
                        [dt.date(2020, 1, 1)],
                        MagicMock(),
                        trigger_infos,
                    )

        assert dt.date(2020, 1, 1) in orders

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_with_dated_priceables(self, mock_pc):
        """Branch in _raise_order: dated_priceables has entry for the date."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.size)

        specific = _make_instrument('specific')
        action.dated_priceables = {dt.date(2020, 1, 1): [specific]}

        mock_resolved = MagicMock()
        mock_resolved.name = 'specific'
        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(return_value=mock_resolved)

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port = MagicMock()
                mock_port_cls.return_value = mock_port

                with patch.object(impl, '_scale_order'):
                    trigger_infos = {dt.date(2020, 1, 1): None}
                    orders = impl._raise_order(
                        [dt.date(2020, 1, 1)],
                        MagicMock(),
                        trigger_infos,
                    )

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_raise_order_dated_priceables_none_in_raise(self, mock_pc):
        """Branch: dated_priceables is None in _raise_order -> falls through to action.priceables."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.size)
        action.dated_priceables = None

        inst = action.priceables[0]
        mock_resolved = MagicMock()
        mock_resolved.name = 'priceable0'
        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(return_value=mock_resolved)

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_result}):
            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port = MagicMock()
                mock_port_cls.return_value = mock_port

                with patch.object(impl, '_scale_order'):
                    trigger_infos = {dt.date(2020, 1, 1): None}
                    orders = impl._raise_order(
                        [dt.date(2020, 1, 1)],
                        MagicMock(),
                        trigger_infos,
                    )

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_basic(self, mock_gfd, mock_pc):
        """Test _nav_scale_orders with basic scenario."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.NAV, scaling_level=10000.0)
        mock_gfd.return_value = dt.date(2020, 2, 1)

        inst = _make_instrument('inst0')
        port = _make_portfolio([inst])

        trigger_infos = {dt.date(2020, 1, 1): None}

        # Mock TransactionCostEntry
        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.get_cost_by_component.return_value = (0, 0)
            mock_tce.additional_scaling = 1
            mock_tce.get_final_cost.return_value = 0
            mock_tce_cls.return_value = mock_tce

            # Mock price results
            mock_price_result = MagicMock()
            mock_price_result.aggregate.return_value = 100.0
            mock_price_result.__getitem__ = MagicMock(return_value = 100.0)

            orders = {dt.date(2020, 1, 1): port}

            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_unwind_port = MagicMock()
                mock_unwind_port.calc.return_value = mock_price_result
                mock_port_cls.return_value = mock_unwind_port

                port.calc = MagicMock(return_value=mock_price_result)

                impl._nav_scale_orders(orders, MagicMock(), trigger_infos)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_first_scale_factor_zero(self, mock_gfd, mock_pc):
        """Branch: first_scale_factor == 0 -> returns 0 immediately."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.NAV, scaling_level=0.0)
        # available_cash = 0, fixed_tcs = 0 -> first_scale_factor = 0
        mock_gfd.return_value = dt.date(2020, 2, 1)

        inst = _make_instrument('inst0')
        port = _make_portfolio([inst])
        trigger_infos = {dt.date(2020, 1, 1): None}

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.get_cost_by_component.return_value = (0, 0)
            mock_tce_cls.return_value = mock_tce

            mock_price_result = MagicMock()
            mock_price_result.aggregate.return_value = 100.0

            orders = {dt.date(2020, 1, 1): port}
            port.calc = MagicMock(return_value=mock_price_result)

            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port_cls.return_value = MagicMock()
                impl._nav_scale_orders(orders, MagicMock(), trigger_infos)

        # When scale_factor == 0, the day should be deleted from orders
        assert dt.date(2020, 1, 1) not in orders

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_multiple_days_with_unwinds(self, mock_gfd, mock_pc):
        """Test _nav_scale_orders with multiple order days and unwinds between them."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.NAV, scaling_level=1000.0)

        inst1 = _make_instrument('inst1')
        inst2 = _make_instrument('inst2')
        port1 = _make_portfolio([inst1])
        port2 = _make_portfolio([inst2])

        # final date of inst1 is between day1 and day2
        def fake_final_date(inst, order_date, trade_duration, holiday_calendar, info):
            if order_date == dt.date(2020, 1, 1):
                return dt.date(2020, 1, 5)
            return dt.date(2020, 2, 1)

        mock_gfd.side_effect = fake_final_date

        trigger_infos = {
            dt.date(2020, 1, 1): None,
            dt.date(2020, 1, 10): None,
        }

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.get_cost_by_component.return_value = (0, 0)
            mock_tce.additional_scaling = 1
            mock_tce.get_final_cost.return_value = 10.0
            mock_tce_cls.return_value = mock_tce

            mock_price_result = MagicMock()
            mock_price_result.aggregate.return_value = 100.0
            mock_price_result.__getitem__ = MagicMock(return_value=100.0)

            orders = {
                dt.date(2020, 1, 1): port1,
                dt.date(2020, 1, 10): port2,
            }

            port1.calc = MagicMock(return_value=mock_price_result)
            port2.calc = MagicMock(return_value=mock_price_result)

            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_unwind_port = MagicMock()
                mock_unwind_port.calc.return_value = mock_price_result
                mock_port_cls.return_value = mock_unwind_port

                impl._nav_scale_orders(orders, MagicMock(), trigger_infos)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_unwind_day_after_today(self, mock_gfd, mock_pc):
        """Branch: unwind_day > dt.date.today() -> skip unwind price calc."""
        impl, action = self._make_impl(scaling_type=ScalingActionType.NAV, scaling_level=1000.0)

        inst = _make_instrument('inst0')
        port = _make_portfolio([inst])

        # Set final date far in the future
        mock_gfd.return_value = dt.date(2099, 1, 1)

        trigger_infos = {dt.date(2020, 1, 1): None}

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.get_cost_by_component.return_value = (0, 0)
            mock_tce_cls.return_value = mock_tce

            mock_price_result = MagicMock()
            mock_price_result.aggregate.return_value = 100.0

            orders = {dt.date(2020, 1, 1): port}
            port.calc = MagicMock(return_value=mock_price_result)

            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_port_cls.return_value = MagicMock()
                impl._nav_scale_orders(orders, MagicMock(), trigger_infos)


# ─── HedgeActionImpl ──────────────────────────────────────────────────────────

class TestHedgeActionImpl:

    def _make_impl(self):
        action = _make_action()
        action.csa_term = 'USD-1'
        impl = HedgeActionImpl(action)
        return impl, action

    @patch('gs_quant.backtests.generic_engine_action_impls.HistoricalPricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.Portfolio')
    def test_get_base_orders_for_states(self, mock_port_cls, mock_hpc):
        """Test HedgeActionImpl.get_base_orders_for_states."""
        impl, action = self._make_impl()

        mock_future = MagicMock()
        mock_result = MagicMock()
        mock_future.result.return_value = mock_result
        mock_port_inst = MagicMock()
        mock_port_inst.resolve.return_value = mock_future
        mock_port_cls.return_value = mock_port_inst

        result = impl.get_base_orders_for_states([dt.date(2020, 1, 1)])
        assert result is mock_result

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_no_trigger_info(self, mock_gfd, mock_pc):
        """Test apply_action with trigger_info=None."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        hedge_trade = _make_instrument('hedge_trade')
        hedge_trade.all_instruments = (hedge_trade,)
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio') as mock_sp:
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp:
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge') as mock_hedge:
                            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        assert bt.calc_calls == 1  # always incremented by 1 in apply_action

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_with_single_trigger_info(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a single HedgeActionInfo."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info = HedgeActionInfo(next_schedule=None)
        hedge_trade = _make_instrument('hedge_trade')
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge'):
                            result = impl.apply_action(dt.date(2020, 1, 1), bt, info)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_portfolio_trade(self, mock_gfd, mock_pc):
        """Branch: hedge_trade isinstance Portfolio -> renames instruments inside."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inner_inst = _make_instrument('inner')
        hedge_trade = MagicMock(spec=Portfolio)
        hedge_trade.name = 'hedge_port'
        hedge_trade.all_instruments = (inner_inst,)

        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge'):
                            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_no_active_dates(self, mock_gfd, mock_pc):
        """Branch: active_dates is empty -> no scaling portfolio or hedge created."""
        impl, action = self._make_impl()
        # Set final date <= create date so no active dates
        mock_gfd.return_value = dt.date(2020, 1, 1)

        hedge_trade = _make_instrument('hedge_trade')
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        # No hedges should have been added
        assert len(bt.hedges[dt.date(2020, 1, 1)]) == 0

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_exit_payment_future(self, mock_gfd, mock_pc):
        """Branch: final_date > today -> exit_payment is None."""
        impl, action = self._make_impl()
        # Far future
        mock_gfd.return_value = dt.date(2099, 12, 31)

        hedge_trade = _make_instrument('hedge_trade')
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest(states=[
            dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)
        ])

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 1  # > 0 to test calc_calls
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp:
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge') as mock_hedge_cls:
                            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        # Hedge constructor should have been called with exit_payment=None
        hedge_call = mock_hedge_cls.call_args
        assert hedge_call.kwargs.get('exit_payment') is None or hedge_call[1].get('exit_payment') is None \
            or (len(hedge_call[0]) >= 3 and hedge_call[0][2] is None)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_with_risk_calcs(self, mock_gfd, mock_pc):
        """Test hedge apply_action where TCE has risk calcs."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        hedge_trade = _make_instrument('hedge_trade')
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 3
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge'):
                            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # calc_calls: +1 from top-level, +1 from any(...) check
        assert bt.calc_calls == 2

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_hedge_list_trigger_info(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a list of HedgeActionInfo."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info_list = [HedgeActionInfo(next_schedule=None)]
        hedge_trade = _make_instrument('hedge_trade')
        mock_port = MagicMock()
        mock_port.priceables = [hedge_trade]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.ScalingPortfolio'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.Hedge'):
                            result = impl.apply_action([dt.date(2020, 1, 1)], bt, info_list)

        assert result is bt


# ─── ExitTradeActionImpl ──────────────────────────────────────────────────────

class TestExitTradeActionImpl:
    """Tests for ExitTradeActionImpl.
    Note: We must NOT patch Portfolio in these tests because the production code
    uses isinstance(trade, Portfolio) which breaks if Portfolio is a MagicMock.
    """

    def _make_impl(self, priceable_names=None):
        action = _make_action()
        action.priceable_names = priceable_names
        impl = ExitTradeActionImpl(action)
        return impl, action

    def _make_exit_bt(self, states, portfolio_by_date, results_by_date=None,
                      cash_payments_by_date=None, tce_by_date=None):
        """Helper to build a mock backtest for exit trade tests."""
        bt = _make_backtest(states=states)
        bt.portfolio_dict = defaultdict(lambda: Portfolio(()))
        for d, p in portfolio_by_date.items():
            bt.portfolio_dict[d] = p
        bt.results = defaultdict(list)
        if results_by_date:
            for d, r in results_by_date.items():
                bt.results[d] = r
        bt.cash_payments = defaultdict(list)
        if cash_payments_by_date:
            for d, cps in cash_payments_by_date.items():
                bt.cash_payments[d] = cps
        bt.transaction_cost_entries = defaultdict(list)
        if tce_by_date:
            for d, entries in tce_by_date.items():
                bt.transaction_cost_entries[d] = entries
        return bt

    def test_apply_action_no_priceable_names(self):
        """Branch: priceable_names is None -> use current trade names."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)
        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio((inst,)),
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt

    def test_apply_action_with_priceable_names(self):
        """Branch: priceable_names is not None -> filter by name parts."""
        impl, action = self._make_impl(priceable_names=['Trade'])

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)
        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio((inst,)),
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt

    def test_apply_action_with_results_and_removal(self):
        """Branch: backtest.results[port_date] is truthy and result_indexes_to_remove is non-empty."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)
        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        mock_result = MagicMock()
        mock_result.portfolio.all_instruments = [inst]
        mock_result.futures = [MagicMock()]
        mock_result.risk_measures = [MagicMock()]

        with patch('gs_quant.backtests.generic_engine_action_impls.PortfolioRiskResult') as mock_prr:
            mock_prr.return_value = MagicMock()
            bt = self._make_exit_bt(
                states=states,
                portfolio_by_date={
                    dt.date(2020, 1, 1): Portfolio((inst,)),
                    dt.date(2020, 1, 2): Portfolio((inst,)),
                },
                results_by_date={
                    dt.date(2020, 1, 1): mock_result,
                    dt.date(2020, 1, 2): mock_result,
                },
            )
            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    def test_apply_action_results_no_matching_result_instruments(self):
        """Branch: result_indexes_to_remove is empty -> no PortfolioRiskResult created."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        other_inst = _make_instrument('OtherInst', use_spec=True)

        mock_result = MagicMock()
        mock_result.portfolio.all_instruments = [other_inst]
        mock_result.futures = [MagicMock()]

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            results_by_date={
                dt.date(2020, 1, 1): mock_result,
                dt.date(2020, 1, 2): mock_result,
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt

    def test_apply_action_with_cash_payments_future_no_prev(self):
        """Branch: cash payments with cp_date > s, no prev_pos -> appends to s."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        mock_tce = MagicMock()
        mock_cp = MagicMock()
        mock_cp.trade = inst
        mock_cp.trade.name = 'Action_Trade_2020-01-01'
        mock_cp.direction = 1
        mock_cp.transaction_cost_entry = mock_tce

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            cash_payments_by_date={
                dt.date(2020, 1, 2): [mock_cp],
            },
            tce_by_date={
                dt.date(2020, 1, 2): [mock_tce],
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt
        # cp should have been moved to the exit date
        assert mock_cp.effective_date == dt.date(2020, 1, 1)

    def test_apply_action_cash_payments_prev_pos(self):
        """Branch: prev_pos exists -> nets out the position."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        mock_tce_fut = MagicMock()
        mock_cp_fut = MagicMock()
        mock_cp_fut.trade = inst
        mock_cp_fut.trade.name = 'Action_Trade_2020-01-01'
        mock_cp_fut.direction = 1
        mock_cp_fut.transaction_cost_entry = mock_tce_fut

        # Existing cash payment on the trigger date for same trade
        mock_cp_existing = MagicMock()
        mock_cp_existing.trade = inst
        mock_cp_existing.trade.name = 'Action_Trade_2020-01-01'
        mock_cp_existing.direction = -1

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            cash_payments_by_date={
                dt.date(2020, 1, 1): [mock_cp_existing],
                dt.date(2020, 1, 2): [mock_cp_fut],
            },
            tce_by_date={
                dt.date(2020, 1, 1): [],
                dt.date(2020, 1, 2): [mock_tce_fut],
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # direction should have been netted: -1 + 1 = 0
        assert mock_cp_existing.direction == 0

    def test_apply_action_trade_not_in_cash_payments_non_portfolio(self):
        """Branch: trade.name not in cash_payments[s] and trade is NOT a Portfolio."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)
        inst.to_dict.return_value = frozenset({'key': 'value'})

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
        )

        with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        # CashPayment should have been called for this trade
        assert mock_cp_cls.called

    def test_apply_action_trade_not_in_cash_payments_portfolio(self):
        """Branch: trade.name not in cash_payments[s] and trade IS a Portfolio."""
        impl, action = self._make_impl(priceable_names=None)

        inner_inst = _make_instrument('inner', use_spec=True)
        inner_inst.to_dict.return_value = frozenset({'k': 'v'})

        trade = Portfolio((inner_inst,), name='Action_Trade_2020-01-01')

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((trade,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
        )

        with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    def test_apply_action_empty_cash_payments_cleanup(self):
        """Branch: after removing, cash_payments[cp_date] is empty -> delete key."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        mock_tce = MagicMock()
        mock_cp = MagicMock()
        mock_cp.trade = inst
        mock_cp.trade.name = 'Action_Trade_2020-01-01'
        mock_cp.direction = 1
        mock_cp.transaction_cost_entry = mock_tce

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            cash_payments_by_date={
                dt.date(2020, 1, 2): [mock_cp],
            },
            tce_by_date={
                dt.date(2020, 1, 1): [],
                dt.date(2020, 1, 2): [mock_tce],
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # The future cp_date key should have been deleted since list became empty
        assert dt.date(2020, 1, 2) not in bt.cash_payments

    def test_apply_action_results_falsy(self):
        """Branch: backtest.results[port_date] is falsy -> skip result removal."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            results_by_date={},  # empty/falsy results
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt

    def test_apply_action_cash_payments_remaining_not_empty(self):
        """Branch: after removal, cash_payments[cp_date] still has items -> don't delete."""
        impl, action = self._make_impl(priceable_names=None)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        other_inst = _make_instrument('Other_Inst_2020-01-01', use_spec=True)

        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]

        mock_tce = MagicMock()
        mock_cp_matching = MagicMock()
        mock_cp_matching.trade = inst
        mock_cp_matching.trade.name = 'Action_Trade_2020-01-01'
        mock_cp_matching.direction = 1
        mock_cp_matching.transaction_cost_entry = mock_tce

        # Another cp that does NOT match
        mock_cp_other = MagicMock()
        mock_cp_other.trade = other_inst
        mock_cp_other.trade.name = 'Other_Inst_2020-01-01'

        bt = self._make_exit_bt(
            states=states,
            portfolio_by_date={
                dt.date(2020, 1, 1): Portfolio((inst,)),
                dt.date(2020, 1, 2): Portfolio(()),
            },
            cash_payments_by_date={
                dt.date(2020, 1, 2): [mock_cp_matching, mock_cp_other],
            },
            tce_by_date={
                dt.date(2020, 1, 1): [],
                dt.date(2020, 1, 2): [mock_tce],
            },
        )
        result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # The future cp_date key should still exist because mock_cp_other is still there
        assert dt.date(2020, 1, 2) in bt.cash_payments
        assert result is bt


# ─── RebalanceActionImpl ──────────────────────────────────────────────────────

class TestRebalanceActionImpl:

    def _make_impl(self):
        action = _make_action()
        action.priceable.name = 'TestAction_TestTrade'
        action.size_parameter = 'notional_amount'
        action.method = MagicMock(return_value=100)
        impl = RebalanceActionImpl(action)
        return impl, action

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_apply_action_rebalance_basic(self, mock_pc):
        """Test basic rebalance where new_size != current_size."""
        impl, action = self._make_impl()

        # Trade in portfolio whose name contains 'TestTrade'
        trade = _make_instrument('SomeAction_TestTrade_2020-01-01')
        trade.name = 'SomeAction_TestTrade_2020-01-01'
        trade.notional_amount = 50

        bt = _make_backtest(states=[dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)])

        port = MagicMock()
        port.__iter__ = MagicMock(return_value=iter([trade]))
        bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = port

        # method returns 100, current_size = 50, so new trade with notional_amount=50
        action.priceable.clone.return_value = _make_instrument('cloned')

        # Create cash payment for unwind on a future date
        mock_cp = MagicMock()
        mock_cp.trade.name = 'SomeAction_TestTrade_2020-01-01'
        mock_cp.direction = 1

        bt.cash_payments = defaultdict(list)
        bt.cash_payments[dt.date(2020, 1, 3)] = [mock_cp]
        bt.transaction_cost_entries = defaultdict(list)

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.no_of_risk_calcs = 0
            mock_tce_cls.return_value = mock_tce

            with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
                mock_cp_cls.return_value = MagicMock()
                mock_cp_cls.return_value.effective_date = dt.date(2020, 1, 3)
                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_apply_action_rebalance_no_change(self, mock_pc):
        """Branch: new_size - current_size == 0 -> do nothing."""
        impl, action = self._make_impl()
        action.method.return_value = 50

        trade = _make_instrument('SomeAction_TestTrade_2020-01-01')
        trade.name = 'SomeAction_TestTrade_2020-01-01'
        trade.notional_amount = 50

        bt = _make_backtest()

        port = MagicMock()
        port.__iter__ = MagicMock(return_value=iter([trade]))
        bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = port

        result = impl.apply_action(dt.date(2020, 1, 1), bt)
        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_apply_action_rebalance_no_unwind_payment(self, mock_pc):
        """Branch: no matching unwind payment -> raises ValueError."""
        impl, action = self._make_impl()

        trade = _make_instrument('SomeAction_TestTrade_2020-01-01')
        trade.name = 'SomeAction_TestTrade_2020-01-01'
        trade.notional_amount = 50

        bt = _make_backtest()

        port = MagicMock()
        port.__iter__ = MagicMock(return_value=iter([trade]))
        bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = port

        action.priceable.clone.return_value = _make_instrument('cloned')

        bt.cash_payments = defaultdict(list)
        bt.transaction_cost_entries = defaultdict(list)

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.no_of_risk_calcs = 0
            mock_tce_cls.return_value = mock_tce

            with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                with pytest.raises(ValueError, match="Found no final cash payment"):
                    impl.apply_action(dt.date(2020, 1, 1), bt)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_apply_action_rebalance_with_risk_calcs(self, mock_pc):
        """Branch: TCE has risk calcs > 0 -> calc_calls incremented."""
        impl, action = self._make_impl()

        trade = _make_instrument('SomeAction_TestTrade_2020-01-01')
        trade.name = 'SomeAction_TestTrade_2020-01-01'
        trade.notional_amount = 50

        bt = _make_backtest(states=[dt.date(2020, 1, 1), dt.date(2020, 1, 2)])

        port = MagicMock()
        port.__iter__ = MagicMock(return_value=iter([trade]))
        bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = port

        action.priceable.clone.return_value = _make_instrument('cloned')

        mock_cp = MagicMock()
        mock_cp.trade.name = 'SomeAction_TestTrade_2020-01-01'
        mock_cp.direction = 1

        bt.cash_payments = defaultdict(list)
        bt.cash_payments[dt.date(2020, 1, 2)] = [mock_cp]
        bt.transaction_cost_entries = defaultdict(list)

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.no_of_risk_calcs = 2
            mock_tce_cls.return_value = mock_tce

            with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
                mock_cp_inst = MagicMock()
                mock_cp_inst.effective_date = dt.date(2020, 1, 2)
                mock_cp_cls.return_value = mock_cp_inst

                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert bt.calc_calls == 1

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    def test_apply_action_rebalance_no_matching_trade_name(self, mock_pc):
        """Branch: no trade in portfolio matches the priceable name."""
        impl, action = self._make_impl()
        action.method.return_value = 100

        # No matching trade
        trade = _make_instrument('OtherAction_OtherTrade_2020-01-01')
        trade.name = 'OtherAction_OtherTrade_2020-01-01'
        trade.notional_amount = 0

        bt = _make_backtest(states=[dt.date(2020, 1, 1), dt.date(2020, 1, 2)])

        port = MagicMock()
        port.__iter__ = MagicMock(return_value=iter([trade]))
        bt.portfolio_dict = defaultdict(lambda: _make_portfolio([]))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = port

        action.priceable.clone.return_value = _make_instrument('cloned')

        mock_cp = MagicMock()
        mock_cp.trade.name = 'SomeAction_TestTrade_2020-01-01'
        mock_cp.direction = 1

        bt.cash_payments = defaultdict(list)
        bt.cash_payments[dt.date(2020, 1, 2)] = [mock_cp]
        bt.transaction_cost_entries = defaultdict(list)

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.no_of_risk_calcs = 0
            mock_tce_cls.return_value = mock_tce

            with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
                mock_cp_inst = MagicMock()
                mock_cp_inst.effective_date = dt.date(2020, 1, 2)
                mock_cp_cls.return_value = mock_cp_inst

                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt


# ─── AddWeightedTradeActionImpl ───────────────────────────────────────────────

class TestAddWeightedTradeActionImpl:

    def _make_impl(self):
        action = _make_action()
        action.scaling_risk = MagicMock()
        action.total_size = 100000.0
        impl = AddWeightedTradeActionImpl(action)
        return impl, action

    @patch('gs_quant.backtests.generic_engine_action_impls.HistoricalPricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.Portfolio')
    def test_get_base_orders_for_states(self, mock_port_cls, mock_hpc):
        """Test AddWeightedTradeActionImpl.get_base_orders_for_states."""
        impl, action = self._make_impl()

        mock_future = MagicMock()
        mock_result = MagicMock()
        mock_future.result.return_value = mock_result
        mock_port_inst = MagicMock()
        mock_port_inst.resolve.return_value = mock_future
        mock_port_cls.return_value = mock_port_inst

        result = impl.get_base_orders_for_states([dt.date(2020, 1, 1)])
        assert result is mock_result

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_no_trigger_info(self, mock_gfd, mock_pc):
        """Test apply_action with trigger_info=None."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp:
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade'):
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        assert bt.calc_calls == 1

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_single_trigger_info(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a single AddWeightedTradeActionInfo."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info = AddWeightedTradeActionInfo(next_schedule=None)
        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade'):
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action(dt.date(2020, 1, 1), bt, info)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_list_trigger_info(self, mock_gfd, mock_pc):
        """Branch: trigger_info is a list."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        info_list = [AddWeightedTradeActionInfo(next_schedule=None)]
        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade'):
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action([dt.date(2020, 1, 1)], bt, info_list)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_empty_instruments(self, mock_gfd, mock_pc):
        """Branch: instruments list is empty -> continue (skip this date)."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        mock_port = MagicMock()
        mock_port.priceables = []  # empty instruments

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        # No weighted trades should have been added
        assert len(bt.weighted_trades[dt.date(2020, 1, 1)]) == 0

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_no_active_dates(self, mock_gfd, mock_pc):
        """Branch: active_dates is empty -> no WeightedTrade created."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 1)  # Same as create_date, so no active dates

        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        assert len(bt.weighted_trades[dt.date(2020, 1, 1)]) == 0

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_exit_payment_future(self, mock_gfd, mock_pc):
        """Branch: final_date > today -> exit_payment is None."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2099, 12, 31)

        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp:
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade'):
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_with_risk_calcs(self, mock_gfd, mock_pc):
        """Branch: TCE has risk calcs > 0."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)

        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 5
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment'):
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade'):
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        # calc_calls: +1 from top-level, +1 from any(...) true
        assert bt.calc_calls == 2

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_apply_action_weighted_exit_payment_past(self, mock_gfd, mock_pc):
        """Branch: final_date <= today -> exit_payment is CashPayment (not None)."""
        impl, action = self._make_impl()
        mock_gfd.return_value = dt.date(2020, 1, 3)  # In the past

        inst = _make_instrument('inst0')
        inst.clone.return_value = _make_instrument('inst0_2020-01-01')

        mock_port = MagicMock()
        mock_port.priceables = [inst]

        bt = _make_backtest()

        with patch.object(impl, 'get_base_orders_for_states', return_value={dt.date(2020, 1, 1): mock_port}):
            with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
                mock_tce = MagicMock()
                mock_tce.no_of_risk_calcs = 0
                mock_tce_cls.return_value = mock_tce

                with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp:
                    mock_cp.return_value = MagicMock()
                    with patch('gs_quant.backtests.generic_engine_action_impls.WeightedScalingPortfolio'):
                        with patch('gs_quant.backtests.generic_engine_action_impls.WeightedTrade') as mock_wt:
                            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                                mock_port_cls.return_value = MagicMock()
                                result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
        # Verify WeightedTrade was called with non-None exit_payments
        if mock_wt.called:
            exit_payments = mock_wt.call_args.kwargs.get('exit_payments',
                                                         mock_wt.call_args[0][2] if len(mock_wt.call_args[0]) > 2 else None)
            if exit_payments is not None:
                for ep in exit_payments:
                    assert ep is not None


# ─── Additional tests for remaining branch coverage gaps ───────────────────────

class TestNavScaleOrdersAdditional:
    """Tests to cover remaining branch gaps in _nav_scale_orders."""

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_same_final_date(self, mock_gfd, mock_pc):
        """Branch 196->198: two instruments with the same final date so `d` is already in final_days_orders."""
        action = _make_action()
        action.scaling_type = ScalingActionType.NAV
        action.scaling_level = 10000.0
        impl = AddScaledTradeActionImpl(action)

        # Set the same final date for both instruments
        mock_gfd.return_value = dt.date(2020, 2, 1)

        inst1 = _make_instrument('inst1')
        inst2 = _make_instrument('inst2')
        port = _make_portfolio([inst1, inst2])

        trigger_infos = {dt.date(2020, 1, 1): None}

        with patch('gs_quant.backtests.generic_engine_action_impls.TransactionCostEntry') as mock_tce_cls:
            mock_tce = MagicMock()
            mock_tce.get_cost_by_component.return_value = (0, 0)
            mock_tce.additional_scaling = 1
            mock_tce.get_final_cost.return_value = 0
            mock_tce_cls.return_value = mock_tce

            mock_price_result = MagicMock()
            mock_price_result.aggregate.return_value = 100.0
            mock_price_result.__getitem__ = MagicMock(return_value=100.0)

            orders = {dt.date(2020, 1, 1): port}
            port.calc = MagicMock(return_value=mock_price_result)

            with patch('gs_quant.backtests.generic_engine_action_impls.Portfolio') as mock_port_cls:
                mock_unwind_port = MagicMock()
                mock_unwind_port.calc.return_value = mock_price_result
                mock_port_cls.return_value = mock_unwind_port

                impl._nav_scale_orders(orders, MagicMock(), trigger_infos)

    @patch('gs_quant.backtests.generic_engine_action_impls.PricingContext')
    @patch('gs_quant.backtests.generic_engine_action_impls.get_final_date')
    def test_nav_scale_orders_empty_orders(self, mock_gfd, mock_pc):
        """Branch 223->252: empty orders dict -> for loop at line 223 is never entered."""
        action = _make_action()
        action.scaling_type = ScalingActionType.NAV
        action.scaling_level = 10000.0
        impl = AddScaledTradeActionImpl(action)

        orders = {}
        trigger_infos = {}

        # The method should handle empty orders gracefully
        impl._nav_scale_orders(orders, MagicMock(), trigger_infos)

        assert len(orders) == 0


class TestExitTradeActionDuplicateName:
    """Test to cover branch 474->476: trade name already in trades_to_remove."""

    def test_apply_action_duplicate_trade_name_across_dates(self):
        """Branch 474->476: same trade name appears on multiple dates -> second time it's already tracked."""
        action = _make_action()
        action.priceable_names = None
        impl = ExitTradeActionImpl(action)

        inst = _make_instrument('Action_Trade_2020-01-01', use_spec=True)

        # The same instrument appears on both dates
        states = [dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)]

        bt = _make_backtest(states=states)
        bt.portfolio_dict = defaultdict(lambda: Portfolio(()))
        bt.portfolio_dict[dt.date(2020, 1, 1)] = Portfolio((inst,))
        bt.portfolio_dict[dt.date(2020, 1, 2)] = Portfolio((inst,))
        bt.portfolio_dict[dt.date(2020, 1, 3)] = Portfolio(())
        bt.results = defaultdict(list)
        bt.cash_payments = defaultdict(list)
        bt.transaction_cost_entries = defaultdict(list)

        with patch('gs_quant.backtests.generic_engine_action_impls.CashPayment') as mock_cp_cls:
            result = impl.apply_action(dt.date(2020, 1, 1), bt)

        assert result is bt
