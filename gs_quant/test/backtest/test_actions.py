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
import warnings
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

import gs_quant.backtests.actions as actions_module
from gs_quant.backtests.actions import (
    Action,
    AddTradeAction,
    AddTradeActionInfo,
    AddScaledTradeAction,
    AddScaledTradeActionInfo,
    EnterPositionQuantityScaledAction,
    ExitTradeAction,
    ExitAllPositionsAction,
    ExitPositionAction,
    HedgeAction,
    HedgeActionInfo,
    RebalanceAction,
    ScalingActionType,
    default_transaction_cost,
)
from gs_quant.backtests.backtest_objects import ConstantTransactionModel
from gs_quant.backtests.backtest_utils import CalcType
from gs_quant.markets.portfolio import Portfolio


# ============================================================
# Helper to create a mock priceable
# ============================================================

def make_mock_priceable(name=None):
    """Create a mock priceable with clone support."""
    mock = MagicMock()
    mock.name = name
    mock.clone.return_value = MagicMock()
    mock.clone.return_value.name = name
    # Make clone return something with the given name by default
    def clone_side_effect(**kwargs):
        result = MagicMock()
        result.name = kwargs.get('name', name)
        return result
    mock.clone.side_effect = clone_side_effect
    return mock


# ============================================================
# ScalingActionType enum
# ============================================================

class TestScalingActionType:
    def test_values(self):
        assert ScalingActionType.risk_measure.value == 'risk_measure'
        assert ScalingActionType.size.value == 'size'
        assert ScalingActionType.NAV.value == 'NAV'


# ============================================================
# default_transaction_cost
# ============================================================

class TestDefaultTransactionCost:
    def test_returns_constant_model_zero(self):
        tc = default_transaction_cost()
        assert isinstance(tc, ConstantTransactionModel)
        assert tc.cost == 0


# ============================================================
# Action (base class)
# ============================================================

class TestAction:
    def test_sub_classes_registered(self):
        subs = Action.sub_classes()
        assert isinstance(subs, tuple)
        assert AddTradeAction in subs
        assert HedgeAction in subs
        assert RebalanceAction in subs

    def test_set_name_auto_generates(self):
        action = Action()
        assert action.name is not None
        assert action.name.startswith('Action')

    def test_set_name_preserves_existing_name(self):
        # When a name is pre-set, set_name should not overwrite it
        action = Action.__new__(Action)
        action.name = 'MyCustomAction'
        action.set_name('MyCustomAction')
        assert action.name == 'MyCustomAction'

    def test_set_name_auto_increments(self):
        initial_count = actions_module.action_count
        a1 = Action()
        a2 = Action()
        # Each unnamed action gets an incrementing name
        assert a1.name != a2.name

    def test_calc_type_default(self):
        action = Action()
        assert action.calc_type == CalcType.simple

    def test_risk_default(self):
        action = Action()
        assert action.risk is None

    def test_transaction_cost_default(self):
        action = Action()
        assert isinstance(action.transaction_cost, ConstantTransactionModel)

    def test_transaction_cost_setter(self):
        action = Action()
        new_tc = ConstantTransactionModel(5)
        action.transaction_cost = new_tc
        assert action.transaction_cost == new_tc

    def test_transaction_cost_exit_default(self):
        action = Action()
        assert action.transaction_cost_exit is None

    def test_transaction_cost_exit_setter(self):
        action = Action()
        new_tc = ConstantTransactionModel(3)
        action.transaction_cost_exit = new_tc
        assert action.transaction_cost_exit == new_tc


# ============================================================
# AddTradeAction
# ============================================================

class TestAddTradeAction:
    def test_priceable_name_is_none_gets_auto_named(self):
        p = make_mock_priceable(name=None)

        action = AddTradeAction(priceables=[p], name='TestAdd')
        assert len(action.priceables) == 1
        assert action.priceables[0].name == 'TestAdd_Priceable0'

    def test_priceable_name_starts_with_action_name(self):
        p = make_mock_priceable(name='TestAdd_existing')

        action = AddTradeAction(priceables=[p], name='TestAdd')
        assert len(action.priceables) == 1
        # Name already starts with action name => keep as-is
        assert action.priceables[0] is p

    def test_priceable_name_gets_prefixed(self):
        p = make_mock_priceable(name='MySwap')

        action = AddTradeAction(priceables=[p], name='TestAdd')
        assert len(action.priceables) == 1
        assert action.priceables[0].name == 'TestAdd_MySwap'

    def test_multiple_priceables(self):
        p1 = make_mock_priceable(name=None)
        p2 = make_mock_priceable(name='Existing')

        action = AddTradeAction(priceables=[p1, p2], name='Multi')
        assert len(action.priceables) == 2
        assert action.priceables[0].name == 'Multi_Priceable0'
        assert action.priceables[1].name == 'Multi_Existing'

    def test_auto_name_when_name_is_none(self):
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=p)
        assert action.name is not None
        assert action.name.startswith('Action')

    def test_transaction_cost_none_defaults(self):
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=[p], name='TC', transaction_cost=None)
        assert isinstance(action.transaction_cost, ConstantTransactionModel)
        assert action.transaction_cost.cost == 0

    def test_transaction_cost_exit_defaults_to_entry(self):
        tc = ConstantTransactionModel(10)
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=[p], name='TCExit', transaction_cost=tc, transaction_cost_exit=None)
        assert action.transaction_cost_exit == tc

    def test_transaction_cost_exit_explicit(self):
        tc_entry = ConstantTransactionModel(10)
        tc_exit = ConstantTransactionModel(5)
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=[p], name='TCExitExplicit',
                                transaction_cost=tc_entry, transaction_cost_exit=tc_exit)
        assert action.transaction_cost_exit == tc_exit

    def test_set_dated_priceables(self):
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=[p], name='Dated')

        mock_p = MagicMock()
        action.set_dated_priceables(dt.date(2021, 1, 4), mock_p)
        assert dt.date(2021, 1, 4) in action.dated_priceables

    def test_dated_priceables_empty_initially(self):
        p = make_mock_priceable(name=None)
        action = AddTradeAction(priceables=[p], name='EmptyDated')
        assert action.dated_priceables == {}


# ============================================================
# AddScaledTradeAction
# ============================================================

class TestAddScaledTradeAction:
    def test_priceable_name_none_auto_named(self):
        p = make_mock_priceable(name=None)
        action = AddScaledTradeAction(priceables=[p], name='Scaled')
        assert len(action.priceables) == 1
        assert action.priceables[0].name == 'Scaled_Priceable0'

    def test_priceable_name_starts_with_action_name_kept(self):
        p = make_mock_priceable(name='Scaled_existing')
        action = AddScaledTradeAction(priceables=[p], name='Scaled')
        assert action.priceables[0] is p

    def test_priceable_name_prefixed(self):
        p = make_mock_priceable(name='Swap')
        action = AddScaledTradeAction(priceables=[p], name='Scaled')
        assert action.priceables[0].name == 'Scaled_Swap'

    def test_transaction_cost_exit_defaults_to_entry(self):
        tc = ConstantTransactionModel(10)
        p = make_mock_priceable(name=None)
        action = AddScaledTradeAction(priceables=[p], name='ScaledTC', transaction_cost=tc)
        assert action.transaction_cost_exit == tc

    def test_transaction_cost_exit_explicit(self):
        tc_entry = ConstantTransactionModel(10)
        tc_exit = ConstantTransactionModel(5)
        p = make_mock_priceable(name=None)
        action = AddScaledTradeAction(priceables=[p], name='ScaledTCE',
                                      transaction_cost=tc_entry, transaction_cost_exit=tc_exit)
        assert action.transaction_cost_exit == tc_exit

    def test_default_scaling_fields(self):
        p = make_mock_priceable(name=None)
        action = AddScaledTradeAction(priceables=[p], name='ScaledDefaults')
        assert action.scaling_type == ScalingActionType.size
        assert action.scaling_risk is None
        assert action.scaling_level == 1

    def test_multiple_priceables(self):
        p1 = make_mock_priceable(name=None)
        p2 = make_mock_priceable(name='MyP')
        action = AddScaledTradeAction(priceables=[p1, p2], name='ScaledMulti')
        assert len(action.priceables) == 2


# ============================================================
# EnterPositionQuantityScaledAction
# ============================================================

class TestEnterPositionQuantityScaledAction:
    def test_priceable_name_none_auto_named(self):
        p = make_mock_priceable(name=None)
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyAction')
        assert len(action.priceables) == 1
        assert action.priceables[0].name == 'QtyAction_Priceable0'

    def test_priceable_name_starts_with_action_name_kept(self):
        p = make_mock_priceable(name='QtyAction_existing')
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyAction')
        assert action.priceables[0] is p

    def test_priceable_name_prefixed(self):
        p = make_mock_priceable(name='Swap')
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyAction')
        assert action.priceables[0].name == 'QtyAction_Swap'

    def test_transaction_cost_exit_defaults(self):
        tc = ConstantTransactionModel(7)
        p = make_mock_priceable(name=None)
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyTC', transaction_cost=tc)
        assert action.transaction_cost_exit == tc

    def test_transaction_cost_exit_explicit(self):
        tc_entry = ConstantTransactionModel(7)
        tc_exit = ConstantTransactionModel(3)
        p = make_mock_priceable(name=None)
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyTCE',
                                                   transaction_cost=tc_entry, transaction_cost_exit=tc_exit)
        assert action.transaction_cost_exit == tc_exit

    def test_default_quantity_fields(self):
        from gs_quant.target.backtests import BacktestTradingQuantityType
        p = make_mock_priceable(name=None)
        action = EnterPositionQuantityScaledAction(priceables=[p], name='QtyDefaults')
        assert action.trade_quantity == 1
        assert action.trade_quantity_type == BacktestTradingQuantityType.quantity


# ============================================================
# ExitTradeAction
# ============================================================

class TestExitTradeAction:
    def test_basic_construction(self):
        action = ExitTradeAction(priceable_names='TestSwap')
        assert action.priceables_names == ['TestSwap']

    def test_multiple_names(self):
        action = ExitTradeAction(priceable_names=['Swap1', 'Swap2'])
        assert action.priceables_names == ['Swap1', 'Swap2']

    def test_none_names(self):
        action = ExitTradeAction(priceable_names=None)
        assert action.priceables_names == []

    def test_auto_name(self):
        action = ExitTradeAction()
        assert action.name is not None
        assert action.name.startswith('Action')


# ============================================================
# ExitAllPositionsAction
# ============================================================

class TestExitAllPositionsAction:
    def test_calc_type_path_dependent(self):
        action = ExitAllPositionsAction()
        assert action.calc_type == CalcType.path_dependent

    def test_inherits_from_exit_trade_action(self):
        assert issubclass(ExitAllPositionsAction, ExitTradeAction)


# ============================================================
# ExitPositionAction
# ============================================================

class TestExitPositionAction:
    def test_basic_construction(self):
        action = ExitPositionAction()
        assert action.name is not None

    def test_class_type(self):
        action = ExitPositionAction()
        assert action.class_type == 'exit_position_action'


# ============================================================
# HedgeAction
# ============================================================

class TestHedgeAction:
    def test_portfolio_priceable_kept(self):
        p = make_mock_priceable(name='MyInst')
        portfolio = Portfolio([p], name='MyPortfolio')

        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeB')
        assert isinstance(action.priceables, Portfolio)

    def test_none_priceable_raises(self):
        with pytest.raises(RuntimeError, match='hedge action only accepts one trade or one portfolio'):
            HedgeAction(risk=MagicMock(), priceables=None, name='HedgeBad')

    def test_invalid_priceable_type_raises(self):
        with pytest.raises((RuntimeError, TypeError, AttributeError)):
            HedgeAction(risk=MagicMock(), priceables='not_a_priceable', name='HedgeBad2')

    def test_calc_type_semi_path_dependent(self):
        p = make_mock_priceable(name='SemiP')
        portfolio = Portfolio([p], name='SemiPort')
        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeSemi')
        assert action.calc_type == CalcType.semi_path_dependent

    def test_transaction_cost_exit_defaults_to_entry(self):
        p = make_mock_priceable(name='HedgeTC')
        portfolio = Portfolio([p], name='TCPort')
        tc = ConstantTransactionModel(10)
        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeTCTest', transaction_cost=tc)
        assert action.transaction_cost_exit == tc

    def test_transaction_cost_exit_explicit(self):
        p = make_mock_priceable(name='HedgeTCE')
        portfolio = Portfolio([p], name='TCEPort')
        tc_entry = ConstantTransactionModel(10)
        tc_exit = ConstantTransactionModel(5)
        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeTCE2',
                             transaction_cost=tc_entry, transaction_cost_exit=tc_exit)
        assert action.transaction_cost_exit == tc_exit

    def test_deprecated_scaling_parameter_warns(self):
        p = make_mock_priceable(name='HedgeWarn')
        portfolio = Portfolio([p], name='WarnPort')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeDeprecated',
                                 scaling_parameter='custom_param')
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert 'scaling_parameter is deprecated' in str(w[0].message)

    def test_default_scaling_parameter_no_warning(self):
        p = make_mock_priceable(name='HedgeNoWarn')
        portfolio = Portfolio([p], name='NoWarnPort')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeNoWarnTest')
            # No deprecation warnings should fire
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 0

    def test_priceable_property(self):
        p = make_mock_priceable(name='PropTest')
        portfolio = Portfolio([p], name='PropPort')
        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeProp')
        assert action.priceable is action.priceables

    def test_naming_of_priceables_in_portfolio(self):
        # Test three naming paths for priceables inside a portfolio
        p_none = make_mock_priceable(name=None)
        p_starts = make_mock_priceable(name='HedgeName_existing')
        p_other = make_mock_priceable(name='OtherSwap')

        portfolio = Portfolio([p_none, p_starts, p_other], name='TestPort')
        action = HedgeAction(risk=MagicMock(), priceables=portfolio, name='HedgeName')

        named_portfolio = action.priceables
        assert isinstance(named_portfolio, Portfolio)
        instruments = list(named_portfolio)
        assert len(instruments) == 3

        # p_none should get auto-name
        assert instruments[0].name == 'HedgeName_Priceable0'
        # p_starts should be kept as-is
        assert instruments[1] is p_starts
        # p_other should be prefixed
        assert instruments[2].name == 'HedgeName_OtherSwap'


# ============================================================
# RebalanceAction
# ============================================================

class TestRebalanceAction:
    def test_calc_type_path_dependent(self):
        p = make_mock_priceable(name='RebP')
        p.unresolved = 'some_unresolved'
        action = RebalanceAction(priceable=p, name='Reb')
        assert action.calc_type == CalcType.path_dependent

    def test_unresolved_none_raises(self):
        p = make_mock_priceable(name='RebBad')
        p.unresolved = None
        with pytest.raises(ValueError, match='Please specify a resolved priceable'):
            RebalanceAction(priceable=p, name='RebBadAction')

    def test_priceable_name_none_gets_named(self):
        p = make_mock_priceable(name=None)
        p.unresolved = 'some_unresolved'
        action = RebalanceAction(priceable=p, name='RebAuto')
        assert action.priceable.name == 'RebAuto_Priceable0'

    def test_priceable_name_exists_gets_prefixed(self):
        p = make_mock_priceable(name='MySwap')
        p.unresolved = 'some_unresolved'
        action = RebalanceAction(priceable=p, name='RebPrefix')
        assert action.priceable.name == 'RebPrefix_MySwap'

    def test_transaction_cost_exit_defaults(self):
        p = make_mock_priceable(name='RebTC')
        p.unresolved = 'some_unresolved'
        tc = ConstantTransactionModel(10)
        action = RebalanceAction(priceable=p, name='RebTCTest', transaction_cost=tc)
        assert action.transaction_cost_exit == tc

    def test_transaction_cost_exit_explicit(self):
        p = make_mock_priceable(name='RebTCE')
        p.unresolved = 'some_unresolved'
        tc_entry = ConstantTransactionModel(10)
        tc_exit = ConstantTransactionModel(5)
        action = RebalanceAction(priceable=p, name='RebTCETest',
                                 transaction_cost=tc_entry, transaction_cost_exit=tc_exit)
        assert action.transaction_cost_exit == tc_exit


# ============================================================
# Named tuple structures
# ============================================================

class TestNamedTuples:
    def test_add_trade_action_info(self):
        info = AddTradeActionInfo(scaling=1.0, next_schedule=dt.date(2021, 1, 4))
        assert info.scaling == 1.0
        assert info.next_schedule == dt.date(2021, 1, 4)

    def test_hedge_action_info(self):
        info = HedgeActionInfo(next_schedule=dt.date(2021, 2, 1))
        assert info.next_schedule == dt.date(2021, 2, 1)

    def test_add_scaled_trade_action_info(self):
        info = AddScaledTradeActionInfo(next_schedule=dt.date(2021, 3, 1))
        assert info.next_schedule == dt.date(2021, 3, 1)


# ============================================================
# Global action_count increments
# ============================================================

class TestActionCount:
    def test_global_counter_increments(self):
        count_before = actions_module.action_count
        a1 = Action()
        a2 = Action()
        # Counter should have incremented by 2
        assert actions_module.action_count == count_before + 2


# ============================================================
# Phase 6 – additional branch-coverage tests
# ============================================================


class TestAddWeightedTradeActionPostInit:
    """Cover branches [284,285], [286,287], [291,-279]."""

    def test_priceable_name_is_none(self):
        """When p.name is None -> clone with generated name [284,285]."""
        p = make_mock_priceable(name=None)
        from gs_quant.backtests.actions import AddWeightedTradeAction
        action = AddWeightedTradeAction(
            priceables=Portfolio([p]),
            name='TestAction',
        )
        # The priceable should have been cloned with a name like 'TestAction_Priceable0'
        p.clone.assert_called()
        clone_name = p.clone.call_args[1]['name']
        assert 'TestAction_Priceable0' in clone_name

    def test_priceable_name_starts_with_action_name(self):
        """When p.name starts with self.name -> keep as is [286,287]."""
        p = make_mock_priceable(name='TestAction_Sub')
        p.name = 'TestAction_Sub'
        from gs_quant.backtests.actions import AddWeightedTradeAction
        action = AddWeightedTradeAction(
            priceables=Portfolio([p]),
            name='TestAction',
        )
        # Should NOT be cloned since name already starts with 'TestAction'
        p.clone.assert_not_called()

    def test_transaction_cost_exit_defaults_to_transaction_cost(self):
        """When transaction_cost_exit is None -> set to transaction_cost."""
        p = make_mock_priceable(name=None)
        from gs_quant.backtests.actions import AddWeightedTradeAction
        action = AddWeightedTradeAction(
            priceables=Portfolio([p]),
            name='TestAction',
            transaction_cost_exit=None,
        )
        assert action.transaction_cost_exit is not None
        assert action.transaction_cost_exit == action.transaction_cost

    def test_transaction_cost_exit_provided(self):
        """When transaction_cost_exit is provided (not None) -> skip default [291,-279]."""
        p = make_mock_priceable(name=None)
        from gs_quant.backtests.actions import AddWeightedTradeAction, ConstantTransactionModel
        custom_cost = ConstantTransactionModel(99)
        action = AddWeightedTradeAction(
            priceables=Portfolio([p]),
            name='TestAction',
            transaction_cost_exit=custom_cost,
        )
        assert action.transaction_cost_exit is custom_cost
        assert action.transaction_cost_exit != action.transaction_cost


class TestRebalanceActionPostInit:
    """Cover branch [488,493]: RebalanceAction with priceable that has a name."""

    def test_priceable_with_name(self):
        """When priceable.name is not None -> clone with prefixed name [488,493]."""
        p = make_mock_priceable(name='MySwap')
        p.name = 'MySwap'
        p.unresolved = MagicMock()  # Not None, so it doesn't raise
        action = RebalanceAction(
            priceable=p,
            name='RebalAction',
        )
        # Should have cloned with name 'RebalAction_MySwap'
        p.clone.assert_called_once()
        clone_name = p.clone.call_args[1]['name']
        assert clone_name == 'RebalAction_MySwap'

    def test_priceable_without_name(self):
        """When priceable.name is None -> clone with Priceable0 suffix."""
        p = make_mock_priceable(name=None)
        p.unresolved = MagicMock()
        action = RebalanceAction(
            priceable=p,
            name='RebalAction',
        )
        p.clone.assert_called_once()
        clone_name = p.clone.call_args[1]['name']
        assert clone_name == 'RebalAction_Priceable0'

    def test_transaction_cost_exit_defaults(self):
        """When transaction_cost_exit is None -> defaults to transaction_cost."""
        p = make_mock_priceable(name='Swap')
        p.name = 'Swap'
        p.unresolved = MagicMock()
        action = RebalanceAction(
            priceable=p,
            name='Rebal',
            transaction_cost_exit=None,
        )
        assert action.transaction_cost_exit == action.transaction_cost


class TestRebalanceActionTransactionCostExitProvided:
    """Cover branch [488,493] exit path: transaction_cost_exit is already set."""

    def test_transaction_cost_exit_not_none(self):
        """When transaction_cost_exit is provided -> skip defaulting [488,493]."""
        from gs_quant.backtests.actions import ConstantTransactionModel
        p = make_mock_priceable(name='Swap')
        p.name = 'Swap'
        p.unresolved = MagicMock()
        custom_cost = ConstantTransactionModel(42)
        action = RebalanceAction(
            priceable=p,
            name='Rebal',
            transaction_cost_exit=custom_cost,
        )
        assert action.transaction_cost_exit is custom_cost
