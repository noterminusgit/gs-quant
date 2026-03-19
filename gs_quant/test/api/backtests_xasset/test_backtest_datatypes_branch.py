"""
Branch coverage tests for gs_quant/api/gs/backtests_xasset/response_datatypes/backtest_datatypes.py
"""

import dataclasses
import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes import (
    RollDateMode,
    Model,
    FixedCostModel,
    ScaledCostModel,
    AggregateCostModel,
    CostAggregationType,
    TransactionCostScalingType,
    basic_tc_tuple_decoder,
    tcm_decoder,
    Transaction,
    TradeEvent,
    TransactionDirection,
    decode_trade_event_tuple_dict,
    AdditionalResults,
    DateConfig,
    Trade,
    TradingCosts,
    TransactionCostConfig,
    Configuration,
    StrategyHedge,
)


class TestRollDateMode:
    """Test RollDateMode enum."""

    def test_otc(self):
        assert RollDateMode('OTC') == RollDateMode.OTC

    def test_listed(self):
        assert RollDateMode('Listed') == RollDateMode.Listed

    def test_missing_none(self):
        """None returns None."""
        result = RollDateMode._missing_(None)
        assert result is None

    def test_missing_case_insensitive(self):
        """Case insensitive lookup works."""
        result = RollDateMode._missing_('otc')
        assert result == RollDateMode.OTC

    def test_missing_unknown(self):
        """Unknown value returns None."""
        result = RollDateMode._missing_('unknown')
        assert result is None


class TestFixedCostModel:
    """Test FixedCostModel."""

    def test_scaling_property(self):
        m = FixedCostModel(cost=1.0)
        assert m.scaling_property == 'cost'

    def test_eq_same(self):
        m1 = FixedCostModel(cost=1.0)
        m2 = FixedCostModel(cost=1.0)
        assert m1 == m2

    def test_eq_different(self):
        m1 = FixedCostModel(cost=1.0)
        m2 = FixedCostModel(cost=2.0)
        assert m1 != m2

    def test_eq_not_fixed(self):
        m1 = FixedCostModel(cost=1.0)
        assert m1 != 'not a cost model'

    def test_set_scaling_property(self):
        m = FixedCostModel(cost=1.0)
        m.set_scaling_property(5.0)
        assert m.cost == 5.0


class TestScaledCostModel:
    """Test ScaledCostModel."""

    def test_scaling_property(self):
        m = ScaledCostModel(scaling_level=1.0)
        assert m.scaling_property == 'scaling_level'

    def test_eq_same(self):
        m1 = ScaledCostModel(scaling_level=1.0, scaling_quantity_type=TransactionCostScalingType.Quantity)
        m2 = ScaledCostModel(scaling_level=1.0, scaling_quantity_type=TransactionCostScalingType.Quantity)
        assert m1 == m2

    def test_eq_different(self):
        m1 = ScaledCostModel(scaling_level=1.0)
        m2 = ScaledCostModel(scaling_level=2.0)
        assert m1 != m2

    def test_eq_not_scaled(self):
        m1 = ScaledCostModel(scaling_level=1.0)
        assert m1 != 'not a cost model'


class TestModelAddition:
    """Test Model.__add__ method."""

    def test_add_non_model_raises(self):
        """Adding non-Model raises TypeError."""
        m = FixedCostModel(cost=1.0)
        with pytest.raises(TypeError, match='Can only add to another cost model'):
            m + 'not a model'

    def test_add_aggregate_to_model(self):
        """Adding AggregateCostModel to a Model delegates to AggregateCostModel.__add__
        which raises TypeError since other (FixedCostModel) is not AggregateCostModel."""
        m1 = FixedCostModel(cost=1.0)
        m2 = FixedCostModel(cost=2.0)
        agg = AggregateCostModel(models=(m2,), aggregation_type=CostAggregationType.Sum)
        # Model.__add__ detects other is AggregateCostModel and does `other + self`
        # But AggregateCostModel.__add__ raises because self (FixedCostModel) is not AggregateCostModel
        with pytest.raises(TypeError, match='same aggregation type'):
            m1 + agg

    def test_add_same_type_can_add_scaling(self):
        """Adding same type with can_add_scaling=True merges scaling."""
        m1 = FixedCostModel(cost=1.0)
        m2 = FixedCostModel(cost=2.0)
        result = m1 + m2
        assert isinstance(result, FixedCostModel)
        assert result.cost == 3.0

    def test_add_same_type_cannot_add_scaling(self):
        """Adding same type that can't merge scaling creates AggregateCostModel."""
        m1 = ScaledCostModel(scaling_level=1.0, scaling_quantity_type=TransactionCostScalingType.Quantity)
        m2 = ScaledCostModel(scaling_level=2.0, scaling_quantity_type=TransactionCostScalingType.Notional)
        result = m1 + m2
        assert isinstance(result, AggregateCostModel)

    def test_add_different_types(self):
        """Adding different model types creates AggregateCostModel."""
        m1 = FixedCostModel(cost=1.0)
        m2 = ScaledCostModel(scaling_level=2.0)
        result = m1 + m2
        assert isinstance(result, AggregateCostModel)

    def test_sub_raises(self):
        m = FixedCostModel(cost=1.0)
        with pytest.raises(NotImplementedError):
            m - FixedCostModel(cost=1.0)

    def test_mul_raises(self):
        m = FixedCostModel(cost=1.0)
        with pytest.raises(NotImplementedError):
            m * FixedCostModel(cost=1.0)

    def test_div_raises(self):
        """__div__ is the old-style division (Python 2); call it directly."""
        m = FixedCostModel(cost=1.0)
        with pytest.raises(NotImplementedError):
            m.__div__(FixedCostModel(cost=1.0))


class TestAggregateCostModel:
    """Test AggregateCostModel."""

    def test_scaling_property_is_none(self):
        m = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        assert m.scaling_property is None

    def test_set_scaling_property_noop(self):
        m = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        m.set_scaling_property(5.0)  # Should be noop

    def test_add_same_aggregation_type(self):
        m1 = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        m2 = AggregateCostModel(
            models=(FixedCostModel(cost=2.0),),
            aggregation_type=CostAggregationType.Sum
        )
        result = m1 + m2
        assert isinstance(result, AggregateCostModel)
        assert len(result.models) == 2

    def test_add_different_aggregation_type_raises(self):
        m1 = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        m2 = AggregateCostModel(
            models=(FixedCostModel(cost=2.0),),
            aggregation_type=CostAggregationType.Max
        )
        with pytest.raises(TypeError, match='same aggregation type'):
            m1 + m2

    def test_eq_same(self):
        m1 = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        m2 = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        assert m1 == m2

    def test_eq_not_aggregate(self):
        m1 = AggregateCostModel(
            models=(FixedCostModel(cost=1.0),),
            aggregation_type=CostAggregationType.Sum
        )
        assert m1 != 'not an aggregate'


class TestBasicTcTupleDecoder:
    """Test basic_tc_tuple_decoder."""

    def test_none(self):
        assert basic_tc_tuple_decoder(None) is None

    def test_fixed_cost_model(self):
        result = basic_tc_tuple_decoder(({'type': 'FixedCostModel', 'cost': 1.0},))
        assert len(result) == 1
        assert isinstance(result[0], FixedCostModel)

    def test_scaled_cost_model(self):
        result = basic_tc_tuple_decoder(({
            'type': 'ScaledCostModel',
            'scalingLevel': 1.0,
            'scalingQuantityType': 'Quantity'
        },))
        assert len(result) == 1
        assert isinstance(result[0], ScaledCostModel)


class TestTcmDecoder:
    """Test tcm_decoder."""

    def test_none(self):
        assert tcm_decoder(None) is None

    def test_fixed_cost_model(self):
        result = tcm_decoder({'type': 'FixedCostModel', 'cost': 1.0})
        assert isinstance(result, FixedCostModel)

    def test_aggregate_cost_model(self):
        result = tcm_decoder({
            'type': 'AggregateCostModel',
            'models': [{'type': 'FixedCostModel', 'cost': 1.0}],
            'aggregationType': 'Sum'
        })
        assert isinstance(result, AggregateCostModel)


class TestDecodeTradeEventTupleDict:
    """Test decode_trade_event_tuple_dict."""

    def test_basic(self):
        data = {
            '2020-01-01': [
                {'direction': 'Entry', 'price': 100.0, 'tradeId': 't1'}
            ]
        }
        result = decode_trade_event_tuple_dict(data)
        assert dt.date(2020, 1, 1) in result
        assert len(result[dt.date(2020, 1, 1)]) == 1
        assert isinstance(result[dt.date(2020, 1, 1)][0], TradeEvent)


class TestAdditionalResults:
    """Test AdditionalResults.from_dict_custom."""

    def test_from_dict_custom_decode_instruments_true(self):
        """decode_instruments=True delegates to from_dict."""
        with patch.object(AdditionalResults, 'from_dict', return_value=MagicMock()) as mock_from_dict:
            data = {'some': 'data'}
            result = AdditionalResults.from_dict_custom(data, decode_instruments=True)
            mock_from_dict.assert_called_once_with(data)

    def test_from_dict_custom_decode_instruments_false(self):
        """decode_instruments=False creates manually."""
        data = {
            'hedges': {'2020-01-01': [{'type': 'IRSwap'}]},
            'hedge_pnl': {'2020-01-01': 1.0},
            'no_of_calculations': 5,
            'trade_events': {
                '2020-01-01': [{'direction': 'Entry', 'price': 100.0}]
            },
            'hedge_events': {
                '2020-01-01': [{'direction': 'Exit', 'price': 99.0}]
            },
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes.decode_daily_portfolio'
        ) as mock_dp:
            mock_dp.return_value = {}
            result = AdditionalResults.from_dict_custom(data, decode_instruments=False)
            assert isinstance(result, AdditionalResults)
            mock_dp.assert_called_once_with(data['hedges'], False)
