"""
Tests targeting Tier 3 branch coverage gaps (2 missing branches each) across 11 files.
Each test exercises a specific untaken branch to close coverage gaps.
"""

import datetime as dt
import typing
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Union, Optional, Dict
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# 1. gs_quant/analytics/core/processor.py: 295->298, 345->348
#    Both branches involve dead code paths:
#    - 295->298: isinstance(self, BaseProcessor) is False - impossible since
#      as_dict is defined on BaseProcessor.
#    - 345->348: isinstance(attribute, dt.date) is False but attribute is
#      (dt.date, dt.datetime) - impossible since dt.datetime IS a dt.date.
#    We test the reachable paths to improve overall coverage.
# ---------------------------------------------------------------------------

class TestProcessorAsDict:
    """Test processor.as_dict() branches"""

    def test_processor_as_dict_with_date_attribute(self):
        """Exercise the dt.date branch (345->346) in as_dict"""
        from gs_quant.analytics.core.processor import BaseProcessor
        from gs_quant.analytics.common import TYPE, PROCESSOR, PARAMETERS, VALUE

        class DateProcessor(BaseProcessor):
            def __init__(self, my_date: dt.date = None, **kwargs):
                super().__init__(**kwargs)
                self.my_date = my_date

            def process(self):
                pass

            def get_plot_expression(self):
                pass

        proc = DateProcessor(my_date=dt.date(2023, 6, 15))
        proc.children = {}
        result = proc.as_dict()
        params = result[PARAMETERS]
        assert 'my_date' in params
        assert params['my_date'][VALUE] == '2023-06-15'

    def test_processor_as_dict_with_datetime_attribute(self):
        """Exercise as_dict with a datetime attribute. Since dt.datetime IS dt.date,
        it takes the dt.date branch (line 346). Verify it works correctly."""
        from gs_quant.analytics.core.processor import BaseProcessor
        from gs_quant.analytics.common import TYPE, PROCESSOR, PARAMETERS, VALUE

        class TSProcessor(BaseProcessor):
            def __init__(self, ts: dt.datetime = None, **kwargs):
                super().__init__(**kwargs)
                self.ts = ts

            def process(self):
                pass

            def get_plot_expression(self):
                pass

        proc = TSProcessor(ts=dt.datetime(2023, 6, 15, 10, 30, 45))
        proc.children = {}
        result = proc.as_dict()
        params = result[PARAMETERS]
        assert 'ts' in params


# ---------------------------------------------------------------------------
# 2. gs_quant/analytics/core/query_helpers.py: 65->66, 70->71
#    Branch 65->66: query.start is a dt.datetime (not dt.date)
#    Branch 70->71: query.end is a dt.datetime (not dt.date)
# ---------------------------------------------------------------------------

class TestQueryHelpers:
    """Test aggregate_queries with datetime start/end"""

    def test_aggregate_queries_with_datetime_start_end(self):
        """Exercise branches 65->66 and 70->71: start/end as dt.datetime"""
        from gs_quant.analytics.core.query_helpers import aggregate_queries
        from gs_quant.analytics.core.processor import DataQueryInfo
        from gs_quant.data import DataFrequency

        coord = MagicMock()
        coord.dataset_id = 'test_dataset'
        coord.measure = 'price'
        coord.dimensions = {'field1': 'val1'}
        coord.frequency = DataFrequency.DAILY
        coord.get_dimensions.return_value = ('field1',)

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.datetime(2023, 1, 1, 10, 0, 0)  # datetime, not date
        query.end = dt.datetime(2023, 6, 30, 15, 0, 0)    # datetime, not date
        query.get_range_string.return_value = 'range1'

        processor = MagicMock()
        entity = MagicMock()

        query_info = DataQueryInfo(attr='a', processor=processor, query=query, entity=entity)
        aggregate_queries([query_info])


# ---------------------------------------------------------------------------
# 3. gs_quant/backtests/backtest_objects.py: 484->486, 562->565
#    Branch 484->486: TransactionAggType.MIN -> returns min
#    Branch 562->565: cost_aggregation_func returns scaled_cost (not fixed_cost)
# ---------------------------------------------------------------------------

class TestBacktestObjects:
    """Test TransactionCostEntry branches"""

    def test_cost_aggregation_func_min(self):
        """Exercise branch 484->486: aggregate_type is MIN"""
        from gs_quant.backtests.backtest_objects import (
            TransactionCostEntry, AggregateTransactionModel, TransactionAggType
        )

        agg_model = AggregateTransactionModel(
            transaction_models=(),
            aggregate_type=TransactionAggType.MIN
        )
        instrument = MagicMock()
        entry = TransactionCostEntry(
            date=dt.date(2023, 1, 1),
            instrument=instrument,
            transaction_model=agg_model
        )
        assert entry.cost_aggregation_func is min

    def test_get_cost_by_component_scaled_wins(self):
        """Exercise branch 562->565: cost_aggregation_func([fixed, scaled]) == scaled_cost"""
        from gs_quant.backtests.backtest_objects import (
            TransactionCostEntry, AggregateTransactionModel, TransactionAggType,
            ScaledTransactionModel, ConstantTransactionModel
        )

        fixed_model = ConstantTransactionModel(cost=5.0)
        scaled_model = ScaledTransactionModel(scaling_type='notional_amount', scaling_level=1.0)
        agg_model = AggregateTransactionModel(
            transaction_models=(fixed_model, scaled_model),
            aggregate_type=TransactionAggType.MAX
        )

        instrument = MagicMock()
        instrument.notional_amount = 100.0
        instrument.all_instruments = (instrument,)

        entry = TransactionCostEntry(
            date=dt.date(2023, 1, 1),
            instrument=instrument,
            transaction_model=agg_model
        )

        # Manually set unit costs
        entry._unit_cost_by_model_by_inst = {
            fixed_model: {instrument: 5.0},
            scaled_model: {instrument: 100.0},
        }

        # max([5.0, 100.0]) == 100.0 == scaled_cost -> returns (0, scaled_cost)
        fixed, scaled = entry.get_cost_by_component()
        assert fixed == 0
        assert scaled == 100.0


# ---------------------------------------------------------------------------
# 4. gs_quant/backtests/predefined_asset_engine.py: 125->127, 254->237
#    Branch 125->127: action_impl_map is not None
#    Branch 254->237: ValuationEvent is processed, loops back to while
# ---------------------------------------------------------------------------

class TestPredefinedAssetEngine:
    """Test PredefinedAssetEngine branches"""

    def test_init_with_custom_action_impl_map(self):
        """Exercise branch 125->127: action_impl_map is not None"""
        from gs_quant.backtests.predefined_asset_engine import (
            PredefinedAssetEngine, SubmitOrderActionImpl
        )
        from gs_quant.backtests.actions import Action

        custom_map = {Action: SubmitOrderActionImpl}
        engine = PredefinedAssetEngine(action_impl_map=custom_map)
        assert engine.action_impl_map is custom_map

    def test_run_processes_valuation_event(self):
        """Exercise branch 254->237: ValuationEvent is processed"""
        from gs_quant.backtests.predefined_asset_engine import PredefinedAssetEngine
        from gs_quant.backtests.backtest_objects import PredefinedAssetBacktest

        engine = PredefinedAssetEngine()
        engine.execution_engine = MagicMock()
        engine.execution_engine.ping.return_value = []
        engine.data_handler = MagicMock()

        engine._eod_valuation_time = MagicMock(return_value=dt.time(10, 0))

        strategy = MagicMock()
        strategy.triggers = []

        backtest = MagicMock(spec=PredefinedAssetBacktest)

        state = dt.datetime(2023, 1, 2, 10, 0)
        timer = [state]

        engine._run(strategy, timer, backtest)

        backtest.mark_to_market.assert_called_once()


# ---------------------------------------------------------------------------
# 5. gs_quant/base.py: 296->297, 548->550
#    Branch 296->297: getattr(tp, '_special', False) is True -> return False
#    Branch 548->550: self.__repr__ == other.__repr__ -> return False
# ---------------------------------------------------------------------------

class TestBase:
    """Test Base class branches"""

    def test_is_type_match_special_generic(self):
        """Exercise branch 296->297: tp has _special=True"""
        from gs_quant.base import Base

        mock_tp = MagicMock()
        mock_tp._special = True
        mock_tp.__class__ = typing._GenericAlias

        result = Base._Base__is_type_match(mock_tp, "value")
        assert result is False

    def test_scenario_lt_same_repr_same_object(self):
        """Exercise branch 548->550: self.__repr__ == other.__repr__

        For bound methods, s.__repr__ == s.__repr__ is True only when
        comparing the same instance. We pass self as both arguments."""
        from gs_quant.base import Scenario

        s = MagicMock()
        s.name = "A"
        # When s is the same object for both, s.__repr__ == s.__repr__ is True
        result = Scenario.__lt__(s, s)
        assert result is False


# ---------------------------------------------------------------------------
# 6. gs_quant/context_base.py: 142->152, 148->142
#    Branch 142->152: stack empties without finding ContextBase -> return self.__class__
#    Branch 148->142: base already in seen, skip and loop back
# ---------------------------------------------------------------------------

class TestContextBase:
    """Test ContextBase._cls property branches"""

    def test_cls_with_direct_context_base_subclass(self):
        """Exercise the normal path where ContextBase is in __bases__"""
        from gs_quant.context_base import ContextBase

        class DirectSub(ContextBase):
            pass

        obj = DirectSub()
        assert obj._cls is DirectSub

    def test_cls_with_multiple_inheritance_seen_skip(self):
        """Exercise branch 148->142: a base already in seen is skipped.
        Diamond inheritance causes the same base to appear multiple times."""
        from gs_quant.context_base import ContextBase

        class A(ContextBase):
            pass

        class B(A):
            pass

        class C(A):
            pass

        class D(B, C):
            pass

        obj = D()
        result = obj._cls
        assert result is A


# ---------------------------------------------------------------------------
# 7. gs_quant/data/utilities.py: 316->317, 441->442
#    Branch 316->317: normalized_records is empty after normalization
#    Branch 441->442: date matches 9999-12-31 but not caught by INFINITY_DATE check
# ---------------------------------------------------------------------------

class TestDataUtilities:
    """Test SecmasterXrefFormatter branches"""

    def test_convert_entity_records_empty(self):
        """Exercise branch 316->317 via empty records"""
        from gs_quant.data.utilities import SecmasterXrefFormatter

        result = SecmasterXrefFormatter._convert_entity_records([])
        assert result == []

    def test_add_one_day_9999_12_31_via_patched_infinity(self):
        """Exercise branch 441->442: date parses to 9999-12-31 but
        INFINITY_DATE is different, so the secondary check catches it."""
        from gs_quant.data.utilities import SecmasterXrefFormatter

        # Normal path: "9999-12-31" matches INFINITY_DATE
        result = SecmasterXrefFormatter._add_one_day("9999-12-31")
        assert result is None

        # Patch INFINITY_DATE so "9999-12-31" passes through the first check
        # but hits the year/month/day check at 441
        with patch.object(SecmasterXrefFormatter, 'INFINITY_DATE', '9999-12-30'):
            result = SecmasterXrefFormatter._add_one_day("9999-12-31")
            assert result is None


# ---------------------------------------------------------------------------
# 8. gs_quant/markets/historical.py: 199->200, 206->209
#    Branch 199->200: start AND dates both provided (raises ValueError)
#    Branch 206->209: neither start nor dates provided (raises ValueError)
# ---------------------------------------------------------------------------

class TestHistorical:
    """Test BackToTheFuturePricingContext init branches"""

    def test_btf_start_and_dates_raises(self):
        """Exercise branch 199->200: both start and dates provided"""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        with pytest.raises(ValueError, match='Must supply start or dates, not both'):
            BackToTheFuturePricingContext(
                start=dt.date(2023, 1, 1),
                end=dt.date(2023, 1, 5),
                dates=[dt.date(2023, 1, 1), dt.date(2023, 1, 2)]
            )

    def test_btf_neither_start_nor_dates_raises(self):
        """Exercise branch 206->209: neither start nor dates provided"""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        with pytest.raises(ValueError, match='Must supply start or dates'):
            BackToTheFuturePricingContext()


# ---------------------------------------------------------------------------
# 9. gs_quant/markets/optimizer.py: 1920->1921, 1975->1976
#    Branch 1920->1921: __result is None -> MqValueError
#    Branch 1975->1976: __result is None -> MqValueError
# ---------------------------------------------------------------------------

class TestOptimizer:
    """Test OptimizerStrategy methods when result is None"""

    def test_get_hedge_exposure_summary_no_result(self):
        """Exercise branch 1920->1921: __result is None"""
        from gs_quant.markets.optimizer import OptimizerStrategy
        from gs_quant.errors import MqValueError

        strategy = MagicMock(spec=OptimizerStrategy)
        strategy._OptimizerStrategy__result = None

        with pytest.raises(MqValueError, match='Please run the optimization'):
            OptimizerStrategy.get_hedge_exposure_summary.__wrapped__(strategy)

    def test_get_hedge_constituents_by_direction_no_result(self):
        """Exercise branch 1975->1976: __result is None"""
        from gs_quant.markets.optimizer import OptimizerStrategy
        from gs_quant.errors import MqValueError

        strategy = MagicMock(spec=OptimizerStrategy)
        strategy._OptimizerStrategy__result = None

        with pytest.raises(MqValueError, match='Please run the optimization'):
            OptimizerStrategy.get_hedge_constituents_by_direction.__wrapped__(strategy)


# ---------------------------------------------------------------------------
# 10. gs_quant/markets/portfolio.py: 215->218, 579->-552
#     Branch 215->218: a portfolio popped from stack is NOT in portfolios.
#         Due to initialization (stack and portfolios start identical),
#         this only fires when sub-portfolios add new items to the stack.
#         Since all initial items hit continue, we need nested sub-portfolios
#         that are NOT in the initial list. This is effectively dead code.
#     Branch 579: result_future is always truthy (PricingFuture).
# ---------------------------------------------------------------------------

class TestPortfolio:
    """Test Portfolio.all_portfolios property"""

    def test_all_portfolios_with_sub_portfolios(self):
        """Exercise the all_portfolios while loop with nested structure"""
        from gs_quant.markets.portfolio import Portfolio
        from gs_quant.instrument import IRSwap

        swap1 = IRSwap('Pay', '5y', 'USD')
        swap2 = IRSwap('Pay', '10y', 'USD')

        inner = Portfolio([swap2], name='inner')
        outer = Portfolio([swap1, inner], name='outer')

        all_p = outer.all_portfolios
        assert inner in all_p

    def test_all_portfolios_empty(self):
        """Test all_portfolios when there are no sub-portfolios"""
        from gs_quant.markets.portfolio import Portfolio
        from gs_quant.instrument import IRSwap

        swap = IRSwap('Pay', '5y', 'USD')
        p = Portfolio([swap])

        all_p = p.all_portfolios
        assert len(all_p) == 0


# ---------------------------------------------------------------------------
# 11. gs_quant/markets/securities.py: 827->820, 1524->1526
#     Branch 827->820: range_start > range_end, geometrically impossible after
#         the overlap test at 821 passes. Dead code.
#     Branch 1524->1526: isinstance(as_of, dt.date) is always True for both
#         dt.date and dt.datetime. We test with both types.
# ---------------------------------------------------------------------------

class TestSecurities:
    """Test SecurityMaster branches"""

    def test_get_asset_query_with_datetime_as_of(self):
        """Exercise get_asset_query with a datetime as_of.
        Since dt.datetime IS dt.date, isinstance check at 1524 is True."""
        from gs_quant.markets.securities import SecurityMaster, AssetIdentifier

        as_of = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        query, result_as_of = SecurityMaster.get_asset_query(
            id_value='AAPL',
            id_type=AssetIdentifier.BLOOMBERG_ID,
            as_of=as_of
        )
        assert isinstance(result_as_of, dt.datetime)
        assert query == {'bbid': 'AAPL'}

    def test_get_asset_query_with_date_as_of(self):
        """Exercise branch 1524->1525: as_of is dt.date, gets converted"""
        from gs_quant.markets.securities import SecurityMaster, AssetIdentifier

        as_of = dt.date(2023, 6, 15)
        query, result_as_of = SecurityMaster.get_asset_query(
            id_value='AAPL',
            id_type=AssetIdentifier.BLOOMBERG_ID,
            as_of=as_of
        )
        assert isinstance(result_as_of, dt.datetime)
        assert result_as_of.tzinfo == dt.timezone.utc

    def test_get_asset_query_with_marquee_id(self):
        """Test query construction with MARQUEE_ID id_type"""
        from gs_quant.markets.securities import SecurityMaster, AssetIdentifier

        as_of = dt.date(2023, 6, 15)
        query, _ = SecurityMaster.get_asset_query(
            id_value='MA123',
            id_type=AssetIdentifier.MARQUEE_ID,
            as_of=as_of
        )
        assert query == {'id': 'MA123'}

    def test_get_asset_query_no_as_of_entered_context(self):
        """Exercise branch 1522-1523: PricingContext is entered, use pricing_date"""
        from gs_quant.markets.securities import SecurityMaster, AssetIdentifier
        from gs_quant.markets import PricingContext

        mock_ctx = MagicMock()
        mock_ctx.is_entered = True
        mock_ctx.pricing_date = dt.date(2023, 3, 15)

        with patch.object(type(PricingContext), 'current', new_callable=PropertyMock, return_value=mock_ctx):
            query, result_as_of = SecurityMaster.get_asset_query(
                id_value='AAPL',
                id_type=AssetIdentifier.BLOOMBERG_ID,
                as_of=None
            )
        assert isinstance(result_as_of, dt.datetime)
