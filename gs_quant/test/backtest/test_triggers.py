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
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd
import pytest

from gs_quant.backtests.actions import (
    Action,
    AddTradeAction,
    AddTradeActionInfo,
    AddScaledTradeAction,
    AddScaledTradeActionInfo,
    HedgeAction,
    HedgeActionInfo,
)
from gs_quant.backtests.backtest_utils import CalcType
from gs_quant.backtests.triggers import (
    TriggerDirection,
    AggType,
    TriggerRequirements,
    TriggerInfo,
    check_barrier,
    PeriodicTriggerRequirements,
    IntradayTriggerRequirements,
    MktTriggerRequirements,
    RiskTriggerRequirements,
    AggregateTriggerRequirements,
    NotTriggerRequirements,
    DateTriggerRequirements,
    PortfolioTriggerRequirements,
    MeanReversionTriggerRequirements,
    TradeCountTriggerRequirements,
    EventTriggerRequirements,
    Trigger,
    PeriodicTrigger,
    IntradayPeriodicTrigger,
    MktTrigger,
    StrategyRiskTrigger,
    AggregateTrigger,
    NotTrigger,
    DateTrigger,
    PortfolioTrigger,
    MeanReversionTrigger,
    TradeCountTrigger,
    EventTrigger,
    OrdersGeneratorTrigger,
)
from gs_quant.instrument import IRSwap


# ============================================================
# TriggerDirection enum
# ============================================================

class TestTriggerDirection:
    def test_values(self):
        assert TriggerDirection.ABOVE.value == 1
        assert TriggerDirection.BELOW.value == 2
        assert TriggerDirection.EQUAL.value == 3


# ============================================================
# AggType enum
# ============================================================

class TestAggType:
    def test_values(self):
        assert AggType.ALL_OF.value == 1
        assert AggType.ANY_OF.value == 2


# ============================================================
# TriggerRequirements base class
# ============================================================

class TestTriggerRequirements:
    def test_sub_classes_registered(self):
        subs = TriggerRequirements.sub_classes()
        assert isinstance(subs, tuple)
        assert PeriodicTriggerRequirements in subs
        assert IntradayTriggerRequirements in subs
        assert MktTriggerRequirements in subs

    def test_get_trigger_times_default(self):
        req = TriggerRequirements()
        assert req.get_trigger_times() == []

    def test_calc_type_default(self):
        req = TriggerRequirements()
        assert req.calc_type == CalcType.simple


# ============================================================
# TriggerInfo
# ============================================================

class TestTriggerInfo:
    def test_triggered_true(self):
        ti = TriggerInfo(True)
        assert bool(ti) is True
        assert ti.info_dict is None

    def test_triggered_false(self):
        ti = TriggerInfo(False)
        assert bool(ti) is False

    def test_triggered_with_info(self):
        ti = TriggerInfo(True, {'key': 'value'})
        assert ti.info_dict == {'key': 'value'}

    def test_eq_uses_is_for_bool(self):
        # __eq__ uses `is` operator comparing self.triggered to other
        ti_true = TriggerInfo(True)
        ti_false = TriggerInfo(False)
        # Comparing to True/False literals uses `is`
        assert ti_true == True  # noqa: E712 — intentional `is` check
        assert ti_false == False  # noqa: E712
        assert not (ti_true == False)  # noqa: E712
        assert not (ti_false == True)  # noqa: E712

    def test_eq_with_non_bool(self):
        # `is` comparison with non-bool should be False because 1 is not True via `is` in general
        # Actually in CPython, True is a singleton so `True is True` is True
        # but let's test with a non-bool value
        ti = TriggerInfo(True)
        # True is not 1 (via `is`)... actually in CPython True is 1 because bool subclasses int
        # Let's test that the eq method returns based on identity
        assert (ti == True) is True  # noqa: E712
        # A non-singleton won't pass the `is` check
        assert (ti == 'True') is False

    def test_bool_returns_triggered(self):
        assert bool(TriggerInfo(True)) is True
        assert bool(TriggerInfo(False)) is False


# ============================================================
# check_barrier
# ============================================================

class TestCheckBarrier:
    def test_above_triggered(self):
        result = check_barrier(TriggerDirection.ABOVE, 10, 5)
        assert result.triggered is True

    def test_above_not_triggered_equal(self):
        result = check_barrier(TriggerDirection.ABOVE, 5, 5)
        assert result.triggered is False

    def test_above_not_triggered_below(self):
        result = check_barrier(TriggerDirection.ABOVE, 3, 5)
        assert result.triggered is False

    def test_below_triggered(self):
        result = check_barrier(TriggerDirection.BELOW, 3, 5)
        assert result.triggered is True

    def test_below_not_triggered_equal(self):
        result = check_barrier(TriggerDirection.BELOW, 5, 5)
        assert result.triggered is False

    def test_below_not_triggered_above(self):
        result = check_barrier(TriggerDirection.BELOW, 10, 5)
        assert result.triggered is False

    def test_equal_triggered(self):
        result = check_barrier(TriggerDirection.EQUAL, 5, 5)
        assert result.triggered is True

    def test_equal_not_triggered(self):
        result = check_barrier(TriggerDirection.EQUAL, 4, 5)
        assert result.triggered is False


# ============================================================
# PeriodicTriggerRequirements
# ============================================================

class TestPeriodicTriggerRequirements:
    @patch('gs_quant.backtests.triggers.RelativeDateSchedule')
    def test_get_trigger_times_lazy(self, mock_rds_cls):
        dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]
        mock_rds_cls.return_value.apply_rule.return_value = dates

        req = PeriodicTriggerRequirements.__new__(PeriodicTriggerRequirements)
        req.start_date = dt.date(2021, 1, 1)
        req.end_date = dt.date(2021, 12, 31)
        req.frequency = '1m'
        req.calendar = None
        req.trigger_dates = []

        result = req.get_trigger_times()
        assert result == dates
        mock_rds_cls.assert_called_once_with('1m', dt.date(2021, 1, 1), dt.date(2021, 12, 31))

        # Second call should not re-fetch (lazy)
        mock_rds_cls.reset_mock()
        result2 = req.get_trigger_times()
        assert result2 == dates
        mock_rds_cls.assert_not_called()

    @patch('gs_quant.backtests.triggers.RelativeDateSchedule')
    def test_has_triggered_in_dates(self, mock_rds_cls):
        dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1), dt.date(2021, 3, 1)]
        mock_rds_cls.return_value.apply_rule.return_value = dates

        req = PeriodicTriggerRequirements.__new__(PeriodicTriggerRequirements)
        req.start_date = dt.date(2021, 1, 1)
        req.end_date = dt.date(2021, 12, 31)
        req.frequency = '1m'
        req.calendar = None
        req.trigger_dates = []

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict is not None
        assert AddTradeAction in result.info_dict
        assert result.info_dict[AddTradeAction].next_schedule == dt.date(2021, 2, 1)
        assert AddScaledTradeAction in result.info_dict
        assert HedgeAction in result.info_dict

    @patch('gs_quant.backtests.triggers.RelativeDateSchedule')
    def test_has_triggered_last_date_next_is_none(self, mock_rds_cls):
        dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]
        mock_rds_cls.return_value.apply_rule.return_value = dates

        req = PeriodicTriggerRequirements.__new__(PeriodicTriggerRequirements)
        req.start_date = dt.date(2021, 1, 1)
        req.end_date = dt.date(2021, 12, 31)
        req.frequency = '1m'
        req.calendar = None
        req.trigger_dates = []

        result = req.has_triggered(dt.date(2021, 2, 1))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].next_schedule is None

    @patch('gs_quant.backtests.triggers.RelativeDateSchedule')
    def test_has_triggered_not_in_dates(self, mock_rds_cls):
        dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]
        mock_rds_cls.return_value.apply_rule.return_value = dates

        req = PeriodicTriggerRequirements.__new__(PeriodicTriggerRequirements)
        req.start_date = dt.date(2021, 1, 1)
        req.end_date = dt.date(2021, 12, 31)
        req.frequency = '1m'
        req.calendar = None
        req.trigger_dates = []

        result = req.has_triggered(dt.date(2021, 1, 15))
        assert result.triggered is False

    @patch('gs_quant.backtests.triggers.RelativeDateSchedule')
    def test_has_triggered_calls_get_trigger_times_if_empty(self, mock_rds_cls):
        dates = [dt.date(2021, 1, 4)]
        mock_rds_cls.return_value.apply_rule.return_value = dates

        req = PeriodicTriggerRequirements.__new__(PeriodicTriggerRequirements)
        req.start_date = dt.date(2021, 1, 1)
        req.end_date = dt.date(2021, 12, 31)
        req.frequency = '1m'
        req.calendar = None
        req.trigger_dates = []

        # trigger_dates is empty, so has_triggered should populate it
        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        mock_rds_cls.assert_called_once()


# ============================================================
# IntradayTriggerRequirements
# ============================================================

class TestIntradayTriggerRequirements:
    def test_post_init_generates_times(self):
        req = IntradayTriggerRequirements.__new__(IntradayTriggerRequirements)
        req.start_time = dt.time(9, 0)
        req.end_time = dt.time(10, 0)
        req.frequency = 30  # minutes
        req.__post_init__()

        times = req.get_trigger_times()
        assert dt.time(9, 0) in times
        assert dt.time(9, 30) in times
        assert dt.time(10, 0) in times
        assert len(times) == 3

    def test_has_triggered_true(self):
        req = IntradayTriggerRequirements.__new__(IntradayTriggerRequirements)
        req.start_time = dt.time(9, 0)
        req.end_time = dt.time(10, 0)
        req.frequency = 30
        req.__post_init__()

        state = dt.datetime(2021, 1, 4, 9, 30)
        result = req.has_triggered(state)
        assert result.triggered is True

    def test_has_triggered_false(self):
        req = IntradayTriggerRequirements.__new__(IntradayTriggerRequirements)
        req.start_time = dt.time(9, 0)
        req.end_time = dt.time(10, 0)
        req.frequency = 30
        req.__post_init__()

        state = dt.datetime(2021, 1, 4, 9, 15)
        result = req.has_triggered(state)
        assert result.triggered is False

    def test_single_time(self):
        req = IntradayTriggerRequirements.__new__(IntradayTriggerRequirements)
        req.start_time = dt.time(9, 0)
        req.end_time = dt.time(9, 0)
        req.frequency = 30
        req.__post_init__()

        times = req.get_trigger_times()
        assert times == [dt.time(9, 0)]


# ============================================================
# MktTriggerRequirements
# ============================================================

class TestMktTriggerRequirements:
    def test_has_triggered_above(self):
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = 105.0

        req = MktTriggerRequirements.__new__(MktTriggerRequirements)
        req.data_source = mock_ds
        req.trigger_level = 100.0
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True

    def test_has_triggered_below_not_triggered(self):
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = 105.0

        req = MktTriggerRequirements.__new__(MktTriggerRequirements)
        req.data_source = mock_ds
        req.trigger_level = 100.0
        req.direction = TriggerDirection.BELOW

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    def test_has_triggered_equal(self):
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = 100.0

        req = MktTriggerRequirements.__new__(MktTriggerRequirements)
        req.data_source = mock_ds
        req.trigger_level = 100.0
        req.direction = TriggerDirection.EQUAL

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True

    def test_type_error_becomes_runtime_error(self):
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = None  # will cause TypeError in comparison

        req = MktTriggerRequirements.__new__(MktTriggerRequirements)
        req.data_source = mock_ds
        req.trigger_level = 100.0
        req.direction = TriggerDirection.ABOVE

        with pytest.raises(RuntimeError, match='unable to determine trigger state'):
            req.has_triggered(dt.date(2021, 1, 4))


# ============================================================
# RiskTriggerRequirements
# ============================================================

class TestRiskTriggerRequirements:
    def test_has_triggered_state_not_in_results(self):
        mock_bt = MagicMock()
        mock_bt.results = {}

        req = RiskTriggerRequirements.__new__(RiskTriggerRequirements)
        req.risk = MagicMock()
        req.trigger_level = 0.5
        req.direction = TriggerDirection.ABOVE
        req.risk_transformation = None

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is False

    def test_has_triggered_no_transformation(self):
        mock_risk = MagicMock()
        mock_risk_result = MagicMock()
        mock_risk_result.aggregate.return_value = 1.0

        mock_bt = MagicMock()
        mock_bt.results = {dt.date(2021, 1, 4): {mock_risk: mock_risk_result}}

        req = RiskTriggerRequirements.__new__(RiskTriggerRequirements)
        req.risk = mock_risk
        req.trigger_level = 0.5
        req.direction = TriggerDirection.ABOVE
        req.risk_transformation = None

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True
        mock_risk_result.aggregate.assert_called_once()

    def test_has_triggered_with_transformation(self):
        mock_risk = MagicMock()
        mock_transform = MagicMock()
        mock_transformed = MagicMock()
        mock_transformed.aggregate.return_value = 0.3

        mock_risk_result = MagicMock()
        mock_risk_result.transform.return_value = mock_transformed

        mock_bt = MagicMock()
        mock_bt.results = {dt.date(2021, 1, 4): {mock_risk: mock_risk_result}}

        req = RiskTriggerRequirements.__new__(RiskTriggerRequirements)
        req.risk = mock_risk
        req.trigger_level = 0.5
        req.direction = TriggerDirection.ABOVE
        req.risk_transformation = mock_transform

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is False  # 0.3 not > 0.5
        mock_risk_result.transform.assert_called_once_with(risk_transformation=mock_transform)
        mock_transformed.aggregate.assert_called_once_with(allow_mismatch_risk_keys=True)

    def test_calc_type_path_dependent(self):
        req = RiskTriggerRequirements.__new__(RiskTriggerRequirements)
        assert req.calc_type == CalcType.path_dependent


# ============================================================
# AggregateTriggerRequirements
# ============================================================

class TestAggregateTriggerRequirements:
    def test_setattr_extracts_trigger_requirements_from_triggers(self):
        mock_req1 = MagicMock(spec=TriggerRequirements)
        mock_req2 = MagicMock(spec=TriggerRequirements)
        mock_trigger1 = MagicMock(spec=Trigger)
        mock_trigger1.trigger_requirements = mock_req1
        mock_trigger2 = MagicMock(spec=Trigger)
        mock_trigger2.trigger_requirements = mock_req2

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ALL_OF
        req.triggers = (mock_trigger1, mock_trigger2)

        assert req.triggers == (mock_req1, mock_req2)

    def test_setattr_leaves_plain_requirements(self):
        mock_req1 = MagicMock(spec=TriggerRequirements)
        mock_req2 = MagicMock(spec=TriggerRequirements)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ALL_OF
        req.triggers = (mock_req1, mock_req2)

        # Not Trigger instances, so no extraction
        assert req.triggers == (mock_req1, mock_req2)

    def test_has_triggered_all_of_all_true(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(True, {'key1': 'val1'})
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(True, {'key2': 'val2'})

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ALL_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict == {'key1': 'val1', 'key2': 'val2'}

    def test_has_triggered_all_of_one_false(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(True, {'key1': 'val1'})
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(False)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ALL_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    def test_has_triggered_all_of_no_info_dict(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(True, None)
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(True, None)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ALL_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict == {}

    def test_has_triggered_any_of_one_true(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(True, {'key1': 'val1'})
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(False)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ANY_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict == {'key1': 'val1'}

    def test_has_triggered_any_of_none_true(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(False)
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(False)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ANY_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    def test_has_triggered_any_of_no_info_dict(self):
        mock_req1 = MagicMock()
        mock_req1.has_triggered.return_value = TriggerInfo(True, None)
        mock_req2 = MagicMock()
        mock_req2.has_triggered.return_value = TriggerInfo(False)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = AggType.ANY_OF
        req.triggers = (mock_req1, mock_req2)

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict == {}

    def test_has_triggered_unrecognised_agg_type(self):
        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.aggregate_type = 'INVALID'
        req.triggers = (MagicMock(),)

        with pytest.raises(RuntimeError, match='Unrecognised aggregation type'):
            req.has_triggered(dt.date(2021, 1, 4))

    def test_calc_type_path_dependent(self):
        mock_req1 = MagicMock()
        type(mock_req1).calc_type = PropertyMock(return_value=CalcType.path_dependent)
        mock_req2 = MagicMock()
        type(mock_req2).calc_type = PropertyMock(return_value=CalcType.simple)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.triggers = (mock_req1, mock_req2)

        assert req.calc_type == CalcType.path_dependent

    def test_calc_type_semi_path_dependent(self):
        mock_req1 = MagicMock()
        type(mock_req1).calc_type = PropertyMock(return_value=CalcType.semi_path_dependent)
        mock_req2 = MagicMock()
        type(mock_req2).calc_type = PropertyMock(return_value=CalcType.simple)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.triggers = (mock_req1, mock_req2)

        assert req.calc_type == CalcType.semi_path_dependent

    def test_calc_type_simple(self):
        mock_req1 = MagicMock()
        type(mock_req1).calc_type = PropertyMock(return_value=CalcType.simple)
        mock_req2 = MagicMock()
        type(mock_req2).calc_type = PropertyMock(return_value=CalcType.simple)

        req = AggregateTriggerRequirements.__new__(AggregateTriggerRequirements)
        req.triggers = (mock_req1, mock_req2)

        assert req.calc_type == CalcType.simple


# ============================================================
# NotTriggerRequirements
# ============================================================

class TestNotTriggerRequirements:
    def test_setattr_extracts_from_trigger(self):
        mock_req = MagicMock(spec=TriggerRequirements)
        mock_trigger = MagicMock(spec=Trigger)
        mock_trigger.trigger_requirements = mock_req

        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.trigger = mock_trigger

        assert req.trigger == mock_req

    def test_setattr_leaves_plain_requirement(self):
        mock_req = MagicMock(spec=TriggerRequirements)

        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.trigger = mock_req

        assert req.trigger == mock_req

    def test_has_triggered_inverts_true(self):
        mock_req = MagicMock()
        mock_req.has_triggered.return_value = TriggerInfo(True)

        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.trigger = mock_req

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    def test_has_triggered_inverts_false(self):
        mock_req = MagicMock()
        mock_req.has_triggered.return_value = TriggerInfo(False)

        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.trigger = mock_req

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True


# ============================================================
# DateTriggerRequirements
# ============================================================

class TestDateTriggerRequirements:
    def test_post_init_entire_day_converts_datetimes(self):
        dates = [dt.datetime(2021, 11, 9, 14, 0), dt.datetime(2021, 11, 10, 14, 0)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = True
        req.__post_init__()

        assert req.dates_from_datetimes == [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]

    def test_post_init_no_entire_day(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        assert req.dates_from_datetimes is None

    def test_post_init_entire_day_with_date_objects(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = True
        req.__post_init__()

        assert req.dates_from_datetimes == [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]

    def test_has_triggered_not_entire_day_date_in_list(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10), dt.date(2021, 11, 11)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        result = req.has_triggered(dt.date(2021, 11, 10))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].next_schedule == dt.date(2021, 11, 11)

    def test_has_triggered_not_entire_day_date_not_in_list(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 11)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        result = req.has_triggered(dt.date(2021, 11, 10))
        assert result.triggered is False

    def test_has_triggered_entire_day_with_datetime_state(self):
        dates = [dt.datetime(2021, 11, 9, 14, 0), dt.datetime(2021, 11, 10, 14, 0)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = True
        req.__post_init__()

        # datetime state should be converted to date
        result = req.has_triggered(dt.datetime(2021, 11, 9, 10, 0))
        assert result.triggered is True

    def test_has_triggered_entire_day_with_date_state(self):
        dates = [dt.datetime(2021, 11, 9, 14, 0), dt.datetime(2021, 11, 10, 14, 0)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = True
        req.__post_init__()

        result = req.has_triggered(dt.date(2021, 11, 9))
        assert result.triggered is True

    def test_has_triggered_last_date_next_is_none(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        result = req.has_triggered(dt.date(2021, 11, 10))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].next_schedule is None

    def test_has_triggered_info_dict_keys(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        result = req.has_triggered(dt.date(2021, 11, 9))
        assert AddTradeAction in result.info_dict
        assert AddScaledTradeAction in result.info_dict
        assert HedgeAction in result.info_dict

    def test_get_trigger_times_entire_day(self):
        dates = [dt.datetime(2021, 11, 9, 14, 0), dt.datetime(2021, 11, 10, 14, 0)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = True
        req.__post_init__()

        result = req.get_trigger_times()
        assert result == [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]

    def test_get_trigger_times_not_entire_day(self):
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        req = DateTriggerRequirements.__new__(DateTriggerRequirements)
        req.dates = dates
        req.entire_day = False
        req.__post_init__()

        result = req.get_trigger_times()
        assert result == dates


# ============================================================
# PortfolioTriggerRequirements
# ============================================================

class TestPortfolioTriggerRequirements:
    def test_has_triggered_len_above(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict = {1: 'a', 2: 'b', 3: 'c'}

        req = PortfolioTriggerRequirements.__new__(PortfolioTriggerRequirements)
        req.data_source = 'len'
        req.trigger_level = 2
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_len_below(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict = {1: 'a'}

        req = PortfolioTriggerRequirements.__new__(PortfolioTriggerRequirements)
        req.data_source = 'len'
        req.trigger_level = 2
        req.direction = TriggerDirection.BELOW

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_len_equal(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict = {1: 'a', 2: 'b'}

        req = PortfolioTriggerRequirements.__new__(PortfolioTriggerRequirements)
        req.data_source = 'len'
        req.trigger_level = 2
        req.direction = TriggerDirection.EQUAL

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_len_not_triggered(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict = {1: 'a', 2: 'b'}

        req = PortfolioTriggerRequirements.__new__(PortfolioTriggerRequirements)
        req.data_source = 'len'
        req.trigger_level = 2
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is False

    def test_has_triggered_non_len_data_source(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict = {1: 'a', 2: 'b', 3: 'c'}

        req = PortfolioTriggerRequirements.__new__(PortfolioTriggerRequirements)
        req.data_source = 'something_else'
        req.trigger_level = 2
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is False


# ============================================================
# MeanReversionTriggerRequirements
# ============================================================

class TestMeanReversionTriggerRequirements:
    def _make_req(self, current_position=0):
        mock_ds = MagicMock()
        req = MeanReversionTriggerRequirements.__new__(MeanReversionTriggerRequirements)
        req.data_source = mock_ds
        req.z_score_bound = 1.0
        req.rolling_mean_window = 20
        req.rolling_std_window = 20
        req.current_position = current_position
        return req

    def _make_data_range_side_effect(self, mean_val, std_val):
        return [
            MagicMock(mean=MagicMock(return_value=mean_val)),
            MagicMock(std=MagicMock(return_value=std_val)),
        ]

    @patch('gs_quant.backtests.triggers.AddTradeActionInfo',
           new=lambda scaling: MagicMock(scaling=scaling))
    def test_no_position_above_mean_enters_short(self):
        req = self._make_req(0)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 110.0  # (110-100)/5 = 2.0 > 1.0; above mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].scaling == -1
        assert req.current_position == -1

    @patch('gs_quant.backtests.triggers.AddTradeActionInfo',
           new=lambda scaling: MagicMock(scaling=scaling))
    def test_no_position_below_mean_enters_long(self):
        req = self._make_req(0)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 90.0  # (90-100)/5 = -2.0, abs=2.0 > 1.0; below mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].scaling == 1
        assert req.current_position == 1

    def test_no_position_within_bound_no_trigger(self):
        req = self._make_req(0)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 102.0  # (102-100)/5 = 0.4 < 1.0

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    @patch('gs_quant.backtests.triggers.AddTradeActionInfo',
           new=lambda scaling: MagicMock(scaling=scaling))
    def test_long_position_exits_when_above_mean(self):
        req = self._make_req(1)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 105.0  # above mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].scaling == -1
        assert req.current_position == 0

    def test_long_position_stays_when_below_mean(self):
        req = self._make_req(1)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 95.0  # below mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    @patch('gs_quant.backtests.triggers.AddTradeActionInfo',
           new=lambda scaling: MagicMock(scaling=scaling))
    def test_short_position_exits_when_above_mean(self):
        req = self._make_req(-1)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 105.0  # above mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].scaling == 1
        assert req.current_position == 0

    def test_short_position_stays_when_below_mean(self):
        req = self._make_req(-1)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 95.0  # below mean

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is False

    def test_unexpected_position_raises(self):
        req = self._make_req(99)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 105.0

        with pytest.raises(RuntimeWarning, match='unexpected current position'):
            req.has_triggered(dt.date(2021, 1, 4))

    def test_no_position_above_mean_hits_type_error_without_mock(self):
        """Verifies the known issue: AddTradeActionInfo is called with only
        'scaling' keyword but requires 'next_schedule' positional arg too,
        resulting in a TypeError at runtime."""
        req = self._make_req(0)
        req.data_source.get_data_range.side_effect = self._make_data_range_side_effect(100.0, 5.0)
        req.data_source.get_data.return_value = 110.0

        with pytest.raises(TypeError):
            req.has_triggered(dt.date(2021, 1, 4))


# ============================================================
# TradeCountTriggerRequirements
# ============================================================

class TestTradeCountTriggerRequirements:
    def test_has_triggered_above(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict.get.return_value = ['trade1', 'trade2', 'trade3']

        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        req.trade_count = 2
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_below(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict.get.return_value = ['trade1']

        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        req.trade_count = 2
        req.direction = TriggerDirection.BELOW

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_equal(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict.get.return_value = ['trade1', 'trade2']

        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        req.trade_count = 2
        req.direction = TriggerDirection.EQUAL

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_has_triggered_not_triggered(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict.get.return_value = ['trade1', 'trade2']

        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        req.trade_count = 2
        req.direction = TriggerDirection.ABOVE

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is False

    def test_has_triggered_empty_portfolio(self):
        mock_bt = MagicMock()
        mock_bt.portfolio_dict.get.return_value = []

        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        req.trade_count = 0
        req.direction = TriggerDirection.EQUAL

        result = req.has_triggered(dt.date(2021, 1, 4), mock_bt)
        assert result.triggered is True

    def test_calc_type_path_dependent(self):
        req = TradeCountTriggerRequirements.__new__(TradeCountTriggerRequirements)
        assert req.calc_type == CalcType.path_dependent


# ============================================================
# EventTriggerRequirements
# ============================================================

class TestEventTriggerRequirements:
    def test_post_init_default_data_source(self):
        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 0
        req.data_source = None
        req.trigger_dates = []
        req.__post_init__()

        from gs_quant.backtests.data_sources import GsDataSource
        assert isinstance(req.data_source, GsDataSource)
        assert req.data_source.data_set == 'MACRO_EVENTS_CALENDAR'

    def test_post_init_preserves_data_source(self):
        mock_ds = MagicMock()
        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 0
        req.data_source = mock_ds
        req.trigger_dates = []
        req.__post_init__()

        assert req.data_source is mock_ds

    def test_get_trigger_times_lazy(self):
        mock_ds = MagicMock()
        mock_index = MagicMock()
        mock_index.__iter__ = MagicMock(return_value=iter([
            dt.datetime(2021, 1, 4),
            dt.datetime(2021, 2, 1),
        ]))
        mock_df = MagicMock()
        mock_df.index = mock_index
        mock_ds.get_data.return_value = mock_df

        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 1
        req.data_source = mock_ds
        req.trigger_dates = []

        result = req.get_trigger_times()
        assert result == [dt.date(2021, 1, 5), dt.date(2021, 2, 2)]  # offset_days=1

        # Second call should not re-fetch (lazy)
        mock_ds.get_data.reset_mock()
        result2 = req.get_trigger_times()
        assert result2 == [dt.date(2021, 1, 5), dt.date(2021, 2, 2)]
        mock_ds.get_data.assert_not_called()

    def test_has_triggered_in_dates(self):
        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 0
        req.data_source = MagicMock()
        req.trigger_dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]

        result = req.has_triggered(dt.date(2021, 1, 4))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].next_schedule == dt.date(2021, 2, 1)

    def test_has_triggered_last_date(self):
        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 0
        req.data_source = MagicMock()
        req.trigger_dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]

        result = req.has_triggered(dt.date(2021, 2, 1))
        assert result.triggered is True
        assert result.info_dict[AddTradeAction].next_schedule is None

    def test_has_triggered_not_in_dates(self):
        req = EventTriggerRequirements.__new__(EventTriggerRequirements)
        req.event_name = 'FOMC'
        req.offset_days = 0
        req.data_source = MagicMock()
        req.trigger_dates = [dt.date(2021, 1, 4), dt.date(2021, 2, 1)]

        result = req.has_triggered(dt.date(2021, 1, 15))
        assert result.triggered is False

    @patch('gs_quant.backtests.triggers.Dataset')
    def test_list_events(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = {'eventName': MagicMock(unique=MagicMock(return_value=['FOMC', 'NFP']))}
        mock_dataset_cls.return_value = mock_ds

        result = EventTriggerRequirements.list_events('USD', start=dt.datetime(2021, 1, 1), end=dt.datetime(2021, 12, 31))
        mock_dataset_cls.assert_called_once_with('MACRO_EVENTS_CALENDAR')
        assert result == ['FOMC', 'NFP']


# ============================================================
# Trigger (base class)
# ============================================================

class TestTrigger:
    def test_sub_classes_registered(self):
        subs = Trigger.sub_classes()
        assert isinstance(subs, tuple)
        assert PeriodicTrigger in subs
        assert IntradayPeriodicTrigger in subs
        assert MktTrigger in subs

    def test_post_init_wraps_actions_in_list(self):
        mock_action = MagicMock(spec=Action)
        mock_req = MagicMock(spec=TriggerRequirements)

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = mock_action
        trigger.__post_init__()

        assert trigger.actions == [mock_action]

    def test_post_init_none_actions_becomes_empty_list(self):
        mock_req = MagicMock(spec=TriggerRequirements)
        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = None
        trigger.__post_init__()

        assert trigger.actions == []

    def test_has_triggered_delegates(self):
        mock_req = MagicMock()
        mock_req.has_triggered.return_value = TriggerInfo(True)

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = None
        trigger.__post_init__()

        result = trigger.has_triggered(dt.date(2021, 1, 4), None)
        assert result.triggered is True
        mock_req.has_triggered.assert_called_once_with(dt.date(2021, 1, 4), None)

    def test_get_trigger_times_delegates(self):
        mock_req = MagicMock()
        mock_req.get_trigger_times.return_value = [dt.date(2021, 1, 4)]

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = None
        trigger.__post_init__()

        result = trigger.get_trigger_times()
        assert result == [dt.date(2021, 1, 4)]

    def test_calc_type_delegates(self):
        mock_req = MagicMock()
        type(mock_req).calc_type = PropertyMock(return_value=CalcType.path_dependent)

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = mock_req

        assert trigger.calc_type == CalcType.path_dependent

    def test_risks_property(self):
        mock_action1 = MagicMock()
        mock_action1.risk = 'risk1'
        mock_action2 = MagicMock()
        mock_action2.risk = None
        mock_action3 = MagicMock()
        mock_action3.risk = 'risk3'

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = MagicMock()
        trigger.actions = [mock_action1, mock_action2, mock_action3]

        risks = trigger.risks
        assert risks == ['risk1', 'risk3']

    def test_risks_property_no_risks(self):
        mock_action = MagicMock()
        mock_action.risk = None

        trigger = Trigger.__new__(Trigger)
        trigger.trigger_requirements = MagicMock()
        trigger.actions = [mock_action]

        assert trigger.risks == []


# ============================================================
# StrategyRiskTrigger
# ============================================================

class TestStrategyRiskTrigger:
    def test_risks_includes_trigger_risk(self):
        mock_req = MagicMock()
        mock_req.risk = 'trigger_risk'

        mock_action = MagicMock()
        mock_action.risk = 'action_risk'

        trigger = StrategyRiskTrigger.__new__(StrategyRiskTrigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = [mock_action]

        risks = trigger.risks
        assert 'action_risk' in risks
        assert 'trigger_risk' in risks

    def test_risks_no_action_risk(self):
        mock_req = MagicMock()
        mock_req.risk = 'trigger_risk'

        mock_action = MagicMock()
        mock_action.risk = None

        trigger = StrategyRiskTrigger.__new__(StrategyRiskTrigger)
        trigger.trigger_requirements = mock_req
        trigger.actions = [mock_action]

        risks = trigger.risks
        assert risks == ['trigger_risk']


# ============================================================
# OrdersGeneratorTrigger
# ============================================================

class TestOrdersGeneratorTrigger:
    def test_post_init_creates_default_action(self):
        trigger = OrdersGeneratorTrigger.__new__(OrdersGeneratorTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        assert len(trigger.actions) == 1
        assert isinstance(trigger.actions[0], Action)

    def test_post_init_preserves_existing_actions(self):
        mock_action = MagicMock(spec=Action)
        trigger = OrdersGeneratorTrigger.__new__(OrdersGeneratorTrigger)
        trigger.trigger_requirements = None
        trigger.actions = [mock_action]
        trigger.__post_init__()

        assert len(trigger.actions) == 1
        assert trigger.actions[0] is mock_action

    def test_get_trigger_times_raises(self):
        trigger = OrdersGeneratorTrigger.__new__(OrdersGeneratorTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        with pytest.raises(RuntimeError, match='get_trigger_times must be implemented'):
            trigger.get_trigger_times()

    def test_generate_orders_raises(self):
        trigger = OrdersGeneratorTrigger.__new__(OrdersGeneratorTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        with pytest.raises(RuntimeError, match='generate_orders must be implemented'):
            trigger.generate_orders(dt.datetime(2021, 1, 4, 9, 0))

    def test_has_triggered_not_in_trigger_times(self):
        class ConcreteOGTrigger(OrdersGeneratorTrigger):
            def get_trigger_times(self):
                return [dt.time(9, 0), dt.time(10, 0)]

            def generate_orders(self, state, backtest=None):
                return [MagicMock()]

        trigger = ConcreteOGTrigger.__new__(ConcreteOGTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        # Call with a time not in trigger_times
        result = trigger.has_triggered(dt.datetime(2021, 1, 4, 11, 0))
        assert result.triggered is False

    def test_has_triggered_in_trigger_times_with_orders(self):
        mock_order = MagicMock()

        class ConcreteOGTrigger(OrdersGeneratorTrigger):
            def get_trigger_times(self):
                return [dt.time(9, 0), dt.time(10, 0)]

            def generate_orders(self, state, backtest=None):
                return [mock_order]

        trigger = ConcreteOGTrigger.__new__(ConcreteOGTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        result = trigger.has_triggered(dt.datetime(2021, 1, 4, 9, 0))
        assert result.triggered is True
        # info_dict should map action type to orders
        for action in trigger.actions:
            assert type(action) in result.info_dict

    def test_has_triggered_in_trigger_times_no_orders(self):
        class ConcreteOGTrigger(OrdersGeneratorTrigger):
            def get_trigger_times(self):
                return [dt.time(9, 0)]

            def generate_orders(self, state, backtest=None):
                return []

        trigger = ConcreteOGTrigger.__new__(ConcreteOGTrigger)
        trigger.trigger_requirements = None
        trigger.actions = None
        trigger.__post_init__()

        result = trigger.has_triggered(dt.datetime(2021, 1, 4, 9, 0))
        assert result.triggered is False


# ============================================================
# Integration tests using real DateTrigger (already tested in existing test file,
# but we augment with additional branch coverage)
# ============================================================

class TestDateTriggerIntegration:
    def test_date_trigger_with_dates_only(self):
        action = AddTradeAction(IRSwap(), name='TestAction')
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10), dt.date(2021, 11, 11)]
        trigger = DateTrigger(DateTriggerRequirements(dates), [action])

        # Not triggered for datetime (not in exact date list as datetime)
        assert not trigger.has_triggered(dt.datetime(2021, 11, 10, 14, 0))
        # Triggered for exact date
        assert trigger.has_triggered(dt.date(2021, 11, 10))
        # Not triggered for date not in list
        assert not trigger.has_triggered(dt.date(2021, 11, 12))

    def test_date_trigger_with_datetimes_entire_day(self):
        action = AddTradeAction(IRSwap(), name='TestAction2')
        dates = [dt.datetime(2021, 11, 9, 14, 0), dt.datetime(2021, 11, 10, 14, 0)]
        trigger = DateTrigger(DateTriggerRequirements(dates, entire_day=True), [action])

        # Both datetime and date should trigger
        assert trigger.has_triggered(dt.datetime(2021, 11, 9, 10, 0))
        assert trigger.has_triggered(dt.date(2021, 11, 9))


class TestNotTriggerIntegration:
    def test_not_trigger_with_date_trigger(self):
        action = AddTradeAction(IRSwap(), name='NotAction')
        dates = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        inner_trigger = DateTrigger(DateTriggerRequirements(dates), [action])

        not_trigger = NotTrigger(NotTriggerRequirements(inner_trigger), [action])

        # Date NOT in dates => trigger fires
        assert not_trigger.has_triggered(dt.date(2021, 11, 8))
        # Date in dates => trigger does not fire
        assert not not_trigger.has_triggered(dt.date(2021, 11, 9))


class TestAggregateTriggerIntegration:
    def test_aggregate_all_of(self):
        action = AddTradeAction(IRSwap(), name='AggAction')
        dates1 = [dt.date(2021, 11, 9), dt.date(2021, 11, 10)]
        dates2 = [dt.date(2021, 11, 10), dt.date(2021, 11, 11)]

        t1 = DateTrigger(DateTriggerRequirements(dates1), [action])
        t2 = DateTrigger(DateTriggerRequirements(dates2), [action])

        agg = AggregateTrigger(
            AggregateTriggerRequirements([t1, t2], aggregate_type=AggType.ALL_OF)
        )

        # Only Nov 10 is in both
        assert not agg.has_triggered(dt.date(2021, 11, 9))
        assert agg.has_triggered(dt.date(2021, 11, 10))
        assert not agg.has_triggered(dt.date(2021, 11, 11))

    def test_aggregate_any_of(self):
        action = AddTradeAction(IRSwap(), name='AggAction2')
        dates1 = [dt.date(2021, 11, 9)]
        dates2 = [dt.date(2021, 11, 10)]

        t1 = DateTrigger(DateTriggerRequirements(dates1), [action])
        t2 = DateTrigger(DateTriggerRequirements(dates2), [action])

        agg = AggregateTrigger(
            AggregateTriggerRequirements([t1, t2], aggregate_type=AggType.ANY_OF)
        )

        assert agg.has_triggered(dt.date(2021, 11, 9))
        assert agg.has_triggered(dt.date(2021, 11, 10))
        assert not agg.has_triggered(dt.date(2021, 11, 11))


# ============================================================
# Trigger subclass instantiation
# ============================================================

class TestTriggerSubclasses:
    def test_periodic_trigger_class_type(self):
        assert PeriodicTrigger.__dataclass_fields__['class_type'].default == 'periodic_trigger'

    def test_intraday_periodic_trigger_class_type(self):
        assert IntradayPeriodicTrigger.__dataclass_fields__['class_type'].default == 'intraday_periodic_trigger'

    def test_mkt_trigger_class_type(self):
        assert MktTrigger.__dataclass_fields__['class_type'].default == 'mkt_trigger'

    def test_strategy_risk_trigger_class_type(self):
        assert StrategyRiskTrigger.__dataclass_fields__['class_type'].default == 'strategy_risk_trigger'

    def test_aggregate_trigger_class_type(self):
        assert AggregateTrigger.__dataclass_fields__['class_type'].default == 'aggregate_trigger'

    def test_not_trigger_class_type(self):
        assert NotTrigger.__dataclass_fields__['class_type'].default == 'not_trigger'

    def test_date_trigger_class_type(self):
        assert DateTrigger.__dataclass_fields__['class_type'].default == 'date_trigger'

    def test_portfolio_trigger_class_type(self):
        assert PortfolioTrigger.__dataclass_fields__['class_type'].default == 'portfolio_trigger'

    def test_mean_reversion_trigger_class_type(self):
        assert MeanReversionTrigger.__dataclass_fields__['class_type'].default == 'mean_reversion_trigger'

    def test_trade_count_trigger_class_type(self):
        assert TradeCountTrigger.__dataclass_fields__['class_type'].default == 'trade_count_trigger'

    def test_event_trigger_class_type(self):
        assert EventTrigger.__dataclass_fields__['class_type'].default == 'event_trigger'


# =============================================================================
# Phase 6 – additional branch-coverage tests
# =============================================================================


class TestNotTriggerRequirementsSetattr:
    """Cover branch [266,-265]: __setattr__ for non-'trigger' keys."""

    def test_setattr_non_trigger_key(self):
        """Setting a key other than 'trigger' should NOT call super().__setattr__
        via the trigger branch -> branch [266,-265]."""
        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        # Setting 'class_type' which is != 'trigger' should go through the else branch
        # (or rather, the `if key == 'trigger'` is False, so __setattr__ does nothing
        # for non-trigger keys in the custom implementation)
        # Actually: the custom __setattr__ only calls super().__setattr__ when key=='trigger'
        # For other keys, nothing happens (the method returns without calling super).
        # But Python's dataclass init will call __setattr__ for all fields.
        # We need to test that setting a non-trigger key doesn't break.
        # The branch [266,-265] means: line 266 `if key == 'trigger'` is False -> return
        req.__dict__['trigger'] = None  # bypass __setattr__
        req.__dict__['class_type'] = 'test'
        # Now call __setattr__ with a non-trigger key
        req.__setattr__('class_type', 'new_val')
        # Since key != 'trigger', the custom __setattr__ does nothing,
        # so the value should NOT change via super().__setattr__
        assert req.__dict__.get('class_type') == 'test'

    def test_setattr_trigger_key_with_trigger_instance(self):
        """Setting trigger key with a Trigger instance extracts trigger_requirements."""
        mock_trigger = MagicMock(spec=Trigger)
        mock_trigger.trigger_requirements = MagicMock(spec=TriggerRequirements)

        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.__dict__['class_type'] = 'not_trigger_requirements'
        req.trigger = mock_trigger
        # Should have been replaced with trigger_requirements
        assert req.trigger is mock_trigger.trigger_requirements

    def test_setattr_trigger_key_with_requirements(self):
        """Setting trigger key with TriggerRequirements directly."""
        mock_req = MagicMock(spec=TriggerRequirements)
        req = NotTriggerRequirements.__new__(NotTriggerRequirements)
        req.__dict__['class_type'] = 'not_trigger_requirements'
        req.trigger = mock_req
        assert req.trigger is mock_req


class TestPortfolioTriggerRequirementsExtraBranches:
    """Cover branches [334,339], [337,339] in PortfolioTriggerRequirements."""

    def test_below_not_triggered(self):
        """BELOW direction, value >= trigger_level -> [334,339]."""
        req = PortfolioTriggerRequirements(
            data_source='len',
            direction=TriggerDirection.BELOW,
            trigger_level=5,
        )
        bt = MagicMock()
        bt.portfolio_dict = {1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e', 6: 'f'}
        result = req.has_triggered(dt.date(2024, 1, 1), bt)
        assert result.triggered is False

    def test_equal_direction_not_triggered(self):
        """EQUAL direction (else branch), value != trigger_level -> [337,339]."""
        req = PortfolioTriggerRequirements(
            data_source='len',
            direction=TriggerDirection.EQUAL,
            trigger_level=5,
        )
        bt = MagicMock()
        bt.portfolio_dict = {1: 'a', 2: 'b'}
        result = req.has_triggered(dt.date(2024, 1, 1), bt)
        assert result.triggered is False


class TestTradeCountTriggerRequirementsExtraBranches:
    """Cover branches [392,397], [395,397] in TradeCountTriggerRequirements."""

    def test_below_not_triggered(self):
        """BELOW direction, value >= trade_count -> [392,397]."""
        req = TradeCountTriggerRequirements(
            direction=TriggerDirection.BELOW,
            trade_count=2,
        )
        bt = MagicMock()
        bt.portfolio_dict = {dt.date(2024, 1, 1): ['a', 'b', 'c']}
        result = req.has_triggered(dt.date(2024, 1, 1), bt)
        assert result.triggered is False

    def test_equal_direction_not_triggered(self):
        """EQUAL direction (else), value != trade_count -> [395,397]."""
        req = TradeCountTriggerRequirements(
            direction=TriggerDirection.EQUAL,
            trade_count=5,
        )
        bt = MagicMock()
        bt.portfolio_dict = {dt.date(2024, 1, 1): ['a', 'b']}
        result = req.has_triggered(dt.date(2024, 1, 1), bt)
        assert result.triggered is False
