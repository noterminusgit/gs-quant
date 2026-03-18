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
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.base import RiskKey, Scenario
from gs_quant.common import RiskMeasure
from gs_quant.markets.markets import CloseMarket
from gs_quant.risk import ErrorValue, DataFrameWithInfo, FloatWithInfo, SeriesWithInfo
from gs_quant.risk.core import UnsupportedValue
from gs_quant.risk.results import (
    PricingFuture,
    CompositeResultFuture,
    MultipleRiskMeasureFuture,
    MultipleRiskMeasureResult,
    MultipleScenarioFuture,
    MultipleScenarioResult,
    HistoricalPricingFuture,
    PortfolioPath,
    PortfolioRiskResult,
    get_default_pivots,
    pivot_to_frame,
    _compose,
    _get_value_with_info,
    _risk_keys_compatible,
    _value_for_measure_or_scen,
)
from gs_quant.target.common import RiskMeasureType, RiskRequestParameters


# ---------------------------------------------------------------------------
# Shared objects for building consistent RiskKeys.
# Using the same market, params, and scenario ensures that
# _risk_keys_compatible and composition_info work correctly.
# ---------------------------------------------------------------------------

_SHARED_MARKET = CloseMarket(date=dt.date(2020, 1, 1), location='NYC')
_SHARED_PARAMS = RiskRequestParameters()
_SHARED_SCENARIO = None  # No scenario


def _risk_key(date=None, measure_name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price):
    date = date or dt.date(2020, 1, 1)
    rm = RiskMeasure(name=measure_name, measure_type=measure_type)
    return RiskKey('GS', date, _SHARED_MARKET, _SHARED_PARAMS, _SHARED_SCENARIO, rm)


def _float_with_info(value=100.0, date=None, measure_name='DollarPrice',
                     measure_type=RiskMeasureType.Dollar_Price):
    rk = _risk_key(date=date, measure_name=measure_name, measure_type=measure_type)
    return FloatWithInfo(rk, value)


def _series_with_info(values=None, dates=None, measure_name='DollarPrice',
                      measure_type=RiskMeasureType.Dollar_Price):
    if values is None:
        values = [1.0, 2.0]
    if dates is None:
        dates = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]
    rk = _risk_key(date=dates[0], measure_name=measure_name, measure_type=measure_type)
    return SeriesWithInfo(pd.Series(values, index=dates), risk_key=rk)


def _make_rm(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price):
    return RiskMeasure(name=name, measure_type=measure_type)


# ---------------------------------------------------------------------------
# PricingFuture
# ---------------------------------------------------------------------------

class TestPricingFuture:
    def test_create_with_result(self):
        f = PricingFuture(42.0)
        assert f.done()
        assert f.result() == 42.0

    def test_create_with_none_result(self):
        f = PricingFuture(None)
        assert f.done()
        assert f.result() is None

    def test_create_without_result_not_done(self):
        """PricingFuture() without args should not be done (uses default PricingContext)."""
        f = PricingFuture()
        assert not f.done()

    def test_set_result_then_done(self):
        f = PricingFuture()
        f.set_result(99.0)
        assert f.done()
        assert f.result() == 99.0

    def test_result_raises_when_context_is_entered(self):
        from gs_quant.markets import PricingContext
        with PricingContext():
            f = PricingFuture()
            with pytest.raises(RuntimeError, match='Cannot evaluate results under the same pricing context'):
                f.result(timeout=0)

    def test_add_with_float_raises(self):
        """Adding a raw float to PricingFuture(FloatWithInfo) triggers _compose which
        does not support (ScalarWithInfo, float) -- it raises RuntimeError."""
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        with pytest.raises(RuntimeError):
            _ = f + 5.0

    def test_add_with_another_pricing_future_same_date(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 1))
        f1 = PricingFuture(fwi1)
        f2 = PricingFuture(fwi2)
        result = f1 + f2
        assert isinstance(result, PricingFuture)
        assert result.done()

    def test_add_with_another_pricing_future_diff_date(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 2))
        f1 = PricingFuture(fwi1)
        f2 = PricingFuture(fwi2)
        result = f1 + f2
        assert isinstance(result, PricingFuture)
        assert isinstance(result.result(), SeriesWithInfo)

    def test_add_with_invalid_type(self):
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        with pytest.raises((ValueError, AttributeError)):
            _ = f + "invalid"

    def test_mul_with_float(self):
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        result = f * 2.0
        assert isinstance(result, PricingFuture)
        assert float(result.result()) == pytest.approx(200.0)

    def test_mul_with_int(self):
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        result = f * 3
        assert isinstance(result, PricingFuture)
        assert float(result.result()) == pytest.approx(300.0)

    def test_mul_with_invalid_type(self):
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        with pytest.raises(ValueError, match='Can only multiply by an int or float'):
            _ = f * "invalid"

    def test_done_callback(self):
        f = PricingFuture()
        callback_results = []
        f.add_done_callback(lambda fut: callback_results.append(fut.result()))
        f.set_result(42.0)
        assert callback_results == [42.0]


# ---------------------------------------------------------------------------
# CompositeResultFuture
# ---------------------------------------------------------------------------

class TestCompositeResultFuture:
    def test_all_futures_done(self):
        f1 = PricingFuture(10)
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        assert crf.done()
        assert crf.result() == [10, 20]

    def test_getitem(self):
        f1 = PricingFuture(10)
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        assert crf[0] == 10
        assert crf[1] == 20

    def test_futures_property(self):
        f1 = PricingFuture(10)
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        assert crf.futures == (f1, f2)

    def test_pending_futures_resolved_via_callback(self):
        f1 = PricingFuture()
        f2 = PricingFuture()
        crf = CompositeResultFuture([f1, f2])
        assert not crf.done()

        f1.set_result(10)
        assert not crf.done()

        f2.set_result(20)
        assert crf.done()
        assert crf.result() == [10, 20]

    def test_single_pending_future(self):
        f1 = PricingFuture()
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        assert not crf.done()

        f1.set_result(10)
        assert crf.done()
        assert crf.result() == [10, 20]

    def test_empty_futures_list(self):
        crf = CompositeResultFuture([])
        assert crf.done()
        assert crf.result() == []


# ---------------------------------------------------------------------------
# MultipleRiskMeasureResult
# ---------------------------------------------------------------------------

class TestMultipleRiskMeasureResult:
    def test_basic_creation(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0)
        fwi2 = _float_with_info(200.0, measure_name='Price', measure_type=RiskMeasureType.PV)
        result = MultipleRiskMeasureResult(instrument, {rm1: fwi1, rm2: fwi2})
        assert rm1 in result
        assert result[rm1] == fwi1
        assert result[rm2] == fwi2

    def test_instrument_property(self):
        instrument = MagicMock()
        result = MultipleRiskMeasureResult(instrument, {})
        assert result.instrument is instrument

    def test_dates_empty(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        assert result.dates == ()

    def test_dates_with_series(self):
        rm = _make_rm()
        instrument = MagicMock()
        swi = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        result = MultipleRiskMeasureResult(instrument, {rm: swi})
        assert dt.date(2020, 1, 1) in result.dates
        assert dt.date(2020, 1, 2) in result.dates

    def test_dates_with_series_non_date_index(self):
        rm = _make_rm()
        instrument = MagicMock()
        rk = _risk_key()
        swi = SeriesWithInfo(pd.Series([1.0, 2.0], index=['a', 'b']), risk_key=rk)
        result = MultipleRiskMeasureResult(instrument, {rm: swi})
        assert result.dates == ()

    def test_mul_with_float(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        scaled = result * 2.0
        assert isinstance(scaled, MultipleRiskMeasureResult)
        assert float(scaled[rm]) == pytest.approx(200.0)

    def test_mul_with_int(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        scaled = result * 3
        assert isinstance(scaled, MultipleRiskMeasureResult)
        assert float(scaled[rm]) == pytest.approx(300.0)

    def test_mul_with_invalid_type(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        val = result * "invalid"
        assert isinstance(val, ValueError)

    def test_add_with_float(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        added = result + 5.0
        assert isinstance(added, MultipleRiskMeasureResult)
        assert float(added[rm]) == pytest.approx(105.0)

    def test_add_with_int(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        added = result + 5
        assert isinstance(added, MultipleRiskMeasureResult)
        assert float(added[rm]) == pytest.approx(105.0)

    def test_add_invalid_type_raises(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        with pytest.raises(ValueError, match='Can only add instances'):
            _ = result + "bad"

    def test_getitem_with_risk_measure_key(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        assert result[rm] == fwi

    def test_getitem_with_date_on_non_historical_raises(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        with pytest.raises(ValueError, match='Can only index by date on historical results'):
            _ = result[dt.date(2020, 1, 1)]

    def test_getitem_with_date_on_series(self):
        rm = _make_rm()
        instrument = MagicMock()
        swi = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        result = MultipleRiskMeasureResult(instrument, {rm: swi})
        sliced = result[dt.date(2020, 1, 1)]
        assert isinstance(sliced, MultipleRiskMeasureResult)

    def test_getitem_with_dates_iterable_on_series(self):
        rm = _make_rm()
        instrument = MagicMock()
        swi = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        result = MultipleRiskMeasureResult(instrument, {rm: swi})
        sliced = result[[dt.date(2020, 1, 1), dt.date(2020, 1, 2)]]
        assert isinstance(sliced, MultipleRiskMeasureResult)

    def test_getitem_with_scenario_on_non_scenario_raises(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        mock_scenario = MagicMock(spec=Scenario)
        with pytest.raises(ValueError, match='Can only index by scenario on multiple scenario results'):
            _ = result[mock_scenario]

    def test_getitem_with_scenario_on_scenario_result(self):
        rm = _make_rm()
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        inner_val = _float_with_info(100.0)
        msr = MultipleScenarioResult(instrument, {scen: inner_val})
        result = MultipleRiskMeasureResult(instrument, {rm: msr})
        sliced = result[scen]
        assert isinstance(sliced, MultipleRiskMeasureResult)

    def test_getitem_with_date_on_scenario_results(self):
        rm = _make_rm()
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        swi = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        msr = MultipleScenarioResult(instrument, {scen: swi})
        result = MultipleRiskMeasureResult(instrument, {rm: msr})
        sliced = result[dt.date(2020, 1, 1)]
        assert isinstance(sliced, MultipleRiskMeasureResult)

    def test_multi_scen_key_empty(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        result = MultipleRiskMeasureResult(instrument, {rm: fwi})
        assert result._multi_scen_key == ()

    def test_multi_scen_key_with_scenario_values(self):
        rm = _make_rm()
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        inner_val = _float_with_info(100.0)
        msr = MultipleScenarioResult(instrument, {scen: inner_val})
        result = MultipleRiskMeasureResult(instrument, {rm: msr})
        assert scen in result._multi_scen_key

    def test_op_with_empty_series(self):
        rm = _make_rm()
        instrument = MagicMock()
        rk = _risk_key()
        swi = SeriesWithInfo(pd.Series([], dtype=float), risk_key=rk)
        result = MultipleRiskMeasureResult(instrument, {rm: swi})
        scaled = result * 2.0
        assert isinstance(scaled, MultipleRiskMeasureResult)

    def test_add_same_instrument_non_overlapping_measures(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0)
        fwi2 = _float_with_info(200.0, measure_name='Price', measure_type=RiskMeasureType.PV)
        mr1 = MultipleRiskMeasureResult(instrument, {rm1: fwi1})
        mr2 = MultipleRiskMeasureResult(instrument, {rm2: fwi2})
        combined = mr1 + mr2
        assert isinstance(combined, MultipleRiskMeasureResult)
        assert rm1 in combined
        assert rm2 in combined

    def test_add_different_instruments(self):
        rm = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        inst1 = MagicMock()
        inst2 = MagicMock()
        inst1.__eq__ = lambda s, o: s is o
        inst2.__eq__ = lambda s, o: s is o
        fwi1 = _float_with_info(100.0)
        fwi2 = _float_with_info(200.0)
        mr1 = MultipleRiskMeasureResult(inst1, {rm: fwi1})
        mr2 = MultipleRiskMeasureResult(inst2, {rm: fwi2})
        combined = mr1 + mr2
        assert isinstance(combined, PortfolioRiskResult)

    def test_add_overlapping_raises(self):
        rm = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0)
        fwi2 = _float_with_info(200.0)
        mr1 = MultipleRiskMeasureResult(instrument, {rm: fwi1})
        mr2 = MultipleRiskMeasureResult(instrument, {rm: fwi2})
        with pytest.raises(ValueError):
            _ = mr1 + mr2

    def test_add_same_instrument_non_overlapping_dates(self):
        rm = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        instrument = MagicMock()
        swi1 = _series_with_info([100.0], [dt.date(2020, 1, 1)])
        swi2 = _series_with_info([200.0], [dt.date(2020, 1, 2)])
        mr1 = MultipleRiskMeasureResult(instrument, {rm: swi1})
        mr2 = MultipleRiskMeasureResult(instrument, {rm: swi2})
        combined = mr1 + mr2
        assert isinstance(combined, MultipleRiskMeasureResult)


# ---------------------------------------------------------------------------
# MultipleRiskMeasureFuture
# ---------------------------------------------------------------------------

class TestMultipleRiskMeasureFuture:
    def test_creation_with_done_futures(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        instrument = MagicMock()
        f1 = PricingFuture(_float_with_info(100.0))
        f2 = PricingFuture(_float_with_info(200.0, measure_name='Price', measure_type=RiskMeasureType.PV))
        mrf = MultipleRiskMeasureFuture(instrument, {rm1: f1, rm2: f2})
        assert mrf.done()
        result = mrf.result()
        assert isinstance(result, MultipleRiskMeasureResult)
        assert float(result[rm1]) == pytest.approx(100.0)
        assert float(result[rm2]) == pytest.approx(200.0)

    def test_measures_to_futures_property(self):
        rm = _make_rm()
        instrument = MagicMock()
        f = PricingFuture(_float_with_info(100.0))
        mrf = MultipleRiskMeasureFuture(instrument, {rm: f})
        assert rm in mrf.measures_to_futures
        assert mrf.measures_to_futures[rm] is f

    def test_add_two_futures(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        instrument = MagicMock()
        f1 = PricingFuture(_float_with_info(100.0))
        f2 = PricingFuture(_float_with_info(200.0, measure_name='Price', measure_type=RiskMeasureType.PV))
        mrf1 = MultipleRiskMeasureFuture(instrument, {rm1: f1})
        mrf2 = MultipleRiskMeasureFuture(instrument, {rm2: f2})
        combined = mrf1 + mrf2
        assert isinstance(combined, MultipleRiskMeasureFuture)

    def test_pending_future_resolution(self):
        rm = _make_rm()
        instrument = MagicMock()
        f = PricingFuture()

        mrf = MultipleRiskMeasureFuture(instrument, {rm: f})
        assert not mrf.done()

        f.set_result(_float_with_info(100.0))
        assert mrf.done()
        result = mrf.result()
        assert isinstance(result, MultipleRiskMeasureResult)

    def test_add_with_non_future(self):
        """When other is not a MultipleRiskMeasureFuture, result is used as-is."""
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        f = PricingFuture(fwi)
        mrf = MultipleRiskMeasureFuture(instrument, {rm: f})
        # Passing a non-MRMF triggers the else branch
        other = MultipleRiskMeasureResult(instrument, {rm: _float_with_info(200.0)})
        combined = mrf + other
        assert isinstance(combined, MultipleRiskMeasureFuture)


# ---------------------------------------------------------------------------
# MultipleScenarioResult
# ---------------------------------------------------------------------------

class TestMultipleScenarioResult:
    def test_basic_creation(self):
        instrument = MagicMock()
        scen1 = MagicMock(spec=Scenario)
        scen2 = MagicMock(spec=Scenario)
        val1 = _float_with_info(100.0)
        val2 = _float_with_info(200.0)
        msr = MultipleScenarioResult(instrument, {scen1: val1, scen2: val2})
        assert msr[scen1] == val1
        assert msr[scen2] == val2

    def test_instrument_property(self):
        instrument = MagicMock()
        msr = MultipleScenarioResult(instrument, {})
        assert msr.instrument is instrument

    def test_scenarios_property(self):
        instrument = MagicMock()
        scen1 = MagicMock(spec=Scenario)
        scen2 = MagicMock(spec=Scenario)
        msr = MultipleScenarioResult(instrument, {scen1: 1, scen2: 2})
        assert scen1 in msr.scenarios
        assert scen2 in msr.scenarios

    def test_getitem_date_on_non_historical_raises(self):
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        val = _float_with_info(100.0)
        msr = MultipleScenarioResult(instrument, {scen: val})
        with pytest.raises(ValueError, match='Can only index by date on historical results'):
            _ = msr[dt.date(2020, 1, 1)]

    def test_getitem_date_on_historical(self):
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        swi = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        msr = MultipleScenarioResult(instrument, {scen: swi})
        sliced = msr[dt.date(2020, 1, 1)]
        assert isinstance(sliced, MultipleScenarioResult)

    def test_getitem_regular_key(self):
        instrument = MagicMock()
        scen = MagicMock(spec=Scenario)
        val = _float_with_info(100.0)
        msr = MultipleScenarioResult(instrument, {scen: val})
        assert msr[scen] == val


# ---------------------------------------------------------------------------
# MultipleScenarioFuture
# ---------------------------------------------------------------------------

class TestMultipleScenarioFuture:
    def test_set_result_non_historical(self):
        instrument = MagicMock()
        scen1 = MagicMock(spec=Scenario)
        scen2 = MagicMock(spec=Scenario)
        rk = _risk_key()

        df = DataFrameWithInfo(
            pd.DataFrame({
                'label': ['scen1', 'scen2'],
                'value': [100.0, 200.0],
            }),
            risk_key=rk,
            unit=None,
            error=None,
        )
        f = PricingFuture(df)
        msf = MultipleScenarioFuture(instrument, [scen1, scen2], [f])
        assert msf.done()
        result = msf.result()
        assert isinstance(result, MultipleScenarioResult)

    def test_set_result_historical(self):
        instrument = MagicMock()
        scen1 = MagicMock(spec=Scenario)
        scen2 = MagicMock(spec=Scenario)
        rk = _risk_key()

        dates = [dt.date(2020, 1, 1), dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 2)]
        df = DataFrameWithInfo(
            pd.DataFrame({
                'label': ['scen1', 'scen2', 'scen1', 'scen2'],
                'value': [100.0, 200.0, 110.0, 210.0],
            }, index=pd.Index(dates, name='date')),
            risk_key=rk,
            unit=None,
            error=None,
        )
        f = PricingFuture(df)
        msf = MultipleScenarioFuture(instrument, [scen1, scen2], [f])
        assert msf.done()
        result = msf.result()
        assert isinstance(result, MultipleScenarioResult)


# ---------------------------------------------------------------------------
# HistoricalPricingFuture
# ---------------------------------------------------------------------------

class TestHistoricalPricingFuture:
    def test_all_errors(self):
        rk = _risk_key()
        err1 = ErrorValue(rk, 'error1')
        err2 = ErrorValue(rk, 'error2')
        f1 = PricingFuture(err1)
        f2 = PricingFuture(err2)
        hpf = HistoricalPricingFuture([f1, f2])
        assert hpf.done()
        assert isinstance(hpf.result(), ErrorValue)

    def test_with_valid_scalar_results(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 2))
        f1 = PricingFuture(fwi1)
        f2 = PricingFuture(fwi2)
        hpf = HistoricalPricingFuture([f1, f2])
        assert hpf.done()
        result = hpf.result()
        assert isinstance(result, SeriesWithInfo)

    def test_with_multiple_risk_measure_results(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 2))
        mr1 = MultipleRiskMeasureResult(instrument, {rm: fwi1})
        mr2 = MultipleRiskMeasureResult(instrument, {rm: fwi2})
        f1 = PricingFuture(mr1)
        f2 = PricingFuture(mr2)
        hpf = HistoricalPricingFuture([f1, f2])
        assert hpf.done()
        result = hpf.result()
        assert isinstance(result, MultipleRiskMeasureResult)

    def test_mixed_error_and_valid(self):
        rk = _risk_key()
        err = ErrorValue(rk, 'error')
        fwi = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        f1 = PricingFuture(err)
        f2 = PricingFuture(fwi)
        hpf = HistoricalPricingFuture([f1, f2])
        assert hpf.done()

    def test_single_error(self):
        rk = _risk_key()
        err = ErrorValue(rk, 'error1')
        f1 = PricingFuture(err)
        hpf = HistoricalPricingFuture([f1])
        assert hpf.done()
        result = hpf.result()
        assert isinstance(result, ErrorValue)


# ---------------------------------------------------------------------------
# PortfolioPath
# ---------------------------------------------------------------------------

class TestPortfolioPath:
    def test_creation_with_int(self):
        pp = PortfolioPath(0)
        assert len(pp) == 1

    def test_creation_with_tuple(self):
        pp = PortfolioPath((0, 1))
        assert len(pp) == 2

    def test_repr(self):
        pp = PortfolioPath(0)
        assert repr(pp) == '(0,)'

    def test_iter(self):
        pp = PortfolioPath((0, 1, 2))
        assert list(pp) == [0, 1, 2]

    def test_add(self):
        pp1 = PortfolioPath(0)
        pp2 = PortfolioPath(1)
        combined = pp1 + pp2
        assert list(combined) == [0, 1]
        assert len(combined) == 2

    def test_eq(self):
        pp1 = PortfolioPath((0, 1))
        pp2 = PortfolioPath((0, 1))
        pp3 = PortfolioPath((0, 2))
        assert pp1 == pp2
        assert pp1 != pp3

    def test_hash(self):
        pp1 = PortfolioPath((0, 1))
        pp2 = PortfolioPath((0, 1))
        assert hash(pp1) == hash(pp2)
        s = {pp1, pp2}
        assert len(s) == 1

    def test_path_property(self):
        pp = PortfolioPath((0, 1))
        assert pp.path == (0, 1)

    def test_call_on_composite_result(self):
        f1 = PricingFuture(10)
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        pp = PortfolioPath(0)
        result = pp(crf)
        assert result is f1

    def test_call_with_rename_to_parent(self):
        f1 = PricingFuture(10)
        f2 = PricingFuture(20)
        crf = CompositeResultFuture([f1, f2])
        pp = PortfolioPath(0)
        result = pp(crf, rename_to_parent=True)
        assert result is f1

    def test_call_deep_path(self):
        """Deep path through nested CompositeResultFutures."""
        inner_f1 = PricingFuture(100)
        inner_f2 = PricingFuture(200)
        inner_crf = CompositeResultFuture([inner_f1, inner_f2])

        outer_f = PricingFuture(300)
        outer_crf = CompositeResultFuture([inner_crf, outer_f])

        # Path (0, 1): outer.futures[0] = inner_crf (a PricingFuture)
        # inner_crf.result() = [100, 200], then [200] => 200
        pp = PortfolioPath((0, 1))
        result = pp(outer_crf)
        assert result == 200

    def test_call_on_list(self):
        items = [10, 20, 30]
        pp = PortfolioPath(1)
        result = pp(items)
        assert result == 20

    def test_call_on_tuple(self):
        items = (10, 20, 30)
        pp = PortfolioPath(2)
        result = pp(items)
        assert result == 30


# ---------------------------------------------------------------------------
# PortfolioRiskResult
# ---------------------------------------------------------------------------

class TestPortfolioRiskResult:
    def _make_simple_prr(self, rm=None, values=None):
        from gs_quant.markets.portfolio import Portfolio
        from gs_quant.instrument import IRSwap

        rm = rm or _make_rm()
        if values is None:
            values = [100.0, 200.0]

        instruments = [
            IRSwap("Pay", "5y", "EUR", fixed_rate=-0.005, name="swap_a"),
            IRSwap("Pay", "10y", "EUR", fixed_rate=-0.005, name="swap_b"),
        ]
        portfolio = Portfolio(instruments, name="test_port")
        futures = [
            MultipleRiskMeasureFuture(inst, {rm: PricingFuture(_float_with_info(val))})
            for inst, val in zip(instruments, values)
        ]
        return PortfolioRiskResult(portfolio, (rm,), futures)

    def test_basic_creation(self):
        prr = self._make_simple_prr()
        assert prr.done()

    def test_len(self):
        prr = self._make_simple_prr()
        assert len(prr) == 2

    def test_risk_measures_property(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        assert rm in prr.risk_measures

    def test_portfolio_property(self):
        prr = self._make_simple_prr()
        assert prr.portfolio is not None
        assert prr.portfolio.name == "test_port"

    def test_result_returns_self(self):
        prr = self._make_simple_prr()
        assert prr.result() is prr

    def test_iter(self):
        prr = self._make_simple_prr()
        items = list(prr)
        assert len(items) == 2

    def test_repr(self):
        prr = self._make_simple_prr()
        r = repr(prr)
        assert 'Results' in r
        assert 'test_port' in r
        assert '(2)' in r

    def test_repr_unnamed_portfolio(self):
        from gs_quant.markets.portfolio import Portfolio
        from gs_quant.instrument import IRSwap
        rm = _make_rm()
        instruments = [IRSwap("Pay", "5y", "EUR", fixed_rate=-0.005, name="s")]
        portfolio = Portfolio(instruments)
        futures = [MultipleRiskMeasureFuture(instruments[0], {rm: PricingFuture(_float_with_info(100.0))})]
        prr = PortfolioRiskResult(portfolio, (rm,), futures)
        r = repr(prr)
        assert 'Results' in r

    def test_contains_risk_measure(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        assert rm in prr

    def test_contains_risk_measure_not_present(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        other_rm = _make_rm('Price', RiskMeasureType.PV)
        assert other_rm not in prr

    def test_contains_date_false(self):
        prr = self._make_simple_prr()
        assert dt.date(2020, 1, 1) not in prr

    def test_contains_instrument(self):
        prr = self._make_simple_prr()
        instruments = list(prr.portfolio)
        assert instruments[0] in prr

    def test_getitem_by_index(self):
        prr = self._make_simple_prr()
        result = prr[0]
        assert isinstance(result, FloatWithInfo)

    def test_getitem_by_name(self):
        prr = self._make_simple_prr()
        result = prr['swap_a']
        assert isinstance(result, FloatWithInfo)

    def test_getitem_by_slice(self):
        prr = self._make_simple_prr()
        result = prr[0:1]
        assert isinstance(result, PortfolioRiskResult)

    def test_getitem_by_single_element_list(self):
        prr = self._make_simple_prr()
        result = prr[[0]]
        assert isinstance(result, FloatWithInfo)

    def test_getitem_by_risk_measure_single(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        result = prr[rm]
        assert result is prr

    def test_getitem_by_risk_measure_not_computed(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        other_rm = _make_rm('Price', RiskMeasureType.PV)
        with pytest.raises(ValueError, match='not computed'):
            _ = prr[other_rm]

    def test_getitem_by_iterable_risk_measure_not_computed(self):
        rm = _make_rm()
        prr = self._make_simple_prr(rm=rm)
        other_rm = _make_rm('Price', RiskMeasureType.PV)
        with pytest.raises(ValueError, match='not computed'):
            _ = prr[[rm, other_rm]]

    def test_getitem_by_instrument_list(self):
        prr = self._make_simple_prr()
        instruments = list(prr.portfolio)
        result = prr[instruments]
        assert isinstance(result, PortfolioRiskResult)

    def test_dates_empty_for_spot(self):
        prr = self._make_simple_prr()
        assert prr.dates == ()

    def test_multi_scen_key_empty(self):
        prr = self._make_simple_prr()
        assert prr._multi_scen_key == ()

    def test_get_with_valid_item(self):
        prr = self._make_simple_prr()
        result = prr.get(0, 'default')
        assert result != 'default'

    def test_get_with_invalid_item(self):
        prr = self._make_simple_prr()
        result = prr.get('nonexistent_name', 'default_val')
        assert result == 'default_val'

    def test_add_invalid_type_raises(self):
        prr = self._make_simple_prr()
        with pytest.raises(ValueError, match='Can only add instances'):
            _ = prr + "bad"

    def test_mul_with_invalid_type(self):
        prr = self._make_simple_prr()
        val = prr * "bad"
        assert isinstance(val, ValueError)

    def test_aggregate_single_measure(self):
        prr = self._make_simple_prr()
        agg = prr.aggregate(allow_mismatch_risk_keys=True)
        assert isinstance(agg, float)

    def test_subset(self):
        prr = self._make_simple_prr()
        paths = prr.portfolio.all_paths
        sub = prr.subset([paths[0]], name='sub')
        assert isinstance(sub, PortfolioRiskResult)

    def test_transform_none(self):
        prr = self._make_simple_prr()
        assert prr.transform(None) is prr


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestGetDefaultPivots:
    def test_multiple_scenario_result_with_dates(self):
        result = get_default_pivots('MultipleScenarioResult', has_dates=True, multi_measures=False, multi_scen=True)
        assert result == ('value', 'scenario', 'dates')

    def test_multiple_scenario_result_without_dates(self):
        result = get_default_pivots('MultipleScenarioResult', has_dates=False, multi_measures=False, multi_scen=True)
        assert result == ('value', 'scenario', None)

    def test_multiple_risk_measure_result_with_scen(self):
        result = get_default_pivots('MultipleRiskMeasureResult', has_dates=False, multi_measures=True, multi_scen=True)
        assert result[1] == ('risk_measure', 'scenario')

    def test_multiple_risk_measure_result_without_scen(self):
        result = get_default_pivots('MultipleRiskMeasureResult', has_dates=False, multi_measures=True, multi_scen=False)
        assert result[1] == 'risk_measure'

    def test_multiple_risk_measure_result_with_dates(self):
        result = get_default_pivots('MultipleRiskMeasureResult', has_dates=True, multi_measures=True, multi_scen=False)
        assert result[2] == 'dates'

    def test_multiple_risk_measure_result_without_dates(self):
        result = get_default_pivots('MultipleRiskMeasureResult', has_dates=False, multi_measures=True, multi_scen=False)
        assert result[2] is None

    def test_portfolio_risk_result_requires_ori_cols(self):
        with pytest.raises(ValueError, match='columns of dataframe required'):
            get_default_pivots('PortfolioRiskResult', has_dates=False, multi_measures=False, multi_scen=False)

    def test_portfolio_risk_result_with_dates_multi_measures(self):
        ori_cols = ['instrument_name', 'risk_measure', 'value']
        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=True, multi_measures=True, multi_scen=False,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

    def test_portfolio_risk_result_with_dates_single_measure(self):
        ori_cols = ['instrument_name', 'risk_measure', 'value']
        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=True, multi_measures=False, multi_scen=False,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

    def test_portfolio_risk_result_no_dates_no_multi_measures_simple_port_false(self):
        ori_cols = ['instrument_name', 'risk_measure', 'value']
        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=False, multi_measures=False, multi_scen=False,
            simple_port=False, ori_cols=ori_cols
        )
        assert result[0] == 'value'

    def test_portfolio_risk_result_no_dates_multi_measures(self):
        ori_cols = ['instrument_name', 'risk_measure', 'value']
        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=False, multi_measures=True, multi_scen=False,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

    def test_portfolio_risk_result_multi_scen(self):
        ori_cols = ['instrument_name', 'risk_measure', 'value', 'scenario']
        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=True, multi_measures=True, multi_scen=True,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=True, multi_measures=False, multi_scen=True,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=False, multi_measures=True, multi_scen=True,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'

        result = get_default_pivots(
            'PortfolioRiskResult', has_dates=False, multi_measures=False, multi_scen=True,
            ori_cols=ori_cols
        )
        assert result[0] == 'value'


class TestPivotToFrame:
    def test_basic_pivot(self):
        df = pd.DataFrame({
            'value': [1.0, 2.0, 3.0, 4.0],
            'instrument': ['a', 'a', 'b', 'b'],
            'measure': ['price', 'delta', 'price', 'delta'],
        })
        result = pivot_to_frame(df, 'value', 'instrument', 'measure', 'sum')
        assert isinstance(result, pd.DataFrame)
        assert result.loc['a', 'price'] == 1.0

    def test_pivot_with_no_index(self):
        df = pd.DataFrame({
            'value': [1.0, 2.0],
            'measure': ['price', 'delta'],
        })
        result = pivot_to_frame(df, 'value', None, 'measure', 'sum')
        assert isinstance(result, pd.DataFrame)

    def test_pivot_with_no_columns(self):
        df = pd.DataFrame({
            'value': [1.0, 2.0],
            'inst': ['a', 'b'],
        })
        result = pivot_to_frame(df, 'value', 'inst', None, 'sum')
        assert isinstance(result, pd.DataFrame)


class TestGetValueWithInfo:
    def test_error_value_passthrough(self):
        rk = _risk_key()
        ev = ErrorValue(rk, 'some error')
        result = _get_value_with_info(ev, rk, None, None)
        assert isinstance(result, ErrorValue)

    def test_unsupported_value_passthrough(self):
        rk = _risk_key()
        uv = UnsupportedValue(rk)
        result = _get_value_with_info(uv, rk, None, None)
        assert isinstance(result, UnsupportedValue)

    def test_dataframe_returns_dataframe_with_info(self):
        rk = _risk_key()
        df = pd.DataFrame({'a': [1, 2]})
        result = _get_value_with_info(df, rk, None, None)
        assert isinstance(result, DataFrameWithInfo)

    def test_series_returns_series_with_info(self):
        rk = _risk_key()
        s = SeriesWithInfo([1.0, 2.0], risk_key=rk)
        result = _get_value_with_info(s, rk, None, None)
        assert isinstance(result, SeriesWithInfo)

    def test_float_returns_float_with_info(self):
        rk = _risk_key()
        result = _get_value_with_info(42.0, rk, None, None)
        assert isinstance(result, FloatWithInfo)
        assert float(result) == pytest.approx(42.0)


class TestValueForMeasureOrScen:
    def test_single_item(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        res = {rm1: 100, rm2: 200}
        filtered = _value_for_measure_or_scen(res, rm1)
        assert rm1 in filtered
        assert rm2 not in filtered

    def test_iterable_items(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        rm3 = _make_rm('Theta', RiskMeasureType.Theta)
        res = {rm1: 100, rm2: 200, rm3: 300}
        filtered = _value_for_measure_or_scen(res, [rm1, rm2])
        assert rm1 in filtered
        assert rm2 in filtered
        assert rm3 not in filtered

    def test_single_item_removes_non_matching(self):
        rm1 = _make_rm('DollarPrice', RiskMeasureType.Dollar_Price)
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        res = {rm1: 100, rm2: 200}
        filtered = _value_for_measure_or_scen(res, rm2)
        assert rm2 in filtered
        assert rm1 not in filtered


class TestCompose:
    def test_scalar_scalar_same_date(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 1))
        result = _compose(fwi1, fwi2)
        assert result is fwi2

    def test_scalar_scalar_different_date(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 2))
        result = _compose(fwi1, fwi2)
        assert isinstance(result, SeriesWithInfo)

    def test_series_series(self):
        s1 = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        s2 = _series_with_info([3.0], [dt.date(2020, 1, 3)])
        result = _compose(s1, s2)
        assert isinstance(result, (SeriesWithInfo, pd.Series))
        assert len(result) == 3

    def test_series_scalar(self):
        s = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        fwi = _float_with_info(3.0, date=dt.date(2020, 1, 3))
        result = _compose(s, fwi)
        assert isinstance(result, (SeriesWithInfo, pd.Series))

    def test_scalar_series(self):
        fwi = _float_with_info(3.0, date=dt.date(2020, 1, 3))
        s = _series_with_info([1.0, 2.0], [dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        result = _compose(fwi, s)
        assert isinstance(result, (SeriesWithInfo, pd.Series))

    def test_multiple_risk_measure_results(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 2))
        mr1 = MultipleRiskMeasureResult(instrument, {rm: fwi1})
        mr2 = MultipleRiskMeasureResult(instrument, {rm: fwi2})
        result = _compose(mr1, mr2)
        assert isinstance(result, MultipleRiskMeasureResult)

    def test_incompatible_types_raises(self):
        with pytest.raises(RuntimeError, match='cannot be composed'):
            _compose(42, 'bad')


class TestRiskKeysCompatible:
    def test_compatible(self):
        fwi1 = _float_with_info(100.0, date=dt.date(2020, 1, 1))
        fwi2 = _float_with_info(200.0, date=dt.date(2020, 1, 1))
        assert _risk_keys_compatible(fwi1, fwi2) is True

    def test_incompatible(self):
        """Different locations make keys incompatible."""
        m2 = CloseMarket(date=dt.date(2020, 1, 1), location='LDN')
        rk2 = RiskKey('GS', dt.date(2020, 1, 1), m2, _SHARED_PARAMS, _SHARED_SCENARIO,
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        fwi1 = _float_with_info(100.0)
        fwi2 = FloatWithInfo(rk2, 200.0)
        assert _risk_keys_compatible(fwi1, fwi2) is False

    def test_with_multiple_risk_measure_result_lhs(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        mr = MultipleRiskMeasureResult(instrument, {rm: fwi})
        fwi2 = _float_with_info(200.0)
        assert _risk_keys_compatible(mr, fwi2) is True

    def test_with_multiple_risk_measure_result_rhs(self):
        rm = _make_rm()
        instrument = MagicMock()
        fwi = _float_with_info(100.0)
        mr = MultipleRiskMeasureResult(instrument, {rm: fwi})
        fwi2 = _float_with_info(200.0)
        assert _risk_keys_compatible(fwi2, mr) is True

    def test_with_nested_multiple_risk_measure_result(self):
        rm1 = _make_rm()
        rm2 = _make_rm('Price', RiskMeasureType.PV)
        instrument = MagicMock()
        fwi1 = _float_with_info(100.0)
        fwi2 = _float_with_info(200.0)
        inner_mr = MultipleRiskMeasureResult(instrument, {rm1: fwi1})
        outer_mr = MultipleRiskMeasureResult(instrument, {rm2: inner_mr})
        assert _risk_keys_compatible(outer_mr, fwi2) is True
