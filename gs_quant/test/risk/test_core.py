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
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.base import RiskKey
from gs_quant.common import RiskMeasure, RiskMeasureType
from gs_quant.config import DisplayOptions
from gs_quant.target.common import PricingLocation
from gs_quant.risk.core import (
    DataFrameWithInfo,
    DictWithInfo,
    ErrorValue,
    FloatWithInfo,
    MQVSValidatorDefn,
    MQVSValidatorDefnsWithInfo,
    MQVSValidationTarget,
    ResultInfo,
    ScalarWithInfo,
    SeriesWithInfo,
    StringWithInfo,
    UnsupportedValue,
    aggregate_results,
    aggregate_risk,
    combine_risk_key,
    sort_risk,
    subtract_risk,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mock_market(location=PricingLocation.NYC):
    m = MagicMock()
    m.location = location
    return m


def _make_risk_key(date=None, location=PricingLocation.NYC, provider='GS'):
    mock_market = _make_mock_market(location)
    return RiskKey(
        provider=provider,
        date=date or dt.date(2020, 1, 1),
        market=mock_market,
        params=MagicMock(),
        scenario=MagicMock(),
        risk_measure=RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
    )


@pytest.fixture
def risk_key():
    return _make_risk_key()


@pytest.fixture
def risk_key2():
    return _make_risk_key(date=dt.date(2020, 1, 2))


# ---------------------------------------------------------------------------
# ResultInfo properties (lines 44-70)
# ---------------------------------------------------------------------------

class TestResultInfoProperties:
    def test_float_with_info_has_result_info_properties(self, risk_key):
        f = FloatWithInfo(risk_key, 1.0, unit={'USD': 1}, error='some error', request_id='req-123')
        assert f.risk_key is risk_key
        assert f.unit == {'USD': 1}
        assert f.error == 'some error'
        assert f.request_id == 'req-123'

    def test_result_info_properties_defaults(self, risk_key):
        f = FloatWithInfo(risk_key, 2.0)
        assert f.risk_key is risk_key
        assert f.unit is None
        assert f.error is None
        assert f.request_id is None


# ---------------------------------------------------------------------------
# ResultInfo.composition_info (lines 74-100)
# ---------------------------------------------------------------------------

class TestCompositionInfo:
    def test_basic_composition(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        c1 = FloatWithInfo(rk1, 10.0, unit={'USD': 1})
        c2 = FloatWithInfo(rk2, 20.0, unit={'USD': 1})

        dates, values, errors, rk, unit = ResultInfo.composition_info([c1, c2])
        assert dates == [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]
        assert values == [10.0, 20.0]
        assert errors == {}
        assert unit == {'USD': 1}

    def test_composition_with_error(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        c1 = FloatWithInfo(rk1, 10.0)
        c2 = ErrorValue(rk2, 'calc error')

        dates, values, errors, rk, unit = ResultInfo.composition_info([c1, c2])
        assert len(dates) == 1
        assert dt.date(2020, 1, 2) in errors
        assert errors[dt.date(2020, 1, 2)] is c2

    def test_composition_with_unsupported(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        c1 = UnsupportedValue(rk1)
        c2 = FloatWithInfo(rk2, 5.0, unit={'EUR': 1})

        dates, values, errors, rk, unit = ResultInfo.composition_info([c1, c2])
        assert len(dates) == 2
        # UnsupportedValue sets unit to None
        assert values[0] is c1

    def test_composition_different_markets_raises(self):
        rk1 = _make_risk_key(date=dt.date(2020, 1, 1), location=PricingLocation.NYC)
        rk2 = _make_risk_key(date=dt.date(2020, 1, 2), location=PricingLocation.LDN)
        c1 = FloatWithInfo(rk1, 1.0)
        c2 = FloatWithInfo(rk2, 2.0)

        with pytest.raises(ValueError, match='Cannot compose results with different markets'):
            ResultInfo.composition_info([c1, c2])


# ---------------------------------------------------------------------------
# ErrorValue (lines 103-119)
# ---------------------------------------------------------------------------

class TestErrorValue:
    def test_init_and_repr(self, risk_key):
        ev = ErrorValue(risk_key, 'something broke', request_id='req-1')
        assert repr(ev) == 'something broke'
        assert ev.error == 'something broke'
        assert ev.request_id == 'req-1'

    def test_raw_value_is_none(self, risk_key):
        ev = ErrorValue(risk_key, 'err')
        assert ev.raw_value is None

    def test_getattr_raises(self, risk_key):
        ev = ErrorValue(risk_key, 'err')
        with pytest.raises(AttributeError, match='ErrorValue object has no attribute'):
            _ = ev.nonexistent_attr

    def test_to_records(self, risk_key):
        ev = ErrorValue(risk_key, 'err')
        records = ev._to_records({'instrument': 'swap'})
        assert len(records) == 1
        assert records[0]['instrument'] == 'swap'
        assert records[0]['value'] is ev


# ---------------------------------------------------------------------------
# UnsupportedValue (lines 122-147)
# ---------------------------------------------------------------------------

class TestUnsupportedValue:
    def test_init_and_repr(self, risk_key):
        uv = UnsupportedValue(risk_key, request_id='req-2')
        assert repr(uv) == 'Unsupported Value'
        assert uv.request_id == 'req-2'

    def test_raw_value(self, risk_key):
        uv = UnsupportedValue(risk_key)
        assert uv.raw_value == 'Unsupported Value'

    def test_compose(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        uv1 = UnsupportedValue(rk1)
        uv2 = UnsupportedValue(rk2)

        result = UnsupportedValue.compose([uv1, uv2])
        assert isinstance(result, SeriesWithInfo)
        assert len(result) == 2

    def test_to_records_show_na_true(self, risk_key):
        uv = UnsupportedValue(risk_key)
        opts = DisplayOptions(show_na=True)
        records = uv._to_records({'instrument': 'swap'}, display_options=opts)
        assert len(records) == 1
        assert records[0]['value'] is uv

    def test_to_records_show_na_false(self, risk_key):
        uv = UnsupportedValue(risk_key)
        opts = DisplayOptions(show_na=False)
        records = uv._to_records({'instrument': 'swap'}, display_options=opts)
        assert records == []

    def test_to_records_default_options(self, risk_key):
        uv = UnsupportedValue(risk_key)
        # When display_options is None, it falls back to gs_quant.config.display_options
        with patch('gs_quant.risk.core.gs_quant.config.display_options', DisplayOptions(show_na=False)):
            records = uv._to_records({'instrument': 'swap'})
            assert records == []

    def test_to_records_invalid_display_options_raises(self, risk_key):
        uv = UnsupportedValue(risk_key)
        with pytest.raises(TypeError, match='display_options must be of type DisplayOptions'):
            uv._to_records({'instrument': 'swap'}, display_options='bad')


# ---------------------------------------------------------------------------
# ScalarWithInfo / FloatWithInfo (lines 150-251)
# ---------------------------------------------------------------------------

class TestFloatWithInfo:
    def test_new_and_raw_value(self, risk_key):
        f = FloatWithInfo(risk_key, 42.5)
        assert float(f) == 42.5
        assert f.raw_value == 42.5

    def test_str(self, risk_key):
        f = FloatWithInfo(risk_key, 3.14)
        assert str(f) == float.__repr__(3.14)

    def test_repr_no_error_no_unit(self, risk_key):
        f = FloatWithInfo(risk_key, 1.23)
        assert repr(f) == '1.23'

    def test_repr_with_error(self, risk_key):
        f = FloatWithInfo(risk_key, 0.0, error='calc failed')
        assert repr(f) == 'calc failed'

    def test_repr_with_unit_numerator_only(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'USD': 1})
        assert repr(f) == '10.0 (USD)'

    def test_repr_with_unit_numerator_power(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'USD': 2})
        assert repr(f) == '10.0 (USD^2)'

    def test_repr_with_unit_denominator_only(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'BP': -1})
        assert repr(f) == '10.0 (1/BP)'

    def test_repr_with_unit_denominator_power(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'BP': -2})
        assert repr(f) == '10.0 (1/BP^2)'

    def test_repr_with_unit_numerator_and_denominator(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'USD': 1, 'BP': -1})
        assert repr(f) == '10.0 (USD/BP)'

    def test_repr_with_empty_unit_dict(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={})
        assert repr(f) == '10.0'

    def test_reduce_pickle_roundtrip(self, risk_key):
        f = FloatWithInfo(risk_key, 99.9, unit={'USD': 1}, error='warn', request_id='r1')
        reduced = f.__reduce__()
        assert reduced[0] is FloatWithInfo
        assert reduced[1] == (risk_key, 99.9, {'USD': 1}, 'warn', 'r1')

    def test_compose(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        c1 = FloatWithInfo(rk1, 1.0, unit={'USD': 1})
        c2 = FloatWithInfo(rk2, 2.0, unit={'USD': 1})

        result = FloatWithInfo.compose([c1, c2])
        assert isinstance(result, SeriesWithInfo)
        assert len(result) == 2

    def test_to_records(self, risk_key):
        f = FloatWithInfo(risk_key, 5.0)
        records = f._to_records({'instrument': 'cap'})
        assert len(records) == 1
        assert records[0]['instrument'] == 'cap'
        assert records[0]['value'] is f

    def test_add_same_unit(self, risk_key):
        f1 = FloatWithInfo(risk_key, 10.0, unit={'USD': 1})
        f2 = FloatWithInfo(risk_key, 20.0, unit={'USD': 1})
        result = f1 + f2
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 30.0
        assert result.unit == {'USD': 1}

    def test_add_different_unit_raises(self, risk_key):
        f1 = FloatWithInfo(risk_key, 10.0, unit={'USD': 1})
        f2 = FloatWithInfo(risk_key, 20.0, unit={'EUR': 1})
        with pytest.raises(ValueError, match='FloatWithInfo unit mismatch'):
            f1 + f2

    def test_add_plain_float(self, risk_key):
        f = FloatWithInfo(risk_key, 10.0, unit={'USD': 1})
        result = f + 5.0
        # Falls through to float.__add__
        assert result == 15.0

    def test_mul_float_with_info(self, risk_key):
        f1 = FloatWithInfo(risk_key, 3.0, unit={'USD': 1})
        f2 = FloatWithInfo(risk_key, 4.0, unit={'USD': 1})
        result = f1 * f2
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 12.0

    def test_mul_scalar(self, risk_key):
        f = FloatWithInfo(risk_key, 5.0, unit={'USD': 1})
        result = f * 3
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 15.0
        assert result.unit == {'USD': 1}

    def test_to_frame_returns_self(self, risk_key):
        f = FloatWithInfo(risk_key, 1.0)
        assert f.to_frame() is f


# ---------------------------------------------------------------------------
# StringWithInfo (lines 254-271)
# ---------------------------------------------------------------------------

class TestStringWithInfo:
    def test_new_and_raw_value(self, risk_key):
        s = StringWithInfo(risk_key, 'hello')
        assert str(s) == 'hello'
        assert s.raw_value == 'hello'

    def test_repr_no_error(self, risk_key):
        s = StringWithInfo(risk_key, 'test')
        assert repr(s) == "'test'"

    def test_repr_with_error(self, risk_key):
        s = StringWithInfo(risk_key, 'test', error='bad')
        assert repr(s) == 'bad'


# ---------------------------------------------------------------------------
# DictWithInfo (lines 273-301)
# ---------------------------------------------------------------------------

class TestDictWithInfo:
    def test_new_init_and_raw_value(self, risk_key):
        d = DictWithInfo(risk_key, {'a': 1, 'b': 2})
        assert d.raw_value == {'a': 1, 'b': 2}
        assert d['a'] == 1

    def test_repr_no_error(self, risk_key):
        d = DictWithInfo(risk_key, {'x': 10})
        assert repr(d) == "{'x': 10}"

    def test_repr_with_error(self, risk_key):
        d = DictWithInfo(risk_key, {'x': 10}, error='dict error')
        assert repr(d) == 'dict error'


# ---------------------------------------------------------------------------
# SeriesWithInfo (lines 303-366)
# ---------------------------------------------------------------------------

class TestSeriesWithInfo:
    def test_init_and_raw_value(self, risk_key):
        s = SeriesWithInfo(pd.Series([1, 2, 3]), risk_key=risk_key, unit={'USD': 1})
        assert isinstance(s.raw_value, pd.Series)
        assert list(s.raw_value) == [1, 2, 3]
        assert s.unit == {'USD': 1}

    def test_repr_no_error(self, risk_key):
        s = SeriesWithInfo(pd.Series([1.0]), risk_key=risk_key)
        r = repr(s)
        assert 'Errors' not in r

    def test_repr_with_error(self, risk_key):
        s = SeriesWithInfo(pd.Series([1.0]), risk_key=risk_key, error='oops')
        r = repr(s)
        assert 'Errors: oops' in r

    def test_constructor_returns_series_with_info(self, risk_key):
        s = SeriesWithInfo(pd.Series([1, 2, 3]), risk_key=risk_key)
        assert s._constructor is SeriesWithInfo

    def test_constructor_expanddim_returns_dataframe_with_info(self, risk_key):
        s = SeriesWithInfo(pd.Series([1, 2, 3]), risk_key=risk_key)
        assert s._constructor_expanddim is DataFrameWithInfo

    def test_compose(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        s1 = SeriesWithInfo(pd.Series([1.0, 2.0]), risk_key=rk1, unit={'USD': 1})
        s2 = SeriesWithInfo(pd.Series([3.0, 4.0]), risk_key=rk2, unit={'USD': 1})

        result = SeriesWithInfo.compose([s1, s2])
        assert isinstance(result, SeriesWithInfo)

    def test_to_records(self, risk_key):
        idx = pd.DatetimeIndex([dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        s = SeriesWithInfo(pd.Series([10.0, 20.0], index=idx), risk_key=risk_key)
        records = s._to_records({'instrument': 'swap'})
        assert len(records) == 2
        assert records[0]['instrument'] == 'swap'
        assert 'dates' in records[0]
        assert 'value' in records[0]

    def test_mul(self, risk_key):
        s = SeriesWithInfo(pd.Series([1.0, 2.0, 3.0]), risk_key=risk_key, unit={'USD': 1}, error='w')
        result = s * 2
        assert isinstance(result, SeriesWithInfo)
        assert list(result) == [2.0, 4.0, 6.0]
        assert result.risk_key is risk_key
        assert result.unit == {'USD': 1}
        assert result.error == 'w'

    def test_copy_with_resultinfo(self, risk_key):
        s = SeriesWithInfo(
            pd.Series([1.0, 2.0]),
            risk_key=risk_key,
            unit={'USD': 1},
            error='err',
            request_id='r1',
        )
        c = s.copy_with_resultinfo()
        assert isinstance(c, SeriesWithInfo)
        assert list(c) == [1.0, 2.0]
        assert c.risk_key is risk_key
        assert c.unit == {'USD': 1}
        assert c.error == 'err'
        assert c.request_id == 'r1'


# ---------------------------------------------------------------------------
# DataFrameWithInfo (lines 369-452)
# ---------------------------------------------------------------------------

class TestDataFrameWithInfo:
    def test_init_and_properties(self, risk_key):
        df = DataFrameWithInfo(
            pd.DataFrame({'a': [1, 2]}),
            risk_key=risk_key,
            unit={'USD': 1},
            error='err',
            request_id='r1',
        )
        assert df.risk_key is risk_key
        assert df.unit == {'USD': 1}
        assert df.error == 'err'
        assert df.request_id == 'r1'

    def test_constructor(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        assert df._constructor is DataFrameWithInfo

    def test_constructor_sliced(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        assert df._constructor_sliced is SeriesWithInfo

    def test_raw_value_empty(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        result = df.raw_value
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_raw_value_with_date_index(self, risk_key):
        data = pd.DataFrame({'value': [1, 2]}, index=[dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        df = DataFrameWithInfo(data, risk_key=risk_key)
        result = df.raw_value
        assert 'dates' in result.columns

    def test_raw_value_without_date_index(self, risk_key):
        data = pd.DataFrame({'value': [1, 2]})
        df = DataFrameWithInfo(data, risk_key=risk_key)
        result = df.raw_value
        assert isinstance(result, pd.DataFrame)
        # No 'dates' column if index is not date-based
        assert 'dates' not in result.columns

    def test_repr_no_error(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame({'a': [1]}), risk_key=risk_key)
        r = repr(df)
        assert 'Errors' not in r

    def test_compose(self):
        shared_market = _make_mock_market()
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        rk2 = RiskKey('GS', dt.date(2020, 1, 2), shared_market, MagicMock(), MagicMock(),
                       RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price))
        df1 = DataFrameWithInfo(pd.DataFrame({'mkt_type': ['IR'], 'value': [1.0]}), risk_key=rk1)
        df2 = DataFrameWithInfo(pd.DataFrame({'mkt_type': ['IR'], 'value': [2.0]}), risk_key=rk2)

        result = DataFrameWithInfo.compose([df1, df2])
        assert isinstance(result, DataFrameWithInfo)

    def test_to_frame_returns_self(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame({'a': [1]}), risk_key=risk_key)
        assert df.to_frame() is df

    def test_to_records_non_empty(self, risk_key):
        data = pd.DataFrame({'value': [10.0, 20.0]})
        df = DataFrameWithInfo(data, risk_key=risk_key)
        records = df._to_records({'instrument': 'swap'})
        assert len(records) == 2
        for r in records:
            assert 'instrument' in r

    def test_to_records_empty_show_na_true(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        opts = DisplayOptions(show_na=True)
        records = df._to_records({'instrument': 'swap'}, display_options=opts)
        assert len(records) == 1
        assert records[0]['value'] is None

    def test_to_records_empty_show_na_false(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        opts = DisplayOptions(show_na=False)
        records = df._to_records({'instrument': 'swap'}, display_options=opts)
        assert records == []

    def test_to_records_empty_default_options(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        with patch('gs_quant.risk.core.gs_quant.config.display_options', DisplayOptions(show_na=False)):
            records = df._to_records({'instrument': 'swap'})
            assert records == []

    def test_to_records_empty_invalid_display_options(self, risk_key):
        df = DataFrameWithInfo(pd.DataFrame(), risk_key=risk_key)
        with pytest.raises(TypeError, match='display_options must be of type DisplayOptions'):
            df._to_records({'instrument': 'swap'}, display_options='bad')

    def test_copy_with_resultinfo(self, risk_key):
        data = pd.DataFrame({'value': [1.0, 2.0]})
        df = DataFrameWithInfo(data, risk_key=risk_key, unit={'USD': 1}, error='err', request_id='r1')
        c = df.copy_with_resultinfo()
        assert isinstance(c, DataFrameWithInfo)
        assert c.risk_key is risk_key
        assert c.unit == {'USD': 1}

    def test_filter_by_coord_string(self, risk_key):
        data = pd.DataFrame({
            'mkt_type': ['IR', 'FX', 'IR'],
            'mkt_asset': ['USD', 'EUR', 'GBP'],
            'mkt_class': [None, None, None],
            'mkt_point': [None, None, None],
            'mkt_quoting_style': [None, None, None],
            'value': [1.0, 2.0, 3.0],
        })
        df = DataFrameWithInfo(data, risk_key=risk_key)

        from gs_quant.markets import MarketDataCoordinate
        coord = MarketDataCoordinate(mkt_type='IR')
        filtered = df.filter_by_coord(coord)
        assert len(filtered) == 2

    def test_filter_by_coord_iterable(self, risk_key):
        data = pd.DataFrame({
            'mkt_type': ['IR', 'FX', 'IR'],
            'mkt_asset': ['USD', 'EUR', 'GBP'],
            'mkt_class': [None, None, None],
            'mkt_point': ['1Y', '2Y', '1Y'],
            'mkt_quoting_style': [None, None, None],
            'value': [1.0, 2.0, 3.0],
        })
        df = DataFrameWithInfo(data, risk_key=risk_key)

        from gs_quant.markets import MarketDataCoordinate
        # mkt_point is Tuple[str, ...] so isinstance check is not str -> uses .isin()
        coord = MarketDataCoordinate(mkt_point=('1Y',))
        filtered = df.filter_by_coord(coord)
        assert len(filtered) == 2


# ---------------------------------------------------------------------------
# MQVSValidatorDefnsWithInfo (line 498)
# ---------------------------------------------------------------------------

class TestMQVSValidatorDefnsWithInfo:
    def test_init_with_tuple(self, risk_key):
        defn = MQVSValidatorDefn(
            validatorType='test',
            targets=(MQVSValidationTarget(),),
            args={'key': 'val'},
        )
        vi = MQVSValidatorDefnsWithInfo(risk_key, (defn,))
        assert vi.raw_value == (defn,)
        assert vi.validators == (defn,)

    def test_init_with_single(self, risk_key):
        defn = MQVSValidatorDefn(
            validatorType='test',
            targets=(MQVSValidationTarget(),),
            args={'key': 'val'},
        )
        vi = MQVSValidatorDefnsWithInfo(risk_key, defn)
        assert vi.validators == (defn,)

    def test_raw_value(self, risk_key):
        defn = MQVSValidatorDefn(
            validatorType='test',
            targets=(MQVSValidationTarget(),),
            args={},
        )
        vi = MQVSValidatorDefnsWithInfo(risk_key, (defn,))
        assert vi.raw_value is vi.validators


# ---------------------------------------------------------------------------
# aggregate_risk (lines 535-549)
# ---------------------------------------------------------------------------

class TestAggregateRisk:
    def test_aggregate_dataframes(self, risk_key):
        df1 = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [10.0]}),
            risk_key=risk_key,
        )
        df2 = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [20.0]}),
            risk_key=risk_key,
        )
        result = aggregate_risk([df1, df2])
        assert isinstance(result, pd.DataFrame)
        assert result['value'].sum() == 30.0

    def test_aggregate_with_threshold(self, risk_key):
        df1 = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR', 'FX'], 'value': [10.0, 0.001]}),
            risk_key=risk_key,
        )
        result = aggregate_risk([df1], threshold=0.01)
        assert len(result) == 1

    def test_aggregate_with_future(self, risk_key):
        df = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [5.0]}),
            risk_key=risk_key,
        )
        future = Future()
        future.set_result(df)
        result = aggregate_risk([future])
        assert result['value'].sum() == 5.0

    def test_aggregate_series_heterogeneous(self, risk_key):
        s = SeriesWithInfo(
            pd.Series([10.0], index=['mkt_type']),
            risk_key=risk_key,
        )
        result = aggregate_risk([s], allow_heterogeneous_types=True)
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# aggregate_results (lines 558-600)
# ---------------------------------------------------------------------------

class TestAggregateResults:
    def test_empty_returns_none(self):
        assert aggregate_results([]) is None

    def test_exception_raises(self, risk_key):
        with pytest.raises(Exception):
            aggregate_results([Exception('bad')])

    def test_error_raises(self, risk_key):
        f = FloatWithInfo(risk_key, 1.0, error='err')
        with pytest.raises(ValueError, match='Cannot aggregate results in error'):
            aggregate_results([f])

    def test_heterogeneous_types_raises(self, risk_key):
        f = FloatWithInfo(risk_key, 1.0)
        s = StringWithInfo(risk_key, 'hello')
        with pytest.raises(ValueError, match='Cannot aggregate heterogeneous types'):
            aggregate_results([f, s])

    def test_different_units_raises(self, risk_key):
        f1 = FloatWithInfo(risk_key, 1.0, unit={'USD': 1})
        f2 = FloatWithInfo(risk_key, 2.0, unit={'EUR': 1})
        with pytest.raises(ValueError, match='Cannot aggregate results with different units'):
            aggregate_results([f1, f2])

    def test_different_risk_keys_raises(self):
        # Need real-ish params to make ex_historical_diddle work
        from gs_quant.target.common import RiskRequestParameters
        params1 = RiskRequestParameters(csa_term='term1', raw_results=False)
        params2 = RiskRequestParameters(csa_term='term2', raw_results=False)
        rk1 = RiskKey(
            provider='GS',
            date=dt.date(2020, 1, 1),
            market=_make_mock_market(),
            params=params1,
            scenario=MagicMock(),
            risk_measure=RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
        )
        rk2 = RiskKey(
            provider='GS',
            date=dt.date(2020, 1, 1),
            market=_make_mock_market(),
            params=params2,
            scenario=MagicMock(),
            risk_measure=RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
        )
        f1 = FloatWithInfo(rk1, 1.0)
        f2 = FloatWithInfo(rk2, 2.0)
        with pytest.raises(ValueError, match='Cannot aggregate results with different pricing keys'):
            aggregate_results([f1, f2])

    def test_aggregate_floats(self, risk_key):
        f1 = FloatWithInfo(risk_key, 10.0, unit={'USD': 1})
        f2 = FloatWithInfo(risk_key, 20.0, unit={'USD': 1})
        result = aggregate_results([f1, f2])
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 30.0

    def test_aggregate_series(self, risk_key):
        s1 = SeriesWithInfo(pd.Series([1.0, 2.0]), risk_key=risk_key)
        s2 = SeriesWithInfo(pd.Series([3.0, 4.0]), risk_key=risk_key)
        result = aggregate_results([s1, s2])
        assert isinstance(result, SeriesWithInfo)

    def test_aggregate_dataframes(self, risk_key):
        df1 = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [10.0]}),
            risk_key=risk_key,
        )
        df2 = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [20.0]}),
            risk_key=risk_key,
        )
        result = aggregate_results([df1, df2])
        assert isinstance(result, DataFrameWithInfo)

    def test_aggregate_allow_mismatch_risk_keys(self):
        from gs_quant.target.common import RiskRequestParameters
        params1 = RiskRequestParameters(csa_term='term1', raw_results=False)
        params2 = RiskRequestParameters(csa_term='term2', raw_results=False)
        rk1 = RiskKey(
            provider='GS',
            date=dt.date(2020, 1, 1),
            market=_make_mock_market(),
            params=params1,
            scenario=MagicMock(),
            risk_measure=RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
        )
        rk2 = RiskKey(
            provider='GS',
            date=dt.date(2020, 1, 1),
            market=_make_mock_market(),
            params=params2,
            scenario=MagicMock(),
            risk_measure=RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
        )
        f1 = FloatWithInfo(rk1, 10.0)
        f2 = FloatWithInfo(rk2, 20.0)
        result = aggregate_results([f1, f2], allow_mismatch_risk_keys=True)
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 30.0


# ---------------------------------------------------------------------------
# subtract_risk (lines 627-633)
# ---------------------------------------------------------------------------

class TestSubtractRisk:
    def test_subtract(self, risk_key):
        left = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [30.0]}),
            risk_key=risk_key,
        )
        right = DataFrameWithInfo(
            pd.DataFrame({'mkt_type': ['IR'], 'value': [10.0]}),
            risk_key=risk_key,
        )
        # Set columns to MultiIndex with 'value' as a name level so the assertion passes
        left.columns = pd.Index(left.columns, name='value')
        right.columns = pd.Index(right.columns, name='value')
        result = subtract_risk(left, right)
        assert isinstance(result, pd.DataFrame)
        assert result['value'].iloc[0] == 20.0


# ---------------------------------------------------------------------------
# combine_risk_key (lines 676-679)
# ---------------------------------------------------------------------------

class TestCombineRiskKey:
    def test_same_fields(self, risk_key):
        result = combine_risk_key(risk_key, risk_key)
        assert result.provider == 'GS'
        assert result.date == dt.date(2020, 1, 1)

    def test_different_fields(self):
        rk1 = _make_risk_key(provider='GS')
        rk2 = _make_risk_key(provider='OTHER')
        result = combine_risk_key(rk1, rk2)
        assert result.provider is None

    def test_mixed_fields(self):
        rk1 = _make_risk_key(date=dt.date(2020, 1, 1))
        rk2 = _make_risk_key(date=dt.date(2020, 1, 2))
        result = combine_risk_key(rk1, rk2)
        assert result.date is None
        # Provider is the same for both
        assert result.provider == 'GS'
