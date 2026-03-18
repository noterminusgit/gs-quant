"""
Copyright 2022 Goldman Sachs.
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
from unittest.mock import MagicMock

import pandas as pd
import pytest

from gs_quant.base import RiskKey
from gs_quant.common import RiskMeasure, RiskMeasureType
from gs_quant.risk.core import FloatWithInfo, SeriesWithInfo, DataFrameWithInfo
from gs_quant.risk.transform import GenericResultWithInfoTransformer, ResultWithInfoAggregator


def _make_risk_key():
    mock_market = MagicMock()
    mock_market.location = 'NYC'
    return RiskKey(
        'GS',
        dt.date(2020, 1, 1),
        mock_market,
        MagicMock(),
        MagicMock(),
        RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price),
    )


class TestGenericResultWithInfoTransformer:
    def test_apply_identity(self):
        fn = lambda data, *args, **kwargs: data
        transformer = GenericResultWithInfoTransformer(fn)

        risk_key = _make_risk_key()
        fi = FloatWithInfo(risk_key, 42.0)
        result = transformer.apply(fi)
        assert float(result) == 42.0

    def test_apply_with_transform(self):
        fn = lambda data, *args, **kwargs: FloatWithInfo(data.risk_key, float(data) * 2)
        transformer = GenericResultWithInfoTransformer(fn)

        risk_key = _make_risk_key()
        fi = FloatWithInfo(risk_key, 10.0)
        result = transformer.apply(fi)
        assert float(result) == 20.0

    def test_apply_with_args(self):
        fn = lambda data, multiplier, **kwargs: FloatWithInfo(data.risk_key, float(data) * multiplier)
        transformer = GenericResultWithInfoTransformer(fn)

        risk_key = _make_risk_key()
        fi = FloatWithInfo(risk_key, 5.0)
        result = transformer.apply(fi, 3)
        assert float(result) == 15.0


class TestResultWithInfoAggregator:
    def test_apply_plain_floats(self):
        agg = ResultWithInfoAggregator()
        results = [1.0, 2.5, 3.5]
        output = agg.apply(results)
        # Plain floats are passed through as-is
        assert output == [1.0, 2.5, 3.5]
        assert all(isinstance(v, float) for v in output)

    def test_apply_float_with_info(self):
        risk_key = _make_risk_key()
        fi1 = FloatWithInfo(risk_key, 100.0)
        fi2 = FloatWithInfo(risk_key, 200.0)

        agg = ResultWithInfoAggregator()
        output = agg.apply([fi1, fi2])

        assert len(output) == 2
        assert all(isinstance(v, FloatWithInfo) for v in output)
        assert float(output[0]) == 100.0
        assert float(output[1]) == 200.0

    def test_apply_series_with_info(self):
        risk_key = _make_risk_key()
        si = SeriesWithInfo(pd.Series({'value': 3.0, 'other': 7.0}), risk_key=risk_key)

        agg = ResultWithInfoAggregator(risk_col='value')
        output = agg.apply([si])

        assert len(output) == 1
        assert isinstance(output[0], FloatWithInfo)
        # getattr(series, 'value') returns the scalar 3.0, .sum() on scalar returns scalar
        assert float(output[0]) == 3.0

    def test_apply_dataframe_with_info_non_empty(self):
        risk_key = _make_risk_key()
        di = DataFrameWithInfo(
            pd.DataFrame({'value': [1.0, 2.0, 3.0]}),
            risk_key=risk_key,
        )

        agg = ResultWithInfoAggregator(risk_col='value')
        output = agg.apply([di])

        assert len(output) == 1
        assert isinstance(output[0], FloatWithInfo)
        assert float(output[0]) == 6.0

    def test_apply_dataframe_with_info_empty(self):
        risk_key = _make_risk_key()
        di = DataFrameWithInfo(
            pd.DataFrame({'value': []}),
            risk_key=risk_key,
        )

        agg = ResultWithInfoAggregator()
        output = agg.apply([di])

        assert len(output) == 1
        assert isinstance(output[0], FloatWithInfo)
        assert float(output[0]) == 0

    def test_apply_dataframe_with_filter_coord(self):
        risk_key = _make_risk_key()
        di = DataFrameWithInfo(
            pd.DataFrame({'value': [10.0, 20.0], 'mkt_type': ['IR', 'FX']}),
            risk_key=risk_key,
        )

        # Mock filter_coord to return a filtered DataFrame
        mock_coord = MagicMock()
        agg = ResultWithInfoAggregator(risk_col='value', filter_coord=mock_coord)

        # Patch filter_by_coord to return a subset
        filtered_df = pd.DataFrame({'value': [10.0]})
        di.filter_by_coord = MagicMock(return_value=filtered_df)

        output = agg.apply([di])

        assert len(output) == 1
        di.filter_by_coord.assert_called_once_with(mock_coord)
        assert float(output[0]) == 10.0

    def test_apply_unsupported_type_raises(self):
        agg = ResultWithInfoAggregator()
        with pytest.raises(ValueError, match='not currently supported'):
            agg.apply(['unsupported_string'])

    def test_apply_mixed_types(self):
        risk_key = _make_risk_key()
        fi = FloatWithInfo(risk_key, 50.0)
        plain = 25.0

        agg = ResultWithInfoAggregator()
        output = agg.apply([plain, fi])

        assert len(output) == 2
        assert isinstance(output[0], float)
        assert not isinstance(output[0], FloatWithInfo)
        assert output[0] == 25.0
        assert isinstance(output[1], FloatWithInfo)
        assert float(output[1]) == 50.0

    def test_default_risk_col(self):
        agg = ResultWithInfoAggregator()
        assert agg.risk_col == 'value'
        assert agg.filter_coord is None
