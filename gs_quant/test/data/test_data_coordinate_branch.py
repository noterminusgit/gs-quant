"""
Branch coverage tests for gs_quant/data/coordinate.py
"""
import datetime as dt
import json
from enum import Enum
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.data.coordinate import BaseDataCoordinate, DataCoordinate
from gs_quant.data.core import DataContext, DataFrequency
from gs_quant.data.fields import DataMeasure, DataDimension


# ---------- BaseDataCoordinate ----------

class ConcreteDataCoordinate(BaseDataCoordinate):
    """Concrete subclass for testing the abstract BaseDataCoordinate"""
    pass


class TestBaseDataCoordinate:
    def test_init_no_dimensions(self):
        coord = ConcreteDataCoordinate(measure='test_measure')
        assert coord.measure == 'test_measure'
        assert coord.dimensions == {}

    def test_init_with_dimensions_string_keys(self):
        dims = {'key1': 'val1', 'key2': 'val2'}
        coord = ConcreteDataCoordinate(measure='m', dimensions=dims)
        assert coord.dimensions == {'key1': 'val1', 'key2': 'val2'}

    def test_init_with_dimensions_enum_keys(self):
        dims = {DataDimension.TENOR: '1m', DataDimension.STRIKE_REFERENCE: 'Delta'}
        coord = ConcreteDataCoordinate(measure='m', dimensions=dims)
        # Enum keys should be converted to their values
        assert coord.dimensions == {'tenor': '1m', 'strikeReference': 'Delta'}

    def test_init_with_mixed_keys(self):
        dims = {DataDimension.TENOR: '1m', 'customKey': 'val'}
        coord = ConcreteDataCoordinate(measure='m', dimensions=dims)
        assert coord.dimensions == {'tenor': '1m', 'customKey': 'val'}

    def test_set_dimensions_with_string_keys(self):
        coord = ConcreteDataCoordinate(measure='m', dimensions={'key1': 'val1'})
        coord.set_dimensions({'key2': 'val2'})
        assert coord.dimensions == {'key1': 'val1', 'key2': 'val2'}

    def test_set_dimensions_with_enum_keys(self):
        coord = ConcreteDataCoordinate(measure='m', dimensions={'key1': 'val1'})
        coord.set_dimensions({DataDimension.TENOR: '3m'})
        assert coord.dimensions == {'key1': 'val1', 'tenor': '3m'}

    def test_get_series_returns_none(self):
        coord = ConcreteDataCoordinate(measure='m')
        assert coord.get_series() is None


# ---------- DataCoordinate ----------

class TestDataCoordinate:
    def test_init_basic(self):
        coord = DataCoordinate(measure='test', dataset_id='ds1')
        assert coord.dataset_id == 'ds1'
        assert coord.measure == 'test'
        assert coord.frequency is None
        assert coord.id is not None

    def test_init_with_frequency(self):
        coord = DataCoordinate(measure='test', frequency=DataFrequency.DAILY)
        assert coord.frequency == DataFrequency.DAILY

    def test_id_setter(self):
        coord = DataCoordinate(measure='test')
        coord.id = 'custom_id'
        assert coord.id == 'custom_id'

    def test_eq_both_str_measures(self):
        c1 = DataCoordinate(measure='impliedVolatility', dataset_id='ds1')
        c2 = DataCoordinate(measure='impliedVolatility', dataset_id='ds1')
        assert c1 == c2

    def test_eq_both_enum_measures(self):
        c1 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1')
        c2 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1')
        assert c1 == c2

    def test_eq_mixed_measure_types(self):
        c1 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1')
        c2 = DataCoordinate(measure='impliedVolatility', dataset_id='ds1')
        assert c1 == c2

    def test_eq_self_str_other_enum(self):
        c1 = DataCoordinate(measure='impliedVolatility', dataset_id='ds1')
        c2 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1')
        assert c1 == c2

    def test_neq_different_datasets(self):
        c1 = DataCoordinate(measure='m', dataset_id='ds1')
        c2 = DataCoordinate(measure='m', dataset_id='ds2')
        assert c1 != c2

    def test_hash(self):
        c1 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1',
                            dimensions={'tenor': '1m'})
        c2 = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1',
                            dimensions={'tenor': '1m'})
        assert hash(c1) == hash(c2)

    def test_get_dimensions(self):
        dims = {'key1': 'v1', 'key2': 'v2'}
        coord = DataCoordinate(measure='m', dimensions=dims)
        result = coord.get_dimensions()
        assert isinstance(result, tuple)
        assert set(result) == {('key1', 'v1'), ('key2', 'v2')}

    def test_str_with_str_measure(self):
        coord = DataCoordinate(measure='impliedVol', dataset_id='ds1', dimensions={'tenor': '1m'})
        s = str(coord)
        assert 'ds1' in s
        assert 'impliedVol' in s
        assert 'tenor' in s

    def test_str_with_enum_measure(self):
        coord = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1')
        s = str(coord)
        assert 'impliedVolatility' in s

    # ---------- get_range ----------

    def test_get_range_with_explicit_start_end(self):
        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.DAILY)
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 12, 31)
        result = coord.get_range(start, end)
        assert result == (start, end)

    def test_get_range_no_start_daily(self):
        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            start, end = coord.get_range(None, dt.date(2021, 6, 1))
            assert start == dt.date(2021, 1, 1)
            assert end == dt.date(2021, 6, 1)

    def test_get_range_no_end_daily(self):
        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            start, end = coord.get_range(dt.date(2021, 6, 1), None)
            assert start == dt.date(2021, 6, 1)
            assert end == dt.date(2021, 12, 31)

    def test_get_range_no_start_realtime(self):
        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.REAL_TIME)
        start_dt = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2021, 12, 31, tzinfo=dt.timezone.utc)
        with DataContext(start_dt, end_dt):
            start, end = coord.get_range(None, dt.datetime(2021, 6, 1))
            assert start == start_dt
            assert end == dt.datetime(2021, 6, 1)

    def test_get_range_no_end_realtime(self):
        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.REAL_TIME)
        start_dt = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2021, 12, 31, tzinfo=dt.timezone.utc)
        with DataContext(start_dt, end_dt):
            start, end = coord.get_range(dt.datetime(2021, 6, 1), None)
            assert start == dt.datetime(2021, 6, 1)
            assert end == end_dt

    # ---------- get_series ----------

    def test_get_series_no_dataset_id(self):
        coord = DataCoordinate(measure='m')
        assert coord.get_series() is None

    @patch('gs_quant.data.coordinate.Dataset')
    def test_get_series_with_dataset(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_ds.get_data_series.return_value = pd.Series([1, 2, 3])
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure='impliedVol', dataset_id='ds1',
                               dimensions={'tenor': '1m'}, frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            result = coord.get_series()
        assert result is not None
        mock_ds.get_data_series.assert_called_once()

    @patch('gs_quant.data.coordinate.Dataset')
    def test_get_series_with_operator(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_ds.get_data_series.return_value = pd.Series([1])
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure='impliedVol', dataset_id='ds1',
                               frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            from gs_quant.data.core import DataAggregationOperator
            result = coord.get_series(operator=DataAggregationOperator.LAST)

        call_args = mock_ds.get_data_series.call_args
        assert 'last(impliedVol)' == call_args[0][0]

    @patch('gs_quant.data.coordinate.Dataset')
    def test_get_series_with_dates(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_ds.get_data_series.return_value = pd.Series([1])
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.DAILY)
        dates_list = [dt.date(2021, 1, 1)]
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            coord.get_series(dates=dates_list)
        call_args = mock_ds.get_data_series.call_args
        assert call_args[1]['dates'] == dates_list

    # ---------- last_value ----------

    def test_last_value_no_dataset_id(self):
        coord = DataCoordinate(measure='m')
        assert coord.last_value() is None

    @patch('gs_quant.data.coordinate.Dataset')
    def test_last_value_with_str_measure(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_result = MagicMock()
        mock_result.get.return_value = 42.0
        mock_ds.get_data_last.return_value = mock_result
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure='impliedVol', dataset_id='ds1',
                               frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            result = coord.last_value()
        assert result == 42.0
        mock_ds.get_data_last.assert_called_once()

    @patch('gs_quant.data.coordinate.Dataset')
    def test_last_value_with_enum_measure(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_result = MagicMock()
        mock_result.get.return_value = 55.0
        mock_ds.get_data_last.return_value = mock_result
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY, dataset_id='ds1',
                               frequency=DataFrequency.DAILY)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            result = coord.last_value()
        assert result == 55.0
        # Verify measure.value was used
        call_args = mock_ds.get_data_last.call_args
        assert call_args[1]['fields'] == ['impliedVolatility']

    @patch('gs_quant.data.coordinate.Dataset')
    def test_last_value_with_before(self, mock_dataset_cls):
        mock_ds = MagicMock()
        mock_result = MagicMock()
        mock_result.get.return_value = 10.0
        mock_ds.get_data_last.return_value = mock_result
        mock_dataset_cls.return_value = mock_ds

        coord = DataCoordinate(measure='m', dataset_id='ds1', frequency=DataFrequency.DAILY)
        before = dt.date(2021, 6, 15)
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)):
            coord.last_value(before=before)
        call_args = mock_ds.get_data_last.call_args
        assert call_args[0][0] == before

    # ---------- as_dict ----------

    def test_as_dict_str_measure_with_dataset(self):
        coord = DataCoordinate(measure='impliedVol', dataset_id='ds1',
                               dimensions={'tenor': '1m'},
                               frequency=DataFrequency.DAILY)
        coord.id = 'test-id'
        d = coord.as_dict()
        assert d['measure'] == 'impliedVol'
        assert d['frequency'] == 'daily'
        assert d['id'] == 'test-id'
        assert d['datasetId'] == 'ds1'
        assert d['dimensions'] == {'tenor': '1m'}

    def test_as_dict_enum_measure(self):
        coord = DataCoordinate(measure=DataMeasure.IMPLIED_VOLATILITY,
                               frequency=DataFrequency.DAILY)
        coord.id = 'test-id'
        d = coord.as_dict()
        assert d['measure'] == 'impliedVolatility'

    def test_as_dict_no_dataset_id(self):
        coord = DataCoordinate(measure='m', frequency=DataFrequency.DAILY)
        coord.id = 'test-id'
        d = coord.as_dict()
        assert 'datasetId' not in d

    def test_as_dict_no_dimensions(self):
        coord = DataCoordinate(measure='m', frequency=DataFrequency.DAILY)
        coord.id = 'test-id'
        d = coord.as_dict()
        assert 'dimensions' not in d

    def test_as_dict_enum_dimension_keys(self):
        coord = DataCoordinate(measure='m', frequency=DataFrequency.DAILY,
                               dimensions={DataDimension.TENOR: '1m'})
        coord.id = 'test-id'
        d = coord.as_dict()
        # Enum keys should have been converted at init time, so as_dict gets string keys
        assert d['dimensions'] == {'tenor': '1m'}

    # ---------- from_dict ----------

    def test_from_dict_with_known_measure(self):
        obj = {
            'measure': 'impliedVolatility',
            'dimensions': {'tenor': '1m'},
            'frequency': 'daily',
            'datasetId': 'ds1',
            'id': 'test-id',
        }
        coord = DataCoordinate.from_dict(obj)
        assert coord.measure == DataMeasure.IMPLIED_VOLATILITY
        assert coord.dataset_id == 'ds1'
        assert coord.frequency == DataFrequency.DAILY
        assert coord.id == 'test-id'

    def test_from_dict_with_unknown_measure(self):
        obj = {
            'measure': 'customMeasure',
            'dimensions': {},
            'frequency': 'daily',
            'datasetId': 'ds1',
        }
        coord = DataCoordinate.from_dict(obj)
        assert coord.measure == 'customMeasure'

    def test_from_dict_no_dataset_id(self):
        obj = {
            'measure': 'impliedVolatility',
            'frequency': 'daily',
        }
        coord = DataCoordinate.from_dict(obj)
        assert coord.dataset_id is None

    def test_from_dict_no_id(self):
        obj = {
            'measure': 'impliedVolatility',
            'frequency': 'daily',
            'datasetId': 'ds1',
        }
        coord = DataCoordinate.from_dict(obj)
        # id should not have been set explicitly (will be auto-generated uuid)
        assert coord.id is not None

    def test_from_dict_with_known_dimension_keys(self):
        obj = {
            'measure': 'customMeasure',
            'dimensions': {'tenor': '1m', 'unknownDim': 'val'},
            'frequency': 'daily',
        }
        coord = DataCoordinate.from_dict(obj)
        dims = coord.dimensions
        assert dims.get('tenor') == '1m' or any(
            k.value == 'tenor' if isinstance(k, Enum) else k == 'tenor'
            for k in dims
        )

    def test_from_dict_with_unknown_dimension_keys(self):
        obj = {
            'measure': 'customMeasure',
            'dimensions': {'unknownDim': 'val'},
            'frequency': 'daily',
        }
        coord = DataCoordinate.from_dict(obj)
        # Unknown dimension keys should be stored as strings
        assert 'unknownDim' in coord.dimensions


class TestAsDict:
    """Cover branch [212,213]: as_dict with Enum dimension key."""

    def test_as_dict_enum_key(self):
        """When dimension key is an Enum -> use key.value [212,213]."""
        from enum import Enum as StdEnum
        from gs_quant.data.core import DataFrequency

        class DimKey(StdEnum):
            TENOR = 'tenor'

        coord = DataCoordinate(
            measure='price',
            dataset_id='DS1',
            dimensions={DimKey.TENOR: '1m'},
            frequency=DataFrequency.DAILY,
        )
        result = coord.as_dict()
        assert 'tenor' in result.get('dimensions', {})
        assert result['dimensions']['tenor'] == '1m'
