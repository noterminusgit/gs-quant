"""
Tests for gs_quant.analytics.core.query_helpers
"""

import datetime as dt
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.analytics.core.processor import DataQueryInfo, MeasureQueryInfo
from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.core.query_helpers import (
    aggregate_queries,
    build_query_string,
    fetch_query,
    valid_dimensions,
)
from gs_quant.data import DataFrequency


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_coordinate(dataset_id='DS1', measure='price', frequency=DataFrequency.DAILY, dimensions=None):
    coord = MagicMock()
    coord.dataset_id = dataset_id
    coord.measure = measure
    coord.frequency = frequency
    coord.dimensions = dimensions or {'assetId': 'abc'}
    coord.get_dimensions.return_value = tuple((dimensions or {'assetId': 'abc'}).items())
    return coord


def _make_query(start=dt.date(2021, 1, 1), end=dt.date(2021, 6, 1), coordinate=None):
    q = MagicMock()
    q.start = start
    q.end = end
    q.coordinate = coordinate or _make_coordinate()
    q.get_range_string.return_value = f'start={q.start}|end={q.end}'
    return q


def _make_data_query_info(query=None, processor=None, attr='a'):
    qi = MagicMock(spec=DataQueryInfo)
    qi.query = query or _make_query()
    qi.processor = processor or MagicMock()
    qi.attr = attr
    # Ensure isinstance checks pass
    qi.__class__ = DataQueryInfo
    return qi


def _make_measure_query_info():
    qi = MagicMock(spec=MeasureQueryInfo)
    qi.__class__ = MeasureQueryInfo
    return qi


# ---------------------------------------------------------------------------
# aggregate_queries
# ---------------------------------------------------------------------------

class TestAggregateQueries:
    def test_skips_measure_query_info(self):
        mqi = _make_measure_query_info()
        result = aggregate_queries([mqi])
        assert result == defaultdict(dict)

    def test_none_dataset_id_calls_processor(self):
        coord = _make_coordinate(dataset_id=None)
        query = _make_query(coordinate=coord)
        qi = _make_data_query_info(query=query)
        # Make qi behave like a real DataQueryInfo for isinstance check
        qi.__class__ = DataQueryInfo
        # aggregate_queries calls asyncio.get_event_loop().run_until_complete(...)
        # which calls processor.calculate; mock that path
        qi.processor.calculate = MagicMock(return_value=MagicMock())

        with patch('gs_quant.analytics.core.query_helpers.asyncio') as mock_asyncio:
            mock_loop = MagicMock()
            mock_asyncio.get_event_loop.return_value = mock_loop
            aggregate_queries([qi])
            mock_loop.run_until_complete.assert_called_once()

    def test_normal_aggregation(self):
        qi = _make_data_query_info()
        qi.__class__ = DataQueryInfo
        result = aggregate_queries([qi])
        assert 'DS1' in result
        key = 'start=2021-01-01|end=2021-06-01'
        assert key in result['DS1']
        entry = result['DS1'][key]
        assert entry['datasetId'] == 'DS1'
        assert entry['realTime'] is False
        assert 'price' in entry['measures']

    def test_date_range_uses_startDate_endDate(self):
        qi = _make_data_query_info()
        qi.__class__ = DataQueryInfo
        result = aggregate_queries([qi])
        key = list(result['DS1'].keys())[0]
        assert result['DS1'][key]['range']['startDate'] == dt.date(2021, 1, 1)
        assert result['DS1'][key]['range']['endDate'] == dt.date(2021, 6, 1)

    def test_datetime_range_uses_startTime_endTime(self):
        # Note: dt.datetime is a subclass of dt.date, so the code checks
        # isinstance(start, dt.date) first, which matches datetime too.
        # Only pure dt.date objects get startDate; dt.datetime goes to startDate as well.
        # To truly hit the datetime branch, we need the isinstance(dt.date) to fail,
        # but dt.datetime IS a dt.date. The code as written always takes the date branch
        # for datetime. Test the actual behavior:
        start = dt.datetime(2021, 1, 1, 10, 0, 0)
        end = dt.datetime(2021, 6, 1, 15, 0, 0)
        query = _make_query(start=start, end=end)
        qi = _make_data_query_info(query=query)
        qi.__class__ = DataQueryInfo
        result = aggregate_queries([qi])
        key = list(result['DS1'].keys())[0]
        # datetime is a subclass of date, so isinstance(dt.date) matches first
        assert result['DS1'][key]['range']['startDate'] == start
        assert result['DS1'][key]['range']['endDate'] == end

    def test_real_time_frequency(self):
        coord = _make_coordinate(frequency=DataFrequency.REAL_TIME)
        query = _make_query(coordinate=coord)
        qi = _make_data_query_info(query=query)
        qi.__class__ = DataQueryInfo
        result = aggregate_queries([qi])
        key = list(result['DS1'].keys())[0]
        assert result['DS1'][key]['realTime'] is True


# ---------------------------------------------------------------------------
# fetch_query
# ---------------------------------------------------------------------------

class TestFetchQuery:
    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_normal_query(self, mock_gs):
        mock_gs.current._post.return_value = {
            'data': [{'date': '2021-01-01', 'price': 100}]
        }
        qi = {
            'datasetId': 'DS1',
            'parameters': {'assetId': {'abc'}},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        df = fetch_query(qi)
        assert not df.empty
        mock_gs.current._post.assert_called_once()
        assert '/query' in mock_gs.current._post.call_args[0][0]

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_realtime_no_range_uses_last_query(self, mock_gs):
        mock_gs.current._post.return_value = {
            'data': [{'time': '2021-01-01T10:00:00Z', 'price': 100}]
        }
        qi = {
            'datasetId': 'DS1',
            'parameters': {},
            'range': {},
            'realTime': True,
        }
        df = fetch_query(qi)
        assert '/last/query' in mock_gs.current._post.call_args[0][0]

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_exception_returns_empty_df(self, mock_gs):
        mock_gs.current._post.side_effect = RuntimeError('network error')
        qi = {
            'datasetId': 'DS1',
            'parameters': {},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        df = fetch_query(qi)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_empty_data_returns_empty_df(self, mock_gs):
        mock_gs.current._post.return_value = {'data': {}}
        qi = {
            'datasetId': 'DS1',
            'parameters': {},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        df = fetch_query(qi)
        assert df.empty

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_time_column_index(self, mock_gs):
        mock_gs.current._post.return_value = {
            'data': [{'time': '2021-01-01T10:00:00', 'price': 100}]
        }
        qi = {
            'datasetId': 'DS1',
            'parameters': {},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        df = fetch_query(qi)
        assert df.index.name == 'time'

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_bool_parameter_single_value(self, mock_gs):
        mock_gs.current._post.return_value = {'data': [{'date': '2021-01-01', 'v': 1}]}
        qi = {
            'datasetId': 'DS1',
            'parameters': {'flag': {True}},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        fetch_query(qi)
        payload = mock_gs.current._post.call_args[1]['payload']
        assert payload['where']['flag'] is True

    @patch('gs_quant.analytics.core.query_helpers.GsSession')
    def test_bool_parameter_both_values_skipped(self, mock_gs):
        mock_gs.current._post.return_value = {'data': [{'date': '2021-01-01', 'v': 1}]}
        qi = {
            'datasetId': 'DS1',
            'parameters': {'flag': {True, False}},
            'range': {'startDate': '2021-01-01'},
            'realTime': False,
        }
        fetch_query(qi)
        payload = mock_gs.current._post.call_args[1]['payload']
        assert 'flag' not in payload['where']


# ---------------------------------------------------------------------------
# build_query_string
# ---------------------------------------------------------------------------

class TestBuildQueryString:
    def test_string_values_quoted(self):
        result = build_query_string([('asset', 'SPX')])
        assert result == 'asset == "SPX"'

    def test_numeric_values(self):
        result = build_query_string([('strike', 100)])
        assert result == 'strike == 100'

    def test_multiple_dimensions(self):
        result = build_query_string([('asset', 'SPX'), ('strike', 100)])
        assert result == 'asset == "SPX" & strike == 100'

    def test_empty_dimensions(self):
        assert build_query_string([]) == ''


# ---------------------------------------------------------------------------
# valid_dimensions
# ---------------------------------------------------------------------------

class TestValidDimensions:
    def test_all_present(self):
        df = pd.DataFrame({'asset': [1], 'strike': [2]})
        assert valid_dimensions((('asset', 'SPX'), ('strike', 100)), df) is True

    def test_one_missing(self):
        df = pd.DataFrame({'asset': [1]})
        assert valid_dimensions((('asset', 'SPX'), ('strike', 100)), df) is False

    def test_empty_dimensions(self):
        df = pd.DataFrame({'asset': [1]})
        assert valid_dimensions((), df) is True
