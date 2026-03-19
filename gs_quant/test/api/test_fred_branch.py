"""
Branch coverage tests for gs_quant/api/fred/data.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests.exceptions import HTTPError

from gs_quant.api.fred.data import FredDataApi
from gs_quant.api.fred.fred_query import FredQuery


class TestFredDataApiInit:
    """Test FredDataApi __init__."""

    def test_init_with_api_key(self):
        """Init with API key sets the key."""
        api = FredDataApi(api_key='test_key')
        assert api.api_key == 'test_key'

    def test_init_without_api_key_raises(self):
        """Init without API key raises ValueError."""
        with pytest.raises(ValueError, match='Please pass a string'):
            FredDataApi()

    def test_init_with_none_api_key_raises(self):
        """Init with None API key raises ValueError."""
        with pytest.raises(ValueError, match='Please pass a string'):
            FredDataApi(api_key=None)


class TestFredDataApiBuildQuery:
    """Test build_query method."""

    def test_build_query_basic(self):
        """Build query with start and end."""
        api = FredDataApi(api_key='test')
        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 12, 31)
        result = api.build_query(start=start, end=end)
        assert isinstance(result, FredQuery)
        assert result.observation_start == start
        assert result.observation_end == end

    def test_build_query_no_dates(self):
        """Build query without dates."""
        api = FredDataApi(api_key='test')
        result = api.build_query()
        assert isinstance(result, FredQuery)

    def test_build_query_mismatched_types_raises(self):
        """start and end of different types raises ValueError."""
        api = FredDataApi(api_key='test')
        with pytest.raises(ValueError, match='Start and end types must match'):
            api.build_query(start=dt.date(2020, 1, 1), end=dt.datetime(2020, 12, 31, 0, 0))

    def test_build_query_with_none_start(self):
        """start=None, end=None does not check types."""
        api = FredDataApi(api_key='test')
        result = api.build_query(start=None, end=None)
        assert isinstance(result, FredQuery)

    def test_build_query_start_none_end_set(self):
        """start=None with end set skips type check (start is None)."""
        api = FredDataApi(api_key='test')
        result = api.build_query(start=None, end=dt.date(2020, 12, 31))
        assert isinstance(result, FredQuery)

    def test_build_query_with_as_of_and_since(self):
        """as_of maps to realtime_end, since to realtime_start."""
        api = FredDataApi(api_key='test')
        as_of = dt.datetime(2020, 6, 1, 0, 0)
        since = dt.datetime(2020, 1, 1, 0, 0)
        result = api.build_query(as_of=as_of, since=since)
        assert result.realtime_end == as_of
        assert result.realtime_start == since


class TestFredDataApiQueryData:
    """Test query_data method."""

    def _make_response(self, json_data, status_code=200, raise_for_status=None):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_data
        mock_resp.raise_for_status = MagicMock()
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
        return mock_resp

    def test_query_data_success(self):
        """Successful query returns a pd.Series."""
        api = FredDataApi(api_key='test_key')
        json_data = {
            'observations': [
                {'date': '2020-01-01', 'value': '100.0'},
                {'date': '2020-01-02', 'value': '101.0'},
            ]
        }
        mock_resp = self._make_response(json_data)

        with patch('gs_quant.api.fred.data.handle_proxy', return_value=mock_resp):
            query = FredQuery()
            result = api.query_data(query, 'GDP')
            assert isinstance(result, pd.Series)
            assert result.name == 'GDP'
            assert len(result) == 2

    def test_query_data_http_error(self):
        """HTTP error raises ValueError."""
        api = FredDataApi(api_key='test_key')
        error_json = {'error_message': 'Bad API Key'}
        mock_resp = self._make_response(
            error_json,
            status_code=400,
            raise_for_status=HTTPError('400')
        )

        with patch('gs_quant.api.fred.data.handle_proxy', return_value=mock_resp):
            query = FredQuery()
            with pytest.raises(ValueError, match='Bad API Key'):
                api.query_data(query, 'GDP')

    def test_query_data_empty_observations(self):
        """Empty observations raises ValueError."""
        api = FredDataApi(api_key='test_key')
        json_data = {'observations': []}
        mock_resp = self._make_response(json_data)

        with patch('gs_quant.api.fred.data.handle_proxy', return_value=mock_resp):
            query = FredQuery()
            with pytest.raises(ValueError, match='No data exists'):
                api.query_data(query, 'GDP')

    def test_query_data_filters_dot_values(self):
        """Values of '.' are filtered out."""
        api = FredDataApi(api_key='test_key')
        json_data = {
            'observations': [
                {'date': '2020-01-01', 'value': '.'},
                {'date': '2020-01-02', 'value': '101.0'},
            ]
        }
        mock_resp = self._make_response(json_data)

        with patch('gs_quant.api.fred.data.handle_proxy', return_value=mock_resp):
            query = FredQuery()
            result = api.query_data(query, 'GDP')
            assert len(result) == 1


class TestFredDataApiLastData:
    """Test last_data method."""

    def test_last_data(self):
        """last_data calls query_data and then .last('1D') on the result."""
        api = FredDataApi(api_key='test_key')
        query = FredQuery()

        mock_series = MagicMock()
        mock_series.last.return_value = MagicMock()

        with patch.object(api, 'query_data', return_value=mock_series):
            result = api.last_data(query, 'GDP')
            mock_series.last.assert_called_once_with('1D')


class TestFredDataApiConstructDataframe:
    """Test construct_dataframe_with_types method."""

    def test_with_series_data(self):
        """Series data returns a DataFrame."""
        api = FredDataApi(api_key='test')
        series = pd.Series([1.0, 2.0], name='GDP')
        result = api.construct_dataframe_with_types('GDP', series)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_with_empty_series(self):
        """Empty series returns empty DataFrame."""
        api = FredDataApi(api_key='test')
        series = pd.Series([], dtype=float)
        result = api.construct_dataframe_with_types('GDP', series)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_with_non_series_data(self):
        """Non-Series data returns empty DataFrame."""
        api = FredDataApi(api_key='test')
        result = api.construct_dataframe_with_types('GDP', 'not_a_series')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestFredDataApiSymbolDimensions:
    """Test symbol_dimensions method."""

    def test_symbol_dimensions(self):
        """symbol_dimensions returns shape of queried data."""
        api = FredDataApi(api_key='test_key')
        json_data = {
            'observations': [
                {'date': '2020-01-01', 'value': '100.0'},
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = json_data
        mock_resp.raise_for_status = MagicMock()

        with patch('gs_quant.api.fred.data.handle_proxy', return_value=mock_resp):
            result = api.symbol_dimensions('GDP')
            assert isinstance(result, tuple)


class TestFredDataApiTimeField:
    """Test time_field method."""

    def test_time_field_returns_none(self):
        """time_field returns None (pass)."""
        api = FredDataApi(api_key='test')
        result = api.time_field('GDP')
        assert result is None
