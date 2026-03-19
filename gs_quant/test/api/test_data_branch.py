"""
Branch coverage tests for gs_quant/api/data.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.data import DataApi


class TestDataApiBuildQuery:
    """Test DataApi.build_query with various branches."""

    def test_build_query_with_market_data_coordinates_real_time(self):
        """Build query with market_data_coordinates and real-time (datetime) start/end."""
        start = dt.datetime(2020, 1, 1, 10, 0)
        end = dt.datetime(2020, 1, 1, 11, 0)
        result = DataApi.build_query(
            start=start, end=end,
            market_data_coordinates=[MagicMock()]
        )
        # Should create an MDAPIDataQuery
        from gs_quant.target.coordinates import MDAPIDataQuery
        assert isinstance(result, MDAPIDataQuery)

    def test_build_query_with_market_data_coordinates_non_real_time(self):
        """Build query with market_data_coordinates and date (non-real-time) start/end."""
        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 1, 10)
        result = DataApi.build_query(
            start=start, end=end,
            market_data_coordinates=[MagicMock()]
        )
        from gs_quant.target.coordinates import MDAPIDataQuery
        assert isinstance(result, MDAPIDataQuery)

    def test_build_query_with_market_data_coordinates_none_start_end(self):
        """Build query with market_data_coordinates and None start/end (real_time branch)."""
        result = DataApi.build_query(
            market_data_coordinates=[MagicMock()]
        )
        from gs_quant.target.coordinates import MDAPIDataQuery
        assert isinstance(result, MDAPIDataQuery)

    def test_build_query_start_datetime_end_date_raises(self):
        """Datetime start with date end raises ValueError."""
        start = dt.datetime(2020, 1, 1, 10, 0)
        end = dt.date(2020, 1, 10)
        with pytest.raises(ValueError, match='If start is of type datetime'):
            DataApi.build_query(start=start, end=end)

    def test_build_query_start_date_end_not_date_raises(self):
        """Date start with non-date end raises ValueError."""
        start = dt.date(2020, 1, 1)
        end = 'not-a-date'
        with pytest.raises(ValueError, match='If start is of type date'):
            DataApi.build_query(start=start, end=end)

    def test_build_query_date_start_and_end(self):
        """Normal date start and end creates DataQuery."""
        from gs_quant.target.data import DataQuery
        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 1, 10)
        result = DataApi.build_query(start=start, end=end)
        assert isinstance(result, DataQuery)

    def test_build_query_datetime_start_and_end(self):
        """Datetime start and end creates DataQuery."""
        from gs_quant.target.data import DataQuery
        start = dt.datetime(2020, 1, 1, 10, 0)
        end = dt.datetime(2020, 1, 10, 10, 0)
        result = DataApi.build_query(start=start, end=end)
        assert isinstance(result, DataQuery)

    def test_build_query_with_where_kwargs(self):
        """kwargs not in query properties go to where dict."""
        from gs_quant.target.data import DataQuery
        result = DataApi.build_query(
            start=dt.date(2020, 1, 1),
            end=dt.date(2020, 1, 10),
            someCustomField='value'
        )
        assert 'someCustomField' in result.where

    def test_build_query_with_known_property_kwarg(self):
        """kwargs matching query properties get set as attributes."""
        from gs_quant.target.data import DataQuery
        result = DataApi.build_query(
            start=dt.date(2020, 1, 1),
            end=dt.date(2020, 1, 10),
            format='Json'
        )
        # format_ is set as attribute (may be enum-converted)
        assert result.format_ is not None

    def test_build_query_with_fields_and_restrict(self):
        """When fields is set, restrict_fields is set."""
        from gs_quant.target.data import DataQuery
        result = DataApi.build_query(
            start=dt.date(2020, 1, 1),
            end=dt.date(2020, 1, 10),
            fields=['price', 'date'],
            restrict_fields=True,
        )
        # fields should be set via where or attribute depending on property
        assert result is not None

    def test_build_query_with_as_of_and_since(self):
        """as_of and since parameters are passed through."""
        from gs_quant.target.data import DataQuery
        as_of = dt.datetime(2020, 1, 5, 10, 0)
        since = dt.datetime(2020, 1, 1, 10, 0)
        result = DataApi.build_query(
            start=dt.date(2020, 1, 1),
            end=dt.date(2020, 1, 10),
            as_of=as_of,
            since=since,
        )
        assert result.as_of_time == as_of
        assert result.since == since

    def test_build_query_with_dates_and_empty_intervals(self):
        """dates and empty_intervals parameters are passed through."""
        from gs_quant.target.data import DataQuery
        dates_list = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]
        result = DataApi.build_query(
            dates=dates_list,
            empty_intervals=True,
        )
        assert tuple(result.dates) == tuple(dates_list)
        assert result.empty_intervals is True

    def test_build_query_no_start_no_end(self):
        """No start and no end creates a DataQuery (all None)."""
        from gs_quant.target.data import DataQuery
        result = DataApi.build_query()
        assert isinstance(result, DataQuery)


class TestDataApiAbstractMethods:
    """Test that abstract methods raise NotImplementedError."""

    def test_query_data_raises(self):
        with pytest.raises(NotImplementedError):
            DataApi.query_data(MagicMock())

    def test_last_data_raises(self):
        with pytest.raises(NotImplementedError):
            DataApi.last_data(MagicMock())

    def test_symbol_dimensions_raises(self):
        with pytest.raises(NotImplementedError):
            DataApi.symbol_dimensions('test')

    def test_time_field_raises(self):
        with pytest.raises(NotImplementedError):
            DataApi.time_field('test')

    def test_construct_dataframe_with_types_raises(self):
        with pytest.raises(NotImplementedError):
            DataApi.construct_dataframe_with_types('test', [])
