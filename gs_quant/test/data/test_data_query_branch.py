"""
Branch coverage tests for gs_quant/data/query.py
Covers branches: [53,54], [53,56], [56,-50], [56,57]
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.data.query import DataQuery, DataQueryType


class TestDataQueryGetSeries:
    """Cover all branches in DataQuery.get_series()."""

    def test_range_type_returns_series(self):
        """[53,54]: RANGE type -> coordinate.get_series is called."""
        coord = MagicMock()
        expected = pd.Series([1, 2, 3])
        coord.get_series.return_value = expected
        q = DataQuery(
            coordinate=coord,
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 6, 1),
            query_type=DataQueryType.RANGE,
        )
        result = q.get_series()
        coord.get_series.assert_called_once_with(dt.date(2024, 1, 1), dt.date(2024, 6, 1))
        pd.testing.assert_series_equal(result, expected)

    def test_last_type_returns_last_value(self):
        """[53,56] + [56,57]: not RANGE, LAST -> coordinate.last_value is called."""
        coord = MagicMock()
        expected = pd.Series([42])
        coord.last_value.return_value = expected
        q = DataQuery(
            coordinate=coord,
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 6, 1),
            query_type=DataQueryType.LAST,
        )
        result = q.get_series()
        coord.last_value.assert_called_once_with(dt.date(2024, 6, 1))
        pd.testing.assert_series_equal(result, expected)

    def test_unknown_type_returns_none(self):
        """[56,-50]: neither RANGE nor LAST -> returns None."""
        coord = MagicMock()
        q = DataQuery(
            coordinate=coord,
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 6, 1),
        )
        # Override query_type to something that is neither RANGE nor LAST
        q.query_type = MagicMock()
        q.query_type.__eq__ = lambda self, other: False
        # Use 'is' comparisons, so we need the same identity to fail
        result = q.get_series()
        assert result is None

    def test_get_data_series(self):
        """Test get_data_series wraps get_series result."""
        coord = MagicMock()
        coord.get_series.return_value = pd.Series([1, 2])
        q = DataQuery(
            coordinate=coord,
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 6, 1),
            query_type=DataQueryType.RANGE,
        )
        ds = q.get_data_series()
        assert ds.coordinate is coord

    def test_get_range_string(self):
        """Test get_range_string formatting."""
        coord = MagicMock()
        q = DataQuery(
            coordinate=coord,
            start=dt.date(2024, 1, 1),
            end=dt.date(2024, 6, 1),
        )
        assert q.get_range_string() == 'start=2024-01-01|end=2024-06-01'
