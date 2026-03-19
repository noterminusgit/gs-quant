"""
Branch coverage tests for gs_quant/data/core.py
"""
import datetime as dt
from unittest.mock import patch

import pytest

from gs_quant.data.core import DataContext, DataFrequency, DataAggregationOperator, IntervalFrequency, _now
from gs_quant.errors import MqTypeError, MqValueError


class TestDataFrequency:
    def test_values(self):
        assert DataFrequency.DAILY.value == 'daily'
        assert DataFrequency.REAL_TIME.value == 'realTime'
        assert DataFrequency.ANY.value == 'any'


class TestDataAggregationOperator:
    def test_values(self):
        assert DataAggregationOperator.MIN == 'min'
        assert DataAggregationOperator.MAX == 'max'
        assert DataAggregationOperator.FIRST == 'first'
        assert DataAggregationOperator.LAST == 'last'


class TestIntervalFrequency:
    def test_values(self):
        assert IntervalFrequency.DAILY.value == 'daily'
        assert IntervalFrequency.WEEKLY.value == 'weekly'
        assert IntervalFrequency.MONTHLY.value == 'monthly'
        assert IntervalFrequency.YEARLY.value == 'yearly'


class TestNow:
    def test_now_returns_utc_datetime(self):
        result = _now()
        assert isinstance(result, dt.datetime)
        assert result.tzinfo == dt.timezone.utc


class TestDataContext:
    def test_init_no_args(self):
        dc = DataContext()
        assert dc.interval is None

    def test_init_with_start_end(self):
        dc = DataContext(start=dt.date(2021, 1, 1), end=dt.date(2021, 12, 31))
        assert dc.start_date == dt.date(2021, 1, 1)
        assert dc.end_date == dt.date(2021, 12, 31)

    def test_init_with_valid_interval(self):
        dc = DataContext(interval='1m')
        assert dc.interval == '1m'

    def test_init_with_valid_interval_multi_digit(self):
        dc = DataContext(interval='15m')
        assert dc.interval == '15m'

    def test_init_with_valid_interval_three_digit(self):
        dc = DataContext(interval='100d')
        assert dc.interval == '100d'

    def test_init_interval_not_str(self):
        with pytest.raises(MqTypeError, match='interval must be a str'):
            DataContext(interval=123)

    def test_init_interval_invalid_format(self):
        with pytest.raises(MqValueError, match='interval must be a valid str'):
            DataContext(interval='abc')

    def test_init_interval_invalid_starts_with_zero(self):
        with pytest.raises(MqValueError, match='interval must be a valid str'):
            DataContext(interval='0m')

    def test_init_interval_no_letter(self):
        with pytest.raises(MqValueError, match='interval must be a valid str'):
            DataContext(interval='123')

    def test_init_interval_uppercase_letter(self):
        with pytest.raises(MqValueError, match='interval must be a valid str'):
            DataContext(interval='1M')

    # ---------- _get_date ----------

    def test_get_date_none(self):
        default = dt.date(2020, 1, 1)
        assert DataContext._get_date(None, default) == default

    def test_get_date_datetime_object(self):
        d = dt.datetime(2021, 6, 15, 10, 30, 0)
        result = DataContext._get_date(d, None)
        assert result == dt.date(2021, 6, 15)

    def test_get_date_date_object(self):
        d = dt.date(2021, 6, 15)
        result = DataContext._get_date(d, None)
        assert result == d

    def test_get_date_string_no_T(self):
        result = DataContext._get_date('2021-06-15', None)
        assert result == dt.date(2021, 6, 15)

    def test_get_date_string_with_T(self):
        result = DataContext._get_date('2021-06-15T10:30:00Z', None)
        assert result == dt.date(2021, 6, 15)

    def test_get_date_invalid_type(self):
        with pytest.raises(ValueError, match='is not a valid date'):
            DataContext._get_date(12345, None)

    # ---------- _get_datetime ----------

    def test_get_datetime_none(self):
        default = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        assert DataContext._get_datetime(None, default) == default

    def test_get_datetime_datetime_object(self):
        d = dt.datetime(2021, 6, 15, 10, 30, 0, tzinfo=dt.timezone.utc)
        result = DataContext._get_datetime(d, None)
        assert result == d

    def test_get_datetime_date_object(self):
        d = dt.date(2021, 6, 15)
        result = DataContext._get_datetime(d, None)
        assert result == dt.datetime(2021, 6, 15, 0, 0, 0, tzinfo=dt.timezone.utc)

    def test_get_datetime_string(self):
        result = DataContext._get_datetime('2021-06-15T10:30:00Z', None)
        assert result == dt.datetime(2021, 6, 15, 10, 30, 0, tzinfo=dt.timezone.utc)

    def test_get_datetime_invalid_type(self):
        with pytest.raises(ValueError, match='is not a valid date'):
            DataContext._get_datetime(12345, None)

    # ---------- properties ----------

    def test_start_date_default(self):
        dc = DataContext()
        result = dc.start_date
        expected = dt.date.today() - dt.timedelta(days=30)
        assert result == expected

    def test_end_date_default(self):
        dc = DataContext()
        result = dc.end_date
        assert result == dt.date.today()

    def test_start_time_default(self):
        dc = DataContext()
        result = dc.start_time
        assert isinstance(result, dt.datetime)
        assert result.tzinfo == dt.timezone.utc

    def test_end_time_default(self):
        dc = DataContext()
        result = dc.end_time
        assert isinstance(result, dt.datetime)
        assert result.tzinfo == dt.timezone.utc

    # ---------- context manager ----------

    def test_context_manager(self):
        with DataContext(dt.date(2021, 1, 1), dt.date(2021, 12, 31)) as dc:
            assert dc.start_date == dt.date(2021, 1, 1)
            assert dc.end_date == dt.date(2021, 12, 31)

    def test_context_manager_datetime(self):
        start = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2021, 12, 31, tzinfo=dt.timezone.utc)
        with DataContext(start, end) as dc:
            assert dc.start_date == dt.date(2021, 1, 1)
            assert dc.end_date == dt.date(2021, 12, 31)
            assert dc.start_time == start
            assert dc.end_time == end

    def test_context_manager_string_datetime(self):
        with DataContext(None, '2019-01-01T00:00:00Z') as dc:
            assert dc.end_date == dt.date(2019, 1, 1)
            assert dc.end_time == dt.datetime(2019, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)

    def test_interval_none_by_default(self):
        dc = DataContext()
        assert dc.interval is None

    def test_interval_property(self):
        dc = DataContext(interval='5h')
        assert dc.interval == '5h'
