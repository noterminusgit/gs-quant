"""
Tests for gs_quant/backtests/data_handler.py

Covers:
- Clock: update with naive vs aware datetimes, backwards time raises RuntimeError
- Clock: reset sets to 1900-01-01 UTC
- Clock: time_check with datetime (naive & aware) and date, lookahead detection
- DataHandler: __init__, reset_clock, update delegation
- DataHandler: _utc_time with naive datetime, aware datetime, and date
- DataHandler: get_data delegates to data_mgr after time_check
- DataHandler: get_data_range delegates, type mismatch raises RuntimeError
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.backtests.data_handler import Clock, DataHandler


class TestClock:
    def test_reset_sets_initial_time(self):
        clock = Clock()
        assert clock._time == dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)

    def test_update_with_aware_datetime(self):
        clock = Clock()
        t = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        clock.update(t)
        assert clock._time == t

    def test_update_with_naive_datetime(self):
        """When time is naive, compare_time should strip tzinfo from self._time."""
        clock = Clock()
        t = dt.datetime(2023, 6, 15, 12, 0, 0)  # naive
        clock.update(t)
        assert clock._time == t

    def test_update_backwards_raises_runtime_error_aware(self):
        clock = Clock()
        t1 = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        clock.update(t1)
        t2 = dt.datetime(2023, 6, 14, 12, 0, 0, tzinfo=dt.timezone.utc)
        with pytest.raises(RuntimeError, match='cannot run backwards'):
            clock.update(t2)

    def test_update_backwards_raises_runtime_error_naive(self):
        clock = Clock()
        t1 = dt.datetime(2023, 6, 15, 12, 0, 0)  # naive
        clock.update(t1)
        t2 = dt.datetime(2023, 6, 14, 12, 0, 0)  # naive, earlier
        with pytest.raises(RuntimeError, match='cannot run backwards'):
            clock.update(t2)

    def test_update_same_time_does_not_raise(self):
        clock = Clock()
        t = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        clock.update(t)
        clock.update(t)  # same time, should not raise

    def test_reset_after_update(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        clock.reset()
        assert clock._time == dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)

    def test_time_check_datetime_naive_no_lookahead(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        # naive datetime before current time (stripped)
        state = dt.datetime(2023, 6, 15, 11, 0, 0)
        clock.time_check(state)  # should not raise

    def test_time_check_datetime_naive_lookahead_raises(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        # naive datetime after current time (stripped to 2023-06-15 12:00:00)
        state = dt.datetime(2023, 6, 15, 13, 0, 0)
        with pytest.raises(RuntimeError, match='accessing data at .* not allowed'):
            clock.time_check(state)

    def test_time_check_datetime_aware_no_lookahead(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.datetime(2023, 6, 15, 11, 0, 0, tzinfo=dt.timezone.utc)
        clock.time_check(state)  # should not raise

    def test_time_check_datetime_aware_lookahead_raises(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.datetime(2023, 6, 15, 13, 0, 0, tzinfo=dt.timezone.utc)
        with pytest.raises(RuntimeError, match='accessing data at .* not allowed'):
            clock.time_check(state)

    def test_time_check_date_no_lookahead(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.date(2023, 6, 15)
        clock.time_check(state)  # should not raise

    def test_time_check_date_lookahead_raises(self):
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.date(2023, 6, 16)
        with pytest.raises(RuntimeError, match='accessing data at .* not allowed'):
            clock.time_check(state)

    def test_time_check_date_equal_no_raise(self):
        """date == self._time.date() should not raise (lookahead is > not >=)."""
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 0, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.date(2023, 6, 15)
        clock.time_check(state)  # should not raise

    def test_update_with_timezone_having_none_utcoffset(self):
        """Test branch: time.tzinfo is not None but utcoffset returns None."""
        clock = Clock()

        class FakeTZ(dt.tzinfo):
            def utcoffset(self, _dt):
                return None

            def tzname(self, _dt):
                return 'FAKE'

            def dst(self, _dt):
                return None

        t = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=FakeTZ())
        clock.update(t)
        assert clock._time == t

    def test_time_check_with_timezone_having_none_utcoffset(self):
        """Datetime with tzinfo but utcoffset returns None is treated as naive."""
        clock = Clock()
        clock.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))

        class FakeTZ(dt.tzinfo):
            def utcoffset(self, _dt):
                return None

            def tzname(self, _dt):
                return 'FAKE'

            def dst(self, _dt):
                return None

        state = dt.datetime(2023, 6, 15, 11, 0, 0, tzinfo=FakeTZ())
        clock.time_check(state)  # naive branch, should not raise


class TestDataHandler:
    def _make_handler(self, tz=dt.timezone.utc):
        data_mgr = MagicMock()
        handler = DataHandler(data_mgr, tz)
        return handler, data_mgr

    def test_init(self):
        handler, data_mgr = self._make_handler()
        assert handler._data_mgr is data_mgr
        assert handler._tz == dt.timezone.utc
        assert isinstance(handler._clock, Clock)

    def test_reset_clock(self):
        handler, _ = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        handler.reset_clock()
        assert handler._clock._time == dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)

    def test_update_delegates_to_clock(self):
        handler, _ = self._make_handler()
        t = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        handler.update(t)
        assert handler._clock._time == t

    def test_utc_time_with_naive_datetime(self):
        """Naive datetime should be converted: replace tz -> astimezone utc -> strip tz."""
        eastern = dt.timezone(dt.timedelta(hours=-5))
        handler, _ = self._make_handler(tz=eastern)
        naive = dt.datetime(2023, 6, 15, 12, 0, 0)
        result = handler._utc_time(naive)
        # 12:00 Eastern = 17:00 UTC, then strip tzinfo
        expected = dt.datetime(2023, 6, 15, 17, 0, 0)
        assert result == expected
        assert result.tzinfo is None

    def test_utc_time_with_aware_datetime(self):
        """Aware datetime should be returned as-is."""
        handler, _ = self._make_handler()
        aware = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        result = handler._utc_time(aware)
        assert result == aware

    def test_utc_time_with_date(self):
        """Date (not datetime) should be returned as-is."""
        handler, _ = self._make_handler()
        d = dt.date(2023, 6, 15)
        result = handler._utc_time(d)
        assert result == d

    def test_utc_time_with_none_utcoffset_datetime(self):
        """Datetime with tzinfo but utcoffset=None should be treated as naive."""
        eastern = dt.timezone(dt.timedelta(hours=-5))
        handler, _ = self._make_handler(tz=eastern)

        class FakeTZ(dt.tzinfo):
            def utcoffset(self, _dt):
                return None

            def tzname(self, _dt):
                return 'FAKE'

            def dst(self, _dt):
                return None

        naive_ish = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=FakeTZ())
        result = handler._utc_time(naive_ish)
        # Should be treated as naive: replace tz with eastern, convert to utc, strip tz
        expected = dt.datetime(2023, 6, 15, 17, 0, 0)
        assert result == expected
        assert result.tzinfo is None

    def test_get_data_calls_data_mgr(self):
        handler, data_mgr = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        state = dt.date(2023, 6, 15)
        key1, key2 = MagicMock(), MagicMock()
        data_mgr.get_data.return_value = 42.0

        result = handler.get_data(state, key1, key2)
        assert result == 42.0
        data_mgr.get_data.assert_called_once_with(state, key1, key2)

    def test_get_data_checks_time(self):
        handler, data_mgr = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        future_state = dt.date(2023, 6, 16)
        with pytest.raises(RuntimeError, match='accessing data at .* not allowed'):
            handler.get_data(future_state)

    def test_get_data_range_calls_data_mgr(self):
        handler, data_mgr = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        start = dt.date(2023, 6, 14)
        end = dt.date(2023, 6, 15)
        key = MagicMock()
        data_mgr.get_data_range.return_value = [1.0, 2.0]

        result = handler.get_data_range(start, end, key)
        assert result == [1.0, 2.0]
        data_mgr.get_data_range.assert_called_once_with(start, end, key)

    def test_get_data_range_checks_end_time(self):
        handler, data_mgr = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        start = dt.date(2023, 6, 14)
        end = dt.date(2023, 6, 16)
        with pytest.raises(RuntimeError, match='accessing data at .* not allowed'):
            handler.get_data_range(start, end)

    def test_get_data_range_type_mismatch_raises(self):
        handler, data_mgr = self._make_handler()
        handler.update(dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc))
        start = dt.date(2023, 6, 14)
        end = dt.datetime(2023, 6, 15, 10, 0, 0, tzinfo=dt.timezone.utc)
        with pytest.raises(RuntimeError, match='expect same type for start and end'):
            handler.get_data_range(start, end)

    def test_get_data_range_with_naive_datetimes(self):
        """Naive datetimes should be converted through _utc_time."""
        eastern = dt.timezone(dt.timedelta(hours=-5))
        handler, data_mgr = self._make_handler(tz=eastern)
        handler.update(dt.datetime(2023, 6, 15, 23, 0, 0, tzinfo=dt.timezone.utc))

        start = dt.datetime(2023, 6, 14, 12, 0, 0)
        end = dt.datetime(2023, 6, 15, 12, 0, 0)
        data_mgr.get_data_range.return_value = []

        handler.get_data_range(start, end)
        # Both should be UTC-converted: 12:00 Eastern -> 17:00 UTC (naive)
        expected_start = dt.datetime(2023, 6, 14, 17, 0, 0)
        expected_end = dt.datetime(2023, 6, 15, 17, 0, 0)
        data_mgr.get_data_range.assert_called_once_with(expected_start, expected_end)

    def test_get_data_with_naive_datetime(self):
        """Naive datetime state should be UTC-converted before passing to data_mgr."""
        eastern = dt.timezone(dt.timedelta(hours=-5))
        handler, data_mgr = self._make_handler(tz=eastern)
        handler.update(dt.datetime(2023, 6, 15, 23, 0, 0, tzinfo=dt.timezone.utc))

        state = dt.datetime(2023, 6, 15, 12, 0, 0)  # naive
        data_mgr.get_data.return_value = 99.0

        handler.get_data(state)
        expected_utc = dt.datetime(2023, 6, 15, 17, 0, 0)
        data_mgr.get_data.assert_called_once_with(expected_utc)
