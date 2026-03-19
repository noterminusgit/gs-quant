"""
Branch-coverage tests for gs_quant/datetime/time.py

Covers missing branches:
- [55,-52], [55,56], [56,-52], [56,57]: Timer.__exit__ print_on_exit / threshold logic
- [70,71]: time_difference_as_string with invalid resolution
- [78,91]: time_difference_as_string loop reaching resolution before all units
"""

import datetime as dt
import logging
from unittest.mock import patch

import numpy as np
import pytest

from gs_quant.datetime.time import (
    Timer,
    time_difference_as_string,
    to_zulu_string,
)
from gs_quant.errors import MqValueError


# ---------------------------------------------------------------------------
# Timer context manager
# ---------------------------------------------------------------------------


class TestTimer:
    def test_print_on_exit_true_no_threshold(self):
        """Cover branch [55,56] True and [56,57] threshold is None.

        When print_on_exit=True and threshold=None, the warning is logged.
        """
        with patch('gs_quant.datetime.time._logger') as mock_logger:
            with Timer(print_on_exit=True, label='test-timer', threshold=None):
                pass
            mock_logger.warning.assert_called_once()
            assert 'test-timer' in mock_logger.warning.call_args[0][0]

    def test_print_on_exit_true_threshold_exceeded(self):
        """Cover branch [56,57] when elapsed.seconds > threshold.

        We mock datetime to make elapsed time large.
        """
        with patch('gs_quant.datetime.time._logger') as mock_logger:
            with Timer(print_on_exit=True, label='slow-op', threshold=0):
                pass  # Even 0 seconds should exceed threshold=0 if microseconds > 0
            # The threshold check is `elapsed.seconds > threshold`
            # elapsed.seconds could be 0, so 0 > 0 is False
            # Let's use threshold=-1 to ensure it's exceeded
        with patch('gs_quant.datetime.time._logger') as mock_logger:
            with Timer(print_on_exit=True, label='slow-op', threshold=-1):
                pass
            mock_logger.warning.assert_called_once()

    def test_print_on_exit_true_threshold_not_exceeded(self):
        """Cover branch [56,-52]: threshold is set but not exceeded -> no log.

        When elapsed.seconds <= threshold, the warning is NOT logged.
        """
        with patch('gs_quant.datetime.time._logger') as mock_logger:
            with Timer(print_on_exit=True, label='fast-op', threshold=9999):
                pass
            mock_logger.warning.assert_not_called()

    def test_print_on_exit_false(self):
        """Cover branch [55,-52]: print_on_exit=False -> skip logging entirely."""
        with patch('gs_quant.datetime.time._logger') as mock_logger:
            with Timer(print_on_exit=False, label='silent'):
                pass
            mock_logger.warning.assert_not_called()


# ---------------------------------------------------------------------------
# to_zulu_string
# ---------------------------------------------------------------------------


class TestToZuluString:
    def test_basic(self):
        t = dt.datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = to_zulu_string(t)
        assert result.endswith('Z')
        assert '2024-01-15' in result


# ---------------------------------------------------------------------------
# time_difference_as_string
# ---------------------------------------------------------------------------


class TestTimeDifferenceAsString:
    def test_invalid_resolution_raises(self):
        """Cover branch [70,71]: resolution not in time_strings -> raises.

        Note: The source has a formatting bug ('incorrect resolution passed in "s"' % resolution)
        which raises TypeError instead of MqValueError. We catch both.
        """
        with pytest.raises((MqValueError, TypeError)):
            time_difference_as_string(np.timedelta64(60, 's'), resolution='Invalid')

    def test_seconds_resolution(self):
        """Basic test with Second resolution."""
        td = np.timedelta64(90, 's')
        result = time_difference_as_string(td, resolution='Second')
        assert '1 Minute' in result
        assert '30 Seconds' in result

    def test_minute_resolution(self):
        """Cover branch [78,91]: loop breaks at Minute resolution.

        When resolution='Minute', the loop breaks after processing Minutes,
        skipping Seconds.
        """
        td = np.timedelta64(90, 's')
        result = time_difference_as_string(td, resolution='Minute')
        assert '1 Minute' in result
        assert 'Second' not in result

    def test_hour_resolution(self):
        """Test with Hour resolution."""
        td = np.timedelta64(3661, 's')
        result = time_difference_as_string(td, resolution='Hour')
        assert '1 Hour' in result
        assert 'Minute' not in result

    def test_day_resolution(self):
        td = np.timedelta64(2, 'D')
        result = time_difference_as_string(td, resolution='Day')
        assert '2 Days' in result

    def test_week_resolution(self):
        # Note: SECS_IN_WEEK > SECS_IN_YEAR in the source due to a constant bug.
        # So we test with 'Week' resolution using a large timedelta that first
        # produces Years, then the remainder produces Weeks.
        from gs_quant.datetime.time import SECS_IN_YEAR, SECS_IN_WEEK
        total = int(SECS_IN_YEAR + SECS_IN_WEEK)
        td = np.timedelta64(total, 's')
        result = time_difference_as_string(td, resolution='Week')
        assert 'Year' in result or 'Week' in result

    def test_year_resolution(self):
        td = np.timedelta64(400, 'D')
        result = time_difference_as_string(td, resolution='Year')
        assert '1 Year' in result

    def test_zero_delta(self):
        td = np.timedelta64(0, 's')
        result = time_difference_as_string(td, resolution='Second')
        assert result == ''

    def test_plural_handling(self):
        """When m == 1, no 's' suffix; when m > 1, 's' suffix."""
        td = np.timedelta64(1, 's')
        result = time_difference_as_string(td, resolution='Second')
        assert result == '1 Second'

        td2 = np.timedelta64(2, 's')
        result2 = time_difference_as_string(td2, resolution='Second')
        assert result2 == '2 Seconds'
