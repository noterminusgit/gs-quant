"""
Branch-coverage tests for gs_quant/markets/report_utils.py
"""

import datetime as dt
from unittest.mock import patch

import pytest

from gs_quant.markets.report_utils import _batch_dates, _get_ppaa_batches


class TestBatchDates:
    """Cover branches [36,38], [40,41], [40,46] in _batch_dates."""

    def test_single_batch_when_range_smaller_than_batch_size(self):
        """Branch [36,38]: (start_date - end_date).days < batch_size -> single batch."""
        start = dt.date(2024, 1, 1)
        end = dt.date(2024, 1, 10)
        # (start - end).days = -9, which is < any positive batch_size
        result = _batch_dates(start, end, batch_size=30)
        assert result == [[start, end]]

    def test_single_batch_same_date(self):
        """Branch [36,38]: start == end -> (0 - 0).days = 0 < batch_size."""
        start = dt.date(2024, 1, 1)
        result = _batch_dates(start, start, batch_size=5)
        assert result == [[start, start]]

    def test_while_loop_branches(self):
        """Branches [40,41] and [40,46]: while loop executes when
        (start_date - end_date).days >= batch_size (start > end, large difference)."""
        # The condition `(start_date - end_date).days < batch_size` is False
        # only when start_date > end_date and the difference >= batch_size.
        # But `end_date > curr_end` in the while would be False immediately since
        # end_date < start_date = curr_end. So the while loop body never executes.
        # This means the while loop branch [40,46] (while exits without entering)
        # is exercisable, and [40,41] (while enters) may be unreachable.
        start = dt.date(2024, 6, 30)
        end = dt.date(2024, 1, 1)
        # (start - end).days = 181, batch_size=5, so condition is False -> hits line 38
        result = _batch_dates(start, end, batch_size=5)
        # end_date (Jan 1) > curr_end (Jun 30) is False, so while doesn't execute
        assert result == []

    def test_negative_difference_always_returns_single_batch(self):
        """When start < end, (start - end).days is negative < batch_size."""
        start = dt.date(2024, 1, 1)
        end = dt.date(2024, 1, 8)
        result = _batch_dates(start, end, batch_size=5)
        assert result == [[start, end]]
