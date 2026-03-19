"""
Branch coverage tests for gs_quant/datetime/date.py
"""
import datetime as dt
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from gs_quant.datetime.date import (
    is_business_day,
    business_day_offset,
    prev_business_date,
    business_day_count,
    date_range,
    today,
    has_feb_29,
    day_count_fraction,
    DayCountConvention,
    PaymentFrequency,
    location_to_tz_mapping,
)
from gs_quant.common import PricingLocation


class TestIsBusinessDay:
    @patch('gs_quant.datetime.date.GsCalendar')
    def test_single_date_true(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        with patch('gs_quant.datetime.date.np.is_busday', return_value=np.bool_(True)):
            result = is_business_day(dt.date(2021, 1, 4))
            assert result is np.bool_(True)

    @patch('gs_quant.datetime.date.GsCalendar')
    def test_iterable_dates(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        with patch('gs_quant.datetime.date.np.is_busday', return_value=np.array([True, False, True])):
            result = is_business_day([dt.date(2021, 1, 4), dt.date(2021, 1, 5), dt.date(2021, 1, 6)])
            assert isinstance(result, tuple)
            assert result == (True, False, True)

    @patch('gs_quant.datetime.date.GsCalendar')
    def test_with_calendars_and_week_mask(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        with patch('gs_quant.datetime.date.np.is_busday', return_value=np.bool_(True)):
            result = is_business_day(dt.date(2021, 1, 4), calendars=('NYSE',), week_mask='1111100')
            assert result is np.bool_(True)
            mock_cal_cls.get.assert_called_with(('NYSE',))
            mock_cal.business_day_calendar.assert_called_with('1111100')


class TestBusinessDayOffset:
    @patch('gs_quant.datetime.date.GsCalendar')
    def test_single_date(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        result_np = MagicMock()
        result_np.astype.return_value = dt.date(2021, 1, 5)

        with patch('gs_quant.datetime.date.np.busday_offset', return_value=result_np):
            result = business_day_offset(dt.date(2021, 1, 4), 1)
            assert result == dt.date(2021, 1, 5)

    @patch('gs_quant.datetime.date.GsCalendar')
    def test_iterable_dates(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        arr = np.array([dt.date(2021, 1, 5), dt.date(2021, 1, 6)])
        result_np = MagicMock()
        result_np.astype.return_value = arr

        with patch('gs_quant.datetime.date.np.busday_offset', return_value=result_np):
            result = business_day_offset([dt.date(2021, 1, 4), dt.date(2021, 1, 5)], 1)
            assert isinstance(result, tuple)


class TestPrevBusinessDate:
    @patch('gs_quant.datetime.date.business_day_offset')
    def test_prev_business_date(self, mock_offset):
        mock_offset.return_value = dt.date(2021, 1, 4)
        result = prev_business_date(dt.date(2021, 1, 5))
        mock_offset.assert_called_once_with(dt.date(2021, 1, 5), -1, roll='forward', calendars=(), week_mask=None)
        assert result == dt.date(2021, 1, 4)


class TestBusinessDayCount:
    @patch('gs_quant.datetime.date.GsCalendar')
    def test_single_count(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        with patch('gs_quant.datetime.date.np.busday_count', return_value=np.int64(5)):
            result = business_day_count(dt.date(2021, 1, 4), dt.date(2021, 1, 11))
            assert result == 5

    @patch('gs_quant.datetime.date.GsCalendar')
    def test_iterable_count(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_bdc = MagicMock()
        mock_cal.business_day_calendar.return_value = mock_bdc
        mock_cal_cls.get.return_value = mock_cal

        with patch('gs_quant.datetime.date.np.busday_count', return_value=np.array([5, 10])):
            result = business_day_count(
                [dt.date(2021, 1, 4), dt.date(2021, 1, 4)],
                [dt.date(2021, 1, 11), dt.date(2021, 1, 18)]
            )
            assert isinstance(result, tuple)
            assert result == (5, 10)


class TestDateRange:
    @patch('gs_quant.datetime.date.business_day_offset')
    def test_begin_date_end_date(self, mock_offset):
        """both begin and end are dates"""
        begin = dt.date(2021, 1, 4)
        end = dt.date(2021, 1, 6)
        # mock business_day_offset to advance day by day
        mock_offset.side_effect = lambda d, n, **kwargs: d + dt.timedelta(days=n)

        result = list(date_range(begin, end))
        assert begin in result
        assert end in result

    @patch('gs_quant.datetime.date.business_day_offset')
    def test_begin_date_end_date_begin_gt_end(self, mock_offset):
        """begin > end should raise ValueError"""
        begin = dt.date(2021, 1, 10)
        end = dt.date(2021, 1, 5)

        gen = date_range(begin, end)
        with pytest.raises(ValueError, match='begin must be <= end'):
            list(gen)

    @patch('gs_quant.datetime.date.business_day_offset')
    def test_begin_date_end_int(self, mock_offset):
        """begin is date, end is int"""
        begin = dt.date(2021, 1, 4)
        mock_offset.side_effect = lambda d, n, **kwargs: d + dt.timedelta(days=n)

        result = list(date_range(begin, 3))
        assert len(result) == 3

    def test_begin_date_end_invalid(self):
        with pytest.raises(ValueError, match='end must be a date or int'):
            list(date_range(dt.date(2021, 1, 4), 'invalid'))

    @patch('gs_quant.datetime.date.business_day_offset')
    def test_begin_int_end_date(self, mock_offset):
        """begin is int, end is date"""
        end = dt.date(2021, 1, 8)
        mock_offset.side_effect = lambda d, n, **kwargs: d + dt.timedelta(days=n)

        result = list(date_range(3, end))
        assert len(result) == 3

    def test_begin_int_end_not_date(self):
        with pytest.raises(ValueError, match='end must be a date if begin is an int'):
            list(date_range(3, 5))

    def test_begin_invalid_type(self):
        with pytest.raises(ValueError, match='begin must be a date or int'):
            list(date_range('invalid', dt.date(2021, 1, 4)))


class TestToday:
    def test_today_no_location(self):
        result = today()
        assert result == dt.date.today()

    def test_today_with_none_location(self):
        result = today(None)
        assert result == dt.date.today()

    def test_today_with_valid_location(self):
        result = today(PricingLocation.NYC)
        assert isinstance(result, dt.date)

    def test_today_with_unrecognized_location(self):
        # Create a mock PricingLocation value that's not in the mapping
        mock_location = MagicMock()
        mock_location.name = 'UNKNOWN'

        with patch.dict('gs_quant.datetime.date.location_to_tz_mapping', {mock_location: None}, clear=False):
            # The mock_location maps to None, so tz will be None
            pass

        # Use a PricingLocation that is NOT in the mapping
        # We need to test the ValueError branch
        with patch('gs_quant.datetime.date.location_to_tz_mapping', {}):
            with pytest.raises(ValueError, match='Unrecognized timezone'):
                today(PricingLocation.NYC)


class TestHasFeb29:
    def test_no_leap_year(self):
        assert not has_feb_29(dt.date(2019, 1, 1), dt.date(2019, 12, 31))

    def test_has_leap_day(self):
        assert has_feb_29(dt.date(2020, 1, 1), dt.date(2020, 12, 31))

    def test_start_exclusive(self):
        # Start is exclusive, so starting at Feb 29 itself shouldn't count
        assert not has_feb_29(dt.date(2020, 2, 29), dt.date(2020, 3, 31))

    def test_end_inclusive(self):
        assert has_feb_29(dt.date(2020, 1, 1), dt.date(2020, 2, 29))

    def test_no_feb_29_before_leap(self):
        assert not has_feb_29(dt.date(2020, 1, 1), dt.date(2020, 2, 28))

    def test_same_dates(self):
        """When start == end, there's no range so no feb 29"""
        assert not has_feb_29(dt.date(2020, 2, 29), dt.date(2020, 2, 29))


class TestDayCountFraction:
    def test_actual_360(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 1, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_360)
        assert result == pytest.approx(30 / 360)

    def test_actual_364(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 1, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_364)
        assert result == pytest.approx(30 / 364)

    def test_actual_365f(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 1, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365F)
        assert result == pytest.approx(30 / 365)

    def test_actual_365l_annually_with_feb_29(self):
        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 12, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365L, PaymentFrequency.ANNUALLY)
        assert result == pytest.approx(365 / 366)

    def test_actual_365l_annually_without_feb_29(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 12, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365L, PaymentFrequency.ANNUALLY)
        assert result == pytest.approx(364 / 365)

    def test_actual_365l_non_annually_leap_year(self):
        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 12, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365L, PaymentFrequency.MONTHLY)
        assert result == pytest.approx(365 / 366)

    def test_actual_365l_non_annually_non_leap_year(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 12, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365L, PaymentFrequency.MONTHLY)
        assert result == pytest.approx(364 / 365)

    def test_actual_365_25(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 1, 31)
        result = day_count_fraction(start, end, DayCountConvention.ACTUAL_365_25)
        assert result == pytest.approx(30 / 365.25)

    def test_one_one(self):
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 12, 31)
        result = day_count_fraction(start, end, DayCountConvention.ONE_ONE)
        assert result == 1

    def test_unknown_convention(self):
        """Test else branch with unknown convention"""
        # Create a mock convention that gets past all elif checks
        mock_conv = MagicMock()
        mock_conv.value = 'UNKNOWN'
        # Make it not equal to any known convention
        mock_conv.__eq__ = lambda self, other: False

        with pytest.raises(ValueError, match='Unknown day count convention'):
            day_count_fraction(dt.date(2021, 1, 1), dt.date(2021, 12, 31), mock_conv)


class TestPaymentFrequency:
    def test_values(self):
        assert PaymentFrequency.DAILY == 252
        assert PaymentFrequency.WEEKLY == 52
        assert PaymentFrequency.ANNUALLY == 1


class TestDayCountConvention:
    def test_values(self):
        assert DayCountConvention.ACTUAL_360.value == 'ACTUAL_360'
        assert DayCountConvention.ONE_ONE.value == 'ONE_ONE'
