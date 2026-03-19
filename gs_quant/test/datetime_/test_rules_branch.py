"""
Branch coverage tests for gs_quant/datetime/rules.py
"""
import datetime as dt
from unittest.mock import patch, MagicMock

import pytest

import gs_quant.datetime.rules as rules


def make_rule(rule_cls, result, **kwargs):
    """Helper to instantiate rule classes with common defaults."""
    defaults = {
        'results': result,
        'number': 0,
        'week_mask': '1111100',
        'currencies': None,
        'exchanges': None,
        'holiday_calendar': None,
        'usd_calendar': None,
        'roll': None,
        'sign': '+',
    }
    defaults.update(kwargs)
    return rule_cls(result, **defaults)


class TestRDateRule:
    def test_init(self):
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), number=1)
        assert r.result == dt.date(2021, 1, 15)
        assert r.number == 1
        assert r.week_mask == '1111100'
        assert r.currencies is None
        assert r.exchanges is None
        assert r.holiday_calendar is None
        assert r.usd_calendar is None
        assert r.roll is None
        assert r.sign == '+'

    def test_is_weekend_saturday(self):
        assert rules.RDateRule.is_weekend(dt.date(2021, 1, 2)) is True  # Saturday

    def test_is_weekend_sunday(self):
        assert rules.RDateRule.is_weekend(dt.date(2021, 1, 3)) is True  # Sunday

    def test_is_weekend_weekday(self):
        assert rules.RDateRule.is_weekend(dt.date(2021, 1, 4)) is False  # Monday

    def test_roll_convention_with_roll(self):
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), roll='forward')
        assert r.roll_convention() == 'forward'

    def test_roll_convention_no_roll_uses_default(self):
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), roll=None)
        assert r.roll_convention('backward') == 'backward'

    def test_roll_convention_no_roll_no_default(self):
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), roll=None)
        assert r.roll_convention() is None


class TestGetHolidays:
    def test_holiday_calendar_provided(self):
        holidays = [dt.date(2021, 1, 18)]
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), holiday_calendar=holidays)
        assert r._get_holidays() == holidays

    def test_holiday_calendar_with_usd_calendar(self):
        holidays = [dt.date(2021, 1, 18)]
        usd = [dt.date(2021, 7, 4)]
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), holiday_calendar=holidays, usd_calendar=usd)
        result = r._get_holidays()
        assert dt.date(2021, 1, 18) in result
        assert dt.date(2021, 7, 4) in result

    def test_holiday_calendar_no_usd_calendar(self):
        holidays = [dt.date(2021, 1, 18)]
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), holiday_calendar=holidays, usd_calendar=None)
        assert r._get_holidays() == holidays

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_no_holiday_calendar_currencies_string(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_cal.holidays = [dt.date(2021, 1, 18)]
        mock_cal_cls.return_value = mock_cal

        r = make_rule(rules.dRule, dt.date(2021, 1, 15), currencies='USD')
        result = r._get_holidays()
        assert result == [dt.date(2021, 1, 18)]
        mock_cal_cls.assert_called_once_with(['USD'])

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_no_holiday_calendar_currencies_list(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_cal.holidays = [dt.date(2021, 1, 18)]
        mock_cal_cls.return_value = mock_cal

        r = make_rule(rules.dRule, dt.date(2021, 1, 15), currencies=['USD', 'GBP'])
        result = r._get_holidays()
        assert result == [dt.date(2021, 1, 18)]
        mock_cal_cls.assert_called_once_with(['USD', 'GBP'])

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_no_holiday_calendar_exchanges_string(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_cal.holidays = []
        mock_cal_cls.return_value = mock_cal

        r = make_rule(rules.dRule, dt.date(2021, 1, 15), exchanges='NYSE')
        result = r._get_holidays()
        mock_cal_cls.assert_called_once_with(['NYSE'])

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_no_holiday_calendar_exchanges_list(self, mock_cal_cls):
        mock_cal = MagicMock()
        mock_cal.holidays = []
        mock_cal_cls.return_value = mock_cal

        r = make_rule(rules.dRule, dt.date(2021, 1, 15), exchanges=['NYSE', 'LSE'])
        result = r._get_holidays()
        mock_cal_cls.assert_called_once_with(['NYSE', 'LSE'])

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_no_holiday_calendar_none_none(self, mock_cal_cls):
        """No holiday_calendar, no currencies, no exchanges"""
        mock_cal = MagicMock()
        mock_cal.holidays = []
        mock_cal_cls.return_value = mock_cal

        r = make_rule(rules.dRule, dt.date(2021, 1, 15))
        result = r._get_holidays()
        mock_cal_cls.assert_called_once_with([])

    @patch('gs_quant.datetime.rules.GsCalendar')
    def test_get_holidays_exception(self, mock_cal_cls):
        """Test the exception branch when GsCalendar raises"""
        mock_cal_cls.side_effect = Exception('Calendar not found')
        r = make_rule(rules.dRule, dt.date(2021, 1, 15))
        result = r._get_holidays()
        assert result == []


class TestApplyBusinessDaysLogic:
    def test_with_explicit_offset(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 15), number=5)
        result = r._apply_business_days_logic([], offset=1)
        assert isinstance(result, dt.date)

    def test_with_none_offset_uses_number(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 15), number=1)
        result = r._apply_business_days_logic([])
        assert isinstance(result, dt.date)

    def test_with_none_offset_and_zero_number(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 15), number=0)
        result = r._apply_business_days_logic([], offset=None)
        assert isinstance(result, dt.date)

    def test_with_none_offset_and_none_number(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 15))
        r.number = None  # explicitly set to None
        result = r._apply_business_days_logic([], offset=None)
        assert isinstance(result, dt.date)


class TestGetNthDayOfMonth:
    def test_get_1st_friday(self):
        r = make_rule(rules.FRule, dt.date(2021, 1, 15), number=1)
        result = r._get_nth_day_of_month(4)  # 4 = Friday (calendar.FRIDAY)
        assert result == dt.date(2021, 1, 1)

    def test_get_3rd_monday(self):
        r = make_rule(rules.MRule, dt.date(2021, 1, 15), number=3)
        result = r._get_nth_day_of_month(0)  # 0 = Monday (calendar.MONDAY)
        assert result == dt.date(2021, 1, 18)


class TestAddYears:
    def test_add_years_result_is_weekday(self):
        """Test add_years when the resulting date is already a weekday (False branch)"""
        # 2021-01-19 + 1 year = 2022-01-19 (Wednesday, a weekday)
        r = make_rule(rules.kRule, dt.date(2021, 1, 19), number=1)
        result = r.add_years([])
        assert isinstance(result, dt.date)
        # Result should be 2022-01-19 (Wednesday)
        assert result == dt.date(2022, 1, 19)

    def test_add_years_result_is_weekend(self):
        """Test add_years when result falls on weekend (True branch)"""
        # 2021-01-15 + 1 year = 2022-01-15 (Saturday)
        r = make_rule(rules.kRule, dt.date(2021, 1, 15), number=1)
        result = r.add_years([])
        # 2022-01-15 is Saturday -> isoweekday=6 -> add 6%5=1 day -> Monday 2022-01-17
        assert result.weekday() < 5  # Should be a weekday


class TestSpecificRules:
    """Test individual rule handle methods"""

    def test_ARule(self):
        r = make_rule(rules.ARule, dt.date(2021, 6, 15), number=2022)
        result = r.handle()
        assert result == dt.date(2022, 1, 1)

    def test_bRule_negative(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 19), number=-1)
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_bRule_positive(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_bRule_zero(self):
        r = make_rule(rules.bRule, dt.date(2021, 1, 19), number=0)
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_dRule(self):
        r = make_rule(rules.dRule, dt.date(2021, 1, 15), number=5)
        result = r.handle()
        assert result == dt.date(2021, 1, 20)

    def test_eRule(self):
        r = make_rule(rules.eRule, dt.date(2021, 2, 15))
        result = r.handle()
        assert result == dt.date(2021, 2, 28)

    def test_FRule(self):
        r = make_rule(rules.FRule, dt.date(2021, 1, 15), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 1)

    def test_gRule(self):
        r = make_rule(rules.gRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_NRule(self):
        r = make_rule(rules.NRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 25)

    def test_GRule(self):
        r = make_rule(rules.GRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 22)

    def test_IRule(self):
        r = make_rule(rules.IRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 23)

    def test_JRule(self):
        r = make_rule(rules.JRule, dt.date(2021, 1, 19))
        result = r.handle()
        assert result == dt.date(2021, 1, 1)

    def test_kRule_weekday(self):
        r = make_rule(rules.kRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_kRule_weekend_result(self):
        """Test when adding years lands on a weekend"""
        # 2022-01-15 is a Saturday
        r = make_rule(rules.kRule, dt.date(2021, 1, 15), number=1, holiday_calendar=[])
        result = r.handle()
        assert result.weekday() < 5

    def test_mRule(self):
        r = make_rule(rules.mRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_MRule(self):
        r = make_rule(rules.MRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 4)

    def test_PRule(self):
        r = make_rule(rules.PRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 24)

    def test_rRule(self):
        r = make_rule(rules.rRule, dt.date(2021, 1, 19), number=0)
        result = r.handle()
        assert result == dt.date(2021, 12, 31)

    def test_rRule_with_number(self):
        r = make_rule(rules.rRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2022, 12, 31)

    def test_RRule(self):
        r = make_rule(rules.RRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 7)

    def test_SRule(self):
        r = make_rule(rules.SRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 21)

    def test_TRule(self):
        r = make_rule(rules.TRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 5)

    def test_uRule_positive(self):
        r = make_rule(rules.uRule, dt.date(2021, 1, 19), number=1, sign='+', holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_uRule_negative_zero(self):
        """sign='-' and number=0 should use 'preceding' roll"""
        r = make_rule(rules.uRule, dt.date(2025, 11, 30), number=0, sign='-', holiday_calendar=[])
        result = r.handle()
        assert result == dt.date(2025, 11, 28)

    def test_uRule_negative_nonzero(self):
        """sign='-' and number != 0 should use 'forward' (number <= 0)"""
        r = make_rule(rules.uRule, dt.date(2021, 1, 19), number=-2, sign='-', holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_uRule_zero_positive_sign(self):
        r = make_rule(rules.uRule, dt.date(2021, 1, 19), number=0, sign='+', holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_URule(self):
        r = make_rule(rules.URule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 19)

    def test_vRule_with_number(self):
        r = make_rule(rules.vRule, dt.date(2021, 1, 19), number=2, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_vRule_no_number(self):
        r = make_rule(rules.vRule, dt.date(2021, 1, 19), number=0, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_VRule(self):
        r = make_rule(rules.VRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 2)

    def test_WRule(self):
        r = make_rule(rules.WRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 6)

    def test_wRule_positive(self):
        r = make_rule(rules.wRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_wRule_negative(self):
        r = make_rule(rules.wRule, dt.date(2021, 1, 19), number=-1, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_wRule_zero(self):
        r = make_rule(rules.wRule, dt.date(2021, 1, 19), number=0, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_xRule(self):
        r = make_rule(rules.xRule, dt.date(2021, 1, 19), holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_XRule(self):
        r = make_rule(rules.XRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 20)

    def test_yRule_weekday(self):
        r = make_rule(rules.yRule, dt.date(2021, 1, 19), number=2, holiday_calendar=[])
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_yRule_weekend_result(self):
        """When adding years lands on a weekend"""
        # 2022-01-15 is Saturday
        r = make_rule(rules.yRule, dt.date(2021, 1, 15), number=1, holiday_calendar=[])
        result = r.handle()
        assert result.weekday() < 5

    def test_ZRule(self):
        r = make_rule(rules.ZRule, dt.date(2021, 1, 19), number=1)
        result = r.handle()
        assert result == dt.date(2021, 1, 3)

    def test_gRule_with_roll_convention(self):
        r = make_rule(rules.gRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_wRule_with_roll_convention(self):
        r = make_rule(rules.wRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_vRule_with_roll_convention(self):
        r = make_rule(rules.vRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_xRule_with_roll_convention(self):
        r = make_rule(rules.xRule, dt.date(2021, 1, 19), holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_kRule_with_roll_convention(self):
        r = make_rule(rules.kRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_yRule_with_roll_convention(self):
        r = make_rule(rules.yRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='forward')
        result = r.handle()
        assert isinstance(result, dt.date)

    def test_mRule_with_roll_convention(self):
        r = make_rule(rules.mRule, dt.date(2021, 1, 19), number=1, holiday_calendar=[], roll='backward')
        result = r.handle()
        assert isinstance(result, dt.date)
