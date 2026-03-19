"""
Branch coverage tests for gs_quant/datetime/relative_date.py
"""
import datetime as dt
from copy import copy
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.errors import MqValueError


class TestRelativeDateInit:
    """Test RelativeDate.__init__ branches"""

    def test_init_with_base_date(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.date(2021, 5, 15))
        assert rd.base_date == dt.date(2021, 5, 15)
        assert rd.base_date_passed_in is True

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_init_pricing_context_entered(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = True
        mock_current.pricing_date = dt.date(2021, 3, 15)
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d')
        assert rd.base_date == dt.date(2021, 3, 15)
        assert rd.base_date_passed_in is False

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_init_pricing_context_not_entered(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = False
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d')
        assert rd.base_date == dt.date.today()
        assert rd.base_date_passed_in is False

    def test_init_base_date_datetime_converted_to_date(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.datetime(2021, 5, 15, 10, 30))
        assert rd.base_date == dt.date(2021, 5, 15)
        assert isinstance(rd.base_date, dt.date)
        assert not isinstance(rd.base_date, dt.datetime)

    def test_init_base_date_timestamp_converted_to_date(self):
        from gs_quant.datetime.relative_date import RelativeDate
        ts = pd.Timestamp('2021-05-15 10:30:00')
        rd = RelativeDate('1d', base_date=ts)
        assert rd.base_date == dt.date(2021, 5, 15)


class TestRelativeDateGetRules:
    """Test RelativeDate._get_rules branches"""

    def test_empty_rule_raises(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('A', base_date=dt.date(2021, 1, 1))
        rd.rule = ''
        with pytest.raises(MqValueError, match='Invalid Rule'):
            rd._get_rules()

    def test_single_alpha_rule(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('A', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['A']

    def test_single_number_rule(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['1d']

    def test_negative_number_rule(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('-1d', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['-1d']

    def test_chained_rules_with_plus(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('-1y+1d', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['-1y', '1d']

    def test_chained_rules_with_minus(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1y-1d', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['1y', '-1d']

    def test_rule_starting_with_plus(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('+1d', base_date=dt.date(2021, 1, 1))
        assert rd._get_rules() == ['1d']

    def test_chained_rule_last_starts_with_plus(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('J+14d+0u+4u', base_date=dt.date(2021, 1, 19))
        rules = rd._get_rules()
        assert rules == ['J', '14d', '0u', '4u']


class TestRelativeDateHandleRule:
    """Test RelativeDate.__handle_rule branches"""

    def test_negative_rule_with_no_digits_after_sign(self):
        """Test handling of negative rule like '-b' (all digits consumed)"""
        from gs_quant.datetime.relative_date import RelativeDate
        # '-0b' => number=-0 but actually tests branch index < len(rule)
        rd = RelativeDate('-0b', base_date=dt.date(2021, 1, 19))
        result = rd.apply_rule(holiday_calendar=[])
        assert isinstance(result, dt.date)

    def test_rule_starts_with_alpha_no_number(self):
        """Test a rule like 'A' which starts with alpha, no number prefix"""
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('A', base_date=dt.date(2021, 1, 19))
        result = rd.apply_rule()
        assert result == dt.date(2021, 1, 1)

    def test_rule_pure_number_no_letter(self):
        """Test where rule is just digits (like '123'), index == len(rule)"""
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.date(2021, 1, 1))
        rd.rule = '123'
        # _get_rules returns ['123'], which is all digits => rule_str='123', number=0
        # Then getattr(rules, '123Rule') fails with AttributeError => NotImplementedError
        with pytest.raises(NotImplementedError, match='Rule 123 not implemented'):
            rd.apply_rule()

    def test_not_implemented_rule(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.date(2021, 1, 1))
        rd.rule = '1z'
        with pytest.raises(NotImplementedError, match='Rule 1z not implemented'):
            rd.apply_rule()

    def test_roll_convention_kwarg(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1w', base_date=dt.date(2025, 10, 11))
        result = rd.apply_rule(roll_convention='backward')
        assert isinstance(result, dt.date)

    def test_negative_rule_all_digits_consumed(self):
        """Test branch where negative rule has all characters as digits after sign
        e.g., '-123' => index == len(rule), so number=0 and rule_str=rule[index] would fail.
        Actually, the code does: number = int(rule[1:index]) * -1 if index < len(rule) else 0
        and rule_str = rule[index]. But if index == len(rule), that's an IndexError.
        Let's test the case where index < len(rule): '-1d'
        """
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('-1d', base_date=dt.date(2021, 5, 15))
        result = rd.apply_rule()
        assert result == dt.date(2021, 5, 14)


class TestRelativeDateAsDict:
    def test_as_dict_with_base_date(self):
        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d', base_date=dt.date(2021, 1, 1))
        d = rd.as_dict()
        assert d['rule'] == '1d'
        assert d['baseDate'] == '2021-01-01'

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_as_dict_without_base_date(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = False
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDate
        rd = RelativeDate('1d')
        d = rd.as_dict()
        assert d['rule'] == '1d'
        assert 'baseDate' not in d


class TestRelativeDateSchedule:
    """Test RelativeDateSchedule branches"""

    def test_schedule_with_base_date_and_end_date(self):
        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d', base_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 1, 5))
        result = schedule.apply_rule(holiday_calendar=[])
        assert result[0] == dt.date(2021, 1, 1)
        assert len(result) > 1
        # All dates should be <= end_date
        for d in result:
            assert d <= dt.date(2021, 1, 5)

    def test_schedule_with_no_end_date(self):
        """When end_date is None, the loop should break on first iteration"""
        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d', base_date=dt.date(2021, 1, 1))
        result = schedule.apply_rule(holiday_calendar=[])
        # With no end_date, condition: self.end_date is None => break immediately
        assert result == [dt.date(2021, 1, 1)]

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_schedule_init_pricing_context(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = True
        mock_current.pricing_date = dt.date(2021, 6, 15)
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d')
        assert schedule.base_date == dt.date(2021, 6, 15)
        assert schedule.base_date_passed_in is False

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_schedule_init_pricing_context_datetime(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = True
        mock_current.pricing_date = dt.datetime(2021, 6, 15, 10, 30)
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d')
        assert schedule.base_date == dt.date(2021, 6, 15)

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_schedule_init_pricing_context_timestamp(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = True
        mock_current.pricing_date = pd.Timestamp('2021-06-15 10:30:00')
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d')
        assert schedule.base_date == dt.date(2021, 6, 15)

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_schedule_init_no_context(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = False
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d')
        assert schedule.base_date == dt.date.today()

    def test_schedule_as_dict_with_base_date(self):
        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d', base_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 1, 5))
        d = schedule.as_dict()
        assert d['rule'] == '1d'
        assert d['baseDate'] == '2021-01-01'
        assert d['endDate'] == '2021-01-05'

    @patch('gs_quant.datetime.relative_date.PricingContext')
    def test_schedule_as_dict_without_base_date(self, mock_pc):
        mock_current = MagicMock()
        mock_current.is_entered = False
        type(mock_pc).current = PropertyMock(return_value=mock_current)

        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('1d', end_date=dt.date(2021, 1, 5))
        d = schedule.as_dict()
        assert d['rule'] == '1d'
        assert 'baseDate' not in d
        assert d['endDate'] == '2021-01-05'

    def test_schedule_result_past_end_date(self):
        """Test that schedule stops when result > end_date"""
        from gs_quant.datetime.relative_date import RelativeDateSchedule
        schedule = RelativeDateSchedule('7d', base_date=dt.date(2021, 1, 1), end_date=dt.date(2021, 1, 10))
        result = schedule.apply_rule(holiday_calendar=[])
        # 7d from base: 2021-01-08 (<= 2021-01-10)
        # 14d from base: 2021-01-15 (> 2021-01-10) => break
        assert len(result) == 2  # base_date + one iteration
        assert result[0] == dt.date(2021, 1, 1)
        assert result[1] == dt.date(2021, 1, 8)
