"""
Tests for gs_quant/backtests/backtest_utils.py

Covers:
- make_list: None, str, non-iterable, iterable
- CalcType enum values
- CustomDuration: __hash__
- get_final_date: None duration, date/datetime duration, inst attribute,
  'next schedule' with and without attribute, CustomDuration, RelativeDate fallback, cache hit
- scale_trade: delegates to inst.scale
- map_ccy_name_to_ccy: str and CurrencyName enum, unknown currency
- interpolate_signal: builds date range and calls interpolate
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.backtests.backtest_utils import (
    CalcType,
    CustomDuration,
    get_final_date,
    make_list,
    map_ccy_name_to_ccy,
    scale_trade,
    interpolate_signal,
    final_date_cache,
)


class TestMakeList:
    def test_none(self):
        assert make_list(None) == []

    def test_string(self):
        assert make_list('hello') == ['hello']

    def test_empty_string(self):
        assert make_list('') == ['']

    def test_list(self):
        assert make_list([1, 2, 3]) == [1, 2, 3]

    def test_tuple(self):
        assert make_list((1, 2)) == [1, 2]

    def test_single_int(self):
        """Non-iterable should be wrapped in list."""
        assert make_list(42) == [42]

    def test_single_float(self):
        assert make_list(3.14) == [3.14]

    def test_generator(self):
        gen = (x for x in range(3))
        assert make_list(gen) == [0, 1, 2]

    def test_set(self):
        result = make_list({1})
        assert result == [1]


class TestCalcType:
    def test_simple(self):
        assert CalcType.simple.value == 'simple'

    def test_semi_path_dependent(self):
        assert CalcType.semi_path_dependent.value == 'semi_path_dependent'

    def test_path_dependent(self):
        assert CalcType.path_dependent.value == 'path_dependent'


class TestCustomDuration:
    def test_hash(self):
        fn = lambda *args: args[0]
        cd = CustomDuration(durations=('1m', '2m'), function=fn)
        h = hash(cd)
        assert isinstance(h, int)

    def test_hash_consistency(self):
        fn = lambda *args: args[0]
        cd1 = CustomDuration(durations=('1m',), function=fn)
        cd2 = CustomDuration(durations=('1m',), function=fn)
        assert hash(cd1) == hash(cd2)


class TestGetFinalDate:
    def setup_method(self):
        # Clear the cache before each test
        final_date_cache.clear()

    def test_none_duration(self):
        inst = MagicMock()
        result = get_final_date(inst, dt.date(2023, 1, 1), None)
        assert result == dt.date.max

    def test_none_duration_cached(self):
        inst = MagicMock()
        result1 = get_final_date(inst, dt.date(2023, 1, 1), None)
        result2 = get_final_date(inst, dt.date(2023, 1, 1), None)
        assert result1 == result2 == dt.date.max

    def test_date_duration(self):
        inst = MagicMock()
        target = dt.date(2024, 6, 15)
        result = get_final_date(inst, dt.date(2023, 1, 1), target)
        assert result == target

    def test_datetime_duration(self):
        inst = MagicMock()
        target = dt.datetime(2024, 6, 15, 12, 0, 0)
        result = get_final_date(inst, dt.date(2023, 1, 1), target)
        assert result == target

    def test_inst_attribute_duration(self):
        """If inst has an attribute matching str(duration), return that."""
        inst = MagicMock()
        inst.expiration_date = dt.date(2024, 12, 31)
        result = get_final_date(inst, dt.date(2023, 1, 1), 'expiration_date')
        assert result == dt.date(2024, 12, 31)

    def test_next_schedule_with_attribute(self):
        inst = MagicMock(spec=[])
        trigger_info = MagicMock()
        trigger_info.next_schedule = dt.date(2023, 7, 1)
        result = get_final_date(inst, dt.date(2023, 1, 1), 'next schedule', trigger_info=trigger_info)
        assert result == dt.date(2023, 7, 1)

    def test_next_schedule_with_none_value(self):
        """When next_schedule is None, return dt.date.max."""
        inst = MagicMock(spec=[])
        trigger_info = MagicMock()
        trigger_info.next_schedule = None
        result = get_final_date(inst, dt.date(2023, 1, 1), 'next schedule', trigger_info=trigger_info)
        assert result == dt.date.max

    def test_next_schedule_without_attribute_raises(self):
        inst = MagicMock(spec=[])
        trigger_info = MagicMock(spec=[])  # no 'next_schedule' attribute
        with pytest.raises(RuntimeError, match='Next schedule not supported by action'):
            get_final_date(inst, dt.date(2023, 1, 1), 'next schedule', trigger_info=trigger_info)

    def test_next_schedule_case_insensitive(self):
        inst = MagicMock(spec=[])
        trigger_info = MagicMock()
        trigger_info.next_schedule = dt.date(2023, 7, 1)
        result = get_final_date(inst, dt.date(2023, 1, 1), 'Next Schedule', trigger_info=trigger_info)
        assert result == dt.date(2023, 7, 1)

    def test_custom_duration(self):
        inst = MagicMock(spec=[])
        fn = MagicMock(return_value=dt.date(2024, 1, 1))
        cd = CustomDuration(durations=(None,), function=fn)
        result = get_final_date(inst, dt.date(2023, 1, 1), cd)
        assert result == dt.date(2024, 1, 1)
        fn.assert_called_once()

    @patch('gs_quant.backtests.backtest_utils.RelativeDate')
    def test_relative_date_fallback(self, mock_rel_date_cls):
        """String durations that are not inst attributes fall through to RelativeDate."""
        inst = MagicMock(spec=[])
        mock_rd = MagicMock()
        mock_rd.apply_rule.return_value = dt.date(2023, 4, 1)
        mock_rel_date_cls.return_value = mock_rd

        result = get_final_date(inst, dt.date(2023, 1, 1), '3m', holiday_calendar=('2023-03-31',))
        assert result == dt.date(2023, 4, 1)
        mock_rel_date_cls.assert_called_once_with('3m', dt.date(2023, 1, 1))
        mock_rd.apply_rule.assert_called_once_with(holiday_calendar=('2023-03-31',))

    def test_cache_hit(self):
        """Second call with same args should return cached value."""
        inst = MagicMock()
        d = dt.date(2023, 1, 1)
        result1 = get_final_date(inst, d, None)
        result2 = get_final_date(inst, d, None)
        assert result1 == result2


class TestScaleTrade:
    def test_scale_trade(self):
        inst = MagicMock()
        scaled_inst = MagicMock()
        inst.scale.return_value = scaled_inst
        result = scale_trade(inst, 2.5)
        assert result is scaled_inst
        inst.scale.assert_called_once_with(2.5)


class TestMapCcyNameToCcy:
    def test_string_known(self):
        assert map_ccy_name_to_ccy('United States Dollar') == 'USD'
        assert map_ccy_name_to_ccy('Euro') == 'EUR'
        assert map_ccy_name_to_ccy('Japanese Yen') == 'JPY'

    def test_string_unknown(self):
        assert map_ccy_name_to_ccy('Martian Dollar') is None

    def test_currency_name_enum(self):
        from gs_quant.common import CurrencyName
        assert map_ccy_name_to_ccy(CurrencyName.Pound_Sterling) == 'GBP'

    def test_currency_name_enum_actual(self):
        from gs_quant.common import CurrencyName
        result = map_ccy_name_to_ccy(CurrencyName.Australian_Dollar)
        assert result == 'AUD'

    def test_all_currencies_mapped(self):
        known = {
            'United States Dollar': 'USD',
            'Australian Dollar': 'AUD',
            'Canadian Dollar': 'CAD',
            'Swiss Franc': 'CHF',
            'Yuan Renminbi (Hong Kong)': 'CNH',
            'Czech Republic Koruna': 'CZK',
            'Euro': 'EUR',
            'Pound Sterling': 'GBP',
            'Japanese Yen': 'JPY',
            'South Korean Won': 'KRW',
            'Malasyan Ringgit': 'MYR',
            'Norwegian Krone': 'NOK',
            'New Zealand Dollar': 'NZD',
            'Polish Zloty': 'PLN',
            'Russian Rouble': 'RUB',
            'Swedish Krona': 'SEK',
            'South African Rand': 'ZAR',
            'Yuan Renminbi (Onshore)': 'CHY',
        }
        for name, code in known.items():
            assert map_ccy_name_to_ccy(name) == code


class TestInterpolateSignal:
    @patch('gs_quant.backtests.backtest_utils.interpolate')
    def test_basic_signal(self, mock_interpolate):
        signal = {
            dt.date(2023, 1, 1): 1.0,
            dt.date(2023, 1, 3): 3.0,
        }
        expected_dates = [
            dt.date(2023, 1, 1),
            dt.date(2023, 1, 2),
            dt.date(2023, 1, 3),
        ]
        mock_interpolate.return_value = pd.Series([1.0, 1.0, 3.0], index=expected_dates)
        result = interpolate_signal(signal)
        assert mock_interpolate.called
        call_args = mock_interpolate.call_args
        # First arg is a sorted pd.Series, second is all_dates
        assert list(call_args[0][1]) == expected_dates

    @patch('gs_quant.backtests.backtest_utils.interpolate')
    def test_single_date_signal(self, mock_interpolate):
        signal = {dt.date(2023, 1, 1): 5.0}
        expected_dates = [dt.date(2023, 1, 1)]
        mock_interpolate.return_value = pd.Series([5.0], index=expected_dates)
        result = interpolate_signal(signal)
        assert mock_interpolate.called
        call_args = mock_interpolate.call_args
        assert list(call_args[0][1]) == expected_dates
