"""
Branch-coverage tests for gs_quant/datetime/gscalendar.py

Covers missing branches:
- _split_list: Enum vs non-Enum items, predicate True/False paths
- GsCalendar.__init__: str/PricingLocation/Currency vs tuple calendars, None calendars
- GsCalendar.is_currency: Currency, PricingLocation, valid string, invalid string
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pytest

from gs_quant.common import PricingLocation, Currency
from gs_quant.datetime.gscalendar import GsCalendar, _split_list


# ---------------------------------------------------------------------------
# _split_list
# ---------------------------------------------------------------------------


class TestSplitList:
    def test_with_enum_items(self):
        """Enum items should use item.value for the result string."""
        items = [Currency.USD, Currency.EUR]
        true_res, false_res = _split_list(items, lambda x: GsCalendar.is_currency(x))
        assert 'USD' in true_res
        assert 'EUR' in true_res
        assert len(false_res) == 0

    def test_with_string_items(self):
        """Non-Enum items should use item.upper() for the result string."""
        items = ['usd', 'nyse']
        true_res, false_res = _split_list(items, lambda x: GsCalendar.is_currency(x))
        assert 'USD' in true_res
        assert 'NYSE' in false_res

    def test_with_mixed_items(self):
        """Mix of Enum and string items."""
        items = [Currency.USD, 'nyse']
        true_res, false_res = _split_list(items, lambda x: GsCalendar.is_currency(x))
        assert 'USD' in true_res
        assert 'NYSE' in false_res

    def test_with_pricing_location_enum(self):
        """PricingLocation is an Enum but is_currency returns False."""
        items = [PricingLocation.NYC]
        true_res, false_res = _split_list(items, lambda x: GsCalendar.is_currency(x))
        assert len(true_res) == 0
        assert PricingLocation.NYC.value in false_res

    def test_empty_list(self):
        true_res, false_res = _split_list([], lambda x: True)
        assert true_res == ()
        assert false_res == ()


# ---------------------------------------------------------------------------
# GsCalendar.__init__
# ---------------------------------------------------------------------------


class TestGsCalendarInit:
    def test_string_calendar(self):
        """Cover line 55-56: isinstance str -> wrap in tuple."""
        cal = GsCalendar('USD')
        assert cal.calendars() == ('USD',)

    def test_pricing_location_calendar(self):
        """Cover line 55-56: isinstance PricingLocation -> wrap in tuple."""
        cal = GsCalendar(PricingLocation.NYC)
        assert cal.calendars() == (PricingLocation.NYC,)

    def test_currency_calendar(self):
        """Cover line 55-56: isinstance Currency -> wrap in tuple."""
        cal = GsCalendar(Currency.USD)
        assert cal.calendars() == (Currency.USD,)

    def test_tuple_calendar(self):
        """Cover line 55 False branch: already a tuple."""
        cal = GsCalendar(('USD', 'EUR'))
        assert cal.calendars() == ('USD', 'EUR')

    def test_none_calendar(self):
        """Cover line 57-58: calendars is None -> empty tuple."""
        cal = GsCalendar(None)
        assert cal.calendars() == ()

    def test_default_calendar(self):
        """No argument -> empty tuple."""
        cal = GsCalendar()
        assert cal.calendars() == ()


# ---------------------------------------------------------------------------
# GsCalendar.is_currency
# ---------------------------------------------------------------------------


class TestIsCurrency:
    def test_currency_enum(self):
        """Cover line 76-77: isinstance Currency -> True."""
        assert GsCalendar.is_currency(Currency.USD) is True

    def test_pricing_location_enum(self):
        """Cover line 78-79: isinstance PricingLocation -> False."""
        assert GsCalendar.is_currency(PricingLocation.NYC) is False

    def test_valid_currency_string(self):
        """Cover line 80-82: string that IS a valid currency -> True."""
        assert GsCalendar.is_currency('USD') is True

    def test_invalid_currency_string(self):
        """Cover line 83-84: string that is NOT a valid currency -> False."""
        assert GsCalendar.is_currency('NOTACURR') is False
        assert GsCalendar.is_currency('NYSE') is False


# ---------------------------------------------------------------------------
# GsCalendar.get (static factory)
# ---------------------------------------------------------------------------


class TestGsCalendarGet:
    def test_get_with_string(self):
        cal = GsCalendar.get('USD')
        assert isinstance(cal, GsCalendar)
        assert cal.calendars() == ('USD',)

    def test_get_with_tuple(self):
        cal = GsCalendar.get(('USD', 'EUR'))
        assert isinstance(cal, GsCalendar)
        assert cal.calendars() == ('USD', 'EUR')


# ---------------------------------------------------------------------------
# GsCalendar.reset
# ---------------------------------------------------------------------------


class TestGsCalendarReset:
    def test_reset_clears_cache(self):
        GsCalendar.reset()  # Should not raise


# ---------------------------------------------------------------------------
# GsCalendar.holidays_from_dataset
# ---------------------------------------------------------------------------


class TestHolidaysFromDataset:
    def test_empty_query_values(self):
        """Cover line 93-94: no query values -> return []."""
        cal = GsCalendar()
        mock_dataset = MagicMock()
        result = cal.holidays_from_dataset(mock_dataset, 'exchange', ())
        assert result == []

    def test_valid_query_with_coverage(self):
        """Cover normal path: items in coverage, data returned."""
        cal = GsCalendar(skip_valid_check=True)
        mock_dataset = MagicMock()

        # Mock coverage
        mock_coverage_df = MagicMock()
        mock_coverage_df.empty = False
        mock_coverage_df.__getitem__ = MagicMock(return_value={'NYSE'})
        mock_dataset.get_coverage.return_value = mock_coverage_df

        # Mock data
        mock_data = MagicMock()
        mock_data.empty = False
        mock_index = MagicMock()
        mock_index.to_pydatetime.return_value = [dt.datetime(2024, 1, 1), dt.datetime(2024, 12, 25)]
        mock_data.index = mock_index
        mock_dataset.get_data.return_value = mock_data

        # Clear caches so our mock is used
        from gs_quant.datetime.gscalendar import _coverage_cache
        _coverage_cache.clear()

        result = cal.holidays_from_dataset(mock_dataset, 'exchange', ('NYSE',))
        assert len(result) == 2
        assert result[0] == dt.date(2024, 1, 1)

    def test_item_not_in_coverage_skip_valid(self):
        """Cover line 97-101: item not in coverage with skip_valid_check=True -> warn."""
        cal = GsCalendar(skip_valid_check=True)
        mock_dataset = MagicMock()

        mock_coverage_df = MagicMock()
        mock_coverage_df.empty = False
        mock_coverage_df.__getitem__ = MagicMock(return_value=set())
        mock_dataset.get_coverage.return_value = mock_coverage_df

        mock_data = MagicMock()
        mock_data.empty = True
        mock_dataset.get_data.return_value = mock_data

        from gs_quant.datetime.gscalendar import _coverage_cache
        _coverage_cache.clear()

        import logging
        with patch('gs_quant.datetime.gscalendar._logger') as mock_logger:
            result = cal.holidays_from_dataset(mock_dataset, 'exchange', ('INVALID',))
        assert result == []

    def test_item_not_in_coverage_strict(self):
        """Cover line 102-103: item not in coverage with skip_valid_check=False -> raise."""
        cal = GsCalendar(skip_valid_check=False)
        mock_dataset = MagicMock()

        mock_coverage_df = MagicMock()
        mock_coverage_df.empty = False
        mock_coverage_df.__getitem__ = MagicMock(return_value=set())
        mock_dataset.get_coverage.return_value = mock_coverage_df

        from gs_quant.datetime.gscalendar import _coverage_cache
        _coverage_cache.clear()

        with pytest.raises(ValueError, match='Invalid calendar'):
            cal.holidays_from_dataset(mock_dataset, 'exchange', ('INVALID',))

    def test_get_data_raises_mq_request_error(self):
        """Cover line 108-109: MqRequestError during get_data -> return []."""
        from gs_quant.errors import MqRequestError

        cal = GsCalendar(skip_valid_check=True)
        mock_dataset = MagicMock()

        mock_coverage_df = MagicMock()
        mock_coverage_df.empty = False
        mock_coverage_df.__getitem__ = MagicMock(return_value={'NYSE'})
        mock_dataset.get_coverage.return_value = mock_coverage_df

        mock_dataset.get_data.side_effect = MqRequestError(500, 'error')

        from gs_quant.datetime.gscalendar import _coverage_cache
        _coverage_cache.clear()

        result = cal.holidays_from_dataset(mock_dataset, 'exchange', ('NYSE',))
        assert result == []
