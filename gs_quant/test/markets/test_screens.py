"""
Tests for gs_quant/markets/screens.py with comprehensive branch coverage.
"""

import datetime as dt
import sys
from enum import Enum
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Pre-import setup: two workarounds needed before importing screens.py
#
# 1. ScreenParameters does not exist in gs_quant.target.screens.
# 2. gs_quant.common.Currency is an Enum with members, and Python 3.12
#    disallows extending an enum that already has members.
# ---------------------------------------------------------------------------

# (1) Inject a fake ScreenParameters class
import gs_quant.target.screens as _target_screens_mod

_SCREEN_PARAM_PROPS = {
    'face_value', 'direction', 'gs_liquidity_score', 'gs_charge_bps',
    'gs_charge_dollars', 'modified_duration', 'yield_to_convention',
    'spread_to_benchmark', 'z_spread', 'g_spread', 'bval_mid_price',
    'maturity', 'amount_outstanding', 'rating_standard_and_poors',
    'seniority', 'currency', 'sector', 'issue_date',
}


class _FakeScreenParameters:
    """Stub that accepts **kwargs so TargetScreenParameters(**payload) works."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def properties():
        return _SCREEN_PARAM_PROPS


_target_screens_mod.ScreenParameters = _FakeScreenParameters

# (2) Replace gs_quant.common.Currency with a member-less Enum base class
#     so that class Currency(CurrencyImport, Enum) works in Python 3.12.
import gs_quant.common as _common_mod

_OrigCurrency = _common_mod.Currency


class _EmptyCurrencyBase(Enum):
    """Marker base with no members -- safe to extend."""
    pass


_common_mod.Currency = _EmptyCurrencyBase

# Now safe to import (only on first load; the module stays cached afterwards)
if 'gs_quant.markets.screens' in sys.modules:
    del sys.modules['gs_quant.markets.screens']

from gs_quant.markets.screens import (  # noqa: E402
    RangeFilter,
    CheckboxType,
    Sector,
    Seniority,
    Direction,
    Currency,
    CheckboxFilter,
    ScreenFilters,
    Screen,
)

# Restore original Currency so the rest of the gs_quant package is unaffected
_common_mod.Currency = _OrigCurrency

from gs_quant.errors import MqValueError  # noqa: E402
from gs_quant.target.common import (  # noqa: E402
    AssetScreenerCreditRequestFilters,
    AssetScreenerRequestFilterLimits,
    AssetScreenerRequestStringOptions,
)
from gs_quant.target.assets_screener import AssetScreenerRequest  # noqa: E402


# Since our Currency enum is empty, create a stand-in member for tests.
class _TestCurrency(Enum):
    USD = "USD"


# ---------------------------------------------------------------------------
# RangeFilter Tests
# ---------------------------------------------------------------------------


class TestRangeFilter:
    def test_init_defaults(self):
        rf = RangeFilter()
        assert rf.min is None
        assert rf.max is None

    def test_init_with_float_values(self):
        rf = RangeFilter(min_=1.5, max_=10.0)
        assert rf.min == 1.5
        assert rf.max == 10.0

    def test_init_with_string_values(self):
        rf = RangeFilter(min_='AAA', max_='BBB')
        assert rf.min == 'AAA'
        assert rf.max == 'BBB'

    def test_str_with_values(self):
        rf = RangeFilter(min_=1, max_=5)
        result = str(rf)
        assert 'Min: 1' in result
        assert 'Max: 5' in result

    def test_str_with_none_values(self):
        rf = RangeFilter()
        result = str(rf)
        assert 'Min: None' in result
        assert 'Max: None' in result

    def test_min_setter(self):
        rf = RangeFilter()
        rf.min = 3.0
        assert rf.min == 3.0

    def test_max_setter(self):
        rf = RangeFilter()
        rf.max = 7.0
        assert rf.max == 7.0


# ---------------------------------------------------------------------------
# Enum Tests
# ---------------------------------------------------------------------------


class TestEnums:
    def test_checkbox_type_values(self):
        assert CheckboxType.INCLUDE.value == "Include"
        assert CheckboxType.EXCLUDE.value == "Exclude"

    def test_sector_values(self):
        assert Sector.ENERGY.value == "Energy"
        assert Sector.FINANCIALS.value == "Financials"
        assert Sector.HEALTH_CARE.value == "Health Care"
        assert Sector.INDUSTRIALS.value == "Industrials"
        assert Sector.MATERIALS.value == "Materials"
        assert Sector.REAL_ESTATE.value == "Real Estate"
        assert Sector.UTILITIES.value == "Utilities"
        assert Sector.COMMUNICATION_SERVICES.value == "Communication Services"
        assert Sector.CONSUMER_DISCRETIONARY.value == "Consumer Discretionary"
        assert Sector.CONSUMER_STAPLES.value == "Consumer Staples"
        assert Sector.INFORMATION_TECHNOLOGY.value == "Information Technology"

    def test_seniority_values(self):
        assert Seniority.JUNIOR_SUBORDINATE.value == "Junior Subordinate"
        assert Seniority.SENIOR.value == "Senior"
        assert Seniority.SENIOR_SUBORDINATE.value == "Senior Subordinate"
        assert Seniority.SUBORDINATE.value == "Subordinate"

    def test_direction_values(self):
        assert Direction.BUY.value == "Buy"
        assert Direction.SELL.value == "Sell"

    def test_currency_is_enum(self):
        assert hasattr(Currency, '__members__')


# ---------------------------------------------------------------------------
# CheckboxFilter Tests
# ---------------------------------------------------------------------------


class TestCheckboxFilter:
    def test_init_defaults(self):
        cf = CheckboxFilter()
        assert cf.checkbox_type is None
        assert cf.selections is None

    def test_init_with_values(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Sector.ENERGY, Sector.FINANCIALS),
        )
        assert cf.checkbox_type == CheckboxType.INCLUDE
        assert Sector.ENERGY in cf.selections
        assert Sector.FINANCIALS in cf.selections

    def test_str(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.EXCLUDE,
            selections=(Seniority.SENIOR,),
        )
        result = str(cf)
        assert 'Type:' in result
        assert 'Selections:' in result

    def test_checkbox_type_setter(self):
        cf = CheckboxFilter()
        cf.checkbox_type = CheckboxType.INCLUDE
        assert cf.checkbox_type == CheckboxType.INCLUDE

    def test_selections_setter(self):
        cf = CheckboxFilter()
        cf.selections = (Sector.ENERGY,)
        assert cf.selections == (Sector.ENERGY,)

    def test_add_new_selections(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Sector.ENERGY,),
        )
        cf.add((Sector.FINANCIALS, Sector.MATERIALS))
        assert Sector.ENERGY in cf.selections
        assert Sector.FINANCIALS in cf.selections
        assert Sector.MATERIALS in cf.selections

    def test_add_duplicate_selections(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Sector.ENERGY, Sector.FINANCIALS),
        )
        cf.add((Sector.ENERGY,))
        count_energy = sum(1 for s in cf.selections if s == Sector.ENERGY)
        assert count_energy == 1

    def test_remove_existing(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Sector.ENERGY, Sector.FINANCIALS, Sector.MATERIALS),
        )
        cf.remove((Sector.FINANCIALS,))
        assert Sector.FINANCIALS not in cf.selections
        assert Sector.ENERGY in cf.selections
        assert Sector.MATERIALS in cf.selections

    def test_remove_nonexistent(self):
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Sector.ENERGY,),
        )
        cf.remove((Sector.FINANCIALS,))
        assert Sector.ENERGY in cf.selections
        assert len(cf.selections) == 1


# ---------------------------------------------------------------------------
# ScreenFilters Tests
# ---------------------------------------------------------------------------


class TestScreenFilters:
    def test_init_defaults(self):
        sf = ScreenFilters()
        assert sf.face_value == 1000000
        assert sf.direction == "Buy"
        assert isinstance(sf.liquidity_score, RangeFilter)
        assert isinstance(sf.gs_charge_bps, RangeFilter)
        assert isinstance(sf.gs_charge_dollars, RangeFilter)
        assert isinstance(sf.duration, RangeFilter)
        assert isinstance(sf.yield_, RangeFilter)
        assert isinstance(sf.spread, RangeFilter)
        assert isinstance(sf.z_spread, RangeFilter)
        assert isinstance(sf.g_spread, RangeFilter)
        assert isinstance(sf.mid_price, RangeFilter)
        assert isinstance(sf.maturity, RangeFilter)
        assert isinstance(sf.amount_outstanding, RangeFilter)
        assert isinstance(sf.rating, RangeFilter)
        assert isinstance(sf.seniority, CheckboxFilter)
        assert isinstance(sf.currency, CheckboxFilter)
        assert isinstance(sf.sector, CheckboxFilter)

    def test_init_custom_values(self):
        rf = RangeFilter(min_=1, max_=5)
        sf = ScreenFilters(face_value=500000, direction="Sell", liquidity_score=rf)
        assert sf.face_value == 500000
        assert sf.direction == "Sell"
        assert sf.liquidity_score.min == 1
        assert sf.liquidity_score.max == 5

    def test_str_includes_truthy_values(self):
        sf = ScreenFilters()
        result = str(sf)
        assert isinstance(result, str)
        assert '1000000' in result
        assert 'Buy' in result

    def test_face_value_setter(self):
        sf = ScreenFilters()
        sf.face_value = 2000000
        assert sf.face_value == 2000000

    def test_direction_setter(self):
        sf = ScreenFilters()
        sf.direction = "Sell"
        assert sf.direction == "Sell"

    def test_liquidity_score_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=1, max_=6)
        sf.liquidity_score = rf
        assert sf.liquidity_score == rf

    def test_gs_charge_bps_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=10)
        sf.gs_charge_bps = rf
        assert sf.gs_charge_bps == rf

    def test_gs_charge_dollars_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=2)
        sf.gs_charge_dollars = rf
        assert sf.gs_charge_dollars == rf

    def test_duration_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=20)
        sf.duration = rf
        assert sf.duration == rf

    def test_yield_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=10)
        sf.yield_ = rf
        assert sf.yield_ == rf

    def test_spread_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=1000)
        sf.spread = rf
        assert sf.spread == rf

    def test_z_spread_setter(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=100)
        sf.z_spread = rf
        assert sf.z_spread == rf

    def test_g_spread_setter(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=100)
        sf.g_spread = rf
        assert sf.g_spread == rf

    def test_mid_price_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=200)
        sf.mid_price = rf
        assert sf.mid_price == rf

    def test_maturity_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=40)
        sf.maturity = rf
        assert sf.maturity == rf

    def test_amount_outstanding_setter_valid(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_=0, max_=1000000000)
        sf.amount_outstanding = rf
        assert sf.amount_outstanding == rf

    def test_rating_setter(self):
        sf = ScreenFilters()
        rf = RangeFilter(min_='AAA', max_='BBB')
        sf.rating = rf
        assert sf.rating == rf

    def test_seniority_setter(self):
        sf = ScreenFilters()
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.INCLUDE,
            selections=(Seniority.SENIOR,),
        )
        sf.seniority = cf
        assert sf.seniority == cf

    def test_currency_setter(self):
        sf = ScreenFilters()
        cf = CheckboxFilter()
        sf.currency = cf
        assert sf.currency == cf

    def test_sector_setter(self):
        sf = ScreenFilters()
        cf = CheckboxFilter(
            checkbox_type=CheckboxType.EXCLUDE,
            selections=(Sector.ENERGY,),
        )
        sf.sector = cf
        assert sf.sector == cf

    # --- Validation: __validate_range_settings branches ---

    def test_validate_range_both_none_returns(self):
        """Branch: min is None and max is None -> return early."""
        sf = ScreenFilters()
        rf_new = RangeFilter(min_=2, max_=5)
        sf.liquidity_score = rf_new  # no error raised

    def test_validate_range_min_below_minimum_raises(self):
        """Branch: min < allowed minimum -> MqValueError."""
        sf = ScreenFilters(liquidity_score=RangeFilter(min_=0, max_=5))
        with pytest.raises(MqValueError):
            sf.liquidity_score = RangeFilter(min_=2, max_=5)

    def test_validate_range_max_above_maximum_raises(self):
        """Branch: max > allowed maximum -> MqValueError."""
        sf = ScreenFilters(liquidity_score=RangeFilter(min_=1, max_=100))
        with pytest.raises(MqValueError):
            sf.liquidity_score = RangeFilter(min_=2, max_=5)

    def test_validate_gs_charge_bps_out_of_range(self):
        sf = ScreenFilters(gs_charge_bps=RangeFilter(min_=-1, max_=5))
        with pytest.raises(MqValueError):
            sf.gs_charge_bps = RangeFilter(min_=0, max_=5)

    def test_validate_gs_charge_dollars_out_of_range(self):
        sf = ScreenFilters(gs_charge_dollars=RangeFilter(min_=-1, max_=1))
        with pytest.raises(MqValueError):
            sf.gs_charge_dollars = RangeFilter(min_=0, max_=1)

    def test_validate_duration_out_of_range(self):
        sf = ScreenFilters(duration=RangeFilter(min_=-1, max_=10))
        with pytest.raises(MqValueError):
            sf.duration = RangeFilter(min_=0, max_=10)

    def test_validate_yield_out_of_range(self):
        sf = ScreenFilters(yield_=RangeFilter(min_=-1, max_=5))
        with pytest.raises(MqValueError):
            sf.yield_ = RangeFilter(min_=0, max_=5)

    def test_validate_spread_out_of_range(self):
        sf = ScreenFilters(spread=RangeFilter(min_=-1, max_=500))
        with pytest.raises(MqValueError):
            sf.spread = RangeFilter(min_=0, max_=500)

    def test_validate_mid_price_out_of_range(self):
        sf = ScreenFilters(mid_price=RangeFilter(min_=-1, max_=100))
        with pytest.raises(MqValueError):
            sf.mid_price = RangeFilter(min_=0, max_=100)

    def test_validate_maturity_out_of_range(self):
        sf = ScreenFilters(maturity=RangeFilter(min_=-1, max_=20))
        with pytest.raises(MqValueError):
            sf.maturity = RangeFilter(min_=0, max_=20)

    def test_validate_amount_outstanding_out_of_range(self):
        sf = ScreenFilters(amount_outstanding=RangeFilter(min_=-1, max_=500000000))
        with pytest.raises(MqValueError):
            sf.amount_outstanding = RangeFilter(min_=0, max_=500000000)

    def test_validate_range_max_only_over(self):
        """Branch: min OK but max > allowed max -> error via the or condition."""
        sf = ScreenFilters(liquidity_score=RangeFilter(min_=1, max_=7))
        with pytest.raises(MqValueError):
            sf.liquidity_score = RangeFilter()

    def test_validate_range_both_within_range(self):
        """Branch 361->exit: min >= allowed min AND max <= allowed max -> no error."""
        sf = ScreenFilters(liquidity_score=RangeFilter(min_=2, max_=5))
        # Old value (min=2, max=5) is within [1, 6], so validation passes
        rf_new = RangeFilter(min_=3, max_=4)
        sf.liquidity_score = rf_new
        assert sf.liquidity_score == rf_new

    def test_str_with_falsy_value(self):
        """Branch 188->187: some attribute is falsy, should be skipped in __str__."""
        sf = ScreenFilters(face_value=0, direction="Buy")
        result = str(sf)
        # face_value=0 is falsy, so it should not appear in the output dict
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Screen Tests
# ---------------------------------------------------------------------------


def _mock_save_deps():
    """Returns stacked decorators for patching both GsScreenApi and TargetScreen."""
    return [
        patch('gs_quant.markets.screens.GsScreenApi'),
        patch('gs_quant.markets.screens.TargetScreen'),
    ]


class TestScreen:
    def test_init_no_filters(self):
        """Branch: filters is None (falsy) -> create default ScreenFilters."""
        screen = Screen()
        assert isinstance(screen.filters, ScreenFilters)
        assert screen.id is None
        today_str = dt.date.today().strftime('%d-%b-%Y')
        assert today_str in screen.name

    def test_init_with_filters(self):
        """Branch: filters is truthy -> use provided filters."""
        sf = ScreenFilters(face_value=500000)
        screen = Screen(filters=sf)
        assert screen.filters is sf
        assert screen.filters.face_value == 500000

    def test_init_with_name(self):
        """Branch: name is not None -> use provided name."""
        screen = Screen(name="My Screen")
        assert screen.name == "My Screen"

    def test_init_with_name_none(self):
        """Branch: name is None -> use default date-based name."""
        screen = Screen(name=None)
        today_str = dt.date.today().strftime('%d-%b-%Y')
        assert today_str in screen.name

    def test_init_with_screen_id(self):
        screen = Screen(screen_id='abc123')
        assert screen.id == 'abc123'

    def test_name_setter(self):
        screen = Screen()
        screen.name = "New Name"
        assert screen.name == "New Name"

    def test_filters_setter(self):
        screen = Screen()
        new_filters = ScreenFilters(face_value=999)
        screen.filters = new_filters
        assert screen.filters.face_value == 999

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_get(self, mock_api):
        mock_target = MagicMock()
        mock_target.id = 'test_id'
        mock_target.name = 'Test Screen'
        mock_target.parameters = ScreenFilters()
        mock_api.get_screen.return_value = mock_target

        screen = Screen.get('test_id')
        mock_api.get_screen.assert_called_once_with(screen_id='test_id')
        assert screen.id == 'test_id'
        assert screen.name == 'Test Screen'

    # --- calculate branches ---

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_calculate_default_format(self, mock_api):
        """Branch: format_ is None -> return DataFrame."""
        mock_api.calculate.return_value = {'results': [{'a': 1}], 'total': 1}
        screen = Screen()
        result = screen.calculate()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.screens.pd.DataFrame')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_calculate_json_format(self, mock_api, mock_df_cls):
        """Branch: format_ == 'json' -> return JSON string."""
        mock_api.calculate.return_value = {'results': [{'a': 1}], 'total': 1}
        mock_df = MagicMock()
        mock_df.__getitem__ = MagicMock(return_value=MagicMock(
            to_json=MagicMock(return_value='{"0":{"a":1}}')
        ))
        mock_df_cls.return_value = mock_df
        screen = Screen()
        result = screen.calculate(format_='json')
        assert isinstance(result, str)

    @patch('gs_quant.markets.screens.pd.DataFrame')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_calculate_csv_format(self, mock_api, mock_df_cls):
        """Branch: format_ == 'csv' -> return CSV string."""
        mock_api.calculate.return_value = {'results': [{'a': 1}], 'total': 1}
        mock_df = MagicMock()
        mock_df.to_csv.return_value = ',results\n0,a\n'
        mock_df_cls.return_value = mock_df
        screen = Screen()
        result = screen.calculate(format_='csv')
        assert isinstance(result, str)

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_calculate_unknown_format(self, mock_api):
        """Branch: format_ is not 'json' or 'csv' -> return DataFrame."""
        mock_api.calculate.return_value = {'results': [{'a': 1}], 'total': 1}
        screen = Screen()
        result = screen.calculate(format_='xml')
        assert isinstance(result, pd.DataFrame)

    # --- save branches ---

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_save_new_screen(self, mock_api, mock_ts):
        """Branch: self.id is None -> create_screen."""
        mock_created = MagicMock()
        mock_created.id = 'new_id'
        mock_api.create_screen.return_value = mock_created

        screen = Screen()
        assert screen.id is None
        screen.save()
        mock_api.create_screen.assert_called_once()
        assert screen.id == 'new_id'

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_save_existing_screen(self, mock_api, mock_ts):
        """Branch: self.id is truthy -> update_screen."""
        screen = Screen(screen_id='existing_id')
        screen.save()
        mock_api.update_screen.assert_called_once()
        mock_api.create_screen.assert_not_called()
        # Verify target_screen.id was set
        ts_instance = mock_ts.return_value
        assert ts_instance.id == 'existing_id'

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_delete(self, mock_api):
        screen = Screen(screen_id='del_id')
        screen.delete()
        mock_api.delete_screen.assert_called_once_with('del_id')

    # --- __to_target_filters branches (via calculate) ---

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_face_value_direction(self, mock_api):
        """Branch: name == 'face_value' or name == 'direction' -> pass through."""
        sf = ScreenFilters(face_value=2000000, direction="Sell")
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        screen.calculate()
        mock_api.calculate.assert_called_once()

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_range_filter(self, mock_api):
        """Branch: isinstance(filters[name], RangeFilter) -> create FilterLimits."""
        sf = ScreenFilters(
            liquidity_score=RangeFilter(min_=1, max_=5),
            gs_charge_bps=RangeFilter(min_=0, max_=8),
        )
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        result = screen.calculate()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_checkbox_with_both(self, mock_api):
        """Branch: CheckboxFilter with both selections and type -> include."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(Seniority.SENIOR,),
            ),
            sector=CheckboxFilter(
                checkbox_type=CheckboxType.EXCLUDE,
                selections=(Sector.ENERGY,),
            ),
        )
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        screen.calculate()
        mock_api.calculate.assert_called_once()

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_checkbox_no_selections(self, mock_api):
        """Branch: CheckboxFilter with selections=None -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=None,
            ),
        )
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        screen.calculate()
        mock_api.calculate.assert_called_once()

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_checkbox_no_type(self, mock_api):
        """Branch: CheckboxFilter with checkbox_type=None -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=None,
                selections=(Seniority.SENIOR,),
            ),
        )
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        screen.calculate()
        mock_api.calculate.assert_called_once()

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_filters_checkbox_empty_selections(self, mock_api):
        """Branch: CheckboxFilter with empty selections tuple -> falsy -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(),
            ),
        )
        mock_api.calculate.return_value = {'results': [], 'total': 0}
        screen = Screen(filters=sf)
        screen.calculate()
        mock_api.calculate.assert_called_once()

    # --- __to_target_parameters branches (via save) ---

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_face_value_direction(self, mock_api, mock_ts):
        """Branch: name == 'face_value' or name == 'direction' -> pass through."""
        sf = ScreenFilters(face_value=2000000, direction="Sell")
        mock_created = MagicMock()
        mock_created.id = 'id1'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_range_filter(self, mock_api, mock_ts):
        """Branch: isinstance RangeFilter -> create FilterLimits."""
        sf = ScreenFilters(
            liquidity_score=RangeFilter(min_=1, max_=5),
        )
        mock_created = MagicMock()
        mock_created.id = 'id2'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_checkbox_with_both(self, mock_api, mock_ts):
        """Branch: CheckboxFilter with selections AND type -> include."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(Seniority.SENIOR,),
            ),
        )
        mock_created = MagicMock()
        mock_created.id = 'id3'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_checkbox_no_selections(self, mock_api, mock_ts):
        """Branch: CheckboxFilter with selections=None -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=None,
            ),
        )
        mock_created = MagicMock()
        mock_created.id = 'id4'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_checkbox_no_type(self, mock_api, mock_ts):
        """Branch: CheckboxFilter with checkbox_type=None -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=None,
                selections=(Seniority.SENIOR,),
            ),
        )
        mock_created = MagicMock()
        mock_created.id = 'id5'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_to_target_parameters_checkbox_empty_selections(self, mock_api, mock_ts):
        """Branch: CheckboxFilter with empty tuple -> falsy -> skip."""
        sf = ScreenFilters(
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(),
            ),
        )
        mock_created = MagicMock()
        mock_created.id = 'id6'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf)
        screen.save()
        mock_api.create_screen.assert_called_once()

    # --- Full integration tests ---

    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_calculate_all_filter_types(self, mock_api):
        """Exercise all branches in __to_target_filters together."""
        sf = ScreenFilters(
            face_value=1000000,
            direction="Buy",
            liquidity_score=RangeFilter(min_=1, max_=5),
            gs_charge_bps=RangeFilter(min_=0, max_=8),
            gs_charge_dollars=RangeFilter(min_=0, max_=1.5),
            duration=RangeFilter(min_=0, max_=15),
            yield_=RangeFilter(min_=0, max_=8),
            spread=RangeFilter(min_=0, max_=500),
            z_spread=RangeFilter(min_=0, max_=300),
            g_spread=RangeFilter(min_=0, max_=200),
            mid_price=RangeFilter(min_=50, max_=150),
            maturity=RangeFilter(min_=0, max_=30),
            amount_outstanding=RangeFilter(min_=0, max_=500000000),
            letter_rating=RangeFilter(min_='AAA', max_='BBB'),
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(Seniority.SENIOR, Seniority.SUBORDINATE),
            ),
            currency=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(_TestCurrency.USD,),
            ),
            sector=CheckboxFilter(
                checkbox_type=CheckboxType.EXCLUDE,
                selections=(Sector.ENERGY, Sector.FINANCIALS),
            ),
        )
        mock_api.calculate.return_value = {'results': [{'bond': 'A'}], 'total': 1}
        screen = Screen(filters=sf)
        result = screen.calculate()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.screens.TargetScreen')
    @patch('gs_quant.markets.screens.GsScreenApi')
    def test_save_all_filter_types(self, mock_api, mock_ts):
        """Exercise all branches in __to_target_parameters together."""
        sf = ScreenFilters(
            face_value=1000000,
            direction="Buy",
            liquidity_score=RangeFilter(min_=1, max_=5),
            gs_charge_bps=RangeFilter(min_=0, max_=8),
            gs_charge_dollars=RangeFilter(min_=0, max_=1.5),
            duration=RangeFilter(min_=0, max_=15),
            yield_=RangeFilter(min_=0, max_=8),
            spread=RangeFilter(min_=0, max_=500),
            z_spread=RangeFilter(min_=0, max_=300),
            g_spread=RangeFilter(min_=0, max_=200),
            mid_price=RangeFilter(min_=50, max_=150),
            maturity=RangeFilter(min_=0, max_=30),
            amount_outstanding=RangeFilter(min_=0, max_=500000000),
            letter_rating=RangeFilter(min_=1, max_=10),
            seniority=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(Seniority.SENIOR,),
            ),
            currency=CheckboxFilter(
                checkbox_type=CheckboxType.INCLUDE,
                selections=(_TestCurrency.USD,),
            ),
            sector=CheckboxFilter(
                checkbox_type=CheckboxType.EXCLUDE,
                selections=(Sector.ENERGY,),
            ),
        )
        mock_created = MagicMock()
        mock_created.id = 'full_id'
        mock_api.create_screen.return_value = mock_created
        screen = Screen(filters=sf, name="Full Test")
        screen.save()
        mock_api.create_screen.assert_called_once()
        assert screen.id == 'full_id'
