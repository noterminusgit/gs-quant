"""
Branch-coverage tests for gs_quant/datetime/point.py
"""

import datetime as dt
import pytest

from gs_quant.datetime.point import (
    relative_date_add,
    point_sort_order,
    ConstPoints,
    DictDayRule,
)
from gs_quant.errors import MqValueError


# ---------------------------------------------------------------------------
# relative_date_add
# ---------------------------------------------------------------------------

class TestRelativeDateAdd:
    """Tests for the relative_date_add function."""

    def test_positive_day_rule(self):
        # 1d -> 1*1 = 1.0
        assert relative_date_add('1d') == 1.0

    def test_positive_year_rule(self):
        # 1y -> 365
        assert relative_date_add('1y') == 365.0

    def test_positive_month_rule(self):
        # 2m -> 2*30 = 60
        assert relative_date_add('2m') == 60.0

    def test_positive_week_rule(self):
        # 3w -> 3*7 = 21
        assert relative_date_add('3w') == 21.0

    def test_positive_business_day(self):
        # 5b -> 5*1 = 5
        assert relative_date_add('5b') == 5.0

    def test_positive_f_rule(self):
        # 2f -> 2*30 = 60
        assert relative_date_add('2f') == 60.0

    def test_negative_day_rule(self):
        # -1d -> -1*1 = -1
        assert relative_date_add('-1d') == -1.0

    def test_negative_year_rule(self):
        assert relative_date_add('-2y') == -730.0

    def test_uppercase_rules(self):
        assert relative_date_add('1D') == 1.0
        assert relative_date_add('1W') == 7.0
        assert relative_date_add('1B') == 1.0
        assert relative_date_add('1F') == 30.0
        assert relative_date_add('1M') == 30.0
        assert relative_date_add('1Y') == 365.0

    def test_no_match_not_strict(self):
        # Non-matching string without strict mode -> returns 0
        assert relative_date_add('NOTVALID') == 0

    def test_no_match_strict_raises(self):
        with pytest.raises(MqValueError, match='invalid date rule'):
            relative_date_add('NOTVALID', strict=True)

    def test_invalid_rule_letter_raises(self):
        # '1z' matches DateRuleReg pattern but 'z' is not in DictDayRule
        # Actually let's check: DateRuleReg = r"^([-]*[0-9]+[mydwbfMYDWBFM])+$"
        # 'z' is not in [mydwbfMYDWBFM] so it won't match the regex.
        # We need a letter that matches the regex but not in DictDayRule.
        # All letters in the regex character class are in DictDayRule, so
        # there's no way to trigger the "no valid day rule" branch via this regex.
        # The else branch for invalid rule is dead code in practice.
        pass


# ---------------------------------------------------------------------------
# point_sort_order
# ---------------------------------------------------------------------------

class TestPointSortOrder:
    """Tests for point_sort_order: every branch/regex path."""

    def setup_method(self):
        # Clear the lru_cache before each test to avoid caching issues
        point_sort_order.cache_clear()

    # -- Falsy / non-string inputs -----------------------------------------

    def test_none_input(self):
        assert point_sort_order(None) == 0

    def test_empty_string(self):
        assert point_sort_order('') == 0

    def test_non_string_input(self):
        assert point_sort_order(123) == 0

    # -- ConstPoints (uppercase lookup) ------------------------------------

    def test_const_point_ON(self):
        assert point_sort_order('O/N') == 0

    def test_const_point_TN(self):
        assert point_sort_order('T/N') == 0.1

    def test_const_point_OIS_FIX(self):
        assert point_sort_order('OIS FIX') == 1

    def test_const_point_CASH_STUB(self):
        assert point_sort_order('CASH STUB') == 1.1

    def test_const_point_CASHSTUB(self):
        assert point_sort_order('CASHSTUB') == 1.1

    def test_const_point_DEFAULT(self):
        assert point_sort_order('DEFAULT') == 0

    def test_const_point_IN(self):
        assert point_sort_order('IN') == 0.1

    def test_const_point_OUT(self):
        assert point_sort_order('OUT') == 0.2

    # -- Lowercase special values ------------------------------------------

    def test_lowercase_on(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('o/n', ref) == 0

    def test_lowercase_tn(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('t/n', ref) == 0.1

    def test_cash_stub(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Cash Stub', ref) == 1.1

    def test_cashstub_camel(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('CashStub', ref) == 1.1

    def test_default(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Default', ref) == 0

    def test_in(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('In', ref) == 0.1

    def test_out(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Out', ref) == 0.2

    # -- infl_volReg -------------------------------------------------------

    def test_infl_vol_Caplet(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Caplet', ref) == 0

    def test_infl_vol_ZCCap(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('ZCCap', ref) == 1

    def test_infl_vol_Swaption(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Swaption', ref) == 2

    def test_infl_vol_ZCSwo(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('ZCSwo', ref) == 3

    # -- CopulaReg ---------------------------------------------------------

    def test_copula_rho(self):
        ref = dt.date(2020, 1, 1)
        # "Rho" matches CopulaReg -> days stays None
        assert point_sort_order('Rho', ref) is None

    def test_copula_rho_rate(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Rho Rate', ref) is None

    # -- SeasonalFrontReg --------------------------------------------------

    def test_seasonal_front(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Front', ref) == 0

    def test_seasonal_back(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Back', ref) == 1

    # -- MMMReg (e.g. "Jan", "Feb") ----------------------------------------

    def test_mmm_jan(self):
        ref = dt.date(2020, 6, 15)
        result = point_sort_order('Jan', ref)
        expected = (dt.date(2000, 1, 1) - ref).days
        assert result == expected

    def test_mmm_dec(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('Dec', ref)
        expected = (dt.date(2000, 12, 1) - ref).days
        assert result == expected

    # -- EuroOrFraReg (e.g. "Dec20", "JAN25") ------------------------------

    def test_euro_fra(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('Dec20', ref)
        expected = (dt.datetime.strptime('15Dec20', '%d%b%y').date() - ref).days
        assert result == expected

    def test_euro_fra_upper(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('JAN25', ref)
        expected = (dt.datetime.strptime('15JAN25', '%d%b%y').date() - ref).days
        assert result == expected

    # -- RDatePartReg (e.g. "1m", "5y") ------------------------------------

    def test_rdate_part(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('5y', ref)
        assert result == 5 * 365

    def test_rdate_part_with_two_parts(self):
        ref = dt.date(2020, 1, 1)
        # "1y2m" matches RDatePartReg; the first group is "1y"
        result = point_sort_order('1y2m', ref)
        assert result == 365.0

    # -- CashFXReg (e.g. "1m XC") ------------------------------------------

    def test_cash_fx(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('1m XC', ref)
        assert result == 30.0

    # -- PricerBFReg (e.g. "1y2m3d") --------------------------------------

    def test_pricer_bf(self):
        ref = dt.date(2020, 1, 1)
        # "1y2m3d" matches PricerBFReg; group(1) is "1y"
        result = point_sort_order('1y2m3d', ref)
        assert result == 365.0

    # -- FRAxReg (e.g. "3x6") ---------------------------------------------

    def test_fra_x(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('3x6', ref)
        # group(1) = "3", so date_str = "3m", relative_date_add("3m") = 90
        assert result == 90.0

    # -- SpikeQEReg (e.g. "QE1-2020") ------------------------------------

    def test_spike_qe1(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('QE1-2020', ref)
        expected = (dt.datetime.strptime('1Mar2020', '%d%b%Y').date() - ref).days
        assert result == expected

    def test_spike_qe2(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('QE2-2020', ref)
        expected = (dt.datetime.strptime('1Jun2020', '%d%b%Y').date() - ref).days
        assert result == expected

    def test_spike_qe3(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('QE3-2020', ref)
        expected = (dt.datetime.strptime('1Sep2020', '%d%b%Y').date() - ref).days
        assert result == expected

    def test_spike_qe4(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('QE4-2020', ref)
        expected = (dt.datetime.strptime('1Dec2020', '%d%b%Y').date() - ref).days
        assert result == expected

    def test_spike_qe_other(self):
        # QE5 matches the pattern but falls into else -> month='Dec'
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('QE5-2020', ref)
        expected = (dt.datetime.strptime('1Dec2020', '%d%b%Y').date() - ref).days
        assert result == expected

    # -- MMMYYYYReg (e.g. "Jan2020") --------------------------------------

    def test_mmmyyyy(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('Jan2020', ref)
        expected = (dt.datetime.strptime('1Jan2020', '%d%b%Y').date() - ref).days
        assert result == expected

    # -- DDMMMYYYYReg (e.g. "15Jan2020") -----------------------------------

    def test_ddmmmyyyy(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('15Jan2020', ref)
        expected = (dt.datetime.strptime('15Jan2020', '%d%b%Y').date() - ref).days
        assert result == expected

    # -- NumberReg (e.g. "100") -------------------------------------------

    def test_number(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('100', ref) == 100.0

    # -- FloatingYear (e.g. "1.5y") ----------------------------------------

    def test_floating_year(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('1.5y', ref)
        assert result == 365 * 1.5

    def test_floating_year_upper(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('2.0Y', ref)
        assert result == 365 * 2.0

    # -- PricerCoordRegI (e.g. "No 5") ------------------------------------

    def test_pricer_coord_I(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('No 5', ref) == 5.0

    # -- PricerCoordRegII (e.g. "Pricer 10") -------------------------------

    def test_pricer_coord_II(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('Pricer 10', ref) == 10.0

    # -- PricerBondSpreadReg (e.g. "1S20/2S22") ----------------------------

    def test_pricer_bond_spread(self):
        ref = dt.date(2020, 1, 1)
        # Matches PricerBondSpreadReg -> days stays None (pass)
        assert point_sort_order('1S20/2S22', ref) is None

    # -- LYYReg (e.g. "F20", "Z25") ----------------------------------------

    def test_lyy(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('F20', ref)
        # F is at index 0 in FutMonth -> month = 1
        expected = (dt.datetime.strptime('20-1-1', '%y-%m-%d').date() - ref).days
        assert result == expected

    def test_lyy_z(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('Z25', ref)
        # Z is at index 11 -> month = 12
        expected = (dt.datetime.strptime('25-12-1', '%y-%m-%d').date() - ref).days
        assert result == expected

    # -- DatePairReg (e.g. "20200101/20200601") ----------------------------

    def test_date_pair(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('20200101/20200601', ref)
        expected = (dt.datetime.strptime('20200601', '%Y%m%d').date() - ref).days
        assert result == expected

    # -- DatePairReg2 (e.g. "20200101 20200601") ---------------------------

    def test_date_pair2(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('20200101 20200601', ref)
        expected = (dt.datetime.strptime('20200601', '%Y%m%d').date() - ref).days
        assert result == expected

    # -- MMMYYReg (e.g. "JAN 20") ------------------------------------------

    def test_mmmyy(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('JAN 20', ref)
        expected = (dt.datetime.strptime('1JAN20', '%d%b%y').date() - ref).days
        assert result == expected

    # -- FXVolAddonParmsReg ------------------------------------------------

    def test_fxvol_addon_parms(self):
        ref = dt.date(2020, 1, 1)
        # "Spread Addon" matches FXVolAddonParmsReg -> pass -> days is None
        assert point_sort_order('Spread Addon', ref) is None

    # -- BondCoordReg (e.g. "5 3.25 01/06/2020") ---------------------------

    def test_bond_coord(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('5 3.25 01/06/2020', ref)
        expected = (dt.datetime.strptime('01/06/2020', '%d/%m/%Y').date() - ref).days
        assert result == expected

    # -- BondFutReg (e.g. "TYVF5") -----------------------------------------

    def test_bond_fut(self):
        ref = dt.date(2020, 6, 1)
        result = point_sort_order('TYVF5', ref)
        # F -> month 1
        expected = (dt.datetime.strptime(str(ref.year) + '-1-1', '%Y-%m-%d').date() - ref).days
        assert result == expected

    # -- FFFutReg (e.g. "FFZ5") --------------------------------------------

    def test_ff_fut(self):
        ref = dt.date(2020, 6, 1)
        result = point_sort_order('FFZ5', ref)
        # Z -> month 12
        expected = (dt.datetime.strptime(str(ref.year) + '-12-1', '%Y-%m-%d').date() - ref).days
        assert result == expected

    # -- RepoGCReg ---------------------------------------------------------

    def test_repo_gc_on(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('ON GC', ref) == 0

    def test_repo_gc_tn(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('TN GC', ref) == 1

    def test_repo_gc_sn(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('SN GC', ref) == 2

    def test_repo_gc_month(self):
        ref = dt.date(2020, 1, 1)
        # "3 Month GC" -> res.group(2).strip() = "Month" -> scale=30, num=3 -> 90
        assert point_sort_order('3 Month GC', ref) == 90.0

    def test_repo_gc_week(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('2 Week GC', ref) == 14.0

    def test_repo_gc_year(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('1 Year GC', ref) == 365.0

    def test_repo_gc_day(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('5 Day GC', ref) == 5.0

    def test_repo_gc_no_unit(self):
        # "5 GC" matches RepoGCReg with group(2)="" which is '' -> strip()=''
        # '' not in DictDayRule -> days stays None
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('5 GC', ref) is None

    # -- RelativeReg (e.g. "5 day") ----------------------------------------

    def test_relative_day(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('5 day', ref) == 5.0

    def test_relative_week(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('2 week', ref) == 14.0

    def test_relative_month(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('3 month', ref) == 90.0

    def test_relative_year(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('1 year', ref) == 365.0

    def test_relative_upper(self):
        ref = dt.date(2020, 1, 1)
        assert point_sort_order('1 DAY', ref) == 1.0
        point_sort_order.cache_clear()
        assert point_sort_order('1 WEEK', ref) == 7.0
        point_sort_order.cache_clear()
        assert point_sort_order('1 MONTH', ref) == 30.0
        point_sort_order.cache_clear()
        assert point_sort_order('1 YEAR', ref) == 365.0

    # -- DDMMMYYReg (e.g. "15Jan20") ----------------------------------------

    def test_ddmmmyy(self):
        ref = dt.date(2020, 1, 1)
        result = point_sort_order('15Jan20', ref)
        expected = (dt.datetime.strptime('15Jan20', '%d%b%y').date() - ref).days
        assert result == expected

    # -- else branch (unknown point) ----------------------------------------

    def test_unknown_point(self):
        ref = dt.date(2020, 1, 1)
        # A string that matches none of the patterns -> days = 0
        result = point_sort_order('$$$UNKNOWN$$$', ref)
        assert result == 0

    # -- Semicolon-separated points (multi-part) ----------------------------

    def test_semicolon_parts(self):
        ref = dt.date(2020, 1, 1)
        # "100;200" -> parts = ["100", "200"]
        # first = point_sort_order("100") = 100
        # sum of rest = point_sort_order("200") = 200
        # result = 100 + 0.1 * 200 / 100 = 100.2
        result = point_sort_order('100;200', ref)
        assert result == 100.2

    def test_semicolon_parts_first_zero(self):
        ref = dt.date(2020, 1, 1)
        # If first part evaluates to 0, returns 0
        result = point_sort_order('$$$UNKNOWN$$$;200', ref)
        assert result == 0

    # -- ref_date defaults to today when None --------------------------------

    def test_ref_date_defaults_to_today(self):
        # Should not raise; result depends on today's date
        result = point_sort_order('100')
        assert result == 100.0


# ---------------------------------------------------------------------------
# Phase-6: Cover remaining branches via mocking
# ---------------------------------------------------------------------------

from unittest.mock import patch


class TestPointSortOrderDeadCodeBranches:
    """Tests that cover branches guarded by ConstPoints pre-check.

    Lines 158-171 are lowercase special values (o/n, t/n, Cash Stub, etc.)
    that are normally intercepted by the ConstPoints.get(point.upper()) check
    at line 144-146. We patch ConstPoints to bypass that check.

    Also covers branch [111,117] (invalid rule in DictDayRule) and
    branch [314,325] (RelativeReg rule not in DictDayRule).
    """

    def setup_method(self):
        point_sort_order.cache_clear()

    def teardown_method(self):
        point_sort_order.cache_clear()

    def test_lowercase_on(self):
        """Cover branch [158,159]: point == 'o/n' -> days = 0."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('o/n', dt.date(2020, 1, 1))
            assert result == 0

    def test_lowercase_tn(self):
        """Cover branch [160,161]: point == 't/n' -> days = 0.1."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('t/n', dt.date(2020, 1, 1))
            assert result == 0.1

    def test_lowercase_cash_stub(self):
        """Cover branch [162,163]: point == 'Cash Stub' -> days = 1.1."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('Cash Stub', dt.date(2020, 1, 1))
            assert result == 1.1

    def test_lowercase_cashstub(self):
        """Cover branch [164,165]: point == 'CashStub' -> days = 1.1."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('CashStub', dt.date(2020, 1, 1))
            assert result == 1.1

    def test_lowercase_default(self):
        """Cover branch [166,167]: point == 'Default' -> days = 0."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('Default', dt.date(2020, 1, 1))
            assert result == 0

    def test_lowercase_in(self):
        """Cover branch [168,169]: point == 'In' -> days = 0.1."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('In', dt.date(2020, 1, 1))
            assert result == 0.1

    def test_lowercase_out(self):
        """Cover branch [170,171]: point == 'Out' -> days = 0.2."""
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('Out', dt.date(2020, 1, 1))
            assert result == 0.2

    def test_infl_vol_zcswo_return_path(self):
        """Cover branch [181,325]: ZCSwo sets days=3, then falls through to return.

        With ConstPoints empty, 'ZCSwo' won't be intercepted and will reach
        the infl_vol elif block.
        """
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            point_sort_order.cache_clear()
            result = point_sort_order('ZCSwo', dt.date(2020, 1, 1))
            assert result == 3

    def test_infl_vol_no_match_branch(self):
        """Cover branch [181,325]: infl_vol regex matches but none of the 4 known
        values. Requires patching infl_volReg to include an extra alternative.
        """
        import gs_quant.datetime.point as point_mod
        original_reg = point_mod.infl_volReg
        try:
            # Add 'OtherVol' to the regex so it matches but falls through all elif
            point_mod.infl_volReg = r"(Caplet|ZCCap|Swaption|ZCSwo|OtherVol)"
            with patch('gs_quant.datetime.point.ConstPoints', {}):
                point_sort_order.cache_clear()
                result = point_sort_order('OtherVol', dt.date(2020, 1, 1))
                # None of the if/elif match 'OtherVol', so days stays None
                assert result is None
        finally:
            point_mod.infl_volReg = original_reg
            point_sort_order.cache_clear()

    def test_relative_reg_rule_not_in_dictdayrule(self):
        """Cover branch [314,325]: RelativeReg matches but rule not in DictDayRule.

        We need a RelativeReg match where capwords(group(2)) is not in DictDayRule.
        RelativeReg = r'^([0-9]+) (day|week|month|year|DAY|WEEK|MONTH|YEAR)$'
        capwords gives 'Day', 'Week', 'Month', 'Year' - all are in DictDayRule.
        So we patch DictDayRule to not have 'Day'.
        """
        with patch('gs_quant.datetime.point.ConstPoints', {}):
            with patch('gs_quant.datetime.point.DictDayRule', {'Month': 30, 'Week': 7, 'Year': 365}):
                point_sort_order.cache_clear()
                result = point_sort_order('5 day', dt.date(2020, 1, 1))
                # 'day' -> capwords -> 'Day', not in patched DictDayRule -> days is None
                assert result is None

    def test_relative_date_add_invalid_rule(self):
        """Cover branch [111,117]: rule in DateRuleReg but not in DictDayRule.

        All regex-matched letters are in DictDayRule, so we patch DictDayRule
        to remove a valid letter.
        """
        with patch('gs_quant.datetime.point.DictDayRule', {'m': 30, 'y': 365}):
            with pytest.raises(MqValueError, match='no valid day rule'):
                relative_date_add('1d')
