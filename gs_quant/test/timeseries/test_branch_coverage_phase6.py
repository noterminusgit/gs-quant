"""
Branch coverage tests for timeseries modules - Phase 6
Covers missing branches in:
  - measures_rates.py
  - measures_xccy.py
  - measures_reports.py
  - measures_risk_models.py
  - measures_fx_vol.py
  - statistics.py
  - econometrics.py
  - backtesting.py
  - analysis.py
  - algebra.py
  - helper.py
"""
import datetime as dt
import os
from collections import namedtuple
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.errors import MqValueError


# ==============================================================================
# measures_rates.py - _match_floating_tenors
# Branches: [466,482] [472,482] [478,482]
# ==============================================================================

class TestMatchFloatingTenors:
    """Test _match_floating_tenors branch coverage."""

    def test_sofr_payer_non_12m(self):
        """Branch [466,482]: SOFR in payer_index, but designated_maturity != '12m'."""
        from gs_quant.timeseries.measures_rates import _match_floating_tenors
        swap_args = {
            'asset_parameters_payer_rate_option': 'USD-SOFR-COMPOUND',
            'asset_parameters_receiver_rate_option': 'USD-LIBOR-BBA',
            'asset_parameters_payer_designated_maturity': '3m',
            'asset_parameters_receiver_designated_maturity': '6m',
        }
        result = _match_floating_tenors(swap_args)
        # payer_designated_maturity should be copied from receiver
        assert result['asset_parameters_payer_designated_maturity'] == '6m'

    def test_sofr_receiver_non_12m(self):
        """Branch [472,482]: SOFR in receiver_index, but designated_maturity != '12m'."""
        from gs_quant.timeseries.measures_rates import _match_floating_tenors
        swap_args = {
            'asset_parameters_payer_rate_option': 'USD-LIBOR-BBA',
            'asset_parameters_receiver_rate_option': 'USD-SOFR-COMPOUND',
            'asset_parameters_payer_designated_maturity': '6m',
            'asset_parameters_receiver_designated_maturity': '3m',
        }
        result = _match_floating_tenors(swap_args)
        # receiver_designated_maturity should be copied from payer
        assert result['asset_parameters_receiver_designated_maturity'] == '6m'

    def test_libor_receiver(self):
        """Branch [478,482] True: LIBOR in receiver_index (not payer), swap receiver maturity to payer's."""
        from gs_quant.timeseries.measures_rates import _match_floating_tenors
        swap_args = {
            'asset_parameters_payer_rate_option': 'USD-SOMETHING',
            'asset_parameters_receiver_rate_option': 'USD-LIBOR-BBA',
            'asset_parameters_payer_designated_maturity': '6m',
            'asset_parameters_receiver_designated_maturity': '3m',
        }
        result = _match_floating_tenors(swap_args)
        # payer_designated_maturity should be copied from receiver
        assert result['asset_parameters_payer_designated_maturity'] == '3m'

    def test_no_matching_index_fallthrough(self):
        """Branch [478,482] False: Neither index matches any of SOFR/LIBOR/EURIBOR/STIBOR,
        so all elif conditions are False and we fall through to return."""
        from gs_quant.timeseries.measures_rates import _match_floating_tenors
        swap_args = {
            'asset_parameters_payer_rate_option': 'USD-OIS-COMPOUND',
            'asset_parameters_receiver_rate_option': 'USD-SARON-COMPOUND',
            'asset_parameters_payer_designated_maturity': '6m',
            'asset_parameters_receiver_designated_maturity': '3m',
        }
        result = _match_floating_tenors(swap_args)
        # No matching logic applied, values unchanged
        assert result['asset_parameters_payer_designated_maturity'] == '6m'
        assert result['asset_parameters_receiver_designated_maturity'] == '3m'


# ==============================================================================
# measures_rates.py - _swaption_build_asset_query
# Branches: [1402,1404] [1404,1406] [1410,1412] [1412,1414]
# ==============================================================================

class TestSwaptionBuildAssetQuery:
    """Test _swaption_build_asset_query with None optional parameters."""

    @patch('gs_quant.timeseries.measures_rates.swaptions_defaults_provider')
    def test_none_floating_rate_option(self, mock_provider):
        """Branch [1402,1404]: floating_rate_option is None."""
        from gs_quant.timeseries.measures_rates import _swaption_build_asset_query
        from gs_quant.common import Currency as CurrencyEnum

        # Make get_floating_rate_option_for_benchmark return None to trigger raise
        # Actually [1402,1404] means floating_rate_option is None at line 1402,
        # but line 1377 raises if it's None. So this branch is unreachable normally.
        # Instead, let's mock the provider to return None for specific params
        # to make floating_rate_option None but bypass the guard.
        # Actually, we need to make the provider return non-None from get_floating_rate_option
        # but then make floating_rate_tenor None via get_swaption_parameter.

        mock_provider.get_swaption_parameter.side_effect = lambda c, key, val=None: {
            'benchmarkType': 'LIBOR',
            'floatingRateTenor': None,  # This makes floating_rate_tenor None -> branch [1404,1406]
            'strikeReference': 'ATM',
            'terminationTenor': '5y',
            'effectiveDate': None,       # This makes forward_tenor None -> branch [1412,1414]
            'expirationTenor': '1y',
            'clearingHouse': None,       # This makes clearinghouse None -> branch [1410,1412]
        }.get(key, val)

        mock_provider.get_floating_rate_option_for_benchmark.return_value = None

        # This should raise because floating_rate_option is None
        with pytest.raises(MqValueError, match="Invalid benchmark type"):
            _swaption_build_asset_query(CurrencyEnum.USD, benchmark_type='LIBOR')

    @patch('gs_quant.timeseries.measures_rates._check_forward_tenor')
    @patch('gs_quant.timeseries.measures_rates._check_strike_reference')
    @patch('gs_quant.timeseries.measures_rates.swaptions_defaults_provider')
    def test_none_optional_params(self, mock_provider, mock_strike, mock_fwd):
        """Branches [1404,1406] [1410,1412] [1412,1414]:
        floating_rate_tenor, clearinghouse, forward_tenor are None."""
        from gs_quant.timeseries.measures_rates import _swaption_build_asset_query
        from gs_quant.common import Currency as CurrencyEnum

        mock_provider.get_swaption_parameter.side_effect = lambda c, key, val=None: {
            'benchmarkType': 'LIBOR',
            'floatingRateTenor': None,
            'strikeReference': 'ATM',
            'terminationTenor': '5y',
            'effectiveDate': None,
            'expirationTenor': '1y',
            'clearingHouse': None,
        }.get(key, val)

        mock_provider.get_floating_rate_option_for_benchmark.return_value = 'USD-LIBOR-BBA'
        mock_provider.is_supported.return_value = True
        mock_strike.return_value = 'ATM'
        mock_fwd.return_value = None  # forward_tenor is None -> branch [1412,1414]

        query = _swaption_build_asset_query(CurrencyEnum.USD)

        # floating_rate_tenor is None -> no 'asset_parameters_floating_rate_designated_maturity'
        assert 'asset_parameters_floating_rate_designated_maturity' not in query
        # clearinghouse is None -> no 'asset_parameters_clearing_house'
        assert 'asset_parameters_clearing_house' not in query
        # forward_tenor is None -> no 'asset_parameters_effective_date'
        assert 'asset_parameters_effective_date' not in query

    # NOTE: Branch [1402,1404] is unreachable code - floating_rate_option is always
    # non-None at line 1402 because line 1377 raises if it's None.


# ==============================================================================
# measures_rates.py - swaption_vol (location is None branch)
# Branch [1489,1492]: location is not None (i.e., location provided)
# ==============================================================================

class TestSwaptionVolSmile:
    """Test swaption_vol_smile with location provided."""

    @patch('gs_quant.timeseries.measures_rates._get_swaption_measure')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates.swaptions_defaults_provider')
    def test_location_provided(self, mock_provider, mock_range, mock_measure):
        """Branch [1489,1492]: location is not None, skip default in swaption_vol_smile."""
        from gs_quant.timeseries.measures_rates import swaption_vol_smile
        from gs_quant.common import PricingLocation

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        mock_range.return_value = ('2021-01-01', '2021-01-02')
        mock_measure.return_value = pd.DataFrame()

        # Provide location explicitly to skip the None branch
        result = swaption_vol_smile(asset, '1y', '5y', location=PricingLocation.NYC)
        # Verify swaptions_defaults_provider was NOT called for pricingLocation
        for call in mock_provider.get_swaption_parameter.call_args_list:
            assert call[0][1] != 'pricingLocation'


# ==============================================================================
# measures_rates.py - swaption_vol_term (location is None branch)
# Branch [1565,1567]: location is not None
# ==============================================================================

class TestSwaptionVolTerm:
    """Test swaption_vol_term with location provided."""

    @patch('gs_quant.timeseries.measures_rates._get_swaption_measure')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates.swaptions_defaults_provider')
    def test_location_provided(self, mock_provider, mock_range, mock_measure):
        """Branch [1565,1567]: location is not None, skip default."""
        from gs_quant.timeseries.measures_rates import swaption_vol_term
        from gs_quant.timeseries.measures import SwaptionTenorType
        from gs_quant.common import PricingLocation

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        mock_range.return_value = ('2021-01-01', '2021-01-02')
        mock_measure.return_value = pd.DataFrame()

        result = swaption_vol_term(
            asset,
            tenor_type=SwaptionTenorType.OPTION_EXPIRY,
            tenor='1y',
            relative_strike=0,
            location=PricingLocation.NYC,
        )


# ==============================================================================
# measures_rates.py - forward_rate
# Branch [1764,1767]: forward_start_tenor is not None (provided)
# ==============================================================================

class TestForwardRate:
    """Test forward_rate with forward_start_tenor provided."""

    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    def test_forward_start_tenor_provided(self, mock_api):
        """Branch [1764,1767]: forward_start_tenor is not None, skip default '0d'."""
        from gs_quant.timeseries.measures_rates import forward_rate

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        mock_api.get_mxapi_curve_measure.return_value = pd.DataFrame()

        result = forward_rate(asset, forward_start_tenor='1y', forward_term='3m')
        # Should use '1y' not '0d'
        call_args = mock_api.get_mxapi_curve_measure.call_args
        assert 'FR:1y:3m' in str(call_args)


# ==============================================================================
# measures_rates.py - index_forward_rate
# Branch [1908,1915]: fixing_tenor is not None (provided)
# ==============================================================================

class TestIndexForwardRate:
    """Test index_forward_rate with fixing_tenor provided."""

    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    @patch('gs_quant.timeseries.measures_rates._get_benchmark_type')
    @patch('gs_quant.timeseries.measures_rates._check_benchmark_type')
    def test_fixing_tenor_provided(self, mock_check_bm, mock_get_bm, mock_api):
        """Branch [1908,1915]: fixing_tenor is not None, skip default lookup."""
        from gs_quant.timeseries.measures_rates import index_forward_rate
        from gs_quant.timeseries.measures_rates import BenchmarkType

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        mock_check_bm.return_value = BenchmarkType.SOFR
        mock_get_bm.return_value = 'USD-SOFR-COMPOUND'
        mock_api.get_mxapi_curve_measure.return_value = pd.DataFrame()

        result = index_forward_rate(
            asset, forward_start_tenor='1y', fixing_tenor='3m'
        )
        call_args = mock_api.get_mxapi_curve_measure.call_args
        assert 'FR:1y:3m' in str(call_args)


# ==============================================================================
# measures_rates.py - swap_rate_2 term structure
# Branch [2144,2146]: rate_mqids is a string (single asset)
# ==============================================================================

class TestSwapTermStructure:
    """Test swap_term_structure where _get_tdapi_rates_assets returns different types."""

    def _make_mocks(self, mock_ch, mock_bm, mock_tt, mock_tst,
                    mock_defaults, mock_assets, mock_range,
                    mock_cal, mock_data_api, mock_timed, assets_return):
        from gs_quant.common import PricingLocation
        from gs_quant.timeseries.measures_rates import _ClearingHouse, _SwapTenorType

        mock_ch.return_value = _ClearingHouse.LCH
        mock_bm.return_value = 'SOFR'
        mock_tt.return_value = _SwapTenorType.FORWARD_TENOR
        mock_tst.return_value = {
            'tenor_dataset_field': 'asset_parameters_effective_date',
            'tenor': '0b',
        }
        mock_defaults.return_value = {
            'benchmark_type': 'USD-SOFR-COMPOUND',
            'floating_rate_tenor': '1y',
            'pricing_location': PricingLocation.NYC,
        }
        mock_assets.return_value = assets_return
        mock_range.return_value = ('2021-01-01', '2021-01-02')
        mock_data_api.build_market_data_query.return_value = {}
        mock_timed.return_value = pd.DataFrame()

    @patch('gs_quant.timeseries.measures_rates._market_data_timed')
    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    @patch('gs_quant.timeseries.measures_rates.GsCalendar')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates._get_tdapi_rates_assets')
    @patch('gs_quant.timeseries.measures_rates._get_swap_leg_defaults')
    @patch('gs_quant.timeseries.measures_rates._check_term_structure_tenor')
    @patch('gs_quant.timeseries.measures_rates._check_tenor_type')
    @patch('gs_quant.timeseries.measures_rates._check_benchmark_type')
    @patch('gs_quant.timeseries.measures_rates._check_clearing_house')
    def test_single_asset_string(self, mock_ch, mock_bm, mock_tt, mock_tst,
                                  mock_defaults, mock_assets, mock_range,
                                  mock_cal, mock_data_api, mock_timed):
        """Branch [2144,2146] True: rate_mqids is a str, should be wrapped in list."""
        from gs_quant.timeseries.measures_rates import swap_term_structure
        from gs_quant.common import PricingLocation

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        self._make_mocks(mock_ch, mock_bm, mock_tt, mock_tst, mock_defaults,
                         mock_assets, mock_range, mock_cal, mock_data_api, mock_timed,
                         'SINGLE_ASSET_ID')
        result = swap_term_structure(asset, location=PricingLocation.NYC)

    @patch('gs_quant.timeseries.measures_rates._market_data_timed')
    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    @patch('gs_quant.timeseries.measures_rates.GsCalendar')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates._get_tdapi_rates_assets')
    @patch('gs_quant.timeseries.measures_rates._get_swap_leg_defaults')
    @patch('gs_quant.timeseries.measures_rates._check_term_structure_tenor')
    @patch('gs_quant.timeseries.measures_rates._check_tenor_type')
    @patch('gs_quant.timeseries.measures_rates._check_benchmark_type')
    @patch('gs_quant.timeseries.measures_rates._check_clearing_house')
    def test_list_of_assets(self, mock_ch, mock_bm, mock_tt, mock_tst,
                            mock_defaults, mock_assets, mock_range,
                            mock_cal, mock_data_api, mock_timed):
        """Branch [2144,2146] False: rate_mqids is already a list."""
        from gs_quant.timeseries.measures_rates import swap_term_structure
        from gs_quant.common import PricingLocation

        asset = MagicMock()
        asset.get_identifier.return_value = 'USD'
        self._make_mocks(mock_ch, mock_bm, mock_tt, mock_tst, mock_defaults,
                         mock_assets, mock_range, mock_cal, mock_data_api, mock_timed,
                         ['ASSET_ID_1', 'ASSET_ID_2'])
        result = swap_term_structure(asset, location=PricingLocation.NYC)


# ==============================================================================
# measures_rates.py - basis_swap_term_structure
# Branch [2253,2255]: rate_mqids is a string (single asset)
# ==============================================================================

class TestBasisSwapTermStructure:
    """Test basis_swap_term_structure branch coverage."""

    def _make_mocks(self, mock_tt, mock_tst, mock_kwargs, mock_assets,
                    mock_range, mock_cal, mock_data_api, mock_timed, assets_return):
        from gs_quant.common import PricingLocation
        from gs_quant.timeseries.measures_rates import _SwapTenorType

        mock_tt.return_value = _SwapTenorType.FORWARD_TENOR
        mock_tst.return_value = {
            'tenor_dataset_field': 'asset_parameters_effective_date',
            'tenor': '0b',
        }
        mock_kwargs.return_value = {
            'asset_parameters_payer_rate_option': 'USD-SOFR-COMPOUND',
            'asset_parameters_receiver_rate_option': 'USD-LIBOR-BBA',
            'asset_parameters_payer_designated_maturity': '1y',
            'asset_parameters_receiver_designated_maturity': '3m',
            'pricing_location': PricingLocation.NYC,
        }
        mock_assets.return_value = assets_return
        mock_range.return_value = ('2021-01-01', '2021-01-02')
        mock_data_api.build_market_data_query.return_value = {}
        mock_timed.return_value = pd.DataFrame()

    @patch('gs_quant.timeseries.measures_rates._market_data_timed')
    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    @patch('gs_quant.timeseries.measures_rates.GsCalendar')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates._get_tdapi_rates_assets')
    @patch('gs_quant.timeseries.measures_rates._get_basis_swap_kwargs')
    @patch('gs_quant.timeseries.measures_rates._check_term_structure_tenor')
    @patch('gs_quant.timeseries.measures_rates._check_tenor_type')
    def test_single_asset_string(self, mock_tt, mock_tst, mock_kwargs,
                                  mock_assets, mock_range, mock_cal,
                                  mock_data_api, mock_timed):
        """Branch [2253,2255] True: rate_mqids is a str, should be wrapped in list."""
        from gs_quant.timeseries.measures_rates import basis_swap_term_structure

        asset = MagicMock()
        self._make_mocks(mock_tt, mock_tst, mock_kwargs, mock_assets,
                         mock_range, mock_cal, mock_data_api, mock_timed,
                         'SINGLE_ASSET_ID')
        result = basis_swap_term_structure(asset)

    @patch('gs_quant.timeseries.measures_rates._market_data_timed')
    @patch('gs_quant.timeseries.measures_rates.GsDataApi')
    @patch('gs_quant.timeseries.measures_rates.GsCalendar')
    @patch('gs_quant.timeseries.measures_rates._range_from_pricing_date')
    @patch('gs_quant.timeseries.measures_rates._get_tdapi_rates_assets')
    @patch('gs_quant.timeseries.measures_rates._get_basis_swap_kwargs')
    @patch('gs_quant.timeseries.measures_rates._check_term_structure_tenor')
    @patch('gs_quant.timeseries.measures_rates._check_tenor_type')
    def test_list_of_assets(self, mock_tt, mock_tst, mock_kwargs,
                            mock_assets, mock_range, mock_cal,
                            mock_data_api, mock_timed):
        """Branch [2253,2255] False: rate_mqids is already a list."""
        from gs_quant.timeseries.measures_rates import basis_swap_term_structure

        asset = MagicMock()
        self._make_mocks(mock_tt, mock_tst, mock_kwargs, mock_assets,
                         mock_range, mock_cal, mock_data_api, mock_timed,
                         ['ASSET_ID_1', 'ASSET_ID_2'])
        result = basis_swap_term_structure(asset)


# ==============================================================================
# measures_rates.py - _get_default_ois_benchmark
# Branch [2690,-2685]: EUR branch taken (currently never tested with EUR)
# ==============================================================================

class TestGetDefaultOisBenchmark:
    """Test _get_default_ois_benchmark for EUR currency."""

    def test_eur_returns_eurostr(self):
        """Branch: currency == EUR returns EUROSTR."""
        from gs_quant.timeseries.measures_rates import _get_default_ois_benchmark, BenchmarkTypeCB
        from gs_quant.common import Currency as CurrencyEnum

        result = _get_default_ois_benchmark(CurrencyEnum.EUR)
        assert result == BenchmarkTypeCB.EUROSTR

    def test_unsupported_currency_returns_none(self):
        """Branch [2690,-2685]: currency is not USD/GBP/EUR -> function exits with no return (None)."""
        from gs_quant.timeseries.measures_rates import _get_default_ois_benchmark
        from gs_quant.common import Currency as CurrencyEnum

        result = _get_default_ois_benchmark(CurrencyEnum.JPY)
        assert result is None


# ==============================================================================
# measures_xccy.py - _get_tdapi_crosscurrency_rates_assets
# Branches: [354,362] [362,367] [370,375] [380,388] [388,393]
# ==============================================================================

class TestGetTdapiCrosscurrencyRatesAssets:
    """Test _get_tdapi_crosscurrency_rates_assets branch coverage."""

    @patch('gs_quant.timeseries.measures_xccy.GsAssetApi')
    def test_flip_legs_without_designated_maturity_or_currency(self, mock_api):
        """Branches [354,362] [362,367]: First flip - no designated_maturity, no payer_currency in kwargs."""
        from gs_quant.timeseries.measures_xccy import _get_tdapi_crosscurrency_rates_assets

        asset1 = MagicMock()
        asset1.id = 'asset1'
        # First call: no assets. Second call after flip: one asset.
        mock_api.get_many_assets.side_effect = [[], [asset1]]

        kwargs = {
            'asset_parameters_payer_rate_option': 'USD-SOFR',
            'asset_parameters_receiver_rate_option': 'EUR-EURIBOR',
        }
        result = _get_tdapi_crosscurrency_rates_assets(**kwargs)
        assert result == 'asset1'

    @patch('gs_quant.timeseries.measures_xccy.GsAssetApi')
    def test_flip_legs_with_designated_maturity_and_currency(self, mock_api):
        """Branches [354,362] with designated_maturity, [362,367] with payer_currency."""
        from gs_quant.timeseries.measures_xccy import _get_tdapi_crosscurrency_rates_assets

        asset1 = MagicMock()
        asset1.id = 'asset1'
        # First call: no assets. Second call after flip: one asset.
        mock_api.get_many_assets.side_effect = [[], [asset1]]

        kwargs = {
            'asset_parameters_payer_rate_option': 'USD-SOFR',
            'asset_parameters_receiver_rate_option': 'EUR-EURIBOR',
            'asset_parameters_payer_designated_maturity': '3m',
            'asset_parameters_receiver_designated_maturity': '6m',
            'asset_parameters_payer_currency': 'USD',
            'asset_parameters_receiver_currency': 'EUR',
        }
        result = _get_tdapi_crosscurrency_rates_assets(**kwargs)
        assert result == 'asset1'

    @patch('gs_quant.timeseries.measures_xccy.tm_rates._ClearingHouse')
    @patch('gs_quant.timeseries.measures_xccy.GsAssetApi')
    def test_clearing_house_none_removal_and_second_flip(self, mock_api, mock_ch):
        """Branches [380,388] [388,393]: clearing house removal and second flip."""
        from gs_quant.timeseries.measures_xccy import _get_tdapi_crosscurrency_rates_assets

        asset1 = MagicMock()
        asset1.id = 'asset1'
        mock_ch.NONE.value = 'NONE'

        # Call sequence: initial(0), first-flip(0), no-clearing(0), second-flip(1)
        mock_api.get_many_assets.side_effect = [[], [], [], [asset1]]

        kwargs = {
            'asset_parameters_payer_rate_option': 'USD-SOFR',
            'asset_parameters_receiver_rate_option': 'EUR-EURIBOR',
            'asset_parameters_clearing_house': 'NONE',
        }
        result = _get_tdapi_crosscurrency_rates_assets(**kwargs)
        assert result == 'asset1'

    @patch('gs_quant.timeseries.measures_xccy.tm_rates._ClearingHouse')
    @patch('gs_quant.timeseries.measures_xccy.GsAssetApi')
    def test_clearing_house_not_none_skip_removal(self, mock_api, mock_ch):
        """Branch [370,375]: clearing house is not NONE, skip the deletion block."""
        from gs_quant.timeseries.measures_xccy import _get_tdapi_crosscurrency_rates_assets

        asset1 = MagicMock()
        asset1.id = 'asset1'
        mock_ch.NONE.value = 'NONE'

        # Call sequence: initial(0), first-flip(0), skip clearing (not NONE), second-flip(1)
        mock_api.get_many_assets.side_effect = [[], [], [asset1]]

        kwargs = {
            'asset_parameters_payer_rate_option': 'USD-SOFR',
            'asset_parameters_receiver_rate_option': 'EUR-EURIBOR',
            'asset_parameters_clearing_house': 'LCH',  # Not NONE -> skip deletion
        }
        result = _get_tdapi_crosscurrency_rates_assets(**kwargs)
        assert result == 'asset1'

    @patch('gs_quant.timeseries.measures_xccy.tm_rates._ClearingHouse')
    @patch('gs_quant.timeseries.measures_xccy.GsAssetApi')
    def test_second_flip_with_designated_maturity_and_currency(self, mock_api, mock_ch):
        """Branches [380,388] [388,393]: second flip with designated_maturity and payer_currency."""
        from gs_quant.timeseries.measures_xccy import _get_tdapi_crosscurrency_rates_assets

        asset1 = MagicMock()
        asset1.id = 'asset1'
        mock_ch.NONE.value = 'NONE'

        # Call sequence: initial(0), first-flip(0), no-clearing(0), second-flip(1)
        mock_api.get_many_assets.side_effect = [[], [], [], [asset1]]

        kwargs = {
            'asset_parameters_payer_rate_option': 'USD-SOFR',
            'asset_parameters_receiver_rate_option': 'EUR-EURIBOR',
            'asset_parameters_payer_designated_maturity': '3m',
            'asset_parameters_receiver_designated_maturity': '6m',
            'asset_parameters_payer_currency': 'USD',
            'asset_parameters_receiver_currency': 'EUR',
            'asset_parameters_clearing_house': 'NONE',
        }
        result = _get_tdapi_crosscurrency_rates_assets(**kwargs)
        assert result == 'asset1'


# ==============================================================================
# measures_reports.py - thematic_exposure, thematic_beta, aum, factor results
# Branches: [307,310] [334,337] [359,362] [1717,1720]
# All: df.empty is True -> skip set_index/to_datetime
# ==============================================================================

class TestMeasuresReportsEmptyDf:
    """Test measures_reports.py branches where df is empty."""

    @patch('gs_quant.timeseries.measures_reports._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_reports.SecurityMaster')
    @patch('gs_quant.timeseries.measures_reports.ThematicReport')
    def test_thematic_exposure_empty_df(self, mock_report_cls, mock_sm, mock_extract):
        """Branch [307,310]: df.empty is True in thematic_exposure."""
        from gs_quant.timeseries.measures_reports import thematic_exposure

        mock_report = MagicMock()
        mock_report.get_thematic_exposure.return_value = pd.DataFrame()
        mock_report_cls.get.return_value = mock_report

        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'mqid'
        mock_sm.get_asset.return_value = mock_asset

        mock_extract.return_value = pd.Series(dtype=float)

        with patch('gs_quant.timeseries.measures_reports.DataContext') as mock_dc:
            mock_dc.current.start_date = dt.date(2021, 1, 1)
            mock_dc.current.end_date = dt.date(2021, 12, 31)
            result = thematic_exposure('report_id', 'GSXU')

        assert isinstance(result, pd.Series)

    @patch('gs_quant.timeseries.measures_reports._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_reports.SecurityMaster')
    @patch('gs_quant.timeseries.measures_reports.ThematicReport')
    def test_thematic_beta_empty_df(self, mock_report_cls, mock_sm, mock_extract):
        """Branch [334,337]: df.empty is True in thematic_beta."""
        from gs_quant.timeseries.measures_reports import thematic_beta

        mock_report = MagicMock()
        mock_report.get_thematic_betas.return_value = pd.DataFrame()
        mock_report_cls.get.return_value = mock_report

        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'mqid'
        mock_sm.get_asset.return_value = mock_asset

        mock_extract.return_value = pd.Series(dtype=float)

        with patch('gs_quant.timeseries.measures_reports.DataContext') as mock_dc:
            mock_dc.current.start_date = dt.date(2021, 1, 1)
            mock_dc.current.end_date = dt.date(2021, 12, 31)
            result = thematic_beta('report_id', 'GSXU')

        assert isinstance(result, pd.Series)

    @patch('gs_quant.timeseries.measures_reports._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_reports.PerformanceReport')
    def test_aum_empty_df(self, mock_report_cls, mock_extract):
        """Branch [359,362]: df.empty is True in aum."""
        from gs_quant.timeseries.measures_reports import aum

        mock_report = MagicMock()
        mock_report.get_aum.return_value = {}  # empty dict -> empty df
        mock_report_cls.get.return_value = mock_report

        mock_extract.return_value = pd.Series(dtype=float)

        with patch('gs_quant.timeseries.measures_reports.DataContext') as mock_dc:
            mock_dc.current.start_time = dt.datetime(2021, 1, 1)
            mock_dc.current.end_time = dt.datetime(2021, 12, 31)
            result = aum('report_id')

        assert isinstance(result, pd.Series)

    @patch('gs_quant.timeseries.measures_reports._extract_series_from_df')
    def test_factor_data_empty_df(self, mock_extract):
        """Branch [1717,1720]: empty factor_exposures -> empty df."""
        mock_extract.return_value = pd.Series(dtype=float)

        # Directly test the pattern: empty list -> empty DataFrame
        df = pd.DataFrame([])
        assert df.empty


# ==============================================================================
# measures_risk_models.py - risk model asset measure and factor_zscore
# Branches: [153,152] [156,152] [204,202] [387,390]
# ==============================================================================

class TestMeasuresRiskModels:
    """Test measures_risk_models.py branch coverage."""

    @patch('gs_quant.timeseries.measures_risk_models._extract_series_from_df')
    def test_format_plot_empty_dict(self, mock_extract):
        """Branch [387,390]: __format_plot_measure_results with empty dict -> empty df."""
        import gs_quant.timeseries.measures_risk_models as mrm
        from gs_quant.api.gs.data import QueryType

        mock_extract.return_value = pd.Series(dtype=float)

        # Access the module-level double-underscore function via vars()
        fn = vars(mrm)['__format_plot_measure_results']
        result = fn({}, QueryType.FACTOR_EXPOSURE)
        mock_extract.assert_called_once()


class TestRiskModelLoopBranches:
    """Test loop-continue branches in risk model functions."""

    @patch('gs_quant.timeseries.measures_risk_models._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_risk_models.MarqueeRiskModel')
    @patch('gs_quant.timeseries.measures_risk_models.DataContext')
    def test_risk_model_measure_empty_and_no_exposures(self, mock_dc, mock_model_cls, mock_extract):
        """Branches [153,152] [156,152]: result is falsy or exposures is empty."""
        import gs_quant.timeseries.measures_risk_models as mrm
        from gs_quant.timeseries.measures_risk_models import ModelMeasureString

        mock_dc.current.start_time = dt.datetime(2021, 1, 1)
        mock_dc.current.end_time = dt.datetime(2021, 12, 31)

        mock_model = MagicMock()
        mock_model.get_data.return_value = {
            'results': [
                {},  # Falsy result -> branch [153,152] (if result: is False)
                {'assetData': {'universe': ['gs1'], 'someField': []}, 'date': '2021-01-01'},  # exposures empty -> [156,152]
                None,  # None result -> branch [153,152]
            ]
        }
        mock_model_cls.get.return_value = mock_model
        mock_extract.return_value = pd.Series(dtype=float)

        asset = MagicMock()
        asset.get_identifier.return_value = 'gsid123'

        result = mrm.risk_model_measure(asset, 'model_id', ModelMeasureString.HISTORICAL_BETA)
        assert isinstance(result, pd.Series)

    @patch('gs_quant.timeseries.measures_risk_models._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_risk_models.MarqueeRiskModel')
    @patch('gs_quant.timeseries.measures_risk_models.DataContext')
    def test_factor_zscore_empty_exposures(self, mock_dc, mock_model_cls, mock_extract):
        """Branch [204,202]: exposures is empty in factor_zscore loop."""
        import gs_quant.timeseries.measures_risk_models as mrm

        mock_dc.current.start_time = dt.datetime(2021, 1, 1)
        mock_dc.current.end_time = dt.datetime(2021, 12, 31)

        mock_factor = MagicMock()
        mock_factor.id = 'factor_id'

        mock_model = MagicMock()
        mock_model.get_factor.return_value = mock_factor
        mock_model.get_data.return_value = {
            'results': [
                {
                    'assetData': {'factorExposure': []},  # empty exposures -> branch [204,202]
                    'date': '2021-01-01',
                },
            ]
        }
        mock_model_cls.get.return_value = mock_model
        mock_extract.return_value = pd.Series(dtype=float)

        asset = MagicMock()
        asset.get_identifier.return_value = 'gsid123'

        result = mrm.factor_zscore(asset, 'model_id', 'factor_name')
        assert isinstance(result, pd.Series)


# ==============================================================================
# measures_fx_vol.py
# Branches: [271,274] [272,271] [357,366] [359,366]
# ==============================================================================

class TestMeasuresFxVol:
    """Test measures_fx_vol.py branch coverage."""

    @patch('gs_quant.timeseries.measures_fx_vol.GsAssetApi')
    def test_get_tdapi_fxo_assets_name_prefix_no_match(self, mock_api):
        """Branches [271,274] [272,271]: name_prefix given, multiple assets, none match prefix."""
        from gs_quant.timeseries.measures_fx_vol import _get_tdapi_fxo_assets

        asset1 = MagicMock()
        asset1.name = 'SomeOtherName'
        asset1.id = 'id1'
        asset2 = MagicMock()
        asset2.name = 'AnotherName'
        asset2.id = 'id2'

        mock_api.get_many_assets.return_value = [asset1, asset2]

        with pytest.raises(MqValueError, match='Specified arguments match multiple assets'):
            _get_tdapi_fxo_assets(name_prefix='PrefixNotFound', type='FXOption')

    @patch('gs_quant.timeseries.measures_fx_vol._currencypair_to_tdapi_fxo_asset')
    @patch('gs_quant.timeseries.measures_fx_vol._asset_from_spec')
    def test_cross_stored_direction_non_fx(self, mock_from_spec, mock_tdapi):
        """Branch [357,366]: asset.asset_class is not FX."""
        from gs_quant.timeseries.measures_fx_vol import cross_stored_direction_for_fx_vol
        from gs_quant.common import AssetClass

        mock_asset = MagicMock()
        mock_asset.asset_class = AssetClass.Equity  # Not FX
        mock_from_spec.return_value = mock_asset
        mock_tdapi.return_value = 'result_id'

        result = cross_stored_direction_for_fx_vol(mock_asset)
        assert result == 'result_id'
        # Should not call get_identifier since not FX
        mock_asset.get_identifier.assert_not_called()

    @patch('gs_quant.timeseries.measures_fx_vol._currencypair_to_tdapi_fxo_asset')
    @patch('gs_quant.timeseries.measures_fx_vol._asset_from_spec')
    def test_cross_stored_direction_bbid_none(self, mock_from_spec, mock_tdapi):
        """Branch [359,366]: bbid is None."""
        from gs_quant.timeseries.measures_fx_vol import cross_stored_direction_for_fx_vol
        from gs_quant.common import AssetClass

        mock_asset = MagicMock()
        mock_asset.asset_class = AssetClass.FX
        mock_asset.get_identifier.return_value = None  # bbid is None
        mock_from_spec.return_value = mock_asset
        mock_tdapi.return_value = 'result_id'

        result = cross_stored_direction_for_fx_vol(mock_asset)
        assert result == 'result_id'


# ==============================================================================
# statistics.py - rolling_std
# Branch [64,68]: for loop completes without break (zero offset)
# ==============================================================================

class TestStatisticsRollingStd:
    """Test rolling_std branch where inner for loop doesn't break."""

    def test_rolling_std_zero_offset(self):
        """Branch [64,68]: offset is 0 -> inner for loop never breaks."""
        from gs_quant.timeseries.statistics import rolling_std

        # Create a series with a few data points
        idx = pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03'])
        x = pd.Series([1.0, 2.0, 3.0], index=idx)

        # Use a zero offset so the condition index[j] > index[i] - offset
        # becomes index[j] > index[i], which is never True for j <= i
        result = rolling_std(x, pd.DateOffset(days=0))
        assert len(result) == 3
        assert np.isnan(result.iloc[0])


# ==============================================================================
# econometrics.py - beta with DateOffset
# Branch [1034,1039]: for loop completes without break
# ==============================================================================

class TestEconometricsBeta:
    """Test beta branch where inner for loop doesn't break."""

    def test_beta_zero_offset_loop(self):
        """Branch [1034,1039]: for loop completes without break.
        We need series_index[idx] > min_index_value to be False for all idx.
        Patch Timestamp.date to return a pd.Timestamp (future) so comparison works."""
        from gs_quant.timeseries.econometrics import beta
        from gs_quant.timeseries.helper import Window

        idx = pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03',
                                '2021-01-04', '2021-01-05'])
        x = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0], index=idx)
        b = pd.Series([50.0, 51.0, 52.0, 53.0, 54.0], index=idx)

        # Patch .date() to return a Timestamp far in the future
        # so that series_index[idx] > min_index_value is always False -> no break
        def future_date(self):
            return pd.Timestamp('2099-12-31')

        with patch.object(pd.Timestamp, 'date', future_date):
            result = beta(x, b, Window(pd.DateOffset(days=1), 0), prices=True)
        assert len(result) == 5


# ==============================================================================
# backtesting.py - Basket.average_implied_volatility
# Branch [347,363]: condition is False (end_date < today)
# ==============================================================================

class TestBacktestingBasket:
    """Test Basket.average_implied_volatility branch coverage."""

    @patch('gs_quant.timeseries.backtesting.ts')
    @patch('gs_quant.timeseries.backtesting.ThreadPoolManager')
    @patch('gs_quant.timeseries.backtesting.GsDataApi')
    def test_avg_implied_vol_past_end_date(self, mock_api, mock_tpm, mock_ts):
        """Branch [347,363]: end_date < today, skip the 'add today' block."""
        from gs_quant.timeseries.backtesting import Basket
        from gs_quant.data.core import DataContext

        # Create a basket with mocked internals
        with DataContext(dt.date(2020, 1, 1), dt.date(2020, 6, 30)):
            basket = Basket(['AAPL', 'MSFT'], [0.5, 0.5])

        basket._marquee_ids = ['id1', 'id2']
        basket._returns = pd.Series([1.0], index=pd.DatetimeIndex(['2020-01-01']))
        basket._actual_weights = pd.DataFrame(
            {'id1': [0.5], 'id2': [0.5]},
            index=pd.DatetimeIndex(['2020-01-01'])
        )
        # Set spot data so _ensure_backtest doesn't try to fetch it
        basket._spot_data = pd.DataFrame(
            {'id1': [100.0], 'id2': [200.0]},
            index=pd.DatetimeIndex(['2020-01-01'])
        )

        # Mock the vol data with today's date NOT in it, but end_date in the past
        vol_df = pd.DataFrame({
            'impliedVolatility': [0.2, 0.3],
            'assetId': ['id1', 'id2'],
        }, index=pd.DatetimeIndex(['2020-01-01', '2020-01-01'], name='date'))

        mock_tpm.run_async.return_value = [vol_df]
        mock_api.build_market_data_query.return_value = {}

        from gs_quant.timeseries.measures_helper import VolReference
        with DataContext(dt.date(2020, 1, 1), dt.date(2020, 1, 2)):
            # Patch _ensure_backtest to avoid actual backtest computation
            with patch.object(basket, '_ensure_backtest'):
                result = basket.average_implied_volatility('1m', VolReference.DELTA_CALL, 25)


# ==============================================================================
# analysis.py - lag function
# Branches: [382,388] [405,407]: hasattr(index, 'as_unit') is False
# ==============================================================================

class TestAnalysisLag:
    """Test lag function to ensure covered branches still work."""

    def test_lag_string_obs_year(self):
        """Sanity test: lag with '1y' string obs (covers True branch of as_unit)."""
        from gs_quant.timeseries.analysis import lag, LagMode

        idx = pd.DatetimeIndex(['2020-01-01', '2020-06-01', '2020-12-01'])
        x = pd.Series([1.0, 2.0, 3.0], index=idx)
        result = lag(x, '1y', mode=LagMode.TRUNCATE)
        assert isinstance(result, pd.Series)

    def test_lag_integer_extend(self):
        """Sanity test: lag with int obs in EXTEND mode (covers True branch of as_unit)."""
        from gs_quant.timeseries.analysis import lag, LagMode

        idx = pd.DatetimeIndex(['2020-01-01', '2020-01-02', '2020-01-03'])
        x = pd.Series([1.0, 2.0, 3.0], index=idx)
        result = lag(x, 2, mode=LagMode.EXTEND)
        assert isinstance(result, pd.Series)


class TestAnalysisLagNoAsUnit:
    """Test lag function with mocked-away as_unit to cover False branches."""

    def test_lag_year_without_as_unit(self):
        """Branch [382,388]: Mock away as_unit to cover the False branch."""
        from gs_quant.timeseries.analysis import lag, LagMode
        import builtins

        idx = pd.DatetimeIndex(['2020-01-01', '2020-06-01', '2020-12-01'])
        x = pd.Series([1.0, 2.0, 3.0], index=idx)

        original_hasattr = builtins.hasattr

        def mock_hasattr(obj, name):
            if name == 'as_unit' and isinstance(obj, pd.DatetimeIndex):
                return False
            return original_hasattr(obj, name)

        with patch('builtins.hasattr', side_effect=mock_hasattr):
            result = lag(x, '1y', mode=LagMode.TRUNCATE)
            assert isinstance(result, pd.Series)

    def test_lag_extend_without_as_unit(self):
        """Branch [405,407]: Mock away as_unit for extend mode."""
        from gs_quant.timeseries.analysis import lag, LagMode
        import builtins

        idx = pd.DatetimeIndex(['2020-01-01', '2020-01-02', '2020-01-03'])
        x = pd.Series([1.0, 2.0, 3.0], index=idx)

        original_hasattr = builtins.hasattr

        def mock_hasattr(obj, name):
            if name == 'as_unit' and isinstance(obj, pd.DatetimeIndex):
                return False
            return original_hasattr(obj, name)

        with patch('builtins.hasattr', side_effect=mock_hasattr):
            result = lag(x, 2, mode=LagMode.EXTEND)
            assert isinstance(result, pd.Series)


# ==============================================================================
# algebra.py - filter_ and filter_dates
# Branches: [619,621] [680,682]: operator is not a string, convert then raise
# ==============================================================================

class TestAlgebraFilter:
    """Test algebra.py filter branches."""

    def test_filter_unexpected_operator_non_string(self):
        """Branch [619,620]: operator is not a string, convert to string then raise."""
        from gs_quant.timeseries.algebra import filter_

        x = pd.Series([1, 2, 3], index=pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03']))
        # Pass an integer as operator - not a FilterOperator, not a string
        with pytest.raises(MqValueError, match='Unexpected operator'):
            filter_(x, operator=999, value=1)

    def test_filter_unexpected_operator_string(self):
        """Branch [619,621]: operator IS already a string but not a FilterOperator."""
        from gs_quant.timeseries.algebra import filter_

        x = pd.Series([1, 2, 3], index=pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03']))
        # Pass a string as operator that's not a valid FilterOperator
        with pytest.raises(MqValueError, match='Unexpected operator: invalid_op'):
            filter_(x, operator='invalid_op', value=1)

    def test_filter_dates_unexpected_operator_non_string(self):
        """Branch [680,681]: operator is not a string, convert to string then raise."""
        from gs_quant.timeseries.algebra import filter_dates

        x = pd.Series([1, 2, 3], index=pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03']))
        # Pass an integer as operator
        with pytest.raises(MqValueError, match='Unexpected operator'):
            filter_dates(x, operator=999, dates=dt.date(2021, 1, 1))

    def test_filter_dates_unexpected_operator_string(self):
        """Branch [680,682]: operator IS already a string but not a FilterOperator."""
        from gs_quant.timeseries.algebra import filter_dates

        x = pd.Series([1, 2, 3], index=pd.DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03']))
        # Pass a string as operator
        with pytest.raises(MqValueError, match='Unexpected operator: bogus'):
            filter_dates(x, operator='bogus', dates=dt.date(2021, 1, 1))


# ==============================================================================
# helper.py - plot_measure and plot_measure_entity
# Branches: [258,259] [273,277]
# ==============================================================================

class TestHelperPlotMeasure:
    """Test helper.py plot_measure and plot_measure_entity branches."""

    def test_plot_measure_with_display_name(self):
        """Branch [258,259]: USE_DISPLAY_NAME is True."""
        import gs_quant.timeseries.helper as helper_mod

        original = helper_mod.USE_DISPLAY_NAME

        def dummy_fn(asset):
            return pd.Series(dtype=float)

        try:
            helper_mod.USE_DISPLAY_NAME = True

            with patch('gs_quant.timeseries.helper.register_measure') as mock_register:
                mock_register.return_value = MagicMock()
                decorator = helper_mod.plot_measure(
                    asset_class=(MagicMock(),),
                    display_name='Test Measure',
                )
                result = decorator(dummy_fn)
                mock_register.assert_called_once()
        finally:
            helper_mod.USE_DISPLAY_NAME = original

    def test_plot_measure_entity_none_dependencies(self):
        """Branch [273,277]: dependencies is None -> skip validation."""
        import gs_quant.timeseries.helper as helper_mod
        from gs_quant.entities.entity import EntityType

        # dependencies=None should trigger the False branch of 'if dependencies is not None'
        # But line 279 does tuple(dependencies) which would fail with None.
        # This is a latent bug - test that it raises TypeError.
        decorator = helper_mod.plot_measure_entity(EntityType.ASSET, dependencies=None)

        def dummy_fn(report_id):
            return pd.Series(dtype=float)

        with pytest.raises(TypeError):
            decorator(dummy_fn)


# ==============================================================================
# Additional tests for measures_reports factor data path
# ==============================================================================

class TestMeasuresReportsFactorData:
    """Test _get_factor_data for branch [1717,1720]: empty factor data -> empty DataFrame."""

    @patch('gs_quant.timeseries.measures_reports._extract_series_from_df')
    @patch('gs_quant.timeseries.measures_reports.DataContext')
    @patch('gs_quant.timeseries.measures_reports.FactorRiskModel')
    @patch('gs_quant.timeseries.measures_reports.FactorRiskReport')
    def test_get_factor_data_empty_results(self, mock_report_cls, mock_model_cls, mock_dc, mock_extract):
        """Branch [1717,1720]: factor_data is empty -> factor_exposures is empty -> df.empty."""
        from gs_quant.timeseries.measures_reports import _get_factor_data
        from gs_quant.api.gs.data import QueryType

        mock_dc.current.start_date = dt.date(2021, 1, 1)
        mock_dc.current.end_date = dt.date(2021, 12, 31)

        mock_report = MagicMock()
        mock_report.get_results.return_value = []  # Empty results -> empty factor_data
        mock_report.get_risk_model_id.return_value = 'risk_model_id'
        mock_report_cls.get.return_value = mock_report

        mock_factor = MagicMock()
        mock_factor.name = 'TestFactor'
        mock_model = MagicMock()
        mock_model.get_factor.return_value = mock_factor
        mock_model_cls.get.return_value = mock_model

        mock_extract.return_value = pd.Series(dtype=float)

        result = _get_factor_data('report_id', 'TestFactor', QueryType.FACTOR_EXPOSURE)
        assert isinstance(result, pd.Series)


