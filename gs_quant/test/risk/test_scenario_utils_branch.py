"""
Branch coverage tests for gs_quant/risk/scenario_utils.py
"""
import datetime as dt
from unittest.mock import patch, MagicMock

import pytest


class TestBuildEqVolScenarioIntraday:
    @patch('gs_quant.risk.scenario_utils.MarketDataVolShockScenario')
    @patch('gs_quant.risk.scenario_utils.Dataset')
    @patch('gs_quant.risk.scenario_utils.SecurityMaster')
    def test_build_intraday(self, mock_sm, mock_ds_cls, mock_scenario):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQID123'
        mock_asset.get_identifier.return_value = 'AAPL.OQ'
        mock_sm.get_asset.return_value = mock_asset

        mock_ds = MagicMock()
        mock_ds.get_data.return_value = MagicMock()
        mock_ds_cls.return_value = mock_ds

        mock_scenario.from_dataframe.return_value = 'scenario_result'

        from gs_quant.risk.scenario_utils import build_eq_vol_scenario_intraday
        from gs_quant.markets.securities import AssetIdentifier

        result = build_eq_vol_scenario_intraday(
            asset_name='AAPL.OQ',
            source_dataset='vol_ds',
            ref_spot=150.0,
        )

        mock_sm.get_asset.assert_called_once_with('AAPL.OQ', AssetIdentifier.REUTERS_ID)
        mock_ds_cls.assert_called_once_with('vol_ds')
        mock_asset.get_marquee_id.assert_called_once()
        mock_asset.get_identifier.assert_called_once_with(AssetIdentifier.REUTERS_ID)
        mock_scenario.from_dataframe.assert_called_once()
        assert result == 'scenario_result'

    @patch('gs_quant.risk.scenario_utils.MarketDataVolShockScenario')
    @patch('gs_quant.risk.scenario_utils.Dataset')
    @patch('gs_quant.risk.scenario_utils.SecurityMaster')
    def test_build_intraday_custom_params(self, mock_sm, mock_ds_cls, mock_scenario):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQID456'
        mock_asset.get_identifier.return_value = 'MSFT.OQ'
        mock_sm.get_asset.return_value = mock_asset

        mock_ds = MagicMock()
        mock_ds.get_data.return_value = MagicMock()
        mock_ds_cls.return_value = mock_ds

        mock_scenario.from_dataframe.return_value = 'scenario_result2'

        from gs_quant.risk.scenario_utils import build_eq_vol_scenario_intraday
        from gs_quant.markets.securities import AssetIdentifier

        start = dt.datetime(2021, 6, 1, 10, 0)
        end = dt.datetime(2021, 6, 1, 11, 0)

        result = build_eq_vol_scenario_intraday(
            asset_name='MSFT',
            source_dataset='vol_ds2',
            ref_spot=200.0,
            asset_name_type=AssetIdentifier.BLOOMBERG_ID,
            start_time=start,
            end_time=end,
        )

        mock_sm.get_asset.assert_called_once_with('MSFT', AssetIdentifier.BLOOMBERG_ID)
        call_kwargs = mock_ds.get_data.call_args[1]
        assert call_kwargs['startTime'] == start
        assert call_kwargs['endTime'] == end


class TestBuildEqVolScenarioEod:
    @patch('gs_quant.risk.scenario_utils.MarketDataVolShockScenario')
    @patch('gs_quant.risk.scenario_utils.Dataset')
    @patch('gs_quant.risk.scenario_utils.SecurityMaster')
    def test_build_eod(self, mock_sm, mock_ds_cls, mock_scenario):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQID789'
        mock_asset.get_identifier.return_value = 'SPX.OQ'
        mock_sm.get_asset.return_value = mock_asset

        mock_ds = MagicMock()
        mock_ds.get_data.return_value = MagicMock()
        mock_ds_cls.return_value = mock_ds

        mock_scenario.from_dataframe.return_value = 'eod_result'

        from gs_quant.risk.scenario_utils import build_eq_vol_scenario_eod
        from gs_quant.markets.securities import AssetIdentifier

        result = build_eq_vol_scenario_eod(
            asset_name='SPX.OQ',
            source_dataset='eod_vol_ds',
            ref_spot=4500.0,
        )

        mock_sm.get_asset.assert_called_once_with('SPX.OQ', AssetIdentifier.REUTERS_ID)
        mock_ds_cls.assert_called_once_with('eod_vol_ds')
        mock_scenario.from_dataframe.assert_called_once_with('SPX.OQ', mock_ds.get_data.return_value, 4500.0)
        assert result == 'eod_result'

    @patch('gs_quant.risk.scenario_utils.MarketDataVolShockScenario')
    @patch('gs_quant.risk.scenario_utils.Dataset')
    @patch('gs_quant.risk.scenario_utils.SecurityMaster')
    def test_build_eod_custom_params(self, mock_sm, mock_ds_cls, mock_scenario):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQID_ABC'
        mock_asset.get_identifier.return_value = 'AAPL.OQ'
        mock_sm.get_asset.return_value = mock_asset

        mock_ds = MagicMock()
        mock_ds.get_data.return_value = MagicMock()
        mock_ds_cls.return_value = mock_ds

        mock_scenario.from_dataframe.return_value = 'eod_result2'

        from gs_quant.risk.scenario_utils import build_eq_vol_scenario_eod
        from gs_quant.markets.securities import AssetIdentifier

        vol_date = dt.date(2021, 6, 15)

        result = build_eq_vol_scenario_eod(
            asset_name='AAPL',
            source_dataset='eod_vol_ds2',
            asset_name_type=AssetIdentifier.BLOOMBERG_ID,
            vol_date=vol_date,
        )

        mock_sm.get_asset.assert_called_once_with('AAPL', AssetIdentifier.BLOOMBERG_ID)
        call_kwargs = mock_ds.get_data.call_args[1]
        assert call_kwargs['startDate'] == vol_date
        assert call_kwargs['endDate'] == vol_date

    @patch('gs_quant.risk.scenario_utils.MarketDataVolShockScenario')
    @patch('gs_quant.risk.scenario_utils.Dataset')
    @patch('gs_quant.risk.scenario_utils.SecurityMaster')
    def test_build_eod_no_ref_spot(self, mock_sm, mock_ds_cls, mock_scenario):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQID_XYZ'
        mock_asset.get_identifier.return_value = 'TSLA.OQ'
        mock_sm.get_asset.return_value = mock_asset

        mock_ds = MagicMock()
        mock_ds.get_data.return_value = MagicMock()
        mock_ds_cls.return_value = mock_ds

        mock_scenario.from_dataframe.return_value = 'eod_no_spot'

        from gs_quant.risk.scenario_utils import build_eq_vol_scenario_eod

        result = build_eq_vol_scenario_eod(
            asset_name='TSLA.OQ',
            source_dataset='eod_vol_ds3',
        )

        mock_scenario.from_dataframe.assert_called_once_with('TSLA.OQ', mock_ds.get_data.return_value, None)
        assert result == 'eod_no_spot'
