"""
Tests for gs_quant/markets/factor.py targeting 100% branch coverage.
"""
import datetime as dt
import math
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gs_quant.markets.factor import Factor, ReturnFormat
from gs_quant.api.gs.risk_models import (
    RiskModelDataMeasure,
    RiskModelDataAssetsRequest,
    IntradayFactorDataSource,
)
from gs_quant.target.risk_models import RiskModelUniverseIdentifierRequest


# ── ReturnFormat enum ────────────────────────────────────────────────────


class TestReturnFormat:
    def test_values(self):
        assert ReturnFormat.JSON.name == 'JSON'
        assert ReturnFormat.DATA_FRAME.name == 'DATA_FRAME'


# ── Factor properties ────────────────────────────────────────────────────


class TestFactorProperties:
    def test_all_properties(self):
        f = Factor(
            risk_model_id='MODEL1',
            id_='factor_id',
            type_='Style',
            name='Momentum',
            category='Style',
            tooltip='A tooltip',
            description='A description',
            glossary_description='Glossary desc',
        )
        assert f.id == 'factor_id'
        assert f.name == 'Momentum'
        assert f.type == 'Style'
        assert f.category == 'Style'
        assert f.tooltip == 'A tooltip'
        assert f.description == 'A description'
        assert f.glossary_description == 'Glossary desc'
        assert f.risk_model_id == 'MODEL1'

    def test_optional_properties_none(self):
        f = Factor(risk_model_id='M', id_='id', type_='t')
        assert f.name is None
        assert f.category is None
        assert f.tooltip is None
        assert f.description is None
        assert f.glossary_description is None


# ── covariance ────────────────────────────────────────────────────────────


class TestCovariance:
    def _make_covariance_matrix(self):
        """Build a small covariance matrix as multi-indexed DataFrame."""
        dates = [dt.date(2021, 1, 4), dt.date(2021, 1, 5)]
        factor_names = ['Momentum', 'Value']
        arrays = []
        for d in dates:
            for f in factor_names:
                arrays.append((d, f))
        index = pd.MultiIndex.from_tuples(arrays, names=['date', 'factor1'])
        data = {
            'Momentum': [0.01, 0.002, 0.012, 0.003],
            'Value': [0.002, 0.02, 0.003, 0.022],
        }
        return pd.DataFrame(data, index=index)

    @patch('gs_quant.markets.factor.get_covariance_matrix_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_covariance_dataframe(self, mock_api, mock_get_cov):
        """Returns DataFrame format (default)."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        cov_df = self._make_covariance_matrix()
        mock_get_cov.return_value = cov_df

        f1 = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        f2 = Factor(risk_model_id='M', id_='id2', type_='Style', name='Value')

        result = f1.covariance(f2, dt.date(2021, 1, 4), dt.date(2021, 1, 5))
        assert isinstance(result, pd.DataFrame)
        assert 'covariance' in result.columns

    @patch('gs_quant.markets.factor.get_covariance_matrix_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_covariance_json(self, mock_api, mock_get_cov):
        """Returns JSON format."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        cov_df = self._make_covariance_matrix()
        mock_get_cov.return_value = cov_df

        f1 = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        f2 = Factor(risk_model_id='M', id_='id2', type_='Style', name='Value')

        result = f1.covariance(f2, dt.date(2021, 1, 4), dt.date(2021, 1, 5), format=ReturnFormat.JSON)
        assert isinstance(result, dict)


# ── variance ──────────────────────────────────────────────────────────────


class TestVariance:
    @patch.object(Factor, 'covariance')
    def test_variance_dataframe(self, mock_cov):
        """Returns DataFrame format (default)."""
        mock_cov.return_value = pd.DataFrame({'covariance': [0.01, 0.02]},
                                             index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)])

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.variance(dt.date(2021, 1, 4), dt.date(2021, 1, 5))
        assert isinstance(result, pd.DataFrame)
        assert 'variance' in result.columns

    @patch.object(Factor, 'covariance')
    def test_variance_json(self, mock_cov):
        """Returns JSON format."""
        mock_cov.return_value = pd.DataFrame({'covariance': [0.01, 0.02]},
                                             index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)])

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.variance(dt.date(2021, 1, 4), dt.date(2021, 1, 5), format=ReturnFormat.JSON)
        assert isinstance(result, dict)


# ── volatility ────────────────────────────────────────────────────────────


class TestVolatility:
    @patch('gs_quant.markets.factor.build_factor_volatility_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_volatility_dataframe(self, mock_api, mock_build_vol):
        """Returns DataFrame format (default)."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        mock_build_vol.return_value = pd.DataFrame({'Momentum': [0.15, 0.16]},
                                                   index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)])

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.volatility(dt.date(2021, 1, 4), dt.date(2021, 1, 5))
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.factor.build_factor_volatility_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_volatility_json(self, mock_api, mock_build_vol):
        """Returns JSON format."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        mock_build_vol.return_value = pd.DataFrame({'Momentum': [0.15, 0.16]},
                                                   index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)])

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.volatility(dt.date(2021, 1, 4), dt.date(2021, 1, 5), format=ReturnFormat.JSON)
        assert isinstance(result, dict)


# ── correlation ───────────────────────────────────────────────────────────


class TestCorrelation:
    def _make_covariance_matrix(self):
        dates = [dt.date(2021, 1, 4)]
        factor_names = ['Momentum', 'Value']
        arrays = []
        for d in dates:
            for f in factor_names:
                arrays.append((d, f))
        index = pd.MultiIndex.from_tuples(arrays, names=['date', 'factor1'])
        data = {
            'Momentum': [0.04, 0.01],
            'Value': [0.01, 0.09],
        }
        return pd.DataFrame(data, index=index)

    @patch('gs_quant.markets.factor.get_covariance_matrix_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_correlation_dataframe(self, mock_api, mock_get_cov):
        """Returns DataFrame format (default)."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        cov_df = self._make_covariance_matrix()
        mock_get_cov.return_value = cov_df

        f1 = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        f2 = Factor(risk_model_id='M', id_='id2', type_='Style', name='Value')

        result = f1.correlation(f2, dt.date(2021, 1, 4), dt.date(2021, 1, 4))
        assert isinstance(result, pd.DataFrame)
        assert 'correlation' in result.columns

    @patch('gs_quant.markets.factor.get_covariance_matrix_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_correlation_json(self, mock_api, mock_get_cov):
        """Returns JSON format."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        cov_df = self._make_covariance_matrix()
        mock_get_cov.return_value = cov_df

        f1 = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        f2 = Factor(risk_model_id='M', id_='id2', type_='Style', name='Value')

        result = f1.correlation(f2, dt.date(2021, 1, 4), dt.date(2021, 1, 4), format=ReturnFormat.JSON)
        assert isinstance(result, dict)


# ── returns ───────────────────────────────────────────────────────────────


class TestReturns:
    @patch('gs_quant.markets.factor.build_factor_data_map')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_returns_dataframe(self, mock_api, mock_build_map):
        """Returns DataFrame format (default)."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        mock_build_map.return_value = pd.DataFrame(
            {'Momentum': [0.01, 0.02]},
            index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)]
        )
        mock_build_map.return_value.columns.name = 'factorName'

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.returns(dt.date(2021, 1, 4), dt.date(2021, 1, 5))
        assert isinstance(result, pd.DataFrame)
        assert 'return' in result.columns

    @patch('gs_quant.markets.factor.build_factor_data_map')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_returns_json(self, mock_api, mock_build_map):
        """Returns JSON format."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        mock_build_map.return_value = pd.DataFrame(
            {'Momentum': [0.01, 0.02]},
            index=[dt.date(2021, 1, 4), dt.date(2021, 1, 5)]
        )
        mock_build_map.return_value.columns.name = 'factorName'

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.returns(dt.date(2021, 1, 4), dt.date(2021, 1, 5), format=ReturnFormat.JSON)
        assert isinstance(result, dict)


# ── intraday_returns ──────────────────────────────────────────────────────


class TestIntradayReturns:
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_dataframe_with_data(self, mock_api):
        """Normal intraday returns => DataFrame with index set to 'time'."""
        start = dt.datetime(2021, 1, 4, 9, 0, 0)
        end = dt.datetime(2021, 1, 4, 16, 0, 0)
        mock_api.get_risk_model_factor_data_intraday.return_value = [
            {'time': '2021-01-04T09:00:00', 'factorReturn': 0.01,
             'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'},
            {'time': '2021-01-04T10:00:00', 'factorReturn': 0.02,
             'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'},
        ]

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(start, end)
        assert isinstance(result, pd.DataFrame)
        assert 'factorReturn' in result.columns
        # Columns that were dropped
        assert 'factorCategory' not in result.columns
        assert 'factor' not in result.columns
        assert 'factorId' not in result.columns

    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_json(self, mock_api):
        """Intraday returns JSON format."""
        start = dt.datetime(2021, 1, 4, 9, 0, 0)
        end = dt.datetime(2021, 1, 4, 16, 0, 0)
        mock_api.get_risk_model_factor_data_intraday.return_value = [
            {'time': '2021-01-04T09:00:00', 'factorReturn': 0.01,
             'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'},
            {'time': '2021-01-04T10:00:00', 'factorReturn': 0.02,
             'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'},
        ]

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(start, end, format=ReturnFormat.JSON)
        assert isinstance(result, dict)

    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_empty_response(self, mock_api):
        """Empty response => KeyError on set_index => empty DataFrame."""
        start = dt.datetime(2021, 1, 4, 9, 0, 0)
        end = dt.datetime(2021, 1, 4, 16, 0, 0)
        mock_api.get_risk_model_factor_data_intraday.return_value = []

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(start, end)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_multiple_intervals(self, mock_api):
        """Time range spans more than max_interval => multiple API calls."""
        start = dt.datetime(2021, 1, 4, 0, 0, 0)
        end = dt.datetime(2021, 1, 6, 0, 0, 0)  # 2 days > 23h59m59s
        mock_api.get_risk_model_factor_data_intraday.side_effect = [
            [{'time': '2021-01-04T09:00:00', 'factorReturn': 0.01,
              'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'}],
            [{'time': '2021-01-05T09:00:00', 'factorReturn': 0.02,
              'factorCategory': 'Style', 'factor': 'Momentum', 'factorId': 'f1'}],
            [],  # third interval potentially empty
        ]

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(start, end)
        assert isinstance(result, pd.DataFrame)
        assert mock_api.get_risk_model_factor_data_intraday.call_count >= 2

    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_custom_data_source(self, mock_api):
        """Pass custom data_source."""
        start = dt.datetime(2021, 1, 4, 9, 0, 0)
        end = dt.datetime(2021, 1, 4, 16, 0, 0)
        mock_api.get_risk_model_factor_data_intraday.return_value = []

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(start, end, data_source='CUSTOM_SOURCE')
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_intraday_start_equals_end(self, mock_api):
        """start_time == end_time => while loop body never executes."""
        t = dt.datetime(2021, 1, 4, 9, 0, 0)
        mock_api.get_risk_model_factor_data_intraday.return_value = []

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.intraday_returns(t, t)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        mock_api.get_risk_model_factor_data_intraday.assert_not_called()


# ── mimicking_portfolio ───────────────────────────────────────────────────


class TestMimickingPortfolio:
    @patch('gs_quant.markets.factor.build_pfp_data_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_mimicking_portfolio_dataframe(self, mock_api, mock_build_pfp):
        """Returns DataFrame format (default)."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        pfp_df = pd.DataFrame({
            'date': [dt.date(2021, 1, 4), dt.date(2021, 1, 4), dt.date(2021, 1, 5), dt.date(2021, 1, 5)],
            'identifier': ['AAPL', 'GOOG', 'AAPL', 'GOOG'],
            'Momentum': [0.5, 0.3, 0.6, 0.2],
        }).set_index(['date', 'identifier'])
        mock_build_pfp.return_value = pfp_df

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.mimicking_portfolio(dt.date(2021, 1, 4), dt.date(2021, 1, 5))
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.factor.build_pfp_data_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_mimicking_portfolio_json(self, mock_api, mock_build_pfp):
        """Returns JSON format."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        pfp_df = pd.DataFrame({
            'date': [dt.date(2021, 1, 4), dt.date(2021, 1, 4)],
            'identifier': ['AAPL', 'GOOG'],
            'Momentum': [0.5, 0.3],
        }).set_index(['date', 'identifier'])
        mock_build_pfp.return_value = pfp_df

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        result = f.mimicking_portfolio(
            dt.date(2021, 1, 4), dt.date(2021, 1, 4), format=ReturnFormat.JSON
        )
        assert isinstance(result, dict)

    @patch('gs_quant.markets.factor.build_pfp_data_dataframe')
    @patch('gs_quant.markets.factor.GsFactorRiskModelApi')
    def test_mimicking_portfolio_with_assets(self, mock_api, mock_build_pfp):
        """Pass custom assets parameter."""
        mock_api.get_risk_model_data.return_value = {'results': 'raw'}
        pfp_df = pd.DataFrame({
            'date': [dt.date(2021, 1, 4)],
            'identifier': ['AAPL'],
            'Momentum': [0.5],
        }).set_index(['date', 'identifier'])
        mock_build_pfp.return_value = pfp_df

        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        assets = RiskModelDataAssetsRequest(
            identifier=RiskModelUniverseIdentifierRequest.bbid,
            universe=('AAPL',)
        )
        result = f.mimicking_portfolio(
            dt.date(2021, 1, 4), dt.date(2021, 1, 4), assets=assets
        )
        assert isinstance(result, pd.DataFrame)
