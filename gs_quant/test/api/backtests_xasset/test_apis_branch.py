"""
Branch coverage tests for gs_quant/api/gs/backtests_xasset/apis.py
"""

import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from gs_quant.api.gs.backtests_xasset.apis import GsBacktestXassetApi, GsBacktestXassetApiAsync


class TestGsBacktestXassetApi:
    """Test GsBacktestXassetApi sync methods."""

    def test_calculate_risk(self):
        """calculate_risk posts request and returns RiskResponse."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {'legRefs': {}, 'riskMeasureRefs': {}, 'results': []}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.sync.post.return_value = mock_response
            with patch('gs_quant.api.gs.backtests_xasset.apis.RiskResponse') as MockRiskResponse:
                MockRiskResponse.from_dict.return_value = MagicMock()
                result = GsBacktestXassetApi.calculate_risk(mock_request)
                MockSession.current.sync.post.assert_called_once_with(
                    '/backtests/xasset/risk',
                    '{"test": "data"}',
                    request_headers={'Accept': 'application/json'},
                    timeout=90,
                )
                MockRiskResponse.from_dict.assert_called_once_with(mock_response)

    def test_calculate_basic_backtest_decode_instruments_true(self):
        """calculate_basic_backtest with decode_instruments=True."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {'measures': {}, 'portfolio': {}, 'transactions': {}, 'additional_results': None}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.sync.post.return_value = mock_response
            with patch('gs_quant.api.gs.backtests_xasset.apis.BasicBacktestResponse') as MockResponse:
                MockResponse.from_dict_custom.return_value = MagicMock()
                result = GsBacktestXassetApi.calculate_basic_backtest(mock_request, decode_instruments=True)
                MockSession.current.sync.post.assert_called_once_with(
                    '/backtests/xasset/strategy/basic',
                    '{"test": "data"}',
                    request_headers={'Accept': 'application/json'},
                    timeout=90,
                )
                MockResponse.from_dict_custom.assert_called_once_with(mock_response, True)

    def test_calculate_basic_backtest_decode_instruments_false(self):
        """calculate_basic_backtest with decode_instruments=False."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {'measures': {}, 'portfolio': {}, 'transactions': {}, 'additional_results': None}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.sync.post.return_value = mock_response
            with patch('gs_quant.api.gs.backtests_xasset.apis.BasicBacktestResponse') as MockResponse:
                MockResponse.from_dict_custom.return_value = MagicMock()
                result = GsBacktestXassetApi.calculate_basic_backtest(mock_request, decode_instruments=False)
                MockResponse.from_dict_custom.assert_called_once_with(mock_response, False)


class TestGsBacktestXassetApiAsync:
    """Test GsBacktestXassetApiAsync async methods."""

    @pytest.mark.asyncio
    async def test_calculate_risk_async(self):
        """Async calculate_risk posts request and returns RiskResponse."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {'legRefs': {}, 'riskMeasureRefs': {}, 'results': []}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.async_.post = AsyncMock(return_value=mock_response)
            with patch('gs_quant.api.gs.backtests_xasset.apis.RiskResponse') as MockRiskResponse:
                MockRiskResponse.from_dict.return_value = MagicMock()
                result = await GsBacktestXassetApiAsync.calculate_risk(mock_request)
                MockSession.current.async_.post.assert_called_once_with(
                    '/backtests/xasset/risk',
                    '{"test": "data"}',
                    request_headers={'Accept': 'application/json'},
                    timeout=90,
                )
                MockRiskResponse.from_dict.assert_called_once_with(mock_response)

    @pytest.mark.asyncio
    async def test_calculate_basic_backtest_async(self):
        """Async calculate_basic_backtest."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {'measures': {}, 'portfolio': {}, 'transactions': {}, 'additional_results': None}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.async_.post = AsyncMock(return_value=mock_response)
            with patch('gs_quant.api.gs.backtests_xasset.apis.BasicBacktestResponse') as MockResponse:
                MockResponse.from_dict_custom.return_value = MagicMock()
                result = await GsBacktestXassetApiAsync.calculate_basic_backtest(
                    mock_request, decode_instruments=True
                )
                MockSession.current.async_.post.assert_called_once_with(
                    '/backtests/xasset/strategy/basic',
                    '{"test": "data"}',
                    request_headers={'Accept': 'application/json'},
                    timeout=90,
                )
                MockResponse.from_dict_custom.assert_called_once_with(mock_response, True)

    @pytest.mark.asyncio
    async def test_calculate_basic_backtest_async_no_decode(self):
        """Async calculate_basic_backtest with decode_instruments=False."""
        mock_request = MagicMock()
        mock_request.to_json.return_value = '{"test": "data"}'

        mock_response = {}

        with patch('gs_quant.api.gs.backtests_xasset.apis.GsSession') as MockSession:
            MockSession.current.async_.post = AsyncMock(return_value=mock_response)
            with patch('gs_quant.api.gs.backtests_xasset.apis.BasicBacktestResponse') as MockResponse:
                MockResponse.from_dict_custom.return_value = MagicMock()
                result = await GsBacktestXassetApiAsync.calculate_basic_backtest(
                    mock_request, decode_instruments=False
                )
                MockResponse.from_dict_custom.assert_called_once_with(mock_response, False)
