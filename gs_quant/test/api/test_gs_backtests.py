"""
Tests for gs_quant.api.gs.backtests - GsBacktestApi, GsBacktestApiAsync
Target: 100% branch coverage
"""

import asyncio
import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.backtests import GsBacktestApi, GsBacktestApiAsync
from gs_quant.errors import MqValueError
from gs_quant.session import DEFAULT_TIMEOUT
from gs_quant.target.backtests import (
    Backtest,
    BacktestResult,
    BacktestRisk,
    BacktestRiskRequest,
    BacktestRefData,
)


def _mock_session():
    session = MagicMock()
    return session


class TestGsBacktestApiGetMany:
    def test_get_many_backtests_defaults(self):
        """All optional params are None"""
        mock_session = _mock_session()
        bt = MagicMock(spec=Backtest)
        mock_session.sync.get.return_value = {'results': (bt,)}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.get_many_backtests()
            url = mock_session.sync.get.call_args[0][0]
            assert 'limit=100' in url
            assert result == (bt,)

    def test_get_many_backtests_all_params(self):
        """All optional params provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsBacktestApi.get_many_backtests(
                limit=50, backtest_id='bt1', owner_id='o1', name='test', mq_symbol='sym1'
            )
            url = mock_session.sync.get.call_args[0][0]
            assert 'id=bt1' in url
            assert 'ownerId=o1' in url
            assert 'name=test' in url
            assert 'mqSymbol=sym1' in url
            assert 'limit=50' in url

    def test_get_many_backtests_partial_params(self):
        """Only some optional params provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsBacktestApi.get_many_backtests(backtest_id='bt1')
            url = mock_session.sync.get.call_args[0][0]
            assert 'id=bt1' in url
            assert 'ownerId' not in url


class TestGsBacktestApiCRUD:
    def test_get_backtest(self):
        mock_session = _mock_session()
        bt = MagicMock(spec=Backtest)
        mock_session.sync.get.return_value = bt
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.get_backtest('bt1')
            mock_session.sync.get.assert_called_once_with('/backtests/bt1', cls=Backtest)
            assert result == bt

    def test_create_backtest(self):
        mock_session = _mock_session()
        bt = MagicMock(spec=Backtest)
        mock_session.sync.post.return_value = bt
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.create_backtest(bt)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/backtests'
            assert call_args[1]['request_headers']['Content-Type'] == 'application/json;charset=utf-8'

    def test_update_backtest(self):
        mock_session = _mock_session()
        bt = MagicMock(spec=Backtest)
        bt.id = 'bt1'
        mock_session.sync.put.return_value = bt
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.update_backtest(bt)
            call_args = mock_session.sync.put.call_args
            assert '/backtests/bt1' == call_args[0][0]

    def test_delete_backtest(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'deleted'}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.delete_backtest('bt1')
            mock_session.sync.delete.assert_called_once_with('/backtests/bt1')
            assert result == {'status': 'deleted'}


class TestGsBacktestApiResults:
    def test_get_results(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'backtestResults': [{'key': 'val'}]}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.get_results('bt1')
            mock_session.sync.get.assert_called_once_with('/backtests/results?id=bt1')
            assert result == [{'key': 'val'}]

    def test_get_comparison_results(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {
            'backtestResults': ['br1'],
            'comparisonResults': ['cr1'],
        }
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            bt_results, comp_results = GsBacktestApi.get_comparison_results(
                limit=50,
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 6, 30),
                backtest_id='bt1',
                comparison_id='comp1',
                owner_id='o1',
                name='test',
                mq_symbol='sym1',
            )
            url = mock_session.sync.get.call_args[0][0]
            assert 'startDate=2023-01-01' in url
            assert 'endDate=2023-06-30' in url
            assert bt_results == ['br1']
            assert comp_results == ['cr1']


class TestGsBacktestApiRun:
    def test_schedule_backtest(self):
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'status': 'scheduled'}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.schedule_backtest('bt1')
            mock_session.sync.post.assert_called_once_with('/backtests/bt1/schedule')
            assert result == {'status': 'scheduled'}

    def test_run_backtest_no_correlation_id(self):
        """Branch: correlation_id is None -> no X-CorrelationId header"""
        mock_session = _mock_session()
        response = {
            'RiskData': {'risk1': [{'date': '2023-01-01', 'value': 1.0}]},
            'Portfolio': [{'pos': 'p1'}],
        }
        mock_session.sync.post.return_value = response
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            bt = MagicMock(spec=Backtest)
            result = GsBacktestApi.run_backtest(bt)
            call_args = mock_session.sync.post.call_args
            headers = call_args[1]['request_headers']
            assert 'X-CorrelationId' not in headers
            assert isinstance(result, BacktestResult)

    def test_run_backtest_with_correlation_id(self):
        """Branch: correlation_id is not None -> adds X-CorrelationId header"""
        mock_session = _mock_session()
        response = {
            'RiskData': {'risk1': [{'date': '2023-01-01', 'value': 1.0}]},
        }
        mock_session.sync.post.return_value = response
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            bt = MagicMock(spec=Backtest)
            result = GsBacktestApi.run_backtest(bt, correlation_id='corr123')
            call_args = mock_session.sync.post.call_args
            headers = call_args[1]['request_headers']
            assert headers['X-CorrelationId'] == 'corr123'

    def test_run_backtest_with_custom_timeout(self):
        """Branch: custom timeout"""
        mock_session = _mock_session()
        response = {
            'RiskData': {'risk1': [{'date': '2023-01-01', 'value': 1.0}]},
        }
        mock_session.sync.post.return_value = response
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            bt = MagicMock(spec=Backtest)
            GsBacktestApi.run_backtest(bt, timeout=300)
            call_args = mock_session.sync.post.call_args
            assert call_args[1]['timeout'] == 300


class TestBacktestResultFromResponse:
    def test_no_risk_data_raises(self):
        """Branch: 'RiskData' not in response -> MqValueError"""
        with pytest.raises(MqValueError, match='No risk data received'):
            GsBacktestApi.backtest_result_from_response({})

    def test_with_portfolio(self):
        """Branch: 'Portfolio' in response -> portfolio is set"""
        response = {
            'RiskData': {'r1': [{'date': '2023-01-01', 'value': 1.0}]},
            'Portfolio': [{'pos': 'p1'}],
        }
        result = GsBacktestApi.backtest_result_from_response(response)
        assert result.portfolio is not None
        assert len(result.risks) == 1
        risk = result.risks[0]
        assert risk.name == 'r1'

    def test_without_portfolio(self):
        """Branch: 'Portfolio' not in response -> portfolio is None"""
        response = {
            'RiskData': {'r1': [{'date': '2023-01-01', 'value': 1.0}]},
        }
        result = GsBacktestApi.backtest_result_from_response(response)
        assert result.portfolio is None
        assert len(result.risks) == 1

    def test_multiple_risk_data_entries(self):
        """Multiple entries in RiskData"""
        response = {
            'RiskData': {
                'r1': [{'date': '2023-01-01', 'value': 1.0}],
                'r2': [{'date': '2023-01-02', 'value': 2.0}, {'date': '2023-01-03', 'value': 3.0}],
            },
        }
        result = GsBacktestApi.backtest_result_from_response(response)
        assert len(result.risks) == 2


class TestGsBacktestApiMisc:
    def test_calculate_position_risk(self):
        mock_session = _mock_session()
        req = MagicMock(spec=BacktestRiskRequest)
        mock_session.sync.post.return_value = {'result': 'data'}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.calculate_position_risk(req)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/backtests/calculate-position-risk'
            assert call_args[1]['timeout'] == DEFAULT_TIMEOUT
            assert result == {'result': 'data'}

    def test_calculate_position_risk_custom_timeout(self):
        mock_session = _mock_session()
        req = MagicMock(spec=BacktestRiskRequest)
        mock_session.sync.post.return_value = {}
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsBacktestApi.calculate_position_risk(req, timeout=120)
            call_args = mock_session.sync.post.call_args
            assert call_args[1]['timeout'] == 120

    def test_get_ref_data(self):
        mock_session = _mock_session()
        ref = MagicMock(spec=BacktestRefData)
        mock_session.sync.get.return_value = ref
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsBacktestApi.get_ref_data()
            mock_session.sync.get.assert_called_once_with('/backtests/refData', cls=BacktestRefData)
            assert result == ref

    def test_update_ref_data(self):
        mock_session = _mock_session()
        ref = MagicMock(spec=BacktestRefData)
        mock_session.sync.put.return_value = ref
        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsBacktestApi.update_ref_data(ref)
            call_args = mock_session.sync.put.call_args
            assert call_args[0][0] == '/backtests/refData'
            assert call_args[1]['request_headers']['Content-Type'] == 'application/json;charset=utf-8'


class TestGsBacktestApiAsync:
    def test_calculate_position_risk_async(self):
        mock_session = _mock_session()
        req = MagicMock(spec=BacktestRiskRequest)

        async def mock_post(*args, **kwargs):
            return {'async_result': 'data'}

        mock_session.async_ = MagicMock()
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsBacktestApiAsync.calculate_position_risk(req))
                assert result == {'async_result': 'data'}
            finally:
                loop.close()

    def test_calculate_position_risk_async_custom_timeout(self):
        mock_session = _mock_session()
        req = MagicMock(spec=BacktestRiskRequest)

        async def mock_post(*args, **kwargs):
            return {}

        mock_session.async_ = MagicMock()
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsBacktestApiAsync.calculate_position_risk(req, timeout=200))
                assert result == {}
            finally:
                loop.close()

    def test_run_backtest_async_no_correlation_id(self):
        """Branch: correlation_id is None"""
        mock_session = _mock_session()
        response = {
            'RiskData': {'r1': [{'date': '2023-01-01', 'value': 1.0}]},
            'Portfolio': [{'p': 1}],
        }

        async def mock_post(*args, **kwargs):
            return response

        mock_session.async_ = MagicMock()
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                bt = MagicMock(spec=Backtest)
                result = loop.run_until_complete(GsBacktestApiAsync.run_backtest(bt))
                assert isinstance(result, BacktestResult)
                assert result.portfolio is not None
            finally:
                loop.close()

    def test_run_backtest_async_with_correlation_id(self):
        """Branch: correlation_id is not None"""
        mock_session = _mock_session()
        response = {
            'RiskData': {'r1': [{'date': '2023-01-01', 'value': 1.0}]},
        }
        captured_kwargs = {}

        async def mock_post(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return response

        mock_session.async_ = MagicMock()
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.backtests.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                bt = MagicMock(spec=Backtest)
                result = loop.run_until_complete(
                    GsBacktestApiAsync.run_backtest(bt, correlation_id='corr456')
                )
                assert isinstance(result, BacktestResult)
                assert captured_kwargs['request_headers']['X-CorrelationId'] == 'corr456'
            finally:
                loop.close()
