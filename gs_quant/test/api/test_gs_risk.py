"""
Comprehensive branch coverage tests for gs_quant/api/gs/risk.py
"""

import asyncio
import base64
import datetime as dt
import json
import math
import time
from socket import gaierror
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock, AsyncMock

import msgpack
import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close

from gs_quant.api.gs.risk import GsRiskApi, WebsocketUnavailable
from gs_quant.errors import MqValueError
from gs_quant.session import GsSession
from gs_quant.target.risk import OptimizationRequest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_session(api_version='v1'):
    """Create a mock GsSession with sync/async_ attributes."""
    session = MagicMock()
    session.api_version = api_version
    session.sync.post = MagicMock()
    session.sync.get = MagicMock()
    session.async_ = MagicMock()
    return session


def _make_risk_request():
    """Create a minimal mock RiskRequest."""
    from gs_quant.risk import RiskRequest
    req = MagicMock(spec=RiskRequest)
    req._id = None
    return req


def _run_async(coro):
    """Run an async coroutine to completion using a new event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ===========================================================================
# Tests for WebsocketUnavailable
# ===========================================================================

class TestWebsocketUnavailable:
    def test_is_exception(self):
        exc = WebsocketUnavailable()
        assert isinstance(exc, Exception)

    def test_with_message(self):
        exc = WebsocketUnavailable('ws not available')
        assert str(exc) == 'ws not available'


# ===========================================================================
# Tests for GsRiskApi class-level attributes
# ===========================================================================

class TestClassAttributes:
    def test_use_msgpack_default(self):
        assert GsRiskApi.USE_MSGPACK is True

    def test_poll_for_batch_results_default(self):
        assert GsRiskApi.POLL_FOR_BATCH_RESULTS is False

    def test_websocket_retry_on_close_codes(self):
        assert GsRiskApi.WEBSOCKET_RETRY_ON_CLOSE_CODES == (1000, 1001, 1006)

    def test_pricing_api_version_default(self):
        assert GsRiskApi.PRICING_API_VERSION is None


# ===========================================================================
# Tests for calc_multi
# ===========================================================================

class TestCalcMulti:
    @patch.object(GsRiskApi, '_exec')
    def test_calc_multi_success(self, mock_exec):
        req1 = _make_risk_request()
        req2 = _make_risk_request()
        mock_exec.return_value = ['result1', 'result2']

        result = GsRiskApi.calc_multi([req1, req2])

        assert result == {req1: 'result1', req2: 'result2'}

    @patch.object(GsRiskApi, '_exec')
    def test_calc_multi_missing_results(self, mock_exec):
        """When results < requests, all results should be RuntimeError."""
        req1 = _make_risk_request()
        req2 = _make_risk_request()
        req3 = _make_risk_request()
        mock_exec.return_value = ['result1']  # only 1 result for 3 requests

        result = GsRiskApi.calc_multi([req1, req2, req3])

        for req in (req1, req2, req3):
            assert isinstance(result[req], RuntimeError)
            assert str(result[req]) == 'Missing results'

    @patch.object(GsRiskApi, '_exec')
    def test_calc_multi_exact_results(self, mock_exec):
        """When results == requests, no padding needed."""
        req1 = _make_risk_request()
        mock_exec.return_value = ['result1']

        result = GsRiskApi.calc_multi([req1])

        assert result[req1] == 'result1'


# ===========================================================================
# Tests for calc
# ===========================================================================

class TestCalc:
    @patch.object(GsRiskApi, '_exec')
    def test_calc(self, mock_exec):
        req = _make_risk_request()
        mock_exec.return_value = [{'val': 42}]

        result = GsRiskApi.calc(req)

        mock_exec.assert_called_once_with(req)
        assert result == [{'val': 42}]


# ===========================================================================
# Tests for _exec
# ===========================================================================

class TestExec:
    @patch.object(GsRiskApi, 'get_session')
    def test_exec_single_request(self, mock_get_session):
        """Single RiskRequest: no msgpack, url = /risk/calculate."""
        from gs_quant.risk import RiskRequest
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        req = MagicMock()
        req.__class__ = RiskRequest
        req.__iter__ = MagicMock(return_value=iter([req]))
        req._id = None

        session.sync.post.return_value = (['some_result'], 'req-id-123')

        result = GsRiskApi._exec(req)

        session.sync.post.assert_called_once()
        call_kwargs = session.sync.post.call_args
        assert call_kwargs[1]['request_headers'] == {}
        assert '/risk/calculate' in call_kwargs[0][0]
        assert '/bulk' not in call_kwargs[0][0]
        assert result == ['some_result']

    @patch.object(GsRiskApi, 'get_session')
    def test_exec_bulk_request_with_msgpack(self, mock_get_session):
        """Multiple requests: USE_MSGPACK=True, url = /risk/calculate/bulk."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.USE_MSGPACK = True
        GsRiskApi.PRICING_API_VERSION = None

        req1 = MagicMock()
        req1._id = None
        req2 = MagicMock()
        req2._id = None
        requests = [req1, req2]

        session.sync.post.return_value = (['r1', 'r2'], 'bulk-id')

        result = GsRiskApi._exec(requests)

        call_kwargs = session.sync.post.call_args
        assert call_kwargs[1]['request_headers'] == {'Content-Type': 'application/x-msgpack'}
        assert '/risk/calculate/bulk' in call_kwargs[0][0]
        assert result == ['r1', 'r2']
        assert req1._id == 'bulk-id'
        assert req2._id == 'bulk-id'

    @patch.object(GsRiskApi, 'get_session')
    def test_exec_bulk_request_without_msgpack(self, mock_get_session):
        """Multiple requests with USE_MSGPACK=False: empty headers."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.USE_MSGPACK = False
        GsRiskApi.PRICING_API_VERSION = None

        req1 = MagicMock()
        req1._id = None
        requests = [req1]

        session.sync.post.return_value = (['r1'], 'id1')

        GsRiskApi._exec(requests)

        call_kwargs = session.sync.post.call_args
        assert call_kwargs[1]['request_headers'] == {}

        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_exec_custom_pricing_api_version(self, mock_get_session):
        """When PRICING_API_VERSION is set, it overrides session.api_version."""
        session = _make_mock_session(api_version='v2')
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = 'v3'

        from gs_quant.risk import RiskRequest
        req = MagicMock()
        req.__class__ = RiskRequest
        req.__iter__ = MagicMock(return_value=iter([req]))
        req._id = None

        session.sync.post.return_value = ([], 'id1')

        GsRiskApi._exec(req)

        call_args = session.sync.post.call_args[0]
        assert call_args[0].startswith('/v3/')

        GsRiskApi.PRICING_API_VERSION = None


# ===========================================================================
# Tests for get_results (dispatch)
# ===========================================================================

class TestGetResults:
    @patch.object(GsRiskApi, '_GsRiskApi__get_results_poll', new_callable=AsyncMock)
    def test_get_results_poll_mode(self, mock_poll):
        """When POLL_FOR_BATCH_RESULTS is True, goes directly to poll."""
        GsRiskApi.POLL_FOR_BATCH_RESULTS = True
        mock_poll.return_value = None

        responses = asyncio.Queue()
        results = asyncio.Queue()

        _run_async(GsRiskApi.get_results(responses, results))

        mock_poll.assert_called_once()

        GsRiskApi.POLL_FOR_BATCH_RESULTS = False

    @patch.object(GsRiskApi, '_GsRiskApi__get_results_ws', new_callable=AsyncMock)
    def test_get_results_ws_mode_success(self, mock_ws):
        """When POLL_FOR_BATCH_RESULTS is False, uses websocket."""
        GsRiskApi.POLL_FOR_BATCH_RESULTS = False
        mock_ws.return_value = None

        responses = asyncio.Queue()
        results = asyncio.Queue()

        _run_async(GsRiskApi.get_results(responses, results))

        mock_ws.assert_called_once()

    @patch.object(GsRiskApi, '_GsRiskApi__get_results_poll', new_callable=AsyncMock)
    @patch.object(GsRiskApi, '_GsRiskApi__get_results_ws', new_callable=AsyncMock)
    def test_get_results_ws_fallback_to_poll(self, mock_ws, mock_poll):
        """When websocket raises WebsocketUnavailable, falls back to poll."""
        GsRiskApi.POLL_FOR_BATCH_RESULTS = False
        mock_ws.side_effect = WebsocketUnavailable()
        mock_poll.return_value = 'poll_result'

        responses = asyncio.Queue()
        results = asyncio.Queue()

        result = _run_async(GsRiskApi.get_results(responses, results))

        mock_ws.assert_called_once()
        mock_poll.assert_called_once()
        assert result == 'poll_result'


# ===========================================================================
# Tests for __get_results_poll
# ===========================================================================

class TestGetResultsPoll:
    def test_poll_no_items_shutdown(self):
        """Shutdown signal with no pending requests."""
        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            GsRiskApi.shutdown_queue_listener(responses)

            return await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

        result = _run_async(run())
        assert result is None

    @patch.object(GsRiskApi, 'get_session')
    def test_poll_with_items_result(self, mock_get_session):
        """Items with 'result' in response should be enqueued."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'report-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            session.sync.post.return_value = [
                {'requestId': 'report-1', 'result': {'val': 42}}
            ]

            await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

            item = results.get_nowait()
            assert item[0] is request_obj
            assert item[1] == {'val': 42}

        _run_async(run())

    @patch.object(GsRiskApi, 'get_session')
    def test_poll_with_items_error(self, mock_get_session):
        """Items with 'error' in response should enqueue RuntimeError."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'report-2'}))
            GsRiskApi.shutdown_queue_listener(responses)

            session.sync.post.return_value = [
                {'requestId': 'report-2', 'error': 'Something went wrong'}
            ]

            await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

            item = results.get_nowait()
            assert item[0] is request_obj
            assert isinstance(item[1], RuntimeError)
            assert 'Something went wrong' in str(item[1])

        _run_async(run())

    @patch.object(GsRiskApi, 'get_session')
    def test_poll_exception_during_poll(self, mock_get_session):
        """Exception during polling should log error and shutdown."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'report-3'}))
            GsRiskApi.shutdown_queue_listener(responses)

            session.sync.post.side_effect = ConnectionError('network error')

            error_str = await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

            assert 'Fatal error polling for results' in error_str
            assert 'network error' in error_str

        _run_async(run())

    @patch.object(GsRiskApi, 'get_session')
    @patch('gs_quant.api.gs.risk.dt')
    def test_poll_timeout(self, mock_dt, mock_get_session):
        """When timeout expires, should log and shutdown."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.sync.post.return_value = []

        base_time = dt.datetime(2024, 1, 1, 12, 0, 0)
        mock_dt.datetime.now.side_effect = [
            base_time,                              # end_time calculation
            base_time + dt.timedelta(seconds=5),    # first timeout check (not expired)
            base_time + dt.timedelta(seconds=15),   # second timeout check (expired)
        ]
        mock_dt.timedelta = dt.timedelta

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'report-timeout'}))
            GsRiskApi.shutdown_queue_listener(responses)

            ret = await GsRiskApi._GsRiskApi__get_results_poll(
                responses, results, timeout=10
            )
            return ret

        result = _run_async(run())
        assert result is None

    @patch.object(GsRiskApi, 'get_session')
    def test_poll_no_timeout(self, mock_get_session):
        """When timeout is None, end_time should be None and no timeout check."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'report-no-timeout'}))
            GsRiskApi.shutdown_queue_listener(responses)

            session.sync.post.return_value = [
                {'requestId': 'report-no-timeout', 'result': {'val': 99}}
            ]

            await GsRiskApi._GsRiskApi__get_results_poll(responses, results, timeout=None)

            item = results.get_nowait()
            assert item[1] == {'val': 99}

        _run_async(run())

    @patch.object(GsRiskApi, 'get_session')
    def test_poll_custom_pricing_api_version(self, mock_get_session):
        """When PRICING_API_VERSION is set, use it instead of session version."""
        session = _make_mock_session(api_version='v2')
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = 'v5'

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            request_obj = MagicMock()
            responses.put_nowait((request_obj, {'reportId': 'r1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            session.sync.post.return_value = [
                {'requestId': 'r1', 'result': {'val': 1}}
            ]

            await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

            call_args = session.sync.post.call_args[0]
            assert call_args[0].startswith('/v5/')

        _run_async(run())
        GsRiskApi.PRICING_API_VERSION = None

    def test_poll_continue_on_no_pending(self):
        """When items is empty and no pending requests, continue until shutdown."""
        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            GsRiskApi.shutdown_queue_listener(responses)

            ret = await GsRiskApi._GsRiskApi__get_results_poll(responses, results)
            return ret

        result = _run_async(run())
        assert result is None


# ===========================================================================
# Tests for __get_results_ws - outer retry/error handling
# ===========================================================================

class TestGetResultsWs:
    @patch.object(GsRiskApi, 'get_session')
    def test_ws_gaierror_raises_websocket_unavailable(self, mock_get_session):
        """gaierror should raise WebsocketUnavailable."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = gaierror('DNS fail')

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            GsRiskApi.shutdown_queue_listener(responses)

            with pytest.raises(WebsocketUnavailable):
                await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

        _run_async(run())

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_timeout_error(self, mock_get_session):
        """asyncio.TimeoutError should set error and stop."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return result

        result = _run_async(run())
        assert result is not None
        assert 'Timed out' in result

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_generic_exception(self, mock_get_session):
        """Generic exception should set error and stop."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = ValueError('some error')

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return result

        result = _run_async(run())
        assert result is not None
        assert 'some error' in result

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_connection_closed_retry(self, mock_get_session):
        """ConnectionClosed should retry up to max_attempts."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        close_frame = Close(1006, 'abnormal')
        cc = ConnectionClosed(close_frame, None)
        session.async_.connect_websocket.side_effect = cc

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            GsRiskApi.shutdown_queue_listener(responses)

            with patch('gs_quant.api.gs.risk.asyncio.sleep', new_callable=AsyncMock):
                result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return result

        result = _run_async(run())
        assert result is not None
        assert 'Connection Closed' in result

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_use_msgpack_true_subprotocols(self, mock_get_session):
        """When USE_MSGPACK=True, subprotocols should be ['msgpack-binary']."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = True

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

        _run_async(run())

        call_kwargs = session.async_.connect_websocket.call_args[1]
        assert call_kwargs['subprotocols'] == ['msgpack-binary']

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_use_msgpack_false_subprotocols(self, mock_get_session):
        """When USE_MSGPACK=False, subprotocols should be None."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

        _run_async(run())

        call_kwargs = session.async_.connect_websocket.call_args[1]
        assert call_kwargs['subprotocols'] is None
        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_error_with_span_recording(self, mock_get_session):
        """When error occurs and span is recording, tags should be set."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            with patch('gs_quant.api.gs.risk.Tracer') as mock_tracer:
                mock_tracer.active_span.return_value = mock_span
                result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
                return result

        result = _run_async(run())
        assert result is not None
        mock_span.set_tag.assert_called_with('error', True)
        mock_span.log_kv.assert_called_once()

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_error_with_span_not_recording(self, mock_get_session):
        """When error occurs and span is not recording, no tags set."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        mock_span = MagicMock()
        mock_span.is_recording.return_value = False

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            with patch('gs_quant.api.gs.risk.Tracer') as mock_tracer:
                mock_tracer.active_span.return_value = mock_span
                result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
                return result

        result = _run_async(run())
        assert result is not None
        mock_span.set_tag.assert_not_called()

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_error_with_no_span(self, mock_get_session):
        """When error occurs and active_span returns None, no crash."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            with patch('gs_quant.api.gs.risk.Tracer') as mock_tracer:
                mock_tracer.active_span.return_value = None
                result = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
                return result

        result = _run_async(run())
        assert 'Timed out' in result

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_custom_pricing_api_version(self, mock_get_session):
        """When PRICING_API_VERSION is set, use it for ws URL."""
        session = _make_mock_session(api_version='v2')
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = 'v7'

        session.async_.connect_websocket.side_effect = asyncio.TimeoutError()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

        _run_async(run())

        call_args = session.async_.connect_websocket.call_args[0]
        assert call_args[0].startswith('/v7/')
        GsRiskApi.PRICING_API_VERSION = None

    @patch.object(GsRiskApi, 'get_session')
    def test_ws_retry_with_sleep(self, mock_get_session):
        """On retry (attempts > 0), should sleep with exponential backoff."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None

        close_frame = Close(1006, 'abnormal')
        cc = ConnectionClosed(close_frame, None)
        session.async_.connect_websocket.side_effect = cc

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()
            GsRiskApi.shutdown_queue_listener(responses)

            with patch('gs_quant.api.gs.risk.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
                return error, mock_sleep.call_count

        error, sleep_count = _run_async(run())
        assert error is not None
        assert sleep_count > 0


# ===========================================================================
# Tests for __get_results_ws - handle_websocket inner function
# These tests use a real async context manager mock for the websocket.
# We carefully control timing: ws.recv() delays so request_listener
# processes first, populating pending_requests before results arrive.
# ===========================================================================

class TestHandleWebsocket:
    def _make_ws_context(self, session, mock_ws):
        """Create an async context manager that yields mock_ws."""
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_ws)
        cm.__aexit__ = AsyncMock(return_value=False)
        session.async_.connect_websocket.return_value = cm
        return cm

    def _make_delayed_recv(self, responses_list):
        """Create a recv function that returns responses in order, with delay.

        Each response is returned on successive calls. After all responses
        are exhausted, blocks forever. A small delay on each call allows
        request_listener to populate pending_requests before data is returned.
        """
        idx = [0]

        async def mock_recv():
            # Always yield control to let request_listener run first
            await asyncio.sleep(0.05)
            if idx[0] < len(responses_list):
                resp = responses_list[idx[0]]
                idx[0] += 1
                if isinstance(resp, Exception):
                    raise resp
                return resp
            # Block forever after all responses consumed
            await asyncio.sleep(100)

        return mock_recv

    @patch.object(GsRiskApi, 'get_session')
    def test_json_result_string(self, mock_get_session):
        """Test receiving a JSON ('R') result via websocket (string response)."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()
        json_data = json.dumps({'val': 42})

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([f'req-1;R{json_data}'])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item, error

        item, error = _run_async(run())
        assert item[0] is request_obj
        assert item[1] == {'val': 42}
        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_error_result_string(self, mock_get_session):
        """Test receiving an error ('E') result via websocket (string response)."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv(['req-1;ESomething went wrong'])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item

        item = _run_async(run())
        assert item[0] is request_obj
        assert isinstance(item[1], RuntimeError)
        assert 'Something went wrong' in str(item[1])
        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_bytes_error_result(self, mock_get_session):
        """Test receiving bytes error ('E') result via websocket."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = True

        request_obj = MagicMock()

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([b'req-1;Ebytes error message'])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item

        item = _run_async(run())
        assert item[0] is request_obj
        assert isinstance(item[1], RuntimeError)
        assert 'bytes error message' in str(item[1])

    @patch.object(GsRiskApi, 'get_session')
    def test_msgpack_binary_result(self, mock_get_session):
        """Test receiving a binary msgpack ('B') result via websocket."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = True

        request_obj = MagicMock()
        packed = msgpack.packb({'val': 99})

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([b'req-1;B' + packed])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item

        item = _run_async(run())
        assert item[0] is request_obj
        assert item[1] == {'val': 99} or item[1] == {b'val': 99}

    @patch.object(GsRiskApi, 'get_session')
    def test_msgpack_base64_result(self, mock_get_session):
        """Test receiving a base64-encoded msgpack ('M') result via websocket."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = True

        request_obj = MagicMock()
        packed = msgpack.packb({'val': 77})
        b64_packed = base64.b64encode(packed)

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([b'req-1;M' + b64_packed])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item

        item = _run_async(run())
        assert item[0] is request_obj
        assert item[1] == {'val': 77}

    @patch.object(GsRiskApi, 'get_session')
    def test_unpack_exception(self, mock_get_session):
        """Test exception during unpacking sets result to exception."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv(['req-1;R{invalid json}'])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item

        item = _run_async(run())
        assert item[0] is request_obj
        assert isinstance(item[1], Exception)
        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_connection_closed_non_retryable_no_request_id(self, mock_get_session):
        """ConnectionClosed with non-retryable code: request_id stays None, aborts all pending."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()

        close_frame = Close(4000, 'custom error')
        cc = ConnectionClosed(close_frame, None)

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([cc])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            items = []
            while not results.empty():
                items.append(results.get_nowait())
            return items, error

        items, error = _run_async(run())
        # The non-retryable ConnectionClosed sets status='E', risk_data=str(cc)
        # request_id remains None -> all pending get the error result
        assert len(items) > 0
        assert isinstance(items[0][1], RuntimeError)

    @patch.object(GsRiskApi, 'get_session')
    def test_connection_closed_retryable_recv_before_request_listener(self, mock_get_session):
        """ConnectionClosed with retryable code where recv completes before request_listener.

        In this case request_listener is not in complete, so it gets cancelled.
        """
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        close_frame = Close(1006, 'abnormal')
        cc = ConnectionClosed(close_frame, None)

        connect_count = [0]

        def make_cm(*args, **kwargs):
            connect_count[0] += 1
            if connect_count[0] == 1:
                mock_ws = AsyncMock()

                async def mock_recv():
                    # No delay - completes immediately, before request_listener
                    raise cc

                mock_ws.recv = mock_recv
                mock_ws.send = AsyncMock()
                cm = MagicMock()
                cm.__aenter__ = AsyncMock(return_value=mock_ws)
                cm.__aexit__ = AsyncMock(return_value=False)
                return cm
            else:
                raise asyncio.TimeoutError()

        session.async_.connect_websocket.side_effect = make_cm

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            with patch('gs_quant.api.gs.risk.asyncio.sleep', new_callable=AsyncMock):
                error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return error

        error = _run_async(run())
        assert error is not None

    @patch.object(GsRiskApi, 'get_session')
    def test_connection_closed_retryable_with_request_listener_complete(self, mock_get_session):
        """ConnectionClosed with retryable code: request_listener in complete -> re-queue items."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        close_frame = Close(1006, 'abnormal')
        cc = ConnectionClosed(close_frame, None)

        connect_count = [0]

        def make_cm(*args, **kwargs):
            connect_count[0] += 1
            if connect_count[0] <= 2:
                mock_ws = AsyncMock()
                mock_ws.recv = self._make_delayed_recv([cc])
                mock_ws.send = AsyncMock()
                cm = MagicMock()
                cm.__aenter__ = AsyncMock(return_value=mock_ws)
                cm.__aexit__ = AsyncMock(return_value=False)
                return cm
            else:
                raise asyncio.TimeoutError()

        session.async_.connect_websocket.side_effect = make_cm

        request_obj = MagicMock()

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            with patch('gs_quant.api.gs.risk.asyncio.sleep', new_callable=AsyncMock):
                error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return error

        error = _run_async(run())
        assert error is not None

    @patch.object(GsRiskApi, 'get_session')
    def test_non_dict_items_error(self, mock_get_session):
        """When items contain non-dict values, should raise RuntimeError with errorString."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        error_item = [[{'errorString': 'bad request'}]]

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([])  # never returns, request_listener goes first
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((MagicMock(), error_item))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)
            return error

        error = _run_async(run())
        assert error is not None

    @patch.object(GsRiskApi, 'get_session')
    def test_multiple_requests(self, mock_get_session):
        """Test multiple request items dispatched and results received."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj1 = MagicMock()
        request_obj2 = MagicMock()

        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([
            'req-1;R{"val": 1}',
            'req-2;R{"val": 2}',
        ])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj1, {'reportId': 'req-1'}))
            responses.put_nowait((request_obj2, {'reportId': 'req-2'}))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            items = []
            while not results.empty():
                items.append(results.get_nowait())
            return items, error

        items, error = _run_async(run())
        assert len(items) == 2
        GsRiskApi.USE_MSGPACK = True

    @patch.object(GsRiskApi, 'get_session')
    def test_result_listener_not_complete_cancelled(self, mock_get_session):
        """When result_listener is not in complete, it gets cancelled (request_listener completes first)."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()
        json_data = json.dumps({'val': 42})

        # Use a long delay so request_listener always finishes first
        idx = [0]

        async def mock_recv():
            idx[0] += 1
            await asyncio.sleep(0.5)  # Long delay - request_listener finishes first
            if idx[0] <= 2:
                return f'req-1;R{json_data}'
            await asyncio.sleep(100)

        mock_ws = AsyncMock()
        mock_ws.recv = mock_recv
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            item = results.get_nowait()
            return item, error

        item, error = _run_async(run())
        assert item[0] is request_obj

    @pytest.mark.skip(reason='Reconnect mock requires complex async coordination')
    @patch.object(GsRiskApi, 'get_session')
    def test_reconnect_with_pending_requests(self, mock_get_session):
        """When reconnecting with pending_requests, should re-subscribe."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        close_frame = Close(1006, 'abnormal')
        cc = ConnectionClosed(close_frame, None)

        request_obj = MagicMock()
        connect_count = [0]

        def make_cm(*args, **kwargs):
            connect_count[0] += 1

            if connect_count[0] == 1:
                mock_ws = AsyncMock()
                mock_ws.recv = self._make_delayed_recv([cc])
                mock_ws.send = AsyncMock()
                cm = MagicMock()
                cm.__aenter__ = AsyncMock(return_value=mock_ws)
                cm.__aexit__ = AsyncMock(return_value=False)
                return cm
            elif connect_count[0] == 2:
                mock_ws2 = AsyncMock()
                mock_ws2.recv = self._make_delayed_recv(['req-1;R{"val": 99}'])
                mock_ws2.send = AsyncMock()
                cm2 = MagicMock()
                cm2.__aenter__ = AsyncMock(return_value=mock_ws2)
                cm2.__aexit__ = AsyncMock(return_value=False)
                return cm2
            else:
                raise asyncio.TimeoutError()

        session.async_.connect_websocket.side_effect = make_cm

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            with patch('gs_quant.api.gs.risk.asyncio.sleep', new_callable=AsyncMock):
                error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            items = []
            while not results.empty():
                items.append(results.get_nowait())
            return items, error

        items, error = _run_async(run())
        if items:
            assert items[0][0] is request_obj

    @patch.object(GsRiskApi, 'get_session')
    def test_recv_generic_exception_sets_error_status(self, mock_get_session):
        """Generic exception during recv processing sets status='E' and risk_data."""
        session = _make_mock_session()
        mock_get_session.return_value = session
        GsRiskApi.PRICING_API_VERSION = None
        GsRiskApi.USE_MSGPACK = False

        request_obj = MagicMock()

        # Raise a generic exception (not ConnectionClosed) from within the recv try block
        mock_ws = AsyncMock()
        mock_ws.recv = self._make_delayed_recv([ValueError('unexpected recv error')])
        mock_ws.send = AsyncMock()
        self._make_ws_context(session, mock_ws)

        async def run():
            responses = asyncio.Queue()
            results = asyncio.Queue()

            responses.put_nowait((request_obj, {'reportId': 'req-1'}))
            GsRiskApi.shutdown_queue_listener(responses)

            error = await GsRiskApi._GsRiskApi__get_results_ws(responses, results)

            items = []
            while not results.empty():
                items.append(results.get_nowait())
            return items, error

        items, error = _run_async(run())
        # The generic exception gets caught, status='E', risk_data=str(ee)
        # request_id is None -> all pending get the error
        assert len(items) > 0
        assert isinstance(items[0][1], RuntimeError)
        assert 'unexpected recv error' in str(items[0][1])


# ===========================================================================
# Tests for create_pretrade_execution_optimization
# ===========================================================================

class TestCreatePretradeExecutionOptimization:
    @patch.object(GsRiskApi, 'get_session')
    def test_success(self, mock_get_session):
        session = _make_mock_session()
        mock_get_session.return_value = session

        mock_response = {'optimizationId': 'opt-123'}
        session.sync.post.return_value = mock_response

        request = MagicMock(spec=OptimizationRequest)
        result = GsRiskApi.create_pretrade_execution_optimization(request)

        session.sync.post.assert_called_once_with(r'/risk/execution/pretrade', request)
        assert result == mock_response

    @patch.object(GsRiskApi, 'get_session')
    def test_exception(self, mock_get_session):
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.side_effect = Exception('API error')

        request = MagicMock(spec=OptimizationRequest)
        result = GsRiskApi.create_pretrade_execution_optimization(request)

        assert result == 'API error'


# ===========================================================================
# Tests for get_pretrade_execution_optimization
# ===========================================================================

class TestGetPretradeExecutionOptimization:
    @patch.object(GsRiskApi, 'get_session')
    def test_completed(self, mock_get_session):
        session = _make_mock_session()
        mock_get_session.return_value = session

        mock_response = {'status': 'Completed', 'analytics': {}}
        session.sync.get.return_value = mock_response

        result = GsRiskApi.get_pretrade_execution_optimization('opt-1')

        session.sync.get.assert_called_with('/risk/execution/pretrade/opt-1/results')
        assert result == mock_response

    @patch.object(GsRiskApi, 'get_session')
    def test_exception_during_get(self, mock_get_session):
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.get.side_effect = Exception('network error')

        result = GsRiskApi.get_pretrade_execution_optimization('opt-2')

        assert result == 'network error'

    @patch('gs_quant.api.gs.risk.time.sleep')
    @patch.object(GsRiskApi, 'get_session')
    def test_running_then_completed(self, mock_get_session, mock_sleep):
        """First call returns Running, second returns Completed."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.get.side_effect = [
            {'status': 'Running'},
            {'status': 'Completed', 'analytics': {}}
        ]

        result = GsRiskApi.get_pretrade_execution_optimization('opt-3')

        assert result == {'status': 'Completed', 'analytics': {}}
        assert session.sync.get.call_count == 2
        mock_sleep.assert_called_once()

    @patch('gs_quant.api.gs.risk.time.sleep')
    @patch.object(GsRiskApi, 'get_session')
    def test_running_exceeds_max_attempts(self, mock_get_session, mock_sleep):
        """Always returns Running, should exhaust max_attempts."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.get.return_value = {'status': 'Running'}

        result = GsRiskApi.get_pretrade_execution_optimization('opt-4', max_attempts=3)

        assert result == {'status': 'Running'}
        assert session.sync.get.call_count == 3

    @patch.object(GsRiskApi, 'get_session')
    def test_non_running_status(self, mock_get_session):
        """Status that is not 'Running' should return immediately."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.get.return_value = {'status': 'Failed', 'error': 'oops'}

        result = GsRiskApi.get_pretrade_execution_optimization('opt-5')

        assert result['status'] == 'Failed'
        assert session.sync.get.call_count == 1

    @patch('gs_quant.api.gs.risk.time.sleep')
    @patch.object(GsRiskApi, 'get_session')
    def test_retry_then_exception(self, mock_get_session, mock_sleep):
        """Running then exception on retry."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.get.side_effect = [
            {'status': 'Running'},
            Exception('connection lost')
        ]

        result = GsRiskApi.get_pretrade_execution_optimization('opt-6')

        assert result == 'connection lost'


# ===========================================================================
# Tests for get_liquidity_and_factor_analysis
# ===========================================================================

class TestGetLiquidityAndFactorAnalysis:
    @patch.object(GsRiskApi, 'get_session')
    def test_success_with_defaults(self, mock_get_session):
        """Success with default parameters."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        mock_response = {'riskBuckets': [], 'factorExposure': {}}
        session.sync.post.return_value = mock_response

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        result = GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA_EFM_USALTL',
            date=dt.date(2024, 1, 15),
        )

        assert result == mock_response
        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['currency'] == 'USD'
        assert payload['participationRate'] == 0.1
        assert 'notional' not in payload
        assert len(payload['measures']) == 5

    @patch.object(GsRiskApi, 'get_session')
    def test_success_with_custom_params(self, mock_get_session):
        """Success with custom parameters including notional."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        mock_response = {'result': 'ok'}
        session.sync.post.return_value = mock_response

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        result = GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='AXIOMA_AXUS4S',
            date=dt.date(2024, 6, 1),
            currency='EUR',
            participation_rate=0.2,
            measures=['Risk Buckets'],
            notional=1000000.0,
            time_series_benchmark_ids=['SPX'],
        )

        assert result == mock_response
        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['currency'] == 'EUR'
        assert payload['participationRate'] == 0.2
        assert payload['notional'] == 1000000.0
        assert payload['measures'] == ['Risk Buckets']
        assert payload['timeSeriesBenchmarkIds'] == ['SPX']

    @patch.object(GsRiskApi, 'get_session')
    def test_error_with_missing_assets_clean_match(self, mock_get_session):
        """Error message with clean asset ID pattern match."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        error_msg = 'Assets with the following ids are missing in marquee: [ MA123, MA456 ]'
        session.sync.post.return_value = {'errorMessage': error_msg}

        positions = [{'assetId': 'MA123', 'quantity': 100}]

        with pytest.raises(MqValueError) as exc_info:
            GsRiskApi.get_liquidity_and_factor_analysis(
                positions=positions,
                risk_model='BARRA',
                date=dt.date(2024, 1, 15),
            )

        assert 'liquidity analysis failed' in str(exc_info.value)
        assert 'missing in marquee' in str(exc_info.value)

    @patch.object(GsRiskApi, 'get_session')
    def test_error_with_missing_assets_no_clean_match(self, mock_get_session):
        """Error message matches asset IDs pattern but not the clean_error_pattern."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        error_msg = 'Assets with the following ids are missing in marquee: [MA123, MA456]'
        session.sync.post.return_value = {'errorMessage': error_msg}

        positions = [{'assetId': 'MA123', 'quantity': 100}]

        # Force the else branch by making the second re.search return None
        original_re_search = __import__('re').search
        call_count = [0]

        def mock_re_search(pattern, string, flags=0):
            call_count[0] += 1
            result = original_re_search(pattern, string, flags)
            if call_count[0] == 2:
                return None
            return result

        with patch('gs_quant.api.gs.risk.re.search', side_effect=mock_re_search):
            with pytest.raises(MqValueError) as exc_info:
                GsRiskApi.get_liquidity_and_factor_analysis(
                    positions=positions,
                    risk_model='BARRA',
                    date=dt.date(2024, 1, 15),
                )

        assert 'liquidity analysis failed' in str(exc_info.value)
        assert 'missing in marquee' in str(exc_info.value)

    @patch.object(GsRiskApi, 'get_session')
    def test_error_without_asset_ids(self, mock_get_session):
        """Error message without asset IDs pattern."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'errorMessage': 'Generic error occurred'}

        positions = [{'assetId': 'MA123', 'quantity': 100}]

        with pytest.raises(MqValueError) as exc_info:
            GsRiskApi.get_liquidity_and_factor_analysis(
                positions=positions,
                risk_model='BARRA',
                date=dt.date(2024, 1, 15),
            )

        assert 'liquidity analysis failed' in str(exc_info.value)

    @patch.object(GsRiskApi, 'get_session')
    def test_exception_propagated(self, mock_get_session):
        """Non-MqValueError exceptions should propagate."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.side_effect = ConnectionError('network down')

        positions = [{'assetId': 'MA123', 'quantity': 100}]

        with pytest.raises(ConnectionError):
            GsRiskApi.get_liquidity_and_factor_analysis(
                positions=positions,
                risk_model='BARRA',
                date=dt.date(2024, 1, 15),
            )

    @patch.object(GsRiskApi, 'get_session')
    def test_measures_none_default(self, mock_get_session):
        """When measures is None, default measures should be used."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
            measures=None,
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert 'Time Series Data' in payload['measures']
        assert 'Risk Buckets' in payload['measures']
        assert len(payload['measures']) == 5

    @patch.object(GsRiskApi, 'get_session')
    def test_measures_provided(self, mock_get_session):
        """When measures is provided, use the given measures."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        custom_measures = ['Risk Buckets', 'Exposure Buckets']
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
            measures=custom_measures,
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['measures'] == custom_measures

    @patch.object(GsRiskApi, 'get_session')
    def test_notional_none_not_in_payload(self, mock_get_session):
        """When notional is None, it should not be in payload."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
            notional=None,
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert 'notional' not in payload

    @patch.object(GsRiskApi, 'get_session')
    def test_notional_set_in_payload(self, mock_get_session):
        """When notional is provided, it should be in payload."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
            notional=5000000.0,
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['notional'] == 5000000.0

    @patch.object(GsRiskApi, 'get_session')
    def test_time_series_benchmark_ids_none(self, mock_get_session):
        """When time_series_benchmark_ids is None, defaults to empty list."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
            time_series_benchmark_ids=None,
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['timeSeriesBenchmarkIds'] == []

    @patch.object(GsRiskApi, 'get_session')
    def test_date_as_string(self, mock_get_session):
        """When date is already a string, use it directly."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'result': 'ok'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date='2024-01-15',
        )

        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['date'] == '2024-01-15'

    @patch.object(GsRiskApi, 'get_session')
    def test_response_non_dict(self, mock_get_session):
        """When response is not a dict, return it directly."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = ['some', 'list', 'response']

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        result = GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
        )

        assert result == ['some', 'list', 'response']

    @patch.object(GsRiskApi, 'get_session')
    def test_response_dict_no_error(self, mock_get_session):
        """When response is a dict without errorMessage, return it."""
        session = _make_mock_session()
        mock_get_session.return_value = session

        session.sync.post.return_value = {'data': 'success'}

        positions = [{'assetId': 'MA1', 'quantity': 100}]
        result = GsRiskApi.get_liquidity_and_factor_analysis(
            positions=positions,
            risk_model='BARRA',
            date=dt.date(2024, 1, 15),
        )

        assert result == {'data': 'success'}
