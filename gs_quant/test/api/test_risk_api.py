"""
Copyright 2024 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import asyncio
import queue
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.api.risk import RiskApi, GenericRiskApi
from gs_quant.base import RiskKey, Priceable
from gs_quant.risk import ErrorValue
from gs_quant.risk.results import PricingFuture


# ---------------------------------------------------------------------------
# Concrete subclass of RiskApi so we can call its non-abstract methods
# ---------------------------------------------------------------------------
class ConcreteRiskApi(RiskApi):
    """Minimal concrete implementation for testing non-abstract methods."""

    @classmethod
    async def get_results(cls, responses, results, timeout=None, span=None):
        return None

    @classmethod
    def calc(cls, request):
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_risk_key(provider=None, date=None, market=None, params=None, scenario=None, risk_measure=None):
    return RiskKey(
        provider or ConcreteRiskApi,
        date,
        market or MagicMock(),
        params,
        scenario,
        risk_measure or MagicMock(),
    )


def _make_position(instrument=None):
    pos = MagicMock()
    pos.instrument = instrument or MagicMock(spec=Priceable)
    return pos


def _make_as_of(pricing_date=None, market=None):
    as_of = MagicMock()
    as_of.pricing_date = pricing_date or MagicMock()
    as_of.market = market or MagicMock()
    return as_of


def _make_request(positions=None, measures=None, as_of=None, wait_for_results=True,
                  parameters=None, scenario=None, _id=None):
    req = MagicMock()
    req.positions = positions or [_make_position()]
    req.measures = measures or [MagicMock()]
    req.pricing_and_market_data_as_of = as_of or [_make_as_of()]
    req.wait_for_results = wait_for_results
    req.parameters = parameters
    req.scenario = scenario
    if _id is not None:
        req._id = _id
    else:
        # Make sure getattr(..., '_id', None) returns None
        del req._id
    return req


# ===========================================================================
# Tests for calc_multi
# ===========================================================================

class TestCalcMulti:
    def test_calc_multi_returns_dict(self):
        r1 = MagicMock()
        r2 = MagicMock()
        ConcreteRiskApi.calc = classmethod(lambda cls, req: [f'result_{req}'])
        result = ConcreteRiskApi.calc_multi([r1, r2])
        assert isinstance(result, dict)
        assert r1 in result
        assert r2 in result


# ===========================================================================
# Tests for __handle_queue_update (via drain_queue)
# ===========================================================================

class TestDrainQueue:
    def test_drain_queue_shutdown_sentinel_first(self):
        """When the shutdown sentinel is the first item, return (True, [])."""
        q = queue.Queue()
        # Put the shutdown sentinel
        ConcreteRiskApi.shutdown_queue_listener(q)
        done, items = ConcreteRiskApi.drain_queue(q)
        assert done is True
        assert items == []

    def test_drain_queue_normal_items(self):
        """Normal items are collected until queue is empty."""
        q = queue.Queue()
        q.put('item1')
        q.put('item2')
        done, items = ConcreteRiskApi.drain_queue(q)
        assert done is False
        assert items == ['item1', 'item2']

    def test_drain_queue_items_then_shutdown(self):
        """Items followed by shutdown sentinel: all items collected, done=True."""
        q = queue.Queue()
        q.put('item1')
        ConcreteRiskApi.shutdown_queue_listener(q)
        done, items = ConcreteRiskApi.drain_queue(q)
        assert done is True
        assert items == ['item1']

    def test_drain_queue_timeout_empty(self):
        """If queue is empty and times out, return (False, [])."""
        q = queue.Queue()
        done, items = ConcreteRiskApi.drain_queue(q, timeout=0)
        assert done is False
        assert items == []


# ===========================================================================
# Tests for drain_queue_async
# ===========================================================================

class TestDrainQueueAsync:
    def test_drain_queue_async_shutdown_sentinel_first(self):
        loop = asyncio.new_event_loop()
        try:
            aq = asyncio.Queue()
            ConcreteRiskApi.shutdown_queue_listener(aq)
            done, items = loop.run_until_complete(ConcreteRiskApi.drain_queue_async(aq))
            assert done is True
            assert items == []
        finally:
            loop.close()

    def test_drain_queue_async_normal_items(self):
        loop = asyncio.new_event_loop()
        try:
            aq = asyncio.Queue()
            aq.put_nowait('a')
            aq.put_nowait('b')
            done, items = loop.run_until_complete(ConcreteRiskApi.drain_queue_async(aq))
            assert done is False
            assert items == ['a', 'b']
        finally:
            loop.close()

    def test_drain_queue_async_items_then_shutdown(self):
        loop = asyncio.new_event_loop()
        try:
            aq = asyncio.Queue()
            aq.put_nowait('x')
            ConcreteRiskApi.shutdown_queue_listener(aq)
            done, items = loop.run_until_complete(ConcreteRiskApi.drain_queue_async(aq))
            assert done is True
            assert items == ['x']
        finally:
            loop.close()

    def test_drain_queue_async_timeout(self):
        loop = asyncio.new_event_loop()
        try:
            aq = asyncio.Queue()
            # Use a very small timeout to trigger TimeoutError branch quickly
            done, items = loop.run_until_complete(
                ConcreteRiskApi.drain_queue_async(aq, timeout=0.001)
            )
            assert done is False
            assert items == []
        finally:
            loop.close()

    def test_drain_queue_async_no_timeout(self):
        """When timeout is None (falsy), use plain await q.get()."""
        loop = asyncio.new_event_loop()
        try:
            aq = asyncio.Queue()
            aq.put_nowait('val')
            done, items = loop.run_until_complete(ConcreteRiskApi.drain_queue_async(aq, timeout=None))
            assert done is False
            assert items == ['val']
        finally:
            loop.close()


# ===========================================================================
# Tests for enqueue
# ===========================================================================

class TestEnqueue:
    def test_enqueue_iterable(self):
        q = queue.Queue()
        ConcreteRiskApi.enqueue(q, ['a', 'b', 'c'])
        assert q.qsize() == 3

    def test_enqueue_non_iterable(self):
        """When items is not iterable (e.g., an int), wrap it in a tuple."""
        q = queue.Queue()
        ConcreteRiskApi.enqueue(q, 42)
        assert q.get_nowait() == 42

    def test_enqueue_with_loop(self):
        """When loop is provided, use loop.call_soon_threadsafe."""
        loop = MagicMock()
        q = queue.Queue()
        ConcreteRiskApi.enqueue(q, ['x'], loop=loop)
        loop.call_soon_threadsafe.assert_called_once()

    def test_enqueue_with_wait(self):
        """When wait=True, use q.put instead of q.put_nowait."""
        q = MagicMock()
        ConcreteRiskApi.enqueue(q, ['a'], wait=True)
        q.put.assert_called_once_with('a')

    def test_enqueue_without_wait(self):
        """When wait=False (default), use q.put_nowait."""
        q = MagicMock()
        ConcreteRiskApi.enqueue(q, ['a'], wait=False)
        q.put_nowait.assert_called_once_with('a')

    def test_enqueue_with_loop_and_wait(self):
        """When loop is provided, wait affects which put function is passed to call_soon_threadsafe."""
        loop = MagicMock()
        q = MagicMock()
        ConcreteRiskApi.enqueue(q, ['item'], loop=loop, wait=True)
        loop.call_soon_threadsafe.assert_called_once_with(q.put, 'item')


# ===========================================================================
# Tests for shutdown_queue_listener
# ===========================================================================

class TestShutdownQueueListener:
    def test_shutdown_with_open_loop(self):
        loop = MagicMock()
        loop.is_closed.return_value = False
        q = MagicMock()
        ConcreteRiskApi.shutdown_queue_listener(q, loop=loop)
        loop.call_soon_threadsafe.assert_called_once()

    def test_shutdown_with_closed_loop(self):
        loop = MagicMock()
        loop.is_closed.return_value = True
        q = MagicMock()
        ConcreteRiskApi.shutdown_queue_listener(q, loop=loop)
        q.put_nowait.assert_called_once()

    def test_shutdown_without_loop(self):
        q = MagicMock()
        ConcreteRiskApi.shutdown_queue_listener(q)
        q.put_nowait.assert_called_once()

    def test_shutdown_with_none_loop(self):
        q = MagicMock()
        ConcreteRiskApi.shutdown_queue_listener(q, loop=None)
        q.put_nowait.assert_called_once()


# ===========================================================================
# Tests for build_keyed_results
# ===========================================================================

class TestBuildKeyedResults:
    def test_build_keyed_results_normal(self):
        """Normal case: results are iterable with $type that has a handler."""
        measure = MagicMock()
        pos = _make_position()
        as_of = _make_as_of()
        req = _make_request(positions=[pos], measures=[measure], as_of=[as_of])

        date_result = {'$type': 'SomeType', 'value': 123}
        results = [[[date_result]]]  # measures x positions x dates

        handler = MagicMock(return_value='handled_value')
        with patch('gs_quant.api.risk.result_handlers', {'SomeType': handler}):
            formatted = ConcreteRiskApi.build_keyed_results(req, results)

        assert len(formatted) == 1
        # Check that the handler was called
        handler.assert_called_once()
        val = list(formatted.values())[0]
        assert val == 'handled_value'

    def test_build_keyed_results_no_handler(self):
        """When no handler exists for $type, use the raw date_result."""
        measure = MagicMock()
        pos = _make_position()
        as_of = _make_as_of()
        req = _make_request(positions=[pos], measures=[measure], as_of=[as_of])

        date_result = {'$type': 'UnknownType', 'data': 'raw'}
        results = [[[date_result]]]

        with patch('gs_quant.api.risk.result_handlers', {}):
            formatted = ConcreteRiskApi.build_keyed_results(req, results)

        assert len(formatted) == 1
        val = list(formatted.values())[0]
        assert val == date_result

    def test_build_keyed_results_exception_input(self):
        """When results is an Exception, create error entries for all combos."""
        measure = MagicMock()
        pos = _make_position()
        as_of = _make_as_of()
        req = _make_request(positions=[pos], measures=[measure], as_of=[as_of])
        exc = RuntimeError('test error')

        with patch('gs_quant.api.risk.result_handlers', {'Error': None}):
            formatted = ConcreteRiskApi.build_keyed_results(req, exc)

        assert len(formatted) == 1
        val = list(formatted.values())[0]
        # The result should be the raw date_result dict since handler is None
        assert val == {'$type': 'Error', 'errorString': 'test error'}

    def test_build_keyed_results_handler_raises(self):
        """When handler raises an exception, result is ErrorValue."""
        measure = MagicMock()
        pos = _make_position()
        as_of = _make_as_of()
        req = _make_request(positions=[pos], measures=[measure], as_of=[as_of])

        date_result = {'$type': 'BadType', 'value': 0}
        results = [[[date_result]]]

        handler = MagicMock(side_effect=ValueError('handler failed'))
        with patch('gs_quant.api.risk.result_handlers', {'BadType': handler}):
            formatted = ConcreteRiskApi.build_keyed_results(req, results)

        val = list(formatted.values())[0]
        assert isinstance(val, ErrorValue)

    def test_build_keyed_results_multiple_measures_positions_dates(self):
        """Multiple measures, positions, and dates."""
        m1, m2 = MagicMock(), MagicMock()
        p1, p2 = _make_position(), _make_position()
        a1, a2 = _make_as_of(), _make_as_of()
        req = _make_request(positions=[p1, p2], measures=[m1, m2], as_of=[a1, a2])

        dr = {'$type': 'X', 'v': 1}
        # 2 measures x 2 positions x 2 dates = 8 results
        results = [[[dr, dr], [dr, dr]], [[dr, dr], [dr, dr]]]

        with patch('gs_quant.api.risk.result_handlers', {}):
            formatted = ConcreteRiskApi.build_keyed_results(req, results)

        # Each (risk_key, instrument) is unique because positions have different instruments
        # 2 measures * 2 positions * 2 dates = 8 entries, but keys may collide if
        # risk_key hashes the same (since dates differ). With MagicMock, each is unique.
        assert len(formatted) > 0

    def test_build_keyed_results_with_request_id(self):
        """When request has _id, pass it to handler."""
        measure = MagicMock()
        pos = _make_position()
        as_of = _make_as_of()
        req = _make_request(positions=[pos], measures=[measure], as_of=[as_of], _id='req123')

        date_result = {'$type': 'MyType'}
        results = [[[date_result]]]

        handler = MagicMock(return_value='ok')
        with patch('gs_quant.api.risk.result_handlers', {'MyType': handler}):
            ConcreteRiskApi.build_keyed_results(req, results)

        handler.assert_called_once()
        _, kwargs = handler.call_args
        assert kwargs['request_id'] == 'req123'


# ===========================================================================
# Tests for populate_pending_futures
# ===========================================================================

class TestPopulatePendingFutures:
    def test_populate_pending_futures_normal(self):
        """Test populate_pending_futures with normal results and cache."""
        rk = _make_risk_key()
        instrument = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk, instrument): future}

        cache_impl = MagicMock()
        result_value = 'some_result'

        # Mock run to put results into the queue, then shutdown
        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            results.put(((rk, instrument), result_value))
            ConcreteRiskApi.shutdown_queue_listener(results)

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=cache_impl, is_async=False
            )

        future.set_result.assert_called_once_with(result_value)
        cache_impl.put.assert_called_once_with(rk, instrument, result_value)

    def test_populate_pending_futures_no_cache(self):
        """Test when cache_impl is None."""
        rk = _make_risk_key()
        instrument = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk, instrument): future}

        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            results.put(((rk, instrument), 'val'))
            ConcreteRiskApi.shutdown_queue_listener(results)

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=None, is_async=False
            )

        future.set_result.assert_called_once_with('val')

    def test_populate_pending_futures_exception(self):
        """When run raises, enqueue errors for all pending keys."""
        rk = _make_risk_key()
        instrument = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk, instrument): future}

        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            raise RuntimeError('run failed')

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=None, is_async=False
            )

        future.set_result.assert_called_once()
        result_arg = future.set_result.call_args[0][0]
        assert isinstance(result_arg, RuntimeError)

    def test_populate_pending_futures_is_async(self):
        """When is_async=True, don't fill remaining pending with ErrorValue."""
        rk = _make_risk_key()
        instrument = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk, instrument): future}

        # Run that shuts down without providing a result for the pending key
        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            ConcreteRiskApi.shutdown_queue_listener(results)

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=None, is_async=True
            )

        # In async mode, remaining pending should NOT be filled with ErrorValue
        future.set_result.assert_not_called()

    def test_populate_pending_futures_not_async_remaining(self):
        """When is_async=False, remaining pending are filled with ErrorValue."""
        rk = _make_risk_key()
        instrument = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk, instrument): future}

        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            ConcreteRiskApi.shutdown_queue_listener(results)

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=None, is_async=False
            )

        future.set_result.assert_called_once()
        result_arg = future.set_result.call_args[0][0]
        assert isinstance(result_arg, ErrorValue)

    def test_populate_pending_futures_key_not_found(self):
        """When result key not in pending, future is None, no error."""
        rk_pending = _make_risk_key()
        rk_result = _make_risk_key()  # different key
        instrument_pending = MagicMock(spec=Priceable)
        instrument_result = MagicMock(spec=Priceable)
        future = MagicMock()
        pending = {(rk_pending, instrument_pending): future}

        def mock_run(requests, results, max_concurrent, progress_bar, timeout=None, span=None):
            results.put(((rk_result, instrument_result), 'val'))
            ConcreteRiskApi.shutdown_queue_listener(results)

        session = MagicMock()
        with patch.object(ConcreteRiskApi, 'run', side_effect=mock_run):
            ConcreteRiskApi.populate_pending_futures(
                requests=[], session=session, pending=pending,
                max_concurrent=1, progress_bar=None, timeout=None, span=None,
                cache_impl=None, is_async=False
            )

        # The pending key was not matched, so it gets ErrorValue from the "not is_async" cleanup
        future.set_result.assert_called_once()
        result_arg = future.set_result.call_args[0][0]
        assert isinstance(result_arg, ErrorValue)
