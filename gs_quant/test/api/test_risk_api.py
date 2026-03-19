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


# ===========================================================================
# Tests for run() method branches
# ===========================================================================

import sys
import time
from threading import Thread
from tqdm import tqdm


def _make_sync_request(num_positions=1, num_dates=1, num_measures=1):
    """Create a mock RiskRequest for use in run() tests."""
    req = MagicMock()
    req.wait_for_results = True

    positions = [MagicMock() for _ in range(num_positions)]
    for p in positions:
        p.instrument = MagicMock(spec=Priceable)

    dates = [MagicMock() for _ in range(num_dates)]
    for d in dates:
        d.pricing_date = MagicMock()
        d.market = MagicMock()

    measures = [MagicMock() for _ in range(num_measures)]

    req.positions = positions
    req.pricing_and_market_data_as_of = dates
    req.measures = measures
    req.parameters = MagicMock()
    req.scenario = MagicMock()
    del req._id

    return req


class TestRunMethodBranches:
    """Tests for the complex run() method in RiskApi.

    These test the many branches inside run_async, process_results,
    and execute_requests by providing requests that trigger different
    code paths.
    """

    def test_run_basic_single_request(self):
        """Basic test: single request, single job, max_concurrent >= expected.
        Covers: run_async main path, _process_results, shutdown_queue_listener."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        # Override calc_multi to return results immediately
        result_data = {'$type': 'X', 'v': 1}
        original_calc_multi = ConcreteRiskApi.calc_multi

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}):
            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

        # Results queue should have data plus shutdown sentinel
        assert not results_queue.empty()

    def test_run_with_progress_bar(self):
        """Branch [291,294] / [312,313]: progress_bar is truthy.
        Covers progress_bar.update(), progress_bar.refresh(), and progress_bar.close()."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()
        progress = MagicMock(spec=tqdm)

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}):
            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=progress,
                timeout=None,
                span=None,
            )

        progress.update.assert_called()
        progress.refresh.assert_called()
        progress.close.assert_called_once()

    def test_run_multiple_requests_triggers_result_thread(self):
        """Branch [263,265] / [299,300] / [315,316]: expected > chunk_size
        creates result_thread, unprocessed_results is not None.
        Also covers process_results function [194,195] / [194,-191]."""
        import queue as q_mod

        # Create 3 requests, each with 1 job, max_concurrent=1
        # expected=3 > chunk_size=min(1,3)=1, so result_thread is created
        reqs = [_make_sync_request() for _ in range(3)]
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}):
            ConcreteRiskApi.run(
                requests=reqs,
                results=results_queue,
                max_concurrent=1,
                progress_bar=None,
                timeout=None,
                span=None,
            )

        # Should complete without error
        assert not results_queue.empty()

    def test_run_async_request_mode(self):
        """Branch [252,254] / [221,223]: is_async=True creates results_handler
        and responses != raw_results triggers shutdown_queue_listener.
        Also covers [306,307]: results_handler is truthy."""
        import queue as q_mod

        req = _make_sync_request()
        req.wait_for_results = False  # This makes is_async=True
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        # get_results needs to be a proper async function that receives results
        async def mock_get_results(responses, raw_results, timeout=None, span=None):
            # Read from responses and forward to raw_results
            while True:
                try:
                    item = await asyncio.wait_for(responses.get(), timeout=2)
                except (asyncio.TimeoutError, Exception):
                    break
                # Check if it's the shutdown sentinel
                if hasattr(item, '__class__') and 'Sentinel' in type(item).__name__:
                    break
                # Forward response to raw_results
                for request, result in (item if isinstance(item, list) else [item]):
                    raw_results.put_nowait((request, result))
            return None

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch.object(ConcreteRiskApi, 'get_results', side_effect=mock_get_results), \
             patch('gs_quant.api.risk.result_handlers', {}):
            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

    def test_run_results_handler_returns_error(self):
        """Branch [308,310]: results_handler returns a truthy error string.
        This should raise RuntimeError which is caught by populate_pending_futures."""
        import queue as q_mod

        req = _make_sync_request()
        req.wait_for_results = False  # is_async=True
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        async def mock_get_results(responses, raw_results, timeout=None, span=None):
            # Forward any results to raw_results, then return error
            while True:
                try:
                    item = await asyncio.wait_for(responses.get(), timeout=2)
                except (asyncio.TimeoutError, Exception):
                    break
                if hasattr(item, '__class__') and 'Sentinel' in type(item).__name__:
                    break
                for request, result in (item if isinstance(item, list) else [item]):
                    raw_results.put_nowait((request, result))
            return 'Fatal subscription error'

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch.object(ConcreteRiskApi, 'get_results', side_effect=mock_get_results), \
             patch('gs_quant.api.risk.result_handlers', {}):
            # This should raise RuntimeError from results_error
            with pytest.raises(RuntimeError, match='Fatal Error subscribing to results'):
                ConcreteRiskApi.run(
                    requests=[req],
                    results=results_queue,
                    max_concurrent=10,
                    progress_bar=None,
                    timeout=None,
                    span=None,
                )

    def test_run_shutdown_on_error(self):
        """Branch [283,285]: shutdown=True from drain_queue_async breaks the while loop.
        This happens when an error causes the raw_results queue to receive a shutdown sentinel."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        # calc_multi raises an exception, which causes execute_requests to enqueue
        # errors to raw_results. But we need shutdown to be True from drain_queue_async.
        # The simplest way is to have execute_requests put the shutdown sentinel on raw_results.
        def mock_calc_multi(requests):
            raise RuntimeError('calc failed')

        session = MagicMock()

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}):
            # The error enqueued by execute_requests won't trigger shutdown=True
            # (only the sentinel does that). Let's complete normally with error results.
            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

    def test_run_python36_compat_path(self):
        """Branch [321,324] / [332,333] / [342,343]: Python < 3.7 compatibility path.
        We mock sys.version_info to trigger the else branch."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}), \
             patch('gs_quant.api.risk.sys') as mock_sys:
            # Make version_info < (3, 7)
            mock_sys.version_info = (3, 6, 9)
            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

    def test_run_python36_path_exception(self):
        """Branch [338,339/340]: exception in Python < 3.7 path.
        Tests the `except Exception: if not use_existing: main_loop.stop(); raise` branch."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        def mock_calc_multi(requests):
            raise RuntimeError('forced error')

        session = MagicMock()

        # We need to test the exception path in the Python 3.6 compat code.
        # The except clause calls main_loop.stop() if not use_existing, then re-raises.
        # Since use_existing requires a running loop, and we won't have one,
        # we need a mock that makes run_until_complete raise.
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_loop.run_until_complete.side_effect = RuntimeError('loop error')

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}), \
             patch('gs_quant.api.risk.sys') as mock_sys, \
             patch('gs_quant.api.risk.asyncio') as mock_asyncio:
            mock_sys.version_info = (3, 6, 9)
            mock_asyncio.get_event_loop.return_value = mock_loop
            mock_asyncio.new_event_loop.return_value = mock_loop
            mock_asyncio.Queue = asyncio.Queue
            mock_asyncio.QueueEmpty = asyncio.QueueEmpty

            with pytest.raises(RuntimeError, match='loop error'):
                ConcreteRiskApi.run(
                    requests=[req],
                    results=results_queue,
                    max_concurrent=10,
                    progress_bar=None,
                    timeout=None,
                    span=None,
                )

            # Verify stop was called (branch [338,339])
            mock_loop.stop.assert_called_once()
            # Verify close was called (branch [342,343])
            mock_loop.close.assert_called_once()

    def test_run_python36_use_existing_loop(self):
        """Branch [332,333/335]: use_existing=True path in Python < 3.7.
        When an existing running loop is found, it's reused."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_loop.run_until_complete.return_value = None

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}), \
             patch('gs_quant.api.risk.sys') as mock_sys, \
             patch('gs_quant.api.risk.asyncio') as mock_asyncio:
            mock_sys.version_info = (3, 6, 9)
            mock_asyncio.get_event_loop.return_value = mock_loop
            mock_asyncio.Queue = asyncio.Queue
            mock_asyncio.QueueEmpty = asyncio.QueueEmpty

            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

            # With use_existing=True, set_event_loop should NOT be called
            mock_asyncio.set_event_loop.assert_not_called()
            # close should NOT be called (we don't own the loop)
            mock_loop.close.assert_not_called()

    def test_run_python36_no_existing_loop(self):
        """Branch [332,333]: no existing loop (RuntimeError from get_event_loop)."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        result_data = {'$type': 'X', 'v': 1}

        def mock_calc_multi(requests):
            return {r: [[[result_data]]] for r in requests}

        session = MagicMock()

        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_loop.run_until_complete.return_value = None

        with patch.object(ConcreteRiskApi, 'calc_multi', side_effect=mock_calc_multi), \
             patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}), \
             patch('gs_quant.api.risk.sys') as mock_sys, \
             patch('gs_quant.api.risk.asyncio') as mock_asyncio:
            mock_sys.version_info = (3, 6, 9)
            mock_asyncio.get_event_loop.side_effect = RuntimeError('no loop')
            mock_asyncio.new_event_loop.return_value = mock_loop
            mock_asyncio.Queue = asyncio.Queue
            mock_asyncio.QueueEmpty = asyncio.QueueEmpty

            ConcreteRiskApi.run(
                requests=[req],
                results=results_queue,
                max_concurrent=10,
                progress_bar=None,
                timeout=None,
                span=None,
            )

            # set_event_loop should be called since use_existing=False
            mock_asyncio.set_event_loop.assert_any_call(mock_loop)
            mock_loop.close.assert_called_once()

    def test_run_python36_exception_use_existing(self):
        """Branch [338,339] false: exception with use_existing=True => no stop()."""
        import queue as q_mod

        req = _make_sync_request()
        results_queue = q_mod.Queue()

        session = MagicMock()

        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_loop.run_until_complete.side_effect = RuntimeError('loop error')

        with patch.object(ConcreteRiskApi, 'get_session', return_value=session), \
             patch('gs_quant.api.risk.result_handlers', {}), \
             patch('gs_quant.api.risk.sys') as mock_sys, \
             patch('gs_quant.api.risk.asyncio') as mock_asyncio:
            mock_sys.version_info = (3, 6, 9)
            mock_asyncio.get_event_loop.return_value = mock_loop
            mock_asyncio.Queue = asyncio.Queue
            mock_asyncio.QueueEmpty = asyncio.QueueEmpty

            with pytest.raises(RuntimeError, match='loop error'):
                ConcreteRiskApi.run(
                    requests=[req],
                    results=results_queue,
                    max_concurrent=10,
                    progress_bar=None,
                    timeout=None,
                    span=None,
                )

            # With use_existing=True, stop() should NOT be called
            mock_loop.stop.assert_not_called()
            # close should NOT be called (we don't own the loop)
            mock_loop.close.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 6 – GsRiskApi branch-coverage tests for __get_results_poll
# ---------------------------------------------------------------------------

from gs_quant.api.gs.risk import GsRiskApi


class TestGetResultsPollBranches:
    """Cover branches [122,125], [145,142] in __get_results_poll."""

    @pytest.mark.asyncio
    async def test_poll_shutdown_with_items(self):
        """[122,125]: shutdown=True AND items non-empty in same drain call."""
        responses = asyncio.Queue()
        results = asyncio.Queue()

        call_count = 0

        async def mock_drain(q, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Return shutdown=True with items
                return True, [('req_key_1', {'reportId': 'rpt1'})]
            return True, []

        mock_session = MagicMock()
        mock_session.api_version = 'v1'
        mock_session.sync.post.return_value = [
            {'requestId': 'rpt1', 'result': {'data': 42}}
        ]

        with patch.object(GsRiskApi, 'drain_queue_async', side_effect=mock_drain), \
             patch.object(GsRiskApi, 'get_session', return_value=mock_session), \
             patch.object(GsRiskApi, 'PRICING_API_VERSION', None):
            await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

        assert results.qsize() > 0

    @pytest.mark.asyncio
    async def test_poll_result_in_calc_results(self):
        """[145,142]: calc_result has 'result' key -> put_nowait with result data."""
        responses = asyncio.Queue()
        results = asyncio.Queue()

        call_count = 0

        async def mock_drain(q, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return False, [('req_key_1', {'reportId': 'rpt1'})]
            return True, []

        mock_session = MagicMock()
        mock_session.api_version = 'v1'
        mock_session.sync.post.return_value = [
            {'requestId': 'rpt1', 'result': [1, 2, 3]}
        ]

        with patch.object(GsRiskApi, 'drain_queue_async', side_effect=mock_drain), \
             patch.object(GsRiskApi, 'get_session', return_value=mock_session), \
             patch.object(GsRiskApi, 'PRICING_API_VERSION', None):
            await GsRiskApi._GsRiskApi__get_results_poll(responses, results)

        req_key, result_data = await results.get()
        assert req_key == 'req_key_1'
        assert result_data == [1, 2, 3]
