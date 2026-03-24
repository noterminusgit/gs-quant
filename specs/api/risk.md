# api/risk.py

## Summary
Defines the abstract risk calculation API layer with queue-based asynchronous execution. `GenericRiskApi` establishes the abstract interface for risk APIs. `RiskApi` extends it with a full implementation of concurrent request dispatch, result subscription, queue-based producer/consumer communication between threads, and result formatting with handler dispatch. The `run` method orchestrates a multi-threaded, async-bridged pipeline that dispatches `RiskRequest` batches, collects raw results, processes them through type-specific handlers, and delivers them to `PricingFuture` instances.

## Dependencies
- Internal: `gs_quant.api.api_session` (`ApiWithCustomSession`)
- Internal: `gs_quant.base` (`RiskKey`, `Sentinel`, `Priceable`)
- Internal: `gs_quant.risk` (`ErrorValue`, `RiskRequest`)
- Internal: `gs_quant.risk.result_handlers` (`result_handlers`)
- Internal: `gs_quant.risk.results` (`PricingFuture`)
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.tracing` (`Tracer`, `TracingSpan`)
- External: `asyncio` (Queue, QueueEmpty, get_event_loop, new_event_loop, run, set_event_loop, wait_for)
- External: `itertools` (chain.from_iterable)
- External: `logging` (getLogger)
- External: `queue` (Queue, Empty)
- External: `sys` (version_info)
- External: `abc` (ABCMeta, abstractmethod)
- External: `concurrent.futures` (TimeoutError)
- External: `threading` (Thread)
- External: `tqdm` (tqdm)

## Type Definitions

### GenericRiskApi (ABC)
Inherits: `ApiWithCustomSession`, metaclass=`ABCMeta`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| batch_dates | `bool` | `True` | Class-level flag indicating dates should be batched |

### RiskApi (ABC)
Inherits: `GenericRiskApi`, metaclass=`ABCMeta`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__SHUTDOWN_SENTINEL` | `Sentinel` | `Sentinel('QueueListenerShutdown')` | Private class-level sentinel used to signal queue shutdown |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GenericRiskApi.populate_pending_futures(cls, requests, session, pending, **kwargs) -> None [abstract]
Purpose: Abstract method that subclasses must implement to dispatch risk requests and populate pending futures with results.

### GenericRiskApi.build_keyed_results(cls, request, results) -> Dict[Tuple[RiskKey, Priceable], Any] [abstract]
Purpose: Abstract method that subclasses must implement to convert raw results into a dict keyed by (RiskKey, Priceable).

---

### RiskApi.populate_pending_futures(cls, requests, session, pending, **kwargs) -> None
Purpose: Dispatch risk requests via `run()`, drain the result queue, set results on PricingFutures, and optionally cache them.

**Algorithm:**
1. Create a `queue.Queue` for results; extract `max_concurrent`, `progress_bar`, `timeout`, `span`, `cache_impl`, `is_async` from kwargs.
2. Try: open `session` context and call `cls.run(requests, results, max_concurrent, progress_bar, timeout, span)`.
3. Branch: exception during run -> enqueue `(key, exception)` for every pending key.
4. Loop while `pending` is non-empty and not done:
   a. Drain queue to get `(done, chunk_results)`.
   b. For each `(risk_key_, priceable_), result`: pop from pending, set result on future.
   c. Branch: `cache_impl is not None` -> cache the result.
5. Branch: `not is_async` -> for any remaining pending entries, set `ErrorValue('No result returned')`.

**Raises:** No direct raises; exceptions from `run()` are caught and enqueued as results.

---

### RiskApi.get_results(cls, responses, results, timeout, span) -> Optional[str] [abstract, async]
Purpose: Abstract async method for subscribing to and collecting async risk results.

### RiskApi.calc(cls, request) -> Iterable [abstract]
Purpose: Abstract method to execute a single risk calculation request.

### RiskApi.calc_multi(cls, requests) -> dict
Purpose: Execute multiple risk requests by calling `calc` on each.

**Algorithm:**
1. Return dict comprehension: `{request: cls.calc(request) for request in requests}`.

---

### RiskApi.__handle_queue_update(cls, q, first) -> Tuple[bool, list]
Purpose: Process queue items starting from `first`, collecting all immediately available items.

**Algorithm:**
1. Branch: `first is cls.__SHUTDOWN_SENTINEL` -> return `(True, [])`.
2. Initialize `ret = [first]`, `shutdown = False`.
3. Loop: call `q.get_nowait()` repeatedly.
   a. Branch: element is shutdown sentinel -> set `shutdown = True`.
   b. Else -> append to `ret`.
   c. Branch: `QueueEmpty` or `queue.Empty` exception -> break.
4. Return `(shutdown, ret)`.

---

### RiskApi.drain_queue(cls, q, timeout) -> Tuple[bool, list]
Purpose: Blocking drain of a synchronous queue.

**Algorithm:**
1. Try: call `q.get(timeout=timeout)`, pass to `__handle_queue_update`.
2. Branch: `queue.Empty` -> return `(False, [])`.

---

### RiskApi.drain_queue_async(cls, q, timeout) -> Tuple[bool, list] [async]
Purpose: Async drain of an asyncio queue with optional timeout.

**Algorithm:**
1. Branch: `timeout` is truthy -> `await asyncio.wait_for(q.get(), timeout=timeout)`.
2. Branch: `timeout` is falsy -> `await q.get()`.
3. Pass result to `__handle_queue_update`.
4. Branch: `TimeoutError` or `asyncio.TimeoutError` -> return `(False, [])`.

---

### RiskApi.enqueue(cls, q, items, loop, wait) -> None
Purpose: Enqueue items onto a sync or async queue, optionally via an event loop.

**Algorithm:**
1. Try: call `iter(items)`.
2. Branch: `TypeError` -> wrap items as single-element tuple `(items,)`.
3. Branch: `wait` is truthy -> use `q.put`; else use `q.put_nowait`.
4. For each item:
   a. Branch: `loop` is truthy -> `loop.call_soon_threadsafe(put, item)`.
   b. Else -> call `put(item)` directly.

---

### RiskApi.shutdown_queue_listener(cls, q, loop) -> None
Purpose: Signal queue shutdown by enqueuing the shutdown sentinel.

**Algorithm:**
1. Branch: `loop` is truthy and `not loop.is_closed()` -> `loop.call_soon_threadsafe(q.put_nowait, sentinel)`.
2. Else -> `q.put_nowait(sentinel)`.

---

### RiskApi.run(cls, requests, results, max_concurrent, progress_bar, timeout, span) -> None
Purpose: Orchestrate the full risk calculation pipeline: dispatch requests in chunks via a worker thread, optionally subscribe to async results, process completed results, and deliver to the results queue.

**Algorithm (nested functions and main flow):**

**Inner: `_process_results(completed)`**
1. Chain together `build_keyed_results(request, result).items()` for each `(request, result)` in completed.
2. Enqueue the chained results with `wait=True`.

**Inner: `process_results(unprocessed_results)`**
1. Loop until shutdown: drain `unprocessed_results` queue, call `_process_results` on each chunk.

**Inner: `execute_requests(outstanding_requests, responses, raw_results, session, loop, active_span)`**
1. Activate tracing span and session context.
2. Loop until shutdown: drain `outstanding_requests`.
   a. Branch: `requests_chunk` is non-empty -> call `calc_multi`, enqueue responses.
   b. Branch: exception -> enqueue `(request, exception)` pairs into `raw_results`.
3. Branch: `responses != raw_results` (async mode) -> shutdown responses queue listener.

**Inner: `run_async(current_span)` [async]**
1. Define `num_risk_jobs(request)` = `len(pricing_and_market_data_as_of) * len(positions)`.
2. Define `num_risk_keys(request)` = `num_risk_jobs(request) * len(measures)`.
3. Determine `is_async` from `requests[0].wait_for_results`.
4. Create asyncio queues; create `outstanding_requests` sync queue.
5. Start `execute_requests` in a daemon Thread.
6. Branch: `is_async` -> create task for `cls.get_results(...)`.
7. Compute `expected` total risk jobs; set `chunk_size = min(max_concurrent, expected)`.
8. Branch: `expected > chunk_size` -> start `process_results` in a daemon thread with its own queue.
9. Loop while `received < expected`:
   a. Branch: `requests` non-empty -> pop and enqueue dispatch requests up to `chunk_size`.
   b. Await `drain_queue_async(raw_results)`.
   c. Branch: shutdown -> break.
   d. Update `chunk_size` based on received results.
   e. Branch: `progress_bar` -> update progress bar.
   f. Branch: `unprocessed_results is not None` -> enqueue to background thread; else call `_process_results` directly.
10. Shutdown `outstanding_requests` queue.
11. Branch: `results_handler` exists -> await it; if error string returned, raise `RuntimeError`.
12. Branch: `progress_bar` -> close it.
13. Branch: `result_thread is not None` -> shutdown its queue and join.
14. Shutdown `results` queue.

**Main flow (after `run_async` definition):**
1. Branch: `sys.version_info >= (3, 7)` -> `asyncio.run(run_async(span))`.
2. Else (Python < 3.7):
   a. Try to get existing event loop.
   b. Branch: `RuntimeError` -> `existing_event_loop = None`.
   c. Branch: existing loop is running -> reuse it; else create new loop.
   d. Branch: not reusing -> `asyncio.set_event_loop(main_loop)`.
   e. Try: `main_loop.run_until_complete(run_async(span))`.
   f. Branch: exception and not reusing -> `main_loop.stop()`, re-raise.
   g. Finally (not reusing): close loop and `set_event_loop(None)`.

---

### RiskApi.build_keyed_results(cls, request, results) -> Dict[Tuple[RiskKey, Priceable], Any]
Purpose: Convert raw API results (or an Exception) into a dict keyed by `(RiskKey, instrument)`, applying type-specific result handlers.

**Algorithm:**
1. Branch: `isinstance(results, Exception)` -> fabricate error dicts for every measure/position/date combination.
2. Triple nested loop over `(measures, positions, pricing_and_market_data_as_of)`:
   a. Look up handler from `result_handlers` dict using `date_result.get('$type')`.
   b. Construct `RiskKey` from `(cls, pricing_date, market, parameters, scenario, risk_measure)`.
   c. Branch: handler exists -> call `handler(date_result, risk_key, instrument, request_id=...)`.
   d. Branch: handler is None -> use `date_result` as-is.
   e. Branch: exception during handler call -> wrap in `ErrorValue`, log error.
3. Return `formatted_results` dict.

**Raises:** No direct raises; handler exceptions are caught and converted to `ErrorValue`.

## State Mutation
- `GenericRiskApi.batch_dates`: Class-level attribute, never mutated at runtime.
- `RiskApi.__SHUTDOWN_SENTINEL`: Class-level constant, never mutated.
- `pending` dict: Mutated in `populate_pending_futures` via `pop` and `popitem` as results arrive.
- Queue objects (`queue.Queue`, `asyncio.Queue`): Created locally within `run()` and `populate_pending_futures()`; mutated by put/get operations across threads.
- `PricingFuture` instances: `set_result()` called once per future when results arrive.
- Thread safety: Communication between threads uses `queue.Queue` (thread-safe) and `asyncio.Queue` (with `call_soon_threadsafe` for cross-thread enqueue). The shutdown sentinel pattern ensures clean thread termination. The `GsSession` context manager is used within each thread.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `run_async` | When async result handler returns a non-empty error string |
| `ErrorValue` (not raised, set as result) | `populate_pending_futures` | When no result is returned for a pending future (non-async mode) |
| `ErrorValue` (not raised, set as result) | `build_keyed_results` | When a result handler raises an exception |

## Edge Cases
- When `run()` itself raises an exception, `populate_pending_futures` catches it and enqueues the exception paired with every pending key, ensuring all futures get resolved.
- In non-async mode (`is_async=False`), any pending futures remaining after queue draining are filled with `ErrorValue('No result returned')`.
- In async mode (`is_async=True`), pending futures are NOT filled with error values since the session may be reused.
- The `enqueue` method handles both iterable and non-iterable items by catching `TypeError` on `iter()`.
- Python version branching: `sys.version_info >= (3, 7)` uses `asyncio.run()`; older versions manually manage the event loop with fallback to existing loops.
- The `__handle_queue_update` works with both `queue.Queue` and `asyncio.Queue` by catching both `queue.Empty` and `asyncio.QueueEmpty`.
- `build_keyed_results` with an Exception input fabricates synthetic error dicts to match the shape of a normal response, ensuring every measure/position/date slot gets an error entry.
- `drain_queue_async` with `timeout=None` (or `0`/falsy) bypasses `wait_for` and calls `q.get()` directly (blocks indefinitely).

## Bugs Found
- Line 101 (potential): `start_date.isoformat()` in `get_comparison_results` will raise `AttributeError` if `start_date` is None since the `isoformat()` call is inside the dict literal (not guarded by the filter). This is in backtests.py, not this file -- no bugs found in risk.py.
- None found in this module.

## Coverage Notes
- Branch count: ~40
  - `populate_pending_futures`: 7 branches (try/except on run, while loop condition, future is not None, cache_impl is not None, is_async check, pending popitem loop)
  - `__handle_queue_update`: 4 branches (sentinel check, loop elem is sentinel vs not, QueueEmpty break)
  - `drain_queue`: 2 branches (success vs Empty)
  - `drain_queue_async`: 3 branches (timeout truthy/falsy, success vs TimeoutError)
  - `enqueue`: 4 branches (iterable vs TypeError, wait vs nowait, loop vs no loop)
  - `shutdown_queue_listener`: 2 branches (loop open vs else)
  - `run/run_async`: ~12 branches (is_async, expected > chunk_size, requests non-empty, shutdown break, progress_bar, unprocessed_results, results_handler, results_error)
  - `run` main flow: ~6 branches (version check, existing loop, use_existing, exception handling)
  - `build_keyed_results`: 4 branches (Exception check, handler exists, handler call success/failure)
- The Python < 3.7 event loop fallback path (lines 323-344) is difficult to cover in modern test environments.
- Abstract methods (`calc`, `get_results`, `populate_pending_futures` on GenericRiskApi, `build_keyed_results` on GenericRiskApi) have no implementation to cover.
