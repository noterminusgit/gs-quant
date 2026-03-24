# utils.py

## Summary
Utility module providing HTTP proxy handling for internal/external network environments and a thread pool manager for running async tasks with proper session, data context, and tracing propagation. These are shared infrastructure utilities used across the API layer.

## Dependencies
- Internal: `gs_quant.data` (DataContext), `gs_quant.errors` (MqUninitialisedError), `gs_quant.session` (GsSession), `gs_quant.tracing` (Tracer, TracingSpan)
- External: `concurrent`, `concurrent.futures.thread` (ThreadPoolExecutor), `typing` (List, Callable, Optional), `requests`, `socket`

## Type Definitions

### ThreadPoolManager (class)
Inherits: object (implicit)

A class-level (all static/class methods) thread pool manager that maintains a single shared `ThreadPoolExecutor`. Provides ordered async execution with automatic propagation of session, data context, and tracing span to worker threads.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__executor` | `ThreadPoolExecutor` | `None` | Class-level shared thread pool executor |

## Enums and Constants
None.

## Functions/Methods

### handle_proxy(url, params) -> requests.Response
Purpose: Make an HTTP GET request, routing through a proxy when running inside a GS internal network.

**Algorithm:**
1. Try to get `internal = GsSession.current.is_internal()`
2. Branch: on `MqUninitialisedError` -> set `internal = False`
3. Branch: if `internal` is `True` OR `socket.getfqdn()` ends with `['gs', 'com']`:
   a. Try to `import gs_quant_auth`
   b. Get `proxies` from `gs_quant_auth.__proxies__`
   c. Make `requests.get(url, params=params, proxies=proxies)`
   d. Branch: on `ModuleNotFoundError` -> raise `RuntimeError('You must install gs_quant_auth to be able to use this endpoint')`
4. Branch: else (external) -> make `requests.get(url, params=params)` without proxies
5. Return `response`

**Raises:** `RuntimeError` when on an internal network but `gs_quant_auth` module is not installed.

### ThreadPoolManager.initialize(cls, max_workers: int)
Purpose: Pre-initialize the shared thread pool with a specific number of workers.

**Algorithm:**
1. Set `cls.__executor = ThreadPoolExecutor(max_workers=max_workers)`

### ThreadPoolManager.run_async(cls, tasks: List[Callable]) -> List
Purpose: Submit a list of callables to the thread pool and return results in the original order.

**Algorithm:**
1. Branch: if `cls.__executor` is falsy (not initialized) -> create `cls.__executor = ThreadPoolExecutor()` with default workers
2. Create `tasks_to_idx` dict mapping futures to their original indices
3. For each `(i, task)` in `enumerate(tasks)`:
   a. Submit `cls.__run(GsSession.current, DataContext.current, Tracer.active_span(), task)` to executor
   b. Map the returned future to index `i`
4. Create `results` list of `None` values with length matching tasks
5. For each completed future via `concurrent.futures.as_completed(tasks_to_idx)`:
   a. Look up original index from `tasks_to_idx`
   b. Store `task.result()` at that index in `results`
6. Return `results`

### ThreadPoolManager.__run(session, data_context, span: Optional[TracingSpan], func) [staticmethod, private]
Purpose: Execute a task within the correct session, data context, and tracing span. This is the wrapper that runs in each worker thread.

**Algorithm:**
1. Activate the tracing span via `Tracer.activate_span(span)` context manager
2. Enter the session context manager (`with session:`)
3. Enter the data context manager (`with data_context:`)
4. Call and return `func()`

## State Mutation
- `ThreadPoolManager.__executor`: Class-level. Set by `initialize`; lazily created by `run_async` if not already set. Once created, reused across all subsequent `run_async` calls.
- Thread safety: `run_async` captures `GsSession.current`, `DataContext.current`, and `Tracer.active_span()` from the calling thread and injects them into worker threads via `__run`. This ensures each worker thread operates with the correct session, context, and span even though those are thread-local or context-managed in the calling thread.
- The `__executor` is shared across the entire process. Multiple concurrent calls to `run_async` share the same pool. The lazy initialization in `run_async` has a race condition if two threads call `run_async` simultaneously when `__executor` is `None` -- both may create a new `ThreadPoolExecutor`, but only the last assignment wins.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `handle_proxy` | When internal network detected but `gs_quant_auth` is not installed |
| `MqUninitialisedError` | `handle_proxy` (caught) | When `GsSession.current` is not initialized; caught and treated as external |
| Any exception from `func` | `run_async` via `task.result()` | Re-raised from worker thread when calling `.result()` on the future |

## Edge Cases
- `handle_proxy` with no active `GsSession` and a non-GS hostname makes a plain `requests.get` call (both conditions for internal are false)
- `handle_proxy` with no active `GsSession` but on a `*.gs.com` hostname still attempts the proxy path (the `or` short-circuits)
- `run_async` with an empty `tasks` list returns `[]` (loop body never executes)
- `run_async` preserves original task order despite `as_completed` returning futures in completion order
- If any task raises an exception, `task.result()` re-raises it, which will propagate out of `run_async` and may leave other results unprocessed
- `socket.getfqdn()` can be slow (DNS lookup) and may return unexpected values in containerized environments

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 7
- Key branches: `MqUninitialisedError` catch in `handle_proxy`, `internal or gs.com` check, `ModuleNotFoundError` catch, `not cls.__executor` lazy init in `run_async`
- Pragmas: none
