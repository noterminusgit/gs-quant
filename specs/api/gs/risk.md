# risk.py

## Summary
GS Risk API client implementing the `RiskApi` abstract base class, providing synchronous and asynchronous risk calculation execution, result retrieval via polling or WebSocket subscription, pre-trade execution optimization, and liquidity/factor analysis. Handles msgpack and JSON serialization, automatic retries on WebSocket disconnections, and structured error response parsing.

## Dependencies
- Internal: `gs_quant.api.risk` (RiskApi), `gs_quant.errors` (MqValueError), `gs_quant.risk` (RiskRequest), `gs_quant.target.risk` (OptimizationRequest), `gs_quant.tracing` (Tracer, TracingSpan)
- External: `asyncio`, `base64`, `datetime` (date, datetime, timedelta), `json`, `logging`, `math`, `os`, `sys`, `time`, `re`, `socket` (gaierror), `typing` (Iterable, Optional, Union), `msgpack`, `websockets` (ConnectionClosed)

## Type Definitions

### WebsocketUnavailable (class)
Inherits: `Exception`

Custom exception raised when the WebSocket connection cannot be established (e.g., DNS resolution failure).

### GsRiskApi (class)
Inherits: `RiskApi`

No instance fields -- all methods are classmethods. Contains class-level configuration constants.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

### GsRiskApi Class Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| USE_MSGPACK | `bool` | `True` | Whether to use msgpack serialization for bulk requests |
| POLL_FOR_BATCH_RESULTS | `bool` | `False` | Whether to poll (True) or use WebSocket (False) for batch results |
| WEBSOCKET_RETRY_ON_CLOSE_CODES | `tuple` | `(1000, 1001, 1006)` | WebSocket close codes that trigger a reconnection attempt |
| PRICING_API_VERSION | `None` | `None` | Override for the API version in the URL; when None, uses session's `api_version` |

## Functions/Methods

### GsRiskApi.calc_multi(requests: Iterable[RiskRequest]) -> dict
Purpose: Execute multiple risk calculation requests as a single bulk operation and return a dict mapping each request to its result.

**API Endpoint:** `POST /{version}/risk/calculate/bulk` (via `_exec`)

**Algorithm:**
1. Convert `requests` to tuple
2. Call `cls._exec(requests)` to execute bulk
3. Branch: `len(results) < len(requests)` -> replace results with `[RuntimeError('Missing results')] * len(requests)`
4. Return `dict(zip(requests, results))`

### GsRiskApi.calc(request: RiskRequest) -> Iterable
Purpose: Execute a single risk calculation request.

**API Endpoint:** `POST /{version}/risk/calculate` (via `_exec`)

**Algorithm:**
1. Call `cls._exec(request)` and return the result

### GsRiskApi._exec(request: Union[RiskRequest, Iterable[RiskRequest]]) -> Union[Iterable, dict]
Purpose: Internal execution method that dispatches a risk calculation to the API.

**API Endpoint:** `POST /{version}/risk/calculate` (single) or `POST /{version}/risk/calculate/bulk` (bulk)

**Algorithm:**
1. Determine `use_msgpack`: True if `cls.USE_MSGPACK` AND request is not a single `RiskRequest`
2. Branch: `use_msgpack` -> set `Content-Type: application/x-msgpack` header; else -> empty headers
3. Get risk session via `cls.get_session()`
4. Determine `version` from `PRICING_API_VERSION` or `risk_session.api_version`
5. POST to `/{version}/risk/calculate` (single) or `/{version}/risk/calculate/bulk` (bulk) with `timeout=181` and `return_request_id=True`
6. Iterate through each sub-request and set `sub_request._id = request_id`
7. Return result

### GsRiskApi.__url(request: Union[RiskRequest, Iterable[RiskRequest]]) -> str
Purpose: Compute the URL path suffix based on whether the request is single or bulk.

**Algorithm:**
1. `is_bulk = not isinstance(request, RiskRequest)`
2. Return `'/risk/calculate'` + `'/bulk'` if bulk, else `''`

### GsRiskApi.get_results(responses: asyncio.Queue, results: asyncio.Queue, timeout: Optional[int] = None, span: Optional[TracingSpan] = None) -> Optional[str]
Purpose: Retrieve calculation results either via polling or WebSocket, falling back to polling if WebSocket is unavailable.

**Algorithm:**
1. Branch: `cls.POLL_FOR_BATCH_RESULTS` is True -> await `__get_results_poll(...)`
2. Branch: False:
   a. Try: await `__get_results_ws(...)`
   b. Catch `WebsocketUnavailable` -> fall back to await `__get_results_poll(...)`

### GsRiskApi.__get_results_poll(responses: asyncio.Queue, results: asyncio.Queue, timeout: Optional[int] = None) -> Optional[str]
Purpose: Poll the API for batch calculation results.

**API Endpoint:** `POST /{version}/risk/calculate/results/bulk`

**Algorithm:**
1. Initialize `pending_requests = {}`, `run = True`, compute `end_time` from timeout
2. Loop while `pending_requests` or `run`:
   a. Branch: timeout exceeded -> log error, shutdown queue listener, return
   b. Drain responses queue with 2-second timeout
   c. Branch: shutdown signal received -> set `run = False`
   d. Branch: items received -> update `pending_requests` with `{reportId: request}`
   e. Branch: no pending requests -> continue
   f. POST `/{version}/risk/calculate/results/bulk` with list of pending request IDs
   g. For each result:
      - Branch: `'error'` in result -> enqueue `(request, RuntimeError(error))`
      - Branch: `'result'` in result -> enqueue `(request, result['result'])`
   h. Catch Exception -> log fatal error, shutdown queue listener, return error string

### GsRiskApi.__get_results_ws(responses: asyncio.Queue, results: asyncio.Queue, timeout: Optional[int] = None) -> Optional[str]
Purpose: Subscribe to calculation results via WebSocket with automatic reconnection on close.

**API Endpoint:** WebSocket `/{version}/risk/calculate/results/subscribe`

**Algorithm:**
1. Define inner `async def handle_websocket()`:
   a. Re-subscribe pending requests if reconnecting
   b. Loop while pending requests or undispatched requests:
      - Create request listener (drain queue) and result listener (ws.recv)
      - `asyncio.wait` for either to complete
      - Branch: result_listener completed:
        - Parse raw response: split on `;` separator to get `request_id` and `status_char + data`
        - Branch: bytes response vs string response -> decode appropriately
        - Handle `ConnectionClosed`: Branch: retryable close code -> re-queue dispatched items, re-raise; else -> set error status
        - Handle other exceptions -> set error status
        - Branch: status `'E'` -> `RuntimeError(risk_data)`
        - Branch: status `'B'` -> `msgpack.unpackb(risk_data)` (raw binary)
        - Branch: status `'M'` -> `msgpack.unpackb(base64.b64decode(risk_data))` (base64 msgpack)
        - Branch: status `'R'` (or other) -> `json.loads(risk_data)`
        - Branch: `request_id is None` -> abort all pending requests with error
        - Branch: `request_id` valid -> enqueue result, remove from pending
      - Branch: result_listener not completed -> cancel it
      - Branch: request_listener completed:
        - Extract `all_requests_dispatched` flag and items
        - Validate items are dicts; raise `RuntimeError` if error item found
        - Extract `reportId`s, update pending requests
        - Send request IDs over WebSocket
        - Update dispatched set
      - Branch: request_listener not completed -> cancel it
2. Initialize `all_requests_dispatched = False`, `pending_requests = {}`, `dispatched = set()`, `max_attempts = 5`, `send_timeout = 30`
3. Retry loop (up to `max_attempts`):
   a. Branch: retry attempt > 0 -> sleep with exponential backoff `2^(attempts-1)`
   b. Connect WebSocket at `/{version}/risk/calculate/results/subscribe` with optional msgpack-binary subprotocol and 50ms close timeout
   c. Call `handle_websocket()`
   d. Branch: success -> set `attempts = max_attempts` (exit loop)
   e. Catch `ConnectionClosed` -> increment attempts, log, retry
   f. Catch `asyncio.TimeoutError` -> set `attempts = max_attempts` (give up)
   g. Catch `gaierror` -> raise `WebsocketUnavailable`
   h. Catch other `Exception` -> set `attempts = max_attempts` (give up)
4. Branch: error is non-empty -> log fatal error, set tracing span error tags, shutdown queue listener, return error string

### GsRiskApi.create_pretrade_execution_optimization(request: OptimizationRequest) -> Union[str, dict]
Purpose: Create a pre-trade execution optimization request.

**API Endpoint:** `POST /risk/execution/pretrade`

**Algorithm:**
1. Try: POST `/risk/execution/pretrade` with request payload
2. Log the created optimization ID
3. Return response
4. Catch Exception -> log error, return error string

### GsRiskApi.get_pretrade_execution_optimization(optimization_id: str, max_attempts: int = 15) -> Union[dict, str]
Purpose: Poll for pre-trade execution optimization results with retry logic.

**API Endpoint:** `GET /risk/execution/pretrade/{optimization_id}/results`

**Algorithm:**
1. Build URL `/risk/execution/pretrade/{optimization_id}/results`
2. Initialize `attempts = 0`, `start = time.perf_counter()`, `results = {}`
3. Loop while `attempts < max_attempts`:
   a. Branch: retry attempt > 0 -> sleep `2^attempts` seconds, log retry
   b. Try: GET the URL
   c. Branch: `status == 'Running'` -> increment attempts, continue
   d. Branch: status is not Running -> break
   e. Catch Exception -> log error, return error string
4. Branch: final `status == 'Running'` -> log still running, return results
5. Branch: completed -> log fetch time, return results

### GsRiskApi.get_liquidity_and_factor_analysis(positions: list, risk_model: str, date: dt.date, currency: str = 'USD', participation_rate: float = 0.1, measures: Optional[list] = None, notional: Optional[float] = None, time_series_benchmark_ids: Optional[list] = None) -> dict
Purpose: Get liquidity and factor analysis for a portfolio.

**API Endpoint:** `POST /risk/liquidity`

**Algorithm:**
1. Branch: `measures` is None -> set default measures list: `["Time Series Data", "Risk Buckets", "Factor Risk Buckets", "Factor Exposure Buckets", "Exposure Buckets"]`
2. Build payload with currency, date (ISO-formatted if `dt.date`), positions, participationRate, riskModel, timeSeriesBenchmarkIds (or `[]`), measures
3. Branch: `notional` is not None -> add to payload
4. Try: POST `/risk/liquidity`
5. Branch: response is dict AND has `'errorMessage'`:
   a. Search for asset IDs pattern in error message: `Assets with the following ids are missing in marquee: [...]`
   b. Branch: pattern found:
      - Try to extract clean error line
      - Branch: clean line found -> raise `MqValueError` with clean message
      - Branch: no clean line -> extract missing asset IDs and raise `MqValueError` with formatted message
   c. Branch: pattern not found -> raise `MqValueError("ERROR: liquidity analysis failed")`
6. Log success, return response
7. Catch Exception -> re-raise

**Raises:** `MqValueError` when the liquidity analysis response contains an error message

## State Mutation
- `sub_request._id`: Set on each sub-request in `_exec()` after the POST returns a `request_id`
- `GsRiskApi.USE_MSGPACK`: Class-level, can be modified externally to toggle msgpack usage
- `GsRiskApi.POLL_FOR_BATCH_RESULTS`: Class-level, can be modified externally to toggle polling vs WebSocket
- `GsRiskApi.PRICING_API_VERSION`: Class-level, can be modified externally to override API version
- Thread safety: `_exec` and `calc` are classmethods with no shared mutable state beyond class constants. The WebSocket result handler uses `asyncio` concurrency primitives. Polling uses `asyncio.Queue` for thread-safe communication.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError('Missing results')` | `calc_multi` | When fewer results returned than requests submitted |
| `WebsocketUnavailable` | `__get_results_ws` | DNS resolution failure (`gaierror`) |
| `RuntimeError` | `__get_results_ws` | Error items in dispatched requests |
| `RuntimeError` | `__get_results_ws` / `__get_results_poll` | Error status in individual result |
| `MqValueError` | `get_liquidity_and_factor_analysis` | Error message in API response (missing assets or generic failure) |

## Edge Cases
- `_exec` only uses msgpack headers for bulk (non-single-`RiskRequest`) requests, even when `USE_MSGPACK=True`
- `__get_results_ws` retries only on `ConnectionClosed` with codes in `WEBSOCKET_RETRY_ON_CLOSE_CODES` (1000, 1001, 1006); other close codes are treated as errors
- `__get_results_ws` uses a 50ms WebSocket close timeout to avoid waiting for server acknowledgment
- `__get_results_ws` has a `send_timeout` of 30 seconds for sending subscription messages
- `__get_results_ws` handles both bytes and string WebSocket messages; parses the `request_id;status_char+data` protocol format
- `__get_results_ws` when `request_id` is None (from a fatal WebSocket error before parsing), all pending results are set to the error and the handler gives up
- `get_pretrade_execution_optimization` uses exponential backoff with `time.sleep(2^attempts)` which grows to 16384 seconds (>4.5 hours) at attempt 14 -- this is effectively a very long-running poll
- `get_liquidity_and_factor_analysis` uses regex to parse structured error messages from the API, extracting missing asset IDs for cleaner error reporting
- `get_liquidity_and_factor_analysis` has a catch-all `except Exception: raise` at the end which re-raises any exception including the `MqValueError` raised within the try block
- `create_pretrade_execution_optimization` catches all exceptions and returns the error string instead of raising, meaning callers receive a string on failure rather than an exception

## Bugs Found
- Line 458-459: The `except Exception: raise` in `get_liquidity_and_factor_analysis` is redundant -- it catches and immediately re-raises, providing no additional value. It may mask the intent of the original developer. (OPEN -- style)
- Line 355: In `get_pretrade_execution_optimization`, `time.sleep(math.pow(2, attempts))` at max attempt 14 would sleep for 16384 seconds (~4.5 hours). The exponential backoff has no upper bound cap. (OPEN)

## Coverage Notes
- Branch count: ~65
- Missing branches: WebSocket binary vs string parsing paths, `ConnectionClosed` retry vs non-retry close codes, msgpack `'B'` vs `'M'` vs JSON `'R'` deserialization branches, `gaierror` path raising `WebsocketUnavailable`
- Pragmas: None
