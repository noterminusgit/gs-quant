# api/gs/backtests.py

## Summary
API client for the GS Backtests service. `GsBacktestApi` provides synchronous CRUD operations for backtest definitions, scheduling, execution, result retrieval, comparison results, position risk calculation, and reference data management. `GsBacktestApiAsync` extends it with async overrides for `run_backtest` and `calculate_position_risk`. The `backtest_result_from_response` method parses raw API responses into structured `BacktestResult` objects.

## Dependencies
- Internal: `gs_quant.common` (`FieldValueMap`)
- Internal: `gs_quant.errors` (`MqValueError`)
- Internal: `gs_quant.session` (`GsSession`, `DEFAULT_TIMEOUT`)
- Internal: `gs_quant.target.backtests` (`Backtest`, `BacktestResult`, `BacktestRisk`, `ComparisonBacktestResult`, `BacktestRiskRequest`, `BacktestRefData`)
- External: `datetime` (date)
- External: `logging` (getLogger)
- External: `typing` (Tuple, Optional)
- External: `urllib.parse` (urlencode)

## Type Definitions

### GsBacktestApi (class)
Inherits: `object`

Stateless API client class. All methods are classmethods; no instance state.

### GsBacktestApiAsync (class)
Inherits: `GsBacktestApi`

Async variant that overrides `calculate_position_risk` and `run_backtest` with async implementations using `GsSession.current.async_`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsBacktestApi.get_many_backtests(cls, limit, backtest_id, owner_id, name, mq_symbol) -> Tuple[Backtest, ...]
Purpose: Retrieve multiple backtests with optional filters.

**Algorithm:**
1. Build a dict with keys `id`, `ownerId`, `name`, `mqSymbol`, `limit`.
2. Filter out items where value is None.
3. URL-encode the filtered dict into a query string.
4. GET `/backtests?{query}` with `cls=Backtest`.
5. Return `['results']` from the response.

---

### GsBacktestApi.get_backtest(cls, backtest_id) -> Backtest
Purpose: Retrieve a single backtest by ID.

**Algorithm:**
1. GET `/backtests/{id}` with `cls=Backtest`.

---

### GsBacktestApi.create_backtest(cls, backtest) -> Backtest
Purpose: Create a new backtest definition.

**Algorithm:**
1. POST `/backtests` with JSON content-type headers, payload=backtest, cls=Backtest.

---

### GsBacktestApi.update_backtest(cls, backtest) -> Backtest
Purpose: Update an existing backtest definition.

**Algorithm:**
1. PUT `/backtests/{id}` with JSON content-type headers, payload=backtest, cls=Backtest.

---

### GsBacktestApi.delete_backtest(cls, backtest_id) -> dict
Purpose: Delete a backtest by ID.

**Algorithm:**
1. DELETE `/backtests/{id}`.

---

### GsBacktestApi.get_results(cls, backtest_id) -> Tuple[BacktestResult, ...]
Purpose: Retrieve backtest results for a given backtest ID.

**Algorithm:**
1. GET `/backtests/results?id={id}`.
2. Return `['backtestResults']` from the response.

---

### GsBacktestApi.get_comparison_results(cls, limit, start_date, end_date, backtest_id, comparison_id, owner_id, name, mq_symbol) -> Tuple[Tuple[BacktestResult, ...], Tuple[ComparisonBacktestResult, ...]]
Purpose: Retrieve comparison backtest results with optional filters.

**Algorithm:**
1. Build a dict with keys `id`, `comparisonIds`, `ownerId`, `name`, `mqSymbol`, `limit`, `startDate` (from `start_date.isoformat()`), `endDate` (from `end_date.isoformat()`).
2. Filter out items where value is None.
3. URL-encode and GET `/backtests/results?{query}`.
4. Return tuple of `(result['backtestResults'], result['comparisonResults'])`.

---

### GsBacktestApi.schedule_backtest(cls, backtest_id) -> dict
Purpose: Schedule a backtest for execution.

**Algorithm:**
1. POST `/backtests/{id}/schedule`.

---

### GsBacktestApi.run_backtest(cls, backtest, correlation_id, timeout) -> BacktestResult
Purpose: Run a backtest calculation synchronously and return parsed results.

**Algorithm:**
1. Build JSON content-type request headers.
2. Branch: `correlation_id is not None` -> add `"X-CorrelationId"` header.
3. POST `/backtests/calculate` with backtest payload and timeout.
4. Delegate to `cls.backtest_result_from_response(response)`.

---

### GsBacktestApi.backtest_result_from_response(cls, response) -> BacktestResult
Purpose: Parse a raw backtest response dict into a `BacktestResult`.

**Algorithm:**
1. Branch: `'RiskData' not in response` -> raise `MqValueError('No risk data received')`.
2. Branch: `'Portfolio' in response` -> extract portfolio; else `portfolio = None`.
3. Build `risks` tuple: for each `(k, v)` in `response['RiskData'].items()`, create `BacktestRisk(name=k, timeseries=tuple(FieldValueMap(date=r['date'], value=r['value']) for r in v))`.
4. Return `BacktestResult(portfolio=portfolio, risks=risks)`.

**Raises:** `MqValueError` when response lacks `'RiskData'` key.

---

### GsBacktestApi.calculate_position_risk(cls, backtestRiskRequest, timeout) -> dict
Purpose: Calculate position-level risk for a backtest.

**Algorithm:**
1. POST `/backtests/calculate-position-risk` with JSON headers, payload, and timeout.

---

### GsBacktestApi.get_ref_data(cls) -> BacktestRefData
Purpose: Retrieve backtest reference data.

**Algorithm:**
1. GET `/backtests/refData` with `cls=BacktestRefData`.

---

### GsBacktestApi.update_ref_data(cls, backtest_ref_data) -> None
Purpose: Update backtest reference data.

**Algorithm:**
1. PUT `/backtests/refData` with JSON headers, payload=backtest_ref_data, cls=backtest_ref_data.

---

### GsBacktestApiAsync.calculate_position_risk(cls, backtestRiskRequest, timeout) -> dict [async]
Purpose: Async version of position risk calculation.

**Algorithm:**
1. Build JSON content-type headers.
2. `await GsSession.current.async_.post(...)` with payload and timeout.
3. Return response.

---

### GsBacktestApiAsync.run_backtest(cls, backtest, correlation_id, timeout) -> BacktestResult [async]
Purpose: Async version of backtest execution.

**Algorithm:**
1. Build JSON content-type headers.
2. Branch: `correlation_id is not None` -> add `"X-CorrelationId"` header.
3. `await GsSession.current.async_.post(...)` with payload and timeout.
4. Delegate to `cls.backtest_result_from_response(response)`.

## State Mutation
- No instance state; all methods are classmethods operating on local variables.
- Thread safety: Stateless; safe for concurrent use. Thread safety of `GsSession.current` is managed externally.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `backtest_result_from_response` | Response dict does not contain `'RiskData'` key |

## Edge Cases
- `get_comparison_results` calls `start_date.isoformat()` and `end_date.isoformat()` unconditionally inside the dict literal before the None-filter runs. If `start_date` or `end_date` is None, this will raise `AttributeError`. Callers must always provide both dates.
- `get_many_backtests` uses `urlencode` on filtered params; if all optional params are None, only `limit` is included.
- `update_ref_data` passes `cls=backtest_ref_data` (the instance, not the class). This appears intentional for the GS session serializer but is unusual.
- `backtest_result_from_response` with an empty `RiskData` dict (key present but empty) will return a `BacktestResult` with an empty `risks` tuple -- this is valid.
- The async class inherits `backtest_result_from_response` from the sync parent; no duplication.
- All request headers use `'application/json;charset=utf-8'` for both Content-Type and Accept.

## Bugs Found
- Line 101: `start_date.isoformat()` is called inside the dict literal in `get_comparison_results` before the `filter(lambda item: item[1] is not None, ...)` runs. If `start_date` is None, `NoneType` has no method `isoformat()`, causing `AttributeError`. Same for `end_date`. This is a latent bug -- the method will crash if either date is None. (OPEN)
- Line 163: `cls=backtest_ref_data` passes an instance rather than a class to the `put` call. May be intentional for the session framework but is inconsistent with all other `cls=` usages in this module that pass class objects. (OPEN -- needs verification)

## Coverage Notes
- Branch count: ~14
  - `get_many_backtests`: 1 branch (filter removes None values, but no conditional logic to branch on)
  - `get_comparison_results`: 1 branch (filter removes None values)
  - `run_backtest`: 2 branches (correlation_id None vs not)
  - `backtest_result_from_response`: 3 branches (no RiskData, Portfolio present vs absent, happy path)
  - `calculate_position_risk`: 1 branch (straight-through)
  - `GsBacktestApiAsync.run_backtest`: 2 branches (correlation_id None vs not)
  - `GsBacktestApiAsync.calculate_position_risk`: 1 branch (straight-through)
  - Other CRUD methods: 1 branch each (straight-through API calls)
- All branches are straightforward to cover with mocked `GsSession`.
- The `_logger` is defined but never used in this module.
