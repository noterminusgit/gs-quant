# api/gs/indices.py

## Summary
API client for the GS Indices service, supporting custom basket and iSelect strategy lifecycle operations: create, edit, rebalance, cancel rebalance, backcast, risk report management, ticker validation, and position data retrieval. `GsIndexApi` uses a `_response_cls` dispatch table to select the correct response type based on the input type. Includes retry logic via `backoff` decorators for position data retrieval.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (`IdList`)
- Internal: `gs_quant.common` (`PositionType`)
- Internal: `gs_quant.errors` (`MqTimeoutError`, `MqInternalServerError`, `MqRateLimitedError`)
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.target.indices` (`CustomBasketsCreateInputs`, `CustomBasketsRebalanceInputs`, `CustomBasketsRebalanceAction`, `CustomBasketsResponse`, `CustomBasketsEditInputs`, `CustomBasketsBackcastInputs`, `CustomBasketsRiskScheduleInputs`, `CustomBasketRiskParams`, `ISelectResponse`, `ISelectRequest`, `ISelectRebalance`, `ISelectActionRequest`, `IndicesDynamicConstructInputs`, `IndicesRebalanceInputs`, `IndicesEditInputs`, `DynamicConstructionResponse`, `ApprovalCustomBasketResponse`, `IndicesBackcastInputs`)
- External: `datetime` (date)
- External: `typing` (Dict, List, Union)
- External: `backoff` (on_exception, expo, constant)

## Type Definitions

### Type Aliases
```
CreateRequest = Union[CustomBasketsCreateInputs, IndicesDynamicConstructInputs]
CreateRepsonse = Union[CustomBasketsResponse, DynamicConstructionResponse]
RebalanceRequest = Union[CustomBasketsRebalanceInputs, ISelectRebalance, ISelectRequest]
RebalanceResponse = Union[CustomBasketsResponse, ISelectResponse]
RebalanceCancelRequest = Union[CustomBasketsRebalanceAction, ISelectActionRequest]
RebalanceCancelResponse = Union[Dict, ISelectResponse]
ValidatedRequest = Union[CreateRequest, RebalanceRequest]
```

Note: `CreateRepsonse` has a typo (missing 'o' in "Response"). This is present in the source code.

### GsIndexApi (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_response_cls` | `dict` | `{CustomBasketsCreateInputs: CustomBasketsResponse, IndicesDynamicConstructInputs: DynamicConstructionResponse, CustomBasketsRebalanceInputs: CustomBasketsResponse, ISelectRebalance: ISelectResponse, ISelectRequest: ISelectResponse, CustomBasketsRebalanceAction: Dict, ISelectActionRequest: ISelectResponse}` | Maps input types to their corresponding response class for deserialization |

## Enums and Constants

None beyond the type aliases and `_response_cls` dispatch table.

## Functions/Methods

### GsIndexApi.create(cls, inputs) -> CreateRepsonse
Purpose: Create a new basket or iSelect strategy.

**Algorithm:**
1. Look up `response_cls` from `cls._response_cls[type(inputs)]`.
2. POST `/indices` with payload=inputs, cls=response_cls.

---

### GsIndexApi.edit(cls, id_, inputs) -> CustomBasketsResponse
Purpose: Update basket metadata.

**Algorithm:**
1. Wrap `inputs` in `IndicesEditInputs(parameters=inputs)`.
2. POST `/indices/{id_}/edit` with payload, cls=CustomBasketsResponse.

---

### GsIndexApi.rebalance(cls, id_, inputs) -> RebalanceResponse
Purpose: Rebalance an existing index with a new composition.

**Algorithm:**
1. Look up `response_cls` from `cls._response_cls[type(inputs)]`.
2. Branch: `not isinstance(inputs, ISelectRequest)` -> wrap in `IndicesRebalanceInputs(parameters=inputs)`.
3. Else (is ISelectRequest) -> use inputs as-is.
4. POST `/indices/{id_}/rebalance` with payload, cls=response_cls.

---

### GsIndexApi.cancel_rebalance(cls, id_, inputs) -> RebalanceCancelResponse
Purpose: Cancel the most recent rebalance submission if not yet approved.

**Algorithm:**
1. Look up `response_cls` from `cls._response_cls[type(inputs)]`.
2. POST `/indices/{id_}/rebalance/cancel` with payload, cls=response_cls.

---

### GsIndexApi.last_rebalance_data(cls, id_) -> Dict
Purpose: Get the latest basket rebalance data.

**Algorithm:**
1. GET `/indices/{id_}/rebalance/data/last`.

---

### GsIndexApi.last_rebalance_approval(cls, id_) -> ApprovalCustomBasketResponse
Purpose: Get the latest basket rebalance approval info.

**Algorithm:**
1. GET `/indices/{id_}/rebalance/approvals/last` with cls=ApprovalCustomBasketResponse.

---

### GsIndexApi.initial_price(cls, id_, date) -> Dict
Purpose: Get the initial basket price for a given date.

**Algorithm:**
1. GET `/indices/{id_}/rebalance/initialprice/{date.isoformat()}`.

---

### GsIndexApi.validate_ticker(cls, ticker) -> None
Purpose: Validate a basket ticker (raises on invalid).

**Algorithm:**
1. POST `/indices/validate` with payload `{'ticker': ticker}`.

---

### GsIndexApi.backcast(cls, _id, inputs) -> CustomBasketsResponse
Purpose: Backcast basket composition history before the live date.

**Algorithm:**
1. Wrap `inputs` in `IndicesBackcastInputs(parameters=inputs)`.
2. POST `/indices/{_id}/backcast` with payload, cls=CustomBasketsResponse, timeout=240.

---

### GsIndexApi.update_risk_reports(cls, _id, inputs) -> None
Purpose: Create, modify, or delete a custom basket factor risk report.

**Algorithm:**
1. Wrap `inputs` in `CustomBasketsRiskScheduleInputs(risk_models=inputs)`.
2. POST `/indices/{_id}/risk/reports` with payload.

---

### GsIndexApi.get_positions_data(asset_id, start_date, end_date, fields, position_type) -> List[dict] [staticmethod]
Purpose: Get position data for an index within a date range. Has exponential backoff retry on timeout/server errors and constant backoff on rate limiting.

**Decorators:**
- `@backoff.on_exception(lambda: backoff.expo(base=2, factor=2), (MqTimeoutError, MqInternalServerError), max_tries=5)`
- `@backoff.on_exception(lambda: backoff.constant(90), MqRateLimitedError, max_tries=5)`

**Algorithm:**
1. Format `start_date` and `end_date` as ISO strings.
2. Build URL: `/indices/{asset_id}/positions/data?startDate={start_date}&endDate={end_date}`.
3. Branch: `fields is not None` -> append `&fields=` joined fields to URL.
4. Branch: `position_type is not None` -> append `&type={position_type.value}` to URL.
5. GET the URL and return `['results']`.

---

### GsIndexApi.get_last_positions_data(asset_id, fields, position_type) -> List[dict] [staticmethod]
Purpose: Get the latest position data for an index.

**Algorithm:**
1. Build base URL: `/indices/{asset_id}/positions/last/data`.
2. Initialize `params = ''`.
3. Branch: `fields is not None` -> append `&fields=` joined fields to params.
4. Branch: `position_type is not None` -> append `&type={position_type.value}` to params.
5. Branch: `len(params) > 0` -> append `?{params}` to URL.
6. GET the URL and return `['results']`.

## State Mutation
- `_response_cls`: Class-level dict, never mutated at runtime.
- No instance state; all methods are classmethods or staticmethods operating on local variables.
- Thread safety: Stateless; safe for concurrent use. The `backoff` decorators add retry state but it is per-call (not shared).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `create`, `rebalance`, `cancel_rebalance` | If `type(inputs)` is not in `_response_cls` (unexpected input type) |
| `MqTimeoutError` | `get_positions_data` | Retried up to 5 times with exponential backoff (base=2, factor=2) |
| `MqInternalServerError` | `get_positions_data` | Retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | `get_positions_data` | Retried up to 5 times with constant 90s backoff |

## Edge Cases
- `get_positions_data` builds fields into the URL with `'&fields='.join([''] + fields)`, which produces `&fields=field1&fields=field2...`. This relies on the server accepting repeated `fields` params.
- `get_last_positions_data` uses the same `&fields=` joining pattern. If `fields` is an empty list, `'&fields='.join([''])` produces just `""`, so no fields param is added. However, the `fields is not None` check would pass for an empty list, resulting in `params` being just `""`.
- `get_last_positions_data` initializes `params = ''` and conditionally appends `&`-prefixed params. If only one param is set, the URL becomes `...data?&fields=...` (with a leading `&` after `?`). This is technically valid but unusual.
- `rebalance` wraps all input types in `IndicesRebalanceInputs` except `ISelectRequest`, which is passed through directly.
- The `backoff` decorators on `get_positions_data` are stacked: the inner decorator (rate limit) is checked first, then the outer (timeout/server error).
- `validate_ticker` returns None implicitly; success is indicated by no exception from the POST.
- `backcast` uses a custom timeout of 240 seconds (vs the default timeout for other methods).
- `CreateRepsonse` type alias has a typo ("Repsonse" instead of "Response"). This should be preserved in the Elixir port for API compatibility if it is part of any public interface.

## Bugs Found
- None found. The URL construction patterns in `get_positions_data` and `get_last_positions_data` are slightly unconventional but functional.

## Coverage Notes
- Branch count: ~14
  - `rebalance`: 2 branches (ISelectRequest vs other)
  - `get_positions_data`: 3 branches (fields None vs not, position_type None vs not, plus happy path)
  - `get_last_positions_data`: 4 branches (fields None vs not, position_type None vs not, params empty vs not)
  - `create`, `cancel_rebalance`: 1 branch each (dispatch table lookup)
  - `edit`, `backcast`, `update_risk_reports`: 1 branch each (wrap and POST)
  - Other methods: 1 branch each (straight-through API calls)
- The `backoff` retry logic is difficult to unit test without triggering actual retries; mock the exceptions to test retry behavior.
- The `_response_cls` dispatch table is implicitly tested by covering each input type variant in `create`, `rebalance`, and `cancel_rebalance`.
