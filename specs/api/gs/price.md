# price.py

## Summary
API client wrapper for GS Price endpoints. Provides methods to price position sets with retry/backoff logic, and to price many positions using the v2 bulk pricing endpoint.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.errors` (MqRateLimitedError, MqTimeoutError, MqInternalServerError), `gs_quant.target.positions_v2_pricing` (PositionsPricingRequest), `gs_quant.target.price` (PositionSetPriceInput, PositionSetPriceResponse)
- External: `backoff`

## Type Definitions
None defined in this module. Uses `PositionSetPriceInput`, `PositionSetPriceResponse` from `gs_quant.target.price` and `PositionsPricingRequest` from `gs_quant.target.positions_v2_pricing`.

## Enums and Constants
None.

## Functions/Methods

### GsPriceApi.price_positions(cls, inputs: PositionSetPriceInput) -> PositionSetPriceResponse
Purpose: Price a position set with exponential and constant backoff retry on transient errors.

**Decorators:**
- `@backoff.on_exception(expo(base=2, factor=2), (MqTimeoutError, MqInternalServerError), max_tries=5)` -- exponential backoff for timeout and server errors
- `@backoff.on_exception(constant(60), MqRateLimitedError, max_tries=5)` -- constant 60s backoff for rate limiting

**Algorithm:**
1. POST `/price/positions` with `inputs` payload, `cls=PositionSetPriceResponse`
2. Return response

### GsPriceApi.price_many_positions(cls, pricing_request: PositionsPricingRequest) -> dict
Purpose: Bulk-price multiple positions using the v2 API endpoint.

**Algorithm:**
1. Set `GsSession.current.api_version` to `"v2"`
2. POST `/positions/price/bulk` with `pricing_request` payload
3. Reset `GsSession.current.api_version` to `"v1"`
4. Extract `"positions"` from response
5. Return positions

## State Mutation
- `GsSession.current.api_version`: Temporarily changed to `"v2"` in `price_many_positions`, then reset to `"v1"`.
- No instance state; all methods are `@classmethod`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqTimeoutError` | `price_positions` | Retried up to 5 times with exponential backoff |
| `MqInternalServerError` | `price_positions` | Retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | `price_positions` | Retried up to 5 times with 60s constant backoff |

## Edge Cases
- `price_many_positions` is not exception-safe regarding `api_version`: if the POST raises, `api_version` is never reset back to `"v1"`, leaving the session in a v2 state
- `price_many_positions` returns `None` if `"positions"` key is missing from response (`.get()` returns `None`)
- The backoff decorators use lambda factories for the wait generator: `lambda: backoff.expo(base=2, factor=2)`

## Bugs Found
- `price_many_positions` does not use try/finally to ensure `api_version` is reset to `"v1"` if the POST call raises an exception. This could leave the session in an inconsistent state. (OPEN)

## Coverage Notes
- Branch count: 0
- Both methods are linear (no conditional branches). Backoff retry logic is handled by the decorator.
- Pragmas: none
