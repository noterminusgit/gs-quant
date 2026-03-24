# apis.py

## Summary
Provides synchronous and asynchronous API clients for the cross-asset backtesting service. `GsBacktestXassetApi` sends risk calculation and basic backtest requests via `GsSession`, and `GsBacktestXassetApiAsync` is its async counterpart.

## Dependencies
- Internal: `gs_quant.api.gs.backtests_xasset.request` (RiskRequest, BasicBacktestRequest), `gs_quant.api.gs.backtests_xasset.response` (RiskResponse, BasicBacktestResponse), `gs_quant.session` (GsSession)
- External: none

## Type Definitions

### GsBacktestXassetApi (class)
Inherits: none

Stateless API client with class-level configuration constants and two `@classmethod` endpoints.

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| HEADERS | `dict` | `{'Accept': 'application/json'}` | Default HTTP request headers |
| TIMEOUT | `int` | `90` | HTTP request timeout in seconds |

## Functions/Methods

### GsBacktestXassetApi.calculate_risk(cls, risk_request: RiskRequest) -> RiskResponse
Purpose: Submit a risk calculation request to `/backtests/xasset/risk` and return the deserialized response.

**Algorithm:**
1. Serialize `risk_request` to JSON via `risk_request.to_json()`.
2. POST to `/backtests/xasset/risk` using `GsSession.current.sync.post` with class headers and timeout.
3. Deserialize response dict via `RiskResponse.from_dict(response)`.
4. Return `RiskResponse`.

### GsBacktestXassetApi.calculate_basic_backtest(cls, backtest_request: BasicBacktestRequest, decode_instruments: bool = True) -> BasicBacktestResponse
Purpose: Submit a basic backtest request to `/backtests/xasset/strategy/basic` and return the deserialized response.

**Algorithm:**
1. Serialize `backtest_request` to JSON via `backtest_request.to_json()`.
2. POST to `/backtests/xasset/strategy/basic` using `GsSession.current.sync.post` with class headers and timeout.
3. Deserialize response via `BasicBacktestResponse.from_dict_custom(response, decode_instruments)`.
4. Return `BasicBacktestResponse`.

### GsBacktestXassetApiAsync (class)
Inherits: `GsBacktestXassetApi`

Overrides both methods with `async` versions that use `GsSession.current.async_.post` (with `await`) instead of `GsSession.current.sync.post`. All other logic is identical.

### GsBacktestXassetApiAsync.calculate_risk(cls, risk_request: RiskRequest) -> RiskResponse
Purpose: Async version of `calculate_risk`. Same algorithm but uses `await GsSession.current.async_.post(...)`.

### GsBacktestXassetApiAsync.calculate_basic_backtest(cls, backtest_request: BasicBacktestRequest, decode_instruments: bool = True) -> BasicBacktestResponse
Purpose: Async version of `calculate_basic_backtest`. Same algorithm but uses `await GsSession.current.async_.post(...)`.

## Elixir Porting Notes
- Replace `@classmethod` with module functions or a behaviour/protocol, since Elixir has no classes.
- The sync/async split can be collapsed: Elixir HTTP clients (e.g. `Req`, `Tesla`) are naturally async-friendly. Expose a single function that returns `{:ok, result}` or `{:error, reason}`.
- `GsSession.current` is thread-local state; in Elixir, pass the session/config as an explicit argument or use process dictionary / context structs.
- Serialization (`to_json`, `from_dict`) maps to `Jason.encode!/1` and custom `from_map/1` functions on the corresponding Elixir struct modules.
- HTTP headers and timeout become options in the HTTP client call.

## Edge Cases
- If the session is not initialized, `GsSession.current` will raise. Elixir port should return `{:error, :no_session}`.
- `decode_instruments=False` skips full instrument deserialization in `calculate_basic_backtest`; this path must be preserved.
