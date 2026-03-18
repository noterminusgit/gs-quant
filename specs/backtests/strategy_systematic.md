# strategy_systematic.py

## Summary
StrategySystematic: equity vol systematic backtest strategy. Builds backtest parameters from underliers, supports xasset backtesting service or legacy GsBacktestApi.

## Class: StrategySystematic

### __init__
1. Process underliers: Iterable → each may be tuple (instrument, percentage) or single
2. Validate not unsupported eq instrument
3. Scale instruments by notional_percentage
4. Build Trade objects for xasset service
5. Handle delta_hedge → StrategyHedge
6. Build configuration for xasset service
7. Build legacy backtest parameters

### __run_service_based_backtest
1. Build BasicBacktestRequest
2. Call GsBacktestXassetApi
3. Parse risks, events, portfolio from response
4. Handle additional_results (trade_events, hedge_events)

### __position_quantity
- AssetClass.Equity: direction from buySell, quantity from numberOfOptions or quantity
- Non-equity → None

### backtest(start, end, is_async, measures)
1. If use_xasset_backtesting_service → __run_service_based_backtest
2. Else: build Backtest object
   a. is_async → create + schedule
   b. Else → run on-the-fly

## Edge Cases
- Unsupported eq instrument → MqValueError
- Single underlier vs iterable handling
- xasset vs legacy service path
- is_async creates and schedules separately

## Bugs Found
None.

## Coverage Notes
- ~25 branches
- Needs GsBacktestApi and GsBacktestXassetApi mocks
