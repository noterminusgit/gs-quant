# econometrics_processors.py

## Summary
Econometric processors: Volatility, SharpeRatio, Correlation, Change, Returns, Beta, FXImpliedCorr. Each wraps a timeseries econometrics function.

## Classes

### VolatilityProcessor
- process(): a_data → success → volatility(data, w, returns_type)

### SharpeRatioProcessor
- __init__: creates additional excess_returns DataQueryInfo child from SharpeAssets[currency]
- process():
  1. Both a_data and excess_returns_data must be ProcessorResult + success
  2. Branch: curve_type == PRICES → compute excess_returns; else use a_data directly
  3. Compute ratio via get_ratio_pure

### CorrelationProcessor
- __init__: creates benchmark DataQueryInfo child
- process(): both a and benchmark must succeed → correlation(a, benchmark, w, type_)

### ChangeProcessor
- process(): a_data success → change(data)

### ReturnsProcessor
- process():
  1. a_data success
  2. Branch: observations is None:
     a. len(data) > 1 → simple return formula
     b. Else → 'Series has is less than 2.' (note typo in message)
  3. Else → returns(data, observations, type_)

### BetaProcessor
- Two-input: a and b, same pattern as CovarianceProcessor
- process(): a success → b exists → b success → beta(a, b, w)

### FXImpliedCorrProcessor (extends MeasureProcessor)
- process(cross1):
  1. Both cross1 and cross2 must be Cross instances
  2. Uses DataContext(start, end) → fx_implied_correlation
  3. On exception → failure with str(e)

## Edge Cases
- SharpeRatioProcessor: returns self.value without setting it if either input fails
- ReturnsProcessor with observations=None and len(data)<=1: returns success with error string
- FXImpliedCorrProcessor: entity types must be Cross, not just Entity

## Bugs Found
None.

## Coverage Notes
- ~30 branches
