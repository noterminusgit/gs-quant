# technicals.py

## Summary
Technical analysis library providing moving averages (simple, smoothed, exponential), volatility indicators (Bollinger bands, RSI, MACD, exponential volatility/spread volatility), and seasonal decomposition functions (seasonally adjusted series, trend extraction). All public functions are decorated with `@plot_function` for integration with the Marquee Plot Service.

## Dependencies
- Internal: `gs_quant.timeseries` (diff, annualize, returns), `gs_quant.timeseries.algebra` (subtract), `gs_quant.timeseries.helper` (Window, plot_function, normalize_window, apply_ramp), `gs_quant.timeseries.statistics` (mean, std, exponential_std), `gs_quant.errors` (MqValueError)
- External: `enum` (Enum), `typing` (Union), `pandas` (pd), `statsmodels.tsa.seasonal` (seasonal_decompose, freq_to_period)

## Type Definitions
None (no custom classes beyond enums).

## Enums and Constants

### Seasonality(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| MONTH | `"month"` | Monthly seasonality period |
| QUARTER | `"quarter"` | Quarterly seasonality period |

### SeasonalModel(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ADDITIVE | `"additive"` | Additive seasonal decomposition model |
| MULTIPLICATIVE | `"multiplicative"` | Multiplicative seasonal decomposition model |

### Frequency(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| WEEK | `"week"` | Weekly cycle frequency |
| MONTH | `"month"` | Monthly cycle frequency |
| QUARTER | `"quarter"` | Quarterly cycle frequency |
| YEAR | `"year"` | Yearly cycle frequency |

### Module Constants
None beyond the enums.

## Functions/Methods

### moving_average(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Compute simple arithmetic moving average over specified window.

**Algorithm:**
1. Normalize window via `normalize_window(x, w)`.
2. Compute `mean(x, Window(w.w, 0))` (rolling mean with zero ramp).
3. Apply ramp to the result via `apply_ramp(result, w)`.
4. Return the ramped series.

### bollinger_bands(x: pd.Series, w: Union[Window, int, str] = Window(None, 0), k: float = 2) -> pd.DataFrame
Purpose: Compute Bollinger bands (upper and lower) around a moving average.

**Algorithm:**
1. Normalize window via `normalize_window(x, w)`.
2. Compute `avg = moving_average(x, w)`.
3. Compute `sigma_t = std(x, w)`.
4. Calculate `upper = avg + k * sigma_t`.
5. Calculate `lower = avg - k * sigma_t`.
6. Return `pd.concat([lower, upper], axis=1)` (DataFrame with two columns).

### smoothed_moving_average(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Compute modified/smoothed moving average (SMMA/RMA) using recursive formula.

**Algorithm:**
1. Normalize window via `normalize_window(x, w)`.
2. Extract `window_size = w.w` and `ramp = w.r`.
3. Compute `means = apply_ramp(mean(x, Window(window_size, 0)), w)`.
4. Branch: If `means.size < 1` -> return empty `pd.Series(dtype=float)`.
5. Set `initial_moving_average = means.iloc[0]`.
6. Branch: If ramp is a positive int or a `pd.DateOffset` -> apply ramp to `x` itself.
7. Create output series `smoothed_moving_averages` as a zero-filled copy of `x`.
8. Set `smoothed_moving_averages.iloc[0] = initial_moving_average`.
9. For each index `i` from 1 to `len(x)-1`:
   a. Branch: If `window_size` is int -> `window_num_elem = window_size`.
   b. Otherwise (DateOffset) -> count elements in the window by filtering `x` index to `(x.index[i] - window_size).date() < index <= x.index[i]`.
   c. `smoothed_moving_averages.iloc[i] = ((window_num_elem - 1) * smoothed_moving_averages.iloc[i-1] + x.iloc[i]) / window_num_elem`.
10. Return `smoothed_moving_averages`.

### relative_strength_index(x: pd.Series, w: Union[Window, int, str] = 14) -> pd.DataFrame
Purpose: Compute RSI (Relative Strength Index) momentum indicator.

**Algorithm:**
1. Normalize window via `normalize_window(x, w)`.
2. Compute one-period differences: `one_period_change = diff(x, 1)[1:]`.
3. Split into gains (negative values zeroed) and losses (positive values zeroed, then negated).
4. Compute `moving_avg_gains = smoothed_moving_average(gains, w)`.
5. Compute `moving_avg_losses = smoothed_moving_average(losses, w)`.
6. Create zero-filled RSI series of same shape as `moving_avg_gains`.
7. For each index:
   a. Branch: If `moving_avg_losses.iloc[index] == 0` -> `rsi.iloc[index] = 100`.
   b. Otherwise -> `relative_strength = moving_avg_gains / moving_avg_losses`; `rsi = 100 - (100 / (1 + relative_strength))`.
8. Return `rsi`.

### exponential_moving_average(x: pd.Series, beta: float = 0.75) -> pd.Series
Purpose: Compute exponentially weighted moving average using pandas EWM.

**Algorithm:**
1. Return `x.ewm(alpha=1 - beta, adjust=False).mean()`.

Note: `beta` controls weight on previous observation. `alpha = 1 - beta` is passed to pandas.

### macd(x: pd.Series, m: int = 12, n: int = 26, s: int = 1) -> pd.Series
Purpose: Compute Moving Average Convergence Divergence (MACD) indicator.

**Algorithm:**
1. Compute short EMA: `a = x.ewm(adjust=False, span=m).mean()`.
2. Compute long EMA: `b = x.ewm(adjust=False, span=n).mean()`.
3. Compute MACD line: `subtract(a, b)`.
4. Apply signal smoothing: `.ewm(adjust=False, span=s).mean()`.
5. Return the result.

Note: `span` parameter is used (not alpha/beta), so `beta = 2/(span+1)`.

### exponential_volatility(x: pd.Series, beta: float = 0.75) -> pd.Series
Purpose: Compute exponentially weighted annualized volatility of returns (in percent).

**Algorithm:**
1. Compute simple returns: `returns(x)`.
2. Compute exponential standard deviation: `exponential_std(returns, beta)`.
3. Annualize: `annualize(result)`.
4. Multiply by 100 to convert to percentage.
5. Return the result.

### exponential_spread_volatility(x: pd.Series, beta: float = 0.75) -> pd.Series
Purpose: Compute exponentially weighted annualized spread (difference) volatility.

**Algorithm:**
1. Compute first differences: `diff(x, 1)`.
2. Compute exponential standard deviation: `exponential_std(diffs, beta)`.
3. Annualize: `annualize(result)`.
4. Return the result.

Note: Unlike `exponential_volatility`, this uses `diff` (absolute differences) instead of `returns` (percentage returns), and does not multiply by 100.

### _freq_to_period(x: pd.Series, freq: Frequency = Frequency.YEAR) -> tuple[pd.Series, int]
Purpose: Convert a time series and desired temporal frequency into a properly-frequencied series and its period (number of data points per cycle).

**Algorithm:**
1. Branch: If `x.index` is not a `pd.DatetimeIndex` -> raise `MqValueError`.
2. Infer the pandas frequency from `x.index.inferred_freq`.
3. Normalize legacy pandas frequencies: `"ME"` or `"M"` -> `"MS"`; `"QE-DEC"` or `"QE"` -> `"QS"`.
4. Convert to a period count via `statsmodels.tsa.seasonal.freq_to_period(pfreq)` (or None if pfreq is None).
5. Branch on `period`:
   - `period in [7, None]` (daily): Set `x = x.asfreq('D', method='ffill')`. Branch on freq:
     - YEAR -> return (x, 365)
     - QUARTER -> return (x, 91)
     - MONTH -> return (x, 30)
     - WEEK -> return (x, 7)
   - `period == 5` (business day): Branch on freq:
     - YEAR -> return (x.asfreq('D', ...), 365)
     - QUARTER -> return (x.asfreq('D', ...), 91)
     - MONTH -> return (x.asfreq('D', ...), 30)
     - WEEK -> return (x.asfreq('B', ...), 5)
   - `period == 52` (weekly): Set `x = x.asfreq('W', method='ffill')`. Branch on freq:
     - YEAR -> return (x, 52)
     - QUARTER -> return (x, 13)
     - MONTH -> return (x, 4)
     - WEEK -> raise `MqValueError` (incompatible)
   - `period == 12` (monthly): Set `x = x.asfreq('ME', method='ffill')`. Branch on freq:
     - YEAR -> return (x, 12)
     - QUARTER -> return (x, 3)
     - MONTH or WEEK -> raise `MqValueError` (incompatible)
6. Default: return `(x, period)`.

**Raises:** `MqValueError` for non-DatetimeIndex or incompatible frequency/period combinations.

### _seasonal_decompose(x: pd.Series, method: SeasonalModel = SeasonalModel.ADDITIVE, freq: Frequency = Frequency.YEAR) -> DecomposeResult
Purpose: Perform seasonal decomposition on a time series.

**Algorithm:**
1. Call `_freq_to_period(x, freq)` to get adjusted series and period.
2. Branch: If `x.shape[0] < 2 * period` -> raise `MqValueError` (need at least two full cycles).
3. Call `statsmodels.tsa.seasonal.seasonal_decompose(x, period=period, model=method.value)`.
4. Return the decomposition result object (has `.trend`, `.seasonal`, `.resid` attributes).

**Raises:** `MqValueError` when series has fewer than 2 complete cycles.

### seasonally_adjusted(x: pd.Series, method: SeasonalModel = SeasonalModel.ADDITIVE, freq: Frequency = Frequency.YEAR) -> pd.Series
Purpose: Remove the seasonal component from a time series.

**Algorithm:**
1. Call `_seasonal_decompose(x, method, freq)`.
2. Branch: If `method == SeasonalModel.ADDITIVE` -> return `trend + resid`.
3. Otherwise (multiplicative) -> return `trend * resid`.

### trend(x: pd.Series, method: SeasonalModel = SeasonalModel.ADDITIVE, freq: Frequency = Frequency.YEAR) -> pd.Series
Purpose: Extract the trend component from a seasonally decomposed time series.

**Algorithm:**
1. Call `_seasonal_decompose(x, method, freq)`.
2. Return `decompose_obj.trend`.

## State Mutation
- No module-level mutable state.
- All functions are pure (stateless); they only operate on input series and return new series/dataframes.
- Thread safety: All functions are thread-safe as they do not modify shared state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_freq_to_period` | Series index is not DatetimeIndex |
| `MqValueError` | `_freq_to_period` | Incompatible frequency/period combination (e.g., weekly data with WEEK freq, monthly data with MONTH/WEEK freq) |
| `MqValueError` | `_seasonal_decompose` | Series has fewer than 2 * period data points |

## Edge Cases
- `smoothed_moving_average` with an empty or very short series returns `pd.Series(dtype=float)` when `means.size < 1`.
- `smoothed_moving_average` with a `pd.DateOffset` window size computes `window_num_elem` dynamically per-element based on actual date range, so the effective window varies with gaps in the data.
- `relative_strength_index` when `moving_avg_losses` is zero at an index sets RSI to 100 (fully bullish).
- `_freq_to_period` with `pfreq=None` (unable to infer frequency) falls into the daily branch (`period in [7, None]`).
- `bollinger_bands` returns a DataFrame with two unnamed columns (lower and upper), not labeled.
- `exponential_moving_average` with `beta=0` gives `alpha=1`, meaning each point equals itself (no smoothing).
- `macd` with `s=1` means the signal line EMA has span=1, effectively no smoothing of the MACD line.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~35
- Key branches: `smoothed_moving_average` empty check (line 181), ramp type check (line 184), window_size int vs DateOffset (line 191-194); `relative_strength_index` zero-loss check (line 246); `_freq_to_period` period dispatch (lines 435-472) with many sub-branches per data frequency; `seasonally_adjusted` additive vs multiplicative (line 525-528).
- Pragmas: none
