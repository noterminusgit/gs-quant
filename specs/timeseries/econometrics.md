# econometrics.py

## Summary
Econometrics timeseries library providing standard economic and financial time series analytics: returns computation, price reconstruction, volatility (including vol-swap convention), Sharpe ratio, correlation (including corr-swap convention), beta, excess returns, index normalization, and max drawdown. All public functions are decorated with `@plot_function` or `@plot_session_function` for chart service exposure.

## Dependencies
- Internal: `gs_quant.timeseries.analysis` (LagMode, lag), `gs_quant.timeseries.datetime` (align, interpolate), `gs_quant.timeseries.helper` (CurveType, Interpolate, Window, normalize_window, apply_ramp, plot_session_function, plot_function, Returns, SeriesType), `gs_quant.timeseries.statistics` (std, product, sum_, mean, MeanType), `gs_quant.data` (DataContext), `gs_quant.datetime` (DayCountConvention, day_count_fraction), `gs_quant.errors` (MqValueError, MqTypeError), `gs_quant.api.gs.data` (GsDataApi, QueryType), `gs_quant.common` (Currency), `gs_quant.markets.securities` (Asset)
- External: `math` (sqrt, log, exp, pow), `numpy` (np.nan, np.average, np.var, np.cov, np.array, np.empty, np.double), `pandas` (pd.Series, pd.DateOffset, pd.DatetimeIndex, pd.DataFrame)

## Type Definitions

### Window (from helper)
```
Window(w: Union[int, str, pd.DateOffset, None], r: Union[int, str, pd.DateOffset, None])
```
Represents a rolling window size `w` and ramp-up period `r`.

## Enums and Constants

### AnnualizationFactor(IntEnum)
| Value | Raw | Description |
|-------|-----|-------------|
| DAILY | `252` | Trading days per year |
| WEEKLY | `52` | Weeks per year |
| SEMI_MONTHLY | `26` | Bi-weekly periods per year |
| MONTHLY | `12` | Months per year |
| QUARTERLY | `4` | Quarters per year |
| ANNUALLY | `1` | Yearly (no scaling) |

### SharpeAssets(Enum)
Maps currency codes to Marquee asset IDs for risk-free rate benchmarks.

| Value | Raw | Description |
|-------|-----|-------------|
| USD | `'MAP35DA6K5B1YXGX'` | USD risk-free rate asset |
| AUD | `'MAFRZWJ790MQY0EW'` | AUD risk-free rate asset |
| CHF | `'MAS0NN4ZX7NYXB36'` | CHF risk-free rate asset |
| EUR | `'MA95W0N1214395N8'` | EUR risk-free rate asset |
| GBP | `'MA41ZEFTWR8Q7HBM'` | GBP risk-free rate asset |
| JPY | `'MA8GXV3SJ0TXH1JV'` | JPY risk-free rate asset |
| SEK | `'MAGNZZY0GJ4TATNG'` | SEK risk-free rate asset |

### RiskFreeRateCurrency(Enum)
Dual-cased currency enum for API compatibility. Contains both uppercase (`USD`, `AUD`, ...) and lowercase (`_USD` = `"usd"`, `_AUD` = `"aud"`, ...) members for each supported currency.

| Value | Raw | Description |
|-------|-----|-------------|
| USD | `"USD"` | US Dollar (uppercase) |
| _USD | `"usd"` | US Dollar (lowercase) |
| AUD | `"AUD"` | Australian Dollar (uppercase) |
| _AUD | `"aud"` | Australian Dollar (lowercase) |
| CHF | `"CHF"` | Swiss Franc (uppercase) |
| _CHF | `"chf"` | Swiss Franc (lowercase) |
| EUR | `"EUR"` | Euro (uppercase) |
| _EUR | `"eur"` | Euro (lowercase) |
| GBP | `"GBP"` | British Pound (uppercase) |
| _GBP | `"gbp"` | British Pound (lowercase) |
| JPY | `"JPY"` | Japanese Yen (uppercase) |
| _JPY | `"jpy"` | Japanese Yen (lowercase) |
| SEK | `"SEK"` | Swedish Krona (uppercase) |
| _SEK | `"sek"` | Swedish Krona (lowercase) |

## Functions/Methods

### excess_returns_pure(price_series: pd.Series, spot_curve: pd.Series) -> pd.Series
Purpose: Compute excess returns of a price series over a spot benchmark curve (pure computation, no API calls).

**Formula:**

E_0 = P_0

E_t = E_{t-1} * (1 + P_t / P_{t-1} - B_t / B_{t-1})

where P is the price series and B is the benchmark spot curve.

**Algorithm:**
1. Align price_series and spot_curve using INTERSECT method
2. Initialize e_returns with first aligned price value
3. For each subsequent index i, compute multiplier = 1 + curve[i]/curve[i-1] - bench[i]/bench[i-1]
4. Append e_returns[-1] * multiplier
5. Return pd.Series with aligned index

### excess_returns(price_series: pd.Series, benchmark_or_rate: Union[Asset, Currency, float], *, day_count_convention=DayCountConvention.ACTUAL_360) -> pd.Series
Purpose: Compute excess returns against a benchmark asset, currency risk-free rate, or fixed rate.

**Formula (float rate R):**

E_0 = P_0

E_t = E_{t-1} + P_t - P_{t-1} * (1 + R * dcf(D_{t-1}, D_t))

where dcf is the day count fraction using the specified convention.

**Algorithm:**
1. Branch: benchmark_or_rate is float -> compute iteratively using day_count_fraction
2. Branch: benchmark_or_rate is Currency -> look up SharpeAssets Marquee ID by currency value
   - Raises MqValueError if currency not in SharpeAssets
3. Branch: benchmark_or_rate is Asset -> call get_marquee_id()
4. For Currency/Asset: query GsDataApi for spot data within date range of price_series
   - Raises MqValueError if returned DataFrame is empty
   - Deduplicates index (keeps first)
5. Delegate to excess_returns_pure with queried spot curve

**Raises:**
- `MqValueError` when currency not supported in SharpeAssets
- `MqValueError` when risk-free rate data cannot be retrieved

### _annualized_return(levels: pd.Series, rolling: Union[int, pd.DateOffset], interpolation_method: Interpolate = Interpolate.NAN) -> pd.Series
Purpose: Compute rolling annualized returns from level series (internal helper).

**Formula:**

R_t = (V_t / V_{t-w})^{365.25 / (D_t - D_{t-w})} - 1

**Algorithm:**
1. Branch: rolling is pd.DateOffset ->
   a. Compute starting dates as [timestamp - rolling for each timestamp]
   b. Interpolate the levels series using interpolation_method
   c. Map each point to annualized return using 365.25/days factor
2. Branch: rolling is int ->
   a. If interpolation_method is not NAN, raise MqValueError
   b. Build starting index array (0-padded for first `rolling` elements, then sequential)
   c. Map each point to annualized return using 365.25/days factor
3. Insert 0 at position 0
4. Return pd.Series with levels index

**Raises:** `MqValueError` when rolling is int but interpolation_method is not NAN

### get_ratio_pure(er: pd.Series, w: Union[Window, int, str], interpolation_method: Interpolate = Interpolate.NAN) -> pd.Series
Purpose: Compute annualized return / volatility ratio (Sharpe-like) from excess returns series.

**Formula:**

S_t = annualized_return_t / volatility_t * 100

**Algorithm:**
1. Normalize window (supports 0 as input, treated as None)
2. Compute _annualized_return with window and interpolation_method
3. Check if series is long enough for the window
4. Branch: long enough -> compute volatility with window; else -> compute volatility over full series
5. Divide annualized return by volatility, multiply by 100
6. Apply ramp and return

### _get_ratio(input_series: pd.Series, benchmark_or_rate: Union[Asset, float, str], w: Union[Window, int, str], *, day_count_convention: DayCountConvention, curve_type: CurveType = CurveType.PRICES, interpolation_method: Interpolate = Interpolate.NAN) -> pd.Series
Purpose: Internal wrapper that computes excess returns (if curve_type is PRICES) then delegates to get_ratio_pure.

**Algorithm:**
1. Branch: curve_type == PRICES -> compute excess_returns from input_series and benchmark_or_rate
2. Branch: curve_type == EXCESS_RETURNS -> use input_series directly (asserts CurveType)
3. Delegate to get_ratio_pure

### excess_returns_(price_series: pd.Series, currency: RiskFreeRateCurrency = RiskFreeRateCurrency.USD) -> pd.Series
Purpose: Plot-session wrapper for excess_returns using RiskFreeRateCurrency enum. Decorated with `@plot_session_function`.

**Algorithm:**
1. Convert currency enum value to Currency, delegate to excess_returns with ACTUAL_360 convention

### sharpe_ratio(series: pd.Series, currency: RiskFreeRateCurrency = RiskFreeRateCurrency.USD, w: Union[Window, int, str] = None, curve_type: CurveType = CurveType.PRICES, method: Interpolate = Interpolate.NAN) -> pd.Series
Purpose: Calculate rolling Sharpe ratio of a price or excess return series. Decorated with `@plot_session_function`.

**Formula:**

S_t = [(E_t / E_{t-w+1})^{365.25 / (D_t - D_{t-w})} - 1] / volatility(E, w)_t

Excess returns: E_t = E_{t-1} + P_t - P_{t-1} * (1 + R * (D_t - D_{t-1}) / 360)

**Algorithm:**
1. Delegate to _get_ratio with Currency(currency.value), ACTUAL_360 convention, curve_type, and interpolation method

### returns(series: pd.Series, obs: Union[Window, int, str] = 1, type: Returns = Returns.SIMPLE) -> pd.Series
Purpose: Calculate returns from a price series.

**Formulas:**

- Simple: R_t = X_t / X_{t-obs} - 1
- Logarithmic: R_t = ln(X_t) - ln(X_{t-obs})
- Absolute: R_t = X_t - X_{t-obs}

**Algorithm:**
1. If series.size < 1, return empty series
2. Compute shifted_series via lag(series, obs, LagMode.TRUNCATE)
3. Branch: obs is str and index is not DatetimeIndex -> copy series with DatetimeIndex cast
4. Branch: type == SIMPLE -> series / shifted - 1
5. Branch: type == LOGARITHMIC -> log(series) - log(shifted)
6. Branch: type == ABSOLUTE -> series - shifted
7. Else -> raise MqValueError

**Raises:** `MqValueError` for unknown returns type

### prices(series: pd.Series, initial: int = 1, type: Returns = Returns.SIMPLE) -> pd.Series
Purpose: Reconstruct price levels from a returns series.

**Formulas:**

- Simple: Y_t = (1 + X_{t-1}) * Y_{t-1}, Y_0 = initial -> product(1 + series) * initial
- Logarithmic: Y_t = e^{X_{t-1}} * Y_{t-1} -> product(exp(series)) * initial
- Absolute: Y_t = X_{t-1} + Y_{t-1} -> sum_(series) + initial

**Algorithm:**
1. If series.size < 1, return empty series
2. Branch: type == SIMPLE -> product(1 + series) * initial
3. Branch: type == LOGARITHMIC -> product(series.apply(math.exp)) * initial
4. Branch: type == ABSOLUTE -> sum_(series) + initial
5. Else -> raise MqValueError

**Raises:** `MqValueError` for unknown returns type

### index(x: pd.Series, initial: int = 1) -> pd.Series
Purpose: Geometric series normalization -- divide by first valid value and scale.

**Formula:**

Y_t = initial * X_t / X_0

where X_0 is the first valid (non-NaN) value.

**Algorithm:**
1. Find first valid index i
2. Branch: x[i] is falsy (zero) -> raise MqValueError (divide by zero)
3. Branch: i is None -> return empty float Series
4. Return initial * x / x[i]

**Raises:** `MqValueError` when first value is zero

### change(x: pd.Series) -> pd.Series
Purpose: Arithmetic series normalization -- difference from first value.

**Formula:**

Y_t = X_t - X_0

**Algorithm:**
1. Return x - x.iloc[0]

### _get_annualization_factor(x) -> AnnualizationFactor
Purpose: Infer annualization factor from average inter-observation distance (internal helper).

**Algorithm:**
1. Compute distance in days between consecutive observations
2. If any distance is 0, raise MqValueError (duplicate dates)
3. Compute average distance
4. Map to AnnualizationFactor by range:
   - avg < 2.1 -> DAILY (252)
   - 6 <= avg < 8 -> WEEKLY (52)
   - 14 <= avg < 17 -> SEMI_MONTHLY (26)
   - 25 <= avg < 35 -> MONTHLY (12)
   - 85 <= avg < 97 -> QUARTERLY (4)
   - 360 <= avg < 386 -> ANNUALLY (1)
   - Otherwise -> raise MqValueError

**Raises:**
- `MqValueError` when multiple data points on same date (distance == 0)
- `MqValueError` when average distance does not match any known frequency

### annualize(x: pd.Series) -> pd.Series
Purpose: Annualize a series by multiplying by sqrt of inferred annualization factor.

**Formula:**

Y_t = X_t * sqrt(F)

where F is inferred from observation frequency (252/52/26/12/4/1).

**Algorithm:**
1. Call _get_annualization_factor(x) to determine F
2. Return x * math.sqrt(F)

### volatility(x: pd.Series, w: Union[Window, int, str] = Window(None, 0), returns_type: Optional[Returns] = Returns.SIMPLE, annualization_factor: Optional[int] = None, assume_zero_mean: bool = False) -> pd.Series
Purpose: Compute rolling annualized realized volatility of a price or return series.

**Formula (standard, assume_zero_mean=False):**

vol_t = sqrt(1/(N-1) * sum_{i=t-w+1}^{t} (R_i - R_bar)^2) * sqrt(F) * 100

**Formula (zero-mean, assume_zero_mean=True):**

vol_t = sqrt(1/N * sum_{i=t-w+1}^{t} R_i^2) * sqrt(F) * 100

**Algorithm:**
1. Normalize window
2. If x.size < 1, return x
3. Branch: returns_type is not None -> compute returns(x, type=returns_type); else use x as-is
4. Create Window(w.w, 0) for inner computation
5. Branch: assume_zero_mean -> use mean(ret, window, MeanType.QUADRATIC); else -> use std(ret, window)
6. Branch: annualization_factor is not None -> vol * sqrt(factor); else -> annualize(vol)
7. Multiply by 100, apply ramp, return

### vol_swap_volatility(prices: pd.Series, n_days: Union[int, Window] = None, annualization_factor: int = 252, assume_zero_mean: bool = True) -> pd.Series
Purpose: Rolling volatility for volatility swap pricing using logarithmic returns.

**Formula (zero-mean, default):**

sigma_t = sqrt(AF * (1/N) * sum R_i^2)

where R_i = ln(P_i / P_{i-1})

**Algorithm:**
1. Branch: n_days is None -> set to len(prices)
2. Branch: n_days is Window -> validate ramp == w - 1, else raise MqTypeError
3. Branch: n_days is int -> create Window(n_days, n_days - 1)
4. Delegate to volatility(prices, window, LOGARITHMIC, annualization_factor, assume_zero_mean)

**Raises:** `MqTypeError` when Window ramp-up is not w - 1

### correlation(x: pd.Series, y: pd.Series, w: Union[Window, int, str] = Window(None, 0), type_: SeriesType = SeriesType.PRICES, returns_type: Returns = Returns.SIMPLE, assume_zero_mean: bool = False) -> pd.Series
Purpose: Rolling correlation of two series (price or return).

**Formula (standard Pearson, assume_zero_mean=False):**

rho_t = sum((R_i - R_bar)(S_i - S_bar)) / ((N-1) * sigma_R * sigma_S)

**Formula (zero-mean, assume_zero_mean=True):**

rho_t = mean(R * S) / (vol_R * vol_S)

where vol is daily (de-annualized) zero-mean volatility.

**Algorithm:**
1. Normalize window
2. If x.size < 1, return x
3. Branch: type_ == PRICES ->
   a. Branch: returns_type is tuple/list ->
      - Validate length == 2 and both are Returns instances
      - Use separate return types for x and y
   b. Branch: returns_type is single value -> use for both
   c. Compute returns for both series
4. Branch: type_ == RETURNS -> use x, y directly
5. Align into DataFrame, drop NaN rows
6. Branch: assume_zero_mean ->
   a. Compute zero_mean_cov = mean(ret1 * ret2, w)
   b. Compute daily_vols for each by de-annualizing volatility
   c. corr = zero_mean_cov / (vols_1 * vols_2)
7. Branch: assume_zero_mean is False ->
   a. Branch: w.w is DateOffset ->
      - Branch: DatetimeIndex -> window slice with > and <=
      - Branch: else -> cast boundary to .date() for comparison
   b. Branch: w.w is int -> rolling(w.w, 0).corr()
8. Apply ramp with NAN interpolation, return

**Raises:**
- `MqValueError` when returns_type list length != 2
- `MqTypeError` when returns_type list elements are not Returns

### corr_swap_correlation(x: pd.Series, y: pd.Series, n_days: Union[int, Window] = None, assume_zero_mean: bool = True) -> pd.Series
Purpose: Rolling correlation for correlation swap pricing using logarithmic returns.

**Formula (zero-mean, default):**

rho_t = sum(R_i * S_i) / sqrt(sum(R_i^2) * sum(S_i^2))

where R_i, S_i are logarithmic returns.

**Algorithm:**
1. Branch: n_days is None -> set to min(len(x), len(y))
2. Branch: n_days is Window -> validate ramp == w - 1, else raise MqTypeError
3. Branch: n_days is int -> create Window(n_days - 1, n_days - 2)
4. Delegate to correlation(x, y, window, PRICES, LOGARITHMIC, assume_zero_mean)

**Raises:** `MqTypeError` when Window ramp-up is not w - 1

### beta(x: pd.Series, b: pd.Series, w: Union[Window, int, str] = Window(None, 0), prices: bool = True) -> pd.Series
Purpose: Compute rolling beta of a series against a benchmark.

**Formula:**

beta_t = Cov(R_t, S_t) / Var(S_t)

where R_t, S_t are simple returns of x and b respectively (when prices=True).

**Algorithm:**
1. If prices is not bool, raise MqTypeError
2. Normalize window
3. Compute returns for x and b (or use directly if prices=False)
4. Branch: w.w is DateOffset ->
   a. Intersect indices of both return series
   b. For each index position, find window start via offset
   c. Compute np.var (ddof=1) and np.cov (ddof=1) over window slices
   d. Result = cov / var
5. Branch: w.w is int ->
   a. cov = ret_series.rolling(w.w, 0).cov(ret_benchmark)
   b. result = cov / ret_benchmark.rolling(w.w, 0).var()
6. Set first 3 values to NaN (avoid extreme values from small samples)
7. Apply ramp with NAN interpolation, return

**Raises:** `MqTypeError` when prices is not a boolean

### max_drawdown(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Compute rolling maximum peak-to-trough drawdown as a ratio (e.g., -0.2 for 20% drawdown).

**Formula:**

DD_t = X_t / max(X_{t-w+1..t}) - 1

MaxDD_t = min(DD_{t-w+1..t})

**Algorithm:**
1. Normalize window
2. Branch: w.w is DateOffset ->
   a. Branch: index is datetime64 ->
      - For each date, compute score = x[idx] / rolling_max_in_window - 1
      - Then compute min of scores over the same window
   b. Branch: else -> raise TypeError
3. Branch: w.w is int ->
   a. rolling_max = x.rolling(w.w, 0).max()
   b. result = (x / rolling_max - 1).rolling(w.w, 0).min()
4. Apply ramp, return

**Raises:** `TypeError` when DateOffset window used with non-datetime index

## State Mutation
- No global state mutations.
- `excess_returns` performs external API calls via `GsDataApi.get_market_data` within a `DataContext` context manager; this may have side effects on session/caching state.
- Thread safety: Functions are stateless and operate on immutable inputs; safe for concurrent use except for API-dependent functions (`excess_returns`, `sharpe_ratio`, `excess_returns_`).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `excess_returns` | Currency not found in SharpeAssets |
| `MqValueError` | `excess_returns` | Risk-free rate data query returns empty DataFrame |
| `MqValueError` | `_annualized_return` | Int rolling with non-NAN interpolation method |
| `MqValueError` | `_get_annualization_factor` | Multiple data points on same date |
| `MqValueError` | `_get_annualization_factor` | Average distance does not match any known frequency |
| `MqValueError` | `returns` | Unknown returns type |
| `MqValueError` | `prices` | Unknown returns type |
| `MqValueError` | `index` | First valid value is zero (divide by zero) |
| `MqValueError` | `correlation` | returns_type list length != 2 |
| `MqTypeError` | `correlation` | returns_type list elements are not Returns |
| `MqTypeError` | `beta` | prices parameter is not boolean |
| `MqTypeError` | `vol_swap_volatility` | Window ramp-up != w - 1 |
| `MqTypeError` | `corr_swap_correlation` | Window ramp-up != w - 1 |
| `TypeError` | `max_drawdown` | DateOffset window with non-datetime index |

## Edge Cases
- `returns` with empty series (size < 1): returns the empty series unchanged
- `prices` with empty series (size < 1): returns the empty series unchanged
- `volatility` with empty series (size < 1): returns the empty series unchanged
- `correlation` with empty x (size < 1): returns x unchanged
- `index` with first valid value of zero: raises MqValueError rather than producing Inf
- `index` with i = None (no valid index found): returns empty float Series
- `beta` sets first 3 values to NaN to avoid extreme values from small sample sizes
- `_get_annualization_factor` has gaps in its distance ranges (e.g., 2.1-6, 8-14, 17-25, 35-85, 97-360) which will raise MqValueError for irregular frequencies
- `excess_returns_pure` assumes aligned series have same length after INTERSECT alignment
- `returns` with string obs on non-DatetimeIndex: silently casts index to DatetimeIndex
- `get_ratio_pure` supports 0 as window input (normalizes to None for full series)

## Bugs Found
None.

## Coverage Notes
- Branch count: ~65
- Key branch points: returns type dispatch (3 types + error), correlation assume_zero_mean path, correlation DateOffset vs int window, beta DateOffset vs int window, max_drawdown DateOffset vs int, _get_annualization_factor distance ranges (7 branches), excess_returns type dispatch (float/Currency/Asset), _annualized_return DateOffset vs int
