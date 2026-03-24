# statistics.py

## Summary
Rolling and expanding statistical functions for timeseries analysis. Provides basic arithmetic, probability, and distribution operations including min, max, range, mean, median, mode, sum, product, standard deviation, variance, covariance, z-scores, winsorization, percentiles, linear regression (static and rolling), and epidemiological compartmental models (SIR/SEIR). Generally not finance-specific routines.

## Dependencies
- Internal: `gs_quant.timeseries.algebra` (ceil, floor)
- Internal: `gs_quant.timeseries.datetime` (interpolate)
- Internal: `gs_quant.timeseries.helper` (Window, normalize_window, rolling_offset, apply_ramp, plot_function, rolling_apply, Interpolate, plot_method)
- Internal: `gs_quant.data` (DataContext)
- Internal: `gs_quant.errors` (MqValueError, MqTypeError)
- Internal: `gs_quant.models.epidemiology` (SIR, SEIR, EpidemicModel)
- External: `numpy` (np -- empty, nan, double, std, nanmin, nanmax, nanmean, nanmedian, nanvar, nansum, nanprod, percentile, sqrt, inf, arange, array, random.default_rng)
- External: `pandas` (pd -- Series, DateOffset, DatetimeIndex, Timestamp, Timedelta, concat, date_range)
- External: `scipy.stats.mstats` (as stats -- zscore, mode)
- External: `scipy.stats` (percentileofscore)
- External: `statsmodels.api` (as sm -- OLS, add_constant)
- External: `statsmodels.regression.rolling` (RollingOLS)
- External: `datetime` (as dt -- date, timedelta)

Optional (try/except):
- Internal: `quant_extensions.timeseries.statistics` (rolling_std) -- falls back to pure-Python implementation if unavailable

## Type Definitions

### Window (imported from helper)
Used throughout as `Union[Window, int, str]` for window parameters. Encapsulates window size `w` and ramp `r`.

### LinearRegression (class)
Fit an OLS linear regression model.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _index_scope | `range` | N/A | Column index range based on fit_intercept |
| _res | `RegressionResultsWrapper` | N/A | statsmodels OLS fit result |
| _fit_intercept | `bool` | N/A | Whether intercept was included |

### RollingLinearRegression (class)
Fit a rolling OLS linear regression model.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _X | `pd.DataFrame` | N/A | Copy of aligned explanatory variables |
| _res | `RollingRegressionResults` | N/A | statsmodels RollingOLS fit result |

### SIRModel (class)
SIR compartmental model for infectious disease transmission.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| s | `pd.Series` | N/A | Susceptible individuals series |
| i | `pd.Series` | N/A | Infectious individuals series |
| r | `pd.Series` | N/A | Recovered individuals series |
| n | `float` | `100` | Total population size |
| beta_init | `float` | `None` | Initial transmission rate |
| gamma_init | `float` | `None` | Initial recovery rate |
| fit | `bool` | `True` | Whether to fit model to data |
| fit_period | `int` | `None` | Number of days back to fit |
| beta_fixed | `bool` | N/A | Whether beta is fixed during fitting |
| gamma_fixed | `bool` | N/A | Whether gamma is fixed during fitting |
| parameters | `dict` | N/A | Model parameter dict from SIR.get_parameters |
| _model | `EpidemicModel` | N/A | Underlying epidemic model instance |

### SEIRModel (class)
Inherits: SIRModel

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| e | `pd.Series` | N/A | Exposed individuals series |
| sigma_init | `float` | `None` | Initial exposed-to-infected rate |
| sigma_fixed | `bool` | N/A | Whether sigma is fixed during fitting |

## Enums and Constants

### MeanType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ARITHMETIC | `"arithmetic"` | Standard arithmetic mean |
| QUADRATIC | `"quadratic"` | Quadratic (root-mean-square) mean |

### Direction(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| START_TODAY | `"start_today"` | Generated series starts from today |
| END_TODAY | `"end_today"` | Generated series ends on today |

### IntradayDirection(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| START_INTRADAY_NOW | `"start_intraday_now"` | Intraday series starts from current time |
| END_INTRADAY_NOW | `"end_intraday_now"` | Intraday series ends at current time |

## Functions/Methods

### rolling_std(x: pd.Series, offset: pd.DateOffset) -> pd.Series
Purpose: Fallback pure-Python implementation of rolling standard deviation over a date-offset window. Only defined if `quant_extensions` is not importable.

**Formula:**
`R[i] = std(X[start:i+1], ddof=1)` where `start` is the first index `j` such that `index[j] > index[i] - offset`. NaN values within the section are excluded before computing std. `R[0] = NaN` always.

**Algorithm:**
1. Allocate results array of NaN, set `results[0] = NaN`
2. For each `i` from 1..size-1, slide `start` forward until `index[start] > index[i] - offset`
3. Slice values `start:i+1`, filter out NaNs, compute `np.std(section, ddof=1)`
4. Return `pd.Series(results, index=x.index)`

### _concat_series(series: List[pd.Series]) -> pd.DataFrame
Purpose: Concatenate list of series into a DataFrame, treating constant series as scalar columns.

**Algorithm:**
1. Separate series where `min != max` (curves) from constant series
2. `pd.concat(curves, axis=1).assign(**constants)`

### min_(x: Union[pd.Series, List[pd.Series]], w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Minimum value of series over given rolling window. Decorated with `@plot_function`.

**Formula:**
`R_t = min(X_{t-w+1} : X_t)`

For list input: `R_t = min(X_{1,t-w+1} : X_{n,t})`

**Algorithm:**
1. If `x` is a list, collapse via `_concat_series(x).min(axis=1)`
2. `normalize_window(x, w)`
3. Assert monotonic increasing index
4. Branch: `w.w` is `DateOffset` -> use `rolling_offset(x, w.w, np.nanmin, 'min')` for Series, or list comprehension with `.loc` filtering for non-Series
5. Branch: `w.w` is int -> `x.rolling(w.w, 0).min()`
6. Apply ramp and return

### max_(x: Union[pd.Series, List[pd.Series]], w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Maximum value of series over given rolling window. Decorated with `@plot_function`.

**Formula:**
`R_t = max(X_{t-w+1} : X_t)`

For list input: `R_t = max(X_{1,t-w+1} : X_{n,t})`

**Algorithm:**
1. If `x` is a list, collapse via `_concat_series(x).max(axis=1)`
2. `normalize_window(x, w)`
3. Assert monotonic increasing index
4. Branch: `w.w` is `DateOffset` -> `rolling_offset(x, w.w, np.nanmax, 'max')` for Series, list comprehension for non-Series
5. Branch: `w.w` is int -> `x.rolling(w.w, 0).max()`
6. Apply ramp and return

### range_(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Range (max - min) of series over given rolling window. Decorated with `@plot_function`.

**Formula:**
`R_t = max(X_{t-w+1}:X_t) - min(X_{t-w+1}:X_t)`

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing index
3. Compute `max_(x, Window(w.w, 0))` and `min_(x, Window(w.w, 0))`
4. Return `apply_ramp(max_v - min_v, w)`

### mean(x: Union[pd.Series, List[pd.Series]], w: Union[Window, int, str] = Window(None, 0), mean_type: MeanType = MeanType.ARITHMETIC) -> pd.Series
Purpose: Rolling arithmetic or quadratic mean. Decorated with `@plot_function`.

**Formula (arithmetic):**
`R_t = (1/N) * SUM(X_i, i=t-w+1..t)`

**Formula (quadratic / RMS):**
`R_t = sqrt((1/N) * SUM(X_i^2, i=t-w+1..t))`

**Algorithm:**
1. If `x` is a list, `pd.concat(x, axis=1)`
2. `normalize_window(x, w)`
3. Assert monotonic increasing index
4. Branch: `mean_type is QUADRATIC` -> square `x` first
5. Branch: `w.w` is `DateOffset` and Series -> `rolling_offset(x, w.w, np.nanmean, 'mean')`
6. Branch: `w.w` is `DateOffset` and DataFrame -> list comprehension with `np.nanmean` and `.loc` date filtering
7. Branch: `w.w` is int and Series -> `x.rolling(w.w, 0).mean()`
8. Branch: `w.w` is int and DataFrame -> list comprehension with `np.nanmean` and `.iloc` slicing
9. If quadratic, take `np.sqrt(result)`
10. Apply ramp and return

### median(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Median value over given rolling window. Decorated with `@plot_function`.

**Formula:**
`d = (w-1)/2`, `R_t = (X[floor(t-d)] + X[ceil(t-d)]) / 2`

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing index
3. Branch: `w.w` is `DateOffset` -> `rolling_offset(x, w.w, np.nanmedian, 'median')` for Series, list comprehension for non-Series
4. Branch: `w.w` is int -> `x.rolling(w.w, 0).median()`
5. Apply ramp and return

### mode(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Most common value over given rolling window. Decorated with `@plot_function`.

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing index
3. Branch: `w.w` is `DateOffset` -> `rolling_apply(x, w.w, lambda a: stats.mode(a).mode[0])` for Series, list comprehension for non-Series
4. Branch: `w.w` is int -> `x.rolling(w.w, 0).apply(lambda y: stats.mode(y).mode, raw=True)`
5. Apply ramp and return

### sum_(x: Union[pd.Series, List[pd.Series]], w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling sum over given window. Decorated with `@plot_function`.

**Formula:**
`R_t = SUM(X_i, i=t-w+1..t)`

For list: `R_t = SUM(SUM(X_ij, j=1..n), i=t-w+1..t)`

**Algorithm:**
1. If `x` is a list, `pd.concat(x, axis=1).sum(axis=1)`
2. `normalize_window(x, w)`
3. Assert monotonic increasing
4. Branch: `w.w` is `DateOffset` -> assert Series, `rolling_offset(x, w.w, np.nansum, 'sum')`
5. Branch: `w.w` is int -> `x.rolling(w.w, 0).sum()`
6. Apply ramp and return

### product(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling product over given window. Decorated with `@plot_function`.

**Formula:**
`R_t = PROD(X_i, i=t-w+1..t)`

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing
3. Branch: `w.w` is `DateOffset` -> `rolling_offset(x, w.w, np.nanprod, 'prod')` for Series, list comprehension for non-Series
4. Branch: `w.w` is int -> `x.rolling(w.w, 0).agg(pd.Series.prod)`
5. Apply ramp and return

### std(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling standard deviation (unbiased, ddof=1). Decorated with `@plot_function`.

**Formula:**
`R_t = sqrt( (1/(N-1)) * SUM((X_i - mean(X))^2, i=t-w+1..t) )`

where `mean(X) = (1/N) * SUM(X_i, i=t-w+1..t)`

**Algorithm:**
1. If `x` is empty, return `x`
2. `normalize_window(x, w)`
3. Assert monotonic increasing index
4. Branch: `w.w` is `DateOffset` -> `apply_ramp(rolling_std(x, w.w), w)`
5. Branch: `w.w` is int -> `apply_ramp(x.rolling(w.w, 0).std(), w)`

### exponential_std(x: pd.Series, beta: float = 0.75) -> pd.Series
Purpose: Exponentially weighted standard deviation. Decorated with `@plot_function`.

**Formula:**
`S_t = sqrt( [EWMA(X_t^2) - EWMA(X_t)^2] * DF_t )`

where `DF_t = (SUM(w_i))^2 / ((SUM(w_i))^2 - SUM(w_i^2))` and `w_i = (1-beta)*beta^i` for `i < t`, `beta^i` for `i = t`.

**Algorithm:**
1. Return `x.ewm(alpha=1 - beta, adjust=False).std()`

### var(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling variance (unbiased, ddof=1). Decorated with `@plot_function`.

**Formula:**
`R_t = (1/(N-1)) * SUM((X_i - mean(X))^2, i=t-w+1..t)`

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing index
3. Branch: `w.w` is `DateOffset` -> `rolling_offset(x, w.w, lambda a: np.nanvar(a, ddof=1), 'var')` for Series, list comprehension for non-Series
4. Branch: `w.w` is int -> `x.rolling(w.w, 0).var()`
5. Apply ramp and return

### cov(x: pd.Series, y: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling covariance. Decorated with `@plot_function`.

**Formula:**
`R_t = (1/(N-1)) * SUM((X_i - mean(X)) * (Y_i - mean(Y)), i=t-w+1..t)`

**Algorithm:**
1. `normalize_window(x, w)`
2. Assert monotonic increasing index
3. Branch: `w.w` is `DateOffset` -> list comprehension with `.loc` date filtering and `.cov(y)`
4. Branch: `w.w` is int -> `x.rolling(w.w, 0).cov(y)`
5. Apply ramp and return

### _zscore(x) -> float
Purpose: Helper to compute the z-score of the last element in a window.

**Algorithm:**
1. If `x.size == 1`, return 0
2. Return `stats.zscore(x, ddof=1)[-1]`

### zscores(x: pd.Series, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling z-scores. Decorated with `@plot_function`.

**Formula:**
`R_t = (X_t - mu) / sigma`

where `mu` and `sigma` are sample mean and standard deviation over the given window.

**Algorithm:**
1. If `x.size < 1`, return `x`
2. If `w` is int, `normalize_window(x, w)`
3. If `w` is str, validate index is DatetimeIndex or date, then `normalize_window(x, w)` (raises `MqValueError` if invalid)
4. Branch: `w.w` is falsy (full series):
   - If `x.size == 1`, return `[0.0]`
   - Drop NaN, compute `stats.zscore(clean, ddof=1)`, interpolate NaN positions back
5. Branch: `w.w` is not int (DateOffset) -> convert index to dates, list comprehension calling `_zscore` with `.loc` filtering
6. Branch: `w.w` is int -> `x.rolling(w.w, 0).apply(_zscore, raw=False)`
7. Apply ramp and return

**Raises:** `MqValueError` when string window is used with non-date index

### winsorize(x: pd.Series, limit: float = 2.5, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Cap and floor extreme values by z-score limit. Decorated with `@plot_function`.

**Formula:**
`upper = mu + sigma * limit`
`lower = mu - sigma * limit`
`R_t = max(min(X_t, upper), lower)`

**Algorithm:**
1. `normalize_window(x, w)`
2. If `x.size < 1`, return `x`
3. Assert `w.w` is truthy
4. Compute `mu = x.mean()`, `sigma = x.std()`
5. `high = mu + sigma * limit`, `low = mu - sigma * limit`
6. Apply `ceil(x, high)` then `floor(ret, low)`
7. Apply ramp and return

### generate_series(length: int, direction: Direction = Direction.START_TODAY) -> pd.Series
Purpose: Generate random price timeseries from normally distributed returns. Decorated with `@plot_function`.

**Formula:**
`R ~ N(0, 1)`, `X_t = (1 + R) * X_{t-1}`, `X_0 = 100`

**Algorithm:**
1. Start at level 100
2. If `direction == END_TODAY`, shift start date back by `length - 1` days
3. For each step, `levels[i+1] = levels[i] * 1 + rng.standard_normal()`
4. Dates increment by 1 ordinal day
5. Return `pd.Series(levels, index=dates)`

### generate_series_intraday(length: int, direction: IntradayDirection = IntradayDirection.START_INTRADAY_NOW) -> pd.Series
Purpose: Generate random intraday timeseries with minute-level granularity. Decorated with `@plot_function`.

**Formula:**
`R ~ N(0, 0.001)` (scaled for intraday volatility), `X_t = (1 + R) * X_{t-1}`, `X_0 = 100`

**Algorithm:**
1. Start at level 100, timestamp floored to minute
2. If `direction == END_INTRADAY_NOW`, shift start back by `length - 1` minutes
3. For each step, `levels[i+1] = levels[i] * 1 + rng.standard_normal()`
4. Times increment by 1 minute via `pd.Timedelta(minutes=1)`
5. Return `pd.Series(levels, index=times)`

### percentiles(x: pd.Series, y: Optional[pd.Series] = None, w: Union[Window, int, str] = Window(None, 0)) -> pd.Series
Purpose: Rolling percentile rank of `y` in the sample distribution of `x`. Decorated with `@plot_function`.

**Formula:**
`R_t = ( SUM([X_i < Y_t]) + 0.5 * SUM([X_i == Y_t]) ) / N * 100%`

over rolling window of length N.

**Algorithm:**
1. If `x` is empty, return `x`
2. If `y` is None, set `y = x.copy()`
3. `normalize_window(y, w)`
4. Validate ramp <= len(y), window <= len(x)
5. Branch: `w.w` is `DateOffset` -> iterate `y.items()`, filter `x` by date window, call `percentileofscore(sample, val, kind='mean')`
6. Branch: `w.w` is int and `y` not empty -> build `rolling_window` on x truncated to y's last index, apply `percentileofscore`, reindex to joined index, forward-fill, select y's index
7. Apply ramp and return

**Raises:** `ValueError` when ramp > len(y)

### percentile(x: pd.Series, n: float, w: Union[Window, int, str] = None) -> Union[pd.Series, float]
Purpose: Nth percentile of a series, optionally rolling. Decorated with `@plot_function`.

**Algorithm:**
1. Validate `0 <= n <= 100`, raise `MqValueError` otherwise
2. Drop NaN from x; if empty, return x
3. Branch: `w is None` -> return `np.percentile(x.values, n)` (scalar)
4. Divide n by 100
5. `normalize_window(x, w)`
6. Branch: `w.w` is `DateOffset` -> list comprehension with `.quantile(n)` and date filtering; if DatetimeIndex use datetime comparison, else use `.date()`
7. Branch: `w.w` is int -> `x.rolling(w.w, 0).quantile(n)`
8. Apply ramp and return

**Raises:** `MqValueError` when n not in [0, 100]; `MqTypeError` when relative dates used with incompatible index

### LinearRegression.__init__(self, X: Union[pd.Series, List[pd.Series]], y: pd.Series, fit_intercept: bool = True)
Purpose: Fit a static OLS linear regression.

**Algorithm:**
1. Validate `fit_intercept` is bool, raise `MqTypeError` otherwise
2. Concatenate X into DataFrame; optionally add constant column
3. Filter out NaN/Inf rows from X and y
4. Align X and y on inner join of index
5. Fit `sm.OLS(y_aligned, df_aligned).fit()`

**Raises:** `MqTypeError` when fit_intercept is not bool

### LinearRegression.coefficient(self, i: int) -> float
Purpose: Return estimated coefficient for the i-th predictor (0 = intercept if used).

### LinearRegression.r_squared(self) -> float
Purpose: Return coefficient of determination (R^2).

### LinearRegression.fitted_values(self) -> pd.Series
Purpose: Return fitted values from the model on original inputs.

### LinearRegression.predict(self, X_predict: Union[pd.Series, List[pd.Series]]) -> pd.Series
Purpose: Predict using the fitted model on new data.

**Algorithm:**
1. Concatenate X_predict into DataFrame
2. Optionally add constant if `fit_intercept`
3. Return `self._res.predict(df)`

### LinearRegression.standard_deviation_of_errors(self) -> float
Purpose: Return `sqrt(MSE_resid)` -- standard deviation of error term.

### RollingLinearRegression.__init__(self, X: Union[pd.Series, List[pd.Series]], y: pd.Series, w: int, fit_intercept: bool = True)
Purpose: Fit a rolling OLS linear regression with window size `w`.

**Algorithm:**
1. Validate `fit_intercept` is bool
2. Concatenate X into DataFrame; optionally add constant
3. Validate `w > number_of_columns`, raise `MqValueError` otherwise
4. Filter out NaN/Inf, align X and y
5. Fit `RollingOLS(y_aligned, df_aligned, w).fit()`

**Raises:** `MqTypeError` when fit_intercept is not bool; `MqValueError` when window <= number of explanatory variables

### RollingLinearRegression.coefficient(self, i: int) -> pd.Series
Purpose: Return rolling estimated coefficients for the i-th predictor.

### RollingLinearRegression.r_squared(self) -> pd.Series
Purpose: Return rolling R^2 values.

### RollingLinearRegression.fitted_values(self) -> pd.Series
Purpose: Compute fitted values at end of each rolling window.

**Algorithm:**
1. Multiply `self._X` element-wise by `self._res.params.values`
2. Sum across columns with `min_count=len(columns)` to propagate NaN

### RollingLinearRegression.standard_deviation_of_errors(self) -> pd.Series
Purpose: Return `sqrt(MSE_resid)` for each rolling window.

### SIRModel.__init__(self, beta, gamma, s, i, r, n, fit, fit_period)
Purpose: Initialize and optionally fit an SIR epidemiological model.

**Differential equations:**
`dS/dt = -beta * S * I / N`
`dI/dt = beta * S * I / N - gamma * I`
`dR/dt = gamma * I`

**Algorithm:**
1. Coerce `n` to scalar (from Series if needed), default 100
2. If no state data provided, disable fitting
3. Default: s=N, i=1, r=0
4. Determine date range from data and DataContext
5. Convert scalar states to single-element Series
6. Build parameters via `SIR.get_parameters()`
7. Create `EpidemicModel`, optionally fit
8. Solve ODE over date range, store predictions as `s_predict`, `i_predict`, `r_predict`

**Raises:** `MqTypeError` when fit is not bool

### SIRModel.s0(self) -> float
Purpose: Return fitted or initial susceptible count.

### SIRModel.i0(self) -> float
Purpose: Return fitted or initial infectious count.

### SIRModel.r0(self) -> float
Purpose: Return fitted or initial recovered count.

### SIRModel.beta(self) -> float
Purpose: Return fitted or initial transmission rate.

### SIRModel.gamma(self) -> float
Purpose: Return fitted or initial recovery rate.

### SIRModel.predict_s(self) -> pd.Series
Purpose: Return predicted susceptible timeseries.

### SIRModel.predict_i(self) -> pd.Series
Purpose: Return predicted infectious timeseries.

### SIRModel.predict_r(self) -> pd.Series
Purpose: Return predicted recovered timeseries.

### SEIRModel.__init__(self, beta, gamma, sigma, s, e, i, r, n, fit, fit_period)
Purpose: Initialize and optionally fit an SEIR epidemiological model. Inherits from SIRModel but does NOT call super().__init__; implements its own initialization.

**Differential equations:**
`dS/dt = -beta * S * I / N`
`dE/dt = beta * S * I / N - sigma * E`
`dI/dt = sigma * E - gamma * I`
`dR/dt = gamma * I`

**Algorithm:**
1. Same coercion/defaults as SIR plus `e` defaults to 1
2. Build parameters via `SEIR.get_parameters()` with sigma
3. Create `EpidemicModel`, optionally fit
4. Solve ODE, store `s_predict`, `e_predict`, `i_predict`, `r_predict`

**Raises:** `MqTypeError` when fit is not bool

### SEIRModel.e0(self) -> float
Purpose: Return fitted or initial exposed count.

### SEIRModel.beta(self) -> float
Purpose: Return fitted or initial transmission rate (overrides SIRModel.beta).

### SEIRModel.gamma(self) -> float
Purpose: Return fitted or initial recovery rate (overrides SIRModel.gamma).

### SEIRModel.sigma(self) -> float
Purpose: Return fitted or initial exposed-to-infected rate.

### SEIRModel.predict_e(self) -> pd.Series
Purpose: Return predicted exposed timeseries.

## State Mutation
- `SIRModel._model.s_predict`, `i_predict`, `r_predict`: Set during `__init__` after solving the ODE
- `SEIRModel._model.e_predict`: Set during `__init__` after solving the ODE
- No global state mutation
- All `@plot_function` decorated functions are pure (no side effects beyond the decorator's behavior)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `zscores` | String window with non-date index |
| `MqValueError` | `percentile` | n not in [0, 100] |
| `MqTypeError` | `percentile` | Relative dates with incompatible index |
| `MqTypeError` | `LinearRegression.__init__` | `fit_intercept` is not bool |
| `MqTypeError` | `RollingLinearRegression.__init__` | `fit_intercept` is not bool |
| `MqValueError` | `RollingLinearRegression.__init__` | Window <= number of explanatory variables |
| `MqTypeError` | `SIRModel.__init__` | `fit` is not bool |
| `MqTypeError` | `SEIRModel.__init__` | `fit` is not bool |
| `ValueError` | `percentiles` | Ramp value > length of series y |
| `AssertionError` | `min_`, `max_`, `range_`, `mean`, `median`, `mode`, `std`, `var`, `cov` | Index is not monotonic increasing |
| `AssertionError` | `sum_`, `product` | Index is not monotonic increasing |
| `AssertionError` | `winsorize` | Window is 0 / falsy |

## Edge Cases
- `std` on an empty series returns the empty series immediately
- `zscores` on a single-element series returns `[0.0]`
- `zscores` on an empty series (`size < 1`) returns `x` unchanged
- `winsorize` on an empty series (`size < 1`) returns `x` unchanged
- `percentiles` on empty `x` returns `x`; when `w.w` (int) > `len(x)` returns empty float Series
- `percentile` with `w=None` returns a scalar, not a Series
- `percentile` on empty series (after dropna) returns the empty series
- `_zscore` on a single element returns 0 (avoids division by zero in std)
- `generate_series` and `generate_series_intraday` use `np.random.default_rng()` (non-reproducible without seed)
- `mean` with `MeanType.QUADRATIC` squares input before averaging, then takes sqrt -- negative inputs will produce real results since squaring makes them positive
- `rolling_std` fallback: `results[0]` is always NaN regardless of data
- `min_` and `max_` with list input and `DateOffset` window: list comprehension path uses `idx - w.w` differently (`.datetime()` for min_, direct for max_) -- potential inconsistency

## Bugs Found
- Line 901: `generate_series` formula `levels[i] * 1 + rng.standard_normal()` -- due to operator precedence this is `(levels[i] * 1) + rng.standard_normal()` rather than the documented `levels[i] * (1 + R)`. Same issue in `generate_series_intraday` at line 963. (OPEN)
- Line 151-152 vs 221: `min_` uses `(idx - w.w).datetime()` while `max_` uses `idx - w.w` for the non-Series DateOffset path -- inconsistent date comparison. (OPEN)

## Coverage Notes
- Branch count: ~72 (estimated across all functions and methods)
- Key branching dimensions: list vs series input, DateOffset vs int window, Series vs DataFrame, fit vs no-fit for epidemic models, None vs provided optional parameters
- The `try/except ImportError` for `quant_extensions` defines a fallback `rolling_std` -- both branches should be covered
- `@plot_function` and `@plot_method` decorators may add additional branch points not visible in this module
