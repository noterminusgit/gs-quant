# measures_reports.py

## Summary
Report-based portfolio analytics measures for the Marquee Plot Service. Provides functions to retrieve and compute factor risk data (exposure, PnL, risk), performance metrics (normalized performance, long/short PnL, AUM), thematic analytics (exposure, beta), and a comprehensive suite of risk-adjusted performance ratios (Sharpe, Sortino, Calmar, Treynor, Jensen's Alpha, Information Ratio, Modigliani, etc.) all keyed off GS report IDs and benchmark/risk-free asset IDs.

## Dependencies
- Internal: `gs_quant.api.gs.data` (QueryType, GsDataApi, DataQuery)
- Internal: `gs_quant.data` (DataMeasure)
- Internal: `gs_quant.data.core` (DataContext)
- Internal: `gs_quant.entities.entity` (EntityType)
- Internal: `gs_quant.errors` (MqValueError)
- Internal: `gs_quant.markets.portfolio_manager` (PortfolioManager)
- Internal: `gs_quant.markets.report` (FactorRiskReport, PerformanceReport, ThematicReport, ReturnFormat, format_aum_for_return_calculation, get_pnl_percent, get_factor_pnl_percent_for_single_factor)
- Internal: `gs_quant.markets.securities` (Bond)
- Internal: `gs_quant.models.risk_model` (FactorRiskModel)
- Internal: `gs_quant.target.reports` (PositionSourceType)
- Internal: `gs_quant.timeseries` (plot_measure_entity, beta, correlation, max_drawdown)
- Internal: `gs_quant.timeseries.algebra` (geometrically_aggregate)
- Internal: `gs_quant.timeseries.measures` (_extract_series_from_df, SecurityMaster, AssetIdentifier)
- External: `datetime` (as dt)
- External: `re`
- External: `enum` (Enum)
- External: `typing` (Optional, Union, List)
- External: `pandas` (pd -- Series, DataFrame, to_datetime, bdate_range)
- External: `numpy` (np -- sum, std, sqrt, nan, array_split)
- External: `math` (ceil)
- External: `scipy.stats` (as stats -- kurtosis, skew, linregress)
- External: `pandas.tseries.offsets` (BDay)
- External: `pydash` (decapitalize)

## Type Definitions

No dataclass or namedtuple definitions. All public functions return `pd.Series`.

## Enums and Constants

### Unit(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| NOTIONAL | `"Notional"` | Return data in notional (absolute) units |
| PERCENT | `"Percent"` | Return data as percentage of AUM |

### Module Constants
None.

## Functions/Methods

All public functions below are decorated with `@plot_measure_entity(EntityType.REPORT, ...)` which handles entity resolution and data context setup.

---

### factor_exposure(report_id: str, factor_name: str, unit: str = 'Notional', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor exposure timeseries from a factor risk report.

**Algorithm:**
1. Delegate to `_get_factor_data(report_id, factor_name, QueryType.FACTOR_EXPOSURE, Unit(unit))`

---

### factor_pnl(report_id: str, factor_name: str, unit: str = 'Notional', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor PnL timeseries from a factor risk report.

**Algorithm:**
1. Delegate to `_get_factor_data(report_id, factor_name, QueryType.FACTOR_PNL, Unit(unit))`

---

### factor_proportion_of_risk(report_id: str, factor_name: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor proportion of risk timeseries.

**Algorithm:**
1. Delegate to `_get_factor_data(report_id, factor_name, QueryType.FACTOR_PROPORTION_OF_RISK)` (no unit param)

---

### daily_risk(report_id: str, factor_name: str = 'Total', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve daily risk timeseries for a factor (must be "Factor", "Specific", or "Total").

**Algorithm:**
1. Delegate to `_get_factor_data(report_id, factor_name, QueryType.DAILY_RISK)`

---

### annual_risk(report_id: str, factor_name: str = 'Total', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve annual risk timeseries for a factor (must be "Factor", "Specific", or "Total").

**Algorithm:**
1. Delegate to `_get_factor_data(report_id, factor_name, QueryType.ANNUAL_RISK)`

---

### normalized_performance(report_id: str, leg: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute normalized performance of a portfolio from a performance report.

**Formula:**
```
NP(L/S)_t = SUM( PNL(L/S)_t / (EXP(L/S)_t - cPNL(L/S)_{t-1}) )
  if EXP(L/S)_t > 0
  else: 1 / SUM(...)

NP_t = NP(L)_t * SUM(EXP(L)) / SUM(GROSS_EXP)
     + NP(S)_t * SUM(EXP(S)) / SUM(GROSS_EXP) + 1
```

**Algorithm:**
1. Get start/end dates from DataContext, shift start back by 1 business day
2. Fetch portfolio constituents with fields `[assetId, pnl, quantity, netExposure]`
3. Branch: if `leg` is provided, filter constituents to long (`quantity > 0`) or short (`quantity < 0`)
4. Split into long side (`quantity > 0`) and short side (`quantity < 0`)
5. Compute `_return_metrics` for each side
6. Calculate `short_exposure`, `long_exposure`, `gross_exposure`
7. Weight each side's metrics by its share of gross exposure
8. Combine: `normalizedPerformance = longRetWeighted + shortRetWeighted + 1`
9. Return Series, dropping NaN

---

### long_pnl(report_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: PnL from long holdings only.

**Algorithm:**
1. Fetch constituents with `[pnl, quantity]` from performance report
2. Filter to `quantity > 0`
3. Group by date, sum PnL
4. Return as named Series "longPnl"

---

### short_pnl(report_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: PnL from short holdings only.

**Algorithm:**
1. Fetch constituents with `[pnl, quantity]` from performance report
2. Filter to `quantity < 0`
3. Group by date, sum PnL
4. Return as named Series "shortPnl"

---

### thematic_exposure(report_id: str, basket_ticker: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Thematic exposure of portfolio to a GS thematic flagship basket.

**Algorithm:**
1. Get ThematicReport by report_id
2. Look up basket asset by ticker via SecurityMaster
3. Call `thematic_report.get_thematic_exposure(...)` with basket marquee ID
4. If not empty, set date index and convert to DatetimeIndex
5. Extract series via `_extract_series_from_df(df, QueryType.THEMATIC_EXPOSURE)`

---

### thematic_beta(report_id: str, basket_ticker: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Thematic beta of portfolio to a GS thematic flagship basket.

**Algorithm:**
1. Get ThematicReport by report_id
2. Look up basket asset by ticker
3. Call `thematic_report.get_thematic_betas(...)`
4. If not empty, set date index and convert to DatetimeIndex
5. Extract series via `_extract_series_from_df(df, QueryType.THEMATIC_BETA)`

---

### aum(report_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: AUM (Assets Under Management) timeseries for a portfolio.

**Algorithm:**
1. Get PerformanceReport, call `get_aum(start_date, end_date)`
2. Convert dict to list of `{date, aum}` dicts
3. Build DataFrame; if not empty, set date index and convert to DatetimeIndex
4. Extract series via `_extract_series_from_df(df, QueryType.AUM)`

---

### pnl(report_id: str, unit: str = 'Notional', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Total PnL from all holdings. If unit is Percent, geometrically aggregates.

**Algorithm:**
1. Get PerformanceReport, call `get_pnl(start_date, end_date)`
2. Branch: `unit == 'Percent'` -> call `get_pnl_percent(performance_report, pnl_df, 'pnl', start_date, end_date)`
3. Branch: else -> set index to date, return `pnl` column as Series

---

### historical_simulation_estimated_pnl(report_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Estimated PnL from replaying historical factor moves on latest positions.

**Algorithm:**
1. Call `_replay_historical_factor_moves_on_latest_positions(report_id, [])`
2. Sum across all factors per date: `factor_attributed_pnl.apply(np.sum, axis=1)`
3. Convert index to datetime, return squeezed Series named "estimatedPnl"

---

### historical_simulation_estimated_factor_attribution(report_id: str, factor_name: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Estimated PnL attributed to a single factor from historical simulation.

**Algorithm:**
1. Call `_replay_historical_factor_moves_on_latest_positions(report_id, [factor_name])`
2. Convert index to datetime, squeeze to Series

---

### hit_rate(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Percentage of positions with positive returns over a rolling period.

**Formula:**
`HitRate_t = count(cumPnl_i > 0 over window) / count(valid_assets over window)`

**Algorithm:**
1. Fetch constituents with `[date, pnl]`, filter to `entryType == 'Holding'`
2. Sort by date, pivot to have assets as columns, pnl as values
3. Rolling sum of pnl over window
4. Count positive sums, divide by total valid asset count
5. Return ratio

---

### portfolio_max_drawdown(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling maximum drawdown of AUM.

**Algorithm:**
1. Get AUM series via `aum(report_id)`
2. Parse window, apply `_max_drawdown` via `.rolling(window).apply()`

---

### drawdown_length(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Length in days between peak and trough of maximum drawdown.

**Algorithm:**
1. Get AUM series via `aum(report_id)`
2. Parse window, apply `_drawdown_length` via `.rolling(window).apply()`

---

### max_recovery_period(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Maximum number of days to recover a previously broken price level.

**Algorithm:**
1. Get AUM series via `aum(report_id)`
2. Parse window, apply `_max_recovery_period` via `.rolling(window).apply()`

---

### standard_deviation(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling standard deviation of daily PnL returns.

**Formula:**
`sigma_t = std(dailyPnl over rolling window)`

**Algorithm:**
1. Get daily PnL via `_get_daily_pnl(report_id)`
2. Parse window, return `portfolio_pnl.rolling(window).std()`

---

### downside_risk(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling downside risk -- std of returns below the mean.

**Formula:**
`DownsideRisk = sqrt( mean( (R_i - mean(R))^2 ) )` for all `R_i < mean(R)`

**Algorithm:**
1. Get daily PnL
2. Parse window, apply `_rolling_downside_risk` via `.rolling(window).apply()`

---

### semi_variance(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling semi-variance -- std of returns below zero.

**Formula:**
`SemiVar = sqrt( mean( (R_i - mean(R))^2 ) )` for all `R_i < 0`

**Algorithm:**
1. Get daily PnL
2. Parse window, apply `_rolling_semi_variance` via `.rolling(window).apply()`

---

### kurtosis(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling excess kurtosis of daily returns (Fisher definition, unbiased).

**Formula:**
`Kurt = scipy.stats.kurtosis(x, fisher=True, bias=False)`

**Algorithm:**
1. Get daily PnL
2. Parse window, apply `stats.kurtosis(x, fisher=True, bias=False)` via `.rolling(window).apply()`

---

### skewness(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling skewness of daily returns (unbiased).

**Formula:**
`Skew = scipy.stats.skew(x, bias=False)`

**Algorithm:**
1. Get daily PnL
2. Parse window, apply `stats.skew(x, bias=False)` via `.rolling(window).apply()`

---

### realized_var(report_id: str, rolling_window: Union[int, str], confidence_interval: float = 0.95, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling Value-at-Risk using historical quantile approach.

**Formula:**
`VaR_t = -1 * quantile(dailyPnl, 1 - confidence_interval)` over rolling window

**Algorithm:**
1. Get daily PnL
2. Parse window, compute `.rolling(window).quantile(1 - confidence_interval)`
3. Multiply by -1 (to express as positive loss)

---

### tracking_error(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling standard deviation of excess return relative to benchmark.

**Formula:**
`TE = std(Rp - Rb)` over rolling window

**Algorithm:**
1. Get benchmark daily returns, portfolio daily PnL
2. Compute active return = `portfolio - benchmark`
3. Return `active_return.rolling(window).std()`

---

### tracking_error_bear(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Tracking error only for periods when benchmark return was negative.

**Formula:**
`TE_bear = std(Rp - Rb)` when `Rb <= 0`, else NaN

**Algorithm:**
1. Get benchmark return (cumulative), portfolio PnL
2. Compute `is_bear = benchmark_returns <= 0`
3. Rolling apply: if `is_bear` at window's last date, compute `np.std(x, ddof=1)`, else `np.nan`

---

### tracking_error_bull(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Tracking error only for periods when benchmark return was positive.

**Formula:**
`TE_bull = std(Rp - Rb)` when `Rb > 0`, else NaN

**Algorithm:**
1. Same as `tracking_error_bear` but with `is_bull = benchmark_returns > 0`

---

### portfolio_sharpe_ratio(report_id: str, risk_free_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling Sharpe ratio.

**Formula:**
`Sharpe = (mean(Rp - Rf) / std(Rp - Rf)) * sqrt(252)`

where `Rf` is the daily risk-free rate: `(1 + annual_Rf)^(1/252) - 1`

**Algorithm:**
1. Get portfolio PnL and risk-free rate (converted to daily)
2. Compute daily excess returns = `portfolio_pnl - risk_free_rate_daily`
3. Rolling mean / rolling std, annualize by `* sqrt(252)`

---

### calmar_ratio(report_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling Calmar ratio.

**Formula:**
`Calmar = AnnualizedReturn / |MaxDrawdown|`

**Algorithm:**
1. Get daily PnL, compute cumulative return: `(1 + pnl)^sqrt(252) - 1`
2. Get AUM series, compute max drawdown via `max_drawdown(aum_series, rolling_window)`
3. Return `cumulative_return / abs(max_drawdown_series)`

---

### sortino_ratio(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling Sortino ratio.

**Formula:**
`Sortino = (mean(Rp - Rb) * 252) / (sqrt(mean(neg_excess^2)) * sqrt(252))`

**Algorithm:**
1. Get portfolio PnL and benchmark PnL
2. Branch: if benchmark is a Bond, use risk-free rate; else use daily benchmark return
3. Compute excess = `portfolio_pnl - benchmark_pnl`
4. Rolling apply `_compute_sortino`

---

### jensen_alpha(report_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Jensen's Alpha -- excess return over CAPM-predicted return.

**Formula:**
`Alpha = Rp - [Rf + Beta * (Rb - Rf)]`

**Algorithm:**
1. Get portfolio PnL, benchmark daily return, risk-free rate (daily)
2. Compute portfolio beta vs benchmark using `beta(portfolio_pnl, benchmark_pnl, prices=False)`
3. Return `portfolio_pnl - (risk_free_rate + beta * (benchmark_pnl - risk_free_rate))`

---

### jensen_alpha_bear(report_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Jensen's Alpha restricted to periods when benchmark return was negative.

**Algorithm:**
1. Same as `jensen_alpha` but filters `benchmark_pnl` to `benchmark_pnl < 0` before computing

---

### jensen_alpha_bull(report_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Jensen's Alpha restricted to periods when benchmark return was positive.

**Algorithm:**
1. Same as `jensen_alpha` but filters `benchmark_pnl` to `benchmark_pnl > 0` before computing

---

### information_ratio(report_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Information ratio -- excess return per unit of tracking error.

**Formula:**
`IR = (Rp - Rb) / std(Rp - Rb)`

Note: uses `np.std` (population std, ddof=0) on full excess series, not rolling.

**Algorithm:**
1. Compute excess = `portfolio_pnl - benchmark_pnl`
2. Compute `portfolio_std = np.std(portfolio_excess)` (scalar)
3. Return `portfolio_excess / portfolio_std`

---

### information_ratio_bear(report_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Information ratio for bear periods only (benchmark < 0).

**Algorithm:**
1. Filter benchmark to `benchmark_pnl < 0`
2. Same formula as `information_ratio`

---

### information_ratio_bull(report_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Information ratio for bull periods only (benchmark > 0).

**Algorithm:**
1. Filter benchmark to `benchmark_pnl > 0`
2. Same formula as `information_ratio`

---

### modigliani_ratio(report_id: str, benchmark_id: str, risk_free_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Modigliani Risk-Adjusted Performance (M2).

**Formula:**
`M2 = (SharpeRatio * sigma_benchmark) + Rf`

**Algorithm:**
1. Get benchmark daily return and risk-free rate (daily)
2. Get portfolio Sharpe ratio via `portfolio_sharpe_ratio(report_id, risk_free_id, rolling_window)`
3. Compute benchmark excess return `= benchmark - risk_free`
4. Rolling std of benchmark excess
5. Return `(sharpe * std_benchmark) + risk_free_rates`

---

### treynor_measure(report_id: str, risk_free_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Treynor ratio -- excess return per unit of beta.

**Formula:**
`Treynor = (Rp - Rf) / Beta`

where `Rp` is annualized: `(1 + dailyPnl)^sqrt(252) - 1`

**Algorithm:**
1. Get portfolio return (annualized), risk-free rate, benchmark pricing
2. Compute beta of AUM vs benchmark pricing
3. Return `(annualized_return - risk_free_rate) / beta_series`

---

### alpha(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling regression intercept (alpha) of portfolio vs benchmark returns.

**Algorithm:**
1. Get portfolio PnL and benchmark return (cumulative)
2. Reindex benchmark to portfolio index
3. For each rolling window: run `scipy.stats.linregress(portfolio_window, benchmark_window)`, store intercept
4. Return Series of intercepts

---

### portfolio_beta(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling beta of portfolio vs benchmark.

**Algorithm:**
1. Get portfolio PnL and benchmark return (cumulative)
2. Delegate to `beta(portfolio_return, benchmark_pnl, rolling_window, prices=False)`

---

### portfolio_correlation(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Rolling correlation coefficient between portfolio AUM and benchmark spot prices.

**Algorithm:**
1. Get AUM series (with DatetimeIndex), benchmark spot pricing
2. Delegate to `correlation(portfolio_pnl, benchmark_pricing, rolling_window)`

---

### capture_ratio(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Ratio of average portfolio return to average benchmark return over rolling window.

**Formula:**
`CaptureRatio = mean(Rp) / mean(Rb)` over rolling window

**Algorithm:**
1. Compute rolling mean of portfolio PnL and benchmark daily return
2. Return `avg_portfolio / avg_benchmark`

---

### r_squared(report_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: R-squared -- squared correlation between portfolio and benchmark.

**Formula:**
`R^2 = correlation(portfolio, benchmark)^2`

**Algorithm:**
1. Get rolling correlation via `portfolio_correlation(report_id, benchmark_id, rolling_window)`
2. Return `r_values ** 2`

---

### _get_factor_data(report_id: str, factor_name: str, query_type: QueryType, unit: Unit = Unit.NOTIONAL) -> pd.Series
Purpose: Internal helper to fetch and process factor risk report data.

**Algorithm:**
1. Get FactorRiskReport by ID
2. Branch: if `factor_name` not in `['Factor', 'Specific', 'Total']`:
   - If query is DAILY_RISK or ANNUAL_RISK, raise `MqValueError`
   - Else resolve factor name via `FactorRiskModel.get().get_factor(factor_name).name`
3. Build column name from query_type value
4. Branch: if factor is not Total AND unit is PERCENT AND query is FACTOR_PNL, also query 'Total'
5. Fetch report results for requested factors
6. Separate total_data and factor_data lists
7. Branch: `unit == PERCENT`:
   - Validate `position_source_type == Portfolio`, else raise `MqValueError`
   - Get PortfolioManager and PerformanceReport
   - Branch: FACTOR_PNL -> geometrically aggregate via `get_factor_pnl_percent_for_single_factor`
   - Branch: else -> divide exposure by AUM * 100 (raises `MqValueError` if AUM missing on any date)
8. Branch: `unit == NOTIONAL` -> simple extraction
9. Build DataFrame, set date index, convert to DatetimeIndex
10. Return via `_extract_series_from_df(df, query_type)`

**Raises:** `MqValueError` when factor_name invalid for risk queries, when unit is percent for non-portfolio reports, or when AUM data is missing

---

### _return_metrics(one_leg: pd.DataFrame, dates: list, name: str) -> pd.DataFrame
Purpose: Compute normalized return metrics for one side (long or short) of a portfolio.

**Formula:**
```
cumulativePnl = cumsum(pnl)
normalizedExposure = exposure - cumulativePnl
metrics = cumulativePnl / normalizedExposure + 1
if exposure < 0: metrics = 1 / metrics
```

**Algorithm:**
1. If empty, return DataFrame with zeros for all dates
2. Group by index, aggregate: sum pnl, sum netExposure
3. Compute cumulative PnL (set first row to 0)
4. Compute normalized exposure = exposure - cumulative PnL
5. Compute `{name}Metrics = cumulativePnl / normalizedExposure + 1`
6. If exposure is negative (short side), invert the metrics

---

### _get_benchmark_return(benchmark_id: str, start_date, end_date) -> pd.Series
Purpose: Get cumulative benchmark return as percentage.

**Formula:**
`return = (price / price[0] - 1) * 100`

**Algorithm:**
1. Look up security, get spot price data coordinate
2. Get pricing series, sort by index
3. Compute cumulative return: `(price / start_price - 1) * 100`

---

### _get_benchmark_daily_return(benchmark_id: str, start_date, end_date) -> pd.Series
Purpose: Get daily percentage change of benchmark price.

**Algorithm:**
1. Look up security, get spot price series
2. Return `benchmark_pricing.pct_change()`

---

### _max_drawdown(series) -> float
Purpose: Compute maximum drawdown for a window (used as rolling apply function).

**Formula:**
`drawdown = (series - cummax) / cummax`, return `cummin(drawdown)[-1]`

---

### _compute_sortino(series) -> float
Purpose: Compute Sortino ratio for a window.

**Formula:**
`downside_std = sqrt(mean(negative_excess^2)) * sqrt(252)`
`mean_excess = mean(excess) * 252`
`sortino = mean_excess / downside_std` (NaN if downside_std == 0)

---

### _rolling_downside_risk(series) -> float
Purpose: Compute downside risk for a window.

**Formula:**
`negative_devs = returns[returns < mean] - mean`
`semi_variance = mean(negative_devs^2)`
`downside_risk = sqrt(semi_variance)`

---

### _rolling_semi_variance(series) -> float
Purpose: Compute semi-variance for a window (returns below 0).

**Formula:**
`negative_devs = returns[returns < 0] - mean(returns)`
`semi_variance = mean(negative_devs^2)`
`result = sqrt(semi_variance)`

---

### _get_daily_pnl(report_id: str) -> pd.Series
Purpose: Get daily PnL as percentage of AUM.

**Algorithm:**
1. Get total PnL via `pnl(report_id)`
2. Convert index to DatetimeIndex
3. Divide by shifted AUM: `portfolio_return / aum(report_id).shift(1)`

---

### _get_risk_free_rate(risk_free_id, start_date, end_date) -> pd.Series
Purpose: Get risk-free yield series.

**Algorithm:**
1. Look up security, get 'yield' data coordinate
2. Get series, drop duplicates

---

### _replay_historical_factor_moves_on_latest_positions(report_id: str, factors: List[str]) -> Union[pd.Series, pd.DataFrame]
Purpose: Replay historical factor returns on a portfolio's latest factor exposures.

**Algorithm:**
1. Get FactorRiskReport, extract risk model ID
2. Fetch factor return data from `RISK_MODEL_FACTOR` dataset in 365-day batches
3. Pivot to have factors as columns, geometrically aggregate returns
4. Get latest factor exposures from the report
5. Multiply aggregated returns by exposures element-wise
6. Return DataFrame of factor-attributed PnL

---

### _parse_window(window: Union[int, str]) -> int
Purpose: Parse a window specification (int passthrough or string like "22d", "1m", "1y").

**Algorithm:**
1. If int, return as-is
2. Match regex `(\d+)([dwmy])`
3. If no match, raise `MqValueError`
4. Multiply value by unit multiplier: `{d: 1, w: 5, m: 22, y: 252}`
5. Return integer result

**Raises:** `MqValueError` when format is invalid

---

### _max_recovery_period(series) -> int
Purpose: Compute maximum recovery period (days) in a window.

**Algorithm:**
1. Track peak, iterate through values
2. When value drops below peak, increment drawdown counter
3. When value recovers above peak, record recovery period, reset
4. Return maximum recovery period (0 if none)

---

### _drawdown_length(series) -> int
Purpose: Compute maximum drawdown length (days) in a window.

**Algorithm:**
1. Track peak and current drawdown length
2. When value >= peak, reset (new peak found)
3. When value < peak, increment length, track maximum
4. Return max drawdown length

## State Mutation
- No global state mutation
- All functions read from `DataContext.current` (thread-local context) but do not modify it
- API calls (`GsDataApi.execute_query`, report `.get_results()`, etc.) perform network I/O
- `_get_factor_data` mutates local variables only

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_get_factor_data` | factor_name not in ['Factor', 'Specific', 'Total'] for DAILY_RISK/ANNUAL_RISK |
| `MqValueError` | `_get_factor_data` | unit=Percent for non-portfolio reports |
| `MqValueError` | `_get_factor_data` | Missing AUM on dates when converting to percent |
| `MqValueError` | `_parse_window` | Invalid window format string |

## Edge Cases
- `_return_metrics` with empty DataFrame returns zeros for all dates
- `_max_recovery_period` returns 0 if no drawdowns occurred
- `_drawdown_length` returns 0 if series is monotonically increasing
- `_compute_sortino` returns `np.nan` if no negative excess returns (downside_std == 0)
- `normalized_performance` filters by leg before splitting into long/short -- when leg="long", short side will be empty, producing zero-weighted contribution
- `information_ratio` uses population std (ddof=0 via `np.std`) rather than sample std -- differs from `tracking_error` which uses `pd.Series.rolling.std()` (ddof=1)
- `tracking_error` uses `_get_benchmark_daily_return` (pct_change) while `tracking_error_bear`/`tracking_error_bull` use `_get_benchmark_return` (cumulative) -- asymmetric benchmark calculation
- `calmar_ratio` annualizes return using `sqrt(252)` as exponent rather than `252` -- nonstandard annualization
- `treynor_measure` also uses `sqrt(252)` for annualization -- same issue
- `alpha` computes regression with `linregress(portfolio, benchmark)` (portfolio as x, benchmark as y) -- this gives the intercept of benchmark regressed on portfolio, which is the reverse of the typical convention

## Bugs Found
- Lines 967, 1372: Annualization uses `(1 + pnl)^sqrt(252) - 1` instead of the standard `(1 + pnl)^252 - 1`. Using `sqrt(252)` (~15.87) as the exponent is nonstandard. (OPEN)
- Line 1418: `stats.linregress(portfolio_window, benchmark_window)` regresses benchmark on portfolio rather than portfolio on benchmark, so the intercept is the wrong alpha. (OPEN)

## Coverage Notes
- Branch count: ~55 (estimated across all public and private functions)
- Key branching dimensions: unit Notional vs Percent, factor_name in special list vs custom, query_type variants, leg filtering (long/short/none), Bond vs non-Bond benchmark, bear/bull filtering, empty DataFrames
- The `@plot_measure_entity` decorator handles entity resolution and may add early-return branches
- Many functions share the pattern: get data -> parse window -> rolling apply, which should allow test reuse
