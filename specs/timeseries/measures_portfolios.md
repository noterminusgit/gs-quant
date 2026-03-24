# timeseries/measures_portfolios.py

## Summary
Provides portfolio-level timeseries measures for the GS Quant platform. Each public function accepts a `portfolio_id`, retrieves the appropriate report (factor risk, thematic, or performance) via `PortfolioManager`, and delegates to the corresponding function in `measures_reports`. The module covers AUM, factor exposure/PnL/risk, thematic exposure, PnL, and a comprehensive suite of performance analytics (hit rate, drawdown, standard deviation, downside risk, semi-variance, kurtosis, skewness, VaR, tracking error, Sharpe/Calmar/Sortino/information/Modigliani/Treynor ratios, Jensen's alpha, regression alpha/beta, correlation, R-squared, and capture ratio). All decorated functions use `@plot_measure_entity` for the `EntityType.PORTFOLIO` entity type.

## Dependencies
- Internal: `gs_quant.timeseries.measures_reports` (imported as `ReportMeasures` -- provides `factor_exposure`, `factor_pnl`, `factor_proportion_of_risk`, `daily_risk`, `annual_risk`, `thematic_exposure`, `pnl`, `hit_rate`, `portfolio_max_drawdown`, `drawdown_length`, `max_recovery_period`, `standard_deviation`, `downside_risk`, `semi_variance`, `kurtosis`, `skewness`, `realized_var`, `tracking_error`, `tracking_error_bear`, `tracking_error_bull`, `portfolio_sharpe_ratio`, `calmar_ratio`, `sortino_ratio`, `information_ratio`, `information_ratio_bull`, `information_ratio_bear`, `modigliani_ratio`, `treynor_measure`, `jensen_alpha`, `jensen_alpha_bear`, `jensen_alpha_bull`, `alpha`, `portfolio_beta`, `portfolio_correlation`, `r_squared`, `capture_ratio`)
- Internal: `gs_quant.api.gs.data` (`QueryType`)
- Internal: `gs_quant.api.gs.portfolios` (`GsPortfolioApi`)
- Internal: `gs_quant.entities.entity` (`EntityType`)
- Internal: `gs_quant.markets.portfolio_manager` (`PortfolioManager`)
- Internal: `gs_quant.timeseries` (`plot_measure_entity`)
- Internal: `gs_quant.timeseries.measures` (`_extract_series_from_df`)
- External: `datetime` (`dt` alias)
- External: `logging`
- External: `typing` (`Optional`, `Union`)
- External: `pandas` (`pd` alias)

## Type Definitions

No custom types are defined in this module. All functions operate on primitive types (`str`, `int`, `float`) and return `pd.Series`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `LOGGER` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger instance |

## Functions/Methods

### aum(portfolio_id: str, start_date: dt.date = None, end_date: dt.date = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Returns the Custom AUM uploaded for a portfolio. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.AUM])`.

**Algorithm:**
1. Call `GsPortfolioApi.get_custom_aum(portfolio_id, start_date, end_date)` to fetch AUM data.
2. Branch: if `data` is already a `pd.DataFrame`, use it directly; otherwise convert from records using `pd.DataFrame.from_records(data)`.
3. Set the index to a `DatetimeIndex` constructed from the `'date'` column.
4. Return `_extract_series_from_df(df, QueryType.AUM, True)`.

### portfolio_factor_exposure(portfolio_id: str, risk_model_id: str, factor_name: str, unit: str = 'Notional', benchmark_id: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor exposure timeseries from a factor risk report. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.FACTOR_EXPOSURE])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the factor risk report via `pm.get_factor_risk_report(risk_model_id=risk_model_id, benchmark_id=benchmark_id)`.
3. Delegate to `ReportMeasures.factor_exposure(report.id, factor_name, unit)` and return the result.

### portfolio_factor_pnl(portfolio_id: str, risk_model_id: str, factor_name: str, unit: str = 'Notional', benchmark_id: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor PnL timeseries from a factor risk report. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.FACTOR_PNL])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the factor risk report via `pm.get_factor_risk_report(risk_model_id=risk_model_id, benchmark_id=benchmark_id)`.
3. Delegate to `ReportMeasures.factor_pnl(report.id, factor_name, unit)` and return the result.

### portfolio_factor_proportion_of_risk(portfolio_id: str, risk_model_id: str, factor_name: str, benchmark_id: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve factor proportion of risk timeseries. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.FACTOR_PROPORTION_OF_RISK])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the factor risk report via `pm.get_factor_risk_report(risk_model_id=risk_model_id, benchmark_id=benchmark_id)`.
3. Delegate to `ReportMeasures.factor_proportion_of_risk(report.id, factor_name)` and return the result.

### portfolio_daily_risk(portfolio_id: str, risk_model_id: str, factor_name: str = 'Total', benchmark_id: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve daily risk timeseries from a factor risk report. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.DAILY_RISK])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the factor risk report via `pm.get_factor_risk_report(risk_model_id=risk_model_id, benchmark_id=benchmark_id)`.
3. Delegate to `ReportMeasures.daily_risk(report.id, factor_name)` and return the result.

### portfolio_annual_risk(portfolio_id: str, risk_model_id: str, factor_name: str = 'Total', benchmark_id: str = None, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve annual risk timeseries from a factor risk report. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.ANNUAL_RISK])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the factor risk report via `pm.get_factor_risk_report(risk_model_id=risk_model_id, benchmark_id=benchmark_id)`.
3. Delegate to `ReportMeasures.annual_risk(report.id, factor_name)` and return the result.

### portfolio_thematic_exposure(portfolio_id: str, basket_ticker: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve thematic exposure of a portfolio to a GS thematic flagship basket. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.THEMATIC_EXPOSURE])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the thematic report via `pm.get_thematic_report()`.
3. Delegate to `ReportMeasures.thematic_exposure(thematic_report.id, basket_ticker)` and return the result.

### portfolio_pnl(portfolio_id: str, unit: str = 'Notional', *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve PnL timeseries from all holdings. If unit is Percent, geometrically aggregated over time frame. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.PNL])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.pnl(performance_report.id, unit)` and return the result.

### portfolio_hit_rate(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute hit rate of a portfolio over a rolling period. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.HIT_RATE])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.hit_rate(performance_report.id, rolling_window)` and return the result.

### portfolio_max_drawdown(portfolio_id: str, rolling_window: int, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute max drawdown of a portfolio over a rolling period. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.MAX_DRAWDOWN])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.portfolio_max_drawdown(performance_report.id, rolling_window)` and return the result.

### portfolio_drawdown_length(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute the length in days between the peak and the trough of the maximum drawdown. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.MAX_DRAWDOWN])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.drawdown_length(performance_report.id, rolling_window)` and return the result.

### portfolio_max_recovery_period(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute the maximum number of days to reach a previously broken price level. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.MAX_DRAWDOWN])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.max_recovery_period(performance_report.id, rolling_window)` and return the result.

### portfolio_standard_deviation(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute volatility of the total return over the stated time frame. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.STANDARD_DEVIATION])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.standard_deviation(performance_report.id, rolling_window)` and return the result.

### portfolio_downside_risk(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute volatility of periodic returns below the mean (downside risk). Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.DOWNSIDE_RISK])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.downside_risk(performance_report.id, rolling_window)` and return the result.

### portfolio_semi_variance(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute volatility of periodic returns below zero (semi-variance). Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.DOWNSIDE_RISK])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.semi_variance(performance_report.id, rolling_window)` and return the result.

### portfolio_kurtosis(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute kurtosis (peakedness/flatness) of the return distribution. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.KURTOSIS])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.kurtosis(performance_report.id, rolling_window)` and return the result.

### portfolio_skewness(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute skewness (degree of symmetry) of the return distribution. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.SKEWNESS])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.skewness(performance_report.id, rolling_window)` and return the result.

### portfolio_realized_var(portfolio_id: str, rolling_window: Union[int, str], confidence_interval: float = 0.95, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute realized Value-at-Risk based on natural distribution of returns. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.REALIZED_VAR])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.realized_var(performance_report.id, rolling_window, confidence_interval)` and return the result.

### portfolio_tracking_error(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute standard deviation of excess return relative to benchmark. Formula: `sqrt(sum((ExcessRi - ExcessRmean)^2) / (N-1))`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.TRACKING_ERROR])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.tracking_error(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_tracking_error_bear(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute tracking error only during periods when benchmark returns are negative. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.TRACKING_ERROR_BEAR])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.tracking_error_bear(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_tracking_error_bull(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute tracking error only during periods when benchmark returns are positive. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.TRACKING_ERROR_BULL])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.tracking_error_bull(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_sharpe_ratio(portfolio_id: str, risk_free_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute risk-adjusted Sharpe ratio: `(Rp - Rf) / sigma_p`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.SHARPE_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.portfolio_sharpe_ratio(performance_report.id, risk_free_id, rolling_window)` and return the result.

### portfolio_calmar_ratio(portfolio_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Calmar ratio: `Annualized Return / Max Drawdown`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.CALMAR_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.calmar_ratio(performance_report.id, rolling_window)` and return the result.

### portfolio_sortino_ratio(portfolio_id: str, comparison_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute risk-adjusted Sortino ratio: `(Rp - Rb) / Downside Deviation`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.SORTINO_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.sortino_ratio(performance_report.id, comparison_id, rolling_window)` and return the result.

### portfolio_information_ratio(portfolio_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute information ratio: `(Rp - Rb) / Tracking Error`. Measures consistency of beating the benchmark. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.INFORMATION_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.information_ratio(performance_report.id, benchmark_id)` and return the result.

### portfolio_information_ratio_bull(portfolio_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute information ratio counting only periods when benchmark return was positive. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.INFORMATION_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.information_ratio_bull(performance_report.id, benchmark_id)` and return the result.

### portfolio_information_ratio_bear(portfolio_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute information ratio counting only periods when benchmark return was negative. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.INFORMATION_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.information_ratio_bear(performance_report.id, benchmark_id)` and return the result.

### portfolio_modigliani_ratio(portfolio_id: str, benchmark_id: str, risk_free_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Modigliani M2 measure: `M2 = (Sharpe Ratio * sigma_b) + Rf`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.MODIGLIANI_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.modigliani_ratio(performance_report.id, benchmark_id, risk_free_id, rolling_window)` and return the result.

### portfolio_treynor_measure(portfolio_id: str, risk_free_id: str, benchmark_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Treynor ratio: `(Rp - Rf) / Beta`. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.TREYNOR_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.treynor_measure(performance_report.id, risk_free_id, benchmark_id)` and return the result.

### portfolio_jensen_alpha(portfolio_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Jensen's Alpha: `Rp - [Rf + Beta * (Rb - Rf)]`. **Not decorated** with `@plot_measure_entity` (bare function).

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.jensen_alpha(performance_report.id, benchmark_id, risk_free_id)` and return the result.

### portfolio_jensen_alpha_bear(portfolio_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Jensen's Alpha only during periods when benchmark return was negative. **Not decorated** with `@plot_measure_entity` (bare function).

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.jensen_alpha_bear(performance_report.id, benchmark_id, risk_free_id)` and return the result.

### portfolio_jensen_alpha_bull(portfolio_id: str, benchmark_id: str, risk_free_id: str, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute Jensen's Alpha only during periods when benchmark return was positive. **Not decorated** with `@plot_measure_entity` (bare function).

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.jensen_alpha_bull(performance_report.id, benchmark_id, risk_free_id)` and return the result.

### portfolio_alpha(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute regression alpha (intercept from portfolio vs benchmark return regression). Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.ALPHA])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.alpha(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_beta(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute regression beta (slope from portfolio vs benchmark return regression). Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.BETA])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.portfolio_beta(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_correlation(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute correlation coefficient between portfolio and benchmark returns. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.CORRELATION])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.portfolio_correlation(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_r_squared(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute R-squared measuring how well portfolio performance correlates with benchmark. Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.R_SQUARED])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.r_squared(performance_report.id, benchmark_id, rolling_window)` and return the result.

### portfolio_capture_ratio(portfolio_id: str, benchmark_id: str, rolling_window: Union[int, str], *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute capture ratio (ratio of portfolio return to benchmark return averaged over the time period). Decorated with `@plot_measure_entity(EntityType.PORTFOLIO, [QueryType.CAPTURE_RATIO])`.

**Algorithm:**
1. Instantiate `PortfolioManager(portfolio_id)`.
2. Get the performance report via `pm.get_performance_report()`.
3. Delegate to `ReportMeasures.capture_ratio(performance_report.id, benchmark_id, rolling_window)` and return the result.

## State Mutation
- No module-level mutable state is modified at runtime.
- `LOGGER` is created at import time and never reassigned.
- All functions are stateless: they instantiate a `PortfolioManager`, retrieve a report, delegate to `ReportMeasures`, and return the result.
- Thread safety: Functions are thread-safe as they create local objects and do not share mutable state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (delegated) | All factor risk functions | `pm.get_factor_risk_report()` may raise if no matching report exists |
| (delegated) | All performance functions | `pm.get_performance_report()` may raise if no performance report exists |
| (delegated) | `portfolio_thematic_exposure` | `pm.get_thematic_report()` may raise if no thematic report exists |
| (delegated) | `aum` | `GsPortfolioApi.get_custom_aum()` may raise on API errors |
| (delegated) | All functions | Underlying `ReportMeasures.*` functions may raise their own errors |

No exceptions are explicitly raised in this module; all error handling is delegated to the called APIs and report measure functions.

## Edge Cases
- **`aum` handles both DataFrame and dict-list returns**: The `isinstance(data, pd.DataFrame)` check handles the case where `GsPortfolioApi.get_custom_aum` returns either a DataFrame directly or a list of dicts.
- **Three Jensen's alpha functions are undecorated**: `portfolio_jensen_alpha`, `portfolio_jensen_alpha_bear`, and `portfolio_jensen_alpha_bull` (lines 909-1026) lack the `@plot_measure_entity` decorator, unlike every other public function. This means they cannot be auto-discovered by the plotting/entity framework.
- **Shared QueryType decorators**: `portfolio_max_drawdown`, `portfolio_drawdown_length`, and `portfolio_max_recovery_period` all share `QueryType.MAX_DRAWDOWN`. Similarly, `portfolio_downside_risk` and `portfolio_semi_variance` share `QueryType.DOWNSIDE_RISK`. And `portfolio_information_ratio`, `portfolio_information_ratio_bull`, and `portfolio_information_ratio_bear` share `QueryType.INFORMATION_RATIO`.
- **`rolling_window` type inconsistency**: `portfolio_max_drawdown` accepts `rolling_window: int` (only int), while most other rolling-window functions accept `Union[int, str]`. This means `portfolio_max_drawdown` does not support string-based window specifications (e.g. `'1y'`).
- **Factor risk functions use `get_factor_risk_report`**: Functions 2-6 (factor exposure, factor pnl, factor proportion of risk, daily risk, annual risk) all retrieve a factor risk report, while the remaining performance functions use `get_performance_report`. `portfolio_thematic_exposure` uniquely uses `get_thematic_report`.
- **Delegation pattern**: Every function except `aum` follows the exact same 3-step pattern (create PortfolioManager, get report, delegate to ReportMeasures). The `aum` function is the only one that directly queries an API and processes the DataFrame locally.

## Bugs Found
- Lines 909, 949, 989: `portfolio_jensen_alpha`, `portfolio_jensen_alpha_bear`, and `portfolio_jensen_alpha_bull` are missing the `@plot_measure_entity` decorator that all other public functions in this module have. This is likely an oversight rather than intentional, as the functions accept the same `source`, `real_time`, and `request_id` keyword-only parameters that the decorator framework uses. (OPEN)

## Coverage Notes
- Branch count: ~3 (the only real branch is in `aum` with the `isinstance(data, pd.DataFrame)` check; all other functions are linear delegation)
- The 30 delegation functions are structurally identical (3-step pattern) and differ only in which report type is fetched and which `ReportMeasures` function is called.
- All actual computation and branching logic lives in `measures_reports` (the `ReportMeasures` module); this module is purely a facade layer.
- Pragmas: none
