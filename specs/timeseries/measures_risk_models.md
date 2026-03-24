# measures_risk_models.py

## Summary
Provides plot measure functions for risk model data retrieval: asset-level measures (risk_model_measure, factor_zscore), factor-level measures (factor_covariance, factor_volatility, factor_correlation, factor_performance), and intraday/percentile measures (factor_returns_intraday, factor_returns_percentile). Uses `@plot_measure` for asset-based queries and `@plot_measure_entity` for entity-based queries. Includes a private helper `__format_plot_measure_results` for converting time-series dicts to pandas Series.

## Dependencies
- Internal: `gs_quant.api.gs.data` (QueryType), `gs_quant.api.gs.risk_models` (IntradayFactorDataSource), `gs_quant.common` (AssetClass, AssetType), `gs_quant.data.core` (DataContext), `gs_quant.entities.entity` (EntityType), `gs_quant.markets.factor` (ReturnFormat), `gs_quant.markets.securities` (Asset, AssetIdentifier), `gs_quant.models.risk_model` (FactorRiskModel, MarqueeRiskModel), `gs_quant.target.risk_models` (RiskModelDataMeasure, RiskModelDataAssetsRequest, RiskModelUniverseIdentifierRequest), `gs_quant.timeseries` (plot_measure_entity, plot_measure, prices), `gs_quant.timeseries.statistics` (percentile), `gs_quant.timeseries.measures` (_extract_series_from_df)
- External: `enum` (Enum), `typing` (Dict, Optional, Union), `pandas`, `pydash` (decapitalize), `datetime`

## Type Definitions
None (no dataclasses or type aliases).

## Enums and Constants

### ModelMeasureString(Enum)
Maps human-readable measure names to their string values. 31 members total.

| Value | Raw | Description |
|-------|-----|-------------|
| ASSET_UNIVERSE | `"Asset Universe"` | Asset universe membership |
| HISTORICAL_BETA | `"Historical Beta"` | Historical beta value |
| TOTAL_RISK | `"Total Risk"` | Total risk measure |
| SPECIFIC_RISK | `"Specific Risk"` | Specific (idiosyncratic) risk |
| SPECIFIC_RETURNS | `"Specific Return"` | Specific return |
| DAILY_RETURNS | `"Daily Returns"` | Daily return |
| ESTIMATION_UNVERSE_WEIGHT | `"Estimation Universe Weight"` | Estimation universe weight |
| RESIDUAL_VARIANCE | `"Residual Variance"` | Residual variance |
| PREDICTED_BETA | `"Predicted Beta"` | Predicted beta |
| GLOBAL_PREDICTED_BETA | `"Global Predicted Beta"` | Global predicted beta |
| UNIVERSE_FACTOR_EXPOSURE | `"Universe Factor Exposure"` | Universe factor exposure |
| R_SQUARED | `"R Squared"` | R-squared |
| FAIR_VALUE_GAP_PERCENT | `"Fair Value Gap Percent"` | Fair value gap percentage |
| FAIR_VALUE_GAP_STANDARD_DEVIATION | `"Fair Value Gap Standard Deviation"` | Fair value gap in std deviations |
| BID_ASK_SPREAD | `"Bid Ask Spread"` | Bid-ask spread |
| BID_AKS_SPREAD_30D | `"Bid Ask Spread 30d"` | 30-day bid-ask spread |
| BID_AKS_SPREAD_60D | `"Bid Ask Spread 60d"` | 60-day bid-ask spread |
| BID_AKS_SPREAD_90D | `"Bid Ask Spread 90d"` | 90-day bid-ask spread |
| TRADING_VOLUME | `"Trading Volume"` | Trading volume |
| TRADING_VOLUME_30D | `"Trading Volume 30d"` | 30-day trading volume |
| TRADING_VOLUME_60D | `"Trading Volume 60d"` | 60-day trading volume |
| TRADING_VOLUME_90D | `"Trading Volume 90d"` | 90-day trading volume |
| TRADING_VALUE_30D | `"Trading Value 30d"` | 30-day trading value |
| COMPOSITE_VOLUME | `"Composite Volume"` | Composite volume |
| COMPOSITE_VOLUME_30D | `"Composite Volume 30d"` | 30-day composite volume |
| COMPOSITE_VOLUME_60D | `"Composite Volume 60d"` | 60-day composite volume |
| COMPOSITE_VOLUME_90D | `"Composite Volume 90d"` | 90-day composite volume |
| COMPOSITE_VALUE_30d | `"Composite Value 30d"` | 30-day composite value |
| COMPOSITE_ISSUER_MARKET_CAP | `"Composite Issuer Market Cap"` | Issuer market cap |
| COMPOSITE_PRICE | `"Composite Price"` | Composite price |
| COMPOSITE_MODEL_PRICE | `"Composite Model Price"` | Composite model price |
| COMPOSITE_CAPITALIZATION | `"Composite Capitalization"` | Composite capitalization |
| COMPOSITE_CURRENCY | `"Composite Currency"` | Composite currency |
| COMPOSITE_UNADJUSTED_SPECIFIC_RISK | `"Composite Unadjusted Specific Risk"` | Unadjusted specific risk |
| DIVIDEND_YIELD | `"Dividend Yield"` | Dividend yield |

### ModelMeasureStr (module-level dict)
Maps the same human-readable strings (e.g., `'Historical Beta'`) to `RiskModelDataMeasure` enum values. Used to translate `ModelMeasureString` enum values into API request parameters.

## Functions/Methods

### risk_model_measure(asset: Asset, risk_model_id: str, risk_model_measure_selected: ModelMeasureString = ModelMeasureString.HISTORICAL_BETA, *, source, real_time, request_id) -> pd.Series
Purpose: Retrieve a risk model measure time-series for a given equity asset.

Decorated with: `@plot_measure((AssetClass.Equity,), (AssetType.Single_Stock,))`

**Algorithm:**
1. Get `MarqueeRiskModel` by `risk_model_id`
2. Get GSID identifier from asset
3. Translate `risk_model_measure_selected.value` through `ModelMeasureStr` dict to get `RiskModelDataMeasure`
4. Query model with selected measure + `Asset_Universe`, using GSID, date range from `DataContext`
5. Iterate over query results:
   - Branch: if `result` is truthy:
     - Extract measure name by diffing `assetData` keys against `{'universe'}`
     - Get exposures list from `assetData[measure_name]`
     - Branch: if `exposures` is non-empty -> store `exposures[0]` keyed by `result['date']`
6. Return formatted series via `__format_plot_measure_results`

### factor_zscore(asset: Asset, risk_model_id: str, factor_name: str, *, source, real_time, request_id) -> pd.Series
Purpose: Get asset factor exposure z-scores for a specific factor in a risk model.

Decorated with: `@plot_measure((AssetClass.Equity,), (AssetType.Single_Stock,))`

**Algorithm:**
1. Get `MarqueeRiskModel` by `risk_model_id`
2. Get factor object from model by `factor_name`
3. Get GSID from asset
4. Query model with `Factor_Name`, `Universe_Factor_Exposure`, `Asset_Universe` measures
5. Iterate over results:
   - Get `factorExposure` list from `assetData`
   - Branch: if `exposures` is non-empty -> store `exposures[0][factor.id]` keyed by date
6. Return formatted series via `__format_plot_measure_results`

### factor_covariance(risk_model_id: str, factor_name_1: str, factor_name_2: str, *, source, real_time, request_id) -> pd.Series
Purpose: Covariance time-series between two factors.

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [QueryType.FACTOR_RETURN])`

**Algorithm:**
1. Get `FactorRiskModel` by ID
2. Get both factor objects
3. Call `factor_1.covariance(factor_2, start, end, ReturnFormat.JSON)`
4. Return formatted series with `QueryType.COVARIANCE`

### factor_volatility(risk_model_id: str, factor_name: str, *, source, real_time, request_id) -> pd.Series
Purpose: Volatility time-series for a factor.

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [QueryType.FACTOR_RETURN])`

**Algorithm:**
1. Get `FactorRiskModel` and factor
2. Call `factor.volatility(start, end, ReturnFormat.JSON)`
3. Return formatted series with `QueryType.VOLATILITY` and `multiplier=100`

### factor_correlation(risk_model_id: str, factor_name_1: str, factor_name_2: str, *, source, real_time, request_id) -> pd.Series
Purpose: Correlation time-series between two factors.

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [QueryType.FACTOR_RETURN])`

**Algorithm:**
1. Get `FactorRiskModel` by ID
2. Get both factor objects
3. Call `factor_1.correlation(factor_2, start, end, ReturnFormat.JSON)`
4. Return formatted series with `QueryType.CORRELATION`

### factor_performance(risk_model_id: str, factor_name: str, *, source, real_time, request_id) -> pd.Series
Purpose: Factor returns as a cumulative price time-series (starting at 100).

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [QueryType.FACTOR_RETURN])`

**Algorithm:**
1. Get `FactorRiskModel` and factor
2. Get factor returns DataFrame for date range
3. Squeeze to Series, divide by 100 (convert from percentage to decimal)
4. Call `prices(factor_returns_series, 100)` to create cumulative price series
5. Return result

### factor_returns_intraday(risk_model_id: str, factor_name: str, data_source: Union[IntradayFactorDataSource, str] = None, *, source, real_time, request_id) -> pd.Series
Purpose: Intraday factor returns time-series.

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [])`

**Algorithm:**
1. Get `FactorRiskModel` and factor
2. Call `factor.intraday_returns(start_time, end_time, data_source)`
3. Squeeze DataFrame to Series
4. Return result

### factor_returns_percentile(risk_model_id: str, factor_name: str, lookback_days: int = 10, n_percentile: float = 90.0, *, source, real_time, request_id) -> pd.Series
Purpose: Percentile of factor returns over a lookback period.

Decorated with: `@plot_measure_entity(EntityType.RISK_MODEL, [])`

**Algorithm:**
1. Compute `start_date` as `DataContext.current.start_time - timedelta(days=lookback_days)`
2. Get factor returns DataFrame from `start_date` to `end_date`
3. Squeeze to Series and compute percentile via `percentile(series, n_percentile)`
4. Return constant series of that percentile value over 2-hour intervals from start_time to end_time

### __format_plot_measure_results(time_series: Dict, query_type: QueryType, multiplier=1, handle_missing_column=False)
Purpose: Convert a `{date: value}` dict into a pandas Series suitable for plot measures.

**Algorithm:**
1. Build column name by removing spaces from `query_type.value` and decapitalizing
2. Build list of `{date: k, col_name: v * multiplier}` dicts
3. Create DataFrame from list
4. Branch: if DataFrame is not empty -> set 'date' as index, convert to DatetimeIndex
5. Call `_extract_series_from_df(df, query_type, handle_missing_column)` and return

## State Mutation
- No module-level mutable state.
- All functions query external APIs and return new series.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised directly) | -- | Errors propagate from underlying risk model API calls |

## Edge Cases
- `risk_model_measure` extracts measure name by diffing `assetData` keys with `{'universe'}` -- if `assetData` has no extra keys, `measure_name.pop()` will raise `KeyError`.
- `factor_returns_percentile` returns a 2-hour interval constant series, which may not align with typical daily data consumers.
- `__format_plot_measure_results` with an empty `time_series` dict produces an empty DataFrame, which is handled by the non-empty check.
- `factor_performance` divides returns by 100 assuming they are in percentage format; if the API returns decimal format, values would be wrong.
- `ModelMeasureString` has a typo in member name: `ESTIMATION_UNVERSE_WEIGHT` (missing 'I' in UNIVERSE).
- `ModelMeasureString` has inconsistent naming: `BID_AKS_SPREAD_30D` should be `BID_ASK_SPREAD_30D` (missing 'S').

## Bugs Found
- None (typos in enum member names are cosmetic, not functional, since the `.value` strings are correct).

## Coverage Notes
- Branch count: ~14
- Key branches: truthy checks on `result` and `exposures` in risk_model_measure/factor_zscore loops, empty DataFrame check in __format_plot_measure_results, multiplier application
- Most functions are thin wrappers around API calls with minimal branching
