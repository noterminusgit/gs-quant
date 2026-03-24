# measures.py

## Summary
The largest module in the codebase (6080 LOC). Provides dozens of decorated market-data measure functions consumed by the GS Plot Service. Each public function is decorated with `@plot_measure` (which tags the function with asset-class / asset-type metadata, dependencies, and display names) and follows a common pattern: validate parameters, build a GS Data API query, fetch data via `_market_data_timed`, and return an `ExtendedSeries`. Covers equity, FX, rates, credit, and commodity asset classes with measures spanning implied volatility, skew, correlation, variance swaps, forward prices, fundamental metrics, ESG, commodity forecasts, and more.

## Dependencies
- Internal:
  - `gs_quant.api.gs.assets` (`GsAssetApi`, `GsIdType`)
  - `gs_quant.api.gs.data` (`MarketDataResponseFrame`, `QueryType`, `GsDataApi`)
  - `gs_quant.api.gs.indices` (`GsIndexApi`)
  - `gs_quant.api.utils` (`ThreadPoolManager`)
  - `gs_quant.common` (`AssetClass`, `AssetType`, `PricingLocation`)
  - `gs_quant.data` (`Dataset`)
  - `gs_quant.data.core` (`DataContext`, `IntervalFrequency`)
  - `gs_quant.data.fields` (`Fields`, `DataMeasure`)
  - `gs_quant.data.log` (`log_debug`, `log_warning`)
  - `gs_quant.datetime` (`DAYS_IN_YEAR`)
  - `gs_quant.datetime.gscalendar` (`GsCalendar`)
  - `gs_quant.datetime.point` (`relative_date_add`)
  - `gs_quant.entities.entity` (`PositionedEntity`)
  - `gs_quant.errors` (`MqValueError`, `MqTypeError`)
  - `gs_quant.markets.securities` (`Asset`, `AssetIdentifier`, `AssetType as SecAssetType`, `SecurityMaster`, `Stock`)
  - `gs_quant.timeseries` (`Basket`, `RelativeDate`, `Returns`, `Window`, `sqrt`, `volatility`)
  - `gs_quant.timeseries.helper` (`FREQ_MONTH_END`, `_month_to_tenor`, `_split_where_conditions`, `_tenor_to_month`, `_to_offset`, `check_forward_looking`, `get_dataset_with_many_assets`, `get_df_with_retries`, `log_return`, `plot_measure`)
  - `gs_quant.timeseries.measures_helper` (`EdrDataReference`, `VolReference`, `preprocess_implied_vol_strikes_eq`)
- External:
  - `calendar` (monthrange, month_name, month_abbr)
  - `datetime` (date, datetime, timedelta)
  - `logging`
  - `re` (fullmatch)
  - `collections` (namedtuple)
  - `enum` (Enum, auto)
  - `functools` (partial)
  - `numbers` (Real)
  - `typing` (Union, Optional, Tuple, List)
  - `cachetools.func` (ttl_cache)
  - `inflection` (camelize)
  - `numpy` (np)
  - `pandas` (pd)
  - `dateutil` (tz, relativedelta)
  - `pandas.tseries.holiday` (AbstractHolidayCalendar, Holiday, USLaborDay, USMemorialDay, USThanksgivingDay, sunday_to_monday)
  - `pydash` (chunk, flatten, get)

## Type Definitions

### GENERIC_DATE (TypeAlias)
```
GENERIC_DATE = Union[dt.date, str]
```

### ASSET_SPEC (TypeAlias)
```
ASSET_SPEC = Union[Asset, str]
```

### MeasureDependency (namedtuple)
| Field | Type | Description |
|-------|------|-------------|
| id_provider | `Callable` | Function that converts asset_spec to a Marquee ID for the dependency |
| query_type | `QueryType` | The query type this dependency provides |

### ExtendedSeries (class)
Inherits: `pd.Series`

Custom pandas Series that carries `dataset_ids` through operations.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| dataset_ids | `tuple` | (dynamic) | Tuple of dataset IDs that contributed data to this series |

**Methods:**
- `_constructor` (property) -> returns `ExtendedSeries` so slicing preserves the type
- `__finalize__(self, other, method=None, **kwargs)` -> copies `dataset_ids` from other if it is an `ExtendedSeries`

### NercCalendar (class)
Inherits: `AbstractHolidayCalendar`

US NERC (power grid) holiday calendar with rules for New Year's, Memorial Day, July 4th, Labor Day, Thanksgiving, Christmas. Used for commodity power bucketization.

## Enums and Constants

### UnderlyingSourceCategory(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ALL | `'All'` | All data sources |
| EDGX | `'EDGX'` | On-exchange (EDGX) |
| TRF | `'TRF'` | Off-exchange (TRF) |

### GICSSector(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| INFORMATION_TECHNOLOGY | `'Information Technology'` | IT sector |
| ENERGY | `'Energy'` | Energy sector |
| MATERIALS | `'Materials'` | Materials sector |
| INDUSTRIALS | `'Industrials'` | Industrials sector |
| CONSUMER_DISCRETIONARY | `'Consumer Discretionary'` | Consumer discretionary sector |
| CONSUMER_STAPLES | `'Consumer Staples'` | Consumer staples sector |
| HEALTH_CARE | `'Health Care'` | Health care sector |
| FINANCIALS | `'Financials'` | Financials sector |
| COMMUNICATION_SERVICES | `'Communication Services'` | Communication services sector |
| REAL_ESTATE | `'Real Estate'` | Real estate sector |
| UTILITIES | `'Utilities'` | Utilities sector |
| ALL | `'All'` | All sectors |

### RetailMeasures(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| RETAIL_PCT_SHARES | `'impliedRetailPctShares'` | Retail percentage of shares |
| RETAIL_PCT_NOTIONAL | `'impliedRetailPctNotional'` | Retail percentage of notional |
| RETAIL_SHARES | `'impliedRetailShares'` | Retail share count |
| RETAIL_NOTIONAL | `'impliedRetailNotional'` | Retail notional |
| SHARES | `'shares'` | Total shares |
| NOTIONAL | `'notional'` | Total notional |
| RETAIL_BUY_NOTIONAL | `'impliedRetailBuyNotional'` | Retail buy notional |
| RETAIL_BUY_PCT_NOTIONAL | `'impliedRetailBuyPctNotional'` | Retail buy pct notional |
| RETAIL_BUY_PCT_SHARES | `'impliedRetailBuyPctShares'` | Retail buy pct shares |
| RETAIL_BUY_SHARES | `'impliedRetailBuyShares'` | Retail buy shares |
| RETAIL_SELL_NOTIONAL | `'impliedRetailSellNotional'` | Retail sell notional |
| RETAIL_SELL_PCT_NOTIONAL | `'impliedRetailSellPctNotional'` | Retail sell pct notional |
| RETAIL_SELL_PCT_SHARES | `'impliedRetailSellPctShares'` | Retail sell pct shares |
| RETAIL_SELL_SHARES | `'impliedRetailSellShares'` | Retail sell shares |

### SkewReference(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DELTA | `'delta'` | Delta-based strike reference |
| NORMALIZED | `'normalized'` | Normalized strike reference |
| SPOT | `'spot'` | Spot-based strike reference |
| FORWARD | `'forward'` | Forward-based strike reference |

### NormalizationMode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| NORMALIZED | `'normalized'` | Divide skew by ATM implied vol |
| OUTRIGHT | `'outright'` | Raw skew difference |

### CdsVolReference(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DELTA_CALL | `'delta_call'` | Delta call reference |
| DELTA_PUT | `'delta_put'` | Delta put reference |
| FORWARD | `'forward'` | Forward/ATMF reference |

### VolSmileReference(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SPOT | `'spot'` | Spot reference |
| FORWARD | `'forward'` | Forward reference |
| DELTA | `'delta'` | Delta reference |
| NORMALIZED | `'normalized'` | Normalized reference |

### FXForwardType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| POINTS | `'points'` | Forward points only |
| OUTRIGHT | `'outright'` | Spot + forward points |

### FxForecastHorizon(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| THREE_MONTH | `'3m'` | 3-month horizon |
| SIX_MONTH | `'6m'` | 6-month horizon |
| TWELVE_MONTH | `'12m'` | 12-month horizon |
| EOY1 | `'EOY1'` | End of year +1 |
| EOY2 | `'EOY2'` | End of year +2 |
| EOY3 | `'EOY3'` | End of year +3 |
| EOY4 | `'EOY4'` | End of year +4 |

### _FxForecastTimeSeriesPeriodType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SHORT_TERM | `'3/6/12-Month'` | Rolling short-term forecasts |
| ANNUAL | `'Annual'` | Annual EOY forecasts |

### FundamentalMetricPeriodDirection(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| FORWARD | `'forward'` | Forward-looking estimate |
| TRAILING | `'trailing'` | Trailing/reported value |

### RatesConversionType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DEFAULT_BENCHMARK_RATE | auto() | e.g. USD-LIBOR-BBA |
| DEFAULT_SWAP_RATE_ASSET | auto() | e.g. USD-3m |
| INFLATION_BENCHMARK_RATE | auto() | e.g. CPI-UKRPI |
| CROSS_CURRENCY_BASIS | auto() | e.g. EUR-3m/USD-3m |
| OIS_BENCHMARK_RATE | auto() | e.g. USD OIS |

### EsgMetric(Enum)
17 members mapping ESG metric names to their camelCase API field names (es_score, es_percentile, g_score, etc.).

### SwaptionTenorType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| OPTION_EXPIRY | `'option_expiry'` | Option expiry tenor |
| SWAP_MATURITY | `'swap_maturity'` | Swap maturity tenor |

### EquilibriumExchangeRateMetric(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| GSDEER | `'gsdeer'` | GS Dynamic Equilibrium Exchange Rate |
| GSFEER | `'gsfeer'` | GS Fair Equilibrium Exchange Rate |

### _FactorProfileMetric(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| GROWTH_SCORE | `'growthScore'` | Growth percentile |
| FINANCIAL_RETURNS_SCORE | `'financialReturnsScore'` | Financial returns percentile |
| MULTIPLE_SCORE | `'multipleScore'` | Multiple percentile |
| INTEGRATED_SCORE | `'integratedScore'` | Average integrated percentile |

### _CommodityForecastType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SPOT | `'spot'` | Spot forecast |
| SPOT_RETURN | `'spotReturn'` | Spot return forecast |
| ROLL_RETURN | `'rollReturn'` | Roll return forecast |
| TOTAL_RETURN | `'totalReturn'` | Total return forecast |

### _CommodityForecastTimeSeriesPeriodType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SHORT_TERM | `'3/6/12-Month Rolling'` | Rolling short-term |
| MONTHLY | `'Monthly'` | Monthly frequency |
| QUARTERLY | `'Quarterly'` | Quarterly frequency |
| ANNUAL | `'Annual'` | Annual frequency |

### _RatingMetric(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| RATING | `'rating'` | Analyst rating (Buy/Neutral/Sell) |
| CONVICTION_LIST | `'convictionList'` | Whether on conviction buy list |

### EUNatGasDataReference(Enum)
Maps EU natural gas hub names and currencies to commodity reference prices and fixing sources (ICE, Heren).

### EUPowerDataReference(Enum)
Maps EU power exchange names (ICE, EEX, NASDAQ) and dataset/product identifiers.

### FXSpotCarry(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ANNUALIZED | `'annualized'` | Annualized carry |
| DAILY | `'daily'` | Daily carry |

### S3Metrics(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| LONG_CROWDING | `("Long Crowding",)` | Long crowding metric |
| SHORT_CROWDING | `("Short Crowding",)` | Short crowding metric |
| LONG_SHORT_CROWDING_RATION | `("Long Short Crowding Ratio",)` | Long/short ratio |
| CURRENT_CONSTITUENTS_LONG_CROWDING | `("Current Constituents Long Crowding",)` | Current const. long crowding |
| CURRENT_CONSTITUENTS_SHORT_CROWDING | `("Current Constituents Short Crowding",)` | Current const. short crowding |
| CURRENT_CONSTITUENTS_LONG_SHORT_CROWDING | `("Current Constituents Long Short Crowding Ratio",)` | Current const. ratio |
| CROWDING_NET_EXPOSURE | `("Crowding Net Exposure",)` | Net exposure |
| CURRENT_CONSTITUENTS_CROWDING_NET_EXPOSURES | `"Current Constituents Crowding Net Exposure"` | Current const. net exposure |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| TD_ONE | `dt.timedelta` | `timedelta(days=1)` | One day timedelta |
| _logger | `Logger` | module logger | Module-level logger |
| _COMMOD_CONTRACT_MONTH_CODES | `str` | `"FGHJKMNQUVXZ"` | Futures month code letters (Jan=F ... Dec=Z) |
| _COMMOD_CONTRACT_MONTH_CODES_DICT | `dict` | `{0:'F', 1:'G', ...}` | Index-to-letter mapping |
| ESG_METRIC_TO_QUERY_TYPE | `dict` | (17 entries) | Maps camelCase ESG metric names to QueryType enum values |
| CURRENCY_TO_OIS_RATE_BENCHMARK | `dict` | (10 entries) | Maps currency codes to OIS benchmark names |
| CURRENCY_TO_DEFAULT_RATE_BENCHMARK | `dict` | (4 entries) | Maps currency codes to default rate benchmarks |
| CURRENCY_TO_INFLATION_RATE_BENCHMARK | `dict` | (2 entries) | Maps GBP/EUR to inflation benchmarks |
| CROSS_TO_CROSS_CURRENCY_BASIS | `dict` | (6 entries) | Maps FX crosses to cross-currency basis names |
| _FACTOR_PROFILE_METRIC_TO_QUERY_TYPE | `dict` | (4 entries) | Maps factor profile metric names to QueryType |
| _RATING_METRIC_TO_QUERY_TYPE | `dict` | (2 entries) | Maps rating/convictionList to QueryType |

## Decorator Patterns

### @plot_measure(asset_class, asset_type, dependencies, asset_type_excluded=None, display_name=None)
Purpose: Marks a function as a plottable measure for the GS Plot Service.

**Behavior:**
1. Validates that `asset_class` is a non-empty tuple and `asset_type` is None or a tuple.
2. Sets attributes on the function: `plot_measure=True`, `entity_type=EntityType.ASSET`, `asset_class`, `asset_type`, `asset_type_excluded`, `dependencies`, `entitlements`.
3. If `USE_DISPLAY_NAME` is True, also sets `display_name` and wraps via `register_measure()`.
4. Does NOT modify the function's runtime behavior (no argument injection, no try/except wrapping). It is purely a metadata decorator.

**Parameters:**
- `asset_class: tuple` -- tuple of `AssetClass` values the measure applies to
- `asset_type: Optional[tuple]` -- tuple of `AssetType` values; None means all types
- `dependencies: List[Union[QueryType, MeasureDependency]]` -- data dependencies; `MeasureDependency` includes an `id_provider` function that transforms the asset ID for the dependency query
- `asset_type_excluded: Optional[tuple]` -- asset types to exclude (mutually exclusive with asset_type)
- `display_name: Optional[str]` -- override for the function name in the plot service

### @log_return(_logger, message)
Purpose: Logs the return value of a function at DEBUG level. Used on `_range_from_pricing_date`.

### @cachetools.func.ttl_cache()
Purpose: TTL-based memoization. Used on `_var_swap_tenors`.

## Functions/Methods

---

### _normalize_dtidx(idx) -> pd.DatetimeIndex
Purpose: Ensure a DatetimeIndex is in nanosecond resolution.

**Algorithm:**
1. Branch: if `idx` is not a `pd.DatetimeIndex` -> convert it
2. Branch: if `idx` has `as_unit` method (pandas >= 2.0) -> call `idx.as_unit('ns')`; else return as-is
3. Return normalized index

---

### _asset_from_spec(asset_spec: ASSET_SPEC) -> Asset
Purpose: Convert an asset spec (Asset or string) to an Asset object.

**Algorithm:**
1. Branch: `asset_spec` is already an `Asset` -> return it directly
2. Else -> call `SecurityMaster.get_asset(asset_spec, AssetIdentifier.MARQUEE_ID)`

---

### _cross_stored_direction_helper(bbid: str) -> str
Purpose: Determine the stored direction for an FX cross based on market convention.

**Algorithm:**
1. Branch: if `bbid` does not match `[A-Z]{6}` -> raise `TypeError`
2. Compute boolean flags: `legit_usd_cross`, `legit_eur_cross`, `legit_jpy_cross`, `odd_cross`
3. Branch: if any flag is True -> return `bbid` as-is
4. Else -> return reversed cross `bbid[3:] + bbid[:3]`

**Raises:** `TypeError` when bbid is not a valid 6-char cross

---

### cross_stored_direction_for_fx_vol(asset_spec: ASSET_SPEC, *, return_asset=False) -> Union[str, Asset]
Purpose: Get the stored direction for FX vol queries, potentially returning the reversed cross.

**Algorithm:**
1. Get asset from spec
2. Try: if asset_class is FX, get BBID, call `_cross_stored_direction_helper`
3. Branch: if cross != bbid -> look up reversed cross asset in SecurityMaster
4. Catch `TypeError` -> fall back to original asset
5. Branch: `return_asset` is True -> return Asset object; else return Marquee ID string

---

### cross_to_usd_based_cross(asset_spec: ASSET_SPEC) -> str
Purpose: Convert an FX cross to its USD-based equivalent for forecast queries.

**Algorithm:**
1. Get asset from spec, get its Marquee ID
2. Try: if FX and BBID doesn't start with "USD" -> reverse and look up
3. Branch: bbid not matching `[A-Z]{6}` -> raise `TypeError`
4. Catch `TypeError` -> fall back to original ID
5. Return Marquee ID

---

### currency_to_default_benchmark_rate(asset_spec: ASSET_SPEC) -> str
Purpose: Map a currency asset to its default benchmark rate Marquee ID.

**Algorithm:**
1. Call `convert_asset_for_rates_data_set` with `DEFAULT_BENCHMARK_RATE`
2. Catch `TypeError` -> return asset's own Marquee ID

---

### currency_to_default_ois_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Map a currency asset to its OIS benchmark rate Marquee ID.

**Algorithm:**
1. Call `convert_asset_for_rates_data_set` with `OIS_BENCHMARK_RATE`
2. Catch `TypeError` -> return asset's own Marquee ID

---

### currency_to_default_swap_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Map a currency asset to its default swap rate asset Marquee ID.

**Algorithm:**
1. Call `convert_asset_for_rates_data_set` with `DEFAULT_SWAP_RATE_ASSET`
2. No try/except -- propagates any error

---

### currency_to_inflation_benchmark_rate(asset_spec: ASSET_SPEC) -> str
Purpose: Map a currency asset to its inflation benchmark rate Marquee ID.

**Algorithm:**
1. Call `convert_asset_for_rates_data_set` with `INFLATION_BENCHMARK_RATE`
2. Catch `TypeError` -> return asset's own Marquee ID

---

### cross_to_basis(asset_spec: ASSET_SPEC) -> str
Purpose: Map an FX cross to its cross-currency basis swap Marquee ID.

**Algorithm:**
1. Call `convert_asset_for_rates_data_set` with `CROSS_CURRENCY_BASIS`
2. Catch `TypeError` -> return asset's own Marquee ID

---

### convert_asset_for_rates_data_set(from_asset: Asset, c_type: RatesConversionType) -> str
Purpose: Core rates conversion function. Maps an asset to the appropriate rates data set identifier.

**Algorithm:**
1. Try: get BBID from asset
2. Branch: BBID is None -> return Marquee ID
3. Branch on c_type:
   - `DEFAULT_BENCHMARK_RATE` -> look up in `CURRENCY_TO_DEFAULT_RATE_BENCHMARK`
   - `DEFAULT_SWAP_RATE_ASSET` -> "USD-3m" if USD, "BBid-6m" if GBP/EUR/CHF/SEK, else bbid
   - `INFLATION_BENCHMARK_RATE` -> look up in `CURRENCY_TO_INFLATION_RATE_BENCHMARK`
   - `OIS_BENCHMARK_RATE` -> look up in `CURRENCY_TO_OIS_RATE_BENCHMARK`
   - Else (CROSS_CURRENCY_BASIS) -> look up in `CROSS_TO_CROSS_CURRENCY_BASIS`
4. Call `GsAssetApi.map_identifiers` to convert mdapi -> Marquee ID
5. Branch: if `to_asset` in identifiers -> return it
6. Branch: if `None` in identifiers -> return it
7. Else -> raise `MqValueError`
8. Catch `KeyError` -> log info, return from_asset Marquee ID

**Raises:** `MqValueError` when unable to map identifier

---

### _get_custom_bd(exchange) -> CustomBusinessDay
Purpose: Get a custom business day offset for a given exchange using GsCalendar.

---

### _range_from_pricing_date(exchange, pricing_date: Optional[GENERIC_DATE] = None, buffer: int = 0) -> tuple
Purpose: Convert a pricing date specification into a (start, end) date range.

**Algorithm:**
1. Branch: `pricing_date` is `dt.date` -> return `(pricing_date, pricing_date)`
2. Compute today, get custom business day for exchange
3. Branch: `pricing_date` is None -> return `(today - bd - buffer*bd, today - bd)`
4. Assert pricing_date is string
5. Branch: matches `\d+b` pattern -> compute `today - bd * N`
6. Else -> use `relative_date_add` to compute end, then `start = end - bd`
7. Return `(start, end)`

Decorated with `@log_return(_logger, 'trying pricing dates')`

---

### _market_data_timed(q, request_id=None, ignore_errors: bool = False) -> MarketDataResponseFrame
Purpose: Thin wrapper around `GsDataApi.get_market_data` that optionally passes request_id.

---

### _extract_series_from_df(df: pd.DataFrame, query_type: QueryType, handle_missing_column=False) -> ExtendedSeries
Purpose: Extract a named column from a DataFrame into an ExtendedSeries.

**Algorithm:**
1. Compute column name from query_type: camelCase with lowercase first letter
2. Branch: df is empty OR (handle_missing_column and col not in columns) -> return empty ExtendedSeries
3. Else -> create ExtendedSeries from df[col_name]
4. Copy dataset_ids from df

---

### _fundamentals_md_query(mqid, period, period_direction, metric, source=None, real_time=False, request_id=None) -> pd.Series
Purpose: Shared helper for fundamental metric queries (used by many `current_constituents_*` and `price_to_earnings_positive_exclusive` functions).

**Algorithm:**
1. Build market data query with metric, period, periodDirection
2. Set vendor to 'Goldman Sachs'
3. Fetch data via `_market_data_timed`
4. Extract and return series

---

### skew(asset, tenor, strike_reference, distance, normalization_mode=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX, AssetClass.Equity, AssetClass.Commod), None, [MeasureDependency(...)], asset_type_excluded=(AssetType.CommodityNaturalGasHub,))`

Purpose: Difference in implied volatility of equidistant OTM put and call options.

**Algorithm:**
1. Branch: real_time and FX/Commod -> raise `MqValueError`
2. Branch: FX:
   - Get stored direction for vol
   - Default normalization: OUTRIGHT
   - Branch: DELTA -> q_strikes = [-distance, distance, 0]
   - Else -> raise `MqValueError`
3. Branch: Equity/Commod:
   - Default normalization: NORMALIZED
   - Branch: DELTA or None -> b=50, q_strikes = [100-distance, distance, 50]
   - Branch: NORMALIZED -> b=0, q_strikes = [-distance, distance, 0]
   - Else (SPOT/FORWARD) -> b=100, q_strikes = [100-distance, 100+distance, 100]
   - Branch: no strike_reference -> raise `MqTypeError`
   - Branch: not NORMALIZED -> divide strikes by 100
4. Build query with tenor, strikeReference, relativeStrike
5. Fetch data, group by relativeStrike column
6. Branch: empty df -> empty series
7. Branch: < 3 curves -> raise `MqValueError`
8. Branch: NORMALIZED mode -> `(put - call) / atm`; OUTRIGHT -> `put - call`

**Raises:** `MqValueError` (real-time on FX/Commod, non-delta FX, insufficient data); `MqTypeError` (missing strike ref for equities)

---

### cds_implied_volatility(asset, expiry, tenor, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Credit,), (AssetType.Index,), [QueryType.IMPLIED_VOLATILITY_BY_DELTA_STRIKE])`

Purpose: CDS index implied volatility.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: FORWARD -> delta_strike="ATMF"; else -> "XDC"
3. option_type: DELTA_CALL -> "payer"; else -> "receiver"
4. Build query, fetch, extract

---

### option_premium_credit(asset, expiry, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Credit,), (AssetType.Index,), [QueryType.OPTION_PREMIUM], display_name='option_premium')`

Purpose: CDS index option premium.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch on strike_reference: FORWARD->"ATMF", DELTA_CALL->"{X}DC", DELTA_PUT->"{X}DP", else->raise `NotImplementedError`
3. Build query, fetch, extract

---

### absolute_strike_credit(asset, expiry, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Credit,), (AssetType.Index,), [QueryType.ABSOLUTE_STRIKE], display_name='absolute_strike')`

Purpose: CDS index absolute strike.

**Algorithm:** Same branching as `option_premium_credit` but queries `ABSOLUTE_STRIKE`.

---

### implied_volatility_credit(asset, expiry, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Credit,), (AssetType.Index,), [QueryType.IMPLIED_VOLATILITY_BY_DELTA_STRIKE], display_name='implied_volatility')`

Purpose: Credit implied vol (named to avoid collision with equity implied_volatility).

**Algorithm:** Same branching as `option_premium_credit` but queries `IMPLIED_VOLATILITY_BY_DELTA_STRIKE`.

---

### cds_spread(asset, spread: int, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Credit,), (AssetType.Default_Swap,), [QueryType.CDS_SPREAD_100], display_name='spread')`

Purpose: CDS spread levels (100, 250, or 500 bps).

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: spread==100 -> CDS_SPREAD_100; 250 -> CDS_SPREAD_250; 500 -> CDS_SPREAD_500; else -> raise `NotImplementedError`
3. Build query with pricingLocation="NYC", fetch, extract

---

### implied_volatility(asset, tenor, strike_reference=None, relative_strike=None, parallelize_queries=True, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.Commod), None, [MeasureDependency(...)], asset_type_excluded=(AssetType.CommodityNaturalGasHub,))`

Purpose: Asset implied volatility (equity/commod/FX).

**Algorithm:**
1. Branch: Commod and not ETF/STOCK -> delegate to `_weighted_average_valuation_curve_for_calendar_strip`
2. Branch: FX -> get stored direction, preprocess FX strikes
3. Else -> preprocess equity strikes
4. Convert tenor months to years (e.g. 12m -> 1y)
5. Call `get_historical_and_last_for_measure` with parallel queries
6. Extract series

---

### _tenor_month_to_year(tenor: str) -> str
Purpose: Convert month tenors divisible by 12 to year tenors (e.g. "12m" -> "1y").

**Algorithm:**
1. Match pattern `\d+m`
2. Branch: month % 12 == 0 -> convert to years
3. Else -> return as-is

---

### implied_volatility_ng(asset, contract_range='F20', price_method='GDD', *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.CommodityNaturalGasHub,), [QueryType.IMPLIED_VOLATILITY], display_name='implied_volatility')`

Purpose: Natural gas implied volatility with calendar strip weighted averaging.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Parse contract range into start/end dates
3. Compute contract month weights
4. Query IMPLIED_VOLATILITY with contract list and priceMethod
5. Branch: empty -> empty series
6. Else -> merge curves by weighted average

---

### _preprocess_implied_vol_strikes_fx(strike_reference, relative_strike) -> (str, Real)
Purpose: Validate and preprocess FX implied vol strike parameters.

**Algorithm:**
1. Branch: relative_strike is None and not DELTA_NEUTRAL -> raise `MqValueError`
2. Branch: FORWARD/SPOT and relative_strike != 100 -> raise `MqValueError`
3. Branch: not in VolReference or NORMALIZED -> raise `MqValueError`
4. Branch: DELTA_NEUTRAL -> set relative_strike=0 if None; raise if != 0
5. Branch: DELTA_PUT -> negate relative_strike
6. Compute ref_string: "delta" for delta types, else strike_reference.value

**Raises:** `MqValueError` for various invalid parameter combinations

---

### _check_top_n(top_n)
Purpose: Validate that top_n is numeric.

**Algorithm:**
1. Branch: None -> return
2. Try `float(top_n)` -> catch ValueError/TypeError -> raise `MqValueError`

---

### implied_correlation(asset, tenor, strike_reference, relative_strike, top_n_of_index=None, composition_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.IMPLIED_CORRELATION])`

Purpose: Equity index implied correlation, optionally for top N constituents.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: top_n is None but composition_date provided -> raise `MqValueError`
3. Validate top_n; Branch: > 100 -> raise `MqValueError`
4. Adjust strikes for DELTA_PUT; divide by 100
5. Branch: top_n is None -> simple query for IMPLIED_CORRELATION
6. Else -> get constituent weights, fetch IMPLIED_VOLATILITY for all constituents + index, calculate implied correlation via `_calculate_implied_correlation`

---

### _calculate_implied_correlation(index_mqid, vol_df, constituents_weights, request_id) -> ExtendedSeries
Purpose: Compute implied correlation from index and constituent implied vols.

**Algorithm:**
1. Create full date range, forward-fill weights
2. For each asset group: remove duplicates, reindex to full range, interpolate vols
3. Divide vols by 100
4. Separate index vol from constituent vols
5. For each date: compute weighted sum and sum-of-squares of constituent vols
6. Correlation = (index_var - sum_of_squares) / (first_moment^2 - sum_of_squares) * 100

---

### implied_correlation_with_basket(asset, tenor, strike_reference, relative_strike, basket, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.IMPLIED_CORRELATION])`

Purpose: Implied correlation between index and a custom stock basket.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Adjust strikes for DELTA_PUT, divide by 100
3. Query IMPLIED_VOLATILITY for basket + index
4. Get basket actual weights
5. Call `_calculate_implied_correlation`

---

### realized_correlation_with_basket(asset, tenor, basket, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.IMPLIED_CORRELATION])`

Purpose: Realized correlation between index and a custom stock basket.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Query index spot data
3. Get basket actual weights and spot data
4. Compute constituent vols, weighted vols
5. Correlation = (idx_vol^2 - sum_weighted_vol^2) / (sum_weighted_vol)^2 - sum_weighted_vol^2) * 100

---

### average_implied_volatility(asset, tenor, strike_reference, relative_strike, top_n_of_index=None, composition_date=None, weight_threshold=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.AVERAGE_IMPLIED_VOLATILITY, QueryType.IMPLIED_VOLATILITY])`

Purpose: Historical weighted average implied volatility of index constituents.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Validate top_n/composition_date combinations
3. Branch: top_n > 100 -> raise `NotImplementedError`
4. Branch: top_n is not None:
   - Get constituent weights
   - Preprocess equity strikes
   - Fetch IMPLIED_VOLATILITY for constituents
   - Branch: missing assets and no weight_threshold -> raise `MqValueError`
   - Branch: weight_threshold and missing weight > threshold -> raise `MqValueError`
   - Compute weighted average vol per date
5. Branch: top_n is None:
   - Adjust strikes for DELTA_PUT, divide by 100
   - Query AVERAGE_IMPLIED_VOLATILITY directly

---

### average_implied_variance(asset, tenor, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.AVERAGE_IMPLIED_VARIANCE])`

Purpose: Historical weighted average implied variance for index underlying assets.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Adjust DELTA_PUT strikes, divide by 100
3. Query AVERAGE_IMPLIED_VARIANCE, extract series

---

### average_realized_volatility(asset, tenor, returns_type=LOGARITHMIC, top_n_of_index=None, composition_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF), [QueryType.AVERAGE_REALIZED_VOLATILITY, QueryType.SPOT])`

Purpose: Historical weighted average realized volatility of index constituents.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Validate top_n/composition_date; Branch: no top_n and non-logarithmic returns -> raise `MqValueError`
3. Branch: top_n > 200 -> raise `MqValueError`
4. Branch: top_n is not None:
   - Get constituent weights, query SPOT data
   - Optionally append last data if end_date >= today
   - For each constituent: compute volatility with Window(tenor, tenor)
   - Concatenate weighted vols, sum with min_count
5. Branch: top_n is None -> query AVERAGE_REALIZED_VOLATILITY directly

---

### _get_index_constituent_weights(asset, top_n_of_index=None, composition_date=None) -> pd.DataFrame
Purpose: Retrieve and normalize constituent weights for an index.

**Algorithm:**
1. Get date range from pricing date
2. Query positions data from GsIndexApi
3. Branch: no positions -> raise `MqValueError`
4. Take latest date, sort by weight descending, take top N
5. Normalize weights to sum to 1
6. Return DataFrame indexed by underlyingAssetId

---

### cap_floor_vol(asset, expiration_tenor, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=currency_to_default_benchmark_rate, query_type=QueryType.CAP_FLOOR_VOL)])`

Purpose: Cap/floor implied normal volatility.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert asset to rates benchmark, query CAP_FLOOR_VOL, extract

---

### cap_floor_atm_fwd_rate(asset, expiration_tenor, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...CAP_FLOOR_ATM_FWD_RATE)])`

Purpose: Cap/floor ATM forward rate.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert asset, query with strike=0, extract

---

### spread_option_vol(asset, expiration_tenor, long_tenor, short_tenor, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...SPREAD_OPTION_VOL)])`

Purpose: Spread option implied normal volatility.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert asset, query SPREAD_OPTION_VOL with long/short tenor and strike, extract

---

### spread_option_atm_fwd_rate(asset, expiration_tenor, long_tenor, short_tenor, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...SPREAD_OPTION_ATM_FWD_RATE)])`

Purpose: Spread option ATM forward rate.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert asset, query with strike=0, extract

---

### zc_inflation_swap_rate(asset, termination_tenor, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...INFLATION_SWAP_RATE)])`

Purpose: Zero coupon inflation swap break-even rate.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert to inflation benchmark, query INFLATION_SWAP_RATE, extract

---

### basis(asset, termination_tenor, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Cross,), [MeasureDependency(id_provider=cross_to_basis, query_type=QueryType.BASIS)])`

Purpose: Cross-currency basis swap spread.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Convert to basis, query BASIS, extract

---

### fx_forecast(asset, relativePeriod=THREE_MONTH, relative_period=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Cross,), [MeasureDependency(id_provider=cross_to_usd_based_cross, query_type=QueryType.FX_FORECAST)])`

Purpose: FX forecasts from GIR macro analysts.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: relativePeriod truthy -> log deprecation warning
3. Get USD-based cross
4. Query FX_FORECAST with `relative_period or relativePeriod`
5. Branch: cross != usd_based_cross -> invert series (1/series)

---

### fx_forecast_time_series(asset, forecastFrequency=ANNUAL, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Cross,), [MeasureDependency(...FX_FORECAST)])`

Purpose: Short and long-term FX forecast time series.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: asset is str -> use directly; Asset -> get_marquee_id; else -> raise ValueError
3. Branch: forecastFrequency is Enum -> extract value
4. Get USD-based cross, query FX_FORECAST (all periods)
5. Group by relativePeriod, take last value per period
6. Branch: SHORT_TERM -> filter 3m/6m/12m, compute future dates
7. Branch: ANNUAL -> filter EOY*, compute year-start dates
8. Else -> raise ValueError
9. Branch: cross != usd_based -> invert series

---

### forward_vol(asset, tenor, forward_start_date, strike_reference, relative_strike, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.FX), None, [QueryType.IMPLIED_VOLATILITY])`

Purpose: Forward volatility computed from two tenors.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: FX -> preprocess FX strikes, get stored direction; Equity -> preprocess eq strikes
3. Compute t1_month (forward_start_date), t2_month (tenor + t1), convert back to tenors
4. Query IMPLIED_VOLATILITY for both tenors simultaneously
5. Attempt to append last (real-time) data if end_date >= today
6. Branch: df empty -> empty series
7. Else -> group by tenor, get short/long groups
8. Compute forward vol: `sqrt((t2*lg^2 - t1*sg^2) / tenor_months)`

---

### _process_forward_vol_term(asset, vol_series, vol_col, series_name) -> pd.Series
Purpose: Compute forward vol term structure from a vol term structure.

**Algorithm:**
1. Branch: empty vol_series -> return empty
2. Get custom business day for asset exchange
3. Compute calendar-time and business-time to expiry
4. Compute multiplier = sqrt(calTime / busTime)
5. Compute forward vol per expiry using shifted pairs
6. Slice to DataContext date range

---

### forward_vol_term(asset, strike_reference, relative_strike, pricing_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.FX), None, [QueryType.IMPLIED_VOLATILITY])`

Purpose: Forward volatility term structure.

**Algorithm:**
1. Call `vol_term(...)` to get the vol term structure
2. Call `_process_forward_vol_term(...)` to convert to forward vol

---

### _get_skew_strikes(asset, strike_reference, distance) -> Tuple[list, int]
Purpose: Calculate strike references necessary for skew computation.

**Algorithm:**
1. Branch: FX -> buffer=1, DELTA strikes [-distance, distance, 0]; else raise
2. Branch: Equity/Commod -> buffer=0
   - DELTA -> b=50, strikes [100-distance, distance, 50]
   - NORMALIZED -> b=0, strikes [-distance, distance, 0]
   - Other truthy -> b=100, strikes [100-distance, 100+distance, 100]
   - Falsy -> raise MqTypeError
   - Not NORMALIZED -> divide by 100

---

### _skew(df, relative_strike_col, iv_col, q_strikes, normalization_mode) -> ExtendedSeries
Purpose: Calculate skew from grouped strike data.

**Algorithm:**
1. Group df by relative_strike_col
2. Branch: < 3 groups -> raise `MqValueError`
3. Branch: NORMALIZED -> (put - call) / atm; else -> put - call

---

### _skew_fetcher(asset_id, query_type, where, source, real_time, request_id=None, allow_exception=False)
Purpose: Helper to fetch skew data with optional exception suppression.

**Algorithm:**
1. Try: build and execute query
2. Catch `MqValueError`: Branch: allow_exception -> log warning, return empty frame; else -> re-raise

---

### skew_term(asset, strike_reference, distance, pricing_date=None, normalization_mode=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX, AssetClass.Equity, AssetClass.Commod), None, [MeasureDependency(...)], asset_type_excluded=(AssetType.CommodityNaturalGasHub,))`

Purpose: Skew term structure.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: FX -> stored direction, OUTRIGHT normalization; else -> NORMALIZED
3. Get strikes and buffer from `_get_skew_strikes`
4. Get pricing date range
5. Branch: Equity and recent pricing date -> try intraday data (tenor + expiration, parallel)
6. Branch: both empty -> try historical data with retries (parallel)
7. Process tenor-based df: convert tenors to dates, compute skew
8. Process expiry-based df: add non-overlapping data points
9. Sort and slice to DataContext range

---

### vol_term(asset, strike_reference, relative_strike, pricing_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.Commod, AssetClass.FX), None, [QueryType.IMPLIED_VOLATILITY], asset_type_excluded=(AssetType.CommodityNaturalGasHub,))`

Purpose: Volatility term structure.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: FX -> preprocess FX strikes, buffer=1; else -> eq strikes, buffer=0
3. Get pricing date range
4. Branch: Equity and recent -> try intraday tenor and expiry data
5. Branch: both empty -> try historical with retries (with MqValueError catch for expiry)
6. Compute latest date across both DataFrames
7. Convert tenor-based data to expiration dates using business day calendar
8. Merge expiry data (non-overlapping dates)
9. Sort, slice to DataContext, store latest in attrs

---

### vol_smile(asset, tenor, strike_reference, pricing_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.IMPLIED_VOLATILITY])`

Purpose: Volatility smile (vols vs. strikes for a given tenor/date).

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Get pricing date range, fetch data with retries
3. Branch: empty -> empty series
4. Else -> take latest date, create series with float strike index

---

### measure_request_safe(parent_fn_name, asset, fn, request_id, *args, **kwargs)
Purpose: Wrapper that catches `MqValueError` and re-raises with asset context.

---

### fwd_term(asset, pricing_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.Commod), None, [QueryType.FORWARD])`

Purpose: Forward term structure for equity/commod.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Query FORWARD with strikeReference='forward', relativeStrike=1
3. Convert tenors to expiration dates
4. Sort, slice to DataContext range

---

### fx_fwd_term(asset, pricing_date=None, fwd_type=OUTRIGHT, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), None, [QueryType.SPOT, QueryType.FORWARD_POINT], display_name='fwd_term')`

Purpose: FX forward term structure (points or outright).

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Get stored direction for FX vol
3. Branch: OUTRIGHT -> also query SPOT in parallel
4. Branch: POINTS -> query FORWARD_POINT only
5. Convert tenors to expiration dates
6. Branch: OUTRIGHT -> add spot to forward points

---

### carry_term(asset, pricing_date=None, annualized=ANNUALIZED, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), None, [QueryType.FORWARD_POINT, QueryType.SPOT])`

Purpose: FX carry term structure.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Query FORWARD_POINT and SPOT in parallel
3. Convert tenors to expiration dates
4. Branch: ANNUALIZED -> carry = fwd_point * sqrt(days/252) / spot
5. Branch: DAILY -> carry = fwd_point / spot

---

### _var_swap_tenors(asset, request_id=None) -> list
Purpose: Get available var swap tenors for an asset from market availability API.

**Algorithm:**
1. Query `/data/markets/{aid}/availability`
2. Find VAR_SWAP dataField, extract tenor values
3. Branch: not found -> raise `MqValueError`

Decorated with `@cachetools.func.ttl_cache()`

---

### forward_var_term(asset, pricing_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.VAR_SWAP])`

Purpose: Forward variance swap term structure.

**Algorithm:**
1. Call `var_term(...)` to get the var swap term structure
2. Call `_process_forward_vol_term(...)` with VAR_SWAP field

---

### _get_latest_term_structure_data(asset_id, query_type, where, groupby, source, request_id)
Purpose: Get the latest intraday term structure data.

**Algorithm:**
1. Build "Last" measure query for today + 1 day
2. Branch: empty df_l -> return empty
3. Build real-time query from df_l.index[-1] - 1 hour to query_end
4. Branch: empty df_r -> return empty
5. Group by groupby columns, take last per group, re-index by date

---

### var_term(asset, pricing_date=None, forward_start_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.Commod), None, [QueryType.VAR_SWAP])`

Purpose: Variance swap term structure.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: pricing_date invalid type -> raise `MqTypeError`
3. Branch: forward_start_date provided:
   - Get available tenors
   - For each tenor: compute diff, call `var_swap(asset, t1, forward_start_date)`
   - Concatenate sub-frames
4. Branch: no forward_start_date:
   - Branch: Equity and recent -> try intraday data
   - Branch: empty -> try historical query
5. Convert tenors to expiration dates, sort, slice
6. Store latest in attrs

---

### _get_var_swap_df(asset, where, source, real_time) -> DataFrame
Purpose: Get var swap data with optional "Last" append.

**Algorithm:**
1. Query VAR_SWAP
2. Branch: not real_time and end_date >= today -> try appending "Last" real-time data
3. Catch Exception -> log warning
4. Branch: df_l empty -> log warning
5. Branch: df_l.index.max() > result.index.max() -> concat

---

### var_swap(asset, tenor, forward_start_date=None, *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity, AssetClass.Commod), None, [QueryType.VAR_SWAP])`

Purpose: Variance swap strike level, optionally forward-starting.

**Algorithm:**
1. Branch: forward_start_date is None -> simple query for single tenor
2. Branch: forward_start_date is not str -> raise `MqTypeError`
3. Compute combined tenor z = x + y
4. Check both yt, zt in available tenors; Branch: missing -> empty series
5. Query both tenors, compute forward var swap: `sqrt((z*zg^2 - y*yg^2) / x)`

---

### _get_iso_data(region: str) -> tuple
Purpose: Get timezone, peak hours, and weekend definition for a power ISO region.

**Algorithm:**
1. Default: US/Eastern, peak 7-23, weekends [5,6]
2. Branch: MISO/ERCOT/SPP -> US/Central, peak 6-22
3. Branch: CAISO -> US/Pacific, weekends [6] only (Saturday), peak 6-22

---

### _get_qbt_mapping(bucket, region) -> list
Purpose: Map a power bucket to its component quantity buckets.

**Algorithm:**
1. Determine weekend_offpeak: "SUH1X16" for CAISO, "2X16H" otherwise
2. Map OFFPEAK, 7X16, 7X24 to component buckets
3. Return mapping or [bucket.upper()] if not a combo

---

### _get_weight_for_bucket(asset, start_contract_range, end_contract_range, bucket) -> pd.DataFrame
Purpose: Compute hour-weights for each contract month within a bucket.

**Algorithm:**
1. Get region from BBID or asset parameters
2. Get contract range dates and NERC holidays
3. For each component bucket: filter dates, count hours per contract month
4. Return concatenated weights DataFrame with columns [contract, weight, quantityBucket]

---

### _filter_by_bucket(df, bucket, holidays, region) -> pd.DataFrame
Purpose: Filter hourly data to a specific power bucket.

**Algorithm:**
1. Branch: 7x24 -> pass (no filter)
2. Branch: offpeak -> holidays, weekends, or off-peak hours
3. Branch: peak -> weekdays, non-holidays, peak hours
4. Branch: 7x8 -> hours outside peak (11pm-7am)
5. Branch: 2x16h / suh1x16 -> weekends/holidays during peak hours
6. Else -> raise `MqValueError`

---

### _string_to_date_interval(interval: str) -> Union[dict, str]
Purpose: Parse a commodity contract interval string into start/end dates.

**Algorithm:**
1. Extract year from last 2 or 4 digits
2. Branch: single letter + year -> month code (F=Jan, G=Feb, ...)
3. Branch: "Cal" prefix or numeric year -> full year
4. Branch: digit + Q -> quarter
5. Branch: digit + H -> half-year
6. Branch: month name/abbreviation -> that month
7. Returns dict with start_date/end_date or error string

---

### _merge_curves_by_weighted_average(forwards_data, weights, keys, measure_column) -> ExtendedSeries
Purpose: Merge pricing data with weights to compute weighted average curve.

**Algorithm:**
1. Create Cartesian product of dates and weights
2. Left join with forwards data
3. Compute weighted_price = weight * measure
4. Filter out dates with any null values
5. Group by dates, sum weights and weighted_prices
6. Final price = sum(weighted_price) / sum(weight)

---

### _weighted_average_valuation_curve_for_calendar_strip(asset, contract_range, query_type, measure_field) -> ExtendedSeries
Purpose: Compute weighted average valuation curve for a commodity calendar strip.

**Algorithm:**
1. Parse contract range to start/end dates
2. Get contract range dates, compute weights
3. Query data for all contracts
4. Merge by weighted average

---

### fair_price(asset, tenor=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), None, [QueryType.FAIR_PRICE])`

Purpose: Fair price for commodity swap instruments.

**Algorithm:**
1. Branch: asset type is INDEX:
   - Branch: tenor is None -> raise `MqValueError`
   - Delegate to `_weighted_average_valuation_curve_for_calendar_strip`
2. Else -> direct query for FAIR_PRICE

---

### forward_price(asset, price_method='LMP', bucket='PEAK', contract_range='F20', *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Index, AssetType.CommodityPowerAggregatedNodes, AssetType.CommodityPowerNode), [QueryType.FORWARD_PRICE])`

Purpose: US power forward prices.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Delegate to `_forward_price_elec`

---

### _forward_price_elec(asset, price_method, bucket, contract_range, *, source, real_time) -> pd.Series
Purpose: US electricity forward price with bucket-weighted averaging.

**Algorithm:**
1. Parse contract range, get bucket weights
2. Query FORWARD_PRICE with priceMethod, quantityBucket, contract
3. Branch: empty -> retry with original case priceMethod
4. Branch: still empty -> empty series
5. Else -> merge by weighted average (keys: quantityBucket, contract, dates)

---

### implied_volatility_elec(asset, price_method, bucket, contract_range, *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Index, ..., AssetType.CommodityPowerNode), [QueryType.IMPLIED_VOLATILITY])`

Purpose: US electricity implied volatility with bucket weighting.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Parse contract range, get bucket weights
3. Query from Dataset 'COMMOD_US_ELEC_ENERGY_IMPLIED_VOLATILITY'
4. Merge by weighted average

---

### eu_ng_hub_to_swap(asset_spec: ASSET_SPEC) -> str
Purpose: Map EU natural gas hub to its ICE swap instrument.

**Algorithm:**
1. Look up ICE commodity reference price for asset
2. Branch: asset is Commod and EU NG hub type -> search for instruments
3. Catch IndexError -> fall back to asset's own Marquee ID

---

### forward_price_ng(asset, contract_range='F20', price_method='GDD', *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.CommodityNaturalGasHub, AssetType.CommodityEUNaturalGasHub), [MeasureDependency(...)], display_name='forward_price')`

Purpose: Natural gas forward prices (US and EU).

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Branch: US NG hub -> delegate to `_forward_price_natgas`
3. Branch: EU NG hub -> default GDD to ICE, delegate to `_forward_price_eu_natgas`
4. Else -> raise `MqTypeError`

---

### _forward_price_natgas(asset, price_method, contract_range, *, source, real_time) -> pd.Series
Purpose: US natural gas forward price with contract-weighted averaging.

---

### get_contract_range(start_contract_range, end_contract_range, timezone) -> pd.DataFrame
Purpose: Generate hourly or daily date range DataFrame with contract month labels.

**Algorithm:**
1. Branch: timezone provided -> hourly range with hour/day columns
2. Branch: no timezone -> daily range
3. Add date, month, year, contract_month columns

---

### bucketize_price(asset, price_method, bucket='7x24', granularity='daily', *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Index, ..., AssetType.CommodityPowerNode), [QueryType.PRICE])`

Purpose: Bucketized commodity electricity energy prices.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Branch: granularity daily/d -> 'D'; monthly/m -> FREQ_MONTH_END; else -> raise `MqValueError`
3. Get region, timezone from BBID
4. Convert start/end to UTC
5. Query PRICE real-time data
6. Branch: empty first try -> retry with original case priceMethod
7. Convert to local timezone, add time columns
8. Remove duplicates, compute frequency, check for missing data
9. Drop dates/months with missing data
10. Filter by bucket, resample by granularity with mean

---

### dividend_yield(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Dividend yield fundamental metric.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Build FUNDAMENTAL_METRIC query with metric="Dividend Yield"
3. Set vendor to "Goldman Sachs"

---

### earnings_per_share(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket, AssetType.Equity_Basket, AssetType.ETF, AssetType.Index), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Earnings per share fundamental metric.

**Algorithm:** Same pattern as dividend_yield with metric="Earnings per Share".

---

### earnings_per_share_positive(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Earnings per share positive fundamental metric.

**Algorithm:** Same pattern with metric="Earnings per Share Positive".

---

### net_debt_to_ebitda(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Net Debt to EBITDA fundamental metric.

**Algorithm:** Same pattern with metric="Net Debt to EBITDA".

---

### price_to_book(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Book fundamental metric.

---

### price_to_cash(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Cash fundamental metric.

---

### price_to_earnings(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Earnings fundamental metric.

---

### price_to_earnings_positive(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Earnings Positive fundamental metric.

---

### price_to_earnings_positive_exclusive(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Earnings Positive Exclusive (excluding negative EPS stocks).

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Delegates to `_fundamentals_md_query` with `DataMeasure.PRICE_TO_EARNINGS_POSITIVE_EXCLUSIVE.value`

---

### price_to_sales(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Price to Sales fundamental metric.

---

### return_on_equity(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Return on Equity fundamental metric.

---

### sales_per_share(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Sales per Share fundamental metric.

---

### current_constituents_dividend_yield(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents dividend yield for flagship baskets.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: not flagship basket -> raise `NotImplementedError`
3. Delegate to `_fundamentals_md_query`

---

### current_constituents_earnings_per_share(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents EPS for flagship baskets.

**Algorithm:** Same as current_constituents_dividend_yield with different metric.

---

### current_constituents_earnings_per_share_positive(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents EPS positive for flagship baskets.

---

### current_constituents_net_debt_to_ebitda(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Net Debt to EBITDA for flagship baskets.

---

### current_constituents_price_to_book(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Price to Book for flagship baskets.

---

### current_constituents_price_to_cash(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Price to Cash for flagship baskets.

---

### current_constituents_price_to_earnings(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Price to Earnings for flagship baskets.

---

### current_constituents_price_to_earnings_positive(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Price to Earnings Positive for flagship baskets.

---

### current_constituents_price_to_sales(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Price to Sales for flagship baskets.

---

### current_constituents_return_on_equity(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Return on Equity for flagship baskets.

---

### current_constituents_sales_per_share(asset, period, period_direction, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Research_Basket, AssetType.Custom_Basket), [QueryType.FUNDAMENTAL_METRIC])`

Purpose: Current constituents Sales per Share for flagship baskets.

---

All `current_constituents_*` functions share the same pattern:
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: `asset.get_entity().get('parameters', {'flagship': False}).get('flagship', False)` is False -> raise `NotImplementedError`
3. Delegate to `_fundamentals_md_query` with the appropriate `DataMeasure` metric

---

### realized_correlation(asset, tenor, top_n_of_index=None, composition_date=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.ETF, AssetType.Custom_Basket, AssetType.Research_Basket), [QueryType.REALIZED_CORRELATION])`

Purpose: Realized correlation of an equity index, optionally for top N constituents.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: no top_n:
   - composition_date provided -> raise `MqValueError`
   - Basket type -> raise `MqValueError`
3. Branch: top_n > 100 -> raise `MqValueError`
4. Branch: no top_n -> direct query REALIZED_CORRELATION
5. Branch: top_n provided:
   - Get constituent weights
   - Extend start date for tenor lookback
   - Query SPOT for all assets
   - Optionally append last data
   - Compute constituent vols with Window(tenor, tenor)
   - Assert sufficient history
   - Compute weighted vols s1, s2
   - Compute index vol
   - Correlation = (idx_vol^2 - s2) / (s1^2 - s2) * 100

---

### realized_volatility(asset, w=Window(None,0), returns_type=LOGARITHMIC, pricing_location=None, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod, AssetClass.Equity, AssetClass.FX, AssetClass.Credit, AssetClass.Rates), None, [QueryType.SPOT], asset_type_excluded=(AssetType.CommodityEUNaturalGasHub, AssetType.CommodityNaturalGasHub))`

Purpose: Realized volatility for an asset.

**Algorithm:**
1. Branch: not FX or real_time -> standard SPOT query
2. Branch: FX and not real_time -> add pricingLocation (default NYC) to query
3. Fetch via `get_historical_and_last_for_measure`
4. Remove duplicate indices, compute volatility
5. Branch: empty -> empty series

---

### esg_headline_metric(asset, metricName=ES_AGGREGATE_SCORE, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), None, [QueryType.ES_SCORE])`

Purpose: ESG scores and percentiles.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Camelize metric name
3. Look up QueryType in ESG_METRIC_TO_QUERY_TYPE
4. Query, extract column by camelCase name

---

### rating(asset, metric=RATING, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Single_Stock,), [QueryType.RATING])`

Purpose: Analyst rating (Buy/Neutral/Sell) or conviction list membership.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Look up QueryType from metric
3. Branch: QueryType.RATING -> replace 'Buy'->1, 'Sell'->-1, 'Neutral'->0

---

### fair_value(asset, metric=GSDEER, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Cross,), [MeasureDependency(id_provider=cross_to_usd_based_cross, query_type=QueryType.FAIR_VALUE)])`

Purpose: GSDEER/GSFEER equilibrium exchange rate estimates.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Get USD-based cross, query Dataset 'GSDEER_GSFEER'
3. Take latest date, convert quarter labels to dates
4. Branch: cross != usd_based -> invert series

---

### factor_profile(asset, metric=GROWTH_SCORE, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Single_Stock,), [QueryType.GROWTH_SCORE])`

Purpose: GS Factor Profile percentiles (growth, financial returns, multiple, integrated).

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Look up QueryType, query, extract

---

### commodity_forecast(asset, forecastPeriod="3m", forecastType=SPOT_RETURN, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Commodity, AssetType.Index), [QueryType.COMMODITY_FORECAST])`

Purpose: Short and long-term commodity forecasts.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Query COMMODITY_FORECAST with forecastPeriod and forecastType
3. Extract series

---

### commodity_forecast_time_series(asset, forecastFrequency=ANNUAL, forecastType=SPOT, forecastHorizonYears=12, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Commodity, AssetType.Index), [QueryType.COMMODITY_FORECAST])`

Purpose: Commodity forecast time series across multiple periods.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: asset is str vs Asset vs other
3. Branch: forecastFrequency is Enum -> extract value
4. Generate list of periods based on frequency:
   - SHORT_TERM: ["3m", "6m", "12m"]
   - MONTHLY: "{year}M{month}" for each month
   - QUARTERLY: "{year}Q{q}" for each quarter
   - ANNUAL: "{year}" for each year
   - Else -> raise ValueError
5. For each period: query, take last value, compute start-of-period date
   - Branch: 'm' in period -> months offset
   - Branch: 'Q' in period -> quarter start
   - Branch: 'M' in period -> month start
   - Else -> year start
6. Build DataFrame, extract series

---

### _get_marketdate_validation(market_date, start_date, end_date, timezone=None)
Purpose: Validate and normalize market_date for forward curves.

**Algorithm:**
1. Branch: not string -> raise `MqTypeError`
2. Branch: empty string -> use today (skip weekends)
3. Else -> parse YYYYMMDD; Branch: invalid format -> raise `MqValueError`
4. Branch: timezone -> use timezone-aware today
5. Branch: future date -> raise `MqValueError`
6. Branch: weekend -> raise `MqValueError`
7. Branch: > end_date -> raise `MqValueError`
8. Branch: > start_date -> adjust start_date

---

### forward_curve(asset, bucket='PEAK', market_date="", *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.Index, ..., AssetType.CommodityPowerNode), [QueryType.FORWARD_PRICE])`

Purpose: US power forward curve for a given market date.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Validate market date with timezone
3. Check dataset for last available date; Branch: market_date > last_date -> use last_date
4. Get bucket weights, query FORWARD_PRICE for contracts
5. Branch: empty -> return empty
6. Map contracts to start dates
7. Branch: combo bucket -> weighted average across quantity buckets

---

### forward_curve_ng(asset, price_method='GDD', market_date="", *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.CommodityNaturalGasHub,), [QueryType.FORWARD_PRICE], display_name='forward_curve')`

Purpose: US natural gas forward curve.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Validate market date
3. Check dataset for last available date
4. Get contract range, query with priceMethod
5. Branch: empty -> return empty
6. Map contracts to dates, create series

---

### _forward_price_eu_natgas(asset, contract_range, price_method, *, source, real_time) -> pd.Series
Purpose: EU natural gas forward prices.

**Algorithm:**
1. Get contract weights
2. Map price_method to EUNatGasDataReference
3. For each contract: find instrument by commodity reference price
4. Query FORWARD_PRICE for all assets
5. Merge by weighted average

---

### fx_implied_correlation(asset, asset_2, tenor, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.FX,), None, [QueryType.IMPLIED_VOLATILITY])`

Purpose: FX implied correlation between two crosses.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Branch: asset_2 not FX -> raise `MqValueError`
3. Get BBIDs for both crosses
4. Branch: crosses have no common currency -> raise `MqValueError`
5. Determine the third (correlation) cross from the non-common currencies
6. Get stored directions, compute invert_factor for sign
7. Query IMPLIED_VOLATILITY for all 3 crosses with delta=0 strike
8. Compute correlation: `invert_factor * (v1^2 + v2^2 - v3^2) / (2*v1*v2)`

---

### get_last_for_measure(asset_ids, query_type, where, *, source, request_id, ignore_errors=False)
Purpose: Get the latest real-time "Last" data point for a measure.

**Algorithm:**
1. Build "Last" measure query for today +/- 2 days
2. Try: fetch data
3. Catch Exception -> log warning, return None
4. Branch: empty -> log warning, return None
5. Convert timezone, normalize index, return

---

### merge_dataframes(dataframes: List[pd.DataFrame]) -> pd.DataFrame
Purpose: Merge multiple DataFrames, dedup, and collect dataset_ids.

**Algorithm:**
1. Branch: None -> return empty
2. Concat all frames
3. Add dummy column for dedup (drop_duplicates ignores index)
4. Sort by mergesort for stability
5. Collect all dataset_ids from source frames

---

### append_last_for_measure(df, asset_ids, query_type, where, *, source, request_id) -> pd.DataFrame
Purpose: Append latest real-time data to a historical DataFrame.

**Algorithm:**
1. Call `get_last_for_measure`
2. Branch: None -> return original df
3. Concat, dedup, merge dataset_ids

---

### get_market_data_tasks(asset_ids, query_type, where, *, source, real_time, request_id, chunk_size=5, ignore_errors=False, parallelize_queries=False) -> list
Purpose: Create a list of partial functions for parallel market data fetching, chunked by asset.

**Algorithm:**
1. Chunk asset_ids by chunk_size
2. For each chunk: build query (may return list if parallelize_queries)
3. Create partial(_market_data_timed, query, ...) for each

---

### get_historical_and_last_for_measure(asset_ids, query_type, where, *, source, real_time, request_id, chunk_size=5, ignore_errors=False, parallelize_queries=False) -> pd.DataFrame
Purpose: Get historical data plus latest real-time data, merged.

**Algorithm:**
1. Build market data tasks (chunked, possibly parallelized)
2. Branch: not real_time and end_date >= today -> add "Last" tasks for each where condition
3. Run all tasks via `ThreadPoolManager.run_async`
4. Merge all result DataFrames

---

### settlement_price(asset, contract='F22', *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Commod,), (AssetType.FutureMarket,), [QueryType.SETTLEMENT_PRICE])`

Purpose: Exchange settlement price for commodity FutureMarket.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Determine dataset from asset parameters:
   - Branch: ICE + Physical Environment -> CARBON_CREDIT_DATASET
   - Branch: EEX/ICE/NASDAQ + PowerFutures -> EU_POWER_EXCHANGE_DATASET
   - Else -> raise `MqTypeError`
3. Branch: no parameters -> raise `MqTypeError`
4. Catch TypeError/ValueError/KeyError -> raise `MqTypeError`
5. Query dataset, return series

---

### hloc_prices(asset, interval_frequency=DAILY, *, source, real_time) -> pd.DataFrame
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Index, AssetType.Single_Stock), [QueryType.SPOT])`

Purpose: High/Low/Open/Close prices.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Return `asset.get_hloc_prices(start, end, interval_frequency)`

---

### thematic_model_exposure(asset, basket_identifier, notional=10000000, *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Custom_Basket, ..., AssetType.ETF), [QueryType.THEMATIC_MODEL_BETA])`

Purpose: Thematic exposure of an asset to a GS flagship basket.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Call `PositionedEntity.get_thematic_exposure`, extract THEMATIC_EXPOSURE

---

### thematic_model_beta(asset, basket_identifier, *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Custom_Basket, ..., AssetType.Single_Stock), [QueryType.THEMATIC_MODEL_BETA])`

Purpose: Thematic beta of an asset to a GS flagship basket.

**Algorithm:**
1. Branch: real_time -> raise `MqValueError`
2. Branch: asset type is Single_Stock -> use `Stock.get_thematic_beta`
3. Else -> use `PositionedEntity.get_thematic_beta`
4. Extract THEMATIC_BETA

---

### retail_interest_agg(asset, measure=RETAIL_PCT_SHARES, data_source=ALL, sector=ALL, *, source, real_time) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Custom_Basket, ..., AssetType.ETF), [QueryType.SPOT])`

Purpose: Aggregated retail interest for equity assets with underliers.

**Algorithm:**
1. Branch: real_time -> raise `NotImplementedError`
2. Get underliers; Branch: empty -> raise `MqValueError`
3. Branch: sector != ALL -> filter underliers by GICS sector
4. Query Dataset RETAIL_FLOW_DAILY_V2_PREMIUM
5. Branch: empty -> empty series
6. Branch: 'Pct' in measure -> compute percentage from constituent sums
7. Else -> simple sum aggregation

---

### s3_long_short_concentration(asset, s3Metric=LONG_CROWDING, *, source, real_time, request_id) -> pd.Series
**Decorator:** `@plot_measure((AssetClass.Equity,), (AssetType.Custom_Basket, AssetType.Research_Basket))`

Purpose: S3 Partners long/short concentration metrics.

**Algorithm:**
1. Query Dataset 'S3_BASKETS_AGG' with assetId and s3Metric
2. Extract S3_AGGREGATE_DATA series

---

## State Mutation
- `ExtendedSeries.dataset_ids`: Set on virtually every returned series to track data provenance
- `ExtendedSeries.attrs['latest']`: Set by `vol_term` and `var_term` to record the selected pricing date
- Module-level: `_var_swap_tenors` is TTL-cached, so repeated calls use in-memory cache
- No global mutable state beyond the cache and logger

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `skew`, `implied_vol` (FX) | real-time not supported for asset class |
| `MqValueError` | `_preprocess_implied_vol_strikes_fx` | Invalid strike/reference combinations for FX |
| `MqValueError` | `implied_correlation`, `average_implied_volatility`, `realized_correlation` | Invalid top_n_of_index or missing composition data |
| `MqValueError` | `skew`, `_skew` | < 3 curves returned (insufficient data) |
| `MqValueError` | `convert_asset_for_rates_data_set` | Unable to map identifier |
| `MqValueError` | `_get_index_constituent_weights` | No positions data |
| `MqValueError` | `_check_top_n` | top_n is not numeric |
| `MqValueError` | `forward_price`, `implied_volatility_ng`, `bucketize_price`, etc. | real_time=True on daily-only measures |
| `MqValueError` | `_filter_by_bucket` | Invalid bucket name |
| `MqValueError` | `_get_marketdate_validation` | Future date, weekend, format error, range error |
| `MqValueError` | `fair_price` | INDEX type without tenor |
| `MqValueError` | `average_implied_volatility` | Missing constituent vols exceeding weight threshold |
| `MqValueError` | `_var_swap_tenors` | var swap not available for asset |
| `MqTypeError` | `skew` | Missing strike reference for equities |
| `MqTypeError` | `var_swap` | forward_start_date is not a string |
| `MqTypeError` | `var_term` | pricing_date is wrong type |
| `MqTypeError` | `_get_marketdate_validation` | market_date is not a string |
| `MqTypeError` | `forward_price_ng` | Unsupported asset type |
| `MqTypeError` | `settlement_price` | Missing/unsupported parameters on asset |
| `NotImplementedError` | Most `@plot_measure` functions | real_time=True when not supported |
| `NotImplementedError` | `cds_spread` | Unsupported spread value |
| `NotImplementedError` | Credit option functions | Unsupported option type |
| `NotImplementedError` | `average_implied_volatility` | top_n > 100 |
| `NotImplementedError` | `current_constituents_*` | Non-flagship basket |
| `TypeError` | `_cross_stored_direction_helper` | bbid not a valid 6-char cross |
| `ValueError` | `fx_forecast_time_series`, `commodity_forecast_time_series` | Invalid asset type or forecastFrequency |

## Edge Cases
- **Empty DataFrames:** Every measure function handles empty query results by returning an empty `ExtendedSeries(dtype=float)` with `dataset_ids` set.
- **Duplicate index entries:** Several functions call `~df.index.duplicated(keep='first')` or `keep='last'` to handle duplicate timestamps.
- **Missing constituents:** `average_implied_volatility` with `top_n_of_index` raises if any constituent is missing vol data (unless `weight_threshold` is set to tolerate some missing weight).
- **FX cross inversion:** Functions like `fx_forecast` and `fair_value` invert the series (1/series) when the USD-based cross differs from the input cross.
- **Normalization mode defaults:** `skew` and `skew_term` default to OUTRIGHT for FX and NORMALIZED for equity/commod.
- **Forward vol edge cases:** If one or both tenor groups are missing from grouped data, returns empty series instead of raising.
- **Weekend market dates:** `_get_marketdate_validation` steps back to the previous weekday for empty market_date, but raises if a weekend date is explicitly provided.
- **`_string_to_date_interval` error strings:** Returns error strings (not exceptions) for invalid intervals; callers must check `isinstance(result, str)` and raise `MqValueError`.
- **`_forward_price_elec` price_method fallback:** Tries uppercase first, then original case if no data returned.
- **`var_swap` missing tenors:** If requested tenors are not in `_var_swap_tenors`, returns empty series instead of raising.
- **`settlement_price` dataset selection:** Complex branching on exchange and product to pick the correct dataset; any missing parameters raise `MqTypeError`.
- **Intraday data fallback:** `vol_term`, `var_term`, `skew_term` all try intraday data first for recent dates, falling back to historical if intraday is empty.

## Coverage Notes
- Branch count: Very high (estimated 200+ branches across 70+ functions)
- Common pattern: Nearly every `@plot_measure` function has an early `if real_time: raise NotImplementedError/MqValueError` branch -- these are trivial to cover but each is a distinct branch
- The `_string_to_date_interval` function has deep branching (~15 branches) for different date interval formats
- `skew` function has ~12 branches covering FX vs equity, delta vs normalized vs spot/forward, normalization modes
- `convert_asset_for_rates_data_set` has 6 branches on conversion type plus key lookup error handling
- `bucketize_price` has ~10 branches for granularity, bucket type, missing data handling
- `implied_volatility` has 3 top-level branches (Commod non-ETF, FX, else)
- `average_implied_volatility` has complex nested branching for top_n vs no-top_n paths, missing asset handling, weight thresholds
- `realized_correlation` has separate code paths for direct query vs top-N constituent calculation
- Forward curve functions (`forward_curve`, `forward_curve_ng`) have market date validation branches
- All `current_constituents_*` functions duplicate the flagship check branch (10 functions)
- `_filter_by_bucket` has 6 branches for different power bucket types
- Pragmas: None observed in source
