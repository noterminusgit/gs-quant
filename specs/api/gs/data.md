# data.py

## Summary
GS-specific implementation of the `DataApi` interface for querying, managing, and caching market data and dataset operations against Goldman Sachs internal data services. This is the largest API file in the codebase (~1522 LOC), providing coordinate-based market data retrieval, dataset CRUD operations, MxAPI curve/measure/backtest queries, coverage enumeration, catalog access, and typed DataFrame construction. Also defines the `QueryType` enum (140+ market data measure types) and the `MarketDataResponseFrame` (a pandas DataFrame subclass that preserves `dataset_ids` metadata through operations).

## Dependencies
- Internal:
  - `gs_quant.api.data` (`DataApi` -- abstract base class)
  - `gs_quant.base` (`Base`)
  - `gs_quant.common` (`MarketDataVendor`, `PricingLocation`, `Format`)
  - `gs_quant.data.core` (`DataContext`, `DataFrequency`)
  - `gs_quant.data.log` (`log_debug`, `log_warning`)
  - `gs_quant.errors` (`MqValueError`)
  - `gs_quant.json_encoder` (`JSONEncoder`)
  - `gs_quant.markets` (`MarketDataCoordinate`)
  - `gs_quant.target.coordinates` (`MDAPIDataBatchResponse`, `MDAPIDataQuery`, `MDAPIDataQueryResponse`, `MDAPIQueryField`)
  - `gs_quant.target.data` (`DataQuery`, `DataQueryResponse`, `DataSetCatalogEntry`, `DataSetEntity`, `DataSetFieldEntity`)
  - `gs_quant.api.gs.assets` (`GsIdType`)
  - `gs_quant.api.api_cache` (`ApiRequestCache`)
  - `gs_quant.target.assets` (`EntityQuery`, `FieldFilterMap`)
- External:
  - `asyncio` (gather)
  - `datetime` (date, datetime, timedelta)
  - `json` (dumps, loads)
  - `logging` (getLogger)
  - `time` (perf_counter)
  - `copy` (copy, deepcopy)
  - `enum` (Enum)
  - `itertools` (chain)
  - `typing` (Iterable, List, Optional, Tuple, Union, Dict)
  - `cachetools` (cached, TTLCache)
  - `pandas` (DataFrame, Series, DatetimeIndex, to_datetime, to_json, concat)
  - `dateutil.parser` (parse)
  - `pydash` (get)

## Type Definitions

### QueryType (Enum)
Enumeration of 140+ market data measure types. Each member's value is a human-readable string used as the `queryType` field in market data API requests.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| IMPLIED_VOLATILITY | `str` | `"Implied Volatility"` | Implied vol measure |
| IMPLIED_VOLATILITY_BY_EXPIRATION | `str` | `"Implied Volatility By Expiration"` | Implied vol by expiry |
| IMPLIED_CORRELATION | `str` | `"Implied Correlation"` | Implied correlation |
| REALIZED_CORRELATION | `str` | `"Realized Correlation"` | Realized correlation |
| AVERAGE_IMPLIED_VOLATILITY | `str` | `"Average Implied Volatility"` | Average implied vol |
| AVERAGE_IMPLIED_VARIANCE | `str` | `"Average Implied Variance"` | Average implied variance |
| AVERAGE_REALIZED_VOLATILITY | `str` | `"Average Realized Volatility"` | Average realized vol |
| SWAP_RATE | `str` | `"Swap Rate"` | Interest rate swap rate |
| SWAP_ANNUITY | `str` | `"Swap Annuity"` | Swap annuity |
| SWAPTION_PREMIUM | `str` | `"Swaption Premium"` | Swaption premium |
| SWAPTION_ANNUITY | `str` | `"Swaption Annuity"` | Swaption annuity |
| BASIS_SWAP_RATE | `str` | `"Basis Swap Rate"` | Basis swap rate |
| XCCY_SWAP_SPREAD | `str` | `"Xccy Swap Spread"` | Cross-currency swap spread |
| SWAPTION_VOL | `str` | `"Swaption Vol"` | Swaption volatility |
| MIDCURVE_VOL | `str` | `"Midcurve Vol"` | Midcurve volatility |
| CAP_FLOOR_VOL | `str` | `"Cap Floor Vol"` | Cap/floor volatility |
| SPREAD_OPTION_VOL | `str` | `"Spread Option Vol"` | Spread option vol |
| INFLATION_SWAP_RATE | `str` | `"Inflation Swap Rate"` | Inflation swap rate |
| FORWARD | `str` | `"Forward"` | Forward rate |
| PRICE | `str` | `"Price"` | Price |
| ATM_FWD_RATE | `str` | `"Atm Fwd Rate"` | At-the-money forward rate |
| BASIS | `str` | `"Basis"` | Basis |
| VAR_SWAP | `str` | `"Var Swap"` | Variance swap |
| MIDCURVE_PREMIUM | `str` | `"Midcurve Premium"` | Midcurve premium |
| MIDCURVE_ANNUITY | `str` | `"Midcurve Annuity"` | Midcurve annuity |
| MIDCURVE_ATM_FWD_RATE | `str` | `"Midcurve Atm Fwd Rate"` | Midcurve ATM fwd rate |
| CAP_FLOOR_ATM_FWD_RATE | `str` | `"Cap Floor Atm Fwd Rate"` | Cap/floor ATM fwd rate |
| SPREAD_OPTION_ATM_FWD_RATE | `str` | `"Spread Option Atm Fwd Rate"` | Spread option ATM fwd rate |
| FORECAST | `str` | `"Forecast"` | Forecast |
| IMPLIED_VOLATILITY_BY_DELTA_STRIKE | `str` | `"Implied Volatility By Delta Strike"` | Implied vol by delta strike |
| FUNDAMENTAL_METRIC | `str` | `"Fundamental Metric"` | Fundamental metric |
| POLICY_RATE_EXPECTATION | `str` | `"Policy Rate Expectation"` | Policy rate expectation |
| CENTRAL_BANK_SWAP_RATE | `str` | `"Central Bank Swap Rate"` | Central bank swap rate |
| FORWARD_PRICE | `str` | `"Forward Price"` | Forward price |
| FAIR_PRICE | `str` | `"Fair Price"` | Fair price |
| PNL | `str` | `"Pnl"` | Profit and loss |
| SPOT | `str` | `"Spot"` | Spot rate |
| AUM | `str` | `"Aum"` | Assets under management |
| RATE | `str` | `"Rate"` | Rate |
| ES_NUMERIC_SCORE | `str` | `"Es Numeric Score"` | ESG numeric score |
| ES_NUMERIC_PERCENTILE | `str` | `"Es Numeric Percentile"` | ESG numeric percentile |
| ES_POLICY_SCORE | `str` | `"Es Policy Score"` | ESG policy score |
| ES_POLICY_PERCENTILE | `str` | `"Es Policy Percentile"` | ESG policy percentile |
| ES_SCORE | `str` | `"Es Score"` | ESG score |
| ES_PERCENTILE | `str` | `"Es Percentile"` | ESG percentile |
| ES_PRODUCT_IMPACT_SCORE | `str` | `"Es Product Impact Score"` | ESG product impact score |
| ES_PRODUCT_IMPACT_PERCENTILE | `str` | `"Es Product Impact Percentile"` | ESG product impact percentile |
| G_SCORE | `str` | `"G Score"` | Governance score |
| G_PERCENTILE | `str` | `"G Percentile"` | Governance percentile |
| ES_MOMENTUM_SCORE | `str` | `"Es Momentum Score"` | ESG momentum score |
| ES_MOMENTUM_PERCENTILE | `str` | `"Es Momentum Percentile"` | ESG momentum percentile |
| G_REGIONAL_SCORE | `str` | `"G Regional Score"` | Governance regional score |
| G_REGIONAL_PERCENTILE | `str` | `"G Regional Percentile"` | Governance regional percentile |
| ES_DISCLOSURE_PERCENTAGE | `str` | `"Es Disclosure Percentage"` | ESG disclosure percentage |
| CONTROVERSY_SCORE | `str` | `"Controversy Score"` | Controversy score |
| CONTROVERSY_PERCENTILE | `str` | `"Controversy Percentile"` | Controversy percentile |
| RATING | `str` | `"Rating"` | Rating |
| CONVICTION_LIST | `str` | `"Conviction List"` | Conviction list |
| FAIR_VALUE | `str` | `"Fair Value"` | Fair value |
| FX_FORECAST | `str` | `"Fx Forecast"` | FX forecast |
| GROWTH_SCORE | `str` | `"Growth Score"` | Growth score |
| FINANCIAL_RETURNS_SCORE | `str` | `"Financial Returns Score"` | Financial returns score |
| MULTIPLE_SCORE | `str` | `"Multiple Score"` | Multiple score |
| INTEGRATED_SCORE | `str` | `"Integrated Score"` | Integrated score |
| COMMODITY_FORECAST | `str` | `"Commodity Forecast"` | Commodity forecast |
| FORECAST_VALUE | `str` | `"Forecast Value"` | Forecast value |
| FORWARD_POINT | `str` | `"Forward Point"` | Forward point |
| FCI | `str` | `"Fci"` | Financial conditions index |
| LONG_RATES_CONTRIBUTION | `str` | `"Long Rates Contribution"` | Long rates contribution to FCI |
| SHORT_RATES_CONTRIBUTION | `str` | `"Short Rates Contribution"` | Short rates contribution to FCI |
| CORPORATE_SPREAD_CONTRIBUTION | `str` | `"Corporate Spread Contribution"` | Corporate spread contribution |
| SOVEREIGN_SPREAD_CONTRIBUTION | `str` | `"Sovereign Spread Contribution"` | Sovereign spread contribution |
| EQUITIES_CONTRIBUTION | `str` | `"Equities Contribution"` | Equities contribution |
| REAL_LONG_RATES_CONTRIBUTION | `str` | `"Real Long Rates Contribution"` | Real long rates contribution |
| REAL_SHORT_RATES_CONTRIBUTION | `str` | `"Real Short Rates Contribution"` | Real short rates contribution |
| REAL_FCI | `str` | `"Real Fci"` | Real FCI |
| DWI_CONTRIBUTION | `str` | `"Dwi Contribution"` | DWI contribution |
| REAL_TWI_CONTRIBUTION | `str` | `"Real Twi Contribution"` | Real TWI contribution |
| TWI_CONTRIBUTION | `str` | `"Twi Contribution"` | TWI contribution |
| COVARIANCE | `str` | `"Covariance"` | Covariance |
| FACTOR_EXPOSURE | `str` | `"Factor Exposure"` | Factor exposure |
| FACTOR_RETURN | `str` | `"Factor Return"` | Factor return |
| HISTORICAL_BETA | `str` | `"Historical Beta"` | Historical beta |
| FACTOR_PNL | `str` | `"Factor Pnl"` | Factor PnL |
| FACTOR_PROPORTION_OF_RISK | `str` | `"Factor Proportion Of Risk"` | Factor proportion of risk |
| DAILY_RISK | `str` | `"Daily Risk"` | Daily risk |
| ANNUAL_RISK | `str` | `"Annual Risk"` | Annual risk |
| VOLATILITY | `str` | `"Volatility"` | Volatility |
| CORRELATION | `str` | `"Correlation"` | Correlation |
| OIS_XCCY | `str` | `"Ois Xccy"` | OIS cross-currency |
| OIS_XCCY_EX_SPIKE | `str` | `"Ois Xccy Ex Spike"` | OIS cross-currency ex spike |
| USD_OIS | `str` | `"Usd Ois"` | USD OIS |
| NON_USD_OIS | `str` | `"Non Usd Ois"` | Non-USD OIS |
| SETTLEMENT_PRICE | `str` | `"Settlement Price"` | Settlement price |
| THEMATIC_EXPOSURE | `str` | `"Thematic Exposure"` | Thematic exposure |
| THEMATIC_BETA | `str` | `"Thematic Beta"` | Thematic beta |
| THEMATIC_MODEL_BETA | `str` | `"Thematic Model Beta"` | Thematic model beta |
| CDS_SPREAD_100 | `str` | `"Spread At100"` | CDS spread at 100 |
| CDS_SPREAD_250 | `str` | `"Spread At250"` | CDS spread at 250 |
| CDS_SPREAD_500 | `str` | `"Spread At500"` | CDS spread at 500 |
| STRIKE_VOL | `str` | `"Strike Vol"` | Strike volatility |
| OPTION_PREMIUM | `str` | `"Option Premium"` | Option premium |
| ABSOLUTE_STRIKE | `str` | `"Absolute Strike"` | Absolute strike |
| RETAIL_PCT_SHARES | `str` | `"impliedRetailPctShares"` | Implied retail pct shares |
| RETAIL_PCT_NOTIONAL | `str` | `"impliedRetailPctNotional"` | Implied retail pct notional |
| RETAIL_SHARES | `str` | `"impliedRetailShares"` | Implied retail shares |
| RETAIL_NOTIONAL | `str` | `"impliedRetailNotional"` | Implied retail notional |
| SHARES | `str` | `"shares"` | Shares |
| NOTIONAL | `str` | `"notional"` | Notional |
| RETAIL_BUY_NOTIONAL | `str` | `"impliedRetailBuyNotional"` | Implied retail buy notional |
| RETAIL_BUY_PCT_NOTIONAL | `str` | `"impliedRetailBuyPctNotional"` | Implied retail buy pct notional |
| RETAIL_BUY_PCT_SHARES | `str` | `"impliedRetailBuyPctShares"` | Implied retail buy pct shares |
| RETAIL_BUY_SHARES | `str` | `"impliedRetailBuyShares"` | Implied retail buy shares |
| RETAIL_SELL_NOTIONAL | `str` | `"impliedRetailSellNotional"` | Implied retail sell notional |
| RETAIL_SELL_PCT_NOTIONAL | `str` | `"impliedRetailSellPctNotional"` | Implied retail sell pct notional |
| RETAIL_SELL_PCT_SHARES | `str` | `"impliedRetailSellPctShares"` | Implied retail sell pct shares |
| RETAIL_SELL_SHARES | `str` | `"impliedRetailSellShares"` | Implied retail sell shares |
| FWD_POINTS | `str` | `"Fwd Points"` | Forward points |
| S3_AGGREGATE_DATA | `str` | `"value"` | S3 aggregate data value |
| HIT_RATE | `str` | `"Hit Rate"` | Hit rate |
| MAX_DRAWDOWN | `str` | `"Max Drawdown"` | Max drawdown |
| STANDARD_DEVIATION | `str` | `"Standard Deviation"` | Standard deviation |
| DOWNSIDE_RISK | `str` | `"Downside Risk"` | Downside risk |
| KURTOSIS | `str` | `"Kurtosis"` | Kurtosis |
| SKEWNESS | `str` | `"Skewness"` | Skewness |
| REALIZED_VAR | `str` | `"Realized VaR"` | Realized VaR |
| TRACKING_ERROR | `str` | `"Tracking Error"` | Tracking error |
| TRACKING_ERROR_BEAR | `str` | `"Tracking Error Bear"` | Tracking error (bear) |
| TRACKING_ERROR_BULL | `str` | `"Tracking Error Bull"` | Tracking error (bull) |
| SHARPE_RATIO | `str` | `"Sharpe Ratio"` | Sharpe ratio |
| CALMAR_RATIO | `str` | `"Calmar Ratio"` | Calmar ratio |
| SORTINO_RATIO | `str` | `"Sortino Ratio"` | Sortino ratio |
| INFORMATION_RATIO | `str` | `"Information Ratio"` | Information ratio |
| MODIGLIANI_RATIO | `str` | `"Modigliani Ratio"` | Modigliani ratio |
| TREYNOR_RATIO | `str` | `"Treynor Ratio"` | Treynor ratio |
| ALPHA | `str` | `"Alpha"` | Alpha |
| BETA | `str` | `"Beta"` | Beta |
| R_SQUARED | `str` | `"R Squared"` | R squared |
| CAPTURE_RATIO | `str` | `"Capture Ratio"` | Capture ratio |

### GsDataApi (class)
Inherits: `DataApi` (which inherits `ApiWithCustomSession`, `ABCMeta`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__definitions` | `dict` | `{}` | Class-level cache of dataset definitions by dataset_id |
| `__asset_coordinates_cache` | `TTLCache` | `TTLCache(10000, 86400)` | TTL cache for asset coordinates (10k items, 24h TTL) |
| `_api_request_cache` | `Optional[ApiRequestCache]` | `None` | Optional request-level cache for API responses |
| `DEFAULT_SCROLL` | `str` | `'30s'` | Default scroll timeout for paginated requests |

### MarketDataResponseFrame (class)
Inherits: `pd.DataFrame`

A pandas DataFrame subclass that preserves a `dataset_ids` attribute through DataFrame operations (slicing, copying, etc.).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_internal_names` | `list` | `pd.DataFrame._internal_names + ['dataset_ids']` | Extended internal names list |
| `_internal_names_set` | `set` | `set(_internal_names)` | Set version for fast lookup |
| `dataset_ids` | `tuple` | (not set by default) | Tuple of dataset IDs that contributed data to this frame |

## Enums and Constants

### QueryType(Enum)
See full table above in Type Definitions. Contains 140+ members mapping market data measure names to their API string representations.

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `_REQUEST_HEADERS` | `str` | `"request_headers"` | Key name for request headers in kwargs |

## Functions/Methods

### GsDataApi.set_api_request_cache(cache: ApiRequestCache) -> None
Purpose: Set the class-level API request cache instance.

**Algorithm:**
1. Assign `cache` to `cls._api_request_cache`

---

### GsDataApi._construct_cache_key(url, **kwargs) -> tuple
Purpose: Build a deterministic cache key from a URL and keyword arguments, serializing query objects and dates.

**Algorithm:**
1. Define `fallback_encoder(v)`: if `v` is a `dt.date`, return `v.isoformat()`; otherwise return `None`
2. Define `serialize_value(v)`:
   - Branch: if `v` is `MDAPIDataQuery` or `DataQuery` -> call `v.to_json(sort_keys=True, default=fallback_encoder)`
   - Branch: otherwise -> call `fallback_encoder(v)`; if result is truthy return it, else return `v` as-is
3. Build `json_kwargs` dict from `kwargs`, excluding the `_REQUEST_HEADERS` key, with each value serialized
4. Return tuple `(url, 'POST', json_kwargs)`

---

### GsDataApi._check_cache(url, **kwargs) -> Tuple[Any, Any, GsSession]
Purpose: Check if a cached response exists for the given URL and parameters.

**Algorithm:**
1. Get session via `cls.get_session()`
2. Initialize `cached_val = None`, `cache_key = None`
3. Branch: if `cls._api_request_cache` is set:
   - Construct `cache_key` via `_construct_cache_key`
   - Look up `cached_val` via `cls._api_request_cache.get(session, cache_key)`
4. Return `(cached_val, cache_key, session)`

---

### GsDataApi._post_with_cache_check(url, validator=lambda x: x, domain=None, **kwargs) -> Any
Purpose: POST to a URL with cache-through semantics -- return cached result if available, otherwise POST, validate, cache, and return.

**Algorithm:**
1. Call `_check_cache(url, **kwargs)` to get `(result, cache_key, session)`
2. Branch: if `result is None`:
   - Execute `validator(session.sync.post(url, domain=domain, **kwargs))`
   - Branch: if `cls._api_request_cache` is set -> store result in cache via `put(session, cache_key, result)`
3. Return result

---

### GsDataApi._get_with_cache_check(url, validator=lambda x: x, domain=None, **kwargs) -> Any
Purpose: GET from a URL with cache-through semantics.

**Algorithm:**
1. Call `_check_cache(url, **kwargs)` to get `(result, cache_key, session)`
2. Branch: if `result is None`:
   - Execute `validator(session.sync.get(url, domain=domain, **kwargs))`
   - Branch: if `cls._api_request_cache` is set -> store result in cache
3. Return result

---

### GsDataApi._get_with_cache_check_async(url, validator=lambda x: x, domain=None, **kwargs) -> Any
Purpose: Async GET from a URL with cache-through semantics.

**Algorithm:**
1. Call `_check_cache(url, **kwargs)` to get `(result, cache_key, session)`
2. Branch: if `result is None`:
   - `await session.async_.get(url, domain=domain, **kwargs)`
   - Apply `validator` to result
   - Branch: if `cls._api_request_cache` is set -> store result in cache
3. Return result

---

### GsDataApi._post_with_cache_check_async(url, validator=lambda x: x, domain=None, **kwargs) -> Any
Purpose: Async POST to a URL with cache-through semantics.

**Algorithm:**
1. Call `_check_cache(url, **kwargs)` to get `(result, cache_key, session)`
2. Branch: if `result is None`:
   - `await session.async_.post(url, domain=domain, **kwargs)`
   - Apply `validator` to result
   - Branch: if `cls._api_request_cache` is set -> store result in cache
3. Return result

---

### GsDataApi.query_data(query: Union[DataQuery, MDAPIDataQuery], dataset_id: str = None, asset_id_type: Union[GsIdType, str] = None) -> Union[MDAPIDataBatchResponse, DataQueryResponse, tuple, list]
Purpose: Execute a data query, dispatching to coordinate-based or dataset-based endpoints. Implements the `DataApi` interface.

**Algorithm:**
1. Branch: if `query` is `MDAPIDataQuery` AND `query.market_data_coordinates` is truthy:
   - Execute query against `'coordinates'` dataset via `cls.execute_query('coordinates', query)`
   - Branch: if `results` is dict -> return `results.get('responses', ())`
   - Branch: if `results` is object -> return `results.responses` if not None, else `()`
2. Otherwise: execute query against `dataset_id` via `cls.execute_query(dataset_id, query)`
3. Return `cls.get_results(dataset_id, response, query)`

---

### GsDataApi.query_data_async(query: Union[DataQuery, MDAPIDataQuery], dataset_id: str = None) -> Union[MDAPIDataBatchResponse, DataQueryResponse, tuple, list]
Purpose: Async version of `query_data`.

**Algorithm:**
1. Branch: if `query` is `MDAPIDataQuery` AND `query.market_data_coordinates` is truthy:
   - `await cls.execute_query_async('coordinates', query)`
   - Branch: if `results` is dict -> return `results.get('responses', ())`
   - Branch: if `results` is object -> return `results.responses` if not None, else `()`
2. Otherwise: `await cls.execute_query_async(dataset_id, query)`
3. `await cls.get_results_async(dataset_id, response, query)` and return

---

### GsDataApi.execute_query(dataset_id: str, query: Union[DataQuery, MDAPIDataQuery]) -> Any
Purpose: POST a data query to the `/data/{dataset_id}/query` endpoint, with optional MessagePack accept header and MDS domain redirect.

**Algorithm:**
1. Build `kwargs = {'payload': query}`
2. Branch: if `query.format` is `Format.MessagePack` or `'MessagePack'`:
   - Add `{'Accept': 'application/msgpack'}` to `kwargs[_REQUEST_HEADERS]`
3. Check MDS domain via `cls._check_data_on_cloud(dataset_id)`
4. POST via `cls._post_with_cache_check('/data/{dataset_id}/query', domain=domain, **kwargs)`

---

### GsDataApi.execute_query_async(dataset_id: str, query: Union[DataQuery, MDAPIDataQuery]) -> Any
Purpose: Async version of `execute_query`.

**Algorithm:**
1. Build `kwargs = {'payload': query}`
2. Branch: if `query.format` is `Format.MessagePack` or `'MessagePack'` -> add Accept header
3. `await cls._check_data_on_cloud_async(dataset_id)` for MDS domain
4. `await cls._post_with_cache_check_async(...)` and return

---

### GsDataApi._check_data_on_cloud(dataset_id: str) -> Optional[str]
Purpose: Check if a dataset should be redirected to the MDS (Market Data Service) domain.

**Algorithm:**
1. Get session
2. Branch: if `session.redirect_to_mds` is truthy AND `dataset_id != 'coordinates'`:
   - GET `/data/datasets/{dataset_id}` via `_get_with_cache_check`
   - Branch: if `parameters.databaseId` exists in response (via `pydash.get`):
     - Return `session._get_mds_domain()`
3. Return `None`

---

### GsDataApi._check_data_on_cloud_async(dataset_id: str) -> Optional[str]
Purpose: Async version of `_check_data_on_cloud`.

**Algorithm:**
1. Same logic as sync version but uses `await _get_with_cache_check` (note: not the `_async` variant -- this may be intentional or a subtle issue)
2. Return MDS domain or `None`

---

### GsDataApi._get_results(response: Union[DataQueryResponse, dict]) -> Tuple[Union[list, Tuple[list, list]], int]
Purpose: Extract data results and total page count from a query response (static method).

**Algorithm:**
1. Branch: if `response` is dict:
   - Extract `total_pages` from `response.get('totalPages')`
   - Extract `results` from `response.get('data', [])`
   - Branch: if `'groups'` in response:
     - For each group: collect group context keys into `group_by` set, merge context into each data row, append rows to results
     - Wrap results as tuple `(results, list(group_by))`
2. Branch: if `response` is `DataQueryResponse` object:
   - `total_pages = response.total_pages if response.total_pages is not None else 0`
   - `results = response.data if response.data is not None else ()`
3. Return `(results, total_pages)`

---

### GsDataApi.get_results(dataset_id: str, response: Union[DataQueryResponse, dict], query: DataQuery) -> Union[list, Tuple[list, list]]
Purpose: Recursively fetch all pages of a query result (static method, sync). Paginates backward from the last page.

**Algorithm:**
1. Call `_get_results(response)` to get `(results, total_pages)`
2. Branch: if `total_pages` is truthy (nonzero):
   - Branch: if `query.page is None`:
     - Set `query.page = total_pages - 1`
     - Recursively call `get_results` with a new `execute_query` and concatenate results
   - Branch: elif `query.page - 1 > 0`:
     - Decrement `query.page`
     - Recursively call `get_results` and concatenate results
   - Branch: else (page <= 1):
     - Return results as-is (base case)
3. Return results

**Note:** This mutates `query.page` in place during recursion.

---

### GsDataApi.get_results_async(dataset_id: str, response: Union[DataQueryResponse, dict], query: DataQuery) -> Union[list, Tuple[list, list]]
Purpose: Async version of `get_results` that fetches all pages in parallel using `asyncio.gather`.

**Algorithm:**
1. Call `_get_results(response)` to get `(results, total_pages)`
2. Branch: if `total_pages` and `total_pages > 1`:
   - For each page 1..total_pages-1: deepcopy query, set page, create future
   - `await asyncio.gather(*futures, return_exceptions=True)`
   - For each response: extract results via `_get_results` and concatenate
3. Return results

---

### GsDataApi.last_data(query: Union[DataQuery, MDAPIDataQuery], dataset_id: str = None, timeout: int = None) -> Union[list, tuple]
Purpose: Get the last/latest data for a query. Implements `DataApi` interface.

**Algorithm:**
1. Build kwargs; Branch: if `timeout is not None` -> add to kwargs
2. Branch: if `query.marketDataCoordinates` is truthy:
   - POST to `/data/coordinates/query/last` with payload=query
   - Return `result.get('responses', ())`
3. Branch: else:
   - Check MDS domain via `_check_data_on_cloud(dataset_id)`
   - POST to `/data/{dataset_id}/last/query`
   - Return `result.get('data', ())`

---

### GsDataApi.symbol_dimensions(dataset_id: str) -> tuple
Purpose: Return the symbol dimensions for a dataset. Implements `DataApi` interface.

**Algorithm:**
1. Get definition via `cls.get_definition(dataset_id)`
2. Return `definition.dimensions.symbolDimensions`

---

### GsDataApi.time_field(dataset_id: str) -> str
Purpose: Return the time field name for a dataset. Implements `DataApi` interface.

**Algorithm:**
1. Get definition via `cls.get_definition(dataset_id)`
2. Return `definition.dimensions.timeField`

---

### GsDataApi._build_params(scroll: str, scroll_id: Optional[str], limit: int, offset: int, fields: List[str], include_history: bool, **kwargs) -> dict
Purpose: Build query parameters dict for coverage and pagination endpoints.

**Algorithm:**
1. Initialize `params = {'limit': limit or 4000, 'scroll': scroll}`
2. Branch: if `scroll_id` -> add `'scrollId'`
3. Branch: if `offset` -> add `'offset'`
4. Branch: if `fields` -> add `'fields'`
5. Branch: if `include_history` -> add `'includeHistory': 'true'`
6. Merge any extra `kwargs` into params
7. Return params

---

### GsDataApi.get_coverage(dataset_id: str, scroll: str = DEFAULT_SCROLL, scroll_id: Optional[str] = None, limit: int = None, offset: int = None, fields: List[str] = None, include_history: bool = False, **kwargs) -> List[dict]
Purpose: Get full coverage data for a dataset, handling scroll-based pagination.

**Algorithm:**
1. Get session; build params via `_build_params`
2. GET `/data/{dataset_id}/coverage` with params
3. Initialize `results = scroll_results = body['results']`; get `total_results = body['totalResults']`
4. While loop: `len(scroll_results) > 0` AND `len(results) < total_results`:
   - Branch: if `body.get('scrollId') is None` -> break (no more scroll IDs)
   - Update `params['scrollId']` from body
   - GET next page; append `scroll_results` to `results`
5. Return results

---

### GsDataApi.get_coverage_async(dataset_id: str, scroll: str = DEFAULT_SCROLL, scroll_id: Optional[str] = None, limit: int = None, offset: int = None, fields: List[str] = None, include_history: bool = False, **kwargs) -> List[dict]
Purpose: Async version of `get_coverage`.

**Algorithm:**
1. Same structure as sync version but uses `await session.async_.get(...)`
2. Difference: does NOT check for `scrollId is None` before accessing `body['scrollId']` (could raise KeyError if missing)
3. Only appends `scroll_results` if truthy (extra guard vs sync version)

---

### GsDataApi.create(definition: Union[DataSetEntity, dict]) -> DataSetEntity
Purpose: Create a new dataset.

**Algorithm:**
1. POST `/data/datasets` with `payload=definition`
2. Return result

---

### GsDataApi.get_catalog_url(dataset_id: str) -> str
Purpose: Build the web URL for a dataset in the catalog UI.

**Algorithm:**
1. Get session
2. Build URL with web domain, path `/s/data-services/datasets/{dataset_id}`, `include_version=False`
3. Return URL

**Note:** Return type annotation says `DataSetEntity` but actually returns a `str` URL.

---

### GsDataApi.delete_dataset(dataset_id: str) -> dict
Purpose: Delete a dataset by ID.

**Algorithm:**
1. DELETE `/data/datasets/{dataset_id}`
2. Return result

---

### GsDataApi.undelete_dataset(dataset_id: str) -> dict
Purpose: Restore a previously deleted dataset.

**Algorithm:**
1. PUT `/data/datasets/{dataset_id}/undelete`
2. Return result

---

### GsDataApi.update_definition(dataset_id: str, definition: Union[DataSetEntity, dict]) -> DataSetEntity
Purpose: Update a dataset definition.

**Algorithm:**
1. PUT `/data/datasets/{dataset_id}` with `payload=definition`, `cls=DataSetEntity`
2. Return result

---

### GsDataApi.upload_data(dataset_id: str, data: Union[pd.DataFrame, list, tuple]) -> dict
Purpose: Upload data to a dataset, converting DataFrames to JSON records format.

**Algorithm:**
1. Branch: if `data` is `pd.DataFrame` -> convert to JSON via `to_json(orient='records')`
2. Get session
3. Branch: if `'us-east'` in `session.domain` -> `headers = None` (no msgpack)
4. Branch: else -> `headers = {'Content-Type': 'application/x-msgpack'}`
5. POST `/data/{dataset_id}` with payload=data and request_headers=headers
6. Return result

---

### GsDataApi.delete_data(dataset_id: str, delete_query: Dict) -> Dict
Purpose: Delete data from a dataset (requires admin access, irreversible).

**Algorithm:**
1. DELETE `/data/{dataset_id}` with `payload=delete_query`, `use_body=True`
2. Return result

---

### GsDataApi.get_definition(dataset_id: str) -> DataSetEntity
Purpose: Get a dataset definition, using an in-memory class-level cache.

**Algorithm:**
1. Look up `cls.__definitions.get(dataset_id)`
2. Branch: if not cached:
   - GET `/data/datasets/{dataset_id}` with `cls=DataSetEntity`
   - Branch: if result is falsy -> raise `MqValueError('Unknown dataset {dataset_id}')`
   - Store in `cls.__definitions[dataset_id]`
3. Return definition

**Raises:** `MqValueError` when dataset not found

---

### GsDataApi.get_many_definitions(limit: int = 100, offset: int = None, scroll: str = DEFAULT_SCROLL, scroll_id: Optional[str] = None) -> Tuple[DataSetEntity, ...]
Purpose: Get many dataset definitions with scroll-based pagination.

**Algorithm:**
1. Build params dict, filtering out `None` values; include `enablePagination='true'`
2. GET `/data/datasets` with params
3. Extract `results` and `total_results` from body
4. While loop: scroll until all results collected (same pattern as `get_coverage`)
5. Return results

---

### GsDataApi.get_catalog(dataset_ids: List[str] = None, limit: int = 100, offset: int = None, scroll: str = DEFAULT_SCROLL, scroll_id: Optional[str] = None) -> Tuple[DataSetCatalogEntry, ...]
Purpose: Get dataset catalog entries, with optional filtering by dataset IDs.

**Algorithm:**
1. Build query string: `dataSetId=X&dataSetId=Y` from dataset_ids if provided
2. Branch: if `len(query) > 0` (dataset_ids provided):
   - GET `/data/catalog?{query}` and return `results`
3. Branch: else (no dataset_ids):
   - Build paginated params (same as `get_many_definitions`)
   - GET `/data/catalog` with pagination
   - Scroll through all pages
   - Return results

---

### GsDataApi.get_many_coordinates(mkt_type: str = None, mkt_asset: str = None, mkt_class: str = None, mkt_point: Tuple[str, ...] = (), *, limit: int = 100, return_type: type = str) -> Union[Tuple[str, ...], Tuple[MarketDataCoordinate, ...]]
Purpose: Query MDAPI for available coordinates matching given filters. Cached via `@cachetools.cached(__asset_coordinates_cache)`.

**Algorithm:**
1. Build `FieldFilterMap` with uppercased values for `mkt_type`, `mkt_asset`, `mkt_class`
2. For each point in `mkt_point`: set `mkt_pointN` attribute (1-indexed, uppercased)
3. POST `/data/mdapi/query` with `EntityQuery(where=where, limit=limit)`
4. Branch: if `return_type is str` -> return tuple of coordinate `'name'` strings
5. Branch: elif `return_type is MarketDataCoordinate` -> return tuple of `MarketDataCoordinate` objects built from dimensions
6. Branch: else -> raise `NotImplementedError('Unsupported return type')`

**Raises:** `NotImplementedError` when `return_type` is not `str` or `MarketDataCoordinate`

---

### GsDataApi._to_zulu(d) -> str
Purpose: Format a datetime as a Zulu-time ISO string.

**Algorithm:**
1. Return `d.strftime('%Y-%m-%dT%H:%M:%SZ')`

---

### GsDataApi.get_mxapi_curve_measure(curve_type=None, curve_asset=None, curve_point=None, curve_tags=None, measure=None, start_time=None, end_time=None, request_id=None, close_location=None, real_time=None) -> pd.DataFrame
Purpose: Query MxAPI for curve measure data (real-time or end-of-day).

**Algorithm:**
1. Determine `real_time`: use param if set, else infer from `isinstance(start_time, dt.datetime)`
2. Branch: if `not start_time`:
   - Branch: if `real_time` -> use `DataContext.current.start_time`
   - Branch: else -> use `DataContext.current.start_date`
3. Branch: if `not end_time`:
   - Branch: if `real_time` -> use `DataContext.current.end_time`
   - Branch: else -> use `DataContext.current.end_date`
4. Branch: if `not real_time and not close_location` -> default `close_location = 'NYC'`
5. Branch: if `real_time and not isinstance(end_time, dt.date)` -> raise `ValueError`
6. Branch: if `real_time`:
   - Build request dict with type `'MxAPI Measure Request'`, `startTime`, `endTime` (Zulu format)
   - URL = `/mxapi/mq/measure`
7. Branch: else:
   - Build request dict with type `'MxAPI Measure Request EOD'`, `startDate`, `endDate` (ISO format), `close`
   - URL = `/mxapi/mq/measure/eod`
8. POST via `_post_with_cache_check`; log timing; on exception log warning and re-raise
9. Branch: if `real_time`:
   - Parse timestamps via `dateutil.parser.parse`
   - Build DataFrame indexed by `timeStamp`
10. Branch: else:
    - Parse dates via `dt.date.fromisoformat`
    - Build DataFrame indexed by `date`
11. Return `MarketDataResponseFrame`

**Raises:** `ValueError` when real_time but end_time is not a date type; re-raises any exception from the POST

---

### GsDataApi.get_mxapi_vector_measure(curve_type=None, curve_asset=None, curve_point=None, curve_tags=None, vector_measure=None, as_of_time=None, request_id=None, close_location=None) -> pd.DataFrame
Purpose: Query MxAPI for vector/curve measure data at a point in time.

**Algorithm:**
1. Branch: if `not vector_measure` -> raise `ValueError("Vector measure must be specified.")`
2. Branch: if `not as_of_time` -> raise `ValueError("As-of date or time must be specified.")`
3. Determine `real_time = isinstance(as_of_time, dt.datetime)`
4. Branch: if `not real_time and not isinstance(as_of_time, dt.date)` -> raise `ValueError`
5. Branch: if `not real_time and not close_location` -> default `'NYC'`
6. Branch: if `real_time`:
   - Build request dict with type `'MxAPI Curve Request'`, `asOfTime` (Zulu)
   - URL = `/mxapi/mq/curve`
7. Branch: else:
   - Build request dict with type `'MxAPI Curve Request EOD'`, `asOfDate` (ISO), `close`
   - URL = `/mxapi/mq/curve/eod`
8. POST via `_post_with_cache_check`; log timing; on exception log warning and re-raise
9. Extract `curve`, `curveName`, `knots`, `knotType` from body
10. Branch: if `len(values) == 0 and len(body['errMsg']) > 0` -> raise `RuntimeError(body['errMsg'])`
11. Build DataFrame indexed by `knotType` column
12. Return `MarketDataResponseFrame`

**Raises:** `ValueError` (3 conditions above); `RuntimeError` when empty curve with error message; re-raises POST exceptions

---

### GsDataApi.get_mxapi_backtest_data(builder, start_time=None, end_time=None, num_samples=120, csa=None, request_id=None, close_location=None, real_time=None) -> pd.DataFrame
Purpose: Run an MxAPI backtest query with an instrument builder.

**Algorithm:**
1. Determine `real_time`: param or infer from `isinstance(start_time, dt.datetime)`
2. Default `start_time`/`end_time` from `DataContext.current` (same branches as `get_mxapi_curve_measure`)
3. Branch: if `not csa` -> default `'Default'`
4. Branch: if `not real_time and not close_location` -> default `'NYC'`
5. Branch: if `real_time and not isinstance(end_time, dt.date)` -> raise `ValueError`
6. Resolve builder: `leg = builder.resolve(in_place=False)`; serialize to JSON dict via `JSONEncoder`
7. Branch: if `real_time`:
   - Build request dict with type `'MxAPI Backtest Request MQ'`, `startTime`, `endTime`, `sampleSize`, `csa`
   - URL = `/mxapi/mq/backtest`
8. Branch: else:
   - Build request dict with type `'MxAPI Backtest Request MQEOD'`, `startDate`, `endDate`, `sampleSize`, `csa`, `close`
   - URL = `/mxapi/mq/backtest/eod`
9. POST, log, handle exceptions (same pattern)
10. Branch: if `real_time` -> parse timestamps, build DataFrame indexed by `timeStamp`
11. Branch: else -> parse dates, build DataFrame indexed by `date`
12. Return `MarketDataResponseFrame`

**Raises:** `ValueError` for mismatched date/time types; re-raises POST exceptions

---

### GsDataApi._get_market_data_filters(asset_ids: List[str], query_type: Union[QueryType, str], where: Union[FieldFilterMap, Dict] = None, source: Union[str] = None, real_time: bool = False, measure='Curve', vendor: str = '') -> dict
Purpose: Build the inner filter dict for a market data measures query (static method).

**Algorithm:**
1. Build dict with `entityIds`, `queryType` (extract `.value` if QueryType enum), `where` (or `{}`), `source` (or `'any'`), `frequency` (`'Real Time'` or `'End Of Day'`), `measures` list
2. Branch: if `vendor != ''` -> add `'vendor'` key
3. Return dict

---

### GsDataApi.build_interval_chunked_market_data_queries(asset_ids: List[str], query_type: Union[QueryType, str], where=None, source=None, real_time: bool = False, measure='Curve', vendor: str = '') -> List[dict]
Purpose: Build multiple queries chunked by 365-day intervals for parallel execution (static method).

**Algorithm:**
1. Define `chunk_time(start, end)` generator yielding `(s, e)` pairs of max 365-day intervals
2. Branch: if `real_time`:
   - Use `DataContext.current.start_time`/`end_time`; keys = `'startTime'`/`'endTime'`
3. Branch: else:
   - Use `DataContext.current.start_date`/`end_date`; keys = `'startDate'`/`'endDate'`
4. For each chunk: copy the filters dict, set start/end keys, wrap in `{'queries': [inner]}`
5. Log debug with query count
6. Return list of query dicts

---

### GsDataApi.build_market_data_query(asset_ids: List[str], query_type: Union[QueryType, str], where=None, source=None, real_time: bool = False, measure='Curve', parallelize_queries: bool = False, vendor: str = '') -> Union[dict, List[dict]]
Purpose: Build a market data query, optionally parallelized by time interval (static method).

**Algorithm:**
1. Branch: if `parallelize_queries` -> delegate to `build_interval_chunked_market_data_queries` and return
2. Build `inner` via `_get_market_data_filters`
3. Branch: if `DataContext.current.interval is not None` -> add `'interval'` key
4. Branch: if `real_time`:
   - Add `startTime`/`endTime` from `DataContext.current`
5. Branch: else:
   - Add `startDate`/`endDate` from `DataContext.current`
6. Return `{'queries': [inner]}`

---

### GsDataApi.get_data_providers(entity_id: str, availability: Optional[Dict] = None) -> Dict
Purpose: Get daily and real-time data providers for an entity.

**Algorithm:**
1. Branch: if `availability` is provided -> use it; else GET `/data/measures/{entity_id}/availability`
2. Branch: if `'errorMessages'` in response -> raise `MqValueError` with request ID and messages
3. Branch: if `'data'` not in response -> return `{}`
4. Sort `response['data']` by `rank` descending
5. For each source mapping:
   - Get `freq` (default `'End Of Day'`), `dataset_field`, `rank`
   - Initialize providers dict entry for `dataset_field` if missing
   - Branch: if `rank` is truthy:
     - Branch: if `freq == 'End Of Day'` -> set `DataFrequency.DAILY` provider
     - Branch: elif `freq == 'Real Time'` -> set `DataFrequency.REAL_TIME` provider
6. Return providers dict

**Raises:** `MqValueError` when response contains error messages

---

### GsDataApi.get_market_data(query, request_id=None, ignore_errors: bool = False) -> pd.DataFrame
Purpose: Execute a market data measures query and return results as a DataFrame.

**Algorithm:**
1. Define inner `validate(body)` function:
   - For each response in `body['responses']`: check first `queryResponse` for `errorMessages`
   - Branch: if errors found -> raise `MqValueError`
2. POST `/data/measures` via `_post_with_cache_check` with validator
3. On exception: log warning and re-raise
4. Log debug with timing
5. For each response container:
   - Extend `ids` with `dataSetIds`
   - Branch: if `'errorMessages'` in container:
     - Branch: if `ignore_errors` -> log warning
     - Branch: else -> raise `MqValueError`
   - Branch: if `'response'` in container:
     - Build `MarketDataResponseFrame` from `container['response']['data']`
     - Set index to `'date'` or `'time'` column
     - Convert index to datetime
     - Append to parts
6. Concatenate parts (or empty `MarketDataResponseFrame` if no parts)
7. Set `df.dataset_ids = tuple(ids)`
8. Return df

**Raises:** `MqValueError` during validation or when `ignore_errors=False` and errors present

---

### GsDataApi.__normalise_coordinate_data(data: Iterable[Union[MDAPIDataQueryResponse, Dict]], fields: Optional[Tuple[MDAPIQueryField, ...]] = None) -> Iterable[Iterable[Dict]]
Purpose: Normalize coordinate query response data, extracting value fields.

**Algorithm:**
1. For each response in data:
   - Branch: if response is `MDAPIDataQueryResponse` -> iterate `.data` as dicts via `.as_dict()`
   - Branch: else (dict) -> iterate `response.get('data', ())`
   - For each row:
     - Branch: if `not pt` (empty/falsy) -> skip
     - Branch: if `not fields and 'value' not in pt`:
       - Get `value_field = pt['mktQuotingStyle']`
       - Branch: if `value_field not in pt` -> skip
       - Move `pt[value_field]` to `pt['value']`
     - Append to coord_data
   - Append coord_data to ret
2. Return ret

---

### GsDataApi.__df_from_coordinate_data(data: Iterable[Dict], *, use_datetime_index: Optional[bool] = True) -> pd.DataFrame
Purpose: Build a DataFrame from coordinate data with optional datetime indexing.

**Algorithm:**
1. Build DataFrame from records; sort via `_sort_coordinate_data`
2. Find `index_field`: first of `'time'`, `'date'` present in columns; or `None`
3. Branch: if `index_field` and `use_datetime_index`:
   - Set index to `DatetimeIndex` from that column
4. Return df

---

### GsDataApi._sort_coordinate_data(df: pd.DataFrame, by: Tuple[str, ...] = ('date', 'time', 'mktType', 'mktAsset', 'mktClass', 'mktPoint', 'mktQuotingStyle', 'value')) -> pd.DataFrame
Purpose: Reorder DataFrame columns to a canonical order.

**Algorithm:**
1. Build `field_order` starting with known fields present in columns
2. Append remaining columns not in `field_order`
3. Return `df[field_order]`

---

### GsDataApi._coordinate_from_str(coordinate_str: str) -> MarketDataCoordinate
Purpose: Parse a coordinate string like `"FX Fwd_USD/EUR_Fwd Pt_2y.Fwd Points"` into a `MarketDataCoordinate`.

**Algorithm:**
1. Split on last `"."` -> `[dimensions_part, quoting_style]` or `[dimensions_part]`
2. Split `dimensions_part` on `"_"` -> dimensions list
3. Branch: if `len(dimensions) < 2` -> raise `MqValueError('invalid coordinate ...')`
4. Extract `mkt_type = dimensions[0]`, `mkt_asset = dimensions[1] or None`
5. Extract `mkt_quoting_style` from after the `.` if present, else `None`
6. Branch: if `len(dimensions) > 2` -> `mkt_class = dimensions[2] or None`
7. Branch: if `len(dimensions) > 3` -> `mkt_point = tuple(dimensions[3:]) or None`
8. Return `MarketDataCoordinate(**kwargs)`

**Raises:** `MqValueError` when coordinate string has fewer than 2 underscore-separated dimensions

---

### GsDataApi.coordinates_last(coordinates: Union[Iterable[str], Iterable[MarketDataCoordinate]], as_of: Union[dt.datetime, dt.date] = None, vendor: MarketDataVendor = MarketDataVendor.Goldman_Sachs, as_dataframe: bool = False, pricing_location: Optional[PricingLocation] = None, timeout: int = None) -> Union[Dict, pd.DataFrame]
Purpose: Get the last/latest value for one or more market data coordinates.

**Algorithm:**
1. Convert string coordinates to `MarketDataCoordinate` via `_coordinate_from_str`
2. Build query via `cls.build_query(end=as_of, market_data_coordinates=..., vendor=vendor, pricing_location=pricing_location)`
3. Branch: if `timeout is not None` -> pass in kwargs
4. Call `cls.last_data(query, **kwargs)`
5. Branch: if `not as_dataframe`:
   - Initialize ret dict: `{coordinate: None for coordinate in market_data_coordinates}`
   - For each coordinate, normalize data; try to get `row[0]['value']`
   - Branch: `IndexError` -> set value to `None`
   - Return ret dict
6. Branch: if `as_dataframe`:
   - Determine datetime_field: `'time'` if `as_of` is datetime, else `'date'`
   - For each coordinate: build dict with coordinate dimensions + value + datetime
   - Branch: `IndexError` -> use `None` for value and datetime
   - Return `__df_from_coordinate_data(ret, use_datetime_index=False)`

---

### GsDataApi.coordinates_data(coordinates: Union[str, MarketDataCoordinate, Iterable[str], Iterable[MarketDataCoordinate]], start=None, end=None, vendor=MarketDataVendor.Goldman_Sachs, as_multiple_dataframes: bool = False, pricing_location=None, fields=None, **kwargs) -> Union[pd.DataFrame, Tuple[pd.DataFrame]]
Purpose: Get time series data for one or more coordinates.

**Algorithm:**
1. Normalize coordinates: if single string or `MarketDataCoordinate`, wrap in 1-tuple
2. Convert any string coordinates to `MarketDataCoordinate`
3. Build query via `cls.build_query(...)` with all params
4. Execute `cls.query_data(query)` and normalize via `__normalise_coordinate_data`
5. Branch: if `as_multiple_dataframes` -> return tuple of DataFrames, one per coordinate
6. Branch: else -> flatten all results via `chain.from_iterable` and return single DataFrame

---

### GsDataApi.coordinates_data_series(coordinates: Union[str, MarketDataCoordinate, Iterable[str], Iterable[MarketDataCoordinate]], start=None, end=None, vendor=MarketDataVendor.Goldman_Sachs, pricing_location=None, **kwargs) -> Union[pd.Series, Tuple[pd.Series]]
Purpose: Get coordinates data as Series (value only, indexed by date/time).

**Algorithm:**
1. Call `cls.coordinates_data(...)` with `as_multiple_dataframes=True`
2. For each DataFrame: Branch: if empty -> `pd.Series(dtype=float)`; else -> `pd.Series(index=df.index, data=df.value.values)`
3. Branch: if `coordinates` is single (str or MarketDataCoordinate) -> return `ret[0]`
4. Branch: else -> return tuple of Series

---

### GsDataApi.get_types(dataset_id: str) -> dict
Purpose: Get field name-to-type mappings for a dataset from the catalog API. Cached via `@cachetools.cached(TTLCache(ttl=3600, maxsize=128))`.

**Algorithm:**
1. GET `/data/catalog/{dataset_id}`
2. Extract `fields` from results
3. Branch: if `fields` is truthy:
   - For each field: extract `type` and `format`; prefer `format` over `type`
   - Return field_types dict
4. Branch: else -> raise `RuntimeError(f"Unable to get Dataset schema for {dataset_id}")`

**Raises:** `RuntimeError` when no fields found

---

### GsDataApi.get_field_types(field_names: Union[str, List[str]]) -> dict
Purpose: Get field name-to-type mappings using the dataset fields API.

**Algorithm:**
1. Try: call `cls.get_dataset_fields(names=field_names, limit=len(field_names))`
2. Branch: on any Exception -> return `{}`
3. Branch: if `fields` is truthy:
   - For each field: get `name`, `type_`, and optionally `parameters.format`; prefer format over type
   - Return field_types dict
4. Branch: else -> return `{}`

---

### GsDataApi.construct_dataframe_with_types(dataset_id: str, data: Union[Base, List, Tuple], schema_varies=False, standard_fields=False) -> pd.DataFrame
Purpose: Construct a DataFrame with correct date/datetime column types. Implements `DataApi` interface.

**Algorithm:**
1. Branch: if `len(data) > 0`:
   - Branch: if `schema_varies` -> sample = all data; else -> sample = `[data[0]]`
   - Infer incoming dtypes from sample
   - Branch: if `not standard_fields` -> get types via `cls.get_types(dataset_id)`
   - Branch: else -> get types via `cls.get_field_types(field_names=list(...))`
   - Branch: if `dataset_types == {} and standard_fields` -> fallback to `cls.get_types(dataset_id)`
   - Create DataFrame with merged column ordering
   - For each field in dataset_types:
     - Branch: if field exists in df AND type is `'date'` or `'date-time'` AND has non-empty values:
       - Branch: if pandas major version > 1 -> use `pd.to_datetime(format='ISO8601')`
       - Branch: else -> use `pd.to_datetime()` without format
   - Branch: if `'date'` in field_names -> set index to `'date'`
   - Branch: elif `'time'` in field_names -> set index to `'time'`
   - Return df
2. Branch: else (empty data) -> return `pd.DataFrame({})`

---

### GsDataApi.get_dataset_fields(ids: Union[str, List[str]] = None, names: Union[str, List[str]] = None, limit: int = 10) -> Union[Tuple[DataSetFieldEntity, ...], Tuple[dict, ...]]
Purpose: Query dataset fields by ID or name.

**Algorithm:**
1. Build `where` dict filtering out None values for `id` and `name`
2. POST `/data/fields/query` with `{'where': where, 'limit': limit}`
3. Return `response['results']`

---

### GsDataApi.create_dataset_fields(fields: List[DataSetFieldEntity]) -> Union[Tuple[DataSetFieldEntity, ...], Tuple[dict, ...]]
Purpose: Bulk create dataset fields.

**Algorithm:**
1. POST `/data/fields/bulk` with `{'fields': fields}`
2. Return `response['results']`

---

### GsDataApi.update_dataset_fields(fields: List[DataSetFieldEntity]) -> Union[Tuple[DataSetFieldEntity, ...], Tuple[dict, ...]]
Purpose: Bulk update dataset fields.

**Algorithm:**
1. PUT `/data/fields/bulk` with `{'fields': fields}`
2. Return `response['results']`

---

### MarketDataResponseFrame._constructor (property) -> type
Purpose: Return the constructor for DataFrame operations so that sliced/copied frames remain `MarketDataResponseFrame`.

**Algorithm:**
1. Return `MarketDataResponseFrame`

---

### MarketDataResponseFrame.__finalize__(self, other, method=None, **kwargs) -> MarketDataResponseFrame
Purpose: Copy custom attributes (dataset_ids) from source DataFrame during pandas operations.

**Algorithm:**
1. Call `super().__finalize__(other, method, **kwargs)`
2. Branch: if `other` is `MarketDataResponseFrame` AND has `dataset_ids`:
   - Copy `dataset_ids` from other
3. Return self

## State Mutation
- `GsDataApi.__definitions` (class-level dict): Populated lazily by `get_definition()`. Never cleared within the module. Acts as an unbounded in-memory cache.
- `GsDataApi.__asset_coordinates_cache` (class-level TTLCache): Populated by `get_many_coordinates()` via `@cachetools.cached` decorator. 10,000 item limit, 24-hour TTL.
- `GsDataApi._api_request_cache` (class-level): Set by `set_api_request_cache()`. Used by all `_*_with_cache_check*` methods for request-level caching.
- `query.page`: Mutated in-place by `get_results()` during recursive pagination. Callers should be aware the query object is modified.
- `MarketDataResponseFrame.dataset_ids`: Set by `get_market_data()` after DataFrame construction. Propagated through pandas operations via `__finalize__`.
- Thread safety: Class-level caches (`__definitions`, `__asset_coordinates_cache`, `_api_request_cache`) are shared across threads with no locking. The `TTLCache` from `cachetools` is not thread-safe by default. The `__definitions` dict uses standard Python dict which has thread-safe individual operations in CPython (GIL) but not compound check-then-set operations.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `get_definition` | Dataset not found (GET returns falsy) |
| `MqValueError` | `get_data_providers` | Response contains `errorMessages` key |
| `MqValueError` | `get_market_data` (validate) | Any `queryResponse` container has `errorMessages` |
| `MqValueError` | `get_market_data` (post-process) | Container has `errorMessages` and `ignore_errors=False` |
| `MqValueError` | `_coordinate_from_str` | Coordinate string has fewer than 2 underscore-separated dimensions |
| `ValueError` | `get_mxapi_curve_measure` | `real_time=True` but `end_time` is not a `dt.date` instance |
| `ValueError` | `get_mxapi_vector_measure` | `vector_measure` not specified; `as_of_time` not specified; `as_of_time` not a date or datetime |
| `ValueError` | `get_mxapi_backtest_data` | `real_time=True` but `end_time` is not a `dt.date` instance |
| `RuntimeError` | `get_types` | No fields returned from catalog API for dataset |
| `RuntimeError` | `get_mxapi_vector_measure` | Empty curve values with non-empty `errMsg` in response |
| `NotImplementedError` | `get_many_coordinates` | `return_type` is neither `str` nor `MarketDataCoordinate` |
| (re-raised) | `get_mxapi_curve_measure`, `get_mxapi_vector_measure`, `get_mxapi_backtest_data`, `get_market_data` | Any exception from the POST call is logged and re-raised |

## Edge Cases
- **`get_results` recursive pagination**: Mutates `query.page` in place, meaning the query object is modified after the call returns. If the same query object is reused, it will have a different `.page` value.
- **`get_results_async` does not use `return_exceptions=True` safely**: Exceptions from `asyncio.gather` are captured as results but then `_get_results(response_crt)` is called on them, which would fail if `response_crt` is an exception.
- **`_check_data_on_cloud_async` uses `_get_with_cache_check` (sync)**: Line 331 calls `await cls._get_with_cache_check(...)` but `_get_with_cache_check` is not async. This works only if the method isn't actually awaited (it would need `_get_with_cache_check_async` for true async). However the `await` on a non-coroutine may silently pass in some contexts.
- **`get_coverage_async` does not guard against missing `scrollId`**: Unlike the sync `get_coverage`, the async variant directly accesses `body['scrollId']` without `.get()` and `None` check, which could raise `KeyError` if the server omits it.
- **`get_catalog_url` return type mismatch**: Annotated as returning `DataSetEntity` but returns a `str` URL.
- **`_get_results` with groups**: When groups are present, the result becomes a tuple of `(list, list)` -- meaning all downstream consumers must handle both `list` and `tuple` return shapes.
- **`upload_data` msgpack header logic**: Uses `'us-east' in session.domain` as a heuristic to decide whether to use msgpack encoding. If the domain string changes this check may silently break.
- **`__normalise_coordinate_data` missing `mktQuotingStyle`**: If a row lacks both `'value'` and `'mktQuotingStyle'`, a `KeyError` will be raised.
- **`construct_dataframe_with_types` pandas version check**: Uses `int(pd.__version__.split('.')[0]) > 1` to branch on ISO8601 format parameter, which will work for pandas 2+ but breaks if version strings change format.
- **Empty `mkt_asset` in `_coordinate_from_str`**: If `dimensions[1]` is empty string, it gets mapped to `None` via `or None`, which changes the semantic meaning.
- **`get_many_definitions` / `get_catalog` scroll loops**: If the server returns the same `scrollId` repeatedly (or `scroll_results` is always non-empty), these loops could run indefinitely.

## Coverage Notes
- Branch count: ~95+ distinct branches across all methods
- Key branching methods:
  - `query_data` / `query_data_async`: 4 branches each (MDAPIDataQuery vs DataQuery, dict vs object response)
  - `get_results`: 4 branches (total_pages truthy, page None, page > 1, else)
  - `_get_results`: 5 branches (dict vs object, groups present, totalPages handling)
  - `get_mxapi_curve_measure`: 8+ branches (real_time inference, start/end defaults, close_location, validation, response parsing)
  - `get_mxapi_vector_measure`: 7+ branches (validation x3, real_time, close_location, error check, normal path)
  - `get_mxapi_backtest_data`: 8+ branches (same structure as curve_measure)
  - `coordinates_last`: 6+ branches (as_dataframe, IndexError handling x2, datetime_field selection)
  - `construct_dataframe_with_types`: 8+ branches (empty data, schema_varies, standard_fields, fallback, pandas version, date/time index)
  - `__normalise_coordinate_data`: 5 branches (response type, empty pt, fields vs no fields, value presence, mktQuotingStyle presence)
  - `get_data_providers`: 5 branches (availability param, errorMessages, no data, frequency type x2)
  - `get_market_data`: 5+ branches (validation errors, ignore_errors, response presence, empty parts)
  - Cache methods (`_post_with_cache_check`, `_get_with_cache_check`, async variants): 2 branches each (cache hit/miss, cache store)
- Pragmas: None observed in source
