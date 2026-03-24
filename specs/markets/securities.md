# securities.py

## Summary
Central security/asset abstraction module providing the full asset class hierarchy (`Asset` base class and 25+ concrete subclasses), temporal identifier resolution, data series retrieval, and the `SecurityMaster` service for looking up assets by various identifiers against either the Marquee Asset Service or a dedicated Security Master backend. Also defines the `SecMasterAsset` subclass with cached temporal identifier management and Marquee-ID validation across date ranges.

## Dependencies
- Internal:
  - `gs_quant.api.gs.assets` (GsAsset, GsIdType, GsAssetApi)
  - `gs_quant.api.gs.data` (GsDataApi)
  - `gs_quant.api.utils` (ThreadPoolManager)
  - `gs_quant.base` (get_enum_value)
  - `gs_quant.common` (AssetClass, AssetParameters, AssetType as GsAssetType, Currency, DateLimit)
  - `gs_quant.context_base` (nullcontext)
  - `gs_quant.data` (DataMeasure, DataFrequency, Dataset, AssetMeasure)
  - `gs_quant.data.coordinate` (DataDimensions, DateOrDatetime)
  - `gs_quant.data.core` (IntervalFrequency, DataAggregationOperator)
  - `gs_quant.entities.entity` (Entity, EntityIdentifier, EntityType, PositionedEntity)
  - `gs_quant.errors` (MqValueError, MqTypeError, MqRequestError)
  - `gs_quant.json_encoder` (JSONEncoder)
  - `gs_quant.markets` (PricingContext)
  - `gs_quant.markets.indices_utils` (BasketType, IndicesDatasets)
  - `gs_quant.session` (GsSession)
  - `gs_quant.target.data` (DataQuery)
  - `gs_quant.tracing` (Tracer)
  - `gs_quant.markets.index` (Index) -- lazy import inside `__gs_asset_to_asset`
  - `gs_quant.markets.baskets` (Basket) -- lazy import inside `__gs_asset_to_asset`
- External:
  - `backoff` (on_exception, expo)
  - `cachetools` (TTLCache, cached, keys.hashkey)
  - `pandas` (Series, DataFrame)
  - `dateutil.relativedelta` (relativedelta)
  - `pydash` (get)
  - `calendar` (monthrange)
  - `datetime`, `json`, `logging`, `threading`, `time`, `copy.deepcopy`
  - `abc` (ABCMeta, abstractmethod)
  - `collections` (defaultdict)
  - `enum` (auto, Enum)
  - `functools` (partial)
  - `typing` (Tuple, Generator, Iterable, Optional, Dict, List, Union)

## Type Definitions

### Asset (abstract class, metaclass=ABCMeta)
Inherits: `Entity`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` | `str` | required | Marquee asset ID (name-mangled as `_Asset__id`) |
| `asset_class` | `AssetClass` | required | Asset class enum (Equity, FX, Commod, etc.) |
| `name` | `str` | required | Display name of the asset |
| `exchange` | `Optional[str]` | `None` | Exchange where asset is traded |
| `currency` | `Optional[str]` | `None` | Denomination currency |
| `parameters` | `AssetParameters` | `None` | Additional asset parameters |
| `entity` | `Optional[Dict]` | `None` | Raw entity dictionary from API |

### SecMasterAsset (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__asset_type` | `AssetType` | required | The security type from Security Master |
| `__cached_identifiers` | `Optional[dict]` | `None` | Lazily loaded temporal identifier cache; maps id_type string to list of `{start_date, end_date, update_date, value}` dicts |

### Stock (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Equity` |

### Cross (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Default `asset_class=AssetClass.FX`; accepts string or enum |

### Future (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum); has `currency` |

### Currency (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Cash` |

### Rate (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Rates` |

### Cash (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Cash` |

### WeatherIndex (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### CommodityReferencePrice (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### CommodityNaturalGasHub (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### CommodityEUNaturalGasHub (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### Cryptocurrency (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied |

### CommodityPowerNode (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### CommodityPowerAggregatedNodes (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### Commodity (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Commod` |

### Bond (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Default `asset_class=AssetClass.Credit` |

### Fund (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied |

### FutureMarket (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum) |

### FutureContract (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum) |

### Swap (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum) |

### Option (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum) |

### Forward (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied (string or enum) |

### ETF (class)
Inherits: `Asset`, `PositionedEntity`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset + PositionedEntity) | | | Dual inheritance; PositionedEntity initialized with `(id_, EntityType.ASSET)` |

### Swaption (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Rates` |

### Binary (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied |

### DefaultSwap (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Credit` |

### XccySwapMTM (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | Fixed `asset_class=AssetClass.Rates`; `get_type()` references non-existent `AssetType.XccySwapMTM` (see Bugs Found) |

### MutualFund (class)
Inherits: `Asset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherits Asset) | | | `asset_class` is caller-supplied |

### Security (class)
No inheritance.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_ids` | `dict` | (from json `identifiers`) | Private dict of identifier key-value pairs |
| (dynamic) | varies | varies | All non-`identifiers` keys from constructor dict are set as attributes via `setattr` |

### SecurityMaster (class)
No inheritance. All methods are `@classmethod` or `@staticmethod`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_source` | `SecurityMasterSource` | `SecurityMasterSource.ASSET_SERVICE` | Class-level source toggle |
| `_page_size` | `int` | `1000` | Page size for paginated identifier queries |

## Enums and Constants

### ExchangeCode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| NASDAQ | `"NASD"` | Nasdaq Global Stock Market |
| NYSE | `"NYSE"` | New York Stock Exchange |

### AssetType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| INDEX | `"Index"` | Index tracking evolving portfolio |
| ETF | `"ETF"` | Exchange traded fund |
| CUSTOM_BASKET | `"Custom Basket"` | Bespoke custom basket |
| RESEARCH_BASKET | `"Research Basket"` | Research basket by GS Investment Research |
| STOCK | `"Single Stock"` | Listed equities |
| FUTURE | `"Future"` | Standardized listed contract |
| CROSS | `"Cross"` | FX cross or currency pair |
| CURRENCY | `"Currency"` | Currency |
| RATE | `"Rate"` | Rate |
| CASH | `"Cash"` | Cash |
| WEATHER_INDEX | `"Weather Index"` | Weather index |
| SWAP | `"Swap"` | Swap |
| SWAPTION | `"Swaption"` | Swaption |
| OPTION | `"Option"` | Option |
| BINARY | `"Binary"` | Binary |
| COMMODITY_REFERENCE_PRICE | `"Commodity Reference Price"` | Commodity reference price |
| COMMODITY_NATURAL_GAS_HUB | `"Commodity Natural Gas Hub"` | Commodity NG hub |
| COMMODITY_EU_NATURAL_GAS_HUB | `"Commodity EU Natural Gas Hub"` | EU NG hub |
| COMMODITY_POWER_NODE | `"Commodity Power Node"` | Power node |
| COMMODITY_POWER_AGGREGATED_NODES | `"Commodity Power Aggregated Nodes"` | Aggregated power nodes |
| BOND | `"Bond"` | Bond |
| FUTURE_MARKET | `"Future Market"` | Future market |
| FUTURE_CONTRACT | `"Future Contract"` | Future contract |
| COMMODITY | `"Commodity"` | Commodity |
| CRYPTOCURRENCY | `"Cryptocurrency"` | Cryptocurrency |
| FORWARD | `"Forward"` | Forward |
| FUND | `"Fund"` | Fund |
| DEFAULT_SWAP | `"Default Swap"` | Default swap (CDS) |
| SYSTEMATIC_HEDGING | `"Systematic Hedging"` | Systematic hedging |
| ACCESS | `"Access"` | Access |
| RISK_PREMIA | `"Risk Premia"` | Risk premia |
| MULTI_ASSET_ALLOCATION | `"Multi-Asset Allocation"` | Multi-asset allocation |
| ADR | `"ADR"` | American Depositary Receipt |
| GDR | `"GDR"` | Global Depositary Receipt |
| DUTCH_CERT | `"Dutch Cert"` | Dutch certificate |
| NYRS | `"NY Reg Shrs"` | NY registered shares |
| RECEIPT | `"Receipt"` | Receipt |
| UNIT | `"Unit"` | Unit |
| MUTUAL_FUND | `"Mutual Fund"` | Mutual fund |
| RIGHT | `"Right"` | Right |
| PREFERRED | `"Preferred"` | Preferred stock |
| MISC | `"Misc."` | Miscellaneous |
| REIT | `"REIT"` | Real estate investment trust |
| PRIVATE_COMP | `"Private Comp"` | Private company |
| PREFERENCE | `"Preference"` | Preference share |
| LIMITED_PARTNERSHIP | `"Ltd Part"` | Limited partnership |
| TRACKING_STOCK | `"Tracking Stk"` | Tracking stock |
| ROYALTY_TRUST | `"Royalty Trst"` | Royalty trust |
| CLOSED_END_FUND | `"Closed-End Fund"` | Closed-end fund |
| OPEN_END_FUND | `"Open-End Fund"` | Open-end fund |
| FUND_OF_FUNDS | `"Fund of Funds"` | Fund of funds |
| MLP | `"MLP"` | Master limited partnership |
| STAPLED_SECURITY | `"Stapled Security"` | Stapled security |
| SAVINGS_SHARE | `"Savings Share"` | Savings share |
| EQUITY_WRT | `"Equity WRT"` | Equity warrant |
| SAVINGS_PLAN | `"Savings Plan"` | Savings plan |
| EQUITY_INDEX | `"Equity Index"` | Equity index |
| COMMON_STOCK | `"Common Stock"` | Common stock |

### AssetIdentifier(EntityIdentifier)
| Value | Raw | Description |
|-------|-----|-------------|
| MARQUEE_ID | `"MQID"` | GS Marquee identifier |
| REUTERS_ID | `"RIC"` | Thomson Reuters Instrument Code |
| BLOOMBERG_ID | `"BBID"` | Bloomberg ID + exchange |
| BLOOMBERG_COMPOSITE_ID | `"BCID"` | Bloomberg composite ID + exchange |
| CUSIP | `"CUSIP"` | CUSIP code |
| ISIN | `"ISIN"` | ISIN |
| SEDOL | `"SEDOL"` | SEDOL code |
| TICKER | `"TICKER"` | Exchange ticker |
| PLOT_ID | `"PLOT_ID"` | Marquee PlotTool ID |
| GSID | `"GSID"` | GS internal ID |
| NAME | `"NAME"` | Asset name |

### SecurityIdentifier(EntityIdentifier)
| Value | Raw | Description |
|-------|-----|-------------|
| GSID | `"gsid"` | GS internal ID |
| RCIC | `"rcic"` | RCIC |
| RIC | `"ric"` | Reuters Instrument Code |
| ID | `"id"` | Security Master internal ID |
| CUSIP | `"cusip"` | CUSIP |
| CUSIP8 | `"cusip8"` | 8-char CUSIP |
| CINS | `"cins"` | CINS |
| SEDOL | `"sedol"` | SEDOL |
| ISIN | `"isin"` | ISIN |
| TICKER | `"ticker"` | Ticker |
| BBID | `"bbid"` | Bloomberg ID |
| BCID | `"bcid"` | Bloomberg composite ID |
| GSS | `"gss"` | GSS |
| PRIMEID | `"primeId"` | Prime ID |
| BBG | `"bbg"` | Bloomberg ticker |
| ASSET_ID | `"assetId"` | Marquee asset ID |
| ANY | `"identifiers"` | Match any identifier |
| BARRA_ID | `"barraId"` | Barra ID |
| AXIOMA_ID | `"axiomaId"` | Axioma ID |

### ReturnType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| EXCESS_RETURN | `"Excess Return"` | Returns excess of funding rate |
| TOTAL_RETURN | `"Total Return"` | Returns inclusive of funding rate |

### SecurityMasterSource(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ASSET_SERVICE | `auto()` | Use GS Asset Service API |
| SECURITY_MASTER | `auto()` | Use Security Master API |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### _get_with_retries(url, payload) -> response
Purpose: Module-level function that wraps `GsSession.current.sync.get` with exponential backoff retry on `MqRequestError`, giving up when status is not 429.

**Algorithm:**
1. Decorated with `@backoff.on_exception(backoff.expo, MqRequestError, giveup=lambda e: e.status != 429)`
2. Call `GsSession.current.sync.get(url, payload=payload)` and return result

---

### Asset.__init__(self, id_: str, asset_class: AssetClass, name: str, exchange: Optional[str] = None, currency: Optional[str] = None, parameters: AssetParameters = None, entity: Optional[Dict] = None)
Purpose: Initialize base Asset, delegating to `Entity.__init__`.

**Algorithm:**
1. Call `super().__init__(id_, EntityType.ASSET, entity=entity)`
2. Store all fields as instance attributes

---

### Asset.get_marquee_id(self) -> str
Purpose: Return the Marquee ID (private `__id`).

**Algorithm:**
1. Return `self.__id`

---

### Asset.get_url(self) -> str
Purpose: Build URL to the asset's product page on Marquee.

**Algorithm:**
1. Check if `'dev'` is in `GsSession.current.domain` -> set `env = '-dev-ext.web'`
2. Check if `'qa'` is in `GsSession.current.domain` -> override `env = '-qa'`
3. Branch: neither dev nor qa -> `env = ''`
4. Return `f'https://marquee{env}.gs.com/s/products/{self.get_marquee_id()}/summary'`

---

### Asset.get_identifiers(self, as_of: dt.date = None) -> dict
Purpose: Get all asset identifiers valid as of a given date.

**Algorithm:**
1. Branch: `as_of` is falsy:
   a. Get `PricingContext.current`
   b. Branch: not entered -> enter context, get `pricing_date`
   c. Branch: entered -> get `pricing_date` directly
   d. Branch: `as_of` is `datetime` -> convert to `.date()`
2. Build `valid_ids` as set of `AssetIdentifier` values
3. Call `GsAssetApi.get_asset_xrefs(self.get_marquee_id())`
4. For each xref, branch: `start_date <= as_of <= end_date` -> collect matching identifiers (uppercased, filtered to valid_ids)
5. Return identifiers dict

---

### Asset.get_identifier(self, id_type: AssetIdentifier, as_of: dt.date = None) -> Optional[str]
Purpose: Get a single identifier value by type. Cached with TTLCache (256 entries, 600s TTL).

**Algorithm:**
1. Cache key: `(marquee_id, id_type, as_of)`; lock: `threading.RLock()`
2. Branch: `id_type == AssetIdentifier.MARQUEE_ID` -> return `self.get_marquee_id()`
3. Call `self.get_identifiers(as_of=as_of)`
4. Return `ids.get(id_type.value)`

---

### Asset.get_asset_measures(self) -> List[AssetMeasure]
Purpose: Get available measures for the asset from the data availability endpoint.

**Algorithm:**
1. GET `/data/measures/{marquee_id}/availability`
2. Branch: `availability_response['data']` is truthy:
   a. For each `measure_set`, create `AssetMeasure.from_dict(measure_set)`
   b. Branch: measure_set has keys `{'type', 'frequency', 'datasetField'}` -> add to set
3. Return list of the set

---

### Asset.get_data_series(self, measure: DataMeasure, dimensions: Optional[DataDimensions] = None, frequency: Optional[DataFrequency] = None, start: Optional[DateOrDatetime] = None, end: Optional[DateOrDatetime] = None, dates: List[dt.date] = None, operator: DataAggregationOperator = None) -> pd.Series
Purpose: Get a time series for a specific data measure.

**Algorithm:**
1. Call `self.get_data_coordinate(measure, dimensions, frequency)`
2. Branch: coordinate is `None` -> raise `MqValueError` (no coordinate found)
3. Branch: coordinate.dataset_id is `None` -> raise `MqValueError` (measure not found)
4. Return `coordinate.get_series(start=start, end=end, dates=dates, operator=operator)`

---

### Asset.get_latest_close_price(self) -> float
Purpose: Get the most recent close price.

**Algorithm:**
1. Get data coordinate for `DataMeasure.CLOSE_PRICE`, `None`, `DataFrequency.DAILY`
2. Branch: coordinate is `None` -> raise `MqValueError`
3. Return `coordinate.last_value()`

---

### Asset.get_close_price_for_date(self, date: dt.date) -> pd.Series
Purpose: Get close price for a specific date.

**Algorithm:**
1. Delegate to `self.get_data_series(DataMeasure.CLOSE_PRICE, None, DataFrequency.DAILY, date, date)`

---

### Asset.get_close_prices(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> pd.Series
Purpose: Get close price time series over a date range.

**Algorithm:**
1. Delegate to `self.get_data_series(DataMeasure.CLOSE_PRICE, None, DataFrequency.DAILY, start, end)`

---

### Asset.get_hloc_prices(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), interval_frequency: IntervalFrequency = IntervalFrequency.DAILY) -> pd.DataFrame
Purpose: Get high, low, open, close price data.

**Algorithm:**
1. Branch: `asset_class == AssetClass.Equity`:
   a. Branch: `interval_frequency == DAILY` -> `dates=None`, `use_field=False`
   b. Branch: `interval_frequency == MONTHLY`:
      - Build list of month-end dates from start to end using `calendar.monthrange` and `relativedelta`
      - `use_field=True`
   c. Branch: else -> raise `MqValueError` (unsupported frequency)
   d. Build 4 partial tasks (high/MAX, low/MIN, open/FIRST, close/LAST) using adjusted price measures
   e. Run tasks via `ThreadPoolManager.run_async(tasks)`
   f. Build DataFrame with columns `{high, low, open, close}`
2. Branch: `asset_class == AssetClass.FX`:
   a. Branch: `interval_frequency != DAILY` -> raise `MqValueError`
   b. Load from `Dataset('FX_HLOC')`, drop `assetId`/`updateTime`, reindex to `{high, low, open, close}`
3. Branch: else -> raise `MqValueError` (unsupported AssetClass)
4. Return `df.dropna()`

---

### Asset.get_type(self) -> AssetType [abstractmethod]
Purpose: Return the asset type. Must be overridden by subclasses.

---

### Asset.entity_type(cls) -> EntityType [classmethod]
Purpose: Return `EntityType.ASSET`.

---

### Asset.data_dimension (property) -> str
Purpose: Return `'assetId'`.

---

### Asset.get(cls, id_value: str, id_type: AssetIdentifier, as_of = None, exchange_code = None, asset_type = None, sort_by_rank: bool = False) -> Optional[Asset] [classmethod]
Purpose: Convenience class method delegating to `SecurityMaster.get_asset`.

**Algorithm:**
1. Call `SecurityMaster.get_asset(id_value, id_type, as_of, exchange_code, asset_type, sort_by_rank)`
2. Return result

---

### SecMasterAsset.__init__(self, id_: str, asset_type: AssetType, asset_class: AssetClass, name: str, exchange: Optional[str] = None, currency: Optional[str] = None, parameters: AssetParameters = None, entity: Optional[Dict] = None)
Purpose: Initialize a Security Master sourced asset.

**Algorithm:**
1. Call `Asset.__init__(self, id_, asset_class=..., name=..., ...)`
2. Store `__asset_type` and `__cached_identifiers = None`

---

### SecMasterAsset.get_type(self) -> AssetType
Purpose: Return stored `__asset_type`.

---

### SecMasterAsset.get_marquee_id(self) -> str
Purpose: Resolve and return the Marquee ID from cached identifiers, raising if not found.

**Algorithm:**
1. Call `self.get_identifier(SecurityIdentifier.ASSET_ID)` to get marquee_id
2. Set `self.__id = marquee_id` (updates in case of context change)
3. Branch: marquee_id is `None`:
   a. Resolve current pricing date (entered/not-entered PricingContext)
   b. Raise `MqValueError` with pricing date info
4. Return marquee_id

---

### SecMasterAsset.get_identifier(self, id_type: Union[AssetIdentifier, SecurityIdentifier], as_of: dt.date = None)
Purpose: Get a single identifier, enforcing `SecurityIdentifier` type.

**Algorithm:**
1. Branch: `id_type` is not `SecurityIdentifier` -> raise `MqTypeError`
2. Branch: `id_type == SecurityIdentifier.GSID` -> return from `self.entity['identifiers']`
3. Branch: `id_type == SecurityIdentifier.ID` -> return `self.entity['id']`
4. Call `self.get_identifiers(as_of=as_of)`, return `ids.get(id_type.value, None)`

---

### SecMasterAsset.get_identifiers(self, as_of: dt.date = None) -> dict
Purpose: Get all identifiers from cached temporal xrefs, resolving as-of date from PricingContext if needed.

**Algorithm:**
1. Branch: `__cached_identifiers is None` -> call `__load_identifiers()`
2. Branch: `as_of is None` -> resolve from PricingContext (entered/not-entered)
3. For each `SecurityIdentifier`, look up history in cache; find xref where `start_date <= as_of <= end_date`; break on first match
4. Always add `ID` from `self.entity['id']` and `GSID` from `self.entity['identifiers']['gsid']`
5. Branch: `ASSET_ID` not in identifiers AND `__asset_type == AssetType.CURRENCY` -> add from `self.entity['identifiers']['assetId']`
6. Return identifiers dict

---

### SecMasterAsset.get_data_series(self, measure, dimensions, frequency, start, end, dates, operator) -> pd.Series
Purpose: Override to validate Marquee ID range before delegating to parent.

**Algorithm:**
1. Get data coordinate; branch: `None` -> raise `MqValueError`
2. Call `coordinate.get_range(start, end)` to get `range_start, range_end`
3. Call `self.__is_validate_range(start=range_start, end=range_end)`
4. If valid, wrap in `PricingContext(range_start)` and call `super().get_data_series(...)`

---

### SecMasterAsset.get_hloc_prices(self, start, end, interval_frequency) -> pd.DataFrame
Purpose: Override to validate Marquee ID range before delegating to parent.

**Algorithm:**
1. Call `self.__is_validate_range(start=start, end=end)`
2. If valid, wrap in `PricingContext(start)` and call `super().get_hloc_prices(...)`

---

### SecMasterAsset.__is_validate_range(self, start: DateOrDatetime, end: DateOrDatetime = dt.date.today()) -> bool
Purpose: Validate that exactly one Marquee ID exists across the requested date range.

**Algorithm:**
1. Branch: `__cached_identifiers is None` -> call `__load_identifiers()`
2. Branch: `start` is `datetime` -> `start_date = start.date` (NOTE: missing `()` -- see Bugs Found)
3. Branch: `end` is `datetime` -> `end_date = end.date` (NOTE: missing `()` -- see Bugs Found)
4. Resolve `start_marquee_id` and `end_marquee_id` via `PricingContext` wrapping `self.get_marquee_id()`
5. Branch: either is `None` OR they differ -> raise `MqValueError`
6. Iterate assetId xrefs, skip non-overlapping ranges, collect unique Marquee IDs and compute overlap ranges
7. Track `output_range_start` / `output_range_end` (NOTE: `output_range_start` initial assignment uses `output_range_start` instead of `range_start` on first iteration -- see Bugs Found)
8. Branch: `len(marquee_ids) > 1` -> raise `MqValueError` (multiple IDs)
9. Branch: `len(marquee_ids) == 0` -> raise `MqValueError` (no ID)
10. Return `True`

---

### SecMasterAsset.__load_identifiers(self) -> None
Purpose: Lazily load temporal identifiers from the Security Master API.

**Algorithm:**
1. Branch: `__cached_identifiers is None`:
   a. GET `/markets/securities/{entity['id']}/identifiers`
   b. For each temporal_xref in results:
      - Build dict with `start_date`, `update_date`, `value`
      - Branch: `endDate == "9999-99-99"` -> set `end_date = dt.datetime.max.date()`
      - Branch: else -> parse `endDate` as date
   c. Store in `defaultdict(list)` keyed by `id_type`
   d. Assign to `self.__cached_identifiers`

---

### Stock.get_type(self) -> AssetType
Purpose: Return `AssetType.STOCK`.

### Stock.get_currency(self) -> Optional[Currency]
Purpose: Return `self.currency`.

### Stock.get_thematic_beta(self, basket_identifier: str, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> pd.DataFrame
Purpose: Get thematic beta for the stock relative to a given basket.

**Algorithm:**
1. Call `GsAssetApi.resolve_assets(identifier=[basket_identifier], fields=['id', 'type'], limit=1)`
2. Extract `_id` and `_type` from response using `pydash.get`
3. Branch: `len(response) == 0` or `_id is None` -> raise `MqValueError`
4. Branch: `_type` not in `BasketType.to_list()` -> raise `MqValueError`
5. Build `DataQuery` with `gsid` and `basketId` where-clause
6. Query `GsDataApi.query_data` against `IndicesDatasets.THEMATIC_FACTOR_BETAS_STANDARD`
7. Build DataFrame from response dicts with columns `{date, gsid, basketId, thematicBeta}`
8. Return `df.set_index('date')`

---

### Cross.__init__(self, id_: str, name: str, entity: Optional[Dict] = None, asset_class: Optional[Union[AssetClass, str]] = AssetClass.FX)
Purpose: Initialize Cross asset, coercing string asset_class to enum.

**Algorithm:**
1. Branch: `asset_class` is `str` -> `get_enum_value(AssetClass, asset_class)`
2. Call `Asset.__init__`

### Cross.get_type(self) -> AssetType
Purpose: Return `AssetType.CROSS`.

---

### Future.__init__(self, id_: str, asset_class: Union[AssetClass, str], name: str, currency: Optional[Currency] = None, entity: Optional[Dict] = None)
Purpose: Initialize Future, coercing string asset_class.

### Future.get_type(self) -> AssetType
Purpose: Return `AssetType.FUTURE`.

### Future.get_currency(self) -> Optional[Currency]
Purpose: Return `self.currency`.

---

### (All simple asset subclasses follow the same pattern: `__init__` -> `Asset.__init__`, `get_type()` -> return specific `AssetType`. Listed above in Type Definitions.)

---

### ETF.__init__(self, id_: str, asset_class: AssetClass, name: str, exchange=None, currency=None, entity=None)
Purpose: Initialize ETF with dual inheritance from Asset and PositionedEntity.

**Algorithm:**
1. Call `Asset.__init__(self, id_, asset_class, name, exchange, currency, entity=entity)`
2. Call `PositionedEntity.__init__(self, id_, EntityType.ASSET)`

### ETF.get_type(self) -> AssetType
Purpose: Return `AssetType.ETF`.

### ETF.get_currency(self) -> Optional[Currency]
Purpose: Return `self.currency`.

---

### Security.__init__(self, json: dict)
Purpose: Construct Security from a raw dict, separating `identifiers` from other attributes.

**Algorithm:**
1. For each key-value in json:
   a. Branch: key == `'identifiers'` -> store as `self._ids` dict
   b. Branch: else -> `setattr(self, k, v)`

### Security.__str__(self) -> str
Purpose: Return string of non-private attributes.

### Security.get_identifiers(self) -> dict
Purpose: Return deep copy of `self._ids`.

---

### SecurityMaster.__gs_asset_to_asset(cls, gs_asset: GsAsset) -> Asset [classmethod, private]
Purpose: Convert a `GsAsset` to the appropriate concrete `Asset` subclass.

**Algorithm:**
1. Extract `asset_type = gs_asset.type.value`
2. Serialize entity to dict via `json.loads(json.dumps(gs_asset.as_dict(), cls=JSONEncoder))`
3. Match `asset_type` against `GsAssetType` values in a long if-chain (30 branches):
   - `Single_Stock` -> `Stock`
   - `ETF` -> `ETF`
   - `Index`, `Access`, `Multi_Asset_Allocation`, `Risk_Premia`, `Systematic_Hedging` -> `Index` (lazy import)
   - `Custom_Basket`, `Research_Basket` -> `Basket` (lazy import)
   - `Future` -> `Future`
   - `Cross` -> `Cross`
   - `Currency` -> `Currency`
   - `Rate` -> `Rate`
   - `Cash` -> `Cash`
   - `WeatherIndex` -> `WeatherIndex`
   - `Swap` -> `Swap`
   - `Option` -> `Option`
   - `CommodityReferencePrice` -> `CommodityReferencePrice`
   - `CommodityNaturalGasHub` -> `CommodityNaturalGasHub`
   - `CommodityEUNaturalGasHub` -> `CommodityEUNaturalGasHub`
   - `CommodityPowerNode` -> `CommodityPowerNode`
   - `CommodityPowerAggregatedNodes` -> `CommodityPowerAggregatedNodes`
   - `Bond` -> `Bond` (with fallback `assetClass or AssetClass.Credit`)
   - `Commodity` -> `Commodity`
   - `FutureMarket` -> `FutureMarket`
   - `FutureContract` -> `FutureContract`
   - `Cryptocurrency` -> `Cryptocurrency`
   - `Forward` -> `Forward`
   - `Fund` -> `Fund`
   - `Default_Swap` -> `DefaultSwap`
   - `Swaption` -> `Swaption`
   - `Binary` -> `Binary`
   - `XccySwapMTM` -> `XccySwapMTM`
   - `Mutual_Fund` -> `MutualFund`
4. Branch: no match -> raise `TypeError` (unsupported asset type)

---

### SecurityMaster.__asset_type_to_gs_types(cls, asset_type: AssetType) -> Tuple[GsAssetType, ...] [classmethod, private]
Purpose: Map local `AssetType` to tuple of `GsAssetType` values for query filtering.

**Algorithm:**
1. Lookup in hardcoded `asset_map` dict (7 entries: STOCK, INDEX, ETF, CUSTOM_BASKET, RESEARCH_BASKET, FUTURE, RATE)
2. Return `asset_map.get(asset_type)` -- returns `None` for unmapped types

---

### SecurityMaster.set_source(cls, source: SecurityMasterSource) [classmethod]
Purpose: Set the class-level `_source` to toggle between Asset Service and Security Master backends.

---

### SecurityMaster.get_asset_query(cls, id_value: Union[str, List[str]], id_type: Union[AssetIdentifier, SecurityIdentifier], as_of = None, exchange_code = None, asset_type = None) -> Tuple[Dict, dt.datetime] [classmethod]
Purpose: Build query dict and resolve as_of datetime for asset lookups.

**Algorithm:**
1. Branch: `as_of` is falsy -> resolve from PricingContext (entered/not-entered)
2. Branch: `as_of` is `dt.date` -> combine with `dt.time(0,0)` and `dt.timezone.utc`
3. Build query: key is `"id"` if MARQUEE_ID else `id_type.value.lower()`; value is `id_value`
4. Branch: `exchange_code is not None` -> add `query['exchange']`
5. Branch: `asset_type is not None` -> add `query['type']` from `__asset_type_to_gs_types`
6. Return `(query, as_of)`

---

### SecurityMaster._get_asset_results(cls, results, sort_by_rank) -> Asset [classmethod]
Purpose: Extract single asset result from a results collection.

**Algorithm:**
1. Branch: `sort_by_rank` is True -> `result = pydash.get(results, '0')`, if truthy convert via `GsAsset.from_dict`
2. Branch: `sort_by_rank` is False -> `result = next(iter(results), None)`
3. Branch: result is truthy -> convert via `__gs_asset_to_asset`
4. Return `None` if no result

---

### SecurityMaster._get_many_assets_results(cls, results) -> List[Asset] [classmethod]
Purpose: Convert list of results to list of Asset objects.

**Algorithm:**
1. Branch: `results is not None` -> map `__gs_asset_to_asset` over results
2. Branch: `results is None` -> return `[]`

---

### SecurityMaster.get_asset(cls, id_value: str, id_type, as_of=None, exchange_code=None, asset_type=None, sort_by_rank=True, fields=None) -> Asset [classmethod]
Purpose: Look up a single asset by identifier.

**Algorithm:**
1. Branch: `_source == SECURITY_MASTER`:
   a. Branch: `id_type` not `SecurityIdentifier` -> raise `MqTypeError`
   b. Branch: `exchange_code` or `asset_type` -> raise `NotImplementedError`
   c. Delegate to `_get_security_master_asset(id_value, id_type, as_of, fields)`
2. Branch: `id_type is AssetIdentifier.MARQUEE_ID`:
   a. Call `GsAssetApi.get_asset(id_value)` directly
   b. Convert and return via `__gs_asset_to_asset`
3. Build query via `get_asset_query`
4. Branch: `sort_by_rank` True -> `GsAssetApi.get_many_assets(as_of, return_type=dict, order_by=['>rank'], **query)`
5. Branch: `sort_by_rank` False -> `GsAssetApi.get_many_assets(as_of, **query)`
6. Return via `_get_asset_results`

---

### SecurityMaster.get_asset_async(cls, id_value, id_type, as_of=None, exchange_code=None, asset_type=None, sort_by_rank=True, fields=None) -> Asset [async classmethod]
Purpose: Async version of `get_asset`. Same branching logic using `GsAssetApi.*_async` and `_get_security_master_asset_async`.

---

### SecurityMaster.get_many_assets(cls, id_values: List[str], id_type: AssetIdentifier, limit: int = 100, as_of = None, exchange_code = None, sort_by_rank: bool = True) -> List[Asset] [classmethod]
Purpose: Look up multiple assets by identifiers.

**Algorithm:**
1. Create optional tracing span
2. Branch: span exists and is recording -> set tag with count
3. Build query via `get_asset_query(id_values, ...)`
4. Branch: `sort_by_rank` -> pass `order_by=['>rank']`
5. Return via `_get_many_assets_results`

---

### SecurityMaster.get_many_assets_async(cls, ...) -> List[Asset] [async classmethod]
Purpose: Async version of `get_many_assets`. Same branching logic.

---

### SecurityMaster._get_security_master_asset_params(cls, id_value: str, id_type: SecurityIdentifier, as_of = None, fields = None) -> dict [classmethod]
Purpose: Build request params for Security Master API.

**Algorithm:**
1. Default `as_of` to `dt.datetime(2100, 1, 1)` if None
2. Build params dict with `{type_: id_value, 'asOfDate': as_of.strftime(...)}`
3. Branch: `fields is not None` -> merge with base fields `{'identifiers', 'assetClass', 'type', 'currency', 'exchange', 'id'}`, add to params
4. Return params

---

### SecurityMaster._get_security_master_asset_response(cls, response) -> SecMasterAsset [classmethod]
Purpose: Parse Security Master API response into a `SecMasterAsset`.

**Algorithm:**
1. Branch: `response['totalResults'] == 0` -> return `None`
2. Extract first result: `asset_dict = response['results'][0]`
3. Extract `asset_id` from `identifiers.assetId` (may be None)
4. Extract `asset_name`, `asset_exchange` (via nested `.get("exchange").get("name")`), `asset_currency`
5. Try: convert `asset_dict['type']` to `AssetType`, `asset_dict['assetClass']` to `AssetClass`
6. Branch: `ValueError` -> raise `NotImplementedError` (unsupported type/class)
7. Return `SecMasterAsset(id_=asset_id, ...)`

---

### SecurityMaster._get_security_master_asset(cls, id_value, id_type, as_of=None, fields=None) -> SecMasterAsset [classmethod]
Purpose: Sync Security Master asset lookup.

**Algorithm:**
1. Build params via `_get_security_master_asset_params`
2. GET `/markets/securities` with params
3. Parse via `_get_security_master_asset_response`

---

### SecurityMaster._get_security_master_asset_async(cls, id_value, id_type, as_of=None, fields=None) -> SecMasterAsset [async classmethod]
Purpose: Async version of `_get_security_master_asset`.

---

### SecurityMaster.get_identifiers(cls, id_values: List[str], id_type: SecurityIdentifier, as_of=None, start=None, end=None) -> dict [classmethod]
Purpose: Get identifiers for given assets from Security Master.

**Algorithm:**
1. Branch: `_source != SECURITY_MASTER` -> raise `NotImplementedError`
2. Default `as_of` to `dt.datetime.now()`, `start` to `1970-01-01`, `end` to `2100-01-01`
3. GET `/markets/securities` with `{type_: id_values, fields: ['id', 'identifiers'], asOfDate: ...}`
4. Build `id_map` from results: `identifiers[type_] -> asset['id']`
5. Branch: `len(id_map) == 0` -> return `{}`
6. For each mapped ID, GET `/markets/securities/{v}/identifiers`, accumulate results
7. Return output dict

---

### SecurityMaster.asset_type_to_str(asset_class: AssetClass, asset_type: AssetType) -> str [staticmethod]
Purpose: Convert AssetType to string representation for Security Master API.

**Algorithm:**
1. Branch: `asset_type == STOCK` -> return `"Common Stock"`
2. Branch: `asset_type == INDEX and asset_class == Equity` -> return `"Equity Index"`
3. Branch: else -> return `asset_type.value`

---

### SecurityMaster.get_all_identifiers_gen(cls, class_: AssetClass = None, types: Optional[List[AssetType]] = None, as_of = None, *, id_type = SecurityIdentifier.ID, use_offset_key=True, sleep=0.5) -> Generator[dict, None, None] [classmethod]
Purpose: Paginated generator that yields dicts of identifiers for all matching assets.

**Algorithm:**
1. Branch: `_source != SECURITY_MASTER` -> raise `NotImplementedError`
2. Default `as_of` to `dt.datetime.now()`
3. Branch: `types is not None` -> map via `asset_type_to_str(class_, type)` to build string set
4. Build params with `fields`, `asOfDate`, `limit=_page_size`, `type`
5. Loop:
   a. Call `_get_with_retries('/markets/securities', params)`
   b. Branch: `totalResults == 0` -> return (stop generator)
   c. For each result:
      - Branch: `class_ is None` or `e['assetClass'] == class_.value` -> include
      - Copy `id` into identifiers box
      - Key by `box[id_type.value]`
      - Branch: `key in box` -> log debug (duplicate key)
   d. Yield output dict
   e. Branch: `use_offset_key`:
      - Branch: `'offsetKey' not in r` -> return (stop)
      - Set `params['offsetKey']`
   f. Branch: not `use_offset_key`:
      - Increment `params['offset']` by `_page_size`
      - Branch: `offset + limit > 10000` -> log warning and return
   g. `time.sleep(sleep)`

---

### SecurityMaster.get_all_identifiers(cls, class_=None, types=None, as_of=None, *, id_type=SecurityIdentifier.ID, use_offset_key=True, sleep=0.5) -> Dict[str, dict] [classmethod]
Purpose: Non-generator wrapper that accumulates all pages from `get_all_identifiers_gen`.

**Algorithm:**
1. Create generator via `get_all_identifiers_gen(...)`
2. Loop: `accumulator.update(next(gen))`
3. Catch `StopIteration` -> return accumulator

---

### SecurityMaster.map_identifiers(cls, input_type: SecurityIdentifier, ids: Iterable[str], output_types: Iterable[SecurityIdentifier] = frozenset([SecurityIdentifier.GSID]), start_date=None, end_date=None, as_of_date=None) -> Dict[dt.date, dict] [classmethod]
Purpose: Map between identifier types for given security IDs.

**Algorithm:**
1. Branch: `ids` is `str` -> raise `MqTypeError` (expected iterable)
2. Define inner `get_asset_id_type` which maps `SecurityIdentifier` to `GsIdType` (raises `MqValueError` on `KeyError`)
3. Branch: `_source == ASSET_SERVICE`:
   a. Convert `output_types` to list
   b. Branch: `len(output_types) != 1` -> raise `MqValueError`
   c. Branch: `start_date or end_date` not None -> raise `MqValueError`
   d. Default `as_of_date` to `dt.date.today()` if None
   e. Call `GsAssetApi.map_identifiers(...)` with `multimap=True`
   f. Branch: empty result -> return it
   g. Restructure and return as `{as_of_str: {k: {output_type: v}}}`
4. Branch: `_source == SECURITY_MASTER` (asserted):
   a. Build params with `input_type`, `toIdentifiers`, `compact=True`
   b. Branch: `as_of_date is not None`:
      - Branch: `start_date or end_date` -> raise `MqValueError` (cannot mix)
      - Set both `startDate` and `endDate` to `as_of_date`
   c. Branch: `start_date is not None` -> add to params
   d. Branch: `end_date is not None` -> add to params
   e. Call `_get_with_retries('/markets/securities/map', params)`
   f. Branch: `results` is dict -> return directly
   g. Iterate result rows, expanding date ranges day by day:
      - Branch: `output_type == "ric"`:
        - Branch: `RIC in output_types` -> append to `ric` list (deduplicated)
        - Branch: `ASSET_ID in output_types and "assetId" in row` -> append to `assetId` list (deduplicated)
      - Branch: `output_type == "bbg"`:
        - Branch: `BBG in output_types` -> append raw value
        - Branch: `BBID in output_types` -> append with exchange suffix if present, else raw
        - Branch: `BCID in output_types and compositeExchange present` -> append with composite exchange
      - Branch: else (generic type):
        - Branch: `SecurityIdentifier(output_type) in output_types` -> append (deduplicated)
   h. Convert datetime keys to `%Y-%m-%d` strings and return

## State Mutation
- `SecurityMaster._source`: Class variable, modified by `SecurityMaster.set_source()`
- `SecMasterAsset.__cached_identifiers`: `None` initially, lazily populated by `__load_identifiers()`, read by `get_identifiers()`, `get_marquee_id()`, `__is_validate_range()`
- `SecMasterAsset.__id`: Updated by `get_marquee_id()` when the assetId changes across pricing contexts
- `Asset.get_identifier` cache: Global `TTLCache(256, 600)` shared across all `Asset` instances, protected by `threading.RLock()`
- Thread safety: `Asset.get_identifier` uses a module-level `RLock` for cache access. `SecMasterAsset.__cached_identifiers` has no explicit synchronization -- not thread-safe for concurrent first-access.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `Asset.get_data_series` | No data coordinate found for params |
| `MqValueError` | `Asset.get_data_series` | Measure not found (dataset_id is None) |
| `MqValueError` | `Asset.get_latest_close_price` | No data coordinate found |
| `MqValueError` | `Asset.get_hloc_prices` | Unsupported `IntervalFrequency` for equity |
| `MqValueError` | `Asset.get_hloc_prices` | Non-daily frequency for FX |
| `MqValueError` | `Asset.get_hloc_prices` | Unsupported `AssetClass` for HLOC |
| `MqValueError` | `SecMasterAsset.get_marquee_id` | No Marquee ID as of current pricing date |
| `MqTypeError` | `SecMasterAsset.get_identifier` | `id_type` not a `SecurityIdentifier` |
| `MqValueError` | `SecMasterAsset.__is_validate_range` | Start and end Marquee IDs differ or are None |
| `MqValueError` | `SecMasterAsset.__is_validate_range` | Multiple Marquee IDs in range |
| `MqValueError` | `SecMasterAsset.__is_validate_range` | No Marquee ID in range |
| `MqValueError` | `SecMasterAsset.get_data_series` | No data coordinate found |
| `MqValueError` | `Stock.get_thematic_beta` | Basket not found |
| `MqValueError` | `Stock.get_thematic_beta` | Asset is not a basket type |
| `MqTypeError` | `SecurityMaster.get_asset` | `id_type` not `SecurityIdentifier` (Security Master source) |
| `NotImplementedError` | `SecurityMaster.get_asset` | `exchange_code`/`asset_type` with Security Master source |
| `NotImplementedError` | `SecurityMaster.get_asset_async` | Same as above |
| `TypeError` | `SecurityMaster.__gs_asset_to_asset` | Unsupported asset type value |
| `NotImplementedError` | `SecurityMaster._get_security_master_asset_response` | `ValueError` when constructing `AssetType` or `AssetClass` from dict |
| `NotImplementedError` | `SecurityMaster.get_identifiers` | Source is not `SECURITY_MASTER` |
| `NotImplementedError` | `SecurityMaster.get_all_identifiers_gen` | Source is not `SECURITY_MASTER` |
| `MqTypeError` | `SecurityMaster.map_identifiers` | `ids` is a bare string |
| `MqValueError` | `SecurityMaster.map_identifiers` | Not exactly one output type (Asset Service) |
| `MqValueError` | `SecurityMaster.map_identifiers` | `start_date`/`end_date` used with Asset Service |
| `MqValueError` | `SecurityMaster.map_identifiers` | Both `as_of_date` and `start_date`/`end_date` provided (Security Master) |
| `MqValueError` | `SecurityMaster.map_identifiers` inner `get_asset_id_type` | Unsupported `SecurityIdentifier` to `GsIdType` mapping |

## Edge Cases
- `Asset.get_identifiers`: If no xref spans the `as_of` date, returns empty dict (last matching xref wins, earlier ones are overwritten)
- `Asset.get_identifier` with `AssetIdentifier.MARQUEE_ID`: Short-circuits without calling `get_identifiers`
- `Asset.get_close_prices` default `end=dt.date.today()`: Evaluated at function definition time, not call time (stale default if process is long-running)
- `Asset.get_hloc_prices` with monthly frequency: Date list includes both start and end months; the loop may produce dates past `end` but the `while d < end` check prevents inclusion
- `SecMasterAsset.get_marquee_id` raises when assetId is None -- callers must handle this for assets that lack a Marquee mapping
- `SecMasterAsset.get_identifiers`: Always injects `ID` and `GSID` even if they are also present in temporal xrefs, potentially overriding temporal values
- `SecMasterAsset.__is_validate_range`: `output_range_start` and `output_range_end` are computed but never used after the loop -- dead code
- `SecurityMaster.__asset_type_to_gs_types` returns `None` for unmapped types, which will cause `get_asset_query` to set `query['type'] = None` (iteration over None raises TypeError)
- `SecurityMaster._get_security_master_asset_response`: `asset_exchange` extraction uses chained `.get("exchange").get("name")` which will `AttributeError` if `"exchange"` key is present but value is None
- `SecurityMaster.get_all_identifiers_gen`: `key in box` duplicate check on line 1986 checks if key is in the identifiers dict (should probably be `key in output`)
- `SecurityMaster.map_identifiers` with Security Master: If `results` is a list, the date expansion iterates day by day which can be slow for large ranges
- `Security.__init__`: Only the `identifiers` key is handled specially; all other keys become instance attributes via `setattr`, which can shadow methods

## Bugs Found
- Line 796: `start_date = start.date` -- missing parentheses, should be `start.date()`. When `start` is a `datetime`, this assigns the bound method, not the date value. Same issue on line 801 for `end.date`. (OPEN)
- Line 830-831: `output_range_start` first assignment: the condition `if output_range_start is not None else output_range_start` means on first iteration (when it is `None`), `output_range_start` stays `None` instead of being set to `range_start`. Same bug for `output_range_end` on lines 833-834. These variables are never used downstream, so the bug is latent dead code. (OPEN)
- Line 1307: `XccySwapMTM.get_type()` returns `AssetType.XccySwapMTM` but `XccySwapMTM` is not defined in the local `AssetType` enum in this file. It is defined in `GsAssetType` in `gs_quant/target/common.py`. Calling `get_type()` on an `XccySwapMTM` instance will raise `AttributeError`. (OPEN)
- Line 1481: `MutualFund` constructor in `__gs_asset_to_asset` passes `gs_asset.asset_class` (snake_case) instead of `gs_asset.assetClass` (camelCase), which may be `None` or raise `AttributeError` depending on the `GsAsset` implementation. (OPEN)
- Line 1986: `if key in box` -- likely intended as `if key in output` to detect duplicate keys across results. As written, it checks if the key string exists as a key in the same identifiers dict it was just read from, which is always true. (OPEN)

## Coverage Notes
- Branch count: ~120+ distinct branches across the module
- Key high-branch-count methods:
  - `SecurityMaster.__gs_asset_to_asset`: ~30 branches (one per asset type + fallthrough TypeError)
  - `SecurityMaster.map_identifiers`: ~20 branches (source check, type validation, date params, output_type matching with ric/bbg/generic, dedup, exchange handling)
  - `Asset.get_hloc_prices`: ~8 branches (asset class, interval frequency, monthly date generation)
  - `SecMasterAsset.__is_validate_range`: ~10 branches (datetime conversion, None checks, marquee ID comparison, overlap counting)
  - `SecurityMaster.get_all_identifiers_gen`: ~8 branches (source check, class filter, pagination mode, offset limit)
- Pragmas: No `pragma: no cover` markers found in the file
- The async variants (`get_asset_async`, `get_many_assets_async`, `_get_security_master_asset_async`) mirror their sync counterparts and share the same branch structure
