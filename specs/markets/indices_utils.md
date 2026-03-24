# indices_utils.py

## Summary
Utility module for retrieving, filtering, and analyzing Goldman Sachs flagship and custom index baskets. Provides enumerations for basket metadata (type, region, style, pricing, weighting) and functions to query basket listings, constituents, performance data, and dataset coverage via the GS Asset and Data APIs.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.data` (GsDataApi), `gs_quant.api.gs.monitors` (GsMonitorsApi), `gs_quant.api.utils` (ThreadPoolManager), `gs_quant.base` (EnumBase), `gs_quant.common` (AssetClass), `gs_quant.datetime.date` (prev_business_date), `gs_quant.session` (GsSession), `gs_quant.target.data` (DataQuery)
- External: `datetime` (dt), `pandas` (pd), `enum` (Enum), `functools` (partial, reduce), `pydash` (get), `time` (sleep), `typing` (Dict, List, Optional, Union)

## Type Definitions

No dataclass/namedtuple definitions. All types are Enum classes (see below) and module-level functions.

## Enums and Constants

### BasketType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| CUSTOM_BASKET | `"Custom Basket"` | Custom basket type |
| RESEARCH_BASKET | `"Research Basket"` | Research basket type |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.
- `to_list(cls) -> List[str]`: Returns list of all member `.value` strings.

### CorporateActionType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ACQUISITION | `"Acquisition"` | Acquisition event |
| CASH_DIVIDEND | `"Cash Dividend"` | Cash dividend payout |
| IDENTIFIER_CHANGE | `"Identifier Change"` | Identifier change event |
| RIGHTS_ISSUE | `"Rights Issue"` | Rights issue event |
| SHARE_CHANGE | `"Share Change"` | Share change event |
| SPECIAL_DIVIDEND | `"Special Dividend"` | Special dividend payout |
| SPIN_OFF | `"Spin Off"` | Spin-off event |
| STOCK_DIVIDEND | `"Stock Dividend"` | Stock dividend payout |
| STOCK_SPLIT | `"Stock Split"` | Stock split event |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.
- `to_list(cls) -> List[str]`: Returns list of all member `.value` strings.

### CustomBasketStyles(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| AD_HOC_DESK_WORK | `"Ad Hoc Desk Work"` | Ad hoc desk work style |
| CLIENT_CONSTRUCTED_WRAPPER | `"Client Constructed/Wrapper"` | Client-constructed wrapper |
| CONSUMER | `"Consumer"` | Consumer sector style |
| ENERGY | `"Energy"` | Energy sector style |
| ENHANCED_INDEX_SOLUTIONS | `"Enhanced Index Solutions"` | Enhanced index solutions |
| ESG | `"ESG"` | ESG-focused |
| FACTORS | `"Factors"` | Factor-based |
| FINANCIALS | `"Financials"` | Financial sector style |
| FLAGSHIP | `"Flagship"` | Flagship basket |
| GEOGRAPHIC | `"Geographic"` | Geography-based |
| GROWTH | `"Growth"` | Growth style |
| HEALTHCARE | `"Health Care"` | Healthcare sector style |
| HEDGING | `"Hedging"` | Hedging-oriented |
| INDUSTRIALS | `"Industrials"` | Industrials sector style |
| MATERIALS | `"Materials"` | Materials sector style |
| MOMENTUM | `"Momentum"` | Momentum style |
| PIPG | `"PIPG"` | PIPG style |
| SECTORS_INDUSTRIES | `"Sectors/Industries"` | Sector/industry based |
| SIZE | `"Size"` | Size-based |
| STRUCTURED_ONE_DELTA | `"Structured One Delta"` | Structured one-delta |
| THEMATIC | `"Thematic"` | Thematic style |
| TMT | `"TMT"` | TMT sector style |
| UTILITIES | `"Utilities"` | Utilities sector style |
| VALUE | `"Value"` | Value style |
| VOLATILITY | `"Volatility"` | Volatility-focused |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### IndicesDatasets(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| BASKET_FUNDAMENTALS | `"BASKET_FUNDAMENTALS"` | Basket fundamentals dataset |
| COMPOSITE_THEMATIC_BETAS | `"COMPOSITE_THEMATIC_BETAS"` | Composite thematic betas |
| CREDIT_EOD_PRICING_V1_STANDARD | `"CREDIT_EOD_PRICING_V1_STANDARD"` | Credit EOD pricing |
| CORPORATE_ACTIONS | `"CA"` | Corporate actions dataset |
| GIRBASKETCONSTITUENTS | `"GIRBASKETCONSTITUENTS"` | GIR basket constituents |
| GSBASKETCONSTITUENTS | `"GSBASKETCONSTITUENTS"` | GS basket constituents |
| GSCB_FLAGSHIP | `"GSCB_FLAGSHIP"` | GS CB flagship price data |
| GSCREDITBASKETCONSTITUENTS | `"GSCREDITBASKETCONSTITUENTS"` | GS credit basket constituents |
| STS_FUNDAMENTALS | `"STS_FUNDAMENTALS"` | STS fundamentals dataset |
| STS_INDICATIVE_LEVELS | `"STS_INDICATIVE_LEVELS"` | STS indicative levels |
| THEMATIC_FACTOR_BETAS_STANDARD | `"THEMATIC_FACTOR_BETAS_V2_STANDARD"` | Thematic factor betas v2 |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### PriceType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| INDICATIVE_CLOSE_PRICE | `"indicativeClosePrice"` | Indicative close price |
| OFFICIAL_CLOSE_PRICE | `"officialClosePrice"` | Official close price |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.
- `to_list(cls) -> List[PriceType]`: Returns list of enum *members* (not `.value`), unlike the other `to_list` methods.

### Region(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| AMERICAS | `"Americas"` | Americas region |
| ASIA | `"Asia"` | Asia region |
| EM | `"EM"` | Emerging markets |
| EUROPE | `"Europe"` | Europe region |
| GLOBAL | `"Global"` | Global region |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### ResearchBasketStyles(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ASIA_EX_JAPAN | `"Asia ex-Japan"` | Asia ex-Japan style |
| EQUITY_THEMATIC | `"Equity Thematic"` | Equity thematic style |
| EUROPE | `"Europe"` | Europe style |
| FUND_OWNERSHIP | `"Fund Ownership"` | Fund ownership style |
| FUNDAMENTALS | `"Fundamentals"` | Fundamentals style |
| FX_OIL | `"FX/Oil"` | FX/Oil style |
| GEOGRAPHICAL_EXPOSURE | `"Geographical Exposure"` | Geographical exposure |
| HEDGE_FUND | `"Hedge Fund"` | Hedge fund style |
| IP_FACTORS | `"Investment Profile (IP) Factors"` | IP factors style |
| JAPAN | `"Japan"` | Japan style |
| MACRO | `"Macro"` | Macro style |
| MACRO_SLICE_STYLES | `"Macro Slice/Styles"` | Macro slice/styles |
| MUTUAL_FUND | `"Mutual Fund"` | Mutual fund style |
| POSITIONING | `"Positioning"` | Positioning style |
| PORTFOLIO_STRATEGY | `"Portfolio Strategy"` | Portfolio strategy |
| RISK_AND_LIQUIDITY | `"Risk & Liquidity"` | Risk and liquidity |
| SECTOR | `"Sector"` | Sector style |
| SHAREHOLDER_RETURN | `"Shareholder Return"` | Shareholder return |
| STYLE_FACTOR_AND_FUNDAMENTAL | `"Style, Factor and Fundamental"` | Style/factor/fundamental |
| STYLES_THEMES | `"Style/Themes"` | Styles/themes |
| TACTICAL_RESEARCH | `"Tactical Research"` | Tactical research |
| THEMATIC | `"Thematic"` | Thematic style |
| US | `"US"` | US style |
| WAVEFRONT_COMPONENTS | `"Wavefront Components"` | Wavefront components |
| WAVEFRONT_PAIRS | `"Wavefront Pairs"` | Wavefront pairs |
| WAVEFRONTS | `"Wavefronts"` | Wavefronts |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### ReturnType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| GROSS_RETURN | `"Gross Return"` | Gross return methodology |
| PRICE_RETURN | `"Price Return"` | Price return methodology |
| TOTAL_RETURN | `"Total Return"` | Total return methodology |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### STSIndexType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ACCESS | `"Access"` | Access type |
| MULTI_ASSET_ALLOCATION | `"Multi-Asset Allocation"` | Multi-asset allocation |
| RISK_PREMIA | `"Risk Premia"` | Risk premia type |
| SYSTEMATIC_HEDGING | `"Systematic Hedging"` | Systematic hedging |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.
- `to_list(cls) -> List[str]`: Returns list of all member `.value` strings.

### WeightingStrategy(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| EQUAL | `"Equal"` | Equal weighting |
| MARKET_CAPITALIZATION | `"Market Capitalization"` | Market cap weighting |
| QUANTITY | `"Quantity"` | Quantity-based weighting |
| WEIGHT | `"Weight"` | Explicit weight strategy |

Methods:
- `__repr__(self) -> str`: Returns `self.value`.

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| QUERY_LIMIT | `int` | `1000` | Max results per scroll query to GsAssetApi |

## Functions/Methods

### get_my_baskets(user_id: str = None) -> Optional[pd.DataFrame]
Purpose: Retrieve a list of baskets the specified (or current) user is permissioned to view, via the monitors API.

**Algorithm:**
1. Branch: if `user_id` is `None` -> use `GsSession.current.client_id`; otherwise use provided `user_id`.
2. Construct tag string `f'Custom Basket:{user_id}'`.
3. Call `GsMonitorsApi.get_monitors(tags=tag)`.
4. Branch: if `len(response)` is truthy (non-empty list):
   a. Extract `row_groups` from `response[0].parameters.row_groups` using `pydash.get`.
   b. For each `row_group` in `row_groups`:
      - Extract `entity_ids` list from `row_group.entity_ids` (list comprehension on `.id`).
      - Call `GsAssetApi.get_many_assets_data(id=entity_ids, fields=['id', 'ticker', 'name', 'liveDate'])`.
      - For each `basket` in result, construct dict with keys: `monitor_name`, `id`, `ticker`, `name`, `live_date`.
      - Append to `my_baskets` list.
   c. Return `pd.DataFrame(my_baskets)`.
5. Branch: if `len(response)` is falsy (empty list) -> implicitly returns `None`.

### __get_baskets(fields: List[str] = [], basket_type: List[BasketType] = BasketType.to_list(), asset_class: List[AssetClass] = [AssetClass.Equity], region: List[Region] = None, styles: List[Union[CustomBasketStyles, ResearchBasketStyles]] = None, as_of: dt.datetime = None, **kwargs) -> Dict
Purpose: Internal helper to construct and execute a scrolling asset query for baskets.

**Algorithm:**
1. Compute `default_fields` as a set of 9 standard field names.
2. Initialize `query = {}`, merge `fields` with `default_fields` into a list.
3. Copy all `kwargs` into `query` dict.
4. Branch: if `region` is truthy -> add `query['region'] = region`.
5. Branch: if `styles` is truthy -> add `query['styles'] = styles`.
6. Build final query dict with `fields`, `type`, `asset_class`, `is_pair_basket=[False]`, `flagship=[True]`, plus all accumulated query params.
7. Call and return `GsAssetApi.get_many_assets_data_scroll(**query, as_of=as_of, limit=QUERY_LIMIT, scroll='1m')`.

### __get_dataset_id(asset_class: AssetClass, basket_type: BasketType, data_type: str) -> str
Purpose: Resolve the appropriate dataset ID based on asset class, basket type, and data type.

**Algorithm:**
1. Branch: if `asset_class == AssetClass.Equity` or `asset_class == AssetClass.Equity.value`:
   a. Branch: if `data_type == 'price'` -> return `IndicesDatasets.GSCB_FLAGSHIP.value` (`"GSCB_FLAGSHIP"`).
   b. Branch: elif `basket_type == BasketType.CUSTOM_BASKET` or `basket_type == BasketType.CUSTOM_BASKET.value` -> return `IndicesDatasets.GSBASKETCONSTITUENTS.value` (`"GSBASKETCONSTITUENTS"`).
   c. Branch: else -> return `IndicesDatasets.GIRBASKETCONSTITUENTS.value` (`"GIRBASKETCONSTITUENTS"`).
2. If asset class is not Equity -> raise `NotImplementedError`.

**Raises:** `NotImplementedError` when `asset_class` is not `Equity`.

### get_flagship_baskets(fields: List[str] = [], basket_type: List[BasketType] = BasketType.to_list(), asset_class: List[AssetClass] = [AssetClass.Equity], region: List[Region] = None, styles: List[Union[CustomBasketStyles, ResearchBasketStyles]] = None, as_of: dt.datetime = None, **kwargs) -> pd.DataFrame
Purpose: Retrieve flagship baskets as a DataFrame.

**Algorithm:**
1. Delegate to `__get_baskets(...)` with all parameters.
2. Return `pd.DataFrame(response)`.

### get_flagships_with_assets(identifiers: List[str], fields: List[str] = [], basket_type: List[BasketType] = BasketType.to_list(), asset_class: List[AssetClass] = [AssetClass.Equity], region: List[Region] = None, styles: List[Union[CustomBasketStyles, ResearchBasketStyles]] = None, as_of: dt.datetime = None, **kwargs) -> pd.DataFrame
Purpose: Retrieve flagship baskets that contain specified asset identifiers.

**Algorithm:**
1. Call `GsAssetApi.resolve_assets(identifier=identifiers, fields=['id'], limit=1)`.
2. Extract mqids: for each resolved asset, take `asset[0].id` via `pydash.get`.
3. Delegate to `__get_baskets(...)` passing `underlying_asset_ids=mqids` as extra kwarg.
4. Return `pd.DataFrame(response)`.

### get_flagships_performance(fields: List[str] = [], basket_type: List[BasketType] = BasketType.to_list(), asset_class: List[AssetClass] = [AssetClass.Equity], region: List[Region] = None, styles: List[Union[CustomBasketStyles, ResearchBasketStyles]] = None, start: dt.date = None, end: dt.date = None, **kwargs) -> pd.DataFrame
Purpose: Retrieve performance/pricing data for flagship baskets over a date range.

**Algorithm:**
1. Default `start` and `end` to `prev_business_date()` if not provided (branch on each being `None`).
2. Call `__get_baskets(...)` to retrieve basket metadata.
3. Build `baskets` dict keyed by `id`.
4. Call `__get_dataset_id(asset_class=asset_class[0], basket_type=basket_type[0], data_type='price')` to get the price dataset.
5. Call `GsDataApi.get_coverage(dataset_id=dataset_id, fields=['id'])`.
6. Filter coverage to only include baskets in the `baskets` dict, extracting `assetId` values into `mqids`.
7. Batch `mqids` into groups of 500.
8. For each batch, call `GsDataApi.query_data(...)` with `DataQuery(where={'assetId': batch}, startDate=start, endDate=end)` and accumulate results.
9. For each result row: look up corresponding basket metadata, merge via `b.update(data)`, remove `assetId` and `updateTime` keys, append to `performance` list.
10. Return `pd.DataFrame(performance)`.

### get_flagships_constituents(fields: List[str] = [], basket_type: List[BasketType] = BasketType.to_list(), asset_class: List[AssetClass] = [AssetClass.Equity], region: List[Region] = None, styles: List[Union[CustomBasketStyles, ResearchBasketStyles]] = None, start: dt.date = None, end: dt.date = None, **kwargs) -> pd.DataFrame
Purpose: Retrieve constituent data for flagship baskets, enriched with asset metadata.

**Algorithm:**
1. Default `start` and `end` to `prev_business_date()` if not provided.
2. Compute `basket_fields` as union of `fields` with 8 standard field names; recompute `fields` as union with `['id']`.
3. Call `__get_baskets(fields=['id'], ...)` to get basket IDs.
4. Extract `basket_ids` list from response.
5. Get coverage dataset via `__get_dataset_id(..., data_type='price')`.
6. Call `GsDataApi.get_coverage(dataset_id=cov_dataset_id, fields=basket_fields, include_history=True)`.
7. Build `basket_map` dict: `{assetId: {**coverage_entry, 'constituents': []}}` for entries whose `assetId` is in `basket_ids`.
8. For each basket in `basket_map.values()`:
   - Call `__get_dataset_id(asset_class=b['assetClass'], basket_type=b['type'], data_type='constituents')`.
   - Branch: if `dataset_id` is truthy -> group basket IDs by dataset in `basket_dataset_query_map`.
9. For each dataset/IDs pair, batch IDs into groups of 25; create `partial(GsDataApi.query_data, ...)` tasks.
10. Group tasks into pairs (batches of 2).
11. For each pair of tasks, call `ThreadPoolManager.run_async(task)`, accumulate results, then `sleep(1)`.
12. Flatten `constituents_data` via `reduce(lambda a, b: a + b, ...)`.
13. Extract unique `underlyingAssetId` values from `constituents_data`.
14. Fetch asset metadata in batches of 100 via `GsAssetApi.get_many_assets_data_scroll(...)`.
15. Build `asset_data_map` keyed by asset `id`.
16. For each constituent row: look up basket by `assetId`, look up asset by `underlyingAssetId`, copy requested fields from asset data, append to basket's `constituents` list.
17. Return `pd.DataFrame([r for r in basket_map.values() if r is not None])`.

### get_constituents_dataset_coverage(basket_type: BasketType = BasketType.CUSTOM_BASKET, asset_class: AssetClass = AssetClass.Equity, as_of: dt.datetime = None) -> pd.DataFrame
Purpose: Retrieve coverage information for a constituents dataset.

**Algorithm:**
1. Build `query` dict with standard fields, `type=[basket_type]`, `asset_class=[asset_class]`, `is_pair_basket=[False]`, `listed=[True]`.
2. Branch: if `asset_class != AssetClass.Equity` -> remove `is_pair_basket` from query.
3. Call `GsAssetApi.get_many_assets_data_scroll(**query, as_of=as_of, limit=QUERY_LIMIT, scroll='1m')`.
4. Return `pd.DataFrame(response)`.

## State Mutation
- No module-level mutable state.
- `QUERY_LIMIT` is a constant; never mutated.
- All functions are pure (no side effects beyond API calls) and return new DataFrames.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `__get_dataset_id` | When `asset_class` is not `AssetClass.Equity` |

## Edge Cases
- `get_my_baskets()` returns `None` (not an empty DataFrame) when the monitors API returns an empty list.
- `PriceType.to_list()` returns enum members (not `.value` strings), unlike the other enum `to_list()` methods. This is an inconsistency.
- `get_flagships_performance` and `get_flagships_constituents` access `asset_class[0]` and `basket_type[0]` without checking if the lists are empty -- will raise `IndexError` if empty lists are passed.
- `get_flagships_constituents` calls `reduce(lambda a, b: a + b, constituents_data)` which will raise `TypeError` if `constituents_data` is empty (no initial value provided).
- `get_flagships_performance` calls `b.pop('updateTime')` on every response row, which will raise `KeyError` if a row lacks `updateTime`.
- `__get_baskets` uses a mutable default argument `fields: List[str] = []` -- shared across calls but immediately reassigned, so safe in practice.
- Batching in `get_flagships_constituents`: tasks are batched in pairs of 2 (not 5 as the comment says), with a 1-second sleep between each pair.

## Coverage Notes
- Branch count: ~18
- Key branches: `get_my_baskets` user_id None vs provided (2), response empty vs non-empty (2); `__get_baskets` region truthy/falsy (2), styles truthy/falsy (2); `__get_dataset_id` asset_class Equity vs other (2), data_type price vs constituents (2), basket_type custom vs research (2); `get_constituents_dataset_coverage` asset_class Equity vs non-Equity (2); `get_flagships_performance` start/end None defaults (2); `get_flagships_constituents` dataset_id truthy check (2).
- Pragmas: none.
