# index.py

## Summary
Provides the `Index` class, representing a tradeable index that tracks an evolving portfolio of securities. Inherits from both `Asset` (securities) and `PositionedEntity` (entity with positions). Supports STS (Structured Trading Solutions) indices with specialized methods for fundamentals, close prices (official and indicative), underlier tree traversal/visualization, constituents retrieval, and constituent instrument resolution.

## Dependencies
- Internal:
  - `gs_quant.api.gs.assets` (`GsAsset`, `GsAssetApi`)
  - `gs_quant.api.gs.data` (`GsDataApi`)
  - `gs_quant.common` (`AssetClass`, `Currency`, `DateLimit`)
  - `gs_quant.data.fields` (`DataMeasure`)
  - `gs_quant.entities.entity` (`EntityType`, `PositionedEntity`)
  - `gs_quant.errors` (`MqValueError`)
  - `gs_quant.instrument` (`Instrument`)
  - `gs_quant.json_encoder` (`JSONEncoder`)
  - `gs_quant.markets.securities` (`Asset`, `AssetType`)
  - `gs_quant.markets.indices_utils` (`ReturnType`, `STSIndexType`, `IndicesDatasets`, `PriceType`)
  - `gs_quant.target.data` (`DataQuery`)
  - `gs_quant.entities.tree_entity` (`AssetTreeNode`, `TreeHelper`)
- External:
  - `datetime` (dt.date)
  - `json` (loads, dumps)
  - `pandas` (pd; DataFrame, merge)
  - `typing` (Dict, Optional, List, Tuple)
  - `pydash` (get)

## Type Definitions

### Index (class)
Inherits: `Asset`, `PositionedEntity`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id_` | `str` | *(required)* | Marquee asset ID (passed to both parent __init__) |
| `asset_class` | `AssetClass` | *(required)* | Asset class enum (passed to Asset.__init__) |
| `name` | `str` | *(required)* | Display name (passed to Asset.__init__) |
| `exchange` | `Optional[str]` | `None` | Exchange identifier |
| `currency` | `Optional[Currency]` | `None` | Currency of the index |
| `entity` | `Optional[Dict]` | `None` | Raw entity dictionary from API |
| `asset_type` | `AssetType` | `AssetType.INDEX` or from `entity['type']` | Type of asset; set from entity dict if provided |
| `tree_helper` | `TreeHelper` | *(conditional)* | Only created if STS index; manages underlier tree |
| `tree_df` | `pd.DataFrame` | `pd.DataFrame()` | Only created if STS index; cached tree data |

Inherited from `Asset`:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` | `str` | same as `id_` | Private asset ID |
| `asset_class` | `AssetClass` | *(required)* | Asset classification |
| `name` | `str` | *(required)* | Asset name |
| `exchange` | `Optional[str]` | `None` | Exchange |
| `currency` | `Optional[str]` | `None` | Currency |
| `parameters` | `AssetParameters` | `None` | Asset parameters (including `index_return_type`) |
| `entity` | `Optional[Dict]` | `None` | Raw entity dict |

Inherited from `PositionedEntity`:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` | `str` | same as `id_` | Entity ID |
| `__entity_type` | `EntityType` | `EntityType.ASSET` | Always ASSET |

## Enums and Constants

### Referenced Enums (defined elsewhere, heavily used)

**AssetType** (from `gs_quant.markets.securities`):
Large enum including `INDEX = 'Index'` and all STS types.

**STSIndexType** (from `gs_quant.markets.indices_utils`):
| Value | Raw |
|-------|-----|
| ACCESS | `"Access"` |
| MULTI_ASSET_ALLOCATION | `"Multi-Asset Allocation"` |
| RISK_PREMIA | `"Risk Premia"` |
| SYSTEMATIC_HEDGING | `"Systematic Hedging"` |

`to_list()` returns `["Access", "Multi-Asset Allocation", "Risk Premia", "Systematic Hedging"]`

**ReturnType** (from `gs_quant.markets.indices_utils`):
| Value | Raw |
|-------|-----|
| GROSS_RETURN | `"Gross Return"` |
| PRICE_RETURN | `"Price Return"` |
| TOTAL_RETURN | `"Total Return"` |

**PriceType** (from `gs_quant.markets.indices_utils`):
| Value | Raw |
|-------|-----|
| INDICATIVE_CLOSE_PRICE | `"indicativeClosePrice"` |
| OFFICIAL_CLOSE_PRICE | `"officialClosePrice"` |

**IndicesDatasets** (from `gs_quant.markets.indices_utils`):
| Value | Raw |
|-------|-----|
| STS_FUNDAMENTALS | `"STS_FUNDAMENTALS"` |
| STS_INDICATIVE_LEVELS | `"STS_INDICATIVE_LEVELS"` |
| *(others)* | *(various dataset IDs)* |

**DataMeasure** (from `gs_quant.data.fields`):
Enum with fundamentals metric values like `DIVIDEND_YIELD`, `FORWARD` direction, etc. Has `list_fundamentals()` class method.

**DateLimit** (from `gs_quant.common`):
| Value | Raw |
|-------|-----|
| LOW_LIMIT | `dt.date(1970, 1, 1)` |

## Functions/Methods

### Index.__init__(self, id_: str, asset_class: AssetClass, name: str, exchange: Optional[str] = None, currency: Optional[Currency] = None, entity: Optional[Dict] = None) -> None
Purpose: Initialize an Index, setting up both Asset and PositionedEntity bases, and conditionally building STS tree infrastructure.

**Algorithm:**
1. Call `Asset.__init__(self, id_, asset_class, name, exchange, currency, entity=entity)`
2. Call `PositionedEntity.__init__(self, id_, EntityType.ASSET)`
3. Branch: if `entity` is truthy:
   - Set `self.asset_type = AssetType(entity['type'])`
4. Else:
   - Set `self.asset_type = AssetType.INDEX`
5. Branch: if `self.__is_sts_index()` returns True:
   - Create `self.tree_helper = TreeHelper(id_, tree_underlier_dataset='STS_UNDERLIER_WEIGHTS')`
   - Initialize `self.tree_df = pd.DataFrame()`

---

### Index.__str__(self) -> str
Purpose: Return the index name as its string representation.

**Algorithm:**
1. Return `self.name`

---

### Index.get_type(self) -> AssetType
Purpose: Return the asset type of this index.

**Algorithm:**
1. Return `self.asset_type`

---

### Index.get_currency(self) -> Optional[Currency]
Purpose: Return the currency of the index.

**Algorithm:**
1. Return `self.currency`

---

### Index.get_return_type(self) -> ReturnType
Purpose: Return the index return type (total return, price return, gross return).

**Algorithm:**
1. Branch: if `self.parameters is None` OR `self.parameters.index_return_type is None`:
   - Return `ReturnType.TOTAL_RETURN` (default)
2. Else:
   - Return `ReturnType(self.parameters.index_return_type)`

---

### Index.get(cls, identifier: str) -> Optional['Index'] (classmethod)
Purpose: Fetch an existing index by any common identifier (ric, ticker, etc.).

**Algorithm:**
1. Call `cls.__get_gs_asset(identifier)` to resolve and fetch the GsAsset
2. Serialize `gs_asset` to a dict via `json.loads(json.dumps(gs_asset.as_dict(), cls=JSONEncoder))`
3. Branch: if `gs_asset.type.value` is in `STSIndexType.to_list()` OR equals `'Index'`:
   - Return new `cls(gs_asset.id, gs_asset.asset_class, gs_asset.name, exchange=gs_asset.exchange, currency=gs_asset.currency, entity=asset_entity)`
4. Else:
   - Raise `MqValueError(f'{identifier} is not an Index identifier')`

**Raises:** `MqValueError` when identifier does not resolve to an index or STS type.

---

### Index.get_fundamentals(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), period: Optional[DataMeasure] = None, direction: DataMeasure = DataMeasure.FORWARD.value, metrics: List[DataMeasure] = DataMeasure.list_fundamentals()) -> pd.DataFrame
Purpose: Retrieve fundamentals data for an STS index across a date range.

**Algorithm:**
1. Branch: if `self.__is_sts_index()`:
   a. Build `where` dict: `{assetId: self.id, periodDirection: direction, metric: metrics}`
   b. Branch: if `period` is truthy -> add `"period": period` to `where`
   c. Construct `DataQuery(where=where, start_date=start, end_date=end)`
   d. Call `GsDataApi.query_data(query=query, dataset_id=IndicesDatasets.STS_FUNDAMENTALS.value)`
   e. Return `pd.DataFrame(response)`
2. Else:
   - Raise `MqValueError('This method currently supports STS indices only')`

**Raises:** `MqValueError` for non-STS indices.

---

### Index.get_latest_close_price(self, price_type: List[PriceType] = None) -> pd.DataFrame
Purpose: Retrieve latest close prices. STS indices support indicative prices in addition to official.

**Algorithm:**
1. Branch: if `not price_type` OR `price_type == [PriceType.OFFICIAL_CLOSE_PRICE]`:
   - Return `super().get_latest_close_price()` (Asset's implementation)
2. Initialize `prices = pd.DataFrame()`
3. Branch: if `PriceType.OFFICIAL_CLOSE_PRICE in price_type`:
   - Get `official_level = super().get_latest_close_price()`
   - Set `prices['date'] = official_level.index`
   - Set `prices['closePrice'] = official_level[0]`
4. Branch: if `PriceType.INDICATIVE_CLOSE_PRICE in price_type`:
   a. Branch: if `self.__is_sts_index()`:
      - Build `where = dict(assetId=self.id)`
      - Query using `DataQuery(where=where)`
      - Call `GsDataApi.last_data(query=query, dataset_id=IndicesDatasets.STS_INDICATIVE_LEVELS.value)`
      - Extract last row's date and indicativeClosePrice into `prices`
   b. Else:
      - Raise `MqValueError('PriceType.INDICATIVE_CLOSE_PRICE currently supports STS indices only')`
5. Return `prices`

**Raises:** `MqValueError` when indicative price requested for non-STS index.

---

### Index.get_close_price_for_date(self, date: dt.date = dt.date.today(), price_type: List[PriceType] = None) -> pd.DataFrame
Purpose: Retrieve close prices for a specific date. STS indices support indicative prices.

**Algorithm:**
1. Branch: if `not price_type` OR `price_type == [PriceType.OFFICIAL_CLOSE_PRICE]`:
   - Return `super().get_close_price_for_date(date)`
2. Initialize `prices = pd.DataFrame()`
3. Branch: if `PriceType.OFFICIAL_CLOSE_PRICE in price_type`:
   - Get `official_level = super().get_close_price_for_date(date)`
   - Set `prices['date'] = official_level.index`, `prices['closePrice'] = official_level[0]`
4. Branch: if `PriceType.INDICATIVE_CLOSE_PRICE in price_type`:
   a. Branch: if `self.__is_sts_index()`:
      - Call `self.__query_indicative_levels_dataset(start=date, end=date)`
      - Build DataFrame, extract date and indicativeClosePrice into `prices`
   b. Else:
      - Raise `MqValueError('PriceType.INDICATIVE_CLOSE_PRICE currently supports STS indices only')`
5. Return `prices`

**Raises:** `MqValueError` when indicative price requested for non-STS index.

---

### Index.get_close_prices(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), price_type: List[PriceType] = None) -> pd.DataFrame
Purpose: Retrieve close prices over a date range. STS indices support indicative prices.

**Algorithm:**
1. Branch: if `not price_type` OR `price_type == [PriceType.OFFICIAL_CLOSE_PRICE]`:
   - Return `super().get_close_prices(start, end)`
2. Initialize `prices = pd.DataFrame()`
3. Branch: if `self.__is_sts_index()`:
   a. Branch: if `price_type == [PriceType.INDICATIVE_CLOSE_PRICE]` (indicative only):
      - Query indicative levels dataset for date range
      - Drop `updateTime`, `assetId` columns
      - Cast `date` column to `datetime64[ns]`
      - Copy `date` and `indicativeClosePrice` into `prices`
      - Return `prices` (early return)
   b. Else (both official and indicative):
      - Get `official_level = super().get_close_prices(start, end).to_frame('closePrice')`
      - Query indicative levels dataset
      - Reset index on official, drop `updateTime`/`assetId` from indicative
      - Cast indicative `date` to match official's dtype
      - Merge official and indicative on `date` with outer join
      - Return merged DataFrame
4. Else (not STS):
   - Raise `MqValueError('PriceType.INDICATIVE_CLOSE_PRICE currently supports STS indices only')`

**Raises:** `MqValueError` when indicative price requested for non-STS index.

---

### Index.get_underlier_tree(self, refresh_tree: Optional[bool] = False) -> AssetTreeNode
Purpose: Get the root node of the underlier tree for an STS index.

**Algorithm:**
1. Branch: if `self.__is_sts_index()`:
   a. Branch: if tree not built (`not self.tree_helper.tree_built`) OR `refresh_tree`:
      - Call `self.tree_helper.build_tree()`
      - Call `self.tree_helper.populate_weights('STS_UNDERLIER_WEIGHTS')`
      - Call `self.tree_helper.populate_attribution('STS_UNDERLIER_ATTRIBUTION')`
      - Set `self.tree_df = self.tree_helper.to_frame()`
   b. Return `self.tree_helper.root`
2. Else:
   - Raise `MqValueError('This method currently supports STS indices only')`

**Raises:** `MqValueError` for non-STS indices.

---

### Index.get_underlier_weights(self) -> pd.DataFrame
Purpose: Get immediate (depth-1) underlier weights for an STS index.

**Algorithm:**
1. Branch: if `self.__is_sts_index()`:
   a. Branch: if `len(self.tree_df) == 0`:
      - Call `self.get_underlier_tree()` to populate `self.tree_df`
   b. Filter `self.tree_df` to rows where `depth == 1`
   c. Drop columns: `absoluteAttribution`, `assetId`, `assetName`, `depth`
   d. Return the filtered DataFrame
2. Else:
   - Raise `MqValueError('This method currently supports STS indices only')`

**Raises:** `MqValueError` for non-STS indices.

---

### Index.get_underlier_attribution(self) -> pd.DataFrame
Purpose: Get immediate (depth-1) underlier attribution for an STS index.

**Algorithm:**
1. Branch: if `self.__is_sts_index()`:
   a. Branch: if `len(self.tree_df) == 0`:
      - Call `self.get_underlier_tree()` to populate `self.tree_df`
   b. Filter `self.tree_df` to rows where `depth == 1`
   c. Drop columns: `weight`, `assetId`, `assetName`, `depth`
   d. Return the filtered DataFrame
2. Else:
   - Raise `MqValueError('This method currently supports STS indices only')`

**Raises:** `MqValueError` for non-STS indices.

---

### Index.visualise_tree(self, visualise_by: Optional[str] = 'asset_name') -> Tree
Purpose: Generate a printable tree visualization of the underlier structure.

**Algorithm:**
1. Branch: if `self.__is_sts_index()`:
   - Return `self.tree_helper.get_visualisation(visualise_by)`
2. Else:
   - Raise `MqValueError('This method currently supports STS indices only')`

**Raises:** `MqValueError` for non-STS indices.

---

### Index.get_latest_constituents(self) -> pd.DataFrame
Purpose: Fetch the latest constituent positions as a DataFrame.

**Algorithm:**
1. Call `self.get_latest_position_set()` (inherited from `PositionedEntity`)
2. Call `.get_positions()` on the result
3. Return the DataFrame

---

### Index.get_constituents_for_date(self, date: dt.date = dt.date.today()) -> pd.DataFrame
Purpose: Fetch constituents for a specific date.

**Algorithm:**
1. Call `self.get_position_set_for_date(date)` (inherited)
2. Call `.get_positions()` on the result
3. Return the DataFrame

---

### Index.get_constituents(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> List[pd.DataFrame]
Purpose: Fetch constituents for each date in a range.

**Algorithm:**
1. Call `self.get_position_sets(start, end)` (inherited)
2. Return `[position_set.get_positions() for position_set in ...]`

---

### Index.get_latest_constituent_instruments(self) -> Tuple[Instrument, ...]
Purpose: Fetch latest constituents as Instrument objects.

**Algorithm:**
1. Get `self.get_latest_position_set().to_target().positions`
2. Return `GsAssetApi.get_instruments_for_positions(positions)`

---

### Index.get_constituent_instruments_for_date(self, date: dt.date = dt.date.today()) -> Tuple[Instrument, ...]
Purpose: Fetch constituents for a date as Instrument objects.

**Algorithm:**
1. Get `self.get_position_set_for_date(date).to_target().positions`
2. Return `GsAssetApi.get_instruments_for_positions(positions)`

---

### Index.get_constituent_instruments(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> Tuple[Tuple[Instrument, ...], ...]
Purpose: Fetch constituents as Instrument objects for a date range.

**Algorithm:**
1. Get `position_sets = self.get_position_sets(start, end)`
2. Return list comprehension: for each `position_set`, call `GsAssetApi.get_instruments_for_positions(position_set.to_target().positions)`

Note: Return type annotation says `Tuple[Tuple[...], ...]` but implementation returns a list comprehension (i.e., `List`).

---

### Index.__is_sts_index(self) -> bool (private)
Purpose: Check whether this index is an STS index type.

**Algorithm:**
1. Branch: if `self.get_type().value` is in `STSIndexType.to_list()`:
   - Return `True`
2. Return `False`

---

### Index.__query_indicative_levels_dataset(self, start=None, end=None) -> pd.DataFrame (private)
Purpose: Query the STS indicative levels dataset, returning a DataFrame with empty-string columns if no data.

**Algorithm:**
1. Build `where = dict(assetId=self.id)`
2. Branch: if `start is None`:
   - Create `query = DataQuery(where=where)` (no date range)
3. Else:
   - Create `query = DataQuery(where=where, start_date=start, end_date=end)`
4. Call `GsDataApi.query_data(query=query, dataset_id=IndicesDatasets.STS_INDICATIVE_LEVELS.value)`
5. Create `indicative_level = pd.DataFrame(response)`
6. Branch: if `len(indicative_level) == 0`:
   - Add empty-string columns: `date`, `assetId`, `updateTime`, `indicativeClosePrice`
7. Return `indicative_level`

---

### Index.__get_gs_asset(identifier: str) -> GsAsset (staticmethod, private)
Purpose: Resolve an index identifier to a GsAsset via the API.

**Algorithm:**
1. Call `GsAssetApi.resolve_assets(identifier=[identifier], fields=['id'], limit=1)` -> dict keyed by identifier
2. Get `response = result[identifier]`
3. Branch: if `len(response) == 0` OR `get(response, '0.id') is None`:
   - Raise `MqValueError(f'Asset could not be found using identifier {identifier}')`
4. Return `GsAssetApi.get_asset(get(response, '0.id'))`

**Raises:** `MqValueError` when identifier cannot be resolved.

## State Mutation
- `self.asset_type`: Set in `__init__` based on entity dict presence
- `self.tree_helper`: Created in `__init__` only for STS indices; its internal state (`tree_built`, `root`) mutated by `get_underlier_tree`
- `self.tree_df`: Initially empty DataFrame; populated by `get_underlier_tree`, lazily triggered by `get_underlier_weights` and `get_underlier_attribution`
- `Asset` fields (`__id`, `asset_class`, `name`, `exchange`, `currency`, `parameters`, `entity`): Set by `Asset.__init__`
- `PositionedEntity` fields (`__id`, `__entity_type`): Set by `PositionedEntity.__init__`
- Thread safety: No internal locking. Concurrent calls to `get_underlier_tree` with `refresh_tree=True` can race on `self.tree_df` and `self.tree_helper` state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `get` (classmethod) | Identifier does not resolve to an Index or STS type |
| `MqValueError` | `__get_gs_asset` | Identifier cannot be resolved at all |
| `MqValueError` | `get_fundamentals` | Called on non-STS index |
| `MqValueError` | `get_latest_close_price` | Indicative price requested for non-STS index |
| `MqValueError` | `get_close_price_for_date` | Indicative price requested for non-STS index |
| `MqValueError` | `get_close_prices` | Indicative price requested for non-STS index |
| `MqValueError` | `get_underlier_tree` | Called on non-STS index |
| `MqValueError` | `get_underlier_weights` | Called on non-STS index |
| `MqValueError` | `get_underlier_attribution` | Called on non-STS index |
| `MqValueError` | `visualise_tree` | Called on non-STS index |

## Edge Cases
- `__init__` with `entity` containing `type` key: overrides default `AssetType.INDEX` with the entity's actual type
- `__init__` for STS index: `TreeHelper` constructor immediately calls `GsAssetApi.get_asset(asset_id=self.id)` which is an API call during construction
- `get_return_type` when `parameters` is `None` or `parameters.index_return_type` is `None`: defaults to `ReturnType.TOTAL_RETURN`
- `get_latest_close_price` with empty `price_type` list (`[]`): evaluates as falsy, delegates to `super().get_latest_close_price()`
- `get_latest_close_price` with `[PriceType.INDICATIVE_CLOSE_PRICE]` only (no OFFICIAL): returns DataFrame with only indicative columns, no date/closePrice from official
- `get_close_prices` with both price types: performs an outer merge, so dates present in only one source will have NaN in the other's columns
- `get_close_prices` with `[PriceType.INDICATIVE_CLOSE_PRICE]` only: early-returns after querying indicative dataset, skips official
- `__query_indicative_levels_dataset` with empty response: creates DataFrame with empty-string columns (not NaN), which may cause type issues downstream
- `get_constituents_for_date` can return `None` if `get_position_set_for_date` returns `None` (for portfolios), then `.get_positions()` would raise `AttributeError` -- but since entity type is ASSET, `get_position_set_for_date` returns a `PositionSet` with empty positions list instead
- `get_constituent_instruments` return type annotation says `Tuple[Tuple[...], ...]` but implementation returns a `list` (list comprehension)
- `get` classmethod: the condition `gs_asset.type.value in STSIndexType.to_list()` checks STS types first, then separately checks `== 'Index'`, meaning both STS and plain Index types are accepted
- `__is_sts_index` is called during `__init__` before `tree_helper`/`tree_df` exist, so for non-STS indices those attributes are never set -- methods that check `self.__is_sts_index()` will raise before accessing them

## Bugs Found
- Line 531: `get_constituent_instruments` has return type annotation `Tuple[Tuple[Instrument, ...], ...]` but returns a `list` (list comprehension), not a `tuple`. (OPEN -- type annotation mismatch)
- Line 553-556: `__query_indicative_levels_dataset` fills empty DataFrame columns with empty string `''` rather than appropriate typed defaults (NaN for numeric, pd.NaT for dates). This can cause downstream type coercion issues. (OPEN -- style/correctness concern)

## Coverage Notes
- Branch count: ~36
- Key branches in `__init__`: `entity` truthy/falsy (asset_type assignment), `__is_sts_index()` true/false (tree_helper creation)
- Key branches in `get_return_type`: `parameters is None`, `parameters.index_return_type is None`, else
- Key branches in `get` classmethod: type in STS list or `'Index'` vs else
- Key branches in `get_fundamentals`: STS vs non-STS, `period` truthy/falsy
- Key branches in `get_latest_close_price`: default/official-only path, OFFICIAL in list, INDICATIVE in list (STS vs non-STS)
- Key branches in `get_close_price_for_date`: same structure as `get_latest_close_price`
- Key branches in `get_close_prices`: default path, STS check, indicative-only vs both, non-STS error
- Key branches in `get_underlier_tree`: STS check, tree_built/refresh_tree
- Key branches in `get_underlier_weights`: STS check, `len(tree_df) == 0` lazy init
- Key branches in `get_underlier_attribution`: STS check, `len(tree_df) == 0` lazy init
- Key branches in `visualise_tree`: STS check
- Key branches in `__is_sts_index`: type value in list vs not
- Key branches in `__query_indicative_levels_dataset`: `start is None`, `len(indicative_level) == 0`
- Key branches in `__get_gs_asset`: `len(response) == 0` or `id is None`
