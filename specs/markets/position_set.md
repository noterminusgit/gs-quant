# position_set.py

## Summary
Defines the `PositionTag`, `Position`, and `PositionSet` classes that form the core data model for managing collections of financial positions (securities with weights, quantities, or notional values) associated with a particular date. The module supports resolving identifiers to Marquee asset IDs, pricing positions (converting between weights/quantities/notionals), serializing to/from DataFrames and dictionaries, and bulk operations (`resolve_many`, `price_many`) over multiple position sets.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi)
- Internal: `gs_quant.api.gs.price` (GsPriceApi)
- Internal: `gs_quant.common` (Position as CommonPosition, PositionPriceInput, PositionSet as CommonPositionSet, PositionTag as PositionTagTarget, Currency, PositionSetWeightingStrategy, MarketDataFrequency)
- Internal: `gs_quant.errors` (MqValueError, MqRequestError)
- Internal: `gs_quant.markets.position_set_utils` (_get_asset_temporal_xrefs, _group_temporal_xrefs_into_discrete_time_ranges, _resolve_many_assets)
- Internal: `gs_quant.models.risk_model_utils` (_repeat_try_catch_request)
- Internal: `gs_quant.target.positions_v2_pricing` (PositionsPricingParameters, PositionsRequest, PositionSetRequest, PositionsPricingRequest)
- Internal: `gs_quant.target.price` (PriceParameters, PositionSetPriceInput, PositionPriceResponse)
- External: `datetime` (date, date.today)
- External: `logging` (getLogger)
- External: `math` (copysign, ceil)
- External: `time` (time)
- External: `typing` (Dict, List, Union, Optional)
- External: `numpy` (np.vectorize, np.nan, np.array_split)
- External: `pandas` (pd.DataFrame, pd.to_datetime, pd.Timestamp)
- External: `pydash` (get)

## Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Type Definitions

### PositionTag (class)
Inherits: `PositionTagTarget` (from `gs_quant.common`)

No additional fields beyond parent. Adds a single class method for constructing from a dict.

### Position (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__identifier` | `str` | required | Identifier string (e.g., Bloomberg ID like `'AAPL UW'`) |
| `__weight` | `float` | `None` | Position weight (fraction of portfolio) |
| `__quantity` | `float` | `None` | Number of shares |
| `__notional` | `float` | `None` | Notional (dollar) exposure |
| `__name` | `str` | `None` | Display name of the asset |
| `__asset_id` | `str` | `None` | Marquee asset ID (set after resolution) |
| `__tags` | `Optional[List[PositionTag]]` | `None` | List of PositionTag instances; dicts in input are converted via `PositionTag.from_dict` |
| `__restricted` | `bool` | `None` | Whether this asset has trading restrictions |
| `__hard_to_borrow` | `bool` | `None` | Whether this asset is hard to borrow |

### PositionSet (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__positions` | `List[Position]` | required | List of positions in the set |
| `__date` | `dt.date` | `dt.date.today()` | Date associated with this position set |
| `__divisor` | `float` | `None` | Divisor applied to overall position set (used for index pricing) |
| `__reference_notional` | `float` | `None` | Reference notional for weight-based pricing |
| `__unresolved_positions` | `List[Position]` | `[]` | Positions that failed identifier resolution |
| `__unpriced_positions` | `List[Position]` | `[]` | Positions that failed pricing |

## Enums and Constants

No enums defined in this module. Uses `PositionSetWeightingStrategy` (Weight, Quantity, Notional), `Currency`, and `MarketDataFrequency` from `gs_quant.common`.

## Functions/Methods

### PositionTag.from_dict(cls, tag_dict: Dict) -> PositionTag
Purpose: Create a PositionTag from a single-entry dictionary.

**Algorithm:**
1. Branch: `len(tag_dict) > 1` -> raise `MqValueError('PositionTag.from_dict only accepts a single key-value pair')`
2. Extract the single key as `name` and single value as `value`
3. Return `cls(name=..., value=...)`

**Raises:** `MqValueError` when dict has more than one key-value pair.

---

### Position.__init__(self, identifier: str, weight: float = None, quantity: float = None, notional: float = None, name: str = None, asset_id: str = None, tags: Optional[List[Union[PositionTag, Dict]]] = None)
Purpose: Initialize a Position with identifier and optional sizing/metadata fields.

**Algorithm:**
1. Store all scalar fields into private attributes
2. Branch: `tags is not None` -> iterate tags list; for each tag, if `isinstance(tag, dict)` convert via `PositionTag.from_dict(tag)`, else keep as-is; store resulting list
3. Branch: `tags is None` -> store `None`
4. Initialize `__restricted` and `__hard_to_borrow` to `None`

### Position.__eq__(self, other) -> bool
Purpose: Compare two positions for equality based on asset_id, weight, notional, quantity, and tags.

**Algorithm:**
1. Branch: `not isinstance(other, Position)` -> return `False`
2. For each property in `['asset_id', 'weight', 'notional', 'quantity', 'tags']`:
   a. Retrieve values from both self and other using `pydash.get`
   b. Branch: property is `'weight'`, `'notional'`, or `'quantity'` -> if both are non-None and `round(slf, 5) != round(oth, 5)`, return `False`
   c. Branch: property is `'asset_id'` or `'tags'` -> if not both None and values differ, return `False`
3. Return `True`

**Note:** The numeric comparison uses `round(_, 5)` to handle insignificant decimal differences from V2 pricing calculations. The condition `not (slf is None or oth is None)` means if either is None the numeric check is skipped (does not detect None vs 0 inequality).

### Position.__hash__(self) -> int
Purpose: Hash based on asset_id XOR identifier.

**Algorithm:**
1. Return `hash(self.asset_id) ^ hash(self.identifier)`

### Position.identifier (property, getter/setter) -> str
### Position.weight (property, getter/setter) -> float
### Position.quantity (property, getter/setter) -> float
### Position.notional (property, getter/setter) -> float
### Position.name (property, getter/setter) -> str
### Position.asset_id (property, getter/setter) -> str
### Position.tags (property, getter/setter) -> List[PositionTag]
Purpose: Standard getters/setters for all fields.

### Position.hard_to_borrow (property, getter) -> bool
Purpose: Read-only public property.

### Position._hard_to_borrow (setter)
Purpose: Write access via underscore-prefixed setter (semi-private write).

### Position.restricted (property, getter) -> bool
Purpose: Read-only public property.

### Position._restricted (setter)
Purpose: Write access via underscore-prefixed setter (semi-private write).

### Position.add_tag(self, name: str, value: str)
Purpose: Add a tag to this position. Tags must have unique names.

**Algorithm:**
1. Branch: `self.tags is None` -> initialize `self.tags = []`
2. Branch: any existing tag has matching name -> raise `MqValueError(f'Position already has tag with name {name}')`
3. Append new `PositionTag(name=name, value=value)`

**Raises:** `MqValueError` when a tag with the same name already exists.

### Position.tags_as_dict(self) -> Dict
Purpose: Convert tags list to a `{name: value}` dictionary.

**Algorithm:**
1. Return `{tag.name: tag.value for tag in self.tags}`

### Position.as_dict(self, tags_as_keys: bool = False) -> Dict
Purpose: Serialize position to a dictionary, optionally flattening tags into top-level keys.

**Algorithm:**
1. Build dict with `identifier`, `weight`, `quantity`, `notional`, `name`, `asset_id`, `restricted`
2. Branch: `self.tags and tags_as_keys` -> merge `tags_as_dict()` into position_dict
3. Branch: else -> set `position_dict['tags'] = self.tags`
4. Filter out keys with `None` values and return

### Position.from_dict(cls, position_dict: Dict, add_tags: bool = True) -> Position
Purpose: Class method to create a Position from a dictionary.

**Algorithm:**
1. Normalize field names to lowercase
2. Branch: both `'id'` and `'asset_id'` in fields -> raise `MqValueError('Position cannot have both id and asset_id')`
3. Branch: `'id'` in fields -> rename to `'asset_id'`
4. Separate tag fields (keys not in `position_fields`)
5. Branch: `add_tags is True` -> create tags from tag_dict entries
6. Branch: `add_tags is False` -> use `position_dict.get('tags')`
7. Return new Position instance

**Raises:** `MqValueError` when dict has both `id` and `asset_id` keys.

### Position.clone(self) -> Position
Purpose: Create a deep clone of this position.

**Algorithm:**
1. Call `self.as_dict(tags_as_keys=True)` then `Position.from_dict(result, add_tags=True)`

### Position.to_target(self, common: bool = True) -> Union[CommonPosition, PositionPriceInput]
Purpose: Convert to API payload types.

**Algorithm:**
1. Branch: `common is True` -> return `CommonPosition(self.asset_id, quantity=self.quantity, tags=self.tags if self.tags else None)`
2. Branch: `common is False` -> return `PositionPriceInput(self.asset_id, quantity=self.quantity, weight=self.weight, notional=self.notional)`

---

### PositionSet.__init__(self, positions: List[Position], date: dt.date = dt.date.today(), divisor: float = None, reference_notional: float = None, unresolved_positions: List[Position] = None, unpriced_positions: List[Position] = None)
Purpose: Initialize a position set with validation for reference_notional mode.

**Algorithm:**
1. Branch: `reference_notional is not None` -> for each position `p`:
   a. Branch: `p.weight is None` -> raise `MqValueError('Position set with reference notionals must have weights for every position.')`
   b. Branch: `p.notional is not None` -> raise `MqValueError('Position sets with reference notionals cannot have positions with notional.')`
   c. Branch: `p.quantity is not None` -> raise `MqValueError('Position sets with reference notionals cannot have positions with quantities.')`
2. Store all fields; default `unresolved_positions` and `unpriced_positions` to `[]` if `None`

**Raises:** `MqValueError` (3 distinct conditions) when reference_notional is set but positions have incorrect sizing fields.

### PositionSet.__eq__(self, other) -> bool
Purpose: Compare two position sets for equality.

**Algorithm:**
1. Branch: `len(self.positions) != len(other.positions)` -> return `False`
2. Branch: `self.date != other.date` -> return `False`
3. Branch: `self.reference_notional != other.reference_notional` -> return `False`
4. Sort both position lists by `asset_id`
5. Compare element-by-element using `Position.__eq__`
6. Return `True` if all match

### PositionSet.positions (property, getter/setter) -> List[Position]
### PositionSet.date (property, getter/setter) -> dt.date
### PositionSet.divisor (property, getter only) -> float
### PositionSet.reference_notional (property, getter/setter) -> float
### PositionSet.unresolved_positions (property, getter only) -> List[Position]
### PositionSet.unpriced_positions (property, getter only) -> List[Position]

### PositionSet.clone(self, keep_reference_notional: bool = False) -> PositionSet
Purpose: Create a clone of the current position set.

**Algorithm:**
1. Convert to frame via `self.to_frame(add_tags=True)`
2. Read `ref_notional = self.reference_notional`
3. Branch: `'quantity' in frame.columns and ref_notional is not None`:
   a. Branch: `keep_reference_notional` -> drop `quantity` column from frame
   b. Branch: else -> set `ref_notional = None`
4. Return `PositionSet.from_frame(frame, date=self.date, reference_notional=ref_notional, divisor=self.divisor, add_tags=True)`

### PositionSet.get_positions(self) -> pd.DataFrame
Purpose: Return positions as a DataFrame.

**Algorithm:**
1. Call `as_dict()` on each position, return `pd.DataFrame(positions)`

### PositionSet.get_unresolved_positions(self) -> pd.DataFrame
Purpose: Return unresolved positions as a DataFrame.

### PositionSet.remove_unresolved_positions(self)
Purpose: Filter out unresolved positions and clear the unresolved list.

**Algorithm:**
1. Set `self.positions` to only those with non-None `asset_id`
2. Set `self.__unresolved_positions = None`

### PositionSet.get_unpriced_positions(self) -> pd.DataFrame
Purpose: Return unpriced positions as a DataFrame.

### PositionSet.remove_unpriced_positions(self)
Purpose: Clear the unpriced positions list.

**Algorithm:**
1. Set `self.__unpriced_positions = None`

### PositionSet.get_restricted_positions(self) -> pd.DataFrame
Purpose: Return positions with `restricted == True` as a DataFrame.

### PositionSet.remove_restricted_positions(self)
Purpose: Remove restricted positions from the position set.

**Algorithm:**
1. Filter: `self.positions = [p for p in self.positions if p.restricted is not True]`

### PositionSet.get_hard_to_borrow_positions(self) -> pd.DataFrame
Purpose: Return positions with `hard_to_borrow == True` as a DataFrame.

### PositionSet.remove_hard_to_borrow_positions(self)
Purpose: Remove hard-to-borrow positions from the position set.

**Algorithm:**
1. Filter: `self.positions = [p for p in self.positions if p.hard_to_borrow is not True]`

### PositionSet.equalize_position_weights(self)
Purpose: Assign equal weight (1/N) to every position; clear quantity and notional.

**Algorithm:**
1. Compute `weight = 1 / len(self.positions)`
2. For each position: set `weight`, clear `quantity` and `notional` to `None`
3. Replace `self.positions` with updated list

### PositionSet.to_frame(self, add_tags: bool = False) -> pd.DataFrame
Purpose: Convert position set to a DataFrame with a row per position.

**Algorithm:**
1. For each position `p`:
   a. Create dict with `date=self.date.isoformat()`
   b. Branch: `self.divisor is not None` -> add `divisor` to dict
   c. Merge in `p.as_dict(tags_as_keys=add_tags)`
2. Return `pd.DataFrame(positions)`

### PositionSet.resolve(self, **kwargs)
Purpose: Resolve unmapped positions (those without asset_id) using the GS Asset API.

**Algorithm:**
1. Collect identifiers for positions where `p.asset_id is None`
2. Branch: `len(unresolved_positions) > 0`:
   a. Call `__resolve_identifiers(unresolved_positions, self.date, **kwargs)`
   b. Set `self.__unresolved_positions` to positions whose identifiers are in the unmapped list
   c. For each position in self.positions: if identifier is in `id_map`, set `asset_id`, `name`, `_restricted` from the map (uses `pydash.get` with escaped dots in identifier)
   d. Filter `self.positions` to only those with non-None `asset_id`

### PositionSet.redistribute_weights(self)
Purpose: Redistribute weights proportionally so they sum to exactly 1.0.

**Algorithm:**
1. Sum all weights; collect list of positions with `weight is None`
2. Branch: any positions missing weights -> raise `MqValueError`
3. Compute `weight_to_distribute`: if `total_weight < 0` then `1 - total_weight`, else `total_weight - 1`
4. For each position: `p.weight = p.weight - (p.weight / total_weight) * weight_to_distribute`; clear `quantity` and `notional`

**Raises:** `MqValueError` when some positions are missing weights.

### PositionSet.price(self, currency: Optional[Currency] = Currency.USD, use_unadjusted_close_price: bool = True, weighting_strategy: Optional[PositionSetWeightingStrategy] = None, handle_long_short: bool = False, fail_on_unpriced_positions: bool = False, **kwargs)
Purpose: Fetch weights from quantities (or vice versa) using the GS Price API.

**Algorithm:**
1. Determine weighting strategy via `__get_default_weighting_strategy`
2. Convert positions via `__convert_positions_for_pricing`
3. Branch: `'fractional_shares' not in kwargs`:
   a. Branch: `weighting_strategy == Notional` -> `should_allow_fractional_shares = True`
   b. Branch: else -> `should_allow_fractional_shares = False`
4. Branch: `'fractional_shares' in kwargs` -> pop and use the value
5. Build `PriceParameters` with currency, divisor, frequency, target_notional, etc.
6. Branch: `'dataset' in kwargs` -> set `asset_data_set_id` on params, set `frequency = None`
7. Apply remaining kwargs as attributes on `price_parameters`
8. Call `GsPriceApi.price_positions(...)`, build lookup map from results keyed by `asset_id + hashed_tags`
9. For each position: look up in result map; if found, set quantity, weight, notional, hard_to_borrow
   a. Branch: `handle_long_short` -> set `weight = math.copysign(w, pos.notional)`
   b. If not found -> add to `unpriced_positions`
10. Branch: `fail_on_unpriced_positions and unpriced_positions` -> raise `MqValueError`
11. Update `self.positions` and `self.__unpriced_positions`
12. Branch: `handle_long_short` -> set `self.reference_notional = results.gross_notional`

**Raises:** `MqValueError` when `fail_on_unpriced_positions` is True and some positions are unpriced.

### PositionSet.get_subset(self, copy: bool = True, **kwargs) -> PositionSet
Purpose: Extract a subset of positions matching tag key-value criteria.

**Algorithm:**
1. For each position:
   a. Branch: `not p.tags` -> raise `MqValueError(f'PositionSet has position {p.identifier} that does not have tags')`
   b. Convert tags to dict; check if all kwargs match
   c. Branch: match found -> append position (cloned if `copy` is True)
2. Return new `PositionSet(positions=subset, date=self.date, reference_notional=self.reference_notional)`

**Raises:** `MqValueError` when any position lacks tags.

### PositionSet.to_target(self, common: bool = True) -> Union[CommonPositionSet, List[PositionPriceInput]]
Purpose: Convert to API payload types.

**Algorithm:**
1. Convert each position via `p.to_target(common)`
2. Branch: `common` -> return `CommonPositionSet(positions, self.date)`
3. Branch: else -> return `list(positions)`

### PositionSet.from_target(cls, position_set: CommonPositionSet, source: Optional[str] = None) -> PositionSet
Purpose: Create PositionSet from API response type.

**Algorithm:**
1. Extract mqids from positions
2. Call `__get_positions_data(mqids, source=source)` to fetch bbid/name mappings
3. For each position: look up in data, create Position with bbid as identifier, set name, asset_id, quantity, tags
4. Return `cls(converted_positions, position_set.position_date, position_set.divisor)`

### PositionSet.from_list(cls, positions: List[str], date: dt.date = dt.date.today()) -> PositionSet
Purpose: Create equally-weighted PositionSet from a list of identifiers.

**Algorithm:**
1. Compute `weight = 1 / len(positions)`
2. Create `Position(identifier=p, weight=weight)` for each identifier
3. Return `cls(converted_positions, date)`

### PositionSet.from_dicts(cls, positions: List[Dict], date: dt.date = dt.date.today(), reference_notional: float = None, add_tags: bool = False) -> PositionSet
Purpose: Create PositionSet from a list of dictionaries.

**Algorithm:**
1. Convert to DataFrame
2. Delegate to `cls.from_frame(...)`

### PositionSet.from_frame(cls, positions: pd.DataFrame, date: dt.date = dt.date.today(), reference_notional: float = None, divisor: float = None, add_tags: bool = False) -> PositionSet
Purpose: Create PositionSet from a DataFrame of positions.

**Algorithm:**
1. Normalize column names via `__normalize_position_columns`
2. Branch: `add_tags` -> get tag columns via `__get_tag_columns`
3. Filter out rows where `identifier` is NaN
4. Branch: no `quantity`, `weight`, or `notional` columns -> `equalize = True`, compute `equal_weight = 1 / len(positions)`
5. For each row: create Position with appropriate weight/quantity/notional
   a. Branch: `equalize` -> use `equal_weight`, set quantity/notional to None
   b. Branch: `len(tag_columns)` -> create tag list from tag columns
6. Return `cls(positions_list, date, reference_notional=..., divisor=...)`

### PositionSet.__get_tag_columns(positions: pd.DataFrame) -> List[str] (static)
Purpose: Identify non-standard columns that represent tags.

**Algorithm:**
1. Return columns not in `['identifier', 'id', 'quantity', 'notional', 'weight', 'date', 'restricted']` (case-insensitive)

### PositionSet.__normalize_position_columns(positions: pd.DataFrame) -> List[str] (static)
Purpose: Normalize column names to lowercase for known fields; rename `asset_id` to `id`.

**Algorithm:**
1. Branch: `'asset_id' in columns and 'id' not in columns` -> rename `asset_id` to `id`
2. For each column: if lowercase matches known fields, lowercase it; otherwise keep original case (preserves tag column casing)

### PositionSet.__resolve_identifiers(identifiers: List[str], date: dt.date, **kwargs) -> List (static)
Purpose: Resolve identifiers to Marquee asset IDs in batches of 500.

**Algorithm:**
1. Initialize `id_map` and `unmapped_assets`
2. For each batch of 500 identifiers:
   a. Call `GsAssetApi.resolve_assets(identifier=batch, fields=['name', 'id', 'tradingRestriction'], limit=1, as_of=date, **kwargs)`
   b. For each identifier in response:
      - Branch: response is not None and len > 0 -> add to `id_map` with id, name, restricted
      - Branch: else -> add to `unmapped_assets`
3. Branch: `len(unmapped_assets) > 0` -> log info message
4. Return `[id_map, unmapped_assets]`

### PositionSet.__get_positions_data(mqids: List[str], source: Optional[str] = None) -> Dict (static)
Purpose: Fetch name/bbid data for a list of Marquee IDs.

**Algorithm:**
1. Call `GsAssetApi.get_many_assets_data(id=mqids, fields=['id', 'name', 'bbid'], source=source)`
2. Build dict keyed by asset id with name and bbid values

### PositionSet.__get_default_weighting_strategy(positions, reference_notional, weighting_strategy) -> PositionSetWeightingStrategy (static)
Purpose: Determine or validate the weighting strategy.

**Algorithm:**
1. Collect lists of positions missing weights, quantities, and exposures
2. Branch: `weighting_strategy is None`:
   a. Branch: all three missing lists are non-empty -> raise `MqValueError` (cannot determine strategy)
   b. Branch: no missing weights AND (reference_notional is not None OR missing quantities) -> `Weight`
   c. Branch: no missing exposures -> `Notional`
   d. Branch: else -> `Quantity`
3. Validate chosen strategy against missing values:
   a. Branch: chosen strategy requires values that are missing -> raise `MqValueError`
4. Branch: `use_weight and reference_notional is None` -> raise `MqValueError('You must specify a reference notional in order to price by weight.')`
5. Return strategy

**Raises:** `MqValueError` (3 distinct conditions: unable to determine, missing values for chosen strategy, missing reference notional for Weight strategy).

### PositionSet.__convert_positions_for_pricing(positions, weighting_strategy) -> List[PositionPriceInput] (static)
Purpose: Convert positions to API pricing inputs.

**Algorithm:**
1. For each position:
   a. Branch: `p.asset_id is None` -> add to `missing_ids`
   b. Branch: else -> create `PositionPriceInput` with the relevant field based on strategy
2. Branch: `len(missing_ids) > 0` -> raise `MqValueError`
3. Return position inputs

**Raises:** `MqValueError` when positions are missing asset IDs.

### PositionSet.__hash_position_tag_list(position_tags: List[PositionTag]) -> str (static)
Purpose: Create a string hash of a tag list for use as a lookup key.

**Algorithm:**
1. Branch: `position_tags is not None` -> concatenate `tag.name + '-' + tag.value` for each tag
2. Branch: `position_tags is None` -> return empty string

### PositionSet.to_frame_many(position_sets: List[PositionSet]) -> pd.DataFrame (static)
Purpose: Convert multiple position sets into a single DataFrame.

**Algorithm:**
1. Create DataFrame from position sets
2. Extract `date`, `divisor`, `reference_notional` as columns
3. Extract `positions` as a column of lists
4. Filter out rows where positions list is empty
5. Explode positions column (one row per position)
6. Call `as_dict()` on each position, extract individual fields
7. Drop intermediate columns and return

### PositionSet.__build_positions_from_frame(...) -> Position (static, np.vectorize)
Purpose: Vectorized builder for Position objects from DataFrame columns.

**Algorithm:**
1. Create Position with given fields; use `None` for falsy weight/notional/quantity values
2. Set `_restricted` and `_hard_to_borrow` on the position
3. Return position

**Note:** Decorated with `@np.vectorize`, operates element-wise across array inputs.

### PositionSet.resolve_many(cls, position_sets: List[PositionSet], **kwargs)
Purpose: Bulk-resolve identifiers across multiple position sets, updating each in place.

**Algorithm:**
1. Convert to DataFrame via `to_frame_many`
2. Drop `name` and `asset_id` columns if present; drop all-NaN columns
3. Validate: raise `MqValueError` if both weight+quantity or quantity+notional present
4. Call `_get_asset_temporal_xrefs` -> `_group_temporal_xrefs_into_discrete_time_ranges` -> `_resolve_many_assets`
5. Merge resolved assets back onto positions DataFrame by identifier
6. Convert dates, fill NaN for startDate/endDate, filter to valid date ranges
7. Rename columns, drop intermediate columns
8. Branch: `'reference_notional' in columns and 'quantity' in columns` -> drop quantity column
9. Extract weight/quantity/notional/tags DataFrames (or None if column missing)
10. Build positions via `__build_positions_from_frame` (vectorized)
11. Group by date; for each input position set:
    a. Branch: `position_set.date` not a `dt.date` -> convert via `pd.Timestamp`
    b. Get positions for that date; split into resolved (non-null assetId) and unresolved (null assetId)
    c. Branch: `unresolved_positions` is non-empty -> set `position_set.__unresolved_positions`

**Raises:** `MqValueError` for invalid weight/quantity/notional combinations.

### PositionSet.price_many(cls, position_sets: List[PositionSet], currency: Optional[Currency] = Currency.USD, weighting_strategy: PositionSetWeightingStrategy = None, carryover_positions_for_missing_dates: bool = False, should_reweight: bool = False, allow_fractional_shares: bool = False, allow_partial_pricing: bool = False, batch_size: int = 20, **kwargs)
Purpose: Bulk-price multiple position sets, modifying them in place.

**Algorithm:**
1. Convert to DataFrame via `to_frame_many`, drop all-NaN columns
2. Validate: raise `MqValueError` if both weight+quantity or notional+weight present
3. Branch: `weighting_strategy` is None:
   a. Branch: has weight+reference_notional -> Weight
   b. Branch: has notional -> Notional
   c. Branch: else -> Quantity
4. Validate weighting strategy is one of Weight/Quantity/Notional; raise `MqValueError` otherwise
5. Branch: Quantity strategy but no quantity column -> raise `MqValueError`
6. Set `should_allow_fractional_shares`: `True` if Notional, else use `allow_fractional_shares` param
7. Build `PositionsPricingParameters`
8. Branch: `kwargs` -> set additional attributes on parameters
9. Log warnings for positions missing values based on weighting strategy
10. Ensure weight/notional/quantity columns exist (fill with None if missing)
11. Vectorize creation of `PositionsRequest` objects
12. Group by date, build `PositionSetRequest` per date (with target_notional only for Weight strategy)
13. Split into batches of `batch_size`
14. For each batch:
    a. Build `PositionsPricingRequest`, call `_repeat_try_catch_request(GsPriceApi.price_many_positions, ...)`
    b. Branch: `MqRequestError` caught:
       - Log error with date range info
       - Branch: `not allow_partial_pricing` -> re-raise
       - Branch: `allow_partial_pricing` -> continue
15. Build `date_to_priced_position_sets` map
16. For each input position set:
    a. Branch: date not a `dt.date` -> convert via `pd.to_datetime`
    b. Branch: no priced data for date -> set `__unpriced_positions = list(positions)`, `positions = None`, continue
    c. Determine merge columns based on weighting strategy (Weight -> weight/referenceWeight, Notional -> notional/notional, Quantity -> quantity/quantity)
    d. Drop duplicate priced positions on `[assetId, merge_column]`
    e. Round merge columns to 5 decimal places for matching
    f. Merge original with priced results
    g. Branch: Weight strategy -> split priced/unpriced by `weight` null check
    h. Branch: else -> split by `quantity` null check
    i. Build Position objects from priced records; build unpriced Position objects from unpriced records
    j. Update `input_position_set.positions` and `__unpriced_positions`
17. Log total processing time

**Raises:** `MqValueError` for invalid combinations or missing data; `MqRequestError` re-raised if `allow_partial_pricing` is False.

## State Mutation
- `Position.__identifier`, `__weight`, `__quantity`, `__notional`, `__name`, `__asset_id`, `__tags`, `__restricted`, `__hard_to_borrow`: Set during `__init__`, modifiable via property setters; `_restricted` and `_hard_to_borrow` use underscore-prefixed setters for semi-private write access
- `PositionSet.__positions`: Set during `__init__`, modified by `resolve()`, `price()`, `remove_*` methods, `equalize_position_weights()`, `redistribute_weights()`, `resolve_many()`, `price_many()`
- `PositionSet.__date`: Set during `__init__`, modifiable via setter; `resolve_many` and `price_many` may convert type in place
- `PositionSet.__unresolved_positions`: Set during `__init__` (defaults to `[]`), updated by `resolve()`, cleared by `remove_unresolved_positions()`; `resolve_many` writes directly to private attribute
- `PositionSet.__unpriced_positions`: Set during `__init__` (defaults to `[]`), updated by `price()`, cleared by `remove_unpriced_positions()`; `price_many` writes directly to private attribute
- `PositionSet.__reference_notional`: Set during `__init__`, modified by `price()` when `handle_long_short` is True
- Thread safety: No locking; not thread-safe. Batch operations (`resolve_many`, `price_many`) mutate input position sets in place.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `PositionTag.from_dict` | Tag dict has more than one key-value pair |
| `MqValueError` | `Position.add_tag` | Tag with same name already exists |
| `MqValueError` | `Position.from_dict` | Both `id` and `asset_id` present |
| `MqValueError` | `PositionSet.__init__` | `reference_notional` set with positions missing weights |
| `MqValueError` | `PositionSet.__init__` | `reference_notional` set with positions having notional |
| `MqValueError` | `PositionSet.__init__` | `reference_notional` set with positions having quantity |
| `MqValueError` | `PositionSet.redistribute_weights` | Some positions missing weights |
| `MqValueError` | `PositionSet.get_subset` | A position has no tags |
| `MqValueError` | `PositionSet.price` | `fail_on_unpriced_positions` True and unpriced positions exist |
| `MqValueError` | `PositionSet.__get_default_weighting_strategy` | Cannot determine strategy (all sizing fields missing) |
| `MqValueError` | `PositionSet.__get_default_weighting_strategy` | Chosen strategy requires values that are missing |
| `MqValueError` | `PositionSet.__get_default_weighting_strategy` | Weight strategy without reference notional |
| `MqValueError` | `PositionSet.__convert_positions_for_pricing` | Positions missing asset IDs |
| `MqValueError` | `PositionSet.resolve_many` | Both weight and quantity present |
| `MqValueError` | `PositionSet.resolve_many` | Both quantity and notional present |
| `MqValueError` | `PositionSet.price_many` | Both weight and quantity present |
| `MqValueError` | `PositionSet.price_many` | Both weight and notional present |
| `MqValueError` | `PositionSet.price_many` | Invalid weighting strategy |
| `MqValueError` | `PositionSet.price_many` | Quantity strategy but no quantity column |
| `MqRequestError` | `PositionSet.price_many` | API pricing request failure (re-raised if not partial pricing) |

## Edge Cases
- `Position.__eq__` with None numeric values: If either weight/notional/quantity is None, the numeric comparison is skipped, which means `None == 0.0` would be treated as equal (both skip numeric round check since one is None, and then the condition `not (slf is None or oth is None)` is False so it doesn't return False).
- `Position.__hash__` with None asset_id: `hash(None)` is valid in Python, so this works but all positions with None asset_id and the same identifier will collide.
- `PositionSet.equalize_position_weights` with empty positions: `1 / len([])` would raise `ZeroDivisionError`.
- `PositionSet.from_list` with empty list: Same `ZeroDivisionError` from `1 / len(positions)`.
- `PositionSet.from_frame` with empty DataFrame (after NaN filter): Same `ZeroDivisionError` from `1 / len(positions)`.
- `PositionSet.__eq__` does not check for `PositionSet` type on `other`; would raise `AttributeError` if `other` lacks `.positions`.
- `resolve_many` writes directly to `position_set.__unresolved_positions` which accesses name-mangled attribute; this works only because the class method is within the same class.
- `price_many` sets `input_position_set.positions = None` when no priced data exists, which breaks the `List[Position]` type contract.
- `Position.tags_as_dict` will raise `TypeError` if `self.tags` is None (iterating over None).
- `redistribute_weights` division by zero: If `total_weight` is 0 (all weights are 0), `p.weight / total_weight` will raise `ZeroDivisionError`.

## Coverage Notes
- Branch count: ~85
- Key branches in `__get_default_weighting_strategy`: 7 distinct branches for weighting strategy selection and validation
- Key branches in `price`: 6 branches (fractional_shares, dataset, handle_long_short, fail_on_unpriced)
- Key branches in `price_many`: ~15 branches (strategy detection, validation, warnings, error handling, merge logic, date type conversion)
- Key branches in `resolve_many`: ~10 branches (column drops, validation, date conversion, unresolved check)
- `__build_positions_from_frame` vectorized: falsy checks on weights/notionals/quantities convert 0 to None (since `0 if 0 else None` -> `None`)
- `Position.__eq__` has 5 properties * 2-3 branches each = ~12 branch paths
