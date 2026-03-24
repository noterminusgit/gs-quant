# baskets.py

## Summary
Defines the `Basket` class, a high-level abstraction for managing custom equity baskets in the Goldman Sachs Marquee platform. A Basket wraps a `GsAsset` and `PositionedEntity`, providing methods to create, update (edit/rebalance), clone, and query baskets. The module includes a `_validate` decorator that enforces initialization state and permission checks before method execution, and an `ErrorMessage` enum for standardized error messaging. Properties cover all configurable basket attributes (pricing, publishing, entitlements, composition) with setter-level validation.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAsset, GsAssetApi)
- Internal: `gs_quant.api.gs.data` (GsDataApi)
- Internal: `gs_quant.api.gs.indices` (GsIndexApi)
- Internal: `gs_quant.api.gs.reports` (GsReportApi)
- Internal: `gs_quant.api.gs.users` (GsUsersApi)
- Internal: `gs_quant.common` (DateLimit, PositionType, EqBasketBacktestParameters, EqBasketHistoryMethodology, BloombergPublishParameters, CashReinvestmentTreatment, CashReinvestmentTreatmentType, EqBasketRebalanceCalendar, AssetClass)
- Internal: `gs_quant.data.fields` (DataMeasure)
- Internal: `gs_quant.entities.entitlements` (Entitlements as BasketEntitlements)
- Internal: `gs_quant.entities.entity` (EntityType, PositionedEntity)
- Internal: `gs_quant.errors` (MqError, MqValueError)
- Internal: `gs_quant.json_encoder` (JSONEncoder)
- Internal: `gs_quant.markets.indices_utils` (BasketType, IndicesDatasets, ReturnType, WeightingStrategy, CorporateActionType)
- Internal: `gs_quant.markets.position_set` (PositionSet)
- Internal: `gs_quant.markets.securities` (Asset, AssetType as SecAssetType)
- Internal: `gs_quant.session` (GsSession)
- Internal: `gs_quant.target.data` (DataQuery)
- Internal: `gs_quant.target.indices` (CustomBasketsCreateInputs, CustomBasketsPricingParameters, PublishParameters, IndicesPositionInput, IndicesPositionSet, CustomBasketsBackcastInputs, CustomBasketsRebalanceAction, CustomBasketRiskParams, IndicesCurrency, CustomBasketsEditInputs, CustomBasketsResponse, CustomBasketsRebalanceInputs)
- Internal: `gs_quant.target.reports` (Report, ReportStatus)
- External: `datetime` (date, timedelta, datetime.strptime)
- External: `json` (loads, dumps)
- External: `logging` (getLogger)
- External: `copy` (deepcopy)
- External: `enum` (Enum)
- External: `functools` (wraps)
- External: `typing` (List, Optional, Union, Tuple)
- External: `pandas` (pd.DataFrame)
- External: `pydash` (has, get, set_)

## Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Enums and Constants

### ErrorMessage(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `NON_ADMIN` | `'You are not permitted to perform this action on this basket...'` | User lacks admin entitlements on the basket |
| `NON_INTERNAL` | `'You are not permitted to access this basket setting.'` | User is not an internal GS user |
| `RESTRICTED_ATTRIBUTE` | `'You are not permitted to access this basket setting'` | User lacks restricted attribute token |
| `UNINITIALIZED` | `'Basket class object must be initialized using one of an existing basket\'s identifiers...'` | Basket object has not been initialized from an existing asset |
| `UNMODIFIABLE` | `'This property can not be modified since the basket has already been created'` | Attempting to modify an immutable property on an existing basket |

## Type Definitions

### Basket (class)
Inherits: `Asset`, `PositionedEntity` (multiple inheritance)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__error_messages` | `Optional[Set[ErrorMessage]]` | `None` initially, then `set()` | Set of error messages representing current restriction state |
| `__id` | `str` | set from `gs_asset.id` | Marquee asset ID |
| `__initial_entitlements` | `dict` | from `gs_asset.entitlements` | Original entitlements at load time (for change detection) |
| `__initial_state` | `dict` | `{}` | Snapshot of initial property values (for change detection in `update`) |
| `__initial_positions` | `Set[Position]` | set of deepcopy of initial positions | For detecting position changes during update |
| `__latest_create_report` | `Report` | `None` | Most recent create/edit/rebalance report |
| `__allow_ca_restricted_assets` | `Optional[bool]` | `None` | Allow corporate-action-restricted constituents |
| `__allow_limited_access_assets` | `Optional[bool]` | `None` | Allow limited-access constituents |
| `__asset_class` | `Optional[AssetClass]` | from gs_asset | Asset class of the basket |
| `__backtest_parameters` | `Optional[EqBasketBacktestParameters]` | `None` | Backtest configuration |
| `__benchmark` | `Optional[str]` | `None` | Benchmark identifier |
| `__bloomberg_publish_parameters` | `Optional[BloombergPublishParameters]` | `None` | Bloomberg publish overrides |
| `__cash_reinvestment_treatment` | `Optional[CashReinvestmentTreatment]` | Default: all `Reinvest_At_Open` | Cash reinvestment treatment options |
| `__clone_parent_id` | `Optional[str]` | `None` | Marquee ID of source basket for clones |
| `__currency` | `Optional[IndicesCurrency]` | `None` | Denomination currency |
| `__default_backcast` | `Optional[bool]` | `True` (new) | Whether to backcast using current composition |
| `__description` | `Optional[str]` | `None` | Free text description |
| `__divisor` | `Optional[float]` | `None` | Divisor for position set; mutually exclusive with initial_price |
| `__entitlements` | `Optional[BasketEntitlements]` | from gs_asset entitlements | Current entitlements |
| `__flagship` | `Optional[bool]` | `None` | Whether basket is flagship (internal only) |
| `__gs_asset_type` | `Optional[type]` | from `gs_asset.type` | Asset type from GsAsset |
| `__hedge_id` | `Optional[str]` | `None` | Marquee ID of source hedge |
| `__historical_methodology` | `Optional[EqBasketHistoryMethodology]` | `Backcast` (new) | Historical methodology |
| `__include_price_history` | `Optional[bool]` | `False` | Include full price history for Bloomberg publishing |
| `__initial_price` | `Optional[float]` | `100` (new, if divisor is None) | Starting tick price |
| `__live_date` | `Optional[dt.date]` | from gs_asset | Date basket went live |
| `__name` | `Optional[str]` | `None` | Display name (max 24 chars) |
| `__parent_basket` | `Optional[str]` | `None` | Ticker of source basket |
| `__position_set` | `Optional[PositionSet]` | `None` (new) / fetched (existing) | Current position set composition |
| `__preferred_risk_model` | `Optional[str]` | `None` | Preferred risk model ID |
| `__publish_to_bloomberg` | `Optional[bool]` | `True` (new) | Publish to Bloomberg |
| `__publish_to_factset` | `Optional[bool]` | `False` (new) | Publish to Factset |
| `__publish_to_reuters` | `Optional[bool]` | `False` (new) | Publish to Reuters |
| `__rebalance_calendar` | `Optional[EqBasketRebalanceCalendar]` | `None` | Rebalance frequency |
| `__return_type` | `Optional[ReturnType]` | `None` | Index calculation methodology for dividends |
| `__reweight` | `Optional[bool]` | `None` | Reweight if weights don't sum to 1 |
| `__target_notional` | `Optional[float]` | `10000000` (new) | Target notional for position set |
| `__ticker` | `Optional[str]` | `None` | 8-character basket identifier |
| `__weighting_strategy` | `Optional[WeightingStrategy]` | `None` | Pricing strategy |
| `__pricing_date` | `Optional[dt.date]` | `None` | Pricing date for rebalance |
| `__action_date` | `Optional[dt.date]` | `None` | User's current action date |
| `__allow_system_approval` | `Optional[bool]` | `None` | Allow system auto-approval of rebalance |

## Functions/Methods

### _validate(*error_msgs) -> decorator
Purpose: Decorator factory that checks basket initialization state and permission errors before calling the decorated function.

**Algorithm:**
1. Return `_outer(fn)` decorator
2. `_outer` returns `_inner(self, *args, **kwargs)` wrapper (preserves original function metadata via `@wraps`)
3. Inside `_inner`:
   a. Branch: `has(self, '_Basket__error_messages') and self._Basket__error_messages is not None`:
      - Branch: `len(self._Basket__error_messages) < 1` -> call `self._Basket__finish_initialization()` (lazy initialization on first validated access)
      - For each `error_msg` in `error_msgs`:
        - Branch: `error_msg in self._Basket__error_messages` -> raise `MqError(error_msg.value)`
   b. Call and return `fn(self, *args, **kwargs)`

**Raises:** `MqError` with the specific `ErrorMessage.value` string.

---

### Basket.__init__(self, gs_asset: GsAsset = None, **kwargs)
Purpose: Initialize basket from an existing GsAsset or with defaults for a new basket.

**Algorithm:**
1. Set `self.__error_messages = None`
2. Branch: `gs_asset` is truthy:
   a. Branch: `gs_asset.type.value not in BasketType.to_list()` -> raise `MqValueError(f'Failed to initialize. Asset {gs_asset.id} is not a basket')`
   b. Store `__id`, `__initial_entitlements`
   c. Serialize `gs_asset` to dict via `json.loads(json.dumps(gs_asset.as_dict(), cls=JSONEncoder))`
   d. Call `Asset.__init__(self, ...)` with id, asset_class, name, exchange, currency, entity
   e. Call `PositionedEntity.__init__(self, gs_asset.id, EntityType.ASSET)`
   f. Call `self.__populate_current_attributes_for_existing_basket(gs_asset)`
3. Branch: `gs_asset` is falsy:
   a. Call `self.__populate_default_attributes_for_new_basket(**kwargs)`
4. Set `self.__error_messages = set([])`
5. Branch: `get(kwargs, '_finish_init', False)` is True -> call `self.__finish_initialization()`

**Raises:** `MqValueError` when gs_asset type is not a basket type.

### Basket.get(cls, identifier: str, **kwargs) -> Basket
Purpose: Class method to fetch an existing basket by identifier.

**Algorithm:**
1. Call `cls.__get_gs_asset(identifier)` to resolve and fetch the GsAsset
2. Return `cls(gs_asset=gs_asset, _finish_init=get(kwargs, '_finish_init', True))`

### Basket.get_details(self) -> pd.DataFrame
Purpose: Get current basket properties as a DataFrame.
Decorated: `@_validate()`

**Algorithm:**
1. Collect all property names from `CustomBasketsPricingParameters`, `PublishParameters`, `CustomBasketsCreateInputs`
2. Sort property names
3. For each property: if `has(self, k)`, include `{name: k, value: get(self, k)}`
4. Return `pd.DataFrame(details)`

### Basket.create(self) -> dict
Purpose: Create a new custom basket in Marquee.

**Algorithm:**
1. Build `inputs`, `pricing`, `publish` dicts from respective property sets using `get(self, prop)` / `set_(...)`
2. Set `position_set` on inputs via `self.position_set.to_target(common=False)`
3. Set `pricing_parameters` and `publish_parameters` as typed objects
4. Build `CustomBasketsCreateInputs(**inputs)`
5. Call `GsIndexApi.create(create_inputs)` -> response
6. Fetch `GsAsset` for the new asset, fetch report
7. Re-initialize self via `self.__init__(gs_asset=gs_asset, _finish_init=True)`
8. Return `response.as_dict()`

### Basket.clone(self) -> Basket
Purpose: Create a new Basket instance with a deep copy of the current position set.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. `deepcopy(self.position_set)`
2. Return `Basket(position_set=position_set, clone_parent_id=self.id, parent_basket=self.ticker)`

### Basket.update(self) -> dict
Purpose: Update an existing basket (edit metadata, rebalance, or both).
Decorated: `@_validate(ErrorMessage.UNINITIALIZED, ErrorMessage.NON_ADMIN)`

**Algorithm:**
1. Call `self.__get_updates()` -> `(edit_inputs, rebal_inputs)`
2. Initialize `response = None`
3. Compare initial and current entitlements:
   a. Branch: entitlements changed -> call `GsAssetApi.update_asset_entitlements(...)`; set `response`
4. Branch: `edit_inputs is None and rebal_inputs is None`:
   a. Branch: `response` (entitlements were updated) -> return `response`
   b. Branch: no response -> raise `MqValueError('Update failed: Nothing on the basket was changed')`
5. Branch: `edit_inputs is not None and rebal_inputs is None` -> `GsIndexApi.edit(...)`
6. Branch: `rebal_inputs is not None and edit_inputs is None` -> `GsIndexApi.rebalance(...)`
7. Branch: both not None -> `self.__edit_and_rebalance(edit_inputs, rebal_inputs)`
8. Fetch updated GsAsset, fetch report
9. Re-initialize self via `self.__init__(gs_asset=gs_asset, _finish_init=True)`
10. Return `response.as_dict()`

**Raises:** `MqValueError` when nothing was changed.

### Basket.upload_position_history(self, position_sets: List[PositionSet]) -> dict
Purpose: Upload historical composition for a basket.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED, ErrorMessage.NON_ADMIN)`

**Algorithm:**
1. Branch: `self.default_backcast` is True -> raise `MqValueError('Unable to upload position history: option must be set during basket creation')`
2. For each position_set:
   a. Call `self.__validate_position_set(position_set)`
   b. Build `IndicesPositionInput(p.asset_id, p.weight)` for each position
   c. Build `IndicesPositionSet(positions_tuple, position_set.date)`
3. Call `GsIndexApi.backcast(self.id, CustomBasketsBackcastInputs(tuple(...)))`
4. Return `response.as_dict()`

**Raises:** `MqValueError` when `default_backcast` is True.

### Basket.poll_status(self, timeout: int = 600, step: int = 30) -> ReportStatus
Purpose: Poll the status of the most recent basket report.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Get report from `self.__latest_create_report` (fallback to `self.__get_latest_create_report()`)
2. Get report ID
3. Delegate to `self.poll_report(report_id, timeout, step)` (inherited from PositionedEntity)

### Basket.get_latest_rebalance_data(self) -> dict
Purpose: Retrieve the most recent rebalance data.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Return `GsIndexApi.last_rebalance_data(self.id)`

### Basket.get_latest_rebalance_date(self) -> dt.date
Purpose: Retrieve the most recent rebalance date.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Call `GsIndexApi.last_rebalance_data(self.id)`
2. Parse `date` field via `dt.datetime.strptime(date_str, '%Y-%m-%d').date()`

### Basket.get_rebalance_approval_status(self) -> str
Purpose: Retrieve the most recent rebalance submission approval status.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Call `GsIndexApi.last_rebalance_approval(self.id)`
2. Return `get(last_approval, 'status')`

### Basket.cancel_rebalance(self) -> dict
Purpose: Cancel the most recent rebalance submission.
Decorated: `@_validate(ErrorMessage.NON_ADMIN)`

**Algorithm:**
1. Return `GsIndexApi.cancel_rebalance(self.id, CustomBasketsRebalanceAction.default_instance())`

### Basket.get_corporate_actions(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today() + dt.timedelta(days=10), ca_type: List[CorporateActionType] = CorporateActionType.to_list()) -> pd.DataFrame
Purpose: Retrieve corporate actions for the basket across a date range.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Build `where` dict with assetId, corporateActionType
2. Build `DataQuery` with where, start_date, end_date
3. Call `GsDataApi.query_data(query=query, dataset_id=IndicesDatasets.CORPORATE_ACTIONS.value)`
4. Return `pd.DataFrame(response)`

### Basket.get_fundamentals(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), period: DataMeasure = DataMeasure.ONE_YEAR.value, direction: DataMeasure = DataMeasure.FORWARD.value, metrics: List[DataMeasure] = DataMeasure.list_fundamentals()) -> pd.DataFrame
Purpose: Retrieve fundamentals data for the basket across a date range.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Build `where` dict with assetId, period, periodDirection, metric
2. Build `DataQuery` with where, start_date, end_date
3. Call `GsDataApi.query_data(query=query, dataset_id=IndicesDatasets.BASKET_FUNDAMENTALS.value)`
4. Return `pd.DataFrame(response)`

### Basket.get_live_date(self) -> Optional[dt.date]
Purpose: Retrieve basket's live date.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Return `self.__live_date`

### Basket.get_type(self) -> Optional[SecAssetType]
Purpose: Retrieve basket type as a SecAssetType enum.

**Algorithm:**
1. Branch: `self.__gs_asset_type` is truthy -> return `SecAssetType[self.__gs_asset_type.name.upper()]`
2. Branch: falsy -> return `None` (implicit)

### Basket.get_latest_position_set(self, position_type: PositionType = PositionType.CLOSE, source: str = "Basket") -> PositionSet
Purpose: Get the latest position set for the basket.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`:
   a. Call `GsAssetApi.get_latest_positions(self.id, position_type)`
   b. Return `PositionSet.from_target(response, source=source)`
2. Branch: else -> raise `NotImplementedError`

### Basket.get_position_set_for_date(self, date: dt.date, position_type: PositionType = PositionType.CLOSE, source: str = "Basket") -> PositionSet
Purpose: Get the position set for a specific date.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`:
   a. Call `GsAssetApi.get_asset_positions_for_date(self.id, date, position_type)`
   b. Branch: `len(response) == 0` -> log info, return `PositionSet([], date=date)`
   c. Return `PositionSet.from_target(response[0], source=source)`
2. Branch: else -> raise `NotImplementedError`

### Basket.get_position_sets(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), position_type: PositionType = PositionType.CLOSE, source: str = "Basket") -> List[PositionSet]
Purpose: Get position sets across a date range.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`:
   a. Call `GsAssetApi.get_asset_positions_for_dates(self.id, start, end, position_type)`
   b. Branch: `len(response) == 0` -> log info, return `[]`
   c. Return list of `PositionSet.from_target(ps, source=source)` for each position set
2. Branch: else -> raise `NotImplementedError`

### Basket.get_url(self) -> str
Purpose: Build URL to basket's product page in Marquee.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED)`

**Algorithm:**
1. Check `GsSession.current.domain`:
   a. Branch: contains `'dev'` -> `env = '-dev'`
   b. Branch: contains `'qa'` -> `env = '-qa'`
   c. Branch: else -> `env = ''`
2. Return `f'https://marquee{env}.gs.com/s/products/{self.id}/summary'`

**Note:** The `qa` check overwrites the `dev` check, so if domain contains both, `qa` wins.

### Basket.add_factor_risk_report(self, risk_model_id: str, fx_hedged: bool)
Purpose: Create and schedule a factor risk report.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED, ErrorMessage.NON_ADMIN)`

**Algorithm:**
1. Build `CustomBasketRiskParams(risk_model=risk_model_id, fx_hedged=fx_hedged)`
2. Return `GsIndexApi.update_risk_reports(payload)`

### Basket.delete_factor_risk_report(self, risk_model_id: str)
Purpose: Delete a factor risk report.
Decorated: `@_validate(ErrorMessage.UNINITIALIZED, ErrorMessage.NON_ADMIN)`

**Algorithm:**
1. Build `CustomBasketRiskParams(risk_model=risk_model_id, delete=True)`
2. Return `GsIndexApi.update_risk_reports(payload)`

---

### Property Getters and Setters

All properties follow the pattern: getter returns the private field, setter (when present) is decorated with `@_validate(ErrorMessage.*)` to enforce permissions.

#### allow_ca_restricted_assets (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`.

#### allow_limited_access_assets (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`.

#### asset_class (getter/setter)
Type: `Optional[AssetClass]`. Setter validated: `UNMODIFIABLE` (cannot change after creation).

#### benchmark (getter/setter)
Type: `Optional[str]`. Setter validated: `NON_ADMIN`, `NON_INTERNAL`.

#### backtest_parameters (getter/setter)
Type: `Optional[EqBasketBacktestParameters]`. Setter validated: `NON_ADMIN`.
**Side effect:** Branch: `value is not None` -> sets `self.__historical_methodology = EqBasketHistoryMethodology.Backtest`.

#### bloomberg_publish_parameters (getter/setter)
Type: `Optional[BloombergPublishParameters]`. Setter validated: `NON_ADMIN`, `NON_INTERNAL`.

#### cash_reinvestment_treatment (getter/setter)
Type: `Optional[CashReinvestmentTreatment]`. Setter validated: `NON_ADMIN`.
**Algorithm:**
1. Branch: `isinstance(value, CashReinvestmentTreatmentType)` -> construct `CashReinvestmentTreatment` with all three treatments set to `value`
2. Branch: else -> store `value` directly

#### clone_parent_id (getter only)
Type: `Optional[str]`. Read-only.

#### currency (getter/setter)
Type: `Optional[IndicesCurrency]`. Setter validated: `UNMODIFIABLE`.

#### default_backcast (getter/setter)
Type: `Optional[bool]`. Setter validated: `UNMODIFIABLE`.
**Side effect:** Branch: `not value` -> sets `self.__historical_methodology = EqBasketHistoryMethodology.Custom`.

#### description (getter/setter)
Type: `Optional[str]`. Setter validated: `NON_ADMIN`.

#### divisor (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.
**Side effect:** Setter clears `self.__initial_price = None`.

#### entitlements (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.
Type: `Optional[BasketEntitlements]`.

#### flagship (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_INTERNAL`.

#### hedge_id (getter only)
Type: `Optional[str]`. Read-only.

#### historical_methodology (getter/setter)
Type: `Optional[EqBasketHistoryMethodology]`. Setter validated: `NON_ADMIN`.
**Side effect:** Sets `self.__default_backcast = (value != EqBasketHistoryMethodology.Custom)`.

#### include_price_history (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`.

#### initial_price (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.
**Side effect:** Setter clears `self.__divisor = None`.

#### name (getter/setter)
Type: `Optional[str]`. Setter validated: `NON_ADMIN`.
**Side effect:** Branch: `len(value) > 24` -> logs info warning (does not raise).

#### parent_basket (getter/setter)
Getter has lazy resolution logic. Setter validated: `UNMODIFIABLE`.
**Getter algorithm:**
1. Branch: `has(self, '__clone_parent_id') and not has(self, '__parent_basket')` -> fetch asset and set `__parent_basket`
2. Return `self.__parent_basket`

**Setter algorithm:**
1. Resolve parent via `self.__get_gs_asset(value)` -> set `__clone_parent_id`
2. Set `self.__parent_basket = value`

#### position_set (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.
**Setter algorithm:**
1. Call `self.__validate_position_set(value)`
2. Store value

#### preferred_risk_model (getter/setter)
Type: `Optional[str]`. Setter validated: `NON_ADMIN`, `NON_INTERNAL`.

#### publish_to_bloomberg (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.

#### publish_to_factset (getter/setter)
Getter decorated: `@_validate()`. Setter validated: `NON_ADMIN`.

#### publish_to_reuters (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`.

#### rebalance_calendar (getter/setter)
Type: `Optional[EqBasketRebalanceCalendar]`. Setter validated: `NON_ADMIN`, `NON_INTERNAL`.

#### return_type (getter/setter)
Type: `Optional[ReturnType]`. Setter validated: `NON_ADMIN`.

#### reweight (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`.

#### target_notional (getter/setter)
Type: `Optional[float]`. Setter validated: `NON_ADMIN`.

#### ticker (getter/setter)
Type: `Optional[str]`. Setter validated: `UNMODIFIABLE`.
**Setter algorithm:**
1. Call `self.__validate_ticker(value)` (validates length and API check)
2. Store value

#### weighting_strategy (getter/setter)
Type: `Optional[WeightingStrategy]`. Setter validated: `NON_ADMIN`.

#### pricing_date (getter/setter)
Type: `Optional[dt.date]`. Setter validated: `NON_ADMIN`, `RESTRICTED_ATTRIBUTE`.

#### action_date (getter/setter)
Type: `Optional[dt.date]`. Setter validated: `NON_ADMIN`, `RESTRICTED_ATTRIBUTE`.

#### allow_system_approval (getter/setter)
Type: `Optional[bool]`. Setter validated: `NON_ADMIN`, `RESTRICTED_ATTRIBUTE`.

---

### Basket.__edit_and_rebalance(self, edit_inputs: CustomBasketsEditInputs, rebal_inputs: CustomBasketsRebalanceInputs) -> CustomBasketsResponse
Purpose: When both edit and rebalance are needed, perform edit first, wait for completion, then rebalance.

**Algorithm:**
1. Log info message
2. Call `GsIndexApi.edit(self.id, edit_inputs)` -> response
3. Store report, poll report status with timeout=600, step=15
4. Branch: `report_status != ReportStatus.done` -> raise `MqError` (rebalance not submitted)
5. Log success, call `GsIndexApi.rebalance(self.id, rebal_inputs)`
6. Return rebalance response

**Raises:** `MqError` when edit report does not complete successfully.

### Basket.__finish_initialization(self)
Purpose: Lazy-load remaining data not retrieved during initial construction.

**Algorithm:**
1. Branch: `has(self, 'id')`:
   a. Branch: `not has(self, '__initial_positions')`:
      - Fetch latest positions via `GsAssetApi.get_latest_positions(self.id, PositionType.ANY)`
      - Convert to `PositionSet.from_target`, store as `__position_set`
      - Extract divisor, create `__initial_positions` as `set(deepcopy(positions))`
      - Store in `__initial_state`
   b. Branch: `not has(self.__initial_state, 'initial_price')`:
      - Fetch initial price from `GsIndexApi.initial_price(self.id, dt.date.today())`
      - Store in `__initial_price` and `__initial_state`
   c. Branch: `not has(self.__initial_state, 'publish_to_bloomberg')`:
      - Fetch latest create report parameters
      - Set `__publish_to_bloomberg`, `__publish_to_factset`, `__publish_to_reuters`, `__backtest_parameters`, `__bloomberg_publish_parameters`
      - Store all in `__initial_state`
   d. Branch: `not has(self, '__entitlements')`:
      - Create `BasketEntitlements.from_target(self.__initial_entitlements)`
2. Call `self.__set_error_messages()`

### Basket.__get_gs_asset(identifier: str) -> GsAsset (static)
Purpose: Resolve a basket identifier to a GsAsset.

**Algorithm:**
1. Call `GsAssetApi.resolve_assets(identifier=[identifier], fields=['id'], limit=1)`
2. Branch: `len(response) == 0 or get(response, '0.id') is None` -> raise `MqValueError`
3. Return `GsAssetApi.get_asset(get(response, '0.id'))`

**Raises:** `MqValueError` when identifier cannot be resolved.

### Basket.__get_latest_create_report(self) -> Report
Purpose: Fetch the most recent basket create report.

**Algorithm:**
1. Call `GsReportApi.get_reports(limit=1, position_source_id=self.id, report_type='Basket Create', order_by='>latestExecutionTime')`
2. Return `get(report, '0')`

### Basket.__get_updates(self) -> Tuple[Optional[CustomBasketsEditInputs], Optional[CustomBasketsRebalanceInputs]]
Purpose: Compare initial and current basket state to determine what updates are needed.

**Algorithm:**
1. Initialize tracking dicts and flags: `edit_inputs`, `eligible_for_edit`, `rebal_inputs`, `eligible_for_rebal`, `pricing`, `pricing_updated`, `publish`, `publish_updated`
2. Check if positions changed: `self.__initial_positions != set(self.position_set.positions)`
3. For each `CustomBasketsEditInputs` property (excluding `action_date`):
   a. Branch: initial state differs from current -> set `eligible_for_edit = True`
   b. Store current value in `edit_inputs`
4. For each `CustomBasketsRebalanceInputs` property (excluding `position_set`, `allow_system_approval`, `action_date`):
   a. Branch: initial state differs -> set `eligible_for_rebal = True`
   b. Store value (skip `position_set`)
5. For each `CustomBasketsPricingParameters` property:
   a. Branch: initial state differs -> set `pricing_updated = True`
   b. Store value
6. For each `PublishParameters` property:
   a. Branch: initial state differs -> set `publish_updated = True`
   b. Store value
7. Determine actions:
   - `should_rebal = pricing_updated or positions_updated or (eligible_for_rebal and not eligible_for_edit)`
   - `should_edit = (publish_updated and not should_rebal) or eligible_for_edit`
8. Branch: `should_rebal`:
   a. Branch: `positions_updated` -> add `position_set` to `rebal_inputs`
   b. Branch: `pricing_updated` -> add `pricing_parameters`
   c. Branch: `publish_updated` -> add `publish_parameters`
9. Branch: `should_edit` -> add `publish_parameters` to `edit_inputs`
10. Construct typed objects (or None) and return tuple

### Basket.__populate_current_attributes_for_existing_basket(self, gs_asset: GsAsset)
Purpose: Extract and store all relevant attributes from a fetched GsAsset.

**Algorithm:**
1. Extract attributes from `gs_asset` using `pydash.get` for nested paths (e.g., `parameters.benchmark`, `xref.ticker`)
2. Set `__include_price_history = False`
3. Initialize `__initial_state = {}`
4. For each property across all input/parameter classes: snapshot current value into `__initial_state`

### Basket.__populate_default_attributes_for_new_basket(self, **kwargs)
Purpose: Set default values for a new basket from kwargs.

**Algorithm:**
1. Set each attribute from kwargs with defaults:
   - `cash_reinvestment_treatment`: default `CashReinvestmentTreatment(all Reinvest_At_Open)`
   - `default_backcast`: default `True`
   - `historical_methodology`: default `EqBasketHistoryMethodology.Backcast`
   - `include_price_history`: default `False`
   - `initial_price`: default `100` if `__divisor is None`, else `None`
   - `publish_to_bloomberg`: default `True`
   - `publish_to_factset`: default `False`
   - `publish_to_reuters`: default `False`
   - `target_notional`: default `10000000`
2. Branch: `self.__parent_basket is not None and self.__clone_parent_id is None` -> resolve parent via `__get_gs_asset` and set `__clone_parent_id`

### Basket.__set_error_messages(self)
Purpose: Determine which error states apply to the current user/basket.

**Algorithm:**
1. Branch: `len(get(self, '__error_messages', [])) > 0` -> return early (already populated)
2. Fetch user tokens via `GsUsersApi.get_current_user_info()`
3. Branch: `'internal' not in user_tokens` -> add `ErrorMessage.NON_INTERNAL`
4. Branch: `'internal' not in user_tokens and 'group:EqBasketRestrictedAttributes' not in user_tokens` -> add `ErrorMessage.RESTRICTED_ATTRIBUTE`
5. Branch: `not has(self, 'id')` (new basket):
   a. Add `ErrorMessage.UNINITIALIZED`
6. Branch: else (existing basket):
   a. Add `ErrorMessage.UNMODIFIABLE`
   b. Get admin tokens from `self.__initial_entitlements`
   c. Branch: no user token matches admin tokens -> add `ErrorMessage.NON_ADMIN`
7. Store as `set(errors)`

### Basket.__validate_position_set(position_set: PositionSet) (static)
Purpose: Validate a position set before use (resolve, check for negatives, check for unresolved).

**Algorithm:**
1. Call `position_set.resolve()`
2. Collect positions with negative weight or quantity (using `(p.weight or 1) < 0` pattern)
3. Branch: `len(neg_pos) > 0` -> raise `MqValueError` with offending identifiers and date
4. Branch: `position_set.unresolved_positions is not None and len(...) > 0` -> raise `MqValueError` with unresolved identifiers

**Raises:** `MqValueError` for negative values or unresolved positions.

### Basket.__validate_ticker(ticker: str) (static)
Purpose: Validate a ticker string before setting.

**Algorithm:**
1. Branch: `len(ticker) != 8` -> raise `MqValueError('Invalid ticker: must be 8 characters')`
2. Call `GsIndexApi.validate_ticker(ticker)` (may raise on server side)
3. Branch: `ticker[:2] != 'GS'` -> log info suggestion to prefix with 'GS' for Bloomberg publishing

**Raises:** `MqValueError` when ticker is not exactly 8 characters.

## State Mutation
- `Basket.__error_messages`: Set to `None` at start of `__init__`, then `set([])` after attribute population, populated by `__set_error_messages()` with applicable error conditions
- `Basket.__init__` calls itself recursively after `create()` and `update()` complete, fully re-initializing the object state from the updated GsAsset
- `Basket.__initial_state`: Snapshot dict capturing all initial property values, used by `__get_updates()` to diff for changes
- `Basket.__initial_positions`: Set of deepcopy'd positions, used for change detection
- `Basket.__divisor` and `Basket.__initial_price`: Mutually exclusive -- setting one clears the other
- `Basket.__default_backcast` and `Basket.__historical_methodology`: Setting one updates the other to maintain consistency
- `Basket.__backtest_parameters` setter: sets `__historical_methodology` as side effect when value is not None
- `_validate` decorator: may trigger `__finish_initialization()` on first access (when `__error_messages` is empty set)
- Thread safety: No locking; not thread-safe. Multiple API calls in `__finish_initialization`, `__edit_and_rebalance`, and `update` are not atomic.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `Basket.__init__` | gs_asset type not in BasketType list |
| `MqValueError` | `Basket.__get_gs_asset` | Identifier cannot be resolved |
| `MqValueError` | `Basket.update` | Nothing changed on the basket |
| `MqValueError` | `Basket.upload_position_history` | default_backcast is True |
| `MqValueError` | `Basket.__validate_position_set` | Negative weights or quantities |
| `MqValueError` | `Basket.__validate_position_set` | Unresolved positions exist |
| `MqValueError` | `Basket.__validate_ticker` | Ticker not 8 characters |
| `MqError` | `_validate` decorator | Error message found in `__error_messages` |
| `MqError` | `Basket.__edit_and_rebalance` | Edit report did not complete successfully |
| `NotImplementedError` | `get_latest_position_set` | Entity type is not ASSET |
| `NotImplementedError` | `get_position_set_for_date` | Entity type is not ASSET |
| `NotImplementedError` | `get_position_sets` | Entity type is not ASSET |

## Edge Cases
- `_validate` decorator lazy initialization: When `__error_messages` is an empty set (length < 1), `__finish_initialization()` is called. This means the first validated property/method access on a new Basket triggers multiple API calls.
- `__set_error_messages` early return: If `__error_messages` already has content, the method returns without re-checking, meaning permission changes during a session are not detected.
- `get_url` environment detection: The `qa` check overwrites the `dev` result, so a domain containing both strings would resolve to `-qa`.
- `parent_basket` getter lazy resolution: Calls `GsAssetApi.get_asset` on first access if `__clone_parent_id` is set but `__parent_basket` is not.
- `name` setter: Does not reject names over 24 characters, only logs a warning, allowing invalid names to be set.
- `__validate_position_set` uses `(p.weight or 1) < 0` pattern: This means a weight of `0` is treated as `1` (truthy default), so a zero weight is never flagged as negative. Similarly, a `None` weight defaults to 1 and passes.
- `__populate_default_attributes_for_new_basket`: `initial_price` is `None` when `divisor` is provided via kwargs, establishing the mutual exclusion.
- `Basket.create()` and `update()` call `self.__init__()` again, fully reinitializing the object -- any external references to old state become stale.
- `clone()` returns a new `Basket` instance that has no `gs_asset` (it goes through the `else` branch of `__init__`), meaning it starts as an uninitialized/new basket.
- `get_position_set_for_date` returns empty `PositionSet([], date=date)` when no positions found, which could fail downstream if operations assume non-empty positions.

## Coverage Notes
- Branch count: ~95
- `_validate` decorator: 3 branches (has error_messages + not None, len < 1, error_msg in set)
- `Basket.__init__`: 3 branches (gs_asset truthy with type check, gs_asset falsy, _finish_init)
- `Basket.update`: 6 branches (entitlements changed, edit only, rebal only, both, nothing changed + response, nothing changed + no response)
- `Basket.__get_updates`: ~12 branches (4 loops with change detection, should_rebal/should_edit determination, nested conditionals)
- `Basket.__finish_initialization`: 4 conditional blocks (positions, initial_price, publish params, entitlements)
- `Basket.__set_error_messages`: 5 branches (early return, non-internal, restricted, uninitialized vs existing, non-admin)
- Property setters with side effects: `backtest_parameters`, `cash_reinvestment_treatment`, `default_backcast`, `divisor`, `historical_methodology`, `initial_price`, `name`, `parent_basket`, `ticker` each have internal branches
- `get_url`: 3 branches (dev, qa, default)
- `get_type`: 2 branches (type truthy, falsy)
- `get_position_set_for_date`: 3 branches (ASSET+empty, ASSET+data, not ASSET)
- `get_position_sets`: 3 branches (ASSET+empty, ASSET+data, not ASSET)
