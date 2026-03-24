# entity.py

## Summary
Core entity system for the GS Quant platform. Defines the abstract base `Entity` class (for first-class entities like countries, KPIs, risk models) and the abstract `PositionedEntity` mixin (for entities with positions such as portfolios and assets). Provides CRUD lookup, entitlements, position management, report retrieval/polling, ESG analytics, carbon analytics, thematic analytics, and factor scenario calculations. Concrete `Entity` subclasses (`Country`, `Subdivision`, `KPI`, `RiskModelEntity`) implement entity-type-specific accessors. `PositionedEntity` is intended to be mixed into classes like `PortfolioManager` and basket/index classes.

## Dependencies
- Internal:
  - `gs_quant.api.gs.assets` (`GsAssetApi`)
  - `gs_quant.api.gs.carbon` (`CarbonCard`, `GsCarbonApi`, `CarbonTargetCoverageCategory`, `CarbonScope`, `CarbonEmissionsAllocationCategory`, `CarbonEmissionsIntensityType`, `CarbonCoverageCategory`, `CarbonEntityType`, `CarbonAnalyticsView`)
  - `gs_quant.api.gs.data` (`GsDataApi`)
  - `gs_quant.api.gs.esg` (`ESGMeasure`, `GsEsgApi`, `ESGCard`)
  - `gs_quant.api.gs.indices` (`GsIndexApi`)
  - `gs_quant.api.gs.portfolios` (`GsPortfolioApi`)
  - `gs_quant.api.gs.reports` (`GsReportApi`)
  - `gs_quant.api.gs.thematics` (`ThematicMeasure`, `GsThematicApi`, `Region`)
  - `gs_quant.api.gs.scenarios` (`GsFactorScenarioApi`)
  - `gs_quant.common` (`DateLimit`, `PositionType`, `Currency`)
  - `gs_quant.data` (`DataCoordinate`, `DataFrequency`, `DataMeasure`)
  - `gs_quant.data.coordinate` (`DataDimensions`)
  - `gs_quant.entities.entitlements` (`Entitlements`)
  - `gs_quant.entities.entity_utils` (`_explode_data`)
  - `gs_quant.errors` (`MqError`, `MqValueError`)
  - `gs_quant.markets.indices_utils` (`BasketType`, `IndicesDatasets`)
  - `gs_quant.markets.position_set` (`PositionSet`)
  - `gs_quant.markets.report` (`PerformanceReport`, `FactorRiskReport`, `Report`, `ThematicReport`, `flatten_results_into_df`, `get_thematic_breakdown_as_df`, `ReturnFormat`)
  - `gs_quant.markets.scenario` (`Scenario`)
  - `gs_quant.session` (`GsSession`)
  - `gs_quant.target.data` (`DataQuery`)
  - `gs_quant.target.reports` (`ReportStatus`, `ReportType`)
- External:
  - `datetime` (dt.date, dt.datetime, dt.timedelta)
  - `logging` (getLogger)
  - `time` (sleep)
  - `abc` (ABCMeta, abstractmethod)
  - `dataclasses` (dataclass)
  - `enum` (Enum)
  - `typing` (Dict, List, Optional, Tuple, Union)
  - `deprecation` (deprecated decorator)
  - `pandas` (pd.DataFrame, pd.Series, pd.concat)
  - `pydash` (get)

## Type Definitions

### EntityKey (dataclass)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id_ | `str` | *(required)* | Marquee ID of the entity |
| entity_type | `EntityType` | *(required)* | The type of the entity |

### Entity (ABC, metaclass=ABCMeta)
Abstract base class for any first-class entity (country, KPI, subdivision, risk model, asset, etc.).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | *(required)* | Private; the Marquee ID |
| __entity_type | `EntityType` | *(required)* | Private; the entity's type enum |
| __entity | `Optional[Dict]` | `None` | Private; raw entity dict from API |

Class attribute:

| Field | Type | Value | Description |
|-------|------|-------|-------------|
| _entity_to_endpoint | `Dict[EntityType, str]` | see below | Maps EntityType to REST API path segment |

```
_entity_to_endpoint = {
    EntityType.ASSET: 'assets',
    EntityType.COUNTRY: 'countries',
    EntityType.SUBDIVISION: 'countries/subdivisions',
    EntityType.KPI: 'kpis',
    EntityType.PORTFOLIO: 'portfolios',
    EntityType.RISK_MODEL: 'risk/models',
    EntityType.DATASET: 'data/datasets',
}
```

### Country (Entity)
Concrete entity for countries.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| *(inherits Entity fields)* | | | |

Inner class: `Country.Identifier(EntityIdentifier)` with values `MARQUEE_ID = 'MQID'`, `NAME = 'name'`.

### Subdivision (Entity)
Concrete entity for country subdivisions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| *(inherits Entity fields)* | | | |

Inner class: `Subdivision.Identifier(EntityIdentifier)` with values `MARQUEE_ID = 'MQID'`, `name = 'name'`.

### KPI (Entity)
Concrete entity for KPIs.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| *(inherits Entity fields)* | | | |

Inner class: `KPI.Identifier(EntityIdentifier)` with values `MARQUEE_ID = "MQID"`, `name = 'name'`.

### RiskModelEntity (Entity)
Concrete entity for risk models.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| *(inherits Entity fields)* | | | |

Inner class: `RiskModelEntity.Identifier(EntityIdentifier)` with values `MARQUEE_ID = "MQID"`, `name = 'name'`.

### PositionedEntity (ABC, metaclass=ABCMeta)
Abstract mixin for entities that have positions (portfolios, assets).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | *(required)* | Private; Marquee ID |
| __entity_type | `EntityType` | *(required)* | Private; PORTFOLIO or ASSET typically |

### EntityIdentifier (Enum)
Empty base enum. Subclasses define per-entity identifier types.

## Enums and Constants

### EntityType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ASSET | `"asset"` | Asset entity |
| BACKTEST | `"backtest"` | Backtest entity |
| COUNTRY | `"country"` | Country entity |
| HEDGE | `"hedge"` | Hedge entity |
| KPI | `"kpi"` | KPI entity |
| PORTFOLIO | `"portfolio"` | Portfolio entity |
| REPORT | `"report"` | Report entity |
| RISK_MODEL | `"risk_model"` | Risk model entity |
| SUBDIVISION | `"subdivision"` | Country subdivision entity |
| DATASET | `"dataset"` | Dataset entity |
| SCENARIO | `"scenario"` | Scenario entity |

### ScenarioCalculationType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| FACTOR_SCENARIO | `"Factor Scenario"` | Factor-based scenario calculation |

### ScenarioCalculationMeasure(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SUMMARY | `"Summary"` | Summary of scenario results |
| ESTIMATED_FACTOR_PNL | `"Factor Pnl"` | PnL attributed to factors |
| ESTIMATED_PNL_BY_SECTOR | `"By Sector Pnl Aggregations"` | PnL aggregated by sector |
| ESTIMATED_PNL_BY_REGION | `"By Region Pnl Aggregations"` | PnL aggregated by region |
| ESTIMATED_PNL_BY_DIRECTION | `"By Direction Pnl Aggregations"` | PnL aggregated by direction |
| ESTIMATED_PNL_BY_ASSET | `"By Asset Pnl"` | PnL by individual asset |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### Entity.__init__(self, id_: str, entity_type: EntityType, entity: Optional[Dict] = None) -> None
Purpose: Initialize base entity with ID, type, and optional raw entity dict.

**Algorithm:**
1. Set `self.__id` to `id_`
2. Set `self.__entity_type` to `entity_type`
3. Set `self.__entity` to `entity`

### Entity.data_dimension (abstract property) -> str
Purpose: Return the data dimension key string for this entity type. Must be implemented by subclasses.

### Entity.entity_type() (abstract classmethod) -> EntityType
Purpose: Return the EntityType enum value for this class. Must be implemented by subclasses.

### Entity.get(cls, id_value: str, id_type: Union[EntityIdentifier, str], entity_type: Optional[Union[EntityType, str]] = None) -> Optional[Entity]
Purpose: Look up an entity by identifier value and type, optionally specifying entity type.

**Algorithm:**
1. Convert `id_type` to its `.value` if it is an Enum instance; otherwise use as-is
2. Branch: `entity_type is None`
   - True: Use `cls.entity_type()` to get the type; look up endpoint from `_entity_to_endpoint`
   - False: Convert `entity_type` to `.value` if Enum; look up endpoint from `_entity_to_endpoint[EntityType(entity_type)]`
3. Branch: `entity_type == 'asset'`
   - True: Import `SecurityMaster` and `AssetIdentifier` from `gs_quant.markets.securities`; return `SecurityMaster.get_asset(id_value, AssetIdentifier.MARQUEE_ID)`
4. Branch: `id_type == 'MQID'`
   - True: GET `/{endpoint}/{id_value}` from session
   - False: GET `/{endpoint}?{id_type.lower()}={id_value}` from session; extract `results.0` via pydash `get`
5. Branch: `result` is truthy
   - True: Return `cls._get_entity_from_type(result, EntityType(entity_type))`
   - False: Return `None` implicitly

### Entity._get_entity_from_type(cls, entity: Dict, entity_type: EntityType = None) -> Optional[Entity]
Purpose: Factory method to create the appropriate Entity subclass from a dict.

**Algorithm:**
1. Extract `id_` from `entity.get('id')`
2. Use `entity_type` if provided, else `cls.entity_type()`
3. Branch: `entity_type == EntityType.COUNTRY` -> return `Country(id_, entity=entity)`
4. Branch: `entity_type == EntityType.KPI` -> return `KPI(id_, entity=entity)`
5. Branch: `entity_type == EntityType.SUBDIVISION` -> return `Subdivision(id_, entity=entity)`
6. Branch: `entity_type == EntityType.RISK_MODEL` -> return `RiskModelEntity(id_, entity=entity)`
7. Otherwise: return `None` implicitly (no matching type)

### Entity.get_marquee_id(self) -> str
Purpose: Return the entity's Marquee ID.

**Algorithm:**
1. Return `self.__id`

### Entity.get_entity(self) -> Optional[Dict]
Purpose: Return the raw entity dict.

**Algorithm:**
1. Return `self.__entity`

### Entity.get_unique_entity_key(self) -> EntityKey
Purpose: Return a unique key combining ID and entity type.

**Algorithm:**
1. Return `EntityKey(self.get_marquee_id(), self.__entity_type)`

### Entity.get_data_coordinate(self, measure: Union[DataMeasure, str], dimensions: Optional[DataDimensions] = None, frequency: DataFrequency = DataFrequency.DAILY, availability=None) -> DataCoordinate
Purpose: Build a DataCoordinate for querying data for this entity.

**Algorithm:**
1. Get Marquee ID
2. Default `dimensions` to `{}` if None
3. Set `dimensions[self.data_dimension]` to the entity's ID
4. Convert `measure` to string if it's not already
5. Call `GsDataApi.get_data_providers(id_, availability)` and get the dict for `measure`
6. Branch: `frequency == DataFrequency.DAILY`
   - True: Get `daily_dataset_id` from available providers; return `DataCoordinate(dataset_id=daily_dataset_id, ...)`
7. Branch: `frequency == DataFrequency.REAL_TIME`
   - True: Get `rt_dataset_id` from available providers; return `DataCoordinate(dataset_id=rt_dataset_id, ...)`
8. Otherwise: return `None` implicitly (no matching frequency)

### Entity.get_entitlements(self) -> Entitlements
Purpose: Parse and return entitlements from the raw entity dict.

**Algorithm:**
1. Get `entitlements_dict` from `self.get_entity().get('entitlements')`
2. Branch: `entitlements_dict is None`
   - True: raise `ValueError('This entity does not have entitlements.')`
3. Return `Entitlements.from_dict(entitlements_dict)`

**Raises:** `ValueError` when entity has no entitlements key

---

### Country.__init__(self, id_: str, entity: Optional[Dict] = None) -> None
Purpose: Initialize a Country entity.

**Algorithm:**
1. Call `super().__init__(id_, EntityType.COUNTRY, entity)`

### Country.data_dimension (property) -> str
Purpose: Return `'countryId'`.

### Country.entity_type() (classmethod) -> EntityType
Purpose: Return `EntityType.COUNTRY`.

### Country.get_by_identifier(cls, id_value: str, id_type: Country.Identifier) -> Optional[Entity]
Purpose: Look up a country by identifier. Delegates to `Entity.get`.

**Algorithm:**
1. Call `super().get(id_value, id_type)` (note: does not return the result -- possible bug)

### Country.get_name(self) -> Optional[str]
Purpose: Return country name from entity dict via pydash `get(self.get_entity(), 'name')`.

### Country.get_region(self) -> Optional[str]
Purpose: Return `'region'` from entity dict.

### Country.get_sub_region(self) -> Any
Purpose: Return `'subRegion'` from entity dict.

### Country.get_region_code(self) -> Any
Purpose: Return `'regionCode'` from entity dict.

### Country.get_sub_region_code(self) -> Any
Purpose: Return `'subRegionCode'` from entity dict.

### Country.get_alpha3(self) -> Any
Purpose: Return `'xref.alpha3'` from entity dict.

### Country.get_bbid(self) -> Any
Purpose: Return `'xref.bbid'` from entity dict.

### Country.get_alpha2(self) -> Any
Purpose: Return `'xref.alpha2'` from entity dict.

### Country.get_country_code(self) -> Any
Purpose: Return `'xref.countryCode'` from entity dict.

---

### Subdivision.__init__(self, id_: str, entity: Optional[Dict] = None) -> None
Purpose: Initialize a Subdivision entity with `EntityType.SUBDIVISION`.

### Subdivision.data_dimension (property) -> str
Purpose: Return `'subdivisionId'`.

### Subdivision.entity_type() (classmethod) -> EntityType
Purpose: Return `EntityType.SUBDIVISION`.

### Subdivision.get_by_identifier(cls, id_value: str, id_type: Subdivision.Identifier) -> Optional[Entity]
Purpose: Look up a subdivision. Delegates to `Entity.get` (same missing-return issue as Country).

### Subdivision.get_name(self) -> Optional[str]
Purpose: Return name from entity dict.

---

### KPI.__init__(self, id_: str, entity: Optional[Dict] = None) -> None
Purpose: Initialize a KPI entity with `EntityType.KPI`.

### KPI.data_dimension (property) -> str
Purpose: Return `'kpiId'`.

### KPI.entity_type() (classmethod) -> EntityType
Purpose: Return `EntityType.KPI`.

### KPI.get_by_identifier(cls, id_value: str, id_type: KPI.Identifier) -> Optional[Entity]
Purpose: Look up a KPI. Delegates to `Entity.get` (same missing-return issue).

### KPI.get_name(self) -> Optional[str]
Purpose: Return name from entity dict.

### KPI.get_category(self) -> Optional[str]
Purpose: Return `'category'` from entity dict.

### KPI.get_sub_category(self) -> Any
Purpose: Return `'subCategory'` from entity dict.

---

### RiskModelEntity.__init__(self, id_: str, entity: Optional[Dict] = None) -> None
Purpose: Initialize a RiskModelEntity with `EntityType.RISK_MODEL`.

### RiskModelEntity.data_dimension (property) -> str
Purpose: Return `'riskModel'`.

### RiskModelEntity.entity_type() (classmethod) -> EntityType
Purpose: Return `EntityType.RISK_MODEL`.

### RiskModelEntity.get_by_identifier(cls, id_value: str, id_type: RiskModelEntity.Identifier) -> Optional[Entity]
Purpose: Look up a risk model. Delegates to `Entity.get` (same missing-return issue).

### RiskModelEntity.get_name(self) -> Optional[str]
Purpose: Return name from entity dict.

### RiskModelEntity.get_coverage(self) -> Optional[str]
Purpose: Return `'coverage'` from entity dict.

### RiskModelEntity.get_term(self) -> Optional[str]
Purpose: Return `'term'` from entity dict.

### RiskModelEntity.get_vendor(self) -> Optional[str]
Purpose: Return `'vendor'` from entity dict.

---

### PositionedEntity.__init__(self, id_: str, entity_type: EntityType) -> None
Purpose: Initialize the positioned entity mixin with ID and type.

**Algorithm:**
1. Set `self.__id` to `id_`
2. Set `self.__entity_type` to `entity_type`

### PositionedEntity.id (property) -> str
Purpose: Return the entity ID.

### PositionedEntity.positioned_entity_type (property) -> EntityType
Purpose: Return the entity type.

### PositionedEntity.get_entitlements(self) -> Entitlements
Purpose: Fetch entitlements from the API based on entity type.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Call `GsPortfolioApi.get_portfolio(self.id)`
2. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Call `GsAssetApi.get_asset(self.id)`
3. Otherwise: raise `NotImplementedError`
4. Return `Entitlements.from_target(response.entitlements)`

**Raises:** `NotImplementedError` when entity type is neither PORTFOLIO nor ASSET.

### PositionedEntity.get_latest_position_set(self, position_type: PositionType = PositionType.CLOSE) -> PositionSet
Purpose: Get the most recent position set.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Call `GsAssetApi.get_latest_positions(self.id, position_type)`; return `PositionSet.from_target(response)`
2. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Call `GsPortfolioApi.get_latest_positions(portfolio_id=self.id, position_type=position_type.value)`; return `PositionSet.from_target(response)`
3. Otherwise: raise `NotImplementedError`

**Raises:** `NotImplementedError` for unsupported entity types.

### PositionedEntity.get_position_set_for_date(self, date: dt.date, position_type: PositionType = PositionType.CLOSE) -> PositionSet
Purpose: Get the position set for a specific date.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Call `GsAssetApi.get_asset_positions_for_date(self.id, date, position_type)`
   - Branch: `len(response) == 0`
     - True: Log info "No positions available for {date}"; return `PositionSet([], date=date)`
   - Return `PositionSet.from_target(response[0])`
2. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Call `GsPortfolioApi.get_positions_for_date(...)`
   - Return `PositionSet.from_target(response) if response else None`
3. Otherwise: raise `NotImplementedError`

**Raises:** `NotImplementedError` for unsupported entity types.

### PositionedEntity.get_position_sets(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), position_type: PositionType = PositionType.CLOSE) -> List[PositionSet]
Purpose: Get all position sets in a date range.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Call `GsAssetApi.get_asset_positions_for_dates(self.id, start, end, position_type)`
   - Branch: `len(response) == 0`
     - True: Log info; return `[]`
   - Return list comprehension `[PositionSet.from_target(ps) for ps in response]`
2. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Call `GsPortfolioApi.get_positions(...)` ; return list comprehension
3. Otherwise: raise `NotImplementedError`

**Raises:** `NotImplementedError` for unsupported entity types.

### PositionedEntity.update_positions(self, position_sets: List[PositionSet], net_positions: bool = True) -> None
Purpose: Update positions for a portfolio entity. Prices positions missing quantities.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True:
     - Branch: `not position_sets` (empty list) -> return early (no-op)
     - Get portfolio currency via `GsPortfolioApi.get_portfolio(self.id).currency`
     - For each `pos_set` in `position_sets`:
       - Check if any position has `quantity is None`
       - Branch: `positions_are_missing_quantities` is True -> call `pos_set.price(currency)`
       - Append to `new_sets`
     - Call `GsPortfolioApi.update_positions(...)` with converted targets
     - Sleep 3 seconds (waits for backend processing)
2. Otherwise: raise `NotImplementedError`

**Raises:** `NotImplementedError` for non-PORTFOLIO entity types.

### PositionedEntity.get_positions_data(self, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today(), fields: [str] = None, position_type: PositionType = PositionType.CLOSE) -> List[Dict]
Purpose: Get raw position data (for assets only; portfolios must use PerformanceReport).

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Return `GsIndexApi.get_positions_data(self.id, start, end, fields, position_type)`
2. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Raise `MqError` with message directing user to use PerformanceReport class
3. Otherwise: raise `NotImplementedError`

**Raises:** `MqError` for portfolio entities; `NotImplementedError` for other types.

### PositionedEntity.get_last_positions_data(self, fields: [str] = None, position_type: PositionType = PositionType.CLOSE) -> List[Dict]
Purpose: Get the most recent position data (assets only).

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Return `GsIndexApi.get_last_positions_data(self.id, fields, position_type)`
2. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Raise `MqError` directing user to PerformanceReport
3. Otherwise: raise `NotImplementedError`

**Raises:** `MqError` for portfolio entities; `NotImplementedError` for other types.

### PositionedEntity.get_position_dates(self) -> Tuple[dt.date, ...]
Purpose: Get all dates for which positions are available.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Return `GsPortfolioApi.get_position_dates(portfolio_id=self.id)`
2. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Return `GsAssetApi.get_position_dates(asset_id=self.id)`
3. Otherwise: raise `NotImplementedError`

### PositionedEntity.get_reports(self, tags: Dict = None) -> List[Report]
Purpose: Fetch and classify all reports associated with this entity.

**Algorithm:**
1. Branch: `self.positioned_entity_type == EntityType.PORTFOLIO`
   - True: Call `GsPortfolioApi.get_reports(portfolio_id=self.id, tags=tags)`
2. Branch: `self.positioned_entity_type == EntityType.ASSET`
   - True: Call `GsAssetApi.get_reports(asset_id=self.id)`
3. Otherwise: raise `NotImplementedError`
4. For each report in `reports_as_target`:
   - Branch: `report.type == ReportType.Portfolio_Performance_Analytics` -> append `PerformanceReport.from_target(report)`
   - Branch: `report.type in [ReportType.Portfolio_Factor_Risk, ReportType.Asset_Factor_Risk]` -> append `FactorRiskReport.from_target(report)`
   - Branch: `report.type in [ReportType.Portfolio_Thematic_Analytics, ReportType.Asset_Thematic_Analytics]` -> append `ThematicReport.from_target(report)`
   - Otherwise: append `Report.from_target(report)`
5. Return `report_objects`

**Raises:** `NotImplementedError` for unsupported entity types.

### PositionedEntity.get_status_of_reports(self, tags: Dict = None) -> pd.DataFrame
Purpose: Return a DataFrame summarizing report statuses.

**Algorithm:**
1. Call `self.get_reports(tags)` to get list of reports
2. Build dict with keys: Name, ID, Latest Execution Time, Latest End Date, Status, Percentage Complete
3. Return `pd.DataFrame.from_dict(reports_dict)`

### PositionedEntity.get_factor_risk_reports(self, fx_hedged: bool = None, tags: Dict = None) -> List[FactorRiskReport]
Purpose: Get all factor risk reports matching parameters.

**Algorithm:**
1. Branch: `self.positioned_entity_type in [EntityType.PORTFOLIO, EntityType.ASSET]`
   - True:
     - Capitalize entity type value for `position_source_type`
     - Call `GsReportApi.get_reports(limit=100, position_source_type=..., position_source_id=self.id, report_type=f'{position_source_type} Factor Risk', tags=tags, scroll='1m')`
     - Branch: `fx_hedged` is truthy -> filter reports where `report.parameters.fx_hedged == fx_hedged`
     - Branch: `len(reports) == 0` -> raise `MqError` with message
     - Return list of `FactorRiskReport.from_target(report)`
2. Otherwise: raise `NotImplementedError`

**Raises:** `MqError` when no matching reports; `NotImplementedError` for unsupported types.

### PositionedEntity.get_factor_risk_report(self, risk_model_id: str = None, fx_hedged: bool = None, benchmark_id: str = None, tags: Dict = None) -> FactorRiskReport
Purpose: Get exactly one factor risk report matching all filter criteria.

**Algorithm:**
1. Get `position_source_type` from entity type
2. Call `self.get_factor_risk_reports(fx_hedged=fx_hedged, tags=tags)`
3. Branch: `risk_model_id` is truthy -> filter reports where `report.parameters.risk_model == risk_model_id`
4. Filter reports where `report.parameters.benchmark == benchmark_id` (always applied, even when benchmark_id is None)
5. Branch: `len(reports) == 0` -> raise `MqError` (no matching reports)
6. Branch: `len(reports) > 1` -> raise `MqError` (ambiguous, multiple matches)
7. Return `reports[0]`

**Raises:** `MqError` when zero or more than one report matches.

### PositionedEntity.get_thematic_report(self, tags: Dict = None) -> ThematicReport
Purpose: Get the thematic analytics report for this entity.

**Algorithm:**
1. Branch: `self.positioned_entity_type in [EntityType.PORTFOLIO, EntityType.ASSET]`
   - True:
     - Capitalize entity type value
     - Call `GsReportApi.get_reports(limit=100, ...)` for thematic analytics
     - Filter reports where `report.parameters.tags == tags`
     - Branch: `len(reports) == 0` -> raise `MqError`
     - Return `ThematicReport.from_target(reports[0])`
2. Otherwise: raise `NotImplementedError`

**Raises:** `MqError` when no thematic report found; `NotImplementedError` for unsupported types.

### PositionedEntity.poll_report(self, report_id: str, timeout: int = 600, step: int = 30) -> ReportStatus
Purpose: Poll a report's execution status until completion, error, or timeout.

**Algorithm:**
1. Clamp `timeout` to max 1800 seconds
2. Clamp `step` to min 15 seconds
3. Compute `end` as `dt.datetime.now() + dt.timedelta(seconds=timeout)`
4. Loop while `poll is True` and `dt.datetime.now() <= end`:
   - Try:
     - Fetch `status` via `Report.get(report_id).status`
     - Branch: `status not in {error, cancelled, done}`
       - True: Log info; sleep `step` seconds; continue polling
     - Branch: `status == ReportStatus.error`
       - True: raise `MqError` (report failed)
     - Branch: `status == ReportStatus.cancelled`
       - True: Log info; return `status`
     - Otherwise (`done`): Log info; return `status`
   - Except `Exception as err`:
     - Raise `MqError(f'Could not fetch report status with error {err}')`
5. After loop (timeout): raise `MqError` (report taking too long)

**Raises:** `MqError` on error status, fetch failure, or timeout.

### PositionedEntity.get_all_esg_data(self, measures: List[ESGMeasure] = None, cards: List[ESGCard] = None, pricing_date: dt.date = None, benchmark_id: str = None) -> Dict
Purpose: Get all ESG data for the entity.

**Algorithm:**
1. Call `GsEsgApi.get_esg(...)` with entity_id, pricing_date, cards (defaulting to all ESGCard values), measures (defaulting to all ESGMeasure values), and benchmark_id
2. Return the result dict

### PositionedEntity.get_esg_summary(self, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get ESG summary data as a DataFrame.

**Algorithm:**
1. Call `GsEsgApi.get_esg(entity_id=self.id, pricing_date=pricing_date, cards=[ESGCard.SUMMARY])`
2. Extract `'summary'` key
3. Return `pd.DataFrame(summary_data)`

### PositionedEntity.get_esg_quintiles(self, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get ESG quintile breakdown for a given measure.

**Algorithm:**
1. Call `GsEsgApi.get_esg(...)` with `cards=[ESGCard.QUINTILES]`, `measures=[measure]`
2. Extract `.get('quintiles')[0].get('results')`
3. Create DataFrame; filter to columns `['description', 'gross', 'long', 'short']`
4. Return filtered DataFrame

### PositionedEntity.get_esg_by_sector(self, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get ESG breakdown by sector. Delegates to `_get_esg_breakdown(ESGCard.MEASURES_BY_SECTOR, ...)`.

### PositionedEntity.get_esg_by_region(self, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get ESG breakdown by region. Delegates to `_get_esg_breakdown(ESGCard.MEASURES_BY_REGION, ...)`.

### PositionedEntity.get_esg_top_ten(self, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get top 10 constituents by ESG percentile. Delegates to `_get_esg_ranked_card(ESGCard.TOP_TEN_RANKED, ...)`.

### PositionedEntity.get_esg_bottom_ten(self, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Get bottom 10 constituents by ESG percentile. Delegates to `_get_esg_ranked_card(ESGCard.BOTTOM_TEN_RANKED, ...)`.

### PositionedEntity._get_esg_ranked_card(self, card: ESGCard, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Internal helper to fetch ranked ESG card data.

**Algorithm:**
1. Call `GsEsgApi.get_esg(...)` with the given card and measure
2. Extract `.get(card.value)[0].get('results')`
3. Return `pd.DataFrame(data)`

### PositionedEntity._get_esg_breakdown(self, card: ESGCard, measure: ESGMeasure, pricing_date: dt.date = None) -> pd.DataFrame
Purpose: Internal helper to fetch ESG breakdown card data (by sector or region).

**Algorithm:**
1. Call `GsEsgApi.get_esg(...)` with the given card and measure
2. Extract `.get(card.value)[0].get('results')`
3. Return `pd.DataFrame(sector_data)`

### PositionedEntity.get_carbon_analytics(self, benchmark_id: str = None, reporting_year: str = 'Latest', currency: Currency = None, include_estimates: bool = False, use_historical_data: bool = False, normalize_emissions: bool = False, cards: List[CarbonCard] = [c for c in CarbonCard], analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> Dict
Purpose: Get carbon analytics data.

**Algorithm:**
1. Delegate to `GsCarbonApi.get_carbon_analytics(entity_id=self.id, ...)` with all parameters
2. Return result dict

### PositionedEntity.get_carbon_coverage(self, reporting_year: str = 'Latest', include_estimates: bool = False, use_historical_data: bool = False, coverage_category: CarbonCoverageCategory = CarbonCoverageCategory.WEIGHTS, analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> pd.DataFrame
Purpose: Get carbon data coverage as a DataFrame.

**Algorithm:**
1. Call `self.get_carbon_analytics(...)` with `cards=[CarbonCard.COVERAGE]`
2. Chain `.get(CarbonCard.COVERAGE.value).get(coverage_category.value, {}).get(CarbonEntityType.PORTFOLIO.value, {})`
3. Return `pd.DataFrame(coverage)`

### PositionedEntity.get_carbon_sbti_netzero_coverage(self, reporting_year: str = 'Latest', include_estimates: bool = False, use_historical_data: bool = False, target_coverage_category: CarbonTargetCoverageCategory = CarbonTargetCoverageCategory.PORTFOLIO_EMISSIONS, analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> pd.DataFrame
Purpose: Get SBTI and net-zero target coverage data.

**Algorithm:**
1. Call `self.get_carbon_analytics(...)` with `cards=[CarbonCard.SBTI_AND_NET_ZERO_TARGETS]`
2. Chain `.get(CarbonCard.SBTI_AND_NET_ZERO_TARGETS.value).get(target_coverage_category.value, {})`
3. Transform coverage dict: for each target, extract `CarbonEntityType.PORTFOLIO.value` sub-dict
4. Return `pd.DataFrame(coverage)`

### PositionedEntity.get_carbon_emissions(self, currency: Currency = None, include_estimates: bool = False, use_historical_data: bool = False, normalize_emissions: bool = False, scope: CarbonScope = CarbonScope.TOTAL_GHG, analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> pd.DataFrame
Purpose: Get carbon emissions data.

**Algorithm:**
1. Call `self.get_carbon_analytics(...)` with `cards=[CarbonCard.EMISSIONS]`
2. Chain `.get(CarbonCard.EMISSIONS.value).get(scope.value, {}).get(CarbonEntityType.PORTFOLIO.value, [])`
3. Return `pd.DataFrame(emissions)`

### PositionedEntity.get_carbon_emissions_allocation(self, reporting_year: str = 'Latest', currency: Currency = None, include_estimates: bool = False, use_historical_data: bool = False, normalize_emissions: bool = False, scope: CarbonScope = CarbonScope.TOTAL_GHG, classification: CarbonEmissionsAllocationCategory = CarbonEmissionsAllocationCategory.GICS_SECTOR, analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> pd.DataFrame
Purpose: Get carbon emissions allocation breakdown by classification.

**Algorithm:**
1. Call `self.get_carbon_analytics(...)` with `cards=[CarbonCard.ALLOCATIONS]`
2. Chain `.get(CarbonCard.ALLOCATIONS.value).get(scope.value, {}).get(CarbonEntityType.PORTFOLIO.value, {}).get(classification.value)`
3. Return `pd.DataFrame(allocation).rename(columns={'name': classification.value})`

### PositionedEntity.get_carbon_attribution_table(self, benchmark_id: str, reporting_year: str = 'Latest', currency: Currency = None, include_estimates: bool = False, use_historical_data: bool = False, scope: CarbonScope = CarbonScope.TOTAL_GHG, intensity_metric: CarbonEmissionsIntensityType = CarbonEmissionsIntensityType.EI_ENTERPRISE_VALUE, analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> pd.DataFrame
Purpose: Get carbon attribution table comparing portfolio to benchmark.

**Algorithm:**
1. Call `self.get_carbon_analytics(benchmark_id=benchmark_id, ...)` with `cards=[CarbonCard.ATTRIBUTION]`
2. Chain `.get(CarbonCard.ATTRIBUTION.value).get(scope.value, [])`
3. For each entry in attribution:
   - Build `new_entry` dict with sector, weightPortfolio, weightBenchmark, weightComparison
   - Update `new_entry` with the sub-dict at `entry.get(intensity_metric.value, {})`
   - Append to `attribution_table`
4. Return `pd.DataFrame(attribution_table)`

### PositionedEntity.get_thematic_exposure(self, basket_identifier: str, notional: int = 10000000, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> pd.DataFrame
Purpose: Get thematic exposure of an asset entity to a basket over time.

**Algorithm:**
1. Branch: `self.positioned_entity_type != EntityType.ASSET` -> raise `NotImplementedError`
2. Call `GsAssetApi.resolve_assets(identifier=[basket_identifier], fields=['id', 'type'], limit=1)` and index by `basket_identifier`
3. Extract `_id` and `_type` from first result via pydash `get`
4. Branch: `len(response) == 0 or _id is None` -> raise `MqValueError` (basket not found)
5. Branch: `_type not in BasketType.to_list()` -> raise `MqValueError` (not a basket)
6. Build `DataQuery` with `assetId=self.id, basketId=_id` and date range
7. Query data from `IndicesDatasets.COMPOSITE_THEMATIC_BETAS`
8. Build list of dicts with date, assetId, basketId, `thematicExposure = beta * notional`
9. Convert to DataFrame; set index to `'date'`
10. Return DataFrame

**Raises:** `NotImplementedError` for non-ASSET; `MqValueError` for invalid basket.

### PositionedEntity.get_thematic_beta(self, basket_identifier: str, start: dt.date = DateLimit.LOW_LIMIT.value, end: dt.date = dt.date.today()) -> pd.DataFrame
Purpose: Get thematic beta of an asset to a basket over time.

**Algorithm:**
1. Branch: `self.positioned_entity_type != EntityType.ASSET` -> raise `NotImplementedError`
2. Resolve basket asset (same as `get_thematic_exposure`)
3. Branch: `len(response) == 0 or _id is None` -> raise `MqValueError`
4. Branch: `_type not in BasketType.to_list()` -> raise `MqValueError`
5. Query `COMPOSITE_THEMATIC_BETAS` dataset
6. Build list of dicts with date, assetId, basketId, `thematicBeta = beta`
7. Return DataFrame indexed by `'date'`

**Raises:** `NotImplementedError` for non-ASSET; `MqValueError` for invalid basket.

### PositionedEntity.get_all_thematic_exposures(self, start_date: dt.date = None, end_date: dt.date = None, basket_ids: List[str] = None, regions: List[Region] = None) -> pd.DataFrame
Purpose: Get all thematic exposures (deprecated since 0.9.110; use ThematicReport class).

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.ALL_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

### PositionedEntity.get_top_five_thematic_exposures(self, start_date: dt.date = None, end_date: dt.date = None, basket_ids: List[str] = None, regions: List[Region] = None) -> pd.DataFrame
Purpose: Get top 5 thematic exposures (deprecated since 0.9.110).

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.TOP_FIVE_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

### PositionedEntity.get_bottom_five_thematic_exposures(self, start_date: dt.date = None, end_date: dt.date = None, basket_ids: List[str] = None, regions: List[Region] = None) -> pd.DataFrame
Purpose: Get bottom 5 thematic exposures (deprecated since 0.9.110).

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.BOTTOM_FIVE_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

### PositionedEntity.get_thematic_breakdown(self, date: dt.date, basket_id: str) -> pd.DataFrame
Purpose: Get by-asset breakdown of thematic exposure to a flagship basket.

**Algorithm:**
1. Delegate to `get_thematic_breakdown_as_df(entity_id=self.id, date=date, basket_id=basket_id)`
2. Return result

### PositionedEntity.get_factor_scenario_analytics(self, scenarios: List[Scenario], date: dt.date, measures: List[ScenarioCalculationMeasure], risk_model: str = None, return_format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, Union[Dict, pd.DataFrame]]
Purpose: Run factor scenarios and return estimated PnL results.

**Algorithm:**
1. Call `self.get_factor_risk_report(risk_model_id=risk_model)` to get the risk report
2. Build `id_to_scenario_map` dict mapping scenario IDs to Scenario objects
3. Build `calculation_request` dict with date, scenarioIds, reportId, measures (as values), riskModel, type
4. Call `GsFactorScenarioApi.calculate_scenario(calculation_request)`
5. Map result scenario IDs back to Scenario objects
6. Branch: `return_format == ReturnFormat.JSON`
   - True: Return `dict(zip(scenarios, calculation_results))`
7. Initialize `all_data` dict with empty lists for keys: summary, factorPnl, bySectorAggregations, byRegionAggregations, byDirectionAggregations, byAsset
8. For each `(i, calc_result)` in `calculation_results`:
   - For each `result_type` in the six keys:
     - Build `scenario_metadata_map` with scenarioId, scenarioName, scenarioType
     - Branch: `result_type == 'summary'`
       - True: Update the calc_result summary dict in-place with metadata; append to all_data
     - Otherwise: Update each element in the list with metadata; extend all_data list
9. For each `(result_type, result_label)` in the mapping dict:
   - Get `estimated_pnl_results_as_json` from `all_data`
   - Branch: results are truthy (non-empty)
     - True:
       - Create DataFrame from results
       - Apply `_explode_data` to each row with `parent_label=result_label`
       - Branch: result is a Series -> `pd.concat(values, ignore_index=True)`
       - Rename columns using a mapping dict (e.g., "factorCategories" -> "Factor Category", "estimatedPnl" -> "Estimated Pnl", etc.)
       - Reorder columns according to a fixed ordering (only including columns present in the DataFrame)
       - Store in `result[result_type]`
10. Return `result` dict

## State Mutation
- `Entity.__id`: Set in `__init__`, never modified afterward
- `Entity.__entity_type`: Set in `__init__`, never modified afterward
- `Entity.__entity`: Set in `__init__`, never modified afterward
- `PositionedEntity.__id`: Set in `__init__`, never modified afterward
- `PositionedEntity.__entity_type`: Set in `__init__`, never modified afterward
- `PositionedEntity.update_positions()`: Mutates remote state (portfolio positions via API); calls `pos_set.price(currency)` which mutates `pos_set` in-place
- `PositionedEntity.get_factor_scenario_analytics()`: Mutates `calc_result` summary dicts in-place via `.update(scenario_metadata_map)` and list elements via `.update()`; also mutates the `all_data` accumulator
- Thread safety: No thread safety mechanisms; `time.sleep()` calls in `update_positions` and `poll_report` are blocking; concurrent use of the same entity instance is not safe

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `Entity.get_entitlements` | When entity dict has no `'entitlements'` key |
| `NotImplementedError` | `PositionedEntity.get_entitlements` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_latest_position_set` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_position_set_for_date` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_position_sets` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.update_positions` | Entity type is not PORTFOLIO |
| `NotImplementedError` | `PositionedEntity.get_positions_data` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_last_positions_data` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_position_dates` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_reports` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_factor_risk_reports` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_thematic_report` | Entity type is not PORTFOLIO or ASSET |
| `NotImplementedError` | `PositionedEntity.get_thematic_exposure` | Entity type is not ASSET |
| `NotImplementedError` | `PositionedEntity.get_thematic_beta` | Entity type is not ASSET |
| `MqError` | `PositionedEntity.get_positions_data` | Entity type is PORTFOLIO (must use PerformanceReport) |
| `MqError` | `PositionedEntity.get_last_positions_data` | Entity type is PORTFOLIO (must use PerformanceReport) |
| `MqError` | `PositionedEntity.get_factor_risk_reports` | No matching factor risk reports found |
| `MqError` | `PositionedEntity.get_factor_risk_report` | Zero or more than one matching report |
| `MqError` | `PositionedEntity.get_thematic_report` | No thematic analytics report found |
| `MqError` | `PositionedEntity.poll_report` | Report status is error, fetch fails, or timeout |
| `MqValueError` | `PositionedEntity.get_thematic_exposure` | Basket not found or not a valid basket type |
| `MqValueError` | `PositionedEntity.get_thematic_beta` | Basket not found or not a valid basket type |

## Edge Cases
- `Entity.get()` returns `None` implicitly when `result` is falsy (e.g., entity not found by non-MQID lookup)
- `Entity._get_entity_from_type()` returns `None` implicitly for unhandled entity types (ASSET, PORTFOLIO, BACKTEST, HEDGE, REPORT, DATASET, SCENARIO) -- only COUNTRY, KPI, SUBDIVISION, RISK_MODEL are handled
- `Entity.get_data_coordinate()` returns `None` implicitly if frequency is neither DAILY nor REAL_TIME
- `Country.get_by_identifier()`, `Subdivision.get_by_identifier()`, `KPI.get_by_identifier()`, `RiskModelEntity.get_by_identifier()` all call `super().get(...)` but do not return the result, so they always return `None`
- `PositionedEntity.get_position_set_for_date()` returns `None` for PORTFOLIO when `response` is falsy, but returns an empty `PositionSet` for ASSET when no positions found -- inconsistent behavior
- `PositionedEntity.update_positions()` silently returns on empty `position_sets` list; has a hardcoded `time.sleep(3)` after API call
- `PositionedEntity.get_factor_risk_report()` always filters by `benchmark_id` even when it is `None`, meaning it filters for reports where `parameters.benchmark == None`
- `PositionedEntity.poll_report()` clamps timeout to 1800s max and step to 15s min regardless of caller input
- `PositionedEntity.get_carbon_analytics()` uses a mutable default argument `cards: List[CarbonCard] = [c for c in CarbonCard]` -- this is a list comprehension re-evaluated at definition time, not a mutable default bug, but is unconventional
- `PositionedEntity.get_factor_scenario_analytics()` mutates `calc_result` dicts in place (the summary entry), which could cause side effects if the caller retains a reference to the raw results
- `PositionedEntity.get_factor_scenario_analytics()` reassigns the local variable `scenarios` (line 1038) shadowing the parameter `scenarios`, which could confuse readers but is functionally correct since the original is no longer needed

## Bugs Found
- Line 232, 280, 303, 333: `get_by_identifier()` in Country, Subdivision, KPI, and RiskModelEntity calls `super().get(...)` but does not `return` the result, so these methods always return `None`. (OPEN)
- Line 391: `get_position_set_for_date()` for PORTFOLIO returns `None` when response is falsy, while for ASSET it returns an empty `PositionSet` -- inconsistent return type. (OPEN)

## Coverage Notes
- Branch count: ~78 (counting each if/elif/else path in entity-type dispatches, len checks, truthy checks, loop conditions, and the multi-branch report classification)
- Key branch areas:
  - `Entity.get()`: 5 branches (entity_type None vs provided, asset shortcut, MQID vs other, result truthy)
  - `Entity._get_entity_from_type()`: 5 branches (COUNTRY, KPI, SUBDIVISION, RISK_MODEL, implicit None)
  - `Entity.get_data_coordinate()`: 3 branches (DAILY, REAL_TIME, implicit None)
  - `PositionedEntity` entity-type dispatch methods: each has 2-3 branches (ASSET, PORTFOLIO, NotImplementedError)
  - `PositionedEntity.get_reports()`: 4 branches for report type classification
  - `PositionedEntity.poll_report()`: 5 branches (still running, error, cancelled, done, exception) plus timeout
  - `PositionedEntity.get_factor_risk_report()`: 4 branches (risk_model_id filter, 0 reports, >1 reports, exactly 1)
  - `PositionedEntity.get_factor_scenario_analytics()`: 3 branches (JSON return, summary vs other result_type, series check) plus iteration logic
  - `PositionedEntity.get_thematic_exposure/beta()`: 3 validation branches each
- Deprecated methods (get_all_thematic_exposures, get_top_five_thematic_exposures, get_bottom_five_thematic_exposures): simple pass-through, low branch complexity
- Pragmas: none marked
