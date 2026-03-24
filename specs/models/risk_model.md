# risk_model.py

## Summary
Core risk model module providing a class hierarchy for interacting with Goldman Sachs Marquee risk models. Defines base `RiskModel`, intermediate `MarqueeRiskModel`, and specialized `FactorRiskModel`, `MacroRiskModel`, and `ThematicRiskModel` classes that wrap the GS Risk Model API for querying factor data, asset data, covariance matrices, and performing risk attribution. Also provides supporting enums for return formats, units, and factor types.

## Dependencies
- Internal: `gs_quant.api.gs.risk_models` (GsFactorRiskModelApi, GsRiskModelApi, IntradayFactorDataSource), `gs_quant.base` (EnumBase), `gs_quant.data` (DataMeasure), `gs_quant.errors` (MqValueError, MqRequestError), `gs_quant.markets.factor` (Factor), `gs_quant.markets.securities` (SecurityMaster, AssetIdentifier), `gs_quant.models.risk_model_utils` (build_pfp_data_dataframe, get_closest_date_index, upload_model_data, get_optional_data_as_dataframe, get_universe_size, get_covariance_matrix_dataframe, build_factor_data_map, build_asset_data_map, build_factor_id_to_name_map, only_factor_data_is_present, batch_and_upload_partial_data, build_factor_volatility_dataframe, batch_and_upload_coverage_data), `gs_quant.target.risk_models` (RiskModel as RiskModelBuilder, RiskModelEventType, RiskModelData, RiskModelCalendar, RiskModelDataAssetsRequest as DataAssetsRequest, RiskModelDataMeasure as Measure, RiskModelCoverage as CoverageType, RiskModelUniverseIdentifier as UniverseIdentifier, Entitlements, RiskModelTerm as Term, RiskModelUniverseIdentifierRequest, Factor as RiskModelFactor, RiskModelType, RiskModelDataMeasure, RiskModelDataAssetsRequest), `gs_quant.common` (Currency)
- External: `datetime` (dt), `math`, `enum` (Enum, auto), `typing` (List, Dict, Tuple, Union), `pandas` (pd), `numpy` (np), `logging`, `deprecation`

## Type Definitions

### RiskModel (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | required | Risk model identifier |
| __name | `str` | required | Risk model name |

### MarqueeRiskModel (class)
Inherits: RiskModel

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __type | `RiskModelType` | required | Type of risk model (converted from str if needed) |
| __vendor | `str` | required | Risk model vendor name |
| __version | `float` | required | Model version number |
| __coverage | `CoverageType` | required | Coverage of model asset universe |
| __universe_identifier | `UniverseIdentifier` | required | Identifier type used for asset universe |
| __term | `Term` | required | Horizon term |
| __universe_size | `int` | `None` | Expected universe size |
| __entitlements | `Entitlements` | `None` | Access entitlements (converted from Dict if needed) |
| __description | `str` | `None` | Model description |
| __expected_update_time | `dt.time` | `None` | Expected daily update time |

### FactorRiskModel (class)
Inherits: MarqueeRiskModel

Same fields as MarqueeRiskModel. Constructor hard-codes `type_=RiskModelType.Factor`.

### MacroRiskModel (class)
Inherits: MarqueeRiskModel

Same fields as MarqueeRiskModel. Constructor hard-codes `type_=RiskModelType.Macro`.

### ThematicRiskModel (class)
Inherits: MarqueeRiskModel

Same fields as MarqueeRiskModel. Constructor hard-codes `type_=RiskModelType.Thematic`.

## Enums and Constants

### ReturnFormat(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| JSON | `auto()` | Return data as JSON dict |
| DATA_FRAME | `auto()` | Return data as pandas DataFrame |

### Unit(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| PERCENT | `auto()` | Return measure in percent |
| STANDARD_DEVIATION | `auto()` | Return measure in standard deviation units |

### FactorType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Factor | `"Factor"` | Individual risk factor |
| Category | `"Category"` | Factor category / aggregation |

## Functions/Methods

### RiskModel.__init__(self, id_: str, name: str)
Purpose: Initialize base risk model with id and name.

**Algorithm:**
1. Store `id_` as private `__id`
2. Store `name` as private `__name`

### RiskModel.id (property) -> str
Purpose: Get risk model id.

### RiskModel.name (property) -> str
Purpose: Get risk model name.

### RiskModel.name (setter) -> None
Purpose: Set risk model name.

### RiskModel.__str__(self) -> str
Purpose: Return string representation as the model id.

### RiskModel.__repr__(self) -> str
Purpose: Return repr string in format `ClassName('id','name')`.

### MarqueeRiskModel.__init__(self, id_: str, name: str, type_: Union[str, RiskModelType], vendor: str, version: float, coverage: CoverageType, universe_identifier: UniverseIdentifier, term: Term, universe_size: int = None, entitlements: Union[Dict, Entitlements] = None, description: str = None, expected_update_time: dt.time = None)
Purpose: Initialize a Marquee risk model with full metadata.

**Algorithm:**
1. Call `super().__init__(id_, name)`
2. Branch: if `type_` is already `RiskModelType` -> use directly; else -> convert via `RiskModelType(type_)`
3. Store vendor, version, coverage, universe_identifier, term directly
4. Branch: if `entitlements` is `Entitlements` -> use directly; if `Dict` -> convert via `Entitlements.from_dict()`; else -> `None`
5. Store description and expected_update_time

### MarqueeRiskModel.type (property) -> RiskModelType
Purpose: Get risk model type.

### MarqueeRiskModel.type (setter)
Purpose: Set risk model type.

### MarqueeRiskModel.vendor (property) -> str
Purpose: Get risk model vendor.

### MarqueeRiskModel.vendor (setter)
Purpose: Set risk model vendor.

### MarqueeRiskModel.version (property) -> float
Purpose: Get risk model version.

### MarqueeRiskModel.version (setter)
Purpose: Set risk model version.

### MarqueeRiskModel.coverage (property) -> CoverageType
Purpose: Get risk model coverage.

### MarqueeRiskModel.coverage (setter)
Purpose: Set risk model coverage.

### MarqueeRiskModel.universe_identifier (property) -> UniverseIdentifier
Purpose: Get risk model universe identifier.

### MarqueeRiskModel.term (property) -> Term
Purpose: Get risk model term.

### MarqueeRiskModel.term (setter)
Purpose: Set risk model term.

### MarqueeRiskModel.description (property) -> str
Purpose: Get risk model description.

### MarqueeRiskModel.description (setter)
Purpose: Set risk model description.

### MarqueeRiskModel.universe_size (property) -> int
Purpose: Get risk model universe size.

### MarqueeRiskModel.universe_size (setter)
Purpose: Set risk model universe size.

### MarqueeRiskModel.entitlements (property) -> Entitlements
Purpose: Get risk model entitlements.

### MarqueeRiskModel.entitlements (setter)
Purpose: Set risk model entitlements.

### MarqueeRiskModel.expected_update_time (property) -> dt.time
Purpose: Get risk model expected update time.

### MarqueeRiskModel.expected_update_time (setter)
Purpose: Set expected update time.

### MarqueeRiskModel.delete(self)
Purpose: Delete existing risk model object from Marquee.

**Algorithm:**
1. Call `GsRiskModelApi.delete_risk_model(self.id)`
2. Return result

### MarqueeRiskModel.get_dates(self, start_date: dt.date = None, end_date: dt.date = None, event_type: RiskModelEventType = None) -> List[dt.date]
Purpose: Get dates for which risk model data is present between start and end date.

**Algorithm:**
1. Call `GsRiskModelApi.get_risk_model_dates(self.id, start_date, end_date, event_type=event_type)`
2. Parse each date string to `dt.date` via `strptime`
3. Return list of date objects

### MarqueeRiskModel.get_calendar(self, start_date: dt.date = None, end_date: dt.date = None) -> RiskModelCalendar
Purpose: Get risk model calendar between start and end date.

**Algorithm:**
1. Fetch full calendar via `GsRiskModelApi.get_risk_model_calendar(self.id)`
2. Branch: if no start_date and no end_date -> return full calendar
3. If start_date provided -> find closest date index via `get_closest_date_index(start_date, ..., 'after')`; else -> 0
4. If end_date provided -> find closest date index via `get_closest_date_index(end_date, ..., 'before')`; else -> len(calendar)
5. Return `RiskModelCalendar` sliced from start_idx to end_idx + 1

### MarqueeRiskModel.upload_calendar(self, calendar: RiskModelCalendar)
Purpose: Upload risk model calendar.

**Algorithm:**
1. Call `GsRiskModelApi.upload_risk_model_calendar(self.id, calendar)`
2. Return result

### MarqueeRiskModel.get_missing_dates(self, start_date: dt.date = None, end_date: dt.date = None) -> List[dt.date]
Purpose: Get dates where data is expected but missing.

**Algorithm:**
1. Fetch posted dates via `self.get_dates()`
2. Branch: if no start_date -> use first posted date
3. Branch: if no end_date -> use yesterday (`dt.date.today() - timedelta(1)`)
4. Fetch calendar for date range and parse to date objects
5. Return dates in calendar that are not in posted dates

### MarqueeRiskModel.get_most_recent_date_from_calendar(self) -> dt.date
Purpose: Get T-1 date from model calendar.

**Algorithm:**
1. Set yesterday = today - 1 day
2. Fetch calendar up to yesterday
3. Return last date in calendar as `dt.date`

### MarqueeRiskModel.save(self)
Purpose: Upload current Risk Model object to Marquee (create or update).

**Algorithm:**
1. Build `RiskModelBuilder` from current properties
2. Branch: if `expected_update_time` exists -> format as `'%H:%M:%S'` string; else -> None
3. Try `GsRiskModelApi.create_risk_model(model)`
4. Branch: if `MqRequestError` -> call `GsRiskModelApi.update_risk_model(model)` instead

### MarqueeRiskModel.get(cls, model_id: str) -> MarqueeRiskModel
Purpose: Class method to fetch a risk model from Marquee by ID.

**Algorithm:**
1. Call `GsRiskModelApi.get_risk_model(model_id)`
2. Return `cls.from_target(model)`

### MarqueeRiskModel.get_asset_universe(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get asset universe data for the risk model.

**Algorithm:**
1. Branch: if assets.universe is empty and no end_date -> set end_date = start_date
2. Call `self.get_data(...)` with `Measure.Asset_Universe`
3. Extract dates and universe lists from results
4. Build dict of dates to universe lists
5. Branch: if DATA_FRAME format -> convert to DataFrame; else -> return dict

### MarqueeRiskModel.get_factor(self, name: str, start_date: dt.date = None, end_date: dt.date = None) -> Factor
Purpose: Get a single risk model factor by name.

**Algorithm:**
1. Call `self.get_factor_data(...)` with JSON format
2. Filter results where name matches
3. Branch: if no matches -> raise `MqValueError`
4. Pop last match and construct `Factor` object with all metadata fields
5. Return Factor

### MarqueeRiskModel.get_many_factors(self, start_date: dt.date = None, end_date: dt.date = None, factor_names: List[str] = None, factor_ids: List[str] = None, factor_type: FactorType = None) -> List[Factor]
Purpose: Get multiple risk model factors by name and/or id.

**Algorithm:**
1. Fetch all factors via `self.get_factor_data(...)` as JSON
2. Branch: if neither factor_names nor factor_ids provided -> use all factors
3. Else -> iterate factors, match by name and/or id, remove matched names/ids from lists
4. Branch: if any factor_names or factor_ids remain unmatched -> raise `MqValueError`
5. Construct and return list of `Factor` objects

### MarqueeRiskModel.save_factor_metadata(self, factor_metadata: RiskModelFactor)
Purpose: Add or update metadata for a factor in the risk model.

**Algorithm:**
1. Try `GsFactorRiskModelApi.update_risk_model_factor(self.id, factor_metadata)`
2. Branch: if `MqRequestError` -> call `create_risk_model_factor` instead

### MarqueeRiskModel.delete_factor_metadata(self, factor_id: str)
Purpose: Delete a factor's metadata from the risk model.

**Algorithm:**
1. Call `GsFactorRiskModelApi.delete_risk_model_factor(self.id, factor_id)`

### MarqueeRiskModel.get_intraday_factor_data(self, start_time: dt.datetime = ..., end_time: dt.datetime = ..., factors: List[str] = None, factor_ids: List[str] = None, data_source: Union[IntradayFactorDataSource, str] = None, category_filter: List[str] = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get intraday factor data for the risk model.

**Algorithm:**
1. Set factor_categories from category_filter (default empty list)
2. Call `GsFactorRiskModelApi.get_risk_model_factor_data_intraday(...)` with all params
3. Branch: if DATA_FRAME format -> wrap in `pd.DataFrame`
4. Return data

### MarqueeRiskModel.get_factor_data(self, start_date: dt.date = None, end_date: dt.date = None, identifiers: List[str] = None, include_performance_curve: bool = False, category_filter: List[str] = None, name_filter: List[str] = None, factor_type: FactorType = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor data for the risk model with optional filtering.

**Algorithm:**
1. Set factor_categories from category_filter (default empty list)
2. Branch: if factor_type is Category:
   - Branch: if factor_categories non-empty -> raise `ValueError` (Category filter not applicable)
   - Set factor_categories to `['Aggregations']`
3. Call `GsFactorRiskModelApi.get_risk_model_factor_data(...)` with all params
4. Branch: if factor_type is Factor:
   - Branch: if 'Aggregations' in factor_categories -> raise `ValueError`
   - Filter to only factors with type == 'Factor'
5. Branch: if DATA_FRAME format -> wrap in `pd.DataFrame`
6. Return data

### MarqueeRiskModel.get_factor_returns_by_name(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get factor return data keyed by factor name.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Return, ..., factors_by_name=True, ...)`

### MarqueeRiskModel.get_factor_returns_by_id(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get factor return data keyed by factor id.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Return, ..., factors_by_name=False, ...)`

### MarqueeRiskModel._get_factor_data_measure(self, requested_measure: RiskModelDataMeasure, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors_by_name=True, factors: List[str] = [], format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Internal helper to retrieve any factor-level data measure.

**Algorithm:**
1. Set limit_factors = False
2. Build measures list: [requested_measure, Factor_Name, Factor_Id]
3. Branch: if assets provided -> add Universe_Factor_Exposure and Asset_Universe to measures; set limit_factors = True
4. Call `self.get_data(...)` and extract results
5. Set identifier to 'factorName' or 'factorId' based on factors_by_name
6. Call `build_factor_data_map(results, identifier, self.id, requested_measure, factors=factors)`
7. Branch: if DATA_FRAME format -> return DataFrame; else -> return `.to_dict()`

### MarqueeRiskModel._get_asset_data_measure(self, requested_measure: RiskModelDataMeasure, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Internal helper to retrieve any asset-level data measure.

**Algorithm:**
1. Call `self.get_data(...)` with [requested_measure, Asset_Universe]
2. Call `build_asset_data_map(results, assets.universe, requested_measure, {})`
3. Branch: if DATA_FRAME format -> wrap in `pd.DataFrame`
4. Return data

### MarqueeRiskModel.get_universe_exposure(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., factors: List[Union[str, Factor]] = None, get_factors_by_name: bool = False, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get universe factor exposure data.

**Algorithm:**
1. Call `self.get_data(...)` with Factor_Name, Factor_Id, Universe_Factor_Exposure, Asset_Universe measures
2. Branch: if get_factors_by_name -> build factor_id_to_name_map from results; else -> empty dict
3. Call `build_asset_data_map(...)` with Universe_Factor_Exposure measure and factor map
4. Branch: if DATA_FRAME format -> convert nested dict to DataFrame using `from_dict` with multi-level keys
5. Return data

### MarqueeRiskModel.get_specific_risk(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get specific risk data.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Specific_Risk, ...)`

### MarqueeRiskModel.get_factor_standard_deviation(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor standard deviation data keyed by name or id.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Standard_Deviation, ...)`

### MarqueeRiskModel.get_factor_mean(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor mean data keyed by name or id.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Mean, ...)`

### MarqueeRiskModel.get_factor_cross_sectional_mean(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor cross-sectional mean data.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Cross_Sectional_Mean, ...)`

### MarqueeRiskModel.get_factor_cross_sectional_standard_deviation(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor cross-sectional standard deviation data.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Cross_Sectional_Standard_Deviation, ...)`

### MarqueeRiskModel.get_data(self, measures: List[Measure], start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., factors: List[Union[str, Factor]] = None, limit_factors: bool = True) -> Dict
Purpose: Get data for multiple measures from the risk model API.

**Algorithm:**
1. Branch: if factors provided -> convert Factor objects to their `.name` strings
2. Try calling `GsFactorRiskModelApi.get_risk_model_data(...)` with all params
3. Branch: on `MqRequestError`:
   - If status > 499 -> log warning about potential timeout, raise new `MqRequestError` with timeout message
   - Else -> re-raise original error

### MarqueeRiskModel.upload_data(self, data: Union[RiskModelData, Dict], max_asset_batch_size: int = 10000, aws_upload: bool = True)
Purpose: Upload risk model data with automatic batching if needed.

**Algorithm:**
1. Branch: if data is `RiskModelData` -> convert via `.as_dict()`; else -> use as-is
2. Check if full data present (factorData and assetData in keys)
3. Check if only factor data present via `only_factor_data_is_present()`
4. Get target universe size (0 if only factor data)
5. Determine if partial request needed: universe > max_batch_size or not full data
6. Branch: if target_universe_size > 0 -> log info
7. Branch: if make_partial_request -> call `batch_and_upload_partial_data(..., aws_upload=aws_upload)`
8. Else -> log info and call `upload_model_data(..., aws_upload=aws_upload)`

### MarqueeRiskModel.upload_partial_data(self, data: Union[RiskModelData, dict], final_upload: bool = None)
Purpose: (Deprecated) Upload partial risk model data.

**Algorithm:**
1. Call `upload_model_data(self.id, data, partial_upload=True, final_upload=final_upload)`

### MarqueeRiskModel.upload_asset_coverage_data(self, date: dt.date = None, batch_size: int = 100)
Purpose: Upload to coverage dataset for the risk model.

**Algorithm:**
1. Branch: if no date -> use last date from `self.get_dates()`
2. Fetch gsid_list via `self.get_asset_universe(date, ..., format=ReturnFormat.JSON).get(date)`
3. Branch: if no gsid_list -> raise `MqRequestError(404, ...)`
4. Call `batch_and_upload_coverage_data(date, gsid_list, self.id, batch_size)`

### MarqueeRiskModel.from_target(cls, model) -> MarqueeRiskModel
Purpose: Class method to construct MarqueeRiskModel from a target model object.

**Algorithm:**
1. Get universe_identifier; convert to `UniverseIdentifier` if needed (handle None)
2. Get coverage; convert to `CoverageType` if needed
3. Get term; convert to `Term` if needed
4. Get type_; convert to `RiskModelType` if needed (handle None)
5. Get expected_update_time; parse from `"%H:%M:%S"` string if present
6. Return new `MarqueeRiskModel` with all fields

### MarqueeRiskModel.from_many_targets(cls, models: Tuple[RiskModelBuilder, ...]) -> list
Purpose: Convert a tuple of target models to a list of MarqueeRiskModel objects.

**Algorithm:**
1. Return list comprehension calling `cls.from_target(model)` for each model

### MarqueeRiskModel.__str__(self) -> str
Purpose: Return model id.

### MarqueeRiskModel.__repr__(self) -> str
Purpose: Return detailed repr string with all properties, conditionally including optional fields.

### FactorRiskModel.__init__(self, id_: str, name: str, coverage: CoverageType, term: Term, universe_identifier: UniverseIdentifier, vendor: str, version: float, universe_size: int = None, entitlements: Union[Dict, Entitlements] = None, description: str = None, expected_update_time: dt.time = None)
Purpose: Create a factor risk model with type hard-coded to `RiskModelType.Factor`.

**Algorithm:**
1. Call `super().__init__(id_, name, RiskModelType.Factor, vendor, version, coverage, universe_identifier, term, ...)`

### FactorRiskModel.from_target(cls, model: RiskModelBuilder) -> FactorRiskModel
Purpose: Class method to construct FactorRiskModel from target model.

**Algorithm:**
1. Same as `MarqueeRiskModel.from_target` but returns `FactorRiskModel` instance
2. Converts coverage, term, universe_identifier, expected_update_time as needed

### FactorRiskModel.get_many(cls, ids: List[str] = None, terms: List[str] = None, vendors: List[str] = None, names: List[str] = None, coverages: List[str] = None, limit: int = None) -> list
Purpose: Get many factor risk models from Marquee.

**Algorithm:**
1. Call `GsRiskModelApi.get_risk_models(...)` with `types=[RiskModelType.Factor.value]`
2. Return `cls.from_many_targets(models)`

### FactorRiskModel.get_total_risk(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get total risk data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Total_Risk, ...)`

### FactorRiskModel.get_historical_beta(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get historical beta data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Historical_Beta, ...)`

### FactorRiskModel.get_predicted_beta(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get predicted beta data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Predicted_Beta, ...)`

### FactorRiskModel.get_global_predicted_beta(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get global predicted beta data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Global_Predicted_Beta, ...)`

### FactorRiskModel.get_daily_return(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get daily asset total return data.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Daily_Return, ...)`

### FactorRiskModel.get_specific_return(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get specific return data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Specific_Return, ...)`

### FactorRiskModel.get_bid_ask_spread(self, start_date: dt.date, end_date: dt.date = None, days: int = 0, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get bid-ask spread data, optionally averaged over a number of days.

**Algorithm:**
1. Try:
   - Branch: if days == 0 -> use `Measure.Bid_Ask_Spread`; else -> use `Measure[f'Bid_Ask_Spread_{days}d']`
   - Delegate to `super()._get_asset_data_measure(requested_measure, ...)`
2. Branch: on `KeyError` -> raise `ValueError` with message about unavailable days

### FactorRiskModel.get_trading_volume(self, start_date: dt.date, end_date: dt.date = None, days: int = 0, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get trading volume data, optionally averaged over a number of days.

**Algorithm:**
1. Try:
   - Branch: if days == 0 -> use `Measure.Trading_Volume`; else -> use `Measure[f'Trading_Volume_{days}d']`
   - Delegate to `super()._get_asset_data_measure(requested_measure, ...)`
2. Branch: on `KeyError` -> raise `ValueError`

### FactorRiskModel.get_traded_value(self, start_date: dt.date, end_date: dt.date = None, days: int = 30, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get traded value data, optionally averaged over a number of days (default 30).

**Algorithm:**
1. Try:
   - Branch: if days == 0 (falsy) -> use `Measure.Traded_Value_30d`; else -> use `Measure[f'Traded_Value_{days}d']`
   - Delegate to `super()._get_asset_data_measure(requested_measure, ...)`
2. Branch: on `KeyError` -> raise `ValueError`

### FactorRiskModel.get_composite_volume(self, start_date: dt.date, end_date: dt.date = None, days: int = 0, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get composite volume data.

**Algorithm:**
1. Try:
   - Branch: if days == 0 -> use `Measure.Composite_Volume`; else -> use `Measure[f'Composite_Volume_{days}d']`
   - Delegate to `super()._get_asset_data_measure(requested_measure, ...)`
2. Branch: on `KeyError` -> raise `ValueError`

### FactorRiskModel.get_composite_value(self, start_date: dt.date, end_date: dt.date = None, days: int = 30, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get composite value data (default 30 days).

**Algorithm:**
1. Try:
   - Branch: if days == 0 (falsy) -> use `Measure.Composite_Value_30d`; else -> use `Measure[f'Composite_Value_{days}d']`
   - Delegate to `super()._get_asset_data_measure(requested_measure, ...)`
2. Branch: on `KeyError` -> raise `ValueError`

### FactorRiskModel.get_issuer_market_cap(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get issuer market capitalization data.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Issuer_Market_Cap, ...)`

### FactorRiskModel.get_asset_price(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get asset price data.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Price, ...)`

### FactorRiskModel.get_asset_capitalization(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get asset capitalization data.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Capitalization, ...)`

### FactorRiskModel.get_currency(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get currency data for assets.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Currency, ...)`

### FactorRiskModel.get_unadjusted_specific_risk(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get unadjusted specific risk data.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Unadjusted_Specific_Risk, ...)`

### FactorRiskModel.get_dividend_yield(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get dividend yield data for assets.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Dividend_Yield, ...)`

### FactorRiskModel.get_universe_factor_exposure(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., factors: List[Union[str, Factor]] = None, get_factors_by_name: bool = False, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get universe factor exposure data.

**Algorithm:**
1. Delegate to `super().get_universe_exposure(start_date, end_date, assets, factors=factors, get_factors_by_name=get_factors_by_name, format=format)`

### FactorRiskModel._build_covariance_matrix_measure(self, covariance_matrix_type: Measure, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Internal helper to build any type of covariance matrix data.

**Algorithm:**
1. Map covariance matrix type to field name string ('covarianceMatrix', 'unadjustedCovarianceMatrix', 'preVRACovarianceMatrix')
2. Set limit_factors = True if assets else False
3. Build measures list: [covariance_matrix_type, Factor_Name, Factor_Id]; add exposure measures if assets
4. Call `self.get_data(...)` and extract results
5. Branch: if JSON format -> return raw results; else -> call `get_covariance_matrix_dataframe(results, covariance_matrix_key=...)`
6. Return data

### FactorRiskModel.get_covariance_matrix(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get covariance matrix of daily factor returns.

**Algorithm:**
1. Delegate to `self._build_covariance_matrix_measure(Measure.Covariance_Matrix, ...)`

### FactorRiskModel.get_unadjusted_covariance_matrix(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get unadjusted covariance matrix.

**Algorithm:**
1. Delegate to `self._build_covariance_matrix_measure(Measure.Unadjusted_Covariance_Matrix, ...)`

### FactorRiskModel.get_pre_vra_covariance_matrix(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get pre-VRA covariance matrix.

**Algorithm:**
1. Delegate to `self._build_covariance_matrix_measure(Measure.Pre_VRA_Covariance_Matrix, ...)`

### FactorRiskModel._build_currency_rates_data(self, rows: List[Dict], currencies: List[Currency], rates_key: str, format: ReturnFormat) -> Union[Dict, pd.DataFrame]
Purpose: Internal helper to build currency rates data from results.

**Algorithm:**
1. Call `get_optional_data_as_dataframe(rows, 'currencyRatesData')`
2. Branch: if currencies provided -> filter DataFrame rows where 'currency' is in currency values
3. Branch: if DATA_FRAME format -> return DataFrame; else -> return `.to_dict()`

### FactorRiskModel.get_risk_free_rate(self, start_date: dt.date, end_date: dt.date = None, currencies: List[Currency] = [], format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get risk-free rates data.

**Algorithm:**
1. Call `self.get_data(measures=[RiskModelDataMeasure.Risk_Free_Rate], ...)` and extract results
2. Delegate to `self._build_currency_rates_data(results, rates_key="riskFreeRate", currencies=currencies, format=format)`

### FactorRiskModel.get_currency_exchange_rate(self, start_date: dt.date, end_date: dt.date = None, currencies: List[Currency] = [], format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get currency exchange rates data.

**Algorithm:**
1. Call `self.get_data(measures=[RiskModelDataMeasure.Currency_Exchange_Rate], ...)` and extract results
2. Delegate to `self._build_currency_rates_data(results, rates_key="exchangeRate", currencies=currencies, format=format)`

### FactorRiskModel.get_factor_volatility(self, start_date: dt.date, end_date: dt.date = None, factors: List[str] = None, get_factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get factor volatility data.

**Algorithm:**
1. Branch: if factors is None -> set to empty list
2. Build measures: [Factor_Volatility, Factor_Name, Factor_Id]
3. Call `self.get_data(...)` and extract results
4. Branch: if JSON format and not get_factors_by_name and not factors -> return raw results
5. Else -> call `build_factor_volatility_dataframe(results, get_factors_by_name, factors)`
6. Branch: if DATA_FRAME format -> return DataFrame; else -> return `.to_dict()`

### FactorRiskModel.get_estimation_universe_weights(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get estimation universe weights.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.Estimation_Universe_Weight, ...)`

### FactorRiskModel.get_issuer_specific_covariance(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get issuer specific covariance data.

**Algorithm:**
1. Call `self.get_data(...)` with `Measure.Issuer_Specific_Covariance` and extract results
2. Branch: if JSON format -> return raw; else -> call `get_optional_data_as_dataframe(isc, 'issuerSpecificCovariance')`
3. Return data

### FactorRiskModel.get_factor_portfolios(self, start_date: dt.date, end_date: dt.date = None, factors: List[str] = None, assets: DataAssetsRequest = ..., get_factors_by_name: bool = False, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get factor portfolios data.

**Algorithm:**
1. Call `self.get_data(...)` with Factor_Id, Factor_Name, Factor_Portfolios measures and extract results
2. Call `build_pfp_data_dataframe(results, return_df=format is DATA_FRAME, get_factors_by_name=get_factors_by_name)`
3. Return data

### FactorRiskModel.get_asset_contribution_to_risk(self, asset_identifier: str, date: dt.date, asset_identifier_type=RiskModelUniverseIdentifierRequest.bbid, get_factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME)
Purpose: Get factor proportion of risk and MCTR for each factor for a single asset on a single date.

**Algorithm:**
1. Get spot price: look up security via `SecurityMaster.get_asset(...)`, get SPOT_PRICE data coordinate, get series
2. Branch: if series empty -> raise `MqValueError` (no price available)
3. Get risk model data with measures: Asset_Universe, Total_Risk, Universe_Factor_Exposure, Covariance_Matrix, Factor_Name, Factor_Id, Factor_Category
4. Branch: if totalRisk list is empty -> raise `MqValueError` (asset not covered)
5. Calculate VaR: `total_risk / 100 * spot_price / sqrt(252)`
6. Get covariance_matrix as numpy array and factor_z_scores
7. Build exposure vector: `factor_z_scores[key] * spot_price` for each factor
8. Compute `cov_x_exp = dot(covariance_matrix, exp)`
9. Compute `mctr = cov_x_exp / var`
10. Compute `rmctr = exp * mctr`
11. Compute `annualized_rmctr = rmctr * sqrt(252) / spot_price * 100`
12. Sum annualized_rmctr for factor total
13. Build result list: for each factor, record Factor name/id, Category, Proportion of Risk (%), MCTR (%)
14. Append "Specific" row: proportion = `(total_risk - factor_sum) / total_risk * 100`
15. Branch: if DATA_FRAME format -> return as DataFrame; else -> return list

### FactorRiskModel.get_asset_factor_attribution(self, asset_identifier: str, start_date: dt.date, end_date: dt.date, asset_identifier_type=RiskModelUniverseIdentifierRequest.bbid, get_factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME)
Purpose: Get factor attribution (in percent) for an asset over a date range.

**Algorithm:**
1. Get risk model data with measures: Asset_Universe, Universe_Factor_Exposure, Factor_Volatility, Factor_Return, Factor_Name, Factor_Id, Factor_Category
2. Branch: if no results -> raise `MqValueError` (asset not covered)
3. Branch: if fewer than 2 results -> raise `MqValueError` (need at least 2 days for attribution)
4. For each day i from 1..len(results):
   - Build attribution_on_date dict with Date
   - For each factor, record factorReturn keyed by name or id; build factor_id_to_name_map
   - Get previous day's factor exposures
   - Multiply each factor's return by previous day's exposure
   - Append to factor_attribution list
5. Branch: if DATA_FRAME format -> return as DataFrame; else -> return list

### FactorRiskModel.__repr__(self) -> str
Purpose: Return detailed repr with conditional optional fields.

### MacroRiskModel.__init__(self, id_: str, name: str, coverage: CoverageType, term: Term, universe_identifier: UniverseIdentifier, vendor: str, version: float, universe_size: int = None, entitlements: Union[Dict, Entitlements] = None, description: str = None, expected_update_time: dt.time = None)
Purpose: Create a macro risk model with type hard-coded to `RiskModelType.Macro`.

**Algorithm:**
1. Call `super().__init__(id_, name, RiskModelType.Macro, vendor, version, coverage, universe_identifier, term, ...)`

### MacroRiskModel.from_target(cls, model: RiskModelBuilder) -> MacroRiskModel
Purpose: Class method to construct MacroRiskModel from target model.

**Algorithm:**
1. Same pattern as FactorRiskModel.from_target but returns `MacroRiskModel`

### MacroRiskModel.get_many(cls, ids=None, terms=None, vendors=None, names=None, coverages=None, limit=None)
Purpose: Get many macro risk models from Marquee.

**Algorithm:**
1. Call `GsRiskModelApi.get_risk_models(...)` with `types=[RiskModelType.Macro.value]`
2. Return `cls.from_many_targets(models)`

### MacroRiskModel.get_universe_sensitivity(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., factor_type: FactorType = FactorType.Factor, get_factors_by_name: bool = False, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get universe factor or factor category sensitivity data.

**Algorithm:**
1. Branch: if factor_type is Factor -> delegate to `super().get_universe_exposure(...)` with format
2. Else (Category) -> get exposure without format override
3. Branch: if factor_type is Factor or DataFrame is empty -> return sensitivity_df directly
4. Fetch factor_data and set index by name or identifier
5. Build MultiIndex columns mapping (factor_category, factor_name) tuples
6. For each unique factor_category -> aggregate sensitivity by summing across factors in category
7. Concatenate category columns into single DataFrame
8. Branch: if JSON format -> convert to nested dict; else -> return DataFrame

### MacroRiskModel.get_r_squared(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get R-squared data for assets.

**Algorithm:**
1. Delegate to `self._get_asset_data_measure(Measure.R_Squared, ...)`

### MacroRiskModel.get_fair_value_gap(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., fair_value_gap_unit: Unit = Unit.STANDARD_DEVIATION, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get fair value gap data in standard deviation or percent units.

**Algorithm:**
1. Branch: if fair_value_gap_unit is STANDARD_DEVIATION -> use `Measure.Fair_Value_Gap_Standard_Deviation`; else -> use `Measure.Fair_Value_Gap_Percent`
2. Delegate to `self._get_asset_data_measure(measure, ...)`

### MacroRiskModel.get_factor_z_score(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = None, factors: List[str] = [], factors_by_name: bool = True, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get factor z-score data.

**Algorithm:**
1. Delegate to `self._get_factor_data_measure(Measure.Factor_Z_Score, ...)`

### MacroRiskModel.get_model_price(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get model price data.

**Algorithm:**
1. Delegate to `super()._get_asset_data_measure(Measure.Model_Price, ...)`

### MacroRiskModel.__repr__(self) -> str
Purpose: Return detailed repr with conditional optional fields.

### ThematicRiskModel.__init__(self, id_: str, name: str, coverage: CoverageType, term: Term, universe_identifier: UniverseIdentifier, vendor: str, version: float, universe_size: int = None, entitlements: Union[Dict, Entitlements] = None, description: str = None, expected_update_time: dt.time = None)
Purpose: Create a thematic risk model with type hard-coded to `RiskModelType.Thematic`.

**Algorithm:**
1. Call `super().__init__(id_, name, RiskModelType.Thematic, vendor, version, coverage, universe_identifier, term, ...)`

### ThematicRiskModel.from_target(cls, model: RiskModelBuilder) -> ThematicRiskModel
Purpose: Class method to construct ThematicRiskModel from target model.

**Algorithm:**
1. Same pattern as FactorRiskModel.from_target but returns `ThematicRiskModel`

### ThematicRiskModel.get_many(cls, ids=None, terms=None, vendors=None, names=None, coverages=None, limit=None)
Purpose: Get many thematic risk models from Marquee.

**Algorithm:**
1. Call `GsRiskModelApi.get_risk_models(...)` with `types=[RiskModelType.Thematic.value]`
2. Return `cls.from_many_targets(models)`

### ThematicRiskModel.get_universe_sensitivity(self, start_date: dt.date, end_date: dt.date = None, assets: DataAssetsRequest = ..., get_factors_by_name: bool = False, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[List[Dict], pd.DataFrame]
Purpose: Get universe basket sensitivity data.

**Algorithm:**
1. Delegate to `super().get_universe_exposure(start_date, end_date, assets, get_factors_by_name=get_factors_by_name, format=format)`

### ThematicRiskModel.__repr__(self) -> str
Purpose: Return detailed repr with conditional optional fields.

## State Mutation
- `self.__name`: Set during `__init__`, updated by `name.setter`
- `self.__type`: Set during `MarqueeRiskModel.__init__`, updated by `type.setter`
- `self.__vendor`: Set during `__init__`, updated by `vendor.setter`
- `self.__version`: Set during `__init__`, updated by `version.setter`
- `self.__coverage`: Set during `__init__`, updated by `coverage.setter`
- `self.__term`: Set during `__init__`, updated by `term.setter`
- `self.__description`: Set during `__init__`, updated by `description.setter`
- `self.__universe_size`: Set during `__init__`, updated by `universe_size.setter`
- `self.__entitlements`: Set during `__init__`, updated by `entitlements.setter`
- `self.__expected_update_time`: Set during `__init__`, updated by `expected_update_time.setter`
- Thread safety: No thread safety considerations. All API calls are synchronous.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `get_factor` | Factor with given name not found in model |
| `MqValueError` | `get_many_factors` | Some factor names or ids not found for the date range |
| `MqValueError` | `get_asset_contribution_to_risk` | No end-of-day price available; asset not covered by model |
| `MqValueError` | `get_asset_factor_attribution` | Asset not covered; fewer than 2 days of data |
| `MqRequestError` | `get_data` | Timeout (status > 499) or other HTTP errors |
| `MqRequestError` | `upload_asset_coverage_data` | No asset data found on date (404) |
| `ValueError` | `get_factor_data` | Category filter used with FactorType.Category; Aggregations used with FactorType.Factor |
| `ValueError` | `get_bid_ask_spread` | Invalid days parameter (no matching Measure) |
| `ValueError` | `get_trading_volume` | Invalid days parameter |
| `ValueError` | `get_traded_value` | Invalid days parameter |
| `ValueError` | `get_composite_volume` | Invalid days parameter |
| `ValueError` | `get_composite_value` | Invalid days parameter |

## Edge Cases
- `MarqueeRiskModel.__init__` entitlements: Three-way branch -- Entitlements instance, Dict, or None
- `MarqueeRiskModel.__init__` type_: Accepts both str and RiskModelType, converts str
- `from_target` methods: Handle None for universe_identifier, type_, and expected_update_time
- `get_calendar`: If both start_date and end_date are None, returns full calendar without slicing
- `get_missing_dates`: If no start_date, defaults to first posted date; if no end_date, defaults to yesterday
- `get_asset_universe`: If assets.universe is empty and no end_date, end_date defaults to start_date
- `get_factor_volatility`: If factors is None, sets to empty list; JSON + no factors_by_name + no factors -> returns raw
- `get_traded_value` / `get_composite_value`: days defaults to 30 (not 0), so `not days` is only True for explicit 0
- `save`: Try create first, fall back to update on `MqRequestError`
- `save_factor_metadata`: Try update first, fall back to create on `MqRequestError`
- `__repr__` methods: Conditionally append optional fields (universe_size, entitlements, description, expected_update_time)
- `upload_data`: Correctly distinguishes RiskModelData vs dict; handles factor-only uploads (universe_size = 0)

## Bugs Found
- Line 269 (SEIRCM in epidemiology.py referenced as example only, this file is clean): No bugs found in risk_model.py
- Line 1353: `__repr__` uses `format(self)` for universe_size, entitlements, description, expected_update_time instead of the actual field values -- this will print the model id (via `__str__`) instead of the actual field values. (OPEN)

## Coverage Notes
- Branch count: ~120+ (numerous format/return-type branches, type conversion branches, error handling)
- Missing branches: Most are API-dependent (require mocking GS API calls)
- Pragmas: `upload_partial_data` is marked with `@deprecation.deprecated`
