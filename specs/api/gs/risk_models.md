# risk_models.py

## Summary
API client for GS Risk Model services. Provides two classes: `GsRiskModelApi` for base risk model CRUD, calendar management, and date queries; and `GsFactorRiskModelApi` (subclass) for factor-specific operations including factor CRUD, factor data retrieval (daily and intraday), model coverage queries, data upload, and model data queries. Includes the `IntradayFactorDataSource` enum for intraday data source selection. All methods are classmethods and communicate via the Marquee REST API through `GsSession`.

## Dependencies
- Internal: `gs_quant.errors` (MqRateLimitedError, MqTimeoutError, MqInternalServerError), `gs_quant.session` (GsSession), `gs_quant.target.risk_models` (RiskModel, RiskModelCalendar, Factor, RiskModelData, RiskModelDataAssetsRequest, RiskModelDataMeasure, RiskModelEventType, RiskModelTerm)
- External: `datetime`, `logging`, `enum` (Enum), `typing` (Tuple, Dict, List, Union), `backoff`

## Type Definitions

### GsRiskModelApi (class)
Inherits: `object` (implicit)

No instance fields -- all methods are `@classmethod`.

### GsFactorRiskModelApi (class)
Inherits: `GsRiskModelApi`

Has an `__init__` that calls `super().__init__()`. All methods are `@classmethod`.

## Enums and Constants

### IntradayFactorDataSource(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `GS_FMP` | `"GS_FMP"` | Goldman Sachs Factor Model Portfolio data source |
| `GS_REGRESSION` | `"GS_Regression"` | Goldman Sachs Regression-based data source |
| `BARRA` | `"BARRA"` | BARRA risk model data source |
| `AXIOMA` | `"AXIOMA"` | Axioma risk model data source |
| `WOLFE` | `"WOLFE"` | Wolfe Research data source |
| `QI` | `"QI"` | Quant Insight data source |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsRiskModelApi.create_risk_model(cls, model: RiskModel) -> RiskModel
Purpose: Create a new risk model.

**Algorithm:**
1. POST `/risk/models` with model payload, deserialize as RiskModel
2. Return result

### GsRiskModelApi.get_risk_model(cls, model_id: str) -> RiskModel
Purpose: Retrieve a single risk model by ID. Decorated with backoff retry.

**Algorithm:**
1. GET `/risk/models/{model_id}` with `cls=RiskModel`
2. Return deserialized RiskModel

### GsRiskModelApi.get_risk_models(cls, ids: List[str] = None, limit: int = None, offset: int = None, terms: List[str] = None, versions: List[str] = None, vendors: List[str] = None, names: List[str] = None, types: List[str] = None, coverages: List[str] = None) -> Tuple[RiskModel, ...]
Purpose: Retrieve multiple risk models with extensive filtering by IDs, terms, versions, vendors, names, types, and coverages.

**Algorithm:**
1. Build base URL `/risk/models?`
2. Branch: if `limit` truthy -> append `&limit={limit}`
3. Branch: if `ids` truthy -> append `&id=` joined IDs
4. Branch: if `offset` truthy -> append `&offset={offset}`
5. Branch: if `terms` truthy -> append `&term={terms}` (note: passes list directly as string, not joined)
6. Branch: if `versions` truthy -> append `&version=` joined versions
7. Branch: if `vendors` truthy -> append `&vendor=` joined vendors
8. Branch: if `names is not None` -> append `&name=` joined names
9. Branch: if `coverages is not None` -> append `&coverage=` joined coverages
10. Branch: if `types is not None` -> append `&type=` joined types
11. GET with `cls=RiskModel`, return `results` key

### GsRiskModelApi.update_risk_model(cls, model: RiskModel) -> RiskModel
Purpose: Update an existing risk model.

**Algorithm:**
1. PUT `/risk/models/{model.id}` with model payload, deserialize as RiskModel
2. Return result

### GsRiskModelApi.delete_risk_model(cls, model_id: str) -> Dict
Purpose: Delete a risk model by ID.

**Algorithm:**
1. DELETE `/risk/models/{model_id}`
2. Return response dict

### GsRiskModelApi.get_risk_model_calendar(cls, model_id: str) -> RiskModelCalendar
Purpose: Retrieve the calendar for a risk model. Decorated with backoff retry.

**Algorithm:**
1. GET `/risk/models/{model_id}/calendar` with `cls=RiskModelCalendar`
2. Return deserialized calendar

### GsRiskModelApi.upload_risk_model_calendar(cls, model_id: str, model_calendar: RiskModelCalendar) -> RiskModelCalendar
Purpose: Upload/replace the calendar for a risk model.

**Algorithm:**
1. PUT `/risk/models/{model_id}/calendar` with calendar payload, deserialize as RiskModelCalendar
2. Return result

### GsRiskModelApi.get_risk_model_dates(cls, model_id: str, start_date: dt.date = None, end_date: dt.date = None, event_type: RiskModelEventType = None) -> List
Purpose: Retrieve available dates for a risk model with optional filtering. Decorated with backoff retry.

**Algorithm:**
1. Build URL `/risk/models/{model_id}/dates?`
2. Branch: if `start_date is not None` -> append `&startDate=` formatted
3. Branch: if `end_date is not None` -> append `&endDate=` formatted
4. Branch: if `event_type is not None` -> append `&eventType={event_type.value}`
5. GET and return `results` key

### GsFactorRiskModelApi.__init__(self)
Purpose: Initialize the subclass by calling the parent constructor.

**Algorithm:**
1. Call `super().__init__()`

### GsFactorRiskModelApi.get_risk_model_factors(cls, model_id: str) -> Tuple[Factor, ...]
Purpose: Retrieve all factors for a risk model.

**Algorithm:**
1. GET `/risk/models/{model_id}/factors` with `cls=Factor`
2. Return `results` key

### GsFactorRiskModelApi.create_risk_model_factor(cls, model_id: str, factor: Factor) -> Factor
Purpose: Create a new factor for a risk model.

**Algorithm:**
1. POST `/risk/models/{model_id}/factors` with factor payload, deserialize as Factor
2. Return result

### GsFactorRiskModelApi.get_risk_model_factor(cls, model_id: str, factor_id: str) -> Factor
Purpose: Retrieve a single factor by model and factor ID.

**Algorithm:**
1. GET `/risk/models/{model_id}/factors/{factor_id}`
2. Return response (note: no `cls=Factor` deserialization specified)

### GsFactorRiskModelApi.update_risk_model_factor(cls, model_id: str, factor: Factor) -> Factor
Purpose: Update an existing factor for a risk model.

**Algorithm:**
1. Build URL `/risk/models/{model_id}/factors/{factor.identifier}`
2. PUT with factor payload, deserialize as Factor
3. Return result

### GsFactorRiskModelApi.delete_risk_model_factor(cls, model_id: str, factor_id: str) -> Dict
Purpose: Delete a factor from a risk model.

**Algorithm:**
1. DELETE `/risk/models/{model_id}/factors/{factor_id}`
2. Return response dict

### GsFactorRiskModelApi.get_risk_model_factor_data(cls, model_id: str, start_date: dt.date = None, end_date: dt.date = None, identifiers: List[str] = None, include_performance_curve: bool = False, factor_categories: List[str] = None, names: List[str] = None) -> List[Dict]
Purpose: Retrieve factor data for a risk model with optional date/identifier/category/name filtering. Decorated with backoff retry.

**Algorithm:**
1. Build URL `/risk/models/{model_id}/factors/data?`
2. Branch: if `start_date is not None` -> append `&startDate=` formatted
3. Branch: if `end_date is not None` -> append `&endDate=` formatted
4. Branch: if `identifiers is not None` -> append `&identifiers=` joined identifiers
5. Branch: if `include_performance_curve` truthy -> append `&includePerformanceCurve=true`
6. Branch: if `names` truthy -> append `&name=` joined names
7. Branch: if `factor_categories` truthy -> append `&factorCategory=` joined categories
8. GET and return `results` key

### GsFactorRiskModelApi.get_risk_model_factor_data_intraday(cls, model_id: str, start_time: dt.datetime = None, end_time: dt.datetime = None, factor_ids: List[str] = None, factor_categories: List[str] = None, factors: List[str] = None, data_source: Union[IntradayFactorDataSource, str] = None) -> List[Dict]
Purpose: Retrieve intraday factor data with datetime-based filtering. Decorated with backoff retry.

**Algorithm:**
1. Build URL `/risk/models/{model_id}/factors/data/intraday?`
2. Branch: if `start_time is not None` -> append `&startTime=` formatted as `%Y-%m-%dT%H:%M:%SZ`
3. Branch: if `end_time is not None` -> append `&endTime=` formatted as `%Y-%m-%dT%H:%M:%SZ`
4. Branch: if `factor_ids is not None` -> append `&factorId=` joined IDs
5. Branch: if `data_source` truthy -> Branch: if `data_source` is a `str` -> use directly; else -> use `data_source.value`; append `&dataSource=`
6. Branch: if `factors` truthy -> append `&factor=` joined factor names
7. Branch: if `factor_categories` truthy -> append `&factorCategory=` joined categories
8. GET and return `results` key

### GsFactorRiskModelApi.get_risk_model_coverage(cls, asset_ids: List[str] = None, as_of_date: dt.datetime = None, sort_by_term: RiskModelTerm = None) -> List[Dict]
Purpose: Query risk model coverage across assets.

**Algorithm:**
1. Initialize empty query dict
2. Branch: if `asset_ids is not None` -> add `assetIds` key
3. Branch: if `as_of_date is not None` -> add `asOfDate` formatted as `%Y-%m-%d`
4. Branch: if `sort_by_term is not None` -> add `sortByTerm` key
5. POST `/risk/models/coverage` with query and 200s timeout
6. Return `results` key

### GsFactorRiskModelApi.upload_risk_model_data(cls, model_id: str, model_data: Union[Dict, RiskModelData], partial_upload: bool = False, target_universe_size: float = None, final_upload: bool = None, aws_upload: bool = False) -> str
Purpose: Upload data for a risk model with options for partial uploads and AWS-accelerated uploads.

**Algorithm:**
1. Build base URL `/risk/models/data/{model_id}`
2. Branch: if `partial_upload` is True:
   - Append `?partialUpload=true`
   - Branch: if `target_universe_size` truthy -> append `&targetUniverseSize={value}`
   - Branch: if `final_upload is not None` -> append `&finalUpload=true` or `&finalUpload=false`
   - Branch: if `aws_upload` truthy -> append `&awsUpload=true`
3. Branch: else (not partial_upload):
   - Branch: if `aws_upload` truthy -> append `?awsUpload=true`
4. POST model_data with 200s timeout
5. Return result

### GsFactorRiskModelApi.get_risk_model_data(cls, model_id: str, start_date: dt.date, end_date: dt.date = None, assets: RiskModelDataAssetsRequest = None, measures: List[RiskModelDataMeasure] = None, factors: list = None, limit_factors: bool = None) -> Dict
Purpose: Query risk model data for a date range with optional asset/measure/factor filtering. Decorated with backoff retry.

**Algorithm:**
1. Branch: if `end_date` is falsy -> call `get_risk_model_dates(model_id)` and use the last date (as string); else -> format `end_date` as `%Y-%m-%d`
2. Build query dict with `startDate` and `endDate`
3. Branch: if `assets is not None` -> add `assets` key
4. Branch: if `measures is not None` -> add `measures` key
5. Branch: if `factors is not None` -> add `factors` key
6. Branch: if `limit_factors is not None` -> add `limitFactors` key
7. POST `/risk/models/data/{model_id}/query` with query and 200s timeout
8. Return response

## State Mutation
- No instance state -- all methods are classmethods operating on the remote API via HTTP
- `GsSession.current` is read but never mutated by this module
- Thread safety: Relies on `GsSession.current` being thread-local or otherwise safe

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqTimeoutError` | backoff-decorated methods (`get_risk_model`, `get_risk_model_calendar`, `get_risk_model_dates`, `get_risk_model_factor_data`, `get_risk_model_factor_data_intraday`, `get_risk_model_data`) | Retried up to 5 times with exponential backoff |
| `MqInternalServerError` | backoff-decorated methods | Retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | backoff-decorated methods | Retried up to 5 times with 90s constant backoff |

## Edge Cases
- `get_risk_models`: The `terms` parameter is appended as `&term={terms}` where `terms` is the raw list, not joined -- this will produce `&term=['Short', 'Medium']` rather than individual query params. This appears to be a bug compared to how `versions`, `vendors`, etc. are handled.
- `get_risk_model_factor`: Unlike other GET methods, this does not pass `cls=Factor` for deserialization -- returns raw dict.
- `get_risk_model_data`: When `end_date` is None, it fetches the model's date list via an additional API call to use the last available date. The fetched date is already a string (from `get_risk_model_dates` which returns a list), while an explicit `end_date` is formatted -- potential type inconsistency if `get_risk_model_dates` returns date objects instead of strings.
- `upload_risk_model_data`: `target_universe_size` and `final_upload` are only relevant when `partial_upload=True`; they are silently ignored otherwise.
- `get_risk_model_factor_data_intraday`: The `data_source` parameter accepts both `IntradayFactorDataSource` enum and raw `str`, with a type check to determine how to extract the value.

## Bugs Found
- Line 85: `get_risk_models` appends `terms` as `f'&term={terms}'` which stringifies the Python list object rather than joining individual values. Should likely use `'&term='.join(terms)` pattern like `versions`/`vendors`. (OPEN)
- Line 268: In `get_risk_model_data`, when `end_date` is not provided, the code calls `cls.get_risk_model_dates(model_id)[-1]` which returns a raw value from the API `results` list. If these are date strings, it works; if they are date objects, the `endDate` in the query would be a date object rather than a string, unlike the explicit `end_date.strftime(...)` path. (OPEN)

## Coverage Notes
- Branch count: ~36 (9 branches in `get_risk_models`, 3 in `get_risk_model_dates`, 6 in factor data, 7 in intraday factor data, 3 in coverage, 5 in upload, 5 in get model data, plus end_date fallback)
- Missing branches: `partial_upload=True` with various sub-option combinations may be under-tested; `terms` parameter handling
- Pragmas: None observed
