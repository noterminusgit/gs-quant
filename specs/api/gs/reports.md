# reports.py

## Summary
API client for GS Reports services. Provides CRUD operations on reports, report scheduling and job management, custom AUM retrieval/upload, factor risk report results/views/tables, and Brinson attribution. All methods are classmethods on `GsReportApi` and communicate via the Marquee REST API through `GsSession`. Includes two supporting enums (`OrderType`, `FactorRiskTableMode`) and uses scroll-based pagination for listing reports.

## Dependencies
- Internal: `gs_quant.base` (EnumBase), `gs_quant.common` (Currency, PositionTag), `gs_quant.errors` (MqTimeoutError, MqInternalServerError, MqRateLimitedError), `gs_quant.session` (GsSession), `gs_quant.target.reports` (Report)
- External: `datetime`, `logging`, `urllib.parse`, `enum` (Enum), `typing` (Tuple, List, Dict), `backoff`

## Type Definitions

### GsReportApi (class)
Inherits: `object` (implicit)

No instance fields -- all methods are `@classmethod`.

## Enums and Constants

### OrderType(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `Ascending` | `"Ascending"` | Sort results in ascending order |
| `Descending` | `"Descending"` | Sort results in descending order |

### FactorRiskTableMode(EnumBase, Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `Pnl` | `"Pnl"` | Display P&L data in factor risk table |
| `Exposure` | `"Exposure"` | Display exposure data in factor risk table |
| `ZScore` | `"ZScore"` | Display Z-score data in factor risk table |
| `Mctr` | `"Mctr"` | Display marginal contribution to risk data |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsReportApi.create_report(cls, report: Report) -> Report
Purpose: Create a new report.

**Algorithm:**
1. POST `/reports` with report payload, deserialize as Report
2. Return result

### GsReportApi.get_report(cls, report_id: str) -> Report
Purpose: Retrieve a single report by ID.

**Algorithm:**
1. GET `/reports/{id}` with `cls=Report`
2. Return deserialized Report

### GsReportApi.get_reports(cls, limit: int = 100, offset: int = None, position_source_type: str = None, position_source_id: str = None, status: str = None, report_type: str = None, order_by: str = None, tags: Dict = None, scroll: str = None) -> Tuple[Report, ...]
Purpose: Retrieve reports with extensive filtering, pagination, and scroll-based iteration. Filters results by tags.

**Algorithm:**
1. Define inner function `build_url(scroll_id=None)`:
   - Build base URL `/reports?limit={limit}`
   - Branch: if `scroll` truthy -> append `&scroll={scroll}`
   - Branch: if `scroll_id` truthy -> append `&scrollId={scroll_id}`
   - Branch: if `offset` truthy -> append `&offset={offset}`
   - Branch: if `position_source_type` truthy -> append `&positionSourceType=`
   - Branch: if `position_source_id` truthy -> append `&positionSourceId=`
   - Branch: if `status` truthy -> append `&status=`
   - Branch: if `report_type` truthy -> append `&reportType=` (URL-encoded)
   - Branch: if `order_by` truthy -> append `&orderBy=`
   - Return URL
2. GET initial page via `build_url()`
3. Extract `results` from response (default `[]`)
4. While response has both `scrollId` and non-empty `results` -> GET next page using `scrollId`, accumulate results
5. Branch: if `tags is not None` -> filter results where `r.parameters.tags` matches tuple of `PositionTag` objects built from tags dict
6. Branch: else (tags is None) -> filter results where `r.parameters.tags is None`
7. Return as tuple

### GsReportApi.update_report(cls, report: Report) -> dict
Purpose: Update an existing report.

**Algorithm:**
1. PUT `/reports/{id}` with report payload, deserialize as Report
2. Return result

### GsReportApi.delete_report(cls, report_id: str) -> dict
Purpose: Delete a report by ID.

**Algorithm:**
1. DELETE `/reports/{id}`
2. Return response dict

### GsReportApi.schedule_report(cls, report_id: str, start_date: dt.date, end_date: dt.date, backcast: bool = False) -> dict
Purpose: Schedule a report for processing over a date range. Decorated with backoff retry.

**Algorithm:**
1. Build schedule request dict with `startDate` and `endDate` formatted as `%Y-%m-%d`
2. Branch: if `backcast` is truthy -> add `parameters.backcast` to request
3. POST `/reports/{id}/schedule` with request payload
4. Return result

### GsReportApi.get_report_status(cls, report_id: str) -> Tuple[dict, ...]
Purpose: Retrieve the processing status of a report.

**Algorithm:**
1. GET `/reports/{id}/status`
2. Return response

### GsReportApi.get_report_jobs(cls, report_id: str) -> Tuple[dict, ...]
Purpose: Retrieve all jobs associated with a report.

**Algorithm:**
1. GET `/reports/{id}/jobs`
2. Return `results` key

### GsReportApi.get_report_job(cls, report_job_id: str) -> dict
Purpose: Retrieve a specific report job by its ID.

**Algorithm:**
1. GET `/reports/jobs/{report_job_id}`
2. Return response

### GsReportApi.reschedule_report_job(cls, report_job_id: str)
Purpose: Reschedule a failed or cancelled report job.

**Algorithm:**
1. POST `/reports/jobs/{report_job_id}/reschedule` with empty dict `{}`

### GsReportApi.cancel_report_job(cls, report_job_id: str) -> dict
Purpose: Cancel a running report job.

**Algorithm:**
1. POST `/reports/jobs/{report_job_id}/cancel`
2. Return response

### GsReportApi.update_report_job(cls, report_job_id: str, status: str) -> dict
Purpose: Update the status of a report job.

**Algorithm:**
1. Build status body dict `{"status": status}`
2. POST `/reports/jobs/{report_job_id}/update` with status body
3. Return response

### GsReportApi.get_custom_aum(cls, report_id: str, start_date: dt.date = None, end_date: dt.date = None) -> dict
Purpose: Retrieve custom AUM data for a report.

**Algorithm:**
1. Build URL `/reports/{report_id}/aum?`
2. Branch: if `start_date` truthy -> append `&startDate=` formatted
3. Branch: if `end_date` truthy -> append `&endDate=` formatted
4. GET and return `data` key

### GsReportApi.upload_custom_aum(cls, report_id: str, aum_data: List[dict], clear_existing_data: bool = None) -> dict
Purpose: Upload custom AUM data for a report.

**Algorithm:**
1. Build URL `/reports/{report_id}/aum`
2. Build payload with `data` key
3. Branch: if `clear_existing_data` truthy -> append `?clearExistingData=true`
4. POST and return result

### GsReportApi.get_factor_risk_report_results(cls, risk_report_id: str, view: str = None, factors: List[str] = None, factor_categories: List[str] = None, currency: Currency = None, start_date: dt.date = None, end_date: dt.date = None, unit: str = None) -> dict
Purpose: Retrieve factor risk report results with various filters. Decorated with backoff retry.

**Algorithm:**
1. Build URL `/risk/factors/reports/{risk_report_id}/results?`
2. Branch: if `view is not None` -> append `&view=`
3. Branch: if `factors is not None` -> URL-encode each factor name via `urllib.parse.quote`, append `&factors=` joined
4. Branch: if `factor_categories is not None` -> append `&factorCategories=` joined
5. Branch: if `currency is not None` -> append `&currency={currency.value}`
6. Branch: if `start_date is not None` -> append `&startDate=` formatted
7. Branch: if `end_date is not None` -> append `&endDate=` formatted
8. Branch: if `unit is not None` -> append `&unit=`
9. GET and return response

### GsReportApi.get_factor_risk_report_view(cls, risk_report_id: str, factor: str = None, factor_category: str = None, currency: Currency = None, start_date: dt.date = None, end_date: dt.date = None, unit: str = None) -> dict
Purpose: Retrieve a factor risk report view using the v2 API.

**Algorithm:**
1. Build query dict from all non-None params: `factor`, `factorCategory`, `currency`, `startDate`, `endDate`, `unit`
2. Filter out None values using `filter(lambda ...)`
3. URL-encode via `urllib.parse.urlencode`
4. **Mutate** `GsSession.current.api_version` to `"v2"`
5. GET `/factor/risk/{risk_report_id}/views?{query_string}`
6. **Restore** `GsSession.current.api_version` to `"v1"`
7. Return response

### GsReportApi.get_factor_risk_report_table(cls, risk_report_id: str, mode: FactorRiskTableMode = None, unit: str = None, currency: Currency = None, date: dt.date = None, start_date: dt.date = None, end_date: dt.date = None) -> dict
Purpose: Retrieve a factor risk report table using the v2 API.

**Algorithm:**
1. **Mutate** `GsSession.current.api_version` to `"v2"`
2. Build URL `/factor/risk/{risk_report_id}/tables?`
3. Branch: if `mode is not None` -> append `&mode={mode.value}`
4. Branch: if `unit is not None` -> append `&unit=`
5. Branch: if `currency is not None` -> append `&currency={currency.value}`
6. Branch: if `date is not None` -> append `&date=` formatted
7. Branch: if `start_date is not None` -> append `&startDate=` formatted
8. Branch: if `end_date is not None` -> append `&endDate=` formatted
9. GET the URL
10. **Restore** `GsSession.current.api_version` to `"v1"`
11. Return response

### GsReportApi.get_brinson_attribution_results(cls, portfolio_id: str, benchmark: str = None, currency: Currency = None, include_interaction: bool = None, aggregation_type: str = None, aggregation_category: str = None, start_date: dt.date = None, end_date: dt.date = None)
Purpose: Retrieve Brinson attribution results for a portfolio.

**Algorithm:**
1. Build URL `/attribution/{portfolio_id}/brinson?`
2. Branch: if `benchmark is not None` -> append `&benchmark=`
3. Branch: if `currency is not None` -> append `&currency={currency.value}`
4. Branch: if `include_interaction is not None` -> append `&includeInteraction={lowercase_bool}`
5. Branch: if `aggregation_type is not None` -> append `&aggregationType=`
6. Branch: if `aggregation_category is not None` -> append `&aggregationCategory=`
7. Branch: if `start_date is not None` -> append `&startDate=` formatted
8. Branch: if `end_date is not None` -> append `&endDate=` formatted
9. GET and return response

## State Mutation
- `get_factor_risk_report_view`: Temporarily mutates `GsSession.current.api_version` from `"v1"` to `"v2"` and back. Not exception-safe -- if GET raises, `api_version` remains `"v2"`.
- `get_factor_risk_report_table`: Same pattern as above -- mutates `api_version` to `"v2"` before the request and restores to `"v1"` after. Not exception-safe.
- Thread safety: The `api_version` mutation is not thread-safe; concurrent calls could interfere.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqTimeoutError` | backoff-decorated methods (`schedule_report`, `get_factor_risk_report_results`) | Retried up to 5 times with exponential backoff |
| `MqInternalServerError` | backoff-decorated methods | Retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | backoff-decorated methods | Retried up to 5 times with 90s constant backoff |

## Edge Cases
- `get_reports`: When `tags` is None, results are filtered to only those with `parameters.tags is None` (not unfiltered)
- `get_reports`: Scroll-based pagination continues until either `scrollId` is absent or `results` is empty
- `get_factor_risk_report_results`: Factor names are URL-encoded to handle special characters (e.g., "Automobiles & Components")
- `get_factor_risk_report_view` / `get_factor_risk_report_table`: If an exception occurs during the GET, `api_version` is left as `"v2"`, which could corrupt subsequent requests
- `schedule_report`: The `backcast` parameter is only included in the payload when truthy, not when explicitly set to `False`

## Bugs Found
- `get_factor_risk_report_view` and `get_factor_risk_report_table`: The `api_version` mutation is not wrapped in a try/finally, so on exception the version stays at `"v2"`. This could silently corrupt subsequent API calls. (OPEN)

## Coverage Notes
- Branch count: ~38 (7 branches in `build_url`, scroll loop, tags filter, 8 branches in factor risk results, 6 in factor risk table, 7 in brinson attribution, plus smaller methods)
- Missing branches: All branch paths in URL building should be testable with appropriate parameter combinations
- Pragmas: None observed
