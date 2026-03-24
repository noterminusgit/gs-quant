# portfolios.py

## Summary
API client for GS Portfolio services. Provides CRUD operations on portfolios, position management (retrieval, update, date queries), quote/workflow management, report scheduling, AUM data, portfolio trees, attribution, and risk model coverage lookups. All methods are classmethods on `GsPortfolioApi` and communicate via the Marquee REST API through `GsSession`.

## Dependencies
- Internal: `gs_quant.api.api_session` (ApiWithCustomSession), `gs_quant.common` (PositionType, PositionTag, RiskRequest, Currency), `gs_quant.errors` (MqInternalServerError, MqTimeoutError, MqRateLimitedError), `gs_quant.instrument` (Instrument), `gs_quant.session` (GsSession), `gs_quant.target.portfolios` (Portfolio, Position, PositionSet, PortfolioTree), `gs_quant.target.reports` (Report), `gs_quant.target.risk_models` (RiskModelTerm), `gs_quant.workflow` (WorkflowPosition, WorkflowPositionsResponse, SaveQuoteRequest)
- External: `datetime`, `backoff`, `deprecation`, `logging`, `time` (sleep), `typing` (Tuple, Union, List, Dict)

## Type Definitions

### GsPortfolioApi (class)
Inherits: `ApiWithCustomSession`

No instance fields -- all methods are `@classmethod`. Uses session from `ApiWithCustomSession.get_session()` or `GsSession.current`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsPortfolioApi.get_portfolios(cls, portfolio_ids: List[str] = None, portfolio_names: List[str] = None, limit: int = 100, **kwargs) -> Tuple[Portfolio, ...]
Purpose: Retrieve multiple portfolios filtered by IDs, names, and arbitrary query parameters.

**Algorithm:**
1. Build base URL `/portfolios?`
2. Branch: if `portfolio_ids` is truthy -> append `&id=` joined IDs
3. Branch: if `portfolio_names` is truthy -> append `&name=` joined names
4. Iterate `kwargs`: Branch per value: if value is a list -> append each item as `&key=item`; else -> append `&key=value`
5. Append `&limit={limit}` and GET; return `results` key

### GsPortfolioApi.get_portfolio(cls, portfolio_id: str) -> Portfolio
Purpose: Retrieve a single portfolio by its ID.

**Algorithm:**
1. GET `/portfolios/{id}` with `cls=Portfolio`
2. Return deserialized Portfolio

### GsPortfolioApi.get_portfolio_by_name(cls, name: str) -> Portfolio
Purpose: Retrieve a portfolio by its exact name, raising if zero or more than one match.

**Algorithm:**
1. GET `/portfolios?name={name}` via `cls.get_session()`
2. Extract `totalResults` (default 0)
3. Branch: `num_found == 0` -> raise `ValueError('Portfolio {name} not found')`
4. Branch: `num_found > 1` -> raise `ValueError('More than one portfolio named {name} found')`
5. Else -> return `Portfolio.from_dict(results[0])`

**Raises:** `ValueError` when zero or more than one portfolio matches the name

### GsPortfolioApi.create_portfolio(cls, portfolio: Portfolio) -> Portfolio
Purpose: Create a new portfolio.

**Algorithm:**
1. POST `/portfolios` with portfolio payload, deserialize as Portfolio
2. Return result

### GsPortfolioApi.update_portfolio(cls, portfolio: Portfolio)
Purpose: Update an existing portfolio.

**Algorithm:**
1. PUT `/portfolios/{id}` with portfolio payload, deserialize as Portfolio
2. Return result

### GsPortfolioApi.delete_portfolio(cls, portfolio_id: str) -> dict
Purpose: Delete a portfolio by ID.

**Algorithm:**
1. DELETE `/portfolios/{id}`
2. Return response dict

### GsPortfolioApi.get_portfolio_analyze(cls, portfolio_id: str) -> dict
Purpose: Retrieve analysis results for a portfolio.

**Algorithm:**
1. GET `/portfolios/{id}/analyze`
2. Return response dict

### GsPortfolioApi.get_positions(cls, portfolio_id: str, start_date: dt.date = None, end_date: dt.date = None, position_type: str = 'close') -> Tuple[PositionSet, ...]
Purpose: Retrieve position sets for a portfolio over a date range. Decorated with exponential backoff on timeout/500 errors and constant backoff on rate-limiting.

**Algorithm:**
1. Build URL `/portfolios/{id}/positions?type={positionType}`
2. Branch: if `start_date is not None` -> append `&startDate={iso}`
3. Branch: if `end_date is not None` -> append `&endDate={iso}`
4. GET the URL
5. Return tuple of `PositionSet.from_dict(v)` for each item in `positionSets` key (defaults to empty tuple)

### GsPortfolioApi.get_positions_for_date(cls, portfolio_id: str, position_date: dt.date, position_type: str = 'close') -> PositionSet
Purpose: Retrieve positions for a specific date.

**Algorithm:**
1. GET `/portfolios/{id}/positions/{date}?type={ptype}` with `cls=PositionSet`
2. Extract `results` list
3. Branch: if `len(position_sets) > 0` -> return first element; else -> return `None`

### GsPortfolioApi.get_position_set_by_position_type(cls, positions_type: str, positions_id: str, activity_type: str = 'position') -> Tuple[PositionSet, ...]
Purpose: Retrieve position sets from the risk-internal API based on position type (ETI vs book).

**Algorithm:**
1. Branch: if `positions_type == 'ETI'` -> root = `'deals'`; else -> root = `'books/' + positions_type`
2. Branch: if `activity_type != 'position'` -> URL includes `?activityType={activity_type}`; else -> URL without activity type
3. GET with 181s timeout via `cls.get_session()`
4. Return tuple by calling `_unpack_position_set()` on each item in `positionSets`

### GsPortfolioApi._unpack_position_set(cls, kvs: dict) -> PositionSet
Purpose: Deserialize a position set dict and clear instrument names (workaround for ETI naming issue).

**Algorithm:**
1. Create `PositionSet.from_dict(kvs)`
2. For each position in `position_set.positions`, set `position.instrument.name = None`
3. Return the position set

### GsPortfolioApi.get_instruments_by_position_type(cls, positions_type: str, positions_id: str, activity_type: str) -> Tuple[Instrument, ...]
Purpose: Extract instruments with enriched metadata from position sets.

**Algorithm:**
1. Call `get_position_set_by_position_type()` to get position sets
2. For each position set, for each position: build `metadata` dict with `trade_date`, `tags`, `external_ids` (dict comprehension from `idType`/`idValue`), `party_from`, `party_to`
3. Assign metadata to instrument, append to list
4. Return as tuple

### GsPortfolioApi.get_latest_positions(cls, portfolio_id: str, position_type: str = 'close') -> Union[PositionSet, dict]
Purpose: Retrieve the most recent position set for a portfolio.

**Algorithm:**
1. GET `/portfolios/{id}/positions/last?type={ptype}`, extract `results`
2. Branch: if `results` is a dict AND has `'positions'` key -> convert each position via `Position.from_dict()`
3. Return `PositionSet.from_dict(results)`

### GsPortfolioApi.get_instruments_by_workflow_id(cls, workflow_id: str, prefer_instruments: bool = False) -> Tuple[Instrument, ...]
Purpose: Retrieve instruments associated with a workflow/quote.

**Algorithm:**
1. Branch: if `prefer_instruments` is False -> URL prefix is `/risk-internal`; else -> `/risk`
2. GET `/{prefix}/quote/{workflow_id}` with 181s timeout
3. Iterate `workflowPositions[workflow_id]` -> for each position's `positions` list -> deserialize instrument via `Instrument.from_dict()`
4. Branch: if instrument dict has a `name` key -> set `instrument.name = name`
5. Append and return as tuple

### GsPortfolioApi.get_position_dates(cls, portfolio_id: str) -> Tuple[dt.date, ...]
Purpose: Retrieve all available position dates for a portfolio.

**Algorithm:**
1. GET `/portfolios/{id}/positions/dates`, extract `results`
2. Parse each date string with `strptime('%Y-%m-%d')` and return as tuple of `date` objects

### GsPortfolioApi.update_positions(cls, portfolio_id: str, position_sets: List[PositionSet], net_positions: bool = True) -> float
Purpose: Update positions for a portfolio. Decorated with backoff retry.

**Algorithm:**
1. Build URL with `netPositions` as lowercase boolean string
2. PUT position_sets to the URL
3. Return result

### GsPortfolioApi.get_positions_data(cls, portfolio_id: str, start_date: dt.date, end_date: dt.date, fields: List[str] = None, performance_report_id: str = None, position_type: PositionType = None, include_all_business_days: bool = False) -> List[dict]
Purpose: Retrieve enriched position data with optional field filtering and report linkage. Decorated with backoff retry.

**Algorithm:**
1. Build base URL with `startDate` and `endDate`
2. Branch: if `fields is not None` -> append `&fields=` joined fields
3. Branch: if `performance_report_id is not None` -> append `&reportId=`
4. Branch: if `position_type is not None` -> append `&type={position_type.value}`
5. Branch: if `include_all_business_days` is truthy -> append `&includeAllBusinessDays=true`
6. GET and return `results`

### GsPortfolioApi.update_quote(cls, quote_id: str, request: RiskRequest)
Purpose: Update an existing quote in the risk-internal service.

**Algorithm:**
1. PUT `/risk-internal/quote/save/{id}` with the request payload

### GsPortfolioApi.save_quote(cls, request: RiskRequest) -> str
Purpose: Save a new quote.

**Algorithm:**
1. POST `/risk-internal/quote/save` with request
2. Return `results` from response

### GsPortfolioApi.update_workflow_quote(cls, quote_id: str, request: SaveQuoteRequest)
Purpose: Update a workflow quote using msgpack serialization.

**Algorithm:**
1. Set `Content-Type: application/x-msgpack` header
2. PUT `/risk-internal/quote/workflow/save/{id}` with `tuple([request])`
3. Return `results`

### GsPortfolioApi.save_workflow_quote(cls, request: SaveQuoteRequest) -> str
Purpose: Save a new workflow quote using msgpack serialization.

**Algorithm:**
1. Set `Content-Type: application/x-msgpack` header
2. POST `/risk-internal/quote/workflow/save` with `tuple([request])`
3. Return `results`

### GsPortfolioApi.share_workflow_quote(cls, request: SaveQuoteRequest) -> str
Purpose: Share a workflow quote using msgpack serialization.

**Algorithm:**
1. Set `Content-Type: application/x-msgpack` header
2. POST `/risk-internal/quote/workflow/share` with `tuple([request])`
3. Return `results`

### GsPortfolioApi.get_workflow_quote(cls, workflow_id: str) -> Tuple[WorkflowPosition, ...]
Purpose: Retrieve a workflow quote by ID.

**Algorithm:**
1. GET `/risk-internal/quote/workflow/{workflow_id}` with 181s timeout
2. Deserialize via `WorkflowPositionsResponse.from_dict(results)`
3. Branch: if `wf_pos_res` is truthy -> return `wf_pos_res.results`; else -> return empty tuple `()`

### GsPortfolioApi.get_shared_workflow_quote(cls, workflow_id: str) -> Tuple[WorkflowPosition, ...]
Purpose: Retrieve a shared workflow quote by ID.

**Algorithm:**
1. GET `/risk-internal/quote/workflow/shared/{workflow_id}` with 181s timeout
2. Deserialize via `WorkflowPositionsResponse.from_dict(results)`
3. Branch: if `wf_pos_res` is truthy -> return `wf_pos_res.results`; else -> return empty tuple `()`

### GsPortfolioApi.save_to_shadowbook(cls, request: RiskRequest, name: str) -> str
Purpose: Save a risk request to a named shadowbook.

**Algorithm:**
1. PUT `/risk-internal/shadowbook/save/{name}` with request
2. Return `results`

### GsPortfolioApi.get_risk_models_by_coverage(cls, portfolio_id: str, term: Term = Term.Medium)
Purpose: Get risk models sorted by coverage term for a portfolio.

**Algorithm:**
1. GET `/portfolios/{portfolio_id}/models?sortByTerm={term.value}`
2. Return `results`

### GsPortfolioApi.get_reports(cls, portfolio_id: str, tags: Dict) -> Tuple[Report, ...]
Purpose: Retrieve reports for a portfolio, optionally filtered by tags. Decorated with backoff retry.

**Algorithm:**
1. GET `/portfolios/{id}/reports` with `cls=Report`, extract `results`
2. Branch: if `tags is not None` -> convert tags dict to tuple of `PositionTag` objects, filter results where `r.parameters.tags` matches
3. Return filtered results

### GsPortfolioApi.schedule_reports(cls, portfolio_id: str, start_date: dt.date = None, end_date: dt.date = None, backcast: bool = False) -> dict
Purpose: Schedule report generation for a portfolio. Handles tag hierarchies by scheduling individual reports with rate limiting. Decorated with backoff retry.

**Algorithm:**
1. Build payload with `backcast` parameter
2. Branch: if `start_date is not None` -> add `startDate` to payload
3. Branch: if `end_date is not None` -> add `endDate` to payload
4. Fetch portfolio via `get_portfolio()`
5. Branch: if `tag_name_hierarchy` is None or empty -> POST `/portfolios/{id}/schedule`
6. Else -> iterate `report_ids` with a counter starting at 10:
   - Branch: if counter reaches 0 -> sleep 2 seconds, reset counter to 10, POST `/reports/{report_id}/schedule`
   - Else -> POST `/reports/{report_id}/schedule`, decrement counter

### GsPortfolioApi.get_schedule_dates(cls, portfolio_id: str, backcast: bool = False) -> List[dt.date]
Purpose: Retrieve the scheduled date range for a portfolio. Decorated with backoff retry.

**Algorithm:**
1. GET `/portfolios/{id}/schedule/dates?backcast={lowercase_bool}`
2. Parse `startDate` and `endDate` from response, return as list of two `dt.date` objects

### GsPortfolioApi.get_custom_aum(cls, portfolio_id: str, start_date: dt.date = None, end_date: dt.date = None) -> dict
Purpose: Retrieve custom AUM data for a portfolio. **Deprecated** since v1.0.10 -- use `GsReportApi.get_custom_aum` instead.

**Algorithm:**
1. Build URL `/portfolios/{id}/aum?`
2. Branch: if `start_date` truthy -> append `&startDate=`
3. Branch: if `end_date` truthy -> append `&endDate=`
4. GET and return `data` key

### GsPortfolioApi.upload_custom_aum(cls, portfolio_id: str, aum_data: List[Dict], clear_existing_data: bool = None) -> dict
Purpose: Upload custom AUM data for a portfolio. **Deprecated** since v1.0.10 -- use `GsReportApi.upload_custom_aum` instead.

**Algorithm:**
1. Build URL `/portfolios/{id}/aum`
2. Build payload with `data` key
3. Branch: if `clear_existing_data` truthy -> append `?clearExistingData=true`
4. POST and return result

### GsPortfolioApi.update_portfolio_tree(cls, portfolio_id: str)
Purpose: Trigger an update of the portfolio tree structure.

**Algorithm:**
1. POST `/portfolios/{id}/tree` with empty dict `{}`

### GsPortfolioApi.get_portfolio_tree(cls, portfolio_id: str)
Purpose: Retrieve the portfolio tree structure.

**Algorithm:**
1. GET `/portfolios/{id}/tree` with `cls=PortfolioTree`

### GsPortfolioApi.get_attribution(cls, portfolio_id: str, start_date: dt.date = None, end_date: dt.date = None, currency: Currency = None, performance_report_id: str = None) -> Dict
Purpose: Retrieve attribution data for a portfolio with optional date/currency/report filters.

**Algorithm:**
1. Build URL `/attribution/{portfolio_id}?`
2. Branch: if `start_date` truthy -> append `&startDate=`
3. Branch: if `end_date` truthy -> append `&endDate=`
4. Branch: if `currency` truthy -> append `&currency={currency.value}`
5. Branch: if `performance_report_id` truthy -> append `&reportId=`
6. GET and return `results`

## State Mutation
- No instance state -- all methods are classmethods operating on the remote API via HTTP
- `GsSession.current` is read but never mutated by this module
- Thread safety: Relies on `GsSession.current` being thread-local or otherwise safe; `sleep()` in `schedule_reports` blocks the calling thread

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_portfolio_by_name` | When zero portfolios match the given name |
| `ValueError` | `get_portfolio_by_name` | When more than one portfolio matches the given name |
| `MqTimeoutError` | backoff-decorated methods | Retried up to 5 times with exponential backoff |
| `MqInternalServerError` | backoff-decorated methods | Retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | backoff-decorated methods | Retried up to 5 times with 90s constant backoff |

## Edge Cases
- `get_portfolios`: kwargs values that are lists are iterated and each item is appended separately as a query parameter
- `get_positions_for_date`: Returns `None` if no position sets found for the date
- `get_latest_positions`: Handles both dict-with-positions and other result shapes from the API
- `get_instruments_by_workflow_id`: URL prefix changes based on `prefer_instruments` flag (risk vs risk-internal)
- `schedule_reports`: Rate-limits individual report scheduling by sleeping 2 seconds after every 10 reports when tag hierarchy exists
- `get_custom_aum` / `upload_custom_aum`: Deprecated methods that delegate to report-based equivalents

## Bugs Found
- Line 321: In `schedule_reports`, when `count == 0`, the sleep and reset happen but then the POST also executes. When `count != 0`, the POST also executes and count decrements. The counter resets to 10 after sleep, meaning 11 requests go out per batch (the 10 before sleep + 1 during the `count == 0` branch). This is likely an off-by-one but may be intentional. (OPEN)
- Lines 332-342 in hedges.py noted separately -- not applicable here.

## Coverage Notes
- Branch count: ~40 (conditional URL building across all methods plus special logic in `schedule_reports`, `get_portfolio_by_name`, `get_latest_positions`)
- Missing branches: Deprecated methods may lack full test coverage
- Pragmas: None observed
