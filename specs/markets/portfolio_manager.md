# portfolio_manager.py

## Summary
Provides `PortfolioManager`, a high-level manager for Marquee portfolios that wraps the GS Portfolio and Report APIs. It supports scheduling/running reports (with batched scheduling), managing entitlements, setting portfolio metadata (currency, AUM source, tag hierarchies), computing macro factor exposure, factor scenario analytics, and risk-model-predicted beta. Also contains the deprecated `CustomAUMDataPoint` value object.

## Dependencies
- Internal:
  - `gs_quant.api.gs.portfolios` (`GsPortfolioApi`)
  - `gs_quant.api.gs.reports` (`GsReportApi`)
  - `gs_quant.common` (`Currency`, `PositionType`)
  - `gs_quant.entities.entitlements` (`Entitlements`, `EntitlementBlock`, `User`)
  - `gs_quant.entities.entity` (`PositionedEntity`, `EntityType`, `ScenarioCalculationMeasure`)
  - `gs_quant.errors` (`MqError`, `MqValueError`)
  - `gs_quant.markets.factor` (`Factor`)
  - `gs_quant.markets.portfolio_manager_utils` (`build_exposure_df`, `build_portfolio_constituents_df`, `build_sensitivity_df`, `get_batched_dates`)
  - `gs_quant.markets.report` (`PerformanceReport`, `ReportJobFuture`)
  - `gs_quant.markets.scenario` (`FactorScenario`)
  - `gs_quant.models.risk_model` (`MacroRiskModel`, `ReturnFormat`, `FactorType`, `FactorRiskModel`)
  - `gs_quant.target.portfolios` (`RiskAumSource`, `PortfolioTree`)
  - `gs_quant.target.risk_models` (`RiskModelDataAssetsRequest`, `RiskModelUniverseIdentifierRequest`)
- External:
  - `datetime` (dt.date, dt.datetime)
  - `logging` (getLogger)
  - `traceback` (format_exc)
  - `time` (sleep)
  - `typing` (List, Union, Dict)
  - `deprecation` (deprecated decorator)
  - `numpy` (np; nan handling)
  - `pandas` (pd; DataFrame, concat, Series)

## Type Definitions

### CustomAUMDataPoint (class, deprecated in 1.0.10)
Inherits: none (plain class)

Represents a portfolio's AUM value for a specific date.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__date` | `dt.date` | *(required)* | Date of the AUM data point |
| `__aum` | `float` | *(required)* | AUM value for that date |

Properties: `date` (get/set), `aum` (get/set) -- both backed by name-mangled private attributes.

### PortfolioManager (class)
Inherits: `PositionedEntity`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__portfolio_id` | `str` | *(required)* | Marquee portfolio identifier |

Also inherits from `PositionedEntity`:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` | `str` | same as `portfolio_id` | Entity ID (set by `PositionedEntity.__init__`) |
| `__entity_type` | `EntityType` | `EntityType.PORTFOLIO` | Always PORTFOLIO |

Properties: `portfolio_id` (get/set), inherited `id` (get-only), inherited `positioned_entity_type` (get-only).

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

### Referenced Enums (defined elsewhere, heavily used)

**RiskAumSource** (from `gs_quant.target.portfolios`):
| Value | Raw |
|-------|-----|
| Gross | `"Gross"` |
| Long | `"Long"` |
| Short | `"Short"` |
| Custom_AUM | `"Custom AUM"` |
| Net | `"Net"` |

**ReturnFormat** (from `gs_quant.models.risk_model`):
| Value | Raw |
|-------|-----|
| JSON | `auto()` |
| DATA_FRAME | `auto()` |

**FactorType** (from `gs_quant.models.risk_model`):
| Value | Raw |
|-------|-----|
| Factor | `"Factor"` |
| Category | `"Category"` |

**ScenarioCalculationMeasure** (from `gs_quant.entities.entity`):
| Value | Raw |
|-------|-----|
| SUMMARY | `"Summary"` |
| ESTIMATED_FACTOR_PNL | `"Factor Pnl"` |
| ESTIMATED_PNL_BY_SECTOR | `"By Sector Pnl Aggregations"` |
| ESTIMATED_PNL_BY_REGION | `"By Region Pnl Aggregations"` |
| ESTIMATED_PNL_BY_DIRECTION | `"By Direction Pnl Aggregations"` |
| ESTIMATED_PNL_BY_ASSET | `"By Asset Pnl"` |

## Functions/Methods

### CustomAUMDataPoint.__init__(self, date: dt.date, aum: float) -> None
Purpose: Initialize a custom AUM data point with a date and value.

**Algorithm:**
1. Store `date` in `self.__date`
2. Store `aum` in `self.__aum`

### CustomAUMDataPoint.date (property getter) -> dt.date
Purpose: Return the date of this AUM data point.

### CustomAUMDataPoint.date (property setter, value: dt.date) -> None
Purpose: Set the date of this AUM data point.

### CustomAUMDataPoint.aum (property getter) -> float
Purpose: Return the AUM value.

### CustomAUMDataPoint.aum (property setter, value: float) -> None
Purpose: Set the AUM value.

---

### PortfolioManager.__init__(self, portfolio_id: str) -> None
Purpose: Initialize a PortfolioManager wrapping a Marquee portfolio.

**Algorithm:**
1. Store `portfolio_id` in `self.__portfolio_id`
2. Call `PositionedEntity.__init__(self, portfolio_id, EntityType.PORTFOLIO)`

### PortfolioManager.portfolio_id (property getter) -> str
Purpose: Return the portfolio ID.

### PortfolioManager.portfolio_id (property setter, value: str) -> None
Purpose: Set the portfolio ID.

---

### PortfolioManager.get_performance_report(self, tags: Dict = None) -> PerformanceReport
Purpose: Retrieve the performance report associated with this portfolio, optionally filtered by fund-of-funds tags.

**Algorithm:**
1. Call `GsReportApi.get_reports(limit=500, position_source_type='Portfolio', position_source_id=self.id, report_type='Portfolio Performance Analytics', tags=tags, scroll='1m')`
2. Branch: if `tags is None` -> filter reports to only those where `report.parameters.tags is None` (root PPA)
3. Branch: if `len(reports) == 0` -> raise `MqError('No performance report found.')`
4. Return `PerformanceReport.from_target(reports[0])`

**Raises:** `MqError` when no matching performance report exists.

---

### PortfolioManager.schedule_reports(self, start_date: dt.date = None, end_date: dt.date = None, backcast: bool = False, months_per_batch: int = None) -> None
Purpose: Schedule all reports for this portfolio, optionally in date-range batches.

**Algorithm:**
1. Branch: if `months_per_batch is None` OR `backcast is True`:
   a. Branch: if `backcast` -> print warning that batching is not supported for backcasted reports
   b. Call `GsPortfolioApi.schedule_reports(self.__portfolio_id, start_date, end_date, backcast=backcast)` -- single call, full range
   c. Return
2. Else (batched scheduling path):
   a. Branch: if `months_per_batch <= 0` -> raise `MqValueError` (invalid input)
   b. Branch: if `end_date is None` OR `start_date is None`:
      - Call `self.get_schedule_dates(backcast=backcast)` to get hints
      - Fill in whichever of `start_date`/`end_date` is None from hints
   c. Branch: if `start_date >= end_date` -> raise `MqValueError` (invalid range)
   d. Get `position_dates` from `self.get_position_dates()`; filter to those within `[start_date, end_date]`
   e. Branch: if `len(position_dates) == 0` -> raise `MqValueError` (no positions in range)
   f. Branch: if `start_date not in position_dates` -> raise `MqError` (first positions must be on start_date)
   g. Else: remove `start_date` from `position_dates` (preparation for sliding window)
   h. Branch: if `end_date not in position_dates` -> append `end_date` to list
   i. Build batch boundaries using a sliding window:
      - Initialize `batch_boundaries = [start_date]`, `prev_date = start_date`
      - For each `(i, d)` in `enumerate(position_dates)`:
        - Compute `current_batch = d - prev_date`
        - Branch: if `current_batch.days > months_per_batch * 30` AND `i > 0`:
          - Set `prev_date = position_dates[i - 1]`
          - Append `prev_date` to `batch_boundaries`
      - Append `end_date` to `batch_boundaries`
   j. Print batch count message
   k. For each consecutive pair in `batch_boundaries`:
      - Print scheduling range
      - Call `GsPortfolioApi.schedule_reports(self.__portfolio_id, batch_boundaries[i], batch_boundaries[i+1], backcast=backcast)`
      - Branch: if `i > 0` AND `i % 10 == 0` -> `sleep(6)` (rate limiting)

**Raises:**
- `MqValueError` when `months_per_batch <= 0`
- `MqValueError` when `start_date >= end_date`
- `MqValueError` when no positions in date range
- `MqError` when first positions not on start_date

---

### PortfolioManager.run_reports(self, start_date: dt.date = None, end_date: dt.date = None, backcast: bool = False, is_async: bool = True, months_per_batch: int = None) -> List[Union[pd.DataFrame, ReportJobFuture]]
Purpose: Schedule and run all reports, returning futures (async) or results (sync with polling).

**Algorithm:**
1. Call `self.schedule_reports(start_date, end_date, backcast, months_per_batch)`
2. Get `reports` from `self.get_reports()`
3. Build `report_futures` = `[report.get_most_recent_job() for report in reports]`
4. Branch: if `is_async` -> return `report_futures` immediately
5. Else (sync polling):
   a. Set `counter = 100`
   b. While `counter > 0`:
      - Check `is_done = [future.done() for future in report_futures]`
      - Branch: if all done (`False not in is_done`) -> return `[job_future.result() for job_future in report_futures]`
      - `sleep(6)`
   c. Note: `counter` is never decremented -- this is an infinite loop if reports never complete. Eventually control falls through after the while (since counter stays 100 and the loop condition is always True, this is actually an infinite loop unless `done()` becomes True).
   d. Raise `MqValueError` with timeout message (unreachable unless the while condition fails, which it never does since counter is never decremented).

**Raises:** `MqValueError` if reports timeout (unreachable in current code due to missing counter decrement -- see Bugs Found).

---

### PortfolioManager.set_entitlements(self, entitlements: Entitlements) -> None
Purpose: Replace the entitlements on the portfolio.

**Algorithm:**
1. Convert `entitlements` to target format via `entitlements.to_target()`
2. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.__portfolio_id)`
3. Set `portfolio_as_target.entitlements = entitlements_as_target`
4. Call `GsPortfolioApi.update_portfolio(portfolio_as_target)`

---

### PortfolioManager.share(self, emails: List[str], admin: bool = False) -> None
Purpose: Share the portfolio with users by email, optionally granting admin access.

**Algorithm:**
1. Fetch `current_entitlements` from `self.get_entitlements()`
2. Resolve `users` from `User.get_many(emails=emails)`
3. Collect `found_emails = [user.email for user in users]`
4. Branch: if `len(found_emails) != len(emails)`:
   - Compute `missing_emails` = emails not in `found_emails`
   - Raise `MqValueError` listing the missing emails
5. Branch: if `admin`:
   - Merge users into `current_entitlements.admin` (set union of existing admin users + new users)
6. Always: merge users into `current_entitlements.view` (set union of existing view users + new users)
7. Call `self.set_entitlements(current_entitlements)`

**Raises:** `MqValueError` when some emails cannot be resolved to users.

---

### PortfolioManager.set_currency(self, currency: Currency) -> None
Purpose: Set the reporting currency of the portfolio.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.__portfolio_id)`
2. Set `portfolio_as_target.currency = currency`
3. Call `GsPortfolioApi.update_portfolio(portfolio_as_target)`

---

### PortfolioManager.get_tag_name_hierarchy(self) -> List
Purpose: Get the ordered list of tag names for fund-of-funds structuring.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.portfolio_id)`
2. Branch: if `portfolio.tag_name_hierarchy` is truthy -> return `list(portfolio.tag_name_hierarchy)`
3. Else -> return `None`

---

### PortfolioManager.set_tag_name_hierarchy(self, tag_names: List) -> None
Purpose: Set the tag name hierarchy for fund-of-funds structuring.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.portfolio_id)`
2. Set `portfolio.tag_name_hierarchy = tag_names`
3. Call `GsPortfolioApi.update_portfolio(portfolio)`

---

### PortfolioManager.update_portfolio_tree(self) -> None
Purpose: Propagate modifications to all sub-portfolios.

**Algorithm:**
1. Call `GsPortfolioApi.update_portfolio_tree(self.portfolio_id)`

---

### PortfolioManager.get_portfolio_tree(self) -> PortfolioTree
Purpose: Get the fund-of-funds portfolio tree.

**Algorithm:**
1. Return `GsPortfolioApi.get_portfolio_tree(self.portfolio_id)`

---

### PortfolioManager.get_all_fund_of_fund_tags(self) -> List
Purpose: Retrieve all unique tag sets from sub-portfolio reports.

**Algorithm:**
1. Initialize `tag_dicts = []`
2. For each report `r` in `self.get_reports()`:
   a. Branch: if `r.parameters.tags is not None`:
      - Build `tags_as_dict = {tag.name: tag.value for tag in r.parameters.tags}`
      - Branch: if `tags_as_dict not in tag_dicts` -> append it
3. Sort `tag_dicts` by number of keys (ascending -- fewer tags first)
4. Return `tag_dicts`

---

### PortfolioManager.get_schedule_dates(self, backcast: bool = False) -> List[dt.date]
Purpose: Get recommended start/end dates for scheduling a report job.

**Algorithm:**
1. Return `GsPortfolioApi.get_schedule_dates(self.id, backcast)`

---

### PortfolioManager.get_aum_source(self) -> RiskAumSource (deprecated 1.0.10)
Purpose: Get the portfolio's AUM source.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.portfolio_id)`
2. Branch: if `portfolio.aum_source is not None` -> return `portfolio.aum_source`
3. Else -> return `RiskAumSource.Long` (default)

---

### PortfolioManager.set_aum_source(self, aum_source: RiskAumSource) -> None (deprecated 1.0.10)
Purpose: Set the portfolio's AUM source.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.portfolio_id)`
2. Set `portfolio.aum_source = aum_source`
3. Call `GsPortfolioApi.update_portfolio(portfolio)`

---

### PortfolioManager.get_custom_aum(self, start_date: dt.date = None, end_date: dt.date = None) -> List[CustomAUMDataPoint] (deprecated 1.0.10)
Purpose: Retrieve custom AUM data points for the portfolio.

**Algorithm:**
1. Call `GsPortfolioApi.get_custom_aum(self.portfolio_id, start_date, end_date)`
2. Return list comprehension: for each `data` dict, create `CustomAUMDataPoint(date=dt.datetime.strptime(data['date'], '%Y-%m-%d'), aum=data['aum'])`

---

### PortfolioManager.get_aum(self, start_date: dt.date, end_date: dt.date) -> Dict (deprecated 1.0.10)
Purpose: Get AUM data as a dict keyed by date string, using the portfolio's configured AUM source.

**Algorithm:**
1. Call `self.get_aum_source()` to get `aum_source`
2. Branch: if `aum_source == RiskAumSource.Custom_AUM`:
   - Get custom AUM points, return dict `{date_str: aum_value}`
3. Branch: if `aum_source == RiskAumSource.Long`:
   - Get long exposure from performance report, return dict `{row['date']: row['longExposure']}`
4. Branch: if `aum_source == RiskAumSource.Short`:
   - Get short exposure, return dict `{row['date']: row['shortExposure']}`
5. Branch: if `aum_source == RiskAumSource.Gross`:
   - Get gross exposure, return dict `{row['date']: row['grossExposure']}`
6. Branch: if `aum_source == RiskAumSource.Net`:
   - Get net exposure, return dict `{row['date']: row['netExposure']}`
7. Implicit: if none match, returns `None` (no explicit default/else)

---

### PortfolioManager.upload_custom_aum(self, aum_data: List[CustomAUMDataPoint], clear_existing_data: bool = None) -> None (deprecated 1.0.10)
Purpose: Upload custom AUM data to the portfolio.

**Algorithm:**
1. Format each `CustomAUMDataPoint` as `{'date': data.date.strftime('%Y-%m-%d'), 'aum': data.aum}`
2. Call `GsPortfolioApi.upload_custom_aum(self.portfolio_id, formatted_aum_data, clear_existing_data)`

---

### PortfolioManager.get_pnl_contribution(self, start_date: dt.date = None, end_date: dt.date = None, currency: Currency = None, tags: Dict = None) -> pd.DataFrame (deprecated 0.9.110)
Purpose: Get PnL contribution broken down by constituents.

**Algorithm:**
1. Branch: if `tags is None` -> set `performance_report_id = None`
2. Else -> set `performance_report_id = self.get_performance_report(tags).id`
3. Return `pd.DataFrame(GsPortfolioApi.get_attribution(self.portfolio_id, start_date, end_date, currency, performance_report_id))`

---

### PortfolioManager.get_macro_exposure(self, model: MacroRiskModel, date: dt.date, factor_type: FactorType, factor_categories: List[Factor] = [], get_factors_by_name: bool = True, tags: Dict = None, return_format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Compute portfolio and per-asset exposure to macro factors or macro factor categories.

**Algorithm:**
1. Get `performance_report` from `self.get_performance_report(tags)`
2. Build `constituents_and_notional_df` from `build_portfolio_constituents_df(performance_report, date)`, rename columns `name -> "Asset Name"`, `netExposure -> "Notional"`
3. Extract `universe` = non-NA asset IDs from the index of the constituents df
4. Build `universe_sensitivities_df` from `build_sensitivity_df(universe, model, date, factor_type, get_factors_by_name)`
5. Get `assets_with_exposure` = list of index values from sensitivities df
6. Branch: if `not assets_with_exposure`:
   - Log warning "The Portfolio is not exposed to any of the requested macro factors"
   - Return empty `pd.DataFrame()`
7. Filter `constituents_and_notional_df` to only rows in `assets_with_exposure`
8. Branch: if `factor_type == FactorType.Factor`:
   - Get `factor_data = model.get_factor_data(date, date, factor_type=FactorType.Factor)`
9. Else:
   - Set `factor_data = pd.DataFrame()` (empty)
10. Build `exposure_df` from `build_exposure_df(constituents_and_notional_df, universe_sensitivities_df, factor_categories, factor_data, get_factors_by_name)`
11. Branch: if `return_format == ReturnFormat.JSON` -> return `exposure_df.to_dict()`
12. Else -> return `exposure_df`

---

### PortfolioManager.get_factor_scenario_analytics(self, scenarios: List[FactorScenario], date: dt.date, measures: List[ScenarioCalculationMeasure], risk_model: str = None, return_format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, Union[Dict, pd.DataFrame]]
Purpose: Delegate factor scenario analytics to the parent class `PositionedEntity`.

**Algorithm:**
1. Return `super().get_factor_scenario_analytics(scenarios, date, measures, risk_model, return_format)`

---

### PortfolioManager.get_risk_model_predicted_beta(self, start_date: dt.date, end_date: dt.date, risk_model_id: str, default_beta_value: int = 1, tags: Dict = None) -> pd.DataFrame
Purpose: Calculate the risk-model-predicted portfolio beta over a date range.

**Algorithm:**
1. Get `performance_report` from `self.get_performance_report(tags=tags)`
2. Get `risk_model = FactorRiskModel.get(risk_model_id)`
3. Get `risk_model_dates` from `risk_model.get_dates(start_date, end_date)`
4. Split into `batched_dates` using `get_batched_dates(risk_model_dates, batch_size=10)`
5. Initialize three empty DataFrames: `risk_model_predicted_beta_timeseries`, `asset_level_betas`, `portfolio_position_net_weights`
6. For each `batch` in `batched_dates`:
   a. Set local `start_date, end_date = batch[0], batch[-1]` (shadows parameter names)
   b. Try:
      - Get `date_wise_net_weight` from `performance_report.get_position_net_weights(...)` with gsid metadata, all business days, CLOSE position type
      - Drop NAs, pivot to `positionDate x gsid` with summed `netWeight`
      - Filter `portfolio_position_gsids` = non-None gsid column names
      - Get `asset_level_betas_batch` from `risk_model.get_predicted_beta(...)` with gsid-based asset request using tuple of gsids
      - Branch: if `asset_level_betas_batch.isna().all().all()`:
        - Log warning about missing data for this batch
        - `continue` to next batch
      - Else:
        - Concatenate `asset_level_betas_batch` into `asset_level_betas` (axis=0)
        - Concatenate `date_wise_net_weight` into `portfolio_position_net_weights` (axis=0)
   c. Except `Exception as ex`:
      - Raise `MqError` with details including the exception and traceback
7. Branch: if `asset_level_betas.empty` -> return empty `pd.DataFrame()`
8. Fill NaN in `asset_level_betas` with `default_beta_value`
9. Compute element-wise product: `risk_model_predicted_beta_timeseries = asset_level_betas * portfolio_position_net_weights`
10. Sum across columns (axis=1), rename to 'beta', replace 0 with NaN, forward-fill
11. Reset index, rename columns `index -> 'date'`, `0 -> 'beta'`
12. Return the resulting DataFrame

**Raises:** `MqError` when risk model data retrieval fails for any batch.

## State Mutation
- `self.__portfolio_id`: Set in `__init__`, can be updated via `portfolio_id` setter
- `PositionedEntity.__id`: Set in `__init__` via super().__init__
- `PositionedEntity.__entity_type`: Set to `EntityType.PORTFOLIO` in `__init__`
- Remote state: Many methods mutate server-side portfolio state via `GsPortfolioApi.update_portfolio()` (e.g., `set_entitlements`, `share`, `set_currency`, `set_tag_name_hierarchy`, `set_aum_source`)
- Remote state: `schedule_reports` and `run_reports` trigger server-side report scheduling/execution
- Thread safety: No internal locking. Multiple concurrent calls to methods that do read-modify-write (e.g., `share`) can race.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqError` | `get_performance_report` | No performance report found for portfolio |
| `MqValueError` | `schedule_reports` | `months_per_batch <= 0` |
| `MqValueError` | `schedule_reports` | `start_date >= end_date` |
| `MqValueError` | `schedule_reports` | No positions in date range |
| `MqError` | `schedule_reports` | First positions not on start_date |
| `MqValueError` | `run_reports` | Reports exceed timeout (unreachable -- see Bugs) |
| `MqValueError` | `share` | Some emails not found as users |
| `MqError` | `get_risk_model_predicted_beta` | Risk model data retrieval exception |

## Edge Cases
- `get_performance_report` with `tags=None`: explicitly filters for root PPA (where `report.parameters.tags is None`), not just taking `reports[0]`
- `schedule_reports` with `backcast=True` and `months_per_batch` set: ignores `months_per_batch`, prints warning, schedules full range
- `schedule_reports` batching: the sliding window uses `months_per_batch * 30` days as the threshold (approximate month length); actual month boundaries are not considered
- `schedule_reports` rate limiting: `sleep(6)` every 10 batches (when `i > 0 and i % 10 == 0`)
- `get_aum` with an unrecognized `RiskAumSource` value: implicitly returns `None` (no else branch)
- `get_macro_exposure` with `factor_categories` default value `[]` (mutable default argument -- shared across calls but only read, not mutated in this method)
- `get_risk_model_predicted_beta` shadows `start_date`/`end_date` parameters inside the batch loop with local rebinding
- `get_risk_model_predicted_beta` when all batches produce NaN betas: returns empty DataFrame
- `get_tag_name_hierarchy` returns `None` (not empty list) when no hierarchy is set

## Bugs Found
- Line 252-257: In `run_reports`, the `counter` variable is initialized to 100 but never decremented inside the `while counter > 0` loop. This creates an infinite loop when reports are running synchronously and never complete. The `MqValueError` on line 258 is unreachable. (OPEN)
- Line 505: In `get_macro_exposure`, the default value for `factor_categories` is a mutable list `[]`. While not mutated here, this is a Python anti-pattern that can cause subtle bugs if the list were ever appended to. (OPEN -- style issue)

## Coverage Notes
- Branch count: ~42
- Key branches in `schedule_reports`: `months_per_batch is None`, `backcast`, `months_per_batch <= 0`, `end_date/start_date is None`, `start_date >= end_date`, `len(position_dates) == 0`, `start_date not in position_dates`, `end_date not in position_dates`, sliding window threshold, sleep rate limiter
- Key branches in `run_reports`: `is_async`, `False not in is_done` (unreachable MqValueError)
- Key branches in `get_aum`: 5 branches for each `RiskAumSource` variant + implicit None return
- Key branches in `get_performance_report`: `tags is None` filter, `len(reports) == 0`
- Key branches in `share`: email count mismatch, `admin` flag
- Key branches in `get_macro_exposure`: empty `assets_with_exposure`, `factor_type == FactorType.Factor`, `return_format == ReturnFormat.JSON`
- Key branches in `get_risk_model_predicted_beta`: per-batch NaN check, exception handling, empty final result
- Deprecated methods (6 total) may warrant `pragma: no cover` or minimal coverage depending on policy
- All `@deprecation.deprecated` decorators add wrapper logic that is not directly testable beyond invocation
