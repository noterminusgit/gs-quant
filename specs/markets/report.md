# report.py

## Summary
Provides classes for creating, scheduling, running, and querying Marquee analytics reports. The module defines a base `Report` class and specialized subclasses -- `PerformanceReport` (portfolio PnL/exposure analytics), `FactorRiskReport` (factor risk and attribution analytics), and `ThematicReport` (thematic basket exposure analytics). It also contains standalone helper functions for PnL percent calculations, factor return smoothing (Carino log-linking), and result formatting.

## Dependencies
- Internal: `gs_quant.api.gs.data` (`GsDataApi`), `gs_quant.api.gs.portfolios` (`GsPortfolioApi`), `gs_quant.api.gs.reports` (`GsReportApi`, `FactorRiskTableMode`), `gs_quant.api.gs.thematics` (`Region`, `GsThematicApi`, `ThematicMeasure`), `gs_quant.common` (`PositionType`, `ReportParameters`, `Currency`, `PositionTag`), `gs_quant.datetime` (`business_day_offset`, `prev_business_date`), `gs_quant.errors` (`MqValueError`), `gs_quant.markets.report_utils` (`_get_ppaa_batches`), `gs_quant.target.coordinates` (`MDAPIDataBatchResponse`), `gs_quant.target.data` (`DataQuery`, `DataQueryResponse`), `gs_quant.target.reports` (`Report as TargetReport`, `ReportType`, `PositionSourceType`, `ReportStatus`), `gs_quant.target.portfolios` (`RiskAumSource`)
- External: `datetime` (dt), `enum` (Enum, auto), `numpy` (np), `time` (sleep), `typing` (Tuple, Union, List, Dict, OrderedDict), `scipy.stats` (st), `pandas` (pd), `dateutil.relativedelta` (relativedelta), `inflection` (titleize)

## Type Definitions

### CustomAUMDataPoint (class)
Inherits: (none -- plain class)

Represents a portfolio's AUM value for a specific date.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__date` | `dt.date` | (required) | Date of the AUM data point |
| `__aum` | `float` | (required) | AUM value on that date |

Properties with getters and setters: `date`, `aum`.

---

### ReportJobFuture (class)
Inherits: (none -- plain class)

Monitors the status and results of an asynchronous report job.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__report_id` | `str` | (required) | Marquee report ID |
| `__job_id` | `str` | (required) | Marquee job ID |
| `__report_type` | `ReportType` | (required) | Type of the report job |
| `__start_date` | `dt.date` | (required) | Job start date |
| `__end_date` | `dt.date` | (required) | Job end date |

Read-only properties: `job_id`, `end_date`.

---

### Report (class)
Inherits: (none -- plain class)

General base report class for all Marquee reports.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` | `str` | `None` | Marquee report ID |
| `__name` | `str` | `None` | Report name |
| `__position_source_id` | `str` | `None` | Position source ID (portfolio or asset) |
| `__position_source_type` | `PositionSourceType` | `None` | Coerced from str or PositionSourceType |
| `__type` | `ReportType` | `None` | Coerced from str or ReportType |
| `__parameters` | `ReportParameters` | `None` | Report parameters |
| `__earliest_start_date` | `dt.date` | `None` | Earliest start date |
| `__latest_end_date` | `dt.date` | `None` | Latest end date |
| `__latest_execution_time` | `dt.datetime` | `None` | Timestamp of last execution |
| `__status` | `ReportStatus` | `ReportStatus.new` | Coerced from str or ReportStatus |
| `__percentage_complete` | `float` | `None` | Completion percentage |

Read-only properties: `id`, `name`, `earliest_start_date`, `latest_end_date`, `latest_execution_time`, `status`, `percentage_complete`.
Read-write properties (with setter coercion): `position_source_id`, `position_source_type`, `type`, `parameters`.

---

### PerformanceReport (class)
Inherits: `Report`

Historical PnL and exposure analytics for a position source. Hardcodes `report_type` to `ReportType.Portfolio_Performance_Analytics`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (all inherited from Report) | | | |

Constructor accepts `**kwargs` (extra keyword args are silently ignored, notably `report_type` from `from_target`).

---

### FactorRiskReport (class)
Inherits: `Report`

Factor risk and attribution analytics. Derives `position_source_type` and `report_type` from the position source ID prefix if not explicitly provided.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `risk_model_id` | `str` | `None` | Risk model ID (stored in `parameters.risk_model`) |
| `fx_hedged` | `bool` | `True` | Whether position source is FX hedged (stored in `parameters.fx_hedged`) |
| `benchmark_id` | `str` | `None` | Optional benchmark asset ID (stored in `parameters.benchmark`) |
| `tags` | `Tuple[PositionTag, ...]` | `None` | Report tags (stored in `parameters.tags`) |
| (all inherited from Report) | | | |

Constructor accepts `**kwargs`.

---

### ThematicReport (class)
Inherits: `Report`

Thematic basket exposure analytics. Derives `position_source_type` and `report_type` from the position source ID prefix if not explicitly provided.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (all inherited from Report) | | | |

Constructor accepts `**kwargs`.

## Enums and Constants

### ReturnFormat(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| JSON | `auto()` | Return results as JSON dict |
| DATA_FRAME | `auto()` | Return results as Pandas DataFrame |

### ReportDataset(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| PPA_DATASET | `"PPA"` | Portfolio Performance Analytics dataset |
| PPAA_DATASET | `"PPAA"` | Portfolio Performance Analytics Asset-level dataset |
| PFR_DATASET | `"PFR"` | Portfolio Factor Risk dataset |
| PFRA_DATASET | `"PFRA"` | Portfolio Factor Risk Asset-level dataset |
| AFR_DATASET | `"AFR"` | Asset Factor Risk dataset |
| AFRA_DATASET | `"AFRA"` | Asset Factor Risk Asset-level dataset |
| ATA_DATASET | `"ATA"` | Asset Thematic Analytics dataset |
| ATAA_DATASET | `"ATAA"` | Asset Thematic Analytics Asset-level dataset |
| PTA_DATASET | `"PTA"` | Portfolio Thematic Analytics dataset |
| PTAA_DATASET | `"PTAA"` | Portfolio Thematic Analytics Asset-level dataset |
| PORTFOLIO_CONSTITUENTS | `"PORTFOLIO_CONSTITUENTS"` | Portfolio constituents dataset |

### FactorRiskViewsMode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Risk | `'Risk'` | Risk view mode |
| Attribution | `'Attribution'` | Attribution view mode |

### FactorRiskResultsMode(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Portfolio | `'Portfolio'` | Portfolio-level results |
| Positions | `'Positions'` | Position-level results |

### FactorRiskUnit(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Percent | `'Percent'` | Return results as percentage |
| Notional | `'Notional'` | Return results as notional value |

### AttributionAggregationType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Arithmetic | `'arithmetic'` | Arithmetic aggregation |
| Geometric | `'geometric'` | Geometric aggregation |

### AggregationCategoryType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| Sector | `'assetClassificationsGicsSector'` | Aggregate by GICS sector |
| Industry | `'assetClassificationsGicsIndustry'` | Aggregate by GICS industry |
| Region | `'region'` | Aggregate by region |
| Country | `'assetClassificationsCountryName'` | Aggregate by country name |

### Module Constants
None.

## Functions/Methods

### CustomAUMDataPoint.__init__(self, date: dt.date, aum: float) -> None
Purpose: Construct a custom AUM data point with a date and AUM value.

**Algorithm:**
1. Store `date` in `self.__date`
2. Store `aum` in `self.__aum`

---

### ReportJobFuture.__init__(self, report_id: str, job_id: str, report_type: ReportType, start_date: dt.date, end_date: dt.date) -> None
Purpose: Construct a report job future with all identifying fields.

**Algorithm:**
1. Store all five parameters in private fields

---

### ReportJobFuture.status(self) -> ReportStatus
Purpose: Query the current status of the report job from the API.

**Algorithm:**
1. Call `GsReportApi.get_report_job(self.__job_id)`
2. Return `ReportStatus(job.get('status'))`

---

### ReportJobFuture.done(self) -> bool
Purpose: Check whether the report job has reached a terminal state.

**Algorithm:**
1. Call `self.status()`
2. Return `True` if status is in `[ReportStatus.done, ReportStatus.error, ReportStatus.cancelled]`
3. Return `False` otherwise

---

### ReportJobFuture.result(self) -> Union[pd.DataFrame, None]
Purpose: Retrieve results of a completed report job as a DataFrame.

**Algorithm:**
1. Get current status via `self.status()`
2. Branch: status == cancelled -> raise `MqValueError` ("cancelled")
3. Branch: status == error -> raise `MqValueError` ("error")
4. Branch: status != done -> raise `MqValueError` ("not done")
5. Branch: report_type in [Portfolio_Factor_Risk, Asset_Factor_Risk] -> call `GsReportApi.get_factor_risk_report_results(...)`, return `pd.DataFrame(results)`
6. Branch: report_type == Portfolio_Performance_Analytics -> construct `DataQuery`, call `GsDataApi.query_data(...)`, return `pd.DataFrame(results)`
7. Otherwise -> return `None`

**Raises:** `MqValueError` when status is cancelled, error, or not done.

---

### ReportJobFuture.wait_for_completion(self, sleep_time: int = 10, max_retries: int = 10, error_on_timeout: bool = True) -> bool
Purpose: Poll until the job is done or retries are exhausted.

**Algorithm:**
1. Set `retries = 0`
2. Loop: while `not self.done()` and `retries < max_retries` -> sleep `sleep_time`, increment retries
3. Branch: `retries == max_retries` and `error_on_timeout` is True -> raise `MqValueError`
4. Branch: `retries == max_retries` and `error_on_timeout` is False -> print message, return `False`
5. Return `True`

**Raises:** `MqValueError` when timeout with `error_on_timeout=True`.

---

### ReportJobFuture.reschedule(self) -> None
Purpose: Reschedule the report job via the API.

**Algorithm:**
1. Call `GsReportApi.reschedule_report_job(self.__job_id)`

---

### Report.__init__(self, report_id: str = None, name: str = None, position_source_id: str = None, position_source_type: Union[str, PositionSourceType] = None, report_type: Union[str, ReportType] = None, parameters: ReportParameters = None, earliest_start_date: dt.date = None, latest_end_date: dt.date = None, latest_execution_time: dt.datetime = None, status: Union[str, ReportStatus] = ReportStatus.new, percentage_complete: float = None) -> None
Purpose: Construct a Report with type coercion for enum-like fields.

**Algorithm:**
1. Store `report_id` -> `self.__id`
2. Store `name` -> `self.__name`
3. Store `position_source_id` -> `self.__position_source_id`
4. Branch: `position_source_type` is `PositionSourceType` or `None` -> store directly; else -> coerce via `PositionSourceType(position_source_type)`
5. Branch: `report_type` is `ReportType` or `None` -> store directly; else -> coerce via `ReportType(report_type)`
6. Store remaining fields directly
7. Branch: `status` is `ReportStatus` -> store directly; else -> coerce via `ReportStatus(status)`

---

### Report.get(cls, report_id: str, acceptable_types: List[ReportType] = None) -> Report (classmethod)
Purpose: Fetch a report by ID from the API and instantiate.

**Algorithm:**
1. Call `GsReportApi.get_report(report_id)`
2. Return `cls.from_target(result)`

Note: `acceptable_types` parameter is accepted but never used.

---

### Report.from_target(cls, report: TargetReport) -> Report (classmethod)
Purpose: Construct a Report from a TargetReport API object.

**Algorithm:**
1. Map all fields from `report` to the `Report` constructor
2. Return new `Report` instance

---

### Report.save(self) -> None
Purpose: Create or update the report in Marquee.

**Algorithm:**
1. Construct `TargetReport` with current field values; use empty `ReportParameters()` if `self.parameters` is falsy
2. Branch: `self.id` is truthy -> set `target_report.id`, call `GsReportApi.update_report(target_report)`
3. Branch: `self.id` is falsy -> call `GsReportApi.create_report(target_report)`, update `self.__id` from response

---

### Report.delete(self) -> None
Purpose: Delete the report from Marquee.

**Algorithm:**
1. Call `GsReportApi.delete_report(self.id)`

---

### Report.set_position_source(self, entity_id: str) -> None
Purpose: Set position source type and ID; also update report type for FactorRiskReport and ThematicReport subclasses.

**Algorithm:**
1. Determine `is_portfolio` = `entity_id.startswith('MP')`
2. Set `self.position_source_type` to `'Portfolio'` or `'Asset'`
3. Set `self.position_source_id` to `entity_id`
4. Branch: `isinstance(self, FactorRiskReport)` -> set type to `Portfolio_Factor_Risk` or `Asset_Factor_Risk`
5. Branch: `isinstance(self, ThematicReport)` -> set type to `Portfolio_Thematic_Analytics` or `Asset_Thematic_Analytics`

---

### Report.get_most_recent_job(self) -> ReportJobFuture
Purpose: Retrieve the most recently created report job.

**Algorithm:**
1. Call `GsReportApi.get_report_jobs(self.id)`
2. Sort by `createdTime` descending, take first element
3. Construct and return `ReportJobFuture` with parsed dates (strptime `%Y-%m-%d`)

---

### Report.schedule(self, start_date: dt.date = None, end_date: dt.date = None, backcast: bool = None) -> None
Purpose: Schedule the report for execution over a date range.

**Algorithm:**
1. Branch: `self.id` or `self.__position_source_id` is None -> raise `MqValueError`
2. Branch: `position_source_type != Portfolio` and either date is None -> raise `MqValueError`
3. Branch: either date is None (portfolio case) ->
   a. Fetch position dates via `GsPortfolioApi.get_position_dates(self.position_source_id)`
   b. Branch: no position dates -> raise `MqValueError`
   c. Branch: `start_date is None` and `backcast` -> `business_day_offset(min(dates) - 1 year, -1, roll='forward')`
   d. Branch: `start_date is None` and not `backcast` -> `min(dates)`
   e. Branch: `end_date is None` and `backcast` -> `min(dates)`
   f. Branch: `end_date is None` and not `backcast` -> `business_day_offset(today, -1, roll='forward')`
4. Call `GsReportApi.schedule_report(...)`

**Raises:** `MqValueError` for missing IDs, missing dates on non-Portfolio sources, or empty positions.

---

### Report.run(self, start_date: dt.date = None, end_date: dt.date = None, backcast: bool = False, is_async: bool = True) -> Union[ReportJobFuture, Any]
Purpose: Schedule and run a report, optionally waiting for results.

**Algorithm:**
1. Call `self.schedule(start_date, end_date, backcast)`
2. Outer loop: `counter = 5`, while `counter > 0`:
   a. Try: `job_future = self.get_most_recent_job()`
   b. Branch: `is_async` -> return `job_future` immediately
   c. Inner loop: `counter = 100`, while `counter > 0`:
      - Branch: `job_future.done()` -> return `job_future.result()`
      - Decrement counter, sleep 6 seconds
   d. If inner loop exhausted -> raise `MqValueError` (taking too long)
   e. Except `IndexError` -> decrement outer counter, retry
3. After outer loop exhausted: get report status via `Report.get(self.id).status`
4. Branch: status == waiting -> raise `MqValueError` ("stuck in waiting")
5. Otherwise -> raise `MqValueError` ("taking longer than expected")

**Raises:** `MqValueError` in multiple timeout/error scenarios.

---

### PerformanceReport.__init__(self, report_id=None, name=None, position_source_id=None, position_source_type=None, parameters=None, earliest_start_date=None, latest_end_date=None, latest_execution_time=None, status=ReportStatus.new, percentage_complete=None, **kwargs) -> None
Purpose: Construct a PerformanceReport, hardcoding report_type to `Portfolio_Performance_Analytics`.

**Algorithm:**
1. Call `super().__init__(...)` with `report_type=ReportType.Portfolio_Performance_Analytics`

Note: `**kwargs` silently absorbs extra keyword arguments (e.g. `report_type` passed by `from_target`).

---

### PerformanceReport.get(cls, report_id: str, **kwargs) -> PerformanceReport (classmethod)
Purpose: Fetch a performance report by ID.

**Algorithm:**
1. Call `GsReportApi.get_report(report_id)`
2. Return `cls.from_target(result)`

---

### PerformanceReport.from_target(cls, report: TargetReport) -> PerformanceReport (classmethod)
Purpose: Construct from a TargetReport with type validation.

**Algorithm:**
1. Branch: `report.type != ReportType.Portfolio_Performance_Analytics` -> raise `MqValueError`
2. Construct and return `PerformanceReport` from report fields

**Raises:** `MqValueError` if report type is wrong.

---

### PerformanceReport.get_pnl(self, start_date=None, end_date=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical portfolio PnL.

**Algorithm:**
1. Delegate to `self.get_pnl_measure("pnl", unit, start_date, end_date)`

---

### PerformanceReport.get_long_exposure(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio long exposure.

**Algorithm:**
1. Delegate to `self.get_measure("longExposure", start_date, end_date)`

---

### PerformanceReport.get_short_exposure(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio short exposure.

**Algorithm:**
1. Delegate to `self.get_measure("shortExposure", start_date, end_date)`

---

### PerformanceReport.get_asset_count(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio asset count.

**Algorithm:**
1. Delegate to `self.get_measure("assetCount", start_date, end_date)`

---

### PerformanceReport.get_turnover(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio turnover.

**Algorithm:**
1. Delegate to `self.get_measure("turnover", start_date, end_date)`

---

### PerformanceReport.get_asset_count_long(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio long asset count.

**Algorithm:**
1. Delegate to `self.get_measure("assetCountLong", start_date, end_date)`

---

### PerformanceReport.get_asset_count_short(self, start_date=None, end_date=None) -> Union[MDAPIDataBatchResponse, DataQueryResponse, tuple, list]
Purpose: Get historical portfolio short asset count.

**Algorithm:**
1. Delegate to `self.get_measure("assetCountShort", start_date, end_date)`

Note: Return type annotation is overly broad; actual return is `pd.DataFrame` or `Dict`.

---

### PerformanceReport.get_net_exposure(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio net exposure.

**Algorithm:**
1. Delegate to `self.get_measure("netExposure", start_date, end_date)`

---

### PerformanceReport.get_gross_exposure(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio gross exposure.

**Algorithm:**
1. Delegate to `self.get_measure("grossExposure", start_date, end_date)`

---

### PerformanceReport.get_position_net_weights(self, start_date: dt.date, end_date: dt.date, asset_metadata_fields: List[str] = ["id", "name", "ticker"], include_all_business_days: bool = True, position_type: PositionType = None) -> pd.DataFrame
Purpose: Get the net weight of each position in the portfolio for a date range.

**Algorithm:**
1. Mutate `asset_metadata_fields` by appending `"netWeight"` (mutable default argument side effect)
2. Try: call `self.get_positions_data(...)`, wrap in `pd.DataFrame`, return
3. Except any Exception -> raise `MqValueError` with wrapped message

**Raises:** `MqValueError` wrapping any underlying exception.

---

### PerformanceReport.get_trading_pnl(self, start_date=None, end_date=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical portfolio trading PnL.

**Algorithm:**
1. Delegate to `self.get_pnl_measure("tradingPnl", unit, start_date, end_date)`

---

### PerformanceReport.get_trading_cost_pnl(self, start_date=None, end_date=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical portfolio trading cost PnL.

**Algorithm:**
1. Delegate to `self.get_pnl_measure("tradingCostPnl", unit, start_date, end_date)`

---

### PerformanceReport.get_servicing_cost_long_pnl(self, start_date=None, end_date=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical portfolio servicing cost long PnL.

**Algorithm:**
1. Delegate to `self.get_pnl_measure("servicingCostLongPnl", unit, start_date, end_date)`

---

### PerformanceReport.get_servicing_cost_short_pnl(self, start_date=None, end_date=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical portfolio servicing cost short PnL.

**Algorithm:**
1. Delegate to `self.get_pnl_measure("servicingCostShortPnl", unit, start_date, end_date)`

---

### PerformanceReport.get_asset_count_priced(self, start_date=None, end_date=None) -> pd.DataFrame
Purpose: Get historical portfolio asset count priced.

**Algorithm:**
1. Delegate to `self.get_measure("assetCountPriced", start_date, end_date)`

---

### PerformanceReport.get_measure(self, field: str, start_date=None, end_date=None, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Query a single metric from the PPA dataset.

**Algorithm:**
1. Build `DataQuery` with `where={'reportId': self.id}`, `fields=(field,)`, date range
2. Call `GsDataApi.query_data(query, dataset_id=ReportDataset.PPA_DATASET.value)`
3. Branch: `return_format == DATA_FRAME` -> return `pd.DataFrame(results)`
4. Else -> return raw `results`

---

### PerformanceReport.get_pnl_measure(self, field: str, unit: FactorRiskUnit, start_date: dt.date, end_date: dt.date) -> pd.DataFrame
Purpose: Get a PnL measure, optionally converting to percent.

**Algorithm:**
1. Call `self.get_measure(field, start_date, end_date)` -> `measure`
2. Branch: `unit == Notional` -> return `measure` as-is
3. Else (Percent) -> call `get_pnl_percent(self, measure, field, start_date, end_date)` to get aggregated PnL
4. Merge: drop `field` column from `measure`, merge with aggregated on date, rename `'return'` column to `field`
5. Return merged DataFrame

---

### PerformanceReport.get_many_measures(self, measures: Tuple[str, ...] = None, start_date=None, end_date=None, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Query multiple metrics from the PPA dataset in a single call.

**Algorithm:**
1. Branch: `measures is None` -> set `measures = []`
2. Build `DataQuery` with `fields=tuple(measures)`, date range
3. Call `GsDataApi.query_data(...)`
4. Branch: `return_format == DATA_FRAME` -> return `pd.DataFrame(results)`; else -> return raw results

---

### PerformanceReport.get_aum_source(self) -> RiskAumSource
Purpose: Get AUM source for the portfolio associated with this report.

**Algorithm:**
1. Call `GsPortfolioApi.get_portfolio(self.position_source_id)`
2. Branch: `portfolio.aum_source is not None` -> return it
3. Else -> return `RiskAumSource.Long`

---

### PerformanceReport.set_aum_source(self, aum_source: RiskAumSource) -> None
Purpose: Set the AUM source on the portfolio.

**Algorithm:**
1. Fetch portfolio via `GsPortfolioApi.get_portfolio(self.position_source_id)`
2. Set `portfolio.aum_source = aum_source`
3. Call `GsPortfolioApi.update_portfolio(portfolio)`

---

### PerformanceReport.get_custom_aum(self, start_date=None, end_date=None) -> List[CustomAUMDataPoint]
Purpose: Get custom AUM data for the report's date range.

**Algorithm:**
1. Call `GsReportApi.get_custom_aum(self.id, start_date, end_date)`
2. Map each result to `CustomAUMDataPoint` with date parsed via `strptime('%Y-%m-%d')`
3. Return list

---

### PerformanceReport.get_aum(self, start_date: dt.date, end_date: dt.date) -> Dict[str, float]
Purpose: Get AUM data using the portfolio's configured AUM source.

**Algorithm:**
1. Call `self.get_aum_source()` -> `aum_source`
2. Branch: `Custom_AUM` -> get custom AUM, return dict of `{date_str: aum}`
3. Branch: `Long` -> get long exposure, return dict of `{date: longExposure}`
4. Branch: `Short` -> get short exposure, return dict of `{date: shortExposure}`
5. Branch: `Gross` -> get gross exposure, return dict of `{date: grossExposure}`
6. Branch: `Net` -> get net exposure, return dict of `{date: netExposure}`
7. If none match -> implicit `None` return (missing else clause)

---

### PerformanceReport.upload_custom_aum(self, aum_data: List[CustomAUMDataPoint], clear_existing_data: bool = None) -> None
Purpose: Upload custom AUM data for the portfolio.

**Algorithm:**
1. Format each data point to `{'date': str, 'aum': float}`
2. Call `GsReportApi.upload_custom_aum(self.id, formatted_aum_data, clear_existing_data)`

---

### PerformanceReport.get_positions_data(self, start=None, end=dt.date.today(), fields=None, include_all_business_days=False, position_type=None) -> List[Dict]
Purpose: Fetch positions data from the portfolio API.

**Algorithm:**
1. Call `GsPortfolioApi.get_positions_data(self.position_source_id, start, end, fields, performance_report_id=self.id, include_all_business_days=include_all_business_days, position_type=position_type)`
2. Return the result
3. Dead code: `raise NotImplementedError` (line 832, unreachable)

Note: The `end` default of `dt.date.today()` is evaluated once at module load time (mutable default).

---

### PerformanceReport.get_portfolio_constituents(self, fields=None, start_date=None, end_date=None, prefer_rebalance_positions=False, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get historical portfolio constituents, batched by asset count.

**Algorithm:**
1. Build `where = {'reportId': self.id}`
2. Call `self.get_asset_count(start_date, end_date)` -> `asset_count`
3. Branch: `asset_count.empty` -> return empty `pd.DataFrame()` or `{}` based on `return_format`
4. Call `_get_ppaa_batches(asset_count, 3000000)` to compute date batches
5. Build a `DataQuery` for each batch, query `PORTFOLIO_CONSTITUENTS` dataset
6. Flatten results with `sum(results, [])`
7. Branch: `prefer_rebalance_positions` ->
   a. Collect all dates with `entryType == 'Rebalance'`
   b. Filter results: keep rows where date is not in rebalance_dates OR entryType is 'Rebalance'
8. Branch: `return_format == DATA_FRAME` -> return `pd.DataFrame(results)`; else -> return raw results

---

### PerformanceReport.get_pnl_contribution(self, start_date=None, end_date=None, currency=None) -> pd.DataFrame
Purpose: Get PnL contribution broken down by constituents.

**Algorithm:**
1. Call `GsPortfolioApi.get_attribution(self.position_source_id, start_date, end_date, currency, self.id)`
2. Return `pd.DataFrame(results)`

---

### PerformanceReport.get_brinson_attribution(self, benchmark=None, currency=None, include_interaction=False, aggregation_type=AttributionAggregationType.Arithmetic, aggregation_category=None, start_date=None, end_date=None, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get Brinson Attribution PnL analytics.

**Algorithm:**
1. Call `GsReportApi.get_brinson_attribution_results(...)` with `aggregation_category.value if aggregation_category else None`
2. Branch: `return_format == DATA_FRAME` ->
   a. Extract `results.get('results')` -> `rows`
   b. Build DataFrame, rename columns via `titleize`
   c. Return DataFrame
3. Else -> return raw results

---

### FactorRiskReport.__init__(self, risk_model_id=None, fx_hedged=True, benchmark_id=None, report_id=None, name=None, position_source_id=None, position_source_type=None, report_type=None, earliest_start_date=None, latest_end_date=None, latest_execution_time=None, status=ReportStatus.new, percentage_complete=None, tags=None, **kwargs) -> None
Purpose: Construct a FactorRiskReport with auto-derived position source type and report type.

**Algorithm:**
1. Branch: `position_source_id` is truthy and `position_source_type` is falsy ->
   - If ID starts with `'MP'` -> `PositionSourceType.Portfolio`; else -> `PositionSourceType.Asset`
2. Branch: `position_source_type` is truthy and `report_type` is falsy ->
   - If Portfolio -> `ReportType.Portfolio_Factor_Risk`; else -> `ReportType.Asset_Factor_Risk`
3. Call `super().__init__(...)` with `parameters=ReportParameters(risk_model=risk_model_id, fx_hedged=fx_hedged, benchmark=benchmark_id, tags=tags)`

---

### FactorRiskReport.get(cls, report_id: str, **kwargs) -> FactorRiskReport (classmethod)
Purpose: Fetch a factor risk report by ID.

**Algorithm:**
1. Call `GsReportApi.get_report(report_id)`
2. Return `cls.from_target(result)`

---

### FactorRiskReport.from_target(cls, report: TargetReport) -> FactorRiskReport (classmethod)
Purpose: Construct from a TargetReport with type validation.

**Algorithm:**
1. Branch: `report.type` not in `[Portfolio_Factor_Risk, Asset_Factor_Risk]` -> raise `MqValueError`
2. Construct and return `FactorRiskReport` from report fields

**Raises:** `MqValueError` if report type is wrong.

---

### FactorRiskReport.get_risk_model_id(self) -> str
Purpose: Return the risk model ID from parameters.

**Algorithm:**
1. Return `self.parameters.risk_model`

---

### FactorRiskReport.get_benchmark_id(self) -> str
Purpose: Return the benchmark ID from parameters.

**Algorithm:**
1. Return `self.parameters.benchmark`

---

### FactorRiskReport.get_results(self, mode=FactorRiskResultsMode.Portfolio, factors=None, factor_categories=None, start_date=None, end_date=None, currency=None, return_format=ReturnFormat.DATA_FRAME, unit=FactorRiskUnit.Notional) -> Union[Dict, pd.DataFrame]
Purpose: Get raw factor risk report results from the API.

**Algorithm:**
1. Call `GsReportApi.get_factor_risk_report_results(...)` with `mode.value`, `unit.value`
2. Branch: `return_format == DATA_FRAME` -> return `pd.DataFrame(results)`; else -> return raw results

---

### FactorRiskReport.get_view(self, factor=None, factor_category=None, start_date=None, end_date=None, currency=None, unit=FactorRiskUnit.Notional) -> Dict
Purpose: Get factor risk report results formatted as on the Marquee UI.

**Algorithm:**
1. Call and return `GsReportApi.get_factor_risk_report_view(...)` with `unit.value`

---

### FactorRiskReport.get_table(self, mode: FactorRiskTableMode, factors=None, factor_categories=None, date=None, start_date=None, end_date=None, unit=None, currency=None, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Get asset-level factor risk table results with smart date defaulting.

**Algorithm:**
1. Branch: both `start_date` and `end_date` are None ->
   - `start_date = self.latest_end_date - 1 month` if mode is Pnl, else `self.latest_end_date`
   - `end_date = self.latest_end_date`
2. Branch: only `start_date` is None ->
   - `start_date = end_date - 1 month` if mode is Pnl, else `end_date`
3. Branch: only `end_date` is None ->
   - `end_date = start_date` if mode is not Pnl, else `self.latest_end_date`
4. Call `GsReportApi.get_factor_risk_report_table(...)` with `unit.value if unit else None`
5. Branch: `'table' not in table` and `'warning' in table` -> raise `MqValueError(table.get('warning'))`
6. Branch: `return_format == DATA_FRAME` ->
   a. Extract `columnInfo` from `table.table.metadata`
   b. Prepend `['name', 'symbol', 'sector']` to first column group
   c. Extract `rows` from `table.table.rows`
   d. Call `_filter_table_by_factor_and_category(column_info, factors, factor_categories)` -> `sorted_columns`
   e. Deduplicate via `OrderedDict.fromkeys`
   f. Build DataFrame, reindex, set 'name' as index
   g. Return DataFrame
7. Else -> return raw table

**Raises:** `MqValueError` when API returns a warning instead of table data.

---

### FactorRiskReport.get_factor_pnl(self, mode=FactorRiskResultsMode.Portfolio, factor_names=None, factor_categories=None, start_date=None, end_date=None, currency=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical factor PnL with optional percent conversion using Carino smoothing.

**Algorithm:**
1. Set `factor_names_to_query = factor_names`
2. Branch: `unit == Percent` and `factor_names_to_query is not None` -> append `'Total'` to the list (mutates caller's list)
3. Call `self.get_results(...)` with JSON format; use `FactorRiskUnit.Notional` if `position_source_type == Portfolio`, else use the given `unit`
4. Branch: `unit == Notional` or `position_source_type != Portfolio` -> return `_format_multiple_factor_table(factor_data, 'pnl')`
5. Else (Percent for Portfolio):
   a. Branch: `factor_names is None` -> derive from `set` of factors in `factor_data`
   b. Fetch all reports for the portfolio, find matching `PerformanceReport` with same `tags`
   c. Call `format_aum_for_return_calculation(...)` -> `aum_df`
   d. Extract `total_data` from `factor_data` where factor == 'Total'
   e. Branch: `total_data` is empty -> re-fetch Total from API
   f. Loop over each factor name: filter factor data, find min date, call `get_factor_pnl_percent_for_single_factor(...)`
   g. Build DataFrame, reset index, rename column
   h. Filter: `result.loc[result['Date'] >= start_date.strftime("%Y-%m-&d")]`

Note: Line 1308 contains a bug -- format string is `"%Y-%m-&d"` instead of `"%Y-%m-%d"`.

---

### FactorRiskReport.get_factor_exposure(self, mode=FactorRiskResultsMode.Portfolio, factor_names=None, factor_categories=None, start_date=None, end_date=None, currency=None, unit=FactorRiskUnit.Notional) -> pd.DataFrame
Purpose: Get historical factor exposure.

**Algorithm:**
1. Call `self.get_results(...)` with JSON format
2. Return `_format_multiple_factor_table(factor_data, 'exposure')`

---

### FactorRiskReport.get_factor_proportion_of_risk(self, factor_names=None, factor_categories=None, start_date=None, end_date=None, currency=None) -> pd.DataFrame
Purpose: Get historical factor proportion of risk.

**Algorithm:**
1. Call `self.get_results(...)` with JSON format (no unit/mode params)
2. Return `_format_multiple_factor_table(factor_data, 'proportionOfRisk')`

---

### FactorRiskReport.get_annual_risk(self, factor_names=None, start_date=None, end_date=None, currency=None) -> pd.DataFrame
Purpose: Get historical annual risk.

**Algorithm:**
1. Call `self.get_results(...)` with JSON format
2. Return `_format_multiple_factor_table(factor_data, 'annualRisk')`

---

### FactorRiskReport.get_daily_risk(self, factor_names=None, start_date=None, end_date=None, currency=None) -> pd.DataFrame
Purpose: Get historical daily risk.

**Algorithm:**
1. Call `self.get_results(...)` with JSON format
2. Return `_format_multiple_factor_table(factor_data, 'dailyRisk')`

---

### FactorRiskReport.get_ex_ante_var(self, confidence_interval: float = 95.0, start_date=None, end_date=None, currency=None) -> pd.DataFrame
Purpose: Get ex-ante Value at Risk using the risk model's daily risk.

**Algorithm:**
1. Call `self.get_results(factors=['Total'], ...)` with JSON format
2. Compute `z_score = st.norm.ppf(confidence_interval / 100)`
3. Loop: for each data point, set `data['var'] = data['dailyRisk'] * z_score`
4. Return `_format_multiple_factor_table(factor_data, 'var')`

---

### ThematicReport.__init__(self, report_id=None, name=None, position_source_id=None, parameters=None, position_source_type=None, report_type=None, earliest_start_date=None, latest_end_date=None, latest_execution_time=None, status=ReportStatus.new, percentage_complete=None, **kwargs) -> None
Purpose: Construct a ThematicReport with auto-derived position source type and report type.

**Algorithm:**
1. Branch: `position_source_id` truthy and `position_source_type` falsy ->
   - If ID starts with `'MP'` -> `PositionSourceType.Portfolio`; else -> `PositionSourceType.Asset`
2. Branch: `position_source_type` truthy and `report_type` falsy ->
   - If Portfolio -> `ReportType.Portfolio_Thematic_Analytics`; else -> `ReportType.Asset_Thematic_Analytics`
3. Call `super().__init__(...)`

---

### ThematicReport.get(cls, report_id: str, **kwargs) -> ThematicReport (classmethod)
Purpose: Fetch a thematic report by ID.

**Algorithm:**
1. Call `GsReportApi.get_report(report_id)`, return `cls.from_target(result)`

---

### ThematicReport.from_target(cls, report: TargetReport) -> ThematicReport (classmethod)
Purpose: Construct from a TargetReport with type validation.

**Algorithm:**
1. Branch: `report.type` not in `[Portfolio_Thematic_Analytics, Asset_Thematic_Analytics]` -> raise `MqValueError`
2. Construct and return `ThematicReport` from report fields

**Raises:** `MqValueError` if report type is wrong.

---

### ThematicReport.get_thematic_data(self, start_date=None, end_date=None, basket_ids=None) -> pd.DataFrame
Purpose: Get thematic exposure and beta for a date range.

**Algorithm:**
1. Call `self._get_measures(["thematicExposure", "grossExposure"], ..., ReturnFormat.JSON)`
2. Loop: compute `thematicBeta = thematicExposure / grossExposure` for each result
3. Return DataFrame filtered to `['date', 'thematicExposure', 'thematicBeta']`

---

### ThematicReport.get_thematic_exposure(self, start_date=None, end_date=None, basket_ids=None) -> pd.DataFrame
Purpose: Get portfolio historical thematic exposure.

**Algorithm:**
1. Delegate to `self._get_measures(["thematicExposure"], ...)`

---

### ThematicReport.get_thematic_betas(self, start_date=None, end_date=None, basket_ids=None) -> pd.DataFrame
Purpose: Get portfolio historical thematic beta.

**Algorithm:**
1. Call `self._get_measures(["thematicExposure", "grossExposure"], ..., ReturnFormat.JSON)`
2. Loop: compute `thematicBeta`, pop `thematicExposure` and `grossExposure` from each result
3. Return `pd.DataFrame(results)`

---

### ThematicReport._get_measures(self, fields: List, start_date=None, end_date=None, basket_ids=None, return_format=ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Internal method to query thematic measures from the correct dataset.

**Algorithm:**
1. Build `where = {'reportId': self.id}`
2. Branch: `basket_ids` is truthy -> add `where['basketId'] = basket_ids`
3. Branch: `position_source_type == Portfolio` -> dataset `PTA_DATASET`; else -> dataset `ATA_DATASET`
4. Build `DataQuery`, call `GsDataApi.query_data(...)`
5. Branch: `return_format == DATA_FRAME` -> return DataFrame; else -> return raw results

---

### ThematicReport.get_all_thematic_exposures(self, start_date=None, end_date=None, basket_ids=None, regions=None) -> pd.DataFrame
Purpose: Get all thematic exposures via the Thematic API.

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.ALL_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

---

### ThematicReport.get_top_five_thematic_exposures(self, start_date=None, end_date=None, basket_ids=None, regions=None) -> pd.DataFrame
Purpose: Get top 5 thematic exposures.

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.TOP_FIVE_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

---

### ThematicReport.get_bottom_five_thematic_exposures(self, start_date=None, end_date=None, basket_ids=None, regions=None) -> pd.DataFrame
Purpose: Get bottom 5 thematic exposures.

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.BOTTOM_FIVE_THEMATIC_EXPOSURES]`
2. Return `flatten_results_into_df(results)`

---

### ThematicReport.get_thematic_breakdown(self, date: dt.date, basket_id: str) -> pd.DataFrame
Purpose: Get by-asset breakdown of thematic exposure on a date.

**Algorithm:**
1. Delegate to `get_thematic_breakdown_as_df(entity_id=self.position_source_id, date=date, basket_id=basket_id)`

---

### get_thematic_breakdown_as_df(entity_id: str, date: dt.date, basket_id: str) -> pd.DataFrame (module-level)
Purpose: Fetch thematic breakdown by asset and format as DataFrame.

**Algorithm:**
1. Call `GsThematicApi.get_thematics(...)` with `measures=[ThematicMeasure.THEMATIC_BREAKDOWN_BY_ASSET]`
2. Navigate nested dict: `results[0].get(THEMATIC_BREAKDOWN_BY_ASSET.value, [{}])[0].get(THEMATIC_BREAKDOWN_BY_ASSET.value, [])`
3. Loop: titleize all keys in each breakdown entry
4. Return `pd.DataFrame(formatted_breakdown)`

---

### flatten_results_into_df(results: List) -> pd.DataFrame (module-level)
Purpose: Flatten nested thematic results into a flat DataFrame.

**Algorithm:**
1. Loop over `results`: for each result, extract `date`
2. Inner loop: for each key in result whose value is a list, titleize all keys in each item, append to `all_results`
3. Build DataFrame, rename `'Basket'` column to `'Basket Id'`
4. Return (redundantly wraps in DataFrame again)

---

### get_pnl_percent(performance_report, pnl_df, field, start_date, end_date) -> pd.Series (module-level)
Purpose: Convert notional PnL to cumulative percent returns.

**Algorithm:**
1. Call `format_aum_for_return_calculation(performance_report, start_date, end_date)` -> `aum_df`
2. Check if first data point date matches `start_date`
3. Call `generate_daily_returns(aum_df, pnl_df, 'aum', field, is_first_data_point_on_start_date)`
4. Return `(return_series + 1).cumprod() - 1) * 100`

---

### get_factor_pnl_percent_for_single_factor(factor_data, total_data, aum_df, start_date) -> pd.Series (module-level)
Purpose: Compute PnL percent for a single factor using Carino smoothing.

**Algorithm:**
1. Call `format_factor_pnl_for_return_calculation(factor_data, total_data)` -> `pnl_df`
2. Check if first data point matches `start_date`
3. Call `generate_daily_returns(aum_df, pnl_df, 'aum', 'pnl', is_start_date_first_data_point)`

---

### format_factor_pnl_for_return_calculation(factor_data: list, total_data: list) -> pd.DataFrame (module-level)
Purpose: Merge factor PnL with total PnL into a single DataFrame.

**Algorithm:**
1. Build `pnl_df` from `factor_data` with columns `['date', 'pnl']`
2. Build `total_returns_df` from `total_data` with columns `['date', 'pnl']`, rename `pnl` to `totalPnl`
3. Inner merge on `date`
4. Return merged DataFrame

---

### format_aum_for_return_calculation(performance_report, start_date, end_date) -> pd.DataFrame (module-level)
Purpose: Fetch AUM and format as a two-column DataFrame.

**Algorithm:**
1. Call `performance_report.get_aum(start_date=prev_business_date(start_date), end_date=end_date)`
2. Return `pd.DataFrame(aum_as_dict.items(), columns=['date', 'aum'])`

---

### generate_daily_returns(aum_df, pnl_df, aum_col_key, pnl_col_key, is_start_date_first_data_point) -> pd.Series (module-level)
Purpose: Compute daily returns as PnL / lagged AUM, with optional Carino smoothing.

**Algorithm:**
1. Branch: `is_start_date_first_data_point` is True ->
   - Set `pnl_df.loc[0, pnl_col_key] = 0`
   - Branch: `'totalPnl' in pnl_df.columns` -> also set `pnl_df.loc[0, 'totalPnl'] = 0`
2. Outer merge `pnl_df` and `aum_df` on `'date'`
3. Set index to `'date'`, sort index
4. Forward-fill AUM column
5. Compute `df['return'] = pnl / aum.shift(1)`
6. Branch: `'totalPnl' in df.columns` ->
   a. Compute `totalPnl / aum.shift(1)`
   b. Fill NaN with 0
   c. Call `__smooth_percent_returns(df['return'], df['totalPnl'])` to apply Carino smoothing
   d. Overwrite `df['return']` with smoothed values
7. Return `pd.Series(df['return']).dropna()`

---

### __smooth_percent_returns(daily_factor_returns: np.array, daily_total_returns: np.array) -> np.array (module-level, name-mangled)
Purpose: Apply Carino log-linking formula for additive decomposition of geometric returns.

**Algorithm:**
1. Compute `total_return = prod(daily_total_returns + 1) - 1`
2. Branch: `total_return != 0` -> `log_scaling_factor = total_return / ln(1 + total_return)`
3. Branch: `total_return == 0` -> `log_scaling_factor = 1`
4. Compute `perturbation_factors = ln(1 + daily_total_returns) / daily_total_returns`
5. Replace NaN with 1 in perturbation factors
6. Return `cumsum(daily_factor_returns * log_scaling_factor * perturbation_factors * 100)`

---

### _format_multiple_factor_table(factor_data: List[Dict], key: str) -> pd.DataFrame (module-level)
Purpose: Pivot a list of factor data dicts into a DataFrame with one column per factor.

**Algorithm:**
1. Init `formatted_data = {}`
2. Loop over `factor_data`: group by `date`, accumulate `{factor: value}` for each date
3. Return `pd.DataFrame(formatted_data.values())`

---

### _filter_table_by_factor_and_category(column_info: Dict, factors: List, factor_categories: List) -> List (module-level)
Purpose: Build an ordered list of column names based on factor and category filters.

**Algorithm:**
1. Branch: both `factors` and `factor_categories` are None ->
   - Concatenate all `columns` from every column group in `column_info`
2. Else ->
   - Start with columns from `column_info[0]` and `column_info[1]` (name/symbol/sector + first data group)
   - Branch: `factors is not None` -> append factor names
   - Branch: `factor_categories is not None` -> for matching column groups, append their columns
3. Return `sorted_columns`

## State Mutation
- `Report.__id`: Set during `__init__`, updated by `save()` when creating a new report
- `Report.__position_source_id`, `__position_source_type`, `__type`: Updated by `set_position_source()`
- `PerformanceReport.get_position_net_weights`: Mutates the `asset_metadata_fields` default list argument in-place (appends `"netWeight"` on every call -- this is a bug with mutable default argument)
- `FactorRiskReport.get_factor_pnl`: Mutates the caller's `factor_names` list when `unit == Percent` by appending `'Total'`
- `FactorRiskReport.get_factor_pnl` (line 1302): Rebinds the `start_date` parameter inside the loop body, overwriting the caller-supplied value with a per-factor minimum date
- `generate_daily_returns`: Mutates `pnl_df` in place at index 0 when `is_start_date_first_data_point` is True
- `__smooth_percent_returns`: Pure function (no side effects)
- Portfolio state (remote): `set_aum_source` and `upload_custom_aum` mutate remote portfolio state via API
- Report state (remote): `save`, `delete`, `schedule`, `run`, `reschedule` mutate remote report/job state
- Thread safety: No thread safety mechanisms. API calls and shared mutable state (e.g., default list arguments) are not protected.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `ReportJobFuture.result` | Status is cancelled |
| `MqValueError` | `ReportJobFuture.result` | Status is error |
| `MqValueError` | `ReportJobFuture.result` | Status is not done |
| `MqValueError` | `ReportJobFuture.wait_for_completion` | Max retries exceeded with `error_on_timeout=True` |
| `MqValueError` | `Report.schedule` | Report ID or position source ID is None |
| `MqValueError` | `Report.schedule` | Non-Portfolio source with missing start/end dates |
| `MqValueError` | `Report.schedule` | Portfolio has no position dates |
| `MqValueError` | `Report.run` | Job takes too long (inner loop timeout) |
| `MqValueError` | `Report.run` | Report stuck in waiting status |
| `MqValueError` | `Report.run` | Report takes too long (outer loop exhausted) |
| `MqValueError` | `PerformanceReport.from_target` | Report type is not Portfolio_Performance_Analytics |
| `MqValueError` | `PerformanceReport.get_position_net_weights` | Any exception from `get_positions_data`, re-wrapped |
| `MqValueError` | `FactorRiskReport.from_target` | Report type not in [Portfolio_Factor_Risk, Asset_Factor_Risk] |
| `MqValueError` | `FactorRiskReport.get_table` | API response has 'warning' but no 'table' |
| `MqValueError` | `ThematicReport.from_target` | Report type not in [Portfolio_Thematic_Analytics, Asset_Thematic_Analytics] |

## Edge Cases
- `PerformanceReport.get_position_net_weights` uses a mutable default argument (`asset_metadata_fields: List[str] = ["id", "name", "ticker"]`). Repeated calls without explicitly passing this argument will keep appending `"netWeight"` to the shared default list, growing it unboundedly.
- `PerformanceReport.get_positions_data` has a `raise NotImplementedError` on line 832 that is dead code (unreachable because the `return` on line 823 always executes first).
- `PerformanceReport.get_positions_data` has `end=dt.date.today()` as a default argument, which is evaluated once at module import time, not on each call.
- `PerformanceReport.get_aum` has no fallback else branch. If `aum_source` does not match any known `RiskAumSource` value, the method implicitly returns `None`.
- `FactorRiskReport.get_factor_pnl` line 1308 has a format string bug: `"%Y-%m-&d"` instead of `"%Y-%m-%d"`, which will produce malformed date strings and likely filter out all rows.
- `FactorRiskReport.get_factor_pnl` mutates the caller's `factor_names` list by appending `'Total'` when `unit == Percent`.
- `FactorRiskReport.get_factor_pnl` overwrites the `start_date` local variable inside the loop (line 1302), so subsequent iterations use the previous factor's min date rather than the original parameter.
- `ThematicReport.get_thematic_data` and `get_thematic_betas` will raise `ZeroDivisionError` if `grossExposure` is 0 for any result.
- `get_thematic_breakdown_as_df` accesses `results[0]` without checking if `results` is empty, which would raise `IndexError`.
- `flatten_results_into_df` wraps the DataFrame in `pd.DataFrame` twice (redundant but harmless).
- `Report.run` catches `IndexError` from `get_most_recent_job` (when no jobs exist yet after scheduling) and retries up to 5 times with no sleep between retries.
- `__smooth_percent_returns` handles `total_return == 0` with a fallback `log_scaling_factor = 1`, and replaces NaN perturbation factors with 1 via `np.nan_to_num`.
- `Report.__init__` coercion: passing a string that is not a valid enum member for `position_source_type`, `report_type`, or `status` will raise a `ValueError` from the respective enum constructor.

## Bugs Found
- Line 832: `raise NotImplementedError` is dead code after a `return` statement (OPEN)
- Line 616: Mutable default argument `asset_metadata_fields=["id", "name", "ticker"]` is mutated in-place, causing accumulation across calls (OPEN)
- Line 818: `end=dt.date.today()` default is evaluated at import time, not call time (OPEN)
- Line 1258: `factor_names_to_query.append('Total')` mutates the caller's list since `factor_names_to_query = factor_names` is a reference, not a copy (OPEN)
- Line 1302: `start_date` is rebound inside the factor loop, overwriting the original parameter value (OPEN)
- Line 1308: Format string `"%Y-%m-&d"` should be `"%Y-%m-%d"` -- the `&` is a typo for `%` (OPEN)

## Coverage Notes
- Branch count: ~85 (estimated from all conditional paths including if/elif/else chains, isinstance checks, ternary expressions, loop conditions, try/except blocks, and early returns)
- Key branching areas:
  - `Report.__init__`: 6 branches (3 coercion checks x 2 paths each)
  - `Report.schedule`: ~10 branches (None checks, position source type, backcast flag, empty positions)
  - `Report.run`: ~8 branches (try/except, is_async, done check, inner/outer loop exhaustion, waiting status)
  - `ReportJobFuture.result`: 5 branches (cancelled, error, not done, factor risk type, performance type, else)
  - `ReportJobFuture.wait_for_completion`: 3 branches (loop exit, error_on_timeout true/false)
  - `PerformanceReport.get_aum`: 5 branches (Custom_AUM, Long, Short, Gross, Net)
  - `PerformanceReport.get_portfolio_constituents`: 4 branches (empty, prefer_rebalance, return_format)
  - `FactorRiskReport.__init__`: 4 branches (auto-derive position_source_type and report_type)
  - `FactorRiskReport.get_table`: ~8 branches (date defaulting, warning, return_format)
  - `FactorRiskReport.get_factor_pnl`: ~10 branches (unit, position_source_type, factor_names None, total_data empty)
  - `ThematicReport._get_measures`: 3 branches (basket_ids, dataset selection, return_format)
  - `_filter_table_by_factor_and_category`: 4 branches (both None, factors, factor_categories)
  - `generate_daily_returns`: 4 branches (is_start_date, totalPnl in columns x 2)
  - `__smooth_percent_returns`: 1 branch (total_return == 0)
- Pragmas: none marked `pragma: no cover`
