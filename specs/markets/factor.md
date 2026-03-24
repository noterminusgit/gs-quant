# factor.py

## Summary
Defines the `Factor` class representing a risk model factor, with methods for retrieving factor-level statistics (covariance, variance, volatility, correlation, returns, intraday returns, mimicking portfolios) from the GS Factor Risk Model API. Also provides the `ReturnFormat` enum for toggling between DataFrame and JSON output.

## Dependencies
- Internal: `gs_quant.api.gs.risk_models` (GsFactorRiskModelApi, RiskModelDataMeasure, RiskModelDataAssetsRequest, IntradayFactorDataSource), `gs_quant.data.core` (DataContext), `gs_quant.datetime` (date, time), `gs_quant.models.risk_model_utils` (get_covariance_matrix_dataframe, build_factor_volatility_dataframe, build_factor_data_map, build_pfp_data_dataframe), `gs_quant.target.risk_models` (RiskModelUniverseIdentifierRequest)
- External: `datetime`, `math` (sqrt), `enum` (auto, Enum), `typing` (Dict, Union), `numpy` (np), `pandas` (pd)

## Type Definitions

### ReturnFormat (Enum)
Inherits: `Enum`

| Value | Raw | Description |
|-------|-----|-------------|
| JSON | `auto()` | Return data as dictionary |
| DATA_FRAME | `auto()` | Return data as pandas DataFrame |

### Factor (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __risk_model_id | `str` | required | Risk model identifier |
| __id | `str` | required | Factor identifier |
| __type | `str` | required | Factor type (e.g., Style, Industry) |
| __name | `str` | `None` | Human-readable factor name |
| __category | `str` | `None` | Factor category |
| __tooltip | `str` | `None` | Short tooltip description |
| __description | `str` | `None` | Full description |
| __glossary_description | `str` | `None` | Glossary description |

## Enums and Constants

### ReturnFormat(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| JSON | `auto()` (1) | Dictionary output format |
| DATA_FRAME | `auto()` (2) | DataFrame output format |

### Module Constants
None.

## Functions/Methods

### Factor.__init__(self, risk_model_id: str, id_: str, type_: str, name: str = None, category: str = None, tooltip: str = None, description: str = None, glossary_description: str = None)
Purpose: Initialize a Factor with its risk model association and metadata.

**Algorithm:**
1. Store all parameters in private fields

### Factor.id (property) -> str
Purpose: Get factor ID.

### Factor.name (property) -> str
Purpose: Get factor name.

### Factor.type (property) -> str
Purpose: Get factor type.

### Factor.category (property) -> str
Purpose: Get factor category.

### Factor.tooltip (property) -> str
Purpose: Get factor tooltip.

### Factor.description (property) -> str
Purpose: Get factor description.

### Factor.glossary_description (property) -> str
Purpose: Get factor glossary description.

### Factor.risk_model_id (property) -> str
Purpose: Get risk model ID.

### Factor.covariance(self, factor: Factor, start_date: date = DataContext.current.start_date, end_date: date = DataContext.current.end_date, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve date-indexed covariance values between this factor and another over a date range.

**Algorithm:**
1. Call `GsFactorRiskModelApi.get_risk_model_data` with `Covariance_Matrix`, `Factor_Name`, `Factor_Id` measures, passing `factors=list({self.name, factor.name})` (deduped set)
2. Extract `'results'` from response
3. Call `get_covariance_matrix_dataframe` on raw data
4. Stack and index-slice at `[self.name, factor.name]`, multiply by 252 (annualization)
5. Branch: `format == ReturnFormat.JSON` -> return `.to_dict()`
6. Branch: else -> return `.to_frame(name="covariance")`

### Factor.variance(self, start_date: date, end_date: date, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve date-indexed variance values (self-covariance).

**Algorithm:**
1. Call `self.covariance(self, ...)` with `ReturnFormat.DATA_FRAME`, rename column to `"variance"`
2. Branch: `format == ReturnFormat.JSON` -> return `.squeeze().to_dict()`
3. Branch: else -> return DataFrame

### Factor.volatility(self, start_date: date, end_date: date, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve date-indexed volatility values.

**Algorithm:**
1. Call `GsFactorRiskModelApi.get_risk_model_data` with `Factor_Volatility`, `Factor_Id`, `Factor_Name` measures
2. Extract `'results'`
3. Call `build_factor_volatility_dataframe` with `annualise=True`, multiply by `math.sqrt(252)`
4. Branch: `format == ReturnFormat.JSON` -> return `.squeeze(axis=1).to_dict()`
5. Branch: else -> return DataFrame

### Factor.correlation(self, other_factor, start_date: date, end_date: date, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve date-indexed correlation values between this factor and another.

**Algorithm:**
1. Call `GsFactorRiskModelApi.get_risk_model_data` with `Covariance_Matrix`, `Factor_Name`, `Factor_Id` measures, passing both factor names
2. Build covariance matrix via `get_covariance_matrix_dataframe`, multiply by 252
3. Extract numerator: `cov[self.name, other_factor.name]`
4. Compute denominator: `sqrt(cov[self, self] * cov[other, other])`
5. Divide to get correlation
6. Branch: `format == ReturnFormat.JSON` -> return `.to_dict()`
7. Branch: else -> return `.to_frame(name="correlation")`

### Factor.returns(self, start_date: date, end_date: date, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve date-indexed factor return values.

**Algorithm:**
1. Call `GsFactorRiskModelApi.get_risk_model_data` with `Factor_Return`, `Factor_Name`, `Factor_Id` measures
2. Build factor data map via `build_factor_data_map`
3. Rename column from `self.name` to `'return'`, reset axis name
4. Branch: `format == ReturnFormat.JSON` -> return `.squeeze().to_dict()`
5. Branch: else -> return DataFrame

### Factor.intraday_returns(self, start_time: time, end_time: time, data_source: Union[IntradayFactorDataSource, str] = IntradayFactorDataSource.GS_FMP, format: ReturnFormat = ReturnFormat.DATA_FRAME) -> Union[Dict, pd.DataFrame]
Purpose: Retrieve timestamp-indexed intraday factor returns, batching requests in ~24h intervals.

**Algorithm:**
1. Set `max_interval` to 23 hours, 59 minutes, 59 seconds
2. Loop while `current_start < end_time`:
   a. Compute `current_end = min(current_start + max_interval, end_time)`
   b. Call `GsFactorRiskModelApi.get_risk_model_factor_data_intraday` for the batch
   c. Extend `all_data` with results
   d. Advance `current_start = current_end + 1 second`
3. Create DataFrame from `all_data`
4. Try: set index to `'time'`
5. Branch: `KeyError` -> replace with empty DataFrame
6. Drop columns `factorCategory`, `factor`, `factorId` (errors='ignore')
7. Branch: `format == ReturnFormat.JSON` -> return `.squeeze().to_dict()`
8. Branch: else -> return DataFrame

### Factor.mimicking_portfolio(self, start_date: date, end_date: date, assets: RiskModelDataAssetsRequest = ..., format: ReturnFormat = ReturnFormat.DATA_FRAME)
Purpose: Retrieve a timeseries of factor mimicking portfolios.

**Algorithm:**
1. Call `GsFactorRiskModelApi.get_risk_model_data` with `Factor_Portfolios`, `Factor_Id`, `Factor_Name` measures
2. Extract `'results'`
3. Call `build_pfp_data_dataframe` on results
4. Reset index, pivot with `index="date"`, `columns="identifier"`, `values=self.name`
5. Branch: `format.value == ReturnFormat.JSON.value` -> return `.to_dict(orient='index')`
6. Branch: else -> return DataFrame

## State Mutation
- All fields are read-only (no setters defined)
- No module-level mutable state
- Default parameter `DataContext.current.start_date` / `DataContext.current.end_date` are evaluated at call time via default argument binding at class definition time (potential gotcha: they are evaluated once at import time, not per-call)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `intraday_returns` | Caught internally when `'time'` column missing from empty response; handled by replacing with empty DataFrame |

## Edge Cases
- `covariance` uses `list({self.name, factor.name})` which deduplicates when computing self-covariance (variance); the set may contain 1 or 2 elements
- `intraday_returns` handles empty API responses gracefully by catching `KeyError` on `set_index`
- `intraday_returns` drops specific columns with `errors='ignore'` to handle partial schemas
- `mimicking_portfolio` compares `format.value == ReturnFormat.JSON.value` (comparing raw enum values) instead of `format == ReturnFormat.JSON` used elsewhere -- this is functionally equivalent but stylistically inconsistent
- Default mutable argument `assets` in `mimicking_portfolio` is constructed once at class definition; however since `RiskModelDataAssetsRequest` is likely immutable this is safe
- DataContext default arguments `DataContext.current.start_date` / `DataContext.current.end_date` are bound at import time, not at call time

## Coverage Notes
- Branch count: ~14
- Key branches: format JSON vs DATA_FRAME in 7 methods (14 branches total), `intraday_returns` try/except KeyError (2), `intraday_returns` while loop (entry/exit), `mimicking_portfolio` format check (2)
- Pragmas: none
