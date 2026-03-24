# risk_model_utils.py

## Summary
Utility functions for the risk model module, providing data transformation (building DataFrames from API results), batching logic for large risk model data uploads, and retry logic for API calls. These functions are consumed primarily by `risk_model.py` to handle data formatting and upload orchestration.

## Dependencies
- Internal: `gs_quant.api.gs.data` (GsDataApi), `gs_quant.api.gs.risk_models` (GsFactorRiskModelApi), `gs_quant.errors` (MqRequestError), `gs_quant.target.risk_models` (RiskModelData, RiskModelType as Type, RiskModelDataMeasure as Measure)
- External: `datetime` (dt), `logging`, `math`, `pydash`, `random`, `time` (sleep), `typing` (List, Union), `pandas` (pd)

## Type Definitions

None (module contains only functions).

## Enums and Constants

None.

## Functions/Methods

### _map_measure_to_field_name(measure: Measure) -> str
Purpose: Map a RiskModelDataMeasure enum to its corresponding JSON field name string.

**Algorithm:**
1. Define dict mapping all Measure values to their camelCase field name strings (37 entries)
2. Return `measure_to_field.get(measure, '')` -- empty string if not found

### build_factor_id_to_name_map(results: List) -> dict
Purpose: Build a mapping from factor IDs to factor names from API results.

**Algorithm:**
1. Initialize empty dict
2. For each row in results:
   - For each factor in row's `factorData` list:
     - Extract `factorId`
     - Branch: if factorId not already in dict -> add mapping factorId -> factorName
3. Return dict

### build_asset_data_map(results: List, requested_universe: tuple, requested_measure: Measure, factor_map: dict) -> dict
Purpose: Build a nested dict mapping asset identifiers to date-keyed measure values.

**Algorithm:**
1. Branch: if results is empty -> return empty dict
2. Get data_field via `_map_measure_to_field_name(requested_measure)`
3. Branch: if requested_universe is empty/falsy -> pull universe from `results[0].assetData.universe` via pydash; else -> use list(requested_universe)
4. For each asset in universe:
   - Initialize date_list dict
   - For each row in results:
     - Branch: if asset in row's universe:
       - Find index i of asset in universe
       - Branch: if data_field is 'factorExposure' -> extract exposures dict at index i, remap factor IDs to names using factor_map
       - Else -> extract scalar value at index i
       - Store in date_list keyed by row date
   - Store date_list in data_map keyed by asset
5. Return data_map

### build_factor_data_map(results: List, identifier: str, risk_model_id: str, requested_measure: Measure, factors: List[str] = []) -> Union[dict, pd.DataFrame]
Purpose: Build a pivoted DataFrame of factor data (rows=dates, columns=factor identifiers).

**Algorithm:**
1. Get field_name via `_map_measure_to_field_name(requested_measure)`
2. Branch: if field_name is empty -> raise `NotImplementedError`
3. For each row in results:
   - Extract date and factorData
   - For each factor_map in factorData:
     - Append dict with date, identifier value, and field value
4. Create DataFrame from data_list
5. Pivot: index="date", columns=identifier, values=field_name
6. Branch: if factors list is non-empty:
   - Compute missing_factors = set(factors) - set(df.columns)
   - Branch: if missing_factors -> raise `ValueError` with details
   - Filter DataFrame to only requested factors
7. Return DataFrame

**Raises:** `NotImplementedError` when measure is not supported; `ValueError` when requested factors not found

### build_pfp_data_dataframe(results: List, return_df: bool = True, get_factors_by_name: bool = True) -> Union[pd.DataFrame, list]
Purpose: Build factor portfolio (PFP) data as DataFrame or list of dicts.

**Algorithm:**
1. Create DataFrame from results with date and factorData columns
2. Explode factorData into individual rows
3. Extract factorId and factorName from each factorData entry
4. Drop factorData column, set index to date
5. Initialize pfp_list
6. Set identifier column name: "assetId" if not get_factors_by_name, else "identifier"
7. For each row in results:
   - Get factor_map_on_date from the exploded DataFrame for this date
   - Branch: if factor_map_on_date is a Series (single factor) -> convert to single-row DataFrame
   - Build factor_id_to_name_map from factorId/factorName columns
   - Build pfp_map dict with universe as identifier column
   - For each factor in factorPortfolios portfolio:
     - Branch: if get_factors_by_name -> key by factor name; else -> key by `factorId: {id}`
     - Store weights
   - Set date in pfp_map
   - Branch: if not return_df -> append pfp_map dict to pfp_list
   - Else -> create DataFrame from pfp_map, append to pfp_list
8. Branch: if not return_df -> return pfp_list (list of dicts)
9. Concatenate all DataFrames; if empty -> return empty DataFrame
10. Set index to "date" and return

### get_optional_data_as_dataframe(results: List, optional_data_key: str) -> pd.DataFrame
Purpose: Convert optional risk model data (e.g., ISC, currency rates) into a multi-indexed DataFrame.

**Algorithm:**
1. For each row in results:
   - Create DataFrame from row's optional_data_key data
   - Append to cov_list
   - Append row's date to date_list
2. Branch: if cov_list non-empty -> concatenate with date keys; else -> return empty DataFrame
3. Return concatenated DataFrame

### get_covariance_matrix_dataframe(results: List[dict], covariance_matrix_key: str = 'covarianceMatrix') -> pd.DataFrame
Purpose: Convert covariance matrix data into a labeled multi-indexed DataFrame.

**Algorithm:**
1. For each row in results:
   - Create DataFrame from row's covariance_matrix_key data
   - Extract factor_names from row's factorData
   - Set DataFrame columns and index to factor_names
   - Append to cov_list
   - Append row's date to date_list
2. Branch: if cov_list non-empty -> concatenate with date keys; else -> return empty DataFrame
3. Return concatenated DataFrame

### build_factor_volatility_dataframe(results: List, group_by_name: bool, factors: List[str]) -> pd.DataFrame
Purpose: Build a DataFrame of factor volatility data.

**Algorithm:**
1. For each row in results: collect dates and factorVolatility data
2. Create DataFrame with data rows and date index
3. Branch: if group_by_name -> rename columns using build_factor_id_to_name_map(results)
4. Branch: if factors list is non-empty:
   - Compute missing_factors = set(factors) - set(df.columns)
   - Branch: if missing_factors -> raise `ValueError`
   - Else -> return df[factors]
5. Return df

**Raises:** `ValueError` when requested factors not found

### get_closest_date_index(date: dt.date, dates: List[str], direction: str) -> int
Purpose: Find the index of the closest date in a sorted date list, searching up to 50 days in a direction.

**Algorithm:**
1. Loop for i in range(50) (days offset):
   - Loop for index in range(len(dates)):
     - Branch: if direction is 'before' -> compute next_date = date - i days
     - Else -> compute next_date = date + i days
     - Branch: if next_date == dates[index] -> return index
2. Return -1 if no match found within 50 days

### divide_request(data, n)
Purpose: Generator that yields chunks of data of size n.

**Algorithm:**
1. For i in range(0, len(data), n):
   - Yield data[i : i + n]

### batch_and_upload_partial_data_use_target_universe_size(model_id: str, data: dict, max_asset_size: int)
Purpose: Upload risk model data for one day, batching by asset data size (legacy version using target universe size).

**Algorithm:**
1. Extract date from data
2. Call `_upload_factor_data_if_present(model_id, data, date)`
3. Sleep random 3-7 seconds
4. Call `_batch_data_if_present(model_id, data, max_asset_size, date)`

### _upload_factor_data_if_present(model_id: str, data: dict, date: str, **kwargs)
Purpose: Upload factor data (and optionally covariance matrices) if present.

**Algorithm:**
1. Get aws_upload from kwargs (default None)
2. Branch: if data has 'factorData':
   - Build factor_data dict with date and factorData
   - Branch: if data has 'covarianceMatrix' -> add to factor_data
   - Branch: if data has 'unadjustedCovarianceMatrix' AND aws_upload -> add to factor_data
   - Branch: if data has 'preVRACovarianceMatrix' AND aws_upload -> add to factor_data
   - Log info and call `_repeat_try_catch_request(GsFactorRiskModelApi.upload_risk_model_data, ..., partial_upload=True, **kwargs)`

### _batch_data_if_present(model_id: str, data, max_asset_size, date)
Purpose: Batch and upload asset data, ISC, and factor portfolios if present.

**Algorithm:**
1. Branch: if data has 'assetData':
   - Call `_batch_input_data({'assetData': data['assetData']}, max_asset_size)` to get batched list and target_size
   - For each batch: upload with `_repeat_try_catch_request`, sleep random 3-7 seconds
2. Branch: if 'issuerSpecificCovariance' or 'factorPortfolios' in data keys:
   - For each optional_key in ['issuerSpecificCovariance', 'factorPortfolios']:
     - Branch: if data has that key:
       - Batch with max_asset_size // 2 (due to data structure)
       - Log info and upload each batch with `_repeat_try_catch_request`, sleep random 3-7 seconds

### only_factor_data_is_present(model_type: Type, data: dict) -> bool
Purpose: Check if the data dict contains only factor data (no asset data).

**Algorithm:**
1. Branch: if model_type is Macro or Thematic:
   - Branch: if data has exactly 2 keys and 'factorData' is one of them -> return True
2. Else (Factor model type):
   - Branch: if data has exactly 3 keys and has both 'factorData' and 'covarianceMatrix' -> return True
3. Return False

### batch_and_upload_partial_data(model_id: str, data: dict, max_asset_size: int, **kwargs)
Purpose: Upload risk model data for one day with batching (v2 version using kwargs).

**Algorithm:**
1. Extract date from data
2. Call `_upload_factor_data_if_present(model_id, data, date, **kwargs)`
3. Sleep random 3-7 seconds
4. Branch: if data has 'currencyRatesData':
   - Upload via `_repeat_try_catch_request` with partial_upload=True, final_upload=True
   - Sleep random 3-7 seconds
5. For each risk_model_data_type in ["assetData", "issuerSpecificCovariance", "factorPortfolios"]:
   - Call `_repeat_try_catch_request(_batch_data_v2, ...)` with all params
   - Sleep random 3-7 seconds

### _batch_data_v2(model_id: str, data: dict, data_type: str, max_asset_size: int, date: Union[str, dt.date], **kwargs)
Purpose: Batch and upload a single data type (asset, ISC, or PFP) for one day.

**Algorithm:**
1. Branch: if data is truthy:
   - Branch: if data_type is ISC or PFP -> halve max_asset_size
   - Call `_batch_input_data({data_type: data}, max_asset_size)` to get data_list
   - For each chunk (index i):
     - Set final_upload = True if i is last chunk
     - Call `GsFactorRiskModelApi.upload_risk_model_data(...)` with partial_upload=True, final_upload, **kwargs
     - Log result

### batch_and_upload_coverage_data(date: dt.date, gsid_list: list, model_id: str, batch_size: int)
Purpose: Upload asset coverage data in batches.

**Algorithm:**
1. Format current time as update_time string
2. Build request_array: one dict per unique gsid with date, gsid, riskModel, updateTime
3. Log info with count
4. Split into batches of batch_size via `divide_request()`
5. Log batch count
6. For each batch: call `_repeat_try_catch_request(GsDataApi.upload_data, data=data, dataset_id="RISK_MODEL_ASSET_COVERAGE")`

### upload_model_data(model_id: str, data: dict, **kwargs)
Purpose: Upload risk model data with retry logic.

**Algorithm:**
1. Call `_repeat_try_catch_request(GsFactorRiskModelApi.upload_risk_model_data, model_id=model_id, model_data=data, **kwargs)`

### risk_model_data_to_json(risk_model_data: RiskModelData) -> dict
Purpose: Convert RiskModelData object to a JSON dict.

**Algorithm:**
1. Convert risk_model_data to JSON via `.to_json()`
2. Convert nested 'assetData' to JSON via `.to_json()`
3. Branch: if 'factorPortfolios' present:
   - Convert to JSON
   - Convert each portfolio entry in 'portfolio' list to JSON
4. Branch: if 'issuerSpecificCovariance' present:
   - Convert to JSON
5. Return dict

### get_universe_size(data_to_split: dict) -> int
Purpose: Determine the universe size from any chunk of risk model data.

**Algorithm:**
1. Branch: if 'assetData' in keys -> return len of assetData.universe
2. Else -> iterate all values:
   - Branch: if value is a string -> skip (continue)
   - Branch: if value has 'universe' key -> return len of universe
   - Branch: if value has 'universeId1' key -> return len of universeId1
3. Raise `ValueError` if no universe found

**Raises:** `ValueError` when no universe found in data

### _batch_input_data(input_data: dict, max_asset_size: int) -> tuple
Purpose: Split input data into batches based on max asset size.

**Algorithm:**
1. Get data_key from first key in input_data
2. Get target_universe_size via `get_universe_size(input_data)`
3. Compute split_num = ceil(target_universe_size / max_asset_size), minimum 1
4. Compute split_idx = ceil(target_universe_size / split_num)
5. For each batch i in range(split_num):
   - Branch: if data_key is 'assetData' -> call `_batch_asset_input(...)`
   - Branch: if data_key is 'factorPortfolios' -> call `_batch_pfp_input(...)`
   - Else -> call `_batch_isc_input(...)`
   - Append result to batched_data_list
6. Return (batched_data_list, target_universe_size)

### _batch_asset_input(input_data: dict, i: int, split_idx: int, split_num: int, target_universe_size: int) -> dict
Purpose: Extract a slice of asset data for batching.

**Algorithm:**
1. Compute end_idx: if last batch -> target_universe_size + 1; else -> (i+1) * split_idx
2. Slice required fields: universe, specificRisk, factorExposure at [i*split_idx : end_idx]
3. Get list of optional fields (all keys minus required three)
4. For each optional field: if present -> slice and add to subset
5. Return subset dict

### _batch_pfp_input(input_data: dict, i: int, split_idx: int, split_num: int, target_universe_size: int) -> dict
Purpose: Extract a slice of factor portfolio data for batching.

**Algorithm:**
1. Compute end_idx same as _batch_asset_input
2. Slice universe at [i*split_idx : end_idx]
3. For each portfolio entry: extract factorId, slice weights at same range
4. Return dict with universe and portfolio slices

### _batch_isc_input(input_data: dict, i: int, split_idx: int, split_num: int, target_universe_size: int) -> dict
Purpose: Extract a slice of issuer specific covariance data for batching.

**Algorithm:**
1. Compute end_idx same as _batch_asset_input
2. Slice universeId1, universeId2, covariance at [i*split_idx : end_idx]
3. Return dict with sliced fields

### _repeat_try_catch_request(input_function, number_retries: int = 5, return_result: bool = False, verbose: bool = True, **kwargs)
Purpose: Retry wrapper for API calls with exponential backoff.

**Algorithm:**
1. Initialize t = 3.0, errors = []
2. For i in range(number_retries):
   - Try:
     - Call `input_function(**kwargs)`
     - Branch: if result is truthy:
       - Branch: if return_result -> return result
       - Else -> log result
     - Clear errors list, break
   - Catch `MqRequestError` as e:
     - Append e to errors
     - Branch: if e.status < 500 and e.status != 429 -> re-raise immediately (client error)
     - Branch: if not last retry -> compute sleep_time = 2.2^t, increment t, log warning if verbose, sleep
     - Else -> log "Maximum retries" warning if verbose
   - Catch any other `Exception`:
     - Append to errors
     - Branch: if not last retry -> exponential backoff same as above
     - Else -> log "Maximum retries" warning if verbose
3. Branch: if errors non-empty -> raise last error via `errors.pop()`

**Raises:** `MqRequestError` when client error (status < 500, not 429); any exception after retries exhausted

## State Mutation
- No module-level mutable state
- All functions are stateless (operate on inputs and return results)
- Side effects: API calls (GsFactorRiskModelApi, GsDataApi), logging, sleep calls
- Thread safety: Functions use `random.uniform()` for sleep times and `sleep()` which are safe for concurrent use, but the upload functions are not designed for concurrent invocation on the same model

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `build_factor_data_map` | Measure not yet supported (empty field_name) |
| `ValueError` | `build_factor_data_map` | Requested factors not found in risk model |
| `ValueError` | `build_factor_volatility_dataframe` | Requested factors not found in data |
| `ValueError` | `get_universe_size` | No universe found in data |
| `MqRequestError` | `_repeat_try_catch_request` | Client error (status < 500, not 429) or retries exhausted |
| `Exception` | `_repeat_try_catch_request` | Unknown exception after retries exhausted |

## Edge Cases
- `build_asset_data_map`: Returns empty dict if results is empty; when requested_universe is empty, pulls universe from first result via pydash
- `build_asset_data_map`: For 'factorExposure' measure, remaps factor IDs to names using factor_map; for other measures, extracts scalar values
- `build_pfp_data_dataframe`: Handles single-factor case where factor_map_on_date is a Series (converts to single-row DataFrame via `.to_frame().T`)
- `build_pfp_data_dataframe`: When return_df=False, returns list of dicts instead of DataFrame
- `get_optional_data_as_dataframe` / `get_covariance_matrix_dataframe`: Return empty DataFrame if no data
- `get_closest_date_index`: Searches up to 50 days in either direction; returns -1 if no match found (potential for index-out-of-bounds if caller doesn't check)
- `_batch_input_data`: split_num has a fallback of 1 via `if math.ceil(...) else 1`, but math.ceil always returns >= 1 for positive inputs so the else branch is effectively dead code for valid inputs (only triggers for universe_size=0)
- `_batch_asset_input`: Last batch uses `target_universe_size + 1` as end_idx to ensure all elements are included
- `_repeat_try_catch_request`: Distinguishes between 429 (rate limit, retry) and other 4xx (client error, fail fast) vs 5xx (server error, retry)
- `_upload_factor_data_if_present`: unadjustedCovarianceMatrix and preVRACovarianceMatrix only included when aws_upload is truthy
- `only_factor_data_is_present`: Different key count expectations for Macro/Thematic (2 keys: date + factorData) vs Factor models (3 keys: date + factorData + covarianceMatrix)
- `risk_model_data_to_json`: Only processes factorPortfolios and issuerSpecificCovariance if they exist in the dict

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~60+
- Key branches: _repeat_try_catch_request has 8+ branches (success, MqRequestError client/server/429, unknown exception, retry vs final, return_result, verbose); _batch_input_data has 3-way branch on data_key; get_universe_size has 4 branches; build_asset_data_map has factorExposure vs scalar branch; only_factor_data_is_present has model-type dependent logic
- Missing branches: Dead code in `_batch_input_data` (else 1 branch when ceil is 0); get_closest_date_index returning -1 may be untested
- Pragmas: None
