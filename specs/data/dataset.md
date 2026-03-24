# dataset.py

## Summary
Provides the core `Dataset` class for querying, uploading, and managing data from Goldman Sachs Marquee data services. Also includes `PTPDataset` (a subclass for PlotTool Pro datasets with auto-sync/plot capabilities) and `MarqueeDataIngestionLibrary` (for creating native Snowflake datasets and uploading data). The module acts as the primary user-facing abstraction over the lower-level `DataApi` provider layer.

## Dependencies
- Internal:
  - `gs_quant.api.data` (`DataApi` -- abstract provider interface)
  - `gs_quant.api.gs.data` (`GsDataApi` -- lazily imported default provider)
  - `gs_quant.api.gs.users` (`GsUsersApi` -- `.get_current_user_info()`, `.get_current_app_managers()`)
  - `gs_quant.data.fields` (`Fields` -- enum of field names)
  - `gs_quant.data.utilities` (`Utilities` -- bulk-download helpers)
  - `gs_quant.errors` (`MqValueError`)
  - `gs_quant.session` (`GsSession`)
  - `gs_quant.target.data` (`DataSetEntity`, `DataSetParameters`, `DataSetDimensions`, `FieldColumnPair`, `DataSetFieldEntity`, `DBConfig`, `DataSetType`)
- External:
  - `datetime` (dt.date, dt.datetime, dt.time, dt.timedelta, dt.timezone)
  - `re` (regex matching/substitution)
  - `webbrowser` (open URLs)
  - `enum` (`Enum`)
  - `typing` (`Iterable`, `Optional`, `Union`, `List`, `Dict`, `Callable`)
  - `urllib.parse` (`quote`)
  - `functools` (`partial`)
  - `inflection` (`underscore`)
  - `numpy` (`np.number`)
  - `pandas` (`pd.DataFrame`, `pd.Series`, `pd.DatetimeIndex`, `pd.api.types`)
  - `pydash` (`camel_case`, `snake_case`)

## Type Definitions

### InvalidInputException (class)
Inherits: `Exception`

Plain exception with no additional fields. Used by `MarqueeDataIngestionLibrary` for validation failures.

### Dataset (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` (private) | `str` | *(required)* | The dataset identifier string, extracted from `dataset_id` param |
| `__provider` (private) | `Optional[DataApi]` | `None` | Explicit data provider; if `None`, falls back to `GsDataApi` |

#### Nested Enum: Dataset.Vendor (Enum)
Empty base enum. Serves as the parent for vendor-specific dataset enums.

#### Nested Enum: Dataset.GS (Vendor)
| Value | Raw String |
|-------|-----------|
| `HOLIDAY` | `"HOLIDAY"` |
| `HOLIDAY_CURRENCY` | `"HOLIDAY_CURRENCY"` |
| `EDRVOL_PERCENT_INTRADAY` | `"EDRVOL_PERCENT_INTRADAY"` |
| `EDRVOL_PERCENT_STANDARD` | `"EDRVOL_PERCENT_STANDARD"` |
| `MA_RANK` | `"MA_RANK"` |
| `EDRVS_INDEX_SHORT` | `"EDRVS_INDEX_SHORT"` |
| `EDRVS_INDEX_LONG` | `"EDRVS_INDEX_LONG"` |
| `CBGSSI` | `"CBGSSI"` |
| `CB` | `"CB"` |
| `STSLEVELS` | `"STSLEVELS"` |
| `CENTRAL_BANK_WATCH` | `"CENTRAL_BANK_WATCH_PREMIUM"` |
| `IR_SWAP_RATES_INTRADAY_CALC_BANK` | `"IR_SWAP_RATES_INTRADAY_CALC_BANK"` |
| `RETAIL_FLOW_DAILY_V2_PREMIUM` | `"RETAIL_FLOW_DAILY_V2_PREMIUM"` |
| `FX_EVENTS_JUMPS` | `"FX_EVENTS_JUMPS"` |
| `FXSPOT_INTRADAY2` | `"FXSPOT_INTRADAY2"` |
| `FXFORWARDPOINTS_PREMIUM` | `"FXFORWARDPOINTS_PREMIUM"` |
| `FXFORWARDPOINTS_INTRADAY` | `"FXFORWARDPOINTS_INTRADAY"` |
| `WEATHER` | `"WEATHER"` |
| `QES_INTRADAY_COVARIANCE` | `"QES_INTRADAY_COVARIANCE_PREMIUM"` |

#### Nested Enum: Dataset.TR (Vendor)
| Value | Raw String |
|-------|-----------|
| `TREOD` | `"TREOD"` |
| `TR` | `"TR"` |
| `TR_FXSPOT` | `"TR_FXSPOT"` |

#### Nested Enum: Dataset.FRED (Vendor)
| Value | Raw String |
|-------|-----------|
| `GDP` | `"GDP"` |

#### Nested Enum: Dataset.TradingEconomics (Vendor)
| Value | Raw String |
|-------|-----------|
| `MACRO_EVENTS_CALENDAR` | `"MACRO_EVENTS_CALENDAR"` |

### PTPDataset (class)
Inherits: `Dataset`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_series` | `pd.DataFrame` | *(required)* | The numeric time-series data (converted from Series if needed) |
| `_name` | `Optional[str]` | `None` | Display name; defaults to `"GSQ Default"` on sync |
| `_fields` | `Dict[str, str]` | *(set by sync)* | Mapping from API field keys to underscore-cased field names (excludes updateTime, date, datasetId) |
| `_id` | `str` | *(set by sync)* | Dataset ID returned by the server after sync |

Note: `PTPDataset.__init__` calls `super().__init__('', None)` -- the parent `Dataset.__id` is initially empty string, then re-initialized after `sync()` with the real ID.

### MarqueeDataIngestionLibrary (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__provider` (private) | `Optional[DataApi]` | `None` | Explicit data provider; if `None`, falls back to `GsDataApi` |
| `user` | `Dict` | *(from API)* | Current user info from `GsUsersApi.get_current_user_info()` |
| `managers` | `Any` | *(from API)* | App managers from `GsUsersApi.get_current_app_managers()` |

## Enums and Constants

All enums are nested within `Dataset` -- see the nested enum tables above under Type Definitions.

### Module-Level Constants
None. Constants are defined locally within methods (e.g., `VALID_SYMBOL_DIMENSION`, `VALID_TIME_DIMENSION`, `INVALID_DRG_NAME_CHARS` inside `create_dataset` / `_create_dimensions`).

### Local Constants (inside MarqueeDataIngestionLibrary.create_dataset / _create_dimensions)
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `VALID_TIME_DIMENSION` | `set[str]` | `{"date"}` | Only "date" is supported for Snowflake time dimension |
| `VALID_SYMBOL_DIMENSION` | `set[str]` | `{"isin", "bbid", "ric", "sedol", "cusip", "ticker", "countryId", "currency"}` | Accepted symbol dimension identifiers |
| `INVALID_DRG_NAME_CHARS` | `str` (regex) | `r"Pvt Ltd.*\|Private Ltd.*\|Limited.*\|Ltd.*\|Inc.*\|LP$\|LLP$\|[^a-zA-Z0-9]"` | Pattern stripped from drgName for external users |

## Functions/Methods

### Dataset.__init__(self, dataset_id: Union[str, Vendor], provider: Optional[DataApi] = None) -> None
Purpose: Initialize a Dataset with an identifier and optional data provider.

**Algorithm:**
1. Call `self._get_dataset_id_str(dataset_id)` to extract the string value.
2. Store result in `self.__id`.
3. Store `provider` in `self.__provider`.

### Dataset._get_dataset_id_str(self, dataset_id) -> str
Purpose: Convert a Vendor enum value to its string, or pass through a string unchanged.

**Algorithm:**
1. Branch: if `dataset_id` is an instance of `Dataset.Vendor` -> return `dataset_id.value`.
2. Else -> return `dataset_id` as-is.

### Dataset.id (property) -> str
Purpose: Return the dataset identifier string (`self.__id`).

### Dataset.name (property) -> None
Purpose: Placeholder property; returns `None` (body is `pass`).

### Dataset.provider (property) -> DataApi
Purpose: Return the data provider, defaulting to `GsDataApi` if none was explicitly set.

**Algorithm:**
1. Lazily import `GsDataApi` from `gs_quant.api.gs.data`.
2. Return `self.__provider or GsDataApi`.

### Dataset._build_data_query(self, start, end, as_of, since, fields, empty_intervals, **kwargs) -> Tuple[query, bool]
Purpose: Build a data query object and determine if the schema varies (has function-style fields).

**Parameters:**
- `start`: `Union[dt.date, dt.datetime]`
- `end`: `Union[dt.date, dt.datetime]`
- `as_of`: `dt.datetime`
- `since`: `dt.datetime`
- `fields`: `Iterable[Union[str, Fields]]`
- `empty_intervals`: `bool`
- `**kwargs`: additional query parameters

**Algorithm:**
1. Branch: if `fields` is `None` -> `field_names = None`.
2. Else -> map each field: if it's a `str`, keep it; otherwise use `.value`. Collect into a list.
3. Compute `schema_varies`: `True` if `field_names` is not `None` AND any field matches regex `\w+\(` (i.e., contains a function call like `difference(tradePrice)`).
4. Branch: if `kwargs` contains key `"date"`:
   a. Get `d = kwargs["date"]`.
   b. Branch: if `type(d) is str` (exact type check, not isinstance):
      - Try to parse with `strptime(d, "%Y-%m-%d")` and replace `kwargs["date"]` with the resulting `date` object.
      - Branch: on `ValueError` -> silently pass (ignore non-standard date formats).
   c. Branch: if `"dates"` not already in `kwargs` AND `start is None` AND `end is None`:
      - Set `kwargs["dates"] = (kwargs["date"],)` (single-element tuple).
5. Call `self.provider.build_query(start=start, end=end, as_of=as_of, since=since, fields=field_names, empty_intervals=empty_intervals, **kwargs)`.
6. Return `(query, schema_varies)`.

### Dataset._build_data_frame(self, data, schema_varies: bool, standard_fields) -> pd.DataFrame
Purpose: Convert raw API response data into a typed DataFrame, handling grouped results.

**Algorithm:**
1. Branch: if `type(data) is tuple`:
   a. Call `provider.construct_dataframe_with_types(self.id, data[0], schema_varies, standard_fields=standard_fields)` to build the DataFrame.
   b. Group by `data[1]` with `group_keys=True`, apply identity lambda `lambda x: x`.
   c. Return the grouped DataFrame.
2. Else:
   a. Call `provider.construct_dataframe_with_types(self.id, data, schema_varies, standard_fields=standard_fields)`.
   b. Return the DataFrame directly.

### Dataset.get_data(self, start, end, as_of, since, fields, asset_id_type, empty_intervals, standard_fields, **kwargs) -> pd.DataFrame
Purpose: Synchronously get data for the given range and parameters.

**Parameters:**
- `start`: `Optional[Union[dt.date, dt.datetime]]` = `None`
- `end`: `Optional[Union[dt.date, dt.datetime]]` = `None`
- `as_of`: `Optional[dt.datetime]` = `None`
- `since`: `Optional[dt.datetime]` = `None`
- `fields`: `Optional[Iterable[Union[str, Fields]]]` = `None`
- `asset_id_type`: `Optional[str]` = `None`
- `empty_intervals`: `Optional[bool]` = `None`
- `standard_fields`: `Optional[bool]` = `False`
- `**kwargs`: extra query args (e.g., `ticker='EDZ19'`)

**Algorithm:**
1. Call `self._build_data_query(start, end, as_of, since, fields, empty_intervals, **kwargs)` -> `(query, schema_varies)`.
2. Call `self.provider.query_data(query, self.id, asset_id_type=asset_id_type)` -> `data`.
3. Call `self._build_data_frame(data, schema_varies, standard_fields)` and return the result.

### Dataset.get_data_async(self, start, end, as_of, since, fields, empty_intervals, standard_fields, **kwargs) -> pd.DataFrame
Purpose: Asynchronously get data for the given range and parameters. Same as `get_data` but uses `await`.

**Parameters:** Same as `get_data` except no `asset_id_type` parameter.

**Algorithm:**
1. Call `self._build_data_query(...)` -> `(query, schema_varies)`.
2. `data = await self.provider.query_data_async(query, self.id)`.
3. Return `self._build_data_frame(data, schema_varies, standard_fields)`.

### Dataset._build_data_series_query(self, field, start, end, as_of, since, dates, **kwargs) -> Tuple[str, query, str]
Purpose: Build query for a single-field time series and validate symbol dimensions.

**Parameters:**
- `field`: `Union[str, Fields]`
- `start`: `Union[dt.date, dt.datetime]`
- `end`: `Union[dt.date, dt.datetime]`
- `as_of`: `dt.datetime`
- `since`: `dt.datetime`
- `dates`: `List[dt.date]`
- `**kwargs`: additional query parameters

**Algorithm:**
1. Branch: if `field` is `str` -> `field_value = field`. Else -> `field_value = field.value`.
2. Build query via `self.provider.build_query(start=start, end=end, as_of=as_of, since=since, fields=(field_value,), dates=dates, **kwargs)`.
3. Get `symbol_dimensions = self.provider.symbol_dimensions(self.id)`.
4. Branch: if `len(symbol_dimensions) != 1` -> raise `MqValueError('get_data_series only valid for symbol_dimensions of length 1')`.
5. Extract `symbol_dimension = symbol_dimensions[0]`.
6. Return `(field_value, query, symbol_dimension)`.

**Raises:** `MqValueError` when symbol dimensions length is not 1.

### Dataset._build_data_series(self, data, field_value: str, symbol_dimension: str, standard_fields: bool) -> pd.Series
Purpose: Convert raw API data into a single-valued time series.

**Algorithm:**
1. Build DataFrame: `df = self.provider.construct_dataframe_with_types(self.id, data, standard_fields=standard_fields)`.
2. Lazily import `GsDataApi`.
3. Branch: if `self.provider` is an instance of `GsDataApi`:
   a. Group `df` by `symbol_dimension`.
   b. Branch: if more than 1 group -> raise `MqValueError('Not a series for a single {symbol_dimension}')`.
4. Branch: if `df.empty` -> return `pd.Series(dtype=float)`.
5. Branch: if `'('` is in `field_value`:
   a. Replace `'('` with `'_'` in `field_value`.
   b. Replace `')'` with `''` in `field_value`.
6. Return `pd.Series(index=df.index, data=df.loc[:, field_value].values)`.

**Raises:** `MqValueError` when multiple groups exist for the symbol dimension.

### Dataset.get_data_series(self, field, start, end, as_of, since, dates, standard_fields, **kwargs) -> pd.Series
Purpose: Synchronously get a single-field time series.

**Parameters:**
- `field`: `Union[str, Fields]`
- `start`: `Optional[Union[dt.date, dt.datetime]]` = `None`
- `end`: `Optional[Union[dt.date, dt.datetime]]` = `None`
- `as_of`: `Optional[dt.datetime]` = `None`
- `since`: `Optional[dt.datetime]` = `None`
- `dates`: `Optional[List[dt.date]]` = `None`
- `standard_fields`: `Optional[bool]` = `False`
- `**kwargs`: extra query args

**Algorithm:**
1. Call `self._build_data_series_query(field, start, end, as_of, since, dates, **kwargs)` -> `(field_value, query, symbol_dimension)`.
2. Call `self.provider.query_data(query, self.id)` -> `data`.
3. Return `self._build_data_series(data, field_value, symbol_dimension, standard_fields)`.

### Dataset.get_data_series_async(self, field, start, end, as_of, since, dates, standard_fields, **kwargs) -> pd.Series
Purpose: Asynchronously get a single-field time series.

**Algorithm:**
1. Same as `get_data_series` but step 2 uses `await self.provider.query_data_async(query, self.id)`.

### Dataset.get_data_last(self, as_of, start, fields, standard_fields, **kwargs) -> pd.DataFrame
Purpose: Get the last data point at or before `as_of`.

**Parameters:**
- `as_of`: `Optional[Union[dt.date, dt.datetime]]` (positional, not keyword-only)
- `start`: `Optional[Union[dt.date, dt.datetime]]` = `None`
- `fields`: `Optional[Iterable[str]]` = `None`
- `standard_fields`: `Optional[bool]` = `False`
- `**kwargs`: additional query parameters

**Algorithm:**
1. Build query: `self.provider.build_query(start=start, end=as_of, fields=fields, format='JSON', **kwargs)`.
2. Set `query.format = None` (the "last" endpoint does not support MessagePack).
3. Call `self.provider.last_data(query, self.id)` -> `data`.
4. Return `self.provider.construct_dataframe_with_types(self.id, data, standard_fields=standard_fields)`.

### Dataset.get_coverage(self, limit, offset, fields, include_history, **kwargs) -> pd.DataFrame
Purpose: Get the assets covered by this dataset.

**Parameters:**
- `limit`: `Optional[int]` = `None`
- `offset`: `Optional[int]` = `None`
- `fields`: `Optional[List[str]]` = `None`
- `include_history`: `bool` = `False`
- `**kwargs`: additional query parameters

**Algorithm:**
1. Call `self.provider.get_coverage(self.id, limit=limit, offset=offset, fields=fields, include_history=include_history, **kwargs)`.
2. Return `pd.DataFrame(coverage)`.

### Dataset.get_coverage_async(self, limit, offset, fields, include_history, **kwargs) -> pd.DataFrame
Purpose: Asynchronously get coverage. Same logic as `get_coverage` but with `await`.

### Dataset.delete(self) -> Dict
Purpose: Delete the dataset definition.

**Algorithm:**
1. Return `self.provider.delete_dataset(self.id)`.

### Dataset.undelete(self) -> Dict
Purpose: Un-delete a previously deleted dataset definition.

**Algorithm:**
1. Return `self.provider.undelete_dataset(self.id)`.

### Dataset.delete_data(self, delete_query: Dict) -> Any
Purpose: Delete data from the dataset (irreversible).

**Algorithm:**
1. Return `self.provider.delete_data(self.id, delete_query)`.

### Dataset.upload_data(self, data: Union[pd.DataFrame, list, tuple]) -> Dict
Purpose: Upload data to this dataset.

**Algorithm:**
1. Return `self.provider.upload_data(self.id, data)`.

### Dataset.get_data_bulk(self, request_batch_size, original_start, final_end, identifier, symbols_per_csv, datetime_delta_override, handler) -> None
Purpose: Extract data from dataset by running parallel queries in batches, writing to CSV or passing to a handler.

**Parameters:**
- `request_batch_size`: `int` (must be > 0 and < 5)
- `original_start`: `dt.datetime` (mandatory start date)
- `final_end`: `Optional[dt.datetime]` = `None` (defaults to `dt.datetime.now()`)
- `identifier`: `str` = `"bbid"`
- `symbols_per_csv`: `int` = `1000`
- `datetime_delta_override`: `Optional[int]` = `None`
- `handler`: `Optional[Callable[[pd.DataFrame], None]]` = `None`

**Algorithm:**
1. Try to create `authenticate` as `partial(GsSession.use, client_id=GsSession.current.client_id, client_secret=GsSession.current.client_secret)`.
2. Branch: on `AttributeError` -> fall back to `authenticate = partial(GsSession.use)`.
3. Call `Utilities.get_dataset_parameter(self)` -> `(time_field, history_time, symbol_dimension, timedelta)`.
4. Set `final_end = final_end or dt.datetime.now()`.
5. Determine `write_to_csv = handler is None`.
6. Call `Utilities.pre_checks(final_end, original_start, time_field, datetime_delta_override, request_batch_size, write_to_csv)` -> `(final_end, target_dir_result)`.
7. Branch: if `write_to_csv` -> print target directory.
8. Branch: if `time_field == 'date'`:
   a. Clamp `original_start` to `max(original_start.date(), history_time.date())`.
   b. Clamp `final_end` to `max(final_end.date(), history_time.date())`.
   c. Branch: if `datetime_delta_override is None` -> use `timedelta` from step 3. Else -> `dt.timedelta(days=datetime_delta_override)`.
9. Branch: elif `time_field == 'time'`:
   a. Convert both `original_start` and `final_end` to UTC, clamp against `history_time` in UTC.
   b. Branch: if `datetime_delta_override is None` -> use `timedelta`. Else -> `dt.timedelta(hours=datetime_delta_override)`.
10. Compute `original_end = min(original_start + datetime_delta_override, final_end)`.
11. Get coverage: `Utilities.get_dataset_coverage(identifier, symbol_dimension, self)`.
12. Batch coverage: `Utilities.batch(coverage, n=symbols_per_csv)`.
13. Initialize `batch_number = 1`.
14. For each `coverage_batch` in `coverage_batches`:
    a. Call `Utilities.iterate_over_series(self, coverage_batch, original_start, original_end, datetime_delta_override, identifier, request_batch_size, authenticate, final_end, write_to_csv, target_dir_result, batch_number, coverage_length, symbols_per_csv, handler)`.
    b. Increment `batch_number`.

---

### PTPDataset.__init__(self, series: Union[pd.Series, pd.DataFrame], name: Optional[str] = None) -> None
Purpose: Initialize a PTP dataset, validating the input series.

**Algorithm:**
1. Branch: if `series` is a `pd.Series`:
   a. Convert to DataFrame with column name from `series.attrs.get('name', 'values')`.
2. Branch: if `series.index` is NOT a `pd.DatetimeIndex`:
   a. Raise `MqValueError('PTP datasets require a Pandas object with a DatetimeIndex.')`.
3. Branch: if `series` is a `pd.DataFrame` AND the count of numeric columns != total columns:
   a. Raise `MqValueError('PTP datasets must contain only numbers.')`.
4. Store `self._series = series`.
5. Store `self._name = name`.
6. Call `super().__init__('', None)`.

**Raises:**
- `MqValueError` when index is not DatetimeIndex.
- `MqValueError` when DataFrame contains non-numeric columns.

### PTPDataset.sync(self) -> None
Purpose: Upload data and register the dataset with the PTP service.

**Algorithm:**
1. Create `temp_ser` by assigning a `date` column from the index (each datetime -> `dt.date.isoformat`).
2. Convert to list of dicts via `.to_dict('records')`.
3. Build kwargs: `data=data, name=self._name if self._name else 'GSQ Default', fields=list(self._series.columns)`.
4. POST to `/plots/datasets` via `GsSession.current.sync.post(...)` -> `res`.
5. Build `self._fields` dict: for each `(key, field)` in `res['fieldMap']`, exclude entries where `field in ['updateTime', 'date', 'datasetId']`, map `key -> inflection.underscore(field)`.
6. Set `self._id = res['dataset']['id']`.
7. Re-initialize parent: `super().__init__(self._id, None)`.

### PTPDataset.plot(self, open_in_browser: bool = True, field: Optional[str] = None) -> str
Purpose: Generate a transient PTP plot expression URL and optionally open it in the browser.

**Algorithm:**
1. Branch: if `field` is falsy -> `fields = self._fields.values()` (all fields).
2. Else -> `fields = [field]` (single field).
3. Transform each field: apply `inflection.underscore(re.sub(r'([a-zA-Z])(\d)', r'\1_\2', f))` to insert underscores before digits.
4. Get domain: `GsSession.current.domain.replace('marquee.web', 'marquee')`.
5. Build base expression: `{domain}/s/plottool/transient?expr=Dataset("{self._id}").{fields[0]}()`.
6. For each remaining field in `fields[1:]`: append `quote("\n") + Dataset("{self._id}").{f}()`.
7. Branch: if `open_in_browser` is `True` -> call `webbrowser.open(expression)`.
8. Return `expression`.

---

### MarqueeDataIngestionLibrary.__init__(self, provider: Optional[DataApi] = None) -> None
Purpose: Initialize the ingestion library, fetching current user info and app managers.

**Algorithm:**
1. Store `self.__provider = provider`.
2. `self.user = GsUsersApi.get_current_user_info()`.
3. `self.managers = GsUsersApi.get_current_app_managers()`.

### MarqueeDataIngestionLibrary.provider (property) -> DataApi
Purpose: Return the data provider, defaulting to `GsDataApi`.

**Algorithm:**
1. Lazily import `GsDataApi`.
2. Return `self.__provider or GsDataApi`.

### MarqueeDataIngestionLibrary._create_parameters(self, time_dimension: str, symbol_dimension: str, is_internal_user: bool) -> DataSetParameters
Purpose: Build `DataSetParameters` with Snowflake configuration.

**Algorithm:**
1. Create `parameters = DataSetParameters()`.
2. Set `parameters.frequency = 'Daily'`.
3. Set `parameters.snowflake_config = DBConfig()`.
4. Branch: if `is_internal_user` -> `db = "TIMESERIES"`. Else -> `db = "EXTERNAL"`.
5. Set `parameters.snowflake_config.date_time_column = self.to_upper_underscore(time_dimension)`.
6. Set `parameters.snowflake_config.id_column = self.to_upper_underscore(symbol_dimension)`.
7. Return `parameters`.

### MarqueeDataIngestionLibrary._check_and_create_field(self, fieldMap: Dict[str, str], dataframe: pd.DataFrame) -> None
Purpose: Check if fields exist in the API; create any that are missing.

**Algorithm:**
1. Initialize `fields_to_create = []`.
2. For each `(column, field_name)` in `fieldMap.items()`:
   a. Branch: if `self.provider.get_dataset_fields(names=field_name)` returns falsy (empty):
      - Infer data type from `pd.api.types.infer_dtype(dataframe[column])`.
      - Branch: if data type is `'floating'` or `'integer'` -> `api_data_type = 'number'`. Else -> use inferred type.
      - Append `DataSetFieldEntity(name=field_name, type_=api_data_type, description=f'field {field_name} created from GSQuant')`.
3. Branch: if `fields_to_create` is non-empty -> call `self.provider.create_dataset_fields(fields_to_create)` and return its result.

### MarqueeDataIngestionLibrary._create_dimensions(self, data, symbol_dimension, time_dimension, dimensions, measures, internal_user) -> DataSetDimensions
Purpose: Build `DataSetDimensions` with field mappings, handling internal vs external user naming conventions.

**Parameters:**
- `data`: `pd.DataFrame`
- `symbol_dimension`: `str`
- `time_dimension`: `str`
- `dimensions`: `Optional[List[str]]`
- `measures`: `List[str]`
- `internal_user`: `bool`

**Algorithm:**
1. Branch: if `len(measures) > 25` -> raise `ValueError("The number of measures exceeds the allowed limit of 25.")`.
2. Create `dataset_dimensions = DataSetDimensions()`.
3. Set `time_field = time_dimension`.
4. Set `symbol_dimensions = [symbol_dimension]`.
5. Branch: if `internal_user`:
   a. Build `fieldMap = {field: self.to_camel_case(field) for field in (dimensions + measures)}`.
6. Else (external user):
   a. Get `drgName = self.user.get("drgName")`.
   b. Branch: if `drgName is None` -> raise `InvalidInputException("drgName is required but was not found.")`.
   c. Strip invalid characters from `drgName` using regex `INVALID_DRG_NAME_CHARS`.
   d. Build `fieldMap`: for each field in `dimensions + measures`, if field is `'updateTime'` use field as-is, otherwise use `f"{field}Org{drgName}"`, truncate to 64 chars, then camelCase.
7. Call `self._check_and_create_field(fieldMap, data)`.
8. Build `non_symbol_dimensions` as tuple of `FieldColumnPair(field_=fieldMap.get(dim), column=self.to_upper_underscore(dim))` for each dim in `dimensions or []`.
9. Build `measures` as tuple of `FieldColumnPair(field_=fieldMap.get(mea), column=self.to_upper_underscore(mea), resolvable=True if mea != 'updateTime' else None)` for each measure.
10. Return `dataset_dimensions`.

**Raises:**
- `ValueError` when measures count exceeds 25.
- `InvalidInputException` when external user has no `drgName`.

### MarqueeDataIngestionLibrary.to_upper_underscore(self, name: str) -> str
Purpose: Convert a name to UPPER_SNAKE_CASE.

**Algorithm:**
1. Return `snake_case(name).upper()` (uses `pydash.snake_case`).

### MarqueeDataIngestionLibrary.to_camel_case(self, name: str) -> str
Purpose: Convert a name to camelCase.

**Algorithm:**
1. Return `camel_case(name)` (uses `pydash.camel_case`).

### MarqueeDataIngestionLibrary.create_dataset(self, data, dataset_id, symbol_dimension, time_dimension, dimensions) -> DataSetEntity
Purpose: Create a native Snowflake dataset from a DataFrame.

**Parameters:**
- `data`: `pd.DataFrame`
- `dataset_id`: `str`
- `symbol_dimension`: `str`
- `time_dimension`: `str`
- `dimensions`: `Optional[List[str]]` = `[]`

**Algorithm:**
1. Branch: if `data.empty` OR `not dataset_id` OR `not symbol_dimension` OR `not time_dimension`:
   a. Print error message.
   b. Raise `InvalidInputException("One or more required parameters are empty or null.")`.
2. Convert `data[time_dimension]` to datetime. Branch: if NOT datetime64 dtype OR any row has non-midnight time:
   a. Raise `InvalidInputException` with message about intraday data not being supported.
3. Create `dataset_definition = DataSetEntity()`.
4. Set `id_ = dataset_id`.
5. Set `name = dataset_id.replace('_', ' ').title()`.
6. Set `description = "Dataset created from GSQuant"`.
7. Compute `all_columns = set(data.columns)`.
8. Compute `specified_columns = set([symbol_dimension] + [time_dimension] + (dimensions or []))`.
9. Compute `measures = list(all_columns - specified_columns)`.
10. Get `internal_user = self.user.get("internal")`.
11. Branch: if NOT `internal_user` AND `self.to_camel_case(symbol_dimension)` NOT in `VALID_SYMBOL_DIMENSION`:
    a. `custom_symbol_dimension = "customId"`.
12. Else -> `custom_symbol_dimension = self.to_camel_case(symbol_dimension)`.
13. Branch: if `time_dimension` NOT in `VALID_TIME_DIMENSION` -> `custom_time_dimensions = "date"`. Else -> use `time_dimension`.
14. Create parameters via `self._create_parameters(time_dimension, symbol_dimension, internal_user)`.
15. Create dimensions via `self._create_dimensions(data, custom_symbol_dimension, custom_time_dimensions, dimensions, measures, internal_user)`.
16. Set `type_ = DataSetType.NativeSnowflake`.
17. Call `self.provider.create(dataset_definition)` -> `result`.
18. Get `url = self.provider.get_catalog_url(dataset_id)`.
19. Print success message with URL.
20. Return `result`.

**Raises:**
- `InvalidInputException` when required params are empty/null.
- `InvalidInputException` when time dimension contains intraday timestamps.

### MarqueeDataIngestionLibrary.write_data(self, df: pd.DataFrame, dataset_id: str) -> None
Purpose: Write DataFrame data to an existing dataset, remapping column names to API field names.

**Parameters:**
- `df`: `pd.DataFrame`
- `dataset_id`: `str`

**Algorithm:**
1. Branch: if `df.empty` -> raise `ValueError("The DataFrame is empty. No data to write.")`.
2. Branch: if `not dataset_id` -> raise `ValueError("Dataset ID is required.")`.
3. Fetch dataset definition: `self.provider.get_definition(dataset_id)`.
4. Build `allFields` by concatenating:
   - `dataset.dimensions.non_symbol_dimensions`
   - `dataset.dimensions.measures`
   - A tuple of two `FieldColumnPair`: one for time field, one for symbol dimension.
5. Copy `df` and rename columns: for each column, find the matching `FieldColumnPair` where `self.to_upper_underscore(column) == field.column`, and rename to `field.field_`.
6. Convert renamed DataFrame to list of dicts.
7. Call `self.provider.upload_data(dataset_id, data)` and return the result.

**Raises:**
- `ValueError` when DataFrame is empty.
- `ValueError` when dataset_id is falsy.

**Note:** The return type annotation says `-> None` but the method actually returns the result of `self.provider.upload_data(...)`.

## State Mutation
- `Dataset.__id`: Set in `__init__`; re-set in `PTPDataset.sync()` when parent `__init__` is called again.
- `Dataset.__provider`: Set in `__init__`; never modified after.
- `PTPDataset._series`: Set in `__init__`; never modified after.
- `PTPDataset._name`: Set in `__init__`; never modified after.
- `PTPDataset._fields`: Set during `sync()` from server response.
- `PTPDataset._id`: Set during `sync()` from server response.
- `MarqueeDataIngestionLibrary.user`: Set in `__init__`; never modified after.
- `MarqueeDataIngestionLibrary.managers`: Set in `__init__`; never modified after.
- `MarqueeDataIngestionLibrary.__provider`: Set in `__init__`; never modified after.
- Side effects: `get_data_bulk` prints to stdout. `PTPDataset.plot` may open a browser. `create_dataset` prints to stdout.
- Thread safety: No explicit thread safety. `get_data_bulk` uses `Utilities.iterate_over_series` which runs parallel queries using `GsSession.use` context manager. The `authenticate` partial is passed for session re-establishment in worker threads.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_build_data_series_query` | When `symbol_dimensions` length is not exactly 1 |
| `MqValueError` | `_build_data_series` | When grouped data has more than 1 symbol group (only when provider is `GsDataApi` instance) |
| `MqValueError` | `PTPDataset.__init__` | When series index is not `pd.DatetimeIndex` |
| `MqValueError` | `PTPDataset.__init__` | When DataFrame contains non-numeric columns |
| `InvalidInputException` | `create_dataset` | When any required parameter is empty or null |
| `InvalidInputException` | `create_dataset` | When time dimension contains intraday timestamps |
| `InvalidInputException` | `_create_dimensions` | When external user has no `drgName` |
| `ValueError` | `_create_dimensions` | When measures count exceeds 25 |
| `ValueError` | `write_data` | When DataFrame is empty |
| `ValueError` | `write_data` | When dataset_id is falsy |
| `ValueError` (caught) | `_build_data_query` | When date string parsing fails; silently ignored |
| `AttributeError` (caught) | `get_data_bulk` | When `GsSession.current` lacks `client_id`/`client_secret`; falls back to `partial(GsSession.use)` |

## Edge Cases
- **Vendor enum as dataset_id**: `Dataset.__init__` accepts both `str` and `Vendor` enum values, extracting `.value` from enums.
- **Empty fields parameter**: When `fields=None` is passed to `_build_data_query`, `field_names` stays `None` and `schema_varies` is `False`.
- **Function-style field names**: Fields like `"difference(tradePrice)"` set `schema_varies=True` and trigger parenthesis-to-underscore renaming in `_build_data_series` (e.g., becomes `"difference_tradePrice"`).
- **Date kwarg handling**: The `date` kwarg in `_build_data_query` uses exact type check `type(d) is str` (not `isinstance`), so subclasses of `str` would NOT be parsed. Only `"%Y-%m-%d"` format is attempted; other formats silently pass.
- **Dates tuple auto-generation**: When `date` is provided but `dates` is not, and both `start` and `end` are `None`, a single-element tuple `(date,)` is set as `dates`.
- **Tuple vs non-tuple data**: `_build_data_frame` checks `type(data) is tuple` (exact type check). Tuple data gets groupby treatment; other data does not.
- **Empty DataFrame in series**: `_build_data_series` returns `pd.Series(dtype=float)` when the DataFrame is empty, before attempting column access.
- **Provider isinstance check**: `_build_data_series` only performs the multiple-group validation when `self.provider` is an instance of `GsDataApi`. Other providers skip this check entirely.
- **PTPDataset Series conversion**: When given a `pd.Series`, uses `series.attrs.get('name', 'values')` as the column name; if the series has no name attribute, defaults to `'values'`.
- **PTPDataset re-initialization**: `sync()` calls `super().__init__` a second time, which resets the parent `Dataset.__id` with the real ID from the server.
- **get_data_last format override**: Builds query with `format='JSON'` then immediately sets `query.format = None` because the "last" endpoint doesn't support MessagePack.
- **get_data_bulk time_field branching**: Only `'date'` and `'time'` branches exist. If `time_field` is neither, `datetime_delta_override` remains `None` and `original_end` computation will fail.
- **External user field naming**: For external users, field names are constructed as `"{field}Org{drgName}"` (truncated to 64 chars), except `'updateTime'` which keeps its original name.
- **write_data return type mismatch**: Annotated as `-> None` but actually returns the result of `provider.upload_data()`.
- **create_dataset default mutable argument**: `dimensions` parameter defaults to `[]` (mutable default), a known Python anti-pattern. The list is used read-only, so it is safe in practice.
- **Custom symbol/time dimension mapping**: External users with non-standard symbol dimensions get mapped to `"customId"`. Non-standard time dimensions always get mapped to `"date"`.

## Coverage Notes
- Branch count: ~45 explicit branches
- Key branch areas:
  - `_build_data_query`: 6 branches (fields None check, schema_varies, date kwarg presence, type(d) is str, ValueError try/except, dates/start/end None check)
  - `_build_data_frame`: 2 branches (tuple vs non-tuple)
  - `_build_data_series`: 5 branches (isinstance GsDataApi, group count > 1, df.empty, parenthesis in field_value -- two replacements)
  - `_build_data_series_query`: 2 branches (field type, symbol_dimensions length)
  - `get_data_bulk`: 6 branches (AttributeError catch, write_to_csv print, time_field == 'date', time_field == 'time', datetime_delta_override None for each time_field branch)
  - `PTPDataset.__init__`: 3 branches (isinstance Series, DatetimeIndex check, numeric-only check)
  - `PTPDataset.plot`: 2 branches (field falsy, open_in_browser)
  - `_create_parameters`: 1 branch (is_internal_user)
  - `_check_and_create_field`: 3 branches (field exists check, data type number check, fields_to_create non-empty)
  - `_create_dimensions`: 3 branches (measures > 25, internal_user, drgName None)
  - `create_dataset`: 4 branches (empty params, intraday check, internal user symbol, time dimension valid)
  - `write_data`: 2 branches (df.empty, dataset_id falsy)
  - `provider` property (both classes): 1 branch each (provider or default)
  - `_get_dataset_id_str`: 1 branch (Vendor isinstance)
- Async methods (`get_data_async`, `get_data_series_async`, `get_coverage_async`) mirror their sync counterparts with identical branch logic.
- Pragmas: None observed in this file.
