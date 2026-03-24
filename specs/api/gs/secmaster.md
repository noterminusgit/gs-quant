# secmaster.py

## Summary
GS Security Master API client providing methods to query, search, and map securities reference data from the Goldman Sachs SecMaster service. Supports identifier lookups, corporate actions retrieval, capital structure queries, exchange data, delta feeds, and underlyer-based security searches, all accessed through paginated REST endpoints via `GsSession`.

## Dependencies
- Internal: `gs_quant.data.utilities` (SecmasterXrefFormatter), `gs_quant.json_encoder` (JSONEncoder), `gs_quant.session` (GsSession), `gs_quant.target.secmaster` (SecMasterAssetType)
- External: `datetime` (date, datetime), `json`, `math`, `enum` (Enum), `functools` (partial), `itertools` (groupby), `typing` (Union, Iterable, Dict, Optional), `tqdm` (tqdm)

## Type Definitions

### GsSecurityMasterApi (class)
Inherits: none (implicit `object`)

Stateless API client class -- all methods are `@classmethod`. No instance fields.

### TypeAlias
```
CapitalStructureIdentifiers = Enum (dynamically created by __extend_enum, combining SecMasterIdentifiers + {"ISSUER_ID": "issuerId"})
```

## Enums and Constants

### SecMasterIdentifiers(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| CUSIP | `"cusip"` | CUSIP identifier |
| TICKER | `"ticker"` | Ticker symbol |
| ISIN | `"isin"` | International Securities Identification Number |
| GSID | `"gsid"` | Goldman Sachs internal ID |
| BBG | `"bbg"` | Bloomberg identifier |
| BBID | `"bbid"` | Bloomberg Barclays ID |
| BCID | `"bcid"` | Bloomberg Composite ID |
| RIC | `"ric"` | Reuters Instrument Code |
| RCIC | `"rcic"` | Reuters Composite Instrument Code |
| ID | `"id"` | Generic ID |
| ASSET_ID | `"assetId"` | Asset ID |
| CUSIP8 | `"cusip8"` | 8-character CUSIP |
| SEDOL | `"sedol"` | SEDOL identifier |
| CINS | `"cins"` | CINS identifier |
| PRIMEID | `"primeId"` | Prime ID |
| FACTSET_REGIONAL_ID | `"factSetRegionalId"` | FactSet Regional ID |
| TOKEN_ID | `"tokenId"` | Token ID |
| COMPOSITE_FIGI | `"compositeFigi"` | Composite FIGI |
| BARRA_ID | `"barraId"` | Barra ID |
| AXIOMA_ID | `"axiomaId"` | Axioma ID |
| FIGI | `"figi"` | Financial Instrument Global Identifier |

### CapitalStructureIdentifiers (dynamic Enum)
All members of `SecMasterIdentifiers` plus:

| Value | Raw | Description |
|-------|-----|-------------|
| ISSUER_ID | `"issuerId"` | Issuer identifier for capital structure lookups |

### ExchangeId(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| RIC_SUFFIX_CODE | `"ricSuffixCode"` | RIC suffix code |
| RIC_EXCHANGE_CODE | `"ricExchangeCode"` | RIC exchange code |
| DATASCOPE_IPC_CODE | `"datascopeIpcCode"` | Datascope IPC code |
| BBG_EXCHANGE_CODE | `"bbgExchangeCode"` | Bloomberg exchange code |
| TAQ_EXCHANGE_CODE | `"taqExchangeCode"` | TAQ exchange code |
| IVERSON_EXCHANGE_CODE | `"iversonExchangeCode"` | Iverson exchange code |
| INDEX_CHANGE_EXCHANGE_CODE | `"indexChangeExchangeCode"` | Index change exchange code |
| DOW_JONES_EXCHANGE_CODE | `"dowJonesExchangeCode"` | Dow Jones exchange code |
| STOXX_EXCHANGE_CODE | `"stoxxExchangeCode"` | STOXX exchange code |
| ML_ETF_EXCHANGE_CODE | `"mlEtfExchangeCode"` | ML ETF exchange code |
| FTSE_EXCHANGE_CODE | `"ftseExchangeCode"` | FTSE exchange code |
| DJGI_EXCHANGE_CODE | `"djgiExchangeCode"` | DJGI exchange code |
| SECDB_EXCHANGE_CODE | `"secdbExchangeCode"` | SecDB exchange code |
| DADD_EXCHANGE_CODE | `"daddExchangeCode"` | DADD exchange code |
| MIC | `"mic"` | Market Identifier Code |
| OPERATING_MIC | `"operatingMic"` | Operating MIC |
| GS_EXCHANGE_ID | `"gsExchangeId"` | GS Exchange ID |
| COUNTRY | `"country"` | Country |
| EXCHANGE_NAME | `"name"` | Exchange name |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| DEFAULT_SCROLL_PAGE_SIZE | `int` | `500` | Default page size for scrolling through paginated results |

## Functions/Methods

### __extend_enum(base_enum: Enum, new_values: dict) -> Enum
Purpose: Module-level helper that creates a new Enum by extending an existing one with additional members.

**Algorithm:**
1. Extract all `{name: value}` pairs from `base_enum`
2. Merge `new_values` into the members dict (overwrites on collision)
3. Return a new dynamically-created `Enum` named `'CapitalStructureIdentifiers'`

### GsSecurityMasterApi.get_security(id_value: str, id_type: SecMasterIdentifiers, effective_date: dt.date = None) -> Optional[dict]
Purpose: Get flattened asset reference data for a single security by identifier.

**API Endpoint:** `GET /markets/securities` (via `get_many_securities` with `flatten=False`)

**Algorithm:**
1. Build args dict `{id_type.value: id_value}`
2. Call `get_many_securities(effective_date=effective_date, flatten=False, **args)`
3. Branch: results is not None -> return `results["results"][0]`
4. Branch: results is None -> return None

### GsSecurityMasterApi.get_many_securities(type_: SecMasterAssetType = None, effective_date: dt.date = None, limit: int = 10, flatten: bool = False, is_primary = None, offset_key: str = None, **query_params: Dict[SecMasterIdentifiers, Union[str, Iterable[str]]]) -> Optional[dict]
Purpose: Get reference data for a single page of securities, with optional filters.

**API Endpoint:** `GET /markets/securities/data` (when `flatten=True`) or `GET /markets/securities` (when `flatten=False`)

**Algorithm:**
1. Branch: `query_params` is None/empty AND `type_` is None -> raise `ValueError`
2. Build params dict with `limit`
3. Call `prepare_params(params, is_primary, offset_key, type_, effective_date)` to add optional filters
4. Merge `query_params` into `params`
5. JSON-encode payload using `JSONEncoder`
6. Branch: `flatten=True` -> GET `/markets/securities/data`; else -> GET `/markets/securities`
7. Branch: `r['totalResults'] == 0` -> return None
8. Return response dict

**Raises:** `ValueError` when neither `type_` nor `query_params` are provided

### GsSecurityMasterApi.get_all_securities(type_: SecMasterAssetType = None, effective_date: dt.date = None, is_primary = None, flatten: bool = False, **query_params) -> Optional[dict]
Purpose: Fetch all securities matching filters by auto-paginating through all result pages.

**API Endpoint:** `GET /markets/securities` or `GET /markets/securities/data` (via `get_many_securities`)

**Algorithm:**
1. Branch: `'limit'` key exists in `query_params` -> extract and delete it; else -> use `DEFAULT_SCROLL_PAGE_SIZE`
2. Call `get_many_securities(...)` for the first page
3. Branch: response is None OR `"offsetKey"` not in response -> return response as-is
4. Branch: `response['totalResults'] == 0` -> return None
5. Extract `results` list and `offsetKey` from first response
6. Build partial function `fn` from `get_many_securities` with all params bound except `offset_key`
7. Call `__fetch_all(fn, offset_key)` to accumulate remaining pages
8. Update `response["totalResults"]` and `response["results"]` with accumulated data
9. Return aggregated response

### GsSecurityMasterApi.get_security_data(id_value: str, id_type: SecMasterIdentifiers, effective_date: dt.date = None) -> Optional[dict]
Purpose: Get flattened asset reference data for a single security.

**API Endpoint:** `GET /markets/securities/data` (via `get_many_securities` with `flatten=True`)

**Algorithm:**
1. Build args dict `{id_type.value: id_value}`
2. Call `get_many_securities(effective_date=effective_date, flatten=True, **args)`
3. Branch: results is not None -> return `results["results"][0]`
4. Branch: results is None -> return None

### GsSecurityMasterApi.get_identifiers(secmaster_id: str) -> dict
Purpose: Get identifier history for a given SecMaster ID.

**API Endpoint:** `GET /markets/securities/{secmaster_id}/identifiers`

**Algorithm:**
1. Branch: `secmaster_id` does not start with `"GS"` -> raise `ValueError`
2. GET `/markets/securities/{secmaster_id}/identifiers`
3. Return `r['results']`

**Raises:** `ValueError` when `secmaster_id` does not start with `"GS"`

### GsSecurityMasterApi.get_many_identifiers(ids: Iterable[str], limit: int = 100, xref_format: bool = False) -> dict
Purpose: Get identifiers for multiple SecMaster IDs, auto-paginating through all result pages.

**API Endpoint:** `GET /markets/securities/identifiers`

**Algorithm:**
1. Branch: `ids` is not Iterable -> raise `ValueError`
2. Branch: `ids` is empty -> raise `ValueError`
3. Convert `ids` to list
4. For each `id_value` in `ids`: Branch: does not start with `"GS"` -> raise `ValueError`
5. Initialize `consolidated_results = {}` and `current_offset_key = None`
6. Loop:
   a. Build payload `{'id': ids}` plus optional `offsetKey` and `limit`
   b. JSON-encode with `JSONEncoder`
   c. GET `/markets/securities/identifiers`
   d. Merge `response['results']` into `consolidated_results` by entity_id (extend lists)
   e. Update `current_offset_key` from response; break if None
7. Branch: `xref_format=True` -> return `SecmasterXrefFormatter.convert(consolidated_results)`
8. Branch: `xref_format=False` -> return `consolidated_results`

**Raises:** `ValueError` when ids is not iterable, is empty, or contains IDs not starting with `"GS"`

### GsSecurityMasterApi.map(input_type: SecMasterIdentifiers, ids: Iterable[str], output_types: Iterable[SecMasterIdentifiers] = frozenset([SecMasterIdentifiers.GSID]), start_date: dt.date = None, end_date: dt.date = None, effective_date: dt.date = None) -> Iterable[dict]
Purpose: Map input identifiers to other identifier types.

**API Endpoint:** `GET /markets/securities/map`

**Algorithm:**
1. Build params: `{input_type.value: list(ids), 'toIdentifiers': [id.value for id in output_types], 'compact': True}`
2. Branch: `effective_date` is not None:
   a. Branch: `start_date` or `end_date` is also not None -> raise `ValueError`
   b. Set `params['effectiveDate']`
3. Branch: `start_date` is not None -> add to params
4. Branch: `end_date` is not None -> add to params
5. JSON-encode payload
6. GET `/markets/securities/map`
7. Return `r['results']`

**Raises:** `ValueError` when both `effective_date` and `start_date`/`end_date` are provided

### GsSecurityMasterApi.search(q: str, limit: int = 10, type_: SecMasterAssetType = None, is_primary: bool = None, active_listing: bool = None) -> Union[Iterable[dict], None]
Purpose: Full-text search for securities by query string among names, identifiers, and companies.

**API Endpoint:** `GET /v2/markets/securities/search` (note: uses `include_version=False`)

**Algorithm:**
1. Build params `{"q": q, "limit": limit}`
2. Branch: `type_` is not None -> add `params["type"] = type_.value`
3. Branch: `is_primary` is not None -> add `params["isPrimary"]`
4. Branch: `active_listing` is not None -> add `params["activeListing"]`
5. JSON-encode payload
6. GET `/v2/markets/securities/search` with `include_version=False`
7. Branch: `r['totalResults'] == 0` -> return None
8. Return `r["results"]`

### GsSecurityMasterApi.__stringify_boolean(bool_value: Any) -> str
Purpose: Convert a boolean value to its lowercase string representation.

**Algorithm:**
1. Return `str(bool_value).lower()`

### GsSecurityMasterApi.__fetch_all(fetch_fn: Callable, offset_key: str, total_batches: int = None, extract_results: bool = True) -> list
Purpose: Generic pagination helper that accumulates results across all pages using a fetch function.

**Algorithm:**
1. Initialize `accumulator = []` and `offset = offset_key`
2. Branch: `total_batches` is None -> create indeterminate tqdm progress bar; else -> create tqdm with range
3. Loop:
   a. Update progress bar
   b. Call `fetch_fn(offset_key=offset)`
   c. Branch: data is not None:
      - Branch: `extract_results=True` -> extend accumulator with `data['results']`
      - Branch: `extract_results=False` -> append entire `data` to accumulator
      - Branch: `'offsetKey'` not in data -> close progress bar, break
      - Update `offset = data["offsetKey"]`
4. Return accumulator

### GsSecurityMasterApi._get_corporate_actions(id_value: str, id_type: SecMasterIdentifiers, effective_date: dt.date, offset_key: Optional[str]) -> dict
Purpose: Fetch a single page of corporate actions for a security.

**API Endpoint:** `GET /markets/corpactions`

**Algorithm:**
1. Build params `{id_type.value: id_value}`
2. Branch: `effective_date` is not None -> add to params
3. Branch: `offset_key` is not None -> add to params
4. JSON-encode payload
5. GET `/markets/corpactions`
6. Return response

### GsSecurityMasterApi.get_corporate_actions(id_value: str, id_type: SecMasterIdentifiers = SecMasterIdentifiers.GSID, effective_date: dt.date = None) -> Iterable[dict]
Purpose: Get all corporate actions for a given security, auto-paginating.

**API Endpoint:** `GET /markets/corpactions` (via `_get_corporate_actions` + `__fetch_all`)

**Algorithm:**
1. Define `supported_identifiers = [GSID, ID]`
2. Branch: `id_type` not in supported_identifiers -> raise `ValueError`
3. Build partial function from `_get_corporate_actions(id_value, id_type, effective_date)`
4. Call `__fetch_all(fn, None)` to retrieve all pages
5. Return accumulated results

**Raises:** `ValueError` when `id_type` is not `GSID` or `ID`

### GsSecurityMasterApi.get_capital_structure(id_value: Union[str, list], id_type: CapitalStructureIdentifiers, type_: SecMasterAssetType = None, is_primary: bool = None, effective_date: dt.date = None) -> dict
Purpose: Get the capital structure of a company, auto-paginating and aggregating by issuer ID.

**API Endpoint:** `GET /markets/capitalstructure` (via `_get_capital_structure` + `__fetch_all`)

**Algorithm:**
1. Call `_get_capital_structure(...)` for first page
2. Branch: `"offsetKey"` not in response -> return response directly
3. Extract `assetTypesTotal`, compute `batch_count = floor(sum(assetTypesTotal.values()) / 100)`
4. Extract `results` and `offsetKey`
5. Build partial function from `_get_capital_structure` with all params bound
6. Call `__fetch_all(fn, offset_key, total_batches=batch_count)` and extend results
7. Call `__capital_structure_aggregate(asset_types_total, results)` to merge by issuer
8. Update response with aggregated results and total count
9. Delete `response["offsetKey"]`
10. Return response

### GsSecurityMasterApi.__capital_structure_aggregate(asset_types_total: dict, results: list) -> Tuple[list, int]
Purpose: Aggregate capital structure results by issuer ID, consolidating asset type data.

**Algorithm:**
1. Group results by `"issuerId"` using `itertools.groupby`
2. For each issuer ID group:
   a. Create `consolidated_types_obj` with empty lists for each key in `asset_types_total`
   b. For each object in the group:
      - Sum lengths of all type lists into `aggregated_total_results`
      - Extend each asset type list in consolidated object with corresponding data
      - Set `issuer_id_data_instance["types"]` to consolidated object
   c. Append the consolidated instance to `aggregated_results`
3. Return `(aggregated_results, aggregated_total_results)`

### GsSecurityMasterApi._get_capital_structure(id_value: Union[str, list], id_type: Union[CapitalStructureIdentifiers, SecMasterIdentifiers], type_, is_primary, effective_date, offset_key: Union[str, None]) -> dict
Purpose: Fetch a single page of capital structure data.

**API Endpoint:** `GET /markets/capitalstructure`

**Algorithm:**
1. Build params `{id_type.value: id_value}`
2. Call `prepare_params(params, is_primary, offset_key, type_, effective_date)`
3. JSON-encode payload
4. GET `/markets/capitalstructure`
5. Return response

### GsSecurityMasterApi.prepare_params(params: dict, is_primary: Optional[bool], offset_key: Optional[str], type_: Optional[SecMasterAssetType], effective_date: Optional[dt.date] = None) -> None
Purpose: Mutate params dict in-place to add optional filter parameters.

**Algorithm:**
1. Branch: `type_` is not None -> `params["type"] = type_.value`
2. Branch: `is_primary` is not None -> `params["isPrimary"] = is_primary`
3. Branch: `offset_key` is not None -> `params["offsetKey"] = offset_key`
4. Branch: `effective_date` is not None -> `params["effectiveDate"] = effective_date`

### GsSecurityMasterApi._get_deltas(start_time: dt.datetime = None, end_time: dt.datetime = None, raw: bool = None, scope: list = None, limit: int = None, offset_key: str = None) -> Iterable[dict]
Purpose: Fetch a single page of identifier delta/change data.

**API Endpoint:** `GET /markets/securities/identifiers/updates-feed`

**Algorithm:**
1. Build params dict, conditionally adding each non-None parameter:
   - `raw` -> converted via `__stringify_boolean(raw)`
   - `startTime`, `endTime`, `scope`, `limit`, `offsetKey`
2. JSON-encode payload
3. GET `/markets/securities/identifiers/updates-feed`
4. Return response

### GsSecurityMasterApi.get_deltas(start_time: dt.datetime = None, end_time: dt.datetime = None, raw: bool = None, scope: list = None, limit: int = None, offset_key: str = None, scroll_all_pages: bool = True) -> Union[dict, Iterable[dict]]
Purpose: Get all identifier changes between two timestamps, optionally auto-paginating.

**API Endpoint:** `GET /markets/securities/identifiers/updates-feed` (via `_get_deltas`)

**Algorithm:**
1. Branch: `scroll_all_pages=True`:
   a. Build partial function from `_get_deltas(start_time, end_time, raw, scope, limit)`
   b. Call `__fetch_all(fn, offset_key, extract_results=False)` to get all pages
   c. Compute `latest_update_time = max(result['lastUpdateTime'] for result in results)`
   d. Flatten all `result["results"]` into single list `res`
   e. Extract `requestId` from first result (or None if empty)
   f. Return `{"results": res, "lastUpdateTime": latest_update_time, "requestId": request_id}`
2. Branch: `scroll_all_pages=False`:
   a. Call `_get_deltas(...)` directly with all params
   b. Return single-page result

### GsSecurityMasterApi.get_exchanges(effective_date: dt.date = None, **query_params: Dict[str, Union[str, Iterable[str]]]) -> dict
Purpose: Get reference data for exchanges (MICs, exchange codes, country, name, etc.), auto-paginating.

**API Endpoint:** `GET /markets/exchanges` (via `_get_exchanges` + `__fetch_all`)

**Algorithm:**
1. Build partial function from `_get_exchanges(effective_date, DEFAULT_SCROLL_PAGE_SIZE, query_params)`
2. Call `__fetch_all(fn, offset_key=None)` and extend results
3. Build response dict with `totalResults` and `results`
4. Return response

### GsSecurityMasterApi._get_exchanges(effective_date: dt.date = None, limit: int = 10, query_params: dict = None, offset_key: Union[str, None] = None) -> Optional[dict]
Purpose: Fetch a single page of exchange reference data with validation of query parameters.

**API Endpoint:** `GET /markets/exchanges`

**Algorithm:**
1. Branch: `query_params` is None -> default to empty dict
2. Extract allowed keys from `ExchangeId._value2member_map_`
3. For each key in `query_params`: Branch: key not in allowed_keys -> raise `ValueError`
4. Build params with `limit`
5. Branch: `effective_date` is not None -> add to params
6. Merge `query_params` into params
7. Branch: `offset_key` is not None -> add to params
8. JSON-encode payload
9. GET `/markets/exchanges`
10. Branch: `r['totalResults'] == 0` -> return None
11. Return response

**Raises:** `ValueError` when a query parameter is not in the allowed set

### GsSecurityMasterApi.get_exchange_identifiers_history(gs_exchange_id: str) -> Iterable[dict]
Purpose: Get identifier history for a specific exchange.

**API Endpoint:** `GET /markets/exchanges/{gs_exchange_id}/identifiers`

**Algorithm:**
1. GET `/markets/exchanges/{gs_exchange_id}/identifiers`
2. Return `r['results']`

### GsSecurityMasterApi._prepare_string_or_list_param(value: Union[str, list], param_name: str) -> list
Purpose: Normalize a parameter to always be a list of strings.

**Algorithm:**
1. Branch: `value` is a `str` -> return `[value]`
2. Branch: `value` is a `list` AND all elements are `str` -> return `value`
3. Branch: otherwise -> raise `ValueError`

**Raises:** `ValueError` when value is neither a string nor a list of strings

### GsSecurityMasterApi._prepare_underlyers_params(params: dict, id_value: Union[str, list, None], offset_key: str = None, type_: Union[SecMasterAssetType, list] = None, effective_date: dt.date = None, country_code: Union[str, list] = None, currency: Union[str, list] = None, include_inactive: bool = None) -> None
Purpose: Mutate params dict in-place to prepare underlyer query parameters with validation.

**Algorithm:**
1. Branch: `id_value` is not None -> normalize via `_prepare_string_or_list_param` and set `params["id"]`
2. Branch: `id_value` is None -> raise `ValueError`
3. Branch: `type_` is not None:
   a. Branch: `type_` is a `list`:
      - Branch: all elements are `SecMasterAssetType` -> set `params["type"]` to list of `.value`
      - Branch: otherwise -> raise `ValueError`
   b. Branch: `type_` is a `SecMasterAssetType` -> set `params["type"] = type_.value`
   c. Branch: otherwise -> raise `ValueError`
4. Branch: `country_code` is not None -> normalize and set `params["countryCode"]`
5. Branch: `currency` is not None -> normalize and set `params["currency"]`
6. Branch: `offset_key` is not None -> set `params["offsetKey"]`
7. Branch: `effective_date` is not None -> set `params["effectiveDate"]`
8. Branch: `include_inactive` is not None -> set `params["includeInactive"]`

**Raises:** `ValueError` for invalid `id_value`, `type_`, `country_code`, or `currency` arguments

### GsSecurityMasterApi._get_securities_by_underlyers(id_value: Union[str, list], type_: Union[SecMasterAssetType, list] = None, effective_date: dt.date = None, limit: int = 100, country_code: Union[str, list] = None, currency: Union[str, list] = None, include_inactive: bool = False, offset_key: str = None) -> Optional[dict]
Purpose: Fetch a single page of securities linked to given underlyers.

**API Endpoint:** `GET /markets/securities/underlyers`

**Algorithm:**
1. Build params with `limit`
2. Call `_prepare_underlyers_params(...)` to populate all filters
3. JSON-encode payload
4. GET `/markets/securities/underlyers`
5. Branch: `r['totalResults'] == 0` -> return None
6. Return response

### GsSecurityMasterApi.get_securities_by_underlyers(id_value: Union[str, list], type_: Union[SecMasterAssetType, list] = None, effective_date: dt.date = None, limit: int = None, offset_key: str = None, country_code: Union[str, list] = None, currency: Union[str, list] = None, include_inactive: bool = False, scroll_all_pages: bool = False) -> Optional[dict]
Purpose: Retrieve listed derivative securities by their underlyers, with optional auto-pagination.

**API Endpoint:** `GET /markets/securities/underlyers` (via `_get_securities_by_underlyers`)

**Algorithm:**
1. Define `supported_types = [Equity_Option, Future, Future_Option]`
2. Branch: `type_` is not None:
   a. Branch: `type_` is a single `SecMasterAssetType`:
      - Branch: not in supported_types -> raise `ValueError`
   b. Branch: `type_` is a `list`:
      - For each element: Branch: not in supported_types -> raise `ValueError`
3. Branch: `scroll_all_pages=True`:
   a. If `limit` is None -> set `limit = 500`
   b. Build partial function from `_get_securities_by_underlyers` with all params bound
   c. Call `__fetch_all(fn, offset_key, extract_results=False)`
   d. Compute `as_of_time = max(result['asOfTime'] for result in results)`
   e. Flatten all `result["results"]` into single list
   f. Extract `requestId` from first result (or None)
   g. Return `{"totalResults": len(res), "results": res, "asOfTime": as_of_time, "requestId": request_id}`
4. Branch: `scroll_all_pages=False`:
   a. Delegate directly to `_get_securities_by_underlyers(...)` with all params
   b. Return result

**Raises:** `ValueError` when `type_` contains unsupported asset types

## State Mutation
- `params` dict: Mutated in-place by `prepare_params()` and `_prepare_underlyers_params()`
- No class-level or global state is modified
- Thread safety: All methods are stateless classmethods; thread-safe assuming `GsSession.current` is thread-local

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_many_securities` | Neither `type_` nor `query_params` provided |
| `ValueError` | `get_identifiers` | `secmaster_id` does not start with `"GS"` |
| `ValueError` | `get_many_identifiers` | `ids` not iterable, empty, or contains IDs not starting with `"GS"` |
| `ValueError` | `map` | Both `effective_date` and `start_date`/`end_date` provided |
| `ValueError` | `get_corporate_actions` | `id_type` not in `[GSID, ID]` |
| `ValueError` | `_get_exchanges` | Query parameter key not in `ExchangeId` allowed values |
| `ValueError` | `_prepare_string_or_list_param` | Value is not a string or list of strings |
| `ValueError` | `_prepare_underlyers_params` | `id_value` is None, or `type_` is invalid |
| `ValueError` | `get_securities_by_underlyers` | `type_` contains unsupported `SecMasterAssetType` values |

## Edge Cases
- `get_all_securities` returns early (no pagination) when first page has no `offsetKey` field
- `get_all_securities` returns `None` when `totalResults == 0` even after successful first fetch
- `get_many_identifiers` with `xref_format=True` passes results through `SecmasterXrefFormatter.convert()` which normalizes infinity dates to `'9999-12-31'`
- `__fetch_all` may run indefinitely if `fetch_fn` always returns data with an `offsetKey` (no max-page guard)
- `__capital_structure_aggregate` uses `itertools.groupby` which requires data to be sorted by `issuerId` for correct grouping -- data from the API must already be ordered
- `map` allows `effective_date` alongside neither/both `start_date`/`end_date` -- the validation only triggers when `effective_date` AND at least one of `start_date`/`end_date` are provided
- `get_securities_by_underlyers` with `scroll_all_pages=True` defaults `limit` to 500 if not provided; with `scroll_all_pages=False`, `limit` can remain None (passed through to the underlying call which defaults to 100)

## Bugs Found
- Line 529: `itertools.groupby` is used without prior sorting. If the API response has non-contiguous issuerId values, records with the same issuerId in different positions will be placed into separate groups, leading to duplicate entries in the aggregated results. (OPEN)
- Line 538-541: List comprehension used purely for side-effects (`extend` returns `None`). This works but is non-idiomatic; a for-loop would be clearer. (OPEN -- style)

## Coverage Notes
- Branch count: ~58
- Missing branches: The `__fetch_all` loop's `data is None` path (line 437 false branch) would require a fetch function returning None mid-pagination
- Pragmas: None
