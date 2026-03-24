# assets.py

## Summary
GS Asset API client providing methods to create, query, resolve, and manage assets, their positions, cross-references (xrefs), entitlements, reports, and identifier mappings within the Goldman Sachs platform. Includes an optional short-term caching layer (`AssetCache`) backed by an in-memory TTL cache, with both synchronous and asynchronous variants for key operations.

## Dependencies
- Internal: `gs_quant.api.api_cache` (ApiRequestCache, InMemoryApiRequestCache), `gs_quant.common` (Entitlements, PositionType), `gs_quant.context_base` (nullcontext), `gs_quant.errors` (MqValueError, MqRateLimitedError, MqTimeoutError, MqInternalServerError), `gs_quant.instrument` (Instrument, Security), `gs_quant.session` (GsSession), `gs_quant.target.assets` (Asset as __Asset, AssetToInstrumentResponse, TemporalXRef, Position, EntityQuery, PositionSet, FieldFilterMap), `gs_quant.target.reports` (Report), `gs_quant.tracing` (Tracer)
- External: `datetime` (date, datetime), `logging`, `os`, `threading` (Lock), `enum` (auto, Enum), `functools` (wraps), `typing` (Iterable, List, Optional, Tuple, Union, Callable), `backoff`, `cachetools` (keys, hashkey), `pandas` (date_range), `pydash` (get, has), `requests.exceptions` (HTTPError)

## Type Definitions

### AssetCache (class)
Inherits: none (implicit `object`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __cache | `ApiRequestCache` | (required) | Underlying cache implementation |
| __ttl | `int` | (required) | Time-to-live in seconds |
| __construct_key_fn | `Callable` | (required) | Function to construct cache keys |

### GsAsset (class)
Inherits: `__Asset` (from `gs_quant.target.assets.Asset`)

Empty subclass used as the GS Asset API object model.

### GsTemporalXRef (class)
Inherits: `TemporalXRef` (from `gs_quant.target.assets`)

Empty subclass used as the GS temporal cross-reference model.

### GsAssetApi (class)
Inherits: none (implicit `object`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _cache | `Optional[AssetCache]` | `None` | Class-level optional cache instance |

### TypeAlias
```
IdList = Union[Tuple[str, ...], List]
```

## Enums and Constants

### GsIdType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ric | `auto()` | Reuters Instrument Code |
| bbid | `auto()` | Bloomberg Barclays ID |
| bcid | `auto()` | Bloomberg Composite ID |
| cusip | `auto()` | CUSIP identifier |
| isin | `auto()` | ISIN identifier |
| sedol | `auto()` | SEDOL identifier |
| mdapi | `auto()` | MDAPI identifier |
| primeId | `auto()` | Prime ID |
| id | `auto()` | Generic ID |
| gsid | `auto()` | Goldman Sachs ID |
| rcic | `auto()` | Reuters Composite Instrument Code |
| ticker | `auto()` | Ticker symbol |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |
| ENABLE_ASSET_CACHING | `str` | `'GSQ_SEC_MASTER_CACHE'` | Environment variable name that enables caching when set |

## Functions/Methods

### get_default_cache() -> AssetCache
Purpose: Create and return a default in-memory `AssetCache` with 30-second TTL and 1024-entry capacity.

**Algorithm:**
1. Set `ttl = 30`
2. Define `in_memory_key_fn(session, *args, **kwargs)`:
   a. Convert any list args to tuples (for hashability)
   b. Convert any list kwargs values to tuples
   c. Return `cachetools.keys.hashkey(session, *args, **kwargs)`
3. Return `AssetCache(cache=InMemoryApiRequestCache(1024, ttl), ttl=ttl, construct_key_fn=in_memory_key_fn)`

### _cached(fn) -> Callable
Purpose: Decorator that adds optional short-term caching to synchronous classmethods, controlled by `GSQ_SEC_MASTER_CACHE` environment variable.

**Algorithm:**
1. Create `_fn_cache_lock = threading.Lock()` and `fallback_cache = get_default_cache()`
2. Define `wrapper(cls, *args, **kwargs)`:
   a. Branch: `ENABLE_ASSET_CACHING` env var is set:
      - Get cache from `cls.get_cache()` or use `fallback_cache`
      - Construct key from `(GsSession.current, fn.__name__, *args, **kwargs)`
      - Acquire lock, check cache for existing result
      - Branch: cache hit -> return cached result
      - Branch: cache miss -> call `fn(cls, *args, **kwargs)`, store result in cache, return result
   b. Branch: env var not set -> call `fn(cls, *args, **kwargs)` directly
3. Return `wrapper`

### _cached_async(fn) -> Callable
Purpose: Decorator that adds optional short-term caching to async classmethods, controlled by `GSQ_SEC_MASTER_CACHE` environment variable.

**Algorithm:**
1. Same as `_cached` but `wrapper` is `async` and uses `await fn(cls, *args, **kwargs)`

### AssetCache.__init__(self, cache: ApiRequestCache, ttl: int, construct_key_fn: Callable) -> None
Purpose: Initialize the cache with its backing store, TTL, and key-construction function.

**Algorithm:**
1. Set `self.__cache = cache`
2. Set `self.__ttl = ttl`
3. Set `self.__construct_key_fn = construct_key_fn`

### AssetCache.ttl (property) -> int
Purpose: Return the cache time-to-live in seconds.

### AssetCache.cache (property) -> ApiRequestCache
Purpose: Return the underlying cache implementation.

### AssetCache.construct_key_fn (property) -> Callable
Purpose: Return the key-construction function.

### AssetCache.construct_key(self, session: GsSession, *args, **kwargs) -> Any
Purpose: Construct a cache key by delegating to the stored key function.

**Algorithm:**
1. Return `self.construct_key_fn(session, *args, **kwargs)`

### GsAssetApi.set_cache(cache: AssetCache) -> None
Purpose: Set the class-level cache instance.

**Algorithm:**
1. Set `cls._cache = cache`

### GsAssetApi.get_cache() -> Optional[AssetCache]
Purpose: Return the class-level cache instance, or None.

### GsAssetApi.__create_query(fields: Union[List, Tuple] = None, as_of: dt.datetime = None, limit: int = None, scroll: str = None, scroll_id: str = None, order_by: List[str] = None, **kwargs) -> EntityQuery
Purpose: Build and validate an `EntityQuery` object from keyword arguments.

**Algorithm:**
1. Compute `valid` = intersection of `kwargs` keys with `FieldFilterMap.properties()`
2. Compute `invalid` = difference of `kwargs` keys and `valid`
3. Branch: `invalid` is non-empty -> raise `KeyError` with formatted bad arguments
4. Return `EntityQuery(where=FieldFilterMap(**kwargs), fields=fields, asOfTime=as_of or dt.datetime.utcnow(), limit=limit, scroll=scroll, scroll_id=scroll_id, order_by=order_by)`

**Raises:** `KeyError` when invalid asset query arguments are provided

### GsAssetApi._set_tags(scope, kwargs) -> None
Purpose: Set tracing span tags from keyword arguments for observability.

**Algorithm:**
1. Branch: `kwargs` AND `scope` AND `scope.span` are all truthy:
   a. For each `(k, v)` in `kwargs`:
      - Branch: `v` is list/tuple:
        - Branch: `len(v) > 5` -> tag with length count
        - Branch: `len(v) <= 5` -> tag with comma-joined string values
      - Branch: `v` is int/float/bool/str -> tag with value directly

### GsAssetApi.get_many_assets(fields: IdList = None, as_of: dt.datetime = None, limit: int = 100, return_type: Optional[type] = GsAsset, order_by: List[str] = None, **kwargs) -> Union[Tuple[GsAsset, ...], Tuple[dict, ...]]
Purpose: Query multiple assets with filters and return a single page of results.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/query`

**Algorithm:**
1. Get active tracing span; create tracer context if recording
2. Set tags on span from kwargs
3. Build query via `__create_query(fields, as_of, limit, order_by=order_by, **kwargs)`
4. POST `/assets/query` with payload=query, cls=return_type
5. Return `response['results']`

### GsAssetApi.get_many_assets_async(fields: IdList = None, as_of: dt.datetime = None, limit: int = 100, return_type: Optional[type] = GsAsset, order_by: List[str] = None, **kwargs) -> Union[Tuple[GsAsset, ...], Tuple[dict, ...]]
Purpose: Async variant of `get_many_assets`.

**Decorators:** `@classmethod`, `@_cached_async`

**API Endpoint:** `POST /assets/query` (async)

**Algorithm:**
1. Same as `get_many_assets` but uses `await GsSession.current.async_.post(...)`

### GsAssetApi.get_many_assets_scroll(scroll: str = '1m', fields: IdList = None, as_of: dt.datetime = None, limit: int = 1000, return_type: Optional[type] = GsAsset, order_by: List[str] = None, **kwargs) -> Union[Tuple[GsAsset, ...], Tuple[dict, ...]]
Purpose: Query multiple assets with auto-scrolling pagination to retrieve all results.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/query` (multiple calls with `scrollId`)

**Algorithm:**
1. Get active tracing span; create tracer context if recording
2. Set tags on span from kwargs
3. Build initial query via `__create_query(fields, as_of, limit, scroll, order_by=order_by, **kwargs)`
4. POST `/assets/query`
5. Extract `results` using `pydash.get(response, 'results')`
6. Loop while response has `scrollId` AND `results` is non-empty:
   a. Build new query including `scroll_id`
   b. POST `/assets/query`
   c. Concatenate new results
7. Return accumulated results

### GsAssetApi.get_many_assets_data(fields: IdList = None, as_of: dt.datetime = None, limit: int = None, source: Optional[str] = None, **kwargs) -> dict
Purpose: Query asset data (non-model form) with optional source-based headers.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/data/query`

**Algorithm:**
1. Get active tracing span; create tracer context if recording
2. Set tags on span from kwargs
3. Build query via `__create_query(fields, as_of, limit, **kwargs)`
4. Branch: `source == "Basket"` -> set `request_headers = {'X-Application': 'Studio'}`; else -> `None`
5. POST `/assets/data/query` with payload and optional headers
6. Return `response['results']`

### GsAssetApi.get_many_assets_data_async(fields: IdList = None, as_of: dt.datetime = None, limit: int = None, **kwargs) -> dict
Purpose: Async variant of `get_many_assets_data` (without source parameter).

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/data/query` (async)

**Algorithm:**
1. Same as `get_many_assets_data` but uses `await GsSession.current.async_.post(...)` and has no `source` parameter

### GsAssetApi.get_many_assets_data_scroll(scroll: str = '1m', fields: IdList = None, as_of: dt.datetime = None, limit: int = None, source: Optional[str] = None, **kwargs) -> dict
Purpose: Query asset data with auto-scrolling pagination and optional source-based headers.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/data/query` (multiple calls with `scrollId`)

**Algorithm:**
1. Get active tracing span; create tracer context if recording
2. Set tags on span from kwargs
3. Build initial query via `__create_query(fields, as_of, limit, scroll, **kwargs)`
4. Branch: `source == "Basket"` -> set `request_headers = {'X-Application': 'Studio'}`; else -> `None`
5. POST `/assets/data/query` with headers
6. Extract results using `pydash.get(response, 'results')`
7. Loop while response has `scrollId` AND `results` is non-empty:
   a. Build new query including `scroll_id`
   b. POST `/assets/data/query` with headers
   c. Concatenate new results
8. Return accumulated results

### GsAssetApi.resolve_assets(identifier: [str], fields: IdList = [], limit: int = 100, as_of: dt.datetime = dt.datetime.today(), **kwargs) -> Tuple[dict, ...]
Purpose: Resolve asset identifiers to asset objects via the positions resolver.

**Decorators:** `@classmethod`, `@_cached`, `@backoff.on_exception(expo(base=2, factor=2), (MqTimeoutError, MqInternalServerError), max_tries=5)`, `@backoff.on_exception(constant(90), MqRateLimitedError, max_tries=5)`

**API Endpoint:** `POST /positions/resolver`

**Algorithm:**
1. Build `where = dict(identifier=identifier, **kwargs)`
2. Build `query = dict(where=where, limit=limit, fields=fields, asOfTime=as_of.strftime(...))`
3. POST `/positions/resolver`
4. Return response

### GsAssetApi.get_many_asset_xrefs(identifier: [str], fields: IdList = [], limit: int = 100, as_of: dt.datetime = dt.datetime.today(), **kwargs) -> Tuple[dict, ...]
Purpose: Query cross-reference data for multiple assets.

**API Endpoint:** `POST /assets/xrefs/query`

**Algorithm:**
1. Build `where` and `query` dicts (same pattern as `resolve_assets`)
2. POST `/assets/xrefs/query`
3. Return `response.get('results')`

### GsAssetApi.get_asset_xrefs(asset_id: str) -> Tuple[GsTemporalXRef, ...]
Purpose: Get temporal cross-references for a specific asset.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `GET /assets/{id}/xrefs`

**Algorithm:**
1. GET `/assets/{asset_id}/xrefs`
2. Return tuple of `GsTemporalXRef.from_dict(x)` for each xref in response

### GsAssetApi.put_asset_xrefs(asset_id: str, xrefs: List[TemporalXRef]) -> Any
Purpose: Update cross-references for a specific asset.

**API Endpoint:** `PUT /assets/{asset_id}/xrefs`

**Algorithm:**
1. PUT `/assets/{asset_id}/xrefs` with xrefs as payload
2. Return response

### GsAssetApi.get_asset(asset_id: str) -> GsAsset
Purpose: Get a single asset by ID.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `GET /assets/{id}`

**Algorithm:**
1. GET `/assets/{asset_id}` with `cls=GsAsset`
2. Return deserialized GsAsset

### GsAssetApi.get_asset_async(asset_id: str) -> GsAsset
Purpose: Async variant of `get_asset`.

**Decorators:** `@classmethod`, `@_cached_async`

**API Endpoint:** `GET /assets/{id}` (async)

### GsAssetApi.get_asset_by_name(name: str) -> GsAsset
Purpose: Find a single asset by exact name match.

**API Endpoint:** `GET /assets?name={name}`

**Algorithm:**
1. GET `/assets?name={name}`
2. Branch: `totalResults == 0` -> raise `ValueError('Asset {} not found')`
3. Branch: `totalResults > 1` -> raise `ValueError('More than one asset named {} found')`
4. Branch: exactly 1 result -> return `GsAsset.from_dict(ret['results'][0])`

**Raises:** `ValueError` when zero or more than one asset found

### GsAssetApi.create_asset(asset: GsAsset) -> GsAsset
Purpose: Create a new asset.

**API Endpoint:** `POST /assets`

**Algorithm:**
1. POST `/assets` with asset payload, cls=GsAsset
2. Return created asset

### GsAssetApi.delete_asset(asset_id: str) -> Any
Purpose: Delete an asset by ID.

**API Endpoint:** `DELETE /assets/{asset_id}`

**Algorithm:**
1. DELETE `/assets/{asset_id}`
2. Return response

### GsAssetApi.get_position_dates(asset_id: str) -> Tuple[dt.date, ...]
Purpose: Get all available position dates for an asset.

**API Endpoint:** `GET /assets/{asset_id}/positions/dates`

**Algorithm:**
1. GET `/assets/{asset_id}/positions/dates`
2. Parse each date string to `dt.date` via `strptime`
3. Return tuple of dates

### GsAssetApi.get_asset_positions_for_date(asset_id: str, position_date: dt.date, position_type: PositionType = None) -> Tuple[PositionSet, ...]
Purpose: Get positions for an asset on a specific date.

**API Endpoint:** `GET /assets/{asset_id}/positions/{date}`

**Algorithm:**
1. Format `position_date` as ISO string
2. Build URL `/assets/{asset_id}/positions/{date}`
3. Branch: `position_type` is not None:
   - Branch: is `str` -> append `?type={position_type}`
   - Branch: is `PositionType` -> append `?type={position_type.value}`
4. GET the URL
5. Return tuple of `PositionSet.from_dict(r)` for each result

### GsAssetApi.get_asset_positions_for_dates(asset_id: str, start_date: dt.date, end_date: dt.date, position_type: PositionType = PositionType.CLOSE) -> Tuple[PositionSet, ...]
Purpose: Get positions for an asset across a date range, splitting into 30-day periods for large ranges.

**API Endpoint:** `GET /assets/{asset_id}/positions?startDate=...&endDate=...&type=...`

**Algorithm:**
1. Normalize `position_type` to string value
2. Compute `periods = (end_date - start_date).days // 30`
3. Branch: `periods > 1`:
   a. Generate `end_dates` via `pd.date_range(start=start_date, end=end_date, periods=periods, inclusive='right')`
   b. Loop through each period end date:
      - GET positions for the sub-range
      - Accumulate `positionSets`
      - Update `start_date_str` to day after current end date
      - Branch: `HTTPError` -> raise `ValueError` with context
4. Branch: `periods <= 1`:
   a. GET positions for the full range
   b. Branch: `HTTPError` -> raise `ValueError` with context
5. Return tuple of `PositionSet.from_dict(r)` for each position set

**Raises:** `ValueError` wrapping `HTTPError` when position fetch fails

### GsAssetApi.get_latest_positions(asset_id: str, position_type: PositionType = None) -> PositionSet
Purpose: Get the latest positions for an asset.

**API Endpoint:** `GET /assets/{asset_id}/positions/last`

**Algorithm:**
1. Build URL `/assets/{id}/positions/last`
2. Branch: `position_type` is not None AND `position_type is not PositionType.ANY`:
   - Append `?type=` with string or enum value
3. GET the URL
4. Return `PositionSet.from_dict(results)`

### GsAssetApi.get_or_create_asset_from_instrument(instrument: Instrument) -> str
Purpose: Create an asset from an instrument definition and return its ID.

**API Endpoint:** `POST /assets`

**Algorithm:**
1. Construct `GsAsset` with instrument's `asset_class`, `type_`, `name` (or `''`), and `parameters` as camel-case dict
2. POST `/assets` with the asset
3. Return `results['id']`

### GsAssetApi.get_instruments_for_asset_ids(asset_ids: Tuple[str, ...]) -> Tuple[Optional[Union[Instrument, Security]]]
Purpose: Retrieve instrument definitions for a batch of asset IDs.

**API Endpoint:** `POST /assets/instruments`

**Algorithm:**
1. POST `/assets/instruments` with `asset_ids`, cls=AssetToInstrumentResponse
2. Build lookup dict `{i.assetId: i.instrument}` for non-None results
3. Return tuple mapping each asset_id to its instrument (or None)

### GsAssetApi.get_instruments_for_positions(positions: Iterable[Position]) -> Tuple[Optional[Union[Instrument, Security]]]
Purpose: Retrieve instruments for positions, using either the position's own instrument or looking up by asset ID, with quantity assignment to the size field.

**API Endpoint:** `POST /assets/instruments`

**Algorithm:**
1. Extract non-None `asset_ids` from positions
2. Branch: `asset_ids` is non-empty -> POST `/assets/instruments`; else -> use empty dict
3. Build lookup dict `{i.assetId: (i.instrument, i.sizeField)}`
4. For each position:
   a. Branch: `position.instrument` exists -> use it directly
   b. Branch: no instrument on position:
      - Lookup by `position.assetId`
      - Branch: found and `instrument`, `size_field`, and `getattr(instrument, size_field)` is None -> `setattr(instrument, size_field, position.quantity)`
   c. Append instrument (or None) to result tuple
5. Return result tuple

### GsAssetApi.get_asset_positions_data(asset_id: str, start_date: dt.date, end_date: dt.date, fields: IdList = None, position_type: PositionType = None) -> List[dict]
Purpose: Get raw position data for an asset across a date range.

**API Endpoint:** `GET /assets/{id}/positions/data?startDate=...&endDate=...`

**Algorithm:**
1. Build URL with `asset_id`, `start_date`, `end_date`
2. Branch: `fields` is not None -> append `&fields=` joined fields
3. Branch: `position_type` is not None -> append `&type=` value
4. GET the URL
5. Return `results`

### GsAssetApi.update_asset_entitlements(asset_id: str, entitlements: Entitlements) -> dict
Purpose: Update entitlements for an asset.

**API Endpoint:** `PUT /assets/{asset_id}/entitlements`

**Algorithm:**
1. PUT `/assets/{asset_id}/entitlements` with entitlements payload
2. Branch: `HTTPError` -> raise `ValueError` wrapping the error
3. Return results

**Raises:** `ValueError` wrapping `HTTPError`

### GsAssetApi.get_reports(asset_id: str) -> Tuple[Report, ...]
Purpose: Get reports associated with an asset.

**API Endpoint:** `GET /assets/{asset_id}/reports`

**Algorithm:**
1. GET `/assets/{asset_id}/reports` with cls=Report
2. Return `response['results']`

### GsAssetApi.map_identifiers(input_type: Union[GsIdType, str], output_type: Union[GsIdType, str], ids: IdList, as_of: dt.datetime = None, multimap: bool = False, limit: int = None, **kwargs) -> dict
Purpose: Map identifiers from one type to another, with optional multi-mapping support.

**Decorators:** `@classmethod`, `@_cached`

**API Endpoint:** `POST /assets/data/query`

**Algorithm:**
1. Branch: `input_type` is `GsIdType` -> convert to `.name`
2. Branch: `input_type` is `str` -> use directly
3. Branch: otherwise -> raise `ValueError`
4. Same conversion for `output_type`
5. Add `ids` to kwargs under `input_type` key
6. Set `limit = limit or 4 * len(ids)`
7. Build query with `(input_type, output_type)` as fields
8. POST `/assets/data/query`
9. Branch: `len(results) >= query.limit` -> raise `MqValueError('number of results may have exceeded capacity')`
10. Branch: `'results'` key in results -> unwrap
11. For each entry in results:
    - Extract `key = entry.get(input_type)` and `value = entry.get(output_type)`
    - Branch: `multimap=True` -> append value to list under key using `setdefault`
    - Branch: `multimap=False`:
      - Branch: key already in `out` -> log warning about duplicate mapping
      - Set `out[key] = value`
12. Return `out`

**Raises:** `ValueError` when `input_type` or `output_type` is not `GsIdType` or `str`; `MqValueError` when result count may have exceeded capacity

## State Mutation
- `GsAssetApi._cache`: Class-level field, set by `set_cache()`, read by `get_cache()`
- `_fn_cache_lock`: Thread lock inside each `_cached`/`_cached_async` decorator closure; protects cache reads/writes
- `instrument.{size_field}`: Mutated via `setattr` in `get_instruments_for_positions()` when size field is None
- Thread safety: Cache operations are lock-protected. `GsSession.current` assumed thread-local. The `_cached` decorator uses a per-decorated-function `threading.Lock`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `__create_query` | Invalid kwargs keys not in `FieldFilterMap.properties()` |
| `ValueError` | `get_asset_by_name` | Zero or more than one asset with that name |
| `ValueError` | `map_identifiers` | `input_type`/`output_type` not `GsIdType` or `str` |
| `MqValueError` | `map_identifiers` | Result count >= query limit (potential truncation) |
| `ValueError` | `get_asset_positions_for_dates` | HTTP error fetching positions (wraps `HTTPError`) |
| `ValueError` | `update_asset_entitlements` | HTTP error updating entitlements (wraps `HTTPError`) |
| `MqTimeoutError` | `resolve_assets` (via backoff retry) | Timeout from API -- retried up to 5 times with exponential backoff |
| `MqInternalServerError` | `resolve_assets` (via backoff retry) | Server error from API -- retried up to 5 times with exponential backoff |
| `MqRateLimitedError` | `resolve_assets` (via backoff retry) | Rate limited -- retried up to 5 times at 90-second intervals |

## Edge Cases
- `_cached` and `_cached_async` only activate when the `GSQ_SEC_MASTER_CACHE` environment variable is set (any truthy value)
- `get_many_assets_scroll` and `get_many_assets_data_scroll` stop scrolling when `results` from a page is empty, even if `scrollId` is present
- `resolve_assets` uses `dt.datetime.today()` as a default parameter -- this is evaluated at function definition time in Python (mutable default argument pattern) but since it uses `today()` which creates a new object, it is re-evaluated each time in practice (though the annotation is misleading)
- `get_asset_positions_for_dates` splits date ranges into periods when `(end_date - start_date).days // 30 > 1`; for ranges of 31-60 days, it does NOT split (periods=1)
- `get_asset_positions_data` builds the fields URL parameter using `'&fields='.join([''] + fields)` which produces `&fields=field1&fields=field2` -- each field gets its own query parameter
- `get_latest_positions` skips the type parameter when `position_type is PositionType.ANY` (identity check, not equality)
- `map_identifiers` modifies the incoming `kwargs` dict in-place by assigning `the_args[input_type] = ids`

## Bugs Found
- Line 316: `get_many_assets_data_async` is decorated with `@_cached` (synchronous cache decorator) instead of `@_cached_async`. Since the function is `async`, the synchronous wrapper will return a coroutine object rather than awaiting it, causing the cache to store unawaited coroutines. (OPEN)
- Lines 363-364: `resolve_assets` uses `dt.datetime.today()` as a default parameter value. While Python evaluates default arguments at definition time, `datetime.today()` returns the current time at import. This means the default `as_of` is frozen to whenever the module was first imported. Same issue on line 377 for `get_many_asset_xrefs`. (OPEN)

## Coverage Notes
- Branch count: ~52
- Missing branches: The `_cached_async` decorator's enabled path is hard to test without setting the env var and mocking async session calls
- Pragmas: None
