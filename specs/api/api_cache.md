# api_cache.py

## Summary
Provides an abstract caching layer for API requests (`ApiRequestCache`) and a concrete in-memory implementation (`InMemoryApiRequestCache`) backed by a TTL cache. The cache supports get/put operations keyed by arbitrary values, with event recording for observability. This is the core request-caching infrastructure used to avoid redundant API calls within a session.

## Dependencies
- Internal: `gs_quant.base` (Base), `gs_quant.session` (GsSession)
- External: `abc` (ABC, abstractmethod), `enum` (Enum), `typing` (Any, Tuple), `cachetools` (TTLCache), `pandas` (DataFrame)

## Type Definitions

### ApiRequestCache (ABC)
Abstract base class defining the cache contract. Subclasses must implement `_get` and `_put`. The public `get` and `put` methods add event recording around the internal implementations.

### InMemoryApiRequestCache
Inherits: ApiRequestCache

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_cache` | `cachetools.TTLCache` | `TTLCache(max_size, ttl_in_seconds)` | The actual TTL-based cache store |
| `_records` | `list` | `[]` | List of `(CacheEvent, key)` tuples recording cache interactions |

## Enums and Constants

### CacheEvent(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| PUT | `"Put"` | A value was stored in the cache |
| GET | `"Get"` | A value was successfully retrieved from the cache (cache hit) |

## Functions/Methods

### ApiRequestCache.get(self, session: GsSession, key: Any, **kwargs) -> Any
Purpose: Look up a value in the cache and record a GET event on cache hits.

**Algorithm:**
1. Call `self._get(session, key, **kwargs)` to perform the actual lookup
2. Branch: if result is not `None` -> call `self.record(session, key, CacheEvent.GET, **kwargs)`
3. Return the lookup result (may be `None` on miss)

### ApiRequestCache._get(self, session: GsSession, key: Any, **kwargs) [abstract]
Purpose: Abstract internal lookup. Must be implemented by subclasses.

### ApiRequestCache.record(self, session: GsSession, key: Any, method: CacheEvent, **kwargs)
Purpose: Default no-op event recorder. Subclasses may override to track cache events.

**Algorithm:**
1. No-op (pass)

### ApiRequestCache.put(self, session: GsSession, key: Any, value, **kwargs)
Purpose: Store a value in the cache and record a PUT event.

**Algorithm:**
1. Call `self._put(session, key, value, **kwargs)` to perform the actual storage
2. Call `self.record(session, key, CacheEvent.PUT, **kwargs)`

### ApiRequestCache._put(self, session: GsSession, key: Any, value, **kwargs) [abstract]
Purpose: Abstract internal storage. Must be implemented by subclasses.

### InMemoryApiRequestCache.__init__(self, max_size=1000, ttl_in_seconds=3600)
Purpose: Initialize the in-memory cache with configurable size and TTL.

**Algorithm:**
1. Create `self._cache` as `cachetools.TTLCache(max_size, ttl_in_seconds)`
2. Create `self._records` as empty list

### InMemoryApiRequestCache.get_events(self) -> Tuple[Tuple[CacheEvent, Any], ...]
Purpose: Return an immutable snapshot of all recorded cache events.

**Algorithm:**
1. Return `tuple(self._records)`

### InMemoryApiRequestCache.clear_events(self)
Purpose: Clear all recorded cache events.

**Algorithm:**
1. Call `self._records.clear()`

### InMemoryApiRequestCache._make_str_key(self, key: Any) -> str
Purpose: Recursively convert an arbitrary key into a deterministic string representation for use as a cache key.

**Algorithm:**
1. Branch: if `key` is `list` or `tuple` -> join `_make_str_key(k)` for each element with `"_"`
2. Branch: elif `key` is `Base` or `pd.DataFrame` -> return `key.to_json()`
3. Branch: elif `key` is `dict` -> recursively call `_make_str_key(list(key.items()))`
4. Branch: else (fallback) -> return `str(key)`

### InMemoryApiRequestCache._get(self, session: GsSession, key: Any, **kwargs) -> Any
Purpose: Look up a value by converting the key to a string and querying the TTL cache.

**Algorithm:**
1. Return `self._cache.get(self._make_str_key(key))`

### InMemoryApiRequestCache.record(self, session: GsSession, key: Any, method: CacheEvent, **kwargs)
Purpose: Append a cache event to the internal records list.

**Algorithm:**
1. Append `(method, key)` to `self._records`

### InMemoryApiRequestCache._put(self, session: GsSession, key: Any, value, **kwargs)
Purpose: Store a value in the TTL cache under a string-converted key.

**Algorithm:**
1. Set `self._cache[self._make_str_key(key)] = value`

## State Mutation
- `self._cache`: Modified by `_put` (adds entries), also auto-evicted by `cachetools.TTLCache` on expiry or when exceeding `max_size`
- `self._records`: Appended to by `record` on every `get` (hit) and `put` call; cleared by `clear_events`
- Thread safety: `cachetools.TTLCache` is NOT thread-safe. Concurrent access to the same `InMemoryApiRequestCache` instance from multiple threads requires external synchronization. The `_records` list is also not thread-safe for concurrent append/read.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None directly raised | - | Cache misses return `None` silently; no exceptions are raised by caching operations |

## Edge Cases
- `get` returns `None` on cache miss and does NOT record a GET event (only hits are recorded)
- `_make_str_key` with a nested structure like `[{"a": [1, 2]}, "b"]` recurses through list -> dict -> list -> int paths
- `_make_str_key` with an empty list returns `""` (joining zero elements)
- `_make_str_key` with an empty dict converts to `_make_str_key([])` which returns `""`
- A cached value that is itself `None` would be treated as a cache miss by `get` (since the `is not None` check cannot distinguish "not found" from "found None")
- TTL expiry happens lazily in `cachetools` -- entries expire on next access, not on a background timer

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 8
- Key branches: `cache_lookup is not None` in `get`, 4 type-dispatch branches in `_make_str_key` (list/tuple, Base/DataFrame, dict, fallback)
- Pragmas: none
