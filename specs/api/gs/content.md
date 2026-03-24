# api/gs/content.py

## Summary
API client for retrieving and decoding content from the GS Content service. Provides methods to query content by channels, asset IDs, author IDs, and tags, with pagination and ordering support. Also provides a helper to base64-decode content bodies. The class `GsContentApi` is a stateless classmethod-based API wrapper around `GsSession` HTTP calls.

## Dependencies
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.target.content` (`ContentResponse`, `GetManyContentsResponse`)
- External: `base64` (`b64decode`), `collections` (`OrderedDict`), `enum` (`Enum`), `typing` (`List`, `Tuple`), `urllib.parse` (`quote`)

## Type Definitions

### OrderBy (Enum)
Inherits: `Enum`

Content ordering direction.

| Member | Value (str) | Description |
|--------|-------------|-------------|
| ASC | `"asc"` | Ascending order |
| DESC | `"desc"` | Descending order |

Custom `__str__` returns the raw string value.

### GsContentApi (class)
Inherits: `object`

Stateless API client. All methods are `@classmethod` or `@staticmethod`. No instance state.

## Enums and Constants

See `OrderBy` in Type Definitions above.

No module-level constants.

## Functions/Methods

### GsContentApi.get_contents(cls, channels: set = None, asset_ids: set = None, author_ids: set = None, tags: set = None, offset: int = 0, limit: int = 10, order_by: dict = {'direction': OrderBy.DESC, 'field': 'createdTime'}) -> List[ContentResponse]
Purpose: Retrieve a list of content items matching the given filters, with pagination and ordering.

**Algorithm:**
1. Branch: if `limit` is truthy and `limit > 1000` -> raise `ValueError`
2. Branch: if `offset` is truthy and (`offset < 0` or `offset >= limit`) -> raise `ValueError`
3. Call `_build_parameters_dict` with keyword arguments, wrapping scalar values (`offset`, `limit`, `order_by`) in single-element lists (or `None` if falsy)
4. Branch: if `parameters_dict` is empty -> `query_string = ''`; otherwise call `_build_query_string`
5. GET `/content{query_string}` deserializing as `GetManyContentsResponse`
6. Return `contents.data`

**Raises:** `ValueError` when limit > 1000 or offset is out of range.

### GsContentApi.get_text(contents: List[ContentResponse]) -> List[Tuple[str, str]]
Purpose: Base64-decode the body of each content item and return (id, decoded_body) tuples.

**Algorithm:**
1. List comprehension: for each `content` in `contents`, produce `(content.id, b64decode(content.content.body))`
2. Return the list

Note: This is a `@staticmethod`.

### GsContentApi._build_parameters_dict(cls, **kwargs) -> dict
Purpose: Build an `OrderedDict` of non-None parameters, each value being a sorted list.

**Algorithm:**
1. Initialize empty dict `parameters`
2. Iterate over `kwargs` items
3. Branch: if `value` is truthy -> `parameters.setdefault(key, []).extend(sorted(value))`
4. Return `OrderedDict(parameters)`

### GsContentApi._build_query_string(cls, parameters: dict) -> str
Purpose: Convert a parameters dict into a URL query string with repeated keys for multi-value params.

**Algorithm:**
1. Initialize `query_string = '?'`
2. Flatten `parameters` dict into a list of `(name, value)` tuples (one tuple per individual value in each list)
3. Iterate with `enumerate` over tuples:
   a. Branch: if `value` is a `str` -> URL-encode via `quote(value.encode())`
   b. Branch: if `name == 'order_by'` -> convert via `_convert_order_by(value)`
   c. Branch: if `index == 0` -> append `name=value`; else append `&name=value`
4. Return `query_string`

### GsContentApi._convert_order_by(cls, order_by: dict) -> str
Purpose: Convert an order_by dict to the Content API's query parameter format (e.g. `>createdTime`).

**Algorithm:**
1. Extract `direction` from `order_by['direction']`
2. Branch: if `direction == OrderBy.DESC` -> prefix is `'>'`
3. Branch: else -> prefix is `'<'`
4. Return `prefix + order_by['field']`

## State Mutation
- No instance state; all methods are classmethods/staticmethods.
- No module-level mutable state.
- Relies on `GsSession.current` for HTTP session (external state).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_contents` | When `limit > 1000` |
| `ValueError` | `get_contents` | When `offset < 0` or `offset >= limit` |

## Edge Cases
- `get_contents` with all default parameters: `offset=0` is falsy so it will not be included in the query params dict; `limit=10` is truthy so it will be wrapped as `[10]`.
- `order_by` default is a mutable dict literal in the function signature (shared across calls) -- standard Python mutable-default-argument concern.
- `_build_parameters_dict` calls `sorted()` on each value, which means the `order_by` list entry (a dict) will be sorted -- sorting dicts by their keys. This works incidentally but is fragile.
- `get_text` returns bytes from `b64decode`, not str, despite the return type annotation claiming `Tuple[str, str]`.

## Bugs Found
- Line 111: `b64decode(content.content.body)` returns `bytes`, but the type annotation says `List[Tuple[str, str]]`. The return type should be `List[Tuple[str, bytes]]`. (OPEN)
- Line 60: `f'Scenario {name}not found'` -- this is in `scenarios.py`, not here. No bugs specific to content.py besides the type annotation mismatch above.

## Coverage Notes
- Branch count: 10
  - `limit > 1000` (true/false)
  - `offset` truthy check (true/false)
  - `offset < 0 or offset >= limit` (true/false for each sub-condition)
  - `parameters_dict` empty check (true/false)
  - `isinstance(value, str)` (true/false)
  - `name == 'order_by'` (true/false)
  - `index == 0` (true/false)
  - `direction == OrderBy.DESC` (true/false)
  - `value` truthiness in `_build_parameters_dict` (true/false)
- Missing branches: None identified
- Pragmas: None
