# api/gs/base_screener.py

## Summary
API client for managing screeners via the GS Data Screeners service. Provides full CRUD operations (create, read, update, delete) plus data publishing and clearing for screener instances. All methods are classmethods on `GsBaseScreenerApi` that delegate to `GsSession` HTTP calls against the `/data/screeners` endpoint.

## Dependencies
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.target.base_screener` (`Screener`)
- External: `logging` (`getLogger`), `typing` (`Tuple`, `Dict`, `Any`, `List`)

## Type Definitions

### GsBaseScreenerApi (class)
Inherits: `object`

Stateless API client. All methods are `@classmethod`. No instance state.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsBaseScreenerApi.get_screeners(cls) -> Tuple[Screener, ...]
Purpose: Retrieve all screeners accessible to the current user.

**Algorithm:**
1. GET `/data/screeners` deserializing as `Screener`
2. Return `['results']` from the response

### GsBaseScreenerApi.get_screener(cls, screener_id: str) -> Screener
Purpose: Retrieve a single screener by its ID.

**Algorithm:**
1. GET `/data/screeners/{screener_id}` deserializing as `Screener`
2. Return the deserialized `Screener` object

### GsBaseScreenerApi.create_screener(cls, screener: Screener) -> Screener
Purpose: Create a new screener from a Screener object.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/data/screeners` with `screener` payload, deserializing as `Screener`
3. Return the new `Screener` object

### GsBaseScreenerApi.edit_screener(cls, screener_id: str, screener: Screener) -> Screener
Purpose: Overwrite an existing screener's schema with the provided Screener object.

**Algorithm:**
1. Assert `screener_id == screener.id` -- raises `AssertionError` if mismatched
2. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
3. PUT `/data/screeners/{screener_id}` with `screener` payload, deserializing as `Screener`
4. Return the updated `Screener` object

**Raises:** `AssertionError` when `screener_id != screener.id`

### GsBaseScreenerApi.publish_to_screener(cls, screener_id: str, data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]
Purpose: Publish additional data rows to an existing screener.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/data/screeners/{screener_id}/publish` with `data` payload
3. Return `['data']` from the response

### GsBaseScreenerApi.clear_screener(cls, screener_id: str) -> Dict[str, Any]
Purpose: Clear all data from a screener without deleting the screener itself.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/data/screeners/{screener_id}/clear` with empty dict `{}` payload
3. Return the response dict

### GsBaseScreenerApi.delete_screener(cls, screener_id: str) -> None
Purpose: Permanently delete a screener and all its data.

**Algorithm:**
1. DELETE `/data/screeners/{screener_id}`
2. Return the response (annotated as `None` but actually returns whatever `GsSession.delete` returns)

## State Mutation
- No instance state; all methods are classmethods.
- No module-level mutable state (aside from `_logger`).
- Relies on `GsSession.current` for HTTP session (external state).
- `edit_screener` and `create_screener` mutate server-side screener state.
- `publish_to_screener` appends data server-side.
- `clear_screener` removes all data server-side.
- `delete_screener` removes the screener server-side.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `AssertionError` | `edit_screener` | When `screener_id != screener.id` |

## Edge Cases
- `edit_screener` uses `assert` for validation, which can be disabled with Python's `-O` flag. In Elixir, this should be a proper guard/validation.
- `delete_screener` returns `None` per annotation but actually returns the HTTP response. The Elixir port should decide on `:ok` vs `{:ok, response}`.
- `publish_to_screener` accesses `['data']` key on the response -- will raise `KeyError` if the server response lacks that key.
- `get_screeners` accesses `['results']` key on the response -- will raise `KeyError` if the server response lacks that key.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: 1
  - `assert screener_id == screener.id` (pass/fail) in `edit_screener`
- Missing branches: None identified
- Pragmas: None
