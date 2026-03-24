# api/gs/data_screen.py

## Summary
API client for managing analytics screens via the GS Data Screens service. Provides CRUD operations (create, read, update, delete), column metadata retrieval, and filtered data queries for `AnalyticsScreen` instances. All methods are classmethods on `GsDataScreenApi` that delegate to `GsSession` HTTP calls against the `/data/screens` endpoint.

## Dependencies
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.target.data_screen` (`AnalyticsScreen`, `FilterRequest`, `DataRow`)
- External: `logging` (`getLogger`), `typing` (`Tuple`, `Dict`)

## Type Definitions

### GsDataScreenApi (class)
Inherits: `object`

Stateless API client. All methods are `@classmethod`. No instance state.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsDataScreenApi.get_screens(cls) -> Tuple[AnalyticsScreen, ...]
Purpose: Retrieve all screens accessible to the current user.

**Algorithm:**
1. GET `/data/screens` deserializing as `AnalyticsScreen`
2. Return `['results']` from the response

### GsDataScreenApi.get_screen(cls, screen_id: str) -> AnalyticsScreen
Purpose: Retrieve a single screen by its ID.

**Algorithm:**
1. GET `/data/screens/{screen_id}` deserializing as `AnalyticsScreen`
2. Return the deserialized `AnalyticsScreen` object

### GsDataScreenApi.get_column_info(cls, screen_id: str) -> Dict[str, Dict]
Purpose: Retrieve column metadata for a screen, useful for constructing filters.

**Algorithm:**
1. GET `/data/screens/{screen_id}/filters`
2. Return `['aggregations']` from the response

### GsDataScreenApi.delete_screen(cls, screen_id: str) -> None
Purpose: Permanently delete a screen and its data.

**Algorithm:**
1. DELETE `/data/screens/{screen_id}`
2. Return the response (annotated as `None`)

### GsDataScreenApi.create_screen(cls, screen: AnalyticsScreen) -> AnalyticsScreen
Purpose: Create a new screen from an AnalyticsScreen object.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/data/screens` with `screen` payload, deserializing as `AnalyticsScreen`
3. Return the new `AnalyticsScreen` object

### GsDataScreenApi.filter_screen(cls, screen_id: str, filter_request: FilterRequest) -> Tuple[DataRow, ...]
Purpose: Query filtered data rows from a screen, temporarily overriding the screen's saved filters.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/data/screens/{screen_id}/filter` with `filter_request` payload, deserializing as `DataRow`
3. Return `['results']` from the response

### GsDataScreenApi.update_screen(cls, screen_id: str, screen: AnalyticsScreen) -> AnalyticsScreen
Purpose: Overwrite an existing screen with the provided AnalyticsScreen object, including its filters.

**Algorithm:**
1. Assert `screen_id == screen.id_` -- raises `AssertionError` if mismatched
2. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
3. PUT `/data/screens/{screen_id}` with `screen` payload, deserializing as `AnalyticsScreen`
4. Return the updated `AnalyticsScreen` object

**Raises:** `AssertionError` when `screen_id != screen.id_`

## State Mutation
- No instance state; all methods are classmethods.
- No module-level mutable state (aside from `_logger`).
- Relies on `GsSession.current` for HTTP session (external state).
- `create_screen` and `update_screen` mutate server-side screen state.
- `delete_screen` removes the screen server-side.
- `filter_screen` is read-only (does not modify the screen's saved filters).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `AssertionError` | `update_screen` | When `screen_id != screen.id_` |

## Edge Cases
- `update_screen` uses `assert` for validation, which can be disabled with Python's `-O` flag. In Elixir, this should be a proper guard/validation.
- `delete_screen` returns `None` per annotation but actually returns whatever `GsSession.delete` returns. The Elixir port should decide on `:ok` vs `{:ok, response}`.
- `filter_screen` accesses `['results']` key -- will raise `KeyError` if server response lacks that key.
- `get_screens` accesses `['results']` key -- will raise `KeyError` if server response lacks that key.
- `get_column_info` accesses `['aggregations']` key -- will raise `KeyError` if server response lacks that key.
- Note: `update_screen` compares against `screen.id_` (with trailing underscore), while `base_screener.edit_screener` compares against `screener.id` (no underscore). This reflects a difference in the target dataclass field naming between `AnalyticsScreen` and `Screener`.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: 1
  - `assert screen_id == screen.id_` (pass/fail) in `update_screen`
- Missing branches: None identified
- Pragmas: None
