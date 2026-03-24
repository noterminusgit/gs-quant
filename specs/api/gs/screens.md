# screens.py

## Summary
API client wrapper for GS Screens and Asset Screener endpoints. Provides full CRUD for screens, plus methods to get screener filter options and execute screener calculations.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.screens` (Screen), `gs_quant.target.assets_screener` (AssetScreenerRequest)
- External: `logging`, `typing` (Tuple, List)

## Type Definitions
None defined in this module. Uses `Screen` from `gs_quant.target.screens` and `AssetScreenerRequest` from `gs_quant.target.assets_screener`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsScreenApi.get_screens(cls, screen_ids: List[str] = None, screen_names: List[str] = None, limit: int = 100) -> Tuple[Screen, ...]
Purpose: Fetch screens with optional filtering by IDs and/or names.

**Algorithm:**
1. Build base URL `/screens?`
2. Branch: if `screen_ids` -> append `&id=` joined params
3. Branch: if `screen_names` -> append `&name=` joined params
4. Append `&limit={limit}`
5. GET with `cls=Screen`, return `['results']`

### GsScreenApi.get_screen(cls, screen_id: str) -> Screen
Purpose: Fetch a single screen by ID.

**Algorithm:**
1. GET `/screens/{screen_id}` with `cls=Screen`
2. Return response

### GsScreenApi.create_screen(cls, screen: Screen) -> Screen
Purpose: Create a new screen.

**Algorithm:**
1. POST `/screens` with screen payload, `cls=Screen`
2. Return response

### GsScreenApi.update_screen(cls, screen: Screen) -> Screen
Purpose: Update an existing screen. Uses `screen.id` for the URL path.

**Algorithm:**
1. PUT `/screens/{screen.id}` with screen payload, `cls=Screen`
2. Return response

### GsScreenApi.delete_screen(cls, screen_id: str) -> str
Purpose: Delete a screen by ID.

**Algorithm:**
1. DELETE `/screens/{screen_id}`
2. Return response

### GsScreenApi.get_filter_options(cls) -> dict
Purpose: Get available filter options for the asset screener.

**Algorithm:**
1. GET `/assets/screener/options`
2. Return response

### GsScreenApi.calculate(cls, payload: AssetScreenerRequest) -> dict
Purpose: Execute an asset screener calculation.

**Algorithm:**
1. POST `/assets/screener` with payload
2. Return response

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_screens` | If response lacks `'results'` key |

## Edge Cases
- `get_screens` with no filters returns all screens up to limit
- URL for `get_screens` starts with `?` then optional `&id=...` params, producing `?&id=...` which is valid but unconventional

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 2
- Key branches: `screen_ids` truthiness, `screen_names` truthiness in `get_screens`
- Pragmas: none
