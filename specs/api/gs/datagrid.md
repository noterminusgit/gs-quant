# datagrid.py

## Summary
API client wrapper for GS DataGrid endpoints. Provides CRUD operations for datagrids with support for listing all datagrids or only the current user's datagrids.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.analytics.datagrid` (DataGrid), `gs_quant.analytics.datagrid.datagrid` (API, DATAGRID_HEADERS)
- External: `json`, `urllib.parse`, `typing` (List), `pydash` (get)

## Type Definitions
None defined in this module. Uses `DataGrid` from `gs_quant.analytics.datagrid`.

## Enums and Constants

### Imported Constants
| Name | Source | Description |
|------|--------|-------------|
| API | `gs_quant.analytics.datagrid.datagrid` | Base API path for datagrid endpoints |
| DATAGRID_HEADERS | `gs_quant.analytics.datagrid.datagrid` | Request headers for datagrid API calls |

## Functions/Methods

### GsDataGridApi.get_datagrids(cls, limit: int = 10, **kwargs) -> List[DataGrid]
Purpose: Fetch datagrids ordered by last updated time with optional additional query params.

**Algorithm:**
1. GET `{API}?limit={limit}&orderBy=>lastUpdatedTime&{urlencode(kwargs)}`
2. Extract `'results'` from response using `pydash.get` with default `[]`
3. Convert each raw dict to `DataGrid` via `DataGrid.from_dict`
4. Return list of `DataGrid` objects

### GsDataGridApi.get_my_datagrids(cls, limit: int = 10, **kwargs) -> List[DataGrid]
Purpose: Fetch only the current user's datagrids.

**Algorithm:**
1. GET `/users/self` to get current user ID
2. GET `{API}?limit={limit}&ownerId={user_id}&orderBy=>lastUpdatedTime&{urlencode(kwargs)}`
3. Extract `'results'` from response using `pydash.get` with default `[]`
4. Convert each raw dict to `DataGrid` via `DataGrid.from_dict`
5. Return list of `DataGrid` objects

### GsDataGridApi.get_datagrid(cls, datagrid_id: str) -> DataGrid
Purpose: Fetch a single datagrid by ID.

**Algorithm:**
1. GET `{API}/{datagrid_id}`
2. Convert response to `DataGrid` via `DataGrid.from_dict`
3. Return `DataGrid` object

### GsDataGridApi.create_datagrid(cls, datagrid: DataGrid) -> DataGrid
Purpose: Create a new datagrid.

**Algorithm:**
1. Serialize datagrid to JSON string via `json.dumps(datagrid.as_dict())`
2. POST `{API}` with JSON string body and `DATAGRID_HEADERS`
3. Convert response to `DataGrid` via `DataGrid.from_dict`
4. Return `DataGrid` object

### GsDataGridApi.update_datagrid(cls, datagrid: DataGrid)
Purpose: Update an existing datagrid. Uses `datagrid.id_` for the URL path.

**Algorithm:**
1. Serialize datagrid to JSON string via `json.dumps(datagrid.as_dict())`
2. PUT `{API}/{datagrid.id_}` with JSON string body and `DATAGRID_HEADERS`
3. Convert response to `DataGrid` via `DataGrid.from_dict`
4. Return `DataGrid` object

### GsDataGridApi.delete_datagrid(cls, datagrid: DataGrid)
Purpose: Delete a datagrid. Takes a `DataGrid` object (not an ID string).

**Algorithm:**
1. DELETE `{API}/{datagrid.id_}`
2. Return response

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_my_datagrids` | If `/users/self` response lacks `'id'` key |

## Edge Cases
- `get_datagrids` and `get_my_datagrids` use `pydash.get` with default `[]`, so missing `results` key returns empty list (no KeyError)
- `delete_datagrid` takes a full `DataGrid` object rather than an ID string, unlike delete methods in other API wrappers
- DataGrid uses `id_` (with underscore) as the ID property name, not `id`
- `create_datagrid` and `update_datagrid` manually serialize to JSON string rather than passing the object directly
- `**kwargs` in `get_datagrids` and `get_my_datagrids` are URL-encoded and appended to the query string

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 0
- All methods are straight-line operations with no conditional branches.
- Pragmas: none
