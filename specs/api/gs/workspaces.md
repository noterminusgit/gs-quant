# workspaces.py

## Summary
API client wrapper for GS Workspaces Markets endpoints. Provides full CRUD operations for market workspaces, lookup by alias, and a method to open a workspace in a web browser.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.workspaces_markets` (Workspace)
- External: `urllib.parse`, `typing` (Tuple, Dict), `pydash` (get), `webbrowser`

## Type Definitions
None defined in this module. Uses `Workspace` from `gs_quant.target.workspaces_markets`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| API | `str` | `"/workspaces/markets"` | Base API path for workspace endpoints |
| WORKSPACES_MARKETS_HEADERS | `Dict[str, str]` | `{'Content-Type': 'application/json;charset=utf-8'}` | Request headers for workspace API calls |

## Functions/Methods

### GsWorkspacesMarketsApi.get_workspaces(cls, limit: int = 10, **kwargs) -> Tuple[Workspace, ...]
Purpose: Fetch workspaces with a limit and optional additional query params.

**Algorithm:**
1. GET `{API}?limit={limit}&{urlencode(kwargs)}` with `cls=Workspace`
2. Return `['results']`

### GsWorkspacesMarketsApi.get_workspace(cls, workspace_id: str)
Purpose: Fetch a single workspace by ID.

**Algorithm:**
1. GET `{API}/{workspace_id}` with `cls=Workspace`
2. Return response

### GsWorkspacesMarketsApi.get_workspace_by_alias(cls, alias: str) -> Workspace
Purpose: Fetch a workspace by its alias. Raises ValueError if not found.

**Algorithm:**
1. GET `{API}?alias={alias}` with `cls=Workspace`
2. Extract first result using `pydash.get(response, 'results.0')`
3. Branch: if workspace is falsy -> raise `ValueError`
4. Return workspace

**Raises:** `ValueError` when no workspace with the given alias is found

### GsWorkspacesMarketsApi.create_workspace(cls, workspace: Workspace) -> Workspace
Purpose: Create a new workspace.

**Algorithm:**
1. POST `{API}` with workspace payload, `cls=Workspace`, and `WORKSPACES_MARKETS_HEADERS`
2. Return response

### GsWorkspacesMarketsApi.update_workspace(cls, workspace: Workspace)
Purpose: Update an existing workspace. Uses `workspace.id` for the URL path.

**Algorithm:**
1. PUT `{API}/{workspace.id}` with workspace payload, `cls=Workspace`, and `WORKSPACES_MARKETS_HEADERS`
2. Return response

### GsWorkspacesMarketsApi.delete_workspace(cls, workspace_id: str) -> Dict
Purpose: Delete a workspace by ID.

**Algorithm:**
1. DELETE `{API}/{workspace_id}`
2. Return response

### GsWorkspacesMarketsApi.open_workspace(cls, workspace: Workspace)
Purpose: Open a workspace in the default web browser using alias or ID.

**Algorithm:**
1. Compute base domain: `GsSession.current.domain.replace(".web", "")`
2. Branch: if `workspace.alias` is truthy -> open `{domain}/s/markets/{workspace.alias}`
3. Branch: else -> open `{domain}/s/markets/{workspace.id}`

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.
- `open_workspace` triggers a side-effect: opens a URL in the system's default web browser.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_workspace_by_alias` | When no workspace matches the given alias |
| `KeyError` | `get_workspaces` | If response lacks `'results'` key |

## Edge Cases
- `get_workspace_by_alias` uses `pydash.get` with dot-notation path `'results.0'` to safely access the first element
- `open_workspace` replaces `".web"` in the domain string -- assumes domain may contain `.web` suffix
- `open_workspace` with a workspace that has both `alias` and `id` will prefer `alias`
- `get_workspace` has no return type annotation
- `**kwargs` in `get_workspaces` are URL-encoded and appended to the query string

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 2
- Key branches: `workspace` truthiness in `get_workspace_by_alias`, `workspace.alias` truthiness in `open_workspace`
- Pragmas: none
