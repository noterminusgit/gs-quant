# plots.py

## Summary
API client wrapper for GS Charts/Plots endpoints. Provides full CRUD operations for charts with both sync and async variants, plus chart sharing functionality.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.charts` (Chart, ChartShare)
- External: `collections.abc` (Iterable), `typing` (Tuple)

## Type Definitions
None defined in this module. Uses `Chart` and `ChartShare` from `gs_quant.target.charts`.

## Enums and Constants
None.

## Functions/Methods

### GsPlotApi.get_many_charts(cls, limit: int = 100) -> Tuple[Chart, ...]
Purpose: Fetch multiple charts with a limit.

**Algorithm:**
1. GET `/charts?limit={limit}` with `cls=Chart`
2. Return `['results']`

### GsPlotApi.get_many_charts_async(cls, limit: int = 100) -> Tuple[Chart, ...]
Purpose: Async variant of `get_many_charts`.

**Algorithm:**
1. Async GET `/charts?limit={limit}` with `cls=Chart`
2. Return `['results']`

### GsPlotApi.get_chart(cls, chart_id: str) -> Chart
Purpose: Fetch a single chart by ID.

**Algorithm:**
1. GET `/charts/{chart_id}` with `cls=Chart`
2. Return response

### GsPlotApi.get_chart_async(cls, chart_id: str) -> Chart
Purpose: Async variant of `get_chart`.

**Algorithm:**
1. Async GET `/charts/{chart_id}` with `cls=Chart`
2. Return response

### GsPlotApi.create_chart(cls, chart: Chart) -> Chart
Purpose: Create a new chart.

**Algorithm:**
1. POST `/charts` with chart payload, `cls=Chart`
2. Return response

### GsPlotApi.create_chart_async(cls, chart: Chart) -> Chart
Purpose: Async variant of `create_chart`.

**Algorithm:**
1. Async POST `/charts` with chart payload, `cls=Chart`
2. Return response

### GsPlotApi.update_chart(cls, chart: Chart)
Purpose: Update an existing chart. Uses `chart.id` for the URL path.

**Algorithm:**
1. PUT `/charts/{chart.id}` with chart payload, `cls=Chart`
2. Return response

### GsPlotApi.update_chart_async(cls, chart: Chart)
Purpose: Async variant of `update_chart`.

**Algorithm:**
1. Async PUT `/charts/{chart.id}` with chart payload, `cls=Chart`
2. Return response

### GsPlotApi.delete_chart(cls, chart_id: str) -> dict
Purpose: Delete a chart by ID.

**Algorithm:**
1. DELETE `/charts/{chart_id}`
2. Return response

### GsPlotApi.delete_chart_async(cls, chart_id: str) -> dict
Purpose: Async variant of `delete_chart`.

**Algorithm:**
1. Async DELETE `/charts/{chart_id}`
2. Return response

### GsPlotApi.share_chart(cls, chart_id: str, users: Iterable)
Purpose: Share a chart with individual users. Validates all user tokens start with `guid:`.

**Algorithm:**
1. Branch: if any user in `users` does not start with `'guid:'` -> raise `ValueError`
2. Fetch the chart via `cls.get_chart(chart_id)` to get version
3. Create `ChartShare(tuple(users), chart.version)`
4. POST `/charts/{chart_id}/share` with share payload, `cls=Chart`
5. Return response

**Raises:** `ValueError` when any user token does not start with `'guid:'`

### GsPlotApi.share_chart_async(cls, chart_id: str, users: Iterable)
Purpose: Async variant of `share_chart`.

**Algorithm:**
1. Branch: if any user in `users` does not start with `'guid:'` -> raise `ValueError`
2. Async fetch the chart via `cls.get_chart_async(chart_id)` to get version
3. Create `ChartShare(tuple(users), chart.version)`
4. Async POST `/charts/{chart_id}/share` with share payload, `cls=Chart`
5. Return response

**Raises:** `ValueError` when any user token does not start with `'guid:'`

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `share_chart`, `share_chart_async` | When any user token does not start with `'guid:'` |

## Edge Cases
- `share_chart` with empty `users` iterable: `any()` on empty returns `False`, so no error raised; creates `ChartShare` with empty tuple
- `users` iterable is consumed by `any(map(...))` -- if `users` is a generator, the validation consumes it and the subsequent `tuple(users)` would be empty. Only safe with re-iterable types (list, tuple, etc.)
- `update_chart` and `update_chart_async` access `chart.id` directly -- will raise `AttributeError` if chart has no `id`

## Bugs Found
- The `users` parameter in `share_chart`/`share_chart_async` typed as `Iterable` could be a single-use iterator; the validation via `any(map(...))` consumes it before `tuple(users)` is called. Using a `List` or `Sequence` type would be safer. (OPEN)

## Coverage Notes
- Branch count: 2
- Key branches: `guid:` prefix validation in `share_chart` (valid vs invalid), same in `share_chart_async`
- Pragmas: none
