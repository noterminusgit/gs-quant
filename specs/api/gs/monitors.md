# monitors.py

## Summary
API client wrapper for GS Monitor endpoints. Provides full CRUD operations for monitors plus a method to calculate/execute monitor data.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.monitor` (Monitor, MonitorResponseData)
- External: `logging`, `typing` (Tuple), `urllib.parse` (urlencode)

## Type Definitions
None defined in this module. Uses `Monitor` and `MonitorResponseData` from `gs_quant.target.monitor`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsMonitorsApi.get_monitors(cls, limit: int = 100, monitor_id: str = None, owner_id: str = None, name: str = None, folder_name: str = None, monitor_type: str = None, tags: str = None) -> Tuple[Monitor, ...]
Purpose: Fetch monitors with optional filters. Uses `urlencode` to build query string from non-None parameters.

**Algorithm:**
1. Build a dict of all filter params: `id`, `ownerId`, `name`, `folderName`, `type`, `tags`, `limit`
2. Filter out entries where value is `None`
3. URL-encode the filtered dict into a query string
4. GET `/monitors?{query_string}` with `cls=Monitor`
5. Return `['results']`

### GsMonitorsApi.get_monitor(cls, monitor_id: str) -> Monitor
Purpose: Fetch a single monitor by ID.

**Algorithm:**
1. GET `/monitors/{monitor_id}` with `cls=Monitor`
2. Return response

### GsMonitorsApi.create_monitor(cls, monitor: Monitor) -> Monitor
Purpose: Create a new monitor.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. POST `/monitors` with monitor payload, custom headers, `cls=Monitor`
3. Return response

### GsMonitorsApi.update_monitor(cls, monitor: Monitor)
Purpose: Update an existing monitor. Uses `monitor.id` for the URL path.

**Algorithm:**
1. Set request headers to `{'Content-Type': 'application/json;charset=utf-8'}`
2. PUT `/monitors/{monitor.id}` with monitor payload, custom headers, `cls=Monitor`
3. Return response

### GsMonitorsApi.delete_monitor(cls, monitor_id: str) -> dict
Purpose: Delete a monitor by ID.

**Algorithm:**
1. DELETE `/monitors/{monitor_id}`
2. Return response

### GsMonitorsApi.calculate_monitor(cls, monitor_id) -> MonitorResponseData
Purpose: Execute/calculate a monitor and return its response data.

**Algorithm:**
1. GET `/monitors/{monitor_id}/data` with `cls=MonitorResponseData`
2. Return response

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_monitors` | If response lacks `'results'` key |

## Edge Cases
- `get_monitors` with no optional filters: only `limit` is included in query string (always non-None due to default value)
- `calculate_monitor` parameter `monitor_id` has no type annotation
- `limit` is always included even when no other filters are specified since it defaults to `100`

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 1
- The filter logic uses `filter(lambda: item[1] is not None, ...)` which produces a single code path (no explicit if/else branches per param). The branching is implicit in which params are None vs not.
- Pragmas: none
