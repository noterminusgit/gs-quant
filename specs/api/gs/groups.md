# groups.py

## Summary
API client wrapper for GS group management endpoints. Provides full CRUD operations for groups and methods to manage group membership (add/remove users).

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.groups` (Group)
- External: `typing` (List, Dict)

## Type Definitions
None defined in this module. Uses `Group` from `gs_quant.target.groups`.

## Enums and Constants
None.

## Functions/Methods

### GsGroupsApi.get_groups(cls, ids: List[str] = None, names: List[str] = None, oe_ids: List[str] = None, owner_ids: List[str] = None, tags: List[str] = None, user_ids: List[str] = None, scroll_id: str = None, scroll_time: str = None, limit: int = 100, offset: int = 0, order_by: str = None) -> List
Purpose: Fetch groups with optional filtering by IDs, names, OE IDs, owner IDs, tags, user IDs, and pagination/scroll support.

**Algorithm:**
1. Build base URL `/groups?limit={limit}&offset={offset}`
2. Branch: if `ids` -> append `&id=` joined params
3. Branch: if `names` -> append `&name=` joined params
4. Branch: if `oe_ids` -> append `&oeId=` joined params
5. Branch: if `owner_ids` -> append `&ownerId=` joined params
6. Branch: if `tags` -> append `&tags=` joined params
7. Branch: if `user_ids` -> append `&userIds=` joined params
8. Branch: if `scroll_id` -> append `&scrollId={scroll_id}`
9. Branch: if `scroll_time` -> append `&scrollTime={scroll_time}`
10. Branch: if `order_by` -> append `&orderBy={order_by}`
11. GET with `cls=Group`, return `['results']`

### GsGroupsApi.create_group(cls, group: Group) -> Dict
Purpose: Create a new group.

**Algorithm:**
1. POST `/groups` with group payload, `cls=Group`
2. Return response

### GsGroupsApi.get_group(cls, group_id: str) -> Group
Purpose: Get a single group by ID.

**Algorithm:**
1. GET `/groups/{group_id}` with `cls=Group`
2. Return response

### GsGroupsApi.update_group(cls, group_id: str, group: Group) -> Group
Purpose: Update an existing group. Strips the `id` field from the payload before sending.

**Algorithm:**
1. Convert group to JSON dict via `group.to_json()`
2. Branch: if `group_dict.get('entitlements')` is truthy -> convert entitlements to JSON via `.to_json()`
3. Remove `'id'` key from dict via `pop('id')`
4. PUT `/groups/{group_id}` with cleaned dict, `cls=Group`
5. Return response

### GsGroupsApi.delete_group(cls, group_id: str)
Purpose: Delete a group by ID.

**Algorithm:**
1. DELETE `/groups/{group_id}`

### GsGroupsApi.get_users_in_group(cls, group_id: str) -> List
Purpose: Get list of user IDs in a group.

**Algorithm:**
1. GET `/groups/{group_id}/users`
2. Return `.get('users', [])`

### GsGroupsApi.add_users_to_group(cls, group_id: str, user_ids: List[str])
Purpose: Add users to a group.

**Algorithm:**
1. POST `/groups/{group_id}/users` with `{'userIds': user_ids}`

### GsGroupsApi.delete_users_from_group(cls, group_id: str, user_ids: List[str])
Purpose: Remove users from a group.

**Algorithm:**
1. DELETE `/groups/{group_id}/users` with `{'userIds': user_ids}` and `use_body=True`

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_groups` | If response lacks `'results'` key |
| `KeyError` | `update_group` | If `group.to_json()` does not contain `'id'` key (pop would raise) |

## Edge Cases
- `get_groups` with no filter params returns all groups up to limit
- `update_group` with a group that has no entitlements skips the entitlements conversion
- `delete_users_from_group` uses `use_body=True` to send body with DELETE request

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 11
- Key branches: 9 filter/pagination params in `get_groups`, 1 entitlements check in `update_group`
- Pragmas: none
