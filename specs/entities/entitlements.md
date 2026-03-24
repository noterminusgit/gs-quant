# entitlements.py

## Summary
Implements a permission/entitlements model for GS Marquee entities. Provides `User`, `Group`, `EntitlementBlock`, and `Entitlements` classes that wrap the Marquee API for managing users, groups, and fine-grained action-based access control (admin, delete, display, upload, edit, execute, plot, query, rebalance, trade, view). Tokens are prefixed strings (`guid:`, `group:`, `role:`) that get resolved to rich objects via API calls.

## Dependencies
- Internal: `gs_quant.api.gs.groups` (GsGroupsApi), `gs_quant.api.gs.users` (GsUsersApi), `gs_quant.common` (Entitlements as TargetEntitlements -- actually re-exported from `gs_quant.target.common`), `gs_quant.errors` (MqValueError, MqRequestError), `gs_quant.target.groups` (Group as TargetGroup)
- External: `logging`, `typing` (List, Dict), `pandas` (pd, DataFrame), `pydash` (get)

## Type Definitions

### User (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` (private) | `str` | required | User's unique Marquee ID |
| `__email` (private) | `str` | `None` | User's email address |
| `__name` (private) | `str` | `None` | User's display name (formatted 'Last, First') |
| `__company` (private) | `str` | `None` | User's company name |

Properties (read-only): `id`, `email`, `name`, `company`

### Group (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__id` (private) | `str` | required | Group's unique Marquee ID |
| `__name` (private) | `str` | required | Group display name |
| `__entitlements` (private) | `Entitlements` or `None` | `None` | Nested entitlements for this group |
| `__description` (private) | `str` | `None` | Group description |
| `__tags` (private) | `List` | `None` | Arbitrary tag list |

Properties (read/write): `name`, `entitlements`, `description`, `tags`
Properties (read-only): `id`

### EntitlementBlock (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__users` (private) | `List[User]` | `[]` | Deduplicated list of entitled users |
| `__groups` (private) | `List[Group]` | `[]` | Deduplicated list of entitled groups |
| `__roles` (private) | `List[str]` | `[]` | Deduplicated list of role strings |
| `__unconverted_tokens` (private) | `List[str]` or `None` | from param | Tokens that could not be resolved to User/Group/Role |

Properties (read/write via setter with dedup): `users`, `groups`, `roles`
Properties (read-only): `unconverted_tokens`

### Entitlements (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__admin` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with admin permission |
| `__delete` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with delete permission |
| `__display` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with display permission |
| `__upload` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with upload permission |
| `__edit` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with edit permission |
| `__execute` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with execute permission |
| `__plot` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with plot permission |
| `__query` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with query permission |
| `__rebalance` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with rebalance permission |
| `__trade` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with trade permission |
| `__view` (private) | `EntitlementBlock` | `EntitlementBlock()` | Users/groups/roles with view permission |

Properties (read/write): `admin`, `delete`, `display`, `upload`, `edit`, `execute`, `plot`, `query`, `rebalance`, `trade`, `view`

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

### Permission Action Strings (implicit enum)
The 11 permission actions used throughout: `admin`, `delete`, `display`, `upload`, `edit`, `execute`, `plot`, `query`, `rebalance`, `trade`, `view`.

### Token Prefixes (implicit constants)
| Prefix | Type | Description |
|--------|------|-------------|
| `guid:` | User token | Prefixed user ID in entitlement token lists |
| `group:` | Group token | Prefixed group ID in entitlement token lists |
| `role:` | Role token | Prefixed role name in entitlement token lists |

## Functions/Methods

### User.__init__(self, user_id: str, name: str = None, email: str = None, company: str = None)
Purpose: Initialize a User with ID, optional name/email/company.

**Algorithm:**
1. Store all params as private (name-mangled) attributes.

### User.__eq__(self, other) -> bool
Purpose: Equality based on user ID only.

**Algorithm:**
1. Return `self.id == other.id`
2. Branch: If `other` has no `id` attribute -> raises `AttributeError` (no guard)

### User.__hash__(self) -> int
Purpose: Hash for set/dict usage.

**Algorithm:**
1. Return `hash(self.id) ^ hash(self.name)`

**Note for Elixir:** Since `name` can be `None`, hash includes `hash(None)`. Two users with same ID but different names produce different hashes despite being `__eq__`. This is a potential correctness issue in Python (violates hash/eq contract).

### User.get(cls, user_id: str = None, name: str = None, email: str = None) -> User
Purpose: Resolve identifiers to a single User via API.

**Algorithm:**
1. Branch: If all three params are `None` -> raise `MqValueError('Please specify a user id, name, or email address')`
2. Branch: If `user_id` is truthy AND starts with `'guid:'` -> strip the prefix (take `[5:]`); else keep as-is
3. Call `GsUsersApi.get_users()` with whichever params are non-None wrapped in single-element lists
4. Branch: If `len(results) > 1` -> raise `MqValueError('Error: This request resolves to more than one user in Marquee')`
5. Branch: If `len(results) == 0` -> raise `MqValueError('Error: No user found')`
6. Return `User(user_id=results[0].id, name=results[0].name, email=results[0].email, company=results[0].company)`

**Raises:** `MqValueError` on no params, multiple results, or zero results.

### User.get_many(cls, user_ids: List[str] = None, names: List[str] = None, emails: List[str] = None, companies: List[str] = None) -> List[User]
Purpose: Resolve multiple identifiers to a list of Users via API.

**Algorithm:**
1. Default each `None` param to `[]`
2. Lowercase all emails if provided
3. Branch: If the concatenation of all four lists is empty -> return `[]`
4. Strip `'guid:'` prefix from each user_id that starts with it
5. Call `GsUsersApi.get_users()` with all four lists
6. Map each API result to a `User` object and return the list

### User.save(self)
Purpose: Placeholder -- not implemented.

**Algorithm:**
1. Raise `NotImplementedError`

### Group.__init__(self, group_id: str, name: str, entitlements=None, description: str = None, tags: List = None)
Purpose: Initialize a Group with ID, name, optional entitlements/description/tags.

**Algorithm:**
1. Store all params as private (name-mangled) attributes.

### Group.__eq__(self, other) -> bool
Purpose: Equality based on group ID only.

**Algorithm:**
1. Return `self.id == other.id`

### Group.__hash__(self) -> int
Purpose: Hash for set/dict usage.

**Algorithm:**
1. Return `hash(self.id) ^ hash(self.name)`

**Note:** Same hash/eq contract concern as `User`.

### Group.get(cls, group_id: str) -> Group
Purpose: Resolve a group ID into a Group object via API.

**Algorithm:**
1. Branch: If `group_id` is truthy AND starts with `'group:'` -> strip the prefix (take `[6:]`); else keep as-is
2. Call `GsGroupsApi.get_group(group_id=group_id)`
3. Branch: If `result.entitlements` is truthy -> convert via `Entitlements.from_target(result.entitlements)`; else `None`
4. Return a new `Group` with all fields populated

**Note:** Lines 182-186 contain a redundant duplicate ternary: `if result.entitlements else None if result.entitlements else None`. The second `if result.entitlements else None` is dead code (always evaluates to `None` when reached, but it is unreachable since the first branch already handles the falsy case).

### Group.get_many(cls, group_ids: List[str] = None, names: List[str] = None) -> List[Group]
Purpose: Resolve multiple group IDs/names to a list of Groups via API.

**Algorithm:**
1. Default each `None` param to `[]`
2. Branch: If concatenation of both lists is empty -> return `[]`
3. Strip `'group:'` prefix from each group_id that starts with it
4. Call `GsGroupsApi.get_groups(ids=group_ids, names=names)`
5. For each result: Branch: if `group.entitlements` truthy -> convert; else `None`
6. Return list of `Group` objects

### Group.save(self) -> Group
Purpose: Create or update a group in Marquee.

**Algorithm:**
1. Call `self._group_exists()` to check existence
2. Branch: If group exists -> log "Updating" and call `GsGroupsApi.update_group()`
3. Branch: If group does not exist -> log "Creating" and call `GsGroupsApi.create_group()`
4. Convert result to `Group` object (same redundant ternary as `get`)
5. Return new `Group`

### Group._group_exists(self) -> bool
Purpose: Check if this group already exists in Marquee.

**Algorithm:**
1. Try `Group.get(self.id)`
2. Branch: Success -> return `True`
3. Branch: `MqRequestError` with `status == 404` -> return `False`
4. Branch: `MqRequestError` with any other status -> re-raise

**Raises:** `MqRequestError` when status != 404

### Group.delete(self) -> None
Purpose: Delete this group from Marquee.

**Algorithm:**
1. Call `GsGroupsApi.delete_group(self.id)`
2. Log deletion message

### Group.get_users(self) -> List[User]
Purpose: Get all users in this group.

**Algorithm:**
1. Call `GsGroupsApi.get_users_in_group(self.id)` -- returns list of dicts
2. Map each dict to a `User` object using `.get()` for safe key access
3. Return list

### Group.add_users(self, users: List[User]) -> None
Purpose: Add users to this group.

**Algorithm:**
1. Extract user IDs from User objects
2. Call `GsGroupsApi.add_users_to_group(group_id=self.id, user_ids=user_ids)`
3. Log success

### Group.delete_users(self, users: List[User]) -> None
Purpose: Remove users from this group.

**Algorithm:**
1. Extract user IDs from User objects
2. Call `GsGroupsApi.delete_users_from_group(group_id=self.id, user_ids=user_ids)`
3. Log success

### Group.to_dict(self) -> dict
Purpose: Return the Group as a plain dictionary.

**Algorithm:**
1. Build dict with keys: `name`, `id`, `description`, `entitlements`, `tags`
2. Branch: If `self.entitlements` is truthy -> call `self.entitlements.to_dict()`; else `None`

### Group.to_target(self) -> TargetGroup
Purpose: Convert Group to the API target dataclass.

**Algorithm:**
1. Create `TargetGroup` with all fields
2. Branch: If `self.entitlements` is truthy -> call `self.entitlements.to_target()`; else `None`

### EntitlementBlock.__init__(self, users: List[User] = None, groups: List[Group] = None, roles: List[str] = None, unconverted_tokens: List[str] = None)
Purpose: Initialize an EntitlementBlock, deduplicating users/groups/roles.

**Algorithm:**
1. Branch: If `users` is truthy -> `list(set(users))`; else `[]`
2. Branch: If `groups` is truthy -> `list(set(groups))`; else `[]`
3. Branch: If `roles` is truthy -> `list(set(roles))`; else `[]`
4. Store `unconverted_tokens` as-is (no dedup)

**Note:** `list(set(...))` loses ordering. In Elixir, use `Enum.uniq/1` to preserve order.

### EntitlementBlock.__eq__(self, other) -> bool
Purpose: Compare two EntitlementBlocks for equality.

**Algorithm:**
1. Branch: If `other` is not an `EntitlementBlock` -> return `False`
2. For each property in `['users', 'groups', 'roles']`:
   a. Use `pydash.get` to fetch from self and other
   b. Branch: If both are `None` -> skip (considered equal)
   c. Branch: If not equal -> return `False`
3. Return `True`

**Note:** `unconverted_tokens` is NOT compared.

### EntitlementBlock.users (setter)(self, value: List[User])
Purpose: Set users with deduplication.

**Algorithm:**
1. `self.__users = list(set(value))`

### EntitlementBlock.groups (setter)(self, value: List[Group])
Purpose: Set groups with deduplication.

**Algorithm:**
1. `self.__groups = list(set(value))`

### EntitlementBlock.roles (setter)(self, value: List[str])
Purpose: Set roles with deduplication.

**Algorithm:**
1. `self.__roles = list(set(value))`

### EntitlementBlock.is_empty(self) -> bool
Purpose: Check if this block has no users, groups, or roles.

**Algorithm:**
1. Return `len(self.users + self.groups + self.roles) == 0`

**Note:** Does NOT consider `unconverted_tokens`. An EntitlementBlock with only unconverted_tokens is considered empty.

### EntitlementBlock.to_list(self, as_dicts: bool = False, action: str = None, include_all_tokens: bool = False) -> list
Purpose: Serialize the block to a flat list of tokens or dicts.

**Algorithm:**
1. Branch: If `as_dicts` is `True`:
   a. For each user -> append `dict(action=action, type='user', name=user.name, id=user.id)`
   b. For each group -> append `dict(action=action, type='group', name=group.name, id=group.id)`
   c. For each role -> append `dict(action=action, type='role', name=role, id=role)`
   d. Return the list of dicts
2. Branch: If `as_dicts` is `False`:
   a. Branch: If `include_all_tokens` is `True` -> unconverted_tokens = `self.unconverted_tokens or []`
   b. Branch: If `include_all_tokens` is `False` -> unconverted_tokens = `[]`
   c. Return concatenation of: `['guid:{user.id}', ...]` + `['group:{group.id}', ...]` + `['role:{role}', ...]` + unconverted_tokens

### Entitlements.__init__(self, admin=None, delete=None, display=None, upload=None, edit=None, execute=None, plot=None, query=None, rebalance=None, trade=None, view=None)
Purpose: Initialize Entitlements with 11 permission-action blocks.

**Algorithm:**
1. For each of the 11 actions: Branch: if param is truthy -> use it; else -> `EntitlementBlock()`

### Entitlements.__eq__(self, other) -> bool
Purpose: Compare two Entitlements for equality.

**Algorithm:**
1. Branch: If `other` is not an `Entitlements` -> return `False`
2. For each property in `['admin', 'delete', 'display', 'upload', 'edit', 'execute', 'plot', 'query', 'rebalance', 'view', 'trade']`:
   a. Use `pydash.get` to fetch from self and other
   b. Branch: If both are `None` -> skip
   c. Branch: If not equal -> return `False`
3. Return `True`

### Entitlements.to_target(self, include_all_tokens: bool = False) -> TargetEntitlements
Purpose: Convert to the API target dataclass.

**Algorithm:**
1. Create `TargetEntitlements.default_instance()`
2. For each of the 11 actions:
   a. Branch: If the block `is_empty()` -> skip
   b. Branch: If not empty -> set attribute on target to `block.to_list(include_all_tokens=include_all_tokens)`
3. Return target

### Entitlements.to_dict(self) -> Dict
Purpose: Convert to a dictionary.

**Algorithm:**
1. Call `self.to_target().as_dict()`

### Entitlements.to_frame(self) -> pd.DataFrame
Purpose: Convert all entitlement blocks to a DataFrame with action/type/name/id columns.

**Algorithm:**
1. For each of the 11 actions: call `block.to_list(True, action_name)` to get dicts
2. Concatenate all dicts into one list
3. Return `pd.DataFrame(all_entitled)`

**Note:** If all blocks are empty, returns an empty DataFrame (no columns).

### Entitlements.from_target(cls, entitlements: TargetEntitlements) -> Entitlements
Purpose: Create Entitlements from a target object.

**Algorithm:**
1. Branch: If `entitlements` is `None` -> use `TargetEntitlements.default_instance()`
2. Call `cls.from_dict(entitlements.as_dict())`

### Entitlements.from_dict(cls, entitlements: Dict) -> Entitlements
Purpose: Create Entitlements from a raw dictionary of token lists, resolving tokens to User/Group/Role objects.

**Algorithm:**
1. Initialize `entitlement_kwargs = {}`, `token_map = {}`
2. Initialize `user_ids = set()`, `group_ids = set()`
3. **First pass** -- scan all token sets across all actions:
   a. For each token `t` in each token set:
      - Branch: If `t.startswith('guid:')` -> add to `user_ids` set
      - Branch: If `t.startswith('group:')` -> add to `group_ids` set
      - Branch: If `t.startswith('role:')` -> add to `token_map` mapping `t -> t[5:]` (strip 'role:' prefix)
      - Branch: else (unrecognized prefix) -> token is not mapped (will become unconverted)
4. Batch-resolve: call `User.get_many(user_ids=list(user_ids))` and `Group.get_many(group_ids=list(group_ids))`
5. Populate `token_map` with resolved users (`'guid:{u.id}' -> User`) and groups (`'group:{g.id}' -> Group`)
6. **Second pass** -- for each `(action, token_set)` in the dict:
   a. Initialize `users, groups, roles, unconverted_tokens = [], [], [], []`
   b. For each token `t`:
      - Branch: If `t` is in `token_map.keys()`:
        - Branch: If `t` in `user_ids` set -> append mapped User to `users`
        - Branch: If `t` in `group_ids` set -> append mapped Group to `groups`
        - Branch: else -> append mapped role string to `roles`
      - Branch: If `t` is NOT in `token_map` -> append to `unconverted_tokens`
   c. Branch: If `unconverted_tokens` is non-empty -> keep it; else -> set to `None`
   d. Branch: If any of `users`, `groups`, `roles`, `unconverted_tokens` are truthy -> create `EntitlementBlock` and add to kwargs
7. Return `Entitlements(**entitlement_kwargs)`

**Note:** A `guid:` token that is in `user_ids` but NOT in `token_map` (i.e., the API returned no matching user) will fall into `unconverted_tokens`. Same for unresolved `group:` tokens.

## State Mutation
- `User`: Immutable after construction (all private fields, read-only properties)
- `Group`: Mutable -- `name`, `entitlements`, `description`, `tags` have setters. `save()` creates or updates in Marquee. `delete()` removes from Marquee. `add_users()`/`delete_users()` modify remote state.
- `EntitlementBlock`: Mutable -- `users`, `groups`, `roles` have setters (with dedup via `list(set(...))`). `unconverted_tokens` is read-only.
- `Entitlements`: Mutable -- all 11 action properties have setters.
- Thread safety: No synchronization. Not safe for concurrent modification.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `User.get` | All params are `None` |
| `MqValueError` | `User.get` | Multiple users found |
| `MqValueError` | `User.get` | No user found |
| `MqRequestError` | `Group._group_exists` | Re-raised when API error status != 404 |
| `NotImplementedError` | `User.save` | Always (not implemented) |
| `AttributeError` | `User.__eq__` / `Group.__eq__` | If `other` has no `id` attribute (no isinstance guard) |

## Edge Cases
- `User.get` with `user_id='guid:abc'` -- prefix is stripped, actual ID `'abc'` is used for API call
- `User.get` with `user_id=''` (empty string) -- falsy, so treated as `None` for the `guid:` strip but still passed to API as `[None]` context depends on `user_id` parameter being `None` vs empty
- `User.__eq__` called with non-User object -- will raise `AttributeError` rather than returning `False` (no isinstance check)
- `Group.__eq__` same issue as User
- `EntitlementBlock` with `users=[]` (empty list) -- falsy in Python, so stored as `[]` (same result, but the branch is taken differently than `users=[User(...)]`)
- `EntitlementBlock.to_list(as_dicts=False, include_all_tokens=True)` when `unconverted_tokens` is `None` -- evaluates `None or []` which yields `[]`
- `Entitlements.from_dict` with a token like `'guid:nonexistent'` -- added to `user_ids` set but not resolved by API, so NOT added to `token_map`, and therefore falls into `unconverted_tokens`
- `Entitlements.from_dict` with empty dict `{}` -- returns `Entitlements()` with all empty blocks
- `Entitlements.__eq__` uses `pydash.get` which accesses properties by name string -- works with Python property accessors
- `Entitlements.to_frame` with all empty blocks -> returns empty DataFrame with no columns
- `Group.get` redundant ternary on lines 182-186 -- the second `if result.entitlements else None` is dead code
- `Group.save` redundant ternary on lines 232-236 -- same dead code pattern
- Hash/eq contract violation: `User` and `Group` both include `name` in `__hash__` but not in `__eq__`, so two objects with same `id` but different `name` are equal but have different hashes

## Bugs Found
- Lines 182-186, 232-236: Redundant duplicate ternary `if result.entitlements else None if result.entitlements else None`. The second guard is unreachable dead code. Functionally harmless but confusing. (OPEN)
- `User.__hash__` / `Group.__hash__`: Include `name` in hash but `__eq__` only compares `id`. This violates the Python contract that `a == b` implies `hash(a) == hash(b)`. Can cause incorrect behavior when objects are used in sets/dicts if name differs between otherwise-equal instances. (OPEN)

## Coverage Notes
- Branch count: ~58 (11 action blocks x 2 branches in `__init__`, `to_target`, `to_frame`; plus conditional branches in `get`, `get_many`, `from_dict`, `_group_exists`, `__eq__`, `to_list`, `is_empty`, `save`, `to_dict`, `to_target` on entitlements check)
- Key branches requiring coverage:
  - `User.get`: all-None params, guid-prefix strip, >1 result, 0 results, exactly 1 result
  - `User.get_many`: empty input, guid-prefix stripping, email lowercasing
  - `Group.get`: group-prefix strip, entitlements present vs absent
  - `Group._group_exists`: found, 404, other error
  - `Group.save`: exists vs not-exists path
  - `EntitlementBlock.__eq__`: non-EntitlementBlock, both-None property, unequal property, all-equal
  - `EntitlementBlock.to_list`: as_dicts True vs False, include_all_tokens True vs False, unconverted_tokens None vs present
  - `Entitlements.__eq__`: non-Entitlements, property-level both-None, unequal, all-equal
  - `Entitlements.from_dict`: guid/group/role token routing, unresolved tokens, empty actions, mixed actions
  - `Entitlements.to_target`: each action block empty vs non-empty, include_all_tokens flag
- Pragmas: none
