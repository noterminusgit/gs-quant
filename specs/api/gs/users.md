# users.py

## Summary
API client wrapper for GS user-related endpoints. Provides methods to query users by various identifiers (ID, email, name, company), retrieve current user info, and search users.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.reports` (User)
- External: `pydash` (get), `typing` (List, Any, Dict, Optional)

## Type Definitions
None defined in this module. Uses `User` from `gs_quant.target.reports`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| DEFAULT_SEARCH_FIELDS | `List[str]` | `["id", "name", "firstName", "lastName", "kerberos", "company", "departmentName", "divisionName", "city", "country", "region", "title", "email", "internal"]` | Default fields returned in user search results |

## Functions/Methods

### GsUsersApi.get_users(cls, user_ids: List[str] = None, user_emails: List[str] = None, user_names: List[str] = None, user_companies: List[str] = None, limit: int = 100, offset: int = 0) -> List
Purpose: Fetch users filtered by IDs, emails, names, and/or companies with pagination.

**Algorithm:**
1. Build base URL `/users?`
2. Branch: if `user_ids` provided -> append `&id=` joined parameters
3. Branch: if `user_emails` provided -> append `&email=` joined parameters
4. Branch: if `user_names` provided -> append `&name=` joined parameters
5. Branch: if `user_companies` provided -> append `&company=` joined parameters
6. Append `&limit=` and `&offset=` to URL
7. GET request with `cls=User`, return `['results']`

### GsUsersApi.get_my_guid(cls) -> str
Purpose: Get the current user's GUID in `guid:{id}` format.

**Algorithm:**
1. GET `/users/self`
2. Return `f"guid:{response['id']}"`

### GsUsersApi.get_current_user_info(cls) -> Dict[str, Any]
Purpose: Get full info dict for the currently authenticated user.

**Algorithm:**
1. GET `/users/self`
2. Return the raw response dict

### GsUsersApi.get_current_app_managers(cls) -> List[str]
Purpose: Get list of current user's app managers as GUID strings.

**Algorithm:**
1. GET `/users/self`
2. Extract `appManagers` list using `pydash.get` (default `[]`)
3. Return list of `f"guid:{manager}"` for each manager

### GsUsersApi.get_many(cls, key_type: str, keys: List[str], fields: Optional[List[str]] = None) -> dict
Purpose: Batch-fetch users by a given key type in chunks of 100, returning a dict keyed by the key_type value.

**Algorithm:**
1. Initialize empty `users_by_key` dict, `chunk_size = 100`
2. Build glue string `"&" + key_type + "="`
3. Branch: if `fields is not None` and `key_type not in fields` -> append `key_type` to fields copy
4. Iterate over `keys` in chunks of 100:
   a. Build `fields_str` query param if `fields` is truthy, else empty string
   b. GET `/users?{fields_str}{key_type}={chunk_joined}&limit=200`
   c. For each user in `response.get('results', [])`, store in `users_by_key[user[key_type]]`
5. Return `users_by_key`

### GsUsersApi.search(cls, query: str, fields: Optional[List[str]] = None, where: Optional[Dict[str, any]] = None) -> Dict[str, Any]
Purpose: Search users with a query string, optional field selection, and optional where filters.

**Algorithm:**
1. Build payload with `"q"` set to query, `"fields"` set to `fields or DEFAULT_SEARCH_FIELDS`
2. Branch: if `where` is truthy -> merge `{"where": where}` into payload
3. POST `/search/users/query` with payload
4. Return response

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_users`, `get_my_guid` | If response lacks expected `'results'` or `'id'` keys |

## Edge Cases
- `get_users` called with no filter params returns all users up to limit
- `get_many` with empty `keys` list returns empty dict (range produces no iterations)
- `get_many` with `fields=[]` (empty but not None) produces `fields_str = ""` since empty list is falsy, so no field filtering occurs despite fields being explicitly set
- `search` with `where={}` (empty dict) is falsy, so `where` key is omitted from payload

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 14
- Key branches: 4 filter params in `get_users`, `fields is not None` + `key_type not in fields` in `get_many`, `fields` truthiness in chunk loop, `where` truthiness in `search`
- Pragmas: none
