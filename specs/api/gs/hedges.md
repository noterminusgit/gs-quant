# hedges.py

## Summary
API client for GS Hedge services. Provides CRUD operations on hedge objects, hedge data/results retrieval, performance hedge query construction, hedge calculation execution, and hedge group sharing with entitlement management. All methods are classmethods on `GsHedgeApi` and communicate via the Marquee REST API through `GsSession`.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.hedge` (Hedge, PerformanceHedgeParameters, ClassificationConstraint, AssetConstraint, Target), `gs_quant.entities.entitlements` (User -- lazy import inside `share_hedge_group`)
- External: `datetime`, `logging`, `typing` (Tuple, List, Dict)

## Type Definitions

### GsHedgeApi (class)
Inherits: `object` (implicit)

No instance fields -- all methods are `@classmethod`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `CALCULATION_TIMEOUT` | `int` | `180` | Timeout in seconds for hedge calculation API calls |

## Functions/Methods

### GsHedgeApi.get_many_hedges(cls, ids: List[str] = None, names: List[str] = None, limit: int = 100)
Purpose: Retrieve multiple hedges filtered by IDs and/or names.

**Algorithm:**
1. Build base URL `/hedges?limit={limit}`
2. Branch: if `ids` truthy -> append `&id=` joined IDs
3. Branch: if `names` truthy -> append `&name=` joined names
4. GET with `cls=Hedge`

### GsHedgeApi.create_hedge(cls, hedge: Dict) -> Hedge
Purpose: Create a new hedge from a dictionary payload.

**Algorithm:**
1. POST `/hedges` with hedge dict, deserialize as Hedge
2. Return result

### GsHedgeApi.get_hedge(cls, hedge_id: str) -> Hedge
Purpose: Retrieve a single hedge by ID.

**Algorithm:**
1. GET `/hedges/{hedge_id}` with `cls=Hedge`
2. Return deserialized Hedge

### GsHedgeApi.get_hedge_data(cls, ids: List[str] = None, names: List[str] = None, limit: int = 100) -> List[Dict]
Purpose: Retrieve hedge data records filtered by IDs and/or names.

**Algorithm:**
1. Build URL `/hedges/data?limit={limit}`
2. Branch: if `ids` truthy -> append `&id=` joined IDs
3. Branch: if `names` truthy -> append `&name=` joined names
4. GET with `cls=Hedge`, return `results` key

### GsHedgeApi.get_hedge_results(cls, hedge_id: str, start_date: dt.date = None, end_date: dt.date = None) -> Dict
Purpose: Retrieve computed results for a hedge over an optional date range.

**Algorithm:**
1. Build URL `/hedges/results?id={hedge_id}`
2. Branch: if `start_date is not None` -> append `&startDate=` formatted
3. Branch: if `end_date is not None` -> append `&endDate=` formatted
4. GET, return first element of `results` array

### GsHedgeApi.update_hedge(cls, hedge_id: str, hedge: Hedge) -> Hedge
Purpose: Update an existing hedge.

**Algorithm:**
1. PUT `/hedges/{hedge_id}` with hedge payload, deserialize as Hedge
2. Return result

### GsHedgeApi.delete_hedge(cls, hedge_id: str)
Purpose: Delete a hedge by ID.

**Algorithm:**
1. DELETE `/hedges/{hedge_id}` with `cls=Hedge`

### GsHedgeApi.construct_performance_hedge_query(cls, hedge_target: str, universe: Tuple[str, ...], notional: float, observation_start_date: dt.date, observation_end_date: dt.date, backtest_start_date: dt.date, backtest_end_date: dt.date, use_machine_learning: bool = False, lasso_weight: float = None, ridge_weight: float = None, max_return_deviation: float = 5, max_adv_percentage: float = 15, max_leverage: float = 100, max_weight: float = 100, min_market_cap: float = None, max_market_cap: float = None, asset_constraints: Tuple[AssetConstraint, ...] = None, benchmarks: Tuple[str, ...] = None, classification_constraints: Tuple[ClassificationConstraint, ...] = None, exclude_corporate_actions: bool = False, exclude_corporate_actions_types: Tuple = None, exclude_hard_to_borrow_assets: bool = False, exclude_restricted_assets: bool = False, exclude_target_assets: bool = True, explode_universe: bool = True, market_participation_rate: float = 10, sampling_period: str = 'Daily') -> dict
Purpose: Construct a performance hedge query dictionary for submission to the hedger API. Pure data construction with no API call.

**Algorithm:**
1. Build a dict with `'objective': 'Replicate Performance'`
2. Create `PerformanceHedgeParameters` with `Target(id=hedge_target)` and all other parameters passed positionally
3. Assign parameters to dict under `'parameters'` key
4. Return the dict

### GsHedgeApi.calculate_hedge(cls, hedge_query: dict) -> dict
Purpose: Submit a hedge query to the performance hedger calculation API and return results.

**Algorithm:**
1. POST `/hedges/calculations` with `hedge_query` as payload and `CALCULATION_TIMEOUT` (180s) timeout
2. Return response dict

### GsHedgeApi.share_hedge_group(cls, hedge_group_id: str, strategy_request: Dict, optimization_response: Dict, hedge_name: str = "Custom Hedge", group_name: str = "New Hedge Group", view_emails: List[str] = None, admin_emails: List[str] = None) -> Dict
Purpose: Share a saved hedge group with other users by updating entitlements via email-to-GUID resolution.

**Algorithm:**
1. Build URL `/hedges/groups/{hedge_group_id}`
2. GET the current hedge group data
3. Extract `ownerId`; Branch: if `current_user_guid` truthy -> prepend `"guid:"` prefix
4. Extract existing `entitlements` dict
5. Branch: if `current_user_guid` truthy -> ensure owner has admin, edit, and view access:
   - For each of `'admin'`, `'edit'`, `'view'`: Branch: if key missing -> create empty list; Branch: if guid not in list -> append
6. Branch: if `view_emails` truthy -> resolve emails to Users via `User.get_many(emails=view_emails)`, for each user: Branch: if `guid:{user.id}` not in view list -> append
7. Branch: if `admin_emails` truthy -> resolve emails to Users via `User.get_many(emails=admin_emails)`, for each user: add to admin, edit, and view lists if not present (3 conditional checks per user)
8. Build complete payload with `active=True`, updated entitlements, hedges array (with first hedgeId from existing data), metadata fields from existing data
9. PUT updated payload to the URL
10. Print success messages with entitlement counts
11. Return result
12. Wrapped in try/except: on any `Exception` -> print failure message, re-raise

**Raises:** Re-raises any `Exception` from the API calls after printing an error message

## State Mutation
- No instance state -- all methods are classmethods operating on the remote API via HTTP
- `share_hedge_group` mutates remote entitlements on the hedge group
- `share_hedge_group` prints to stdout as a side effect
- Thread safety: No shared mutable state; relies on `GsSession.current`

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` (any) | `share_hedge_group` | Re-raised after printing error; wraps any API or User resolution failure |

## Edge Cases
- `get_hedge_results`: Always returns `results[0]` -- will raise `IndexError` if results array is empty
- `construct_performance_hedge_query`: All parameters are passed positionally to `PerformanceHedgeParameters`; parameter order is critical
- `share_hedge_group`: Uses lazy import of `User` from `gs_quant.entities.entitlements` to avoid circular imports
- `share_hedge_group`: Extracts first element of `hedgeIds` list for the hedge ID in payload; returns `None` if `hedgeIds` is empty

## Bugs Found
- Lines 332-342: Dead code after `raise` in the except block. There is a duplicate `print("Hedge group shared successfully!")` block and a second `except Exception as e:` clause that are unreachable because `raise` on line 330 transfers control out of the except block. This is dead code that will never execute. (OPEN)
- Line 341: The dead-code except block uses a unicode character in its print string (`"Failed to share hedge: {e}"`) while the reachable one on line 329 does not -- inconsistency, though moot since it's unreachable. (OPEN)

## Coverage Notes
- Branch count: ~24 (URL conditionals in get methods, entitlement presence checks in share_hedge_group, view_emails/admin_emails iteration)
- Missing branches: Dead code on lines 332-342 is unreachable and should show as uncovered
- Pragmas: None observed
