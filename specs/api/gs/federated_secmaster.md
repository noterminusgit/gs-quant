# api/gs/federated_secmaster.py

## Summary
API client for the GS Federated Security Master service. Provides methods to look up individual securities, retrieve identifier histories, query/search multiple securities with pagination, and optionally flatten results. All methods are classmethods that delegate to `GsSession.current.sync.get()`. The `FederatedIdentifiers` enum defines the set of supported identifier types for query parameters.

## Dependencies
- Internal: `gs_quant.target.secmaster` (`SecMasterAssetType`)
- Internal: `gs_quant.common` (`AssetType`, `AssetClass`)
- Internal: `gs_quant.json_encoder` (`JSONEncoder`)
- Internal: `gs_quant.session` (`GsSession`)
- External: `enum` (Enum)
- External: `json` (loads, dumps)
- External: `datetime` (date)
- External: `typing` (Union, Iterable, Dict, Optional)

## Type Definitions

### GsSecurityMasterFederatedApi (class)
Inherits: `object`

Stateless API client class. All methods are classmethods; no instance state.

## Enums and Constants

### FederatedIdentifiers(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| IDENTIFIER | `"identifier"` | Generic identifier |
| ID | `"id"` | Internal ID |
| ASSET_ID | `"assetId"` | Marquee asset ID |
| GSID | `"gsid"` | GS internal ID |
| TICKER | `"ticker"` | Exchange ticker |
| BBID | `"bbid"` | Bloomberg ID |
| BCID | `"bcid"` | Bloomberg composite ID |
| RIC | `"ric"` | Reuters instrument code |
| RCIC | `"rcic"` | Reuters composite instrument code |
| CUSIP | `"cusip"` | CUSIP identifier |
| CINS | `"cins"` | CINS identifier |
| SEDOL | `"sedol"` | SEDOL identifier |
| ISIN | `"isin"` | ISIN identifier |
| PRIMEID | `"primeId"` | Prime ID |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `SECURITIES_FEDERATED` | `str` | `"/markets/securities/federated"` | Base URL path for the federated secmaster API |

## Functions/Methods

### GsSecurityMasterFederatedApi.get_a_security(cls, id, effective_date) -> Optional[dict]
Purpose: Get security or asset data for a given security ID (GS-prefixed) or asset ID (MA-prefixed).

**Algorithm:**
1. Branch: `id` does not start with `"GS"` and does not start with `"MA"` -> raise `ValueError`.
2. Initialize empty `params` dict.
3. Branch: `effective_date is not None` -> add `"effectiveDate"` to params.
4. JSON-encode params via `json.loads(json.dumps(params, cls=JSONEncoder))`.
5. GET `{SECURITIES_FEDERATED}/{id}` with payload.

**Raises:** `ValueError` when `id` does not start with `"GS"` or `"MA"`.

---

### GsSecurityMasterFederatedApi.get_security_identifiers(cls, id) -> Optional[dict]
Purpose: Get identifier history for a given security ID or asset ID.

**Algorithm:**
1. Branch: `id` does not start with `"GS"` and does not start with `"MA"` -> raise `ValueError`.
2. GET `{SECURITIES_FEDERATED}/{id}/identifiers`.

**Raises:** `ValueError` when `id` does not start with `"GS"` or `"MA"`.

---

### GsSecurityMasterFederatedApi.get_many_securities(cls, type_, effective_date, limit, is_primary, offset_key, **query_params) -> Optional[dict]
Purpose: Get a single page of reference data (non-flattened). Delegates to `__query_securities` with `flatten=False`.

**Algorithm:**
1. Delegate to `cls.__query_securities(...)` with `flatten=False` and all other params forwarded.

---

### GsSecurityMasterFederatedApi.get_securities_data(cls, type_, effective_date, limit, is_primary, offset_key, **query_params) -> Optional[dict]
Purpose: Get a single page of flattened reference data. Delegates to `__query_securities` with `flatten=True`.

**Algorithm:**
1. Delegate to `cls.__query_securities(...)` with `flatten=True` and all other params forwarded.

---

### GsSecurityMasterFederatedApi.search_many_securities(cls, q, limit, offset_key, asset_class, type_, is_primary) -> Optional[dict]
Purpose: Full-text search of securities (non-flattened). Delegates to `__search_securities` with `flatten=False`.

**Algorithm:**
1. Delegate to `cls.__search_securities(...)` with `flatten=False` and all other params forwarded.

---

### GsSecurityMasterFederatedApi.search_securities_data(cls, q, limit, offset_key, asset_class, type_, is_primary) -> Optional[dict]
Purpose: Full-text search of securities (flattened). Delegates to `__search_securities` with `flatten=True`.

**Algorithm:**
1. Delegate to `cls.__search_securities(...)` with `flatten=True` and all other params forwarded.

---

### GsSecurityMasterFederatedApi.__query_securities(cls, type_, effective_date, limit, flatten, is_primary, offset_key, **query_params) -> Optional[dict]
Purpose: Internal method that builds query parameters and issues a GET to retrieve securities.

**Algorithm:**
1. Branch: `query_params` is None or empty AND `type_` is None -> raise `ValueError("Neither '_type' nor 'query_params' are provided")`.
2. Initialize `params = {"limit": limit}`.
3. Call `cls.__prepare_params(params, effective_date, offset_key, is_primary, type_, None)`.
4. Merge `query_params` into `params`.
5. JSON-encode params.
6. Branch: `flatten` is True -> GET `{SECURITIES_FEDERATED}/data`.
7. Else -> GET `{SECURITIES_FEDERATED}`.

**Raises:** `ValueError` when neither `type_` nor `query_params` are provided.

---

### GsSecurityMasterFederatedApi.__search_securities(cls, q, limit, offset_key, flatten, asset_class, type_, is_primary) -> Optional[dict]
Purpose: Internal method that builds search parameters and issues a GET to search securities.

**Algorithm:**
1. Branch: `q is None` -> raise `ValueError("No search query provided")`.
2. Initialize `params = {"q": q, "limit": limit}`.
3. Call `cls.__prepare_params(params, None, offset_key, is_primary, type_, asset_class)`.
4. JSON-encode params.
5. Branch: `flatten` is True -> GET `{SECURITIES_FEDERATED}/search/data`.
6. Else -> GET `{SECURITIES_FEDERATED}/search`.

**Raises:** `ValueError` when `q` is None.

---

### GsSecurityMasterFederatedApi.__prepare_params(cls, params, effective_date, offset_key, is_primary, type_, asset_class) -> None
Purpose: Mutate the `params` dict in-place, adding optional query parameters when they are not None.

**Algorithm:**
1. Branch: `effective_date is not None` -> `params["effectiveDate"] = effective_date`.
2. Branch: `offset_key is not None` -> `params["offsetKey"] = offset_key`.
3. Branch: `is_primary is not None` -> `params["isPrimary"] = is_primary`.
4. Branch: `type_ is not None` -> `params["type"] = type_.value`.
5. Branch: `asset_class is not None` -> `params["assetClass"] = asset_class.value`.

## State Mutation
- `params` dict: Mutated in-place by `__prepare_params()` and by the callers (`__query_securities`, `__search_securities`, `get_a_security`).
- No instance state; all methods are classmethods operating on local variables.
- Thread safety: Stateless; safe for concurrent use. Thread safety of the underlying `GsSession.current` is managed externally.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_a_security` | `id` does not start with `"GS"` or `"MA"` |
| `ValueError` | `get_security_identifiers` | `id` does not start with `"GS"` or `"MA"` |
| `ValueError` | `__query_securities` | Both `query_params` is empty/None and `type_` is None |
| `ValueError` | `__search_securities` | `q` is None |

## Edge Cases
- An `id` like `"GLOBAL"` starts with `"G"` but not `"GS"`, so it will be rejected. The prefix check is strict: exactly `"GS"` or `"MA"`.
- An empty string `""` for `id` will be rejected (does not start with `"GS"` or `"MA"`).
- `get_many_securities` with `type_=None` and no `query_params` will raise `ValueError` from `__query_securities`.
- `get_many_securities` with `type_` set but no `query_params` is valid -- the `query_params is None or len(query_params) == 0` check passes but `type_ is None` is False so the guard does not trigger.
- The `limit` parameter defaults differ: 50 for query methods, 10 for search methods.
- `__prepare_params` accesses `.value` on `type_` and `asset_class`, so these must be enum instances (not raw strings) when provided.
- The JSON round-trip (`json.loads(json.dumps(params, cls=JSONEncoder))`) is used to serialize `datetime.date` objects and enum values via the custom encoder.

## Bugs Found
- None found.

## Coverage Notes
- Branch count: ~20
  - `get_a_security`: 3 branches (invalid id, effective_date None vs not, happy path)
  - `get_security_identifiers`: 2 branches (invalid id, happy path)
  - `__query_securities`: 3 branches (validation error, flatten true/false)
  - `__search_securities`: 3 branches (q is None, flatten true/false)
  - `__prepare_params`: 5 branches (one per optional param being None vs not)
  - `get_many_securities`, `get_securities_data`, `search_many_securities`, `search_securities_data`: 1 branch each (delegation)
- All branches are straightforward to cover with mocked `GsSession`.
