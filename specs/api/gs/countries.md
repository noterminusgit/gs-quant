# countries.py

## Summary
API client wrapper for GS Country and Subdivision endpoints. Provides full CRUD operations for both countries and subdivisions, with async variants for read operations on countries.

## Dependencies
- Internal: `gs_quant.session` (GsSession), `gs_quant.target.countries` (Country, Subdivision)
- External: `typing` (Tuple)

## Type Definitions
None defined in this module. Uses `Country` and `Subdivision` from `gs_quant.target.countries`.

## Enums and Constants
None.

## Functions/Methods

### GsCountryApi.get_many_countries(cls, limit: int = 100) -> Tuple[Country, ...]
Purpose: Fetch multiple countries with a limit.

**Algorithm:**
1. GET `/countries?limit={limit}` with `cls=Country`
2. Return `['results']`

### GsCountryApi.get_many_countries_async(cls, limit: int = 100) -> Tuple[Country, ...]
Purpose: Async variant of `get_many_countries`.

**Algorithm:**
1. Async GET `/countries` with payload `{"limit": limit}`, `cls=Country`
2. Return `response.get("results")`

### GsCountryApi.get_country(cls, country_id: str) -> Country
Purpose: Fetch a single country by ID.

**Algorithm:**
1. GET `/countries/{country_id}` with `cls=Country`
2. Return response

### GsCountryApi.get_country_async(cls, country_id: str) -> Country
Purpose: Async variant of `get_country`.

**Algorithm:**
1. Async GET `/countries/{country_id}` with `cls=Country`
2. Return response

### GsCountryApi.create_country(cls, country: Country) -> Country
Purpose: Create a new country.

**Algorithm:**
1. POST `/countries` with country payload, `cls=Country`
2. Return response

### GsCountryApi.update_country(cls, country: Country)
Purpose: Update an existing country. Uses `country.id` for the URL path.

**Algorithm:**
1. PUT `/countries/{country.id}` with country payload, `cls=Country`
2. Return response

### GsCountryApi.delete_country(cls, country_id: str) -> dict
Purpose: Delete a country by ID.

**Algorithm:**
1. DELETE `/countries/{country_id}`
2. Return response

### GsCountryApi.get_many_subdivisions(cls, limit: int = 100) -> Tuple[Subdivision, ...]
Purpose: Fetch multiple subdivisions with a limit.

**Algorithm:**
1. GET `/countries/subdivisions?limit={limit}` with `cls=Subdivision`
2. Return `['results']`

### GsCountryApi.get_subdivision(cls, subdivision_id: str) -> Subdivision
Purpose: Fetch a single subdivision by ID.

**Algorithm:**
1. GET `/countries/subdivisions/{subdivision_id}` with `cls=Subdivision`
2. Return response

### GsCountryApi.create_subdivision(cls, subdivision: Subdivision) -> Subdivision
Purpose: Create a new subdivision.

**Algorithm:**
1. POST `/countries/subdivisions` with subdivision payload, `cls=Subdivision`
2. Return response

### GsCountryApi.update_subdivision(cls, subdivision: Subdivision)
Purpose: Update an existing subdivision. Uses `subdivision.id` for the URL path.

**Algorithm:**
1. PUT `/countries/subdivisions/{subdivision.id}` with subdivision payload, `cls=Subdivision`
2. Return response

### GsCountryApi.delete_subdivision(cls, subdivision_id: str) -> dict
Purpose: Delete a subdivision by ID.

**Algorithm:**
1. DELETE `/countries/subdivisions/{subdivision_id}`
2. Return response

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_many_countries`, `get_many_subdivisions` | If response lacks `'results'` key |

## Edge Cases
- Sync `get_many_countries` uses `['results']` (KeyError on missing), while async variant uses `.get("results")` (returns None on missing) -- inconsistent error behavior
- Sync and async GET variants use different API calling conventions: sync passes limit as URL query param, async passes limit as payload dict
- `get_country_async` wraps response in a local variable and returns it (no-op indirection)

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 0
- All methods are straight-line CRUD operations with no conditional branches.
- Pragmas: none
