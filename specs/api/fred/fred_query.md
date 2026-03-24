# fred/fred_query.py

## Summary
Simple dataclass representing a query to the FRED (Federal Reserve Economic Data) API. Holds all parameters needed to construct a FRED observations request: API key, series identifier, file type, date range for observations, and realtime date range. Used by `FredDataApi` to build and serialize requests via `dataclasses.asdict` and `dataclasses.replace`.

## Dependencies
- Internal: none
- External: `typing` (Union), `datetime` (date, datetime), `dataclasses` (dataclass)

## Type Definitions

### FredQuery (dataclass)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `api_key` | `str` | `''` | FRED API key for authentication |
| `series_id` | `str` | `''` | FRED series identifier (e.g., `'GDP'`, `'UNRATE'`) |
| `file_type` | `str` | `'json'` | Response format; always `'json'` by default |
| `observation_start` | `Union[dt.date, dt.datetime]` | `None` | Start date for observations window |
| `observation_end` | `Union[dt.date, dt.datetime]` | `None` | End date for observations window |
| `realtime_end` | `dt.datetime` | `None` | End of realtime period (maps to `as_of` in `FredDataApi.build_query`) |
| `realtime_start` | `dt.datetime` | `None` | Start of realtime period (maps to `since` in `FredDataApi.build_query`) |

## Enums and Constants
None.

## Functions/Methods
No explicit methods defined. As a `@dataclass`, the following are auto-generated:
- `__init__`: Constructor with all fields as keyword arguments with defaults
- `__repr__`: String representation
- `__eq__`: Equality comparison based on all fields

The dataclass is used with:
- `dataclasses.asdict(query)` -- to serialize all fields into a dict for HTTP params
- `dataclasses.replace(query, api_key=..., series_id=...)` -- to create modified copies with specific fields overridden

## State Mutation
- All fields are mutable (no `frozen=True`). Fields can be reassigned after construction, though in practice `FredDataApi` uses `replace` to create new copies.
- Thread safety: Dataclass instances are not inherently thread-safe; however, the usage pattern (create, optionally `replace` into a new copy, then read) is safe.

## Error Handling
None. This is a plain data container with no validation logic.

## Edge Cases
- Default construction `FredQuery()` produces a query with empty `api_key` and `series_id` -- submitting this to the FRED API would return an error
- `observation_start` and `observation_end` accept both `date` and `datetime`, but FRED's API expects date strings; serialization via `asdict` will produce `datetime` objects that must be string-formatted by the caller or the HTTP library
- `realtime_start` and `realtime_end` are typed as `dt.datetime` only (not `Union[date, datetime]`), but `FredDataApi.build_query` passes `since` (datetime) and `as_of` (datetime) which matches the type annotations
- All `None` defaults mean `asdict` will include `None` values in the dict; the FRED API ignores unknown/null params, so this is harmless for HTTP requests

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 0 (pure data container, no branching logic)
- Pragmas: none
