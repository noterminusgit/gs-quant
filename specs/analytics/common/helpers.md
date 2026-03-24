# helpers.py

## Summary
Utility functions for the analytics module: type checking for built-in types, entity resolution from reference lists with caching, and relative date cache key generation for entity-date mappings.

## Dependencies
- Internal:
  - `gs_quant.analytics.common` (`TYPE`, `DATA_ROW`, `PROCESSOR`, `REFERENCE`, `PARAMETER`, `ENTITY_ID`, `ENTITY_TYPE`)
  - `gs_quant.datetime.relative_date` (`RelativeDate`)
  - `gs_quant.entities.entity` (`Entity`)
  - `gs_quant.errors` (`MqValueError`, `MqRequestError`)
- External:
  - `logging` (`logging.getLogger`)
  - `typing` (`List`, `Dict`)

## Type Definitions
None. This module defines no classes or dataclasses.

### Module-Level Variables
| Name | Type | Description |
|------|------|-------------|
| `_logger` | `logging.Logger` | Module-level logger via `logging.getLogger(__name__)` |

## Enums and Constants
None.

## Functions/Methods

### is_of_builtin_type(obj: Any) -> bool
Purpose: Returns whether the given object's type is a Python built-in type.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `obj` | `Any` | (required) | Object to check |

**Return type:** `bool`

**Algorithm:**
1. Get `type(obj).__module__`
2. Return `True` if module is `'builtins'` or `'__builtin__'`, else `False`

### resolve_entities(reference_list: List[Dict], entity_cache: Dict = None) -> None
Purpose: Fetches and resolves entities (assets, countries, etc.) from a list of entity references, populating the reference objects with resolved entity instances.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `reference_list` | `List[Dict]` | (required) | List of dicts with keys: `ENTITY_ID`, `ENTITY_TYPE`, `TYPE`, `REFERENCE`, and optionally `PARAMETER` |
| `entity_cache` | `Dict` | `None` | Optional cache mapping entity_id to Entity; defaults to `{}` if `None` |

**Return type:** `None`

**Algorithm:**
1. If `entity_cache` is `None`, default to empty dict `{}`
2. For each `reference` in `reference_list`:
   a. Extract `entity_id` from `reference.get(ENTITY_ID)`
   b. Branch: if `entity_id` is in `entity_cache` -> use cached entity
   c. Branch: else -> call `Entity.get(entity_id, 'MQID', reference.get(ENTITY_TYPE))`
      - On `MqRequestError`: log warning, set `entity = entity_id` (string fallback)
   d. Branch on `reference[TYPE]`:
      - `== DATA_ROW`: set `reference[REFERENCE].entity = entity`
      - `== PROCESSOR`:
        1. `setattr(reference[REFERENCE], reference[PARAMETER], entity)`
        2. Get `data_query_info = reference[REFERENCE].children.get(reference[PARAMETER])`
        3. If `data_query_info` is falsy -> raise `MqValueError`
        4. Set `data_query_info.entity = entity`

**Raises:** `MqValueError` when `reference[PARAMETER]` does not exist in processor's children

### get_rdate_cache_key(rule: str, base_date: str, currencies: List[str], exchanges: List[str]) -> str
Purpose: Generates a cache key string for a relative date rule with currency/exchange context.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `rule` | `str` | (required) | The relative date rule string |
| `base_date` | `str` | (required) | The base date as string |
| `currencies` | `List[str]` | (required) | List of currency codes |
| `exchanges` | `List[str]` | (required) | List of exchange codes |

**Return type:** `str`

**Algorithm:**
1. Return `f'{rule}::{base_date}::{currencies}::{exchanges}'`

### get_entity_rdate_key(entity_id: str, rule: str, base_date: Any) -> str
Purpose: Generates a cache key string for an entity-specific relative date.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `entity_id` | `str` | (required) | The entity Marquee ID |
| `rule` | `str` | (required) | The relative date rule string |
| `base_date` | (untyped) | (required) | The base date (any type, converted via f-string) |

**Return type:** `str`

**Algorithm:**
1. Return `f'{entity_id}::{rule}::{base_date}'`

### get_entity_rdate_key_from_rdate(entity_id: str, rdate: RelativeDate) -> str
Purpose: Generates a cache key string from an entity ID and a `RelativeDate` object.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `entity_id` | `str` | (required) | The entity Marquee ID |
| `rdate` | `RelativeDate` | (required) | A RelativeDate instance |

**Return type:** `str`

**Algorithm:**
1. Branch: if `rdate.base_date_passed_in` is truthy -> `base_date = str(rdate.base_date)`
2. Else -> `base_date = None`
3. Return `f'{entity_id}::{rdate.rule}::{base_date}'`

## State Mutation
- `reference[REFERENCE].entity`: Set by `resolve_entities()` when `reference[TYPE] == DATA_ROW`
- `reference[REFERENCE].<parameter>`: Set via `setattr` by `resolve_entities()` when `reference[TYPE] == PROCESSOR`
- `data_query_info.entity`: Set by `resolve_entities()` when `reference[TYPE] == PROCESSOR`
- `_logger`: Module-level logger; read-only after creation
- Thread safety: No synchronization. `resolve_entities` calls `Entity.get()` which may perform network I/O; not safe for concurrent calls on overlapping reference lists.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `resolve_entities` | When `reference[PARAMETER]` not found in `reference[REFERENCE].children` |
| `MqRequestError` | `Entity.get` (caught) | When entity fetch fails; caught and logged as warning, entity falls back to string ID |

## Edge Cases
- `entity_cache=None` defaults to `{}` (not shared across calls)
- `MqRequestError` during entity fetch causes entity to become the raw string `entity_id`
- `reference[PARAMETER]` not in processor children raises `MqValueError`
- `get_entity_rdate_key_from_rdate` with `rdate.base_date_passed_in=False` produces key ending in `::None`
- `get_rdate_cache_key` includes the list repr (e.g., `"['USD']"`) in the key, not joined strings

## Bugs Found
None.

## Coverage Notes
- Branch count: ~8
- `Entity.get()` requires mock for testing
- `MqRequestError` path requires mock to raise during `Entity.get()`
- Both `DATA_ROW` and `PROCESSOR` branches in `resolve_entities` need separate test cases
