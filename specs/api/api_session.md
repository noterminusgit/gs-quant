# api_session.py

## Summary
Provides a mixin class `ApiWithCustomSession` that allows any API class to override its session context. By default, API classes use `GsSession.current`, but callers can inject a custom session supplier (callable) or a specific session instance. This is the session-management foundation for all API classes that need per-API session overrides.

## Dependencies
- Internal: `gs_quant.session` (GsSession)
- External: `typing` (Callable, Optional)

## Type Definitions

### ApiWithCustomSession (class)
Inherits: object (implicit)

A mixin class intended to be inherited by API classes. Provides class-level session management with a private class variable for the session supplier.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__SESSION_SUPPLIER` | `Optional[Callable[[], GsSession]]` | `None` | Class-level callable that returns a `GsSession`, or `None` to use the global default |

## Enums and Constants
None.

## Functions/Methods

### ApiWithCustomSession.set_session_provider(cls, session_supplier: Callable[[], GsSession])
Purpose: Set a factory/supplier callable that will be invoked each time `get_session` is called, allowing dynamic session resolution.

**Algorithm:**
1. Set `cls.__SESSION_SUPPLIER = session_supplier`

### ApiWithCustomSession.set_session(cls, session: GsSession)
Purpose: Set a specific session instance (or clear the override by passing `None`).

**Algorithm:**
1. Branch: if `session is None` -> set `cls.__SESSION_SUPPLIER = None`
2. Branch: else -> set `cls.__SESSION_SUPPLIER = lambda: session` (wrap in a lambda for uniform callable interface)

### ApiWithCustomSession.get_session(cls) -> GsSession
Purpose: Retrieve the current session, either from the custom supplier or from the global `GsSession.current`.

**Algorithm:**
1. Branch: if `cls.__SESSION_SUPPLIER` is truthy -> return `cls.__SESSION_SUPPLIER()`
2. Branch: else -> return `GsSession.current`

## State Mutation
- `__SESSION_SUPPLIER`: Class-level private variable. Modified by `set_session_provider` and `set_session`. Because Python name-mangling applies to `__` prefixed attributes, each subclass that calls these methods modifies its own class-level copy via `cls`.
- Thread safety: No synchronization. If `set_session` / `set_session_provider` is called from one thread while `get_session` is called from another, a race condition exists. In practice, session configuration is expected to happen during initialization, not concurrently with API calls.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None directly raised | - | If `GsSession.current` is not initialized, `get_session` will propagate whatever error `GsSession.current` raises (typically `MqUninitialisedError`) |

## Edge Cases
- `set_session(None)` clears the supplier, reverting to `GsSession.current` -- this is the only way to reset the override
- `set_session_provider` with a supplier that raises an exception will cause `get_session` to propagate that exception on every call
- Because `__SESSION_SUPPLIER` is name-mangled per class, subclass A and subclass B each have independent session overrides; setting a supplier on `DataApi` does not affect `AssetApi` even though both inherit from `ApiWithCustomSession`
- `get_session` checks truthiness of the supplier, so a supplier set to `0`, `False`, or `""` (non-callable falsy values) would fall through to `GsSession.current` rather than raising a `TypeError`

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 4
- Key branches: `session is None` in `set_session` (2 paths), `cls.__SESSION_SUPPLIER` truthiness in `get_session` (2 paths)
- Pragmas: none
