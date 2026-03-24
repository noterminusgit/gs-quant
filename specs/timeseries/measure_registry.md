# measure_registry.py

## Summary
Implements a measure registry pattern that dispatches measure function calls based on asset class and asset type. The `MultiMeasure` class acts as a callable dispatcher that selects the appropriate registered function for a given asset. The module-level `registry` dict stores `MultiMeasure` instances keyed by display name. The `register_measure` function is used as a decorator/callback to register measure functions.

## Dependencies
- Internal: `gs_quant.errors` (MqError)
- External: `re`

## Type Definitions
None.

### MultiMeasure (class)
A callable dispatcher that selects and invokes the correct measure function based on an asset's class and type.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| display_name | `str` | (required) | Human-readable name of the measure |
| measure_map | `dict` | `{}` | Maps `AssetClass` -> tuple of registered functions |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `registry` | `dict` | `{}` | Global registry mapping display names to `MultiMeasure` instances |

## Functions/Methods

### MultiMeasure.__init__(self, display_name)
Purpose: Initialize a MultiMeasure with a display name and empty measure map.

**Algorithm:**
1. Set `self.display_name = display_name`
2. Set `self.measure_map = {}`

### MultiMeasure.get_fn(self, asset) -> function
Purpose: Look up the registered function that matches the given asset's class and type.

**Algorithm:**
1. Get `asset_class` from `asset.asset_class`
2. Get `asset_type` from `asset.get_type()`
3. Get tuple of registered functions for this asset class: `fns = self.measure_map.get(asset_class, ())`
4. Define inner `canonicalize(word)`: strip non-word characters via `re.sub(r"[^\w]", "", word)` then `.casefold()`
5. Canonicalize the asset type value
6. Iterate over registered functions `fns`:
   - Branch: if `fn.asset_type is None` OR canonicalized asset type is in `map(canonicalize, fn.asset_type)`:
     - AND Branch: if `fn.asset_type_excluded is None` OR canonicalized asset type is NOT in `map(canonicalize, fn.asset_type_excluded)`:
       - Return `fn`
7. Branch: if no function matches -> raise `MqError` with display name, asset class, and asset type

**Raises:** `MqError` when no registered function matches the asset's class and type.

### MultiMeasure.__call__(self, asset, *args, **kwargs)
Purpose: Make MultiMeasure callable -- dispatches to the appropriate registered function.

**Algorithm:**
1. Call `self.get_fn(asset)` to find the matching function
2. Call `fn(asset, *args, **kwargs)` and return result

### MultiMeasure.register(self, function)
Purpose: Register a measure function for its declared asset classes.

**Algorithm:**
1. Iterate over `function.asset_class` (tuple of AssetClass values)
2. For each asset class, append function to existing tuple in `self.measure_map` (or create new tuple)
3. Uses tuple concatenation: `self.measure_map[asset_class] = self.measure_map.get(asset_class, ()) + (function,)`

### register_measure(fn) -> MultiMeasure
Purpose: Top-level registration function that creates or retrieves a `MultiMeasure` and registers the given function.

**Algorithm:**
1. Get `display_name` from `fn.display_name` if it exists, else `fn.__name__`
2. Branch: if `display_name` not in `registry` -> create new `MultiMeasure` and store in `registry`
3. Call `multi_measure.register(fn)` to register the function
4. Return the `MultiMeasure` instance (this means the original function name in the module namespace gets replaced by the `MultiMeasure`)

## State Mutation
- `registry`: Global mutable dict. Modified by `register_measure()` when new measures are registered. Once populated (typically at import time), it is read by measure dispatch.
- `MultiMeasure.measure_map`: Modified by `register()` to add functions. Uses tuple concatenation (immutable tuples replaced), so individual entries are not mutated in-place.
- Thread safety: No locking. Registration is expected to happen at module import time (single-threaded). Runtime dispatch via `get_fn` and `__call__` is read-only on the measure_map.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqError` | `MultiMeasure.get_fn` | No registered function matches the asset's class and type |

## Edge Cases
- `get_fn` uses `map()` (lazy iterator) for the canonicalization of `fn.asset_type` and `fn.asset_type_excluded`. The `in` operator works correctly with lazy iterators but evaluates elements one at a time.
- `canonicalize` strips all non-word characters (including spaces, hyphens, underscores are kept since `\w` includes `_`). This means asset types like `"Single Stock"` become `"singlestock"` after casefolding.
- Registration order matters: `get_fn` returns the FIRST matching function in the tuple for a given asset class. Later registrations for the same asset class are appended to the end.
- `register_measure` returns the `MultiMeasure` instance, not the original function. This means calling `register_measure` as a decorator replaces the function name with the `MultiMeasure` object in the calling module's namespace. Multiple functions registered under the same display name all share the same `MultiMeasure`.
- If `fn.display_name` is falsy (e.g., empty string or `None`), `fn.__name__` is used as fallback.
- If `fn.asset_type` is `None`, the function matches ANY asset type for its asset class (wildcard).
- If `fn.asset_type_excluded` is `None`, no types are excluded.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~8
- Key branches: asset_type is None vs specific types, asset_type_excluded is None vs specific exclusions, match found vs MqError, display_name existence check, registry hit vs miss in register_measure
- The `canonicalize` inner function is called on every dispatch, not cached
