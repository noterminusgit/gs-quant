# config/options.py

## Summary
Provides the `DisplayOptions` configuration class and a module-level singleton instance (`display_options`) that controls display behavior across gs_quant, such as whether to show N/A values.

## Dependencies
- Internal: none
- External: none

## Type Definitions

### DisplayOptions (class)
Inherits: object

Simple configuration holder with getter/setter properties for display-related options.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __show_na | `bool` | `False` | Whether to display N/A values in output |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| display_options | `DisplayOptions` | `DisplayOptions()` | Module-level singleton instance with default settings (`show_na=False`) |

## Functions/Methods

### DisplayOptions.__init__(self, show_na: bool = False)
Purpose: Initialize display options with default values.

**Algorithm:**
1. Set `self.__show_na = show_na`

### DisplayOptions.show_na (property getter) -> bool
Purpose: Return current `show_na` setting.

### DisplayOptions.show_na (property setter)
Purpose: Update the `show_na` setting.

**Algorithm:**
1. Set `self.__show_na = show_na`

## State Mutation
- `display_options` (module-level singleton): Mutable; any code can set `display_options.show_na = True/False` to change display behavior globally.
- Thread safety: No synchronization. Concurrent reads/writes to `show_na` are benign (simple boolean assignment is atomic in CPython) but not formally thread-safe.

## Error Handling
None. No validation on inputs.

## Edge Cases
- `show_na` setter accepts any value, not just `bool`. Passing a non-bool value would not raise but could cause unexpected behavior downstream.
- The module-level `display_options` instance is shared globally; importing the module in multiple places references the same object.

## Elixir Porting Notes
- Map to an application environment config (`Application.get_env/3`) or a simple `Agent` holding display options.
- Elixir's immutable data means "setter" becomes updating the Agent state or reconfiguring the application env.
- For a struct-based approach: `%DisplayOptions{show_na: false}` with a module-level Agent or persistent_term for the singleton.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0 (no conditional logic)
- All paths are straightforward getter/setter -- full coverage is trivial.
