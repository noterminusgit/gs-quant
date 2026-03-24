# workflow/workflow.py

## Summary
Module-level codec registration for workflow hedge types. Registers `dataclasses_json` decoders for `HedgeTypes` (and optional variants) using deserialization functions from `gs_quant.json_convertors`. This enables automatic polymorphic deserialization of hedge type objects when decoding workflow-related JSON payloads.

## Dependencies
- Internal: `gs_quant.json_convertors` (decode_hedge_type, decode_hedge_types), `gs_quant.target.workflow_quote` (HedgeTypes)
- External: `typing` (Optional, Tuple), `dataclasses_json` (global_config)

## Type Definitions
None (no classes defined in this module).

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| global_config | `GlobalConfig` | imported from `dataclasses_json` | The canonical global codec configuration |

## Functions/Methods
None (module contains only import-time codec registration statements).

## State Mutation
- `global_config.decoders[Optional[HedgeTypes]]`: Set to `decode_hedge_type` at import time.
- `global_config.decoders[HedgeTypes]`: Set to `decode_hedge_type` at import time.
- `global_config.decoders[Optional[Tuple[HedgeTypes, ...]]]`: Set to `decode_hedge_types` at import time.
- Thread safety: Registration occurs once at module import. Subsequent access is read-only.

## Error Handling
None. Module-level statements; any import errors propagate as `ImportError`/`ModuleNotFoundError`.

## Edge Cases
- Importing this module has the side effect of registering decoders globally. Double-importing is harmless (same values overwritten).
- The comment `# noqa - We need to import this one from target` indicates that `HedgeTypes` must come from `gs_quant.target.workflow_quote` specifically, not from a re-export.

## Elixir Porting Notes
- The codec registration pattern maps to protocol implementations or an application startup callback that registers decoder functions in a registry (ETS table or `persistent_term`).
- `decode_hedge_type` / `decode_hedge_types` would be functions in a `JsonConvertors` module, called during JSON decoding via a dispatcher.
- Since Elixir has no global mutable config equivalent to `dataclasses_json.global_config`, consider a compile-time registry or a `Codec` protocol with implementations for each hedge type.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0 (no conditional logic)
- All lines are import-time registration statements. Coverage requires simply importing the module.
- Pragmas: none observed
