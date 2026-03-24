# quote_reports/core.py

## Summary
Polymorphic deserialization module for quote reports, custom comments, and hedge types. Provides `from_dict` and `from_dicts` functions that inspect a type-discriminator field in incoming dictionaries and construct the appropriate typed dataclass instance. Used by the `dataclasses_json` codec system.

## Dependencies
- Internal: `gs_quant.base` (CustomComments), `gs_quant.workflow` (VisualStructuringReport, BinaryImageComments, HyperLinkImageComments, CustomDeltaHedge, DeltaHedge, HedgeTypes)
- External: `typing` (Dict, Any, Iterable, Union), `dataclasses_json.cfg` (_GlobalConfig)

## Type Definitions
None (no classes defined in this module).

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| global_config | `_GlobalConfig` | `_GlobalConfig()` | Local `dataclasses_json` global config instance (note: separate from the canonical `dataclasses_json.global_config`) |

## Functions/Methods

### quote_report_from_dict(quote_report_dict: Union[Dict[str, Any], VisualStructuringReport]) -> Optional[VisualStructuringReport]
Purpose: Deserialize a single quote report dict into a `VisualStructuringReport` instance.

**Algorithm:**
1. Branch: `quote_report_dict` is `None` -> return `None`
2. Branch: already a `VisualStructuringReport` instance -> return as-is
3. Read `reportType` from dict
4. Branch: `reportType == 'VisualStructuringReport'` -> call `VisualStructuringReport.from_dict()` and return
5. Default: return `None` (unrecognized type)

### quote_reports_from_dicts(quote_report_dicts: Iterable[Dict[str, Any]]) -> Optional[list]
Purpose: Deserialize an iterable of quote report dicts.

**Algorithm:**
1. Branch: `quote_report_dicts` is `None` -> return `None`
2. Iterate each dict, call `quote_report_from_dict()`, collect into list
3. Return list (may contain `None` entries for unrecognized types)

### custom_comment_from_dict(in_dict: Union[Dict[str, Any], CustomComments]) -> Optional[Union[BinaryImageComments, HyperLinkImageComments]]
Purpose: Deserialize a single custom comment dict based on `commentType` discriminator.

**Algorithm:**
1. Branch: `in_dict` is `None` -> return `None`
2. Branch: already a `CustomComments` instance -> return as-is
3. Read `commentType` from dict
4. Branch: `commentType == 'binaryImageComments'` -> `BinaryImageComments.from_dict(in_dict)`
5. Branch: `commentType == 'hyperLinkImageComments'` -> `HyperLinkImageComments.from_dict(in_dict)`
6. Default: return `None`

### custom_comments_from_dicts(in_dicts: Iterable[Dict[str, Any]]) -> Optional[list]
Purpose: Deserialize an iterable of custom comment dicts.

**Algorithm:**
1. Branch: `in_dicts` is `None` -> return `None`
2. Iterate each dict, call `custom_comment_from_dict()`, collect into list
3. Return list

### hedge_type_from_dict(hedge_type_dict: Union[Dict[str, Any], HedgeTypes]) -> Optional[Union[CustomDeltaHedge, DeltaHedge]]
Purpose: Deserialize a single hedge type dict based on `type` discriminator.

**Algorithm:**
1. Branch: `hedge_type_dict` is `None` -> return `None`
2. Branch: already a `HedgeTypes` instance -> return as-is
3. Read `type` from dict
4. Branch: `type == 'CustomDeltaHedge'` -> `CustomDeltaHedge.from_dict(hedge_type_dict)`
5. Branch: `type == 'DeltaHedge'` -> `DeltaHedge.from_dict(hedge_type_dict)`
6. Default: return `None`

### hedge_type_from_dicts(in_dicts: Iterable[Dict[str, Any]]) -> Optional[list]
Purpose: Deserialize an iterable of hedge type dicts.

**Algorithm:**
1. Branch: `in_dicts` is `None` -> return `None`
2. Iterate each dict, call `hedge_type_from_dict()`, collect into list
3. Return list

## State Mutation
- `global_config`: Module-level instance created at import time. Not observed to be modified after creation in this module.
- No instance state; all functions are pure (stateless) transformations.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised) | all functions | Unrecognized types silently return `None` |

Note: Errors from `from_dict()` calls on the target classes (e.g., missing fields) will propagate uncaught.

## Edge Cases
- All `from_dicts` functions can return lists containing `None` entries when individual items have unrecognized types. Callers must handle mixed `None` values.
- Passing an already-typed instance (e.g., `VisualStructuringReport`) to the `from_dict` functions returns it unchanged (passthrough pattern).
- The local `global_config = _GlobalConfig()` on line 31 creates a separate instance from `dataclasses_json.global_config`. This appears unused within the module itself.
- Variable shadowing: local variable `type` shadows the Python builtin `type()` in several functions (lines 38, 59, 83). Not a bug since the builtin is not needed, but poor practice.

## Elixir Porting Notes
- The type-discriminator dispatch pattern maps naturally to Elixir pattern matching in function heads:
  ```elixir
  def from_dict(%{"reportType" => "VisualStructuringReport"} = dict), do: VisualStructuringReport.from_dict(dict)
  def from_dict(%VisualStructuringReport{} = report), do: report
  def from_dict(_), do: nil
  ```
- The `from_dicts` functions are simple `Enum.map/2` calls.
- Consider using tagged tuples `{:ok, result}` / `{:error, :unrecognized_type}` instead of returning `nil` for unrecognized types, to make error handling explicit.

## Bugs Found
None.

## Coverage Notes
- Branch count: ~18 (3 functions x 4-5 branches each, plus 3 list functions x 2 branches)
- All branches are straightforward type-check dispatches.
- Pragmas: none observed
