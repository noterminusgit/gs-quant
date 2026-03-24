# module_name.py

## Summary
1-3 sentences on module purpose and role in the overall system.

## Dependencies
- Internal: `gs_quant.module.submodule` (ClassName, function_name)
- External: `package_name` (specific imports used)

## Type Definitions

### ClassName (dataclass | class | namedtuple | ABC)
Inherits: ParentClass

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| field_name | `str` | `None` | Purpose of field |
| field_name | `List[OtherType]` | `[]` | Purpose of field |

### TypeAlias
```
DataCoordinateOrProcessor = Union[DataCoordinate, BaseProcessor, DataQueryInfo]
```

## Enums and Constants

### EnumName(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| MEMBER_A | `"member_a"` | When this value is used |
| MEMBER_B | `"member_b"` | When this value is used |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| CONST_NAME | `int` | `100` | Purpose |

## Functions/Methods

### function_name(param1: type, param2: type = default) -> ReturnType
Purpose: One line description.

**Algorithm:**
1. Step description
2. Branch: condition_a → outcome_a
3. Branch: condition_b → outcome_b
4. Return description

**Raises:** `ErrorType` when condition

### ClassName.method_name(self, param1: type) -> ReturnType
Purpose: One line description.

**Algorithm:**
1. Step description
2. Branch logic with outcomes

## State Mutation
- `global_var`: Modified by `function_name()` when condition
- `self.field`: Set during `__init__`, updated by `method_name()`
- Thread safety: notes on concurrent access patterns

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `method_name` | When param < 0 |
| `MqRequestError` | `api_call` | When HTTP status != 200 |

## Edge Cases
- Description of edge case and expected behavior
- Description of edge case and expected behavior

## Bugs Found
- Line X: description (FIXED | OPEN)

## Coverage Notes
- Branch count: N
- Missing branches: [line numbers] -> reason
- Pragmas: lines marked `pragma: no cover` and why
