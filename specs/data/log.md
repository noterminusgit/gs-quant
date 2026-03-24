# data/log.py

## Summary
Provides three thin logging wrapper functions (`log_debug`, `log_warning`, `log_info`) that prepend a request ID to log messages. If the request ID is falsy (None, empty string, etc.), the constant `NO_REQUEST_ID` is used as a fallback prefix. These wrappers standardize log formatting across the data layer.

## Dependencies
- Internal: (none)
- External: (none -- uses only Python built-in features; the `logger` object is passed in by the caller)

## Type Definitions

No classes, dataclasses, or type aliases are defined in this module.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `NO_REQUEST_ID` | `str` | `"no-request-id"` | Fallback prefix used in log messages when no request ID is provided |

## Functions/Methods

### log_debug(request_id, logger, fmt_str, *args, **kwargs) -> None
Purpose: Log a debug-level message prefixed with the request ID.

**Algorithm:**
1. Evaluate `request_id or NO_REQUEST_ID` -- if `request_id` is falsy (None, `""`, `0`, `False`), use `NO_REQUEST_ID`.
2. Call `logger.debug(f'{resolved_id}: {fmt_str}', *args, **kwargs)`.

**Branches:**
- `request_id` is truthy -> prefix is `request_id`
- `request_id` is falsy -> prefix is `"no-request-id"`

**Parameter types (implicit):**
| Parameter | Expected Type | Description |
|-----------|--------------|-------------|
| `request_id` | `Optional[str]` | Request ID to prepend; falsy values trigger fallback |
| `logger` | `logging.Logger` | Standard library logger instance |
| `fmt_str` | `str` | Format string for the log message (after the prefix) |
| `*args` | `Any` | Positional args passed through to logger method |
| `**kwargs` | `Any` | Keyword args passed through to logger method |

---

### log_warning(request_id, logger, fmt_str, *args, **kwargs) -> None
Purpose: Log a warning-level message prefixed with the request ID.

**Algorithm:**
1. Evaluate `request_id or NO_REQUEST_ID`.
2. Call `logger.warning(f'{resolved_id}: {fmt_str}', *args, **kwargs)`.

**Branches:** Same as `log_debug` (truthy vs falsy `request_id`).

---

### log_info(request_id, logger, fmt_str, *args, **kwargs) -> None
Purpose: Log an info-level message prefixed with the request ID.

**Algorithm:**
1. Evaluate `request_id or NO_REQUEST_ID`.
2. Call `logger.info(f'{resolved_id}: {fmt_str}', *args, **kwargs)`.

**Branches:** Same as `log_debug` (truthy vs falsy `request_id`).

## State Mutation
- No module-level state is mutated.
- Side effects: Each function calls a method on the passed-in `logger` object, which writes to the logging system (file, console, etc.).
- Thread safety: Thread safety depends on the `logger` implementation. Standard library `logging.Logger` is thread-safe.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none) | -- | This module does not raise any exceptions. Errors from the logger itself (e.g., formatting errors) would propagate uncaught. |

## Edge Cases
- `request_id` is `None`: The `or` expression evaluates to `NO_REQUEST_ID`, so the message is prefixed with `"no-request-id"`.
- `request_id` is an empty string `""`: Also falsy, so the fallback is used.
- `request_id` is `0` or `False`: Also falsy; the fallback is used. This is unlikely but technically possible.
- `fmt_str` contains `%`-style format placeholders: The `*args` and `**kwargs` are passed through to the logger, which uses `%`-style formatting internally. The f-string prefix `f'{request_id or NO_REQUEST_ID}: {fmt_str}'` is evaluated first, then the logger applies `%`-formatting with args. This means `fmt_str` should use `%s`/`%d` style, not f-string style, for additional arguments.
- If `logger` is None or not a proper logger: Will raise `AttributeError` when `.debug()`, `.warning()`, or `.info()` is called.

## Coverage Notes
- Branch count: 6 (2 per function: truthy vs falsy request_id)
- All branches are straightforward to test by passing `None` vs a real string for `request_id`.
- No pragmas or excluded lines.
