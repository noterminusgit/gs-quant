# errors.py

## Summary
Defines the custom exception hierarchy for the gs_quant library. All domain-specific errors inherit from `MqError`, which itself extends Python's built-in `Exception`. Includes specialized HTTP request errors keyed by status code, and a factory function `error_builder` that maps HTTP status codes to the appropriate exception subclass.

## Dependencies
- Internal: none
- External: `sys` (used for `sys.version_info` in `MqRequestError.__str__`)

## Type Definitions

### Exception Hierarchy

```
Exception
  └── MqError
        ├── MqValueError (also inherits ValueError)
        ├── MqTypeError
        ├── MqWrappedError
        ├── MqUninitialisedError
        └── MqRequestError
              ├── MqAuthenticationError
              ├── MqAuthorizationError
              ├── MqRateLimitedError
              ├── MqTimeoutError
              └── MqInternalServerError
```

### MqError (class)
Inherits: `Exception`

Base class for all errors in the gs_quant module. No additional fields or methods beyond what `Exception` provides. Body is `pass`.

### MqValueError (class)
Inherits: `MqError`, `ValueError` (multiple inheritance, in that order)

Used for value validation errors specific to gs_quant. Has no additional fields or methods. By also inheriting `ValueError`, code that catches standard `ValueError` will also catch `MqValueError`.

### MqTypeError (class)
Inherits: `MqError`

Used for type-related errors. No additional fields or methods. Body is `pass`.

### MqWrappedError (class)
Inherits: `MqError`

Used to wrap other errors. No additional fields or methods. Body is `pass`.

### MqUninitialisedError (class)
Inherits: `MqError`

Raised when attempting to use an uninitialized resource (e.g., GsSession not yet established). No additional fields or methods. Body is `pass`.

### MqRequestError (class)
Inherits: `MqError`

Represents an HTTP request error with status code, message, and optional context.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| status | `int` | (required) | HTTP status code of the failed request |
| message | `str` | (required) | Error message from the server or client |
| context | `Optional[str]` | `None` | Additional context about where/why the error occurred |

### MqAuthenticationError (class)
Inherits: `MqRequestError`

Raised for HTTP 401 Unauthorized responses. No additional fields or methods. Body is `pass`. Constructor is inherited from `MqRequestError`.

### MqAuthorizationError (class)
Inherits: `MqRequestError`

Raised for HTTP 403 Forbidden responses. No additional fields or methods. Body is `pass`. Constructor is inherited from `MqRequestError`.

### MqRateLimitedError (class)
Inherits: `MqRequestError`

Raised for HTTP 429 Too Many Requests responses. Created specifically to enable use with the backoff decorator for retry logic. No additional fields or methods. Body is `pass`.

### MqTimeoutError (class)
Inherits: `MqRequestError`

Raised for HTTP 504 Gateway Timeout responses. No additional fields or methods. Body is `pass`.

### MqInternalServerError (class)
Inherits: `MqRequestError`

Raised for HTTP 500 Internal Server Error responses. No additional fields or methods. Body is `pass`.

## Enums and Constants
None.

## Functions/Methods

### MqRequestError.__init__(self, status, message, context=None)
Purpose: Initialize an HTTP request error with status, message, and optional context.

**Algorithm:**
1. Set `self.status = status`
2. Set `self.message = message`
3. Set `self.context = context` (defaults to `None`)

Note: Does NOT call `super().__init__()` explicitly. The `Exception` base class receives no arguments.

### MqRequestError.__str__(self) -> str
Purpose: Produce a human-readable string representation of the request error.

**Algorithm:**
1. Branch: if `self.context` is truthy -> set `prepend = 'context: {context}\n'`
2. Branch: if `self.context` is falsy (None, empty string, etc.) -> set `prepend = ''`
3. Build `result = '{prepend}status: {status}, message: {message}'`
4. Branch: if `sys.version_info.major < 3` -> encode result to ASCII with `'ignore'` error handler (pragma: no cover -- Python 2 compatibility path, unreachable in Python 3)
5. Return `result`

**Output format examples:**
- With context: `"context: some_context\nstatus: 401, message: Unauthorized"`
- Without context: `"status: 404, message: Not Found"`

### error_builder(status: int, message: str, context=None) -> MqRequestError
Purpose: Factory function that constructs the appropriate `MqRequestError` subclass based on HTTP status code.

**Algorithm:**
1. Branch: `status == 401` -> return `MqAuthenticationError(status, message, context)`
2. Branch: `status == 403` -> return `MqAuthorizationError(status, message, context)`
3. Branch: `status == 429` -> return `MqRateLimitedError(status, message, context)`
4. Branch: `status == 500` -> return `MqInternalServerError(status, message, context)`
5. Branch: `status == 504` -> return `MqTimeoutError(status, message, context)`
6. Branch: else (any other status code) -> return `MqRequestError(status, message, context)`

**Status code mapping summary:**

| Status Code | Exception Class |
|-------------|----------------|
| 401 | `MqAuthenticationError` |
| 403 | `MqAuthorizationError` |
| 429 | `MqRateLimitedError` |
| 500 | `MqInternalServerError` |
| 504 | `MqTimeoutError` |
| any other | `MqRequestError` |

## State Mutation
- `MqRequestError.__init__` sets `self.status`, `self.message`, `self.context` on the instance.
- No module-level mutable state.
- No thread safety concerns -- exception objects are typically created and raised in a single thread context.

## Error Handling
This module defines errors; it does not raise them itself. The `error_builder` function is a pure factory -- it constructs and returns exception instances without raising them. The caller is responsible for raising the returned exception.

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised in this module) | -- | -- |

## Edge Cases
- `error_builder` with a status code not in {401, 403, 429, 500, 504} falls through to the generic `MqRequestError`. This includes common codes like 400, 404, 502, 503, etc.
- `MqRequestError.__str__` with `context=None` produces no context prefix. With `context=''` (empty string), the falsy check also produces no context prefix.
- `MqValueError` can be caught by either `except MqError` or `except ValueError` due to multiple inheritance. This is intentional for interop with standard Python exception handling.
- `MqRequestError.__init__` does not validate types of `status` or `message`. Any values are accepted.
- The Python 2 encoding branch in `__str__` is dead code under Python 3 (guarded by `sys.version_info.major < 3`).

## Coverage Notes
- Branch count: 8 (2 in `__str__` for context truthy/falsy, 6 in `error_builder` for the 5 status codes + else)
- The `sys.version_info.major < 3` branch in `MqRequestError.__str__` (line 47) is marked `pragma: no cover` because it is Python 2 compatibility code that is unreachable in any Python 3 runtime. In the Elixir port, this branch should be omitted entirely.
- All other branches are straightforward and fully testable.
