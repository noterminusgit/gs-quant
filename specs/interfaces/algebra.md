# interfaces/algebra.py

## Summary
Defines the `AlgebraicType` abstract base class, an interface for types that support basic arithmetic operations (addition, subtraction, multiplication, division). Provides default reverse-operator implementations for `__radd__` and `__rmul__` that delegate to the forward operators.

## Dependencies
- Internal: none
- External: `abc` (ABC, abstractmethod)

## Type Definitions

### AlgebraicType (ABC)
Inherits: abc.ABC

Abstract interface requiring implementors to define `__add__`, `__sub__`, `__mul__`, and `__div__`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | | | Pure interface with no instance fields |

## Enums and Constants
None.

## Functions/Methods

### AlgebraicType.__add__(self, other) -> (abstract)
Purpose: Define addition of two algebraic values. Must be implemented by subclasses.

### AlgebraicType.__radd__(self, other)
Purpose: Right-hand addition; delegates to `self.__add__(other)`.

**Algorithm:**
1. Call `self.__add__(other)` and return the result

### AlgebraicType.__sub__(self, other) -> (abstract)
Purpose: Define subtraction. Must be implemented by subclasses.

### AlgebraicType.__mul__(self, other) -> (abstract)
Purpose: Define multiplication. Must be implemented by subclasses.

### AlgebraicType.__rmul__(self, other)
Purpose: Right-hand multiplication; delegates to `self.__mul__(other)`.

**Algorithm:**
1. Call `self.__mul__(other)` and return the result

### AlgebraicType.__div__(self, other) -> (abstract)
Purpose: Define division. Must be implemented by subclasses.

## State Mutation
None. Pure interface with no state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | Python runtime | Instantiating `AlgebraicType` directly or a subclass that does not implement all abstract methods |

## Edge Cases
- `__radd__` assumes commutativity of addition (`a + b == b + a`). If a subclass represents non-commutative addition, `__radd__` must be overridden.
- `__rmul__` assumes commutativity of multiplication. Same caveat as above.
- Uses `__div__` (Python 2 style) rather than `__truediv__` -- in Python 3, `__div__` is not called by the `/` operator. Subclasses need to also implement `__truediv__` to support `/` in Python 3.

## Elixir Porting Notes
- Map to an Elixir protocol (e.g., `GsQuant.Algebra`) with functions `add/2`, `sub/2`, `mul/2`, `div/2`.
- Reverse operators (`__radd__`, `__rmul__`) are unnecessary in Elixir since protocol dispatch is on the first argument; callers would ensure the algebraic type is the first operand.
- Alternatively, implement the `Kernel` operator overloads if using custom structs, though Elixir convention prefers explicit function calls over operator overloading.

## Bugs Found
- `__div__` is the Python 2 division operator. In Python 3, the `/` operator calls `__truediv__`, not `__div__`. Any subclass relying solely on this interface for `/` support would need to separately implement `__truediv__`. (OPEN)

## Coverage Notes
- Branch count: 0 (no conditional logic)
- All methods are either abstract (no body to cover) or single-line delegations.
