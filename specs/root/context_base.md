# context_base.py

## Summary
Provides a metaclass-driven context manager framework for thread-local, nestable runtime contexts. `ContextMeta` is a custom metaclass that stores context instances in a thread-local stack (tuple), enabling `with`-block scoping with push/pop semantics. `ContextBase` is the base class for all context-managed objects in gs_quant (e.g., `PricingContext`, `Session`). `ContextBaseWithDefault` extends it with automatic default-instance creation. A `nullcontext` polyfill is included for Python < 3.7 compatibility.

## Dependencies
- Internal: `gs_quant.errors` (MqUninitialisedError, MqValueError)
- External: `threading` (threading.local for thread-local storage), `contextlib` (nullcontext or AbstractContextManager as fallback)

## Type Definitions

### thread_local (module-level global)
Type: `threading.local`

A single module-level `threading.local()` instance. ALL context state is stored as dynamic attributes on this object, keyed by class name. This is the central state store for the entire context system.

### ContextMeta (type)
Inherits: `type` (it is a metaclass)

This is a **metaclass** -- its properties and methods are available on the **class objects** themselves (not on instances). It uses Python's descriptor protocol: properties defined on a metaclass become class-level properties on classes that use that metaclass.

| Field / Property | Type | Storage | Description |
|------------------|------|---------|-------------|
| `__path_key` | `str` | computed | Private property. Returns `"{ClassName}_path"` -- the key used to store the context stack in `thread_local`. Name-mangled to `_ContextMeta__path_key`. |
| `__default_key` | `str` | computed | Private property. Returns `"{ClassName}_default"` -- the key used to store the lazily-created default instance. Name-mangled to `_ContextMeta__default_key`. |
| `path` | `tuple` | `thread_local` | Public property. Returns the current context stack as a tuple. Most-recently-pushed context is at index 0. Returns `()` if no context has been pushed. |
| `current` | getter/setter | `thread_local` | Public property. Getter: returns the top of the stack, or the default instance if the stack is empty. Setter: replaces the entire path with a 1-tuple containing the new value, subject to validation. |
| `current_is_set` | `bool` | computed | Public property. `True` if the path is non-empty OR the default is not None. |
| `__default` | `object \| None` | `thread_local` | Private property. Lazily loads the default value from `thread_local`, or creates it via `cls.default_value()` and caches it. Name-mangled to `_ContextMeta__default`. |
| `prior` | same as cls | computed | Public property. Returns `path[1]` -- the context one level below current in the stack. |
| `has_prior` | `bool` | computed | Public property. `True` if `len(path) >= 2`. |

### ContextBase (class)
Inherits: (implicitly `object`), metaclass=ContextMeta

Provides `__enter__`/`__exit__` and async equivalents. Instances are pushed onto/popped from the class-level thread-local stack.

| Field / Property | Type | Storage | Description |
|------------------|------|---------|-------------|
| `__entered_key` | `str` | computed | Private property. Returns `"{id(self)}_entered"` -- unique per-instance key for tracking entered state. Name-mangled to `_ContextBase__entered_key`. |
| `_cls` | `ContextMeta` | computed | Property. Walks the MRO to find the first class whose `__bases__` contains `ContextBase` or `ContextBaseWithDefault`. This determines which class-level stack the instance pushes onto. |
| `is_entered` | `bool` | `thread_local` | Property. Returns whether this specific instance is currently inside a `with` block. |

### ContextBaseWithDefault (class)
Inherits: `ContextBase`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | -- | -- | No additional fields. Overrides `default_value()` classmethod. |

### nullcontext (class, polyfill only)
Only defined when `contextlib.nullcontext` is not available (Python < 3.7). Otherwise imported from `contextlib`.

Inherits: `AbstractContextManager`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enter_result` | `object \| None` | `None` | Value returned by `__enter__`. |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `thread_local` | `threading.local` | `threading.local()` | Module-level thread-local storage instance shared by all context classes. |

## Functions/Methods

### ContextMeta.__path_key (property, private) -> str
Purpose: Compute the thread-local attribute name for storing this class's context stack.

**Algorithm:**
1. Return `f"{cls.__name__}_path"`

### ContextMeta.__default_key (property, private) -> str
Purpose: Compute the thread-local attribute name for storing this class's default instance.

**Algorithm:**
1. Return `f"{cls.__name__}_default"`

### ContextMeta.default_value() -> object
Purpose: Class method that returns the default value for this context type. Intended to be overridden by subclasses.

**Algorithm:**
1. Return `None`

### ContextMeta.path (property) -> tuple
Purpose: Retrieve the current context stack for this class from thread-local storage.

**Algorithm:**
1. Return `getattr(thread_local, cls.__path_key, ())` -- empty tuple if not set.

### ContextMeta.current (property getter) -> instance
Purpose: Get the current (top-of-stack) context instance, falling back to the default.

**Algorithm:**
1. Get `path = cls.path`
2. Branch: if `path` is empty (falsy) -> use `cls.__default`; else -> use `next(iter(path))` (i.e., `path[0]`)
3. Branch: if resolved value is `None` -> raise `MqUninitialisedError(f"{cls.__name__} is not initialised")`
4. Return the resolved value

### ContextMeta.current (property setter) -> None
Purpose: Set the current context, replacing the entire stack with a single-element tuple. Only allowed when not in a nested context.

**Algorithm:**
1. Get `path = cls.path`
2. Branch: if `cls.has_prior` is `True` -> raise `MqValueError(f"Cannot set current while in a nested context {cls.__name__}")`
3. Branch: if `len(path) == 1`:
   a. Get `cur = cls.current`
   b. Try: if `cur.is_entered` is `True` -> raise `MqValueError(f"Cannot set current while in a nested context {cls.__name__}")`
   c. Except `AttributeError`: pass (the current object may not have `is_entered`)
4. Set `thread_local.{cls.__path_key}` to `(current,)` (a 1-tuple with the new value)

### ContextMeta.current_is_set (property) -> bool
Purpose: Check whether a current context is available (either via stack or default).

**Algorithm:**
1. Return `bool(cls.path) or cls.__default is not None`

### ContextMeta.__default (property, private) -> object | None
Purpose: Lazily create and cache the default instance for this context class.

**Algorithm:**
1. Get `default = getattr(thread_local, cls.__default_key, None)`
2. Branch: if `default is None`:
   a. Call `default = cls.default_value()`
   b. Branch: if `default is not None` -> store it: `setattr(thread_local, cls.__default_key, default)`
3. Return `default`

### ContextMeta.prior (property) -> instance
Purpose: Get the context instance one level below the current top of stack.

**Algorithm:**
1. Get `path = cls.path`
2. Branch: if `len(path) < 2` -> raise `MqValueError(f"Current {cls.__name__} has no prior")`
3. Return `path[1]`

### ContextMeta.has_prior (property) -> bool
Purpose: Check whether there is a prior (parent) context below the current one.

**Algorithm:**
1. Get `path = cls.path`
2. Return `len(path) >= 2`

### ContextMeta.push(cls, context) -> None
Purpose: Push a context instance onto the top of the stack.

**Algorithm:**
1. Set `thread_local.{cls.__path_key}` to `(context,) + cls.path` -- prepend to tuple

### ContextMeta.pop(cls) -> instance
Purpose: Pop the top context instance from the stack and return it.

**Algorithm:**
1. Get `path = cls.path`
2. Set `thread_local.{cls.__path_key}` to `path[1:]` -- remove first element
3. Return `path[0]`

Note: No bounds checking -- will raise `IndexError` if stack is empty.

### ContextBase.__enter__(self) -> self
Purpose: Enter a synchronous context manager. Push self onto the class stack.

**Algorithm:**
1. Call `self._cls.push(self)` -- push onto the correct class-level stack
2. Set `thread_local.{self.__entered_key}` to `True`
3. Call `self._on_enter()` -- subclass hook
4. Return `self`

### ContextBase.__exit__(self, exc_type, exc_val, exc_tb) -> None
Purpose: Exit a synchronous context manager. Pop self from the class stack.

**Algorithm:**
1. Try: call `self._on_exit(exc_type, exc_val, exc_tb)` -- subclass hook
2. Finally (always executes):
   a. Call `self._cls.pop()`
   b. Set `thread_local.{self.__entered_key}` to `False`

Note: Does NOT return `True`, so exceptions are not suppressed.

### ContextBase.__aenter__(self) -> self
Purpose: Enter an async context manager. Push self onto the class stack.

**Algorithm:**
1. Call `self._cls.push(self)`
2. Set `thread_local.{self.__entered_key}` to `True`
3. Await `self._on_aenter()` -- async subclass hook
4. Return `self`

### ContextBase.__aexit__(self, exc_type, exc_val, exc_tb) -> None
Purpose: Exit an async context manager. Pop self from the class stack.

**Algorithm:**
1. Try: await `self._on_aexit(exc_type, exc_val, exc_tb)` -- async subclass hook
2. Finally (always executes):
   a. Call `self._cls.pop()`
   b. Set `thread_local.{self.__entered_key}` to `False`

### ContextBase.__entered_key (property, private) -> str
Purpose: Compute a unique thread-local key for tracking whether this specific instance is entered.

**Algorithm:**
1. Return `f"{id(self)}_entered"` -- uses the object's memory address/id for uniqueness

### ContextBase._cls (property) -> ContextMeta
Purpose: Walk the class hierarchy to find the "registration class" -- the first class that directly inherits from `ContextBase` or `ContextBaseWithDefault`. This determines which stack the instance is pushed onto (e.g., a `PricingContext` subclass should push onto the `PricingContext` stack, not its own).

**Algorithm:**
1. Initialize `seen = set()`, `stack = [self.__class__]`, `cls = None`
2. While `stack` is not empty:
   a. Pop `base` from stack
   b. Branch: if `ContextBase in base.__bases__` OR `ContextBaseWithDefault in base.__bases__` -> set `cls = base`, break
   c. Branch: if `base not in seen` -> add to `seen`, extend stack with `base.__bases__` filtered to subclasses of `ContextBase`
3. Return `cls or self.__class__` -- fallback to `self.__class__` if no intermediate base found

This is a depth-first search up the MRO looking for the "anchor" class.

### ContextBase.is_entered (property) -> bool
Purpose: Check if this specific instance is currently inside a `with` block.

**Algorithm:**
1. Return `getattr(thread_local, self.__entered_key, False)`

### ContextBase._on_enter(self) -> None
Purpose: Subclass hook called after push during `__enter__`. Default is no-op.

### ContextBase._on_exit(self, exc_type, exc_val, exc_tb) -> None
Purpose: Subclass hook called before pop during `__exit__`. Default is no-op.

### ContextBase._on_aenter(self) -> None
Purpose: Async subclass hook called after push during `__aenter__`. Default is no-op.

### ContextBase._on_aexit(self, exc_type, exc_val, exc_tb) -> None
Purpose: Async subclass hook called before pop during `__aexit__`. Default is no-op.

### ContextBaseWithDefault.default_value() -> object
Purpose: Override of `ContextMeta.default_value()`. Creates a default instance by calling `cls()` (the no-arg constructor).

**Algorithm:**
1. Return `cls()` -- instantiate the class with no arguments

### nullcontext.__init__(self, enter_result=None) -> None
Purpose: Store the value to be returned on enter.

### nullcontext.__enter__(self) -> object | None
Purpose: Return `self.enter_result`.

### nullcontext.__exit__(self, exc_type, exc_val, exc_tb) -> None
Purpose: No-op.

## State Mutation

### Thread-local storage keys (on `thread_local`)
All state is stored as dynamic attributes on the module-level `threading.local()` instance. Each thread gets its own independent copy.

- `{ClassName}_path` (tuple): The context stack for a given class. Modified by:
  - `ContextMeta.push()` -- prepends an element
  - `ContextMeta.pop()` -- removes the first element
  - `ContextMeta.current` setter -- replaces entirely with a 1-tuple
  - Indirectly via `ContextBase.__enter__` / `__exit__` / `__aenter__` / `__aexit__`

- `{ClassName}_default` (object | None): The lazily-created default instance. Modified by:
  - `ContextMeta.__default` getter -- set once on first access if `default_value()` returns non-None

- `{id(instance)}_entered` (bool): Whether a specific instance is inside a `with` block. Modified by:
  - `ContextBase.__enter__` / `__aenter__` -- set to `True`
  - `ContextBase.__exit__` / `__aexit__` -- set to `False` (in `finally` block, guaranteed)

### Context lifecycle (enter/exit)
1. **Enter**: `__enter__` pushes `self` onto `cls.path` (prepend to tuple), marks `is_entered = True`, calls `_on_enter()` hook
2. **Exit**: `__exit__` calls `_on_exit()` hook first (may raise), then in `finally` pops from `cls.path`, marks `is_entered = False`
3. **Nesting**: Multiple enters create a stack: `(innermost, ..., outermost)`. `current` returns index 0, `prior` returns index 1.
4. **Async**: `__aenter__`/`__aexit__` follow the same pattern but await the hooks.

### Thread safety
- `threading.local()` provides per-thread isolation. Each thread has its own independent context stacks.
- No locks are used -- thread-local storage is inherently thread-safe for single-thread access.
- Async code sharing the same thread shares the same `thread_local` state (important for asyncio event loops).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqUninitialisedError` | `ContextMeta.current` getter | When path is empty AND default is None |
| `MqValueError` | `ContextMeta.current` setter | When `cls.has_prior` is True (nested context) |
| `MqValueError` | `ContextMeta.current` setter | When `len(path) == 1` and `cur.is_entered` is True |
| `MqValueError` | `ContextMeta.prior` | When `len(path) < 2` |

## Edge Cases
- **Empty stack pop**: `ContextMeta.pop()` does no bounds checking. If called on an empty stack, `path[0]` will raise `IndexError` on empty tuple.
- **Nested context current setter**: The setter has two distinct guards against setting current while nested: (1) `has_prior` check catches depth >= 2, (2) `is_entered` check catches depth == 1 where the single entry is in an active `with` block. The `AttributeError` catch in guard (2) handles cases where the current object does not have `is_entered` (e.g., a plain default value).
- **Default value caching**: `default_value()` is called at most once per thread per class. If it returns `None`, it will be called again on next access (no negative caching).
- **_cls MRO walk**: For deeply nested class hierarchies, the DFS walk finds the "anchor" class -- the first class that directly extends `ContextBase` or `ContextBaseWithDefault`. This means subclasses of e.g. `PricingContext` share the same context stack as `PricingContext` itself. If no such anchor is found (which shouldn't happen for valid subclasses), falls back to `self.__class__`.
- **Exception during _on_exit**: If `_on_exit()` raises, the `finally` block still executes `pop()` and sets `is_entered = False`, ensuring the stack is consistent.
- **id-based entered key**: `__entered_key` uses `id(self)`, which is the memory address. In CPython, if an object is garbage-collected and a new one allocated at the same address, the old key could theoretically collide. In practice this is not an issue because the context manager holds a reference.
- **nullcontext polyfill**: Only used on Python < 3.7. The polyfill inherits from `AbstractContextManager` and provides a no-op context manager.

## Elixir Porting Notes
- **ContextMeta metaclass -> Process dictionary or Agent**: Python's metaclass properties (`path`, `current`, `prior`) that operate on thread-local storage map naturally to Elixir's process dictionary (`Process.get/put`) since each Elixir process is analogous to a Python thread. An `Agent` could also work for shared state.
- **Context stack as tuple**: The immutable tuple stack `(newest, ..., oldest)` maps directly to an Elixir list `[newest | rest]` with `[head | tail]` pattern matching for push/pop.
- **with-block -> Elixir function with try/after**: `__enter__`/`__exit__` maps to a function that takes a callback: `with_context(ctx, fn)` that does push, calls `fn.()`, then pops in an `after` block.
- **Async context**: Elixir doesn't need async context managers since all processes are concurrent. The async hooks can be regular function calls.
- **Name-mangled properties**: Python's `__name` mangling (e.g., `_ContextMeta__path_key`) does not apply in Elixir. Use module attributes or simple function names.
- **ContextBaseWithDefault**: In Elixir, implement as a behaviour with a `default_value/0` callback that returns `{:ok, struct}` or `:none`.

## Coverage Notes
- Branch count: 18 (across all properties/methods with conditionals)
- Pragmas: Line 179 `except ImportError: # pragma: no cover` -- nullcontext polyfill for Python < 3.7, excluded from coverage
- Key branches: `current` getter (path empty vs non-empty, result nil check), `current` setter (has_prior, len==1, is_entered, AttributeError), `__default` (default is None, default_value returns None vs non-None), `_cls` walk (found anchor vs fallback)
