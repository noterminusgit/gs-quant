# base.py

## Summary
Core base classes and utilities for the gs_quant type system. Provides the foundational `Base` class (an abstract dataclass with camelCase/snake_case field coercion, JSON serialization support, field mapping, and cloning), along with `Priceable`, `InstrumentBase`, `Scenario`, `Market`, and supporting types (`EnumBase`, `HashableDict`, `DictBase`, `RiskKey`, `Sentinel`, `QuoteReport`, `CustomComments`, `MarketDataScenario`). Also configures global JSON encoders/decoders for date, time, datetime, float, instrument, and market types via `dataclasses_json`.

## Dependencies

### Internal
- `gs_quant.context_base` (`ContextBase`, `ContextMeta`)
- `gs_quant.json_convertors` (`encode_date_or_str`, `decode_date_or_str`, `decode_optional_date`, `encode_datetime`, `decode_datetime`, `decode_float_or_str`, `decode_instrument`, `encode_dictable`, `decode_quote_report`, `decode_quote_reports`, `decode_custom_comment`, `decode_custom_comments`, `decode_optional_time`, `encode_optional_time`)
- `gs_quant.target.common` (`RiskRequestParameters`) -- lazy import inside `RiskKey.ex_measure` and `RiskKey.ex_historical_diddle`

### External
- `builtins` (used to build `__builtins` set of builtin names)
- `copy` (`copy.copy`)
- `datetime` as `dt` (`dt.date`, `dt.time`, `dt.datetime`)
- `logging` (`logging.getLogger`)
- `sys` (`sys.version_info`)
- `typing` (`Iterable`, `Mapping`, `Optional`, `Union`, `Tuple`, `typing._GenericAlias`)
- `abc` (`ABC`, `ABCMeta`, `abstractmethod`)
- `collections` (`namedtuple`)
- `dataclasses` (`Field`, `InitVar`, `MISSING`, `dataclass`, `field`, `fields`, `replace`)
- `enum` (`EnumMeta`, `Enum`)
- `functools` (`update_wrapper`)
- `numpy` as `np` (`np.generic`)
- `dataclasses_json` (`config`, `global_config`, `LetterCase`, `dataclass_json`)
- `dataclasses_json.core` (`_decode_generic`, `_is_supported_generic`)
- `inflection` (`camelize`, `underscore`)

## Type Definitions

### RiskKey (namedtuple)
Inherits: `namedtuple('RiskKey', ('provider', 'date', 'market', 'params', 'scenario', 'risk_measure'))`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| provider | untyped | (required) | Risk provider |
| date | untyped | (required) | Pricing date |
| market | untyped | (required) | Market data reference |
| params | untyped | (required) | `RiskRequestParameters` instance |
| scenario | untyped | (required) | Scenario reference |
| risk_measure | untyped | (required) | Risk measure specification |

### EnumBase (mixin class)
Inherits: nothing (intended to be used alongside `Enum` in MRO)

No fields. Provides behavior methods for Enum subclasses.

### HashableDict (class)
Inherits: `dict`

No additional fields. Wraps a standard dict to make it hashable by converting all items (recursively for nested dicts) into a tuple.

### DictBase (class)
Inherits: `HashableDict`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _PROPERTIES | `set` | `set()` | Class-level set of allowed property names (snake_case). Empty set means no restriction. |

### Base (ABC, dataclass-compatible)
Inherits: `ABC`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __fields_by_name | `Optional[dict]` | `None` | Class-level cache: `{field_name: dataclasses.Field}`. Lazily populated. Private, name-mangled as `_Base__fields_by_name`. |
| __field_mappings | `Optional[dict]` | `None` | Class-level cache: `{camelCase_mapped_name: snake_case_field_name}`. Lazily populated. Private, name-mangled as `_Base__field_mappings`. |

Note: `Base` itself is not decorated with `@dataclass` -- subclasses are expected to be.

### Priceable (dataclass)
Inherits: `Base`
Decorators: `@dataclass_json`, `@dataclass`

No additional fields. All methods raise `NotImplementedError`.

### Scenario (dataclass, ABC, context manager)
Inherits: `Base`, `ContextBase`, `ABC`
Metaclass: `__ScenarioMeta` (combines `ABCMeta` and `ContextMeta`)
Decorators: `@dataclass`

No additional fields defined here (subclasses add fields).

### RiskMeasureParameter (dataclass, ABC)
Inherits: `Base`, `ABC`
Decorators: `@dataclass`

No additional fields defined here (subclasses add fields like `parameter_type`).

### InstrumentBase (dataclass, ABC)
Inherits: `Base`, `ABC`
Decorators: `@dataclass`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| quantity_ | `InitVar[float]` | `1` | Instrument quantity. `init=False` so not passed to `__init__`, stored as field with default `1`. |

Private attributes (set at instance level, not dataclass fields):
| Attribute | Type | Description |
|-----------|------|-------------|
| __resolution_key | `Optional[RiskKey]` | Set by `resolved()` and `from_instance()`. Accessed via `resolution_key` property. |
| __unresolved | `Optional[InstrumentBase]` | Copy of original pre-resolution instrument. Set by `resolved()` and `from_instance()`. Accessed via `unresolved` property. |
| __metadata | `Optional[Any]` | Arbitrary metadata. Set via `metadata` setter. Accessed via `metadata` property. |

### Market (dataclass, ABC)
Inherits: `ABC`
Decorators: `@dataclass`

No fields defined here. Subclasses must implement `market` and `location` abstract properties.

### Sentinel (class)
No parent.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __name | `str` | (required) | Identity name for sentinel comparison |

### QuoteReport (dataclass, ABC)
Inherits: `Base`, `ABC`
Decorators: `@dataclass`

No fields defined. Abstract marker class.

### CustomComments (dataclass, ABC)
Inherits: `Base`, `ABC`
Decorators: `@dataclass`

No fields defined. Abstract marker class.

### MarketDataScenario (dataclass)
Inherits: `Base`
Decorators: `@handle_camel_case_args`, `@dataclass_json(letter_case=LetterCase.CAMEL)`, `@dataclass(unsafe_hash=True, repr=False)`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| scenario | `Scenario` | `None` | The scenario to apply. Metadata: `field_metadata` (exclude if None). |
| subtract_base | `Optional[bool]` | `False` | Whether to subtract base scenario. Metadata: `field_metadata` (exclude if None). |
| name | `Optional[str]` | `None` | Display name. Metadata: `name_metadata` (always excluded from serialization). |

### __ScenarioMeta (metaclass)
Inherits: `ABCMeta`, `ContextMeta`

No fields or methods. Combines the two metaclasses for `Scenario`.

### Type Aliases / Module-level Config Objects
```python
field_metadata = config(exclude=exclude_none)   # dataclasses_json metadata: exclude field if value is None
name_metadata = config(exclude=exclude_always)   # dataclasses_json metadata: always exclude field
```

## Enums and Constants

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `__builtins` | `set` | `set(dir(builtins))` | Set of Python builtin names |
| `__getattribute__` | `function` | `object.__getattribute__` | Cached reference to avoid repeated lookup |
| `__setattr__` | `function` | `object.__setattr__` | Cached reference to avoid repeated lookup |
| `_rename_cache` | `dict` | `{}` | Memoization cache for `underscore()` conversions |
| `_is_supported_generic_cache` | `dict` | `{}` | Memoization cache for `_is_supported_generic()` calls |
| `field_metadata` | `dict` | `config(exclude=exclude_none)` | Dataclass field metadata that excludes None values during JSON serialization |
| `name_metadata` | `dict` | `config(exclude=exclude_always)` | Dataclass field metadata that always excludes the field during JSON serialization |

### No Enums Defined
`EnumBase` is a mixin, not an enum itself. `EnumMeta` and `Enum` are imported for use.

## Functions/Methods

### exclude_none(o) -> bool
Purpose: Predicate that returns True if value is None (used for dataclasses_json exclusion).

**Algorithm:**
1. Return `o is None`

### exclude_always(_o) -> bool
Purpose: Predicate that always returns True (used for dataclasses_json exclusion of name fields).

**Algorithm:**
1. Return `True`

### is_iterable(o, t) -> bool
Purpose: Check if `o` is an iterable of items all of type `t`.

**Algorithm:**
1. Return `isinstance(o, Iterable) and all(isinstance(it, t) for it in o)`

Note: Short-circuits -- if `o` is not Iterable, does not check items.

### is_instance_or_iterable(o, t) -> bool
Purpose: Check if `o` is of type `t` OR is an iterable of `t`.

**Algorithm:**
1. Return `isinstance(o, t) or is_iterable(o, t)`

### _get_underscore(arg: str) -> str
Purpose: Cached conversion from camelCase to snake_case via `inflection.underscore`.

**Algorithm:**
1. If `arg` not in `_rename_cache`:
   - Compute `underscore(arg)` and store in `_rename_cache[arg]`
2. Return `_rename_cache[arg]`

### _get_is_supported_generic(arg) -> bool
Purpose: Cached wrapper around `dataclasses_json.core._is_supported_generic`.

**Algorithm:**
1. If `arg` in `_is_supported_generic_cache`:
   - Return cached value
2. Else:
   - Compute `_is_supported_generic(arg)`
   - Store in `_is_supported_generic_cache[arg]`
   - Return result

### handle_camel_case_args(cls) -> cls
Purpose: Class decorator that wraps `__init__` to normalize camelCase kwargs to snake_case and apply field mappings.

**Algorithm:**
1. Save reference to original `cls.__init__` as `init`
2. Define inner `wrapper(self, *args, **kwargs)`:
   a. Create empty `normalised_kwargs` dict
   b. For each `(arg, value)` in `kwargs.items()`:
      - Branch: if `arg.isupper()` is False (not all uppercase):
        - Compute `snake_case_arg = _get_underscore(arg)`
        - Branch: if `snake_case_arg != arg` AND `snake_case_arg in kwargs`:
          - Raise `ValueError('{} and {} both specified'.format(arg, snake_case_arg))`
        - Set `arg = snake_case_arg`
      - Apply field mapping: `arg = cls._field_mappings().get(arg, arg)`
      - Store `normalised_kwargs[arg] = value`
   c. Return `init(self, *args, **normalised_kwargs)`
3. Set `cls.__init__ = update_wrapper(wrapper=wrapper, wrapped=init)`
4. Return `cls`

### static_field(val) -> Field
Purpose: Create a dataclass field that is not passed to `__init__` with a fixed default value.

**Algorithm:**
1. Return `field(init=False, default=val)`

### get_enum_value(enum_type: EnumMeta, value: Union[EnumBase, str]) -> Union[EnumBase, str, None]
Purpose: Safely convert a value to an enum member, logging a warning if it does not match.

**Algorithm:**
1. Branch: if `value in (None,)` -> return `None`
2. Branch: if `isinstance(value, enum_type)` -> return `value` (already the right type)
3. Try `enum_value = enum_type(value)`:
   - Branch: on `ValueError`:
     - Log warning: `'Setting value to {}, which is not a valid entry in {}'.format(value, enum_type)`
     - Set `enum_value = value` (pass through the raw value)
4. Return `enum_value`

---

### RiskKey.ex_measure (property) -> RiskKey
Purpose: Return a copy of this RiskKey with `risk_measure=None` and `params.raw_results` preserved but historical diddle set to False.

**Algorithm:**
1. Lazy import `RiskRequestParameters` from `gs_quant.target.common`
2. Return new `RiskKey(self.provider, self.date, self.market, RiskRequestParameters(self.params.csa_term, self.params.raw_results, False, self.params.market_behaviour), self.scenario, None)`

### RiskKey.ex_historical_diddle (property) -> RiskKey
Purpose: Return a copy of this RiskKey with historical diddle set to False but risk_measure preserved.

**Algorithm:**
1. Lazy import `RiskRequestParameters` from `gs_quant.target.common`
2. Return new `RiskKey(self.provider, self.date, self.market, RiskRequestParameters(self.params.csa_term, self.params.raw_results, False, self.params.market_behaviour), self.scenario, self.risk_measure)`

### RiskKey.fields (property) -> tuple
Purpose: Return the field names of the namedtuple.

**Algorithm:**
1. Return `self._fields`

---

### EnumBase._missing_(cls: EnumMeta, key) -> Optional[Enum]
Purpose: Case-insensitive enum lookup fallback. Called by Enum metaclass when standard lookup fails.

**Algorithm:**
1. Branch: if `not isinstance(key, str)`:
   - Convert: `key = str(key)`
2. Return `next((m for m in cls.__members__.values() if m.value.lower() == key.lower()), None)`

### EnumBase.__reduce_ex__(self, protocol) -> tuple
Purpose: Support pickling by returning `(class, (value,))`.

**Algorithm:**
1. Return `(self.__class__, (self.value,))`

### EnumBase.__lt__(self: EnumMeta, other) -> bool
Purpose: Less-than comparison by raw value.

**Algorithm:**
1. Return `self.value < other.value`

### EnumBase.__repr__(self) -> str
Purpose: String representation.

**Algorithm:**
1. Return `str(self)` (delegates to `__str__`)

### EnumBase.__str__(self) -> str
Purpose: String conversion returns the raw value.

**Algorithm:**
1. Return `self.value`

---

### HashableDict.hashables(in_dict) -> Tuple (staticmethod)
Purpose: Recursively convert a dict into a hashable nested tuple structure.

**Algorithm:**
1. Create empty list `hashables`
2. For each `it` in `in_dict.items()`:
   - Branch: if `isinstance(it[1], dict)`:
     - Append `(it[0], HashableDict.hashables(it[1]))` (recursive)
   - Else:
     - Append `it` (the key-value tuple)
3. Return `tuple(hashables)`

### HashableDict.__hash__(self) -> int
Purpose: Make dict hashable.

**Algorithm:**
1. Return `hash(HashableDict.hashables(self))`

---

### DictBase.__init__(self, *args, **kwargs)
Purpose: Initialize with optional property validation and camelCase key conversion, excluding None values.

**Algorithm:**
1. Branch: if `self._PROPERTIES` is truthy (non-empty set):
   - Find first `k` in `kwargs.keys()` where `k not in self._PROPERTIES`
   - Branch: if such `invalid_arg` is found:
     - Raise `AttributeError(f"'{self.__class__.__name__}' has no attribute '{invalid_arg}'")`
2. Call `super().__init__(*args, **{camelize(k, uppercase_first_letter=False): v for k, v in kwargs.items() if v is not None})`

### DictBase.__getitem__(self, item)
Purpose: Look up by camelCase-converted key.

**Algorithm:**
1. Return `super().__getitem__(camelize(item, uppercase_first_letter=False))`

### DictBase.__setitem__(self, key, value)
Purpose: Set value with camelCase-converted key, ignoring None values.

**Algorithm:**
1. Branch: if `value is not None`:
   - Return `super().__setitem__(camelize(key, uppercase_first_letter=False), value)`
2. (Implicitly returns None if value is None -- no-op)

### DictBase.__getattr__(self, item)
Purpose: Attribute-style access to dict values.

**Algorithm:**
1. Branch: if `self._PROPERTIES` is truthy:
   - Branch: if `_get_underscore(item) in self._PROPERTIES`:
     - Return `self.get(item)` (dict `.get()`, returns None if missing)
2. Else (no PROPERTIES restriction):
   - Branch: if `item in self`:
     - Return `self[item]` (uses `__getitem__`)
3. Raise `AttributeError(f"'{self.__class__.__name__}' has no attribute '{item}'")`

### DictBase.__setattr__(self, key, value)
Purpose: Attribute-style setting to dict values, with validation.

**Algorithm:**
1. Branch: if `key in dir(self)`:
   - Return `super().__setattr__(key, value)` (normal attribute set for class-defined attributes)
2. Branch: elif `self._PROPERTIES` is truthy AND `_get_underscore(key) not in self._PROPERTIES`:
   - Raise `AttributeError(f"'{self.__class__.__name__}' has no attribute '{key}'")`
3. Set `self[key] = value` (uses `__setitem__`)

### DictBase.properties(cls) -> set (classmethod)
Purpose: Return the set of allowed properties.

**Algorithm:**
1. Return `cls._PROPERTIES`

---

### Base.__getattr__(self, item)
Purpose: Attribute access with camelCase-to-snake_case fallback and field mapping.

**Algorithm:**
1. Call `fields_by_name = __getattribute__(self, '_fields_by_name')()`
2. Branch: if `item.startswith('_')` OR `item in fields_by_name`:
   - Return `__getattribute__(self, item)` (standard lookup)
3. Compute `snake_case_item = _get_underscore(item)`
4. Get `field_mappings = __getattribute__(self, '_field_mappings')()`
5. Apply mapping: `snake_case_item = field_mappings.get(snake_case_item, snake_case_item)`
6. Try: return `__getattribute__(self, snake_case_item)`
7. Except `AttributeError`: return `__getattribute__(self, item)`

### Base.__setattr__(self, key, value)
Purpose: Attribute setting with camelCase normalization, field mapping, init-check, and type coercion.

**Algorithm:**
1. Compute `snake_case_key = _get_underscore(key)`
2. Apply field mapping: `snake_case_key = self._field_mappings().get(snake_case_key, snake_case_key)`
3. Look up `fld = self._fields_by_name().get(snake_case_key)`
4. Branch: if `fld` is truthy (field exists):
   - Branch: if `not fld.init`:
     - Raise `ValueError(f'{key} cannot be set')`
   - Set `key = snake_case_key`
   - Set `value = self.__coerce_value(fld.type, value)`
5. Call `__setattr__(self, key, value)` (module-level cached `object.__setattr__`)

### Base.__repr__(self) -> str
Purpose: String representation showing name and class.

**Algorithm:**
1. Branch: if `self.name is not None`:
   - Return `f'{self.name} ({self.__class__.__name__})'`
2. Else:
   - Return `super().__repr__()`

### Base.__is_type_match(cls, tp, val) -> bool (classmethod, private)
Purpose: Check if `val` matches type `tp`, handling generics (Union, Tuple), Python 3.9+ GenericAlias, Python 3.10+ UnionType.

**Algorithm:**
1. Branch: if `sys.version_info >= (3, 9)`:
   - Import `GenericAlias` from `types`
   - Set `is_generic_alias = isinstance(tp, (typing._GenericAlias, GenericAlias))`
   - Branch: if `sys.version_info >= (3, 10)` AND `not is_generic_alias`:
     - Import `UnionType` from `types`
     - Branch: if `isinstance(tp, UnionType)`:
       - Return `any(cls.__is_type_match(arg, val) for arg in tp.__args__)`
2. Else (Python < 3.9):
   - Set `is_generic_alias = isinstance(tp, typing._GenericAlias)`
3. Branch: if `not is_generic_alias`:
   - Compute `is_enum_to_str = isinstance(val, Enum) and tp is str`
   - Return `isinstance(tp, type) and (isinstance(val, tp) or is_enum_to_str)`
4. Branch: if `getattr(tp, '_special', False)`:
   - Return `False`
5. Extract `origin = tp.__origin__`, `args = tp.__args__`
6. Branch: if `float in args`:
   - Extend `args += (int,)` (int is compatible with float)
7. Branch: if `origin == Union`:
   - Return `any(cls.__is_type_match(arg, val) for arg in args)`
8. Branch: if `origin is tuple`:
   - Branch: if `not isinstance(val, tuple)` OR `not args`:
     - Return `False`
   - Branch: if `len(args) == 1` OR `args[1] == Ellipsis`:
     - Return `all(cls.__is_type_match(args[0], x) for x in val)` (homogeneous tuple)
   - Else:
     - Return `len(args) == len(val) and all(cls.__is_type_match(arg, x) for arg, x in zip(args, val))` (heterogeneous tuple)
9. Return `False`

### Base.__coerce_value(cls, typ: type, value) -> Any (classmethod, private)
Purpose: Coerce a value to match the expected field type.

**Algorithm:**
1. Branch: if `cls.__is_type_match(typ, value)`:
   - Return `value` (already correct type)
2. Branch: if `isinstance(value, np.generic)`:
   - Return `value.item()` (convert numpy scalar to Python native)
3. Branch: elif `hasattr(value, 'tolist')`:
   - Return `value.tolist()` (convert numpy array/scalar to Python native)
4. Branch: elif `typ in (DictBase, Optional[DictBase])` AND `isinstance(value, Base)`:
   - Return `value.to_dict()` (convert Base instance to dict)
5. Compute `is_supported_generic = _get_is_supported_generic(typ)`
6. Branch: if `is_supported_generic`:
   - Return `_decode_generic(typ, value, False)` (use dataclasses_json decoding)
7. Else:
   - Return `value` (pass through unchanged)

### Base._fields_by_name(cls) -> Mapping[str, Field] (classmethod)
Purpose: Lazily build and cache a dict mapping field names to `dataclasses.Field` objects.

**Algorithm:**
1. Branch: if `cls is Base`:
   - Return `{}` (Base itself has no fields)
2. Branch: if `cls.__fields_by_name is None`:
   - Set `cls.__fields_by_name = {f.name: f for f in fields(cls)}`
3. Return `cls.__fields_by_name`

### Base._field_mappings(cls) -> Mapping[str, str] (classmethod)
Purpose: Lazily build and cache a dict mapping camelCase names (from dataclasses_json letter_case config) back to snake_case field names.

**Algorithm:**
1. Branch: if `cls is Base`:
   - Return `{}`
2. Branch: if `cls.__field_mappings is None`:
   - Create empty `field_mappings` dict
   - For each `fld` in `fields(cls)`:
     - Get `config_fn = fld.metadata.get('dataclasses_json', {}).get('letter_case')`
     - Branch: if `config_fn` is truthy:
       - Compute `mapped_name = config_fn('field_name')`
       - Branch: if `mapped_name` is truthy:
         - Store `field_mappings[mapped_name] = fld.name`
   - Set `cls.__field_mappings = field_mappings`
3. Return `cls.__field_mappings`

### Base.clone(self, **kwargs) -> Base
Purpose: Create a shallow copy with specified fields overridden.

**Algorithm:**
1. Return `replace(self, **kwargs)` (uses `dataclasses.replace`)

### Base.properties(cls) -> set (classmethod)
Purpose: Return set of public property names (strips trailing underscores from field names).

**Algorithm:**
1. Return `set(f[:-1] if f[-1] == '_' else f for f in cls._fields_by_name().keys())`

### Base.properties_init(cls) -> set (classmethod)
Purpose: Return set of public property names for fields that are passed to `__init__`.

**Algorithm:**
1. Return `set(f[:-1] if f[-1] == '_' else f for f, v in cls._fields_by_name().items() if v.init)`

### Base.as_dict(self, as_camel_case: bool = False) -> dict
Purpose: Return dict of non-None field values, optionally with camelCase keys.

**Algorithm:**
1. Create empty `ret` dict
2. Build reverse field mappings: `field_mappings = {v: k for k, v in self._field_mappings().items()}`
3. For each `key` in `self.__fields_by_name.keys()`:
   - Get `value = __getattribute__(self, key)`
   - Apply reverse mapping: `key = field_mappings.get(key, key)`
   - Branch: if `value is not None`:
     - Branch: if `as_camel_case`:
       - Convert `key = camelize(key, uppercase_first_letter=False)`
     - Store `ret[key] = value`
4. Return `ret`

Note: Uses `self.__fields_by_name` directly (name-mangled to `_Base__fields_by_name`), which must be populated first (via `_fields_by_name()` call).

### Base.default_instance(cls) -> Base (classmethod)
Purpose: Construct a default instance with all required init fields set to None and optional fields set to their defaults.

**Algorithm:**
1. Build `required = {f.name: None if f.default == MISSING else f.default for f in fields(cls) if f.init}`
2. Return `cls(**required)`

### Base.from_instance(self, instance)
Purpose: Copy all init field values from another instance of the same type into self.

**Algorithm:**
1. Branch: if `not isinstance(instance, type(self))`:
   - Raise `ValueError('Can only use from_instance with an object of the same type')`
2. For each `fld` in `fields(self.__class__)`:
   - Branch: if `fld.init`:
     - Set `__setattr__(self, fld.name, __getattribute__(instance, fld.name))`

---

### Priceable.resolve(self, in_place: bool = True)
Purpose: Resolve non-supplied properties of an instrument. Abstract placeholder.

**Algorithm:**
1. Raise `NotImplementedError`

### Priceable.dollar_price(self)
Purpose: Compute present value in USD. Abstract placeholder.

**Algorithm:**
1. Raise `NotImplementedError`

### Priceable.price(self)
Purpose: Compute present value in local currency. Abstract placeholder.

**Algorithm:**
1. Raise `NotImplementedError`

### Priceable.calc(self, risk_measure, fn=None)
Purpose: Calculate the value of a risk measure. Abstract placeholder.

**Algorithm:**
1. Raise `NotImplementedError`

---

### Scenario.__lt__(self, other) -> bool
Purpose: Comparison for sorting scenarios.

**Algorithm:**
1. Branch: if `self.__repr__ != other.__repr__` (comparing method objects, not return values):
   - Return `self.name < other.name`
2. Return `False`

Note: This compares the bound method objects (identity), not the repr strings. This means it returns False only if `self` and `other` are the same object or have the same `__repr__` method identity.

### Scenario.__repr__(self) -> str
Purpose: String representation of a scenario.

**Algorithm:**
1. Branch: if `self.name` is truthy:
   - Return `self.name`
2. Else:
   - Get `params = self.as_dict()`
   - Sort keys: `sorted_keys = sorted(params.keys(), key=lambda x: x.lower())`
   - Build params string: join `f'{k}:{params[k].__repr__ if isinstance(params[k], Base) else params[k]}'` for each k (note: for Base instances, this prints the method object, not the repr string -- likely a bug; should be `params[k].__repr__()`)
   - Return `self.scenario_type + '(' + params + ')'`

---

### RiskMeasureParameter.__repr__(self) -> str
Purpose: String representation of a risk measure parameter.

**Algorithm:**
1. Get `params = self.as_dict()`
2. Remove `"parameter_type"` key from params (via `params.pop("parameter_type", None)`)
3. Sort keys: `sorted_keys = sorted(params.keys(), key=lambda x: x.lower())`
4. Build params string: join `f"{k}:{params[k].value if isinstance(params[k], EnumBase) else params[k]}"` for each k
5. Return `f"{self.parameter_type}({params})"`

---

### InstrumentBase.provider (abstract property)
Purpose: Must be implemented by subclasses to return the risk provider.

### InstrumentBase.instrument_quantity (property) -> float
Purpose: Return the instrument quantity.

**Algorithm:**
1. Return `self.quantity_`

### InstrumentBase.resolution_key (property) -> Optional[RiskKey]
Purpose: Return the resolution key if set.

**Algorithm:**
1. Try: return `self.__resolution_key`
2. Except `AttributeError`: return `None`

### InstrumentBase.unresolved (property) -> Optional[InstrumentBase]
Purpose: Return the pre-resolution copy of the instrument.

**Algorithm:**
1. Try: return `self.__unresolved`
2. Except `AttributeError`: return `None`

### InstrumentBase.metadata (property) -> Optional[Any]
Purpose: Return metadata if set.

**Algorithm:**
1. Try: return `self.__metadata`
2. Except `AttributeError`: return `None`

### InstrumentBase.metadata (setter)
Purpose: Set the metadata value.

**Algorithm:**
1. Set `self.__metadata = value`

### InstrumentBase.from_instance(self, instance)
Purpose: Override `Base.from_instance` to also copy resolution state.

**Algorithm:**
1. Set `self.__resolution_key = None`
2. Call `super().from_instance(instance)` (copies all init fields)
3. Set `self.__unresolved = instance.__unresolved` (name-mangled: `instance._InstrumentBase__unresolved`)
4. Set `self.__resolution_key = instance.__resolution_key` (name-mangled: `instance._InstrumentBase__resolution_key`)

### InstrumentBase.resolved(self, values: dict, resolution_key: RiskKey) -> InstrumentBase
Purpose: Create a new resolved instrument from current + resolved values.

**Algorithm:**
1. Get `all_values = self.as_dict(True)` (camelCase keys)
2. Update with resolved values: `all_values.update(values)`
3. Create `new_instrument = self.from_dict(all_values)` (uses dataclasses_json deserialization)
4. Set `new_instrument.name = self.name`
5. Set `new_instrument.__unresolved = copy.copy(self)` (shallow copy of current)
6. Set `new_instrument.__resolution_key = resolution_key`
7. Return `new_instrument`

### InstrumentBase.clone(self, **kwargs) -> InstrumentBase
Purpose: Clone with preservation of resolution state and metadata.

**Algorithm:**
1. Call `new_instrument = super().clone(**kwargs)` (uses `dataclasses.replace`)
2. Set `new_instrument.__unresolved = self.unresolved`
3. Set `new_instrument.metadata = self.metadata`
4. Set `new_instrument.__resolution_key = self.resolution_key`
5. Return `new_instrument`

---

### Market.__hash__(self) -> int
Purpose: Hash by market or location.

**Algorithm:**
1. Return `hash(self.market or self.location)`

### Market.__eq__(self, other) -> bool
Purpose: Equality by market or location.

**Algorithm:**
1. Return `(self.market or self.location) == (other.market or other.location)`

### Market.__lt__(self, other) -> bool
Purpose: Comparison by repr string.

**Algorithm:**
1. Return `repr(self) < repr(other)`

### Market.to_dict(self) -> dict
Purpose: Serialize to dict.

**Algorithm:**
1. Return `self.market.to_dict()`

---

### Sentinel.__init__(self, name: str)
Purpose: Initialize sentinel with a name.

**Algorithm:**
1. Set `self.__name = name`

### Sentinel.__eq__(self, other) -> bool
Purpose: Compare sentinels by name.

**Algorithm:**
1. Return `self.__name == other.__name`

Note: Will raise `AttributeError` if `other` is not a `Sentinel` (no `_Sentinel__name` attribute).

## State Mutation

- `_rename_cache` (module-level dict): Grows as new camelCase-to-snake_case conversions are encountered. Modified by `_get_underscore()`.
- `_is_supported_generic_cache` (module-level dict): Grows as new types are checked. Modified by `_get_is_supported_generic()`.
- `Base.__fields_by_name` (class-level, per subclass): Set to None initially. Lazily populated on first call to `_fields_by_name()` for each concrete subclass. Never reset.
- `Base.__field_mappings` (class-level, per subclass): Set to None initially. Lazily populated on first call to `_field_mappings()` for each concrete subclass. Never reset.
- `InstrumentBase.__resolution_key`: Set to `None` in `from_instance()`, then overwritten. Set in `resolved()` and `clone()`.
- `InstrumentBase.__unresolved`: Set in `from_instance()`, `resolved()`, and `clone()`.
- `InstrumentBase.__metadata`: Set via property setter, preserved in `clone()`.
- `global_config.encoders` / `global_config.decoders`: Modified at module import time to register custom serializers/deserializers for `dt.date`, `dt.time`, `dt.datetime`, `Union[dt.date, str]`, `Union[float, str]`, `InstrumentBase`, `QuoteReport`, `CustomComments`, `Market`, and their `Optional` variants.
- Thread safety: The module-level caches (`_rename_cache`, `_is_supported_generic_cache`) are not thread-safe but dict operations in CPython are effectively thread-safe due to the GIL. The class-level caches on `Base` subclasses face the same consideration.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `handle_camel_case_args` wrapper | Both camelCase and snake_case forms of same kwarg are passed simultaneously |
| `ValueError` | `Base.__setattr__` | Attempting to set a field where `fld.init is False` (static/computed field) |
| `ValueError` | `Base.from_instance` | `instance` is not the same type as `self` |
| `AttributeError` | `DictBase.__init__` | A kwarg key is not in `_PROPERTIES` (when `_PROPERTIES` is non-empty) |
| `AttributeError` | `DictBase.__getattr__` | Attribute not found in properties or dict keys |
| `AttributeError` | `DictBase.__setattr__` | Setting an attribute not in `_PROPERTIES` (when `_PROPERTIES` is non-empty) |
| `NotImplementedError` | `Priceable.resolve` | Always (abstract placeholder) |
| `NotImplementedError` | `Priceable.dollar_price` | Always (abstract placeholder) |
| `NotImplementedError` | `Priceable.price` | Always (abstract placeholder) |
| `NotImplementedError` | `Priceable.calc` | Always (abstract placeholder) |

## Edge Cases

- **EnumBase._missing_ with non-string key**: Converts non-string keys to string before case-insensitive matching. E.g., passing an int `123` will compare `"123"` against enum member values.
- **DictBase.__setitem__ with None**: Silently ignores the set operation (no-op). This means you cannot store `None` values in a `DictBase`.
- **DictBase.__getattr__ with PROPERTIES set**: Returns `None` (from `dict.get()`) for known properties that haven't been set, rather than raising `AttributeError`. This differs from the behavior when `_PROPERTIES` is empty, where missing keys raise `AttributeError`.
- **Base.__repr__ assumes `self.name` exists**: Will raise `AttributeError` if the subclass does not define a `name` field (though all expected subclasses do via dataclass fields).
- **Base.as_dict accesses `self.__fields_by_name` directly**: Uses the name-mangled attribute `_Base__fields_by_name` without calling `_fields_by_name()` first. This works only if `_fields_by_name()` has been called at least once prior (which it typically has via `__setattr__` or `__getattr__`). If not, `__fields_by_name` is `None` and calling `.keys()` on it will fail.
- **Scenario.__lt__ compares method objects**: `self.__repr__ != other.__repr__` compares bound method objects (by identity), not their return values. Two distinct instances of the same class will always have different method objects, so this will typically be `True`, and the method falls through to `self.name < other.name`.
- **Scenario.__repr__ for Base params**: When a param value is a `Base` instance, it uses `params[k].__repr__` (the bound method) rather than `params[k].__repr__()` (the result). This will print something like `<bound method ...>` rather than the intended representation.
- **Sentinel.__eq__ with non-Sentinel**: Comparing a `Sentinel` with a non-`Sentinel` object will raise `AttributeError` because `other.__name` is name-mangled to `_Sentinel__name`.
- **handle_camel_case_args preserves all-uppercase kwargs**: If `arg.isupper()` is True, the arg is not converted to snake_case. This preserves acronym-style parameter names.
- **InstrumentBase.quantity_ field**: Declared as `InitVar[float]` with `init=False` and default `1`. Despite `InitVar`, `init=False` means it's stored as a regular field with default value 1, not passed through `__post_init__`.
- **__is_type_match int/float coercion**: When a type annotation includes `float`, `int` is also accepted as a valid match by appending `(int,)` to the args tuple.

## Bugs Found
- Line 548: `Scenario.__lt__` compares `self.__repr__ != other.__repr__` which compares bound method objects, not repr strings. Likely intended to be `self.__repr__() != other.__repr__()`. (OPEN)
- Line 559: `Scenario.__repr__` uses `params[k].__repr__` (without calling it) for Base instances, producing `<bound method ...>` output instead of the actual repr. Likely intended to be `params[k].__repr__()` or `repr(params[k])`. (OPEN)

## Coverage Notes

### Global Config Registration (lines 703-729)
These are executed unconditionally at module import. No branches.

### Branch Count Estimate: ~65 branches

Major branch points:
- `handle_camel_case_args` wrapper: 4 branches (isupper check, snake != arg AND snake in kwargs, field mapping exists)
- `Base.__getattr__`: 4 branches (starts with '_', in fields_by_name, snake_case found, fallback)
- `Base.__setattr__`: 3 branches (fld exists, fld.init check, coerce)
- `Base.__repr__`: 2 branches (name is not None)
- `Base.__is_type_match`: ~14 branches (version checks, generic alias, special, Union, tuple variants)
- `Base.__coerce_value`: 7 branches (type match, np.generic, tolist, DictBase, supported generic, else)
- `Base._fields_by_name`: 3 branches (is Base, cache miss, cache hit)
- `Base._field_mappings`: 4 branches (is Base, cache miss with config_fn and mapped_name checks, cache hit)
- `Base.as_dict`: 3 branches (value not None, as_camel_case)
- `Base.default_instance`: 2 branches (MISSING vs has default)
- `Base.from_instance`: 2 branches (type check, init check per field)
- `DictBase.__init__`: 3 branches (PROPERTIES set, invalid_arg found)
- `DictBase.__getattr__`: 4 branches (PROPERTIES set, underscore in PROPERTIES, item in self)
- `DictBase.__setattr__`: 3 branches (key in dir, PROPERTIES + not in PROPERTIES)
- `EnumBase._missing_`: 2 branches (isinstance check)
- `get_enum_value`: 3 branches (None, isinstance, ValueError)
- `HashableDict.hashables`: 2 branches per item (dict vs not)
- `InstrumentBase` properties: 3 x 2 branches (try/except for resolution_key, unresolved, metadata)
- `Scenario.__lt__`: 2 branches
- `Scenario.__repr__`: 2 branches (name truthy, isinstance Base per param)
- `_get_underscore`: 2 branches (cache hit/miss)
- `_get_is_supported_generic`: 2 branches (cache hit/miss)

### Pragmas
No `pragma: no cover` markers in this file.
