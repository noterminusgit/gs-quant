# transform.py

## Summary
Provides an abstract transformer framework and concrete implementations for transforming and aggregating risk computation results. `Transformer` is a generic ABC. `GenericResultWithInfoTransformer` wraps an arbitrary callable. `ResultWithInfoAggregator` is a dataclass that flattens heterogeneous risk result types (`float`, `FloatWithInfo`, `SeriesWithInfo`, `DataFrameWithInfo`) into a list of `FloatWithInfo` values by summing risk columns with optional coordinate filtering.

## Dependencies
- Internal: `gs_quant.risk.core` (`ResultType`, `DataFrameWithInfo`, `SeriesWithInfo`, `FloatWithInfo`)
- External: `abc` (`ABC`, `abstractmethod`)
- External: `dataclasses` (`dataclass`)
- External: `dataclasses_json` (`dataclass_json`)
- External: `typing` (`Generic`, `Callable`, `Sequence`, `Any`, `TypeVar`, `Iterable`, `Union`, `Optional`)

## Type Definitions

### TypeVars
```python
_InputT = TypeVar('_InputT')
_ResultT = TypeVar('_ResultT')
```

### ResultType (from core)
```python
ResultType = Union[None, dict, tuple, DataFrameWithInfo, FloatWithInfo, SeriesWithInfo]
```

### Transformer (ABC, Generic[_InputT, _ResultT])
Inherits: `ABC`, `Generic[_InputT, _ResultT]`

Abstract base class for all transformers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (none) | - | - | Pure interface |

### GenericResultWithInfoTransformer (class)
Inherits: `Transformer[ResultType, ResultType]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__fn` | `Callable[[ResultType, Sequence[Any]], ResultType]` | (required) | Wrapped transformation function |

### ResultWithInfoAggregator (dataclass, dataclass_json)
Inherits: `Transformer[Iterable[ResultType], FloatWithInfo]`

Decorated with `@dataclass_json` and `@dataclass`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `risk_col` | `str` | `'value'` | Column name to sum for aggregation |
| `filter_coord` | `Optional[object]` | `None` | Optional coordinate filter for DataFrameWithInfo results |

## Enums and Constants

None.

## Functions/Methods

### Transformer.apply(self, data: _InputT, *args, **kwargs) -> _ResultT
Purpose: Abstract method that subclasses must implement to transform input data.

**Algorithm:**
1. Abstract -- no implementation.

---

### GenericResultWithInfoTransformer.__init__(self, fn: Callable[[ResultType, Sequence[Any]], ResultType])
Purpose: Store a callable for later application.

**Algorithm:**
1. Set `self.__fn = fn`.

---

### GenericResultWithInfoTransformer.apply(self, data: ResultType, *args, **kwargs) -> ResultType
Purpose: Delegate to the stored callable.

**Algorithm:**
1. Return `self.__fn(data, *args, **kwargs)`.

---

### ResultWithInfoAggregator.apply(self, results: Iterable[Union[float, FloatWithInfo, SeriesWithInfo, DataFrameWithInfo]], *args, **kwargs) -> Iterable[Union[float, FloatWithInfo]]
Purpose: Flatten heterogeneous risk results into a list of `FloatWithInfo` values by summing the risk column.

**Algorithm:**
1. Initialize `flattened_results = []`.
2. For each `result` in `results`:
   a. Branch: `isinstance(result, float)` -> append `result` directly to `flattened_results`.
   b. Branch: `isinstance(result, FloatWithInfo)` -> set `val = result.raw_value`.
   c. Branch: `isinstance(result, SeriesWithInfo)` -> set `val = getattr(result, self.risk_col).sum()`.
   d. Branch: `isinstance(result, DataFrameWithInfo)`:
      - Branch: `result.empty` -> set `val = 0`.
      - Branch: `self.filter_coord is not None` -> `df = result.filter_by_coord(self.filter_coord)`, then `val = getattr(df, self.risk_col).sum()`.
      - Branch: else -> `val = getattr(result, self.risk_col).sum()`.
   e. Branch: none of the above -> raise `ValueError(f'Aggregation of {type(result).__name__} not currently supported.')`.
   f. For non-float results (steps b-d): extract `risk_key = result.risk_key`, `unit = result.unit`, `error = result.error`.
   g. Append `FloatWithInfo(value=val, risk_key=risk_key, unit=unit, error=error)`.
3. Return `flattened_results`.

**Note on return type:** The declared return type is `Iterable[Union[float, FloatWithInfo]]` but the type annotation on the class says `Transformer[Iterable[ResultType], FloatWithInfo]`. The actual return is always a `list`. Plain `float` inputs pass through without being wrapped in `FloatWithInfo`.

**Note on isinstance ordering:** `FloatWithInfo` inherits from `float`, so the `isinstance(result, float)` check at step 2a catches both plain floats AND `FloatWithInfo` instances. However, `FloatWithInfo` is checked AFTER `float` in the code... Actually, re-reading: step 2a is `isinstance(result, float)` which would match `FloatWithInfo` (since it inherits float). But step 2b checks `isinstance(result, FloatWithInfo)` which is more specific. Since 2a is checked first, `FloatWithInfo` instances will hit the `float` branch and be appended as-is without extracting `raw_value`. This is a significant behavioral quirk (see Bugs Found).

## State Mutation
- `GenericResultWithInfoTransformer.__fn`: Set once in `__init__`, never modified.
- `ResultWithInfoAggregator.risk_col`, `filter_coord`: Dataclass fields, technically mutable but treated as configuration.
- Thread safety: `Transformer.apply` and its implementations are stateless beyond their configuration. Safe for concurrent use if the wrapped `fn` and input data are thread-safe.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `ResultWithInfoAggregator.apply` | When a result item is not `float`, `FloatWithInfo`, `SeriesWithInfo`, or `DataFrameWithInfo` |
| `AttributeError` (implicit) | `ResultWithInfoAggregator.apply` | When `self.risk_col` is not a valid attribute/column on `SeriesWithInfo` or `DataFrameWithInfo` |

## Edge Cases
- `ResultWithInfoAggregator.apply` with an empty `results` iterable returns an empty list.
- Plain `float` results are appended as-is without wrapping in `FloatWithInfo` (no `risk_key`, `unit`, or `error` metadata).
- `FloatWithInfo` inherits from `float`, so `isinstance(FloatWithInfo_instance, float)` is `True`. This means `FloatWithInfo` instances hit the `float` branch (step 2a) and are appended unchanged, rather than being re-wrapped. This is likely intentional to preserve existing `FloatWithInfo` metadata.
- `DataFrameWithInfo.empty` is checked before attempting column access, avoiding errors on empty DataFrames.
- `filter_coord` is typed as `Optional[object]` (very permissive); its actual type depends on what `DataFrameWithInfo.filter_by_coord` accepts.
- `SeriesWithInfo` inherits from `pd.Series` which also has numeric behavior; the `isinstance` check order matters.
- The `@dataclass_json` decorator enables JSON serialization/deserialization of `ResultWithInfoAggregator`, which is important for persistence/transport of aggregation configurations.

## Bugs Found
- Lines 53-55: `isinstance(result, float)` is checked before `isinstance(result, FloatWithInfo)`. Since `FloatWithInfo` inherits from `float`, `FloatWithInfo` instances match the first branch and are appended as raw floats. This means their `risk_key`/`unit`/`error` metadata is preserved (the object itself is still a `FloatWithInfo`) but the code path differs from what the branching structure suggests. If the intent was to always re-extract `raw_value` from `FloatWithInfo`, this is a bug. If the intent was to pass through `FloatWithInfo` unchanged, this is correct but confusing. (OPEN -- likely intentional)

## Coverage Notes
- Branch count: ~8
- Key branches in `ResultWithInfoAggregator.apply`: float, FloatWithInfo, SeriesWithInfo, DataFrameWithInfo (empty, filtered, unfiltered), unsupported type
- Note: The `isinstance(result, float)` branch actually covers both `float` and `FloatWithInfo` due to inheritance. The `FloatWithInfo` branch (step 2b) is dead code -- it can never be reached because step 2a matches first.
- Pragmas: none observed
