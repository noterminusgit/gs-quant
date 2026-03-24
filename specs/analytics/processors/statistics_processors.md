# statistics_processors.py

## Summary
Statistical processor implementations for computing rolling or full-series statistics on time series data. Provides: `PercentilesProcessor`, `PercentileProcessor`, `MeanProcessor`, `SumProcessor`, `StdDevProcessor`, `VarianceProcessor`, `CovarianceProcessor`, `ZscoresProcessor`, `StdMoveProcessor`, and `CompoundGrowthRate`. All inherit from `BaseProcessor` and follow a common pattern of checking child data availability before computing.

## Dependencies
- Internal: `gs_quant.analytics.core.processor` (BaseProcessor, DataCoordinateOrProcessor, DateOrDatetimeOrRDate)
- Internal: `gs_quant.analytics.core.processor_result` (ProcessorResult)
- Internal: `gs_quant.timeseries` (returns)
- Internal: `gs_quant.timeseries.statistics` (percentiles, percentile, Window, mean, sum_, std, var, cov, zscores)
- External: `typing` (Optional, Union)
- External: `pandas` (pd, pd.Series)

## Type Definitions

### Common base fields (inherited from BaseProcessor)
All processors inherit:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id | `str` | auto-generated | `"{ClassName}-{uuid4()}"` |
| value | `ProcessorResult` | `ProcessorResult(False, 'Value not set')` | Current computed result |
| parent | `Optional[BaseProcessor]` | `None` | Parent processor reference |
| parent_attr | `Optional[str]` | `None` | Attribute name in parent |
| children | `Dict[str, ...]` | `{}` | Child processors/coordinates |
| children_data | `Dict[str, ProcessorResult]` | `{}` | Resolved child data |
| data_cell | `Any` | `None` | Data cell reference |
| last_value | `bool` | `False` | Whether to keep only last value |
| measure_processor | `bool` | `False` | Whether this is a measure processor |

### PercentilesProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `Optional[DataCoordinateOrProcessor]` | `None` | Optional second series for two-argument percentiles |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size for rolling computation |

### PercentileProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| n | `float` | (required) | Percentile value to compute |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `None` | Window size; None means full series |

### MeanProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `None` | Window size; None means full series |

### SumProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `None` | Window size; None means full series |

### StdDevProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size; default uses full series with no ramp |

### VarianceProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size; default uses full series with no ramp |

### CovarianceProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `DataCoordinateOrProcessor` | (required) | Second series (positional, not optional) |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size |

### ZscoresProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `None` | Window size; None triggers default `Window(None, 0)` at process time |

### StdMoveProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| w | `Union[Window, int]` | `None` | Window size; None triggers default `Window(None, 0)` at process time |

### CompoundGrowthRate (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| n | `Optional[float]` | `None` | Number of time periods for growth rate calculation |

### Type Aliases (from processor module)
```
DataCoordinateOrProcessor = Union[DataCoordinate, BaseProcessor]
DateOrDatetimeOrRDate = Union[DateOrDatetime, RelativeDate]
```

## Enums and Constants
None defined in this module.

## Functions/Methods

### PercentilesProcessor.__init__(self, a: DataCoordinateOrProcessor, *, b: Optional[DataCoordinateOrProcessor] = None, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, w: Union[Window, int] = Window(None, 0), **kwargs) -> None
Purpose: Initialize percentiles processor with one or two input series and window parameter.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Set `self.children['b'] = b` (may be None)
4. Set `self.start = start`, `self.end = end`, `self.w = w`

### PercentilesProcessor.process(self) -> ProcessorResult
Purpose: Compute percentiles of series a, optionally using series b.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `a_data.success`
     - Get `b_data = self.children_data.get('b')`
     - Branch: `self.children.get('b') and isinstance(b_data, ProcessorResult)`
       - Branch: `b_data.success` -> compute `result = percentiles(a_data.data, b_data.data, w=self.w)`, set `self.value = ProcessorResult(True, result)`
       - Branch: not `b_data.success` -> set `self.value = ProcessorResult(True, 'PercentilesProcessor: b is not a valid series.')`
     - **ALWAYS (fall-through)**: compute `result = percentiles(a_data.data, w=self.w)`, set `self.value = ProcessorResult(True, result)` -- this OVERWRITES any value set in the b-branch above
   - Branch: not `a_data.success` -> set `self.value = ProcessorResult(False, "PercentilesProcessor does not have 'a' series values yet")`
3. Branch: not `isinstance(a_data, ProcessorResult)` -> set `self.value = ProcessorResult(False, "PercentilesProcessor does not have 'a' series yet")`
4. Return `self.value`

### PercentilesProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### PercentileProcessor.__init__(self, a: DataCoordinateOrProcessor, *, n: float, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, w: Union[Window, int] = None, **kwargs) -> None
Purpose: Initialize with series, percentile value, and optional window.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Set `self.n = n`, `self.start = start`, `self.end = end`, `self.w = w`

### PercentileProcessor.process(self) -> ProcessorResult
Purpose: Compute the nth percentile of the series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `a_data.success`
     - `series_length = len(a_data.data)`
     - `window = None`
     - Branch: `self.w` is truthy -> `window = self.w if self.w <= series_length else series_length` (clamp window)
     - Compute `result = percentile(a_data.data, self.n, w=window)`
     - Branch: `not isinstance(result, pd.Series)` -> wrap: `result = pd.Series(result)`
     - Set `self.value = ProcessorResult(True, result)`
   - Branch: not `a_data.success` -> failure
3. Branch: not ProcessorResult -> failure
4. Return `self.value`

### PercentileProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### MeanProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start=None, end=None, w=None, **kwargs) -> None
Purpose: Initialize with series and optional window.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set children['a'], start, end, w

### MeanProcessor.process(self) -> ProcessorResult
Purpose: Compute arithmetic mean, optionally over a rolling window.

**Algorithm:**
1. Get `a_data` from `children_data`
2. Branch: isinstance ProcessorResult
   - Branch: success
     - `series_length = len(a_data.data)`
     - `window = None`
     - Branch: `self.w` truthy -> clamp: `window = self.w if self.w <= series_length else series_length`
     - `result = mean(a_data.data, w=window)`
     - Set `self.value = ProcessorResult(True, result)`
   - Branch: not success -> `ProcessorResult(False, "MeanProcessor does not have 'a' series values yet")`
3. Branch: not ProcessorResult -> `ProcessorResult(False, "MeanProcessor does not have 'a' series yet")`
4. Return `self.value`

### MeanProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### SumProcessor.__init__(self, a, *, start=None, end=None, w=None, **kwargs) -> None
Purpose: Initialize with series and optional window.

**Algorithm:** Same pattern as MeanProcessor.

### SumProcessor.process(self) -> ProcessorResult
Purpose: Compute sum of series, optionally over rolling window.

**Algorithm:** Same as MeanProcessor but calls `sum_(a_data.data, w=window)`. Error messages use "SumProcessor".

### SumProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### StdDevProcessor.__init__(self, a, *, start=None, end=None, w=Window(None, 0), **kwargs) -> None
Purpose: Initialize with series and window (default: `Window(None, 0)`).

**Algorithm:** Same pattern. Note default `w=Window(None, 0)` unlike MeanProcessor/SumProcessor which default to `None`.

### StdDevProcessor.process(self) -> ProcessorResult
Purpose: Compute standard deviation.

**Algorithm:** Same windowed pattern, calls `std(a_data.data, w=window)`. Error messages use "StdDevProcessor".

### StdDevProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### VarianceProcessor.__init__(self, a, *, start=None, end=None, w=Window(None, 0), **kwargs) -> None
Purpose: Initialize with series and window (default: `Window(None, 0)`).

### VarianceProcessor.process(self) -> ProcessorResult
Purpose: Compute variance.

**Algorithm:** Same windowed pattern, calls `var(a_data.data, w=window)`. Error messages use "VarianceProcessor".

### VarianceProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### CovarianceProcessor.__init__(self, a: DataCoordinateOrProcessor, b: DataCoordinateOrProcessor, *, start=None, end=None, w=Window(None, 0), **kwargs) -> None
Purpose: Initialize with two series (both required) and window.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`, `self.children['b'] = b`
3. Set `self.start`, `self.end`, `self.w`

### CovarianceProcessor.process(self) -> ProcessorResult
Purpose: Compute covariance between two series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `a_data.success`
     - Get `b_data = self.children_data.get('b')`
     - Branch: `self.children.get('b') and isinstance(b_data, ProcessorResult)`
       - Branch: `b_data.success` -> `result = cov(a_data.data, b_data.data, w=self.w)`, set `self.value = ProcessorResult(True, result)`
       - Branch: not `b_data.success` -> set `self.value = ProcessorResult(True, "CovarianceProcessor does not 'b' series values yet.")`  (NOTE: success=True with error string)
     - Branch: children 'b' not set or b_data not ProcessorResult -> set `self.value = ProcessorResult(True, 'CovarianceProcessor: b is not a valid series.')` (NOTE: success=True with error string)
   - Branch: not `a_data.success` -> `ProcessorResult(False, "CovarianceProcessor does not have 'a' series values yet")`
3. Branch: not ProcessorResult -> `ProcessorResult(False, "CovarianceProcessor does not have 'a' series yet")`
4. Return `self.value`

### CovarianceProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### ZscoresProcessor.__init__(self, a, *, start=None, end=None, w=None, **kwargs) -> None
Purpose: Initialize with series and optional window.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Set `self.start`, `self.end`, `self.w`

### ZscoresProcessor.process(self) -> ProcessorResult
Purpose: Compute z-scores (standard scores) of the series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: isinstance ProcessorResult
   - Branch: success -> `result = zscores(a_data.data, w=Window(None, 0) if self.w is None else self.w)` -- substitutes default Window when w is None
   - Set `self.value = ProcessorResult(True, result)`
   - Branch: not success -> failure message
3. Branch: not ProcessorResult -> failure message
4. Return `self.value`

### ZscoresProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### StdMoveProcessor.__init__(self, a, *, start=None, end=None, w=None, **kwargs) -> None
Purpose: Initialize with series and optional window.

### StdMoveProcessor.process(self) -> ProcessorResult
Purpose: Compute the most recent return normalized by the standard deviation of historical returns.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: isinstance ProcessorResult
   - Branch: success
     - `data_series = a_data.data`
     - `change_pd = data_series.tail(2)` -- last 2 values
     - `change = returns(change_pd).iloc[-1]` -- compute return of last 2 values, take last
     - `returns_series = returns(data_series.head(-1))` -- compute returns of all except last value
     - `std_result = std(returns_series, w=Window(None, 0) if self.w is None else self.w).iloc[-1]` -- std of returns series, take last value
     - Branch: `change is not None and std_result != 0` -> `self.value = ProcessorResult(True, pd.Series([change / std_result]))`
     - Branch: change is None or std_result == 0 -> `self.value = ProcessorResult(False, "StdMoveProcessor returns a NaN")`
   - Branch: not success -> failure
3. Branch: not ProcessorResult -> failure
4. Return `self.value`

### StdMoveProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### CompoundGrowthRate.__init__(self, a, *, start=None, end=None, n: Optional[float] = None, **kwargs) -> None
Purpose: Initialize with series and time period count.

### CompoundGrowthRate.process(self) -> ProcessorResult
Purpose: Compute compound growth rate as `(last/first)^(1/n) - 1`.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: isinstance ProcessorResult
   - Branch: success
     - `data_series = a_data.data`
     - Compute `(data_series.iloc[-1] / data_series.iloc[0]) ** (1 / self.n) - 1`
     - Wrap in `pd.Series([...])` and set as `ProcessorResult(True, ...)`
   - Branch: not success -> failure
3. Branch: not ProcessorResult -> failure
4. Return `self.value`

### CompoundGrowthRate.get_plot_expression(self) -> None
Purpose: No-op.

## State Mutation
- `self.value`: Set by `process()` in all processors. Stores the `ProcessorResult`.
- `self.children`: Set during `__init__` for all processors.
- `self.children_data`: Populated by inherited `BaseProcessor.update()` before `process()` is called.
- `self.start`, `self.end`, `self.w`, `self.n`: Set during `__init__`, read-only during `process()`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised explicitly) | | All failures return `ProcessorResult(False, message)` |

Potential runtime exceptions (not caught):
| Exception | Source | Condition |
|-----------|--------|-----------|
| `ZeroDivisionError` | `CompoundGrowthRate.process` | When `self.n` is `0` or `None` (divides by `self.n`) |
| `TypeError` | `CompoundGrowthRate.process` | When `self.n` is `None` (cannot use `None` in `1 / None`) |
| `IndexError` | `CompoundGrowthRate.process` | When `data_series` is empty (`.iloc[-1]` / `.iloc[0]`) |
| `IndexError` | `StdMoveProcessor.process` | When series has fewer than 2 values |
| `ZeroDivisionError` | `StdMoveProcessor.process` | Guarded by `std_result != 0` check |

## Edge Cases
- **Window clamping**: For `PercentileProcessor`, `MeanProcessor`, `SumProcessor`, `StdDevProcessor`, `VarianceProcessor`: when `self.w > series_length`, window is clamped to `series_length`. When `self.w` is `0` (falsy), no window is applied (treated as None).
- **PercentilesProcessor fall-through bug**: After the b-branch (lines 62-67), execution always falls through to line 68 which computes single-argument `percentiles(a_data.data, w=self.w)` and overwrites `self.value`. The two-argument result is always lost.
- **CovarianceProcessor success=True on failure**: Lines 395-397 return `ProcessorResult(True, error_string)` when b is unavailable or unsuccessful. This reports success with an error message string as data.
- **StdDevProcessor/VarianceProcessor default w**: Default is `Window(None, 0)`, which is truthy, so the window clamping branch is always entered. `Window(None, 0)` compared with `<=` to `series_length` may raise TypeError depending on Window's `__le__` implementation.
- **CompoundGrowthRate with n=None**: `self.n` defaults to `None`, and `1 / self.n` will raise `TypeError`.
- **CompoundGrowthRate with n=0**: `1 / 0` raises `ZeroDivisionError`.
- **CompoundGrowthRate with empty series**: `.iloc[-1]` and `.iloc[0]` raise `IndexError`.
- **StdMoveProcessor with short series**: `data_series.tail(2)` on a single-element series gives 1 element, and `returns()` on 1 element may return empty series, causing `.iloc[-1]` to raise.
- **ZscoresProcessor w=None handling**: When `self.w is None`, substitutes `Window(None, 0)` at process time rather than in `__init__`.

## Bugs Found
- **PercentilesProcessor lines 64-68**: The two-argument percentiles result (when b is provided and successful) is always overwritten by the single-argument percentiles call on line 68. The `result = percentiles(a_data.data, w=self.w)` line is not inside an `else` block. The two-arg computation is effectively dead code. (OPEN)
- **CovarianceProcessor lines 395-397**: Returns `ProcessorResult(True, error_string)` when b is not available. Should be `ProcessorResult(False, ...)`. (OPEN)
- **CovarianceProcessor line 395**: Error message is malformed: `"CovarianceProcessor does not 'b' series values yet."` -- missing "have". (OPEN)
- **CompoundGrowthRate**: No guard against `self.n` being `None` or `0`, which will cause `TypeError` or `ZeroDivisionError`. (OPEN)

## Coverage Notes
- Branch count: ~42
- Key branch categories:
  - isinstance(a_data, ProcessorResult) check: 10 processors
  - a_data.success check: 10 processors
  - self.w truthy / window clamping: 6 processors (Percentile, Mean, Sum, StdDev, Variance, StdMove via indirect)
  - b-series branches: 2 processors (Percentiles, Covariance)
  - PercentileProcessor: result isinstance pd.Series check (1)
  - StdMoveProcessor: change not None and std_result != 0 (1)
  - ZscoresProcessor: self.w is None (1)
- Pragmas: none
