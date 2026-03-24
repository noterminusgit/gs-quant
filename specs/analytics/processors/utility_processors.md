# utility_processors.py

## Summary
Utility processors for basic data manipulation and arithmetic operations on time series. Provides: `LastProcessor` (last value), `MinProcessor` (minimum), `MaxProcessor` (maximum), `AppendProcessor` (concatenate two series), `AdditionProcessor`, `SubtractionProcessor`, `MultiplicationProcessor`, `DivisionProcessor` (arithmetic with scalar or second series), `OneDayProcessor` (previous day's last two values), and `NthLastProcessor` (nth-from-last value).

## Dependencies
- Internal: `gs_quant.analytics.core.processor` (BaseProcessor, DataCoordinateOrProcessor, DateOrDatetimeOrRDate)
- Internal: `gs_quant.analytics.core.processor_result` (ProcessorResult)
- External: `typing` (Optional)
- External: `pandas` (pd, pd.Series)

## Type Definitions

### Common base fields (inherited from BaseProcessor)
All processors inherit:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id | `str` | auto-generated | `"{ClassName}-{uuid4()}"` |
| value | `ProcessorResult` | `ProcessorResult(False, 'Value not set')` | Current computed result |
| parent | `Optional[BaseProcessor]` | `None` | Parent processor |
| parent_attr | `Optional[str]` | `None` | Attribute name in parent |
| children | `Dict[str, ...]` | `{}` | Child processors/coordinates |
| children_data | `Dict[str, ProcessorResult]` | `{}` | Resolved child data |
| data_cell | `Any` | `None` | Data cell reference |
| last_value | `bool` | `False` | Whether to keep only last value |
| measure_processor | `bool` | `False` | Whether this is a measure processor |

### LastProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |

### MinProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |

### MaxProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |

### AppendProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `DataCoordinateOrProcessor` | (required) | Second series to append |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |

### AdditionProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `Optional[DataCoordinateOrProcessor]` | `None` | Optional second series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| addend | `Optional[float]` | `None` | Scalar to add to all values |

### SubtractionProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `Optional[DataCoordinateOrProcessor]` | `None` | Optional second series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| subtrahend | `Optional[float]` | `None` | Scalar to subtract from all values |

### MultiplicationProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series |
| children['b'] | `Optional[DataCoordinateOrProcessor]` | `None` | Optional second series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| factor | `Optional[float]` | `None` | Scalar to multiply all values by |

### DivisionProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | First series (numerator) |
| children['b'] | `Optional[DataCoordinateOrProcessor]` | `None` | Optional second series (denominator) |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| dividend | `Optional[float]` | `None` | Scalar divisor (misleadingly named; actually acts as divisor) |

### OneDayProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |

Note: No `start`, `end`, or `**kwargs` handling; takes `**kwargs` and passes to `super().__init__(**kwargs)`.

### NthLastProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | (required) | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date filter |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date filter |
| n | `int` | `1` | Position from end (1 = last, 2 = second-to-last, etc.) |

## Enums and Constants
None defined in this module.

## Functions/Methods

### LastProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start=None, end=None, **kwargs) -> None
Purpose: Initialize with a series.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Set `self.start = start`, `self.end = end`

### LastProcessor.process(self) -> ProcessorResult
Purpose: Return the last value of the series as a single-element Series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `a_data.success and isinstance(a_data.data, pd.Series)` -> `self.value = ProcessorResult(True, pd.Series(a_data.data[-1:]))`
   - Branch: not success or not Series -> **no assignment to self.value** (returns default)
3. Branch: not ProcessorResult -> **no assignment to self.value**
4. Return `self.value`

Note: When conditions fail, `self.value` retains its previous value (initial default: `ProcessorResult(False, 'Value not set')`). No explicit failure message is set.

### LastProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### MinProcessor.__init__(self, a, *, start=None, end=None, **kwargs) -> None
Purpose: Initialize with a series.

### MinProcessor.process(self) -> ProcessorResult
Purpose: Return the minimum value of the series.

**Algorithm:**
1. Get `a = self.children_data.get('a')`
2. Branch: `isinstance(a, ProcessorResult)`
   - Branch: `a.success and isinstance(a.data, pd.Series)` -> `self.value = ProcessorResult(True, pd.Series(min(a.data)))` -- uses Python builtin `min()`, wraps scalar in `pd.Series`
   - Branch: not success or not Series -> `self.value = ProcessorResult(False, "Processor does not data series yet")` (typo: "does not data")
3. Branch: not ProcessorResult -> `self.value = ProcessorResult(False, "Processor does not have series yet")`
4. Return `self.value`

### MinProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### MaxProcessor.__init__(self, a, *, start=None, end=None, **kwargs) -> None
Purpose: Initialize with a series.

### MaxProcessor.process(self) -> ProcessorResult
Purpose: Return the maximum value of the series.

**Algorithm:**
1. Get `a = self.children_data.get('a')`
2. Branch: `isinstance(a, ProcessorResult)`
   - Branch: `a.success and isinstance(a.data, pd.Series)` -> `self.value = ProcessorResult(True, pd.Series(max(a.data)))` -- uses Python builtin `max()`, wraps scalar in `pd.Series`
   - Branch: not success or not Series -> `self.value = ProcessorResult(False, "Processor does not have data series yet")`
3. Branch: not ProcessorResult -> `self.value = ProcessorResult(False, "Processor does not have series yet")`
4. Return `self.value`

### MaxProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### AppendProcessor.__init__(self, a: DataCoordinateOrProcessor, b: DataCoordinateOrProcessor, *, start=None, end=None, **kwargs) -> None
Purpose: Initialize with two series.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`, `self.children['b'] = b`
3. Set `self.start = start`, `self.end = end`

### AppendProcessor.process(self) -> ProcessorResult
Purpose: Concatenate series a and series b.

**Algorithm:**
1. Get `a_data`, `b_data` from `children_data`
2. Branch: both are ProcessorResult
   - Branch: both `.success` -> `result = a_data.data.append(b_data.data)`, set `self.value = ProcessorResult(True, result)`
   - Branch: either not success -> `ProcessorResult(False, "Processor does not have A and B data yet")`
3. Branch: either not ProcessorResult -> `ProcessorResult(False, "Processor does not have A and B data yet")`
4. Return `self.value`

Note: Uses `pd.Series.append()` which is deprecated in pandas 1.4+ and removed in pandas 2.0+.

### AppendProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### AdditionProcessor.__init__(self, a: DataCoordinateOrProcessor, b: Optional[DataCoordinateOrProcessor] = None, *, start=None, end=None, addend: Optional[float] = None, **kwargs) -> None
Purpose: Initialize with series and optional scalar addend.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`, `self.children['b'] = b`
3. Set `self.start`, `self.end`, `self.addend`

### AdditionProcessor.process(self) -> ProcessorResult
Purpose: Add a scalar or second series to the first series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `not a_data.success` -> `self.value = a_data`, return (propagate failure)
   - Branch: `self.addend` truthy -> `value = a_data.data.add(self.addend)`, return `ProcessorResult(True, value)`
   - Get `b_data = self.children_data.get('b')`
   - Branch: `isinstance(b_data, ProcessorResult)`
     - Branch: `b_data.success` -> `value = a_data.data.add(b_data.data)`, set `self.value = ProcessorResult(True, value)`
     - Branch: not `b_data.success` -> `self.value = ProcessorResult(True, b_data.data)` (NOTE: success=True with failure data)
   - Branch: b_data not ProcessorResult -> **no assignment** (returns default or previous value)
3. Branch: a_data not ProcessorResult -> **no assignment** (returns default or previous value)
4. Return `self.value`

### AdditionProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### SubtractionProcessor.__init__(self, a, b=None, *, start=None, end=None, subtrahend=None, **kwargs) -> None
Purpose: Initialize with series and optional scalar subtrahend.

### SubtractionProcessor.process(self) -> ProcessorResult
Purpose: Subtract a scalar or second series from the first series.

**Algorithm:**
1. Get `a_data`
2. Branch: isinstance ProcessorResult
   - Branch: not success -> `self.value = a_data`, return (propagate failure)
   - Branch: `self.subtrahend` truthy -> `value = a_data.data.sub(self.subtrahend)`, return
   - Get `b_data`
   - Branch: isinstance ProcessorResult
     - Branch: success -> `value = a_data.data.sub(b_data.data)`, set result
     - Branch: not success -> `self.value = b_data` (propagate b failure)
   - Branch: b_data not ProcessorResult -> no assignment
3. Branch: a_data not ProcessorResult -> no assignment
4. Return `self.value`

### SubtractionProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### MultiplicationProcessor.__init__(self, a, b=None, *, start=None, end=None, factor=None, **kwargs) -> None
Purpose: Initialize with series and optional scalar factor.

### MultiplicationProcessor.process(self) -> ProcessorResult
Purpose: Multiply the series by a scalar or second series.

**Algorithm:**
1. Get `a_data`
2. Branch: isinstance ProcessorResult
   - Branch: not success -> `self.value = a_data`, **return a_data** (NOTE: returns `a_data` directly, not `self.value`)
   - Branch: `self.factor` truthy -> `value = a_data.data.mul(self.factor)`, return
   - Get `b_data`
   - Branch: isinstance ProcessorResult
     - Branch: success -> `value = a_data.data.mul(b_data.data)`, set result
     - Branch: not success -> `self.value = b_data` (propagate b failure)
   - Branch: b_data not ProcessorResult -> no assignment
3. Branch: a_data not ProcessorResult -> no assignment
4. Return `self.value`

### MultiplicationProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### DivisionProcessor.__init__(self, a, b=None, *, start=None, end=None, dividend=None, **kwargs) -> None
Purpose: Initialize with series and optional scalar divisor.

### DivisionProcessor.process(self) -> ProcessorResult
Purpose: Divide the series by a scalar or second series.

**Algorithm:**
1. Get `a_data`
2. Branch: isinstance ProcessorResult
   - Branch: not success -> `self.value = a_data`, return (propagate failure)
   - Branch: `self.dividend` truthy -> `value = a_data.data.div(self.dividend)`, return
   - Get `b_data`
   - Branch: isinstance ProcessorResult
     - Branch: success -> `value = a_data.data.div(b_data.data)`, set result
     - Branch: not success -> `self.value = b_data` (propagate b failure)
   - Branch: b_data not ProcessorResult -> no assignment
3. Branch: a_data not ProcessorResult -> no assignment
4. Return `self.value`

### DivisionProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### OneDayProcessor.__init__(self, a: DataCoordinateOrProcessor, **kwargs) -> None
Purpose: Initialize with a series.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`

### OneDayProcessor.process(self) -> ProcessorResult
Purpose: Return the last two values from the day before the most recent date.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `not a_data.success` -> `self.value = a_data`, return (propagate failure)
   - `data = a_data.data`
   - Branch: `len(data) >= 2`
     - `value = data.drop(data.index[-1].date(), errors='ignore')` -- drops ALL entries matching the last date
     - Branch: `len(value) >= 2` -> `self.value = ProcessorResult(True, value[-2:])`, return
   - Fall-through: `self.value = ProcessorResult(False, 'Not enough values given to OneDayProcessor.')`
3. Branch: a_data not ProcessorResult -> fall-through to failure
4. Return `self.value`

### OneDayProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### NthLastProcessor.__init__(self, a: DataCoordinateOrProcessor, *, n: int = 1, start=None, end=None, **kwargs) -> None
Purpose: Initialize with a series and position from end.

**Algorithm:**
1. `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Set `self.start`, `self.end`, `self.n = n`

### NthLastProcessor.process(self) -> ProcessorResult
Purpose: Return the nth element from the end of the series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - Branch: `a_data.success and isinstance(a_data.data, pd.Series)`
     - `index = -1 * self.n`
     - `self.value = ProcessorResult(True, pd.Series(a_data.data[index]))` -- uses label/positional index depending on series index type
   - Branch: not success or not Series -> `ProcessorResult(False, "NthLastProcessor does not have 'a' series values yet")`
3. Branch: not ProcessorResult -> `ProcessorResult(False, "NthLastProcessor does not have 'a' series values yet")`
4. Return `self.value`

### NthLastProcessor.get_plot_expression(self) -> None
Purpose: No-op.

## State Mutation
- `self.value`: Set by `process()` in all processors. Stores the `ProcessorResult`.
- `self.children`: Set during `__init__`.
- `self.children_data`: Populated by inherited `BaseProcessor.update()`.
- Arithmetic processors (Addition, Subtraction, Multiplication, Division): May do early return before setting `self.value` in some branches (when a_data is not ProcessorResult, or b_data is not ProcessorResult), leaving the default/previous value.
- `LastProcessor.process()`: Does NOT set `self.value` on failure paths (no explicit failure assignment).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised explicitly) | | All failures return `ProcessorResult(False, message)` |

Potential runtime exceptions (not caught):
| Exception | Source | Condition |
|-----------|--------|-----------|
| `IndexError` | `NthLastProcessor.process` | When `self.n > len(series)` (negative index out of range) |
| `AttributeError` | `AppendProcessor.process` | When using pandas 2.0+ (`.append()` removed) |
| `AttributeError` | `OneDayProcessor.process` | When `data.index[-1]` has no `.date()` method (non-datetime index) |

## Edge Cases
- **Arithmetic scalar with value 0**: `self.addend`, `self.subtrahend`, `self.factor`, `self.dividend` are checked with truthiness (`if self.addend:`). A value of `0` or `0.0` is falsy, so scalar=0 falls through to the b_data branch instead of applying the scalar operation. This is particularly notable for `AdditionProcessor` with `addend=0` and `DivisionProcessor` with `dividend=0` (would bypass the division-by-zero case).
- **AdditionProcessor with failed b**: Returns `ProcessorResult(True, b_data.data)` -- success flag is `True` but data is an error message string.
- **MultiplicationProcessor return mismatch**: On `not a_data.success`, the method returns `a_data` directly instead of `self.value`. Both variables hold the same value, but the pattern differs from other processors.
- **LastProcessor silent failure**: Does not set `self.value` on failure -- returns the default `ProcessorResult(False, 'Value not set')` without a processor-specific message.
- **MinProcessor/MaxProcessor use builtin min()/max()**: These iterate the Series values, not the pandas `.min()`/`.max()` methods. With NaN values, `min()` raises TypeError while `pd.Series.min()` would skip NaN.
- **AppendProcessor pandas compatibility**: Uses the deprecated `pd.Series.append()` which was removed in pandas 2.0. Modern equivalent is `pd.concat([a, b])`.
- **OneDayProcessor date-based drop**: `data.drop(data.index[-1].date(), errors='ignore')` drops ALL entries matching the last date. For intraday data with multiple entries per day, this drops all entries for the most recent day.
- **NthLastProcessor with n > len**: `a_data.data[-n]` where n exceeds series length raises `IndexError`.
- **DivisionProcessor dividend naming**: The parameter is called `dividend` but semantically acts as a divisor (the series is divided BY this value). The mathematical dividend is the number being divided, not the divisor.

## Bugs Found
- **AdditionProcessor line 231**: `self.value = ProcessorResult(True, b_data.data)` returns success=True with failure error string data when b fails. Should be `self.value = b_data` (propagate failure) or `ProcessorResult(False, b_data.data)`. (OPEN)
- **MinProcessor line 91**: Error message typo: `"Processor does not data series yet"` -- missing "have". (OPEN)
- **MultiplicationProcessor line 330**: `return a_data` returns the raw ProcessorResult instead of `return self.value`. While functionally equivalent (same object), it breaks the pattern and means `self.value` is set to `a_data` but the return is also `a_data`. (OPEN)
- **DivisionProcessor**: Parameter named `dividend` is semantically incorrect; it's used as a divisor. (OPEN)

## Coverage Notes
- Branch count: ~42
- Key branch categories:
  - isinstance(a_data, ProcessorResult): 10 processors
  - success check: 10 processors
  - isinstance(a.data, pd.Series) additional check: LastProcessor, MinProcessor, MaxProcessor, NthLastProcessor (4)
  - Scalar truthy check: AdditionProcessor, SubtractionProcessor, MultiplicationProcessor, DivisionProcessor (4)
  - b_data instanceof + success: 5 processors (Append + 4 arithmetic)
  - OneDayProcessor len checks: 2 (len(data) >= 2, len(value) >= 2)
- Pragmas: none
