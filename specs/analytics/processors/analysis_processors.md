# analysis_processors.py

## Summary
Single processor module containing `DiffProcessor`, which computes the difference in series values over a given lag using the `timeseries.diff` function. Extends `BaseProcessor` and follows the standard processor pattern: store children, process when data arrives, return `ProcessorResult`.

## Dependencies
- Internal:
  - `gs_quant.analytics.core.processor` (BaseProcessor, DataCoordinateOrProcessor, DateOrDatetimeOrRDate)
  - `gs_quant.analytics.core.processor_result` (ProcessorResult)
  - `gs_quant.timeseries` (diff)
- External:
  - `typing` (Optional)

## Type Definitions

### DiffProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | Input series (DataCoordinate or BaseProcessor) |
| obs | `int` | `1` | Number of observations to lag in diff calculation |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date/time for underlying data query |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date/time for underlying data query |

Inherited fields from BaseProcessor:
- `value`: `ProcessorResult` -- current computed value
- `children`: `Dict[str, Any]` -- child processors/coordinates
- `children_data`: `Dict[str, ProcessorResult]` -- resolved child data
- `parent`: reference to parent DataCell or processor

### TypeAlias (imported)
```
DataCoordinateOrProcessor = Union[DataCoordinate, BaseProcessor, DataQueryInfo]
DateOrDatetimeOrRDate = Union[dt.date, dt.datetime, RelativeDate]
```

## Enums and Constants
None.

## Functions/Methods

### DiffProcessor.__init__(self, a: DataCoordinateOrProcessor, *, obs: int = 1, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, **kwargs) -> None
Purpose: Initialize DiffProcessor with input series and lag parameter.

**Algorithm:**
1. Call `super().__init__(**kwargs)` to initialize BaseProcessor
2. Set `self.children['a'] = a`
3. Store `self.obs = obs`
4. Store `self.start = start`
5. Store `self.end = end`

### DiffProcessor.process(self) -> ProcessorResult
Purpose: Compute the difference of the input series over the specified lag.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - True:
     a. Branch: `a_data.success`
        - True: `result = diff(a_data.data, self.obs)`, set `self.value = ProcessorResult(True, result)`
        - False: `self.value = ProcessorResult(False, "DiffProcessor does not have 'a' series values yet")`
   - False: `self.value = ProcessorResult(False, "DiffProcessor does not have 'a' series yet")`
3. Return `self.value`

### DiffProcessor.get_plot_expression(self) -> None
Purpose: Placeholder for plot expression generation (not implemented).

**Algorithm:**
1. `pass` -- returns None

## State Mutation
- `self.children['a']`: Set during `__init__`
- `self.value`: Updated during `process()` -- always overwritten on each call
- `self.children_data`: Populated externally by the processor framework before `process()` is called

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None raised directly | - | All error states are expressed via `ProcessorResult(False, message)` |

Note: If `diff()` raises an exception (e.g., invalid data type), it will propagate uncaught.

## Edge Cases
- `children_data.get('a')` returns `None` (key missing): falls to outer else branch, returns failure
- `a_data` is a `ProcessorResult` with `success=False`: returns failure with "does not have values" message
- `a_data` is some non-ProcessorResult type: returns failure with "does not have series" message
- `obs=0`: valid, `diff` with lag 0 returns original series minus itself (all zeros)

## Bugs Found
None.

## Coverage Notes
- Branch count: 4 (3 distinct paths + 1 implicit)
  - Path 1: `a_data` is not ProcessorResult -> failure
  - Path 2: `a_data` is ProcessorResult, `success=False` -> failure
  - Path 3: `a_data` is ProcessorResult, `success=True` -> compute diff, success
  - Branches: isinstance check (2), success check (2) = 4 branches
- `get_plot_expression`: no branches (pass)
- No pragmas
