# scale_processors.py

## Summary
Scale visualization processors for rendering scale components in DataGrid columns. Provides `SpotMarkerProcessor` (single-value markers with PIPE or DIAMOND shapes), `BarMarkerProcessor` (range markers with start/end values), `ScaleProcessor` (composite scale with min/max and a list of markers), and the `validate_markers_data` helper function that validates marker values against the scale range.

## Dependencies
- Internal: `gs_quant.analytics.common.enumerators` (ScaleShape)
- Internal: `gs_quant.analytics.core` (BaseProcessor)
- Internal: `gs_quant.analytics.core.processor_result` (ProcessorResult)
- External: `math` (isnan)
- External: `typing` (Union, List, Tuple, Dict)

## Type Definitions

### SpotMarkerProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `BaseProcessor` | (required) | Child processor that resolves to a single value |
| name | `str` | (required) | Display name of the scale marker |
| shape | `ScaleShape` | (required) | Must be `ScaleShape.PIPE` or `ScaleShape.DIAMOND` |

Inherited from BaseProcessor:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id | `str` | auto-generated | `"{ClassName}-{uuid4()}"` |
| value | `ProcessorResult` | `ProcessorResult(False, 'Value not set')` | Current computed result |
| parent | `Optional[BaseProcessor]` | `None` | Parent processor reference |
| parent_attr | `Optional[str]` | `None` | Attribute name in parent |
| children | `Dict[str, Union[DataCoordinateOrProcessor, DataQueryInfo]]` | `{}` | Child processors/coordinates |
| children_data | `Dict[str, ProcessorResult]` | `{}` | Resolved child data |
| data_cell | `Any` | `None` | Data cell reference |
| last_value | `bool` | `False` | Whether to keep only last value after post_process |
| measure_processor | `bool` | `False` | Whether this is a measure processor |

### BarMarkerProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['start'] | `BaseProcessor` | (required) | Child processor for the starting value |
| children['end'] | `BaseProcessor` | (required) | Child processor for the ending value |
| name | `str` | (required) | Display name of the scale marker |
| shape | `ScaleShape` | `ScaleShape.BAR` | Always set to BAR (hardcoded) |

### ScaleProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['minimum'] | `BaseProcessor` | (required) | Child processor for the scale minimum |
| children['maximum'] | `BaseProcessor` | (required) | Child processor for the scale maximum |
| children[marker.name] | `SpotOrBarMarker` | (per marker) | Each marker also registered as a child by its name |
| markers | `List[SpotOrBarMarker]` | (required) | List of spot or bar markers to display |

### SpotOrBarMarker (TypeAlias)
```
SpotOrBarMarker = Union[SpotMarkerProcessor, BarMarkerProcessor]
```

## Enums and Constants

### ScaleShape(Enum)
(Defined in `gs_quant.analytics.common.enumerators`)

| Value | Raw | Description |
|-------|-----|-------------|
| DIAMOND | `"diamond"` | Diamond-shaped spot marker |
| PIPE | `"pipe"` | Pipe-shaped spot marker |
| BAR | `"bar"` | Bar range marker spanning start to end |

## Functions/Methods

### SpotMarkerProcessor.__init__(self, a: BaseProcessor, *, name: str, shape: ScaleShape) -> None
Purpose: Construct a spot marker processor, validating that shape is PIPE or DIAMOND.

**Algorithm:**
1. Check if `shape` is in `[ScaleShape.PIPE, ScaleShape.DIAMOND]`
2. Branch: shape not in allowed list -> raise `TypeError("SpotMarkerProcessor only allows PIPE or DIAMOND ScaleShapes.")`
3. Call `super().__init__()`
4. Set `self.children['a'] = a`
5. Set `self.name = name`, `self.shape = shape`

**Raises:** `TypeError` when shape is not PIPE or DIAMOND (e.g., BAR)

### SpotMarkerProcessor.process(self) -> ProcessorResult
Purpose: Resolve the child value into a marker dictionary with name, value, and shape.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `a_data` is `ProcessorResult`
   - Branch: `a_data.success` is True -> set `self.value = ProcessorResult(True, {'name': self.name, 'value': a_data.data.get(-1), 'shape': self.shape.value})`
   - Branch: `a_data.success` is False -> set `self.value = ProcessorResult(False, 'Could not compute pipe marker')`
3. Branch: `a_data` is not `ProcessorResult` -> set `self.value = ProcessorResult(False, 'Processor does not have data')`
4. Return `self.value`

### SpotMarkerProcessor.get_plot_expression(self) -> None
Purpose: No-op. Not applicable for scale markers.

### BarMarkerProcessor.__init__(self, start: BaseProcessor, end: BaseProcessor, *, name: str) -> None
Purpose: Construct a bar marker processor with start and end child processors.

**Algorithm:**
1. Call `super().__init__()`
2. Set `self.children['start'] = start`
3. Set `self.children['end'] = end`
4. Set `self.name = name`
5. Set `self.shape = ScaleShape.BAR` (hardcoded)

### BarMarkerProcessor.process(self) -> ProcessorResult
Purpose: Resolve start and end child values into a bar marker dictionary.

**Algorithm:**
1. Get `start = self.children_data.get('start')`, `end = self.children_data.get('end')`
2. Branch: both `start` and `end` are `ProcessorResult`
   - Branch: both `.success` -> set `self.value = ProcessorResult(True, {'name': self.name, 'start': start.data.get(0), 'end': end.data.get(0), 'shape': self.shape.value})`
   - Branch: either not `.success` -> set `self.value = ProcessorResult(False, "Processor does not have start and end values yet")`
3. Branch: either is not `ProcessorResult` -> set `self.value = ProcessorResult(False, "Processor does not have start and end data yet")`
4. Return `self.value`

### BarMarkerProcessor.get_plot_expression(self) -> None
Purpose: No-op.

### validate_markers_data(result: Dict, marker_data: Dict) -> Tuple[bool, str]
Purpose: Validate that a marker's data values are within the scale's min/max range.

**Algorithm:**
1. Extract `min_val = result['min']`, `max_val = result['max']`
2. Branch: `not min_val or math.isnan(min_val)` -> set `result['min'] = None`, return `(False, 'Min Value needs to exist for Scale to render')`
3. Branch: `not max_val or math.isnan(max_val)` -> set `result['max'] = None`, return `(False, 'Max Value needs to exist for Scale to render')`
4. Extract `marker_name = marker_data["name"]`
5. Branch: `ScaleShape(marker_data['shape']) == ScaleShape.BAR`
   - Extract `starting_val = marker_data['start']`, `ending_val = marker_data['end']`
   - Branch: `starting_val > ending_val or starting_val < min_val or starting_val > max_val` -> return `(False, f'Invalid marker={marker_name} with starting value={starting_val}...')`
   - Branch: `ending_val < starting_val or ending_val < min_val or ending_val > max_val` -> return `(False, f'Invalid marker={marker_name} with ending value={ending_val}...')`
6. Branch: shape is not BAR (PIPE or DIAMOND)
   - Branch: `marker_data['value'] < min_val or marker_data['value'] > max_val` -> return `(False, f'Invalid marker=... with value=... Has to be within range...')`
7. Return `(True, '')`

### ScaleProcessor.__init__(self, minimum: BaseProcessor, maximum: BaseProcessor, *, markers: List[SpotOrBarMarker]) -> None
Purpose: Construct a scale processor with min/max processors and a list of markers.

**Algorithm:**
1. Call `super().__init__()`
2. Set `self.children['minimum'] = minimum`
3. Set `self.children['maximum'] = maximum`
4. Set `self.markers = markers`
5. For each marker in markers: set `self.children[marker.name] = marker` (registers each marker as a named child)

### ScaleProcessor.process(self) -> ProcessorResult
Purpose: Combine min, max, and all marker data into a complete scale result dictionary.

**Algorithm:**
1. Get `min_data = self.children_data.get('minimum')`, `max_data = self.children_data.get('maximum')`
2. Collect `markers_data = [self.children_data.get(marker.name) for marker in self.markers]`
3. Branch: both `min_data` and `max_data` are `ProcessorResult`
   - Branch: both `.success`
     - Build `result = {'min': min_data.data.get(0), 'max': max_data.data.get(0), 'markers': []}`
     - For each `marker_data` in `markers_data`:
       - Branch: `marker_data and marker_data.success and marker_data.data` (truthy check)
         - Call `valid, reason = validate_markers_data(result, marker_data.data)`
         - Branch: `valid` -> append `marker_data.data` to `result['markers']`
         - Branch: not `valid` -> append `{**marker_data.data, **{'invalidReason': reason}}` to `result['markers']`
       - Branch: marker_data is falsy/None/not success -> skip (no append)
     - Set `self.value = ProcessorResult(True, result)`
   - Branch: either not `.success` -> set `self.value = ProcessorResult(False, "Processor does not have min, max values yet")`
4. Branch: either is not `ProcessorResult` -> set `self.value = ProcessorResult(False, "Processor does not have min, max data yet")`
5. Return `self.value`

### ScaleProcessor.get_plot_expression(self) -> None
Purpose: No-op.

## State Mutation
- `self.value`: Set by `process()` in all three processor classes. Stores the `ProcessorResult`.
- `self.children`: Set during `__init__` for all processors. Maps string keys to child processors.
- `self.children_data`: Populated by the inherited `BaseProcessor.update()` method before `process()` is called.
- `result['min']` / `result['max']`: `validate_markers_data` mutates the passed-in `result` dict, setting `result['min'] = None` or `result['max'] = None` when validation fails. This side-effect persists in the `ScaleProcessor.process()` result dict.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `TypeError` | `SpotMarkerProcessor.__init__` | When `shape` is not `ScaleShape.PIPE` or `ScaleShape.DIAMOND` |

Note: No other exceptions are explicitly raised. All failure paths return `ProcessorResult(False, message)` rather than raising.

## Edge Cases
- `min_val = 0` or `max_val = 0`: `validate_markers_data` uses `not min_val` / `not max_val` on line 102/105, which treats `0` as falsy. A scale with min=0 would be rejected as invalid even though 0 is a legitimate minimum value.
- `SpotMarkerProcessor` constructed with `ScaleShape.BAR` -> `TypeError` at construction time.
- `a_data.data.get(-1)` in `SpotMarkerProcessor.process()`: Uses `.get(-1)` on the data (expected to be a `pd.Series`). `pd.Series.get(-1)` looks up key `-1` in the index, NOT the last element by position. If the series has no key `-1`, returns `None`.
- `start.data.get(0)` / `end.data.get(0)` in `BarMarkerProcessor.process()`: Same issue -- looks up key `0` in the index.
- Markers with `None` or falsy `children_data` entries are silently skipped (not appended to result).
- `validate_markers_data` mutates the `result` dict passed in (setting min/max to None), which affects the result returned by `ScaleProcessor.process()`.
- BAR marker validation: the second condition (`ending_val < starting_val`) is partially redundant with the first condition (`starting_val > ending_val`), but both are checked separately.

## Bugs Found
- Line 102: `not min_val` rejects `0` as a valid minimum value. Same on line 105 for `max_val`. This is a semantic bug for scales that start or end at zero. (OPEN)
- Line 47: `a_data.data.get(-1)` uses label-based lookup, not positional. If the Series index is datetime-based, `-1` will never match and will return `None`. (OPEN)

## Coverage Notes
- Branch count: ~22
- Key branches: isinstance checks on ProcessorResult (3 processors x 2 = 6), success checks (6), validate_markers_data min/max/nan checks (4), BAR vs PIPE/DIAMOND shape branch (2), valid/invalid marker (2), SpotMarkerProcessor shape validation (1), marker_data truthy check (1)
- Pragmas: none
