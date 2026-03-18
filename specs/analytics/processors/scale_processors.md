# scale_processors.py

## Summary
Scale visualization processors: SpotMarkerProcessor, BarMarkerProcessor, ScaleProcessor, and validate_markers_data helper.

## Functions

### validate_markers_data(result, marker_data)
1. Check min_val: if falsy or NaN → set to None, return (False, 'Min Value...')
2. Check max_val: if falsy or NaN → set to None, return (False, 'Max Value...')
3. Branch on shape:
   a. BAR: validate start/end within [min, max] range and start <= end
   b. Else (PIPE/DIAMOND): validate value within [min, max] range
4. Return (True, '') if valid

## Classes

### SpotMarkerProcessor
- __init__: validates shape is PIPE or DIAMOND, raises TypeError otherwise
- process(): get a_data → if success → dict with name/value/shape; else failure

### BarMarkerProcessor
- __init__: takes start and end processors, shape=BAR
- process(): needs both start and end ProcessorResults with success

### ScaleProcessor
- __init__: takes minimum, maximum processors + list of markers
- process():
  1. Check min_data and max_data are successful ProcessorResults
  2. Build result dict with min, max, markers[]
  3. For each marker: if valid → append; else append with invalidReason

## Edge Cases
- min/max = 0 → validate_markers_data treats 0 as falsy (potential issue but 0 is a valid min)
- SpotMarkerProcessor with BAR shape → TypeError at construction
- marker_data is None or not success → silently skipped

## Bugs Found
None critical. Note: `not min_val` on line 102 would reject 0 as min value — could be an issue for scales starting at 0.

## Coverage Notes
- ~20 branches
