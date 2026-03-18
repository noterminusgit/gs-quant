# special_processors.py

## Summary
Special processors: EntityProcessor (gets field from entity), CoordinateProcessor (gets dimension from coordinate), MeasureProcessor (base for measure-based processors).

## Classes

### EntityProcessor
- process(entity):
  1. If entity is str → failure (couldn't fetch)
  2. Try entity.get_entity() → get(dict, field)
  3. If data truthy → success
  4. Else search identifiers list for matching type
  5. If identifier found → success with value
  6. Else → failure 'Unable to find...'
  7. On ValueError → failure
- update(): no-op
- get_plot_expression(): no-op

### CoordinateProcessor
- process():
  1. Get key: if dimension is Enum → .value, else use as-is
  2. Get coordinate from children['a']
  3. Get dimension_value from coordinate.dimensions
  4. If truthy → success; else failure

### MeasureProcessor
- Base class, sets measure_processor=True
- process(): no-op (pass)

## Edge Cases
- EntityProcessor: entity is string (failed fetch) → immediate failure
- EntityProcessor: field not in entity dict AND not in identifiers → failure
- CoordinateProcessor: coordinate is None → dimension_value is None → failure

## Bugs Found
None.

## Coverage Notes
- ~10 branches
