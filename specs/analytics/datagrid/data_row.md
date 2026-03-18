# data_row.py

## Summary
Row types for DataGrid: Override (base), ValueOverride, DimensionsOverride, ProcessorOverride, RowSeparator, DataRow. Handles serialization and deserialization of row configurations including overrides.

## Classes

### Override (ABC)
- as_dict(): returns {'columnNames': self.column_names}
- from_dict(): pass (base class)

### ValueOverride(Override)
- as_dict(): adds type=VALUE_OVERRIDE, value
- from_dict(): direct construction

### DimensionsOverride(Override)
- __init__: converts Enum keys to .value in dimensions dict
- as_dict(): adds type, dimensions, coordinate.as_dict(); coordinateId if set
- from_dict(): parses dimensions through DataDimension._value2member_map_

### ProcessorOverride(Override)
- as_dict():
  1. If self.processor → set processor dict + processorName
  2. Else → set processor=None, then processor['processorName']=None ← **BUG 3**
- from_dict(): uses BaseProcessor.from_dict

### RowSeparator
- as_dict(): {'type': ROW_SEPARATOR, 'name': self.name}
- from_dict(): direct

### DataRow
- as_dict():
  1. Branch: entity is Entity → get_marquee_id() / entity_type().value
  2. Else → entity as-is, empty string type
  3. If overrides → add overrides list
- from_dict():
  1. For each override_dict: dispatch by type (PROCESSOR/DIMENSIONS/else→Value)
  2. Create DataRow with entity=None (resolved later)
  3. Append to reference_list

## Edge Cases
- ProcessorOverride.as_dict() with processor=None → TypeError (None['processorName'])
- DataRow entity can be a string if Entity fetch failed

## Bugs Found
- **Bug 3** (line 140-141): When processor is None, code sets `override['processor'] = None` then tries `override['processor']['processorName'] = None` which raises TypeError. Should return early or handle None case properly.

## Coverage Notes
- ~18 branches
