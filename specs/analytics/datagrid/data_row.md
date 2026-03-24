# data_row.py

## Summary
Row types for DataGrid: `Override` (abstract base), `ValueOverride`, `DimensionsOverride`, `ProcessorOverride`, `RowSeparator`, and `DataRow`. Handles serialization and deserialization of row configurations including overrides. Each override type specializes how a cell's computation is replaced (by value, by alternate dimensions, or by alternate processor).

## Dependencies
- Internal:
  - `gs_quant.analytics.core` (BaseProcessor)
  - `gs_quant.data` (DataCoordinate)
  - `gs_quant.data.fields` (DataDimension)
  - `gs_quant.entities.entity` (Entity)
- External:
  - `abc` (ABC)
  - `enum` (Enum)
  - `typing` (Dict, List, Optional, Union)

## Type Definitions

### TypeAlias
```
DataDimensions = Dict[Union[DataDimension, str], Union[str, float]]
```

### Override (ABC)
Inherits: ABC

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| column_names | `List[str]` | required | Column names this override applies to |

### ValueOverride (class)
Inherits: Override

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| column_names | `List[str]` | required | Inherited from Override |
| value | `Union[float, str, bool]` | required | Static value to set on matching cells |

### DimensionsOverride (class)
Inherits: Override

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| column_names | `List[str]` | required | Inherited from Override |
| dimensions | `Dict[str, Union[str, float]]` | required | Dimension overrides (Enum keys converted to .value) |
| coordinate | `DataCoordinate` | required | Coordinate to apply the override to |
| coordinate_id | `str` | `None` | Optional coordinate ID for additional specificity |

### ProcessorOverride (class)
Inherits: Override

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| column_names | `List[str]` | required | Inherited from Override |
| processor | `BaseProcessor` | required | Replacement processor (can be None) |

### RowSeparator (class)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| name | `str` | required | Name/label for the row separator |

### DataRow (class)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| entity | `Entity` | required | Entity for this row (may be None during deserialization) |
| overrides | `List[Override]` | `[]` | List of overrides for this row |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| DIMENSIONS_OVERRIDE | `str` | `'dimensionsOverride'` | Override type identifier for dimensions |
| PROCESSOR_OVERRIDE | `str` | `'processorOverride'` | Override type identifier for processor |
| VALUE_OVERRIDE | `str` | `'valueOverride'` | Override type identifier for value |
| DATA_ROW | `str` | `'dataRow'` | Row type identifier for data rows |
| ROW_SEPARATOR | `str` | `'rowSeparator'` | Row type identifier for separator rows |

## Functions/Methods

### Override.__init__(self, column_names: List[str]) -> None
Purpose: Initialize base override with column names.

**Algorithm:**
1. Store `self.column_names = column_names`
2. Call `super().__init__()`

### Override.as_dict(self) -> Dict
Purpose: Serialize base override fields.

**Algorithm:**
1. Return `{'columnNames': self.column_names}`

### Override.from_dict(cls, obj, reference_list) -> None
Purpose: Abstract class method stub (no-op).

**Algorithm:**
1. `pass` -- returns None

### ValueOverride.__init__(self, column_names: List[str], value: Union[float, str, bool]) -> None
Purpose: Initialize value override with column names and static value.

**Algorithm:**
1. Call `super().__init__(column_names)`
2. Store `self.value = value`

### ValueOverride.as_dict(self) -> Dict
Purpose: Serialize value override to dict.

**Algorithm:**
1. Call `override = super().as_dict()` to get base dict with columnNames
2. Set `override['type'] = VALUE_OVERRIDE`
3. Set `override['value'] = self.value`
4. Return `override`

### ValueOverride.from_dict(cls, obj, ref) -> ValueOverride
Purpose: Deserialize ValueOverride from dict.

**Algorithm:**
1. Return `ValueOverride(column_names=obj.get('columnNames', []), value=obj['value'])`

### DimensionsOverride.__init__(self, column_names: List[str], dimensions: DataDimensions, coordinate: DataCoordinate, coordinate_id: str = None) -> None
Purpose: Initialize dimensions override, converting Enum keys to string values.

**Algorithm:**
1. Call `super().__init__(column_names)`
2. Convert dimensions dict: for each key `k`, branch:
   - `isinstance(k, Enum)` -> use `k.value` as key
   - else -> use `k` as key
3. Store `self.dimensions`, `self.coordinate`, `self.coordinate_id`

### DimensionsOverride.as_dict(self) -> Dict
Purpose: Serialize dimensions override to dict.

**Algorithm:**
1. Call `override = super().as_dict()` for base dict
2. Set `override['type'] = DIMENSIONS_OVERRIDE`
3. Set `override['dimensions'] = self.dimensions`
4. Set `override['coordinate'] = self.coordinate.as_dict()`
5. Branch: `self.coordinate_id` is truthy -> set `override['coordinateId'] = self.coordinate_id`
6. Return `override`

### DimensionsOverride.from_dict(cls, obj, reference_list) -> DimensionsOverride
Purpose: Deserialize DimensionsOverride from dict, parsing dimension keys through DataDimension.

**Algorithm:**
1. Initialize `parsed_dimensions = {}`
2. Get `data_dimension_map = DataDimension._value2member_map_`
3. For each `(key, value)` in `obj.get('dimensions', {}).items()`:
   a. Branch: `key in data_dimension_map` -> `parsed_dimensions[DataDimension(key)] = value`
   b. Else -> `parsed_dimensions[key] = value`
4. Return `DimensionsOverride(column_names=..., dimensions=parsed_dimensions, coordinate=DataCoordinate.from_dict(...), coordinate_id=obj.get('coordinateId'))`

### ProcessorOverride.__init__(self, column_names: List[str], processor: BaseProcessor) -> None
Purpose: Initialize processor override.

**Algorithm:**
1. Call `super().__init__(column_names=column_names)`
2. Store `self.processor = processor`

### ProcessorOverride.as_dict(self) -> Dict
Purpose: Serialize processor override to dict.

**Algorithm:**
1. Call `override = super().as_dict()` for base dict
2. Set `override['type'] = PROCESSOR_OVERRIDE`
3. Branch: `self.processor` is truthy:
   - True: `override['processor'] = self.processor.as_dict()`, then `override['processor']['processorName'] = self.processor.__class__.__name__`
   - False: `override['processor'] = None` (note: code continues but does not try to access None -- see Bugs)
4. Return `override`

### ProcessorOverride.from_dict(cls, obj, reference_list) -> ProcessorOverride
Purpose: Deserialize ProcessorOverride from dict.

**Algorithm:**
1. Return `ProcessorOverride(column_names=obj.get('columnNames', []), processor=BaseProcessor.from_dict(obj.get('processor', {}), reference_list))`

### RowSeparator.__init__(self, name: str) -> None
Purpose: Initialize row separator with name.

**Algorithm:**
1. Store `self.name = name`

### RowSeparator.as_dict(self) -> Dict
Purpose: Serialize row separator.

**Algorithm:**
1. Return `{'type': ROW_SEPARATOR, 'name': self.name}`

### RowSeparator.from_dict(cls, obj) -> RowSeparator
Purpose: Deserialize RowSeparator from dict.

**Algorithm:**
1. Return `RowSeparator(obj['name'])`

### DataRow.__init__(self, entity: Entity, overrides: Optional[List[Override]] = None) -> None
Purpose: Initialize data row with entity and optional overrides.

**Algorithm:**
1. Store `self.entity = entity`
2. Store `self.overrides = overrides or []` (None defaults to empty list)

### DataRow.as_dict(self) -> Dict
Purpose: Serialize data row, handling Entity vs string entity.

**Algorithm:**
1. Branch: `isinstance(self.entity, Entity)`:
   - True: `entityId = self.entity.get_marquee_id()`, `entityType = self.entity.entity_type().value`
   - False: `entityId = self.entity`, `entityType = ''`
2. Create `data_row` dict with `type=DATA_ROW`, `entityId`, `entityType`
3. Branch: `len(self.overrides)` > 0 -> add `data_row['overrides'] = [override.as_dict() for override in self.overrides]`
4. Return `data_row`

### DataRow.from_dict(cls, obj, reference_list) -> DataRow
Purpose: Deserialize DataRow from dict, dispatching overrides by type.

**Algorithm:**
1. Initialize `overrides = []`
2. For each `override_dict` in `obj.get('overrides', [])`:
   a. Get `override_type = override_dict.get('type')`
   b. Branch on `override_type`:
      - `== PROCESSOR_OVERRIDE` -> `ProcessorOverride.from_dict(override_dict, reference_list)`
      - `== DIMENSIONS_OVERRIDE` -> `DimensionsOverride.from_dict(override_dict, reference_list)`
      - else -> `ValueOverride.from_dict(override_dict, reference_list)`
   c. Append override to `overrides`
3. Create `data_row = DataRow(entity=None, overrides=overrides)` (entity resolved later)
4. Append reference dict to `reference_list` with type, entityId, entityType, and reference to data_row
5. Return `data_row`

## State Mutation
- `DimensionsOverride.__init__`: Converts Enum keys in `dimensions` dict to string values
- `DataRow.from_dict`: Appends to `reference_list` (side effect on caller's list) for later entity resolution
- `DataRow.entity`: Set to `None` during `from_dict`; resolved externally later

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `ValueOverride.from_dict` | When `obj['value']` key is missing |
| `KeyError` | `RowSeparator.from_dict` | When `obj['name']` key is missing |

## Edge Cases
- `ProcessorOverride.as_dict()` with `processor=None`: Sets `override['processor'] = None` and returns -- no TypeError in current code (the original spec's Bug 3 note about a subsequent `None['processorName']` access does not occur because the else branch returns without that line)
- `DataRow.entity` can be a string if Entity fetch failed -- `as_dict()` handles this with isinstance check
- `DataRow.overrides` defaults to `[]` when `None` is passed
- `DimensionsOverride.from_dict`: dimension keys not found in `DataDimension._value2member_map_` are preserved as plain strings
- `DataRow.as_dict()` with empty overrides list: `len(self.overrides)` is 0, so `overrides` key is omitted from output

## Bugs Found
- The original spec noted Bug 3 (line 140-141): `ProcessorOverride.as_dict()` with `processor=None` was thought to raise TypeError. On re-examination of the actual code, the else branch at line 140 only sets `override['processor'] = None` and then the method returns `override` at line 141. There is no subsequent `override['processor']['processorName']` access in the else branch. The `processorName` assignment at line 138 is only in the if-true branch. Status: NOT A BUG.

## Coverage Notes
- Branch count: ~18
  - `DimensionsOverride.__init__`: isinstance Enum check per key (2 per iteration)
  - `DimensionsOverride.as_dict`: coordinate_id truthy (2)
  - `DimensionsOverride.from_dict`: key in data_dimension_map (2 per iteration)
  - `ProcessorOverride.as_dict`: processor truthy (2)
  - `DataRow.as_dict`: isinstance Entity (2), len(overrides) > 0 (2)
  - `DataRow.from_dict`: override_type dispatch (3 paths: PROCESSOR, DIMENSIONS, else/VALUE)
- No pragmas
