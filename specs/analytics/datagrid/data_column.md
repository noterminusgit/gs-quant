# data_column.py

## Summary
Defines column configuration classes for DataGrid: `RenderType` (string constants for display modes), `HeatMapColorRange` (color theme for heatmaps), `MultiColumnGroup` (grouping columns for heatmap rendering), `ColumnFormat` (formatting options per column), and `DataColumn` (the column definition holding a processor and format). Supports serialization to/from dict for API persistence.

## Dependencies
- Internal:
  - `gs_quant.analytics.core.processor` (BaseProcessor)
- External:
  - `dataclasses` (dataclass, asdict, fields, field)
  - `typing` (Dict, List, Union)

## Type Definitions

### RenderType (class)
Inherits: None (plain class with string constants)

No instance fields. Only class-level constants (see Constants section).

### HeatMapColorRange (dataclass)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| low | `str` | required | Hex color for low end of range, e.g. '#ffffff' |
| mid | `str` | required | Hex color for mid-point of range |
| high | `str` | required | Hex color for high end of range |

### MultiColumnGroup (dataclass)
Inherits: None

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id | `Union[int, str]` | `0` | Identifier for the group |
| columnIndices | `List[int]` | `[]` (field default_factory=list) | Column indices in the group |
| groupAll | `bool` | `False` | If True, group all columns |
| heatMapColorRange | `HeatMapColorRange` | `None` | Optional color theme for heatmap |

### ColumnFormat (class)
Inherits: None (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| renderType | `str` (RenderType constant) | `RenderType.DEFAULT` (`'default'`) | Type of rendering for the column |
| precision | `int` | `2` | Number of decimal precision points |
| humanReadable | `bool` | `True` | Format numbers with commas |
| tooltip | `str` | `None` | Helper text for column tooltip |
| displayValues | `bool` | `True` | Show numerical values in graphical render types |

### DataColumn (class)
Inherits: None (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| name | `str` | required | Name of the column |
| processor | `BaseProcessor` | `None` | Processor for column calculation |
| format_ | `ColumnFormat` | `ColumnFormat()` | Formatting information |
| width | `int` | `DEFAULT_WIDTH` (100) | Column width in pixels |

## Enums and Constants

### RenderType Constants
| Name | Value | Description |
|------|-------|-------------|
| DEFAULT | `'default'` | Standard text rendering |
| HEATMAP | `'heatmap'` | Heatmap color rendering |
| BOXPLOT | `'boxplot'` | Boxplot rendering |
| SCALE | `'scale'` | Scale rendering |
| DATE_MMM_YY | `'dateMmmYy'` | Date formatted as MMM YY |
| TIME_HH_MM | `'timeHhMm'` | Time formatted as HH:MM |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| DEFAULT_WIDTH | `int` | `100` | Default column width in pixels |

## Functions/Methods

### HeatMapColorRange.from_dict(cls, dict_: dict) -> HeatMapColorRange
Purpose: Construct HeatMapColorRange from a dict, filtering to valid class fields only.

**Algorithm:**
1. Get `class_fields` as set of field names via `fields(cls)`
2. Filter `dict_` to keys present in `class_fields`
3. Return `HeatMapColorRange(**filtered_dict)`

### MultiColumnGroup.asdict(self) -> dict
Purpose: Serialize MultiColumnGroup to dict, conditionally including optional fields.

**Algorithm:**
1. Create `obj` with `id` and `columnIndices`
2. Branch: `self.groupAll` is truthy -> add `obj['groupAll'] = True`
3. Branch: `self.heatMapColorRange` is truthy -> add `obj['heatMapColorRange'] = asdict(self.heatMapColorRange)`
4. Return `obj`

### MultiColumnGroup.from_dict(cls, dict_: dict) -> MultiColumnGroup
Purpose: Deserialize MultiColumnGroup from a dict.

**Algorithm:**
1. Get `heat_map_color_range` from `dict_.get('heatMapColorRange')`
2. Branch: `heat_map_color_range` is truthy -> parse via `HeatMapColorRange.from_dict(heat_map_color_range)`; else `None`
3. Return `MultiColumnGroup(id=..., groupAll=..., columnIndices=..., heatMapColorRange=...)`

### ColumnFormat.__init__(self, *, renderType: RenderType = RenderType.DEFAULT, precision: int = 2, humanReadable: bool = True, tooltip: str = None, displayValues: bool = True) -> None
Purpose: Initialize column format options. All parameters are keyword-only.

**Algorithm:**
1. Store all parameters as instance attributes

### ColumnFormat.as_dict(self) -> dict
Purpose: Serialize ColumnFormat to dict, conditionally including tooltip and displayValues.

**Algorithm:**
1. Create `format_` dict with `renderType`, `precision`, `humanReadable`
2. Branch: `self.tooltip` is truthy -> add `format_['tooltip'] = self.tooltip`
3. Branch: `self.renderType != RenderType.DEFAULT` -> add `format_['displayValues'] = self.displayValues`
4. Return `format_`

### ColumnFormat.from_dict(cls, obj: dict) -> ColumnFormat
Purpose: Deserialize ColumnFormat from a dict.

**Algorithm:**
1. Return `ColumnFormat(renderType=obj.get('renderType'), precision=obj.get('precision'), humanReadable=obj.get('humanReadable'), tooltip=obj.get('tooltip'), displayValues=obj.get('displayValues'))`

### DataColumn.__init__(self, name: str, processor: BaseProcessor = None, *, format_: ColumnFormat = ColumnFormat(), width: int = DEFAULT_WIDTH) -> None
Purpose: Initialize a DataColumn with name, optional processor, format, and width.

**Algorithm:**
1. Store `name`, `processor`, `format_`, `width`

### DataColumn.as_dict(self) -> dict
Purpose: Serialize DataColumn to dict, including processor info if present.

**Algorithm:**
1. Call `self.format_.as_dict()` to get format dict
2. Create `column` dict with `name`, `format`, `width`
3. Branch: `processor` is truthy:
   - True: Add `column['processorName'] = processor.__class__.__name__`, then `column.update(**processor.as_dict())`
   - False: no processor info added
4. Return `column`

### DataColumn.from_dict(cls, obj: Dict, reference_list: List) -> DataColumn
Purpose: Deserialize DataColumn from a dict, reconstructing processor via BaseProcessor.from_dict.

**Algorithm:**
1. Call `BaseProcessor.from_dict(obj, reference_list)` to get `processor`
2. Return `DataColumn(name=obj['name'], processor=processor, format_=ColumnFormat.from_dict(obj.get('format', {})), width=obj.get('width', DEFAULT_WIDTH))`

## State Mutation
- `MultiColumnGroup.asdict()`: read-only, creates new dict
- `ColumnFormat.as_dict()`: read-only
- `DataColumn.as_dict()`: read-only
- All `from_dict` methods: constructors, no mutation of input
- `reference_list` in `DataColumn.from_dict`: may be modified by `BaseProcessor.from_dict`

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None raised directly | - | No explicit exception handling in this module |

Note: `HeatMapColorRange.from_dict` will raise `TypeError` if required fields (low, mid, high) are missing from dict after filtering.

## Edge Cases
- `MultiColumnGroup.from_dict`: `heatMapColorRange` may be `None` in input dict -- handled with conditional
- `ColumnFormat.from_dict`: all fields use `obj.get()` which returns `None` for missing keys -- constructor accepts `None` for all keyword params
- `DataColumn.as_dict()`: when `processor` is `None`, no processor keys are added to the output dict
- `DataColumn.from_dict`: `format` key missing from obj defaults to empty dict `{}`; width defaults to `DEFAULT_WIDTH` (100)
- `ColumnFormat.as_dict()`: tooltip is only included when truthy (empty string excluded); displayValues only included for non-DEFAULT render types

## Bugs Found
None.

## Coverage Notes
- Branch count: ~10
  - `MultiColumnGroup.asdict`: groupAll truthy (2), heatMapColorRange truthy (2)
  - `MultiColumnGroup.from_dict`: heat_map_color_range truthy (2)
  - `ColumnFormat.as_dict`: tooltip truthy (2), renderType != DEFAULT (2)
  - `DataColumn.as_dict`: processor truthy (2)
- No pragmas
