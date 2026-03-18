# data_column.py

## Summary
DataColumn, ColumnFormat, RenderType, HeatMapColorRange, MultiColumnGroup — column configuration for DataGrid.

## Classes

### RenderType
Plain class with string constants (DEFAULT, HEATMAP, BOXPLOT, SCALE, DATE_MMM_YY, TIME_HH_MM).

### HeatMapColorRange (dataclass)
- from_dict: filters to class fields

### MultiColumnGroup (dataclass)
- asdict(): includes groupAll only if True, heatMapColorRange only if set
- from_dict: parses nested HeatMapColorRange if present

### ColumnFormat
- as_dict():
  1. Always includes renderType, precision, humanReadable
  2. If tooltip → add tooltip
  3. If renderType != DEFAULT → add displayValues
- from_dict: direct field mapping

### DataColumn
- as_dict():
  1. Build format dict
  2. If processor exists → add processorName + processor.as_dict()
- from_dict: creates processor via BaseProcessor.from_dict, builds column

## Edge Cases
- MultiColumnGroup.from_dict: heatMapColorRange may be None
- ColumnFormat.from_dict: all fields come from obj.get() (can be None)

## Bugs Found
None.

## Coverage Notes
- ~10 branches
