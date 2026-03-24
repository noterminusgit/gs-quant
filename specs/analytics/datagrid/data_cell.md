# data_cell.py

## Summary
`DataCell` represents a single cell in a DataGrid. Each cell holds a processor (deep-copied), an entity reference, dimension overrides, and a computed value. It builds a cell-level processor graph and updates its value from processor results. Cells are uniquely identified by a UUID.

## Dependencies
- Internal:
  - `gs_quant.analytics.common` (DATA_CELL_NOT_CALCULATED)
  - `gs_quant.analytics.core` (BaseProcessor)
  - `gs_quant.analytics.core.processor` (DataQueryInfo, MeasureQueryInfo)
  - `gs_quant.analytics.core.processor_result` (ProcessorResult)
  - `gs_quant.analytics.datagrid` (Override)
  - `gs_quant.analytics.datagrid.utils` (get_utc_now)
  - `gs_quant.entities.entity` (Entity)
- External:
  - `copy` (deepcopy)
  - `uuid` (uuid4)
  - `typing` (List, Optional, Dict, Set, Tuple, Union)
  - `pandas` (pd.Series)

## Type Definitions

### DataCell (class)
Inherits: None (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| cell_id | `str` | `str(uuid.uuid4())` | Unique identifier for the cell |
| processor | `BaseProcessor` | deep copy of input | Root processor for the cell's computation graph |
| entity | `Entity` | required | Entity associated with this cell's row |
| name | `str` | required | Column name for the cell |
| dimension_overrides | `List[Override]` | required | Dimension overrides from the row |
| column_index | `int` | required | Column position index |
| row_index | `int` | required | Row position index |
| row_group | `str` | `None` | Row group name from RowSeparator |
| updated_time | `Optional[str]` | `None` | ISO UTC timestamp of last update |
| value | `ProcessorResult` | `ProcessorResult(False, DATA_CELL_NOT_CALCULATED)` | Current computed value |
| data_queries | `List[DataQueryInfo]` | `[]` | Data queries for leaf processors |

## Enums and Constants
None.

## Functions/Methods

### DataCell.__init__(self, name: str, processor: BaseProcessor, entity: Entity, dimension_overrides: List[Override], column_index: int, row_index: int, row_group: str = None) -> None
Purpose: Initialize a DataCell with a deep-copied processor and default uncomputed value.

**Algorithm:**
1. Generate `cell_id` via `str(uuid.uuid4())`
2. Deep-copy `processor` via `copy.deepcopy(processor)` and store
3. Store `entity`, `name`, `dimension_overrides`, `column_index`, `row_index`, `row_group`
4. Set `updated_time = None`
5. Set `value = ProcessorResult(False, DATA_CELL_NOT_CALCULATED)`
6. Initialize `data_queries = []`

### DataCell.build_cell_graph(self, all_queries: List[Union[DataQueryInfo, MeasureQueryInfo]], rdate_entity_map: Dict[str, Set[Tuple]]) -> None
Purpose: Generate and store the cell's processor graph and data queries.

**Algorithm:**
1. Branch: `self.processor` is truthy
   - True:
     a. Set `self.processor.parent = self`
     b. Create empty `cell_queries: List[DataQueryInfo] = []`
     c. Call `self.processor.build_graph(self.entity, self, cell_queries, rdate_entity_map, self.dimension_overrides)`
     d. Store `self.data_queries = cell_queries`
     e. Extend `all_queries` with `cell_queries`
   - False: no-op (implicit)

### DataCell.update(self, result: ProcessorResult) -> None
Purpose: Set the cell's value from a processor result, handling Series specially.

**Algorithm:**
1. Branch: `isinstance(result.data, pd.Series)`
   - True:
     a. Branch: `result.data.empty`
        - True: `self.value = ProcessorResult(False, 'Empty series as a result of processing.')`
        - False: `self.value = ProcessorResult(True, result.data.iloc[-1])`
   - False: `self.value = ProcessorResult(True, result.data)`
2. Set `self.updated_time = get_utc_now()`

## State Mutation
- `self.cell_id`: Set once during `__init__`, never modified
- `self.processor`: Deep-copied during `__init__`; `parent` attribute set during `build_cell_graph()`
- `self.value`: Initialized to failure result in `__init__`; updated by `update()` or externally by DataGrid
- `self.updated_time`: Set to None in `__init__`; updated to UTC timestamp in `update()`
- `self.data_queries`: Initialized empty in `__init__`; populated during `build_cell_graph()`
- `all_queries` (parameter): Extended in-place during `build_cell_graph()` -- side effect on caller's list

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| None raised directly | - | This class does not raise exceptions directly |

Note: Deep copy in `__init__` may raise if processor contains non-copyable objects.

## Edge Cases
- Empty series result: `update()` stores a failure ProcessorResult with message 'Empty series as a result of processing.'
- Non-series data (dict, str, number, None): `update()` wraps raw data as success ProcessorResult
- `processor` is None/falsy: `build_cell_graph()` is a no-op -- no queries generated
- `result.data` is a non-empty Series: extracts last element via `iloc[-1]`

## Bugs Found
None.

## Coverage Notes
- Branch count: 6
  - `build_cell_graph`: processor truthy (2 branches)
  - `update`: isinstance Series (2 branches), empty check (2 branches)
- No pragmas
