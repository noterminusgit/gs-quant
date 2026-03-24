# constants.py

## Summary
String constants for the analytics module. These constants serve as canonical keys used throughout serialization/deserialization of processors, entities, data coordinates, and datagrid structures. They ensure consistent key naming across JSON/dict representations.

## Dependencies
- Internal: None
- External: None (pure Python, no imports)

## Type Definitions
None. This module defines only module-level string constants.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `DATAGRID_HELP_MSG` | `str` | `'\nDatagrid is currently experimental...'` | User-facing help message for experimental datagrid API |
| `DATA_CELL_NOT_CALCULATED` | `str` | `'Cell has not been calculated'` | Default message for uncalculated cells |
| `CELL_GRAPH` | `str` | `'cell_graph'` | Key for cell graph structure in serialized datagrid |
| `QUERIES_TO_PROCESSORS` | `str` | `'queries_to_processors'` | Key for query-to-processor mapping |
| `DATA_COORDINATE` | `str` | `'dataCoordinate'` | Type discriminator for data coordinate objects |
| `PROCESSOR` | `str` | `'processor'` | Type discriminator for processor objects |
| `PROCESSOR_NAME` | `str` | `'processorName'` | Key for the processor class name in serialized form |
| `ENTITY` | `str` | `'entity'` | Type discriminator for entity objects |
| `ENTITY_ID` | `str` | `'entityId'` | Key for entity Marquee ID |
| `ENTITY_TYPE` | `str` | `'entityType'` | Key for entity type string |
| `DATE` | `str` | `'date'` | Type discriminator for date values |
| `DATETIME` | `str` | `'datetime'` | Type discriminator for datetime values |
| `TYPE` | `str` | `'type'` | Generic type discriminator key used in all serialized dicts |
| `LIST` | `str` | `'list'` | Type discriminator for list values |
| `VALUE` | `str` | `'value'` | Key for the actual value in a typed parameter dict |
| `PARAMETER` | `str` | `'parameter'` | Key for parameter name in entity reference resolution |
| `PARAMETERS` | `str` | `'parameters'` | Key for the parameters dict in serialized processor |
| `DATA_ROW` | `str` | `'dataRow'` | Type discriminator for data row references |
| `REFERENCE` | `str` | `'reference'` | Key for the object reference in entity resolution dicts |
| `RELATIVE_DATE` | `str` | `'relativeDate'` | Type discriminator for RelativeDate values |

## Functions/Methods
None. This module contains no functions or methods.

## State Mutation
None. All values are module-level string constants assigned once at import time. They are never mutated.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none) | -- | -- |

## Edge Cases
- All constants use camelCase string values to match the JSON/API serialization format, not snake_case.
- `DATAGRID_HELP_MSG` includes leading and trailing newlines.

## Bugs Found
None.

## Coverage Notes
- Branch count: 0
- No branches to cover; this is a pure constants module.
- Pragmas: none
