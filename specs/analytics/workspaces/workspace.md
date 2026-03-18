# workspace.py

## Summary
Workspace model: WorkspaceCallToAction, WorkspaceTab, WorkspaceColumn, WorkspaceRow, Workspace. Handles layout parsing, serialization, and CRUD via GsSession API.

## Classes

### WorkspaceCallToAction
- as_dict(): actions (RelatedLink → as_dict, else raw), text, name if set
- from_dict(): actions (Dict → RelatedLink.from_dict, else raw)

### WorkspaceTab
- Simple id_/name pair

### WorkspaceColumn
- components setter:
  1. Validates max 12 components
  2. Calculates width_sum from existing components (not WorkspaceRow)
  3. Validates total width <= 12
- get_layout(count):
  1. If no explicit widths → equally spread (12 / len)
  2. Else → use explicit widths, fill remainder
  3. Handles nested WorkspaceRow/WorkspaceColumn recursively

### WorkspaceRow
- Same validation and layout logic as WorkspaceColumn
- get_layout wraps in 'r(...)'
- Handles nested WorkspaceColumn

### Workspace
- PERSISTED_COMPONENTS: maps component types to API paths
- Properties: name, alias, rows, entitlements, description, disclaimer, maintainers, tabs, selector_components, call_to_action, tags
- get_by_id/get_by_alias: GsSession._get
- save(): PUT if id exists, else check alias → PUT or POST
- open(): builds URL, calls webbrowser.open
- create(): POST, store id
- delete(): requires id, DELETE
- delete_all(include_tabs): recursively deletes components and optionally tabs
- _parse(layout, components): recursive layout parser using stack-based bracket matching
- from_dict(): parses layout string, builds rows, extracts selector components
- as_dict(): builds layout string from rows, serializes all fields

### __get_layout (module-level function)
- Appears to be an older/duplicate version of WorkspaceRow.get_layout
- Same logic but standalone function

## Edge Cases
- save() with no id and no alias → does nothing
- open() before save → MqValueError
- delete() without id → MqValueError
- WorkspaceColumn/Row: width=None components counted as width=1 in validation
- from_dict: component_count never updated from 0 → all components become selectors
- Layout parsing: nested brackets tracked via deque stack

## Bugs Found
- from_dict (line 544-550): `component_count` starts at 0 and is never incremented, so the selector_components loop always processes ALL components. This means every component is duplicated as a selector component. However, this may be mitigated by the layout also parsing the same components.

## Coverage Notes
- ~50 branches
- Requires GsSession mock for all CRUD methods
- webbrowser.open needs mock
