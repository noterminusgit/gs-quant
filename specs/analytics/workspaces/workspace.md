# workspace.py

## Summary
Workspace model for the Marquee markets workspace system. Provides `WorkspaceCallToAction`, `WorkspaceTab`, `WorkspaceColumn`, `WorkspaceRow`, and `Workspace` classes for building, serializing, and managing workspace layouts. Handles nested row/column layout composition, serialization to/from a custom layout string format (e.g., `r(c6($0)c6($1))`), and CRUD operations via `GsSession` REST API. Also contains a module-level `__get_layout` function that duplicates `WorkspaceRow.get_layout` logic.

## Dependencies
- Internal: `gs_quant.analytics.workspaces.components` (Component, TYPE_TO_COMPONENT, RelatedLink, DataGridComponent, MonitorComponent, PlotComponent, DataScreenerComponent)
- Internal: `gs_quant.common` (Entitlements as Entitlements_)
- Internal: `gs_quant.entities.entitlements` (Entitlements)
- Internal: `gs_quant.errors` (MqValueError, MqRequestError)
- Internal: `gs_quant.session` (GsSession)
- External: `logging` (getLogger)
- External: `webbrowser` (open)
- External: `collections` (deque)
- External: `typing` (List, Tuple, Union, Dict)
- External: `pydash` (get)

## Type Definitions

### WorkspaceCallToAction (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| actions | `List[RelatedLink]` | (required) | List of action links (RelatedLink objects or raw dicts) |
| text | `str` | (required) | Description text below the link |
| name | `str` | `None` | Optional name of the link/button |

### WorkspaceTab (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| id_ | `str` | (required) | Alias of the workspace to create a tab to |
| name | `str` | (required) | Display name of the tab |

### WorkspaceColumn (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __components | `List[Union[Component, WorkspaceRow]]` | `[]` | Components in this column (validated on set) |
| __width | `int` | `None` | Column width (1-12 grid units) |

Properties with getters/setters: `components`, `width`

### WorkspaceRow (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __components | `List[Union[Component, WorkspaceColumn]]` | `[]` | Components in this row (validated on set) |

Properties with getters/setters: `components`

### Workspace (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | `None` | Server-assigned workspace ID (set on create/save) |
| __name | `str` | (required) | Workspace display name |
| __rows | `List[WorkspaceRow]` | `[]` | Layout rows |
| __selector_components | `List[Component]` | `[]` | Hidden selector components not in layout |
| __alias | `str` | `None` | URL-friendly alias |
| __entitlements | `Union[Entitlements, Entitlements_, None]` | `None` | Access control entitlements |
| __description | `str` | `None` | Workspace description |
| __disclaimer | `str` | `None` | Disclaimer text |
| __maintainers | `List[str]` | `[]` | List of maintainer IDs |
| __tabs | `List[WorkspaceTab]` | `[]` | Tabs linking to other workspaces |
| __call_to_action | `Union[WorkspaceCallToAction, Dict, None]` | `None` | CTA configuration |
| __tags | `List[str]` | `[]` | Tags for categorization |

Properties with getters/setters: `name`, `alias`, `rows`, `entitlements`, `description`, `disclaimer`, `maintainers`, `tabs`, `selector_components`, `call_to_action`, `tags`, `id` (read-only)

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| API | `str` | `'/workspaces/markets'` | Base API path for workspace CRUD |
| HEADERS | `Dict` | `{'Content-Type': 'application/json;charset=utf-8'}` | HTTP headers for API requests |
| _logger | `Logger` | `logging.getLogger(__name__)` | Module logger |

### Workspace.PERSISTED_COMPONENTS (class variable)
```python
PERSISTED_COMPONENTS = {
    DataGridComponent: '/data/grids',
    MonitorComponent: '/monitors',
    PlotComponent: '/charts',
    DataScreenerComponent: '/data/screens',
}
```
Maps component types to their API paths for individual deletion.

## Functions/Methods

### WorkspaceCallToAction.__init__(self, actions: List[RelatedLink], text: str, name: str = None) -> None
Purpose: Store CTA actions, text, and optional name.

### WorkspaceCallToAction.as_dict(self) -> Dict
Purpose: Serialize to dict, handling mixed RelatedLink objects and raw dicts.

**Algorithm:**
1. Initialize `actions = []`
2. For each `action` in `self.actions`:
   - Branch: `isinstance(action, RelatedLink)` -> `actions.append(action.as_dict())`
   - Branch: else -> `actions.append(action)` (pass through raw dict)
3. Build `cta_dict = {'actions': actions, 'text': self.text}`
4. Branch: `self.name` truthy -> `cta_dict['name'] = self.name`
5. Return `cta_dict`

### WorkspaceCallToAction.from_dict(cls, obj) -> WorkspaceCallToAction
Purpose: Deserialize from dict, handling mixed dict and non-dict actions.

**Algorithm:**
1. Initialize `actions = []`
2. For each `action` in `obj['actions']`:
   - Branch: `isinstance(action, Dict)` -> `actions.append(RelatedLink.from_dict(action))`
   - Branch: else -> `actions.append(action)` (pass through)
3. Return `WorkspaceCallToAction(actions=actions, text=obj['text'], name=obj['name'])`

Note: `obj['name']` will raise `KeyError` if 'name' key is missing. Should be `obj.get('name')`.

### WorkspaceTab.__init__(self, id_: str, name: str) -> None
Purpose: Store tab id and name.

### WorkspaceTab.as_dict(self) -> Dict
Purpose: Serialize to `{'id': self.id_, 'name': self.name}`.

### WorkspaceTab.from_dict(cls, obj) -> WorkspaceTab
Purpose: Deserialize from `WorkspaceTab(id_=obj['id'], name=obj['name'])`.

### WorkspaceColumn.__init__(self, components: List[Union[Component, WorkspaceRow]], width: int = None) -> None
Purpose: Initialize column with validated components.

**Algorithm:**
1. Set `self.__components = []` (empty list for initial validation)
2. Set `self.components = components` (triggers setter validation)
3. Set `self.__width = width`

### WorkspaceColumn.components (setter)
Purpose: Validate and set components list.

**Algorithm:**
1. Branch: `len(value) > 12` -> raise `MqValueError(f'{value} exceeds the max number of columns of 12.')`
2. Calculate `width_sum` from existing `self.__components` (NOT new value): for each component that is not WorkspaceRow, add `component.width`
3. Branch: `width_sum > 12` -> raise `MqValueError`
4. Calculate `without_width_count` from new `value`: for each component that is not WorkspaceRow, add `component.width or 1`
5. Branch: `width_sum + without_width_count > 12` -> raise `MqValueError`
6. Set `self.__components = value`

Note: Step 2 sums widths of EXISTING (old) components, not new ones. On first call from `__init__`, `self.__components` is `[]`, so `width_sum = 0`.

### WorkspaceColumn.get_layout(self, count: int) -> Tuple[str, int]
Purpose: Generate layout string for this column's components.

**Algorithm:**
1. Calculate `width_sum` of components (excluding WorkspaceRow), using `component.width or 0`
2. `components_length = len(self.__components)`
3. Branch: `width_sum == 0` (no explicit widths)
   - `size = int(12 / components_length)` (equal distribution)
   - `last_size = 12 % components_length`
   - For each `(i, component)`:
     - Branch: isinstance WorkspaceRow -> recurse `component.get_layout(count)`, append sub_layout
     - Branch: isinstance WorkspaceColumn -> recurse, wrap in `c{size}(...)`
     - Branch: else (Component) -> append `c{size}(${count})`, increment count
     - Last component gets `size + last_size` if `last_size != 0`
4. Branch: `width_sum > 0` (explicit widths)
   - Branch: `width_sum == 12` -> `default_width = 0`
   - Branch: else -> `default_width = int(12 - width_sum / sum(1 for c in components if c.width is None))` (NOTE: operator precedence issue -- see Bugs)
   - For each `(i, component)`:
     - Branch: last component with no width -> `c{12 - used_sum}(${count})`
     - Branch: `component.width is None` -> `c{default_width}(${count})`
     - Branch: else -> `c{component.width or 1}(${count})`
     - Increment `count` for each
5. Return `(layout, count)`

### WorkspaceRow.__init__(self, components: List[Union[Component, WorkspaceColumn]]) -> None
Purpose: Initialize row with validated components.

**Algorithm:**
1. Set `self.__components = []`
2. Set `self.components = components` (triggers setter)

### WorkspaceRow.components (setter)
Purpose: Validate and set components list. Same validation logic as WorkspaceColumn.components setter.

**Algorithm:** Same as WorkspaceColumn.components setter, but error message says "row" instead of "column".

### WorkspaceRow.get_layout(self, count: int) -> Tuple[str, int]
Purpose: Generate layout string wrapped in `r(...)`.

**Algorithm:**
1. `layout = 'r('`
2. Calculate `width_sum` (excluding WorkspaceRow), using `component.width or 0`
3. `components_length = len(self.__components)`
4. Branch: `width_sum == 0`
   - Equal distribution: `size = int(12 / components_length)`, `last_size = 12 % components_length`
   - For each `(i, component)`:
     - Branch: isinstance WorkspaceColumn -> recurse, wrap in `c{size}(...)`
     - Branch: else (Component) -> `c{size}(${count})`, increment count
     - Last gets remainder
5. Branch: `width_sum > 0`
   - Calculate `default_width`:
     - Branch: `width_sum == 12` -> `default_width = 0`
     - Branch: `len(self.components) == 1` -> `default_width = self.components[0].width`
     - Branch: else -> `default_width = int(12 - width_sum / sum(...))` (same precedence issue)
   - For each `(i, component)`:
     - Branch: last component, no width:
       - Branch: isinstance WorkspaceColumn -> recurse, `c{12 - used_sum}(...)`
       - Branch: else -> `c{12 - used_sum}(${count})`
     - Branch: `component.width is None`:
       - Branch: isinstance WorkspaceColumn -> recurse, `c{default_width}(...)`
       - Branch: else -> `c{default_width}(${count})`
     - Branch: else (has width):
       - Branch: isinstance WorkspaceColumn -> recurse, `c{width}(...)`
       - Branch: else -> `c{width}(${count})`
     - Increment `used_sum` for non-last, and `count` for leaf components
6. `layout += ')'`
7. Return `(layout, count)`

### WorkspaceRow._add_components(self, components: List)
Purpose: Recursively collect all leaf component dicts from the row tree.

**Algorithm:**
1. For each component in `self.__components`:
   - Branch: isinstance (WorkspaceRow, WorkspaceColumn) -> recurse `component._add_components(components)`
   - Branch: else -> `components.append(component.as_dict())`

### WorkspaceColumn._add_components(self, components: List)
Purpose: Same as WorkspaceRow._add_components.

### Workspace.__init__(self, name, rows=None, alias=None, description=None, entitlements=None, tabs=None, selector_components=None, disclaimer=None, maintainers=None, call_to_action=None, tags=None) -> None
Purpose: Initialize workspace with all properties.

**Algorithm:**
1. Set `self.__id = None`
2. Set all fields with defaults: `rows or []`, `selector_components or []`, `maintainers or []`, `tabs or []`, `tags or []`

### Workspace.get_by_id(cls, workspace_id: str) -> Workspace
Purpose: Fetch workspace by ID from API.

**Algorithm:**
1. `resp = GsSession.current.sync.get(f'{API}/{workspace_id}')`
2. Return `Workspace.from_dict(resp)`

### Workspace.get_by_alias(cls, alias: str) -> Workspace
Purpose: Fetch workspace by alias from API.

**Algorithm:**
1. `resp = get(GsSession.current.sync.get(f'{API}?alias={alias}'), 'results.0')`
2. Branch: `not resp` -> raise `MqValueError(f'Workspace not found with alias {alias}')`
3. Return `Workspace.from_dict(resp)`

### Workspace.save(self) -> None
Purpose: Create or update workspace via API.

**Algorithm:**
1. Branch: `self.__id` truthy -> PUT to `{API}/{self.__id}`
2. Branch: `self.__alias` truthy (no id)
   - `id_ = get(GsSession.current.sync.get(f'{API}?alias={self.__alias}'), 'results.0.id')`
   - Branch: `id_` truthy -> PUT to `{API}/{id_}`, set `self.__id` from response
   - Branch: `id_` falsy -> POST to `{API}`, set `self.__id` from response
3. Branch: neither id nor alias -> **does nothing** (silent no-op)

### Workspace.open(self) -> None
Purpose: Open workspace in web browser.

**Algorithm:**
1. Branch: `self.__id is None` -> raise `MqValueError('Workspace must be created or saved before opening.')`
2. `domain = GsSession.current.domain.replace(".web", "")`
3. Branch: `domain == 'https://api.gs.com'` -> `domain = 'https://marquee.gs.com'`
4. `url = f'{domain}/s/markets/{self.__alias or self.__id}'`
5. `webbrowser.open(url)`

### Workspace.create(self) -> None
Purpose: Create new workspace via POST.

**Algorithm:**
1. `resp = GsSession.current.sync.post(f'{API}', self.as_dict(), request_headers=HEADERS)`
2. `self.__id = resp['id']`

### Workspace.delete(self) -> None
Purpose: Delete workspace by ID.

**Algorithm:**
1. Branch: `self.__id is None` -> raise `MqValueError('Workspace must have an id to be deleted.')`
2. `resp = GsSession.current.sync.delete(f'{API}/{self.__id}')`
3. `self.__id = resp['id']` (sets id from delete response)

### Workspace.delete_all(self, include_tabs: bool = False) -> None
Purpose: Delete workspace and all persisted components, optionally including tabs.

**Algorithm:**
1. For each `row` in `self.__rows`: call `self.__delete_components(row.components)`
2. Call `self.__delete_components(self.__selector_components)`
3. Branch: `include_tabs`
   - For each `tab` in `self.__tabs`:
     - `tab_workspace = self.get_by_alias(tab.id_)`
     - `tab_workspace.delete_all()`

Note: Does NOT call `self.delete()` on the workspace itself -- only deletes components.

### Workspace.__delete_components(cls, components: List[Component]) (classmethod, private)
Purpose: Recursively delete persisted components via API.

**Algorithm:**
1. For each `component` in `components`:
   - Branch: isinstance (WorkspaceRow, WorkspaceColumn) -> recurse on `component.components`
   - Branch: else
     - `type_ = type(component)`
     - Branch: `type_` in `cls.PERSISTED_COMPONENTS`
       - Try: `GsSession.current.sync.delete(f'{cls.PERSISTED_COMPONENTS[type_]}/{component.id_}')`
       - Except `MqRequestError as ex`: `_logger.warning(f'Failed to delete {type_.__name__} with id {component.id_} due to {ex.message}')`

### Workspace._parse(cls, layout: str, workspace_components: List[Dict]) -> List
Purpose: Recursively parse a layout string into components, columns, and rows.

**Algorithm:**
1. Initialize `current_str = ''`, `outside_components = []`, `stack = deque()`
2. For each char `c` in `layout`:
   - `current_str += c`
   - Branch: `c == '('` -> `stack.append('(')`
   - Branch: `c == ')'` -> `stack.pop()`
     - Branch: `len(stack) == 0` (top-level expression complete)
       - Branch: `current_str.startswith('c')` (column)
         - Determine `is_component`: check if content after `(` starts with `$`
         - Branch: `is_component`
           - Parse `scale` and `id_` from `c{scale}(${id_})`
           - Look up `workspace_components[id_]` by integer index
           - Dispatch to `TYPE_TO_COMPONENT[component_type].from_dict(component, scale)`
           - Append to `outside_components`
         - Branch: not is_component (nested column)
           - Extract inner layout string
           - Recurse: `Workspace._parse(column_layout, workspace_components)`
           - Create `WorkspaceColumn(components, width)`
           - Append to `outside_components`
       - Branch: `current_str.startswith('r')` (row)
         - Extract inner layout string
         - Recurse: `Workspace._parse(row_layout, workspace_components)`
         - Create `WorkspaceRow(components)`
         - Append to `outside_components`
       - Reset `current_str = ''`
3. Return `outside_components`

### Workspace.from_dict(cls, obj) -> Workspace
Purpose: Deserialize workspace from API response dict.

**Algorithm:**
1. Extract `workspace_components = obj['parameters']['components']`
2. Extract `layout = obj['parameters']['layout']`
3. Parse top-level row layouts using stack-based bracket matching on `layout[1:]` (skips first char)
4. For each parsed `row_layout`: strip to start from first `c`, call `Workspace._parse(row_layout, workspace_components)`
5. Build `workspace_rows = [WorkspaceRow(components=...) for row_layout in row_layouts]`
6. `component_count = 0` (hardcoded, never incremented)
7. Branch: `component_count < len(workspace_components)` (always True if components exist)
   - For `i in range(0, len(workspace_components))`: deserialize ALL components as selector_components
8. Parse tabs, entitlements, other fields
9. Return `Workspace(...)`

### Workspace.as_dict(self) -> Dict
Purpose: Serialize workspace to API-compatible dict.

**Algorithm:**
1. Initialize `components = []`, `count = 0`, `layout = ''`
2. For each `row` in `self.__rows`:
   - `row_layout, count = row.get_layout(count)`
   - `layout += row_layout`
   - For each `component` in `row.components`:
     - Branch: isinstance (WorkspaceRow, WorkspaceColumn) -> `component._add_components(components)`
     - Branch: else -> `components.append(component.as_dict())`
3. Extend `components` with serialized `self.__selector_components`
4. Build `parameters = {'layout': layout, 'components': components}`
5. Branch: `len(self.__maintainers)` -> add maintainers
6. Branch: `self.__call_to_action`
   - Branch: isinstance WorkspaceCallToAction -> `.as_dict()`
   - Branch: else -> pass through raw dict
7. Branch: `len(self.__tabs)` -> add serialized tabs
8. Branch: `self.__disclaimer` -> add disclaimer
9. Build `dict_ = {'name': self.__name, 'parameters': parameters}`
10. Branch: `self.__alias` -> add alias
11. Branch: `self.__entitlements`
    - Branch: isinstance Entitlements_ -> `.as_dict()`
    - Branch: isinstance Entitlements -> `.to_dict()`
    - Branch: else -> pass through
12. Branch: `len(self.__tags)` -> add tags
13. Branch: `self.__description` -> add description
14. Return `dict_`

### __get_layout(components, count) (module-level function)
Purpose: Standalone layout generation function. Duplicates WorkspaceRow.get_layout logic.

**Algorithm:** Same as WorkspaceRow.get_layout but handles both WorkspaceRow and WorkspaceColumn via recursive calls to itself. Wraps output in `r(...)`.

Note: This is a module-level function with double-underscore prefix (name-mangled to `_workspace__get_layout`). Appears unused and is a duplicate of WorkspaceRow.get_layout.

## State Mutation
- `Workspace.__id`: Set by `create()`, `save()`, `delete()`. Initial value `None`.
- `Workspace.__rows`, `__selector_components`, etc.: Set during `__init__` and via property setters.
- `WorkspaceColumn.__components` / `WorkspaceRow.__components`: Validated and set via setter. Initial empty list is set before setter runs.
- `GsSession` side effects: `save()`, `create()`, `delete()`, `delete_all()`, `get_by_id()`, `get_by_alias()` all make HTTP requests.
- `webbrowser.open()`: Side effect in `Workspace.open()`.
- `Workspace._parse`: Mutates nothing; creates new component objects.
- `Workspace.from_dict`: Stateless construction.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `Workspace.open` | When `self.__id is None` |
| `MqValueError` | `Workspace.delete` | When `self.__id is None` |
| `MqValueError` | `Workspace.get_by_alias` | When no workspace found with alias |
| `MqValueError` | `WorkspaceColumn.components` setter | When len > 12 or widths exceed 12 |
| `MqValueError` | `WorkspaceRow.components` setter | When len > 12 or widths exceed 12 |
| `MqRequestError` | `Workspace.__delete_components` | Caught and logged when component deletion fails |

## Edge Cases
- **save() with no id and no alias**: Does nothing silently. No error, no API call.
- **save() with alias, no existing workspace**: Creates new workspace via POST.
- **save() with alias, existing workspace**: Updates via PUT using the found id.
- **delete() response**: Sets `self.__id = resp['id']` from the delete response, which may be unexpected.
- **open() domain replacement**: Only replaces `.web` in domain. Production domain `https://api.gs.com` is special-cased to `https://marquee.gs.com`.
- **from_dict component_count bug**: `component_count` is always 0 and never incremented, so ALL components are duplicated into `selector_components` regardless of whether they appear in the layout.
- **from_dict layout parsing**: Skips first character of layout string with `layout[1:]`. This assumes the layout starts with a known prefix character.
- **WorkspaceColumn/Row components setter**: Validates widths of OLD components (step 2) plus new components (step 4). On first call from `__init__`, old list is `[]` so `width_sum = 0`. On subsequent calls, old widths are cumulated.
- **WorkspaceColumn.get_layout explicit widths branch**: `default_width = int(12 - width_sum / sum(...))` has operator precedence issue. Division binds tighter than subtraction, so this computes `12 - (width_sum / count)` instead of `(12 - width_sum) / count`.
- **WorkspaceRow.get_layout**: Same precedence issue. Also has special case for single component: `default_width = self.components[0].width`.
- **delete_all does not delete the workspace itself**: Only deletes persisted components. The workspace remains.
- **delete_all recursive tab deletion**: Calls `tab_workspace.delete_all()` but does NOT pass `include_tabs=True`, so nested tab workspaces' tabs are not deleted.
- **WorkspaceCallToAction.from_dict**: Uses `obj['name']` instead of `obj.get('name')`, will raise KeyError if name is missing.

## Bugs Found
- **from_dict lines 544-548**: `component_count = 0` is never incremented, so `range(component_count, len(workspace_components))` always starts at 0. ALL workspace components are duplicated as selector_components. (OPEN)
- **WorkspaceCallToAction.from_dict line 76**: `obj['name']` should be `obj.get('name')` since `name` is optional in `__init__`. Will raise `KeyError` if 'name' is not present. (OPEN)
- **WorkspaceColumn.get_layout line 171**: `int(12 - width_sum / sum(...))` has operator precedence bug. Should be `int((12 - width_sum) / sum(...))`. Same issue in WorkspaceRow.get_layout line 263. (OPEN)
- **WorkspaceRow.get_layout lines 260-263**: `default_width` computation has the same precedence bug as WorkspaceColumn, plus an additional branch for single-component case that sets `default_width = self.components[0].width` which may be None. (OPEN)

## Coverage Notes
- Branch count: ~55
- Key branch categories:
  - Workspace CRUD: save id/alias/post (3), open domain check (2), delete id check (1), get_by_alias not-found (1)
  - WorkspaceColumn/Row validation: len > 12 (2), width_sum > 12 (2), width_sum + without_width_count > 12 (2)
  - WorkspaceColumn.get_layout: width_sum == 0 branch (1), each component type check (3 per: Row/Column/Component), explicit width sub-branches (3)
  - WorkspaceRow.get_layout: same structure (~10 branches)
  - Workspace._parse: startswith 'c' vs 'r' (2), is_component (1), stack empty (1)
  - Workspace.from_dict: component_count check (1), tabs parsing (1)
  - Workspace.as_dict: maintainers/cta/tabs/disclaimer/alias/entitlements/tags/description checks (~10)
  - delete_all: include_tabs (1), persisted component type check (1), MqRequestError catch (1)
  - WorkspaceCallToAction: isinstance RelatedLink/Dict checks (2 each in as_dict/from_dict), name check (1)
- Requires GsSession mock for all CRUD methods
- Requires webbrowser.open mock
- Pragmas: none
