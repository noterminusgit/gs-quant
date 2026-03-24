# components.py

## Summary
Workspace UI component classes for building workspace layouts. Defines helper data classes (`Selection`, `LegendItem`, `RelatedLink`), an abstract base `Component` class with `as_dict`/`from_dict` serialization, and 12 concrete component types (`PlotComponent`, `DataVizComponent`, `DataGridComponent`, `DataScreenerComponent`, `ArticleComponent`, `CommentaryComponent`, `ContainerComponent`, `SelectorComponent`, `PromoComponent`, `SeparatorComponent`, `LegendComponent`, `MonitorComponent`, `RelatedLinksComponent`). Also defines the `TYPE_TO_COMPONENT` dispatch mapping.

## Dependencies
- Internal: none (self-contained module)
- External: `uuid` (uuid4)
- External: `abc` (ABC, abstractmethod)
- External: `enum` (Enum)
- External: `typing` (Dict, List, Optional)
- External: `pydash` (unset, snake_case)

## Type Definitions

### Selection (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __selector_id | `str` | (required) | Identifier of the selector this selection belongs to |
| __tag | `str` | (required) | Tag matched by the selector; shown as dropdown option |

Properties with getters/setters: `selector_id`, `tag`

### LegendItem (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| color | `str` | (required) | Color of the legend item |
| icon | `str` | (required) | Icon of the legend item |
| name | `str` | (required) | Display name |
| tooltip | `str` | `None` | Optional tooltip on the name |

### RelatedLink (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type_ | `RelatedLinkType` | (required) | Type of the link |
| name | `str` | (required) | Display name |
| link | `str` | (required) | URL/anchor to navigate to |
| description | `str` | `None` | Optional description |

### Component (ABC)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | `f'{ClassName}-{uuid4()[0:5]}'` | Auto-generated if not provided |
| _height | `Optional[int]` | `None` | Height in pixels |
| __width | `int` | `None` | Width (1-12 grid units) |
| __selections | `List[Selection]` | `None` | Selection options for selectors |
| __container_ids | `List[str]` | `None` | IDs of containers affected |
| _type | `None` | `None` | Component type string, set by subclasses |

Properties with getters/setters: `id_`, `width`, `height`, `selections`, `container_ids`

Note: `id_` setter generates a new UUID-based id if value is falsy (None, empty string).

### PlotComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'plot'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text on chart name |
| hide_legend | `bool` | `False` | Whether to hide series legend |

Constructor params: `height: int`, `id_: str`, `*, width: int = None`, `selections: List[Selection] = None`, `tooltip: str = None`, `hide_legend: bool = False`

### DataVizComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'dataviz'` | Component type identifier |

Constructor params: `height: int`, `id_: str`, `*, width: int = None`

### DataGridComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'datagrid'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text |

Constructor params: `height: int`, `id_: str`, `*, width: int = None`, `selections: List[Selection] = None`, `tooltip: str = None`

### DataScreenerComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'screener'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text |

Constructor params: `height: int`, `id_: str`, `*, width: int = None`, `tooltip: str = None`

### ArticleComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'article'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text |
| commentary_channels | `List[str]` | `None` | Commentary data channels |
| commentary_to_desktop_link | `bool` | `None` | Show desktop link in header |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, width: int = None`, `selections: List[Selection] = None`, `tooltip: str = None`, `commentary_channels: List[str] = None`, `commentary_to_desktop_link: bool = None`

### CommentaryComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'plot'` | **Set to 'plot', same as PlotComponent** |
| tooltip | `str` | `None` | Tooltip text |
| commentary_channels | `List[str]` | `None` | Commentary data channels |
| commentary_to_desktop_link | `bool` | `None` | Show desktop link in header |

Constructor params: same as ArticleComponent

### ContainerComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'container'` | Component type identifier |
| component_id | `str` | `None` | Default component ID to display |

Constructor params: `id_: Optional[str] = None`, `*, width: int = None`, `component_id: str = None`

Note: No `height` parameter in constructor; passes no height to super. `selections=None` passed explicitly.

### SelectorComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'selector'` | Component type identifier |
| container_ids | `List[str]` | (required) | Container IDs affected by this selector |
| title | `str` | `None` | Label next to selector dropdown |
| default_option_index | `int` | `None` | Default selected dropdown index |
| tooltip | `str` | `None` | Tooltip on the title |
| parent_selector_id | `str` | `None` | Parent selector ID for nested selections |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, container_ids: List[str]`, `width: int = None`, `title: str = None`, `default_option_index: int = None`, `tooltip: str = None`, `parent_selector_id: str = None`

Note: `container_ids` is keyword-only and required. `selections=None` passed to super.

### PromoComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'promo'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text |
| transparent | `bool` | `None` | Transparent background |
| body | `str` | `None` | HTML body text |
| size | `PromoSize` | `None` | Text size enum |
| hide_border | `bool` | `None` | Whether to hide border |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, width: int = None`, `selections: List[Selection] = None`, `tooltip: str = None`, `transparent: bool = None`, `body: str = None`, `size: PromoSize = None`, `hide_border: bool = None`

### SeparatorComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'separator'` | Component type identifier |
| name | `str` | `None` | Separator text |
| size | `str` | `None` | Size string |
| show_more_url | `str` | `None` | URL for "show more" link |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, width: int = None`, `selections: List[Selection] = None`, `name: str = None`, `size: str = None`, `show_more_url: str = None`

### LegendComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'legend'` | Component type identifier |
| items | `List[LegendItem]` | `None` | Legend items to display |
| position | `str` | `None` | Legend position |
| transparent | `bool` | `None` | Transparent background |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, width: int = None`, `selections: List[Selection] = None`, `items: List[LegendItem] = None`, `position: str = None`, `transparent: bool = None`

### MonitorComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'monitor'` | Component type identifier |
| tooltip | `str` | `None` | Tooltip text |

Constructor params: `height: int`, `id_: str`, `*, width: int = None`, `selections: List[Selection] = None`, `tooltip: str = None`

### RelatedLinksComponent (class)
Inherits: Component

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _type | `str` | `'relatedLinks'` | Component type identifier |
| links | `List[RelatedLink]` | (required) | Links to display |
| title | `str` | (required) | Component title |

Constructor params: `height: int`, `id_: Optional[str] = None`, `*, width: int = None`, `selections: List[Selection] = None`, `links: List[RelatedLink]`, `title: str`

## Enums and Constants

### RelatedLinkType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| anchor | `'anchor'` | Anchor link within page |
| internal | `'internal'` | Internal site navigation |
| external | `'external'` | External URL |
| mail | `'mail'` | mailto: link |
| notification | `'notification'` | Notification action |

### PromoSize(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DEFAULT | `'default'` | Default text size |
| LARGE | `'large'` | Large text size |

### Module Constants

#### TYPE_TO_COMPONENT
```python
TYPE_TO_COMPONENT = {
    'article': ArticleComponent,
    'container': ContainerComponent,
    'datagrid': DataGridComponent,
    'dataviz': DataVizComponent,
    'legend': LegendComponent,
    'monitor': MonitorComponent,
    'plot': PlotComponent,
    'promo': PromoComponent,
    'relatedLinks': RelatedLinksComponent,
    'selector': SelectorComponent,
    'separator': SeparatorComponent,
    'screener': DataScreenerComponent,
}
```

## Functions/Methods

### Selection.as_dict(self) -> Dict
Purpose: Serialize to dict with camelCase keys.

**Algorithm:**
1. Return `{'selectorId': self.__selector_id, 'tag': self.__tag}`

### Selection.from_dict(cls, obj) -> Selection
Purpose: Deserialize from dict.

**Algorithm:**
1. Return `Selection(obj['selectorId'], obj['tag'])`

### LegendItem.as_dict(self) -> Dict
Purpose: Serialize to dict.

**Algorithm:**
1. Build `dict_ = {'color': self.color, 'icon': self.icon, 'name': self.name}`
2. Branch: `self.tooltip` truthy -> add `dict_['tooltip'] = self.tooltip`
3. Return `dict_`

### LegendItem.from_dict(cls, obj) -> LegendItem
Purpose: Deserialize from dict.

**Algorithm:**
1. Return `LegendItem(color=obj['color'], icon=obj['icon'], name=obj['name'], tooltip=obj.get('tooltip'))`

### RelatedLink.as_dict(self) -> Dict
Purpose: Serialize to dict.

**Algorithm:**
1. Build `dict_ = {'type': self.type_.value, 'name': self.name, 'link': self.link}`
2. Branch: `self.description` truthy -> add `dict_['description'] = self.description`
3. Return `dict_`

### RelatedLink.from_dict(cls, obj) -> RelatedLink
Purpose: Deserialize from dict.

**Algorithm:**
1. Return `RelatedLink(type_=RelatedLinkType(obj['type']), name=obj['name'], link=obj['link'], description=obj.get('description'))`

### Component.as_dict(self) -> Dict  (abstractmethod)
Purpose: Base serialization for all components.

**Algorithm:**
1. Build `dict_ = {'id': self.__id, 'type': self._type, 'parameters': {'height': self._height or 200}}`
2. Branch: `self.__selections` truthy -> `dict_['selections'] = [s.as_dict() for s in self.__selections]`
3. Branch: `self.__container_ids` truthy -> `dict_['containerIds'] = [cid for cid in self.__container_ids]`
4. Return `dict_`

### Component.from_dict(cls, obj, scale: int = None) -> Component
Purpose: Factory method that dispatches to the correct component subclass.

**Algorithm:**
1. Extract `parameters = obj.get('parameters', {})`
2. Extract `height = parameters.get('height', 200)`
3. Call `unset(parameters, 'height')` -- mutates the parameters dict, removing 'height'
4. Call `unset(parameters, 'width')` -- mutates the parameters dict, removing 'width'
5. Lookup component class: `TYPE_TO_COMPONENT[obj['type']]`
6. Construct: pass `id_=obj['id'], height=height, width=scale, **{snake_case(k): v for k, v in parameters.items()}`
7. Extract `selections`, `container_ids`, `tags` from `obj`
8. Branch: `selections` truthy -> `component.selections = [Selection.from_dict(s) for s in selections]`
9. Branch: `container_ids` truthy -> `component.__container_ids = [cid for cid in container_ids]`  (NOTE: accesses name-mangled private attribute)
10. Branch: `tags` truthy -> `component.tags = tags` (sets attribute that may not exist on all subclasses)
11. Return `component`

### PlotComponent.as_dict(self) -> Dict
Purpose: Serialize plot component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Set `dict_['parameters']['hideLegend'] = self.hide_legend` (always included)
3. Branch: `self.tooltip` truthy -> `dict_['parameters']['tooltip'] = self.tooltip`
4. Return `dict_`

### DataVizComponent.as_dict(self) -> Dict
Purpose: Serialize data viz component (delegates entirely to super).

**Algorithm:**
1. Return `super().as_dict()`

### DataGridComponent.as_dict(self) -> Dict
Purpose: Serialize data grid component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.tooltip` truthy -> `dict_['parameters']['tooltip'] = self.tooltip`
3. Return `dict_`

### DataScreenerComponent.as_dict(self) -> Dict
Purpose: Serialize data screener component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.tooltip` truthy -> `dict_['parameters']['tooltip'] = self.tooltip`
3. Return `dict_`

### ArticleComponent.as_dict(self) -> Dict
Purpose: Serialize article component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.tooltip` truthy -> add tooltip
3. Branch: `self.commentary_channels` truthy -> add `commentaryChannels`
4. Branch: `self.commentary_to_desktop_link` truthy -> add `commentaryToDesktopLink`
5. Return `dict_`

### CommentaryComponent.as_dict(self) -> Dict
Purpose: Serialize commentary component. Identical to ArticleComponent.as_dict().

**Algorithm:** Same as ArticleComponent.as_dict().

### ContainerComponent.as_dict(self) -> Dict
Purpose: Serialize container component (no height, optional componentId).

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.component_id` truthy -> `dict_['parameters']['componentId'] = self.component_id`
3. `del dict_['parameters']['height']` -- always deletes height from parameters
4. Return `dict_`

### SelectorComponent.as_dict(self) -> Dict
Purpose: Serialize selector component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Set `dict_['parameters']['containerIds'] = self.container_ids` (always included)
3. Branch: `self.default_option_index` truthy -> add `defaultOptionIndex`
4. Branch: `self.title` truthy -> add `title`
5. Branch: `self.tooltip` truthy -> add `tooltip`
6. Branch: `self.parent_selector_id` truthy -> add `parentSelectorId`
7. Return `dict_`

### SelectorComponent.from_dict(cls, obj, scale: int = None) -> SelectorComponent
Purpose: Custom deserialization for selector.

**Algorithm:**
1. Extract `parameters = obj.get('parameters', {})`
2. Construct `SelectorComponent` with `id_=obj['id']`, `height=parameters.get('height', 200)`, `width=scale`, `title=parameters.get('title')`, `container_ids=parameters['containerIds']`, `tooltip=parameters.get('tooltip')`, `default_option_index=parameters.get('defaultOptionIndex')`, `parent_selector_id=parameters.get('parentSelectorId')`

### PromoComponent.as_dict(self) -> Dict
Purpose: Serialize promo component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.tooltip` truthy -> add tooltip
3. Branch: `self.body` truthy -> add body
4. Branch: `self.size` truthy -> add `size` as `self.size.value`
5. Branch: `self.hide_border is not None` -> add `hideBorder` as `self.hide_border` (uses `is not None` check, not truthiness)
6. Branch: `self.transparent is not None` -> add `transparent` as `self.transparent` (uses `is not None` check)
7. Return `dict_`

### PromoComponent.from_dict(cls, obj: Dict, scale: int = None) -> PromoComponent
Purpose: Custom deserialization for promo.

**Algorithm:**
1. Extract `parameters = obj.get('parameters', {})`
2. Get `size = parameters.get('size')`, then `size = PromoSize(size) if size else None`
3. Construct `PromoComponent(id_=obj['id'], height=parameters.get('height', 200), width=scale, tooltip=..., body=..., size=size, hide_border=parameters.get('hideBorder'))`

### SeparatorComponent.as_dict(self) -> Dict
Purpose: Serialize separator component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.name` truthy -> add name
3. Branch: `self.size` truthy -> add size
4. Branch: `self.show_more_url` truthy -> add `showMoreUrl`
5. Return `dict_`

### LegendComponent.as_dict(self) -> Dict
Purpose: Serialize legend component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Set `dict_['parameters']['items'] = [item.as_dict() for item in self.items]` (always included, will raise if items is None)
3. Branch: `self.position` truthy -> add position
4. Branch: `self.transparent` truthy -> add transparent
5. Return `dict_`

### LegendComponent.from_dict(cls, obj: Dict, scale: int = None) -> LegendComponent
Purpose: Custom deserialization for legend.

**Algorithm:**
1. Extract `parameters = obj.get('parameters', {})`
2. Build `items = [LegendItem.from_dict(item) for item in parameters.get('items', [])]`
3. Construct `LegendComponent(id_=obj['id'], height=parameters.get('height', 200), width=scale, selections=obj.get('selections'), position=parameters.get('position'), transparent=parameters.get('transparent'), items=items)`

Note: `selections` is passed as raw dicts (not deserialized via `Selection.from_dict`).

### MonitorComponent.as_dict(self) -> Dict
Purpose: Serialize monitor component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Branch: `self.tooltip` truthy -> add tooltip
3. Return `dict_`

### RelatedLinksComponent.as_dict(self) -> Dict
Purpose: Serialize related links component.

**Algorithm:**
1. `dict_ = super().as_dict()`
2. Set `dict_['parameters']['title'] = self.title` (always)
3. Set `dict_['parameters']['links'] = [link.as_dict() for link in self.links]` (always)
4. Return `dict_`

### RelatedLinksComponent.from_dict(cls, obj, scale: int = None) -> RelatedLinksComponent
Purpose: Custom deserialization for related links.

**Algorithm:**
1. Extract `parameters = obj.get('parameters', {})`
2. Construct with `title=parameters['title']`, `links=[RelatedLink.from_dict(link) for link in parameters['links']]`

## State Mutation
- `Component.__init__`: Sets `__id`, `_height`, `__width`, `__selections`, `__container_ids`, `_type`
- `Component.id_` setter: Regenerates UUID-based id if value is falsy
- `Component.from_dict`: Mutates the `parameters` dict in-place via `unset(parameters, 'height')` and `unset(parameters, 'width')` before passing remaining params to constructor. Also sets `component.__container_ids` (private name-mangled attribute) and `component.tags` (dynamic attribute).
- `ContainerComponent.as_dict`: Calls `del dict_['parameters']['height']` which modifies the returned dict.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `Component.from_dict` | When `obj['type']` is not in `TYPE_TO_COMPONENT` |
| `KeyError` | `SelectorComponent.from_dict` | When `parameters['containerIds']` is missing |
| `KeyError` | `RelatedLinksComponent.from_dict` | When `parameters['title']` or `parameters['links']` is missing |
| `TypeError` | `LegendComponent.as_dict` | When `self.items` is `None` (iterating None) |
| `ValueError` | `RelatedLink.from_dict` | When `obj['type']` is not a valid `RelatedLinkType` value |
| `ValueError` | `PromoComponent.from_dict` | When `parameters['size']` is not a valid `PromoSize` value |

## Edge Cases
- **CommentaryComponent._type = 'plot'**: Shares the same type string as `PlotComponent`. When deserializing via `TYPE_TO_COMPONENT['plot']`, always creates `PlotComponent`, never `CommentaryComponent`. Round-trip serialization of CommentaryComponent produces PlotComponent.
- **Component.from_dict container_ids**: Line 205 accesses `component.__container_ids` -- due to Python name mangling, this creates a new attribute `_Component__container_ids` on the component instead of setting the subclass's private attribute. The actual container_ids property may not reflect this assignment.
- **Component.from_dict tags**: Sets `component.tags` dynamically, which is not a defined attribute on most component subclasses.
- **LegendComponent.from_dict selections**: Passes raw `obj.get('selections')` (list of dicts) without deserializing via `Selection.from_dict`. Other components handle this in `Component.from_dict`.
- **SelectorComponent.default_option_index = 0**: Truthiness check `if self.default_option_index:` means index 0 is treated as falsy and not serialized.
- **PromoComponent vs other components**: PromoComponent uses `is not None` checks for `hide_border` and `transparent`, while most other components use truthiness. This means PromoComponent correctly serializes `False` values.
- **ContainerComponent**: No height parameter, but `super().__init__()` is called without height, so `_height = None`. `as_dict()` first creates `{'height': None or 200}` (= 200) then deletes it.
- **Component.from_dict mutates input**: `unset(parameters, 'height')` and `unset(parameters, 'width')` modify the original dict from the caller.

## Bugs Found
- **Line 205**: `component.__container_ids = [...]` in `Component.from_dict` -- Python name mangling means this sets `_Component__container_ids` rather than the subclass's mangled name. The `container_ids` property reads from the subclass-mangled attribute, so this assignment may be invisible to the property getter. (OPEN)
- **CommentaryComponent._type = 'plot'**: Cannot be distinguished from PlotComponent during deserialization. (OPEN, likely intentional design choice)
- **LegendComponent.from_dict**: Passes raw dict list for `selections` instead of deserializing via `Selection.from_dict`. (OPEN)

## Coverage Notes
- Branch count: ~45
- Key branches: Component.as_dict selections/container_ids (2), Component.from_dict selections/container_ids/tags (3), PlotComponent tooltip (1), DataGridComponent tooltip (1), DataScreenerComponent tooltip (1), ArticleComponent tooltip/channels/desktop_link (3), CommentaryComponent tooltip/channels/desktop_link (3), ContainerComponent component_id (1), SelectorComponent default_option_index/title/tooltip/parent_selector_id (4), PromoComponent tooltip/body/size/hide_border/transparent (5), SeparatorComponent name/size/show_more_url (3), LegendComponent position/transparent (2), MonitorComponent tooltip (1), RelatedLinksComponent (0 branches -- all fields always set), LegendItem tooltip (1), RelatedLink description (1)
- Many `from_dict` methods on subclasses need dedicated testing
- Pragmas: none
