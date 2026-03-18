# components.py

## Summary
Workspace UI component classes: Selection, LegendItem, RelatedLink, Component (ABC), and 11 concrete component types (Plot, DataViz, DataGrid, DataScreener, Article, Commentary, Container, Selector, Promo, Separator, Legend, Monitor, RelatedLinks). Plus TYPE_TO_COMPONENT mapping.

## Classes

### Selection
- as_dict/from_dict: simple selectorId + tag

### LegendItem
- as_dict: always color/icon/name; tooltip if set
- from_dict: direct

### RelatedLink
- as_dict: always type/name/link; description if set
- from_dict: direct with RelatedLinkType enum

### Component (ABC)
- __init__: auto-generates id if not provided, stores height/width/selections/container_ids
- as_dict(): base dict with id, type, parameters.height (default 200); selections/containerIds if set
- from_dict(cls, obj, scale): dispatches to TYPE_TO_COMPONENT, handles selections/container_ids/tags

### PlotComponent
- as_dict(): super + hideLegend, tooltip if set

### DataVizComponent, DataGridComponent, DataScreenerComponent, MonitorComponent
- Simple: super + optional tooltip

### ArticleComponent, CommentaryComponent
- super + tooltip, commentary_channels, commentary_to_desktop_link
- Note: CommentaryComponent sets _type='plot' (same as PlotComponent)

### ContainerComponent
- No height, has component_id
- as_dict(): deletes height from parameters

### SelectorComponent
- Has container_ids, title, default_option_index, tooltip, parent_selector_id
- Custom from_dict

### PromoComponent
- as_dict():
  1. tooltip, body, size (as .value)
  2. **Bug 4** (line 513): `hideBorder` is set to `self.size` instead of `self.hide_border`
  3. transparent if not None
- Custom from_dict

### SeparatorComponent
- super + name, size, show_more_url

### LegendComponent
- super + items (list of LegendItem), position, transparent
- Custom from_dict

### RelatedLinksComponent
- super + title, links (list of RelatedLink)
- Custom from_dict

## Edge Cases
- Component.from_dict with unknown type → KeyError in TYPE_TO_COMPONENT
- ContainerComponent.as_dict deletes height → could fail if called before super sets it
- CommentaryComponent._type = 'plot' means it shares type with PlotComponent

## Bugs Found
- **Bug 4** (line 513): PromoComponent.as_dict() sets `dict_['parameters']['hideBorder'] = self.size` — should be `self.hide_border`

## Coverage Notes
- ~40 branches across all components
- Many from_dict methods need testing
