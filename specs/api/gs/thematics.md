# thematics.py

## Summary
API client wrapper for GS Thematic analysis endpoints. Provides a single method to fetch thematic exposure data for an entity, optionally filtered by baskets, regions, date range, measures, and notional.

## Dependencies
- Internal: `gs_quant.session` (GsSession)
- External: `datetime` (dt.date), `json`, `enum` (Enum), `typing` (List)

## Type Definitions
None (beyond the enums below).

## Enums and Constants

### ThematicMeasure(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ALL_THEMATIC_EXPOSURES | `"allThematicExposures"` | All thematic exposures |
| TOP_FIVE_THEMATIC_EXPOSURES | `"topFiveThematicExposures"` | Top 5 thematic exposures |
| BOTTOM_FIVE_THEMATIC_EXPOSURES | `"bottomFiveThematicExposures"` | Bottom 5 thematic exposures |
| THEMATIC_BREAKDOWN_BY_ASSET | `"thematicBreakdownByAsset"` | Breakdown by asset |
| NO_THEMATIC_DATA | `"noThematicData"` | No thematic data available |
| NO_PRICING_DATA | `"noPricingData"` | No pricing data available |

Custom `__str__` returns `self.value`.

### Region(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| AMERICAS | `"Americas"` | Americas region |
| ASIA | `"Asia"` | Asia region |
| EUROPE | `"Europe"` | Europe region |

No custom `__str__`.

## Functions/Methods

### GsThematicApi.get_thematics(cls, entity_id: str, basket_ids: List[str] = None, regions: List[Region] = None, start_date: dt.date = None, end_date: dt.date = None, measures: List[ThematicMeasure] = None, notional: float = None) -> List
Purpose: Fetch thematic exposure data for a given entity with optional filters.

**Algorithm:**
1. Build base payload with `{'id': entity_id}`
2. Branch: if `basket_ids` -> add `'basketId'` to payload
3. Branch: if `regions` -> add `'region'` as list of `r.value` for each region
4. Branch: if `start_date` -> add `'startDate'` formatted as `YYYY-MM-DD`
5. Branch: if `end_date` -> add `'endDate'` formatted as `YYYY-MM-DD`
6. Branch: if `measures` -> add `'measures'` as list of `m.value` for each measure
7. Branch: if `notional` -> add `'notional'` to payload
8. POST `/thematics` with `json.dumps(payload)` as payload
9. Return `response.get('results', [])`

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| Network/session errors | `get_thematics` | Propagated from `GsSession` HTTP call |

## Edge Cases
- Payload is serialized with `json.dumps` before passing as `payload=` parameter -- double-serialization could occur if `GsSession.sync.post` also serializes
- `notional` check `if notional` is falsy for `0.0` -- a notional of zero would be omitted from the payload
- `regions` list is converted by extracting `.value` from each enum member
- Missing `results` key in response returns empty list (`.get('results', [])`)

## Bugs Found
- `if notional` is falsy for `0.0`, so a notional value of zero is silently dropped. Should use `if notional is not None` instead. (OPEN)

## Coverage Notes
- Branch count: 6
- Key branches: `basket_ids`, `regions`, `start_date`, `end_date`, `measures`, `notional` truthiness checks
- Pragmas: none
