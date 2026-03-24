# esg.py

## Summary
API client wrapper for GS ESG (Environmental, Social, Governance) endpoints. Provides a single method to fetch ESG data for an entity, optionally filtered by benchmark, date, measures, and cards.

## Dependencies
- Internal: `gs_quant.session` (GsSession)
- External: `datetime` (dt.date), `logging`, `enum` (Enum), `typing` (List, Dict)

## Type Definitions
None (beyond the enums below).

## Enums and Constants

### ESGCard(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| SUMMARY | `"summary"` | Summary card |
| QUINTILES | `"quintiles"` | Quintiles card |
| WEIGHTS_BY_SECTOR | `"weightsBySector"` | Sector weight breakdown |
| MEASURES_BY_SECTOR | `"measuresBySector"` | Sector measure breakdown |
| WEIGHTS_BY_REGION | `"weightsByRegion"` | Region weight breakdown |
| MEASURES_BY_REGION | `"measuresByRegion"` | Region measure breakdown |
| TOP_TEN_RANKED | `"topTenRanked"` | Top 10 ranked entities |
| BOTTOM_TEN_RANKED | `"bottomTenRanked"` | Bottom 10 ranked entities |
| NO_ESG_DATA | `"noEsgData"` | No ESG data available |
| NO_PRICING_DATA | `"noPricingData"` | No pricing data available |

Custom `__str__` returns `self.value`.

### ESGMeasure(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| G_PERCENTILE | `"gPercentile"` | Governance percentile |
| G_REGIONAL_PERCENTILE | `"gRegionalPercentile"` | Regional governance percentile |
| ES_PERCENTILE | `"esPercentile"` | Environmental/Social percentile |
| ES_DISCLOSURE_PERCENTAGE | `"esDisclosurePercentage"` | ES disclosure percentage |
| ES_MOMENTUM_PERCENTILE | `"esMomentumPercentile"` | ES momentum percentile |

Custom `__str__` returns `self.value`.

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsEsgApi.get_esg(cls, entity_id: str, benchmark_id: str = None, pricing_date: dt.date = None, measures: List[ESGMeasure] = [], cards: List[ESGCard] = []) -> Dict
Purpose: Fetch ESG data for a given entity with optional benchmark, date, measure, and card filters.

**Algorithm:**
1. Build base URL `/esg/{entity_id}?`
2. Branch: if `pricing_date` -> append `&date={pricing_date formatted as YYYY-MM-DD}`
3. Branch: if `benchmark_id` -> append `&benchmark={benchmark_id}`
4. Iterate over `measures`: append `&measure={measure}` for each (uses `__str__` which returns value)
5. Iterate over `cards`: append `&card={card}` for each (uses `__str__` which returns value)
6. GET the constructed URL
7. Return response dict

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| Network/session errors | `get_esg` | Propagated from `GsSession` HTTP call |

## Edge Cases
- Default mutable arguments: `measures=[]` and `cards=[]` use mutable default lists -- standard Python anti-pattern but safe here since the lists are only iterated, never mutated
- URL begins with `?` followed by `&` params, producing `?&date=...` which is valid but unconventional
- Empty measures and cards lists produce no additional query params

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 4
- Key branches: `pricing_date` truthiness, `benchmark_id` truthiness, measures loop (empty vs non-empty), cards loop (empty vs non-empty)
- Pragmas: none
