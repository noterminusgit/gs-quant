# api/gs/carbon.py

## Summary
API client for retrieving carbon analytics from the GS Carbon service. Defines multiple enums for carbon analysis dimensions (cards, scopes, coverage categories, entity types, analytics views) and a single API class `GsCarbonApi` that constructs a URL with query parameters and issues a GET request via `GsSession`.

## Dependencies
- Internal: `gs_quant.common` (`Currency`)
- Internal: `gs_quant.session` (`GsSession`)
- External: `logging` (`getLogger`), `enum` (`Enum`), `typing` (`Dict`, `List`), `urllib.parse` (`urlencode`)

## Type Definitions

### GsCarbonApi (class)
Inherits: `object`

Stateless API client. Contains a single classmethod `get_carbon_analytics`. No instance state.

## Enums and Constants

### CarbonCard (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| COVERAGE | `"coverage"` | Coverage analytics card |
| SBTI_AND_NET_ZERO_TARGETS | `"sbtiAndNetZeroTargets"` | SBTi and net zero targets card |
| EMISSIONS | `"emissions"` | Emissions analytics card |
| ALLOCATIONS | `"allocations"` | Allocations analytics card |
| ATTRIBUTION | `"attribution"` | Attribution analytics card |

Custom `__str__` returns `self.value`. Note: comment says `highestEmitters` should not be accessible through API.

### CarbonCoverageCategory (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| WEIGHTS | `"weights"` | Coverage by weights |
| NUMBER_OF_COMPANIES | `"numberOfCompanies"` | Coverage by number of companies |

Custom `__str__` returns `self.value`.

### CarbonTargetCoverageCategory (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| CAPITAL_ALLOCATED | `"capitalAllocated"` | Target coverage by capital allocated |
| PORTFOLIO_EMISSIONS | `"portfolioEmissions"` | Target coverage by portfolio emissions |

Custom `__str__` returns `self.value`.

### CarbonScope (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| TOTAL_GHG | `"totalGHG"` | Total greenhouse gas emissions |
| SCOPE1 | `"scope1"` | Scope 1 direct emissions |
| SCOPE2 | `"scope2"` | Scope 2 indirect emissions |

Custom `__str__` returns `self.value`.

### CarbonEmissionsAllocationCategory (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| GICS_SECTOR | `"gicsSector"` | Allocation by GICS sector |
| GICS_INDUSTRY | `"gicsIndustry"` | Allocation by GICS industry |
| REGION | `"region"` | Allocation by region |

Custom `__str__` returns `self.value`.

### CarbonEmissionsIntensityType (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| EI_ENTERPRISE_VALUE | `"emissionsIntensityEnterpriseValue"` | Intensity by enterprise value |
| EI_REVENUE | `"emissionsIntensityRevenue"` | Intensity by revenue |
| EI_MARKETCAP | `"emissionsIntensityMarketCap"` | Intensity by market cap |

Custom `__str__` returns `self.value`.

### CarbonEntityType (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| PORTFOLIO | `"portfolio"` | Portfolio-level analytics |
| BENCHMARK | `"benchmark"` | Benchmark-level analytics |

Custom `__str__` returns `self.value`.

### CarbonAnalyticsView (Enum)
| Member | Value (str) | Description |
|--------|-------------|-------------|
| LONG | `"Long"` | Long component view |
| SHORT | `"Short"` | Short component view |

Custom `__str__` returns `self.value`.

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsCarbonApi.get_carbon_analytics(cls, entity_id: str, benchmark_id: str = None, reporting_year: str = 'Latest', currency: Currency = None, include_estimates: bool = False, use_historical_data: bool = False, normalize_emissions: bool = False, cards: List[CarbonCard] = [], analytics_view: CarbonAnalyticsView = CarbonAnalyticsView.LONG) -> Dict
Purpose: Retrieve carbon analytics for a given entity, with optional benchmark comparison and filtering.

**Algorithm:**
1. Build base URL: `/carbon/{entity_id}?`
2. Construct a dict of query parameters:
   - `benchmark` = `benchmark_id` (may be None)
   - `reportingYear` = `reporting_year`
   - `currency` = `currency.value` if `currency is not None`, else `None`
   - `includeEstimates` = `str(include_estimates).lower()` (e.g. `"false"`)
   - `useHistoricalData` = `str(use_historical_data).lower()`
   - `normalizeEmissions` = `str(normalize_emissions).lower()`
   - `card` = all `CarbonCard` members if `len(cards) == 0`, else `cards`
   - `analyticsView` = `analytics_view.value`
3. Filter out items where value is `None` via `filter(lambda item: item[1] is not None, ...)`
4. URL-encode with `urlencode(..., True)` (doseq=True for list values)
5. Append encoded string to URL
6. GET the URL via `GsSession.current.sync.get(url)`
7. Return the raw response dict

## State Mutation
- No instance state; single classmethod.
- No module-level mutable state (aside from `_logger`).
- Relies on `GsSession.current` for HTTP session (external state).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none explicitly raised) | -- | All errors would propagate from `GsSession` or `urlencode` |

## Edge Cases
- `cards` default is a mutable list `[]` in the function signature -- standard Python mutable-default-argument concern. However, the code checks `len(cards) == 0` and replaces with all `CarbonCard` members, so the mutation risk is mitigated in practice.
- When `cards` is empty, `[c for c in CarbonCard]` produces a list of enum members. `urlencode` with `doseq=True` will call `str()` on each, which invokes the custom `__str__` returning the value string.
- Branch: `currency is not None` controls whether `currency.value` is accessed; passing a non-`Currency` value with a `.value` attribute would still work.
- `benchmark_id=None` is filtered out by the `filter(lambda ...)` step since `None` values are excluded.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: 3
  - `currency is not None` (true/false)
  - `len(cards) == 0` (true/false)
  - `item[1] is not None` filter lambda (true/false per item)
- Missing branches: None identified
- Pragmas: None
