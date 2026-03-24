# measures_countries.py

## Summary
Provides a single plot measure function `fci` for retrieving daily Financial Conditions Index (FCI) data for countries and regions. Supports multiple FCI sub-measures (contributions from rates, spreads, equities, etc.) with special handling for `REAL_FCI` and `REAL_TWI_CONTRIBUTION` measures that use direct Dataset access instead of the standard market data query path.

## Dependencies
- Internal: `gs_quant.api.gs.data` (QueryType, GsDataApi), `gs_quant.data` (Dataset), `gs_quant.data.core` (DataContext), `gs_quant.entities.entity` (EntityType), `gs_quant.timeseries` (plot_measure_entity), `gs_quant.timeseries.measures` (_market_data_timed, _extract_series_from_df, ExtendedSeries)
- External: `logging`, `enum` (Enum), `typing` (Optional), `pandas`, `inflection` (titleize)

## Type Definitions
None.

## Enums and Constants

### _FCI_MEASURE(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| FCI | `"fci"` | Overall Financial Conditions Index |
| LONG_RATES_CONTRIBUTION | `"long_rates_contribution"` | Long rates contribution to FCI |
| SHORT_RATES_CONTRIBUTION | `"short_rates_contribution"` | Short rates contribution to FCI |
| CORPORATE_SPREAD_CONTRIBUTION | `"corporate_spread_contribution"` | Corporate spread contribution to FCI |
| SOVEREIGN_SPREAD_CONTRIBUTION | `"sovereign_spread_contribution"` | Sovereign spread contribution to FCI |
| EQUITIES_CONTRIBUTION | `"equities_contribution"` | Equities contribution to FCI |
| REAL_LONG_RATES_CONTRIBUTION | `"real_long_rates_contribution"` | Real long rates contribution |
| REAL_SHORT_RATES_CONTRIBUTION | `"real_short_rates_contribution"` | Real short rates contribution |
| DWI_CONTRIBUTION | `"dwi_contribution"` | DWI contribution to FCI |
| REAL_FCI | `"real_fci"` | Real FCI (uses Dataset path) |
| REAL_TWI_CONTRIBUTION | `"real_twi_contribution"` | Real TWI contribution (uses Dataset path) |
| TWI_CONTRIBUTION | `"twi_contribution"` | TWI contribution to FCI |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `LOGGER` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### fci(country_id: str, measure: _FCI_MEASURE = _FCI_MEASURE.FCI, *, source, real_time, request_id) -> pd.Series
Purpose: Retrieve daily FCI data for a country or region.

Decorated with: `@plot_measure_entity(EntityType.COUNTRY, [QueryType.FCI])`

**Algorithm:**
1. Branch: if `real_time` is True -> raise `NotImplementedError('real-time FCI data is not available')`
2. Create `QueryType` from titleized measure value (e.g., `"fci"` -> `"Fci"`)
3. Get start/end dates from `DataContext.current`
4. Branch: if measure is `REAL_FCI` or `REAL_TWI_CONTRIBUTION`:
   - Fetch data from `Dataset('FCI')` with `geographyId=country_id`
   - Branch: if measure is `REAL_FCI` -> set column name to `'realFCI'`
   - Branch: else (REAL_TWI_CONTRIBUTION) -> set column name to `'realTWIContribution'`
   - Branch: if column name not in df columns -> return empty `ExtendedSeries(dtype=float)`
   - Branch: else -> return `ExtendedSeries(df[measure])` with `dataset_ids = ('FCI',)`
5. Branch: else (all other measures):
   - Build market data query with `country_id`, titleized query type, source, and real_time
   - Execute query via `_market_data_timed`
   - Return `_extract_series_from_df(df, type_, True)` (with `handle_missing_column=True`)

**Raises:** `NotImplementedError` when `real_time=True`.

## State Mutation
- No module-level mutable state.
- No mutation of inputs.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `fci` | `real_time=True` |

## Edge Cases
- The `measure` local variable is reassigned from `_FCI_MEASURE` enum to a string (`'realFCI'` or `'realTWIContribution'`) inside the REAL_FCI/REAL_TWI_CONTRIBUTION branch, which shadows the original parameter. This works because the reassigned string is immediately used as a DataFrame column key.
- `inflection.titleize` converts `"fci"` to `"Fci"`, `"long_rates_contribution"` to `"Long Rates Contribution"`, etc. This is used to construct `QueryType` values, so the titleized strings must match valid `QueryType` members.
- If the `'FCI'` Dataset returns a DataFrame missing both `'realFCI'` and `'realTWIContribution'` columns, the function returns an empty series rather than raising an error.
- The function passes `handle_missing_column=True` for the standard path, which means a missing column in the result DataFrame will produce an empty series rather than an error.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~7
- Key branches: real_time check, REAL_FCI vs REAL_TWI_CONTRIBUTION vs other measures, column presence check in Dataset path
- The REAL_FCI and REAL_TWI_CONTRIBUTION paths bypass the standard market data query infrastructure entirely
