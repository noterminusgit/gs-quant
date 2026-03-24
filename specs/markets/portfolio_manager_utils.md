# portfolio_manager_utils.py

## Summary
Provides utility functions for building macro portfolio exposure DataFrames. Contains functions to construct portfolio constituent data, compute universe sensitivity, build exposure matrices with optional factor category grouping, and batch date lists for API pagination. These utilities support the `PortfolioManager` class's macro factor exposure analysis.

## Dependencies
- Internal: `gs_quant.markets.report` (PerformanceReport), `gs_quant.models.risk_model` (MacroRiskModel, DataAssetsRequest, RiskModelUniverseIdentifierRequest as UniverseIdentifierRequest, FactorType), `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.errors` (MqValueError)
- External: `typing` (List, Dict), `numpy` (np), `pandas` (pd), `datetime` (dt)

## Type Definitions
No classes defined. Module contains only standalone functions.

## Enums and Constants
None.

## Functions/Methods

### build_macro_portfolio_exposure_df(df_constituents_and_notional: pd.DataFrame, universe_sensitivities_df: pd.DataFrame, factor_dict: Dict, factor_category_dict: Dict, factors_by_name: bool) -> pd.DataFrame
Purpose: Build a DataFrame showing each asset's exposure to macro factors, with optional factor category grouping.

**Algorithm:**
1. Branch: `factors_by_name` is `True` -> rename sensitivity columns using `factor_dict` (id-to-name mapping)
2. Determine `columns_to_keep`: Branch on `factors_by_name` -> `factor_dict.values()` if True, `factor_dict.keys()` if False
3. Drop columns not in `columns_to_keep`
4. Rename `df_constituents_and_notional` columns: `"name"` -> `"Asset Name"`, `"netExposure"` -> `"Notional"`
5. Get `gsids_with_exposure` from the sensitivity DataFrame index
6. Branch: `gsids_with_exposure` is empty ->
   a. Print warning message
   b. Return empty `pd.DataFrame()`
7. Filter `notional_df` to only assets with exposure
8. Divide all sensitivity values by 100
9. Multiply each factor column by the asset's Notional
10. Merge `notional_df` with `universe_sensitivities_df` on `'Asset Identifier'`
11. Branch: `factor_category_dict` is truthy ->
    a. Create `pd.MultiIndex` columns from `(factor_category, factor)` tuples using `factor_category_dict.get(factor, "Asset Information")`
    b. Aggregate sum over all factor columns to get portfolio total exposure
    c. Concatenate totals row to the DataFrame
    d. Group and aggregate by `"Macro Factor Category"` to get category-level totals
    e. Build `factor_and_category_zip` mapping
    f. Compute per-category exposure list via `map`
    g. Concatenate category totals row
    h. Rename index `0` -> `"Portfolio Exposure Per Macro Factor"`
    i. Set `"Asset Name"` to `NaN` for total rows
    j. Sort by `"Portfolio Exposure Per Macro Factor Category"` descending
    k. Reorder columns: Asset Name, Notional first
12. Branch: `factor_category_dict` is falsy ->
    a. Aggregate sum over non-`"Asset Name"` columns
    b. Concatenate totals row
    c. Rename index `0` -> `"Portfolio Exposure Per Macro Factor"`
    d. Set `"Asset Name"` to `NaN` for total row
    e. Sort by `"Portfolio Exposure Per Macro Factor"` descending
    f. Move `"Asset Name"` column to position 0
13. Set `index.name = 'Asset Identifier'`
14. Return portfolio exposure DataFrame

### build_portfolio_constituents_df(performance_report: PerformanceReport, date: dt.date) -> pd.DataFrame
Purpose: Retrieve and assemble a DataFrame of portfolio constituents with names, GSIDs, and net exposures.

**Algorithm:**
1. Call `performance_report.get_portfolio_constituents(fields=['netExposure'], start_date=date, end_date=date)`
2. Branch: `constituents_df.empty` -> raise `MqValueError` with date in message
3. Select `["assetId", "netExposure"]` columns
4. Drop NaN rows, set index to `"assetId"`, rename axis to `"Asset Identifier"`
5. Call `GsAssetApi.get_many_assets_data_scroll` with `fields=['name', 'gsid', 'id']`, `as_of=datetime(date...)`, `limit=1000`, filtered by constituent asset IDs
6. Build `assets_data_df` from records, set index to `"id"`, fillna `"Name not available"` for name
7. Merge `assets_data_df` with `constituents_df` on `"Asset Identifier"`
8. Reset index, set index to `"gsid"`, rename axis to `"Asset Identifier"`, sort by index
9. Return constituents DataFrame

**Raises:** `MqValueError` when no constituents found on the given date

### build_sensitivity_df(universe: List, model: MacroRiskModel, date: dt.date, factor_type: FactorType, by_name: bool) -> pd.DataFrame
Purpose: Retrieve universe sensitivity data from a macro risk model.

**Algorithm:**
1. Call `model.get_universe_sensitivity(start_date=date, end_date=date, assets=DataAssetsRequest(UniverseIdentifierRequest.gsid, universe), factor_type=factor_type, get_factors_by_name=by_name)`
2. Branch: `universe_sensitivities_df.empty` ->
   a. Print warning message
   b. Return empty `pd.DataFrame()`
3. Reset level-1 index (drop it), rename axis to `"Asset Identifier"`, sort by index
4. Return sensitivity DataFrame

### build_exposure_df(notional_df: pd.DataFrame, universe_sensitivities_df: pd.DataFrame, factor_categories: List, factor_data: pd.DataFrame, by_name: bool) -> pd.DataFrame
Purpose: Compute exposure DataFrame by multiplying notional by sensitivity, with optional factor category grouping.

**Algorithm:**
1. Divide all sensitivity values by 100
2. Multiply each factor column by notional
3. Branch: `factor_data.empty` ->
   a. Branch: `factor_categories` is truthy ->
      i. Compute `categories_names`: Branch on `by_name` -> `[f.name for f]` if True, `[f.id for f]` if False
      ii. Filter sensitivity columns to `categories_names`
   b. Aggregate sum row (`"Total Factor Category Exposure"`)
   c. Concatenate sum row
   d. Sort by `"Total Factor Category Exposure"` descending
   e. Aggregate notional sum row similarly
   f. Join notional with sensitivities, rename axis to `"Factor Category"`
4. Branch: `factor_data` is not empty ->
   a. Set `factor_data` index: Branch on `by_name` -> index by `"name"` if True, `"identifier"` if False
   b. Build `MultiIndex` column tuples `(factorCategory, factor)` or `(factorCategoryId, factor)`: Branch on `by_name`
   c. Set MultiIndex axis, rename to `("Factor Category", "Factor")`
   d. Aggregate sum row (`"Total Factor Exposure"`)
   e. Concatenate sum row, sort descending
   f. Branch: `factor_categories` is truthy ->
      i. Compute `categories_names` (same branch on `by_name`)
      ii. Filter columns to selected categories
   g. Aggregate notional sum row
   h. Set notional columns as `MultiIndex` with `("Asset Information", "Asset Name"/"Notional")`
   i. Join notional with sensitivities
5. Return exposure DataFrame

### get_batched_dates(dates: List[dt.date], batch_size: int = 90) -> List[List[dt.date]]
Purpose: Split a list of dates into batches of a specified size.

**Algorithm:**
1. Return list comprehension: `[dates[i : i + batch_size] for i in range(0, len(dates), batch_size)]`

## State Mutation
- No module-level mutable state
- All functions are pure (no side effects beyond API calls and `print`)
- DataFrames are modified in-place within function scope (e.g., `/= 100`, column multiplication)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `build_portfolio_constituents_df` | When `constituents_df` is empty (no constituents found on the given date) |

## Edge Cases
- `build_macro_portfolio_exposure_df`: returns empty DataFrame when no assets have exposure to requested factors
- `build_macro_portfolio_exposure_df`: uses `factor_category_dict.get(factor, "Asset Information")` as fallback category for unmapped factors
- `build_sensitivity_df`: returns empty DataFrame and prints a warning when no assets are exposed
- `build_exposure_df`: two fundamentally different code paths depending on whether `factor_data` is empty or not
- `build_exposure_df`: `factor_categories` filtering only applies when the list is truthy (non-empty)
- `build_exposure_df`: `by_name` flag controls both column naming convention and `factor_data` index key
- `get_batched_dates`: returns `[[]]` (list containing one empty list) when `dates` is empty, since `range(0, 0, 90)` yields no iterations -- actually returns `[]` since the comprehension produces nothing
- `_get_ppaa_batches` (from report_utils) is a separate module; this module has no `_get_ppaa_batches`

## Coverage Notes
- Branch count: ~20
- Key branches: `factors_by_name` (2 in `build_macro_portfolio_exposure_df`, repeated in `build_exposure_df`), `factor_category_dict` truthy/falsy (2), `gsids_with_exposure` empty (2), `constituents_df.empty` (2), `universe_sensitivities_df.empty` (2), `factor_data.empty` (2), `factor_categories` truthy/falsy (2 occurrences), `by_name` in `build_exposure_df` (4 occurrences)
- Pragmas: none
