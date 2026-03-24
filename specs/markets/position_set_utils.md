# position_set_utils.py

## Summary
Provides utility functions for resolving and enriching position set data by looking up temporal cross-references (xrefs) for assets. Contains functions to retrieve asset xrefs, infer identifier types, group temporal xref data into discrete non-overlapping time ranges, and resolve asset identifiers in batches via the GS Asset API.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi)
- External: `typing` (Tuple), `pandas` (pd), `numpy` (np), `datetime` (dt), `math` (ceil)

## Type Definitions
No classes defined. Module contains only standalone functions.

## Enums and Constants

### Module Constants (implicit)
| Name | Type | Value | Description |
|------|------|-------|-------------|
| all_possible_identifier_types | `list[str]` | `["ticker", "bbid", "bcid", "ric", "cusip", "isin", "sedol", "gss", "gsid", "primeId", "gsn"]` | Local variable (not module-level) defining the set of known identifier types for type inference |

## Functions/Methods

### _get_asset_temporal_xrefs(position_sets_df: pd.DataFrame) -> Tuple[pd.DataFrame, str]
Purpose: Retrieve temporal cross-reference data for all assets in a position set and infer the identifier type.

**Algorithm:**
1. Extract unique identifiers from `position_sets_df['identifier']`
2. Split into batches of 500 using `np.array_split`
3. Determine `earliest_position_date` from `position_sets_df['date'].min()`, convert to `pd.Timestamp` then `pydatetime`
4. For each batch: call `GsAssetApi.get_many_asset_xrefs(list(batch), limit=500)`, accumulate results
5. For each result in `results`:
   a. Get `xrefs_list = res.get('xrefs')`
   b. Branch: `not xrefs_list` -> `continue` (skip this result)
   c. For each xref item in `xrefs_list`:
      i. Parse `endDate` string to datetime
      ii. Branch: `endDate >= earliest_position_date` ->
         - Build `new_xref_map` with `assetId`, `startDate`, `endDate`
         - Merge in `item.get('identifiers')`
         - Append to `all_xrefs`
   d. Extend `asset_xrefs_final` with `all_xrefs`
6. Build `xref_df` DataFrame from collected xref maps
7. Compute `identifiers_found` = intersection of `xref_df.columns` and `all_possible_identifier_types`
8. For each identifier type in `identifiers_found`:
   a. Count matches between `xref_df[each_id_type]` values and `universe`
   b. Branch: `number_of_matches > largest_count` -> update `inferred_identifier_type` and `largest_count`
9. Drop rows where `inferred_identifier_type` column is NaN
10. Fill NaN in `'delisted'` column with `'no'`
11. Filter to rows where `delisted == 'no'`
12. Return `(xref_df, inferred_identifier_type)`

### _group_temporal_xrefs_into_discrete_time_ranges(xref_df: pd.DataFrame) -> None
Purpose: Assign group numbers to xref rows so that each group has non-overlapping time intervals.

**Algorithm:**
1. Define inner function `group_fn(df)`:
   a. Sort by `endDate`
   b. Compute `where_next_group_should_start = df['startDate'].shift(-1) > df['endDate']`
   c. Compute group numbers via `cumsum().shift(1).fillna(0).astype(int)`
   d. Return group numbers
2. Parse `xref_df['startDate']` strings to `datetime` objects (in-place list comprehension)
3. Parse `xref_df['endDate']` strings to `datetime` objects (in-place list comprehension)
4. Call `group_fn(xref_df)` to compute groups
5. Assign `xref_df['group'] = groups`

Note: This function mutates the input DataFrame in-place (adds `'group'` column, converts date string columns to datetime).

### _resolve_many_assets(historical_xref_df: pd.DataFrame, identifier_type: str, **kwargs) -> pd.DataFrame
Purpose: Resolve asset identifiers by group, calling the Asset API for each temporal group.

**Algorithm:**
1. Group `historical_xref_df` by `'group'` column
2. For each group:
   a. Initialize `unmapped = []`, `all_results = []`
   b. Compute `as_of = min(grouped_df['endDate'])`
   c. Get `identifiers` list from `grouped_df[identifier_type]`
   d. Split identifiers into batches of 500
   e. Initialize `resolved_positions = {}`
   f. For each batch:
      i. Call `GsAssetApi.resolve_assets(identifier=list(batch), as_of=as_of, limit=500, fields=[...], **kwargs)`
      ii. Merge result into `resolved_positions`
   g. For each `(asset_identifier, asset_resolved_data)` in `resolved_positions.items()`:
      i. Branch: `asset_resolved_data` is truthy -> append `asset_resolved_data[0]` to `all_results`
      ii. Branch: else -> append `{identifier_type: asset_identifier}` to `unmapped`
   h. Build `df` from `all_results + unmapped`
   i. Set `df['asOfDate'] = as_of`
   j. Merge `df` with `grouped_df` on `["id", identifier_type]` / `["assetId", identifier_type]`
   k. Select columns: `["assetId", "name", identifier_type, "tradingRestriction", "asOfDate", "startDate", "endDate"]`
   l. Append to `all_dfs`
3. Branch: `all_dfs` is truthy -> `pd.concat(all_dfs)`
4. Branch: else -> empty `pd.DataFrame()`
5. Return `final_df`

## State Mutation
- `_group_temporal_xrefs_into_discrete_time_ranges` mutates its input DataFrame in-place:
  - Converts `startDate` and `endDate` columns from strings to `datetime` objects
  - Adds a `'group'` column
- Other functions do not mutate their inputs (they create new DataFrames)
- No module-level mutable state

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised explicitly) | -- | -- |

Potential runtime errors:
- `KeyError` if `position_sets_df` lacks expected columns (`'identifier'`, `'date'`)
- `KeyError` if xref data lacks expected keys (`'xrefs'`, `'assetId'`, `'endDate'`, etc.)
- `ValueError` from `strptime` if date strings are not in `'%Y-%m-%d'` format

## Edge Cases
- `_get_asset_temporal_xrefs`: skips xref entries entirely when `xrefs_list` is falsy (None or empty list)
- `_get_asset_temporal_xrefs`: filters out xref items whose `endDate` is before the earliest position date
- `_get_asset_temporal_xrefs`: infers identifier type by finding the type with the most matches against the input universe; ties go to the last-iterated type (set ordering)
- `_get_asset_temporal_xrefs`: filters out delisted assets (where `delisted != 'no'`), treating NaN delisted values as not delisted
- `_group_temporal_xrefs_into_discrete_time_ranges`: the grouping logic uses `shift(-1)` which produces NaN for the last row; `cumsum` treats NaN as 0, and the final `shift(1).fillna(0)` ensures the first row is group 0
- `_resolve_many_assets`: for unresolved assets, creates a minimal dict with only the identifier_type key, which will have NaN for all other columns after DataFrame construction
- `_resolve_many_assets`: returns empty DataFrame when `all_dfs` is empty (no groups or all groups empty)
- `_resolve_many_assets`: the merge uses `inner` join on `["id", identifier_type]` / `["assetId", identifier_type]`, so unmapped entries (which lack `"id"`) will be dropped from the final result
- Batch size of 500 is hardcoded throughout

## Coverage Notes
- Branch count: ~12
- Key branches: `not xrefs_list` skip (2), `endDate >= earliest_position_date` filter (2), `number_of_matches > largest_count` (2), `asset_resolved_data` truthy/falsy (2), `all_dfs` truthy/falsy (2), outer loop iterations (batches, groups)
- Pragmas: none
