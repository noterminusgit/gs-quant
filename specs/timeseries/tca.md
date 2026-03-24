# tca.py

## Summary
Provides a single Transaction Cost Analysis (TCA) measure: intraday covariance between two equity assets. Uses the `QES_INTRADAY_COVARIANCE` dataset to retrieve covariance estimates computed via machine learning for US, EMEA, and Japan equity markets.

## Dependencies
- Internal: `gs_quant.api.gs.data` (QueryType), `gs_quant.common` (AssetClass), `gs_quant.data` (Dataset, DataContext), `gs_quant.markets.securities` (Asset), `gs_quant.timeseries` (plot_measure), `gs_quant.timeseries.measures` (_extract_series_from_df)
- External: `typing` (Optional), `pandas`

## Type Definitions
None.

## Enums and Constants
None.

## Functions/Methods

### covariance(asset: Asset, asset_2: Asset, bucket_start: str = '"08:00:00"', bucket_end: str = '"08:30:00"', *, source, real_time, request_id) -> pd.Series
Purpose: Retrieve intraday covariance estimates between two equity assets for a given time bucket.

Decorated with: `@plot_measure((AssetClass.Equity,), None, [])`

**Algorithm:**
1. Get start and end dates from `DataContext.current`
2. Create `Dataset(Dataset.GS.QES_INTRADAY_COVARIANCE)`
3. Build `where` dict with `assetId` (Marquee ID of first asset), `asset2Id` (Marquee ID of second asset), `bucketStart`, `bucketEnd`
4. Call `ds.get_data(start=start, end=end, where=where)` to fetch data
5. Call `_extract_series_from_df(data, QueryType.COVARIANCE)` to extract the covariance series
6. Set `series.dataset_ids = (Dataset.GS.QES_INTRADAY_COVARIANCE.value,)`
7. Return series

## State Mutation
- No module-level mutable state.
- No mutation of inputs.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised directly) | -- | Errors propagate from Dataset API and _extract_series_from_df |

## Edge Cases
- Default `bucket_start` and `bucket_end` values include embedded double quotes: `'"08:00:00"'` and `'"08:30:00"'`. These quoted strings are passed directly to the dataset query -- the API must accept them with the surrounding quotes.
- The `@plot_measure` decorator is called with `asset_type=None`, meaning this measure accepts any equity asset type.
- The third argument to `@plot_measure` is an empty list `[]`, meaning no `MeasureDependency` is declared.
- If no data is returned for the given asset pair and time bucket, `_extract_series_from_df` will return an empty series.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~1 (essentially linear flow)
- No conditional branches in the function itself; all branching is delegated to `_extract_series_from_df`
- The function is a straightforward data retrieval wrapper
