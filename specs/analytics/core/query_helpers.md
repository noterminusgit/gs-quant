# query_helpers.py

## Summary
Helpers for aggregating, fetching, and building DataGrid data queries. Interfaces with GsSession for data retrieval.

## Functions

### aggregate_queries(query_infos)
1. For each query_info:
   a. If MeasureQueryInfo → skip (continue)
   b. Extract coordinate, dataset_id
   c. If dataset_id is None → create failure ProcessorResult, call calculate, continue
   d. Group by dataset_id → query_key → accumulate dimensions
   e. Branch on query.start type (dt.date vs dt.datetime) → startDate vs startTime
   f. Same for query.end → endDate vs endTime
   g. Track realTime flag from coordinate.frequency

### fetch_query(query_info)
1. Build where clause from parameters:
   a. For bool values: if single value → set; if both True/False → skip
   b. Non-bool → use list of values
2. Build query dict with where + range + aliases
3. Branch: realTime AND no range → POST to /last/query; else → POST to /query
4. On exception → log error, return empty DataFrame
5. If df empty → return
6. Set index to 'date' or 'time' column, convert to datetime, remove tz

### build_query_string(dimensions)
1. For each dimension (count, (key, value)):
   a. If string value → wrap in quotes
   b. First dimension: no prefix; subsequent: ' & ' prefix

### valid_dimensions(query_dimensions, df)
1. For each dimension: if dimension[0] not in df.columns → return False
2. All present → return True

## Edge Cases
- dataset_id=None → immediate failure
- Bool dimension with both True and False → skipped entirely
- Empty DataFrame from API → returned as-is
- 'date' vs 'time' column detection

## Bugs Found
None.

## Coverage Notes
- ~38 branches, 10.5% coverage currently
- GsSession calls need mocking
