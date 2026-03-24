# report_utils.py

## Summary
Provides utility functions for batching date ranges used by portfolio performance attribution analysis (PPAA) report queries. Contains logic to estimate optimal batch sizes based on asset counts and a helper to split date ranges into business-day-aligned batches.

## Dependencies
- Internal: none
- External: `datetime` (dt), `pandas` (pd), `math` (ceil), `typing` (List), `pandas.tseries.offsets` (BDay)

## Type Definitions
No classes defined. Module contains only standalone functions.

## Enums and Constants
None.

## Functions/Methods

### _get_ppaa_batches(asset_count: pd.DataFrame, max_row_limit: int) -> List[List[dt.date]]
Purpose: Calculate date batches for PPAA queries based on average position counts and a row limit.

**Algorithm:**
1. Get `start_row = asset_count.iloc[0]`
2. Get `end_row = asset_count.iloc[-1]`
3. Compute `avg_positions = start_row['assetCount'] + end_row['assetCount'] / 2`
   - Note: Due to operator precedence, this computes `start + (end / 2)` rather than `(start + end) / 2`. This appears to be a bug.
4. Parse `start_date` from `start_row['date']` string via `strptime('%Y-%m-%d')`
5. Parse `end_date` from `end_row['date']` string via `strptime('%Y-%m-%d')`
6. Compute `days_per_batch = math.ceil(max_row_limit / (avg_positions * 5))`
   - The factor of 5 accounts for the number of fields per asset: pnl, exposure, asset id, report id, date
7. Return `_batch_dates(start_date, end_date, days_per_batch)`

### _batch_dates(start_date: dt.date, end_date: dt.date, batch_size: int) -> List[List[dt.date]]
Purpose: Split a date range into batches of business days.

**Algorithm:**
1. Branch: `(start_date - end_date).days < batch_size` ->
   - Note: `start_date - end_date` is typically negative (start before end), so `.days` is negative. Since `batch_size` is positive, this condition is almost always True (negative < positive). This means for typical inputs, the function returns `[[start_date, end_date]]` immediately.
   - Return `[[start_date, end_date]]`
2. Initialize `date_list = []`, `curr_end = start_date`
3. While `end_date > curr_end`:
   a. Compute `curr_end = (start_date + BDay(batch_size)).date()`
   b. Branch: `curr_end < end_date` -> use `curr_end`; else cap at `end_date`
   c. Append `[start_date, curr_end]` to `date_list`
   d. Advance `start_date = curr_end + timedelta(days=1)`
4. Return `date_list`

## State Mutation
- No module-level mutable state
- All functions are pure (no side effects)
- Input DataFrames are not mutated

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none raised explicitly) | -- | -- |

Potential runtime errors:
- `IndexError` from `iloc[0]` or `iloc[-1]` if `asset_count` DataFrame is empty
- `KeyError` if `asset_count` lacks `'assetCount'` or `'date'` columns
- `ZeroDivisionError` if `avg_positions * 5` equals 0 (unlikely with typical data)
- `ValueError` from `strptime` if date strings are malformed

## Edge Cases
- `_get_ppaa_batches` operator precedence bug: `start_row['assetCount'] + end_row['assetCount'] / 2` computes `start + (end/2)` instead of the likely intended `(start + end) / 2`
- `_batch_dates` early return condition `(start_date - end_date).days < batch_size` is effectively always true for valid date ranges where `start_date <= end_date`, because the difference is negative or zero and `batch_size` is positive. The while-loop path is only reached if `start_date > end_date` (unusual/invalid) or `batch_size` is negative (invalid).
- `_batch_dates` while-loop always recomputes from the original `start_date + BDay(batch_size)` on the first iteration, but then advances `start_date` for subsequent iterations
- `_batch_dates` uses `BDay` (business day offset) for advancement, so batch boundaries align with business days
- When `start_date == end_date`, the early return produces `[[start_date, end_date]]` (a single batch with identical start and end)

## Bugs Found
- Line 28: `avg_positions = start_row['assetCount'] + end_row['assetCount'] / 2` -- operator precedence issue. Division binds tighter than addition, computing `start + (end/2)` instead of the likely intended average `(start + end) / 2`. (OPEN)
- Line 36: `(start_date - end_date).days < batch_size` -- the condition uses `start_date - end_date` which is negative for valid ranges (start < end), making the condition always true when `batch_size > 0`. The while-loop code path is effectively dead code for normal inputs. Likely should be `(end_date - start_date).days < batch_size`. (OPEN)

## Coverage Notes
- Branch count: ~5
- Key branches: `_batch_dates` early return (2), while loop entry (2), `curr_end < end_date` cap (2)
- The while-loop path in `_batch_dates` is effectively unreachable for normal inputs due to the bug in the early-return condition, making those branches difficult to cover without contrived inputs
- Pragmas: none
