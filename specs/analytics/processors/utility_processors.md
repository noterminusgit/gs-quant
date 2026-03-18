# utility_processors.py

## Summary
Utility processors: Last, Min, Max, Append, Addition, Subtraction, Multiplication, Division, OneDay, NthLast. Arithmetic and data manipulation on time series.

## Common Pattern (arithmetic processors: Addition, Subtraction, Multiplication, Division)
1. Get a_data
2. If not ProcessorResult → return default value
3. If not success → return a_data (failure propagation)
4. If scalar param (addend/subtrahend/factor/dividend) truthy → apply scalar op, return
5. Get b_data → if success → apply binary op; else → propagate b failure

## Processor-Specific Notes

### LastProcessor
- process(): a_data success AND isinstance Series → return last element as Series

### MinProcessor / MaxProcessor
- process(): a_data success AND isinstance Series → min/max of series as pd.Series

### AppendProcessor
- process(): a.append(b) — uses deprecated pd.Series.append (pandas <2.0)

### OneDayProcessor
- process():
  1. a_data success, len >= 2
  2. Drop last date's data
  3. If remaining len >= 2 → return last 2 values
  4. Else → failure

### NthLastProcessor
- process(): a_data success AND isinstance Series → return nth-from-last element

## Edge Cases
- AppendProcessor uses deprecated .append() — will fail on pandas 2.0+
- Addition/Subtraction/etc. with addend=0 → falsy, falls through to b_data path
- NthLastProcessor with n > len(series) → IndexError
- OneDayProcessor: drop by date may remove multiple intraday entries

## Bugs Found
None critical. AppendProcessor's use of .append() is a compatibility issue with newer pandas.

## Coverage Notes
- ~40 branches total
