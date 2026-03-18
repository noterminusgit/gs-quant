# statistics_processors.py

## Summary
Statistical processor implementations: Percentiles, Percentile, Mean, Sum, StdDev, Variance, Covariance, Zscores, StdMove, CompoundGrowthRate. All follow the same pattern: check a_data is ProcessorResult, check success, compute, store result.

## Common Pattern (all single-input processors)
1. Get a_data from children_data
2. If not ProcessorResult → failure "does not have 'a' series yet"
3. If not success → failure "does not have 'a' series values yet"
4. Compute result using timeseries function
5. Store ProcessorResult(True, result)

## Processor-Specific Branches

### PercentilesProcessor
- Has optional `b` input
- Branch: children['b'] exists AND b_data is ProcessorResult AND success → use two-arg percentiles
- **Note**: After b branch, falls through to single-arg percentiles regardless (line 68) — appears intentional as fallback

### PercentileProcessor
- Branch: self.w truthy → clamp window to series_length
- Branch: result not pd.Series → wrap in pd.Series

### MeanProcessor, SumProcessor, StdDevProcessor, VarianceProcessor
- Branch: self.w truthy → clamp window to series_length

### CovarianceProcessor
- Two-input: requires both a and b
- Branch pattern: a success → check b exists → b success → compute

### ZscoresProcessor
- Branch: self.w is None → use Window(None, 0) default

### StdMoveProcessor
- Computes returns of last 2 values, std of all-but-last
- Branch: change is not None AND std_result != 0 → success; else failure

### CompoundGrowthRate
- Formula: (last/first)^(1/n) - 1
- No window parameter

## Edge Cases
- Window larger than series → clamped to series length
- Empty series → would cause IndexError in CompoundGrowthRate
- self.n = 0 in CompoundGrowthRate → ZeroDivisionError

## Bugs Found
None critical. PercentilesProcessor has a control flow issue where successful b computation is followed by single-arg fallback on line 68 (may be intentional).

## Coverage Notes
- ~40 branches total across all processors
