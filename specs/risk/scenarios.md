# scenarios.py

## Summary
Provides convenience wrapper classes for constructing market data scenarios used in risk computation. `MarketDataShockBasedScenario` simplifies creating shock-based scenarios from a mapping of patterns to shocks. `MarketDataVolShockScenario` adds a `from_dataframe` class method to construct volatility surface override scenarios from a pandas DataFrame of implied volatility data.

## Dependencies
- Internal: `gs_quant.target.risk` (`MarketDataPattern`, `MarketDataShock`, `MarketDataPatternAndShock`, `MarketDataShockBasedScenario` as `__MarketDataShockBasedScenario`, `MarketDataVolShockScenario` as `__MarketDataVolShockScenario`, `MarketDataVolSlice`, `MarketDataShockType`)
- External: `typing` (`Mapping`, `Optional`)
- External: `pandas` (`pd`)

## Type Definitions

### MarketDataShockBasedScenario (class)
Inherits: `gs_quant.target.risk.MarketDataShockBasedScenario` (aliased as `__MarketDataShockBasedScenario`)

No additional fields. This is a convenience wrapper that accepts a `Mapping` instead of a pre-built tuple.

### MarketDataVolShockScenario (class)
Inherits: `gs_quant.target.risk.MarketDataVolShockScenario` (aliased as `__MarketDataVolShockScenario`)

No additional fields. Adds a `from_dataframe` class method.

## Enums and Constants

None.

## Functions/Methods

### MarketDataShockBasedScenario.__init__(self, shocks: Mapping[MarketDataPattern, MarketDataShock], name: Optional[str] = None)
Purpose: Create a shock-based scenario from a dict/mapping of pattern-to-shock pairs.

**Algorithm:**
1. Convert `shocks` mapping to a tuple of `MarketDataPatternAndShock(p, s)` objects for each `(p, s)` in `shocks.items()`.
2. Call `super().__init__(tuple_of_pattern_and_shock, name=name)`.

---

### MarketDataVolShockScenario.from_dataframe(cls, asset_ric: str, df: pd.DataFrame, ref_spot: float = None, name: str = None) -> MarketDataVolShockScenario
Purpose: Class method to construct a vol shock scenario from a DataFrame of implied volatility data.

**Algorithm:**
1. Find `last_datetime = max(list(df.index))` -- the most recent timestamp in the DataFrame index.
2. Filter to only rows at `last_datetime`: `df_filtered = df.loc[df.index == last_datetime]`.
3. Group by `'expirationDate'` column: `df_grouped = df_filtered.groupby('expirationDate')`.
4. Initialize `vol_slices = []`.
5. For each `key` in `df_grouped.groups`:
   a. Get the group: `value = df_grouped.get_group(key)`.
   b. Sort by `'absoluteStrike'`: `df_sorted = value.sort_values(['absoluteStrike'])`.
   c. Extract `strikes = list(df_sorted.absoluteStrike)`.
   d. Extract `levels = list(df_sorted.impliedVolatility)`.
   e. Create `vol_slice = MarketDataVolSlice(key.date(), strikes, levels)`.
   f. Append to `vol_slices`.
6. Construct scenario: `MarketDataVolShockScenario(MarketDataPattern('Eq Vol', asset_ric), MarketDataShockType.Override, vol_slices, ref_spot, name=name)`.
7. Return the scenario.

**Expected DataFrame schema:**
- Index: datetime (timestamps)
- Columns: `expirationDate` (datetime with `.date()` method), `absoluteStrike` (float), `impliedVolatility` (float)

## State Mutation
- No mutable state. Both classes produce new immutable scenario objects.
- The input `df` DataFrame is not modified (filtering and sorting produce new DataFrames).
- Thread safety: Stateless class methods are thread-safe.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` (implicit) | `from_dataframe` | If `df` is empty, `max(list(df.index))` raises `ValueError` |
| `KeyError` (implicit) | `from_dataframe` | If `df` lacks `expirationDate`, `absoluteStrike`, or `impliedVolatility` columns |

## Edge Cases
- `from_dataframe` with an empty DataFrame will raise `ValueError` from `max()` on empty sequence.
- `from_dataframe` uses `key.date()` which assumes `key` (the `expirationDate` group key) is a datetime-like object with a `.date()` method. If the column contains plain `date` objects or strings, this will fail.
- If multiple rows share the same `(expirationDate, absoluteStrike)` pair at the last datetime, all duplicates are included (no deduplication).
- `ref_spot=None` is valid and passed through to the parent constructor.

## Coverage Notes
- Branch count: ~4
- Key branches: `MarketDataShockBasedScenario.__init__`, `from_dataframe` loop (empty vs non-empty groups), `name` presence
- Pragmas: none observed
