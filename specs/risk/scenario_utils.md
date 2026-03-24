# scenario_utils.py

## Summary
Utility module providing two convenience functions for building equity volatility shock scenarios from market data. `build_eq_vol_scenario_intraday` fetches intraday vol data within a time window, and `build_eq_vol_scenario_eod` fetches end-of-day vol data for a given date. Both functions resolve an asset via `SecurityMaster`, query a `Dataset`, and delegate to `MarketDataVolShockScenario.from_dataframe`.

## Dependencies
- Internal: `gs_quant.markets.securities` (`AssetIdentifier`, `SecurityMaster`)
- Internal: `gs_quant.risk.scenarios` (`MarketDataVolShockScenario`)
- Internal: `gs_quant.data` (`Dataset`)
- External: `datetime` (`dt`)

## Type Definitions

None. This module contains only free functions.

## Enums and Constants

None.

## Functions/Methods

### build_eq_vol_scenario_intraday(asset_name: str, source_dataset: str, ref_spot: float = None, asset_name_type: AssetIdentifier = AssetIdentifier.REUTERS_ID, start_time: dt.datetime = dt.datetime.now() - dt.timedelta(hours=1), end_time: dt.datetime = dt.datetime.now()) -> MarketDataVolShockScenario
Purpose: Build an equity vol shock scenario from intraday vol data within a time window.

**Algorithm:**
1. Resolve asset: `asset = SecurityMaster.get_asset(asset_name, asset_name_type)`.
2. Create dataset: `vol_dataset = Dataset(source_dataset)`.
3. Query data: `vol_data = vol_dataset.get_data(assetId=[asset.get_marquee_id()], strikeReference='forward', startTime=start_time, endTime=end_time)`.
4. Get Reuters ID: `asset_ric = asset.get_identifier(AssetIdentifier.REUTERS_ID)`.
5. Return `MarketDataVolShockScenario.from_dataframe(asset_ric, vol_data, ref_spot)`.

**Note on default parameters:** `start_time` and `end_time` default values use `dt.datetime.now()` evaluated at module import time, NOT at call time. This is a mutable default argument anti-pattern -- all calls using defaults will use the same fixed timestamp from when the module was first imported.

---

### build_eq_vol_scenario_eod(asset_name: str, source_dataset: str, ref_spot: float = None, asset_name_type: AssetIdentifier = AssetIdentifier.REUTERS_ID, vol_date: dt.date = dt.date.today()) -> MarketDataVolShockScenario
Purpose: Build an equity vol shock scenario from end-of-day vol data for a specific date.

**Algorithm:**
1. Resolve asset: `asset = SecurityMaster.get_asset(asset_name, asset_name_type)`.
2. Create dataset: `vol_dataset = Dataset(source_dataset)`.
3. Query data: `vol_data = vol_dataset.get_data(assetId=[asset.get_marquee_id()], strikeReference='forward', startDate=vol_date, endDate=vol_date)`.
4. Get Reuters ID: `asset_ric = asset.get_identifier(AssetIdentifier.REUTERS_ID)`.
5. Return `MarketDataVolShockScenario.from_dataframe(asset_ric, vol_data, ref_spot)`.

**Note on default parameter:** `vol_date` defaults to `dt.date.today()` evaluated at module import time, NOT at call time. Same mutable default anti-pattern as above.

## State Mutation
- No module-level mutable state.
- Both functions are effectful: they call `SecurityMaster.get_asset()` (network/cache) and `Dataset.get_data()` (network).
- Thread safety: Depends on thread safety of `SecurityMaster` and `Dataset`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (propagated) | `SecurityMaster.get_asset` | Asset not found |
| (propagated) | `Dataset.get_data` | Dataset query failure |
| `ValueError` (propagated) | `MarketDataVolShockScenario.from_dataframe` | Empty result DataFrame |

## Edge Cases
- Default `start_time`/`end_time`/`vol_date` are evaluated once at import time and will be stale if the module is long-lived. Callers should always pass explicit time values.
- If `asset.get_identifier(AssetIdentifier.REUTERS_ID)` returns `None` (asset has no Reuters ID), the downstream `MarketDataVolShockScenario.from_dataframe` will receive `None` as `asset_ric`.
- If the dataset query returns an empty DataFrame, the `from_dataframe` call will raise `ValueError` from `max()` on an empty sequence.

## Bugs Found
- Lines 29-30, 46: Default mutable datetime arguments (`dt.datetime.now()`, `dt.date.today()`) are evaluated at import time, not call time. This is a known Python anti-pattern. (OPEN)

## Coverage Notes
- Branch count: ~2 (one per function, essentially linear)
- Key branches: none (both functions are straight-line code)
- Pragmas: none observed
- Note: Testing these functions requires mocking `SecurityMaster`, `Dataset`, and `MarketDataVolShockScenario.from_dataframe` due to network dependencies.
