# backtesting.py

## Summary
Backtesting module that provides basket construction and backtesting capabilities. Includes a core `backtest_basket` function that simulates a rebalancing portfolio strategy with transaction costs, a convenience `basket_series` wrapper, and a full-featured `Basket` class that resolves stock assets from SecurityMaster and computes weighted-average price, implied volatility, realized volatility, realized correlation, and forward volatility measures.

## Dependencies
- Internal: `gs_quant.timeseries.econometrics` (volatility, correlation), `gs_quant.timeseries.measures_helper` (VolReference, preprocess_implied_vol_strikes_eq), `gs_quant.timeseries` (as ts: get_historical_and_last_for_measure, append_last_for_measure), `gs_quant.timeseries.algebra` (sqrt), `gs_quant.timeseries.helper` (_create_enum, _tenor_to_month, _month_to_tenor, requires_session, Returns, plot_function, plot_method, Window), `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.data` (GsDataApi, MarketDataResponseFrame, QueryType), `gs_quant.api.utils` (ThreadPoolManager), `gs_quant.data` (DataContext), `gs_quant.data.log` (log_debug), `gs_quant.errors` (MqTypeError, MqValueError)
- External: `logging`, `typing` (Optional, Union), `numpy` (np), `pandas` (pd), `datetime` (dt), `functools` (partial, reduce), `numbers` (Real), `dateutil.relativedelta` (relativedelta as rdelta), `pydash` (chunk)

## Type Definitions

### RebalFreq (Enum, dynamic)
Created via `_create_enum('RebalFreq', ['Daily', 'Weekly', 'Monthly'])`.

| Value | Raw | Description |
|-------|-----|-------------|
| DAILY | `"daily"` | Rebalance every trading day |
| WEEKLY | `"weekly"` | Rebalance weekly |
| MONTHLY | `"monthly"` | Rebalance monthly |

### ReturnType (Enum, dynamic)
Created via `_create_enum('ReturnType', ['excess_return'])`.

| Value | Raw | Description |
|-------|-----|-------------|
| EXCESS_RETURN | `"excess_return"` | Excess return type |

### Basket (class)
Construct and analyze a basket of stocks with rebalancing.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| bbids | `list` | (constructor arg) | List of Bloomberg IDs for stocks |
| rebal_freq | `RebalFreq` | `RebalFreq.DAILY` | Rebalancing frequency |
| weights | `list` | `None` | List of portfolio weights |
| _marquee_ids | `Optional[list]` | `None` | Cached Marquee asset IDs |
| start | `dt.date` | `DataContext.current.start_date` | Start date of data context |
| end | `dt.date` | `DataContext.current.end_date` | End date of data context |
| _spot_data | `Optional[MarketDataResponseFrame]` | `None` | Cached spot price data |
| _returns | `Optional[pd.Series]` | `None` | Cached backtest return series |
| _actual_weights | `Optional[pd.DataFrame]` | `None` | Cached actual weights after rebalancing |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `RebalFreq` | `Enum` | dynamic | Rebalancing frequency enum |
| `ReturnType` | `Enum` | dynamic | Return type enum |

## Functions/Methods

### backtest_basket(series: list, weights: list, costs: list = None, rebal_freq: RebalFreq = RebalFreq.DAILY) -> tuple[pd.Series, pd.DataFrame]
Purpose: Core backtesting engine that simulates a rebalancing portfolio with transaction costs.

**Algorithm:**
1. Set `num_assets = len(series)`.
2. Default `costs` to `[0] * num_assets` if None.
3. Default `weights` to `[1/num_assets] * num_assets` if None (though parameter is required, this is a fallback).
4. Branch: If not all items in `series` are `pd.Series` -> raise `MqTypeError`.
5. Branch: If `len(weights) != num_assets` or `len(weights) != len(costs)` -> raise `MqValueError`.
6. Compute calendar intersection (`cal`) of all pd.Series indices across `series`, `weights`, and `costs`.
7. Reindex all series to `cal`; convert weights and costs to DataFrames (scalars broadcast).
8. Determine rebalance dates:
   - Branch `DAILY`: `rebal_dates = cal`.
   - Branch `WEEKLY`: Generate dates at weekly intervals from `cal[0]`, then snap to nearest calendar date >= each date.
   - Branch `MONTHLY`: Generate dates at monthly intervals from `cal[0]`, then snap to nearest calendar date >= each date.
9. Initialize arrays: `output_arr[0] = 100`, `units_arr[0] = 100 * weights[0] / series[0]`, `actual_weights_arr[0] = weights[0]`.
10. For each subsequent date `i`:
    a. Update performance: `output_arr[i] = output_arr[i-1] + dot(units[i-1], price_change)`.
    b. Update actual weights based on price drift since last rebalance.
    c. Branch: If date is a rebal date:
       - Deduct costs: `output_arr[i] -= dot(costs, |target_weights - actual_weights|) * portfolio_value`.
       - Rebalance units: `units[i] = output_value * weights[i] / prices[i]`.
       - Update `prev_rebal = i`.
       - Reset actual weights to target weights.
    d. Otherwise: `units[i] = units[i-1]` (no rebalance).
11. Return `(pd.Series(output_arr, index=cal), pd.DataFrame(actual_weights_arr, index=cal))`.

**Raises:** `MqTypeError` when series list contains non-Series items. `MqValueError` when series/weights/costs lengths mismatch.

### basket_series(series: list, weights: list = None, costs: list = None, rebal_freq: RebalFreq = RebalFreq.DAILY, return_type: ReturnType = ReturnType.EXCESS_RETURN) -> pd.Series
Purpose: Convenience wrapper that returns only the return series from `backtest_basket`.

**Algorithm:**
1. Call `backtest_basket(series, weights, costs, rebal_freq)`.
2. Return index `[0]` (the return series) from the result tuple.

### Basket.__init__(self, stocks: list, weights: list = None, rebal_freq: RebalFreq = RebalFreq.DAILY) -> None
Purpose: Initialize a Basket with stock IDs, weights, and rebalancing frequency.

**Algorithm:**
1. Branch: If `weights` is truthy and `len(weights) > 0` and `len(stocks) != len(weights)` -> raise `MqValueError`.
2. Store `bbids`, `rebal_freq`, `weights`.
3. Set `_marquee_ids = None`.
4. Set `start` and `end` from `DataContext.current`.
5. Set `_spot_data`, `_returns`, `_actual_weights` to None.

**Raises:** `MqValueError` when stocks and weights have different lengths.

### Basket._reset(self) -> None
Purpose: Reset cached data when data context dates change.

**Algorithm:**
1. Update `start` and `end` from current `DataContext`.
2. Set `_spot_data`, `_returns`, `_actual_weights` to None.

### Basket.get_marquee_ids(self) -> list
Purpose: Resolve Bloomberg IDs to Marquee asset IDs, caching the result.

**Algorithm:**
1. Branch: If `_marquee_ids` is None:
   a. Call `GsAssetApi.get_many_assets_data(bbid=self.bbids, fields=('id', 'bbid', 'rank'), limit=2*len(bbids), order_by=['>rank'])`.
   b. Reverse the results (so higher-rank assets come later and overwrite duplicates).
   c. Build `assets_dict` mapping bbid -> id (last seen wins = highest rank).
   d. Branch: If `len(assets_dict) != len(set(self.bbids))` -> raise `MqValueError` listing missing stocks.
   e. Map `self.bbids` to IDs preserving order.
   f. Cache in `_marquee_ids`.
2. Return `_marquee_ids`.

**Raises:** `MqValueError` when some BBIDs cannot be found in SecurityMaster.

### Basket._ensure_spot_data(self, request_id: Optional[str] = None) -> None
Purpose: Lazily fetch and cache spot price data for all basket constituents.

**Algorithm:**
1. Branch: If DataContext dates changed since last fetch -> call `_reset()`.
2. Branch: If `_spot_data` is None:
   a. Build market data query for SPOT data.
   b. Fetch via `ts.get_historical_and_last_for_measure`.
   c. Pivot to wide format (date x assetId), deduplicate, reindex to marquee_ids order.
   d. Wrap in `MarketDataResponseFrame` and store as `_spot_data`.

### Basket._ensure_backtest(self, request_id: Optional[str] = None) -> None
Purpose: Lazily compute and cache backtest results (returns + actual weights).

**Algorithm:**
1. Branch: If DataContext dates changed since last fetch -> call `_reset()`.
2. Branch: If `_returns` or `_actual_weights` is None:
   a. Get spot data, drop NaN rows.
   b. Split into per-asset series list.
   c. Run `backtest_basket(spot_series, self.weights, rebal_freq=self.rebal_freq)`.
   d. Cache `_returns` and `_actual_weights` (columns set to marquee_ids).

### Basket.get_returns(self, request_id: Optional[str] = None) -> pd.Series
Purpose: Return cached backtest return series (ensuring backtest is computed).

**Algorithm:**
1. Call `_ensure_backtest(request_id)`.
2. Return `self._returns`.

### Basket.get_actual_weights(self, request_id: Optional[str] = None) -> pd.DataFrame
Purpose: Return cached actual weights DataFrame (ensuring backtest is computed).

**Algorithm:**
1. Call `_ensure_backtest(request_id)`.
2. Return `self._actual_weights`.

### Basket.get_spot_data(self, request_id: Optional[str] = None) -> MarketDataResponseFrame
Purpose: Return cached spot data (ensuring data is fetched).

**Algorithm:**
1. Call `_ensure_spot_data(request_id)`.
2. Return `self._spot_data`.

### Basket.price(self, *, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Weighted average price (backtest return series). Decorated with `@requires_session` and `@plot_method`.

**Algorithm:**
1. Branch: If `real_time` is True -> raise `NotImplementedError`.
2. Return `self.get_returns(request_id)`.

### Basket.average_implied_volatility(self, tenor: str, strike_reference: VolReference, relative_strike: Real, *, real_time: bool = False, request_id: Optional[str] = None, source: Optional[str] = None) -> pd.Series
Purpose: Compute weighted average implied volatility across basket constituents. Decorated with `@requires_session` and `@plot_method`.

**Algorithm:**
1. Branch: If `real_time` is True -> raise `NotImplementedError`.
2. Preprocess strike parameters via `preprocess_implied_vol_strikes_eq`.
3. Build `where` dict with tenor, strikeReference, relativeStrike.
4. Chunk marquee IDs into groups of 3.
5. For each chunk, build IMPLIED_VOLATILITY query and create a partial task.
6. Run all tasks async via `ThreadPoolManager.run_async`.
7. Concatenate results into `vol_data`.
8. Get `actual_weights` from backtest.
9. Branch: If not real_time AND end_date >= today AND today not in vol_data index -> append last data via `ts.append_last_for_measure`.
10. Branch: If `vol_data` is empty -> return empty `pd.Series(dtype=float)`.
11. Pivot vol_data to wide format (date x assetId).
12. Reindex actual_weights to match vol dates, forward-fill.
13. Return `actual_weights.mul(vols).sum(axis=1, skipna=False)`.

**Raises:** `NotImplementedError` for real-time data.

### Basket.average_realized_volatility(self, tenor: str, returns_type: Returns = Returns.LOGARITHMIC, *, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute weighted average realized volatility across basket constituents. Decorated with `@requires_session` and `@plot_method`.

**Algorithm:**
1. Branch: If `real_time` is True -> raise `NotImplementedError`.
2. Get spot data and actual weights.
3. For each asset in spot_df, compute `volatility(spot_df[asset_id], Window(tenor, tenor), returns_type)`.
4. Concatenate all vol series into DataFrame.
5. Reindex actual_weights to match, forward-fill.
6. Return `actual_weights.mul(vols).sum(axis=1, skipna=False)`.

**Raises:** `NotImplementedError` for real-time data.

### Basket.average_realized_correlation(self, w: Union[Window, int, str] = Window(None, 0), *, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Compute weighted average pairwise realized correlation between basket constituents. Decorated with `@requires_session` and `@plot_method`.

**Algorithm:**
1. Branch: If `real_time` is True -> raise `NotImplementedError`.
2. Get spot data and actual weights.
3. Initialize zero series `tot` (weighted sum) and `tot_wt` (weight sum).
4. For each pair `(i, j)` where `j > i`:
   a. `corr = correlation(spot_df.iloc[:, i], spot_df.iloc[:, j], w)`.
   b. `wt = actual_weights.iloc[:, i] * actual_weights.iloc[:, j]`.
   c. Accumulate `tot += corr * wt` and `tot_wt += wt`.
5. Return `pd.to_numeric(tot / tot_wt, errors='coerce')`.

**Raises:** `NotImplementedError` for real-time data.

### Basket.average_forward_vol(self, tenor: str, forward_start_date: str, strike_reference: VolReference, relative_strike: Real, *, real_time: bool = False, request_id: Optional[str] = None, source: Optional[str] = None) -> pd.Series
Purpose: Compute weighted average forward volatility for basket constituents. Decorated with `@requires_session` and `@plot_method`.

**Algorithm:**
1. Branch: If `real_time` is True -> raise `NotImplementedError`.
2. Preprocess strike parameters.
3. Convert forward_start_date to months (`t1_month`); compute `t2_month = t1_month + tenor_in_months`.
4. Convert back to tenor strings `t1` and `t2`.
5. Build `where` dict with `tenor=[t1, t2]`, strike reference, relative strike.
6. Fetch implied volatility data for all assets via `ts.get_historical_and_last_for_measure`.
7. Branch: If data is empty -> return empty `pd.Series(dtype=float)`.
8. Group by assetId, then by tenor within each group.
9. For each asset:
   a. Try to get group for t1 and t2 tenors.
   b. Branch: `KeyError` -> log debug, set series to empty.
   c. Otherwise -> compute forward vol: `sqrt((t2_month * lg^2 - t1_month * sg^2) / tenor_months)`.
10. Assemble into DataFrame.
11. Reindex actual_weights to match, forward-fill.
12. Return `actual_weights.mul(vols).sum(axis=1, skipna=False)`.

**Raises:** `NotImplementedError` for real-time data.

## State Mutation
- `Basket._marquee_ids`: Set on first call to `get_marquee_ids()`, never cleared.
- `Basket.start`, `Basket.end`: Updated by `_reset()` when DataContext dates change.
- `Basket._spot_data`: Cached on first `_ensure_spot_data()` call; cleared by `_reset()`.
- `Basket._returns`, `Basket._actual_weights`: Cached on first `_ensure_backtest()` call; cleared by `_reset()`.
- Thread safety: The `Basket` class is not thread-safe. Concurrent calls could race on the cached fields. `ThreadPoolManager.run_async` is used for parallel API calls but operates on independent data.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqTypeError` | `backtest_basket` | `series` list contains non-pd.Series elements |
| `MqValueError` | `backtest_basket` | `series`, `weights`, and `costs` have different lengths |
| `MqValueError` | `Basket.__init__` | `stocks` and `weights` have different lengths |
| `MqValueError` | `Basket.get_marquee_ids` | One or more BBIDs not found in SecurityMaster |
| `NotImplementedError` | `Basket.price` | `real_time=True` |
| `NotImplementedError` | `Basket.average_implied_volatility` | `real_time=True` |
| `NotImplementedError` | `Basket.average_realized_volatility` | `real_time=True` |
| `NotImplementedError` | `Basket.average_realized_correlation` | `real_time=True` |
| `NotImplementedError` | `Basket.average_forward_vol` | `real_time=True` |

## Edge Cases
- `backtest_basket` with `weights` containing pd.Series objects: the calendar intersection logic includes them, allowing time-varying weights.
- `backtest_basket` with `costs` containing pd.Series objects: similarly supports time-varying costs.
- When the data context dates change between method calls on the same `Basket` instance, `_reset()` is triggered to invalidate all cached data, but `_marquee_ids` is NOT reset (it is context-independent).
- `average_implied_volatility` chunks assets into groups of 3 for API calls to avoid request-size limits.
- `average_forward_vol` handles missing tenor data per-asset gracefully by catching `KeyError` and producing an empty series for that asset.
- `basket_series` accepts `return_type` parameter but ignores it (only excess_return is supported).
- `Basket.__init__` weight validation: `if weights and len(weights)` means an empty weights list `[]` passes validation even when `stocks` is non-empty.

## Bugs Found
- None identified.

## Coverage Notes
- Branch count: ~40
- Key branches: `backtest_basket` type/length validation (lines 67-71), rebal frequency dispatch (lines 86-99), rebal date check in loop (line 124); `Basket.__init__` weight length check (line 194); `Basket.get_marquee_ids` missing stocks check (line 226-228); `_ensure_spot_data`/`_ensure_backtest` date-change detection (lines 234, 253); all `real_time` checks in Basket methods; `average_implied_volatility` empty check (line 363) and today-data append (lines 347-360); `average_forward_vol` KeyError catch (line 506) and empty check (line 496).
- Pragmas: none
