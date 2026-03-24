# factor_analytics.py

## Summary
Provides the `FactorAnalytics` class for performing style factor analysis on portfolio position sets via the GS Risk/Liquidity API, and creating Plotly visualizations (bar charts, time series, heatmap comparisons) of factor exposures, risk metrics, and performance data.

## Dependencies
- Internal: `gs_quant.api.gs.risk` (GsRiskApi), `gs_quant.markets.position_set` (PositionSet), `gs_quant.errors` (MqValueError)
- External: `logging`, `typing` (Dict, List), `pandas` (pd), `plotly.graph_objects` (go), `re` (imported lazily inside exception handler)

## Type Definitions

### FactorAnalytics (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| risk_model_id | `str` | (required) | Risk model identifier (e.g., `'AXIOMA_AXUS4S'`) |
| currency | `str` | `'USD'` | Currency for analysis |
| participation_rate | `float` | `0.1` | Market participation rate (10%) |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### FactorAnalytics.__init__(self, risk_model_id: str, currency: str = 'USD', participation_rate: float = 0.1)
Purpose: Initialize instance with risk model configuration.

**Algorithm:**
1. Set `self.risk_model_id = risk_model_id`.
2. Set `self.currency = currency`.
3. Set `self.participation_rate = participation_rate`.

### FactorAnalytics.get_factor_analysis(self, position_set: PositionSet) -> Dict
Purpose: Get style factor analysis for a position set via the liquidity endpoint.

**Algorithm:**
1. Branch: if `not position_set` or `not position_set.positions` -> raise `MqValueError("Position set is empty")`.
2. Branch: if `not position_set.date` -> raise `MqValueError("Position set must have a date")`.
3. Build list of unresolved positions: `[p for p in position_set.positions if not p.asset_id]`.
4. Branch: if `unresolved` is non-empty -> log info, call `position_set.resolve()`.
5. Initialize `api_positions = []` and `position_mapping = {}`.
6. For each position in `position_set.positions`:
   a. Branch: if `not position.asset_id` -> log warning with `position.identifier`, skip via `continue`.
   b. Store `position_mapping[position.asset_id] = position.identifier`.
   c. Branch: if `position.quantity is not None` -> append `{"assetId": asset_id, "quantity": quantity}`.
   d. Branch: elif `position.weight is not None` -> append `{"assetId": asset_id, "weight": weight * 100}` (note: weight is multiplied by 100 to convert to percentage).
   e. Branch: else -> log warning that position has no quantity or weight (position is skipped).
7. Branch: if `not api_positions` (all positions were invalid) -> raise `MqValueError("No valid positions to analyze")`.
8. Determine `notional`: `position_set.reference_notional` if truthy, else `None`.
9. Try: call `GsRiskApi.get_liquidity_and_factor_analysis(positions=api_positions, risk_model=self.risk_model_id, date=position_set.date, currency=self.currency, participation_rate=self.participation_rate, notional=notional, measures=[...])` with 5 measure strings. Return result.
10. Except `MqValueError as e`:
    a. Convert to string: `error_msg = str(e)`.
    b. Branch: if `'missing in marquee'` in `error_msg.lower()`:
       - Import `re`.
       - Extract asset IDs matching pattern `r'MA[A-Z0-9]+'` from `error_msg`.
       - Branch: if `asset_ids_match` is non-empty:
         - Build `problematic_positions` list by looking up each asset_id in `position_mapping` (defaulting to `f"Unknown (Asset ID: {asset_id})"`).
         - Raise new `MqValueError` with detailed diagnostic message including the problematic positions and original error.
    c. If the error does not match `'missing in marquee'` pattern, or no asset IDs found -> re-raise original exception.
11. Except generic `Exception as e` -> log error, re-raise.

**Raises:**
- `MqValueError` when position set is empty, has no date, has no valid positions, or API returns asset resolution error.
- Any `Exception` from the API call is logged and re-raised.

### FactorAnalytics.convert_hedge_factor_exposures(self, style_factors: List) -> Dict
Purpose: Convert hedge result style factor exposures to the same format as `get_factor_analysis()`.

**Algorithm:**
1. Branch: if `not style_factors` (empty/None) -> raise `MqValueError("Style factor exposures data is empty")`.
2. (Dead code) Second check `if not style_factors` -> logs warning but never reached because step 1 already raised.
3. Build `sub_factors` list: `[{'name': item['factor'], 'value': item['exposure']} for item in style_factors]`.
4. Return dict with keys: `factorExposureBuckets` (list with one entry `{'name': 'Style', 'subFactors': sub_factors}`), `notional` (0), `currency` (`'USD'`), `riskBuckets` (empty list).

**Raises:** `MqValueError` when `style_factors` is empty or falsy.

### FactorAnalytics.create_exposure_bar_chart(self, exposures: Dict[str, float], title: str, horizontal: bool = True) -> go.Figure
Purpose: Create a bar chart of style factor exposures with green/red color coding.

**Algorithm:**
1. Branch: if `not exposures` (empty dict) -> return figure with annotation "No data available".
2. Extract `names` and `values` from the exposures dict.
3. Compute `colors`: `'green'` if value >= 0, else `'red'`.
4. Create `go.Figure()`.
5. Branch: if `horizontal` is `True`:
   a. Add horizontal `go.Bar` trace (y=names, x=values, orientation='h').
   b. Set x-axis title "Exposure", y-axis title "".
6. Branch: else (vertical):
   a. Add vertical `go.Bar` trace (x=names, y=values).
   b. Set x-axis title "", y-axis title "Exposure".
7. Compute `chart_height`:
   - Branch: if `horizontal` -> `max(300, len(names) * 40 + 150)`.
   - Branch: else -> `500`.
8. Update layout with `title`, computed `height`, margin (l=200 if horizontal else 50).
9. Return figure.

### FactorAnalytics.create_style_factor_chart(self, factor_analysis: Dict, rows: int = None, title: str = "Style Factor Exposures") -> go.Figure
Purpose: Create a bar chart showing top positive and negative style factor exposures.

**Algorithm:**
1. Branch: if `'factorExposureBuckets'` not in `factor_analysis` -> return figure with annotation "No style factor data available".
2. Iterate through `factor_analysis['factorExposureBuckets']`:
   a. For each bucket, Branch: if `bucket.get('name') == 'Style'`:
      - For each sub_factor in `bucket.get('subFactors', [])`:
        - Extract `factor_name` and `factor_value` (default 0).
        - Branch: if `factor_name` is truthy -> add to `style_factors` dict.
      - `break` after processing the Style bucket.
3. Branch: if `not style_factors` (empty) -> return figure with annotation.
4. Split into `positive_factors` (v > 0) and `negative_factors` (v < 0). Note: factors with v == 0 are excluded from both.
5. Compute `top_negative_limit`: `rows` if `rows is not None`, else `None` (no limit).
6. Sort `negative_factors` ascending by value, slice to `[:top_negative_limit]`.
7. Sort `positive_factors` descending by value, slice to `[:top_positive_limit]`, then reverse for ascending display order.
8. Merge into `selected_factors = {**top_negative, **top_positive}`.
9. Branch: if `not selected_factors` -> return figure with annotation.
10. Branch: if `rows is not None` -> modify title to include `"(Top {rows} Positive & Top {rows} Negative, {total_factors} Total)"`.
11. Delegate to `self.create_exposure_bar_chart(selected_factors, subset_title, horizontal=True)`.

### FactorAnalytics.create_exposure_summary_table(self, factor_analysis: Dict) -> pd.DataFrame
Purpose: Create a summary DataFrame of key portfolio risk metrics.

**Algorithm:**
1. Extract `notional` from `factor_analysis` (default 0).
2. Extract `currency` from `factor_analysis` (default `'USD'`).
3. Build `risk_buckets` dict from `factor_analysis.get('riskBuckets', [])`: maps bucket `name` to `value`.
4. Construct DataFrame with columns `Metric` and `Value`, including 6 rows: Notional (formatted as `$X`), Currency, Market Risk, Specific Risk, Sector Risk, Style Risk (each formatted to 4 decimal places).
5. Return DataFrame.

### FactorAnalytics.create_performance_chart(self, performance_data: pd.DataFrame, metric: str = 'cumulativePnl', title: str = "Performance") -> go.Figure
Purpose: Create a time series line chart from performance data.

**Algorithm:**
1. Branch: if `performance_data.empty` -> return figure with annotation "No performance data available".
2. Create `go.Figure()`.
3. Add `go.Scatter` trace:
   a. x-axis: Branch: if `'date'` in `performance_data.columns` -> use `performance_data['date']`; else -> use `performance_data.index`.
   b. y-axis: Branch: if `metric` in `performance_data.columns` -> use `performance_data[metric]`; else -> use `performance_data.iloc[:, 0]`.
4. Update layout with title, axis labels, height=500, hovermode='x unified'.
5. Return figure.

### FactorAnalytics.create_dynamic_performance_chart(self, factor_analysis: Dict, title: str = "Portfolio Performance Metrics") -> go.Figure
Purpose: Create a chart with toggleable cumulative PnL and normalized performance metrics.

**Algorithm:**
1. Extract `timeseries_data = factor_analysis.get('timeseriesData', [])`.
2. Branch: if `not timeseries_data` -> return figure with annotation about missing time series data.
3. Iterate `timeseries_data` to find entry where `item.get('name') == 'total'`; assign to `total_data`.
   - Note: if no `'total'` entry exists, `total_data` remains `None` and next line will raise `AttributeError`.
4. Extract `cumulative_pnl_raw` and `normalized_performance_raw` from `total_data`.
5. Branch: if both are empty -> return figure with annotation.
6. Parse `cumulative_pnl_raw`: for each item, Branch: if `len(item) == 2 and isinstance(item[0], str)` -> extract date and value.
7. Parse `normalized_performance_raw` similarly.
8. Fallback: Branch: if `not cumulative_dates and cumulative_values` (dates empty but values present) -> use range indices.
9. Fallback: Branch: if `not normalized_dates and normalized_values` -> use range indices.
10. Create figure.
11. Branch: if `cumulative_values` -> add Scatter trace (visible=True, blue line).
12. Branch: if `normalized_values` -> add Scatter trace (visible=False, green line).
13. Build dropdown buttons for toggling between the two metrics.
14. Update layout with dropdown menu, annotations, and standard formatting.
15. Return figure.

### FactorAnalytics.create_factor_heatmap_comparison(self, initial_analysis: Dict, hedged_analysis: Dict, title: str = "Style Factor Comparison: Initial vs Hedged") -> go.Figure
Purpose: Create a grouped horizontal bar chart comparing style factor exposures between two analyses.

**Algorithm:**
1. Define inner function `extract_style_factors(analysis)`:
   a. Iterate through `analysis.get('factorExposureBuckets', [])`.
   b. Branch: if `bucket.get('name') == 'Style'` -> return dict of `{sf['name']: sf['value']}`.
   c. If no Style bucket found -> return `{}`.
2. Extract factors from both analyses.
3. Compute `all_factors` as union of both factor name sets.
4. Branch: if `not all_factors` -> return figure with annotation "No factor data available".
5. Sort factors by `abs(initial_factors.get(f, 0))` descending.
6. Build value lists for both analyses (defaulting missing factors to 0).
7. Add two `go.Bar` traces: "Initial Portfolio" (blue, `#4472C4`) and "Hedged Portfolio" (green, `#70AD47`), both horizontal.
8. Compute `chart_height = max(500, len(sorted_factors) * 35 + 150)`.
9. Update layout with grouped bar mode, legend, margins.
10. Add vertical dashed reference line at x=0.
11. Return figure.

## State Mutation
- `self.risk_model_id`, `self.currency`, `self.participation_rate`: Set in `__init__`, never modified by other methods.
- `position_set.resolve()` is called as a side effect in `get_factor_analysis()` when unresolved positions exist -- mutates the passed-in `PositionSet`.
- Thread safety: No shared mutable state; instances can be used from a single thread safely. The `position_set.resolve()` call is not thread-safe if the same `PositionSet` is shared.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `get_factor_analysis` | Position set is empty (no positions) |
| `MqValueError` | `get_factor_analysis` | Position set has no date |
| `MqValueError` | `get_factor_analysis` | No valid positions remain after filtering |
| `MqValueError` | `get_factor_analysis` | API returns "missing in marquee" error with identifiable asset IDs |
| `MqValueError` | `convert_hedge_factor_exposures` | `style_factors` is empty or falsy |
| `Exception` | `get_factor_analysis` | Any other API failure (logged and re-raised) |

## Edge Cases
- `convert_hedge_factor_exposures` has a dead-code second `if not style_factors` check (line 143) that can never execute because the first check (line 140) raises before it.
- `create_dynamic_performance_chart` will raise `AttributeError` if `timeseries_data` is non-empty but contains no item with `name == 'total'`, because `total_data` will be `None` when `.get('cumulativePnl', [])` is called.
- `create_exposure_bar_chart` colors values of exactly 0 as green (>= 0 check).
- `create_style_factor_chart` excludes factors with value exactly 0 from both positive and negative sets.
- `create_dynamic_performance_chart` fallback date logic: `if not cumulative_dates and cumulative_values` -- this is `(not []) and ([...])` which is `True` only when dates list is empty AND values list is non-empty. Uses integer range indices as fallback dates.
- `get_factor_analysis` multiplies `position.weight` by 100 when converting to API format, assuming weight is expressed as a decimal (0.0-1.0) input.
- `get_factor_analysis` error handling regex `r'MA[A-Z0-9]+'` only matches Marquee asset IDs starting with "MA" followed by uppercase letters/digits.
- Visualization methods return `go.Figure` with annotation text rather than raising exceptions when data is missing -- caller must inspect the figure to detect this condition.

## Coverage Notes
- Branch count: ~40
- Key branches in `get_factor_analysis`: empty position_set (2), no date (2), unresolved positions (2), per-position asset_id/quantity/weight (4 paths per position), empty api_positions (2), notional truthy/falsy (2), exception handling missing-in-marquee (2), asset_ids_match (2).
- Key branches in `create_exposure_bar_chart`: empty exposures (2), horizontal vs vertical (2 x 2 for trace and height).
- Key branches in `create_style_factor_chart`: missing key (2), empty style_factors (2), empty selected (2), rows None vs int (2).
- Key branches in `create_performance_chart`: empty data (2), date column presence (2), metric column presence (2).
- Key branches in `create_dynamic_performance_chart`: empty timeseries (2), both raw empty (2), item parsing len/type (2), fallback dates (2 each), cumulative/normalized values truthy (2 each).
- Key branches in `create_factor_heatmap_comparison`: empty all_factors (2).
- Pragmas: none.
