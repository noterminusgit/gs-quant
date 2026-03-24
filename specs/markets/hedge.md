# hedge.py

## Summary
Provides hedge construction and optimization functionality for equity portfolios. Contains data classes for hedge exclusions, constraints, and performance-replication parameters, as well as the core `Hedge` class that orchestrates API-based hedge calculation, result formatting, transaction cost computation, and hyperparameter optimization via grid search.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.hedges` (GsHedgeApi), `gs_quant.errors` (MqValueError), `gs_quant.markets.position_set` (PositionSet), `gs_quant.session` (GsSession), `gs_quant.target.hedge` (HedgeObjective, CorporateActionsTypes)
- External: `datetime`, `logging`, `collections` (defaultdict), `enum` (Enum), `typing` (Union, List, Dict, Optional), `numpy` (np), `pandas` (pd), `dateutil.relativedelta` (relativedelta)

## Type Definitions

### FactorExposureCategory(Enum)
Inherits: Enum

| Value | Raw | Description |
|-------|-----|-------------|
| COUNTRY | `"country"` | Country factor exposure |
| SECTOR | `"sector"` | Sector factor exposure |
| INDUSTRY | `"industry"` | Industry factor exposure |
| STYLE | `"style"` | Style factor exposure |

### ConstraintType(Enum)
Inherits: Enum

| Value | Raw | Description |
|-------|-----|-------------|
| ASSET | `"Asset"` | Individual asset constraint |
| COUNTRY | `"Country"` | Country classification constraint |
| REGION | `"Region"` | Region classification constraint |
| SECTOR | `"Sector"` | Sector classification constraint |
| INDUSTRY | `"Industry"` | Industry classification constraint |
| ESG | `"Esg"` | ESG-related constraint |

### HedgeExclusions (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| assets | `List[str]` | `None` | Asset IDs to exclude from universe |
| countries | `List[str]` | `None` | Countries to exclude |
| regions | `List[str]` | `None` | Regions to exclude |
| sectors | `List[str]` | `None` | Sectors to exclude |
| industries | `List[str]` | `None` | Industries to exclude |

### Constraint (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| constraint_name | `str` | (required) | Name or asset ID of constraint target |
| minimum | `float` | `0` | Minimum weight percentage |
| maximum | `float` | `100` | Maximum weight percentage |
| constraint_type | `Optional[ConstraintType]` | `None` | Type of constraint |

### HedgeConstraints (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| assets | `List[Constraint]` | `None` | Asset-level constraints |
| countries | `List[Constraint]` | `None` | Country classification constraints |
| regions | `List[Constraint]` | `None` | Region classification constraints |
| sectors | `List[Constraint]` | `None` | Sector classification constraints |
| industries | `List[Constraint]` | `None` | Industry classification constraints |
| esg | `List[Constraint]` | `None` | ESG classification constraints |

### PerformanceHedgeParameters (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| initial_portfolio | `PositionSet` | (required) | Target positions to hedge |
| universe | `List[str]` | (required) | Asset identifiers for the hedge universe |
| exclusions | `Optional[HedgeExclusions]` | `None` | Exclusions from universe |
| constraints | `Optional[HedgeConstraints]` | `None` | Constraints on universe |
| observation_start_date | `dt.date` | `None` | Start date for observation window |
| sampling_period | `str` | `"Daily"` | Sampling frequency |
| max_leverage | `float` | `100` | Max pct notional for hedging |
| percentage_in_cash | `Optional[float]` | `None` | Pct of hedge notional in cash |
| explode_universe | `bool` | `True` | Decompose universe into underliers |
| exclude_target_assets | `bool` | `True` | Exclude target assets from hedge |
| exclude_corporate_actions_types | `Optional[List[Union[CorporateActionsTypes, str]]]` | `None` | Corporate actions to exclude |
| exclude_hard_to_borrow_assets | `bool` | `False` | Exclude hard-to-borrow assets |
| exclude_restricted_assets | `bool` | `False` | Exclude restricted assets |
| max_adv_percentage | `float` | `15` | Max pct of ADV |
| max_return_deviation | `float` | `5` | Max annualized return deviation |
| max_weight | `float` | `100` | Max constituent weight |
| min_market_cap | `Optional[float]` | `None` | Minimum market cap filter |
| max_market_cap | `Optional[float]` | `None` | Maximum market cap filter |
| market_participation_rate | `float` | `10` | Max market participation rate |
| lasso_weight | `float` | `0` | Lasso hyperparameter for ML hedges |
| ridge_weight | `float` | `0` | Ridge hyperparameter for ML hedges |
| benchmarks | `List[str]` | `None` | Benchmark asset IDs |

### Hedge (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| parameters | `PerformanceHedgeParameters` (or similar) | (required) | Hedge calculation parameters |
| objective | `HedgeObjective` | (required) | Hedge objective enum value |
| result | `Dict` | `{}` | Calculation results (read-only property) |

### PerformanceHedge (class)
Inherits: Hedge

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| parameters | `PerformanceHedgeParameters` | `None` | Performance hedge parameters |

Constructs with `objective = HedgeObjective.Replicate_Performance`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### HedgeExclusions.to_dict(self) -> dict
Purpose: Serialize exclusions to API-compatible dictionary format.

**Algorithm:**
1. Initialize empty `response` dict and `all_constraints` list
2. Branch: if `self.countries` is truthy -> append country exclusions via `_get_exclusions`
3. Branch: if `self.regions` is truthy -> append region exclusions
4. Branch: if `self.sectors` is truthy -> append sector exclusions
5. Branch: if `self.industries` is truthy -> append industry exclusions
6. Branch: if `len(all_constraints) > 0` -> set `response['classificationConstraints']`
7. Branch: if `self.assets` is truthy -> set `response['assetConstraints']` via `_get_exclusions`
8. Return response

### HedgeExclusions._get_exclusions(exclusions_list: List, constraint_type: ConstraintType) -> list  [staticmethod]
Purpose: Convert a list of exclusion names into Constraint dicts with min=0, max=0.

**Algorithm:**
1. For each exclusion in the list, create a `Constraint(constraint_name=exclusion, constraint_type=constraint_type, minimum=0, maximum=0)` and call `.to_dict()`
2. Return list of dicts

### Constraint.from_dict(cls, as_dict: Dict) -> Constraint  [classmethod]
Purpose: Deserialize a Constraint from a dictionary.

**Algorithm:**
1. Branch: if `as_dict.get('type')` is not None -> `constraint_type = ConstraintType(as_dict.get('type'))`
2. Branch: elif `as_dict.get('assetId')` is not None -> `constraint_type = ConstraintType.ASSET`
3. Branch: else -> `constraint_type = ConstraintType.ESG`
4. Return Constraint with `constraint_name = as_dict.get('name') or as_dict.get('assetId')`, min/max from dict

### Constraint.to_dict(self) -> dict
Purpose: Serialize a Constraint to API-compatible dictionary.

**Algorithm:**
1. Build base dict: `{'name': self.constraint_name, 'min': self.minimum, 'max': self.maximum}`
2. Branch: if constraint_type is not ESG and not ASSET -> add `'type': self.constraint_type.value`
3. Branch: if constraint_type is ASSET -> replace `'name'` key with `'assetId'` key
4. Return dict

### HedgeConstraints.__init__(self, assets, countries, regions, sectors, industries, esg)
Purpose: Initialize constraints, tagging each Constraint with the appropriate ConstraintType.

**Algorithm:**
1. For each constraint list (assets, countries, regions, sectors, industries, esg), if not None, iterate and set `constraint_type` on each Constraint object
2. Store all lists as private fields

### HedgeConstraints.to_dict(self) -> dict
Purpose: Serialize all constraints to API-compatible dictionary.

**Algorithm:**
1. Collect classification constraints from countries, regions, sectors, industries
2. Branch: if `len(classification_constraints) > 0` -> add `'classificationConstraints'`
3. Collect ESG constraints
4. Branch: if `len(esg_constraints) > 0` -> add `'esgConstraints'`
5. Collect asset constraints
6. Branch: if `len(asset_constraints) > 0` -> add `'assetConstraints'`
7. Return dict

### PerformanceHedgeParameters.to_dict(self, resolved_identifiers) -> dict
Purpose: Build the full API request payload for the performance hedge calculation.

**Algorithm:**
1. Build positions list from `initial_portfolio.positions`, conditionally including `quantity` and `weight`
2. Build pricing payload with positions and parameters (currency=USD, useUnadjustedClosePrice=True, etc.)
3. Branch: if `initial_portfolio.reference_notional` -> add `targetNotional` and `weightingStrategy="Weight"`
4. POST to `/price/positions` API endpoint via `GsSession.current.sync.post`
5. Branch: if exception -> raise `MqValueError`
6. Branch: if `'errorMessage'` in response -> raise `MqValueError`
7. Branch: if `reference_notional is None` -> set from `actualNotional` in response
8. Extract positions with assetId/quantity from response
9. Resolve universe identifiers using `resolved_identifiers` map
10. Branch: if `self.benchmarks is not None` -> resolve benchmark identifiers
11. Branch: if `self.exclusions is not None` and `self.exclusions.assets is not None` -> resolve exclusion asset IDs
12. Branch: if `self.constraints is not None` and `self.constraints.assets is not None` -> resolve constraint asset names
13. Set `observation_start_date` to provided value or `hedge_date - 1 year`
14. Build main dict with all parameters
15. Merge exclusions and constraints dicts for `classificationConstraints` and `assetConstraints`
16. Branch: if `'esgConstraints'` in constraints_as_dict -> add ESG constraints
17. Branch: if `percentage_in_cash is not None` -> add to dict
18. Branch: if `exclude_corporate_actions_types` -> add `.value` for each
19. Branch: if `min_market_cap is not None` -> add
20. Branch: if `max_market_cap is not None` -> add
21. Branch: if `benchmarks is not None` and `len(benchmarks) > 0` -> add
22. Return dict

**Raises:** `MqValueError` when position pricing fails or returns an error message

**Note:** Line 634 has a likely bug: `excludeRestrictedAssets` is set to `self.exclude_hard_to_borrow_assets` instead of `self.exclude_restricted_assets`.

### PerformanceHedgeParameters.resolve_identifiers_in_payload(self, hedge_date) -> tuple
Purpose: Resolve all identifiers (universe, exclusion assets, benchmarks, constraint assets) via GsAssetApi.

**Algorithm:**
1. Collect all identifiers from universe
2. Branch: if exclusions and exclusions.assets -> add exclusion asset IDs
3. Branch: if benchmarks -> add benchmark IDs
4. Branch: if constraints and constraints.assets -> add constraint asset names
5. Call `GsAssetApi.resolve_assets(identifier=identifiers, fields=['id'], as_of=hedge_date)`
6. Return resolver result

### Hedge.__init__(self, parameters, objective: HedgeObjective)
Purpose: Initialize Hedge with parameters, objective, and empty result dict.

### Hedge.calculate(self) -> Dict
Purpose: Execute the hedge calculation via API and format results.

**Algorithm:**
1. Call `parameters.resolve_identifiers_in_payload` with portfolio date
2. Call `parameters.to_dict(resolved_identifiers)` to build payload
3. Call `GsHedgeApi.calculate_hedge` with objective and parameters
4. Branch: if `'errorMessage'` in results and `'result'` not in results -> raise `MqValueError`
5. Format results via `_format_hedge_calculate_results`
6. Enhance with benchmark curves via `_enhance_result_with_benchmark_curves`
7. Store in `self.__result` and return

**Raises:** `MqValueError` when hedge calculation returns an error

### Hedge.get_constituents(self) -> pd.DataFrame
Purpose: Extract hedge constituents from result and format as DataFrame.

**Algorithm:**
1. Get constituents list from `self.result['Hedge']['Constituents']`
2. For each row, capitalize and space-separate camelCase keys
3. Return DataFrame

### Hedge.get_statistics(self) -> pd.DataFrame
Purpose: Extract all float-valued statistics from result into a DataFrame.

**Algorithm:**
1. Initialize dict with keys 'Portfolio', 'Hedge', 'Hedged Portfolio'
2. For each key/inner_key pair, Branch: if value is float -> include
3. Return DataFrame

### Hedge.get_backtest_performance(self) -> pd.DataFrame
Purpose: Get backtest performance timeseries.

**Algorithm:**
1. Delegate to `_get_timeseries('Backtest Performance')`

### Hedge.get_backtest_correlation(self) -> pd.DataFrame
Purpose: Get backtest correlation timeseries.

**Algorithm:**
1. Delegate to `_get_timeseries('Backtest Correlation')`

### Hedge._get_timeseries(self, timeseries_name: str) -> pd.DataFrame
Purpose: Generic timeseries extractor from hedge results.

**Algorithm:**
1. For each top-level key in result, Branch: if `timeseries_name` exists in that sub-dict -> extract
2. For each data point `[date, value]`, merge into `results` dict keyed by date
3. Branch: if date already exists in results -> add value under current key
4. Branch: else -> create new entry with Date and key
5. Return DataFrame indexed by 'Date'

### Hedge._format_hedge_calculate_results(calculation_results) -> dict  [staticmethod]
Purpose: Rename raw API result keys to human-readable names.

**Algorithm:**
1. Map `target` -> `Portfolio`, `hedge` -> `Hedge`, `hedgedTarget` -> `Hedged Portfolio`
2. For each, call `format_dictionary_key_to_readable_format`
3. Return formatted dict

### Hedge._enhance_result_with_benchmark_curves(formatted_results, benchmark_results, resolver) -> dict  [staticmethod]
Purpose: Add benchmark performance curves to formatted results.

**Algorithm:**
1. Build reverse map: asset_id -> provided_identifier from resolver
2. Branch: if `len(benchmark_results) > 0` -> for each benchmark, add formatted entry keyed by original identifier
3. Return enhanced results

### Hedge.format_dictionary_key_to_readable_format(renamed_results) -> dict  [staticmethod]
Purpose: Convert camelCase dictionary keys to "Capitalized Spaced" format.

**Algorithm:**
1. For each key, capitalize first char and insert space before each uppercase letter
2. Return new dict with transformed keys

### Hedge.find_optimal_hedge(hedge_query: dict, hyperparams: dict, metric: str) -> Union[dict, float]  [staticmethod]
Purpose: Grid search over Concentration/Diversity hyperparameters to find optimal hedge by metric.

**Algorithm:**
1. Get optimization type (minimize/maximize) from `create_optimization_mappings()`
2. Build cartesian product of `hyperparams['Concentration']` x `hyperparams['Diversity']`
3. For each pair, set `lasso_weight` and `ridge_weight` on parameters
4. Call `GsHedgeApi.calculate_hedge` and record result keyed by metric value
5. Branch: if optimization_type == 'minimize' -> take `min(keys)`; else -> take `max(keys)`
6. Return (optimized_hedge, optimized_metric, optimized_hyperparams)

### Hedge.create_optimization_mappings() -> dict  [staticmethod]
Purpose: Return mapping of metric names to optimization direction.

**Algorithm:**
1. Return dict: `rSquared` -> maximize, `correlation` -> maximize, `holdingError` -> minimize, `trackingError` -> minimize, `transactionCost` -> minimize, `annualizedReturn` -> maximize

### Hedge.construct_portfolio_weights_and_asset_numbers(results: dict) -> Union[dict, List]  [staticmethod]
Purpose: Extract and sort portfolio constituents, return weights and asset indices.

**Algorithm:**
1. Get constituents from `results["result"]["hedge"]["constituents"]`
2. Sort by weight descending
3. Extract weights list and generate asset_numbers as `range(len(portfolio))`
4. Return (portfolio, weights, asset_numbers)

### Hedge.asset_id_diffs(portfolio_asset_ids, thomson_reuters_asset_ids) -> list  [staticmethod]
Purpose: Find portfolio assets not present in Thomson Reuters data.

**Algorithm:**
1. Return `list(set(portfolio_asset_ids) - set(thomson_reuters_asset_ids))`

### Hedge.create_transaction_cost_data_structures(portfolio_asset_ids, portfolio_quantities, thomson_reuters_eod_data, backtest_dates) -> tuple  [staticmethod]
Purpose: Build data structures for transaction cost computation.

**Algorithm:**
1. Get Thomson Reuters asset IDs for backtest end date
2. Find diffs (missing assets) and remove them from `portfolio_asset_ids` (mutates input list)
3. Build `id_quantity_map`: asset_id -> quantity (excluding diffs)
4. Build `id_prices_map`: asset_id -> list of close prices over all backtest dates
5. Compute notionals per asset per day: `abs(prices * quantity)`
6. Build `id_to_notional_map`: asset_id -> list of daily notionals
7. Sum across assets to get `total_notionals_each_day`
8. Build `id_to_weight_map`: asset_id -> list of daily weights (notional / total_notional)
9. Return (id_quantity_map, id_prices_map, id_to_notional_map, id_to_weight_map)

### Hedge.t_cost(basis_points, notional_traded) -> float  [staticmethod]
Purpose: Compute transaction cost from basis points and notional traded.

**Algorithm:**
1. Return `(basis_points * 1e-4) * notional_traded`

### Hedge.compute_notional_traded(notional_on_the_day, prev_weight, curr_weight) -> float  [staticmethod]
Purpose: Compute notional amount of an asset traded on a given day.

**Algorithm:**
1. Return `sum([np.abs(curr_weight - prev_weight) * notional_on_the_day])`

### Hedge.compute_tcosts(basis_points, asset_weights, asset_notionals, backtest_dates, portfolio_asset_ids) -> pd.Series  [staticmethod]
Purpose: Compute cumulative transaction costs for portfolio rebalancing.

**Algorithm:**
1. For each date/index in backtest_dates:
   a. For each asset_id, get prev_weights (use index 0 if first day, else idx-1)
   b. Compute notional traded and transaction cost
   c. Sum costs for the day
2. Append `abs(tcost_today)` for each day
3. Return `pd.Series(np.cumsum(tcosts_each_day))`

### PerformanceHedge.__init__(self, parameters: PerformanceHedgeParameters = None, **kwargs)
Purpose: Initialize with Replicate_Performance objective.

**Algorithm:**
1. Call `super().__init__(parameters, HedgeObjective.Replicate_Performance)`

## State Mutation
- `self.__result`: Set to `{}` in `__init__`, updated by `calculate()`
- `self.parameters.initial_portfolio.reference_notional`: Mutated in `to_dict()` when originally `None` (set from API response `actualNotional`)
- `self.parameters.universe`: Mutated in `to_dict()` - resolved from identifiers to asset IDs
- `self.parameters.benchmarks`: Mutated in `to_dict()` - resolved from identifiers to asset IDs
- `self.parameters.exclusions.assets`: Mutated in `to_dict()` - resolved from identifiers to asset IDs
- `self.parameters.constraints.assets[*].constraint_name`: Mutated in `to_dict()` - resolved to asset IDs
- `portfolio_asset_ids` (input list): Mutated in `create_transaction_cost_data_structures()` via `list.remove()`
- Thread safety: No concurrent access patterns; all operations are synchronous

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `PerformanceHedgeParameters.to_dict` | When position pricing POST fails or returns `errorMessage` |
| `MqValueError` | `Hedge.calculate` | When `errorMessage` in result and `result` key not present |

## Edge Cases
- `to_dict` mutates `self.universe`, `self.benchmarks`, and exclusion/constraint assets in place; calling it twice would double-resolve
- `observation_start_date` defaults to `hedge_date - 1 year` using `relativedelta(years=1)` if not provided
- `resolved_identifiers.get(asset, [{'id': asset}])` falls back to the original identifier if resolution fails
- `create_transaction_cost_data_structures` mutates the `portfolio_asset_ids` list passed in (removes diffs)
- `find_optimal_hedge` will overwrite `hedge_query['parameters']` lasso/ridge weights in place, affecting the caller
- If multiple hedges produce the same metric value, `min()`/`max()` on dict keys picks one arbitrarily

## Bugs Found
- Line 634: `excludeRestrictedAssets` is set to `self.exclude_hard_to_borrow_assets` instead of `self.exclude_restricted_assets` (OPEN)
- Line 63 (`PricingCache.clear`): Local variable `__cache` is assigned but `cls.__cache` is never reset, so `clear()` is a no-op (OPEN - in core.py, noted for cross-reference)

## Coverage Notes
- Branch count: ~48
- Key branches: `to_dict` has ~15 conditional branches for optional fields; `from_dict` has 3-way type dispatch; `find_optimal_hedge` has minimize/maximize branch
- `_get_timeseries` has date-exists/not-exists branch
- `create_transaction_cost_data_structures` has diff-removal loop that mutates input
