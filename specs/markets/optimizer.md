# optimizer.py

## Summary
Portfolio optimization module that provides a comprehensive framework for constructing factor-hedged portfolios using the Axioma Portfolio Optimizer via the GS Marquee API. The module defines constraint types (asset, country, sector, industry, factor), objective functions (minimize factor risk), optimizer settings (unidirectional/bidirectional hedging), and a strategy orchestrator (`OptimizerStrategy`) that assembles all inputs into an API payload, executes the optimization, and exposes result-extraction methods for PnL, exposures, risk buckets, and performance summaries.

## Dependencies
- Internal: `gs_quant.api.gs.hedges` (`GsHedgeApi`), `gs_quant.api.gs.assets` (`GsAssetApi`), `gs_quant.errors` (`MqValueError`), `gs_quant.markets.factor` (`Factor`), `gs_quant.markets.position_set` (`PositionSet`, `Position`), `gs_quant.markets.securities` (`Asset`), `gs_quant.models.risk_model` (`FactorRiskModel`), `gs_quant.session` (`GsSession`), `gs_quant.target.hedge` (`CorporateActionsTypes`)
- External: `logging`, `enum` (`Enum`), `functools` (`wraps`), `typing` (`List`, `Dict`, `Optional`, `Union`, `Final`), `dateutil.relativedelta` (`relativedelta`), `pandas` (as `pd`), `numpy` (as `np`), `math`, `datetime` (as `dt`)

## Type Definitions

### resolve_assets_in_batches (module-level function)
See Functions/Methods section.

### OptimizationConstraintUnit (Enum)
See Enums section.

### HedgeTarget (Enum)
See Enums section.

### OptimizerObjective (Enum)
See Enums section.

### OptimizerRiskType (Enum)
See Enums section.

### OptimizerObjectiveTerm (class)
Inherits: `object`

Represents a single term in the optimizer objective function with risk weighting parameters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__weight` | `float` | `1` | Weight multiplier for this objective term |
| `__params` | `Dict[str, float]` | `DEFAULT_RISK_PARAMS` merged with input | Risk parameters dict containing `factor_weight`, `specific_weight`, `risk_type` |

Class constant:

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `DEFAULT_RISK_PARAMS` | `Final[Dict]` | `{'factor_weight': 1, 'specific_weight': 1, 'risk_type': OptimizerRiskType.VARIANCE}` | Default risk parameters merged into every term |

### OptimizerObjectiveParameters (class)
Inherits: `object`

Wraps a single optimizer objective and its list of terms.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__objective` | `OptimizerObjective` | `OptimizerObjective.MINIMIZE_FACTOR_RISK` | The optimization objective type |
| `__terms` | `List[OptimizerObjectiveTerm]` | `[OptimizerObjectiveTerm.DEFAULT_RISK_PARAMS]` | List of objective terms (currently must be exactly 1) |

### OptimizerType (Enum)
See Enums section.

### PrioritySetting (Enum)
See Enums section.

### TurnoverNotionalType (Enum)
See Enums section.

### AssetUniverse (class)
Inherits: `object`

Resolves human-readable identifiers to internal Marquee asset IDs.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__identifiers` | `List[str]` | (required) | Human-readable asset identifiers (e.g., tickers) |
| `__as_of_date` | `dt.date` | `dt.date.today()` | Date as of which to resolve identifiers |
| `__asset_ids` | `List[str]` | `None` | Resolved internal Marquee asset IDs; populated by `resolve()` |

### AssetConstraint (class)
Inherits: `object`

Constrains weight/notional for a single asset in the optimization.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__asset` | `Union[Asset, str]` | (required) | The asset object or its string asset ID |
| `__minimum` | `float` | `0` | Minimum weight/notional for this asset |
| `__maximum` | `float` | `100` | Maximum weight/notional for this asset |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit of the min/max values |

### CountryConstraint (class)
Inherits: `object`

Constrains the notional held in a particular country.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__country_name` | `str` | (required) | Country name |
| `__minimum` | `float` | `0` | Minimum allocation |
| `__maximum` | `float` | `100` | Maximum allocation |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit (PERCENT or DECIMAL only; NOTIONAL raises) |

### SectorConstraint (class)
Inherits: `object`

Constrains the notional held in a particular GICS Sector.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__sector_name` | `str` | (required) | GICS Sector name |
| `__minimum` | `float` | `0` | Minimum allocation |
| `__maximum` | `float` | `100` | Maximum allocation |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit (PERCENT or DECIMAL only) |

### IndustryConstraint (class)
Inherits: `object`

Constrains the notional held in a particular GICS Industry.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__industry_name` | `str` | (required) | GICS Industry name |
| `__minimum` | `float` | `0` | Minimum allocation |
| `__maximum` | `float` | `100` | Maximum allocation |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit (PERCENT or DECIMAL only) |

### FactorConstraint (class)
Inherits: `object`

Constrains a factor by a maximum exposure.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__factor` | `Factor` | (required) | The factor to constrain |
| `__max_exposure` | `float` | (required) | Maximum exposure to the factor |

### OptimizerUniverse (class)
Inherits: `object`

Defines the universe of assets from which the optimizer can select positions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__assets` | `Union[List[Asset], AssetUniverse]` | `None` | Assets in the universe |
| `__explode_composites` | `bool` | `True` | Expand composites to include constituents |
| `__exclude_initial_position_set_assets` | `bool` | `True` | Exclude assets in initial holdings |
| `__exclude_corporate_actions_types` | `List[CorporateActionsTypes]` | `[]` | Corporate action types to exclude |
| `__exclude_hard_to_borrow_assets` | `bool` | `False` | Exclude hard-to-borrow assets (>=200 bps) |
| `__exclude_restricted_assets` | `bool` | `False` | Exclude restricted assets |
| `__min_market_cap` | `float` | `None` | Minimum market cap filter |
| `__max_market_cap` | `float` | `None` | Maximum market cap filter |

### MaxFactorProportionOfRiskConstraint (class)
Inherits: `object`

Constrains the maximum proportion of risk attributable to any single factor.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__max_factor_proportion_of_risk` | `float` | (required, normalized) | Max factor MCTR; stored as decimal (divided by 100 if input is PERCENT) |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit of the input value |

### MaxProportionOfRiskByGroupConstraint (class)
Inherits: `object`

Constrains the maximum proportion of risk from a group of factors.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__factors` | `List[Factor]` | (required) | Group of factors to constrain together |
| `__max_factor_proportion_of_risk` | `float` | (required, normalized) | Max proportion of risk; stored as decimal |
| `__unit` | `OptimizationConstraintUnit` | `OptimizationConstraintUnit.PERCENT` | Unit of the input value |

### OptimizerConstraints (class)
Inherits: `object`

Aggregates all constraint types into a single container for the optimizer.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__asset_constraints` | `List[AssetConstraint]` | `[]` | Per-asset weight/notional constraints |
| `__country_constraints` | `List[CountryConstraint]` | `[]` | Country allocation constraints |
| `__sector_constraints` | `List[SectorConstraint]` | `[]` | GICS Sector constraints |
| `__industry_constraints` | `List[IndustryConstraint]` | `[]` | GICS Industry constraints |
| `__factor_constraints` | `List[FactorConstraint]` | `[]` | Factor exposure constraints |
| `__max_factor_proportion_of_risk` | `MaxFactorProportionOfRiskConstraint` | `None` | Single-factor max MCTR |
| `__max_proportion_of_risk_by_groups` | `List[MaxProportionOfRiskByGroupConstraint]` | `None` | Group-factor max MCTR |

### ConstraintPriorities (class)
Inherits: `object`

Priority levels (0-5) for relaxable classification constraints. Priority 0 is hard (optimization fails if unmet); 1-5 are soft/relaxed (lower number = higher priority).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__min_sector_weights` | `PrioritySetting` | `None` | Priority for min sector weight constraints |
| `__max_sector_weights` | `PrioritySetting` | `None` | Priority for max sector weight constraints |
| `__min_industry_weights` | `PrioritySetting` | `None` | Priority for min industry weight constraints |
| `__max_industry_weights` | `PrioritySetting` | `None` | Priority for max industry weight constraints |
| `__min_region_weights` | `PrioritySetting` | `None` | Priority for min region weight constraints |
| `__max_region_weights` | `PrioritySetting` | `None` | Priority for max region weight constraints |
| `__min_country_weights` | `PrioritySetting` | `None` | Priority for min country weight constraints |
| `__max_country_weights` | `PrioritySetting` | `None` | Priority for max country weight constraints |
| `__style_factor_exposures` | `PrioritySetting` | `None` | Priority for style factor exposure constraints |

### OptimizerSettings (class)
Inherits: `object`

Settings for the optimizer, covering unidirectional (short-only) and bidirectional (long+short) hedger modes.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__notional` | `float` | `10000000` | Hedge notional (used in unidirectional mode or as fallback in bidirectional) |
| `__allow_long_short` | `bool` | `False` | Enable bidirectional hedging (both long and short positions) |
| `__gross_notional` | `float` | `None` | Total absolute notional for bidirectional mode |
| `__net_notional` | `float` | `None` | Net notional for bidirectional mode (0 = market neutral) |
| `__min_names` | `float` | `0` | Minimum number of assets in hedge |
| `__max_names` | `float` | `100` | Maximum number of assets in hedge |
| `__min_weight_per_constituent` | `float` | `None` | Min absolute weight per constituent (decimal) |
| `__max_weight_per_constituent` | `float` | `None` | Max absolute weight per constituent (decimal) |
| `__max_adv` | `float` | `15` | Max percentage of ADV that can be traded per constituent |
| `__constraint_priorities` | `ConstraintPriorities` | `None` | Priority settings for classification constraints |

### TurnoverConstraint (class)
Inherits: `object`

Specifies turnover limits relative to a reference portfolio.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__turnover_portfolio` | `PositionSet` | (required) | Reference portfolio for turnover measurement |
| `__max_turnover_percent` | `float` | (required) | Max turnover percentage (e.g., 80 = 20% minimum overlap) |
| `__turnover_notional_type` | `Optional[TurnoverNotionalType]` | `None` | Notional type for turnover calculation (Net, Long, Gross) |

### OptimizerStrategy (class)
Inherits: `object`

The main orchestrator class that assembles all inputs, runs the optimization via the Marquee API, and provides result-extraction methods.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__initial_position_set` | `PositionSet` | (required) | Original holdings as of a specific date |
| `__universe` | `OptimizerUniverse` | (required) | Universe of assets for the optimization |
| `__risk_model` | `FactorRiskModel` | (required) | Risk model for risk calculation |
| `__constraints` | `OptimizerConstraints` | `None` | Optimization constraints |
| `__turnover` | `TurnoverConstraint` | `None` | Turnover constraint |
| `__settings` | `OptimizerSettings` | `None` | Optimizer settings |
| `__objective` | `OptimizerObjective` | `OptimizerObjective.MINIMIZE_FACTOR_RISK` | Optimization objective |
| `__result` | `Dict` | `None` | Stores raw API result after `run()` |
| `__objective_parameters` | `OptimizerObjectiveParameters` | `None` | Additional objective parameters |

Class constant:

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `VERBOSE_ERROR_MSG` | `Final[Dict]` | dict mapping error prefixes to lambda formatters | Maps known error message prefixes to user-friendly verbose messages |

## Enums and Constants

### OptimizationConstraintUnit(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `DECIMAL` | `'Decimal'` | Values expressed as decimals (0.0-1.0); converted to percent (*100) in to_dict |
| `NOTIONAL` | `'Notional'` | Values expressed as absolute notional amounts |
| `PERCENT` | `'Percent'` | Values expressed as percentages (0-100); used as-is |

### HedgeTarget(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `HEDGED_TARGET` | `"hedgedTarget"` | Combined hedged+target portfolio result key |
| `HEDGE` | `"hedge"` | Hedge-only result key |
| `TARGET` | `"target"` | Target (initial) portfolio result key |

### OptimizerObjective(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `MINIMIZE_FACTOR_RISK` | `'Minimize Factor Risk'` | Objective to minimize factor risk in the portfolio |

### OptimizerRiskType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `VARIANCE` | `'Variance'` | Risk is measured by variance |

### OptimizerType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `AXIOMA_PORTFOLIO_OPTIMIZER` | `'Axioma Portfolio Optimizer'` | Axioma-based portfolio optimizer backend |

### PrioritySetting(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `ZERO` | `'0'` | Highest priority (hard constraint -- optimization fails if unmet) |
| `ONE` | `'1'` | Relaxed priority level 1 |
| `TWO` | `'2'` | Relaxed priority level 2 |
| `THREE` | `'3'` | Relaxed priority level 3 |
| `FOUR` | `'4'` | Relaxed priority level 4 |
| `FIVE` | `'5'` | Lowest priority (most relaxed) |

### TurnoverNotionalType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `NET` | `'Net'` | Net notional turnover |
| `LONG` | `'Long'` | Long-only notional turnover |
| `GROSS` | `'Gross'` | Gross notional turnover |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### resolve_assets_in_batches(identifiers: List[str], fields: List[str] = None, as_of_date: dt.date = dt.date.today(), batch_size: int = 100, **kwargs) -> List[Dict]
Purpose: Resolve human-readable asset identifiers to Marquee asset records in batches to avoid API payload limits.

**Algorithm:**
1. Build `all_fields` = `["id", "name", "bbid"]`; if `fields` is provided, append them.
2. Branch: if `len(identifiers) > batch_size` -> split identifiers into sub-arrays using `np.array_split` with `math.ceil(len(identifiers) / batch_size)` chunks.
3. Branch: else -> wrap identifiers in a single-element list.
4. For each batch, call `GsAssetApi.resolve_assets(identifier=list(batch), as_of=datetime_combined, fields=all_fields, limit=1, **kwargs)`.
5. Merge each batch result into `all_assets_resolved` dict.
6. For each identifier in `all_assets_resolved`: Branch: if the value is truthy (non-empty list) -> append `{"identifier": identifier, **first_record}` to output.
7. Return the list of dicts.

---

### OptimizerObjectiveTerm.__init__(self, weight: float = 1, params: Dict[str, float] = DEFAULT_RISK_PARAMS)
Purpose: Initialize an objective term with weight and risk parameters.

**Algorithm:**
1. Set `__weight = weight`.
2. Merge `DEFAULT_RISK_PARAMS` with provided `params` (input overrides defaults): `__params = {**DEFAULT_RISK_PARAMS, **params}`.

### OptimizerObjectiveTerm.params (property getter) -> Dict
Purpose: Return the risk parameters dict.

### OptimizerObjectiveTerm.params (property setter, params: Dict[str, float])
Purpose: Set risk parameters, merging with defaults.

**Algorithm:**
1. `__params = {**DEFAULT_RISK_PARAMS, **params}` (defaults are always the base).

### OptimizerObjectiveTerm.weight (property getter) -> float
Purpose: Return the weight.

### OptimizerObjectiveTerm.weight (property setter, weight: float)
Purpose: Set the weight.

### OptimizerObjectiveTerm.to_dict(self) -> Dict
Purpose: Convert to API payload format.

**Algorithm:**
1. Return dict with keys `factorWeight`, `specificWeight`, `riskType` (enum `.value`), `weight`.

---

### OptimizerObjectiveParameters.__init__(self, objective: OptimizerObjective = MINIMIZE_FACTOR_RISK, terms: List[OptimizerObjectiveTerm] = [DEFAULT_RISK_PARAMS])
Purpose: Initialize objective parameters with an objective type and terms list.

**Algorithm:**
1. Set `__objective` and `__terms`.

### OptimizerObjectiveParameters.objective (property getter/setter)
Purpose: Get/set the objective type.

### OptimizerObjectiveParameters.terms (property getter/setter)
Purpose: Get/set the terms list.

### OptimizerObjectiveParameters.to_dict(self)
Purpose: Serialize to API payload.

**Algorithm:**
1. Branch: if `len(self.__terms) != 1` -> raise `MqValueError('Only single risk term is supported')`.
2. Return `{'parameters': self.__terms[0].to_dict()}`.

**Raises:** `MqValueError` when terms list length is not exactly 1.

---

### AssetUniverse.__init__(self, identifiers: List[str], asset_ids: List[str] = None, as_of_date: dt.date = dt.date.today())
Purpose: Create an asset universe from identifiers that can be resolved to asset IDs.

### AssetUniverse.identifiers (property getter/setter)
### AssetUniverse.asset_ids (property getter/setter)
### AssetUniverse.as_of_date (property getter/setter)

### AssetUniverse.resolve(self)
Purpose: Resolve identifiers to internal Marquee asset IDs if not already resolved.

**Algorithm:**
1. Branch: if `self.__asset_ids` is falsy (None or empty) ->
   a. Call `resolve_assets_in_batches(identifiers=self.identifiers, as_of_date=self.as_of_date, batch_size=250)`.
   b. Convert result to DataFrame, set index to `"identifier"`, reindex by `self.identifiers` to preserve order.
   c. Set `self.asset_ids` to the `'id'` column values as a list.
2. Branch: if `self.__asset_ids` is truthy -> do nothing (already resolved).

---

### AssetConstraint.__init__(self, asset: Union[Asset, str], minimum: float = 0, maximum: float = 100, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a constraint on a single asset's allocation.

### AssetConstraint.asset (property getter/setter) -> Union[Asset, str]
### AssetConstraint.minimum (property getter/setter) -> float
### AssetConstraint.maximum (property getter/setter) -> float
### AssetConstraint.unit (property getter/setter) -> OptimizationConstraintUnit

### AssetConstraint.to_dict(self)
Purpose: Serialize to API format with unit conversion.

**Algorithm:**
1. `assetId`: Branch: if `self.asset` is `str` -> use it directly; else -> call `self.asset.get_marquee_id()`.
2. `min`: Branch: if unit is `DECIMAL` -> multiply by 100; else -> use as-is.
3. `max`: Branch: if unit is `DECIMAL` -> multiply by 100; else -> use as-is.
4. Return dict with keys `assetId`, `min`, `max`.

### AssetConstraint.build_many_constraints(cls, asset_constraints: Union[pd.DataFrame, List[Dict]], as_of_date: dt.date = dt.date.today(), fail_on_unresolved_positions: bool = True, **kwargs)
Purpose: Batch-create `AssetConstraint` objects from a DataFrame or list of dicts.

**Algorithm:**
1. Branch: if `asset_constraints` is a `list` -> convert to DataFrame; else -> use as-is.
2. Check for missing required columns `['identifier', 'minimum', 'maximum', 'unit']`.
3. Branch: if any columns missing -> raise `MqValueError` listing them.
4. Branch: if more than one unique `unit` value -> raise `MqValueError('All asset constraints must be in the same unit')`.
5. Branch: if `'assetId'` not in columns ->
   a. Extract identifiers, call `resolve_assets_in_batches(identifiers, as_of_date, batch_size=250, **kwargs)`.
   b. Left-merge resolved data onto constraints DataFrame.
   c. Branch: if `fail_on_unresolved_positions` is True AND any `'id'` values are NaN -> raise `MqValueError` listing unresolved identifiers.
   d. Branch: else (fail_on_unresolved_positions is False or no NaN) -> filter out rows with NaN `'id'`.
   e. Rename `'id'` to `'assetId'`, select relevant columns.
6. Convert DataFrame to list of records.
7. Return list of `AssetConstraint(asset=row['assetId'], minimum=row['minimum'], maximum=row['maximum'], unit=OptimizationConstraintUnit(row['unit']))`.

**Raises:** `MqValueError` when missing columns, multiple units, or unresolved identifiers with `fail_on_unresolved_positions=True`.

---

### CountryConstraint.__init__(self, country_name: str, minimum: float = 0, maximum: float = 100, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a country allocation constraint.

**Algorithm:**
1. Branch: if `unit` not in `[PERCENT, DECIMAL]` -> raise `MqValueError('Country constraints can only be set by percent or decimal.')`.
2. Store fields.

### CountryConstraint.country_name (property getter/setter)
### CountryConstraint.minimum (property getter/setter)
### CountryConstraint.maximum (property getter/setter)

### CountryConstraint.unit (property setter, value: OptimizationConstraintUnit)
Purpose: Set unit with validation.

**Algorithm:**
1. Branch: if `value` not in `[PERCENT, DECIMAL]` -> raise `MqValueError`.
2. Set `__unit = value`.

### CountryConstraint.to_dict(self)
Purpose: Serialize with unit conversion.

**Algorithm:**
1. Return dict with `type='Country'`, `name=self.country_name`.
2. `min`: Branch: if DECIMAL -> multiply by 100; else -> as-is.
3. `max`: Branch: if DECIMAL -> multiply by 100; else -> as-is.

### CountryConstraint.build_many_constraints(cls, country_constraints: Union[pd.DataFrame, List[Dict]])
Purpose: Batch-create `CountryConstraint` objects.

**Algorithm:**
1. Branch: if input is `list` -> convert to DataFrame; else -> use as-is.
2. Check for missing required columns `['country', 'minimum', 'maximum', 'unit']`.
3. Branch: if missing columns -> raise `MqValueError`.
4. Convert to records, return list of `CountryConstraint(country_name=row['country'], ...)`.

**Raises:** `MqValueError` when missing columns.

---

### SectorConstraint.__init__(self, sector_name: str, minimum: float = 0, maximum: float = 100, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a GICS Sector allocation constraint.

**Algorithm:**
1. Branch: if `unit` not in `[PERCENT, DECIMAL]` -> raise `MqValueError('Sector constraints can only be set by percent or decimal.')`.
2. Store fields.

### SectorConstraint.sector_name (property getter/setter)
### SectorConstraint.minimum (property getter/setter)
### SectorConstraint.maximum (property getter/setter)

### SectorConstraint.unit (property setter, value: OptimizationConstraintUnit)
Purpose: Set unit with validation.

**Algorithm:**
1. Branch: if `value` not in `[PERCENT, DECIMAL]` -> raise `MqValueError('Sector constraints can only be set by percent.')`.
2. Set `__unit = value`.

### SectorConstraint.to_dict(self)
Purpose: Serialize with `type='Sector'` and unit conversion identical to CountryConstraint pattern.

### SectorConstraint.build_many_constraints(cls, sector_constraints: Union[pd.DataFrame, List[Dict]])
Purpose: Batch-create `SectorConstraint` objects.

**Algorithm:**
1. Branch: if input is `list` -> convert to DataFrame.
2. Check required columns `['sector', 'minimum', 'maximum', 'unit']`.
3. Branch: if missing -> raise `MqValueError`.
4. Return list of `SectorConstraint(sector_name=row['sector'], ...)`.

**Raises:** `MqValueError` when missing columns.

---

### IndustryConstraint.__init__(self, industry_name: str, minimum: float = 0, maximum: float = 100, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a GICS Industry allocation constraint.

**Algorithm:**
1. Branch: if `unit` not in `[PERCENT, DECIMAL]` -> raise `MqValueError('Industry constraints can only be set by percent or decimal.')`.
2. Store fields.

### IndustryConstraint.industry_name (property getter/setter)
### IndustryConstraint.minimum (property getter/setter)
### IndustryConstraint.maximum (property getter/setter)

### IndustryConstraint.unit (property setter, value: OptimizationConstraintUnit)
Purpose: Set unit with validation.

**Algorithm:**
1. Branch: if `value` not in `[PERCENT, DECIMAL]` -> raise `MqValueError('Industry constraints can only be set by percent.')`.
2. Set `__unit = value`.

### IndustryConstraint.to_dict(self)
Purpose: Serialize with `type='Industry'` and decimal-to-percent conversion.

### IndustryConstraint.build_many_constraints(cls, industry_constraints: Union[pd.DataFrame, List[Dict]])
Purpose: Batch-create `IndustryConstraint` objects.

**Algorithm:**
1. Branch: if input is `list` -> convert to DataFrame.
2. Check required columns `['industry', 'minimum', 'maximum', 'unit']`.
3. Branch: if missing -> raise `MqValueError`.
4. Return list of `IndustryConstraint(industry_name=row['industry'], ...)`.

**Raises:** `MqValueError` when missing columns.

---

### FactorConstraint.__init__(self, factor: Factor, max_exposure: float)
Purpose: Create a factor exposure constraint.

### FactorConstraint.factor (property getter/setter) -> Factor
### FactorConstraint.max_exposure (property getter/setter) -> float

### FactorConstraint.to_dict(self)
Purpose: Serialize to `{'factor': self.factor.name, 'exposure': self.max_exposure}`.

### FactorConstraint.build_many_constraints(cls, factor_constraints: Union[pd.DataFrame, List[Dict]], risk_model_id: str)
Purpose: Batch-create `FactorConstraint` objects by resolving factor names against a risk model.

**Algorithm:**
1. Branch: if input is `list` -> convert to DataFrame.
2. Check required columns `['factor', 'exposure']`.
3. Branch: if missing -> raise `MqValueError`.
4. Call `FactorRiskModel.get(risk_model_id)` then `risk_model.get_many_factors(factor_names=...)`.
5. Build name-to-Factor-object mapping DataFrame, inner-merge with constraints.
6. Convert to records, return list of `FactorConstraint(factor=row['factor'], max_exposure=row['exposure'])`.

**Raises:** `MqValueError` when missing columns.

---

### OptimizerUniverse.__init__(self, assets, explode_composites, exclude_initial_position_set_assets, exclude_corporate_actions_types, exclude_hard_to_borrow_assets, exclude_restricted_assets, min_market_cap, max_market_cap)
Purpose: Define the investable universe with filtering options.

### OptimizerUniverse (all property getters/setters)
Standard getters and setters for all 8 fields.

### OptimizerUniverse.to_dict(self)
Purpose: Serialize universe to API format.

**Algorithm:**
1. Branch: if `self.assets` is `AssetUniverse` instance -> call `self.assets.resolve()`, use `self.assets.asset_ids`.
2. Branch: else (List[Asset]) -> map each asset to `asset.get_marquee_id()`.
3. Build dict with keys: `hedgeUniverse` (containing `assetIds` and empty `assetTypes`), `excludeCorporateActions` (True if list is non-empty), `excludeCorporateActionsTypes` (enum values), `excludeHardToBorrowAssets`, `excludeRestrictedAssets`, `excludeTargetAssets`, `explodeUniverse`.
4. Branch: if `self.min_market_cap` is truthy -> add `minMarketCap`.
5. Branch: if `self.max_market_cap` is truthy -> add `maxMarketCap`.
6. Return dict.

---

### MaxFactorProportionOfRiskConstraint.__init__(self, max_factor_proportion_of_risk: float, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a max factor MCTR constraint.

**Algorithm:**
1. Branch: if `unit` not in `[PERCENT, DECIMAL]` -> raise `MqValueError`.
2. Branch: if `unit == PERCENT` -> divide value by 100 to normalize to decimal.
3. Store normalized value and unit.

### MaxFactorProportionOfRiskConstraint.max_factor_proportion_of_risk (property getter/setter)

---

### MaxProportionOfRiskByGroupConstraint.__init__(self, factors: List[Factor], max_factor_proportion_of_risk: float, unit: OptimizationConstraintUnit = PERCENT)
Purpose: Create a group-level max MCTR constraint.

**Algorithm:**
1. Branch: if `unit` not in `[PERCENT, DECIMAL]` -> raise `MqValueError`.
2. Branch: if `unit == PERCENT` -> divide value by 100.
3. Store factors, normalized value, and unit.

### MaxProportionOfRiskByGroupConstraint.factors (property getter/setter)
### MaxProportionOfRiskByGroupConstraint.max_factor_proportion_of_risk (property getter/setter)

### MaxProportionOfRiskByGroupConstraint.to_dict(self)
Purpose: Serialize to `{'factors': [f.name for f in self.factors], 'max': self.max_factor_proportion_of_risk}`.

---

### OptimizerConstraints.__init__(self, asset_constraints, country_constraints, sector_constraints, industry_constraints, factor_constraints, max_factor_proportion_of_risk, max_proportion_of_risk_by_groups)
Purpose: Aggregate all constraints.

### OptimizerConstraints (all property getters/setters)
Standard getters and setters for all 7 fields.

### OptimizerConstraints.to_dict(self)
Purpose: Serialize all constraints to a single API payload dict.

**Algorithm:**
1. Collect all units from `self.asset_constraints` into a set.
2. Branch: if `len(types) > 1` -> raise `MqValueError('All asset constraints need to have the same unit')`.
3. `constrain_by_notional`: True if asset constraints exist AND the single unit is `NOTIONAL`.
4. Concatenate country + sector + industry constraints into `classification_constraints`.
5. Build dict with `assetConstraints`, `classificationConstraints`, `factorConstraints`, `constrainAssetsByNotional`.
6. Branch: if `self.max_factor_proportion_of_risk` is truthy -> add `maxFactorMCTR`.
7. Branch: if `self.max_proportion_of_risk_by_groups` is truthy -> add `maxFactorMCTRByGroup` list.
8. Return dict.

**Raises:** `MqValueError` when asset constraints have mixed units.

---

### ConstraintPriorities.__init__(self, min_sector_weights, max_sector_weights, min_industry_weights, max_industry_weights, min_region_weights, max_region_weights, min_country_weights, max_country_weights, style_factor_exposures)
Purpose: Set priority levels for each constraint category.

### ConstraintPriorities (all property getters/setters)
Standard getters and setters for all 9 fields.

### ConstraintPriorities.to_dict(self) -> Dict
Purpose: Serialize non-None priorities to API format.

**Algorithm:**
1. Build full dict mapping camelCase keys to field values (note: condition `if self is not None` is always True -- effectively dead branch).
2. Filter out keys whose values are `None`, extract `.value` from remaining `PrioritySetting` enums.
3. Branch: if resulting dict is non-empty -> return it; else -> return `None`.

---

### OptimizerSettings.__init__(self, notional, allow_long_short, gross_notional, net_notional, min_names, max_names, min_weight_per_constituent, max_weight_per_constituent, max_adv, constraint_priorities)
Purpose: Initialize optimizer settings and validate consistency.

**Algorithm:**
1. Store all fields.
2. Call `self._validate_settings()`.

### OptimizerSettings._validate_settings(self)
Purpose: Validate settings for consistency across hedger modes and weight constraints.

**Algorithm:**
1. Branch: if `min_weight_per_constituent` is not None AND < 0 -> set it to 0, raise `Warning`.
2. Branch: if `max_weight_per_constituent` is not None AND < 0 -> raise `MqValueError`.
3. Branch: if both weight constraints are not None AND min > max -> raise `MqValueError`.
4. Branch: if `allow_long_short` is True ->
   a. Branch: if both `gross_notional` and `net_notional` are set AND `|net_notional| > gross_notional` -> raise `MqValueError`.
   b. Otherwise, no additional validation (allows bidirectional mode with just notional).
5. Branch: if `allow_long_short` is False ->
   a. Branch: if `gross_notional` is not None AND `net_notional` is not None AND `gross_notional != net_notional` -> raise `MqValueError`.

**Raises:** `Warning` (min_weight negative), `MqValueError` (max_weight negative, min>max, invalid notional config).

### OptimizerSettings (all property getters/setters)
Most setters call `self._validate_settings()` after mutation: `notional`, `allow_long_short`, `gross_notional`, `net_notional`, `min_weight_per_constituent`, `max_weight_per_constituent`. The setters for `min_names`, `max_names`, `max_adv`, `constraint_priorities` do NOT re-validate.

### OptimizerSettings.to_dict(self)
Purpose: Convert settings to API payload format.

**Algorithm:**
1. Build base dict with `minNames`, `maxNames`, `maxAdvPercentage`.
2. Branch: if `allow_long_short` is True ->
   a. Set `allowLongShort = True`.
   b. Branch: if both `gross_notional` and `net_notional` are not None -> set `grossNotional` and `netNotional`.
   c. Branch: elif `notional` is not None -> set `hedgeNotional = notional`.
3. Branch: else (unidirectional) ->
   a. Set `hedgeNotional = notional`, `allowLongShort = False`.
4. Branch: if `min_weight_per_constituent` is not None -> set `minWeight = value * 100`.
5. Branch: if `max_weight_per_constituent` is not None -> set `maxWeight = value * 100`.
6. Branch: if `constraint_priorities` is truthy -> set `constraintPrioritySettings = constraint_priorities.to_dict()`.
7. Return dict.

---

### TurnoverConstraint.__init__(self, turnover_portfolio, max_turnover_percent, turnover_notional_type)
Purpose: Define turnover limit relative to a reference portfolio.

### TurnoverConstraint (all property getters/setters)

### TurnoverConstraint.to_dict(self)
Purpose: Serialize turnover constraint.

**Algorithm:**
1. Get positions from `self.turnover_portfolio.positions`.
2. Build payload with `turnoverPortfolio` (list of `{assetId, quantity}`) and `maxTurnoverPercentage`.
3. Branch: if `self.turnover_notional_type` is truthy -> add `turnoverNotionalType` (enum `.value`).
4. Return payload.

---

### _ensure_completed(func) (module-level decorator)
Purpose: Decorator that ensures the optimization has been run before accessing results.

**Algorithm:**
1. Wraps `func` with `@wraps(func)`.
2. Extract `self` from `args[0]`.
3. Branch: if `self._OptimizerStrategy__result` is `None` -> raise `MqValueError('Please run the optimization before calling this method')`.
4. Otherwise, call and return `func(*args, **kwargs)`.

---

### OptimizerStrategy.__init__(self, initial_position_set, universe, risk_model, constraints, turnover, settings, objective, objective_parameters)
Purpose: Create an optimizer strategy with all required inputs.

### OptimizerStrategy (property getters/setters)
Standard getters and setters for: `initial_position_set`, `universe`, `risk_model`, `constraints`, `turnover`, `settings`, `objective`, `objective_parameters`.

**Note:** The `objective_parameters` setter on line 1647-1648 has a bug -- it is decorated as a setter but has no parameter and returns `self.__objetive_parameters` (with a typo in the attribute name). See Bugs Found section.

### OptimizerStrategy.to_dict(self, fail_on_unpriced_positions: bool = True) -> Dict
Purpose: Convert all strategy inputs into the API request payload. Does not modify initial_position_set.

**Algorithm:**
1. Branch: if `self.constraints` is `None` -> set to default `OptimizerConstraints()`.
2. Branch: if `self.settings` is `None` -> set to default `OptimizerSettings()`.
3. Compute `backtest_start_date` = position date minus 1 year.
4. Convert initial positions to DataFrame.
5. Branch: if `self.initial_position_set.reference_notional` is truthy -> select `['asset_id', 'weight']` columns.
6. Branch: else -> select `['asset_id', 'quantity']` columns.
7. Rename `asset_id` to `assetId`, convert to records.
8. Build `parameters` dict with `hedgeTarget`, `hedgeDate`, `backtestStartDate`, `backtestEndDate`, `comparisons=[]`, `fxHedged=False`, `marketParticipationRate=10`.
9. Merge constraints, settings, universe dicts into `parameters` (skipping None values).
10. Set `parameters['riskModel'] = self.risk_model.id`.
11. Branch: if `self.turnover` is truthy ->
    a. Branch: if turnover portfolio has `reference_notional` -> call `.price()`.
    b. Merge turnover dict into parameters (skipping None values).
12. Branch: if `initial_position_set.reference_notional` is not None -> set `parameters['targetNotional']`.
13. Branch: if `self.__objective_parameters` is not None -> set `parameters['hedgeObjectiveParameters']`.
14. Build pricing `payload` dict with `positions` and `parameters` sub-dict (currency, pricingDate, useUnadjustedClosePrice=False, frequency, priceRegardlessOfAssetsMissingPrices, fallbackDate).
15. Branch: if `reference_notional` is not None -> add `targetNotional` to pricing payload parameters.
16. POST to `/price/positions`.
17. Branch: if POST throws exception -> raise `MqValueError` wrapping the error.
18. Branch: if `'errorMessage'` in response ->
    a. Branch: if `assetIdsMissingPrices` is non-empty -> log warning.
    b. Raise `MqValueError`.
19. Branch: if `reference_notional` is None -> set `targetNotional` from `actualNotional` in response.
20. Branch: else -> rebuild `hedgeTarget.positions` from response with `{assetId, quantity}`.
21. Return `{'objective': self.objective.value, 'parameters': parameters}`.

**Raises:** `MqValueError` on pricing errors or missing prices.

### OptimizerStrategy.handle_error(self, error_message: str) -> List
Purpose: Map known error prefixes to verbose user-friendly messages.

**Algorithm:**
1. For each `(key, val)` in `VERBOSE_ERROR_MSG`:
   a. Branch: if `error_message.startswith(key)` -> return `[val(error_message), True]` (predefined).
2. Return `[error_message, False]` (not predefined).

### OptimizerStrategy.run(self, optimizer_type: OptimizerType = AXIOMA_PORTFOLIO_OPTIMIZER, fail_on_unpriced_positions: bool = True)
Purpose: Execute the optimization via the Marquee API with retry logic (up to 5 attempts).

**Algorithm:**
1. Branch: if `optimizer_type` is `None` -> raise `MqValueError('You must pass an optimizer type.')`.
2. Branch: if `optimizer_type == AXIOMA_PORTFOLIO_OPTIMIZER` ->
   a. Convert strategy to dict via `self.to_dict(fail_on_unpriced_positions)`.
   b. Set `counter = 5`, `predefined_error = False`.
   c. While `counter > 0`:
      - Try: Call `GsHedgeApi.calculate_hedge(strategy_as_dict)`.
        - Branch: if `result` key is `None` in response ->
          - Branch: if `'errorMessage'` in response ->
            - Call `self.handle_error(error_message)`.
            - Branch: if predefined error -> set `counter = 0`, raise `MqValueError` with verbose message.
            - Branch: else -> raise `MqValueError` with raw error message + guidance.
          - Branch: elif `counter == 1` -> raise `MqValueError` (last attempt exhausted).
          - Decrement `counter`.
        - Branch: else (result exists) -> store `self.__result = result`, set `counter = 0`.
      - Except:
        - Branch: if `predefined_error` -> re-raise immediately.
        - Branch: if `counter == 1` -> raise `MqValueError` (exhausted).
        - Decrement `counter`.

**Raises:** `MqValueError` on null optimizer_type, predefined errors, non-predefined errors, or exhausted retries.

### OptimizerStrategy.run_save_share(self, optimizer_type: OptimizerType = AXIOMA_PORTFOLIO_OPTIMIZER, fail_on_unpriced_positions: bool = True) -> Tuple[Dict, Dict]
Purpose: Same as `run()` but returns `(strategy_as_dict, optimization_results)` for use with save_to_marquee.

**Algorithm:** Identical to `run()` except:
1. After the while loop, returns `(strategy_as_dict, optimization_results)`.

**Raises:** Same as `run()`.

### OptimizerStrategy.__construct_position_set_from_hedge_result(self, result_key: str, by_weight: bool = True) -> PositionSet
Purpose: Build a PositionSet from the raw API result.

**Algorithm:**
1. Extract `result = self.__result[result_key]`.
2. Build `PositionSet` with:
   - `date` = initial position set date.
   - `reference_notional` = `result['netExposure']` if `by_weight` else `None`.
   - `positions` = list of `Position` objects from `result['constituents']`:
     - `identifier` = `asset.get('bbid', asset['name'])` (fallback to name if no bbid).
     - `asset_id` = `asset['assetId']`.
     - `quantity` = `asset['shares']` if not `by_weight` else `None`.
     - `weight` = `asset['weight']`.

### OptimizerStrategy.get_optimization(self, by_weight: bool = False) -> PositionSet
Purpose: Get the hedge-only positions from optimization results. Decorated with `@_ensure_completed`.

**Algorithm:**
1. Return `self.__construct_position_set_from_hedge_result('hedge', by_weight)`.

### OptimizerStrategy.get_optimized_position_set(self, by_weight: bool = False) -> PositionSet
Purpose: Get the hedged-target combined positions. Decorated with `@_ensure_completed`.

**Algorithm:**
1. Return `self.__construct_position_set_from_hedge_result('hedgedTarget', by_weight)`.

### OptimizerStrategy.get_hedge_exposure_summary(self) -> Dict
Purpose: Get a summary of hedge exposures (gross, net, long, short). Decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError` (redundant with decorator).
2. Define inner function `get_exposure_dict(result_key)`:
   a. Branch: if `result_key` not in `self.__result` -> return `None`.
   b. Extract `grossExposure`, `netExposure`, `longExposure` (default 0), `shortExposure` (default 0), `numberOfPositions`.
   c. Branch: if `result_key == 'hedge'` ->
      - Branch: if `longExposure > 0` -> set `mode = 'bidirectional'`.
      - Branch: else -> set `mode = 'unidirectional (short positions only)'`.
   d. Return exposure dict.
3. Return dict with keys `hedge`, `target`, `hedged_target` each mapped to `get_exposure_dict()` calls.

### OptimizerStrategy.get_hedge_constituents_by_direction(self) -> Dict
Purpose: Split hedge constituents into long and short positions. Decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError` (redundant with decorator).
2. Branch: if `self.__result.get('hedge')` is `None` -> raise `MqValueError`.
3. Extract `constituents` list.
4. Branch: if `constituents` is empty -> return dict with empty DataFrames and zero summary.
5. Convert to DataFrame.
6. Branch: if `'notional'` in columns -> filter `long_positions` (notional > 0) and `short_positions` (notional < 0).
7. Branch: else -> empty DataFrames.
8. Calculate summary: `num_long`, `num_short`, `total_long_notional`, `total_short_notional`.
9. Return dict with `long_positions`, `short_positions`, `summary`.

### OptimizerStrategy.get_cumulative_pnl_performance(self, target: HedgeTarget = HEDGED_TARGET) -> pd.DataFrame
Purpose: Get cumulative PnL performance as a DataFrame. NOT decorated with `@_ensure_completed` (performs its own check).

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError`.
2. Branch: if target value not in result -> raise `MqValueError`.
3. Extract `cumulativePnl`, convert to DataFrame with columns `['date', 'cumulativePnl']`.
4. Convert `date` column to datetime.
5. Return DataFrame.

### OptimizerStrategy.get_style_factor_exposures(self, target: HedgeTarget = HEDGED_TARGET) -> List
Purpose: Get style factor exposures from the result. NOT decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError`.
2. Branch: if target value not in result -> raise `MqValueError`.
3. Branch: if `factorExposures` is `None` for the target -> raise `MqValueError`.
4. Extract `factorExposures.style` list.
5. Return list.

### OptimizerStrategy.get_risk_buckets(self, target: HedgeTarget = HEDGED_TARGET) -> Dict
Purpose: Get risk buckets from the result. NOT decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError`.
2. Branch: if target value not in result -> raise `MqValueError`.
3. Branch: if `riskBuckets` is `None` for the target -> raise `MqValueError`.
4. Return `{"risk_buckets": risk_buckets}`.

### OptimizerStrategy.get_transaction_and_liquidity_constituents_performance(self, target: HedgeTarget = HEDGED_TARGET) -> pd.DataFrame
Purpose: Get filtered constituent performance data (transaction costs, liquidity). NOT decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError`.
2. Branch: if target value not in result -> raise `MqValueError`.
3. Extract constituents list.
4. Define `keys_to_keep` set: `name`, `assetId`, `bbid`, `notional`, `shares`, `price`, `weight`, `currency`, `transactionCost`, `marginalCost`, `advPercentage`, `borrowCost`.
5. For each constituent, filter to only `keys_to_keep`.
6. Return as DataFrame.

### OptimizerStrategy.get_performance_summary(self) -> Dict
Purpose: Build a multi-section performance summary comparing initial vs hedged portfolio. NOT decorated with `@_ensure_completed`.

**Algorithm:**
1. Branch: if `self.__result` is `None` -> raise `MqValueError`.
2. Branch: if `hedgedTarget` not in result -> raise `MqValueError`.
3. Branch: if `target` not in result -> raise `MqValueError`.
4. Extract `target` and `hedged_target` dicts from result.
5. Build `risk_df` DataFrame: Annualized Volatility, Specific Risk, Factor Risk, Factor Risk Delta.
   - Factor Risk Delta: Branch: if both systematicExposure values are not None -> compute difference; else -> None.
6. Build `performance_df` DataFrame: PnL, PnL Delta.
   - PnL Delta: Branch: if both totalPnl values are not None -> compute difference; else -> None.
7. Build `transaction_cost_df` DataFrame: Market Impact, Borrow Cost (bps).
8. Build `comparison_df` DataFrame: Overlap with Core.
9. Concatenate all 4 DataFrames with a `Category` column into `combined_df`.
10. Return dict with keys: `risk`, `performance`, `transaction_cost`, `comparison`, `combined`.

### OptimizerStrategy.build_hedge_payload(self, strategy_request: Dict, optimization_response: Dict, hedge_name: str = "Custom Hedge", group_name: str = "New Hedge Group") -> Dict
Purpose: Construct the payload for saving a hedge to the Marquee API.

**Algorithm:**
1. Build payload dict with `name`, `objective` (from strategy_request, defaulting to "Minimize Factor Risk"), and `hedges` list containing one item with `name`, `objective`, `parameters`, `result`.
2. Return payload.

### OptimizerStrategy.save_to_marquee(self, strategy_request: Dict, optimization_response: Dict, hedge_name: str = "Custom Hedge", group_name: str = "New Hedge Group") -> Dict
Purpose: POST the hedge to Marquee API and print save confirmation.

**Algorithm:**
1. Call `self.build_hedge_payload(...)` to build the payload.
2. POST to `/hedges/groups` via `GsSession.current.sync.post(url, payload=payload)`.
3. Extract `hedge_group_id` from response.
4. Print success messages including hedge group ID, name, created time.
5. Branch: if `hedge_group_id != 'N/A'` -> print Marquee UI URL.
6. Return response.
7. Except: print failure message.
   - Branch: if exception `hasattr(e, 'response')` -> print response text.
   - Raise `MqValueError` wrapping the exception.

**Raises:** `MqValueError` when save fails.

## State Mutation
- `AssetUniverse.__asset_ids`: Populated by `resolve()` when initially `None`; calls external API.
- `OptimizerStrategy.__result`: Set to `None` in `__init__`; populated by `run()` or `run_save_share()` on successful API response.
- `OptimizerStrategy.__constraints`: Mutated from `None` to default `OptimizerConstraints()` inside `to_dict()` if not set.
- `OptimizerStrategy.__settings`: Mutated from `None` to default `OptimizerSettings()` inside `to_dict()` if not set.
- `OptimizerSettings.__min_weight_per_constituent`: Silently set to `0` by `_validate_settings()` if negative (before raising Warning).
- All property setters on `OptimizerSettings` that call `_validate_settings()` may raise during mutation: `notional`, `allow_long_short`, `gross_notional`, `net_notional`, `min_weight_per_constituent`, `max_weight_per_constituent`.
- Thread safety: No thread-safety mechanisms. The `run()` method performs network I/O and mutates `__result`. Concurrent access to a single `OptimizerStrategy` instance is unsafe.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `OptimizerObjectiveParameters.to_dict` | When `len(terms) != 1` |
| `MqValueError` | `CountryConstraint.__init__` | When `unit` is `NOTIONAL` |
| `MqValueError` | `CountryConstraint.unit` setter | When `unit` is `NOTIONAL` |
| `MqValueError` | `SectorConstraint.__init__` | When `unit` is `NOTIONAL` |
| `MqValueError` | `SectorConstraint.unit` setter | When `unit` is `NOTIONAL` |
| `MqValueError` | `IndustryConstraint.__init__` | When `unit` is `NOTIONAL` |
| `MqValueError` | `IndustryConstraint.unit` setter | When `unit` is `NOTIONAL` |
| `MqValueError` | `MaxFactorProportionOfRiskConstraint.__init__` | When `unit` is `NOTIONAL` |
| `MqValueError` | `MaxProportionOfRiskByGroupConstraint.__init__` | When `unit` is `NOTIONAL` |
| `MqValueError` | `OptimizerConstraints.to_dict` | When asset constraints have mixed units |
| `MqValueError` | `OptimizerSettings._validate_settings` | When `max_weight_per_constituent < 0` |
| `MqValueError` | `OptimizerSettings._validate_settings` | When `min_weight > max_weight` |
| `MqValueError` | `OptimizerSettings._validate_settings` | When bidirectional mode and `|net| > gross` |
| `MqValueError` | `OptimizerSettings._validate_settings` | When unidirectional mode and `gross != net` |
| `Warning` | `OptimizerSettings._validate_settings` | When `min_weight_per_constituent < 0` (raised as `Warning`, not `MqValueError`) |
| `MqValueError` | `AssetConstraint.build_many_constraints` | Missing required columns |
| `MqValueError` | `AssetConstraint.build_many_constraints` | Multiple units in input |
| `MqValueError` | `AssetConstraint.build_many_constraints` | Unresolved positions when `fail_on_unresolved_positions=True` |
| `MqValueError` | `CountryConstraint.build_many_constraints` | Missing required columns |
| `MqValueError` | `SectorConstraint.build_many_constraints` | Missing required columns |
| `MqValueError` | `IndustryConstraint.build_many_constraints` | Missing required columns |
| `MqValueError` | `FactorConstraint.build_many_constraints` | Missing required columns |
| `MqValueError` | `OptimizerStrategy.to_dict` | Pricing API error or missing prices |
| `MqValueError` | `OptimizerStrategy.run` | `optimizer_type` is `None` |
| `MqValueError` | `OptimizerStrategy.run` | Predefined error from optimizer |
| `MqValueError` | `OptimizerStrategy.run` | Non-predefined error from optimizer |
| `MqValueError` | `OptimizerStrategy.run` | Retries exhausted (counter reaches 0) |
| `MqValueError` | `OptimizerStrategy.run_save_share` | Same conditions as `run()` |
| `MqValueError` | `_ensure_completed` decorator | When `__result` is `None` |
| `MqValueError` | `get_cumulative_pnl_performance` | When `__result` is `None` or target not in result |
| `MqValueError` | `get_style_factor_exposures` | When `__result` is `None`, target not in result, or no factorExposures |
| `MqValueError` | `get_risk_buckets` | When `__result` is `None`, target not in result, or no riskBuckets |
| `MqValueError` | `get_transaction_and_liquidity_constituents_performance` | When `__result` is `None` or target not in result |
| `MqValueError` | `get_performance_summary` | When `__result` is `None` or missing hedgedTarget/target |
| `MqValueError` | `get_hedge_exposure_summary` | When `__result` is `None` (redundant with decorator) |
| `MqValueError` | `get_hedge_constituents_by_direction` | When `__result` is `None` or no hedge data |
| `MqValueError` | `save_to_marquee` | When API POST fails |

## Edge Cases
- `resolve_assets_in_batches`: When `identifiers` list is empty, `math.ceil(0 / batch_size)` = 0 causes `np.array_split` to produce 0 chunks; the `len(identifiers) > batch_size` branch is False so `[identifiers]` wraps the empty list, producing one empty batch sent to the API.
- `AssetConstraint.build_many_constraints`: When `fail_on_unresolved_positions=False` and ALL identifiers are unresolved, the DataFrame filter removes all rows and an empty list is returned.
- `OptimizerConstraints.to_dict`: When `asset_constraints` is empty, `types` is an empty set. The code then calls `types.pop()` on line 1076 which would raise `KeyError` on an empty set. However, `len(self.asset_constraints) > 0` short-circuits the `and`, so `types.pop()` is only called when there are constraints. If the list is empty, `constrain_by_notional` is False.
- `ConstraintPriorities.to_dict`: The condition `if self is not None` (line 1217) is always True for an instance method call; this is dead code. The else branch `{}` is unreachable.
- `OptimizerSettings._validate_settings`: The `Warning` raised for negative `min_weight_per_constituent` uses `raise Warning(...)` which raises a `Warning` exception (not a standard warning via `warnings.warn`). This exception propagates and prevents construction. The field is also set to 0 before the raise, but since the raise prevents further use, the mutation has no observable effect from the caller's perspective.
- `OptimizerStrategy.objective_parameters` setter (line 1647): Decorated as `@objective_parameters.setter` but takes no `value` parameter and has a typo (`__objetive_parameters` missing a `c`). Attempting to set `strategy.objective_parameters = x` will raise a `TypeError` because the setter signature does not accept a value.
- `OptimizerStrategy.run`: The retry loop runs up to 5 times. On the last attempt (`counter == 1`), both the inner "no result + no error message" branch and the except branch raise `MqValueError`. If a non-`MqValueError` exception occurs on attempt 1-4, it is silently swallowed and retried.
- `OptimizerStrategy.run_save_share`: Returns `optimization_results` which may contain error data if the while loop exits after setting `counter = 0` on a successful result, but `optimization_results` could also be the last failed response if it never succeeded (though an exception would be raised first).
- `OptimizerUniverse.to_dict`: When `min_market_cap` or `max_market_cap` is `0`, the truthiness check `if self.min_market_cap` evaluates to False, so a cap of 0 would be silently ignored.
- `OptimizerObjectiveParameters.__init__`: The default `terms` parameter is a mutable default (`[OptimizerObjectiveTerm.DEFAULT_RISK_PARAMS]`). Since `DEFAULT_RISK_PARAMS` is a dict (not an `OptimizerObjectiveTerm`), calling `to_dict()` on the default terms would fail because a dict does not have a `.to_dict()` method.

## Bugs Found
- Line 1647-1648: `objective_parameters` property setter takes no `value` parameter and references `self.__objetive_parameters` (typo: missing `c` in `objective`). This means: (1) assignment `strategy.objective_parameters = x` raises `TypeError`, and (2) even if it worked, the attribute name is misspelled vs. `__objective_parameters` used elsewhere. (OPEN)
- Line 1217: `if self is not None` is a tautology in an instance method; the else branch is dead code. (OPEN -- cosmetic)
- Line 137: Default `terms` parameter is `[OptimizerObjectiveTerm.DEFAULT_RISK_PARAMS]` which is a `Dict`, not an `OptimizerObjectiveTerm`. Calling `to_dict()` on default terms would crash. (OPEN)
- Line 1300: `raise Warning(...)` raises a `Warning` exception rather than issuing a `warnings.warn()` call. This is technically valid Python but unusual and may confuse callers who catch only `MqValueError`. (OPEN -- design issue)

## Coverage Notes
- Branch count: ~120 explicit branches (if/elif/else, while, for, try/except, ternary, short-circuit and/or)
- Key branching areas:
  - `resolve_assets_in_batches`: 3 branches (batch split, identifier resolution truthy check, fields None check)
  - `AssetConstraint.to_dict`: 3 branches (isinstance check, 2x DECIMAL unit checks)
  - `AssetConstraint.build_many_constraints`: 8 branches (list vs DF, missing columns, multiple units, assetId present, fail_on_unresolved, NaN check)
  - `CountryConstraint.__init__` + unit setter: 2 branches each (unit validation)
  - `SectorConstraint.__init__` + unit setter: 2 branches each
  - `IndustryConstraint.__init__` + unit setter: 2 branches each
  - `MaxFactorProportionOfRiskConstraint.__init__`: 3 branches (unit validation, PERCENT normalization)
  - `MaxProportionOfRiskByGroupConstraint.__init__`: 3 branches
  - `OptimizerConstraints.to_dict`: 4 branches (mixed units, constrain_by_notional, max_factor, max_groups)
  - `ConstraintPriorities.to_dict`: 3 branches (self is not None, filter None, empty check)
  - `OptimizerSettings._validate_settings`: 8 branches (min<0, max<0, min>max, allow_long_short true/false sub-branches)
  - `OptimizerSettings.to_dict`: 7 branches (allow_long_short, gross+net, notional, weights, priorities)
  - `TurnoverConstraint.to_dict`: 1 branch (turnover_notional_type)
  - `OptimizerStrategy.to_dict`: 12+ branches (constraints None, settings None, reference_notional, turnover, pricing errors)
  - `OptimizerStrategy.run` / `run_save_share`: 10+ branches each (optimizer_type None, retry loop, result None, errorMessage, predefined, counter==1)
  - `OptimizerStrategy.get_*` methods: 2-3 branches each (result None, target missing, sub-field missing)
  - `get_hedge_constituents_by_direction`: 5 branches (result None, hedge None, empty constituents, notional column check)
  - `get_performance_summary`: 5 branches (result None, missing targets, None checks for deltas)
  - `save_to_marquee`: 3 branches (exception, hasattr response, hedge_group_id)
- Pragmas: none
- The `ConstraintPriorities.to_dict` else branch (line 1218) is unreachable dead code
- The `OptimizerStrategy.objective_parameters` setter is broken and cannot be tested via normal property assignment
