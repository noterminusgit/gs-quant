# api/gs/scenarios.py

## Summary
API client for managing risk scenarios via the GS Risk Scenarios service. Provides two classes: `GsScenarioApi` with full CRUD operations, name-based lookup, and scenario calculation; and `GsFactorScenarioApi` which extends it with factor-specific query parameters (risk model, shocked factors, date ranges, tags). Both delegate to `GsSession` HTTP calls against the `/risk/scenarios` and `/scenarios/calculate` endpoints.

## Dependencies
- Internal: `gs_quant.session` (`GsSession`)
- Internal: `gs_quant.target.risk` (`Scenario`)
- External: `datetime` (as `dt`), `logging` (`getLogger`), `typing` (`Dict`, `List`, `Tuple`)

## Type Definitions

### GsScenarioApi (class)
Inherits: `object`

Stateless API client. All methods are `@classmethod`. No instance state.

### GsFactorScenarioApi (class)
Inherits: `GsScenarioApi`

Extends the base scenario API with factor-scenario-specific query parameters. Has a `__init__` that calls `super().__init__()` (no additional state). Overrides `get_many_scenarios` and `calculate_scenario`.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsScenarioApi.create_scenario(cls, scenario: Scenario) -> Scenario
Purpose: Create a new risk scenario.

**Algorithm:**
1. POST `/risk/scenarios` with `scenario` payload, deserializing as `Scenario`
2. Return the new `Scenario` object

### GsScenarioApi.get_scenario(cls, scenario_id: str) -> Scenario
Purpose: Retrieve a single scenario by its ID.

**Algorithm:**
1. GET `/risk/scenarios/{scenario_id}` deserializing as `Scenario`
2. Return the deserialized `Scenario` object

### GsScenarioApi.get_many_scenarios(cls, ids: List[str] = None, names: List[str] = None, limit: int = 100, **kwargs) -> Tuple[Scenario, ...]
Purpose: Query multiple scenarios with optional ID, name, and arbitrary filters.

**Algorithm:**
1. Build base URL: `/risk/scenarios?limit={limit}`
2. Branch: if `ids` is truthy -> append `&id=` joined IDs to URL
3. Branch: if `names` is truthy -> append `&name=` joined names to URL
4. Branch: if `kwargs` is truthy -> iterate over items:
   a. Branch: if `v` is a `list` -> append `&{k}=` joined values
   b. Branch: else -> append `&{k}={v}`
5. GET the URL deserializing as `Scenario`
6. Return `.get('results', [])` from the response

### GsScenarioApi.get_scenario_by_name(cls, name: str) -> Scenario
Purpose: Look up a single scenario by exact name, raising an error if zero or multiple results.

**Algorithm:**
1. GET `/risk/scenarios?name={name}` deserializing as `Scenario`
2. Extract `totalResults` from response, defaulting to `0`
3. Branch: if `num_found == 0` -> raise `ValueError` with message `f'Scenario {name}not found'`
4. Branch: elif `num_found > 1` -> raise `ValueError` with message `f'More than one scemario named {name}'`
5. Branch: else -> return `ret['results'][0]`

**Raises:** `ValueError` when no scenario found or more than one found.

### GsScenarioApi.update_scenario(cls, scenario: Scenario) -> Dict
Purpose: Update an existing scenario.

**Algorithm:**
1. PUT `/risk/scenarios/{scenario.id_}` with `scenario` payload, deserializing as `Scenario`
2. Return the response

### GsScenarioApi.delete_scenario(cls, scenario_id: str) -> Dict
Purpose: Delete a scenario by its ID.

**Algorithm:**
1. DELETE `/risk/scenarios/{scenario_id}`
2. Return the response

### GsScenarioApi.calculate_scenario(cls, request: Dict) -> Dict
Purpose: Execute a scenario calculation.

**Algorithm:**
1. POST `/scenarios/calculate` with `request` payload
2. Return the response

### GsFactorScenarioApi.__init__(self)
Purpose: Initialize instance (no additional state beyond parent).

**Algorithm:**
1. Call `super().__init__()`

### GsFactorScenarioApi.get_many_scenarios(cls, ids: List[str] = None, names: List[str] = None, limit: int = 100, type: str = None, risk_model: str = None, shocked_factors: List[str] = None, shocked_factor_categories: List[str] = None, start_date: dt.date = None, end_date: dt.date = None, tags: List[str] = None) -> Tuple[Scenario, ...]
Purpose: Query factor scenarios with factor-specific filters, then filter to only those with a non-falsy `type_` field.

**Algorithm:**
1. Build `factor_scenario_args` dict conditionally:
   a. Branch: if `risk_model` truthy -> add `'riskModel'`
   b. Branch: if `type` truthy -> add `'factorScenarioType'`
   c. Branch: if `shocked_factors` truthy -> add `'shockedFactor'`
   d. Branch: if `shocked_factor_categories` truthy -> add `'shockedFactorCategory'`
   e. Branch: if `start_date` truthy -> add `'historicalSimulationStartDate'`
   f. Branch: if `end_date` truthy -> add `'historicalSimulationEndDate'`
   g. Branch: if `tags` truthy -> add `'tags'`
2. Call `super().get_many_scenarios(ids=ids, names=names, limit=limit, **factor_scenario_args)`
3. Filter results: keep only scenarios where `scenario.type_` is truthy
4. Return filtered tuple

### GsFactorScenarioApi.calculate_scenario(cls, calculation_request: Dict) -> Dict
Purpose: Delegate to parent's `calculate_scenario` with renamed parameter.

**Algorithm:**
1. Call `super().calculate_scenario(request=calculation_request)`
2. Return the result

## State Mutation
- No instance state on `GsScenarioApi`; all methods are classmethods.
- `GsFactorScenarioApi` has `__init__` but stores no additional state.
- No module-level mutable state (aside from `_logger`).
- Relies on `GsSession.current` for HTTP session (external state).
- `create_scenario`, `update_scenario`, `delete_scenario` mutate server-side state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_scenario_by_name` | When `totalResults == 0` (no scenario found) |
| `ValueError` | `get_scenario_by_name` | When `totalResults > 1` (ambiguous name) |

## Edge Cases
- `get_scenario_by_name` error message on line 60 has a missing space: `f'Scenario {name}not found'` should be `f'Scenario {name} not found'`.
- `get_scenario_by_name` error message on line 62 has a typo: `"scemario"` should be `"scenario"`.
- `get_many_scenarios` in `GsScenarioApi` builds multi-value query params by joining with `&id=` / `&name=` etc., producing correct repeated query parameters.
- `GsFactorScenarioApi.get_many_scenarios` post-filters results client-side by `scenario.type_`, which means the total count may be less than `limit`.
- `kwargs` iteration in `GsScenarioApi.get_many_scenarios`: when `v` is a list, the join pattern `f"&{k}=".join(v)` produces values separated by `&k=`, and prepends `&{k}=` -- resulting in correct repeated query params.
- The `type` parameter in `GsFactorScenarioApi.get_many_scenarios` shadows the Python builtin `type`.

## Bugs Found
- Line 60: Missing space in error message: `f'Scenario {name}not found'` -- should be `f'Scenario {name} not found'` (OPEN)
- Line 62: Typo in error message: `"scemario"` should be `"scenario"` (OPEN)

## Coverage Notes
- Branch count: 17
  - `ids` truthy (true/false) in `GsScenarioApi.get_many_scenarios`
  - `names` truthy (true/false) in `GsScenarioApi.get_many_scenarios`
  - `kwargs` truthy (true/false) in `GsScenarioApi.get_many_scenarios`
  - `isinstance(v, list)` (true/false) in kwargs iteration
  - `num_found == 0` (true/false) in `get_scenario_by_name`
  - `num_found > 1` (true/false) in `get_scenario_by_name`
  - 7 truthy checks in `GsFactorScenarioApi.get_many_scenarios` (`risk_model`, `type`, `shocked_factors`, `shocked_factor_categories`, `start_date`, `end_date`, `tags`) -- each true/false
  - `scenario.type_` truthy filter (true/false)
- Missing branches: None identified
- Pragmas: None
