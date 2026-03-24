# scenario.py

## Summary
Defines Marquee factor-based scenario objects for creating, retrieving, updating, and deleting factor shock and historical simulation scenarios. Provides data classes for factor shocks, factor shock parameters, historical simulation parameters, and a top-level `FactorScenario` class that wraps the `GsFactorScenarioApi` for CRUD operations.

## Dependencies
- Internal: `gs_quant.api.gs.scenarios` (GsFactorScenarioApi), `gs_quant.markets.factor` (Factor), `gs_quant.target.risk` (Scenario as TargetScenario, FactorScenarioType), `gs_quant.entities.entitlements` (Entitlements), `gs_quant.errors` (MqValueError)
- External: `enum` (Enum), `typing` (List, Union, Dict), `pydash` (get), `copy` (deepcopy), `datetime` (date, datetime), `pandas` (DataFrame)

## Type Definitions

### ScenarioCalculationType(Enum)
Inherits: `Enum`

| Value | Raw | Description |
|-------|-----|-------------|
| FACTOR_SCENARIO | `"Factor Scenario"` | Identifies a factor scenario calculation |

### FactorShock (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __factor | `Union[str, Factor]` | required | The factor being shocked (name string or Factor object) |
| __shock | `float` | required | The shock magnitude applied to the factor |

### FactorShockParameters (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __factor_shocks | `List[FactorShock]` | `None` | List of individual factor shocks |
| __propagate_shocks | `bool` | `None` | Whether to propagate shocks to non-shocked factors |
| __risk_model | `str` | `None` | Risk model identifier |

### HistoricalSimulationParameters (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __start_date | `dt.date` | `None` | Start date for historical simulation |
| __end_date | `dt.date` | `None` | End date for historical simulation |

### ScenarioParameters (TypeAlias)
```
ScenarioParameters = Union[FactorShockParameters, HistoricalSimulationParameters]
```

### FactorScenario (class)
Inherits: none

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __id | `str` | `None` | Marquee unique scenario identifier |
| __name | `str` | required | Scenario name |
| __type | `Union[str, FactorScenarioType]` | required | Scenario type (Factor_Shock or Factor_Historical_Simulation) |
| __description | `str` | `None` | Scenario description |
| __parameters | `Union[FactorShockParameters, HistoricalSimulationParameters]` | required | Scenario parameters (converted from Dict if needed) |
| __entitlements | `Union[Dict, Entitlements]` | `None` | Access entitlements |
| __tags | `List[str]` | `None` | User-defined tags |

### Scenario (TypeAlias)
```
Scenario = Union[FactorScenario]
```

## Enums and Constants

### ScenarioCalculationType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| FACTOR_SCENARIO | `"Factor Scenario"` | Factor scenario calculation type |

## Functions/Methods

### FactorShock.__init__(self, factor: Union[str, Factor], shock: float)
Purpose: Initialize a factor shock with a factor identifier and shock value.

**Algorithm:**
1. Store `factor` in `self.__factor`
2. Store `shock` in `self.__shock`

### FactorShock.__eq__(self, other) -> bool
Purpose: Compare two FactorShock instances by factor name and shock value.

**Algorithm:**
1. Branch: `other` is not `FactorShock` -> return `False`
2. Extract `factor_name`: Branch: `self.factor` is `Factor` instance -> use `.name`; else use string directly
3. Extract `other_factor_name`: Branch: `other.factor` is `Factor` instance -> use `.name`; else use string directly
4. Return `factor_name == other_factor_name and self.shock == other.shock`

### FactorShock.__repr__(self) -> str
Purpose: Return a string representation of the FactorShock.

**Algorithm:**
1. Return formatted string: `'ClassName(factor=..., shock=...)'`

### FactorShock.factor (property, getter) -> Union[str, Factor]
Purpose: Get the factor being shocked.

### FactorShock.shock (property, getter) -> float
Purpose: Get the shock value.

### FactorShock.shock (property, setter) (shock: float)
Purpose: Set the shock value.

### FactorShock.to_dict(self) -> dict
Purpose: Serialize factor shock to dictionary.

**Algorithm:**
1. Branch: `self.factor` is `Factor` instance -> use `self.factor.name`; else use `self.factor` directly
2. Return `{"factor": <resolved_name>, "shock": self.shock}`

### FactorShock.from_dict(cls, obj) -> FactorShock
Purpose: Deserialize a FactorShock from a dictionary.

**Algorithm:**
1. Return `FactorShock(factor=obj.get("factor"), shock=obj.get("shock"))`

### FactorShockParameters.__init__(self, factor_shocks: List[FactorShock] = None, propagate_shocks: bool = None, risk_model: str = None)
Purpose: Initialize factor shock parameters.

**Algorithm:**
1. Store all three parameters in private fields

### FactorShockParameters.__eq__(self, other) -> bool
Purpose: Compare two FactorShockParameters instances.

**Algorithm:**
1. Branch: `other` is not `FactorShockParameters` -> return `False`
2. Return comparison of all three fields: `factor_shocks`, `propagate_shocks`, `risk_model`

### FactorShockParameters.__repr__(self) -> str
Purpose: Return string representation with risk_model, propagate_shocks, and factor_shocks.

### FactorShockParameters.factor_shocks (property, getter) -> List[FactorShock]
Purpose: Get factor shocks list.

### FactorShockParameters.factor_shocks (property, setter) (factor_shocks: Union[List[FactorShock], Dict, pd.DataFrame])
Purpose: Set factor shocks from multiple input formats.

**Algorithm:**
1. Branch: `factor_shocks` is `pd.DataFrame` ->
   a. Convert to dict with `orient='split'`
   b. Zip columns and data to create `FactorShock` objects
2. Branch: `factor_shocks` is `Dict` ->
   a. Iterate key-value pairs, create `FactorShock(factor=k, shock=v)` for each
3. Branch: else (assumed `List[FactorShock]`) ->
   a. Assign directly

### FactorShockParameters.propagate_shocks (property, getter) -> bool
Purpose: Get propagate_shocks flag.

### FactorShockParameters.propagate_shocks (property, setter) (propagate_shocks: bool)
Purpose: Set propagate_shocks flag.

### FactorShockParameters.risk_model (property, getter) -> str
Purpose: Get risk model identifier (read-only, no setter).

### FactorShockParameters.from_dict(cls, obj: Dict) -> FactorShockParameters
Purpose: Deserialize from a camelCase dictionary.

**Algorithm:**
1. Deserialize `factorShocks` list via `FactorShock.from_dict` for each entry
2. Extract `riskModel` and `propagateShocks`
3. Return new `FactorShockParameters`

### FactorShockParameters.to_dict(self) -> Dict
Purpose: Serialize to a camelCase dictionary.

**Algorithm:**
1. Return dict with keys `riskModel`, `propagateShocks`, `factorShocks` (each shock serialized via `to_dict`)

### HistoricalSimulationParameters.__init__(self, start_date: dt.date = None, end_date: dt.date = None)
Purpose: Initialize historical simulation date range.

### HistoricalSimulationParameters.__eq__(self, other) -> bool
Purpose: Compare two HistoricalSimulationParameters.

**Algorithm:**
1. Branch: `other` is not `HistoricalSimulationParameters` -> return `False`
2. Return `self.start_date == other.start_date and self.end_date == other.end_date`

### HistoricalSimulationParameters.__repr__(self) -> str
Purpose: Return string representation with start_date and end_date.

### HistoricalSimulationParameters.start_date (property, getter/setter) -> dt.date
Purpose: Get/set simulation start date.

### HistoricalSimulationParameters.end_date (property, getter/setter) -> dt.date
Purpose: Get/set simulation end date.

### HistoricalSimulationParameters.from_dict(cls, obj: Dict) -> HistoricalSimulationParameters
Purpose: Deserialize from camelCase dictionary, parsing date strings.

**Algorithm:**
1. Parse `startDate` string with `strptime("%Y-%m-%d")` and convert to `date`
2. Parse `endDate` string with `strptime("%Y-%m-%d")` and convert to `date`
3. Return new `HistoricalSimulationParameters`

### HistoricalSimulationParameters.to_dict(self) -> Dict
Purpose: Serialize to camelCase dictionary.

**Algorithm:**
1. Return `{"startDate": self.start_date, "endDate": self.end_date}`

### FactorScenario.__init__(self, name: str, type: Union[str, FactorScenarioType], parameters: Union[Dict, HistoricalSimulationParameters, FactorShockParameters], entitlements: Union[Dict, Entitlements] = None, id_: str = None, description: str = None, tags: List[str] = None)
Purpose: Initialize a FactorScenario, auto-deserializing parameters from Dict if needed.

**Algorithm:**
1. Store `id_`, `name`, `type`, `description`, `entitlements`, `tags`
2. Branch: `parameters` is already `FactorShockParameters` or `HistoricalSimulationParameters` -> assign directly
3. Branch: `type == FactorScenarioType.Factor_Shock` -> call `FactorShockParameters.from_dict(parameters)`
4. Branch: else -> call `HistoricalSimulationParameters.from_dict(parameters)`

### FactorScenario.__repr__(self) -> str
Purpose: Return verbose representation with all fields.

### FactorScenario.__str__(self) -> str
Purpose: Return human-readable string representation.

### FactorScenario.id (property, getter) -> str
Purpose: Get Marquee scenario ID (read-only).

### FactorScenario.name (property, getter/setter) -> str
Purpose: Get/set scenario name.

### FactorScenario.type (property, getter) -> Union[str, FactorScenarioType]
Purpose: Get scenario type (read-only).

### FactorScenario.description (property, getter/setter) -> str
Purpose: Get/set scenario description.

### FactorScenario.parameters (property, getter/setter) -> ScenarioParameters
Purpose: Get/set scenario parameters.

### FactorScenario.entitlements (property, getter/setter) -> Entitlements
Purpose: Get/set entitlements.

### FactorScenario.tags (property, getter/setter) -> List[str]
Purpose: Get/set tags.

### FactorScenario.from_dict(cls, scenario_as_dict: Dict) -> FactorScenario
Purpose: Construct a FactorScenario from a flat dictionary.

**Algorithm:**
1. Extract keys: `name`, `description`, `id` (mapped to `id_`), `type`, `parameters`, `entitlements`, `tags` using `dict.get` and `pydash.get`
2. Pass to constructor (which handles parameter deserialization)

### FactorScenario.from_target(cls, target_scenario: TargetScenario) -> FactorScenario
Purpose: Construct from a `TargetScenario` API object.

**Algorithm:**
1. Branch: `target_scenario.type == FactorScenarioType.Factor_Shock` -> `FactorShockParameters.from_dict`
2. Branch: else -> `HistoricalSimulationParameters.from_dict`
3. Construct `FactorScenario` with all fields from target, converting entitlements via `Entitlements.from_target`

### FactorScenario.get(cls, scenario_id: str) -> FactorScenario
Purpose: Fetch a scenario by ID from the API.

**Algorithm:**
1. Call `GsFactorScenarioApi.get_scenario(scenario_id)`
2. Return `cls.from_target(scenario)`

### FactorScenario.get_by_name(cls, scenario_name: str) -> FactorScenario
Purpose: Fetch a scenario by name from the API.

**Algorithm:**
1. Call `GsFactorScenarioApi.get_scenario_by_name(scenario_name)`
2. Return `cls.from_target(scenario)`

### FactorScenario.get_many(cls, ids, names, type, risk_model, shocked_factors, shocked_factor_categories, propagated_shocks, start_date, end_date, tags, limit=100) -> List[FactorScenario]
Purpose: Fetch multiple scenarios with various filters.

**Algorithm:**
1. Branch: `type` is `FactorScenarioType` -> convert to `.value` string; else pass as-is
2. Call `GsFactorScenarioApi.get_many_scenarios` with all filter arguments
3. Convert each result via `cls.from_target`
4. Branch: `propagated_shocks is not None` ->
   a. Filter scenarios: include if `scenario.parameters.propagate_shocks == propagated_shocks` OR `scenario.type != FactorScenarioType.Factor_Shock`
   b. Return filtered list
5. Branch: else -> return all scenarios

### FactorScenario.save(self)
Purpose: Create or update a scenario in Marquee.

**Algorithm:**
1. Build `TargetScenario` from current fields
2. Branch: `self.description` truthy -> use it; else `None`
3. Branch: `self.entitlements` truthy -> call `.to_target()`; else `None`
4. Branch: `self.tags` truthy -> convert to tuple; else empty tuple `()`
5. Branch: `self.id` truthy (existing scenario) ->
   a. Set `target_scenario.id_` and call `GsFactorScenarioApi.update_scenario`
6. Branch: else (new scenario) ->
   a. Call `GsFactorScenarioApi.create_scenario`
   b. Set `self.__id` from returned scenario

### FactorScenario.delete(self)
Purpose: Delete a scenario from Marquee.

**Algorithm:**
1. Branch: `self.id` is falsy -> raise `MqValueError`
2. Call `GsFactorScenarioApi.delete_scenario(self.id)`

**Raises:** `MqValueError` when scenario has no ID (not yet created)

### FactorScenario.clone(self) -> FactorScenario
Purpose: Create a deep copy of the scenario with " copy" appended to the name and no ID.

**Algorithm:**
1. Deep copy `self.parameters`
2. Return new `FactorScenario` with `name=f"{self.name} copy"`, same description, type, and parameters; no id_, entitlements, or tags

## State Mutation
- `self.__id`: Set during `__init__`, updated by `save()` on creation
- `self.__factor_shocks`: Set during `__init__`, updated by `factor_shocks` setter (accepts `List`, `Dict`, or `DataFrame`)
- `self.__shock`: Set during `__init__`, mutable via `shock` setter
- All other private fields are mutable via their respective setters

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `FactorScenario.delete` | When `self.id` is falsy (scenario not yet persisted) |

## Edge Cases
- `FactorShock.__eq__` handles mixed `Factor` objects and plain strings by extracting `.name` only when the factor is a `Factor` instance
- `FactorShockParameters.factor_shocks` setter accepts three distinct types: `pd.DataFrame` (split-orient), `Dict`, or `List[FactorShock]`
- `FactorScenario.__init__` parameters arg accepts both pre-built parameter objects and raw dicts, dispatching on `isinstance` checks and then on `type`
- `FactorScenario.save` converts `tags` to `tuple` or empty `tuple` (not `None`)
- `FactorScenario.clone` produces a copy with no ID, so it will be treated as a new scenario on `save()`
- `get_many` with `propagated_shocks` filter also passes through any non-Factor_Shock type scenarios unconditionally

## Coverage Notes
- Branch count: ~28
- Key branches: `FactorShock.__eq__` isinstance checks (2), factor name extraction (4 paths for self/other Factor vs str), `FactorShockParameters.factor_shocks` setter (3 branches: DataFrame/Dict/List), `FactorScenario.__init__` parameters dispatch (3 branches), `save` create-vs-update (2), `delete` id check (2), `get_many` propagated_shocks filter (2), `from_target` type check (2)
- Pragmas: none
