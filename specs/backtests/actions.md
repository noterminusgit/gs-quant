# actions.py

## Summary
Defines all action classes used by the backtesting framework to specify what trades or operations to perform when a trigger fires. Includes the `Action` base class with auto-naming via a global counter, and concrete subclasses: `AddTradeAction`, `AddScaledTradeAction`, `AddWeightedTradeAction`, `EnterPositionQuantityScaledAction`, `ExitPositionAction`, `ExitTradeAction`, `ExitAllPositionsAction`, `HedgeAction`, and `RebalanceAction`. All action dataclasses use `@dataclass_json @dataclass` decorators and share a common priceable-naming pattern in `__post_init__`.

## Dependencies
- Internal: `gs_quant.backtests.backtest_objects` (`ConstantTransactionModel`, `TransactionModel`), `gs_quant.backtests.backtest_utils` (`make_list`, `CalcType`, `CustomDuration`), `gs_quant.base` (`Priceable`, `static_field`), `gs_quant.common` (`RiskMeasure`), `gs_quant.instrument` (`Instrument`), `gs_quant.json_convertors` (`decode_named_instrument`, `dc_decode`, `encode_named_instrument`, `decode_date_or_str`, `decode_dict_date_key_or_float`), `gs_quant.json_convertors_common` (`decode_risk_measure`, `encode_risk_measure`), `gs_quant.markets.portfolio` (`Portfolio`), `gs_quant.risk.transform` (`Transformer`), `gs_quant.target.backtests` (`BacktestTradingQuantityType`)
- External: `datetime` (`dt.date`, `dt.timedelta`), `warnings`, `collections` (`namedtuple`), `dataclasses` (`field`, `dataclass`), `enum` (`Enum`), `typing` (`List`, `Optional`, `Iterable`, `Union`, `Callable`, `TypeVar`, `ClassVar`), `dataclasses_json` (`config`, `dataclass_json`)

## Type Definitions

### Duration (type alias)
```
Duration = Union[str, dt.date, dt.timedelta, CustomDuration]
```

### ScalingActionType (Enum)

| Value | Raw | Description |
|-------|-----|-------------|
| risk_measure | `"risk_measure"` | Scale by a risk measure |
| size | `"size"` | Scale by size/notional |
| NAV | `"NAV"` | Scale by NAV |

### Action (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _needs_scaling | `bool` (class var) | `False` | Whether this action requires scaling |
| _calc_type | `CalcType` (class var) | `CalcType.simple` | Calculation type for the engine |
| _risk | `None` (class var) | `None` | Risk measure associated with this action |
| _transaction_cost | `TransactionModel` (class var) | `ConstantTransactionModel(0)` | Default transaction cost model |
| _transaction_cost_exit | `Optional[TransactionModel]` (class var) | `None` | Default exit transaction cost model |
| name | `Optional[str]` (class var) | `None` | Action name (auto-assigned if None) |
| __sub_classes | `ClassVar[List[type]]` | `[]` | Registry of all Action subclasses |

### TAction (TypeVar)
```
TAction = TypeVar('TAction', bound='Action')
```

### AddTradeAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceables | `Union[Instrument, Iterable[Instrument]]` | `None` | Instrument(s) to trade; decoded/encoded via named instrument codecs |
| trade_duration | `Duration` | `None` | How long the trade lasts (instrument attr, date, tenor, timedelta, or `'next schedule'`) |
| name | `str` | `None` | Optional name prefix for priceable naming |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost model; defaults to `transaction_cost` if None |
| holiday_calendar | `Iterable[dt.date]` | `None` | Holiday dates for date computations |
| class_type | `str` | `'add_trade_action'` (static_field) | Serialization discriminator |
| _dated_priceables | `dict` | `{}` (set in `__post_init__`) | State-specific priceable overrides |

### AddScaledTradeAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceables | `Union[Priceable, Iterable[Priceable]]` | `None` | Priceable(s) to trade |
| trade_duration | `Duration` | `None` | Trade duration specification |
| name | `str` | `None` | Optional name prefix |
| scaling_type | `ScalingActionType` | `ScalingActionType.size` | How to scale the trade |
| scaling_risk | `RiskMeasure` | `None` | Risk measure for scaling (when `scaling_type == risk_measure`) |
| scaling_level | `Union[float, dict[dt.date, Union[float, int]]]` | `1` | Target scaling level (constant or time-varying) |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost; defaults to entry if None |
| holiday_calendar | `Iterable[dt.date]` | `None` | Holiday dates |
| dated_priceables | `dict[dt.date, Priceable]` | `None` | Pre-assigned date-specific priceables |
| class_type | `str` | `'add_scaled_trade_action'` (static_field) | Serialization discriminator |

### AddWeightedTradeAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceables | `Portfolio` | `None` | Portfolio of instruments to weight |
| trade_duration | `Duration` | `None` | Trade duration specification |
| name | `str` | `None` | Optional name prefix |
| scaling_risk | `RiskMeasure` | `None` | Risk measure used for weighting |
| total_size | `float` | `100000.0` | Total notional to distribute by risk |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost; defaults to entry if None |
| holiday_calendar | `Iterable[dt.date]` | `None` | Holiday dates |
| class_type | `str` | `'add_weighted_trade_action'` (static_field) | Serialization discriminator |

### EnterPositionQuantityScaledAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceables | `Union[Priceable, Iterable[Priceable]]` | `None` | Priceable(s) to enter |
| trade_duration | `Duration` | `None` | Trade duration specification |
| name | `str` | `None` | Optional name prefix |
| trade_quantity | `Union[float, dict[dt.date, Union[float, int]]]` | `1` | Quantity to trade (constant or time-varying) |
| trade_quantity_type | `BacktestTradingQuantityType` | `BacktestTradingQuantityType.quantity` | Units for `trade_quantity` (quantity, notional, etc.) |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost; defaults to entry if None |
| class_type | `str` | `'enter_position_quantity_scaled_action'` (static_field) | Serialization discriminator |

### ExitPositionAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| name | `str` | `None` | Optional action name |
| class_type | `str` | `'exit_position_action'` | Serialization discriminator (not static_field) |

### ExitTradeAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceable_names | `Union[str, Iterable[str]]` | `None` | Names of priceables to exit |
| name | `str` | `None` | Optional action name |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Transaction cost model for exit |
| class_type | `str` | `'exit_trade_action'` (static_field) | Serialization discriminator |
| priceables_names | `list` | *(set in `__post_init__`)* | Normalized list form of `priceable_names` |

### ExitAllPositionsAction (dataclass_json, dataclass)
Inherits: `ExitTradeAction`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| class_type | `str` | `'exit_all_positions_action'` (static_field) | Serialization discriminator |

### HedgeAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| risk | `RiskMeasure` | `None` | Risk measure to hedge |
| priceables | `Optional[Priceable]` | `None` | Hedging instrument(s); converted to `Portfolio` in `__post_init__` |
| trade_duration | `Duration` | `None` | Trade duration specification |
| name | `str` | `None` | Optional name prefix |
| csa_term | `str` | `None` | CSA term for pricing |
| scaling_parameter | `str` | `'notional_amount'` | **Deprecated.** Formerly used for scaling; now ignored |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost; defaults to entry if None |
| risk_transformation | `Transformer` | `None` | Optional transformation applied to raw risk before hedging |
| holiday_calendar | `Iterable[dt.date]` | `None` | Holiday dates |
| risk_percentage | `float` | `100` | Percentage of risk to hedge (100 = full hedge) |
| class_type | `str` | `'hedge_action'` (static_field) | Serialization discriminator |

### RebalanceAction (dataclass_json, dataclass)
Inherits: `Action`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceable | `Priceable` | `None` | The resolved priceable to rebalance |
| size_parameter | `Union[str, float]` | `None` | Parameter controlling rebalance size |
| method | `Callable` | `None` | Rebalancing method/function |
| transaction_cost | `TransactionModel` | `ConstantTransactionModel(0)` | Entry transaction cost model |
| transaction_cost_exit | `Optional[TransactionModel]` | `None` | Exit transaction cost; defaults to entry if None |
| name | `str` | `None` | Optional action name |

### Named Tuples (info carriers for trigger context)
```
AddTradeActionInfo = namedtuple('AddTradeActionInfo', ['scaling', 'next_schedule'])
HedgeActionInfo = namedtuple('HedgeActionInfo', 'next_schedule')
ExitTradeActionInfo = namedtuple('ExitTradeActionInfo', 'not_applicable')
RebalanceActionInfo = namedtuple('RebalanceActionInfo', 'not_applicable')
AddScaledTradeActionInfo = namedtuple('AddScaledActionInfo', 'next_schedule')
AddWeightedTradeActionInfo = namedtuple('AddWeightedActionInfo', 'next_schedule')
```

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| action_count | `int` | `1` (initial) | Global auto-incrementing counter for action naming; modified by `Action.set_name` |

### default_transaction_cost() -> ConstantTransactionModel
Purpose: Factory function returning `ConstantTransactionModel(0)`. Used as `default_factory` in dataclass fields to avoid mutable default sharing.

## Functions/Methods

### Action.__init_subclass__(cls, **kwargs) -> None
Purpose: Register every Action subclass in the `__sub_classes` class variable.

**Algorithm:**
1. Call `super().__init_subclass__(**kwargs)`
2. Append `cls` to `Action.__sub_classes`

### Action.sub_classes() -> tuple (staticmethod)
Purpose: Return a tuple of all registered Action subclasses.

**Algorithm:**
1. Return `tuple(Action.__sub_classes)`

### Action.__post_init__(self) -> None
Purpose: Called after dataclass initialization; delegates to `set_name`.

**Algorithm:**
1. Call `self.set_name(self.name)`

### Action.set_name(self, name: str) -> None
Purpose: Auto-assign a unique name if none was provided.

**Algorithm:**
1. Branch: `self.name is None` -> set `self.name = 'Action{action_count}'`, increment global `action_count`
2. Branch: `self.name is not None` -> do nothing

### Action.calc_type (property) -> CalcType
Purpose: Read-only accessor for `_calc_type`.

### Action.risk (property) -> Optional[RiskMeasure]
Purpose: Read-only accessor for `_risk`.

### Action.transaction_cost (property/setter) -> TransactionModel
Purpose: Read/write accessor for `_transaction_cost`.

### Action.transaction_cost_exit (property/setter) -> Optional[TransactionModel]
Purpose: Read/write accessor for `_transaction_cost_exit`.

### AddTradeAction.__post_init__(self) -> None
Purpose: Initialize dated_priceables dict, name all priceables, set default transaction costs.

**Algorithm:**
1. Call `super().__post_init__()` (triggers `Action.set_name`)
2. Set `self._dated_priceables = {}`
3. For each `(i, p)` in `enumerate(make_list(self.priceables))`:
   a. Branch: `p.name is None` -> clone with name `'{self.name}_Priceable{i}'`
   b. Branch: `p.name.startswith(self.name)` -> keep as-is
   c. Branch: else -> clone with name `'{self.name}_{p.name}'`
4. Set `self.priceables = named_priceables`
5. Branch: `self.transaction_cost is None` -> set to `ConstantTransactionModel(0)`
6. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`

### AddTradeAction.set_dated_priceables(self, state: dt.date, priceables: Any) -> None
Purpose: Store date-specific priceable overrides.

**Algorithm:**
1. Set `self._dated_priceables[state] = make_list(priceables)`

### AddTradeAction.dated_priceables (property) -> dict
Purpose: Read-only accessor for `_dated_priceables`.

### AddScaledTradeAction.__post_init__(self) -> None
Purpose: Name all priceables and set default exit transaction cost.

**Algorithm:**
1. Call `super().__post_init__()`
2. For each `(i, p)` in `enumerate(make_list(self.priceables))`:
   a. Branch: `p.name is None` -> clone with name `'{self.name}_Priceable{i}'`
   b. Branch: `p.name.startswith(self.name)` -> keep as-is
   c. Branch: else -> clone with name `'{self.name}_{p.name}'`
3. Set `self.priceables = named_priceables`
4. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`

### AddWeightedTradeAction.__post_init__(self) -> None
Purpose: Set calc_type to semi_path_dependent, name priceables, set default exit cost.

**Algorithm:**
1. Call `super().__post_init__()`
2. Set `self._calc_type = CalcType.semi_path_dependent`
3. For each `(i, p)` in `enumerate(make_list(self.priceables))`:
   a. Branch: `p.name is None` -> clone with name `'{self.name}_Priceable{i}'`
   b. Branch: `p.name.startswith(self.name)` -> keep as-is
   c. Branch: else -> clone with name `'{self.name}_{p.name}'`
4. Set `self.priceables = named_priceables`
5. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`

### EnterPositionQuantityScaledAction.__post_init__(self) -> None
Purpose: Name all priceables and set default exit transaction cost.

**Algorithm:**
1. Call `super().__post_init__()`
2. Same priceable naming loop as `AddTradeAction` (3 branches per priceable)
3. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`

### ExitTradeAction.__post_init__(self) -> None
Purpose: Normalize `priceable_names` to a list.

**Algorithm:**
1. Call `super().__post_init__()`
2. Set `self.priceables_names = make_list(self.priceable_names)`

### ExitAllPositionsAction.__post_init__(self) -> None
Purpose: Set calc_type to path_dependent.

**Algorithm:**
1. Call `super().__post_init__()` (which calls `ExitTradeAction.__post_init__`)
2. Set `self._calc_type = CalcType.path_dependent`

### HedgeAction.__post_init__(self) -> None
Purpose: Set calc_type, wrap priceables in Portfolio, name them, set default exit cost, warn on deprecated parameter.

**Algorithm:**
1. Call `super().__post_init__()`
2. Set `self._calc_type = CalcType.semi_path_dependent`
3. Compute `portfolio`:
   a. Branch: `isinstance(self.priceables, Portfolio)` -> use as-is
   b. Branch: `isinstance(self.priceables, Priceable)` -> wrap in `Portfolio(self.priceables.clone(name=None), name=self.priceables.name)`
   c. Branch: else -> `None`
4. Branch: `not portfolio` -> raise `RuntimeError('hedge action only accepts one trade or one portfolio')`
5. For each `(i, priceable)` in `enumerate(portfolio)`:
   a. Branch: `priceable.name is None` -> clone with `'{self.name}_Priceable{i}'`
   b. Branch: `priceable.name.startswith(self.name)` -> keep as-is
   c. Branch: else -> clone with `'{self.name}_{priceable.name}'`
6. Wrap `named_priceables` in `Portfolio(named_priceables, name=portfolio.name)`
7. Set `self.priceables = named_priceable`
8. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`
9. Branch: `self.scaling_parameter != 'notional_amount'` -> emit `DeprecationWarning`

### HedgeAction.priceable (property) -> Portfolio
Purpose: Alias returning `self.priceables`.

### RebalanceAction.__post_init__(self) -> None
Purpose: Set calc_type, validate priceable has unresolved, name priceable, set default exit cost.

**Algorithm:**
1. Call `super().__post_init__()`
2. Set `self._calc_type = CalcType.path_dependent`
3. Branch: `self.priceable.unresolved is None` -> raise `ValueError("Please specify a resolved priceable to rebalance.")`
4. Branch: `self.priceable is not None`:
   a. Branch: `self.priceable.name is None` -> clone with `'{self.name}_Priceable0'`
   b. Branch: else -> clone with `'{self.name}_{self.priceable.name}'`
5. Branch: `self.transaction_cost_exit is None` -> set to `self.transaction_cost`

## State Mutation
- `action_count` (global): Incremented by `Action.set_name()` each time an action is created without an explicit name. Never reset.
- `Action.__sub_classes` (class var): Appended to by `__init_subclass__` for every `Action` subclass defined at import time. Immutable after module load.
- `self.priceables`: Overwritten in `__post_init__` of most subclasses (list of cloned, named priceables replaces input).
- `self._dated_priceables`: Dict, mutable via `set_dated_priceables()`.
- `self._calc_type`: Set in `__post_init__` of `HedgeAction`, `AddWeightedTradeAction`, `ExitAllPositionsAction`, `RebalanceAction`.
- `self.transaction_cost_exit`: Defaulted to `self.transaction_cost` if `None` during `__post_init__`.
- `self.priceables_names`: Set in `ExitTradeAction.__post_init__`.
- Thread safety: `action_count` is a global with no synchronization.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `HedgeAction.__post_init__` | When `priceables` is neither `Portfolio` nor `Priceable` (evaluates to falsy `portfolio`) |
| `ValueError` | `RebalanceAction.__post_init__` | When `self.priceable.unresolved is None` |
| `DeprecationWarning` | `HedgeAction.__post_init__` | When `scaling_parameter != 'notional_amount'` |
| `AttributeError` | Priceable naming loop | If a priceable lacks `.name` attribute (unexpected type) |

## Edge Cases
- Global `action_count` increments across all actions in the process lifetime; test isolation requires resetting it.
- `HedgeAction.__post_init__` line 437: `if not portfolio` -- the variable `portfolio` here is correctly the local variable (not the class `Portfolio`). The previous spec noted a bug referencing `if not Portfolio` (the class), but the actual code uses the local `portfolio` variable. However, a `Portfolio` instance with no instruments is falsy if `Portfolio.__bool__` returns `False` for empty portfolios, which could incorrectly raise `RuntimeError` for an empty portfolio.
- `ExitPositionAction` has no `__post_init__` override beyond `Action.__post_init__`.
- `ExitTradeAction.__post_init__` sets `self.priceables_names` (note: different attribute name from `self.priceable_names` -- the dataclass field uses singular, the derived attribute uses plural).
- `AddScaledTradeAction` does NOT set `_needs_scaling = True` despite being a scaling action -- `_needs_scaling` remains `False` from the base class.
- `RebalanceAction.__post_init__` checks `self.priceable.unresolved is None` before checking `self.priceable is not None`, so passing `priceable=None` will raise `AttributeError` (not `ValueError`).
- All priceable naming loops share the same 3-branch pattern: `name is None`, `name.startswith(self.name)`, else.

## Bugs Found
- Line 437: The condition `if not portfolio:` correctly references the local variable. However, the check on line 486 (`if self.priceable.unresolved is None`) in `RebalanceAction` runs before the `if self.priceable is not None` check on line 488, meaning `priceable=None` would cause `AttributeError` instead of a clean error message.

## Coverage Notes
- Branch count: ~35
- `Action.set_name`: 2 branches (name is None / not None)
- Priceable naming loop (shared pattern, appears 6 times): 3 branches each (name None / startswith / else)
- `transaction_cost_exit is None` check: 1 branch each, appears in 6 subclasses
- `AddTradeAction.__post_init__` extra: `transaction_cost is None` check (1 branch)
- `HedgeAction.__post_init__`: 3 branches for portfolio creation + 1 for `not portfolio` + 1 for deprecated scaling_parameter
- `RebalanceAction.__post_init__`: 1 branch for `unresolved is None` + 2 for priceable naming + 1 for `priceable is not None`
- `ExitTradeAction.__post_init__`: delegates to `make_list` (branches counted in `backtest_utils`)
- `ExitAllPositionsAction.__post_init__`: 0 additional branches (just sets `_calc_type`)
- Mocking notes: All subclass `__post_init__` methods call `make_list` and `.clone()` on priceables. Tests need mocked `Priceable`/`Instrument` objects with `.name`, `.clone()`, and `.unresolved` attributes. `HedgeAction` additionally needs `Portfolio` iteration support. Global `action_count` must be reset between tests for deterministic naming.
- Pragmas: none
