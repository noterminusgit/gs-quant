# actions.py

## Summary
Action classes for backtest triggers: Action (base), AddTradeAction, AddScaledTradeAction, EnterPositionQuantityScaledAction, ExitPositionAction, ExitTradeAction, ExitAllPositionsAction, HedgeAction, RebalanceAction. Global action_count for auto-naming.

## Key Patterns
- All use @dataclass_json @dataclass
- All have __post_init__ that calls super().__post_init__() and names priceables
- Priceable naming: None → f'{name}_Priceable{i}', starts with name → keep, else → f'{name}_{p.name}'
- transaction_cost_exit defaults to transaction_cost if None

## Classes

### Action (base)
- set_name: if name is None → 'Action{count}' with global counter
- sub_classes registry

### AddTradeAction
- Stores priceables, trade_duration, holiday_calendar
- _dated_priceables dict for state-specific trades

### AddScaledTradeAction
- Like AddTradeAction + scaling_type, scaling_risk, scaling_level

### EnterPositionQuantityScaledAction
- Like AddTradeAction + trade_quantity, trade_quantity_type

### HedgeAction
- Converts priceables to Portfolio if not already
- _calc_type = semi_path_dependent
- Warns on deprecated scaling_parameter
- Bug note: `if not Portfolio` on line 380 — always True since Portfolio is a class, not the instance

### RebalanceAction
- _calc_type = path_dependent
- Requires priceable.unresolved to exist

## Edge Cases
- Global action_count increments across all actions in process
- HedgeAction line 380: `if not Portfolio` is always False (Portfolio is a truthy class), so the RuntimeError never triggers

## Bugs Found
- Line 380: `if not Portfolio:` should be `if not portfolio:` (lowercase). The class Portfolio is always truthy, so this check is dead code.

## Coverage Notes
- ~30 branches
