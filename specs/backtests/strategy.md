# strategy.py

## Summary
Strategy dataclass: holds initial_portfolio, triggers, cash_accrual, risks. Supports finding compatible backtest engines.

## Functions

### _backtest_engines()
Lazy import of EquityVolEngine, GenericEngine, PredefinedAssetEngine. Returns list of instances.

## Class: Strategy

### __post_init__
1. If initial_portfolio is not dict → make_list
2. triggers = make_list(triggers)
3. risks = get_risks()

### get_risks()
Collects all risks from triggers (flattened).

### get_available_engines()
Filters _backtest_engines() by engine.supports_strategy(self).

## Edge Cases
- initial_portfolio as dict → preserved as-is
- triggers with no risks → empty risk list

## Bugs Found
None.

## Coverage Notes
- ~6 branches
