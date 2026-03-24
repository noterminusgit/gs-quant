# strategy.py

## Summary
Defines the `Strategy` dataclass which holds an initial portfolio, triggers, cash accrual model, and computed risks. Provides engine discovery via `get_available_engines()` which lazily imports and filters compatible backtest engines. The `_backtest_engines()` module-level function performs lazy imports to avoid circular dependencies.

## Dependencies
- Internal: `gs_quant.backtests.backtest_objects` (CashAccrualModel)
- Internal: `gs_quant.backtests.backtest_utils` (make_list)
- Internal: `gs_quant.backtests.triggers` (Trigger)
- Internal: `gs_quant.base` (Priceable)
- Internal: `gs_quant.json_convertors` (decode_named_instrument, encode_named_instrument, dc_decode)
- Internal (lazy): `gs_quant.backtests.equity_vol_engine` (EquityVolEngine)
- Internal (lazy): `gs_quant.backtests.generic_engine` (GenericEngine)
- Internal (lazy): `gs_quant.backtests.predefined_asset_engine` (PredefinedAssetEngine)
- External: `dataclasses` (dataclass, field)
- External: `dataclasses_json` (dataclass_json, config)
- External: `typing` (Tuple, Optional, Union, Iterable)

## Type Definitions

### Strategy (dataclass)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| initial_portfolio | `Optional[Union[Tuple[Priceable, ...], dict]]` | `None` | Initial holdings; dict preserves date-keyed portfolios |
| triggers | `Union[Trigger, Iterable[Trigger]]` | `None` | Trigger(s) defining strategy logic |
| cash_accrual | `CashAccrualModel` | `None` | Model for cash accrual between dates |
| risks | `Any` | `None` | Computed list of risk measures (set in __post_init__) |

## Enums and Constants
None.

## Functions/Methods

### _backtest_engines() -> List[BacktestBaseEngine]
Purpose: Lazily import and instantiate all available backtest engines.

**Algorithm:**
1. Import `EquityVolEngine`, `GenericEngine`, `PredefinedAssetEngine` (lazy, avoids circular imports)
2. Return `[GenericEngine(), PredefinedAssetEngine(), EquityVolEngine()]`

### Strategy.__post_init__(self) -> None
Purpose: Normalize fields and compute risks from triggers.

**Algorithm:**
1. Branch: `not isinstance(self.initial_portfolio, dict)` -> `self.initial_portfolio = make_list(self.initial_portfolio)`
2. Branch: else (is dict) -> preserve as-is
3. `self.triggers = make_list(self.triggers)`
4. `self.risks = self.get_risks()`

### Strategy.get_risks(self) -> List[RiskMeasure]
Purpose: Collect all risk measures from all triggers' actions.

**Algorithm:**
1. Initialize `risk_list = []`
2. For each trigger `t` in `self.triggers`:
   a. Branch: `t.risks is not None` -> extend `risk_list` with `t.risks`
   b. Branch: `t.risks is None` -> extend with `[]`
3. Return `risk_list`

### Strategy.get_available_engines(self) -> List[BacktestBaseEngine]
Purpose: Return engines that support this strategy.

**Algorithm:**
1. Call `_backtest_engines()` to get all engines
2. Filter by `engine.supports_strategy(self)` for each
3. Return filtered list

## State Mutation
- `self.initial_portfolio`: Converted from single/None to list in `__post_init__` unless it is a dict
- `self.triggers`: Converted from single/None to list in `__post_init__`
- `self.risks`: Computed from triggers in `__post_init__`

## Error Handling
None raised directly. Errors may propagate from `make_list` or engine `supports_strategy`.

## Edge Cases
- `initial_portfolio` as dict is preserved as-is (not wrapped in a list)
- `initial_portfolio` as `None` is converted to `[]` by `make_list`
- `triggers` with no risks on any action -> empty risk list
- `_backtest_engines()` performs lazy imports each time it is called
- `EquityVolEngine` is instantiated as a plain object (not via `cls()`) since it uses classmethods

## Bugs Found
None.

## Coverage Notes
- Branch count: ~6
- Key branches: initial_portfolio dict vs non-dict (2), trigger.risks None vs not-None (2 per trigger), engine supports_strategy True vs False
