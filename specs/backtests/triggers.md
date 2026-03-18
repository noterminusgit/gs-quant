# triggers.py

## Summary
Trigger framework: TriggerRequirements (base), TriggerInfo, Trigger (base), and many concrete trigger types. Each has has_triggered(state, backtest) → TriggerInfo.

## Key Types

### TriggerInfo
- triggered: bool, info_dict: Optional[dict]
- __eq__: compares triggered with `is` (identity check with bool)
- __bool__: returns triggered

### check_barrier(direction, test_value, trigger_level)
- ABOVE: test_value > level
- BELOW: test_value < level
- EQUAL: test_value == level

### PeriodicTriggerRequirements
- get_trigger_times: lazy RelativeDateSchedule
- has_triggered: checks state in trigger_dates, provides next_state

### IntradayTriggerRequirements
- __post_init__: generates time series at frequency intervals
- has_triggered: state.time() in _trigger_times

### MktTriggerRequirements
- has_triggered: gets data, calls check_barrier, TypeError → RuntimeError

### RiskTriggerRequirements
- has_triggered: checks backtest.results, optionally transforms risk
- calc_type: path_dependent

### AggregateTriggerRequirements
- __setattr__: if triggers are Trigger objects, extract trigger_requirements
- has_triggered: ALL_OF (all must trigger), ANY_OF (at least one)
- calc_type: most complex of children

### NotTriggerRequirements
- Inverts child trigger result

### DateTriggerRequirements
- entire_day: converts datetimes to dates for comparison
- has_triggered: checks sorted dates, provides next_state

### PortfolioTriggerRequirements
- data_source == 'len' → len(backtest.portfolio_dict) vs trigger_level

### MeanReversionTriggerRequirements
- Z-score based entry/exit logic
- Bug: line 368 uses `self._current_position = 0` (underscore prefix) instead of `self.current_position = 0`

### TradeCountTriggerRequirements
- len(backtest.portfolio_dict.get(state, [])) vs trade_count

### EventTriggerRequirements
- Lazy loads from MACRO_EVENTS_CALENDAR dataset
- list_events static method

### Trigger (base)
- delegates has_triggered to trigger_requirements
- risks: collects from actions

### OrdersGeneratorTrigger
- Creates default Action if none provided
- has_triggered: checks time, generates orders

## Bugs Found
- MeanReversionTriggerRequirements line 368: `self._current_position = 0` should be `self.current_position = 0` (missing underscore creates a new attribute instead of updating the existing one)

## Coverage Notes
- ~80 branches total
- PeriodicTriggerRequirements needs RelativeDateSchedule mock
- EventTriggerRequirements needs Dataset mock
