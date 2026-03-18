# predefined_asset_engine.py

## Summary
PredefinedAssetEngine: event-driven backtest engine for predefined assets. Handles action dispatch, timer generation, calendar-aware date management, and the main event loop.

## Classes

### AddTradeActionImpl(ActionHandler)
- generate_orders: for each priceable, create OrderAtMarket; if trade_duration is timedelta, also create close order
- apply_action: calls generate_orders

### SubmitOrderActionImpl
- apply_action: returns info directly (passthrough)

### PredefinedAssetEngineActionFactory
- get_action_handler: lookup in action_impl_map, RuntimeError if not found

### PredefinedAssetEngine
- __init__: sets up data_handler, default action_impl_map
- _eod_valuation_time: window.end if exists, else dt.time(23)
- _timer: generates all datetime states from date range × trigger times + eod time; handles calendars
- _adjust_date: moves to nearest business day
- run_backtest: initializes, creates timer, runs event loop
- _run: main loop:
  1. For each state: update data_handler, check fills, generate events
  2. Process events: Market → check triggers → generate orders; Order → submit; Fill → update; Valuation → mark_to_market

## Edge Cases
- states parameter overrides timer generation
- Calendar handling: 'weekend' → use None calendar
- tz-aware dates in timer: combine with tzinfo
- Action type not in impl_map → RuntimeError

## Bugs Found
None.

## Coverage Notes
- ~30 branches
- Needs mock DataManager, DataHandler
