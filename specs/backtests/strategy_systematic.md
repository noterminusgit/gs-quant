# strategy_systematic.py

## Summary
Implements `StrategySystematic`, an equity vol systematic backtest strategy class that builds backtest parameters from underlier instruments, supports both the xasset backtesting service (`GsBacktestXassetApi`) and the legacy `GsBacktestApi`, handles delta hedging configuration, trade signal construction, transaction cost configuration, and instrument scaling/validation. The module also defines module-level constants for backtest type identification and ISO date format matching.

## Dependencies
- Internal: `gs_quant.api.gs.backtests` (`GsBacktestApi`), `gs_quant.api.gs.backtests_xasset.apis` (`GsBacktestXassetApi`), `gs_quant.api.gs.backtests_xasset.request` (`BasicBacktestRequest`), `gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes` (`DateConfig`, `Trade`, `Configuration`, `RollDateMode`, `TransactionCostConfig`, `StrategyHedge`), `gs_quant.backtests.core` (`Backtest`, `TradeInMethod`), `gs_quant.base` (`get_enum_value`, `Base`), `gs_quant.common` (`Currency`, `FieldValueMap`, `AssetClass`), `gs_quant.errors` (`MqValueError`), `gs_quant.instrument` (`EqOption`, `EqVarianceSwap`, `Instrument`), `gs_quant.target.backtests` (`BacktestResult`, `BacktestRisk`, `BacktestTradingQuantityType`, `DeltaHedgeParameters`, `BacktestSignalSeriesItem`, `BacktestStrategyUnderlier`, `BacktestStrategyUnderlierHedge`, `EquityMarketModel`, `BacktestTradingParameters`, `FlowVolBacktestMeasure`)
- External: `datetime` (`dt.date`, `dt.timedelta`), `logging` (`getLogger`), `typing` (`Iterable`, `Union`, `Tuple`), `gs_quant.target.backtests` (as `backtests` -- used for dynamic `getattr`)

## Type Definitions

### StrategySystematic (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _supported_eq_instruments | `tuple` (class var) | `(EqOption, EqVarianceSwap)` | Tuple of supported equity instrument types |
| __cost_netting | `bool` | `False` | Whether to net transaction costs |
| __currency | `Currency` | `Currency.USD` | Backtest currency (resolved via `get_enum_value`) |
| __name | `Optional[str]` | `None` | Strategy name |
| __backtest_type | `str` | `'VolatilityFlow'` | Backtest type identifier (always `BACKTEST_TYPE_NAME`) |
| __cash_accrual | `bool` | `True` | Whether to accrue cash |
| __trading_parameters | `BacktestTradingParameters` | *(constructed)* | Trading parameters (quantity, type, method, frequency, signals, roll mode) |
| __underliers | `list[BacktestStrategyUnderlier]` | `[]` | List of strategy underliers with instrument, notional percentage, hedge, and market model |
| __trades | `tuple[Trade]` | *(constructed)* | Trade specification tuple for xasset service |
| __hedge_params | `Optional[StrategyHedge]` | `None` | Delta hedge parameters for xasset service |
| __transaction_cost_config | `Optional[TransactionCostConfig]` | `None` | Transaction cost configuration |
| __xasset_bt_service_config | `Configuration` | *(constructed)* | Xasset backtest service configuration |
| __backtest_parameters | `Base` | *(constructed)* | Legacy backtest parameters (dynamic class from `backtests` module) |
| __use_xasset_backtesting_service | `bool` | `True` | Whether to use xasset service or legacy API |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| BACKTEST_TYPE_NAME | `str` | `'VolatilityFlow'` | Type name used for legacy parameter class lookup |
| BACKTEST_TYPE_VALUE | `str` | `'Volatility Flow'` | Type value used when constructing `Backtest` object |
| ISO_FORMAT | `str` | `r"^([0-9]{4})-([0-9]{2})-([0-9]{2})$"` | ISO date format regex (defined but not used in this module) |
| _logger | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger (defined but not used in this module) |

## Functions/Methods

### StrategySystematic.__init__(self, underliers: Union[Instrument, Iterable[Instrument]], quantity: float = 1, quantity_type: Union[BacktestTradingQuantityType, str] = BacktestTradingQuantityType.notional, trade_in_method: Union[TradeInMethod, str] = TradeInMethod.FixedRoll, roll_frequency: str = None, scaling_method: str = None, index_initial_value: float = 0.0, delta_hedge: DeltaHedgeParameters = None, name: str = None, cost_netting: bool = False, currency: Union[Currency, str] = Currency.USD, trade_in_signals: Tuple[BacktestSignalSeriesItem, ...] = None, trade_out_signals: Tuple[BacktestSignalSeriesItem, ...] = None, market_model: Union[EquityMarketModel, str] = EquityMarketModel.SFK, roll_date_mode: str = None, expiry_date_mode: str = None, cash_accrual: bool = True, combine_roll_signal_entries: bool = False, transaction_cost_config: TransactionCostConfig = None, use_xasset_backtesting_service: bool = True) -> None
Purpose: Initialize the strategy, process underliers, build trade and hedge configurations for both xasset and legacy services.

**Algorithm:**
1. Store scalar config: `__cost_netting`, `__currency` (via `get_enum_value`), `__name`, `__backtest_type = BACKTEST_TYPE_NAME`, `__cash_accrual`
2. Resolve `trade_in_method` and `market_model` via `get_enum_value`, extract `.value`
3. Construct `__trading_parameters = BacktestTradingParameters(...)` with resolved values
4. Initialize `__underliers = []`, `trade_instruments = []`
5. Define local helper `is_unsupported_eq_instrument(inst)`: returns `True` if class name starts with `'Eq'` but is not in `_supported_eq_instruments`
6. Branch: `isinstance(underliers, Iterable)` ->
   a. For each `underlier`:
      - Branch: `isinstance(underlier, tuple)` -> `instrument = underlier[0]`, `notional_percentage = underlier[1]`
      - Branch: else -> `instrument = underlier`, `notional_percentage = 100`
      - Branch: `is_unsupported_eq_instrument(instrument)` -> raise `MqValueError('The format of the backtest asset is incorrect.')`
      - Branch: else -> `instrument = instrument.scale(notional_percentage / 100, in_place=False, check_resolved=False)`
      - Append instrument to `trade_instruments`
      - Append `BacktestStrategyUnderlier(...)` to `__underliers`
7. Branch: else (single instrument) ->
   - `instrument = underliers`
   - Branch: `is_unsupported_eq_instrument(instrument)` -> raise `MqValueError`
   - `notional_percentage = 100`
   - Append to `trade_instruments` and `__underliers`
8. Build xasset trade signals:
   - Branch: `trade_in_signals is not None` -> `trade_buy_dates = tuple(s.date for s in trade_in_signals if s.value)`
   - Branch: else -> `trade_buy_dates = None`
   - Branch: `trade_out_signals is not None` -> `trade_exit_dates = tuple(s.date for s in trade_out_signals if s.value)`
   - Branch: else -> `trade_exit_dates = None`
9. Construct `__trades` tuple with single `Trade(...)` entry
10. Branch: `delta_hedge` is truthy ->
    a. Create `__hedge_params = StrategyHedge()`
    b. Branch: `delta_hedge.frequency` is truthy ->
       - Branch: `delta_hedge.frequency == 'Daily'` -> set `__hedge_params.frequency = '1b'`
       - Branch: else -> set `__hedge_params.frequency = delta_hedge.frequency`
    c. Branch: `delta_hedge.notional` is truthy -> set `__hedge_params.risk_percentage = delta_hedge.notional`
11. Branch: `delta_hedge` is falsy -> `__hedge_params = None`
12. Store `__transaction_cost_config`
13. Construct `__xasset_bt_service_config = Configuration(...)`:
    - Branch: `roll_date_mode is not None` -> `RollDateMode(roll_date_mode)`; else -> `None`
    - Branch: `market_model` is truthy -> `EquityMarketModel(market_model)`; else -> `None`
14. Build legacy backtest parameters via dynamic `getattr(backtests, self.__backtest_type + 'BacktestParameters')` -> `.from_dict(...)`
15. Store `__use_xasset_backtesting_service`

### StrategySystematic.__run_service_based_backtest(self, start: dt.date, end: dt.date, measures: Iterable[FlowVolBacktestMeasure]) -> BacktestResult
Purpose: Execute a backtest via the xasset backtesting service API and parse the response into a `BacktestResult`.

**Algorithm:**
1. Create `date_cfg = DateConfig(start, end)`
2. Branch: `not measures` (falsy) -> default to `(FlowVolBacktestMeasure.PNL,)`
3. Construct `BasicBacktestRequest` with dates, trades, measures, transaction_costs, configuration, hedge
4. Call `GsBacktestXassetApi.calculate_basic_backtest(basic_bt_request, decode_instruments=False)`
5. Build `risks` tuple from `basic_bt_response.measures`: for each `(k, v)` -> `BacktestRisk(name=k.value, timeseries=...)`
6. Initialize `events = []`
7. Branch: `basic_bt_response.additional_results is not None` ->
   a. Branch: `basic_bt_response.additional_results.trade_events is not None` -> append `BacktestRisk(name="trade_events", ...)` to events
   b. Branch: `basic_bt_response.additional_results.hedge_events is not None` -> append `BacktestRisk(name="hedge_events", ...)` to events
8. Build `portfolio` list: for each date `d` in sorted union of portfolio and transaction keys:
   a. Branch: `d in basic_bt_response.portfolio` -> build positions list with instrument and quantity
   b. Branch: else -> `positions = []`
   c. Branch: `d in basic_bt_response.transactions` -> for each transaction, build trades list:
      - Branch: `t.portfolio is not None` -> map instruments to trade dicts with price and quantity
      - Branch: else -> `trades = []`
   d. Append `{'date': d, 'positions': positions, 'transactions': transactions}`
9. Return `BacktestResult(risks=risks, events=tuple(events), portfolio=portfolio)`

### StrategySystematic.__position_quantity(self, instrument: dict) -> Optional[float]
Purpose: Compute the signed position quantity from an instrument dict.

**Algorithm:**
1. Branch: `instrument.get('assetClass') == AssetClass.Equity.value` ->
   a. `direction`: Branch: `instrument['buySell'] == 'Buy'` -> `1`; else -> `-1`
   b. `quantity`: Branch: `instrument['type'] == 'Option'` -> `instrument['numberOfOptions']`; else -> `instrument['quantity']`
   c. Return `direction * quantity`
2. Branch: else -> return `None`

### StrategySystematic.backtest(self, start: dt.date = None, end: dt.date = dt.date.today() - dt.timedelta(days=1), is_async: bool = False, measures: Iterable[FlowVolBacktestMeasure] = (FlowVolBacktestMeasure.ALL_MEASURES,), correlation_id: str = None) -> Union[Backtest, BacktestResult]
Purpose: Execute the backtest via either xasset service or legacy API.

**Algorithm:**
1. Branch: `self.__use_xasset_backtesting_service` -> return `self.__run_service_based_backtest(start, end, measures)`
2. Branch: else (legacy path) ->
   a. Build `params_dict` from `self.__backtest_parameters.as_dict()`
   b. Add measures values to `params_dict['measures']`
   c. Reconstruct parameters via dynamic class `from_dict`
   d. Construct `Backtest` object with name, parameters, dates, type, asset class, currency, cost netting, cash accrual
   e. Branch: `is_async` ->
      - Call `GsBacktestApi.create_backtest(backtest)` -> `response`
      - Call `GsBacktestApi.schedule_backtest(backtest_id=response.id)`
   f. Branch: else ->
      - Call `GsBacktestApi.run_backtest(backtest, correlation_id)` -> `response`
   g. Return `response`

## State Mutation
- `self.__underliers`: Built incrementally during `__init__` by appending `BacktestStrategyUnderlier` objects; immutable after construction
- `self.__hedge_params`: Conditionally created in `__init__` based on `delta_hedge`; attributes `frequency` and `risk_percentage` set conditionally; immutable after construction
- `self.__trades`: Constructed once in `__init__`; immutable after
- `self.__backtest_parameters`: Constructed once via dynamic `from_dict`; immutable after
- `instrument` (local in `__init__`): Scaled in-place via `.scale(..., in_place=False)` -- actually creates a new instrument, so original is not mutated
- `trade_instruments` (local): Built during `__init__`, used only for `Trade` construction
- Thread safety: No shared mutable state; instances are not thread-safe if `backtest()` is called concurrently on the same instance (but there is no reason to do so)

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `__init__` | When an underlier instrument's class name starts with `'Eq'` but is not `EqOption` or `EqVarianceSwap` |
| `KeyError` (potential) | `__position_quantity` | When `instrument` dict lacks `'buySell'`, `'type'`, `'numberOfOptions'`, or `'quantity'` keys for an equity instrument |
| `AttributeError` (potential) | `__init__` | If `getattr(backtests, self.__backtest_type + 'BacktestParameters')` fails because the class doesn't exist |
| API errors | `__run_service_based_backtest`, `backtest` | When `GsBacktestXassetApi.calculate_basic_backtest` or `GsBacktestApi` methods fail due to network/auth issues |

## Edge Cases
- Single underlier (non-iterable) vs iterable: the `isinstance(underliers, Iterable)` check handles both paths, but a single `Instrument` that happens to be iterable (e.g., a `Portfolio`) would take the iterable path
- Tuple underliers: `(instrument, percentage)` tuples allow per-instrument notional scaling; non-tuple items default to 100%
- `is_unsupported_eq_instrument` checks if class name starts with `'Eq'` but type is not in `_supported_eq_instruments`; this means custom `Eq*` subclasses not in the tuple are rejected
- `trade_in_signals` / `trade_out_signals` filtering: only signals where `s.value` is truthy are included in buy/exit dates
- `delta_hedge.frequency == 'Daily'` is converted to `'1b'` for the xasset service; all other frequency strings pass through unchanged
- `delta_hedge.notional` and `delta_hedge.frequency` are each checked for truthiness independently; either or both could be falsy
- `__run_service_based_backtest` defaults `measures` to `(FlowVolBacktestMeasure.PNL,)` when the input is falsy (empty tuple, None, etc.)
- `__position_quantity` returns `None` for non-equity instruments; callers must handle `None` quantities
- `__position_quantity` uses `instrument['type'] == 'Option'` to decide between `numberOfOptions` and `quantity` -- any non-`'Option'` type uses `quantity`
- Legacy backtest path uses `getattr(backtests, self.__backtest_type + 'BacktestParameters')` which is a dynamic class lookup; if `BACKTEST_TYPE_NAME` were changed, this would raise `AttributeError`
- `ISO_FORMAT` constant and `_logger` are defined but never used in this module
- `__run_service_based_backtest` handles `None` instruments in portfolio by substituting `{}` empty dict
- In `__run_service_based_backtest`, `t.quantity` is checked for `None` and passed through as `None` if so

## Bugs Found
- None confirmed. The `ISO_FORMAT` and `_logger` definitions are unused but not bugs per se.

## Coverage Notes
- Branch count: ~40
- `__init__`: ~15 branches (Iterable check, tuple check, unsupported instrument check x2 paths, trade_in_signals None check, trade_out_signals None check, delta_hedge truthy, delta_hedge.frequency truthy, frequency == 'Daily', delta_hedge.notional truthy, roll_date_mode None, market_model truthy)
- `__run_service_based_backtest`: ~8 branches (measures falsy, additional_results None, trade_events None, hedge_events None, d in portfolio, d in transactions, t.portfolio None, instrument None substitution)
- `__position_quantity`: 4 branches (assetClass == Equity, buySell == Buy, type == Option, fallthrough return None)
- `backtest`: 3 branches (use_xasset, is_async, else sync)
- Mocking notes: `GsBacktestApi` and `GsBacktestXassetApi` must be mocked to avoid real API calls. `get_enum_value` is used for `Currency`, `TradeInMethod`, `EquityMarketModel`, `BacktestTradingQuantityType`. Dynamic `getattr` on `backtests` module needs the `VolatilityFlowBacktestParameters` class to exist. `instrument.scale()` needs to be mockable. `BacktestResult`, `BacktestRisk`, `FieldValueMap` from `gs_quant.target.backtests` are used to construct return values.
- Pragmas: none
