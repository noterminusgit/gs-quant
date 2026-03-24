# Dependency Graph — Elixir Port Reading Order

This documents the module dependency order for porting gs-quant to Elixir. Read and port modules from top to bottom — each layer only depends on layers above it.

## Layer 0: Foundation Types (Port First)

These define the core type system everything else depends on.

```
errors.py              → Elixir: defmodule GsQuant.Errors (custom exception structs)
context_base.py        → Elixir: Process dictionary / GenServer for context stack
json_encoder.py        → Elixir: Jason.Encoder implementations
json_convertors_common.py → Elixir: shared JSON encode/decode helpers
common.py              → Elixir: RiskMeasure struct, PayReceive enum
base.py                → Elixir: Base, InstrumentBase, RiskKey, Scenario (core structs)
priceable.py           → Elixir: Priceable behaviour
session.py             → Elixir: GenServer (GsSession singleton, HTTP transport)
json_convertors.py     → Elixir: JSON codec functions (depends on base.py types)
```

**Elixir design notes:**
- `context_base.py` uses thread-local storage + context manager → use Elixir process dictionary or a `Registry`-backed GenServer
- `session.py` is a global singleton with OAuth2/Kerberos auth → Elixir `GenServer` with `:via` tuple naming
- `base.py` defines `__init_subclass__` and metaclass magic for type registration → Elixir compile-time module attributes or Protocol

## Layer 1: Data Infrastructure

```
data/fields.py         → Field name/type definitions (simple data module)
data/query.py          → DataQuery struct
data/log.py            → DataSetLog (trivial)
data/stream.py         → WebSocket stream wrapper
data/coordinate.py     → DataCoordinate key type (depends on common enums)
data/core.py           → Base DataApi behaviour
data/utilities.py      → Data chunking/formatting helpers
data/dataset.py        → Dataset class (depends on all above)
```

## Layer 2: DateTime

```
datetime/time.py       → Time enum wrapper
datetime/rules.py      → Business day rules (RDateRule)
datetime/gscalendar.py → Holiday calendar (depends on rules)
datetime/date.py       → Business date math (depends on gscalendar, rules)
datetime/point.py      → Relative date point parsing
datetime/relative_date.py → RelativeDateSchedule (depends on date, point, rules)
```

**Elixir design notes:**
- Business day calculations → consider using a NIF wrapping Rust `chrono` or Elixir `Date` with holiday sets
- `RelativeDateSchedule` generates date sequences → lazy `Stream` in Elixir

## Layer 3: Risk Framework

```
risk/measures.py       → Risk measure type re-exports
risk/scenarios.py      → Scenario types (MarketDataShock, etc.)
risk/scenario_utils.py → Scenario builder helpers
risk/transform.py      → Result transformers
risk/result_handlers.py → Type-specific result parsing (DataFrame/scalar dispatch)
risk/results.py        → PricingFuture, PortfolioRiskResult, CompositeResultFuture
risk/core.py           → PricingContext, RiskApi orchestration
```

**Elixir design notes:**
- `PricingFuture` wraps `concurrent.futures.Future` → Elixir `Task` or `GenServer` holding async state
- `PortfolioRiskResult` supports lazy aggregation → Elixir could use `Stream` + `Enum.reduce`
- `CompositeResultFuture` chains multiple futures → `Task.async_stream` or supervised tasks
- `PricingContext` is a context manager that batches risk requests → GenServer with `handle_call` for pricing

## Layer 4: Entities & Instruments

```
entities/entity_utils.py  → Utility functions
entities/entitlements.py  → Entitlements management
entities/entity.py        → Entity base class (depends on session, base)
entities/tree_entity.py   → Tree-structured entities (depends on entity)

instrument/overrides.py   → Override helpers
instrument/core.py        → Instrument metaclass registration (depends on base, json_convertors)

interfaces/algebra.py     → Algebra mixin (add/subtract/multiply Series)
```

**Elixir design notes:**
- `Entity` uses `__init_subclass__` for auto-registration → Elixir `@behaviour` or module attribute accumulation
- `Instrument` metaclass creates typed builders → Elixir macros or `use` callback

## Layer 5: Markets

Port in this order within the layer:

```
markets/markets.py              → CloseMarket, OverlayMarket (depends on base, context_base)
markets/core.py                 → PricingContext, HistoricalPricingContext (depends on risk/core)
markets/historical.py           → HistoricalPricingFuture
markets/position_set_utils.py   → Position manipulation helpers
markets/position_set.py         → PositionSet, Position structs
markets/report_utils.py         → Report formatting
markets/report.py               → Report base class (depends on entity)
markets/factor.py               → Risk factor definitions
markets/factor_analytics.py     → Factor analytics computations
markets/scenario.py             → MarketDataScenario
markets/screens.py              → Screener framework
markets/portfolio.py            → Portfolio (depends on position_set, instrument)
markets/portfolio_manager.py    → PortfolioManager (depends on portfolio, report)
markets/portfolio_manager_utils.py → PM utilities
markets/index.py                → Index (depends on baskets)
markets/baskets.py              → Basket (depends on position_set, entity)
markets/indices_utils.py        → Index utilities
markets/hedge.py                → Hedging optimizer (depends on API)
markets/optimizer.py            → Portfolio optimizer (depends on API, position_set)
markets/securities.py           → SecurityMaster (depends on entity, API)
```

## Layer 6: API Layer

The API layer wraps HTTP calls. Port these after session.py (Layer 0) and entities (Layer 4).

```
api/api_session.py              → API session abstraction
api/api_cache.py                → Caching decorator
api/utils.py                    → Paginated request helpers

api/gs/assets.py                → Asset CRUD
api/gs/data.py                  → Market data queries (largest API file)
api/gs/secmaster.py             → Security master lookups
api/gs/risk.py                  → Risk calculation API
api/gs/portfolios.py            → Portfolio CRUD
api/gs/hedges.py                → Hedge computation API
api/gs/reports.py               → Report CRUD
api/gs/risk_models.py           → Risk model API
api/gs/federated_secmaster.py   → Federated SecMaster
api/gs/indices.py               → Index management
api/gs/backtests.py             → Backtest API
api/gs/scenarios.py             → Scenario API
api/gs/monitors.py              → Monitor API
api/gs/screens.py               → Screen API
api/gs/datagrid.py              → DataGrid API
api/gs/users.py                 → User API
api/gs/content.py               → Content API
api/gs/esg.py                   → ESG data API
api/gs/groups.py                → Groups API
api/gs/plots.py                 → Plots API
api/gs/carbon.py                → Carbon data API
api/gs/thematics.py             → Thematic baskets API
api/gs/price.py                 → Pricing API
api/gs/base_screener.py         → Base screener
api/gs/workspaces.py            → Workspaces API
api/gs/countries.py             → Countries API
api/gs/parser.py                → Parser API
api/gs/data_screen.py           → Data screen API

api/risk.py                     → Risk request/response serialization
api/fred/fred_query.py          → FRED query types
api/fred/data.py                → FRED data API

api/gs/backtests_xasset/        → Cross-asset backtest API (12 files, self-contained)
```

**Elixir design notes:**
- Each API module is stateless (class methods calling `GsSession.current`) → Elixir modules with functions taking a session pid/ref
- `GsDataApi` has 140+ QueryType enum members → large Elixir atom or string enum
- Pagination helpers → `Stream.resource` pattern

## Layer 7: Timeseries

All timeseries functions depend on pandas Series → Elixir equivalent needs a DataFrame library (Explorer/Nx).

```
timeseries/helper.py            → Plot decorator, normalization, apply_ramp
timeseries/measures_helper.py   → Shared measure helpers
timeseries/measure_registry.py  → Multi-measure registry

timeseries/algebra.py           → Series arithmetic (add, subtract, multiply)
timeseries/datetime.py          → Date-based series operations
timeseries/statistics.py        → Statistical functions (vol, corr, percentile)
timeseries/econometrics.py      → Econometric models (regression, PCA, GARCH)
timeseries/technicals.py        → Technical indicators (MA, Bollinger, RSI)
timeseries/analysis.py          → Time series analysis utilities
timeseries/backtesting.py       → Strategy backtesting on series

timeseries/measures.py          → Core market data measures (6080 LOC — largest file)
timeseries/measures_rates.py    → Interest rate measures
timeseries/measures_fx_vol.py   → FX volatility measures
timeseries/measures_xccy.py     → Cross-currency measures
timeseries/measures_inflation.py → Inflation measures
timeseries/measures_factset.py  → FactSet data measures
timeseries/measures_portfolios.py → Portfolio-level measures
timeseries/measures_reports.py  → Report-based measures
timeseries/measures_risk_models.py → Risk model measures
timeseries/measures_countries.py → Country-level measures
timeseries/tca.py               → Transaction cost analysis
```

**Elixir design notes:**
- `@plot_measure` decorator pattern → Elixir macro that wraps function with data retrieval + normalization
- pandas Series operations → Explorer.DataFrame or custom Series struct backed by Nx tensors
- Most measure functions follow: validate args → fetch data via API → transform → return Series

## Layer 8: Models

```
models/risk_model_utils.py      → Factor risk model utilities
models/risk_model.py            → RiskModel classes (3316 LOC)
models/epidemiology.py          → SIR/SEIR epidemic models
```

## Layer 9: Backtests (Port Last)

The backtest engine is the most complex subsystem — it orchestrates pricing, risk, portfolio management.

```
backtests/event.py              → Event types
backtests/order.py              → Order types
backtests/decorator.py          → @requires_auth decorator
backtests/action_handler.py     → ActionHandler base + factory
backtests/actions.py            → Action types (AddTrade, Hedge, Exit, Rebalance)
backtests/triggers.py           → Trigger types (Periodic, MktTrigger, Strategy, Agile)
backtests/backtest_utils.py     → Utility functions
backtests/data_sources.py       → Data source abstractions
backtests/data_handler.py       → DataHandler (caching data source)
backtests/backtest_objects.py   → BackTest state containers
backtests/backtest_engine.py    → BacktestBaseEngine
backtests/strategy.py           → Strategy definition
backtests/core.py               → Backtest orchestrator (entry point)
backtests/execution_engine.py   → Execution engine
backtests/strategy_systematic.py → Systematic strategy
backtests/generic_engine.py     → GenericEngine (main engine, most complex)
backtests/generic_engine_action_impls.py → Action implementations
backtests/equity_vol_engine.py  → Equity vol-specific engine
backtests/predefined_asset_engine.py → Predefined asset engine
```

**Elixir design notes:**
- `GenericEngine` maintains mutable state across dates → GenServer with date-stepping `handle_cast`
- Action/trigger dispatch → Protocol or behaviour callbacks
- Portfolio mutations → immutable map updates (Elixir's strength)
- Transaction cost tracking → accumulator in GenServer state

## Layer 10: Utility Modules

These can be ported at any time as they have minimal dependencies:

```
config/options.py               → Configuration options
quote_reports/core.py           → Quote report generation
tracing/tracing.py              → OpenTelemetry-style tracing
workflow/workflow.py             → Workflow state machine
```

## Cross-Cutting Elixir Patterns

| Python Pattern | Elixir Equivalent |
|---------------|-------------------|
| `threading.local()` context stack | Process dictionary or `Registry` |
| `concurrent.futures.Future` | `Task.async/await` |
| Context manager (`with`) | `try/after` or custom macro |
| Singleton session | Named `GenServer` |
| `@dataclass` | `defstruct` + `@type t :: %__MODULE__{}` |
| `Enum` class | Atom set or `@type` union |
| `isinstance` dispatch | Protocol implementation |
| `__init_subclass__` registration | `@behaviour` + module attribute accumulation |
| `pandas.Series` | `Explorer.Series` or custom NIF |
| `asyncio` event loop | OTP supervision tree |
| `@property` | Pattern match in function head |
| Class inheritance | Behaviours + composition (no inheritance) |
| Mutable dict/list state | Immutable maps in GenServer state |
| `try/except` chains | `with` + pattern matching |
| `@staticmethod` / `@classmethod` | Module functions |
| Decorator pattern (`@plot_measure`) | Macro (`defmeasure`) |
