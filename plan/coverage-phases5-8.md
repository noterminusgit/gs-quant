# Phases 5-8: Advanced Module Coverage

## Phase 5: Session Module

| File | Lines | Test File |
|------|-------|-----------|
| `gs_quant/session.py` | 1,061 | Extend `test/test_session.py` |

Approach:
- Mock `requests.Session`, `httpx.AsyncClient`, `ssl` module
- Test `GsSession.__init__`: environment string matching, SSL version branching, proxy handling
- Test `_request_with_retries` (sync + async): success, JSON error, non-JSON error, msgpack
- Test `OAuth2Session._authenticate`, `KerberosSession`, `PassthroughSession`
- Use `pytest-asyncio` for async session methods

## Phase 6: Analytics Module

| File | Lines | New Test File |
|------|-------|---------------|
| `analytics/core/processor.py` | 436 | `test/analytics/test_processor.py` |
| `analytics/core/processor_result.py` | 26 | `test/analytics/test_processor_result.py` |
| `analytics/core/query_helpers.py` | 131 | `test/analytics/test_query_helpers.py` |
| `analytics/common/helpers.py` | 77 | `test/analytics/test_common.py` |
| `analytics/common/constants.py` | 41 | (same file) |
| `analytics/common/enumerators.py` | 23 | (same file) |
| `analytics/processors/*.py` | 1,782 | `test/analytics/test_*_processors.py` (6 files) |
| `analytics/workspaces/components.py` | 710 | `test/analytics/test_components.py` |
| `analytics/datagrid/*.py` | ~1,300 | Extend existing + new test files |

Approach:
- Processors: each has `process()` method transforming `pd.Series` — create test series, verify output
- `processor.py`: create concrete `BaseProcessor` subclass, mock `Entity`/`DataCoordinate`
- `components.py`: test component construction and serialization
- Create `test/analytics/conftest.py` with shared entity/coordinate fixtures

## Phase 7: Backtests Module

| File | Lines | New Test File |
|------|-------|---------------|
| `backtests/action_handler.py` | 49 | `test/backtest/test_action_handler.py` |
| `backtests/data_handler.py` | 82 | `test/backtest/test_data_handler.py` |
| `backtests/data_sources.py` | 218 | `test/backtest/test_data_sources.py` |
| `backtests/order.py` | 217 | `test/backtest/test_order.py` |
| `backtests/strategy.py` | 66 | `test/backtest/test_strategy.py` |
| `backtests/strategy_systematic.py` | 313 | `test/backtest/test_strategy_systematic.py` |
| `backtests/execution_engine.py` | 52 | `test/backtest/test_execution_engine.py` |
| `backtests/backtest_objects.py` | 798 | Extend existing tests |
| `backtests/actions.py` | 437 | Extend existing tests |
| `backtests/backtest_utils.py` | 127 | `test/backtest/test_backtest_utils.py` |

Approach:
- `data_handler.py`: test timezone handling (naive vs aware), lookahead error, UTC conversion
- `data_sources.py`: mock `Dataset` class, test `GsDataSource`/`GenericDataSource`
- `order.py`: test each order type's `execution_price`/`execution_quantity` with NaN edge cases
- Create `test/backtest/conftest.py` with mock instruments, data sources, pricing context

## Phase 8: Deepen Existing Coverage

Priority files by size and impact:

| File | Lines | Focus Areas |
|------|-------|-------------|
| `timeseries/measures.py` | 6,068 | Error paths, edge cases, optional param combos |
| `models/risk_model.py` | 3,317 | Various model types, error handling |
| `markets/optimizer.py` | 2,286 | Mocked API responses for optimizer configs |
| `markets/securities.py` | 2,165 | Query paths, identifier resolution |
| `markets/report.py` | 1,866 | Report creation, scheduling, result retrieval |
| `markets/position_set.py` | 1,618 | Position type handling, date validation |
| `api/gs/data.py` | 1,520 | Query construction, caching, response parsing |
| `markets/baskets.py` | 1,267 | Basket operations |
| `markets/hedge.py` | 1,041 | Hedging strategies |
| `risk/results.py` | 972 | PricingFuture callbacks, aggregation |
| `entities/entity.py` | 1,144 | Entity resolution, permissions |
| `data/dataset.py` | 934 | Dataset construction, query building |
| `tracing/tracing.py` | 707 | OpenTelemetry config branches |

Approach: use `pytest --cov-report=html` to identify specific uncovered branches, then write targeted tests.
