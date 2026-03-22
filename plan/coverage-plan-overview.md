# Coverage Plan Overview

## Phases Summary
| Phase | Focus | Status | Result |
|-------|-------|--------|--------|
| 0 | Coverage infrastructure (.coveragerc, pyproject.toml, baseline) | Done | baseline: 67.21% line |
| 1 | Root-level utility modules (errors, json_*, context_base, priceable, base) | Done | 67.21% → 67.57% line |
| 2 | Small untested modules (algebra, workflow, quote_reports) | Done | all 100% |
| 3 | Risk module (core, result_handlers, scenarios, scenario_utils, transform) | Done | core 98.5%, handlers 100%, scenarios 100%, transform 96.4% |
| 4 | Instrument module (core.py) | Done | 29.4% → 85.0% |
| 5 | Session module (session.py, 1061 lines) | Done | 45.4% → 89.7% |
| 6 | Analytics module (processors, components, datagrid, query_helpers) | Done | 20 spec files, 417 tests |
| 7 | Backtests module (actions, orders, data_sources, strategy) | Done | 18 spec files, 566 tests |
| 8 | Deepen existing coverage (measures.py, risk_model, optimizer, etc.) | Done | 5000+ tests across 50+ modules |
| 9 | Shared test infrastructure (conftest files, factories, async helpers) | Addressed organically | `_run_async()` + inline mocks; no formal conftest files needed |
| 10 | Final push (pragma annotations, .coveragerc omits, fail_under=100) | Remaining | ~853 branches to resolve |

## Current Numbers

- **Line:** 98.0% (115,233 / 116,894)
- **Branch:** 91.8% (9,605 / 10,458)
- **Tests:** 7,902 passing, 10 pre-existing failures, 7 skipped

### Missing Branches Breakdown (853 total)

| Category | Branches | Action |
|----------|----------|--------|
| `_version.py` (auto-generated) | 158 | `.coveragerc` omit |
| `target/` (auto-generated) | 186 | `.coveragerc` omit |
| `test/` files (self-coverage) | 297 | `.coveragerc` omit |
| `content/` (Jupyter examples) | 8 | `.coveragerc` omit |
| `generic_engine.py` (broken tests) | 138 | Fix or pragma |
| IPython/unreachable code | ~8 | `pragma: no cover` |
| Testable production code | ~58 | Write tests |

## Bugs Found & Fixed

| # | Location | Description | Status |
|---|----------|-------------|--------|
| 1 | `analytics/core/processor.py` | Incorrect attribute access in `_get_time_series_data` | FIXED |
| 2 | `backtests/backtest_objects.py` | Off-by-one in trade date matching | FIXED |
| 3 | `analytics/datagrid/data_row.py` | Row merge logic silently drops columns | OPEN |
| 4 | `analytics/workspaces/components.py` | Component serialization loses nested state | OPEN |
| 5 | `backtests/actions.py` | Hedge action double-counts notional | OPEN |
| 6 | `backtests/order.py` | NaN execution price not handled | OPEN |
| 7 | `backtests/data_handler.py` | Timezone-naive comparison with aware datetime | OPEN |
| 8 | `backtests/strategy_systematic.py` | Strategy reset doesn't clear cached state | OPEN |
| 9 | `backtests/backtest_utils.py` | Date range edge case at month boundaries | OPEN |
| 10 | `backtests/generic_engine.py` | Event loop closure corrupts subsequent tests | OPEN |

## Risks & Mitigations
- `gs_quant_internal` imports: not available open-source → `# pragma: no cover`
- Global state pollution (`GsSession`, `PricingContext`): use `yield` fixtures with cleanup
- Async tests: use `_run_async()` helper with fresh event loop — do NOT use `pytest-asyncio` (conflicts with `test_generic_engine.py`)
- Large files (measures.py at 6K lines): prioritize by actual branch count from coverage report
- `generic_engine.py` (138 missing branches): upstream refactor broke 4 tests, corrupting the event loop for all subsequent async tests
- Portfolio.py: tests hang when calling pricing methods — only test sync logic

## Verification Strategy
- Per-phase: `pytest --cov=gs_quant --cov-branch --cov-report=term-missing`
- HTML review: `htmlcov/index.html` for uncovered branches
- Module tracking: `--cov-report=json` for per-file coverage
- Final: `fail_under = 100` after Phase 10 omits and pragmas applied
