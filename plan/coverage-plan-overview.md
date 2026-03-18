# Coverage Plan Overview

## Phases Summary
| Phase | Focus | Status |
|-------|-------|--------|
| 0 | Coverage infrastructure (.coveragerc, pyproject.toml, baseline) | Not started |
| 1 | Root-level utility modules (errors, json_*, context_base, priceable, base) | Not started |
| 2 | Small untested modules (algebra, workflow, quote_reports) | Not started |
| 3 | Risk module (core, result_handlers, scenarios, scenario_utils, transform) | Not started |
| 4 | Instrument module (core.py) | Not started |
| 5 | Session module (session.py, 1061 lines) | Not started |
| 6 | Analytics module (processors, components, datagrid, query_helpers) | Not started |
| 7 | Backtests module (actions, orders, data_sources, strategy) | Not started |
| 8 | Deepen existing coverage (measures.py, risk_model, optimizer, etc.) | Not started |
| 9 | Shared test infrastructure (conftest files, factories, async helpers) | Not started |
| 10 | Final push (pragma annotations, fail_under=100) | Not started |

## Risks & Mitigations
- `gs_quant_internal` imports: not available open-source -> `# pragma: no cover`
- Global state pollution (`GsSession`, `PricingContext`): use `yield` fixtures with cleanup
- Async tests: use `pytest-asyncio` with deterministic mocks, no real network
- Large files (measures.py at 6K lines): prioritize by actual branch count from coverage report

## Verification Strategy
- Per-phase: `pytest --cov=gs_quant --cov-branch --cov-report=term-missing`
- HTML review: `htmlcov/index.html` for uncovered branches
- Module tracking: `--cov-report=json` for per-file coverage
- Ratchet `fail_under` upward after each phase
