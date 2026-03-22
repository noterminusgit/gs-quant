# Module Specifications

Spec files mirror production source files and document logic, branches, edge cases, and bugs.

## Format Template

```markdown
# module_name.py

## Summary
1-3 sentences on module purpose.

## Dependencies
- Internal: ...
- External: ...

## Classes/Functions

### ClassName / function_name
1. Branch point 1: condition → outcome A / outcome B
2. Branch point 2: ...

## Edge Cases
- ...

## Bugs Found
- Line X: description (FIXED / OPEN)

## Coverage Notes
- Branch count: N
- Pragmas needed: ...
```

For files < 30 lines or pure data/enum files, use lightweight format (summary + branch list only).

## Scope

Specs were written for **Phase 6 (Analytics)** and **Phase 7 (Backtests)** — the two most complex modules that benefited from upfront analysis before test writing.

Phase 8+ files did not need specs because tests were written directly from coverage gap reports (missing branch numbers from `coverage.json`). The coverage report itself served as the spec.

Empty subdirectories (`api/`, `config/`, `data/`, `datetime/`, `entities/`, `instrument/`, `interfaces/`, `markets/`, `models/`, `quote_reports/`, `risk/`, `root/`, `timeseries/`, `tracing/`, `workflow/`) exist in the directory structure but contain no spec files — these modules were covered without needing formal specs.

## Progress

### Phase 6: Analytics (20 files)
| File | Spec | Bugs | Tests |
|------|------|------|-------|
| analytics/common/enumerators.py | done | - | done |
| analytics/common/constants.py | done | - | done |
| analytics/core/processor_result.py | done | - | done |
| analytics/common/helpers.py | done | - | done |
| analytics/datagrid/serializers.py | done | - | done |
| analytics/datagrid/utils.py | done | - | done |
| analytics/datagrid/data_cell.py | done | - | done |
| analytics/datagrid/data_column.py | done | - | done |
| analytics/datagrid/data_row.py | done | Bug 3 | done |
| analytics/core/query_helpers.py | done | - | done |
| analytics/processors/statistics_processors.py | done | - | done |
| analytics/processors/scale_processors.py | done | - | done |
| analytics/processors/econometrics_processors.py | done | - | done |
| analytics/processors/utility_processors.py | done | - | done |
| analytics/processors/analysis_processors.py | done | - | done |
| analytics/processors/special_processors.py | done | - | done |
| analytics/core/processor.py | done | Bug 1 (FIXED) | done |
| analytics/workspaces/components.py | done | Bug 4 | done |
| analytics/workspaces/workspace.py | done | - | done |
| analytics/datagrid/datagrid.py | done | - | done |

### Phase 7: Backtests (18 files)
| File | Spec | Bugs | Tests |
|------|------|------|-------|
| backtests/action_handler.py | done | - | done |
| backtests/actions.py | done | Bug 5 | done |
| backtests/backtest_engine.py | done | - | done |
| backtests/backtest_objects.py | done | Bug 2 (FIXED) | done |
| backtests/backtest_utils.py | done | Bug 9 | done |
| backtests/core.py | done | - | done |
| backtests/data_handler.py | done | Bug 7 | done |
| backtests/data_sources.py | done | - | done |
| backtests/decorator.py | done | - | done |
| backtests/equity_vol_engine.py | done | - | done |
| backtests/event.py | done | - | done |
| backtests/execution_engine.py | done | - | done |
| backtests/generic_engine.py | done | Bug 10 | done |
| backtests/order.py | done | Bug 6 | done |
| backtests/predefined_asset_engine.py | done | - | done |
| backtests/strategy.py | done | - | done |
| backtests/strategy_systematic.py | done | Bug 8 | done |
| backtests/triggers.py | done | - | done |

### Phase 8: Large files
Tests written directly from coverage gap reports — no specs needed. See [coverage-phases5-8.md](../plan/coverage-phases5-8.md) for the full file list.
