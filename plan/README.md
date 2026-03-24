# gs-quant Project Plan

## Project Overview
- Goldman Sachs quantitative finance toolkit (open source)
- ~172 production files, ~67,500 LOC
- Auto-generated code in `gs_quant/target/` and `gs_quant/_version.py` — excluded from coverage
- Branch: master, remote: origin/master

## Completed: Elixir Port Readiness

### Goal
Make the codebase fully specified and tested so it can be ported to Elixir using specs and tests as the specification.

### What Was Done

**Phase 0: Preparation**
- Created enhanced spec template at `specs/TEMPLATE.md`
- Generated coverage gap inventory from `coverage.json` → `plan/coverage-gaps-inventory.md`

**Phase 1: Root Foundation (9 specs)**
- `base.py`, `session.py`, `common.py`, `context_base.py`, `errors.py`, `priceable.py`, `json_convertors.py`, `json_convertors_common.py`, `json_encoder.py`

**Phase 2: Data + DateTime (14 specs)**
- 8 data files + 6 datetime files

**Phase 3: Risk + Entities (11 specs)**
- 7 risk files + 4 entity files

**Phase 4: Markets (19 specs)**
- All 19 markets module files including optimizer (2286 LOC), securities (2163 LOC)

**Phase 5: Timeseries (21 specs)**
- All 21 timeseries files including measures.py (6080 LOC — largest file)

**Phase 6: Models (3 specs)**
- `risk_model.py` (3316 LOC), `epidemiology.py`, `risk_model_utils.py`

**Phase 7-8: API + Small Modules (~48 specs)**
- 38 API files + instrument, interfaces, config, quote_reports, tracing, workflow

**Phase 9: Enhance Existing Specs**
- Enhanced all 38 pre-existing analytics/backtests specs with type tables, typed signatures, dependencies

**Phase 10-11: Coverage Gap Closure**
- Tier 2: `api/gs/risk.py` (5 branches), `risk/results.py` (4), `markets/core.py` (3), `markets/factor_analytics.py` (3)
- Tier 3: 12 files with 2 missing branches each
- Tier 4: 20 files with 1 missing branch each
- Added `pragma: no cover` to unreachable IPython/PyXll/gs_quant_internal code
- Created `.coveragerc` with proper omit rules

**Phase 12: Documentation**
- Updated `specs/README.md` with full 172-file inventory
- Created `specs/DEPENDENCY_GRAPH.md` with 10-layer porting order + Elixir pattern mapping
- Updated this file

### Final State
- **172 spec files** covering all production modules
- **~8,000+ tests** passing
- **Branch coverage** significantly improved via targeted test additions
- Specs include: type definitions, function signatures, branch logic, state mutation, error handling, dependencies

### Artifacts
| Artifact | Path |
|----------|------|
| Spec files | `specs/` (172 .md files) |
| Spec template | `specs/TEMPLATE.md` |
| Dependency graph | `specs/DEPENDENCY_GRAPH.md` |
| Coverage config | `.coveragerc` |
| Coverage gap inventory | `plan/coverage-gaps-inventory.md` |

## Key Testing Patterns

1. **Async safety:** `test_generic_engine.py` closes the event loop — all new async tests MUST use `_run_async()` helper (creates a fresh loop per call)
2. **Portfolio tests:** `Portfolio.py` tests hang if they call `PricingContext/calc/resolve/price` — only test sync logic
3. **API method names:** Use `GsSession.current.sync.post` (not the old `GsSession.current._post`)
4. **isinstance mocking:** Use `MagicMock(spec=Entity)` for `isinstance` checks to pass
5. **Coverage DB corruption:** Transient — just delete `.coverage` and re-run
6. **Branch notation:** `[X,Y]` means execution at line X jumps to line Y
7. **JSON report:** `coverage.json` is the authoritative source for gap analysis
8. **Pre-existing failures (10):** 4 in `test_generic_engine.py`, 4 in `test_measures.py` (tzdata), 1 in `test_session.py`, 1 in `test_utils.py`

## Detailed Plan Files
- [coverage-plan-overview.md](coverage-plan-overview.md) — phases, priorities, risks
- [coverage-phase0-infra.md](coverage-phase0-infra.md) — coverage infrastructure setup
- [coverage-phases1-4.md](coverage-phases1-4.md) — root utils, small modules, risk, instrument
- [coverage-phases5-8.md](coverage-phases5-8.md) — session, analytics, backtests, deepen existing
- [coverage-phases9-10.md](coverage-phases9-10.md) — shared infra, final push, verification
- [learnings.md](learnings.md) — testing patterns and pitfalls reference
- [coverage-gaps-inventory.md](coverage-gaps-inventory.md) — per-file coverage gap data
