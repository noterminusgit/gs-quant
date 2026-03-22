# gs-quant Project Plan

## Project Overview
- Goldman Sachs quantitative finance toolkit (open source)
- ~473 measured production files, ~120 test files
- Auto-generated code in `gs_quant/target/` and `gs_quant/_version.py` — excluded from coverage
- Branch: master, remote: origin/master

## Active Goal: 100% Branch Coverage

**Current state:** 98.0% line (115,233 / 116,894) | 91.8% branch (9,605 / 10,458) | 7,902 tests passing

See detailed plan files:
- [coverage-plan-overview.md](coverage-plan-overview.md) — phases, priorities, risks
- [coverage-phase0-infra.md](coverage-phase0-infra.md) — coverage infrastructure setup
- [coverage-phases1-4.md](coverage-phases1-4.md) — root utils, small modules, risk, instrument
- [coverage-phases5-8.md](coverage-phases5-8.md) — session, analytics, backtests, deepen existing
- [coverage-phases9-10.md](coverage-phases9-10.md) — shared infra, final push, verification
- [learnings.md](learnings.md) — testing patterns and pitfalls reference

**Phases 0-8: COMPLETE.** Only Phase 10 (final push) remains.

## Key Testing Patterns

1. **Async safety:** `test_generic_engine.py` closes the event loop — all new async tests MUST use `_run_async()` helper (creates a fresh loop per call)
2. **Portfolio tests:** `Portfolio.py` tests hang if they call `PricingContext/calc/resolve/price` — only test sync logic
3. **API method names:** Use `GsSession.current.sync.post` (not the old `GsSession.current._post`)
4. **isinstance mocking:** Use `MagicMock(spec=Entity)` for `isinstance` checks to pass
5. **Coverage DB corruption:** Transient — just delete `.coverage` and re-run
6. **Branch notation:** `[X,Y]` means execution at line X jumps to line Y
7. **JSON report:** `coverage.json` is the authoritative source for gap analysis
8. **Parallel agents:** 7-10 agents per round targeting specific files = massive throughput
9. **Diminishing returns:** After ~95% branch, remaining branches are dead code, IPython-only, or complex async
10. **Pre-existing failures (10):** 4 in `test_generic_engine.py`, 4 in `test_measures.py` (tzdata), 1 in `test_session.py`, 1 in `test_utils.py`
