# gs-quant Project Plan

## Project Overview
- Goldman Sachs quantitative finance toolkit (open source)
- ~173 production files (~74K LOC), ~120 test files (~43K LOC)
- Auto-generated code in `gs_quant/target/` — always exclude from coverage
- Branch: master, remote: origin/master

## Active Goal: 100% Branch Coverage
See detailed plan files:
- [coverage-plan-overview.md](coverage-plan-overview.md) — phases, priorities, risks
- [coverage-phase0-infra.md](coverage-phase0-infra.md) — coverage infrastructure setup
- [coverage-phases1-4.md](coverage-phases1-4.md) — root utils, small modules, risk, instrument
- [coverage-phases5-8.md](coverage-phases5-8.md) — session, analytics, backtests, deepen existing
- [coverage-phases9-10.md](coverage-phases9-10.md) — shared infra, final push, verification

## Key Patterns
- Tests live in `gs_quant/test/` mirroring the source structure
- `gs_quant_internal` imports are not available open-source — use `# pragma: no cover`
- Global state (`GsSession`, `PricingContext`) needs `yield` fixtures with cleanup
- Use `pytest-asyncio` for async tests, no real network calls
