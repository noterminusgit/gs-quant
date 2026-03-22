# Phases 9-10: Shared Infrastructure & Final Push

## Phase 9: Shared Test Infrastructure

**Status: Addressed organically — no formal conftest files created.**

The original plan called for shared conftest files and helper modules. In practice, this was unnecessary:

- **`_run_async()` helper:** Inlined in each test file that needs async support. Creates a fresh event loop per call to avoid contamination from `test_generic_engine.py`'s loop closure.
- **Inline mocks:** `MagicMock(spec=...)` patterns proved simpler and more maintainable than shared fixture factories.
- **No conftest files needed:** Each test file is self-contained, which actually improved reliability by eliminating cross-file fixture dependencies.

Planned but not created:
- ~~`test/risk/conftest.py`~~ — RiskKey factory inlined
- ~~`test/backtest/conftest.py`~~ — mock instruments inlined
- ~~`test/analytics/conftest.py`~~ — mock entities inlined
- ~~`test/instrument/conftest.py`~~ — mock session inlined
- ~~`test/utils/test_factories.py`~~ — not needed
- ~~`test/utils/async_helpers.py`~~ — `_run_async()` inlined

## Phase 10: Final Push

**Status: REMAINING — the only incomplete phase.**

### Step 1: Create/Update `.coveragerc`

Omit auto-generated and non-production code from measurement. This eliminates ~649 of 853 missing branches.

```ini
[run]
source = gs_quant
branch = true
omit =
    gs_quant/target/*
    gs_quant/_version.py
    gs_quant/content/*
    gs_quant/test/*

[report]
fail_under = 100
```

### Step 2: Add `pragma: no cover` for unreachable code (~8 branches)

- `__init__.py` lines 50-54: PyXll import (Excel add-in environment only)
- `__init__.py` lines 60-64: IPython `nest_asyncio` (notebook environment only)
- `tracing/tracing.py` lines 685-705: IPython cell magic registration
- Any `gs_quant_internal` import paths (internal GS package, not available open-source)

### Step 3: Write tests for remaining ~58 production branches

Across 37 files, mostly 1-2 branches each:

| File | Missing Branches | Priority |
|------|-----------------|----------|
| `api/gs/risk.py` | 5 | High |
| `risk/results.py` | 4 | High |
| `markets/core.py` | 3 | High |
| `markets/factor_analytics.py` | 3 | Medium |
| `markets/position_set.py` | 3 | Medium |
| 32 other files | 1-2 each | Low |

### Step 4: Address `generic_engine.py` (138 branches)

The 4 pre-existing test failures in `test_generic_engine.py` corrupt the event loop, preventing coverage of 138 branches. Options:

| Option | Description | Recommendation |
|--------|-------------|----------------|
| A | Fix the 4 broken tests (upstream refactor broke them) | Best but highest effort |
| B | Targeted `pragma: no cover` on async internals | Pragmatic compromise |
| C | Omit from `.coveragerc` | Not recommended — it's real production code |

### Step 5: Set `fail_under = 100` and verify

```bash
pytest --cov=gs_quant --cov-branch \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-report=json \
  --cov-fail-under=100
```

### Verification Checklist
- [ ] `.coveragerc` omits `target/`, `_version.py`, `content/`, `test/`
- [ ] All `pragma: no cover` annotations are justified
- [ ] All 37 files with testable branches have new tests
- [ ] `generic_engine.py` resolved (Option A, B, or C)
- [ ] `fail_under = 100` passes
- [ ] 0 regressions in existing 7,902 tests
