# Testing Patterns & Learnings

Reference document consolidating all patterns discovered during the gs-quant coverage project (Phases 0-8).

## Async Testing

### The `_run_async()` Pattern (CRITICAL)

`test_generic_engine.py` closes the event loop as a side effect of its tests. Any test using `asyncio.get_event_loop()` or `pytest-asyncio` after those tests run will fail or hang.

**Solution:** All async tests MUST use this helper:

```python
def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
```

This creates a fresh event loop per call, isolated from any global loop state.

### Portfolio.py Hang Risk

`Portfolio.py` tests hang indefinitely if they call `PricingContext`, `calc()`, `resolve()`, or `price()`. These methods trigger async pricing chains that wait for responses that never come in a test environment.

**Rule:** Only test sync logic in Portfolio tests (construction, serialization, property access).

## API Patterns

### Session Method Names

Use `GsSession.current.sync.post` (not the old `GsSession.current._post`). The `_post` method was renamed during a refactor but some test code still references it.

### isinstance Mocking

When production code checks `isinstance(obj, Entity)`, a bare `MagicMock()` will fail the check. Use:

```python
mock_entity = MagicMock(spec=Entity)
```

The `spec=` parameter makes `isinstance` checks pass.

## Pre-existing Test Failures (10 total)

These failures exist in the codebase before any coverage work and are NOT caused by our tests:

| Test File | Count | Root Cause |
|-----------|-------|------------|
| `test_generic_engine.py` | 4 | Upstream refactor broke tests; event loop closure |
| `test_measures.py` | 4 | Missing `tzdata` package for timezone operations |
| `test_session.py` | 1 | SSL mock incompatibility |
| `test_utils.py` | 1 | Utility function signature change |

## Coverage Tooling

### Coverage DB Corruption

The `.coverage` SQLite database occasionally becomes corrupted, especially when running many test processes in parallel. Symptoms: `CoverageException` or `sqlite3.OperationalError`.

**Fix:** Delete `.coverage` and re-run. It's always regenerated.

### Branch Notation

In coverage reports, `[X,Y]` means "execution at line X jumps to line Y". This typically indicates:
- `[X, exit]` — early return or break
- `[X, Y]` where Y > X+1 — skipped branch (else clause not taken)
- `[X, Y]` where Y < X — loop back

### JSON Report

`coverage.json` (from `--cov-report=json`) is the authoritative source for gap analysis. It provides per-file line and branch coverage with exact missing line/branch numbers. Far more useful than HTML for programmatic analysis.

## Strategy Insights

### Parallel Agent Approach

Running 7-10 agents in parallel, each targeting a specific file with its coverage gap data, produced massive throughput:
- Each agent receives: the source file, existing test file (if any), and list of missing branches
- Each agent writes targeted tests for those specific branches
- Results are merged after each round

### Diminishing Returns Curve

| Branch Coverage | Effort Level | Typical Blockers |
|----------------|--------------|-----------------|
| 0% → 70% | Low | Write basic happy-path tests |
| 70% → 85% | Medium | Error paths, edge cases, mocking |
| 85% → 95% | High | Complex async, deep mocking chains |
| 95% → 100% | Very High | Dead code, IPython-only, `gs_quant_internal`, broken upstream tests |

After ~95% branch coverage, the cost per branch increases dramatically. The remaining branches fall into categories that require `.coveragerc` omits or `pragma: no cover` rather than new tests.

### Key Decision: No Conftest Files

The original plan called for shared conftest files (Phase 9). In practice, inline mocks proved superior:
- Each test file is self-contained and independently runnable
- No hidden fixture dependencies
- Easier to understand what each test is doing
- No risk of fixture state leaking between test modules
