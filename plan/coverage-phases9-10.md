# Phases 9-10: Shared Infrastructure & Final Push

## Phase 9: Shared Test Infrastructure (Built Throughout)

### New conftest files:
- `test/risk/conftest.py` — `RiskKey` factory, mock market objects
- `test/backtest/conftest.py` — mock instruments, data sources, pricing context
- `test/analytics/conftest.py` — mock entities, data coordinates
- `test/instrument/conftest.py` — mock session fixture

### New helper modules:
- `test/utils/test_factories.py` — factory functions for test instruments, risk measures, DataFrames
- `test/utils/async_helpers.py` — async test utilities

## Phase 10: Final Push

### Steps:
1. Run full `pytest --cov-report=html` and review every file below 100%
2. Add `# pragma: no cover` for genuinely unreachable code:
   - `gs_quant_internal` import paths (internal GS package not available)
   - Python version checks for Python 2 (e.g., `sys.version_info.major < 3`)
   - Platform-specific SSL branches (mock both paths or pragma one)
3. Force exception paths via `mock.side_effect` for all try/except blocks
4. Set `.coveragerc` `fail_under = 100`

### Verification:
- Run: `pytest --cov=gs_quant --cov-branch --cov-report=term-missing --cov-report=html --cov-report=json`
- Check `htmlcov/index.html` for any remaining uncovered branches
- Check `coverage.json` for per-file metrics
- Ensure `fail_under = 100` passes
