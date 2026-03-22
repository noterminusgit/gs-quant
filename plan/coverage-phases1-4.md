# Phases 1-4: Core Module Coverage

**Status: COMPLETE**

## Phase 1: Root-Level Utility Modules

| File | Lines | New Test File | Approach | Result |
|------|-------|---------------|----------|--------|
| `gs_quant/errors.py` | 89 | `test/test_errors.py` | Test `error_builder()` with each status code (401, 403, 429, 500, 504, default) | 100% |
| `gs_quant/json_encoder.py` | 44 | `test/test_json_encoder.py` | Test each type branch in encoder | 100% |
| `gs_quant/json_convertors.py` | 349 | `test/test_json_convertors.py` | Test each decode/encode function with valid, None, and invalid inputs | 100% |
| `gs_quant/json_convertors_common.py` | 89 | `test/test_json_convertors_common.py` | Test common convertor functions | 100% |
| `gs_quant/context_base.py` | 190 | `test/test_context_base.py` | Create concrete subclass, test enter/exit/nesting/async | 100% |
| `gs_quant/common.py` | 116 | `test/test_common.py` | Test common utilities | 100% |
| `gs_quant/priceable.py` | 140 | `test/test_priceable.py` | Mock `PricingContext.current` for various states | 100% |
| `gs_quant/base.py` | 729 | Extend existing `test/test_base.py` | Add tests for uncovered branches | 95%+ |

Notes: Most are pure functions or simple classes — minimal mocking needed.

## Phase 2: Small Untested Modules

| File | Lines | New Test File | Approach | Result |
|------|-------|---------------|----------|--------|
| `gs_quant/interfaces/algebra.py` | 37 | `test/interfaces/test_algebra.py` | Concrete `AlgebraicType` subclass to test `__radd__`/`__rmul__` | 100% |
| `gs_quant/workflow/workflow.py` | 26 | `test/workflow/test_workflow.py` | Verify global decoder registration | 100% |
| `gs_quant/quote_reports/core.py` | 100 | `test/quote_reports/test_core.py` | Test each `*_from_dict` dispatch with each type variant | 100% |
| `gs_quant/instrument/overrides.py` | 15 | No code to test (header only) | Skip | N/A |

## Phase 3: Risk Module

| File | Lines | New Test File | Approach | Result |
|------|-------|---------------|----------|--------|
| `gs_quant/risk/core.py` | 686 | `test/risk/test_core.py` | Construct `FloatWithInfo`, `DataFrameWithInfo` with mock `RiskKey`; test arithmetic, repr, compose | 98.5% |
| `gs_quant/risk/result_handlers.py` | 516 | `test/risk/test_result_handlers.py` | Construct mock result iterators, test each handler dispatch path | 100% |
| `gs_quant/risk/scenarios.py` | 65 | `test/risk/test_scenarios.py` | Test scenario construction | 100% |
| `gs_quant/risk/scenario_utils.py` | 56 | `test/risk/test_scenario_utils.py` | Mock `SecurityMaster.get_asset`, `Dataset.get_data` | 100% |
| `gs_quant/risk/transform.py` | 76 | `test/risk/test_transform.py` | Test `ResultWithInfoAggregator.apply` with float, Series, DataFrame inputs | 96.4% |

Shared `RiskKey` factory fixture was used inline rather than in a formal conftest.

## Phase 4: Instrument Module

| File | Lines | New Test File | Result |
|------|-------|---------------|--------|
| `gs_quant/instrument/core.py` | 383 | `test/instrument/test_core.py` | 85.0% |

Approach:
- Test `Instrument.resolve()`: in_place True/False, HistoricalPricingContext, ErrorValue, None result
- Test `Instrument.calc()`: single/multiple measures, MultiScenario, deprecated measures warning, callback
- Test `Instrument.from_dict()`: with/without `$type`, various asset_class/type combos
- Mock `PricingContext`, `GsRiskApi`; use `IRSwap` as concrete test subject
- `gs_quant_internal` import paths: marked with `# pragma: no cover`

## Lessons Learned
- Formal conftest files were not needed — inline mocks and `_run_async()` helper were sufficient
- Discovered pandas 3.x compatibility bug during testing (DataFrame comparison behavior change)
- `GsSession` global state cleanup between tests is critical to avoid cross-test pollution
