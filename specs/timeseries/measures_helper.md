# measures_helper.py

## Summary
Provides helper enums and a preprocessing function for implied volatility strike handling in equity measures. Defines `EdrDataReference` and `VolReference` enums for strike reference types, and `preprocess_implied_vol_strikes_eq` which validates and normalizes strike parameters for equity implied volatility queries.

## Dependencies
- Internal: `gs_quant.errors` (MqValueError)
- External: `enum` (Enum), `numbers` (Real)

## Type Definitions
None.

## Enums and Constants

### EdrDataReference(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DELTA_CALL | `"delta_call"` | Delta call reference |
| DELTA_PUT | `"delta_put"` | Delta put reference |
| SPOT | `"spot"` | Spot reference |

### VolReference(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| DELTA_CALL | `"delta_call"` | Delta call strike reference |
| DELTA_PUT | `"delta_put"` | Delta put strike reference |
| DELTA_NEUTRAL | `"delta_neutral"` | Delta neutral strike reference |
| NORMALIZED | `"normalized"` | Normalized strike reference |
| SPOT | `"spot"` | Spot strike reference |
| FORWARD | `"forward"` | Forward strike reference |

## Functions/Methods

### preprocess_implied_vol_strikes_eq(strike_reference: VolReference = None, relative_strike: Real = None) -> tuple[str, Real]
Purpose: Validate and normalize strike reference and relative strike for equity implied volatility queries.

**Algorithm:**
1. Branch: if `relative_strike is None` AND `strike_reference != VolReference.DELTA_NEUTRAL` -> raise `MqValueError('Relative strike must be provided if your strike reference is not delta_neutral')`
2. Branch: if `strike_reference == VolReference.DELTA_NEUTRAL` -> raise `MqValueError('delta_neutral strike reference is not supported for equities.')`
3. Branch: if `strike_reference == VolReference.DELTA_PUT` -> transform `relative_strike = abs(100 - relative_strike)`
4. Branch: if `strike_reference == VolReference.NORMALIZED` -> keep `relative_strike` as-is
5. Branch: else (DELTA_CALL, DELTA_PUT, SPOT, FORWARD) -> divide `relative_strike` by 100
6. Build `ref_string`:
   - Branch: if `strike_reference` is one of `DELTA_CALL`, `DELTA_PUT`, `DELTA_NEUTRAL` -> `ref_string = "delta"`
   - Branch: else (NORMALIZED, SPOT, FORWARD) -> `ref_string = strike_reference.value`
7. Return `(ref_string, relative_strike)`

**Raises:** `MqValueError` when relative_strike is None with non-delta-neutral reference, or when strike_reference is DELTA_NEUTRAL.

## State Mutation
- No module-level mutable state.
- No mutation of inputs (relative_strike is a local rebinding).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `preprocess_implied_vol_strikes_eq` | `relative_strike is None` and `strike_reference != DELTA_NEUTRAL` |
| `MqValueError` | `preprocess_implied_vol_strikes_eq` | `strike_reference == DELTA_NEUTRAL` |

## Edge Cases
- The two validation checks at the top create a situation where `DELTA_NEUTRAL` always raises an error: if `relative_strike` is None with `DELTA_NEUTRAL`, the first check passes (doesn't raise), but the second check raises. If `relative_strike` is provided with `DELTA_NEUTRAL`, the first check passes, but the second check still raises. So `DELTA_NEUTRAL` is effectively always rejected for equities.
- When `strike_reference is None` and `relative_strike is None`, the first check raises because `None != VolReference.DELTA_NEUTRAL`.
- When `strike_reference is None` and `relative_strike` is provided, the function proceeds past validations. Step 3 does not trigger (None != DELTA_PUT). Step 4: `None != NORMALIZED` so relative_strike is divided by 100. Step 6: `None` is not in the delta set, so `ref_string = None.value` which raises `AttributeError`.
- The `DELTA_PUT` transformation `abs(100 - relative_strike)` converts the strike, then step 5 divides by 100 (since DELTA_PUT is not NORMALIZED). So for DELTA_PUT with relative_strike=25, the result is `abs(100-25)/100 = 0.75`.
- `EdrDataReference` enum is defined but not used within this module.

## Bugs Found
- None identified (the `strike_reference=None` edge case is likely prevented by callers).

## Coverage Notes
- Branch count: ~7
- Key branches: relative_strike None check, DELTA_NEUTRAL check, DELTA_PUT transformation, NORMALIZED vs other division, delta group vs value ref_string
- All branches are reachable with appropriate parameter combinations (except DELTA_NEUTRAL + valid relative_strike which always raises at step 2)
