# measures.py

## Summary
Defines custom risk measure classes and pre-configured risk measure instances for GS Quant. Provides `PnlExplain`, `PnlExplainClose`, `PnlExplainLive`, and `PnlPredictLive` as relative risk measures that compute P&L attribution against a target market. Also defines seven parameterised risk measure constants (`IRBasisParallel`, `InflationDeltaParallel`, `IRDeltaParallel`, `IRDeltaLocalCcy`, `IRXccyDeltaParallel`, `IRVegaParallel`, `IRVegaLocalCcy`) used across the risk system.

## Dependencies
- Internal: `gs_quant.base` (`Market` -- the ABC)
- Internal: `gs_quant.common` (`AssetClass`, `AggregationLevel`, `RiskMeasure`, `RiskMeasureType`, `RiskMeasureUnit`)
- Internal: `gs_quant.target.measures` (`IRBasis`, `IRVega`, `IRDelta`, `IRXccyDelta`, `InflationDelta`)
- Internal (lazy): `gs_quant.markets` (`PricingContext`, `RelativeMarket`, `CloseMarket`, `LiveMarket`)
- External: `typing` (`Union`)

## Type Definitions

### __RelativeRiskMeasure (class, private)
Inherits: `RiskMeasure`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__to_market` | `Market` | (required) | Target market for relative pricing |

Constructor also passes through `asset_class`, `measure_type`, `unit`, `value`, `name` to `RiskMeasure.__init__`.

### PnlExplain (class)
Inherits: `__RelativeRiskMeasure`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (inherited) `__to_market` | `Market` | (required) | Target market for P&L explain |

Docstring: `"Pnl Explained"`

### PnlExplainClose (class)
Inherits: `PnlExplain`

No additional fields. Constructor uses `CloseMarket()` as target market.

### PnlExplainLive (class)
Inherits: `PnlExplain`

No additional fields. Constructor uses `LiveMarket()` as target market.

### PnlPredictLive (class)
Inherits: `__RelativeRiskMeasure`

No additional fields. Constructor uses `LiveMarket()` as target market with `RiskMeasureType.PnlPredict`.

Docstring: `"Pnl Predicted"`

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `DEPRECATED_MEASURES` | `dict` | `{}` | Empty dict for deprecated measure tracking |
| `IRBasisParallel` | `IRBasis` | `IRBasis(aggregation_level=AggregationLevel.Asset, name='IRBasisParallel')` | Parallel IR basis risk |
| `InflationDeltaParallel` | `InflationDelta` | `InflationDelta(aggregation_level=AggregationLevel.Type, name='InflationDeltaParallel')` | Parallel inflation delta |
| `IRDeltaParallel` | `IRDelta` | `IRDelta(aggregation_level=AggregationLevel.Asset, name='IRDeltaParallel')` | Parallel IR delta |
| `IRDeltaLocalCcy` | `IRDelta` | `IRDelta(currency='local', name='IRDeltaLocalCcy')` | IR delta in local currency |
| `IRXccyDeltaParallel` | `IRXccyDelta` | `IRXccyDelta(aggregation_level=AggregationLevel.Type, name='IRXccyDeltaParallel')` | Parallel cross-currency delta |
| `IRVegaParallel` | `IRVega` | `IRVega(aggregation_level=AggregationLevel.Asset, name='IRVegaParallel')` | Parallel IR vega |
| `IRVegaLocalCcy` | `IRVega` | `IRVega(currency='local', name='IRVegaLocalCcy')` | IR vega in local currency |

## Functions/Methods

### __RelativeRiskMeasure.__init__(self, to_market: Market, asset_class: Union[AssetClass, str] = None, measure_type: Union[RiskMeasureType, str] = None, unit: Union[RiskMeasureUnit, str] = None, value: Union[float, str] = None, name: str = None)
Purpose: Initialize a relative risk measure that prices against a target market.

**Algorithm:**
1. Call `super().__init__(asset_class=asset_class, measure_type=measure_type, unit=unit, value=value)`.
2. Store `to_market` in private attribute `self.__to_market`.
3. Set `self.name = name`.

---

### __RelativeRiskMeasure.pricing_context (property) -> PricingContext
Purpose: Create a cloned pricing context with a `RelativeMarket`.

**Algorithm:**
1. Lazy import `PricingContext`, `RelativeMarket` from `gs_quant.markets`.
2. Get `current = PricingContext.current`.
3. Return `current.clone(market=RelativeMarket(from_market=current.market, to_market=self.__to_market))`.

---

### PnlExplain.__init__(self, to_market: Market)
Purpose: Initialize PnlExplain with a specific target market.

**Algorithm:**
1. Call `super().__init__(to_market, measure_type=RiskMeasureType.PnlExplain, name=RiskMeasureType.PnlExplain.value)`.

---

### PnlExplainClose.__init__(self)
Purpose: Initialize PnlExplain against the close market.

**Algorithm:**
1. Lazy import `CloseMarket` from `gs_quant.markets`.
2. Call `super().__init__(CloseMarket())`.

---

### PnlExplainLive.__init__(self)
Purpose: Initialize PnlExplain against the live market.

**Algorithm:**
1. Lazy import `LiveMarket` from `gs_quant.markets`.
2. Call `super().__init__(LiveMarket())`.

---

### PnlPredictLive.__init__(self)
Purpose: Initialize PnlPredict against the live market.

**Algorithm:**
1. Lazy import `LiveMarket` from `gs_quant.markets`.
2. Call `super().__init__(LiveMarket(), measure_type=RiskMeasureType.PnlPredict, name=RiskMeasureType.PnlPredict.value)`.

## State Mutation
- `self.__to_market`: Set once during `__init__`, never modified.
- `self.name`: Set during `__init__`, may be overwritten if `RiskMeasure` parent also sets it.
- Module-level constants (`IRBasisParallel`, etc.) are created at import time and are effectively immutable singleton instances.
- Thread safety: `pricing_context` property accesses `PricingContext.current` which is context-variable-based (thread-local-like). The property itself does not mutate shared state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none explicit) | - | No explicit error handling; errors propagate from parent constructors or lazy imports |

## Edge Cases
- `PnlExplainClose` and `PnlExplainLive` construct their market objects during `__init__`, not lazily. If `CloseMarket` or `LiveMarket` have initialization side effects, they trigger immediately.
- `pricing_context` property uses lazy imports to avoid circular dependencies with `gs_quant.markets`.
- `DEPRECATED_MEASURES` is defined but never populated in this file (may be used externally).

## Coverage Notes
- Branch count: ~6 (constructor branches, property access)
- Key branches: each constructor, `pricing_context` property
- Pragmas: none observed
