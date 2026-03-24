# data/fields.py

## Summary
Defines the core enumeration types for data fields used across the gs_quant data layer. Contains `DataMeasure` (quantitative facts like prices and volumes), `DataDimension` (contextual attributes like asset ID and tenor), and `Fields` (a unified enum that is dynamically populated from both `DataMeasure` and `DataDimension` at module load time using `aenum.extend_enum`). Also defines the `AssetMeasure` frozen dataclass for typed asset-measure metadata.

## Dependencies
- External: `aenum` (`Enum`, `extend_enum`) -- note: uses `aenum` (advanced enum), NOT stdlib `enum`
- External: `typing` (`Optional`)
- External: `dataclasses_json` (`LetterCase`, `dataclass_json`)
- External: `dataclasses` (`dataclass`, `field`)

## Type Definitions

### DataMeasure (Enum)
Inherits: `aenum.Enum`

Enumeration of quantitative measures available through data APIs. These represent facts/quantities that can be aggregated over time.

| Member | Value (str) | Description |
|--------|-------------|-------------|
| ASK_PRICE | `"askPrice"` | Ask price |
| BID_PRICE | `"bidPrice"` | Bid price |
| HIGH_PRICE | `"highPrice"` | High price |
| MID_PRICE | `"midPrice"` | Mid price |
| LOW_PRICE | `"lowPrice"` | Low price |
| OPEN_PRICE | `"openPrice"` | Open price |
| CLOSE_PRICE | `"closePrice"` | Close price |
| TRADE_PRICE | `"tradePrice"` | Trade price |
| SPOT_PRICE | `"spot"` | Spot price |
| VOLUME | `"volume"` | Volume |
| ADJUSTED_ASK_PRICE | `"adjustedAskPrice"` | Adjusted ask price |
| ADJUSTED_BID_PRICE | `"adjustedBidPrice"` | Adjusted bid price |
| ADJUSTED_HIGH_PRICE | `"adjustedHighPrice"` | Adjusted high price |
| ADJUSTED_LOW_PRICE | `"adjustedLowPrice"` | Adjusted low price |
| ADJUSTED_OPEN_PRICE | `"adjustedOpenPrice"` | Adjusted open price |
| ADJUSTED_CLOSE_PRICE | `"adjustedClosePrice"` | Adjusted close price |
| ADJUSTED_TRADE_PRICE | `"adjustedTradePrice"` | Adjusted trade price |
| ADJUSTED_VOLUME | `"adjustedVolume"` | Adjusted volume |
| IMPLIED_VOLATILITY | `"impliedVolatility"` | Implied volatility |
| VAR_SWAP | `"varSwap"` | Variance swap |
| PRICE | `"price"` | Generic price |
| NAV_PRICE | `"navPrice"` | NAV price |
| SPREAD | `"spread"` | Spread |
| NAV_SPREAD | `"navSpread"` | NAV spread |
| IMPLIED_VOLATILITY_BY_DELTA_STRIKE | `"impliedVolatilityByDeltaStrike"` | Implied volatility by delta strike |
| FORWARD_POINT | `"forwardPoint"` | Forward point |
| DIVIDEND_YIELD | `"Dividend Yield"` | Dividend yield (fundamental) |
| EARNINGS_PER_SHARE | `"Earnings per Share"` | Earnings per share (fundamental) |
| EARNINGS_PER_SHARE_POSITIVE | `"Earnings per Share Positive"` | Earnings per share positive (fundamental) |
| NET_DEBT_TO_EBITDA | `"Net Debt to EBITDA"` | Net debt to EBITDA (fundamental) |
| PRICE_TO_BOOK | `"Price to Book"` | Price to book (fundamental) |
| PRICE_TO_CASH | `"Price to Cash"` | Price to cash (fundamental) |
| PRICE_TO_EARNINGS | `"Price to Earnings"` | Price to earnings (fundamental) |
| PRICE_TO_EARNINGS_POSITIVE | `"Price to Earnings Positive"` | Price to earnings positive (fundamental) |
| PRICE_TO_EARNINGS_POSITIVE_EXCLUSIVE | `"Price to Earnings Positive Exclusive"` | Price to earnings positive exclusive (fundamental) |
| PRICE_TO_SALES | `"Price to Sales"` | Price to sales (fundamental) |
| RETURN_ON_EQUITY | `"Return on Equity"` | Return on equity (fundamental) |
| SALES_PER_SHARE | `"Sales per Share"` | Sales per share (fundamental) |
| CURRENT_CONSTITUENTS_DIVIDEND_YIELD | `"Current Constituents Dividend Yield"` | Current constituents dividend yield (fundamental) |
| CURRENT_CONSTITUENTS_EARNINGS_PER_SHARE | `"Current Constituents Earnings per Share"` | Current constituents EPS (fundamental) |
| CURRENT_CONSTITUENTS_EARNINGS_PER_SHARE_POSITIVE | `"Current Constituents Earnings per Share Positive"` | Current constituents EPS positive (fundamental) |
| CURRENT_CONSTITUENTS_NET_DEBT_TO_EBITDA | `"Current Constituents Net Debt to EBITDA"` | Current constituents net debt to EBITDA (fundamental) |
| CURRENT_CONSTITUENTS_PRICE_TO_BOOK | `"Current Constituents Price to Book"` | Current constituents price to book (fundamental) |
| CURRENT_CONSTITUENTS_PRICE_TO_CASH | `"Current Constituents Price to Cash"` | Current constituents price to cash (fundamental) |
| CURRENT_CONSTITUENTS_PRICE_TO_EARNINGS | `"Current Constituents Price to Earnings"` | Current constituents price to earnings (fundamental) |
| CURRENT_CONSTITUENTS_PRICE_TO_EARNINGS_POSITIVE | `"Current Constituents Price to Earnings Positive"` | Current constituents price to earnings positive (fundamental) |
| CURRENT_CONSTITUENTS_PRICE_TO_SALES | `"Current Constituents Price to Sales"` | Current constituents price to sales (fundamental) |
| CURRENT_CONSTITUENTS_RETURN_ON_EQUITY | `"Current Constituents Return on Equity"` | Current constituents return on equity (fundamental) |
| CURRENT_CONSTITUENTS_SALES_PER_SHARE | `"Current Constituents Sales per Share"` | Current constituents sales per share (fundamental) |
| ONE_YEAR | `"1y"` | One year period |
| TWO_YEARS | `"2y"` | Two year period |
| THREE_YEARS | `"3y"` | Three year period |
| FORWARD | `"forward"` | Forward period |
| TRAILING | `"trailing"` | Trailing period |

Total: 50 members.

### DataDimension (Enum)
Inherits: `aenum.Enum`

Enumeration of dimensions (contextual attributes) available through data APIs.

| Member | Value (str) | Description |
|--------|-------------|-------------|
| ASSET_ID | `"assetId"` | Asset identifier |
| NAME | `"name"` | Asset name |
| RIC | `"ric"` | Reuters Instrument Code |
| TENOR | `"tenor"` | Tenor (maturity period) |
| STRIKE_REFERENCE | `"strikeReference"` | Strike reference type |
| RELATIVE_STRIKE | `"relativeStrike"` | Relative strike value |
| EXPIRATION_DATE | `"expirationDate"` | Expiration date |
| UPDATE_TIME | `"updateTime"` | Time of last update |

Total: 8 members.

### Fields (Enum)
Inherits: `aenum.Enum`

A unified field enum that is initially empty and then dynamically populated at module load time by iterating over all members of `DataMeasure` and `DataDimension` and calling `extend_enum(Fields, member.name, member.value)` for each. After module initialization, `Fields` contains all 50 `DataMeasure` members plus all 8 `DataDimension` members = 58 total members, with the same names and values as the source enums.

**Dynamic population mechanism (module-level code):**
```
for enum in DataMeasure:
    extend_enum(Fields, enum.name, enum.value)

for enum in DataDimension:
    extend_enum(Fields, enum.name, enum.value)
```

### AssetMeasure (frozen dataclass, with JSON serialization)
Decorators: `@dataclass_json(letter_case=LetterCase.CAMEL)`, `@dataclass(frozen=True)`

A frozen (immutable) dataclass representing typed metadata about an asset measure. The `dataclass_json` decorator with `LetterCase.CAMEL` enables automatic camelCase JSON serialization/deserialization (e.g., `dataset_field` becomes `"datasetField"` in JSON).

| Field | Type | Default | JSON Key (camelCase) | Description |
|-------|------|---------|---------------------|-------------|
| `dataset_field` | `str` | `None` | `"datasetField"` | The dataset field name |
| `frequency` | `str` | `None` | `"frequency"` | The frequency of the measure |
| `type` | `str` | `None` | `"type"` | The type of the measure |

Note: All fields default to `None` despite being typed as `str`. This means the effective type is `Optional[str]` in practice even though the annotation says `str`.

## Enums and Constants

All enums are documented in Type Definitions above. There are no module-level constants.

## Functions/Methods

### DataMeasure.__repr__(self) -> str
Purpose: Return the enum member's value as its repr (instead of the default `<DataMeasure.MEMBER: 'value'>` format).

**Algorithm:**
1. Return `self.value`.

---

### DataMeasure.list_fundamentals(cls) -> List[str]  [classmethod]
Purpose: Return a list of string values for all fundamental metric members.

**Algorithm:**
1. Build a list comprehension: `[metric.value for metric in [...]]`
2. The explicit list of fundamental metrics included (22 total):
   - DIVIDEND_YIELD, EARNINGS_PER_SHARE, EARNINGS_PER_SHARE_POSITIVE, NET_DEBT_TO_EBITDA
   - PRICE_TO_BOOK, PRICE_TO_CASH, PRICE_TO_EARNINGS, PRICE_TO_EARNINGS_POSITIVE
   - PRICE_TO_EARNINGS_POSITIVE_EXCLUSIVE, PRICE_TO_SALES, RETURN_ON_EQUITY, SALES_PER_SHARE
   - CURRENT_CONSTITUENTS_DIVIDEND_YIELD, CURRENT_CONSTITUENTS_EARNINGS_PER_SHARE
   - CURRENT_CONSTITUENTS_EARNINGS_PER_SHARE_POSITIVE, CURRENT_CONSTITUENTS_NET_DEBT_TO_EBITDA
   - CURRENT_CONSTITUENTS_PRICE_TO_BOOK, CURRENT_CONSTITUENTS_PRICE_TO_CASH
   - CURRENT_CONSTITUENTS_PRICE_TO_EARNINGS, CURRENT_CONSTITUENTS_PRICE_TO_EARNINGS_POSITIVE
   - CURRENT_CONSTITUENTS_PRICE_TO_SALES, CURRENT_CONSTITUENTS_RETURN_ON_EQUITY
   - CURRENT_CONSTITUENTS_SALES_PER_SHARE
3. Return the list of 22 string values.

Note: `PRICE_TO_EARNINGS_POSITIVE_EXCLUSIVE` is included but its corresponding `CURRENT_CONSTITUENTS_` variant is NOT included. This appears to be intentional but could be a subtle omission.

---

### Fields.unit (property) -> Optional[str]
Purpose: Placeholder property intended to return the unit for this field.

**Algorithm:**
1. Return `None` (always). Contains a TODO comment: "Define units and look up appropriate unit for self".

## State Mutation
- Module-level mutation: At import time, the `Fields` enum is mutated via `extend_enum()` calls. After module load, `Fields` contains all 58 members. This is a one-time, import-time side effect.
- `AssetMeasure`: Frozen dataclass; no mutation possible after construction.
- Thread safety: Enum members are immutable after module initialization. `AssetMeasure` is frozen. Both are safe for concurrent access.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none) | -- | This module does not explicitly raise any exceptions |

Note: `extend_enum` could theoretically raise if a duplicate name were added, but since `DataMeasure` and `DataDimension` have no overlapping member names, this does not occur.

## Edge Cases
- `Fields` enum is empty at class definition time and only populated at module level after the class body. Any code that inspects `Fields` during class definition would see an empty enum.
- `AssetMeasure` fields are typed as `str` but default to `None`. A strict type checker would flag this. In Elixir, these should be typed as `String.t() | nil` (or `optional(:string)`).
- `DataMeasure.__repr__` returns just the value string, not the standard enum repr. This means `repr(DataMeasure.VOLUME)` returns `'volume'`, not `"<DataMeasure.VOLUME: 'volume'>"`.
- The `aenum` library (not stdlib `enum`) is used specifically to support `extend_enum`, which allows adding members to an enum after class creation. Standard library `enum.Enum` does not support this.
- `AssetMeasure` uses `dataclass(frozen=True)`, meaning instances are hashable and cannot be modified after creation. Attempting to set an attribute will raise `FrozenInstanceError`.

## Coverage Notes
- Branch count: 2
  - `list_fundamentals`: 1 path (always returns the same list).
  - `Fields.unit`: 1 path (always returns None).
- The two `for` loops at module level for `extend_enum` are always executed at import time.
- No pragmas or excluded lines.
