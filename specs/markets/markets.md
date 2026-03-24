# markets.py

## Summary
Defines market data environment objects used throughout the pricing system. Provides concrete `Market` subclasses (`CloseMarket`, `LiveMarket`, `TimestampedMarket`, `OverlayMarket`, `RelativeMarket`, `RefMarket`, `LocationOnlyMarket`) and helper functions for resolving market dates and locations. Also provides `MarketDataCoordinate` and `MarketDataCoordinateValue` wrappers with custom string representations.

## Dependencies
- Internal: `gs_quant.base` (Market, RiskKey), `gs_quant.common` (CloseMarket as _CloseMarket, LiveMarket as _LiveMarket, OverlayMarket as _OverlayMarket, RelativeMarket as _RelativeMarket, TimestampedMarket as _TimestampedMarket, RefMarket as _RefMarket, PricingLocation), `gs_quant.datetime.date` (prev_business_date, location_to_tz_mapping), `gs_quant.target.data` (MarketDataCoordinate as __MarketDataCoordinate, MarketDataCoordinateValue as __MarketDataCoordinateValue), `gs_quant.api.gs.data` (GsDataApi) [lazy import], `gs_quant.markets.core` (PricingContext) [lazy import]
- External: `datetime`, `re`, `typing` (Mapping, Optional, Tuple, Union)

## Type Definitions

### MarketDataCoordinate (class)
Inherits: `__MarketDataCoordinate` (from `gs_quant.target.data`)

No additional fields; overrides `__repr__` and adds `from_string`.

### MarketDataCoordinateValue (class)
Inherits: `__MarketDataCoordinateValue` (from `gs_quant.target.data`)

No additional fields; overrides `__repr__`.

### Coordinates (TypeAlias)
```
Coordinates = Tuple[MarketDataCoordinate, ...]
```

### MarketDataMap (TypeAlias)
```
MarketDataMap = Mapping[MarketDataCoordinate, float]
```

### LocationOnlyMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __location | `Optional[PricingLocation]` | required | Pricing location, coerced from string if necessary |

### CloseMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __date | `Optional[dt.date]` | `None` | Market close date |
| __location | `Optional[PricingLocation]` | `None` | Pricing location, coerced from string if necessary |
| check | `Optional[bool]` | `True` | Whether to validate/resolve date and location from PricingContext |
| __date_cache | `dict` (class-level) | `{}` | Unused class-level date cache |
| roll_hr_and_min | `Tuple[int, int]` (class-level) | `(24, 0)` | Hour and minute of expected market data availability in location timezone |

### TimestampedMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __timestamp | `dt.datetime` | required | Market data timestamp |
| __location | `Optional[PricingLocation]` | `None` | Pricing location |

### LiveMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __location | `Optional[PricingLocation]` | `None` | Pricing location |

### OverlayMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __base_market | `Market` | `CloseMarket()` | Base market to overlay on |
| __market_data | `dict` | `{}` (filtered) | Map of coordinates to float values, excluding redacted |
| __market_model_data | `Optional[str]` | `None` | Binary market model data |
| __redacted_coordinates | `Tuple[MarketDataCoordinate, ...]` | `()` | Coordinates that were redacted and cannot be overridden |

### RefMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __market | `_RefMarket` | required | Underlying reference market object |

### RelativeMarket (class)
Inherits: `Market`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __from_market | `Market` | required | Starting market state |
| __to_market | `Market` | required | Ending market state |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| CloseMarket.__date_cache | `dict` | `{}` | Class-level cache (currently unused) |
| CloseMarket.roll_hr_and_min | `Tuple[int, int]` | `(24, 0)` | Default roll time: midnight (next day) |

## Functions/Methods

### historical_risk_key(risk_key: RiskKey) -> RiskKey
Purpose: Create a new RiskKey with a `LocationOnlyMarket` (strips date from market).

**Algorithm:**
1. Create `LocationOnlyMarket` from `risk_key.market.location`
2. Return new `RiskKey` with `date=None` and the location-only market, preserving other fields

### market_location(location: Optional[PricingLocation] = None) -> PricingLocation
Purpose: Resolve a PricingLocation, defaulting to PricingContext's location or LDN.

**Algorithm:**
1. Lazy import `PricingContext`
2. Get `default` from `PricingContext.current.market_data_location`
3. Branch: `location is None` -> return `default or PricingLocation.LDN`
4. Branch: else -> return `location` as-is

### close_market_date(location: Optional[Union[PricingLocation, str]] = None, date: Optional[dt.date] = None, roll_hr_and_min: Tuple[int, int] = (24, 0)) -> dt.date
Purpose: Determine the correct close market date based on current time relative to the roll time in the given timezone.

**Algorithm:**
1. Lazy import `PricingContext`
2. Branch: `date` is `None` -> use `PricingContext.current.pricing_date`
3. Get timezone from `location_to_tz_mapping[PricingLocation(location)]`
4. Compute `now_time` in the location timezone (stripped of tzinfo)
5. Compute `roll_time` from `date` + `roll_hr_and_min` offset
6. Branch: `now_time < roll_time` -> call `prev_business_date(date)` and use that
7. Return date

### MarketDataCoordinate.__repr__(self) -> str
Purpose: Build an underscore-separated string from mkt_type, mkt_asset, mkt_class, mkt_point, and mkt_quoting_style.

**Algorithm:**
1. Join `mkt_type`, `mkt_asset`, `mkt_class` with `_`, replacing `None` with `''`
2. Branch: `self.mkt_point` truthy -> append comma-joined mkt_point with `_` prefix
3. Branch: `self.mkt_quoting_style` truthy -> append with `.` prefix
4. Return result string

### MarketDataCoordinate.from_string(cls, value: str) -> MarketDataCoordinate
Purpose: Parse a coordinate from a string representation.

**Algorithm:**
1. Lazy import `GsDataApi`
2. Call `GsDataApi._coordinate_from_str(value)`
3. Branch: `len(ret.mkt_point) == 1` -> re-split the single point string on `[,_;]` delimiters
4. Return coordinate

### MarketDataCoordinateValue.__repr__(self) -> str
Purpose: Return `'{coordinate} --> {value}'` string.

### LocationOnlyMarket.__init__(self, location: Optional[Union[str, PricingLocation]])
Purpose: Store a PricingLocation, coercing string to PricingLocation.

**Algorithm:**
1. Branch: `location` is `PricingLocation` or `None` -> assign directly
2. Branch: else -> coerce via `PricingLocation(location)`

### LocationOnlyMarket.market (property) -> None
Purpose: Always returns `None` (no market data backing).

### LocationOnlyMarket.location (property) -> PricingLocation
Purpose: Return the stored location.

### CloseMarket.__init__(self, date: Optional[dt.date] = None, location: Optional[Union[str, PricingLocation]] = None, check: Optional[bool] = True)
Purpose: Initialize a close market with optional date and location.

**Algorithm:**
1. Store `__date` and `check`
2. Branch: `location` is `PricingLocation` or `None` -> assign directly
3. Branch: else -> coerce via `PricingLocation(location)`

### CloseMarket.__repr__(self) -> str
Purpose: Return `'{date} ({location.value})'`.

### CloseMarket.market (property) -> _CloseMarket
Purpose: Build the underlying `_CloseMarket` target object.

### CloseMarket.to_dict(self) -> dict
Purpose: Serialize to dict with `date`, `location`, and `marketType`.

### CloseMarket.__hash__(self) -> int
Purpose: Hash on `(date, location)` tuple.

### CloseMarket.__eq__(self, other) -> bool
Purpose: Compare by type, date, and location.

**Algorithm:**
1. Return `isinstance(other, CloseMarket) and self.date == other.date and self.location == other.location`

### CloseMarket.location (property) -> PricingLocation
Purpose: Resolve location.

**Algorithm:**
1. Branch: `self.__location is not None and not self.check` -> return `self.__location` directly
2. Branch: else -> return `market_location(self.__location)` (resolves from PricingContext)

### CloseMarket.date (property) -> dt.date
Purpose: Resolve date.

**Algorithm:**
1. Branch: `self.__date is not None and not self.check` -> return `self.__date` directly
2. Branch: else -> return `close_market_date(self.location, self.__date, self.roll_hr_and_min)`

### TimestampedMarket.__init__(self, timestamp: dt.datetime, location: Optional[Union[str, PricingLocation]] = None)
Purpose: Initialize with a specific timestamp and optional location.

**Algorithm:**
1. Store `__timestamp`
2. Coerce location: Branch on `isinstance(location, PricingLocation) or location is None`

### TimestampedMarket.__repr__(self) -> str
Purpose: Return `'{timestamp} ({location.value})'`.

### TimestampedMarket.market (property) -> _TimestampedMarket
Purpose: Build underlying target object.

### TimestampedMarket.location (property) -> PricingLocation
Purpose: Resolve location via `market_location`.

### LiveMarket.__init__(self, location: Optional[Union[str, PricingLocation]] = None)
Purpose: Initialize with optional location.

**Algorithm:**
1. Coerce location: same pattern as other market classes

### LiveMarket.__repr__(self) -> str
Purpose: Return `'Live ({location.value})'`.

### LiveMarket.location (property) -> PricingLocation
Purpose: Resolve location via `market_location`.

### LiveMarket.market (property) -> _LiveMarket
Purpose: Build underlying target object.

### OverlayMarket.__init__(self, market_data: Optional[MarketDataMap] = None, base_market: Optional[Market] = None, binary_mkt_data: Optional[str] = None)
Purpose: Initialize overlay market, separating redacted coordinates.

**Algorithm:**
1. Default `market_data` to `{}` if `None`
2. Default `base_market` to `CloseMarket()` if `None`
3. Filter `market_data` to exclude entries where `value == 'redacted'` -> store in `__market_data`
4. Store `binary_mkt_data` in `__market_model_data`
5. Extract keys where `value == 'redacted'` -> store as tuple in `__redacted_coordinates`

### OverlayMarket.__getitem__(self, item) -> Optional[float]
Purpose: Retrieve a market data value by coordinate.

**Algorithm:**
1. Branch: `item` is `str` -> convert via `MarketDataCoordinate.from_string`
2. Return `self.__market_data.get(item)`

### OverlayMarket.__setitem__(self, key, value)
Purpose: Set a market data value by coordinate.

**Algorithm:**
1. Branch: `key` is `str` -> convert via `MarketDataCoordinate.from_string`
2. Branch: `key in self.redacted_coordinates` -> raise `KeyError`
3. Assign `self.__market_data[key] = value`

**Raises:** `KeyError` when attempting to override a redacted coordinate

### OverlayMarket.__repr__(self) -> str
Purpose: Return `'Overlay ({id}): {base_market repr}'`.

### OverlayMarket.market_data (property) -> Tuple[MarketDataCoordinateValue, ...]
Purpose: Return all market data as tuple of coordinate-value pairs.

### OverlayMarket.market_model_data (property) -> str
Purpose: Return binary market model data.

### OverlayMarket.market_data_dict (property) -> MarketDataMap
Purpose: Return market data as a coordinate->value dictionary.

### OverlayMarket.location (property) -> PricingLocation
Purpose: Delegate to base market's location.

### OverlayMarket.market (property) -> _OverlayMarket
Purpose: Build underlying target object with base_market.market, market_data, and market_model_data.

### OverlayMarket.coordinates (property) -> Coordinates
Purpose: Return tuple of all (non-redacted) coordinate keys.

### OverlayMarket.redacted_coordinates (property) -> Coordinates
Purpose: Return tuple of redacted coordinate keys.

### RefMarket.__init__(self, market_ref: str)
Purpose: Initialize with a market reference string.

### RefMarket.__repr__(self) -> str
Purpose: Return `'Market Ref ({market_ref})'`.

### RefMarket.market (property) -> _RefMarket
Purpose: Return the underlying `_RefMarket` object.

### RefMarket.location (property) -> PricingLocation
Purpose: Resolve location via `market_location()` with no argument.

### RelativeMarket.__init__(self, from_market: Market, to_market: Market)
Purpose: Initialize with from and to markets.

### RelativeMarket.__repr__(self) -> str
Purpose: Return `'{from_market repr} -> {to_market repr}'`.

### RelativeMarket.market (property) -> _RelativeMarket
Purpose: Build underlying target object.

### RelativeMarket.location (property) -> Optional[PricingLocation]
Purpose: Return location if both markets share the same location.

**Algorithm:**
1. Branch: `from_market.location == to_market.location` -> return `from_market.location`
2. Branch: else -> return `None`

## State Mutation
- `CloseMarket.__date_cache`: Class-level dict, currently unused but present as mutable shared state
- `OverlayMarket.__market_data`: Mutable dict, modified via `__setitem__`
- All `__init__` methods store to private fields; only `OverlayMarket` supports post-init mutation

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `OverlayMarket.__setitem__` | When key is in `redacted_coordinates` |

## Edge Cases
- `market_location` returns `PricingLocation.LDN` as ultimate fallback when both `location` and `PricingContext.current.market_data_location` are `None`
- `close_market_date` rolls to previous business date only if current time in location timezone is before the roll time
- `MarketDataCoordinate.from_string` re-splits on `[,_;]` only when `mkt_point` has exactly 1 element (handles multiple delimiter conventions)
- `OverlayMarket.__init__` silently filters out `'redacted'` string values from the market_data dict rather than raising
- `CloseMarket.location` and `CloseMarket.date` short-circuit resolution when `check=False` and values are provided
- `RelativeMarket.location` returns `None` (not an error) when locations differ
- Location coercion pattern (`isinstance(location, PricingLocation) or location is None`) is repeated in 4 classes

## Coverage Notes
- Branch count: ~30
- Key branches: `market_location` None/non-None (2), `close_market_date` now_time vs roll_time (2), `CloseMarket.location` check bypass (2), `CloseMarket.date` check bypass (2), location coercion in 4 constructors (8), `OverlayMarket.__getitem__/__setitem__` str check (4), redacted check (2), `RelativeMarket.location` equality check (2), `MarketDataCoordinate.__repr__` mkt_point/mkt_quoting_style (4), `from_string` mkt_point length (2)
- Pragmas: none
