# measures_fx_vol.py

## Summary
Provides GS end-of-day FX volatility, forward points, vol swap strike, and spot carry market data retrieval functions. The module defines asset lookup helpers that translate currency pairs into Marquee TDAPI asset identifiers (for FX options, FX forwards, and FX volatility swaps), resolves cross stored-direction conventions, and queries the GS Data API. All public measure functions are decorated with `@plot_measure` for Chart Service integration.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (`GsAssetApi`), `gs_quant.api.gs.data` (`QueryType`, `GsDataApi`), `gs_quant.common` (`AssetClass`, `AssetType`, `PricingLocation`), `gs_quant.data` (`DataContext`, `Dataset`), `gs_quant.errors` (`MqValueError`), `gs_quant.markets.securities` (`AssetIdentifier`, `Asset`, `SecurityMaster`), `gs_quant.timeseries` (`ASSET_SPEC`, `plot_measure`, `MeasureDependency`, `FXSpotCarry`, `ExtendedSeries`), `gs_quant.timeseries.measures_rates` (aliased `tm_rates`; uses `_is_valid_relative_date_tenor`), `gs_quant.timeseries.measures` (`_asset_from_spec`, `_market_data_timed`, `_cross_stored_direction_helper`, `_preprocess_implied_vol_strikes_fx`, `_tenor_month_to_year`), `gs_quant.timeseries.measures_helper` (`VolReference`)
- External: `logging` (stdlib), `enum` (`Enum`), `numbers` (`Real`), `typing` (`Union`, `Optional`), `pandas` (`pd`)

## Type Definitions

### OptionType(Enum)
Represents the type of FX vanilla option.

| Value | Raw | Description |
|-------|-----|-------------|
| CALL | `"Call"` | Call option |
| PUT | `"Put"` | Put option |
| STRADDLE | `"Straddle"` | Straddle (combined call + put at same strike) |

### TdapiFXDefaultsProvider (class)
Purpose: Thin wrapper around a dictionary of FX cross defaults, providing a method to retrieve per-cross default parameters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| defaults | `dict` | (required) | Map of cross string (e.g. `"EURUSD"`) to dict of default parameters |

#### Class Constant
| Name | Type | Value | Description |
|------|------|-------|-------------|
| EMPTY_PROPERTY | `str` | `"null"` | Sentinel flag indicating a property should be excluded from an asset query |

### ASSET_SPEC
```
ASSET_SPEC = Union[Asset, str]
```
Imported from `gs_quant.timeseries`. Represents either a security master `Asset` object or a string identifier.

### VolReference(Enum)
Imported from `gs_quant.timeseries.measures_helper`.

| Value | Raw | Description |
|-------|-----|-------------|
| DELTA_CALL | `"delta_call"` | Delta-referenced call strike |
| DELTA_PUT | `"delta_put"` | Delta-referenced put strike |
| DELTA_NEUTRAL | `"delta_neutral"` | Delta-neutral strike |
| NORMALIZED | `"normalized"` | Normalized strike |
| SPOT | `"spot"` | Spot reference |
| FORWARD | `"forward"` | Forward (ATMF) reference |

### FXSpotCarry(Enum)
Imported from `gs_quant.timeseries.measures`.

| Value | Raw | Description |
|-------|-----|-------------|
| ANNUALIZED | `"annualized"` | Return carry in annualized terms |
| DAILY | `"daily"` | Return carry in daily terms |

## Enums and Constants

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `FX_DEFAULTS` | `dict[str, dict]` | (see below) | Maps 53 FX cross strings to dicts with keys `under`, `over`, `expirationTime`, `premiumPaymentDate` |
| `fx_defaults_provider` | `TdapiFXDefaultsProvider` | `TdapiFXDefaultsProvider(FX_DEFAULTS)` | Module-level singleton defaults provider |
| `FX_VOL_SWAP_DEFAULTS` | `list[str]` | 13 cross strings | Crosses supported for FX vol swap queries |
| `CURRENCY_TO_DUMMY_FFO_BBID` | `dict[str, str]` | 53 entries | Maps each FX cross to a Marquee dummy asset ID for FX options availability checks |
| `CURRENCY_TO_DUMMY_FFO_BBID_VOL_SWAPS` | `dict[str, str]` | 13 entries | Maps each FX vol swap cross to a Marquee dummy asset ID |

**FX_DEFAULTS structure** (each entry):
```python
{
    "under": str,              # Underlying currency (e.g. "EUR")
    "over": str,               # Over currency (e.g. "USD")
    "expirationTime": str,     # Default expiration time location (always "NYC")
    "premiumPaymentDate": str  # Default premium payment date (always "Fwd Settle")
}
```

**FX_VOL_SWAP_DEFAULTS** (supported crosses):
`EURUSD`, `GBPUSD`, `USDCHF`, `DKKUSD`, `NOKUSD`, `SEKUSD`, `USDCAD`, `USDJPY`, `AUDUSD`, `NZDUSD`, `USDCNH`, `INRUSD`, `USDSGD`

## Functions/Methods

### TdapiFXDefaultsProvider.__init__(self, defaults: dict)
Purpose: Store the defaults dictionary.

**Algorithm:**
1. Set `self.defaults = defaults`.

---

### TdapiFXDefaultsProvider.get_defaults_for_cross(self, cross: str) -> dict
Purpose: Return a copy of the default parameters for a given FX cross.

**Algorithm:**
1. Look up `self.defaults.get(cross)`.
2. Return `dict(...)` (a shallow copy of the retrieved dict).

**Note:** If `cross` is not in `self.defaults`, `self.defaults.get(cross)` returns `None`, and `dict(None)` will raise `TypeError`.

---

### _currencypair_to_tdapi_fxfwd_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Convert a currency pair asset spec to a TDAPI FX Forward asset Marquee ID.

**Algorithm:**
1. Resolve `asset` from `asset_spec` via `_asset_from_spec`.
2. Get the Bloomberg ID (`bbid`) of the asset.
3. Build kwargs dict with `asset_class='FX'`, `type='Forward'`, `asset_parameters_pair=bbid`, `asset_parameters_settlement_date='1y'`.
4. Call `_get_tdapi_fxo_assets(**kwargs)` to look up the Marquee ID.
5. Return the Marquee ID string.

---

### _currencypair_to_tdapi_fxo_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Convert a currency pair asset spec to a TDAPI FX Option dummy asset Marquee ID for availability checking.

**Algorithm:**
1. Resolve `asset` from `asset_spec` via `_asset_from_spec`.
2. Get the Bloomberg ID (`bbid`) of the asset.
3. Look up `bbid` in `CURRENCY_TO_DUMMY_FFO_BBID`.
4. Branch: if found -> return the mapped dummy ID.
5. Branch: else -> return `asset.get_marquee_id()` as fallback.

---

### _currencypair_to_tdapi_fx_vol_swap_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Convert a currency pair asset spec to a TDAPI FX Vol Swap dummy asset Marquee ID.

**Algorithm:**
1. Resolve `asset` from `asset_spec` via `_asset_from_spec`.
2. Get the Bloomberg ID (`bbid`) of the asset.
3. Look up `bbid` in `CURRENCY_TO_DUMMY_FFO_BBID_VOL_SWAPS`.
4. Branch: if found -> return the mapped dummy ID.
5. Branch: else -> return `asset.get_marquee_id()` as fallback.

---

### _get_tdapi_fxo_assets(**kwargs) -> Union[str, list]
Purpose: Query GS Asset API for FX option/forward assets matching the given criteria and return a single Marquee ID.

**Algorithm:**
1. Branch: if `"pricing_location"` is in kwargs -> delete it (sanitize).
2. Pop `"name_prefix"` from kwargs (may be `None`).
3. Call `GsAssetApi.get_many_assets(**kwargs)` to retrieve matching assets.
4. Branch: if `len(assets) > 1`:
   - Branch: if `name_prefix` is truthy -> iterate assets; if any asset's name starts with `name_prefix`, return `asset.id`.
   - If no match found or no `name_prefix` -> raise `MqValueError('Specified arguments match multiple assets' + str(kwargs))`.
5. Branch: if `len(assets) == 0` -> raise `MqValueError('Specified arguments did not match any asset in the dataset' + str(kwargs))`.
6. Branch: else (exactly one asset) -> return `assets[0].id`.

**Raises:** `MqValueError` when zero or multiple assets match (and no name_prefix disambiguation succeeds).

---

### get_fxo_asset(asset: Asset, expiry_tenor: str, strike: str, option_type: str = None, expiration_location: str = None, premium_payment_date: str = None) -> str
Purpose: Build the full parameter set for an FX vanilla option asset query and return its Marquee ID.

**Algorithm:**
1. Get `cross = asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
2. Branch: if `cross` not in `FX_DEFAULTS.keys()` -> raise `NotImplementedError('Data not available for {} FX Vanilla options')`.
3. Fetch `defaults = _get_fxo_defaults(cross)`.
4. Branch: if expiry_tenor fails `tm_rates._is_valid_relative_date_tenor()` -> raise `MqValueError('invalid expiry ' + expiry_tenor)`.
5. Branch: if `expiration_location is None` -> use `defaults["expirationTime"]` (assigned to `_` -- value unused).
6. Branch: else -> use the provided `expiration_location` (also assigned to `_` -- value unused).
7. Branch: if `premium_payment_date is None` -> use `defaults["premiumPaymentDate"]` as `premium_date`.
8. Branch: else -> use the provided `premium_payment_date`.
9. Branch: if `option_type == "Put"`:
   - `call_ccy = defaults["over"]`, `put_ccy = defaults["under"]`.
10. Branch: else (Call, Straddle, or None):
    - `call_ccy = defaults["under"]`, `put_ccy = defaults["over"]`.
11. Build kwargs dict with `asset_class='FX'`, `type='Option'`, call/put currencies, expiration date, option type, premium payment date, and strike price relative.
12. Return `_get_tdapi_fxo_assets(**kwargs)`.

**Raises:** `NotImplementedError` for unsupported crosses. `MqValueError` for invalid expiry tenor.

---

### _get_tdapi_fxo_assets_vol_swaps(**kwargs) -> Union[str, list]
Purpose: Query for FX Volatility Swap assets and match by expiry tenor parameter.

**Algorithm:**
1. Extract `expiry_tenor` from kwargs.
2. Build `inputs` dict by filtering out `"expiry_tenor"` and `"pricing_location"` keys.
3. Call `GsAssetApi.get_many_assets(**inputs)`.
4. Branch: if `len(assets) == 0` -> raise `MqValueError('No assets found matching search criteria' + str(kwargs))`.
5. Branch: if `expiry_tenor is not None`:
   - Iterate assets; for each asset, check if `asset.parameters["lastFixingDate"].lower() == expiry_tenor.lower()`.
   - Branch: if match found -> return `asset.id`.
6. If no match found (or `expiry_tenor` is `None` with assets present) -> raise `MqValueError('Specified arguments did not match any asset in the dataset' + str(kwargs))`.

**Raises:** `MqValueError` when no assets found or no asset matches the expiry tenor.

---

### cross_stored_direction_for_fx_vol(asset_spec: ASSET_SPEC) -> Union[str, Asset]
Purpose: Resolve the stored-direction cross for an FX vol asset and return its dummy FX option Marquee ID.

**Algorithm:**
1. Resolve `asset` from `asset_spec` via `_asset_from_spec`.
2. Set `result = asset`.
3. Try:
   - Branch: if `asset.asset_class is AssetClass.FX`:
     - Get `bbid` from Bloomberg ID.
     - Branch: if `bbid is not None`:
       - Call `_cross_stored_direction_helper(bbid)` to get the canonical `cross`.
       - Branch: if `cross != bbid` -> look up `cross_asset` via `SecurityMaster.get_asset(cross, AssetIdentifier.BLOOMBERG_ID)` and set `result = cross_asset`.
4. Branch: except `TypeError` -> set `result = asset` (fallback on type errors).
5. Return `_currencypair_to_tdapi_fxo_asset(result)`.

---

### _get_fxo_defaults(cross: str) -> dict
Purpose: Retrieve default FX option parameters for a cross. Thin wrapper around the module-level `fx_defaults_provider`.

**Algorithm:**
1. Return `fx_defaults_provider.get_defaults_for_cross(cross)`.

---

### _get_fx_csa_terms() -> dict
Purpose: Return the default CSA terms for FX.

**Algorithm:**
1. Return `dict(csaTerms='USD-1')`.

---

### _get_fx_vol_swap_data(asset: Asset, expiry_tenor: str, strike_type: str = None, location: PricingLocation = None, source: str = None, real_time: bool = False, query_type: QueryType = QueryType.STRIKE_VOL) -> pd.DataFrame
Purpose: Fetch FX vol swap market data from the GS Data API.

**Algorithm:**
1. Branch: if `real_time` -> raise `NotImplementedError('realtime FX Vol swap data not implemented')`.
2. Get `cross = asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
3. Branch: if `cross` not in `FX_VOL_SWAP_DEFAULTS` -> raise `NotImplementedError('Data not available for {} FX Vol Swaps')`.
4. Build kwargs for vol swap asset query: `asset_class='FX'`, `type='VolatilitySwap'`, `expiry_tenor`, `asset_parameters_pair=cross`.
5. Look up the Marquee ID via `_get_tdapi_fxo_assets_vol_swaps(**kwargs)`.
6. Branch: if `location is None` -> `pricing_location = PricingLocation.NYC`.
7. Branch: else -> `pricing_location = PricingLocation(location)`.
8. Build a market data query with `pricingLocation` where-clause.
9. Execute `_market_data_timed(q)` and return the resulting DataFrame.

**Raises:** `NotImplementedError` for real-time queries or unsupported crosses.

---

### _get_fxfwd_data(asset: Asset, settlement_date: str, location: str = None, source: str = None, real_time: bool = False, query_type: QueryType = QueryType.FWD_POINTS) -> pd.DataFrame
Purpose: Fetch FX forward points market data from the GS Data API.

**Algorithm:**
1. Branch: if `real_time`:
   - Get Marquee ID directly from asset.
   - Build query with `QueryType.FORWARD_POINT` and `where={'tenor': settlement_date}`.
   - Execute and return the DataFrame.
2. (Non-real-time path):
3. Get `cross = asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
4. Branch: if `settlement_date` fails `tm_rates._is_valid_relative_date_tenor()` -> raise `MqValueError('invalid settlements date ' + settlement_date)`.
5. Build kwargs with `asset_class='FX'`, `type='Forward'`, `asset_parameters_pair=cross`, `asset_parameters_settlement_date=settlement_date`, `name_prefix='FX Forward'`.
6. Look up Marquee ID via `_get_tdapi_fxo_assets(**kwargs)`.
7. Branch: if `location is None` -> `pricing_location = PricingLocation.NYC`.
8. Branch: else -> `pricing_location = PricingLocation(location)`.
9. Build market data query with `pricingLocation` where-clause.
10. Execute and return the DataFrame.

**Raises:** `MqValueError` for invalid settlement date tenor.

---

### _get_fxo_data(asset: Asset, expiry_tenor: str, strike: str, option_type: str = None, expiration_location: str = None, location: PricingLocation = None, premium_payment_date: str = None, source: str = None, real_time: bool = False, query_type: QueryType = QueryType.IMPLIED_VOLATILITY) -> pd.DataFrame
Purpose: Fetch FX vanilla option market data (typically implied volatility) from the GS Data API.

**Algorithm:**
1. Branch: if `real_time` -> raise `NotImplementedError('realtime FX Option data not implemented')`.
2. Call `get_fxo_asset(...)` to resolve the Marquee ID for the specific option.
3. Branch: if `location is None` -> `pricing_location = PricingLocation.NYC`.
4. Branch: else -> `pricing_location = PricingLocation(location)`.
5. Build market data query with `pricingLocation` where-clause.
6. Execute `_market_data_timed(q)` and return the DataFrame.

**Raises:** `NotImplementedError` for real-time queries. Propagates errors from `get_fxo_asset`.

---

### implied_volatility_new(asset: Asset, expiry_tenor: str, strike: str, option_type: str = None, expiration_location: str = None, location: PricingLocation = None, premium_payment_date: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Retrieve GS end-of-day FX vanilla implied volatilities for a specific option specification.

**Algorithm:**
1. Call `_get_fxo_data(...)` with `query_type=QueryType.IMPLIED_VOLATILITY`.
2. Branch: if `df` is empty -> create `ExtendedSeries(dtype=float)`.
3. Branch: else -> create `ExtendedSeries(df['impliedVolatility'])`.
4. Attach `dataset_ids` from the DataFrame (defaulting to empty tuple).
5. Return the series.

---

### implied_volatility_fxvol(asset: Asset, tenor: str, strike_reference: VolReference = None, relative_strike: Real = None, location: Optional[PricingLocation] = None, legacy_implementation: bool = False, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: High-level FX implied volatility measure with delta/spot/forward strike references and automatic cross-direction handling.

**Decorator:** `@plot_measure` with `AssetClass.FX`, `AssetType.Cross`, dependency on `cross_stored_direction_for_fx_vol` for `IMPLIED_VOLATILITY`, display name `"implied_volatility"`.

**Algorithm:**
1. Get `bbid = asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
2. Branch: if `bbid is not None`:
   - Call `_cross_stored_direction_helper(bbid)` to get canonical `cross`.
   - Branch: if `cross != bbid` (cross is stored in reverse direction):
     - Look up the canonical `cross_asset` via `SecurityMaster.get_asset(cross, AssetIdentifier.BLOOMBERG_ID)`.
     - Branch: if `strike_reference.value == VolReference.DELTA_CALL.value` -> flip to `VolReference.DELTA_PUT`.
     - Branch: elif `strike_reference.value == VolReference.DELTA_PUT.value` -> flip to `VolReference.DELTA_CALL`.
   - Branch: else (`cross == bbid`) -> `cross_asset = asset`.
3. Branch: else (`bbid is None`) -> raise `MqValueError('Badly setup cross ' + asset.name)`.
4. Call `_preprocess_implied_vol_strikes_fx(strike_reference, relative_strike)` to get `(ref_string, relative_strike)`.
5. Branch: if `ref_string == 'delta'`:
   - Branch: if `relative_strike == 0` -> `strike = 'DN'`, `option_type = 'Call'`.
   - Branch: elif `relative_strike > 0` -> `strike = str(relative_strike) + 'D'`, `option_type = 'Call'`.
   - Branch: else (`relative_strike < 0`) -> `strike = str(-relative_strike) + 'D'`, `option_type = 'Put'`.
6. Branch: elif `ref_string == VolReference.SPOT.value` -> `strike = 'Spot'`, `option_type = 'Call'`.
7. Branch: elif `ref_string == VolReference.FORWARD.value` -> `strike = 'ATMF'`, `option_type = 'Call'`.
8. Branch: else -> raise `MqValueError('unknown strike_reference and relative_strike combination')`.
9. Normalize `tenor` via `_tenor_month_to_year(tenor)`.
10. Call `implied_volatility_new(cross_asset, tenor, strike, option_type, location=location, source=source, real_time=real_time)`.
11. Return the resulting series.

**Raises:** `MqValueError` when `bbid` is `None` or unknown strike combination.

---

### fwd_points(asset: Asset, settlement_date: str, location: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Retrieve GS end-of-day FX forward points for G3, G10, and EM crosses.

**Decorator:** `@plot_measure` with `AssetClass.FX`, `AssetType.Cross`, dependency on `_currencypair_to_tdapi_fxfwd_asset` for `FWD_POINTS`, display name `"forward_point"`.

**Algorithm:**
1. Call `_get_fxfwd_data(asset=asset, settlement_date=settlement_date, location=location, source=source, real_time=real_time, query_type=QueryType.FWD_POINTS)`.
2. Branch: if `real_time`:
   - Branch: if `df` is empty -> `ExtendedSeries(dtype=float)`.
   - Branch: else -> `ExtendedSeries(df['forwardPoint'])`.
3. Branch: else (EOD):
   - Branch: if `df` is empty -> `ExtendedSeries(dtype=float)`.
   - Branch: else -> `ExtendedSeries(df['fwdPoints'])`.
4. Attach `dataset_ids`.
5. Return the series.

---

### vol_swap_strike(asset: Asset, expiry_tenor: str, strike_type: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Retrieve GS end-of-day FX vol swap strike volatilities across major crosses.

**Decorator:** `@plot_measure` with `AssetClass.FX`, `AssetType.Cross`, dependency on `_currencypair_to_tdapi_fx_vol_swap_asset` for `STRIKE_VOL`.

**Algorithm:**
1. Call `_get_fx_vol_swap_data(asset=asset, expiry_tenor=expiry_tenor, strike_type=strike_type, location=location, source=source, real_time=real_time, query_type=QueryType.STRIKE_VOL)`.
2. Branch: if `df` is empty -> `ExtendedSeries(dtype=float)`.
3. Branch: else -> `ExtendedSeries(df['strikeVol'])`.
4. Attach `dataset_ids`.
5. Return the series.

---

### spot_carry(asset: Asset, tenor: str, annualized: FXSpotCarry = FXSpotCarry.DAILY, pricing_location: Optional[PricingLocation] = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Calculate FX spot carry from the forward term structure (forward points / spot), with optional annualization.

**Decorator:** `@plot_measure` with `AssetClass.FX`, `None` asset type, `QueryType.FORWARD_POINT`.

**Algorithm:**
1. Branch: if `real_time` -> raise `NotImplementedError('realtime spot_carry not implemented')`.
2. Branch: if `tenor` not in the allowed list (`'1m'` through `'2y'`, 16 values) -> raise `MqValueError('tenor not included in dataset')`.
3. Get `cross = asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
4. Build kwargs for FX Forward asset lookup with `name_prefix='FX Forward'`.
5. Look up Marquee ID via `_get_tdapi_fxo_assets(**kwargs)`.
6. Build and execute a market data query for `QueryType.FWD_POINTS`.
7. Branch: if `df` is empty -> return `pd.Series(dtype=float)`.
8. Extract `dataset_ids` from the DataFrame.
9. Instantiate `Dataset(dataset_ids[0])` and query for full data with `pricingLocation`, `start`, and `end` from `DataContext.current`.
10. Branch: if `mq_df` is empty -> return `pd.Series(dtype=float)`.
11. Reset index and compute `ann_factor = 360 / (settlementDate - date).days` for each row.
12. Set index back to `'date'`.
13. Branch: if `annualized == FXSpotCarry.ANNUALIZED`:
    - `carry = -1 * ann_factor * fwdPoints / spot`.
14. Branch: else (DAILY):
    - `carry = -1 * fwdPoints / spot`.
15. Create `ExtendedSeries(mq_df['carry'], name='spotCarry')`, attach `dataset_ids`.
16. Return the series.

**Raises:** `NotImplementedError` for real-time. `MqValueError` for invalid tenor.

**Allowed tenors:** `'1m'`, `'2m'`, `'3m'`, `'4m'`, `'5m'`, `'6m'`, `'7m'`, `'8m'`, `'9m'`, `'10m'`, `'11m'`, `'1y'`, `'15m'`, `'18m'`, `'21m'`, `'2y'`

## State Mutation
- `fx_defaults_provider`: Module-level singleton, initialized once at import. Not mutated after initialization.
- No global state is modified by any function.
- All functions are stateless beyond reading module-level constants and calling external APIs.
- Thread safety: Functions are safe for concurrent use as they do not write shared state. However, they depend on `DataContext.current` (thread-local) and the GS API session context.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `get_fxo_asset` | Cross not in `FX_DEFAULTS` |
| `MqValueError` | `get_fxo_asset` | Invalid expiry tenor |
| `MqValueError` | `_get_tdapi_fxo_assets` | Zero assets match the query |
| `MqValueError` | `_get_tdapi_fxo_assets` | Multiple assets match with no name_prefix disambiguation |
| `MqValueError` | `_get_tdapi_fxo_assets_vol_swaps` | Zero assets match the query |
| `MqValueError` | `_get_tdapi_fxo_assets_vol_swaps` | No asset matches the expiry tenor |
| `NotImplementedError` | `_get_fx_vol_swap_data` | `real_time=True` |
| `NotImplementedError` | `_get_fx_vol_swap_data` | Cross not in `FX_VOL_SWAP_DEFAULTS` |
| `MqValueError` | `_get_fxfwd_data` | Invalid settlement date tenor (non-real-time path) |
| `NotImplementedError` | `_get_fxo_data` | `real_time=True` |
| `MqValueError` | `implied_volatility_fxvol` | `bbid` is `None` |
| `MqValueError` | `implied_volatility_fxvol` | Unknown strike_reference/relative_strike combination |
| `NotImplementedError` | `spot_carry` | `real_time=True` |
| `MqValueError` | `spot_carry` | Tenor not in allowed list |
| `TypeError` | `TdapiFXDefaultsProvider.get_defaults_for_cross` | Cross key not present (returns `None`, `dict(None)` raises) |

## Edge Cases
- `_currencypair_to_tdapi_fxo_asset` and `_currencypair_to_tdapi_fx_vol_swap_asset`: Fall back to the asset's own Marquee ID when the cross is not in the hardcoded lookup dictionaries.
- `_get_tdapi_fxo_assets` with multiple matching assets and a `name_prefix`: iterates all assets to find one whose name starts with the prefix; if none match, still raises `MqValueError`.
- `_get_tdapi_fxo_assets_vol_swaps` with `expiry_tenor=None`: will always reach the final `raise MqValueError` since the `expiry_tenor is not None` guard prevents the iteration.
- `cross_stored_direction_for_fx_vol`: catches `TypeError` to handle cases where `asset.asset_class` comparison fails (e.g., if asset_class is not an enum).
- `implied_volatility_fxvol` delta strike flipping: When the cross is stored in reverse direction, delta call becomes delta put and vice versa, but delta neutral and other references are not flipped.
- `implied_volatility_fxvol` with `relative_strike == 0` and `ref_string == 'delta'`: maps to delta-neutral (`'DN'`) strike with Call option type.
- `implied_volatility_fxvol`: The `legacy_implementation` parameter is accepted but completely ignored (deprecated).
- `fwd_points` real-time vs EOD: uses different DataFrame column names (`'forwardPoint'` for real-time, `'fwdPoints'` for EOD).
- `spot_carry` uses the first `dataset_id` from the initial query to make a second, richer dataset query; if the initial query returns data but with no `dataset_ids`, this will fail with an `IndexError` on `dataset_ids[0]`.
- `spot_carry` computes `ann_factor` as `360 / (settlementDate - date).days`; if `settlementDate == date` (zero days), this will raise `ZeroDivisionError`.
- `get_fxo_asset`: The resolved `expiration_location` is assigned to `_` (throwaway) and never used in the asset query kwargs, meaning the `expiration_location` parameter has no effect on the returned asset.
- Empty DataFrame handling: `implied_volatility_new`, `fwd_points`, and `vol_swap_strike` all return an empty `ExtendedSeries(dtype=float)` when the API returns no data.

## Bugs Found
- Lines 299-302 (`get_fxo_asset`): The `expiration_location` parameter is resolved but assigned to `_` (a throwaway variable) and never included in the kwargs passed to `_get_tdapi_fxo_assets`. The parameter has no actual effect on the query. (OPEN)
- Line 350 (`_get_tdapi_fxo_assets_vol_swaps`): When `expiry_tenor is None`, the function falls through to `raise MqValueError(...)` even when assets were found, because the loop that matches by tenor is guarded by `if expiry_tenor is not None`. This means a `None` tenor always fails regardless of available assets. (OPEN)

## Coverage Notes
- Branch count: 58 (across all functions)
- Key branch counts: `_get_tdapi_fxo_assets` has 5 branches (pricing_location cleanup + name_prefix disambiguation + 3-way length check); `get_fxo_asset` has 6 branches (cross check + tenor validation + 3 None checks + put/else); `_get_tdapi_fxo_assets_vol_swaps` has 3 branches; `cross_stored_direction_for_fx_vol` has 5 branches (try/except + asset_class + bbid + cross comparison); `_get_fxfwd_data` has 4 branches (real_time + tenor validation + location); `_get_fxo_data` has 3 branches; `implied_volatility_fxvol` has 11 branches (bbid check + cross direction + 2 delta flips + ref_string dispatch with 3 delta sub-branches + spot + forward + else); `fwd_points` has 4 branches (real_time x empty); `spot_carry` has 7 branches (real_time + tenor + empty df + empty mq_df + annualized); `_get_fx_vol_swap_data` has 4 branches; `implied_volatility_new` has 2 branches.
- Pragmas: None marked.
- Many branches are behind API calls (GsAssetApi, GsDataApi, SecurityMaster) requiring mocked sessions for test coverage.
