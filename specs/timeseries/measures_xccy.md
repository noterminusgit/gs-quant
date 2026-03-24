# measures_xccy.py

## Summary
Cross-currency swap rate measures module that provides end-of-day zero-coupon cross-currency swap curves across major currencies (AUD, CAD, CHF, DKK, EUR, GBP, JPY, NOK, NZD, SEK, SGD, USD). It maps currencies to rate option benchmarks and designated maturities, resolves TDAPI assets by querying the GS asset API (with leg-flip and clearing-house fallback logic), and exposes a `crosscurrency_swap_rate` plot measure that returns cross-currency swap spread time series.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.data` (QueryType, GsDataApi), `gs_quant.errors` (MqValueError), `gs_quant.markets.securities` (AssetIdentifier, Asset, SecurityMaster), `gs_quant.common` (Currency as CurrencyEnum, AssetClass, AssetType, PricingLocation), `gs_quant.timeseries` (ASSET_SPEC, plot_measure, MeasureDependency, GENERIC_DATE), `gs_quant.timeseries.measures` (_asset_from_spec, _market_data_timed, ExtendedSeries), `gs_quant.timeseries.measures_rates` (as tm_rates: _ClearingHouse, CURRENCY_TO_PRICING_LOCATION, _is_valid_relative_date_tenor, _check_clearing_house, _check_forward_tenor, _pricing_location_normalized)
- External: `logging`, `collections` (OrderedDict), `enum` (Enum), `typing` (Optional, Union, Dict), `pandas` (pd)

## Type Definitions

### CrossCurrencyRateOptionType (Enum)
Inherits: `Enum`

Represents the benchmark/rate-option type for a cross-currency swap leg.

| Value | Raw | Description |
|-------|-----|-------------|
| LIBOR | `"LIBOR"` | LIBOR-based benchmark |
| OIS | `"OIS"` | Overnight index swap benchmark |
| EUROSTR | `"EUROSTR"` | Euro short-term rate benchmark |
| SOFR | `"SOFR"` | Secured overnight financing rate benchmark |
| SOFRVLIBOR | `"SOFRVLIBOR"` | SOFR vs LIBOR basis benchmark |
| TestRateOption | `"TestRateOption"` | Test-only rate option type |

### TdapiCrossCurrencyRatesDefaultsProvider (class)
Provides default rate-option and designated-maturity lookups for cross-currency rates by currency and benchmark type.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| defaults | `dict` | (constructor arg) | Raw defaults dictionary, augmented with `MAPPING` and `MATURITIES` sub-dicts |
| EMPTY_PROPERTY | `str` (class-level) | `"null"` | Sentinel indicating a property should be excluded from asset queries |

## Enums and Constants

### CrossCurrencyRateOptionType(Enum)
See Type Definitions above.

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `CROSSCURRENCY_RATES_DEFAULTS` | `dict` | (large dict, see below) | Default configuration for currencies: rate options, designated maturities, pricing locations, and common parameters (payerSpread=ATM, clearingHouse=NONE, terminationTenor=5y, effectiveDate=0b) |
| `crossCurrencyRates_defaults_provider` | `TdapiCrossCurrencyRatesDefaultsProvider` | instantiated from `CROSSCURRENCY_RATES_DEFAULTS` | Singleton provider for rate-option/maturity defaults |
| `CURRENCY_TO_XCCY_SWAP_RATE_BENCHMARK` | `Dict[str, OrderedDict]` | OrderedDicts mapping benchmark type strings to rate-option strings for each of 12 currencies | Maps currency ISO codes to ordered benchmark-type -> rate-option mappings |
| `CURRENCY_TO_DUMMY_CROSSCURRENCY_SWAP_BBID` | `Dict[str, str]` | Map of 11 currency codes to Marquee IDs | Dummy asset IDs used for availability checking per currency |

#### CROSSCURRENCY_RATES_DEFAULTS structure
```
{
  "CURRENCIES": {
    "<CCY>": [
      {"BenchmarkType": str, "rateOption": str, "designatedMaturity": str, "pricingLocation": [str]},
      ...
    ],
    ...  # AUD, CAD, CHF, DKK, EUR, GBP, JPY, NOK, NZD, SEK, SGD, USD
  },
  "COMMON": {"payerSpread": "ATM", "clearingHouse": "NONE", "terminationTenor": "5y", "effectiveDate": "0b"}
}
```

## Functions/Methods

### TdapiCrossCurrencyRatesDefaultsProvider.__init__(self, defaults: dict) -> None
Purpose: Initialize the provider by building MAPPING and MATURITIES lookup dicts from the CURRENCIES sub-dict.

**Algorithm:**
1. Store `defaults` as `self.defaults`.
2. Initialize empty `benchmark_mappings` and `maturity_mappings` dicts.
3. For each currency key `k` in `defaults["CURRENCIES"]`:
   a. Create empty sub-dicts `benchmark_mappings[k]` and `maturity_mappings[k]`.
   b. For each entry `e` in the currency's list:
      - Map `e["BenchmarkType"]` -> `e["rateOption"]` in `benchmark_mappings[k]`.
      - Map `e["BenchmarkType"]` -> `e["designatedMaturity"]` in `maturity_mappings[k]`.
4. Store as `self.defaults["MAPPING"]` and `self.defaults["MATURITIES"]`.

### TdapiCrossCurrencyRatesDefaultsProvider.get_rateoption_for_benchmark(self, currency: CurrencyEnum, benchmark: str) -> str
Purpose: Look up the rate option string for a given currency and benchmark type.

**Algorithm:**
1. Return `self.defaults["MAPPING"][currency.value][benchmark]`.

### TdapiCrossCurrencyRatesDefaultsProvider.get_maturity_for_benchmark(self, currency: CurrencyEnum, benchmark: str) -> str
Purpose: Look up the designated maturity for a given currency and benchmark type.

**Algorithm:**
1. Return `self.defaults["MATURITIES"][currency.value][benchmark]`.

### _currency_to_tdapi_crosscurrency_swap_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Convert an asset spec to a TDAPI cross-currency swap rate asset ID (for dependency checking).

**Algorithm:**
1. Resolve asset from spec via `_asset_from_spec(asset_spec)`.
2. Get Bloomberg ID via `asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
3. Look up `bbid` in `CURRENCY_TO_DUMMY_CROSSCURRENCY_SWAP_BBID`; if not found, fall back to `asset.get_marquee_id()`.
4. Return the result.

### _get_tdapi_crosscurrency_rates_assets(allow_many: bool = False, **kwargs) -> Union[str, list]
Purpose: Query GS Asset API for cross-currency rate assets matching given parameters, with fallback logic for leg-flipping and clearing-house removal.

**Algorithm:**
1. Remove `pricing_location` from kwargs if present (not a valid asset query param).
2. Query `GsAssetApi.get_many_assets(**kwargs)` -> `assets`.
3. **First leg flip:** If `len(assets) == 0` and `asset_parameters_payer_rate_option` in kwargs:
   a. Swap payer/receiver rate options.
   b. If payer designated maturity exists, swap payer/receiver designated maturities.
   c. If payer currency exists, swap payer/receiver currencies.
   d. Re-query assets.
4. **Clearing house removal:** If `len(assets) == 0` and `asset_parameters_clearing_house` in kwargs:
   a. If clearing house value is `_ClearingHouse.NONE.value`, delete the clearing house param and re-query.
5. **Second leg flip:** If `len(assets) == 0` and `asset_parameters_payer_rate_option` in kwargs:
   a. Repeat the same swap logic as step 3 and re-query.
6. Branch on result count:
   - `len(assets) > 1`: If `asset_parameters_termination_date` not in kwargs OR `asset_parameters_effective_date` not in kwargs OR `allow_many` is True -> return list of asset IDs. Otherwise raise `MqValueError('Specified arguments match multiple assets')`.
   - `len(assets) == 0`: Raise `MqValueError('Specified arguments did not match any asset in the dataset' + str(kwargs))`.
   - `len(assets) == 1`: Return `assets[0].id`.

**Raises:** `MqValueError` when no assets found or multiple assets found with fully specified termination/effective dates.

### _check_crosscurrency_rateoption_type(currency: CurrencyEnum, benchmark_type: Union[CrossCurrencyRateOptionType, str]) -> CrossCurrencyRateOptionType
Purpose: Validate and convert a benchmark type (string or enum) for a given currency.

**Algorithm:**
1. If `benchmark_type` is a string:
   a. If `benchmark_type.upper()` is in `CrossCurrencyRateOptionType.__members__`, convert to enum.
   b. Otherwise raise `MqValueError` listing valid options.
2. If `benchmark_type` is a `CrossCurrencyRateOptionType` and its value is not in `CURRENCY_TO_XCCY_SWAP_RATE_BENCHMARK[currency.value]` keys:
   a. Raise `MqValueError('%s is not supported for %s', ...)`.
3. Return the validated `benchmark_type`.

**Raises:** `MqValueError` when benchmark type string is invalid or not supported for the currency.

### _get_crosscurrency_swap_leg_defaults(currency: CurrencyEnum, benchmark_type: CrossCurrencyRateOptionType = None) -> Dict
Purpose: Get default leg parameters (currency, rateOption, designatedMaturity, pricing_location) for a cross-currency swap leg.

**Algorithm:**
1. Look up pricing location from `tm_rates.CURRENCY_TO_PRICING_LOCATION`, defaulting to `PricingLocation.LDN`.
2. If `benchmark_type` is None, default to the first key in `CURRENCY_TO_XCCY_SWAP_RATE_BENCHMARK[currency.value]`.
3. Look up rate option string from `CURRENCY_TO_XCCY_SWAP_RATE_BENCHMARK[currency.value]` for the benchmark type (default empty string if not found).
4. Look up designated maturity from `crossCurrencyRates_defaults_provider`.
5. Return dict with keys: `currency`, `rateOption`, `designatedMaturity`, `pricing_location`.

### _get_crosscurrency_swap_csa_terms(curr: str, crosscurrencyindextype: str) -> dict
Purpose: Return CSA terms dictionary for a cross-currency swap.

**Algorithm:**
1. Return `{"csaTerms": curr + "-1"}`.

### _get_crosscurrency_swap_data(asset1: Asset, asset2: Asset, swap_tenor: str, rateoption_type: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: tm_rates._ClearingHouse = None, source: str = None, real_time: bool = False, query_type: QueryType = QueryType.SWAP_RATE, location: PricingLocation = None) -> pd.DataFrame
Purpose: Fetch cross-currency swap market data for a pair of currency assets.

**Algorithm:**
1. Branch: `real_time` is True -> raise `NotImplementedError`.
2. Extract `currency1` and `currency2` from asset Bloomberg IDs.
3. Branch: If either currency not in `CURRENCY_TO_XCCY_SWAP_RATE_BENCHMARK` keys -> raise `NotImplementedError`.
4. Validate rate option types for both currencies via `_check_crosscurrency_rateoption_type`.
5. Branch: If `rateoption_type1 != rateoption_type2` -> raise `MqValueError`.
6. Validate clearing house via `tm_rates._check_clearing_house`.
7. Get leg defaults for both currencies.
8. Branch: If `swap_tenor` is not a valid relative date tenor -> raise `MqValueError`.
9. Determine default pricing location: if currency1's location is NYC, use currency2's location and currency2; otherwise use currency1's.
10. If `location` is None, use default; otherwise use provided location. Normalize via `_pricing_location_normalized`.
11. Validate forward tenor via `tm_rates._check_forward_tenor`.
12. Build kwargs dict for asset lookup (asset_class=Rates, type=XccySwapMTM, etc.).
13. Resolve asset via `_get_tdapi_crosscurrency_rates_assets`.
14. Build market data query and fetch via `_market_data_timed`.
15. Return the DataFrame.

**Raises:** `NotImplementedError` for real-time or unsupported currencies. `MqValueError` for mismatched rate option types or invalid swap tenor.

### crosscurrency_swap_rate(asset: Asset, swap_tenor: str, rateoption_type: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: tm_rates._ClearingHouse = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day Zero Coupon CrossCurrency Swap curves across major currencies. Decorated with `@plot_measure` for AssetClass.Cash/FX and AssetType.Currency/Cross.

**Algorithm:**
1. Branch on asset type:
   - `AssetType.Cross`: Split 6-char pair name into two 3-char currency codes; look up both as assets from SecurityMaster by Bloomberg ID.
   - `AssetType.Currency`: Use asset as asset1; look up "USD" as asset2.
   - Otherwise: Raise `MqValueError('Asset type not supported')`.
2. Call `_get_crosscurrency_swap_data(asset1, asset2, ...)` with `query_type=QueryType.XCCY_SWAP_SPREAD`.
3. If DataFrame is empty, return empty `ExtendedSeries(dtype=float)`; otherwise return `ExtendedSeries(df['xccySwapSpread'])`.
4. Attach `dataset_ids` from DataFrame to the series.
5. Return the series.

**Raises:** `MqValueError` when asset type is not Currency or Cross.

## State Mutation
- `crossCurrencyRates_defaults_provider`: Module-level singleton instantiated at import time from `CROSSCURRENCY_RATES_DEFAULTS`. Its `defaults` dict is augmented with `MAPPING` and `MATURITIES` during `__init__`.
- No thread safety concerns noted; all functions are stateless beyond reading the module-level constants.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_get_tdapi_crosscurrency_rates_assets` | No matching assets found, or multiple assets matched with fully specified dates |
| `MqValueError` | `_check_crosscurrency_rateoption_type` | Invalid benchmark type string, or benchmark type not supported for currency |
| `MqValueError` | `_get_crosscurrency_swap_data` | Rate option types differ between currencies; invalid swap tenor |
| `MqValueError` | `crosscurrency_swap_rate` | Unsupported asset type |
| `NotImplementedError` | `_get_crosscurrency_swap_data` | `real_time=True`, or currency not in supported set |

## Edge Cases
- When `_get_tdapi_crosscurrency_rates_assets` finds no assets on initial query, it tries up to 3 additional fallback strategies (flip legs, remove clearing house, flip legs again) before raising.
- The second leg flip (step 5 in `_get_tdapi_crosscurrency_rates_assets`) may re-flip legs back to the original configuration if clearing house removal already yielded no results.
- `_get_crosscurrency_swap_leg_defaults` with `benchmark_type=None` defaults to the first key in the OrderedDict for the currency, which depends on insertion order.
- `_get_crosscurrency_swap_csa_terms` ignores the `crosscurrencyindextype` parameter entirely.
- `crosscurrency_swap_rate` splits FX cross names by fixed 3-char slicing (e.g., "EURUSD" -> "EUR", "USD"), so non-standard pair names would break.

## Bugs Found
- Line 428: `MqValueError('%s is not supported for %s', benchmark_type.value, currency.value)` passes extra args as positional to MqValueError rather than using string formatting (should use `%` operator or f-string). The error message will only show the format string, not the substituted values. (OPEN)

## Coverage Notes
- Branch count: ~30
- Key branches: leg-flip logic (lines 349-367, 375-393), clearing house removal (369-372), multiple/zero/single asset result (395-408), asset type dispatch in `crosscurrency_swap_rate` (585-594), real-time check (470-471), currency support checks (476-479), rate option mismatch (484-485), pricing location NYC check (496-501), location None check (503-506)
- Pragmas: none
