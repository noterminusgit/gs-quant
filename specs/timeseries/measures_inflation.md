# measures_inflation.py

## Summary
Provides end-of-day Zero Coupon Inflation Swap curve data across major currencies. Contains helpers for resolving inflation swap assets from the GS asset catalog, validating inflation index types, and building market data queries. Exposes two `@plot_measure` decorated public functions: `inflation_swap_rate` (single tenor) and `inflation_swap_term` (forward term structure).

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.data` (QueryType, GsDataApi), `gs_quant.common` (Currency as CurrencyEnum, AssetClass, AssetType, PricingLocation), `gs_quant.data` (DataContext), `gs_quant.datetime` (GsCalendar), `gs_quant.errors` (MqValueError), `gs_quant.markets.securities` (AssetIdentifier, Asset), `gs_quant.timeseries` (ASSET_SPEC, plot_measure, MeasureDependency, GENERIC_DATE, ExtendedSeries, check_forward_looking), `gs_quant.timeseries.measures` (_asset_from_spec, _market_data_timed, _range_from_pricing_date, _get_custom_bd), `gs_quant.timeseries.measures_rates` (_get_term_struct_date, _ClearingHouse, CURRENCY_TO_PRICING_LOCATION, _check_clearing_house, _is_valid_relative_date_tenor, _check_forward_tenor, _default_pricing_location, _pricing_location_normalized)
- External: `logging`, `collections` (OrderedDict), `enum` (Enum), `typing` (Optional, Union), `pandas`

## Type Definitions
None (no dataclasses or type aliases beyond enums).

## Enums and Constants

### InflationIndexType(Enum)
Represents valid inflation index benchmark identifiers. Each member's value is the same as its name (e.g., `AUCPI = 'AUCPI'`).

| Value | Raw | Description |
|-------|-----|-------------|
| AUCPI | `"AUCPI"` | Australia CPI |
| BECPHLTH | `"BECPHLTH"` | Belgium CPI Health |
| CACPI | `"CACPI"` | Canada CPI |
| CPALBE | `"CPALBE"` | CPI Albania |
| CPALEMU | `"CPALEMU"` | CPI All EMU |
| CPUPAXFE | `"CPUPAXFE"` | US CPI Urban ex-food/energy |
| CPURNSA | `"CPURNSA"` | US CPI Urban NSA |
| CPUS | `"CPUS"` | US CPI |
| CPXTEMU | `"CPXTEMU"` | EMU CPI ex-tobacco |
| DNCPINEW | `"DNCPINEW"` | Denmark CPI |
| FRCPXTOB | `"FRCPXTOB"` | France CPI ex-tobacco |
| GKCPIUHL | `"GKCPIUHL"` | Greece CPI UHL |
| GKCPNEWL | `"GKCPNEWL"` | Greece CPI New |
| GRCP2010 | `"GRCP2010"` | Germany CPI 2010 |
| GRCPTK | `"GRCPTK"` | Germany CPI TK |
| IECPALL | `"IECPALL"` | Ireland CPI All |
| IECPEUI | `"IECPEUI"` | Ireland CPI EUI |
| IECPINEW | `"IECPINEW"` | Ireland CPI New |
| ILCPI | `"ILCPI"` | Israel CPI |
| INFINFY | `"INFINFY"` | India Inflation |
| ISCPIL | `"ISCPIL"` | Israel CPI L |
| ITCPI | `"ITCPI"` | Italy CPI |
| ITCPNICT | `"ITCPNICT"` | Italy CPI NICT |
| JCPNGENF | `"JCPNGENF"` | Japan CPI General |
| KRCPI | `"KRCPI"` | Korea CPI |
| MXNInfl | `"MXNInfl"` | Mexico Inflation |
| NECPIND | `"NECPIND"` | Netherlands CPI |
| NOCPI | `"NOCPI"` | Norway CPI |
| POCPILB | `"POCPILB"` | Poland CPI |
| PPFXFDE | `"PPFXFDE"` | US PPI FD&E |
| RUCP2000 | `"RUCP2000"` | Russia CPI 2000 |
| SACPI | `"SACPI"` | South Africa CPI |
| SPCPEU | `"SPCPEU"` | Spain CPI EU |
| SPIPC | `"SPIPC"` | Spain IPC |
| SWCPI | `"SWCPI"` | Sweden CPI |
| UKCPI | `"UKCPI"` | UK CPI |
| UKCPIH | `"UKCPIH"` | UK CPIH |
| UKRPI | `"UKRPI"` | UK RPI |
| TESTCPI | `"TESTCPI"` | Test CPI (testing only) |

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `INFLATION_RATES_DEFAULTS` | `dict` | See source | Default configuration for inflation rate queries by currency (EUR, USD, GBP, JPY), including index types, pricing locations, and common parameters |
| `inflationRates_defaults_provider` | `TdapiInflationRatesDefaultsProvider` | Constructed from `INFLATION_RATES_DEFAULTS` | Pre-built defaults provider instance |
| `CURRENCY_TO_INDEX_BENCHMARK` | `dict[str, OrderedDict]` | See source | Maps currency code strings (e.g., `'USD'`, `'EUR'`) to ordered dicts of `{index_type_str: cpi_index_str}`. 14 currencies supported. |
| `CURRENCY_TO_DUMMY_INFLATION_SWAP_BBID` | `dict[str, str]` | 4 entries (EUR, GBP, JPY, USD) | Maps currency codes to dummy Marquee asset IDs used for availability checks |

## Functions/Methods

### TdapiInflationRatesDefaultsProvider.__init__(self, defaults: dict)
Purpose: Initialize provider and build MAPPING from CURRENCIES config.

**Algorithm:**
1. Store `defaults` dict as `self.defaults`
2. Initialize empty `benchmark_mappings` dict
3. Iterate over `defaults["CURRENCIES"]` items (currency -> list of entries)
4. For each entry in the list, set `benchmark_mappings[currency] = {entry["IndexType"]: entry["Index"]}`
5. Note: only the LAST entry per currency is retained because each iteration overwrites the key
6. Store the mapping as `self.defaults['MAPPING']`

### TdapiInflationRatesDefaultsProvider.get_index_for_benchmark(self, currency: CurrencyEnum, benchmark: str)
Purpose: Look up the CPI index name for a given currency and benchmark type.

**Algorithm:**
1. Chain dictionary lookups: `self.defaults["MAPPING"][currency.value][benchmark]`
2. Return result (may raise KeyError if currency or benchmark not found)

### _currency_to_tdapi_inflation_swap_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Resolve an asset spec to a Marquee asset ID for inflation swap rate availability checks.

**Algorithm:**
1. Call `_asset_from_spec(asset_spec)` to get Asset object
2. Get Bloomberg ID from the asset
3. Look up BBID in `CURRENCY_TO_DUMMY_INFLATION_SWAP_BBID`
4. Branch: if BBID found in mapping -> return dummy asset ID
5. Branch: if BBID not found -> return `asset.get_marquee_id()`

### _get_tdapi_inflation_rates_assets(allow_many=False, **kwargs) -> Union[str, list]
Purpose: Query GS asset catalog for inflation swap assets matching given parameters.

**Algorithm:**
1. Branch: if `"pricing_location"` in kwargs -> delete it (sanitize input)
2. Call `GsAssetApi.get_many_assets(**kwargs)` to get assets
3. Branch: if `len(assets) == 0` AND `asset_parameters_clearing_house` in kwargs:
   - Branch: if clearing house value is `_ClearingHouse.NONE.value` -> delete clearing house param and retry query
4. Branch: if `len(assets) > 1`:
   - Branch: if `termination_date` not in kwargs OR `effective_date` not in kwargs OR `allow_many` is True -> return list of all asset IDs
   - Branch: else -> raise `MqValueError('Specified arguments match multiple assets')`
5. Branch: if `len(assets) == 0` -> raise `MqValueError` with details
6. Branch: else (exactly 1 asset) -> return `assets[0].id`

**Raises:** `MqValueError` when multiple assets match unexpectedly, or when no assets match.

### _check_inflation_index_type(currency, benchmark_type: Union[InflationIndexType, str]) -> InflationIndexType
Purpose: Validate and normalize a benchmark type, ensuring it is a valid InflationIndexType for the given currency.

**Algorithm:**
1. Branch: if `benchmark_type` is a string:
   - Branch: if `benchmark_type.upper()` is in `InflationIndexType.__members__` -> convert to enum
   - Branch: else -> raise `MqValueError` listing valid index types
2. Branch: if `benchmark_type` is an InflationIndexType AND its value is NOT in `CURRENCY_TO_INDEX_BENCHMARK[currency.value]` keys -> raise `MqValueError`
3. Branch: else -> return `benchmark_type`

**Raises:** `MqValueError` when string is not a valid index type or index type is not supported for the currency.

### _get_inflation_swap_leg_defaults(currency: CurrencyEnum, benchmark_type: InflationIndexType = None) -> dict
Purpose: Build default parameters dict for inflation swap leg queries.

**Algorithm:**
1. Look up pricing location from `CURRENCY_TO_PRICING_LOCATION`, default to `PricingLocation.LDN`
2. Branch: if `benchmark_type` is None -> use first key from `CURRENCY_TO_INDEX_BENCHMARK[currency.value]`
3. Look up `benchmark_type_input` from `CURRENCY_TO_INDEX_BENCHMARK`; if not found, fall back to `"CPI-" + benchmark_type.value`
4. Return dict with `currency`, `index_type`, `pricing_location`

### _get_inflation_swap_csa_terms(curr: str, inflationindextype: str) -> dict
Purpose: Return CSA terms dict for an inflation swap.

**Algorithm:**
1. Return `dict(csaTerms=curr + '-1')`

### _get_inflation_swap_data(asset, swap_tenor, index_type, forward_tenor, clearing_house, source, real_time, query_type, location, allow_many, request_id) -> pd.DataFrame
Purpose: Core data retrieval function for inflation swap market data.

**Algorithm:**
1. Branch: if `real_time` is True -> raise `NotImplementedError`
2. Get currency from asset Bloomberg ID
3. Branch: if currency not in `CURRENCY_TO_INDEX_BENCHMARK` keys -> raise `NotImplementedError`
4. Validate index type via `_check_inflation_index_type`
5. Validate clearing house via `_check_clearing_house`
6. Get leg defaults via `_get_inflation_swap_leg_defaults`
7. Branch: if swap_tenor is not a valid relative date tenor -> raise `MqValueError`
8. Validate forward tenor via `_check_forward_tenor`
9. Build kwargs dict for asset query
10. Get rate asset IDs via `_get_tdapi_inflation_rates_assets`
11. Branch: if `location` is None -> use `_default_pricing_location(currency)`
12. Branch: else -> use `PricingLocation(location)`
13. Normalize pricing location
14. Build and execute market data query
15. Return DataFrame

**Raises:** `NotImplementedError` for real-time data or unsupported currencies. `MqValueError` for invalid swap tenor.

### inflation_swap_rate(asset, swap_tenor, index_type, forward_tenor, clearing_house, location, *, source, real_time) -> pd.Series
Purpose: GS end-of-day Zero Coupon Inflation Swap rate for a specific tenor.

Decorated with: `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...)])`

**Algorithm:**
1. Call `_get_inflation_swap_data(...)` with `QueryType.SWAP_RATE`
2. Branch: if df is empty -> create empty `ExtendedSeries`
3. Branch: else -> create `ExtendedSeries` from `df['swapRate']`
4. Attach `dataset_ids` from df
5. Return series

### inflation_swap_term(asset, index_type, forward_tenor, pricing_date, clearing_house, location, *, source, real_time, request_id) -> pd.Series
Purpose: Forward term structure of GS end-of-day inflation swaps.

Decorated with: `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(...)])`

**Algorithm:**
1. Get currency from asset Bloomberg ID
2. Branch: if currency not in `CURRENCY_TO_INDEX_BENCHMARK` -> raise `NotImplementedError`
3. Branch: if `location` is None -> use `_default_pricing_location(currency)`
4. Branch: else -> use `PricingLocation(location)`
5. Branch: if `pricing_date` is not None AND it is a holiday in the location's calendar -> raise `MqValueError`
6. Compute start/end range from pricing date
7. Within `DataContext(start, end)`, call `_get_inflation_swap_data(...)` with `allow_many=True` and `swap_tenor=None`
8. Branch: if df is empty -> create empty `ExtendedSeries`
9. Branch: else:
   - Select latest date from df index
   - Filter to that date
   - Apply `_get_term_struct_date` to compute expiration dates from `terminationTenor`
   - Set expiration date as index, sort, filter to DataContext range
   - Branch: if filtered df is empty -> empty series
   - Branch: else -> series from `df['swapRate']`
10. Branch: if series is empty -> call `check_forward_looking` to raise descriptive error
11. Return series

**Raises:** `NotImplementedError` for unsupported currencies. `MqValueError` for holiday pricing dates.

## State Mutation
- `inflationRates_defaults_provider`: Module-level instance created at import time. Its `defaults` dict is mutated in `__init__` to add `'MAPPING'` key.
- `_logger`: Module-level logger, no mutable state concerns.
- No thread safety concerns beyond standard GS session/API state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `_get_inflation_swap_data` | `real_time=True` |
| `NotImplementedError` | `_get_inflation_swap_data`, `inflation_swap_term` | Currency not in `CURRENCY_TO_INDEX_BENCHMARK` |
| `MqValueError` | `_get_tdapi_inflation_rates_assets` | Multiple assets match (when not allowed) or no assets match |
| `MqValueError` | `_check_inflation_index_type` | Invalid index type string or index type not supported for currency |
| `MqValueError` | `_get_inflation_swap_data` | Invalid swap tenor |
| `MqValueError` | `inflation_swap_term` | Pricing date is a holiday |

## Edge Cases
- `TdapiInflationRatesDefaultsProvider.__init__` overwrites each currency's mapping on each iteration of the inner loop, so only the LAST entry per currency survives in the mapping. For EUR this means only FRCPXTOB is retained (not CPXTEMU).
- `_get_tdapi_inflation_rates_assets` retries without clearing house only when the clearing house is `NONE`; other clearing house values that return 0 results will raise immediately.
- `_get_inflation_swap_leg_defaults` falls back to `"CPI-" + benchmark_type.value` if the benchmark type is not in the `CURRENCY_TO_INDEX_BENCHMARK` dict for that currency, which can produce invalid index strings.
- `inflation_swap_term` uses `df.loc[latest]` which can return a Series (single row) or DataFrame (multiple rows for same date), affecting downstream processing.
- The `check_forward_looking` call at the end of `inflation_swap_term` is only reached when the final series is empty, providing user feedback for stale data contexts.

## Bugs Found
- Line 89-90: In `TdapiInflationRatesDefaultsProvider.__init__`, the inner loop overwrites `benchmark_mappings[k]` on each iteration rather than merging, so for currencies with multiple entries (e.g., EUR with CPXTEMU and FRCPXTOB), only the last entry is retained. (OPEN)

## Coverage Notes
- Branch count: ~30
- Key branches: real_time checks, currency support checks, len(assets) three-way split, string vs enum benchmark_type, None vs provided optional params, empty vs non-empty DataFrames
- The `_get_inflation_swap_csa_terms` function appears unused in this module
