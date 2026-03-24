# measures_rates.py

## Summary
Interest rate market data retrieval module providing end-of-day and intraday swap rates, swaption volatilities, basis swap spreads, OIS cross-currency rates, forward rates, discount factors, and central bank policy rate expectations across 25+ global currencies. Acts as the primary rates data layer, translating high-level user queries into TDAPI asset lookups, GS Data API market data queries, and MXAPI curve/backtest computations. All public functions are decorated with `@plot_measure` for integration into the GS Quant plotting framework.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.data` (QueryType, GsDataApi), `gs_quant.common` (Currency/CurrencyEnum, AssetClass, AssetType, PricingLocation, SwapClearingHouse), `gs_quant.data` (DataContext, Dataset), `gs_quant.datetime.gscalendar` (GsCalendar), `gs_quant.errors` (MqValueError), `gs_quant.instrument` (IRSwap), `gs_quant.markets.securities` (AssetIdentifier, Asset), `gs_quant.timeseries` (currency_to_default_ois_asset, convert_asset_for_rates_data_set, RatesConversionType), `gs_quant.timeseries.helper` (_to_offset, check_forward_looking, plot_measure, Entitlement), `gs_quant.timeseries.measures` (_market_data_timed, _range_from_pricing_date, _get_custom_bd, ExtendedSeries, SwaptionTenorType, _extract_series_from_df, GENERIC_DATE, _asset_from_spec, ASSET_SPEC, MeasureDependency, _logger)
- External: `datetime` (dt), `logging`, `re`, `collections` (OrderedDict), `enum` (Enum), `typing` (Optional, Union, Dict, List), `pandas` (pd)

## Type Definitions

### TdapiRatesDefaultsProvider (class)
Inherits: object

Resolves default parameters for swaption and swap asset queries from a nested dictionary of per-currency configuration. Provides lookup methods for floating rate options, benchmark types, and other swaption fields.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| defaults | `dict` | *(required)* | Nested dictionary with keys "CURRENCIES", "COMMON", and auto-generated "MAPPING" |
| EMPTY_PROPERTY | `str` (class constant) | `"null"` | Sentinel indicating a property should be excluded from asset queries |

### ASSET_SPEC (imported type alias)
```
ASSET_SPEC = Union[Asset, str]
```

### GENERIC_DATE (imported type alias)
```
GENERIC_DATE = Union[dt.date, str]
```

## Enums and Constants

### _ClearingHouse(Enum)
Internal enum for clearing house selection in swap queries.

| Value | Raw | Description |
|-------|-----|-------------|
| LCH | `"LCH"` | London Clearing House (default) |
| EUREX | `"EUREX"` | Eurex clearing |
| JSCC | `"JSCC"` | Japan Securities Clearing Corporation |
| CME | `"CME"` | Chicago Mercantile Exchange |
| NONE | `"NONE"` | No clearing house |

### _SwapTenorType(Enum)
Selects which tenor dimension to fix in term structure queries.

| Value | Raw | Description |
|-------|-----|-------------|
| FORWARD_TENOR | `"forward_tenor"` | Fix forward start date, vary swap maturity |
| SWAP_TENOR | `"swap_tenor"` | Fix swap maturity, vary forward start date |

### EventType(Enum)
Central bank watch event types.

| Value | Raw | Description |
|-------|-----|-------------|
| MEETING | `"Meeting Forward"` | Forward expectations for future CB meetings |
| EOY | `"EOY Forward"` | Forward expectations at end-of-year dates |
| SPOT | `"Spot"` | Current effective OIS/policy rate |

### RateType(Enum)
Central bank watch rate representation types.

| Value | Raw | Description |
|-------|-----|-------------|
| ABSOLUTE | `"absolute"` | Absolute rate level |
| RELATIVE | `"relative"` | Forward minus spot rate (hikes/cuts priced in) |

### BenchmarkType(Enum)
All supported floating rate benchmark types across global currencies.

| Value | Raw | Description |
|-------|-----|-------------|
| LIBOR | `"LIBOR"` | London Interbank Offered Rate |
| EURIBOR | `"EURIBOR"` | Euro Interbank Offered Rate |
| EUROSTR | `"EUROSTR"` | Euro Short-Term Rate |
| STIBOR | `"STIBOR"` | Stockholm Interbank Offered Rate |
| OIS | `"OIS"` | Overnight Index Swap |
| CDKSDA | `"CDKSDA"` | Certificate of Deposit KSDA |
| SOFR | `"SOFR"` | Secured Overnight Financing Rate |
| SARON | `"SARON"` | Swiss Average Rate Overnight |
| EONIA | `"EONIA"` | Euro Overnight Index Average |
| SONIA | `"SONIA"` | Sterling Overnight Index Average |
| TONA | `"TONA"` | Tokyo Overnight Average Rate |
| Fed_Funds | `"Fed_Funds"` | Federal Funds Rate |
| NIBOR | `"NIBOR"` | Norwegian Interbank Offered Rate |
| CIBOR | `"CIBOR"` | Copenhagen Interbank Offered Rate |
| BBR | `"BBR"` | Bank Bill Rate (AUD/NZD) |
| BA | `"BA"` | Bankers' Acceptance |
| KSDA | `"KSDA"` | Korean Securities Dealers Association |
| REPO | `"REPO"` | Repurchase Agreement Rate |
| SOR | `"SOR"` | Swap Offer Rate (SGD) |
| HIBOR | `"HIBOR"` | Hong Kong Interbank Offered Rate |
| MIBOR | `"MIBOR"` | Mumbai Interbank Offered Rate |
| CDOR | `"CDOR"` | Canadian Dollar Offered Rate |
| CDI | `"CDI"` | Certificado de Deposito Interbancario |
| TNA | `"TNA"` | Tasa Nominal Anual (Chile) |
| IBR | `"IBR"` | Indicador Bancario de Referencia (Colombia) |
| TIIE | `"TIIE"` | Tasa de Interes Interbancaria de Equilibrio (Mexico) |
| AONIA | `"AONIA"` | AUD Overnight Index Average |
| NZIONA | `"NZIONA"` | NZD Overnight Index Average |
| NOWA | `"NOWA"` | Norwegian Overnight Weighted Average |
| CORRA | `"CORRA"` | Canadian Overnight Repo Rate Average |
| SIOR | `"SIOR"` | Swedish Overnight Rate |
| SORA | `"SORA"` | Singapore Overnight Rate Average |

### BenchmarkTypeCB(Enum)
Subset of benchmark types supported for central bank watch real-time data.

| Value | Raw | Description |
|-------|-----|-------------|
| EUROSTR | `"EUROSTR"` | For EUR central bank watch |
| SOFR | `"SOFR"` | For USD SOFR-based CB watch |
| EONIA | `"EONIA"` | For EUR legacy CB watch |
| SONIA | `"SONIA"` | For GBP central bank watch |
| Fed_Funds | `"Fed_Funds"` | For USD Fed Funds CB watch |

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| CCY_TO_CB | `Dict[str, str]` | `{'EUR': 'ecb', 'USD': 'frb', 'GBP': 'mpc'}` | Maps currency to central bank prefix for meeting tenor strings |
| CENTRAL_BANK_WATCH_START_DATE | `dt.date` | `dt.date(2016, 1, 1)` | Earliest date for central bank watch data queries |
| CURRENCY_TO_SWAP_RATE_BENCHMARK | `Dict[str, OrderedDict/dict]` | *(25 currencies)* | Maps ISO currency code to ordered dict of benchmark_name -> floating_rate_option string. Covers AUD, BRL, CAD, CHF, CLP, CNY, COP, CZK, DKK, EUR, GBP, HKD, HUF, ILS, INR, JPY, KRW, MXN, NOK, NZD, PLN, RUB, SEK, SGD, THB, USD, ZAR |
| BENCHMARK_TO_DEFAULT_FLOATING_RATE_TENORS | `Dict[str, str]` | *(37 entries)* | Maps floating_rate_option string to default fixing tenor (e.g. `'USD-LIBOR-BBA'` -> `'3m'`, `'EUR-EURIBOR-TELERATE'` -> `'6m'`, `'USD-SOFR-COMPOUND'` -> `'1y'`) |
| CURRENCY_TO_PRICING_LOCATION | `Dict[CurrencyEnum, PricingLocation]` | *(20 entries)* | Default pricing location per currency. JPY/AUD/NZD -> TKO, USD/CAD/BRL/COP/CLP/MXN -> NYC, EUR/GBP/CHF/DKK/NOK/SEK -> LDN, CNY/HKD/INR/KRW/SGD -> HKG |
| CURRENCY_TO_DUMMY_SWAP_BBID | `Dict[str, str]` | *(20 entries)* | Maps currency code to Marquee asset ID used for swap rate availability checks |
| SUPPORTED_INTRADAY_CURRENCY_TO_DUMMY_SWAP_BBID | `Dict[str, str]` | *(11 entries)* | All currencies map to single asset `'MACF6R4J5FY4KGBZ'` for intraday |
| CROSS_BBID_TO_DUMMY_OISXCCY_ASSET | `Dict[str, str]` | *(10 entries)* | Maps FX cross pair (e.g. `'EURUSD'`) to OIS cross-currency swap dummy asset ID |
| CURRENCY_TO_CSA_DEFAULT_MAP | `Dict[str, str]` | `{'USD': 'USD-SOFR', 'EUR': 'EUR-EUROSTR'}` | Default CSA terms by currency |
| SWAPTION_DEFAULTS | `dict` | *(nested)* | Per-currency swaption configuration with benchmarkType, floatingRateOption, floatingRateTenor, assetIdForAvailabilityCheck, pricingLocation, strikeReference. Currencies: AUD, EUR, GBP, JPY, KRW, NZD, USD. COMMON section: strikeReference=ATM, clearingHouse=LCH, terminationTenor=5y, expirationTenor=1y, effectiveDate=0b |
| swaptions_defaults_provider | `TdapiRatesDefaultsProvider` | `TdapiRatesDefaultsProvider(SWAPTION_DEFAULTS)` | Module-level singleton instance initialized at import time |

## Functions/Methods

### TdapiRatesDefaultsProvider.__init__(self, defaults: dict) -> None
Purpose: Initialize the defaults provider, building a benchmark-to-floating-rate-option mapping from the CURRENCIES sub-dict.

**Algorithm:**
1. Store `defaults` dict on `self.defaults`
2. Iterate over each currency key in `defaults["CURRENCIES"]`
3. For each currency, build a dict mapping `benchmarkType` -> `floatingRateOption` from the list of config entries
4. Store the result in `self.defaults['MAPPING']`

### TdapiRatesDefaultsProvider.is_supported(self, currency: CurrencyEnum) -> bool
Purpose: Check if a currency is in the configured CURRENCIES dict.

**Algorithm:**
1. Return whether `currency.value` is a key in `self.defaults["CURRENCIES"]`

### TdapiRatesDefaultsProvider.get_floating_rate_option_for_benchmark(self, currency: CurrencyEnum, benchmark: str) -> Optional[str]
Purpose: Look up the floating rate option string for a given currency and benchmark type.

**Algorithm:**
1. Return `self.defaults["MAPPING"][currency.value][benchmark]`
2. Returns `None` if benchmark not found (via dict `.get()` chain)

### TdapiRatesDefaultsProvider.get_swaption_parameter(self, currency: Union[CurrencyEnum, str], field: str, value: Optional = None) -> Optional
Purpose: Resolve a swaption parameter with fallback to currency-specific defaults then common defaults.

**Algorithm:**
1. If `value == EMPTY_PROPERTY` ("null") -> return `None` (exclude from query)
2. If `value is not None` -> return `value` as-is (user override)
3. Extract `currency_name` string from CurrencyEnum or str
4. Search through two entries: first element of currency-specific config, then COMMON config
5. For each entry containing `field`, extract value; if value is a list, take the first element
6. Return the resolved value (may be `None` if not found in either)

### _pricing_location_normalized(location: PricingLocation, ccy: CurrencyEnum) -> PricingLocation
Purpose: Normalize HKG/TKO pricing locations based on currency-specific defaults.

**Algorithm:**
1. If `location` is HKG or TKO:
   - If currency is in CURRENCY_TO_PRICING_LOCATION and its default is HKG -> return HKG
   - Otherwise -> return TKO
2. Otherwise -> return `location` unchanged

### _default_pricing_location(ccy: CurrencyEnum) -> PricingLocation
Purpose: Return the default pricing location for a currency.

**Algorithm:**
1. If `ccy` is in `CURRENCY_TO_PRICING_LOCATION` -> return the mapped PricingLocation
2. Otherwise -> raise `MqValueError`

**Raises:** `MqValueError` when currency has no default location configured

### _cross_to_fxfwd_xcswp_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Map an FX cross asset to its OIS cross-currency swap dummy asset ID.

**Algorithm:**
1. Resolve asset from spec via `_asset_from_spec`
2. Get Bloomberg ID
3. Look up in `CROSS_BBID_TO_DUMMY_OISXCCY_ASSET`; fall back to Marquee ID

### _currency_to_tdapi_swap_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Map a currency asset to the dummy swap rate asset ID used for TDAPI availability checks.

**Algorithm:**
1. Resolve asset, get Bloomberg ID
2. Look up in `CURRENCY_TO_DUMMY_SWAP_BBID`; fall back to Marquee ID

### _currency_to_tdapi_swap_rate_asset_for_intraday(asset_spec: ASSET_SPEC) -> str
Purpose: Return the single hardcoded intraday swap asset ID.

**Algorithm:**
1. Return `'MACF6R4J5FY4KGBZ'` unconditionally (ignores `asset_spec`)

### _currency_to_tdapi_asset_base(asset_spec: ASSET_SPEC, allowed_bbids: Optional[list] = None) -> str
Purpose: General-purpose swaption asset ID resolver with optional Bloomberg ID whitelist.

**Algorithm:**
1. Resolve asset, get Bloomberg ID
2. If bbid is None, or `allowed_bbids` is set and bbid not in it -> return Marquee ID
3. Try `swaptions_defaults_provider.get_swaption_parameter(bbid, "assetIdForAvailabilityCheck")`
4. On `TypeError` -> log and return Marquee ID
5. Return the resolved asset ID

### _currency_to_tdapi_midcurve_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Resolve swaption asset ID restricted to midcurve-supported currencies (GBP, EUR, USD).

**Algorithm:**
1. Delegate to `_currency_to_tdapi_asset_base(asset_spec, ['GBP', 'EUR', 'USD'])`

### _currency_to_tdapi_swaption_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Resolve swaption asset ID for any supported currency.

**Algorithm:**
1. Delegate to `_currency_to_tdapi_asset_base(asset_spec)` (no bbid restriction)

### _currency_to_tdapi_basis_swap_rate_asset(asset_spec: ASSET_SPEC) -> str
Purpose: Map currency to basis swap dummy asset ID via hardcoded lookup.

**Algorithm:**
1. Resolve asset, get Bloomberg ID
2. Check bbid against 11 hardcoded currencies (EUR, GBP, JPY, USD, CAD, AUD, NZD, SEK, NOK, DKK, CHF) returning corresponding Marquee IDs
3. If no match -> return Marquee ID

### _match_floating_tenors(swap_args: dict) -> dict
Purpose: Align payer and receiver floating rate tenors for basis swap queries where one leg is SOFR or LIBOR/EURIBOR/STIBOR.

**Algorithm:**
1. Extract payer and receiver rate options
2. If they differ:
   - If payer contains `'SOFR'` -> set payer tenor = receiver tenor; if `"12m"` convert to `"1y"`
   - Elif receiver contains `'SOFR'` -> set receiver tenor = payer tenor; if `"12m"` convert to `"1y"`
   - Elif payer contains `'LIBOR'`, `'EURIBOR'`, or `'STIBOR'` -> set receiver tenor = payer tenor
   - Elif receiver contains `'LIBOR'`, `'EURIBOR'`, or `'STIBOR'` -> set payer tenor = receiver tenor
3. Return modified `swap_args`

### _get_tdapi_rates_assets(allow_many: bool = False, **kwargs) -> Union[str, list]
Purpose: Query GsAssetApi for rate assets matching given parameters; flip basis swap legs if first query returns empty.

**Algorithm:**
1. Remove `pricing_location` from kwargs (not a valid asset query param)
2. Call `GsAssetApi.get_many_assets(**kwargs)`
3. If 0 results AND kwargs has `asset_parameters_payer_rate_option` -> swap payer/receiver rate options and designated maturities, retry query
4. If >1 assets:
   - If `termination_date` or `effective_date` missing from kwargs, or `allow_many=True` -> return list of IDs
   - Otherwise raise `MqValueError('Specified arguments match multiple assets')`
5. If 0 assets -> raise `MqValueError`
6. If exactly 1 -> return its `.id`

**Raises:** `MqValueError` when no assets match or multiple assets match unexpectedly

### _check_forward_tenor(forward_tenor: Union[str, dt.date, None]) -> GENERIC_DATE
Purpose: Validate and normalize a forward tenor string.

**Algorithm:**
1. If `forward_tenor` is a `dt.date` -> return as-is
2. If in `['Spot', 'spot', 'SPOT']` -> return `'0b'`
3. If not a valid relative date tenor AND not matching `imm[1-4]|frb[1-9]|ecb[1-9]` -> raise `MqValueError`
4. Otherwise return `forward_tenor` unchanged

**Raises:** `MqValueError` when tenor format is invalid

### _check_benchmark_type(currency: CurrencyEnum, benchmark_type: Union[BenchmarkType, str], nothrow: bool = False) -> Union[BenchmarkType, str]
Purpose: Validate and convert benchmark type string to BenchmarkType enum, checking currency support.

**Algorithm:**
1. If `benchmark_type` is a string:
   - If uppercase form is in `BenchmarkType.__members__` -> convert to enum
   - Elif matches fed_funds variants -> `BenchmarkType.Fed_Funds`
   - Elif matches eurostr variants (`'estr'`, `'ESTR'`, `'eurostr'`, `'EuroStr'`) -> `BenchmarkType.EUROSTR`
   - Elif not `nothrow` -> raise `MqValueError` with valid options
   - Else return the string as-is (pass-through for custom benchmarks)
2. If result is a `BenchmarkType` and its value is not in `CURRENCY_TO_SWAP_RATE_BENCHMARK[currency.value]` -> raise `MqValueError`
3. Return the validated benchmark_type

**Raises:** `MqValueError` when benchmark string is invalid (unless nothrow) or unsupported for currency

### _check_clearing_house(clearing_house: Union[_ClearingHouse, str, None]) -> _ClearingHouse
Purpose: Validate and convert clearing house input, defaulting to LCH.

**Algorithm:**
1. If string and uppercase is in `_ClearingHouse.__members__` -> convert to enum
2. If `None` -> return `_ClearingHouse.LCH`
3. If `_ClearingHouse` instance -> return as-is
4. Otherwise -> raise `MqValueError`

**Raises:** `MqValueError` when clearing house string is invalid

### _check_tenor_type(tenor_type: Union[_SwapTenorType, str, None]) -> _SwapTenorType
Purpose: Validate and convert tenor type input, defaulting to FORWARD_TENOR.

**Algorithm:**
1. If string and uppercase is in `_SwapTenorType.__members__` -> convert to enum
2. If `None` -> return `_SwapTenorType.FORWARD_TENOR`
3. If `_SwapTenorType` instance -> return as-is
4. Otherwise -> raise `MqValueError`

**Raises:** `MqValueError` when tenor_type string is invalid

### _check_term_structure_tenor(tenor_type: _SwapTenorType, tenor: str) -> Dict
Purpose: Determine which dataset field to query and which column to plot based on tenor type.

**Algorithm:**
1. If `tenor_type == FORWARD_TENOR`:
   - Validate `tenor` via `_check_forward_tenor`
   - `tenor_to_plot = 'terminationTenor'`, `tenor_dataset_field = 'asset_parameters_effective_date'`
2. Elif `tenor` does not match `(\d+)([bdwmy])` or matches `frb[1-9]`:
   - Raise `MqValueError('invalid swap tenor')`
3. Else (SWAP_TENOR):
   - `tenor_to_plot = 'effectiveTenor'`, `tenor_dataset_field = 'asset_parameters_termination_date'`
4. Return dict with keys `tenor`, `tenor_to_plot`, `tenor_dataset_field`

**Raises:** `MqValueError` when swap tenor format is invalid

### _get_benchmark_type(currency: CurrencyEnum, benchmark_type: Optional[BenchmarkType] = None) -> str
Purpose: Resolve benchmark type to its floating rate option string, with per-currency defaults.

**Algorithm:**
1. If `benchmark_type is None`:
   - EUR -> `BenchmarkType.EURIBOR`
   - SEK -> `BenchmarkType.STIBOR`
   - Others -> first key in `CURRENCY_TO_SWAP_RATE_BENCHMARK[currency.value]`
2. Look up `CURRENCY_TO_SWAP_RATE_BENCHMARK[currency.value][benchmark_type.value]`
3. Return the floating rate option string

### _get_swap_leg_defaults(currency: CurrencyEnum, benchmark_type: Union[BenchmarkType, str] = None, floating_rate_tenor: str = None) -> dict
Purpose: Assemble default swap leg parameters (pricing location, benchmark type string, floating rate tenor).

**Algorithm:**
1. Get `pricing_location` from `CURRENCY_TO_PRICING_LOCATION` (default LDN)
2. If `benchmark_type` is not a raw string -> resolve via `_get_benchmark_type`; else use as-is
3. If `floating_rate_tenor is None`:
   - Look up in `BENCHMARK_TO_DEFAULT_FLOATING_RATE_TENORS`
   - If not found -> raise `MqValueError`
4. Return dict with `currency`, `benchmark_type` (string), `floating_rate_tenor`, `pricing_location`

**Raises:** `MqValueError` when no default fixing tenor exists for the benchmark

### _get_swap_csa_terms(curr: str, benchmark_type: str) -> dict
Purpose: Determine CSA terms for a swap based on currency and benchmark.

**Algorithm:**
1. If benchmark_type is EUR-EURIBOR or USD-LIBOR -> return empty dict (no explicit CSA)
2. If benchmark_type is EUR-EUROSTR -> return `{'csaTerms': curr + '-EuroSTR'}`
3. Otherwise -> return `{'csaTerms': curr + '-1'}`

### _get_basis_swap_csa_terms(curr: str, payer_benchmark: str, receiver_benchmark: str) -> dict
Purpose: Determine CSA terms for basis swaps based on both leg benchmarks.

**Algorithm:**
1. If either leg is EUR-EURIBOR or USD-LIBOR -> return empty dict
2. If either leg is EUR-EUROSTR -> return `{'csaTerms': curr + '-EuroSTR'}`
3. Otherwise -> return `{'csaTerms': curr + '-1'}`

### _get_swap_data(asset: Asset, swap_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, source: str = None, real_time: bool = False, location: PricingLocation = None, query_type: QueryType = QueryType.SWAP_RATE) -> pd.DataFrame
Purpose: Core data retrieval for fixed-floating swap measures (rate, annuity). Builds asset query, resolves TDAPI asset, queries market data.

**Algorithm:**
1. If `real_time` and query_type is not SWAP_RATE -> raise `NotImplementedError`
2. Extract currency from asset Bloomberg ID
3. Validate currency is in `CURRENCY_TO_SWAP_RATE_BENCHMARK`
4. Validate and resolve `benchmark_type` via `_check_benchmark_type`
5. Validate and resolve `clearing_house` via `_check_clearing_house`
6. Get swap leg defaults via `_get_swap_leg_defaults`
7. Validate `swap_tenor` matches `(\d+)([bdwmy])` or `forward_tenor` matches `(frb[1-9]|ecb[1-6])`
8. Validate floating rate tenor format
9. Validate forward_tenor via `_check_forward_tenor`
10. Resolve pricing_location (user-provided or default)
11. Build kwargs dict for TDAPI asset query
12. Resolve `rate_mqid` via `_get_tdapi_rates_assets`
13. Normalize pricing location
14. Build and execute market data query via `GsDataApi.build_market_data_query` + `_market_data_timed`
15. Return DataFrame

**Raises:** `NotImplementedError` when real_time requested for non-SWAP_RATE queries, or currency not supported. `MqValueError` for invalid tenors.

### _get_swap_data_calc(asset: Asset, swap_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, csa: str = None, real_time: bool = False, location: PricingLocation = None) -> pd.DataFrame
Purpose: Compute swap rate via MXAPI backtest (on-the-fly calculation) for intraday data.

**Algorithm:**
1. Extract currency from asset Bloomberg ID
2. Validate benchmark_type with `nothrow=True`
3. Set clearing_house = LCH; if `csa` is EUREX/JSCC/CME override to that
4. Get swap leg defaults
5. Validate swap_tenor format
6. Validate forward_tenor
7. Build `IRSwap` instrument with resolved params
8. If `forward_tenor` is truthy, set `builder.effective_date`
9. Call `GsDataApi.get_mxapi_backtest_data` and return result

**Raises:** `MqValueError` for invalid swap tenor

### _get_term_struct_date(tenor: Union[str, dt.datetime], index: dt.datetime, business_day) -> dt.datetime
Purpose: Convert a tenor string or date to an actual datetime, adjusting by business day offset.

**Algorithm:**
1. If `tenor` is a `dt.datetime` or `dt.date` -> return as-is
2. Try to parse as `YYYY-MM-DD` string -> return `dt.datetime(year, month, day)`
3. On `ValueError`:
   - If `tenor == '0b'` -> return `index + business_day - business_day` (adjust to nearest biz day)
   - Else -> return `index + _to_offset(tenor) + business_day - business_day`

### _get_swaption_measure(asset: Asset, benchmark_type: str = None, floating_rate_tenor: str = None, effective_date: str = None, expiration_tenor: str = None, termination_tenor: str = None, strike_reference: Union[str, int] = None, clearing_house: str = None, start: str = ..., end: str = ..., source: str = None, real_time: bool = False, allow_many: bool = False, query_type: QueryType = QueryType.SWAPTION_PREMIUM, location: PricingLocation = None) -> pd.DataFrame
Purpose: Central dispatcher for all swaption-related data retrieval (premium, annuity, vol, ATM forward rate).

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Extract currency; validate via `swaptions_defaults_provider.is_supported`
3. Build asset query via `_swaption_build_asset_query`
4. Resolve asset IDs via `_get_tdapi_rates_assets`; ensure result is a list
5. Resolve and normalize pricing_location
6. Build market data query within `DataContext(start, end)` scope
7. Execute via `_market_data_timed`
8. Return DataFrame

**Raises:** `NotImplementedError` for real-time or unsupported currency

### _swaption_build_asset_query(currency, benchmark_type=None, effective_date=None, expiration_tenor=None, floating_rate_tenor=None, strike_reference=None, termination_tenor=None, clearinghouse=None) -> dict
Purpose: Build the TDAPI asset query dict for swaptions, resolving all defaults from `swaptions_defaults_provider`.

**Algorithm:**
1. Resolve `benchmark_type` via provider
2. Look up `floating_rate_option` from provider mapping; raise `MqValueError` if None
3. Resolve `floating_rate_tenor`, `strike_reference`, `termination_tenor`, `effective_date`, `expiration_tenor`, `clearinghouse` from provider
4. Validate termination and expiration tenors via `_is_valid_relative_date_tenor`
5. Validate forward_tenor via `_check_forward_tenor`
6. Validate strike_reference via `_check_strike_reference`
7. Build query dict with `asset_class='Rates'`, `type='Swaption'`, conditionally adding each resolved param if not None
8. Return query dict

**Raises:** `MqValueError` for invalid benchmark, tenor, or strike reference

### _check_strike_reference(strike_reference: Union[str, float, int, list, None]) -> Union[str, list, None]
Purpose: Normalize and validate swaption strike reference (absolute offset from ATM).

**Algorithm:**
1. If `None` -> return `None`
2. If float or int:
   - If 0 -> `"ATM"`
   - Else -> format as `"ATM%+f"` stripped of trailing zeros/dots (e.g. `"ATM+50"`)
3. If string and uppercase is `"SPOT"` -> `"ATM"`
4. Wrap in list if not already for validation
5. For each element, validate against regex `ATM|ATM[-+]?([0-9]*\.[0-9]+|[0-9]+)`
6. Return validated `strike_reference`

**Raises:** `MqValueError` for invalid strike reference format

### _is_valid_relative_date_tenor(tenor: Optional[str]) -> bool
Purpose: Check if a tenor string matches the `(\d+)([bdwmy])` pattern.

**Algorithm:**
1. If `None` -> return `True`
2. If matches regex `(\d+)([bdwmy])` -> return `True`
3. Otherwise -> return `False`

### swap_annuity(asset: Asset, swap_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day IRS annuity values (in years) for the paying leg.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset, query_type=QueryType.SWAP_ANNUITY)])`

**Algorithm:**
1. Delegate to `_get_swap_data` with `query_type=QueryType.SWAP_ANNUITY`
2. If DataFrame empty -> return empty `ExtendedSeries`
3. Otherwise compute `abs(df['swapAnnuity'] * 1e4 / 1e8)` (convert to years)
4. Attach `dataset_ids` and return

### swaption_premium(asset: Asset, expiration_tenor: str = None, termination_tenor: str = None, relative_strike: str = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day swaption premium.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.SWAPTION_PREMIUM)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with effective_date=`"0b"`, `query_type=QueryType.SWAPTION_PREMIUM`
2. Extract series via `_extract_series_from_df`
3. Return series

### swaption_annuity(asset: Asset, expiration_tenor: str = None, termination_tenor: str = None, relative_strike: float = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day swaption annuity.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.SWAPTION_ANNUITY)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with effective_date=`"0b"`, `query_type=QueryType.SWAPTION_ANNUITY`
2. Extract series via `_extract_series_from_df`
3. Return series

### midcurve_premium(asset: Asset, expiration_tenor: str, forward_tenor: str, termination_tenor: str, relative_strike: float = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day midcurve swaption premium.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_midcurve_asset, query_type=QueryType.MIDCURVE_PREMIUM)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with user-provided `forward_tenor` as effective_date, `query_type=QueryType.MIDCURVE_PREMIUM`
2. Extract series via `_extract_series_from_df`
3. Return series

### midcurve_annuity(asset: Asset, expiration_tenor: str, forward_tenor: str, termination_tenor: str, relative_strike: float = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day midcurve swaption annuity.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_midcurve_asset, query_type=QueryType.MIDCURVE_ANNUITY)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with user-provided `forward_tenor` as effective_date, `query_type=QueryType.MIDCURVE_ANNUITY`
2. Extract series via `_extract_series_from_df`
3. Return series

### swaption_atm_fwd_rate(asset: Asset, expiration_tenor: str = None, termination_tenor: str = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day ATM forward rate for swaption vol matrices.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.ATM_FWD_RATE)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with effective_date=`"0b"`, no strike_reference, `query_type=QueryType.ATM_FWD_RATE`
2. Extract series via `_extract_series_from_df`
3. Return series

### swaption_vol(asset: Asset, expiration_tenor: str = None, termination_tenor: str = None, relative_strike: float = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day implied normal volatility for swaption vol matrices.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.SWAPTION_VOL)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with effective_date=`"0b"`, `query_type=QueryType.SWAPTION_VOL`
2. Extract series via `_extract_series_from_df`
3. Return series

### midcurve_vol(asset: Asset, expiration_tenor: str, forward_tenor: str, termination_tenor: str, relative_strike: float = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day implied normal volatility for midcurve swaption vol matrices.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_midcurve_asset, query_type=QueryType.MIDCURVE_VOL)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with user-provided `forward_tenor` as effective_date, `query_type=QueryType.MIDCURVE_VOL`
2. Extract series via `_extract_series_from_df`
3. Return series

### midcurve_atm_fwd_rate(asset: Asset, expiration_tenor: str, forward_tenor: str, termination_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day ATM forward rate for midcurve swaption vol matrices.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_midcurve_asset, query_type=QueryType.MIDCURVE_ATM_FWD_RATE)])`

**Algorithm:**
1. Delegate to `_get_swaption_measure` with user-provided `forward_tenor` as effective_date, no strike_reference, `query_type=QueryType.MIDCURVE_ATM_FWD_RATE`
2. Extract series via `_extract_series_from_df`
3. Return series

### swaption_vol_smile(asset: Asset, expiration_tenor: str, termination_tenor: str, pricing_date: Optional[GENERIC_DATE] = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day swaption vol smile (vol vs. relative strike) for a single expiration/termination pair on a given pricing date.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.SWAPTION_VOL)])`

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Extract currency; resolve default pricing location from swaptions provider if not specified
3. Compute `start, end` from `_range_from_pricing_date`
4. Query `_get_swaption_measure` with `strike_reference=EMPTY_PROPERTY` (null), `allow_many=True` to get all strikes
5. If DataFrame empty -> return empty series
6. Parse `strikeRelative` column: convert `"ATM+20"` -> `20.0`, `"ATM"` -> `0.0`
7. Select latest date, set `strikeRelative` as index
8. Sort by index, build `ExtendedSeries` of `swaptionVol` values
9. Attach dataset_ids and return

**Raises:** `NotImplementedError` for real-time

### swaption_vol_term(asset: Asset, tenor_type: SwaptionTenorType, tenor: str, relative_strike: float, pricing_date: Optional[GENERIC_DATE] = None, benchmark_type: str = None, floating_rate_tenor: str = None, clearing_house: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Term structure of GS end-of-day swaption implied normal volatility, fixing either option expiry or swap maturity.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swaption_rate_asset, query_type=QueryType.SWAPTION_VOL)])`

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Extract currency; resolve default pricing location
3. Compute `start, end` from `_range_from_pricing_date`
4. Branch on `tenor_type`:
   - `OPTION_EXPIRY` -> fix expiration_tenor=`tenor`, set termination_tenor=EMPTY_PROPERTY, plot `terminationTenor`
   - else (SWAP_MATURITY) -> fix termination_tenor=`tenor`, set expiration_tenor=EMPTY_PROPERTY, plot `expirationTenor`
5. Both branches call `_get_swaption_measure` with `allow_many=True`
6. If DataFrame empty -> return empty series
7. Select latest pricing date, compute expiration dates from tenor offsets + business day adjustment
8. Set `expirationDate` as index, sort, filter to DataContext date range
9. Build `ExtendedSeries` of `swaptionVol`
10. If empty -> call `check_forward_looking` for descriptive error
11. Return series

**Raises:** `NotImplementedError` for real-time

### swap_rate(asset: Asset, swap_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day fixed-floating IRS swap rate curves across major currencies.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset, query_type=QueryType.SWAP_RATE)])`

**Algorithm:**
1. Delegate to `_get_swap_data` with `query_type=QueryType.SWAP_RATE`
2. If DataFrame empty -> empty ExtendedSeries
3. Otherwise -> `ExtendedSeries(df['swapRate'])`
4. Attach dataset_ids and return

### swap_rate_calc(asset: Asset, swap_tenor: str, benchmark_type: str = None, floating_rate_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, csa: str = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS intraday IRS swap rate via on-the-fly MXAPI backtest calculation.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset_for_intraday, query_type=QueryType.SPOT)])`

**Algorithm:**
1. Delegate to `_get_swap_data_calc`
2. If DataFrame empty -> empty ExtendedSeries
3. Otherwise -> `ExtendedSeries(df['ATMRate'])`
4. Set `dataset_ids = ()` (no dataset for calc) and return

### forward_rate(asset: Asset, forward_start_tenor: str = None, forward_term: str = None, csa: str = None, close_location: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS forward rate from stored discount curves.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset_for_intraday, query_type=QueryType.SPOT)])`

**Algorithm:**
1. Extract currency; default `csa='Default'`, `close_location='NYC'`
2. Validate `forward_term` is specified -> raise `MqValueError` if not
3. Default `forward_start_tenor` to `'0d'` if not specified
4. Build measure string `f'FR:{forward_start_tenor}:{forward_term}'`
5. Call `GsDataApi.get_mxapi_curve_measure('DISCOUNT CURVE', ...)` with curve type, currency, CSA, measure
6. Return ExtendedSeries from result or empty series

**Raises:** `MqValueError` when `forward_term` not specified

### discount_factor(asset: Asset, tenor: str = None, csa: str = None, close_location: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS discount factor from stored discount curves. Internal entitlement only.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset_for_intraday, query_type=QueryType.SPOT)], entitlements=[Entitlement.INTERNAL])`

**Algorithm:**
1. Extract currency; default `close_location='NYC'`
2. Validate `tenor` is specified -> raise `MqValueError` if not
3. Default `csa='Default'`
4. Build measure string `f'DF:{tenor}'`
5. Call `GsDataApi.get_mxapi_curve_measure('DISCOUNT CURVE', ...)`
6. Return ExtendedSeries from result or empty series

**Raises:** `MqValueError` when `tenor` not specified

### instantaneous_forward_rate(asset: Asset, tenor: str = None, csa: str = None, close_location: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS annualised instantaneous forward rate from stored discount curves.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset_for_intraday, query_type=QueryType.SPOT)])`

**Algorithm:**
1. Extract currency; default `close_location='NYC'`
2. Validate `tenor` is specified -> raise `MqValueError` if not
3. Default `csa='Default'`
4. Build measure string `f'IFR:{tenor}'`
5. Call `GsDataApi.get_mxapi_curve_measure('DISCOUNT CURVE', ...)`
6. Return ExtendedSeries from result or empty series

**Raises:** `MqValueError` when `tenor` not specified

### index_forward_rate(asset: Asset, forward_start_tenor: str = None, benchmark_type: str = None, fixing_tenor: str = None, close_location: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS annualised forward rate from stored index forward curves for a given floating rate benchmark.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset_for_intraday, query_type=QueryType.SPOT)])`

**Algorithm:**
1. Extract currency; default `close_location='NYC'`
2. Validate `forward_start_tenor` -> raise `MqValueError` if not specified
3. Validate `benchmark_type` with `nothrow=True`; resolve to string via `_get_benchmark_type` if enum
4. If `fixing_tenor is None`:
   - Look up in `BENCHMARK_TO_DEFAULT_FLOATING_RATE_TENORS`; raise `MqValueError` if not found
5. Build measure string `f'FR:{forward_start_tenor}:{fixing_tenor}'`
6. Call `GsDataApi.get_mxapi_curve_measure('INDEX CURVE', benchmark_type_input, [fixing_tenor], [f'{currency.value}-1'], ...)`
7. Return ExtendedSeries from result or empty series

**Raises:** `MqValueError` when `forward_start_tenor` not specified, or default fixing tenor not found

### _get_basis_swap_kwargs(asset: Asset, spread_benchmark_type: str = None, spread_tenor: str = None, reference_benchmark_type: str = None, reference_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None) -> dict
Purpose: Build kwargs dict for basis swap asset queries with validation.

**Algorithm:**
1. Extract currency; validate in supported list (JPY, EUR, USD, GBP, CHF, DKK, NOK, SEK, CAD, AUD, NZD)
2. Validate clearing_house, spread and reference benchmark types
3. Get swap leg defaults for both legs
4. Validate floating rate tenors for both legs
5. Validate forward_tenor
6. Resolve pricing_location (user or default), normalize
7. Build kwargs dict with payer/receiver rate options, designated maturities, clearing house, forward tenor, currency
8. Call `_match_floating_tenors` to align tenors
9. Return kwargs

**Raises:** `NotImplementedError` for unsupported currencies. `MqValueError` for invalid tenors.

### basis_swap_spread(asset: Asset, swap_tenor: str = '1y', spread_benchmark_type: str = None, spread_tenor: str = None, reference_benchmark_type: str = None, reference_tenor: str = None, forward_tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day floating-floating IRS basis swap spread curves.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_basis_swap_rate_asset, query_type=QueryType.BASIS_SWAP_RATE)])`

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Validate swap_tenor matches `(\d+)([bdwmy])` or forward_tenor matches `(frb[1-9])`
3. Build kwargs via `_get_basis_swap_kwargs`
4. Add `asset_parameters_termination_date = swap_tenor`
5. Resolve `rate_mqid` via `_get_tdapi_rates_assets`
6. Build and execute market data query for `QueryType.BASIS_SWAP_RATE`
7. Return `ExtendedSeries(df['basisSwapRate'])` or empty series

**Raises:** `NotImplementedError` for real-time. `MqValueError` for invalid swap tenor.

### swap_term_structure(asset: Asset, benchmark_type: str = None, floating_rate_tenor: str = None, tenor_type: _SwapTenorType = None, tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None, pricing_date: Optional[GENERIC_DATE] = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day IRS swap rate term structure across major currencies. Plots rates against expiration dates.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_swap_rate_asset, query_type=QueryType.SWAP_RATE)])`

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Extract and validate currency
3. Validate clearing_house, benchmark_type, tenor_type, and term structure tenor
4. Get swap leg defaults; validate floating rate tenor format
5. Resolve pricing_location; normalize
6. Check pricing_date is not a holiday in the calendar
7. Build asset query kwargs; add tenor to appropriate dataset field
8. Fetch multiple assets with `allow_many=True`
9. Compute date range from pricing_date
10. Execute market data query within DataContext
11. If DataFrame empty -> empty series
12. Otherwise:
    - Select latest date row(s)
    - If single row (pd.Series) -> build single-point series with date from tenor
    - If DataFrame:
      - If `effectiveTenor` column, filter out IMM date entries
      - Compute expiration dates via `_get_term_struct_date`
      - Set as DatetimeIndex, sort, filter to DataContext date range
      - Build ExtendedSeries of `swapRate`
13. Attach dataset_ids; if empty call `check_forward_looking`
14. Return series

**Raises:** `NotImplementedError` for real-time or unsupported currency. `MqValueError` for invalid tenors or holiday pricing date.

### basis_swap_term_structure(asset: Asset, spread_benchmark_type: str = None, spread_tenor: str = None, reference_benchmark_type: str = None, reference_tenor: str = None, tenor_type: _SwapTenorType = None, tenor: Optional[GENERIC_DATE] = None, clearing_house: _ClearingHouse = None, location: PricingLocation = None, pricing_date: Optional[GENERIC_DATE] = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day floating-floating IRS basis swap term structure.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=_currency_to_tdapi_basis_swap_rate_asset, query_type=QueryType.BASIS_SWAP_RATE)])`

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Validate tenor_type, get term structure tenor dict
3. Build basis swap kwargs via `_get_basis_swap_kwargs`
4. Add tenor to appropriate dataset field
5. Check pricing_date is not a holiday
6. Fetch multiple assets with `allow_many=True`
7. Execute market data query within DataContext
8. If DataFrame empty -> empty series
9. Otherwise:
    - Select latest date; if single row (pd.Series) -> build single-point series
    - If DataFrame:
      - Filter out IMM entries if `effectiveTenor`
      - Compute expiration dates, sort, filter to DataContext range
      - Build ExtendedSeries of `basisSwapRate`
10. Attach dataset_ids; if empty call `check_forward_looking`
11. Return series

**Raises:** `NotImplementedError` for real-time. `MqValueError` for holiday pricing date.

### _get_fxfwd_xccy_swp_rates_data(asset: Asset, tenor: str, real_time: bool = False, source: str = None, query_type: QueryType = None) -> pd.DataFrame
Purpose: Retrieve FX forward / OIS cross-currency swap rate data for G10 crosses.

**Algorithm:**
1. If `real_time` -> raise `NotImplementedError`
2. Get pair from asset Bloomberg ID; validate in `CROSS_BBID_TO_DUMMY_OISXCCY_ASSET`
3. Validate tenor matches `(\d+)([wfmy])` regex
4. Remap `'m'` to `'f'` in tenor (month to forward convention)
5. Extract non-USD currency; get default pricing location
6. Build kwargs with `type='Forward'`, settlement_date, pair
7. Resolve asset ID; build and execute market data query
8. Return DataFrame

**Raises:** `NotImplementedError` for real-time or unsupported pair. `MqValueError` for invalid tenor.

### ois_xccy(asset: Asset, tenor: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day OIS cross-currency spread curves across G10 crosses.

**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Forward, AssetType.Cross), [MeasureDependency(id_provider=_cross_to_fxfwd_xcswp_asset, query_type=QueryType.OIS_XCCY)])`

**Algorithm:**
1. Delegate to `_get_fxfwd_xccy_swp_rates_data` with `query_type=QueryType.OIS_XCCY`
2. Return `ExtendedSeries(df['oisXccy'])` or empty series

### ois_xccy_ex_spike(asset: Asset, tenor: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day OIS cross-currency spread curves excluding spikes.

**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Forward, AssetType.Cross), [MeasureDependency(id_provider=_cross_to_fxfwd_xcswp_asset, query_type=QueryType.OIS_XCCY_EX_SPIKE)])`

**Algorithm:**
1. Delegate to `_get_fxfwd_xccy_swp_rates_data` with `query_type=QueryType.OIS_XCCY_EX_SPIKE`
2. Return `ExtendedSeries(df['oisXccyExSpike'])` or empty series

### non_usd_ois(asset: Asset, tenor: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day non-USD domestic OIS rate curve derived from G10 FX crosses.

**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Forward, AssetType.Cross), [MeasureDependency(id_provider=_cross_to_fxfwd_xcswp_asset, query_type=QueryType.NON_USD_OIS)])`

**Algorithm:**
1. Delegate to `_get_fxfwd_xccy_swp_rates_data` with `query_type=QueryType.NON_USD_OIS`
2. Return `ExtendedSeries(df['nonUsdOis'])` or empty series

### usd_ois(asset: Asset, tenor: str = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: GS end-of-day USD domestic OIS rate curve derived from G10 FX crosses.

**Decorator:** `@plot_measure((AssetClass.FX,), (AssetType.Forward, AssetType.Cross), [MeasureDependency(id_provider=_cross_to_fxfwd_xcswp_asset, query_type=QueryType.USD_OIS)])`

**Algorithm:**
1. Delegate to `_get_fxfwd_xccy_swp_rates_data` with `query_type=QueryType.USD_OIS`
2. Return `ExtendedSeries(df['usdOis'])` or empty series

### get_cb_swaps_kwargs(currency: CurrencyEnum, benchmark_type: BenchmarkTypeCB) -> Dict
Purpose: Build kwargs for querying all central bank meeting-dated swap assets for a given currency.

**Algorithm:**
1. Validate benchmark_type and clearing_house (defaults to LCH)
2. Get swap leg defaults for the benchmark
3. Generate possible tenors: `['{cb_prefix}{i}' for i in range(0, 20)]` for both swap and forward tenors
4. Append `'0b'` to forward tenors
5. Build and return asset query kwargs dict

### get_cb_meeting_swaps(currency: CurrencyEnum, benchmark_type: BenchmarkTypeCB) -> List
Purpose: Fetch all central bank meeting-dated swap asset IDs for a currency.

**Algorithm:**
1. Build kwargs via `get_cb_swaps_kwargs`
2. Return `_get_tdapi_rates_assets(allow_many=True, **kwargs)`

### get_cb_meeting_swap(currency: CurrencyEnum, benchmark_type: BenchmarkTypeCB, forward_tenor: str, swap_tenor: str) -> str
Purpose: Fetch a single central bank meeting swap asset by specific forward and swap tenors.

**Algorithm:**
1. Build kwargs via `get_cb_swaps_kwargs`
2. Validate `swap_tenor` matches `{cb_prefix}[0-9]|1[0-9]` or `forward_tenor` matches same pattern or `0b`
3. Set specific `termination_date` and `effective_date` in kwargs
4. Return `_get_tdapi_rates_assets(**kwargs)`

**Raises:** `MqValueError` for invalid tenor format

### get_cb_swap_data(currency: CurrencyEnum, rate_mqids: list = None) -> pd.DataFrame
Purpose: Fetch intraday central bank swap rate data from the IR_SWAP_RATES_INTRADAY_CALC_BANK dataset.

**Algorithm:**
1. Create `Dataset(Dataset.GS.IR_SWAP_RATES_INTRADAY_CALC_BANK)`
2. Get default pricing location for currency, normalize
3. Call `ds.get_data` with asset IDs, pricing location, start/end times from `DataContext`
4. Return DataFrame

### policy_rate_term_structure(asset: Asset, event_type: EventType = EventType.MEETING, rate_type: RateType = RateType.ABSOLUTE, valuation_date: Optional[GENERIC_DATE] = None, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Forward policy rate expectations for future CB meetings or EOY dates as of a specified valuation date. Indexed by meeting date.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=currency_to_default_ois_asset, query_type=QueryType.CENTRAL_BANK_SWAP_RATE)])`

**Algorithm:**
1. Call `check_forward_looking` for validation
2. If `real_time` -> raise `NotImplementedError` (use `valuation_date="Intraday"` instead)
3. Validate `event_type` is EventType and `rate_type` is RateType
4. If `valuation_date` is string `"intraday"` (case-insensitive) -> delegate to `policy_rate_term_structure_rt`
5. Otherwise:
   - Parse valuation_date via `parse_meeting_date`
   - Convert asset to OIS benchmark rate ID
   - Query CENTRAL_BANK_WATCH dataset:
     - SPOT: query by rateType only (no valuation date); if relative -> raise MqValueError
     - MEETING/EOY: query with valuationDate filter
6. If empty DataFrame -> return empty series
7. If `rate_type == RELATIVE` -> subtract spot (meetingNumber=0) value from all values
8. Reset index, set meetingDate as index, build ExtendedSeries
9. On KeyError (no data) -> return empty series
10. Return series

**Raises:** `NotImplementedError` for real_time. `MqValueError` for invalid event_type, rate_type, or RELATIVE with SPOT.

### policy_rate_expectation(asset: Asset, event_type: EventType = EventType.MEETING, rate_type: RateType = RateType.ABSOLUTE, meeting_date: Union[dt.date, int, str] = 0, *, source: str = None, real_time: bool = False) -> pd.Series
Purpose: Historical evolution of OIS/policy rate expectations for a specific meeting date or EOY date.

**Decorator:** `@plot_measure((AssetClass.Cash,), (AssetType.Currency,), [MeasureDependency(id_provider=currency_to_default_ois_asset, query_type=QueryType.POLICY_RATE_EXPECTATION)])`

**Algorithm:**
1. Validate event_type, rate_type, meeting_date types
2. If `real_time` -> delegate to `policy_rate_expectation_rt`
3. Otherwise:
   - Convert asset to OIS benchmark rate ID
   - Query CENTRAL_BANK_WATCH dataset:
     - SPOT: query by rateType only
     - Integer meeting_date (0-20): query by meetingNumber
     - Date/string meeting_date: parse and query by meetingDate
4. If empty DataFrame -> raise `MqValueError`
5. If `rate_type == RELATIVE`:
   - Query spot data (meetingNumber=0)
   - If spot empty -> raise `MqValueError`
   - Merge meeting data with spot, compute `value - spotValue`
   - Set `valuationDate` as index, return `relValue`
6. If ABSOLUTE -> set `valuationDate` as index, return `value`
7. Attach dataset_ids and return

**Raises:** `MqValueError` for invalid types, empty data, meeting_number out of range (0-20), or empty spot data for relative.

### parse_meeting_date(valuation_date: Optional[GENERIC_DATE] = None) -> dt.date
Purpose: Parse a valuation date from various formats into a dt.date.

**Algorithm:**
1. If string with 3 dash-separated parts (YYYY-MM-DD) -> parse directly
2. If other string (relative like '1m') -> call `_range_from_pricing_date('USD', valuation_date)`, return end date
3. If `dt.date` or `None` -> call `_range_from_pricing_date(None, valuation_date)`, return end date
4. Otherwise -> raise `MqValueError`
5. Convert `pd.Timestamp` to `dt.date` if needed

**Raises:** `MqValueError` for unsupported valuation_date types

### _get_default_ois_benchmark(currency: CurrencyEnum) -> BenchmarkTypeCB
Purpose: Return the default OIS benchmark type for central bank watch currencies.

**Algorithm:**
1. USD -> `BenchmarkTypeCB.Fed_Funds`
2. GBP -> `BenchmarkTypeCB.SONIA`
3. EUR -> `BenchmarkTypeCB.EUROSTR`
4. Other -> returns None implicitly

### _check_cb_ccy_benchmark_rt(asset: Asset, benchmark_type: BenchmarkTypeCB) -> tuple
Purpose: Validate currency and benchmark type for real-time central bank data. Only EUR, GBP, USD supported.

**Algorithm:**
1. Extract currency from asset Bloomberg ID
2. If not in [EUR, GBP, USD] -> raise `MqValueError`
3. If `benchmark_type is None` -> resolve via `_get_default_ois_benchmark`
4. Validate benchmark is in `CURRENCY_TO_SWAP_RATE_BENCHMARK` for the currency
5. Return `(currency, benchmark_type)` tuple

**Raises:** `MqValueError` for unsupported currency or benchmark

### _get_swap_from_meeting_date(currency: CurrencyEnum, benchmark_type: BenchmarkTypeCB, meeting_date: Union[dt.date, int, str]) -> str
Purpose: Resolve a single CB meeting swap asset ID from a meeting date identifier.

**Algorithm:**
1. If `meeting_date` is int:
   - If 0 -> forward_tenor=`'0b'`; else forward_tenor=`'{cb_prefix}{meeting_date}'`
   - swap_tenor = `'{cb_prefix}{meeting_date + 1}'`
2. If string matching `{cb_prefix}[0-9]|1[0-9]`:
   - If last char is `'0'` -> forward_tenor=`'0b'`; else forward_tenor from string
   - swap_tenor = next meeting number
3. Otherwise -> raise `MqValueError`
4. Call `get_cb_meeting_swap` and return result

**Raises:** `MqValueError` for invalid meeting_date format

### policy_rate_expectation_rt(asset: Asset, event_type: EventType = EventType.MEETING, rate_type: RateType = RateType.ABSOLUTE, meeting_date: Union[dt.date, int, str] = 0, benchmark_type: BenchmarkTypeCB = None) -> ExtendedSeries
Purpose: Real-time policy rate expectation for a single meeting date using intraday CB swap data.

**Algorithm:**
1. Validate currency and benchmark via `_check_cb_ccy_benchmark_rt`
2. Resolve swap asset ID via `_get_swap_from_meeting_date`
3. Fetch intraday data via `get_cb_swap_data`
4. If empty -> raise `MqValueError`
5. Branch on event_type:
   - SPOT + ABSOLUTE -> return `df['rate']`
   - SPOT + RELATIVE -> raise `MqValueError`
   - MEETING + RELATIVE -> fetch spot swap (0b -> cb1), merge, compute `rate_meeting - rate_spot`
   - MEETING + ABSOLUTE -> return `df['rate']` directly
   - EOY -> raise `MqValueError` (not supported for RT)
6. Attach dataset_ids and return

**Raises:** `MqValueError` for empty data, SPOT+RELATIVE, or EOY event type

### policy_rate_term_structure_rt(asset: Asset, event_type: EventType = EventType.MEETING, rate_type: RateType = RateType.ABSOLUTE, benchmark_type: BenchmarkTypeCB = None, source: str = None) -> ExtendedSeries
Purpose: Real-time policy rate term structure across all CB meeting dates using intraday data.

**Algorithm:**
1. Validate currency and benchmark via `_check_cb_ccy_benchmark_rt`
2. Branch on event_type:
   - SPOT + RELATIVE -> raise `MqValueError`
   - SPOT + ABSOLUTE -> fetch single spot swap, return `df['rate']`
   - MEETING -> fetch all meeting swaps via `get_cb_meeting_swaps`, get data
   - EOY -> raise `MqValueError` (not supported for RT)
3. If MEETING data empty -> return empty ExtendedSeries
4. If `rate_type == RELATIVE`:
   - Fetch spot swap data
   - If empty -> raise `MqValueError`
   - Merge meeting data with spot, compute `rate_meeting - rate_spot`
   - Rename `effectiveDate_meeting` -> `effectiveDate`
5. If joined_df empty -> empty series
6. Otherwise:
   - Select latest timestamp
   - Compute expiration dates from `effectiveDate` column
   - Set `expirationDate` as DatetimeIndex, sort, filter to DataContext range
   - Build ExtendedSeries of `rate`
7. Attach dataset_ids; if empty call `check_forward_looking`
8. Return series

**Raises:** `MqValueError` for SPOT+RELATIVE, EOY, or empty spot data

## State Mutation
- `swaptions_defaults_provider`: Module-level singleton initialized at import. Its `defaults` dict is mutated during `__init__` to add the `'MAPPING'` key. Not modified after initialization.
- All public measure functions are stateless / read-only (they query external APIs and return new Series).
- `DataContext.current`: Used as default for `start`/`end` parameters; some functions use `with DataContext(...)` context manager which temporarily changes the current context.
- Thread safety: No explicit thread safety mechanisms. `swaptions_defaults_provider` is effectively immutable after import. External API calls and `DataContext` usage may have thread-safety implications from upstream modules.

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `_default_pricing_location` | Currency not in CURRENCY_TO_PRICING_LOCATION |
| `MqValueError` | `_get_tdapi_rates_assets` | Zero assets match, or multiple match unexpectedly |
| `MqValueError` | `_check_forward_tenor` | Invalid forward tenor format |
| `MqValueError` | `_check_benchmark_type` | Invalid benchmark string (when nothrow=False), or benchmark not supported for currency |
| `MqValueError` | `_check_clearing_house` | Invalid clearing house string |
| `MqValueError` | `_check_tenor_type` | Invalid tenor type string |
| `MqValueError` | `_check_term_structure_tenor` | Invalid swap tenor format |
| `MqValueError` | `_get_swap_leg_defaults` | No default fixing tenor for benchmark |
| `MqValueError` | `_get_swap_data` | Invalid swap/floating rate tenor |
| `MqValueError` | `_get_swap_data_calc` | Invalid swap tenor |
| `MqValueError` | `_swaption_build_asset_query` | Invalid benchmark->FRO mapping, invalid tenor, invalid strike |
| `MqValueError` | `_check_strike_reference` | Invalid strike reference format |
| `MqValueError` | `forward_rate` | forward_term not specified |
| `MqValueError` | `discount_factor` | tenor not specified |
| `MqValueError` | `instantaneous_forward_rate` | tenor not specified |
| `MqValueError` | `index_forward_rate` | forward_start_tenor not specified, or no default fixing tenor |
| `MqValueError` | `_get_basis_swap_kwargs` | Invalid floating rate tenors |
| `MqValueError` | `basis_swap_spread` | Invalid swap tenor |
| `MqValueError` | `swap_term_structure` | Invalid floating rate tenor, or holiday pricing date |
| `MqValueError` | `basis_swap_term_structure` | Holiday pricing date |
| `MqValueError` | `_get_fxfwd_xccy_swp_rates_data` | Invalid tenor format |
| `MqValueError` | `get_cb_meeting_swap` | Invalid swap/forward tenor |
| `MqValueError` | `parse_meeting_date` | Invalid valuation_date type |
| `MqValueError` | `policy_rate_term_structure` | Invalid event_type/rate_type; RELATIVE + SPOT |
| `MqValueError` | `policy_rate_expectation` | Invalid types; empty data; meeting_number out of range; empty spot for relative |
| `MqValueError` | `_check_cb_ccy_benchmark_rt` | Unsupported currency or benchmark for RT CB data |
| `MqValueError` | `_get_swap_from_meeting_date` | Invalid meeting_date format |
| `MqValueError` | `policy_rate_expectation_rt` | Empty data; SPOT+RELATIVE; EOY event |
| `MqValueError` | `policy_rate_term_structure_rt` | SPOT+RELATIVE; EOY; empty spot data |
| `NotImplementedError` | `_get_swap_data` | Real-time for non-SWAP_RATE query; unsupported currency |
| `NotImplementedError` | `_get_swaption_measure` | Real-time; unsupported currency |
| `NotImplementedError` | `swaption_vol_smile` | Real-time |
| `NotImplementedError` | `swaption_vol_term` | Real-time |
| `NotImplementedError` | `swap_term_structure` | Real-time; unsupported currency |
| `NotImplementedError` | `basis_swap_spread` | Real-time |
| `NotImplementedError` | `basis_swap_term_structure` | Real-time |
| `NotImplementedError` | `_get_fxfwd_xccy_swp_rates_data` | Real-time; unsupported pair |
| `NotImplementedError` | `_get_basis_swap_kwargs` | Unsupported currency |
| `NotImplementedError` | `policy_rate_term_structure` | Real-time (with instruction to use Intraday valuation_date) |
| `KeyError` | `policy_rate_term_structure` | Caught internally when DataFrame has no 'meetingDate' column; returns empty series |

## Edge Cases
- `_currency_to_tdapi_swap_rate_asset_for_intraday` ignores its `asset_spec` argument entirely, always returning the same hardcoded Marquee ID `'MACF6R4J5FY4KGBZ'`
- `TdapiRatesDefaultsProvider.get_swaption_parameter` iterates through both currency-specific and common defaults, with the common entry potentially overwriting the currency-specific one (last-write-wins). If `field` appears in both, the COMMON value is returned
- `_get_swap_data` validates forward_tenor with `re.fullmatch('(frb[1-9]|ecb[1-6])', forward_tenor)` even when swap_tenor already passes validation; this means `forward_tenor` must not be None if swap_tenor is invalid
- `_match_floating_tenors` converts `"12m"` to `"1y"` only for SOFR legs, not for LIBOR/EURIBOR/STIBOR legs
- `_check_term_structure_tenor` for SWAP_TENOR case: the regex check `re.fullmatch('(frb[1-9])', tenor)` is evaluated with `or`, meaning a valid relative tenor that also matches `frb[1-9]` would raise an error
- `swaption_vol_smile` parses `strikeRelative` column using `d.split("ATM")[1]` which produces empty string for `"ATM"`, handled by the ternary returning `0`
- `policy_rate_term_structure` with `event_type=SPOT` and `rate_type=RELATIVE` raises `MqValueError`, but `policy_rate_expectation` with SPOT event does not (it fetches data differently)
- `policy_rate_term_structure_rt` uses `.loc[:, 'expirationDate']` with `SettingWithCopyWarning` potential when assigning to a slice of a filtered DataFrame
- `_get_default_ois_benchmark` returns `None` implicitly for currencies other than USD, GBP, EUR, which could propagate as a None benchmark_type
- `_get_fxfwd_xccy_swp_rates_data` remaps tenor `'m'` to `'f'` (e.g. `'3m'` -> `'3f'`) for FX forward settlement date convention; also strips `'USD'` from pair to get non-USD currency, which assumes USD is always one side
- `get_cb_meeting_swap` regex only handles single-digit meeting numbers for the `cb_prefix[0-9]` part; the `1[0-9]` alternative covers 10-19 but there is a regex grouping issue where `|` separates the whole pattern

## Bugs Found
- Line 727: `_get_swap_data` checks `re.fullmatch('(frb[1-9]|ecb[1-6])', forward_tenor)` which will raise `TypeError` if `forward_tenor` is `None` (since `re.fullmatch` on None raises). This means `forward_tenor` cannot be None when `swap_tenor` is also invalid. (OPEN)
- Line 633: In `_check_term_structure_tenor`, the `or` condition `re.fullmatch('(frb[1-9])', tenor)` means that if a tenor like `'frb1'` is passed for SWAP_TENOR type, it would match the second regex, making the `not(... or ...)` evaluate to `False`, so it would NOT raise. But `'frb1'` is not a valid swap tenor. The logic is inverted from the apparent intent. (OPEN)
- Line 2476-2477: In `get_cb_meeting_swap`, the regex pattern `f"({CCY_TO_CB[currency.value]}[0-9]|1[0-9])"` has incorrect grouping. The `|` splits between `{prefix}[0-9]` and `1[0-9]`, meaning `10`-`19` would match without the prefix. Should likely be `f"({CCY_TO_CB[currency.value]}([0-9]|1[0-9]))"`. (OPEN)
- Line 2829: `joined_df.loc[:, 'expirationDate'] = ...` may trigger `SettingWithCopyWarning` since `joined_df` may be a view from `.loc[latest]`. Using `.assign()` or `.copy()` would be safer. (OPEN)

## Coverage Notes
- Branch count: ~180+ distinct branches across all validation functions, data retrieval paths, and result formatting logic
- Key high-branch-count functions: `_check_benchmark_type` (~8 branches), `_get_swap_data` (~10 branches), `swap_term_structure` (~12 branches), `policy_rate_term_structure` (~10 branches), `policy_rate_expectation` (~12 branches), `policy_rate_term_structure_rt` (~10 branches), `policy_rate_expectation_rt` (~8 branches), `_swaption_build_asset_query` (~10 branches), `_get_tdapi_rates_assets` (~6 branches)
- External API dependencies (`GsAssetApi.get_many_assets`, `GsDataApi.build_market_data_query`, `_market_data_timed`, `GsDataApi.get_mxapi_backtest_data`, `GsDataApi.get_mxapi_curve_measure`, `Dataset.get_data`) require mocking in tests
- All `@plot_measure`-decorated functions require mock asset objects with `get_identifier(AssetIdentifier.BLOOMBERG_ID)` returning valid currency codes
- Real-time branches are largely `NotImplementedError` raises except for central bank watch functions which delegate to `*_rt` functions
- Pragmas: None observed in source
