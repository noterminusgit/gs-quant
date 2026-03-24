# instrument/core.py

## Summary
Core module defining the `Instrument` class hierarchy -- the primary abstraction for financial instruments in gs_quant. Provides the pricing interface (`calc`, `resolve`), deserialization from dicts and text (`from_dict`, `from_quick_entry`, `from_asset_ids`), scaling/flipping operations, and codec registration with `dataclasses_json`. Also defines `DummyInstrument` (for testing), `Security` (identifier-based instrument via XRef), and encoder helper functions.

## Dependencies
- Internal: `gs_quant.api.gs.parser` (GsParserApi), `gs_quant.api.gs.risk` (GsRiskApi), `gs_quant.api.gs.assets` (GsAssetApi -- lazy import), `gs_quant.base` (get_enum_value, InstrumentBase, Priceable, Scenario), `gs_quant.common` (AssetClass, AssetType, XRef, RiskMeasure, MultiScenario), `gs_quant.markets` (HistoricalPricingContext, MarketDataCoordinate, PricingContext), `gs_quant.priceable` (PriceableImpl), `gs_quant.risk` (FloatWithInfo, DataFrameWithInfo, SeriesWithInfo, ResolvedInstrumentValues, DEPRECATED_MEASURES), `gs_quant.risk.results` (ErrorValue, MultipleRiskMeasureFuture, PricingFuture, MultipleScenarioFuture), `gs_quant_internal.base` (decode_quill_value -- lazy import)
- External: `datetime`, `inspect`, `logging`, `warnings`, `copy` (deepcopy), `typing`, `dataclasses_json` (global_config)

## Type Definitions

### Instrument (class)
Inherits: PriceableImpl, InstrumentBase

The central base class for all tradeable instruments. Provides pricing (`calc`), resolution (`resolve`), deserialization (`from_dict`, `from_quick_entry`, `from_asset_ids`), and position manipulation (`scale`, `flip`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| PROVIDER | `type` | `GsRiskApi` | Class-level: default risk API provider |
| __instrument_mappings | `dict` | `{}` | Class-level private: lazy-populated cache mapping `(AssetClass, AssetType)` tuples to instrument subclasses |

### DummyInstrument (class)
Inherits: Instrument

A testing stub with a configurable dummy result.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| dummy_result | `Union[str, float]` | `None` | The mock result returned by this dummy instrument |

### Security (class)
Inherits: XRef, Instrument

A financial security identified by a standard identifier (ticker, BBID, RIC, ISIN, CUSIP, or Prime ID).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| ticker | `str` | `None` | Exchange ticker |
| bbid | `str` | `None` | Bloomberg identifier |
| ric | `str` | `None` | Reuters Instrument Code |
| isin | `str` | `None` | International Securities Identification Number |
| cusip | `str` | `None` | CUSIP identifier |
| prime_id | `str` | `None` | Goldman Sachs internal Prime identifier |
| quantity_ | `float` | `1` | Quantity (contracts for exchange-traded, notional for bonds) |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### Instrument.__repr__(self) -> str
Purpose: Human-readable string representation including class name and optional name field.

**Algorithm:**
1. Return `ClassName` if `self.name` is falsy
2. Return `ClassName(name)` if `self.name` is truthy

### Instrument.__asset_class_and_type_to_instrument(cls) -> dict
Purpose: Lazily build and return the mapping from `(AssetClass, AssetType)` to instrument subclass.

**Algorithm:**
1. Branch: if `cls.__instrument_mappings` is empty, populate it
2. Dynamically import `gs_quant.target.instrument`
3. Enumerate all classes in the module that are subclasses of `Instrument` (excluding `Instrument` itself)
4. Hard-code `(Cash, Currency) -> Forward` mapping
5. For each instrument subclass, call `default_instance()` and map `(asset_class, type)` to the class
6. Return the cached mapping

### Instrument.provider (property) -> type
Purpose: Return the PROVIDER class attribute (GsRiskApi by default).

### Instrument.resolve(self, in_place: bool = True) -> Optional[Union[PriceableImpl, PricingFuture, dict]]
Purpose: Resolve non-supplied properties of an instrument by calling the pricing engine.

**Algorithm:**
1. Determine if current context is `HistoricalPricingContext`
2. Define inner `handle_result` closure:
   - Branch: result is `ErrorValue` -> log error; return `{date: None}` if historical, else `None`
   - Branch: result is `None` -> log error; return `{date: self}` if historical, else `self`
   - Branch: `in_place` is True and result is valid -> call `self.from_instance(result)`, return `None`
   - Branch: `in_place` is False -> return result as-is
3. Branch: `in_place` and `is_historical` -> raise `RuntimeError`
4. Branch: `in_place` and any `MultiScenario` in `Scenario.path` -> raise `RuntimeError`
5. Delegate to `self.calc(ResolvedInstrumentValues, fn=handle_result)`

**Raises:**
- `RuntimeError` when resolving in-place under `HistoricalPricingContext`
- `RuntimeError` when resolving in-place under `MultiScenario` context

### Instrument.calc(self, risk_measure: Union[RiskMeasure, Iterable[RiskMeasure]], fn=None) -> Union[DataFrameWithInfo, ErrorValue, FloatWithInfo, PriceableImpl, PricingFuture, SeriesWithInfo, Tuple[MarketDataCoordinate, ...]]
Purpose: Calculate risk measure(s) for this instrument using the current pricing context.

**Algorithm:**
1. Determine if `risk_measure` is a single `RiskMeasure` or iterable
2. Check `Scenario.path` for any `MultiScenario` instance
3. Define inner `get_inst_futures(curr_measure)`:
   - Branch: `multi_scenario` exists -> wrap in `MultipleScenarioFuture`
   - Branch: no multi_scenario -> call `curr_measure.pricing_context.calc(self, curr_measure)`
4. Within `self._pricing_context`:
   - Branch: single measure -> call `get_inst_futures(risk_measure)`
   - Branch: multiple measures -> wrap in `MultipleRiskMeasureFuture` with dict of futures
5. For each measure, check if its name is in `DEPRECATED_MEASURES`:
   - Branch: deprecated -> issue `DeprecationWarning`
6. Branch: `fn` is not None -> create new `PricingFuture`, attach callback `cb` that applies `fn` to result (catching exceptions)
7. Branch: `self._return_future` -> return future; else return `future.result()`

### Instrument.from_dict(cls, values: dict) -> Optional[Instrument]
Purpose: Deserialize an instrument from a dictionary representation.

**Algorithm:**
1. Branch: `values` is falsy -> return `None`
2. Branch: `cls` has `asset_class` attribute -> use `cls` directly as instrument class
3. Branch: no `asset_class` on cls:
   a. Look for `$type` key or nested builder/defn `$type`
   b. Branch: `builder_type` found -> delegate to `decode_quill_value` (internal import)
   c. Look for `asset_class` or `assetClass` field
   d. Branch: no asset_class field -> raise `ValueError('assetClass/asset_class not specified')`
   e. Branch: no `type` field -> raise `ValueError('type not specified')`
   f. Pop `type` and `asset_class` from values
   g. Determine `default_type`: `Security` if both asset_type and asset_class are in `(None, '', 'Security')`, else `None`
   h. Look up instrument class from `__asset_class_and_type_to_instrument()` mapping
   i. Branch: instrument is `None` -> raise `ValueError('unable to build instrument')`
4. Call `instrument.from_dict(values)` recursively on the resolved concrete class

**Raises:**
- `ValueError` when `assetClass`/`asset_class` not specified
- `ValueError` when `type` not specified
- `ValueError` when instrument class cannot be resolved

### Instrument.from_quick_entry(cls, text: str, asset_class: Optional[AssetClass] = None) -> Instrument
Purpose: Parse a human-readable text description into an instrument using the parser API.

**Algorithm:**
1. Branch: `asset_class` not provided -> try `cls.default_instance().asset_class`
2. Branch: still no `asset_class` -> call `GsParserApi.get_instrument_from_text(text)`
   - Branch: results non-empty -> pop first instrument
   - Branch: empty -> raise `ValueError('Could not resolve instrument')`
3. Branch: `asset_class` available -> call `GsParserApi.get_instrument_from_text_asset_class(text, asset_class.value)`
4. Call `cls.from_dict(instrument)`, catching `AttributeError` -> raise `ValueError('Invalid instrument specification')`

**Raises:**
- `ValueError` when instrument cannot be resolved from text
- `ValueError` when instrument specification is invalid

### Instrument.from_asset_ids(cls, asset_ids: Tuple[str, ...]) -> Tuple[InstrumentBase, ...]
Purpose: Retrieve instruments by their GS asset IDs.

**Algorithm:**
1. Lazy-import `GsAssetApi`, call `get_instruments_for_asset_ids(asset_ids)`
2. Try `cls.default_instance()` to get expected `asset_class` and `type`
   - Branch: any returned instrument does not match -> raise `ValueError`
3. `AttributeError` caught silently (for generic `Instrument` base class calls)
4. Return instruments tuple

**Raises:** `ValueError` when not all instruments match the expected type

### Instrument.from_asset_id(cls, asset_id: str) -> InstrumentBase
Purpose: Convenience wrapper for single asset ID; delegates to `from_asset_ids((asset_id,))[0]`.

### Instrument.compose(components: Iterable) -> dict (staticmethod)
Purpose: Compose a date-keyed dict from resolved components, using `risk_key.date` for `ErrorValue` instances and `resolution_key.date` otherwise.

### Instrument.flip(self, in_place: bool = True)
Purpose: Flip the instrument direction by scaling by -1. Delegates to `self.scale(-1, in_place)`.

### Instrument.scale(self, scaling: float, in_place: bool = True, check_resolved=True) -> Optional[Instrument]
Purpose: Scale the instrument's notional/quantity by a factor.

**Algorithm:**
1. Branch: `scaling` is `None` -> return `self` unchanged
2. Branch: `self` does not have `scale_in_place` method -> raise `NotImplementedError`
3. Branch: `in_place` -> call `self.scale_in_place(scaling, check_resolved=check_resolved)`, return `None`
4. Branch: not `in_place` -> `deepcopy(self)`, call `scale` on copy, return copy

**Raises:** `NotImplementedError` when `scale_in_place` is not implemented on the concrete subclass

### DummyInstrument.__init__(self, dummy_result: Union[str, float] = None)
Purpose: Create a dummy instrument with an optional mock result.

### DummyInstrument.type (property) -> AssetType
Purpose: Always returns `AssetType.Any`.

### Security.__init__(self, ticker, bbid, ric, isin, cusip, prime_id, quantity)
Purpose: Create a security from exactly one identifier and an optional quantity.

**Algorithm:**
1. Branch: more than one identifier is not None -> raise `ValueError('Only specify one identifier')`
2. Call `XRef.__init__` and `Instrument.__init__` (MRO-aware dual init)
3. Set `self.quantity_`

**Raises:** `ValueError` when more than one identifier is specified

### Security.from_dict(cls, env) -> Security
Purpose: Construct a Security from a dict, filtering keys to match the constructor signature.

### encode_instrument(instrument: Optional[Instrument]) -> Optional[dict]
Purpose: Encode a single instrument to dict via `to_dict()`, returning `None` for `None` input.

### encode_instruments(instruments: Optional[Iterable[Instrument]]) -> Optional[Iterable[Optional[dict]]]
Purpose: Encode an iterable of instruments to a list of dicts. Returns `None` if input is `None`.

## State Mutation
- `Instrument.__instrument_mappings`: Class-level dict, lazily populated on first call to `__asset_class_and_type_to_instrument()`. Shared across all instances.
- `self` fields (via `from_instance`): Mutated in-place by `resolve(in_place=True)` when resolution succeeds.
- `global_config.decoders` / `global_config.encoders`: Module-level side effect at import time -- registers Instrument, InstrumentBase, Priceable codecs.
- Thread safety: `__instrument_mappings` has a benign race on lazy init (dict is replaced atomically). `resolve()` and `scale()` mutate `self` in-place when `in_place=True` -- not thread-safe for concurrent access to the same instance.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `resolve` | `in_place=True` under `HistoricalPricingContext` |
| `RuntimeError` | `resolve` | `in_place=True` under `MultiScenario` context |
| `ValueError` | `from_dict` | Missing `assetClass`/`asset_class` field |
| `ValueError` | `from_dict` | Missing `type` field |
| `ValueError` | `from_dict` | Unable to resolve instrument class from mapping |
| `ValueError` | `from_quick_entry` | Parser returns empty results |
| `ValueError` | `from_quick_entry` | `AttributeError` during `from_dict` |
| `ValueError` | `from_asset_ids` | Returned instruments do not match expected type |
| `NotImplementedError` | `scale` | Concrete class does not implement `scale_in_place` |
| `ValueError` | `Security.__init__` | More than one identifier provided |

## Edge Cases
- `from_dict({})` or `from_dict(None)` returns `None` (falsy check on line 218-219)
- `Security.__init__` identifier uniqueness check on line 355 uses `filter(None, (f is not None for ...))` which always yields `True` values since the generator yields booleans -- the check effectively counts how many identifiers are not `None` and requires at most 1. (Note: `filter(None, ...)` on booleans filters out `False`, so this counts True values.)
- `scale(None)` is a no-op, returning `self`
- `scale(in_place=True)` returns `None` (implicit), while `scale(in_place=False)` returns the new copy
- `calc` with deprecated measures issues warnings but still computes -- warning format is overridden globally via `warnings.formatwarning`
- `from_dict` with `$type` key delegates to internal `decode_quill_value` which requires `gs_quant_internal` -- will raise `ImportError` in open-source builds

## Elixir Porting Notes
- The class hierarchy `Instrument -> PriceableImpl -> Priceable -> Base` maps to Elixir behaviours/protocols. Define an `Instrument` behaviour with callbacks for `calc/2`, `resolve/1`, `scale/3`.
- `from_dict` polymorphic dispatch (mapping `(AssetClass, AssetType)` to module) maps naturally to a registry pattern or protocol dispatch in Elixir.
- The lazy `__instrument_mappings` cache can be an ETS table or a `persistent_term`.
- `PricingContext` context manager pattern maps to Elixir process dictionary or explicit context passing.
- `PricingFuture` / callback chaining maps to `Task` or `GenServer` call patterns.
- `warnings` usage for deprecated measures maps to `Logger.warning/1`.
- `deepcopy` in `scale(in_place=False)` is not needed in Elixir due to immutable data; just return a modified struct.
- `global_config` encoder/decoder registration at module load is a side effect; in Elixir, use protocol implementations or a codec registry in application startup.

## Bugs Found
- Line 355: The `filter(None, ...)` construct receives a generator of `True`/`False` booleans. `filter(None, (True, True, False))` yields `(True, True)` -- the `len(tuple(...))` check works but is convoluted. A simpler `sum(x is not None for x in (ticker, bbid, ...)) > 1` would be clearer. Not a bug, but fragile and confusing. (OPEN)

## Coverage Notes
- Branch count: ~30
- Key branches: `resolve` (4 paths in `handle_result`, 2 RuntimeError guards), `calc` (single vs multiple measures, multi_scenario, deprecated warning, fn callback), `from_dict` (6+ paths), `from_quick_entry` (4 paths), `scale` (4 paths)
- Pragmas: none observed
