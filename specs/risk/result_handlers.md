# result_handlers.py

## Summary
Dispatch module that transforms raw risk computation result dictionaries into typed result objects (`DataFrameWithInfo`, `FloatWithInfo`, `SeriesWithInfo`, `StringWithInfo`, `ErrorValue`, etc.). Each handler function corresponds to a `$type` tag in the server response. The module-level `result_handlers` dictionary maps type-tag strings to handler functions, and is itself referenced recursively by `simple_valtable_handler`.

## Dependencies
- Internal: `gs_quant.base` (`InstrumentBase`, `RiskKey`)
- Internal: `gs_quant.common` (`RiskMeasure`, `AssetClass`, `RiskMeasureType`)
- Internal: `gs_quant.risk.measures` (`PnlExplain`)
- Internal: `gs_quant.risk.core` (`DataFrameWithInfo`, `ErrorValue`, `UnsupportedValue`, `FloatWithInfo`, `SeriesWithInfo`, `StringWithInfo`, `sort_values`, `MQVSValidatorDefnsWithInfo`, `MQVSValidatorDefn`, `DictWithInfo`)
- External: `datetime` (dt)
- External: `logging`
- External: `typing` (`Iterable`, `Optional`, `Union`)

## Type Definitions

### Module-level logger
```
_logger = logging.getLogger(__name__)
```

### TypeAlias (implicit)
```
ResultHandler = Callable[[dict, RiskKey, InstrumentBase, Optional[str]], ResultInfo]
```

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `result_handlers` | `dict[str, Callable]` | See mapping table below | Maps `$type` tags to handler functions |

### result_handlers mapping
| Key | Handler Function |
|-----|-----------------|
| `'Error'` | `error_handler` |
| `'IRPCashflowTable'` | `cashflows_handler` |
| `'LegDefinition'` | `leg_definition_handler` |
| `'Message'` | `message_handler` |
| `'MDAPITable'` | `mdapi_table_handler` |
| `'MMAPITable'` | `mmapi_table_handler` |
| `'MMAPIPCATable'` | `mmapi_pca_table_handler` |
| `'MMAPIPCAHedgeTable'` | `mmapi_pca_hedge_table_handler` |
| `'MQVSValidators'` | `mqvs_validators_handler` |
| `'NumberAndUnit'` | `number_and_unit_handler` |
| `'PriceGrid'` | `dict_risk_handler` |
| `'RequireAssets'` | `required_assets_handler` |
| `'Risk'` | `risk_handler` |
| `'RiskByClass'` | `risk_by_class_handler` |
| `'RiskVector'` | `risk_vector_handler` |
| `'FixingTable'` | `fixing_table_handler` |
| `'Table'` | `simple_valtable_handler` |
| `'CanonicalProjectionTable'` | `canonical_projection_table_handler` |
| `'RiskSecondOrderVector'` | `mdapi_second_order_table_handler` |
| `'RiskTheta'` | `risk_float_handler` |
| `'Market'` | `market_handler` |
| `'Unsupported'` | `unsupported_handler` |

## Functions/Methods

### __dataframe_handler(result: Iterable, mappings: tuple, risk_key: RiskKey, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Private helper that converts an iterable of dict-like rows into a sorted `DataFrameWithInfo` using column mappings.

**Algorithm:**
1. Get `first_row` via `next(iter(result), None)`.
2. Branch: `first_row is None` -> return empty `DataFrameWithInfo(risk_key=risk_key, request_id=request_id)`.
3. Build `indices` boolean list: for each key in `first_row`, check if the key is in the reverse `mappings_lookup` (`{source: dest}`). If present, mark index as `True` and accumulate the destination column name.
4. Build `records` by filtering each row's values using the boolean `indices` mask, then sort via `sort_values(data, columns, columns)`.
5. Construct `DataFrameWithInfo(records, risk_key, request_id)` and set `df.columns` to the destination column names.
6. Return the DataFrame.

**Notes for Elixir port:**
- The `mappings_lookup` inverts the (dest, src) tuples to {src: dest}.
- `sort_values` is imported from `core` and sorts by all columns using `point_sort_order` for known column names.

---

### __dataframe_handler_unsorted(result: Iterable, mappings: tuple, date_cols: tuple, risk_key: RiskKey, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Private helper that converts an iterable of dict-like rows into an unsorted `DataFrameWithInfo`, with date-string parsing.

**Algorithm:**
1. Get `first_row` via `next(iter(result), None)`.
2. Branch: `first_row is None` -> return empty `DataFrameWithInfo(risk_key=risk_key, request_id=request_id)`.
3. Build `records` generator: for each row, extract values via `row.get(field_from)` for each `(field_to, field_from)` in mappings.
4. Construct `DataFrameWithInfo(records, risk_key, request_id)` and set `df.columns` to `[m[0] for m in mappings]`.
5. For each column name in `date_cols`, map values: Branch: `isinstance(x, str)` -> parse via `datetime.strptime(x, '%Y-%m-%d').date()`; else keep as-is.
6. Return the DataFrame.

---

### cashflows_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Transform a cashflows result dict into a DataFrame with 14 columns.

**Algorithm:**
1. Define `mappings` tuple of 14 `(dest, src)` pairs: `currency`, `payment_date`/`payDate`, `set_date`/`setDate`, `accrual_start_date`/`accStart`, `accrual_end_date`/`accEnd`, `payment_amount`/`payAmount`, `notional`, `payment_type`/`paymentType`, `floating_rate_option`/`index`, `floating_rate_designated_maturity`/`indexTerm`, `day_count_fraction`/`dayCountFraction`, `spread`, `rate`, `discount_factor`/`discountFactor`.
2. Define `date_cols = ('payment_date', 'set_date', 'accrual_start_date', 'accrual_end_date')`.
3. Delegate to `__dataframe_handler_unsorted(result['cashflows'], mappings, date_cols, risk_key, request_id)`.

---

### error_handler(result: dict, risk_key: RiskKey, instrument: InstrumentBase, request_id: Optional[str] = None) -> ErrorValue
Purpose: Construct an `ErrorValue` from an error result, logging the error.

**Algorithm:**
1. Extract `error = result.get('errorString', 'Unknown error')`.
2. Branch: `request_id` is truthy -> append `. request Id={request_id}` to error string.
3. Log error at ERROR level with risk_measure, instrument, date, and error message.
4. Return `ErrorValue(risk_key, error, request_id=request_id)`.

---

### leg_definition_handler(result: dict, risk_key: RiskKey, instrument: InstrumentBase, request_id: Optional[str] = None) -> InstrumentBase
Purpose: Resolve an instrument from leg definition data.

**Algorithm:**
1. Return `instrument.resolved(result, risk_key)`.

**Note:** `request_id` is accepted but not used.

---

### message_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> Union[StringWithInfo, ErrorValue]
Purpose: Extract a message string from result, or return an error if missing.

**Algorithm:**
1. Extract `message = result.get('message')`.
2. Branch: `message is None` -> return `ErrorValue(risk_key, "No result returned", request_id=request_id)`.
3. Branch: `message is not None` -> return `StringWithInfo(risk_key, message, request_id=request_id)`.

---

### number_and_unit_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> FloatWithInfo
Purpose: Extract a numeric value with optional unit.

**Algorithm:**
1. Return `FloatWithInfo(risk_key, result.get('value', float('nan')), unit=result.get('unit'), request_id=request_id)`.

---

### required_assets_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Build a two-column DataFrame of required market data assets.

**Algorithm:**
1. Define `mappings = (('mkt_type', 'type'), ('mkt_asset', 'asset'))`.
2. Delegate to `__dataframe_handler(result['requiredAssets'], mappings, risk_key, request_id)`.

---

### dict_risk_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DictWithInfo
Purpose: Wrap the raw result dict in a `DictWithInfo`.

**Algorithm:**
1. Return `DictWithInfo(risk_key, result, unit=None, request_id=request_id)`.

---

### risk_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> Union[DataFrameWithInfo, FloatWithInfo]
Purpose: Handle a scalar or multi-leg risk result.

**Algorithm:**
1. Branch: `result.get('children')` is truthy (multi-leg result):
   a. Initialize `classes = []`.
   b. Branch: `result.get('val')` is truthy -> append `{'path': 'parent', 'value': result.get('val')}` to `classes`.
   c. For each `(key, val)` in `result.get('children').items()`, append `{'path': key, 'value': val}`.
   d. Define `mappings = (('path', 'path'), ('value', 'value'))`.
   e. Delegate to `__dataframe_handler(classes, mappings, risk_key, request_id)` and return.
2. Branch: `result.get('children')` is falsy (scalar result):
   a. Return `FloatWithInfo(risk_key, result.get('val', float('nan')), unit=result.get('unit'), request_id=request_id)`.

---

### risk_by_class_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> Union[DataFrameWithInfo, FloatWithInfo]
Purpose: Handle risk-by-class results, with special aggregation for parallel measures and SPIKE/JUMP filtering.

**Algorithm:**
1. Extract `types = [c['type'] for c in result['classes']]`.
2. Define `external_risk_by_class_val = ['IRBasisParallel', 'IRDeltaParallel', 'IRVegaParallel', 'PnlExplain']`.
3. Branch: `risk_key.risk_measure.name` is in `external_risk_by_class_val` AND `len(types) <= 2` AND `len(set(types)) == 1`:
   a. Return `FloatWithInfo(risk_key, sum(result.get('values', (float('nan'),))), unit=result.get('unit'), request_id=request_id)`.
4. Branch: else (full table result):
   a. Initialize `classes = []`, `skip = []`.
   b. Find `crosses_idx`: index of first element in `result['classes']` where `type == 'CROSSES'`, or `None`.
   c. For each `(idx, (clazz, value))` in `enumerate(zip(result['classes'], result['values']))`:
      - Extract `mkt_type = clazz['type']`.
      - Branch: `'SPIKE' in mkt_type` or `'JUMP' in mkt_type` -> append `idx` to `skip`; Branch: `crosses_idx is not None` -> add `value` to `result['classes'][crosses_idx]['value']` (in-place mutation).
      - Update `clazz` with `{'value': value}`.
   d. For each `(idx, clazz)` in `enumerate(result['classes'])`: Branch: `idx not in skip` -> append to `classes`.
   e. Define `mappings = (('mkt_type', 'type'), ('mkt_asset', 'asset'), ('value', 'value'))`.
   f. Branch: `isinstance(risk_key.risk_measure, PnlExplain)` -> delegate to `__dataframe_handler_unsorted(classes, mappings, (), risk_key, request_id)`.
   g. Branch: else -> delegate to `__dataframe_handler(classes, mappings, risk_key, request_id)`.

---

### risk_vector_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle vector risk results (e.g., delta ladders).

**Algorithm:**
1. Extract `assets = result['asset']`.
2. Branch: `len(assets) == 1` AND `risk_key.risk_measure.name.startswith('Eq')` -> return `FloatWithInfo(risk_key, assets[0], request_id=request_id)` (scalar equity measure).
3. For each `(points, value)` in `zip(result['points'], assets)`: update `points` dict with `{'value': value}` (in-place mutation).
4. Define `mappings` with 6 columns: `mkt_type`/`type`, `mkt_asset`/`asset`, `mkt_class`/`class_`, `mkt_point`/`point`, `mkt_quoting_style`/`quoteStyle`, `value`/`value`.
5. Delegate to `__dataframe_handler(result['points'], mappings, risk_key, request_id)`.

---

### fixing_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> SeriesWithInfo
Purpose: Convert fixing table rows into a date-indexed Series.

**Algorithm:**
1. Extract `rows = result['fixingTableRows']`.
2. Initialize `dates = []`, `values = []`.
3. For each `row` in `rows`: append `dt.date.fromisoformat(row["fixingDate"])` to `dates`, append `row["fixing"]` to `values`.
4. Return `SeriesWithInfo(values, index=dates, risk_key=risk_key, request_id=request_id)`.

---

### simple_valtable_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle a simple valuation table whose values are themselves typed results (recursive handler dispatch).

**Algorithm:**
1. Define inner function `get_value(value)`:
   a. Look up handler from module-level `result_handlers` dict using `value.get('$type')`.
   b. Call `handler(value, risk_key, _instrument, request_id)` and return result.
2. Extract `raw_res = result['rows']`.
3. Build DataFrame from list comprehension: `[(res['label'], get_value(res['value'])) for res in raw_res]`.
4. Pass `unit=raw_res[0]['value'].get('unit')` for the DataFrame unit.
5. Set `df.columns = ['label', 'value']`.
6. Return the DataFrame.

**Note:** This function recursively references `result_handlers` (the module-level dict), creating a circular reference between the dict and this function.

---

### canonical_projection_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Transform canonical projection rows into a 21-column unsorted DataFrame.

**Algorithm:**
1. Define `mappings` tuple of 21 `(dest, src)` pairs: `asset_class`/`assetClass`, `asset`/`asset`, `asset_family`/`assetFamily`, `asset_sub_family`/`assetSubFamily`, `product`/`product`, `product_family`/`productFamily`, `product_sub_family`/`productSubFamily`, `side`/`side`, `size`/`size`, `size_unit`/`sizeUnit`, `quote_level`/`quoteLevel`, `quote_unit`/`quoteUnit`, `start_date`/`startDate`, `end_date`/`endDate`, `expiration_date`/`expiryDate`, `strike`/`strike`, `strike_unit`/`strikeUnit`, `option_type`/`optionType`, `option_style`/`optionStyle`, `tenor`/`tenor`, `tenor_unit`/`tenorUnit`, `premium_currency`/`premiumCcy`, `currency`/`currency`.
2. Define `date_cols = ('start_date', 'end_date', 'expiration_date')`.
3. Delegate to `__dataframe_handler_unsorted(result['rows'], mappings, date_cols, risk_key, request_id)`.

---

### risk_float_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> FloatWithInfo
Purpose: Extract a single float from the `values` array.

**Algorithm:**
1. Return `FloatWithInfo(risk_key, result['values'][0], request_id=request_id)`.

---

### map_coordinate_to_column(coordinate_struct: dict, tag: str) -> dict
Purpose: Flatten a coordinate dict by prefixing keys with a tag, filtering to known coordinate fields.

**Algorithm:**
1. Build `updated_struct`: for each `(k, v)` in `coordinate_struct.items()`, include only keys in `['type', 'asset', 'class_', 'point', 'quoteStyle']`, prefixed with `tag + "_"`.
2. Extract `raw_point = updated_struct.get('point', '')`.
3. Branch: `isinstance(raw_point, list)` -> join with `';'`; else keep as-is.
4. Set `updated_struct['point'] = point`.
5. Return `updated_struct`.

**Note:** The `point` key set at step 4 is NOT prefixed with `tag_`. This means it overwrites any `tag_point` from step 1. This looks like a potential bug (see Bugs Found).

---

### __is_single_row_2nd_order_risk(risk_key: RiskKey) -> bool
Purpose: Check if a risk key represents a single-row second-order risk result.

**Algorithm:**
1. Return `True` if ALL of:
   - `risk_key is not None`
   - `isinstance(risk_key.risk_measure, RiskMeasure)`
   - `risk_key.risk_measure.asset_class == AssetClass.Rates`
   - `risk_key.risk_measure.measure_type` is in `(RiskMeasureType.ParallelGamma, RiskMeasureType.ParallelGammaLocalCcy)`
2. Else return `False`.

---

### mdapi_second_order_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> Union[DataFrameWithInfo, FloatWithInfo]
Purpose: Handle second-order risk (gamma) tables with inner/outer coordinate pairs.

**Algorithm:**
1. Branch: `len(result['values']) == 1` AND `__is_single_row_2nd_order_risk(risk_key)` -> delegate to `risk_float_handler(result, risk_key, _instrument, request_id)` and return.
2. Branch: `len(result['innerPoints']) != len(result['outerPoints'])` -> raise `Exception("Found inner and outer points of different size")`.
3. For each `(inner, outer, value)` in `zip(result['innerPoints'], result['outerPoints'], result['values'])`:
   a. Merge `map_coordinate_to_column(inner, 'inner')` and `map_coordinate_to_column(outer, 'outer')` into `row_dict`.
   b. Set `row_dict['value'] = value`.
   c. Append to `coordinate_pairs`.
4. Define `mappings` with 12 columns: `inner_mkt_type`/`inner_type`, `inner_mkt_asset`/`inner_asset`, `inner_mkt_class`/`inner_class_`, `inner_mkt_point`/`inner_point`, `inner_mkt_quoting_style`/`inner_quotingStyle`, `outer_mkt_type`/`outer_type`, `outer_mkt_asset`/`outer_asset`, `outer_mkt_class`/`outer_class_`, `outer_mkt_point`/`outer_point`, `outer_mkt_quoting_style`/`outer_quotingStyle`, `value`/`value`, `permissions`/`permissions`.
5. Delegate to `__dataframe_handler(coordinate_pairs, mappings, risk_key, request_id)`.

**Raises:** `Exception` when inner and outer points have different lengths.

---

### mdapi_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle MDAPI table results with coordinate/value/permissions rows.

**Algorithm:**
1. Initialize `coordinates = []`.
2. For each `r` in `result['rows']`:
   a. Extract `raw_point = r['coordinate'].get('point', '')`.
   b. Branch: `isinstance(raw_point, list)` -> join with `';'`; else keep as-is.
   c. Update `r['coordinate']` with `{'point': point}`, `{'value': r.get('value', None)}`, `{'permissions': r['permissions']}`.
   d. Append `r['coordinate']` to `coordinates`.
3. Define `mappings` with 7 columns: `mkt_type`/`type`, `mkt_asset`/`asset`, `mkt_class`/`assetClass`, `mkt_point`/`point`, `mkt_quoting_style`/`quotingStyle`, `value`/`value`, `permissions`/`permissions`.
4. Delegate to `__dataframe_handler(coordinates, mappings, risk_key, request_id)`.

---

### mmapi_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle MMAPI table results with model coordinates and timeseries values.

**Algorithm:**
1. Initialize `coordinates = []`.
2. For each `r` in `result['rows']`:
   a. Extract and join `point` from `r['modelCoordinate'].get('point', '')` (list -> `';'`-joined string).
   b. Extract and join `tags` from `r['modelCoordinate'].get('tags', '')` (list -> `';'`-joined string).
   c. Extract `rows = r['value'].get('value', '')`.
   d. For each `row` in `rows`: build `DataPoints` list of `[dt.date.fromisoformat(row["date"]), row["value"]]`.
   e. Update `r['modelCoordinate']` with `{'point': point, 'tags': tags, 'value': DataPoints}`.
   f. Append `r['modelCoordinate']` to `coordinates`.
3. Define `mappings` with 6 columns: `mkt_type`/`type`, `mkt_asset`/`asset`, `mkt_point`/`point`, `mkt_tags`/`tags`, `mkt_quoting_style`/`quotingStyle`, `value`/`value`.
4. Delegate to `__dataframe_handler(coordinates, mappings, risk_key, request_id)`.

---

### mmapi_pca_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle MMAPI PCA table results with coordinate and multiple layer fields.

**Algorithm:**
1. Initialize `coordinates = []`.
2. For each `r` in `result['rows']`:
   a. Extract and join `point` from `r['coordinate'].get('point', '')`.
   b. Update `r['coordinate']` with: `point`, `value`, `layer1`, `layer2`, `layer3`, `layer4`, `level`, `sensitivity`, `irDelta`, `endDate` (all from `r`).
   c. Append `r['coordinate']` to `coordinates`.
3. Define `mappings` with 14 columns: `mkt_type`/`type`, `mkt_asset`/`asset`, `mkt_class`/`assetClass`, `mkt_point`/`point`, `mkt_quoting_style`/`quotingStyle`, `value`/`value`, `layer1`/`layer1`, `layer2`/`layer2`, `layer3`/`layer3`, `layer4`/`layer4`, `level`/`level`, `sensitivity`/`sensitivity`, `irDelta`/`irDelta`, `endDate`/`endDate`.
4. Delegate to `__dataframe_handler(coordinates, mappings, risk_key, request_id)`.

---

### mmapi_pca_hedge_table_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> DataFrameWithInfo
Purpose: Handle MMAPI PCA hedge table results with size, fixedRate, and irDelta fields.

**Algorithm:**
1. Initialize `coordinates = []`.
2. For each `r` in `result['rows']`:
   a. Extract and join `point` from `r['coordinate'].get('point', '')`.
   b. Update `r['coordinate']` with: `point`, `size` (via `r.get('size')`), `fixedRate` (via `r.get('fixedRate')`), `irDelta` (via `r.get('irDelta')`).
   c. Append `r['coordinate']` to `coordinates`.
3. Define `mappings` with 8 columns: `mkt_type`/`type`, `mkt_asset`/`asset`, `mkt_class`/`assetClass`, `mkt_point`/`point`, `mkt_quoting_style`/`quotingStyle`, `size`/`size`, `fixedRate`/`fixedRate`, `irDelta`/`irDelta`.
4. Delegate to `__dataframe_handler(coordinates, mappings, risk_key, request_id)`.

---

### mqvs_validators_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> MQVSValidatorDefnsWithInfo
Purpose: Deserialize MQVS validator definitions.

**Algorithm:**
1. Build `validators` list: `[MQVSValidatorDefn.from_dict(r) for r in result['validators']]`.
2. Return `MQVSValidatorDefnsWithInfo(risk_key, tuple(validators), request_id=request_id)`.

---

### market_handler(result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> StringWithInfo
Purpose: Extract market reference string.

**Algorithm:**
1. Return `StringWithInfo(risk_key, result.get('marketRef'), request_id=request_id)`.

---

### unsupported_handler(_result: dict, risk_key: RiskKey, _instrument: InstrumentBase, request_id: Optional[str] = None) -> UnsupportedValue
Purpose: Return an `UnsupportedValue` sentinel for unsupported result types.

**Algorithm:**
1. Return `UnsupportedValue(risk_key, request_id=request_id)`.

## State Mutation
- `__dataframe_handler` consumes the first element of the `result` iterator via `next(iter(result), None)`, then iterates the rest. The first element is only used for schema detection and is NOT included in the output records (the original iterable `result` is iterated from its current position).
- `risk_by_class_handler` mutates `result['classes']` in-place: adds `'value'` key to each clazz dict, and accumulates SPIKE/JUMP values into the CROSSES entry.
- `risk_vector_handler` mutates `result['points']` dicts in-place by adding `'value'` key.
- `mdapi_table_handler` mutates `r['coordinate']` dicts in-place.
- `mmapi_table_handler` mutates `r['modelCoordinate']` dicts in-place.
- `mmapi_pca_table_handler` mutates `r['coordinate']` dicts in-place.
- `mmapi_pca_hedge_table_handler` mutates `r['coordinate']` dicts in-place.
- Thread safety: No thread-safety mechanisms. Handlers are pure functions with respect to module state (only `_logger` is shared).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` | `mdapi_second_order_table_handler` | `len(result['innerPoints']) != len(result['outerPoints'])` |
| `KeyError` (implicit) | Multiple handlers | When expected keys are missing from `result` dict (e.g., `result['cashflows']`, `result['classes']`, `result['asset']`, etc.) |

## Edge Cases
- `__dataframe_handler` / `__dataframe_handler_unsorted`: empty result iterable returns an empty DataFrame (no columns set).
- `__dataframe_handler`: the first row is consumed by `next()` for schema detection but is NOT included in output records. This means the first row is lost if `result` is a non-rewindable iterator.
- `risk_handler`: when `result.get('children')` is truthy but `result.get('val')` is falsy, the parent row is omitted from the DataFrame.
- `risk_by_class_handler`: if no CROSSES entry exists (`crosses_idx is None`), SPIKE/JUMP values are skipped but not accumulated anywhere.
- `risk_vector_handler`: single-element equity vector returns `FloatWithInfo` instead of `DataFrameWithInfo`.
- `simple_valtable_handler`: will raise `KeyError` if `raw_res` is empty (accesses `raw_res[0]`).
- `map_coordinate_to_column`: the `point` key is written without the tag prefix, potentially overwriting the tagged `tag_point` key.
- `number_and_unit_handler`: missing `value` key defaults to `float('nan')`.
- `error_handler`: missing `errorString` key defaults to `'Unknown error'`.

## Bugs Found
- Line 306-307 (`map_coordinate_to_column`): `updated_struct['point'] = point` writes an un-prefixed `point` key, but the struct was built with `tag + "_" + k` prefixed keys. The `point` key should likely be `tag + "_point"`. However, this may be intentional if the downstream `mappings` reference `inner_point` / `outer_point` which map to `inner_point` / `outer_point` from step 1. The net effect is both `inner_point` (or `outer_point`) and a bare `point` key exist in the dict. (OPEN - likely benign since `__dataframe_handler` only reads keys that appear in mappings)
- Line 56-58 (`__dataframe_handler`): The first row from the iterable is consumed by `next()` but never re-inserted into the records. If `result` is a single-pass iterator (e.g., generator), the first row is lost. This is intentional for schema detection but callers must pass a re-iterable or a pre-consumed iterator. (OPEN - design quirk)

## Coverage Notes
- Branch count: ~35
- Key branches: empty result (2 handlers), children presence, SPIKE/JUMP filtering, PnlExplain vs sorted, single-row equity vector, single-row 2nd-order risk, message presence, request_id presence
- Pragmas: none observed
