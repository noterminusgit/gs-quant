# results.py

## Summary
Core risk results module providing future-based asynchronous pricing result containers and portfolio-level result aggregation. Defines `PricingFuture` (extends `concurrent.futures.Future`), `CompositeResultFuture` (aggregates multiple futures), `PortfolioRiskResult` (hierarchical portfolio result traversal with multi-dimensional slicing by risk measure, date, scenario, and instrument), and supporting dict-based result containers (`MultipleRiskMeasureResult`, `MultipleScenarioResult`). All result types support arithmetic operations, DataFrame pivoting via `to_frame()`, and composition across time series. In Elixir, the `Future`-based patterns map to `Task` / `Task.async` / `Task.await`, and the callback-driven `CompositeResultFuture` maps to `Task.yield_many` or a GenServer collecting results.

## Dependencies
- Internal:
  - `gs_quant.base` (`Priceable`, `RiskKey`, `Sentinel`, `InstrumentBase`, `is_instance_or_iterable`, `is_iterable`, `Scenario`)
  - `gs_quant.common` (`RiskMeasure`)
  - `gs_quant.config` (`DisplayOptions`)
  - `gs_quant.risk` (`DataFrameWithInfo`, `ErrorValue`, `UnsupportedValue`, `FloatWithInfo`, `SeriesWithInfo`, `ResultInfo`, `ScalarWithInfo`, `aggregate_results`)
  - `gs_quant.risk.transform` (`Transformer`)
  - `gs_quant.markets` (lazy imports: `CloseMarket`, `PricingContext`, `historical_risk_key`, `Portfolio`)
- External:
  - `copy` (copy, deepcopy of result objects)
  - `datetime` (`dt.date`)
  - `logging` (module-level logger)
  - `operator` (`op.mul`, `op.add`)
  - `weakref` (weak reference to pricing context)
  - `concurrent.futures` (`Future`)
  - `itertools` (`chain`)
  - `typing` (`Any`, `Iterable`, `Mapping`, `Optional`, `Tuple`, `Union`)
  - `pandas` (`pd.DataFrame`, `pd.Series`, `pd.DataFrame.from_records`, `pivot_table`)
  - `more_itertools` (`unique_everseen`)

## Type Definitions

### PricingFuture (class)
Inherits: `concurrent.futures.Future`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__RESULT_SENTINEL` | `Sentinel` | `Sentinel('PricingFuture')` | Class-level sentinel to distinguish "no result provided" from `None` |
| `__pricing_context` | `Optional[weakref.ref]` | `None` | Weak reference to the active `PricingContext` at creation time; set only when no immediate result is provided |

**Elixir mapping:** A `Task` struct wrapping `Task.async/1`. The sentinel pattern becomes a tagged tuple `{:pending, task}` vs `{:resolved, value}`. The weak reference to pricing context becomes a process registry lookup or caller PID tracking.

---

### CompositeResultFuture (class)
Inherits: `PricingFuture`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__futures` | `Tuple[PricingFuture, ...]` | required | Immutable tuple of child futures |
| `__pending` | `Set[PricingFuture]` | `set()` | Tracks which child futures are still incomplete; when empty, triggers `_set_result()` |

**Elixir mapping:** A GenServer or Agent that holds a list of `Task` refs and uses `Task.yield_many/2` or individual `Task.await/2` calls. The done-callback pattern maps to monitoring tasks with `Process.monitor/1` and handling `:DOWN` messages, or using `Task.async_stream/3`.

---

### MultipleRiskMeasureResult (class)
Inherits: `dict`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__instrument` | `InstrumentBase` | required | The instrument these results belong to |
| (dict keys) | `RiskMeasure` | -- | Keys are risk measures |
| (dict values) | `ResultInfo \| MultipleScenarioResult` | -- | Values are result data per risk measure |

**Elixir mapping:** A struct `%MultipleRiskMeasureResult{instrument: instrument, results: %{risk_measure => value}}` using a plain map for the results storage, with protocol implementations for `Access` and `Enumerable`.

---

### MultipleRiskMeasureFuture (class)
Inherits: `CompositeResultFuture`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__measures_to_futures` | `Mapping[RiskMeasure, PricingFuture]` | required | Map from risk measure to its future |
| `__instrument` | `InstrumentBase` | required | The instrument |

---

### MultipleScenarioFuture (class)
Inherits: `CompositeResultFuture`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__instrument` | `InstrumentBase` | required | The instrument |
| `__scenarios` | `Iterable[Scenario]` | required | The scenario keys corresponding to futures |

---

### MultipleScenarioResult (class)
Inherits: `dict`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__instrument` | `InstrumentBase` | required | The instrument these results belong to |
| (dict keys) | `Scenario` | -- | Keys are scenario objects |
| (dict values) | `ResultInfo` | -- | Values are result data per scenario |

---

### HistoricalPricingFuture (class)
Inherits: `CompositeResultFuture`

No additional fields. Overrides `_set_result()` to compose time-series results.

---

### PortfolioPath (class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__path` | `Tuple[int, ...]` | required | Tuple of integer indices representing a path through nested portfolio hierarchy |

**Elixir mapping:** A simple struct `%PortfolioPath{path: [integer()]}` implementing `Access`-like traversal.

---

### PortfolioRiskResult (class)
Inherits: `CompositeResultFuture`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__portfolio` | `Portfolio` | required | The portfolio whose results this represents |
| `__risk_measures` | `Tuple[RiskMeasure, ...]` | required | The risk measures computed |

**Elixir mapping:** A struct containing the portfolio, risk measures, and a list of resolved/pending tasks. Slicing operations become pattern-matched function heads.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger for error reporting |
| `PricingFuture.__RESULT_SENTINEL` | `Sentinel` | `Sentinel('PricingFuture')` | Class-level sentinel distinguishing "no argument" from `None` |

## Functions/Methods

### get_default_pivots(cls: str, has_dates: bool, multi_measures: bool, multi_scen: bool, simple_port: bool = None, ori_cols = None) -> Tuple[str, Union[str, Tuple, List], Optional[str]]
Purpose: Determine default pivot table parameters (values, index, columns) based on the result class type and data characteristics.

**Algorithm:**
1. Branch: `cls == 'MultipleScenarioResult'` -> return `('value', 'scenario', 'dates' if has_dates else None)`
2. Branch: `cls == 'MultipleRiskMeasureResult'` -> return `('value', ('risk_measure', 'scenario') if multi_scen else 'risk_measure', 'dates' if has_dates else None)`
3. Branch: `cls == 'PortfolioRiskResult'`:
   - Branch: `ori_cols is None` -> raise `ValueError('columns of dataframe required to get default pivots')`
   - Extract `portfolio_names` from `ori_cols` (columns matching `'portfolio_name_'` prefix)
   - Build `port_and_inst_names = portfolio_names + ['instrument_name']`
   - Define 8 pivot rules as a list of `[has_dates, multi_measures, simple_port, multi_scen, output_tuple]`
   - Define inner `match(rule_value, check_value) -> bool`:
     - Branch: `rule_value is None` -> return `True` (wildcard)
     - Branch: `callable(rule_value)` -> return `rule_value(check_value)`
     - Else: return `rule_value == check_value`
   - Iterate rules; first rule where all 4 conditions match returns its output
   - Branch: no rule matches -> return `(None, None, None)`
4. Implicit branch: `cls` is none of the above -> falls through to PortfolioRiskResult branch or returns `(None, None, None)` if no cls matches (only reachable for PortfolioRiskResult since there's no final else)

**Pivot rules table (PortfolioRiskResult):**

| # | has_dates | multi_measures | simple_port | multi_scen | Output (values, index, columns) |
|---|-----------|---------------|-------------|------------|-------------------------------|
| 1 | True | True | None(any) | False | `('value', 'dates', port_and_inst + ['risk_measure'])` |
| 2 | True | False | None(any) | False | `('value', 'dates', port_and_inst)` |
| 3 | False | False | False | False | `('value', portfolio_names, 'instrument_name')` |
| 4 | False | None(any) | None(any) | False | `('value', port_and_inst, 'risk_measure')` |
| 5 | True | True | None(any) | True | `('value', 'dates', port_and_inst + ['risk_measure', 'scenario'])` |
| 6 | True | False | None(any) | True | `('value', 'dates', port_and_inst + ['scenario'])` |
| 7 | False | True | None(any) | True | `('value', port_and_inst, ['risk_measure', 'scenario'])` |
| 8 | False | False | None(any) | True | `('value', port_and_inst, 'scenario')` |

**Raises:** `ValueError` when `cls == 'PortfolioRiskResult'` and `ori_cols is None`

---

### pivot_to_frame(df: pd.DataFrame, values, index, columns, aggfunc) -> pd.DataFrame
Purpose: Create a pivot table from a DataFrame, preserving original row/column ordering where possible.

**Algorithm:**
1. Try `df.pivot_table(values=values, index=index, columns=columns, aggfunc=aggfunc)`
2. Branch: `ValueError` raised -> raise `RuntimeError('Unable to successfully pivot data')`
3. Try to reorder index/columns to match original DataFrame order:
   - Branch: `index is not None` -> reindex rows using unique index from original df
   - Branch: `columns is not None` -> reindex columns using unique index from original df
   - Return reindexed pivot_df
4. Branch: `KeyError` during reindexing -> return pivot_df as-is (order not preserved)

**Raises:** `RuntimeError` when pivot_table raises `ValueError`

---

### _compose(lhs: ResultInfo, rhs: ResultInfo) -> ResultInfo
Purpose: Compose (merge/combine) two result objects, typically for combining historical results across dates.

**Algorithm:**
1. Branch: `lhs` is `ScalarWithInfo`:
   - Branch: `rhs` is `ScalarWithInfo` -> return `rhs` if same date, else `lhs.compose((lhs, rhs))`
   - Branch: `rhs` is `SeriesWithInfo` -> compose lhs to series, combine_first with rhs, sort_index
2. Branch: `lhs` is `SeriesWithInfo`:
   - Branch: `rhs` is `SeriesWithInfo` -> `rhs.combine_first(lhs).sort_index()`
   - Branch: `rhs` is `ScalarWithInfo` -> compose rhs to series, combine_first with lhs, sort_index
3. Branch: `lhs` is `DataFrameWithInfo`:
   - If `lhs.index.name != 'date'` -> assign date column from risk_key and set as index
   - Branch: `rhs` is `DataFrameWithInfo`:
     - If `rhs.index.name != 'date'` -> assign date column from risk_key and set as index
     - Return lhs rows not in rhs appended with rhs, sorted by index
4. Branch: `lhs` is `MultipleRiskMeasureResult`:
   - Branch: `rhs` is `MultipleRiskMeasureResult` -> return `lhs + rhs`
5. If no branch matched -> raise `RuntimeError(f'{lhs} and {rhs} cannot be composed')`

**Raises:** `RuntimeError` when types are incompatible for composition

---

### _value_for_date(result: Union[DataFrameWithInfo, SeriesWithInfo], date: Union[Iterable, dt.date]) -> Union[DataFrameWithInfo, ErrorValue, FloatWithInfo, SeriesWithInfo]
Purpose: Extract the value(s) for a specific date or dates from a time-indexed result.

**Algorithm:**
1. Lazy import `CloseMarket`
2. Branch: `result.empty` -> return result as-is
3. Extract raw_value:
   - Branch: result is `DataFrameWithInfo` AND date is `dt.date` -> use `result.loc[[date]]` (preserves DataFrame type)
   - Else -> use `result.loc[date]`
4. Build new `RiskKey` with the date and a `CloseMarket` constructed from the date and location
   - Branch: `date` is `dt.date` -> use date directly in RiskKey
   - Branch: `date` is iterable -> use `tuple(date)` in RiskKey
   - Branch: original market is `CloseMarket` -> use its location; else `None`
5. Extract unit and error from result
6. Branch: `raw_value` is `DataFrameWithInfo`:
   - Set index to 'dates', then:
     - Branch: `date` is `dt.date` -> reset_index(drop=True)
     - Else -> keep 'dates' index
7. Branch: `raw_value` is `float`:
   - Branch: `unit` is truthy -> `unit = result.unit.get(date, result.unit)`
   - Branch: `unit` is falsy -> `unit = None`
8. Return `_get_value_with_info(raw_value, risk_key, unit, error)`

---

### _get_value_with_info(value, risk_key, unit, error) -> Union[ErrorValue, UnsupportedValue, DataFrameWithInfo, SeriesWithInfo, FloatWithInfo]
Purpose: Wrap a raw value with the appropriate typed result info container.

**Algorithm:**
1. Branch: `value` is `(ErrorValue, UnsupportedValue)` -> return value unchanged
2. Branch: `value` is `pd.DataFrame` -> return `DataFrameWithInfo(value, risk_key, unit, error)`
3. Branch: `value` is `pd.Series` -> return `SeriesWithInfo(value.raw_value, risk_key, unit, error)`
4. Else -> return `FloatWithInfo(risk_key, value, unit, error)`

---

### _risk_keys_compatible(lhs, rhs) -> bool
Purpose: Check if two results have compatible risk keys (same scenario/location), unwrapping `MultipleRiskMeasureResult` wrappers.

**Algorithm:**
1. Lazy import `historical_risk_key`
2. While `lhs` is `MultipleRiskMeasureResult` -> unwrap to first value
3. While `rhs` is `MultipleRiskMeasureResult` -> unwrap to first value
4. Return `historical_risk_key(lhs.risk_key).ex_measure == historical_risk_key(rhs.risk_key).ex_measure`

---

### _value_for_measure_or_scen(res: dict, item: Union[Iterable, RiskMeasure, Scenario]) -> dict
Purpose: Filter a dict of results to keep only entries matching the given measure(s) or scenario(s).

**Algorithm:**
1. `result = copy.copy(res)` (shallow copy)
2. Branch: `item` is `Iterable`:
   - Delete all keys not in `item`
3. Else (single item):
   - Delete all keys not equal to `item`
4. Return filtered result

---

### PricingFuture.__init__(self, result: Optional[Any] = __RESULT_SENTINEL) -> None
Purpose: Initialize a pricing future, optionally with an immediate result.

**Algorithm:**
1. Call `super().__init__()` (Future.__init__)
2. Set `self.__pricing_context = None`
3. Branch: `result is not self.__RESULT_SENTINEL` -> call `self.set_result(result)` (immediately resolved)
4. Branch: `result is self.__RESULT_SENTINEL` (no result provided):
   - Lazy import `PricingContext`
   - Store `weakref.ref(PricingContext.current.active_context)` in `self.__pricing_context`

**Elixir mapping:** `{:resolved, value}` tuple for immediate results, `Task.async(fn)` for deferred. Context tracking via process dictionary or explicit parameter.

---

### PricingFuture.__add__(self, other) -> PricingFuture
Purpose: Add two PricingFutures or a PricingFuture and a number.

**Algorithm:**
1. Branch: `other` is `(int, float)` -> `operand = other`
2. Branch: `other` is same class -> `operand = other.result()`
3. Else -> raise `ValueError` (note: bug on line 226 uses `other.__class__.name` instead of `other.__class__.__name__`)
4. Return `PricingFuture(_compose(self.result(), operand))`

**Raises:** `ValueError` when other is incompatible type

---

### PricingFuture.__mul__(self, other) -> PricingFuture
Purpose: Multiply a PricingFuture's result by a scalar.

**Algorithm:**
1. Branch: `other` is `(int, float)` -> return `PricingFuture(self.result() * other)`
2. Else -> raise `ValueError('Can only multiply by an int or float')`

**Raises:** `ValueError` when other is not int or float

---

### PricingFuture.result(self, timeout=None) -> Any
Purpose: Return the result, with a guard against evaluating within the producing pricing context.

**Algorithm:**
1. Branch: `not self.done()`:
   - Resolve weak ref: `pricing_context = self.__pricing_context() if self.__pricing_context else None`
   - Branch: `pricing_context is not None and pricing_context.is_entered` -> raise `RuntimeError('Cannot evaluate results under the same pricing context being used to produce them')`
2. Return `super().result(timeout=timeout)`

**Raises:**
- `RuntimeError` when attempting to evaluate inside the producing pricing context
- `CancelledError` if future was cancelled (from parent)
- `TimeoutError` if timeout exceeded (from parent)

**Elixir mapping:** `Task.await/2` with timeout. The context guard becomes a check against `self()` or a process flag.

---

### CompositeResultFuture.__init__(self, futures: Iterable[PricingFuture]) -> None
Purpose: Initialize with child futures, registering callbacks on pending ones.

**Algorithm:**
1. Call `super().__init__()` (PricingFuture.__init__ with no result -> sets up pricing context ref)
2. `self.__futures = tuple(futures)`
3. `self.__pending = set()`
4. For each future in `self.__futures`:
   - Branch: `not future.done()` -> add callback `self.__cb`, add to `__pending`
5. Branch: `not self.__pending` (all already done) -> call `self._set_result()`

**Elixir mapping:** Spawn a collector process. For each child Task, if already completed, collect immediately; else monitor and collect on `:DOWN`. When all collected, compute and store aggregate result.

---

### CompositeResultFuture.__getitem__(self, item) -> Any
Purpose: Index into the resolved composite result.

**Algorithm:**
1. Return `self.result()[item]`

---

### CompositeResultFuture.__cb(self, future: PricingFuture) -> None
Purpose: Callback invoked when a child future completes.

**Algorithm:**
1. `self.__pending.discard(future)`
2. Branch: `not self.__pending` -> call `self._set_result()`

---

### CompositeResultFuture._set_result(self) -> None
Purpose: Collect all child results into a list and set as this future's result.

**Algorithm:**
1. `self.set_result([f.result() for f in self.__futures])`

---

### CompositeResultFuture.futures (property) -> Tuple[PricingFuture, ...]
Purpose: Return the tuple of child futures.

---

### MultipleRiskMeasureResult.__init__(self, instrument, dict_values: Iterable) -> None
Purpose: Initialize with an instrument and key-value pairs of risk measure -> result.

**Algorithm:**
1. Call `super().__init__(dict_values)` (dict.__init__)
2. Store `self.__instrument = instrument`

---

### MultipleRiskMeasureResult.__getitem__(self, item) -> Union[MultipleRiskMeasureResult, Any]
Purpose: Multi-dimensional indexing by date, scenario, or risk measure key.

**Algorithm:**
1. Branch: `item` is instance or iterable of `dt.date`:
   - Branch: all values are `(DataFrameWithInfo, SeriesWithInfo)` -> return new `MultipleRiskMeasureResult` with `_value_for_date` applied to each value
   - Branch: all values are `MultipleScenarioResult` -> return new `MultipleRiskMeasureResult` with `v[item]` for each value
   - Else -> raise `ValueError('Can only index by date on historical results')`
2. Branch: `item` is instance or iterable of `Scenario`:
   - Branch: all values are `MultipleScenarioResult` -> return new `MultipleRiskMeasureResult` with `_value_for_measure_or_scen` applied
   - Else -> raise `ValueError('Can only index by scenario on multiple scenario results')`
3. Else -> `super().__getitem__(item)` (standard dict lookup by risk measure key)

**Raises:** `ValueError` for incompatible indexing

---

### MultipleRiskMeasureResult.__mul__(self, other) -> Union[MultipleRiskMeasureResult, ValueError]
Purpose: Multiply all results by a scalar.

**Algorithm:**
1. Branch: `other` is `(int, float)` -> return `self.__op(op.mul, other)`
2. Else -> return `ValueError(...)` (note: this is a bug -- should be `raise ValueError(...)`, it returns the exception object instead)

---

### MultipleRiskMeasureResult.__add__(self, other) -> Union[MultipleRiskMeasureResult, PortfolioRiskResult]
Purpose: Add results together -- combines by risk measure, date, or instrument.

**Algorithm:**
1. Branch: `other` is `(int, float)` -> return `self.__op(op.add, other)`
2. Branch: `other` is `MultipleRiskMeasureResult`:
   - Check `_risk_keys_compatible(self, other)` -> if not compatible, raise `ValueError`
   - Check `instruments_equal = self.__instrument == other.__instrument`
   - Compute `self_dt` and `other_dt` (single-element list if no dates, else `.dates`)
   - Compute `dates_overlap`
   - Branch: risk measures overlap AND instruments equal AND dates overlap -> raise `ValueError('Results overlap...')`
   - Compute `all_keys = set(chain(self.keys(), other.keys()))`
   - Branch: `not instruments_equal`:
     - Lazy import `Portfolio`
     - Return `PortfolioRiskResult` wrapping both instruments with `MultipleRiskMeasureFuture` per instrument
   - Branch: instruments equal:
     - Merge results using `_compose` for overlapping keys
     - Return new `MultipleRiskMeasureResult`
3. Else -> raise `ValueError('Can only add instances of MultipleRiskMeasureResult or int, float')`

**Raises:** `ValueError` for incompatible additions or overlapping results

---

### MultipleRiskMeasureResult.__op(self, operator, operand) -> MultipleRiskMeasureResult
Purpose: Apply an arithmetic operator to all values with a scalar operand.

**Algorithm:**
1. For each `(key, value)` in self:
   - Branch: `value` is `SeriesWithInfo` or `DataFrameWithInfo`:
     - `new_value = value.copy_with_resultinfo()`
     - Branch: `not value.empty` -> `new_value.value = operator(value.value, operand)`
   - Branch: `value` is `pd.DataFrame` or `pd.Series`:
     - `new_value = value.copy()`
     - `new_value.value = operator(value.value, operand)`
   - Else -> `new_value = operator(value, operand)`
2. Return `MultipleRiskMeasureResult(self.__instrument, values)`

---

### MultipleRiskMeasureResult.instrument (property) -> InstrumentBase
Purpose: Return the instrument.

---

### MultipleRiskMeasureResult.dates (property) -> Tuple[dt.date, ...]
Purpose: Collect all unique dates from time-indexed result values.

**Algorithm:**
1. Initialize empty set `dates`
2. For each value in self:
   - Branch: value is `(DataFrameWithInfo, SeriesWithInfo)`:
     - Branch: all index elements are `dt.date` -> update dates set
3. Return `tuple(sorted(dates))`

---

### MultipleRiskMeasureResult._multi_scen_key (property) -> Iterable[Scenario]
Purpose: Extract scenario keys from the first `MultipleScenarioResult` value found.

**Algorithm:**
1. For each value in self:
   - Branch: value is `MultipleScenarioResult` -> return `tuple(value.scenarios)`
2. Return `tuple()` (empty)

---

### MultipleRiskMeasureResult.to_frame(self, values='default', index='default', columns='default', aggfunc="sum", display_options: DisplayOptions = None) -> pd.DataFrame
Purpose: Convert results to a pivoted DataFrame.

**Algorithm:**
1. Build DataFrame from `self._to_records({}, display_options)`
2. Branch: `values is None and index is None and columns is None` -> return raw df
3. Branch: all are `'default'`:
   - Branch: `'mkt_type' in df.columns` -> return `df.set_index('risk_measure')` (bucketed risk)
   - Else -> call `get_default_pivots('MultipleRiskMeasureResult', ...)` to get pivot params
4. Else (custom pivoting):
   - Normalize `values` to `'value'` if default or `['value']`
   - Normalize `index`/`columns` to `None` if default
5. Return `pivot_to_frame(df, values, index, columns, aggfunc)`

---

### MultipleRiskMeasureResult._to_records(self, extra_dict, display_options: DisplayOptions = None) -> list
Purpose: Flatten results into a list of record dicts, adding `'risk_measure'` key.

**Algorithm:**
1. For each risk measure `rm` in self:
   - Get `self[rm]._to_records(extra_dict, display_options)`
   - Add `{'risk_measure': rm}` to each record dict
2. Return flattened list via `chain.from_iterable`

---

### MultipleRiskMeasureFuture.__init__(self, instrument: InstrumentBase, measures_to_futures: Mapping[RiskMeasure, PricingFuture]) -> None
Purpose: Initialize with instrument and risk-measure-to-future mapping.

**Algorithm:**
1. Store `__measures_to_futures` and `__instrument`
2. Call `super().__init__(measures_to_futures.values())`

---

### MultipleRiskMeasureFuture.__add__(self, other) -> MultipleRiskMeasureFuture
Purpose: Add two MultipleRiskMeasureFutures.

**Algorithm:**
1. Branch: `other` is `MultipleRiskMeasureFuture` -> `result = self.result() + other.result()`
2. Else -> `result = ... + other` (delegates to MultipleRiskMeasureResult.__add__)
3. Return new `MultipleRiskMeasureFuture` wrapping PricingFutures for each key-value pair

---

### MultipleRiskMeasureFuture._set_result(self) -> None
Purpose: Zip measure keys with resolved future results into a `MultipleRiskMeasureResult`.

**Algorithm:**
1. `self.set_result(MultipleRiskMeasureResult(self.__instrument, zip(keys, (f.result() for f in self.futures))))`

---

### MultipleRiskMeasureFuture.measures_to_futures (property) -> Mapping[RiskMeasure, PricingFuture]
Purpose: Return the measures-to-futures mapping.

---

### MultipleScenarioFuture.__init__(self, instrument: InstrumentBase, scenarios: Iterable[Scenario], futures: Iterable[PricingFuture]) -> None
Purpose: Initialize with instrument, scenarios, and corresponding futures.

**Algorithm:**
1. Store `__instrument` and `__scenarios`
2. Call `super().__init__(futures)`

---

### MultipleScenarioFuture._set_result(self) -> None
Purpose: Parse scenario results from the first future's DataFrame result.

**Algorithm:**
1. `res = next(iter(self.futures)).result()`
2. Branch: `res.index.name == 'date'` (historical scenario):
   - Extract unique labels via `unique_everseen(res['label'].values)`
   - For each label, filter rows where `res['label'] == label`, take `'value'` column
3. Else (non-historical):
   - Extract `tuple(v for v in res['value'])`
4. Wrap each value with `_get_value_with_info(v, res.risk_key, res.unit, res.error)`
5. `self.set_result(MultipleScenarioResult(self.__instrument, {k: v for k, v in zip(self.__scenarios, val_w_info)}))`

---

### MultipleScenarioResult.__init__(self, instrument, dict_values: Iterable) -> None
Purpose: Initialize with instrument and scenario-to-result pairs.

---

### MultipleScenarioResult.__getitem__(self, item) -> Union[MultipleScenarioResult, Any]
Purpose: Index by date or scenario key.

**Algorithm:**
1. Branch: `item` is instance or iterable of `dt.date`:
   - Branch: all values are `(DataFrameWithInfo, SeriesWithInfo)` -> return new `MultipleScenarioResult` with `_value_for_date` applied
   - Else -> raise `ValueError('Can only index by date on historical results')`
2. Else -> `super().__getitem__(item)` (dict key lookup by Scenario)

**Raises:** `ValueError` for non-historical date indexing

---

### MultipleScenarioResult.to_frame(self, values='default', index='default', columns='default', aggfunc="sum", display_options: DisplayOptions = None) -> pd.DataFrame
Purpose: Convert scenario results to a pivoted DataFrame.

**Algorithm:**
1. Build DataFrame from `self._to_records({}, display_options)`
2. Branch: all None -> return raw df
3. Branch: all default:
   - Branch: `'mkt_type' in df.columns` -> `df.set_index('scenario')`
   - Else -> get default pivots for `'MultipleScenarioResult'`
4. Else -> normalize user params
5. Return `pivot_to_frame(...)`

---

### MultipleScenarioResult.instrument (property) -> InstrumentBase

---

### MultipleScenarioResult.scenarios (property) -> KeysView
Purpose: Return the dict keys (scenario objects).

---

### MultipleScenarioResult._to_records(self, extra_dict, display_options: DisplayOptions = None) -> list
Purpose: Flatten results adding `'scenario'` key to each record.

---

### HistoricalPricingFuture._set_result(self) -> None
Purpose: Compose historical results across dates, handling errors.

**Algorithm:**
1. `results = [f.result() for f in self.futures]`
2. Find `base` = first result that is not `(ErrorValue, Exception)`
3. Branch: `base is None`:
   - Log error: `_logger.error(f'Historical pricing failed: {results[0]}')`
   - `self.set_result(results[0])`
4. Branch: `base` is `MultipleRiskMeasureResult`:
   - For each key in base, compose all results for that key: `base[k].compose(r[k] for r in results)`
   - Wrap in new `MultipleRiskMeasureResult`
5. Else -> `result = base.compose(results)`
6. `self.set_result(result)`

---

### PortfolioPath.__init__(self, path) -> None
Purpose: Initialize with an integer or tuple path.

**Algorithm:**
1. Branch: `path` is `int` -> `self.__path = (path,)`
2. Else -> `self.__path = path`

---

### PortfolioPath.__repr__(self) -> str
Purpose: Return `repr(self.__path)`.

---

### PortfolioPath.__iter__(self) -> Iterator[int]
Purpose: Iterate over path elements.

---

### PortfolioPath.__len__(self) -> int
Purpose: Return number of path elements.

---

### PortfolioPath.__add__(self, other) -> PortfolioPath
Purpose: Concatenate two paths.

**Algorithm:**
1. Return `PortfolioPath(self.__path + other.__path)`

---

### PortfolioPath.__eq__(self, other) -> bool
Purpose: Equality by path tuple.

---

### PortfolioPath.__hash__(self) -> int
Purpose: Hash by path tuple.

---

### PortfolioPath.__call__(self, target, rename_to_parent: Optional[bool] = False) -> Any
Purpose: Traverse a nested future/result structure using the path indices.

**Algorithm:**
1. `parent = None`, `path = list(self.__path)`
2. While `path` is non-empty:
   - `elem = path.pop(0)`
   - `parent = target` if depth > 1 (i.e., `len(self) - len(path) > 1`), else `None`
   - Branch: `target` is `CompositeResultFuture` -> `target = target.futures[elem]`
   - Else -> `target = target[elem]`
   - Branch: `target` is `PricingFuture` AND `path` still has elements -> `target = target.result()`
3. Branch: `rename_to_parent` AND `parent` AND `parent` has `name` attribute AND target is not `InstrumentBase`:
   - `target = copy.copy(target)`
   - `target.name = parent.name`
4. Return `target`

**Elixir mapping:** Recursive list traversal with pattern matching on the structure type at each level.

---

### PortfolioPath.path (property) -> Tuple[int, ...]
Purpose: Return the raw path tuple.

---

### PortfolioRiskResult.__init__(self, portfolio, risk_measures: Iterable[RiskMeasure], futures: Iterable[PricingFuture]) -> None
Purpose: Initialize with portfolio, risk measures, and child futures.

**Algorithm:**
1. Call `super().__init__(futures)` (CompositeResultFuture.__init__)
2. Store `self.__portfolio = portfolio`
3. Store `self.__risk_measures = tuple(risk_measures)`

---

### PortfolioRiskResult.__getitem__(self, item) -> Union[PortfolioRiskResult, MultipleRiskMeasureResult, Any]
Purpose: Multi-dimensional slicing by risk measure, scenario, date, instrument, or index.

**Algorithm:**
1. Branch: `item` is instance or iterable of `RiskMeasure`:
   - Validate item is in `self.risk_measures`; raise `ValueError` if not
   - Branch: `len(self.risk_measures) == 1` -> return `self`
   - Else: iterate portfolio, for each entry:
     - Branch: result is `PortfolioRiskResult` -> recurse `result[item]`
     - Else -> create `MultipleRiskMeasureFuture` with filtered measures
   - Return new `PortfolioRiskResult` with filtered measures and futures

2. Branch: `item` is instance or iterable of `Scenario`:
   - Validate item is in `self._multi_scen_key`; raise `ValueError` if not
   - Branch: `len(self._multi_scen_key) == 0` -> return `self`
   - Else: iterate portfolio, for each entry:
     - Branch: result is `PortfolioRiskResult` -> recurse
     - Branch: result is `MultipleRiskMeasureResult` -> filter each measure's scenarios
     - Branch: result is `MultipleScenarioResult` -> filter scenarios directly
   - Return new `PortfolioRiskResult`

3. Branch: `item` is instance or iterable of `dt.date`:
   - For each portfolio entry:
     - Branch: result is `(MultipleRiskMeasureResult, PortfolioRiskResult, MultipleScenarioResult)` -> `result[item]`
     - Branch: result is `(DataFrameWithInfo, SeriesWithInfo)` -> `_value_for_date(result, item)`
     - Else -> raise `RuntimeError('Can only index by date on historical results')`
   - Return new `PortfolioRiskResult`

4. Branch: `item` is iterable of `InstrumentBase` -> `self.subset(item)`

5. Branch: `item` is list with `len == 1` -> `self.__results(items=item[0])` (unwrap single-element list)

6. Else -> `self.__results(items=item)` (general lookup by int/slice/str/Priceable)

**Raises:**
- `ValueError` when risk measure or scenario not computed
- `RuntimeError` when date-indexing non-historical results

---

### PortfolioRiskResult.__contains__(self, item) -> bool
Purpose: Check membership by risk measure, date, or instrument.

**Algorithm:**
1. Branch: `item` is `RiskMeasure` -> `item in self.__risk_measures`
2. Branch: `item` is `dt.date` -> `item in self.dates`
3. Else -> `item in self.__portfolio`

---

### PortfolioRiskResult.__repr__(self) -> str
Purpose: String representation showing risk measures, portfolio name, and count.

**Algorithm:**
1. `ret = f'{self.__risk_measures} Results'`
2. Branch: `self.__portfolio.name` is truthy -> append `f' for {self.__portfolio.name}'`
3. Append `f' ({len(self)})'`

---

### PortfolioRiskResult.__len__(self) -> int
Purpose: Return number of child futures.

---

### PortfolioRiskResult.__iter__(self) -> Iterator
Purpose: Iterate over resolved results for all portfolio paths.

---

### PortfolioRiskResult.__mul__(self, other) -> Union[PortfolioRiskResult, ValueError]
Purpose: Multiply all futures by a scalar.

**Algorithm:**
1. Branch: `other` is `(int, float)` -> return `PortfolioRiskResult(portfolio, measures, [f * other for f in self.futures])`
2. Else -> return `ValueError(...)` (bug: should be `raise`)

---

### PortfolioRiskResult.__add__(self, other) -> PortfolioRiskResult
Purpose: Add two PortfolioRiskResults or add a scalar.

**Algorithm:**
1. Define inner `as_multiple_result_futures(portfolio_result)`:
   - Branch: `len(risk_measures) > 1` -> return as-is
   - Else: wrap each future into `MultipleRiskMeasureFuture` with the single risk measure
   - Handles nested `PortfolioRiskResult` via recursion

2. Define inner `set_value(dest_result, src_result, src_risk_measure)`:
   - For each (priceable, future) in dest portfolio:
     - Branch: future is `PortfolioRiskResult` -> recurse
     - Else: try to get `src_result[priceable]`, unwrap if `MultipleRiskMeasureResult`, set in dest future's result dict
     - Branch: `KeyError` -> pass (instrument not in source)

3. Define inner `first_value(portfolio_result)`:
   - Branch: `len(risk_measures) > 1` -> get first instrument's first measure value
   - Else -> get first instrument's value directly

4. Branch: `other` is `(int, float)` -> return `PortfolioRiskResult(portfolio, measures, [f + other for f in futures])`
5. Branch: `other` is `PortfolioRiskResult`:
   - Validate risk key compatibility AND overlapping instruments -> raise `ValueError` if incompatible
   - Validate no overlap on (risk_measures AND dates AND instruments) -> raise `ValueError`
   - Convert both to multiple-result-futures form
   - Branch: same portfolio -> zip-add futures
   - Branch: different portfolios -> concatenate portfolios and futures
   - Build result `PortfolioRiskResult`
   - Branch: different portfolios AND multiple risk measures -> fill overlapping values via `set_value`
   - Return result
6. Else -> raise `ValueError('Can only add instances of PortfolioRiskResult or int, float')`

**Raises:** `ValueError` for incompatible or overlapping results

---

### PortfolioRiskResult.portfolio (property) -> Portfolio

---

### PortfolioRiskResult.risk_measures (property) -> Tuple[RiskMeasure, ...]

---

### PortfolioRiskResult.dates (property) -> Tuple[dt.date, ...]
Purpose: Collect all unique dates across all portfolio results.

**Algorithm:**
1. For each result in `self.__results()`:
   - Branch: result is `(MultipleRiskMeasureResult, PortfolioRiskResult)` -> update from `result.dates`
   - Branch: result is `(pd.DataFrame, pd.Series)` -> update from `result.index`
   - (Only if all index elements are `dt.date`)
2. Try `tuple(sorted(dates))`
3. Branch: `TypeError` during sort -> return `tuple()` (empty)

---

### PortfolioRiskResult._multi_scen_key (property) -> Iterable[Scenario]
Purpose: Find scenario keys from first matching result.

**Algorithm:**
1. For each result:
   - Branch: `MultipleScenarioResult` -> return its scenarios
   - Branch: `MultipleRiskMeasureResult` or `PortfolioRiskResult` -> return `result._multi_scen_key`
2. Return `tuple()`

---

### PortfolioRiskResult.result(self, timeout: Optional[int] = None) -> PortfolioRiskResult
Purpose: Wait for resolution and return self.

**Algorithm:**
1. Call `super().result(timeout=timeout)` (triggers wait)
2. Return `self`

---

### PortfolioRiskResult.subset(self, items: Iterable[Union[int, str, PortfolioPath, Priceable]], name: Optional[str] = None) -> PortfolioRiskResult
Purpose: Create a sub-result for a subset of portfolio instruments.

**Algorithm:**
1. Convert items to `PortfolioPath` tuples, using `self.__paths(i)` for non-path items
2. `sub_portfolio = self.__portfolio.subset(paths, name=name)`
3. Return `PortfolioRiskResult(sub_portfolio, self.risk_measures, [p(self.futures) for p in paths])`

---

### PortfolioRiskResult.transform(self, risk_transformation: Transformer = None) -> Union[PortfolioRiskResult, MultipleRiskMeasureResult]
Purpose: Apply a risk transformation to the results.

**Algorithm:**
1. Branch: `risk_transformation is None` -> return `self`
2. Branch: `len(self.__risk_measures) > 1`:
   - Return `MultipleRiskMeasureResult(self.portfolio, ((r, self[r].transform(risk_transformation)) for r in measures))`
3. Branch: `len(self.__risk_measures) == 1`:
   - `flattened_results = risk_transformation.apply(self.__results())`
   - Wrap each result in a new `PricingFuture`, call `.done()` on it
   - Return new `PortfolioRiskResult`
4. Branch: `len(self.__risk_measures) == 0` -> return `self`

---

### PortfolioRiskResult.aggregate(self, allow_mismatch_risk_keys=False, allow_heterogeneous_types=False) -> Union[float, pd.DataFrame, pd.Series, MultipleRiskMeasureResult]
Purpose: Aggregate all portfolio results into a single value.

**Algorithm:**
1. Branch: `len(self.__risk_measures) > 1`:
   - Return `MultipleRiskMeasureResult(self.portfolio, ((r, self[r].aggregate()) for r in measures))`
2. Else:
   - Return `aggregate_results(self.__results(), allow_mismatch_risk_keys, allow_heterogeneous_types)`

---

### PortfolioRiskResult._to_records(self, display_options: DisplayOptions = None) -> list
Purpose: Build flat record list from futures and portfolio records.

**Algorithm:**
1. Define inner `get_records(rec)` (recursive):
   - For each future `f` in `rec.futures`:
     - Branch: `f.result()` is `(ResultInfo, MultipleRiskMeasureResult, MultipleScenarioResult)` -> append result
     - Else -> recurse into `f.result()` (nested PortfolioRiskResult)
2. Get `future_records` and `portfolio_records`
3. Branch: lengths match -> zip and extend records
4. Return records list

---

### PortfolioRiskResult.to_frame(self, values='default', index='default', columns='default', aggfunc="sum", display_options: DisplayOptions = None) -> Optional[pd.DataFrame]
Purpose: Convert portfolio risk results to a pivoted DataFrame.

**Algorithm:**
1. `final_records = self._to_records(display_options)`
2. Branch: `len(final_records) > 0`:
   - Build `ori_df` from records
   - Branch: `'risk_measure' not in ori_df.columns` -> add `risk_measures[0]` column
3. Branch: `len(final_records) == 0` -> return `None`
4. Fill N/A in non-value columns
5. Determine `has_dt`, build `other_cols` (sorted portfolio columns + instrument_name + risk_measure + optional dates)
6. Build `val_cols`, ensure `'value'` is last
7. Reorder columns: `sorted_col = other_cols + val_cols`
8. Branch: all None -> return raw df
9. Branch: all default:
   - Compute `multi_scen`, `has_bucketed`, `has_cashflows`, `multi_rm`, `port_depth_one`
   - Branch: `has_bucketed or has_cashflows` -> return `ori_df.set_index(other_cols)`
   - Else -> get default pivots for `'PortfolioRiskResult'`
10. Else (user-defined) -> normalize values
11. Return `pivot_to_frame(ori_df, values, index, columns, aggfunc)`

---

### PortfolioRiskResult.__paths(self, items: Union[int, slice, str, Priceable]) -> Tuple[PortfolioPath, ...]
Purpose: Convert various item types to PortfolioPath tuples.

**Algorithm:**
1. Branch: `items` is `int` -> return `(PortfolioPath(items),)`
2. Branch: `items` is `slice` -> return tuple of PortfolioPaths for range
3. Branch: `items` is `(str, Priceable)`:
   - `paths = self.__portfolio.paths(items)`
   - Branch: `not paths` AND `items` is `InstrumentBase` AND `items.unresolved`:
     - Try `self.__portfolio.paths(items.unresolved)`
     - Branch: still no paths -> raise `KeyError(f'{items} not in portfolio')`
     - Filter paths by matching `resolution_key.ex_measure`
     - Branch: no matching paths -> raise `KeyError(f'Cannot slice {items} which is resolved in a different pricing context')`
   - Return paths

**Raises:** `KeyError` when instrument not found or resolved in different context

---

### PortfolioRiskResult.__results(self, items: Optional[Union[int, slice, str, Priceable]] = None) -> Union[tuple, Any, PortfolioRiskResult]
Purpose: Get results for all paths or specific items.

**Algorithm:**
1. Branch: `items is None` -> return tuple of `self.__result(p)` for all portfolio paths
2. `paths = self.__paths(items)`
3. Branch: `not paths` -> raise `KeyError(f'{items}')`
4. Branch: `items` is not `slice` -> return `self.__result(paths[0])` (single result)
5. Branch: `items` is `slice` -> return `self.subset(paths)` (sub-portfolio result)

**Raises:** `KeyError` when items not found

---

### PortfolioRiskResult.__result(self, path: PortfolioPath, risk_measure: Optional[RiskMeasure] = None) -> Any
Purpose: Get a single result for a path, optionally extracting a specific risk measure.

**Algorithm:**
1. `res = path(self.futures).result()`
2. Branch: `len(self.risk_measures) == 1` AND `not risk_measure` -> `risk_measure = self.risk_measures[0]`
3. Branch: `risk_measure` AND `res` is `(MultipleRiskMeasureResult, PortfolioRiskResult)` -> return `res[risk_measure]`
4. Else -> return `res`

---

### PortfolioRiskResult.get(self, item, default) -> Any
Purpose: Safe get with default value.

**Algorithm:**
1. Try `self.__getitem__(item)`
2. Branch: `(KeyError, ValueError)` -> return `default`

## State Mutation
- `PricingFuture.__pricing_context`: Set in `__init__`; weak reference, may become `None` if context is garbage collected
- `PricingFuture` result state: Set via `set_result()` (inherited from `Future`); once set, immutable
- `CompositeResultFuture.__pending`: Modified by `__cb()` callback (discard); thread-safe concern -- `set.discard` is atomic in CPython but callback order is not guaranteed
- `CompositeResultFuture.__futures`: Set once in `__init__`; never modified
- `MultipleRiskMeasureResult.__add__` with `set_value`: Directly mutates `future.result()[src_risk_measure]` in PortfolioRiskResult.__add__ -- modifies the dict result of an already-resolved future
- `PortfolioRiskResult.__portfolio`, `__risk_measures`: Set in `__init__`; never modified
- Thread safety: `Future` provides thread-safe `set_result`/`result` via internal lock. The `__cb` callback in `CompositeResultFuture` relies on CPython's GIL for safe `set.discard` operations. The `set_value` inner function in `PortfolioRiskResult.__add__` performs post-hoc mutation on resolved futures which could be unsafe under concurrent access.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `get_default_pivots` | `cls == 'PortfolioRiskResult'` and `ori_cols is None` |
| `RuntimeError` | `pivot_to_frame` | `df.pivot_table` raises `ValueError` |
| `RuntimeError` | `_compose` | Incompatible types for composition |
| `ValueError` | `PricingFuture.__add__` | `other` is not int/float/PricingFuture |
| `ValueError` | `PricingFuture.__mul__` | `other` is not int/float |
| `RuntimeError` | `PricingFuture.result` | Evaluating result under same pricing context that produced it |
| `ValueError` | `MultipleRiskMeasureResult.__getitem__` | Date indexing on non-historical results, or scenario indexing on non-scenario results |
| `ValueError` | `MultipleRiskMeasureResult.__add__` | Incompatible risk keys, overlapping results, or unsupported operand type |
| `ValueError` | `MultipleScenarioResult.__getitem__` | Date indexing on non-historical results |
| `ValueError` | `PortfolioRiskResult.__getitem__` | Risk measure or scenario not computed |
| `RuntimeError` | `PortfolioRiskResult.__getitem__` | Date indexing on non-historical result type |
| `KeyError` | `PortfolioRiskResult.__paths` | Instrument not in portfolio or resolved in different context |
| `KeyError` | `PortfolioRiskResult.__results` | Items not found via `__paths` |
| `ValueError` | `PortfolioRiskResult.__add__` | Incompatible risk keys, overlapping results, or unsupported operand type |

## Edge Cases
- `PricingFuture.__init__` with `result=None`: The sentinel check `result is not self.__RESULT_SENTINEL` passes for `None`, so `None` is set as the result (the future resolves immediately with `None`). This is intentional -- `None` is a valid result.
- `CompositeResultFuture` with empty futures iterable: `__futures` becomes `()`, `__pending` stays empty, so `_set_result()` fires immediately with an empty list `[]`.
- `CompositeResultFuture` with all-done futures: All callbacks are skipped, `_set_result()` fires synchronously in `__init__`.
- `_compose` with `ScalarWithInfo` + `ScalarWithInfo` on same date: Returns `rhs` (later value wins).
- `_compose` with `DataFrameWithInfo` whose index is already `'date'`: Skips the `assign(date=...)` step.
- `_value_for_date` on empty result: Returns result unchanged, no indexing attempted.
- `_value_for_date` with iterable of dates: Uses `result.loc[date]` which may return a sub-DataFrame/Series depending on how many dates match.
- `MultipleRiskMeasureResult.__mul__` returns `ValueError` instead of raising it (line 319) -- callers will get a `ValueError` object as a return value, not an exception.
- `PortfolioRiskResult.__mul__` same bug (line 703) -- returns `ValueError` instead of raising.
- `PortfolioRiskResult.__getitem__` with `Scenario` and `isinstance(item, Iterable)` branch (line 631): The `else` on line 633 checks `item not in self._multi_scen_key` but `item` could be a single Scenario that is not iterable, falling through the iterable check.
- `PortfolioRiskResult.to_frame` with all-error results: Returns `None` when `final_records` is empty (line 886).
- `PortfolioRiskResult.dates` with mixed types in result index: Catches `TypeError` on sort (line 798) and returns empty tuple.
- `PortfolioRiskResult.__results` with slice: Returns a `PortfolioRiskResult` (subset) rather than a tuple.
- `PortfolioRiskResult.__add__` with same portfolio: Zip-adds futures pairwise. With different portfolios: concatenates and then back-fills overlapping risk measure values via mutable `set_value`.
- `HistoricalPricingFuture._set_result` with all errors: Logs error and sets result to `results[0]` (the first error).
- `PortfolioPath.__call__` with `rename_to_parent=True`: Only renames if parent has a `name` attribute and target is not an `InstrumentBase`.
- `_risk_keys_compatible` with deeply nested `MultipleRiskMeasureResult`: Unwraps via while loop until a non-MRMR value is found.

## Bugs Found
- Line 226: `other.__class__.name` should be `other.__class__.__name__` -- will raise `AttributeError` since `type` objects don't have a `.name` attribute (they have `.__name__`). (OPEN)
- Line 319: `MultipleRiskMeasureResult.__mul__` returns `ValueError(...)` instead of raising it. Should be `raise ValueError(...)`. (OPEN)
- Line 703: `PortfolioRiskResult.__mul__` returns `ValueError(...)` instead of raising it. Should be `raise ValueError(...)`. (OPEN)

## Coverage Notes
- Branch count: ~95 explicit branches (if/elif/else chains, try/except, for/while loops with conditionals)
- Key high-branch methods:
  - `get_default_pivots`: 8 pivot rules + 3 cls branches + match function = ~15 branches
  - `PortfolioRiskResult.__getitem__`: 6 top-level branches with sub-branches = ~15 branches
  - `PortfolioRiskResult.__add__`: 3 top-level + many inner branches = ~12 branches
  - `_compose`: 8 type-combination branches + 1 fallthrough
  - `_value_for_date`: ~8 branches
  - `MultipleRiskMeasureResult.__add__`: ~10 branches
  - `MultipleRiskMeasureResult.__op__`: 4 type branches
- Pragmas: None observed
- Hard-to-test branches:
  - `PricingFuture.result()` pricing context guard (requires active `PricingContext` with `is_entered`)
  - `PortfolioRiskResult.__add__` inner `set_value` function (requires cross-portfolio addition with overlapping risk measures)
  - `PortfolioRiskResult.__paths` with resolved/unresolved instrument matching (requires specific resolution state)
  - `HistoricalPricingFuture._set_result` all-error case
  - `pivot_to_frame` `KeyError` fallback path
  - `PortfolioRiskResult.dates` `TypeError` catch
