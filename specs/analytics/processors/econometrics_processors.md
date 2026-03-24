# econometrics_processors.py

## Summary
Econometric processor classes that wrap timeseries econometrics functions: `VolatilityProcessor`, `SharpeRatioProcessor`, `CorrelationProcessor`, `ChangeProcessor`, `ReturnsProcessor`, `BetaProcessor`, and `FXImpliedCorrProcessor`. Each extends `BaseProcessor` (or `MeasureProcessor` for FX) and follows the standard processor pattern of storing children, processing when data arrives, and returning `ProcessorResult`.

## Dependencies
- Internal:
  - `gs_quant.analytics.core.processor` (BaseProcessor, DataCoordinateOrProcessor, DataQueryInfo, DateOrDatetimeOrRDate)
  - `gs_quant.analytics.core.processor_result` (ProcessorResult)
  - `gs_quant.analytics.processors.special_processors` (MeasureProcessor)
  - `gs_quant.common` (Currency)
  - `gs_quant.data` (DataFrequency)
  - `gs_quant.data.coordinate` (DataCoordinate)
  - `gs_quant.data.query` (DataQuery)
  - `gs_quant.entities.entity` (Entity)
  - `gs_quant.markets.securities` (Stock, Cross)
  - `gs_quant.timeseries` (correlation, Window, SeriesType, DataMeasure, DataContext, fx_implied_correlation)
  - `gs_quant.timeseries` (excess_returns_pure)
  - `gs_quant.timeseries.econometrics` (get_ratio_pure, SharpeAssets, change, returns, volatility, Returns, beta)
  - `gs_quant.timeseries.helper` (CurveType)
- External:
  - `typing` (Optional, Union)
  - `pandas` (pd)

## Type Definitions

### VolatilityProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | Input price series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date for data query |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date for data query |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size and ramp up |
| returns_type | `Returns` | `Returns.SIMPLE` | Returns type: simple, logarithmic, or absolute |

### SharpeRatioProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | Input price/returns series |
| children['excess_returns'] | `DataQueryInfo` | computed | Auto-generated query for risk-free rate benchmark |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date for data query |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date for data query |
| currency | `Currency` | required | Currency for risk-free rate lookup |
| w | `Union[Window, int]` | `None` | Window size |
| curve_type | `CurveType` | `CurveType.PRICES` | Whether input is prices or excess returns |

### CorrelationProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | First input series |
| children['benchmark'] | `DataQueryInfo` | computed | Auto-generated query for benchmark entity |
| benchmark | `Entity` | required | Benchmark entity for correlation |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size |
| type_ | `SeriesType` | `SeriesType.PRICES` | Input series type: prices or returns |

### ChangeProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date |

### ReturnsProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | Input series |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date |
| observations | `Optional[int]` | `None` | Number of observations for rolling returns |
| type_ | `Returns` | `Returns.SIMPLE` | Returns type |

### BetaProcessor (class)
Inherits: BaseProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| children['a'] | `DataCoordinateOrProcessor` | required | First series |
| children['b'] | `DataCoordinateOrProcessor` | required | Second series (benchmark) |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date |
| w | `Union[Window, int]` | `Window(None, 0)` | Window size |

### FXImpliedCorrProcessor (class)
Inherits: MeasureProcessor

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| cross2 | `Entity` | `None` | Second FX cross for correlation |
| tenor | `str` | `'3m'` | Tenor for implied correlation |
| start | `Optional[DateOrDatetimeOrRDate]` | `None` | Start date |
| end | `Optional[DateOrDatetimeOrRDate]` | `None` | End date |

## Enums and Constants
None defined in this module (uses imported enums: Returns, CurveType, SeriesType, Currency).

## Functions/Methods

### VolatilityProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, w: Union[Window, int] = Window(None, 0), returns_type: Returns = Returns.SIMPLE, **kwargs) -> None
Purpose: Initialize volatility processor with input series and parameters.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Store `start`, `end`, `w`, `returns_type`

### VolatilityProcessor.process(self) -> ProcessorResult
Purpose: Compute volatility of input series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - True:
     a. Branch: `a_data.success`
        - True: `result = volatility(a_data.data, self.w, self.returns_type)`, set success result
        - False: set failure result `'Could not compute volatility'`
   - False: set failure result `'Processor does not have data'`
3. Return `self.value`

### VolatilityProcessor.get_plot_expression(self) -> None
Purpose: Placeholder (not implemented). Returns None via `pass`.

### SharpeRatioProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, currency: Currency, w: Union[Window, int] = None, curve_type: CurveType = CurveType.PRICES, **kwargs) -> None
Purpose: Initialize Sharpe ratio processor, creating additional excess_returns query.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Store `start`, `end`, `currency`, `w`, `curve_type`
4. Set `self.children['excess_returns'] = self.get_excess_returns_query()`

### SharpeRatioProcessor.get_excess_returns_query(self) -> DataQueryInfo
Purpose: Build a DataQueryInfo for the risk-free rate benchmark.

**Algorithm:**
1. Look up `marquee_id = SharpeAssets[self.currency.value].value`
2. Create `entity = Stock(marquee_id, "", "")`
3. Create `coordinate = DataCoordinate(measure=DataMeasure.CLOSE_PRICE, frequency=DataFrequency.DAILY)`
4. Create `data_query = DataQuery(coordinate=coordinate, start=self.start, end=self.end)`
5. Return `DataQueryInfo('excess_returns', None, data_query, entity)`

### SharpeRatioProcessor.process(self) -> ProcessorResult
Purpose: Compute Sharpe ratio from series and risk-free benchmark.

**Algorithm:**
1. Get `a_data` and `excess_returns_data` from `self.children_data`
2. Branch: both are `isinstance ProcessorResult`
   - True:
     a. Branch: both `success`
        - True:
          i. Branch: `self.curve_type == CurveType.PRICES`
             - True: compute `excess_returns = excess_returns_pure(a_data.data, excess_returns_data.data)`
             - False: `excess_returns = a_data.data` (already excess returns)
          ii. Compute `ratio = get_ratio_pure(excess_returns, self.w)`
          iii. Set `self.value = ProcessorResult(True, ratio)`
        - False: no-op (self.value unchanged)
   - False: no-op (self.value unchanged)
3. Return `self.value`

### SharpeRatioProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

### CorrelationProcessor.__init__(self, a: DataCoordinateOrProcessor, *, benchmark: Entity, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, w: Union[Window, int] = Window(None, 0), type_: SeriesType = SeriesType.PRICES, **kwargs) -> None
Purpose: Initialize correlation processor with input and benchmark.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Store `benchmark` entity
4. Store `start`, `end`
5. Set `self.children['benchmark'] = self.get_benchmark_coordinate()`
6. Store `w`, `type_`

### CorrelationProcessor.get_benchmark_coordinate(self) -> DataQueryInfo
Purpose: Build DataQueryInfo for the benchmark entity.

**Algorithm:**
1. Create `coordinate = DataCoordinate(measure=DataMeasure.CLOSE_PRICE, frequency=DataFrequency.DAILY)`
2. Create `data_query = DataQuery(coordinate=coordinate, start=self.start, end=self.end)`
3. Return `DataQueryInfo('benchmark', None, data_query, self.benchmark)`

### CorrelationProcessor.process(self) -> ProcessorResult
Purpose: Compute correlation between input and benchmark series.

**Algorithm:**
1. Get `a_data` and `benchmark_data` from `self.children_data`
2. Branch: both are `isinstance ProcessorResult`
   - True:
     a. Branch: both `success`
        - True: `result = correlation(a_data.data, benchmark_data.data, w=self.w, type_=self.type_)`, set success
        - False: set failure `"Processor does not have A and Benchmark data yet"`
   - False: set failure `"Processor does not have A and Benchmark data yet"`
3. Return `self.value`

### CorrelationProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

### ChangeProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, **kwargs) -> None
Purpose: Initialize change processor.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Store `start`, `end`

### ChangeProcessor.process(self) -> ProcessorResult
Purpose: Compute change of input series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - True:
     a. Branch: `a_data.success`
        - True: `value = change(a_data.data)`, set `self.value = ProcessorResult(True, value)`
        - False: no-op (self.value unchanged)
   - False: no-op (self.value unchanged)
3. Return `self.value`

### ChangeProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

### ReturnsProcessor.__init__(self, a: DataCoordinateOrProcessor, *, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, observations: Optional[int] = None, type_: Returns = Returns.SIMPLE, **kwargs) -> None
Purpose: Initialize returns processor.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`
3. Store `start`, `end`, `observations`, `type_`

### ReturnsProcessor.process(self) -> ProcessorResult
Purpose: Compute returns of input series, with special handling for total return when observations is None.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - True:
     a. Branch: `a_data.success`
        - True:
          i. Get `data = a_data.data`
          ii. Branch: `self.observations is None`
              - True:
                a. Branch: `len(data) > 1`
                   - True: compute total return `(data.iloc[-1] - data.iloc[0]) / data.iloc[0]`, wrap in `pd.Series`, set success
                   - False: set `self.value = ProcessorResult(True, 'Series has is less than 2.')` (note: typo in message is in original code)
              - False: `value = returns(a_data.data, self.observations, self.type_)`, set success
        - False: no-op
   - False: no-op
3. Return `self.value`

### ReturnsProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

### BetaProcessor.__init__(self, a: DataCoordinateOrProcessor, b: DataCoordinateOrProcessor, *, start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, w: Union[Window, int] = Window(None, 0), **kwargs) -> None
Purpose: Initialize beta processor with two input series.

**Algorithm:**
1. Call `super().__init__(**kwargs)`
2. Set `self.children['a'] = a`, `self.children['b'] = b`
3. Store `start`, `end`, `w`

### BetaProcessor.process(self) -> ProcessorResult
Purpose: Compute beta between two series.

**Algorithm:**
1. Get `a_data = self.children_data.get('a')`
2. Branch: `isinstance(a_data, ProcessorResult)`
   - True:
     a. Branch: `a_data.success`
        - True:
          i. Get `b_data = self.children_data.get('b')`
          ii. Branch: `self.children.get('b') and isinstance(b_data, ProcessorResult)`
              - True:
                a. Branch: `b_data.success`
                   - True: `result = beta(a_data.data, b_data.data, w=self.w)`, set `ProcessorResult(True, result)`
                   - False: set `ProcessorResult(True, "BetaProcessor does not have 'b' series values yet.")`
              - False: set `ProcessorResult(True, 'BetaProcessor: b is not a valid series.')`
        - False: set `ProcessorResult(False, "BetaProcessor does not have 'a' series values yet")`
   - False: set `ProcessorResult(False, "BetaProcessor does not have 'a' series yet")`
3. Return `self.value`

### BetaProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

### FXImpliedCorrProcessor.__init__(self, *, cross2: Entity = None, tenor: str = '3m', start: Optional[DateOrDatetimeOrRDate] = None, end: Optional[DateOrDatetimeOrRDate] = None, **kwargs) -> None
Purpose: Initialize FX implied correlation processor.

**Algorithm:**
1. Call `super().__init__(**kwargs)` (MeasureProcessor)
2. Store `cross2`, `tenor`, `start`, `end`

### FXImpliedCorrProcessor.process(self, cross1: Entity) -> ProcessorResult
Purpose: Compute FX implied correlation between two crosses.

**Algorithm:**
1. Branch: `isinstance(cross1, Cross) and isinstance(self.cross2, Cross)`
   - True:
     a. Try:
        - Enter `DataContext(self.start, self.end)` context manager
        - `result = fx_implied_correlation(cross1, self.cross2, self.tenor)`
        - Set `self.value = ProcessorResult(True, result)`
     b. Except `Exception as e`:
        - Set `self.value = ProcessorResult(False, str(e))`
   - False: set `self.value = ProcessorResult(False, "Processor does not have valid crosses as inputs")`
2. Return `self.value`

### FXImpliedCorrProcessor.get_plot_expression(self) -> None
Purpose: Placeholder. Returns None via `pass`.

## State Mutation
- All processors: `self.value` is updated during `process()` calls
- `self.children`: Set during `__init__` for each processor
- `self.children_data`: Populated externally by the processor framework before `process()` is called
- `SharpeRatioProcessor.__init__`: Creates a `Stock` entity and `DataQueryInfo` for excess returns
- `CorrelationProcessor.__init__`: Creates a `DataQueryInfo` for benchmark

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` (caught) | `FXImpliedCorrProcessor.process` | Any exception from `fx_implied_correlation` -- caught, stored as `ProcessorResult(False, str(e))` |
| `KeyError` (potential) | `SharpeRatioProcessor.__init__` | If `self.currency.value` is not in `SharpeAssets` enum |

All other processors express errors via `ProcessorResult(False, message)` without raising exceptions.

## Edge Cases
- `SharpeRatioProcessor.process()`: If either `a_data` or `excess_returns_data` is not a ProcessorResult, or either has `success=False`, the method returns `self.value` unchanged (which is the default from BaseProcessor, likely `ProcessorResult(False, ...)`)
- `ChangeProcessor.process()`: If `a_data` is not ProcessorResult or not successful, returns `self.value` unchanged (no explicit failure message set)
- `ReturnsProcessor.process()` with `observations=None` and `len(data) <= 1`: Returns `ProcessorResult(True, 'Series has is less than 2.')` -- note: success=True with a string error message is misleading
- `ReturnsProcessor.process()`: Message contains typo: "Series has is less than 2." (extra "has")
- `BetaProcessor.process()`: When `b` fails or is missing, returns `ProcessorResult(True, ...)` with error string -- success=True is misleading for error states
- `FXImpliedCorrProcessor.process()`: Both `cross1` and `cross2` must be `Cross` instances specifically, not just `Entity`
- `FXImpliedCorrProcessor`: `cross2=None` by default, so if not provided, process() always returns failure

## Bugs Found
- `BetaProcessor.process()`: Lines 348-350 return `ProcessorResult(True, error_string)` when b data is missing or failed. Using `success=True` for error states is inconsistent with other processors.
- `ReturnsProcessor.process()`: Line 291 returns `ProcessorResult(True, 'Series has is less than 2.')` -- both a typo ("has is") and misleading success=True for an error condition.

## Coverage Notes
- Branch count: ~30
  - `VolatilityProcessor.process`: 4 branches (isinstance 2, success 2)
  - `SharpeRatioProcessor.process`: 6 branches (isinstance both 2, success both 2, curve_type 2)
  - `CorrelationProcessor.process`: 6 branches (isinstance both 2, success both 2)
  - `ChangeProcessor.process`: 4 branches (isinstance 2, success 2)
  - `ReturnsProcessor.process`: 8 branches (isinstance 2, success 2, observations None 2, len > 1 2)
  - `BetaProcessor.process`: 10 branches (isinstance a 2, success a 2, children.get b + isinstance 2, success b 2)
  - `FXImpliedCorrProcessor.process`: 4 branches (isinstance both Cross 2, try/except 2)
- All `get_plot_expression` methods: 0 branches (pass)
- No pragmas
