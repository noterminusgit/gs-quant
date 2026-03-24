# triggers.py

## Summary
Defines the trigger framework for the backtesting engine. Contains `TriggerRequirements` (abstract base), `TriggerInfo` (result container), `Trigger` (base trigger with action delegation), and many concrete trigger types: `PeriodicTriggerRequirements`, `IntradayTriggerRequirements`, `MktTriggerRequirements`, `RiskTriggerRequirements`, `AggregateTriggerRequirements`, `NotTriggerRequirements`, `DateTriggerRequirements`, `PortfolioTriggerRequirements`, `MeanReversionTriggerRequirements`, `TradeCountTriggerRequirements`, `EventTriggerRequirements`, and `OrdersGeneratorTrigger`. Each trigger requirements class implements `has_triggered(state, backtest) -> TriggerInfo` with direction/barrier/schedule logic. Concrete `Trigger` subclasses are thin wrappers pairing a `TriggerRequirements` with serialization metadata.

## Dependencies
- Internal: `gs_quant.backtests.actions` (`AddTradeAction`, `AddTradeActionInfo`, `AddScaledTradeAction`, `AddScaledTradeActionInfo`, `HedgeAction`, `HedgeActionInfo`, `Action`), `gs_quant.backtests.backtest_objects` (`BackTest`, `PredefinedAssetBacktest`), `gs_quant.backtests.backtest_utils` (`make_list`, `CalcType`), `gs_quant.backtests.data_sources` (`DataSource`, `GsDataSource`), `gs_quant.base` (`field_metadata`, `exclude_none`, `static_field`), `gs_quant.data` (`Dataset`), `gs_quant.datetime.relative_date` (`RelativeDateSchedule`), `gs_quant.json_convertors` (`decode_iso_date_or_datetime`, `decode_date_tuple`, `dc_decode`), `gs_quant.json_convertors_common` (`encode_risk_measure`, `decode_risk_measure`), `gs_quant.risk` (`RiskMeasure`), `gs_quant.risk.transform` (`Transformer`)
- External: `datetime` (`dt.date`, `dt.time`, `dt.datetime`, `dt.timedelta`), `dataclasses` (`dataclass`, `field`), `enum` (`Enum`), `typing` (`ClassVar`, `List`, `Optional`, `Iterable`, `Union`), `dataclasses_json` (`dataclass_json`, `config`)

## Type Definitions

### TriggerInfo (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| triggered | `bool` | *(required)* | Whether the trigger fired |
| info_dict | `Optional[dict]` | `None` | Mapping of action type to action-specific info (e.g. `AddTradeActionInfo`) |

### TriggerRequirements (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __sub_classes | `ClassVar[List[type]]` | `[]` | Registry of all TriggerRequirements subclasses |

### PeriodicTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| start_date | `Optional[dt.date]` | `None` | Schedule start date |
| end_date | `Optional[dt.date]` | `None` | Schedule end date |
| frequency | `Optional[str]` | `None` | Schedule frequency string (e.g. `'1m'`) |
| calendar | `Optional[Iterable[dt.date]]` | `None` | Holiday calendar for schedule generation |
| trigger_dates | `list` (class var) | `[]` | Lazily populated list of trigger dates |
| class_type | `str` | `'periodic_trigger_requirements'` (static_field) | Serialization discriminator |

### IntradayTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| start_time | `Optional[dt.time]` | `None` | Intraday trigger start time |
| end_time | `Optional[dt.time]` | `None` | Intraday trigger end time |
| frequency | `Optional[float]` | `None` | Interval in minutes between trigger times |
| class_type | `str` | `'intraday_trigger_requirements'` (static_field) | Serialization discriminator |
| _trigger_times | `list` | *(set in `__post_init__`)* | Generated list of `dt.time` values at frequency intervals |

### MktTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_source | `DataSource` | `None` | Source for market data values |
| trigger_level | `float` | `None` | Barrier level to compare against |
| direction | `TriggerDirection` | `None` | Barrier direction (ABOVE/BELOW/EQUAL) |
| class_type | `str` | `'mkt_trigger_requirements'` (static_field) | Serialization discriminator |

### RiskTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| risk | `RiskMeasure` | `None` | Risk measure to evaluate from backtest results |
| trigger_level | `float` | `None` | Barrier level to compare against |
| direction | `TriggerDirection` | `None` | Barrier direction (ABOVE/BELOW/EQUAL) |
| risk_transformation | `Optional[Transformer]` | `None` | Optional transformation applied before comparison |
| class_type | `str` | `'risk_trigger_requirements'` (static_field) | Serialization discriminator |

### AggregateTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| triggers | `Iterable[TriggerRequirements]` | `None` | Child triggers to aggregate |
| aggregate_type | `AggType` | `AggType.ALL_OF` | Aggregation mode (ALL_OF or ANY_OF) |
| class_type | `str` | `'aggregate_trigger_requirements'` (static_field) | Serialization discriminator |

### NotTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trigger | `TriggerRequirements` | `None` | Child trigger to invert |
| class_type | `str` | `'not_trigger_requirements'` (static_field) | Serialization discriminator |

### DateTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| dates | `Iterable[Union[dt.datetime, dt.date]]` | `None` | Explicit trigger dates/datetimes |
| entire_day | `bool` | `False` | Whether to match on date portion only (strip time from datetimes) |
| class_type | `str` | `'date_trigger_requirements'` (static_field) | Serialization discriminator |
| dates_from_datetimes | `list` (class var) | `[]` | Dates extracted from datetimes when `entire_day=True`; set in `__post_init__` |

### PortfolioTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_source | `str` | `None` | Data source key; currently only `'len'` is supported |
| trigger_level | `float` | `None` | Barrier level to compare against |
| direction | `TriggerDirection` | `None` | Barrier direction (ABOVE/BELOW/EQUAL) |
| class_type | `str` | `'portfolio_trigger_requirements'` (static_field) | Serialization discriminator |

### MeanReversionTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| data_source | `DataSource` | `None` | Source for price data and rolling windows |
| z_score_bound | `float` | `None` | Z-score threshold for entry |
| rolling_mean_window | `int` | `None` | Window size for rolling mean |
| rolling_std_window | `int` | `None` | Window size for rolling standard deviation |
| current_position | `int` (class var) | `0` | Tracks position state: 0=flat, 1=long, -1=short |
| class_type | `str` | `'mean_reversion_trigger_requirements'` (static_field) | Serialization discriminator |

### TradeCountTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trade_count | `float` | `None` | Number of trades to compare against |
| direction | `TriggerDirection` | `None` | Barrier direction (ABOVE/BELOW/EQUAL) |
| class_type | `str` | `'trade_count_requirements'` (static_field) | Serialization discriminator |

### EventTriggerRequirements (dataclass_json, dataclass)
Inherits: `TriggerRequirements`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| event_name | `str` | `None` | Name of macro event to trigger on |
| offset_days | `int` | `0` | Days to offset from event date |
| data_source | `DataSource` | `None` | Data source; defaults to `GsDataSource('MACRO_EVENTS_CALENDAR')` in `__post_init__` |
| class_type | `str` | `'event_requirements'` (static_field) | Serialization discriminator |
| trigger_dates | `list` (class var) | `[]` | Lazily populated event dates |

### Trigger (dataclass_json, dataclass)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| trigger_requirements | `Optional[TriggerRequirements]` | `None` | The trigger requirements defining when to fire |
| actions | `Union[Action, Iterable[Action]]` | `None` | Action(s) to execute when trigger fires |
| __sub_classes | `ClassVar[List[type]]` | `[]` | Registry of all Trigger subclasses |

### Concrete Trigger Subclasses (all dataclass_json, dataclass, inherit Trigger)
| Class | Requirements Type | class_type |
|-------|-------------------|------------|
| `PeriodicTrigger` | `PeriodicTriggerRequirements` | `'periodic_trigger'` |
| `IntradayPeriodicTrigger` | `IntradayTriggerRequirements` | `'intraday_periodic_trigger'` |
| `MktTrigger` | `MktTriggerRequirements` | `'mkt_trigger'` |
| `StrategyRiskTrigger` | `RiskTriggerRequirements` | `'strategy_risk_trigger'` |
| `AggregateTrigger` | `AggregateTriggerRequirements` | `'aggregate_trigger'` |
| `NotTrigger` | `NotTriggerRequirements` | `'not_trigger'` |
| `DateTrigger` | `DateTriggerRequirements` | `'date_trigger'` |
| `PortfolioTrigger` | `PortfolioTriggerRequirements` | `'portfolio_trigger'` |
| `MeanReversionTrigger` | `MeanReversionTriggerRequirements` | `'mean_reversion_trigger'` |
| `TradeCountTrigger` | `TradeCountTriggerRequirements` | `'trade_count_trigger'` |
| `EventTrigger` | `EventTriggerRequirements` | `'event_trigger'` |
| `OrdersGeneratorTrigger` | *(none -- uses base Trigger)* | *(none)* |

## Enums and Constants

### TriggerDirection(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ABOVE | `1` | Test if value is above trigger level |
| BELOW | `2` | Test if value is below trigger level |
| EQUAL | `3` | Test if value equals trigger level |

### AggType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ALL_OF | `1` | All child triggers must fire |
| ANY_OF | `2` | At least one child trigger must fire |

### Module-Level Metadata Patching
After all classes are defined, the module patches the `triggers` field metadata on `AggregateTriggerRequirements` and the `trigger` field metadata on `NotTriggerRequirements` to use `dc_decode(*TriggerRequirements.sub_classes(), allow_missing=True)`. This is necessary because these fields reference `TriggerRequirements` subclasses that are not yet defined at class definition time.

## Functions/Methods

### check_barrier(direction: TriggerDirection, test_value: float, trigger_level: float) -> TriggerInfo
Purpose: Compare a test value against a trigger level using the specified direction.

**Algorithm:**
1. Branch: `direction == TriggerDirection.ABOVE`:
   - Branch: `test_value > trigger_level` -> return `TriggerInfo(True)`
   - Branch: else -> fall through
2. Branch: `direction == TriggerDirection.BELOW`:
   - Branch: `test_value < trigger_level` -> return `TriggerInfo(True)`
   - Branch: else -> fall through
3. Branch: else (EQUAL):
   - Branch: `test_value == trigger_level` -> return `TriggerInfo(True)`
   - Branch: else -> fall through
4. Return `TriggerInfo(False)`

### TriggerInfo.__eq__(self, other) -> bool
Purpose: Compare `triggered` field with `other` using identity check (`is`).

**Algorithm:**
1. Return `self.triggered is other`

*Note: This means `TriggerInfo(True) == True` uses identity, not equality. Works because Python interns `True` and `False`.*

### TriggerInfo.__bool__(self) -> bool
Purpose: Allow TriggerInfo to be used in boolean context.

**Algorithm:**
1. Return `self.triggered`

### TriggerRequirements.__init_subclass__(cls, **kwargs) -> None
Purpose: Register every TriggerRequirements subclass in the `__sub_classes` class variable.

**Algorithm:**
1. Call `super().__init_subclass__(**kwargs)`
2. Append `cls` to `TriggerRequirements.__sub_classes`

### TriggerRequirements.sub_classes() -> tuple (staticmethod)
Purpose: Return a tuple of all registered TriggerRequirements subclasses.

**Algorithm:**
1. Return `tuple(TriggerRequirements.__sub_classes)`

### TriggerRequirements.get_trigger_times(self) -> list
Purpose: Return trigger times for schedule-based triggers. Base returns empty list.

**Algorithm:**
1. Return `[]`

### TriggerRequirements.calc_type (property) -> CalcType
Purpose: Return the calculation complexity type. Base returns `CalcType.simple`.

### PeriodicTriggerRequirements.get_trigger_times(self) -> [dt.date]
Purpose: Lazily compute and return trigger dates from a RelativeDateSchedule.

**Algorithm:**
1. Branch: `not self.trigger_dates` -> generate dates via `RelativeDateSchedule(self.frequency, self.start_date, self.end_date).apply_rule(holiday_calendar=self.calendar)`, store in `self.trigger_dates`
2. Return `self.trigger_dates`

### PeriodicTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Check if state is one of the periodic schedule dates.

**Algorithm:**
1. Branch: `not self.trigger_dates` -> call `self.get_trigger_times()` to populate
2. Branch: `state in self.trigger_dates`:
   a. Set `next_state = None`
   b. Branch: `self.trigger_dates.index(state) != len(self.trigger_dates) - 1` -> set `next_state` to next date in list
   c. Return `TriggerInfo(True, {AddTradeAction: AddTradeActionInfo(scaling=None, next_schedule=next_state), AddScaledTradeAction: AddScaledTradeActionInfo(next_schedule=next_state), HedgeAction: HedgeActionInfo(next_schedule=next_state)})`
3. Return `TriggerInfo(False)`

### IntradayTriggerRequirements.__post_init__(self) -> None
Purpose: Generate all trigger times at frequency-minute intervals from start_time to end_time.

**Algorithm:**
1. Initialize `self._trigger_times = []`
2. Set `time = self.start_time`
3. While `time <= self.end_time`:
   a. Append `time` to `self._trigger_times`
   b. Advance `time` by `self.frequency` minutes (via `dt.datetime.combine` + `dt.timedelta`)

### IntradayTriggerRequirements.get_trigger_times(self) -> list
Purpose: Return pre-computed intraday trigger times.

### IntradayTriggerRequirements.has_triggered(self, state: Union[dt.date, dt.datetime], backtest: BackTest = None) -> TriggerInfo
Purpose: Check if state's time component is in the trigger times.

**Algorithm:**
1. Return `TriggerInfo(state.time() in self._trigger_times)`

### MktTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Fetch market data and compare against trigger level.

**Algorithm:**
1. Call `data_value = self.data_source.get_data(state)`
2. Try: call `check_barrier(self.direction, data_value, self.trigger_level)`
3. Branch: `TypeError` raised -> raise `RuntimeError(f'unable to determine trigger state on {state}, data value was {data_value}')`
4. Return triggered result

### RiskTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Check backtest risk results against trigger level.

**Algorithm:**
1. Branch: `state not in backtest.results` -> return `TriggerInfo(False)`
2. Branch: `self.risk_transformation is None` -> `risk_value = backtest.results[state][self.risk].aggregate()`
3. Branch: `self.risk_transformation is not None` -> `risk_value = backtest.results[state][self.risk].transform(risk_transformation=self.risk_transformation).aggregate(allow_mismatch_risk_keys=True)`
4. Return `check_barrier(self.direction, risk_value, self.trigger_level)`

### RiskTriggerRequirements.calc_type (property) -> CalcType
Purpose: Override to return `CalcType.path_dependent`.

### AggregateTriggerRequirements.__setattr__(self, key, value) -> None
Purpose: Auto-extract trigger_requirements from Trigger objects when setting `triggers` field.

**Algorithm:**
1. Branch: `key == 'triggers'`:
   a. Branch: `all(isinstance(v, Trigger) for v in value)` -> replace `value` with `tuple(v.trigger_requirements for v in value)`
2. Call `super().__setattr__(key, value)`

### AggregateTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Aggregate child trigger results using ALL_OF or ANY_OF logic.

**Algorithm:**
1. Initialize `info_dict = {}`
2. Branch: `self.aggregate_type == AggType.ALL_OF`:
   a. For each `trigger` in `self.triggers`:
      - Call `t_info = trigger.has_triggered(state, backtest)`
      - Branch: `not t_info` -> return `TriggerInfo(False)` (short-circuit)
      - Branch: `t_info.info_dict` is truthy -> `info_dict.update(t_info.info_dict)`
   b. Return `TriggerInfo(True, info_dict)`
3. Branch: `self.aggregate_type == AggType.ANY_OF`:
   a. Set `triggered = False`
   b. For each `trigger` in `self.triggers`:
      - Call `t_info = trigger.has_triggered(state, backtest)`
      - Branch: `t_info` is truthy -> set `triggered = True`
        - Branch: `t_info.info_dict` is truthy -> `info_dict.update(t_info.info_dict)`
   c. Branch: `triggered` -> return `TriggerInfo(True, info_dict)`
   d. Branch: not triggered -> return `TriggerInfo(False)`
4. Branch: else -> raise `RuntimeError(f'Unrecognised aggregation type: {self.aggregate_type}')`

### AggregateTriggerRequirements.calc_type (property) -> CalcType
Purpose: Return the most complex calc_type among child triggers.

**Algorithm:**
1. Collect `seen_types` from all child triggers' `calc_type`
2. Branch: `CalcType.path_dependent in seen_types` -> return `CalcType.path_dependent`
3. Branch: `CalcType.semi_path_dependent in seen_types` -> return `CalcType.semi_path_dependent`
4. Branch: else -> return `CalcType.simple`

### NotTriggerRequirements.__setattr__(self, key, value) -> None
Purpose: Auto-extract trigger_requirements from a Trigger object when setting `trigger` field.

**Algorithm:**
1. Branch: `key == 'trigger'`:
   a. Branch: `isinstance(value, Trigger)` -> replace `value` with `value.trigger_requirements`
   b. Call `super().__setattr__(key, value)`

*Note: `super().__setattr__` is only called inside the `key == 'trigger'` branch due to indentation. Setting any other attribute will silently fail (no-op). This is a latent bug.*

### NotTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Invert the child trigger result.

**Algorithm:**
1. Call `t_info = self.trigger.has_triggered(state, backtest)`
2. Branch: `t_info` is truthy -> return `TriggerInfo(False)`
3. Branch: `t_info` is falsy -> return `TriggerInfo(True)`

### DateTriggerRequirements.__post_init__(self) -> None
Purpose: Pre-compute date-only versions of datetimes when `entire_day=True`.

**Algorithm:**
1. Branch: `self.entire_day` is truthy -> set `self.dates_from_datetimes` to list of `d.date() if isinstance(d, dt.datetime) else d` for each `d` in `self.dates`
2. Branch: `self.entire_day` is falsy -> set `self.dates_from_datetimes = None`

### DateTriggerRequirements.has_triggered(self, state: Union[dt.date, dt.datetime], backtest: BackTest = None) -> TriggerInfo
Purpose: Check if state is one of the specified dates, providing next_schedule info.

**Algorithm:**
1. Branch: `self.entire_day`:
   a. Sort `self.dates_from_datetimes` into `dates`
   b. Branch: `isinstance(state, dt.datetime)` -> convert `state = state.date()`
2. Branch: not `self.entire_day` -> sort `self.dates` into `dates`
3. Branch: `state in dates`:
   a. Set `next_state = None`
   b. Branch: `dates.index(state) < len(dates) - 1` -> set `next_state` to next date
   c. Return `TriggerInfo(True, {AddTradeAction: ..., AddScaledTradeAction: ..., HedgeAction: ...})`
4. Return `TriggerInfo(False)`

### DateTriggerRequirements.get_trigger_times(self) -> list
Purpose: Return trigger dates (dates_from_datetimes if available, else dates).

**Algorithm:**
1. Return `self.dates_from_datetimes or self.dates`

### PortfolioTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Check portfolio characteristics against trigger level.

**Algorithm:**
1. Branch: `self.data_source == 'len'`:
   a. Set `value = len(backtest.portfolio_dict)`
   b. Branch: `self.direction == TriggerDirection.ABOVE`:
      - Branch: `value > self.trigger_level` -> return `TriggerInfo(True)`
   c. Branch: `self.direction == TriggerDirection.BELOW`:
      - Branch: `value < self.trigger_level` -> return `TriggerInfo(True)`
   d. Branch: else (EQUAL):
      - Branch: `value == self.trigger_level` -> return `TriggerInfo(True)`
2. Return `TriggerInfo(False)`

*Note: Only `data_source == 'len'` is implemented; any other value silently returns `TriggerInfo(False)`.*

### MeanReversionTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Implement Z-score based mean reversion entry/exit logic.

**Algorithm:**
1. Compute `rolling_mean`, `rolling_std`, `current_price` from `self.data_source`
2. Branch: `self.current_position == 0` (flat):
   a. Branch: `abs((current_price - rolling_mean) / rolling_std) > self.z_score_bound`:
      - Branch: `current_price > rolling_mean` -> set `self.current_position = -1`, return `TriggerInfo(True, {AddTradeAction: AddTradeActionInfo(scaling=-1)})`
      - Branch: else -> set `self.current_position = 1`, return `TriggerInfo(True, {AddTradeAction: AddTradeActionInfo(scaling=1)})`
3. Branch: `self.current_position == 1` (long):
   a. Branch: `current_price > rolling_mean` -> set `self.current_position = 0`, return `TriggerInfo(True, {AddTradeAction: AddTradeActionInfo(scaling=-1)})`
4. Branch: `self.current_position == -1` (short):
   a. Branch: `current_price > rolling_mean` -> set `self.current_position = 0`, return `TriggerInfo(True, {AddTradeAction: AddTradeActionInfo(scaling=1)})`
5. Branch: else -> raise `RuntimeWarning(f'unexpected current position: {self.current_position}')`
6. Return `TriggerInfo(False)`

### TradeCountTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Check the number of trades at the current state against the trade count.

**Algorithm:**
1. Set `value = len(backtest.portfolio_dict.get(state, []))`
2. Branch: `self.direction == TriggerDirection.ABOVE`:
   - Branch: `value > self.trade_count` -> return `TriggerInfo(True)`
3. Branch: `self.direction == TriggerDirection.BELOW`:
   - Branch: `value < self.trade_count` -> return `TriggerInfo(True)`
4. Branch: else (EQUAL):
   - Branch: `value == self.trade_count` -> return `TriggerInfo(True)`
5. Return `TriggerInfo(False)`

### TradeCountTriggerRequirements.calc_type (property) -> CalcType
Purpose: Override to return `CalcType.path_dependent`.

### EventTriggerRequirements.__post_init__(self) -> None
Purpose: Set default data source if none provided.

**Algorithm:**
1. Branch: `self.data_source is None` -> set `self.data_source = GsDataSource(data_set='MACRO_EVENTS_CALENDAR', asset_id=None, value_header='eventName')`

### EventTriggerRequirements.get_trigger_times(self) -> [dt.date]
Purpose: Lazily load event dates from the data source.

**Algorithm:**
1. Branch: `not self.trigger_dates` -> fetch data from `self.data_source.get_data(None, **kwargs)`, extract index dates with offset, store in `self.trigger_dates`
2. Return `self.trigger_dates`

### EventTriggerRequirements.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Check if state matches a loaded event date.

**Algorithm:**
1. Sort `self.trigger_dates` into `dates`
2. Branch: `state in dates`:
   a. Set `next_state = None`
   b. Branch: `dates.index(state) < len(dates) - 1` -> set `next_state` to next date
   c. Return `TriggerInfo(True, {AddTradeAction: ..., AddScaledTradeAction: ..., HedgeAction: ...})`
3. Return `TriggerInfo(False)`

### EventTriggerRequirements.list_events(currency: str, start=Optional[dt.datetime], end=Optional[dt.datetime], **kwargs) -> ndarray (staticmethod)
Purpose: Query MACRO_EVENTS_CALENDAR dataset for unique event names.

**Algorithm:**
1. Set `kwargs['currency'] = currency`
2. Create `Dataset('MACRO_EVENTS_CALENDAR')`, call `get_data(start, end, **kwargs)`
3. Return `['eventName'].unique()`

### Trigger.__init_subclass__(cls, **kwargs) -> None
Purpose: Register every Trigger subclass in the `__sub_classes` class variable.

**Algorithm:**
1. Call `super().__init_subclass__(**kwargs)`
2. Append `cls` to `Trigger.__sub_classes`

### Trigger.sub_classes() -> tuple (staticmethod)
Purpose: Return a tuple of all registered Trigger subclasses.

### Trigger.__post_init__(self) -> None
Purpose: Normalize actions to a list.

**Algorithm:**
1. Set `self.actions = make_list(self.actions)`

### Trigger.has_triggered(self, state: dt.date, backtest: BackTest = None) -> TriggerInfo
Purpose: Delegate to trigger_requirements.

**Algorithm:**
1. Return `self.trigger_requirements.has_triggered(state, backtest)`

### Trigger.get_trigger_times(self) -> list
Purpose: Delegate to trigger_requirements.

### Trigger.calc_type (property) -> CalcType
Purpose: Delegate to trigger_requirements.

### Trigger.risks (property) -> list
Purpose: Collect risk measures from all actions.

**Algorithm:**
1. Return `[x.risk for x in make_list(self.actions) if x.risk is not None]`

### StrategyRiskTrigger.risks (property) -> list
Purpose: Override to include the trigger's own risk measure.

**Algorithm:**
1. Return `[x.risk for x in make_list(self.actions) if x.risk is not None] + [self.trigger_requirements.risk]`

### OrdersGeneratorTrigger.__post_init__(self) -> None
Purpose: Ensure at least one default Action exists, then delegate to Trigger.__post_init__.

**Algorithm:**
1. Branch: `not self.actions` -> set `self.actions = [Action()]`
2. Call `super().__post_init__()`

### OrdersGeneratorTrigger.get_trigger_times(self) -> list
Purpose: Abstract -- must be implemented by subclass.

**Algorithm:**
1. Raise `RuntimeError('get_trigger_times must be implemented by subclass')`

### OrdersGeneratorTrigger.generate_orders(self, state: dt.datetime, backtest: PredefinedAssetBacktest = None) -> list
Purpose: Abstract -- must be implemented by subclass.

**Algorithm:**
1. Raise `RuntimeError('generate_orders must be implemented by subclass')`

### OrdersGeneratorTrigger.has_triggered(self, state: dt.datetime, backtest: PredefinedAssetBacktest = None) -> TriggerInfo
Purpose: Check if state time is a trigger time; if so, generate orders.

**Algorithm:**
1. Branch: `state.time() not in self.get_trigger_times()` -> return `TriggerInfo(False)`
2. Branch: else:
   a. Call `orders = self.generate_orders(state, backtest)`
   b. Branch: `len(orders)` is truthy -> return `TriggerInfo(True, {type(a): orders for a in self.actions})`
   c. Branch: `len(orders) == 0` -> return `TriggerInfo(False)`

## State Mutation
- `TriggerRequirements.__sub_classes` (class var): Appended to by `__init_subclass__` for every subclass at import time. Immutable after module load.
- `Trigger.__sub_classes` (class var): Same pattern as TriggerRequirements.
- `PeriodicTriggerRequirements.trigger_dates` (class var / instance): Lazily populated on first call to `get_trigger_times()` or `has_triggered()`. Shared across all instances since it is a class variable (mutable default list on the class).
- `EventTriggerRequirements.trigger_dates` (class var): Same shared-mutable-default issue as PeriodicTriggerRequirements.
- `IntradayTriggerRequirements._trigger_times`: Set in `__post_init__`, immutable thereafter.
- `DateTriggerRequirements.dates_from_datetimes` (class var / instance): Set in `__post_init__`.
- `MeanReversionTriggerRequirements.current_position` (class var / instance): Mutated during `has_triggered()` to track position state (0, 1, -1). Since it starts as a class variable, the first assignment creates an instance variable shadowing the class variable.
- `self.actions`: Normalized to list in `Trigger.__post_init__`.
- `AggregateTriggerRequirements.triggers` / `NotTriggerRequirements.trigger`: May be replaced via `__setattr__` when Trigger objects are passed.
- Thread safety: `current_position` on MeanReversionTriggerRequirements has no synchronization. `trigger_dates` class variables are shared mutable state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `RuntimeError` | `MktTriggerRequirements.has_triggered` | When `check_barrier` raises `TypeError` (data_value incompatible with comparison) |
| `RuntimeError` | `AggregateTriggerRequirements.has_triggered` | When `aggregate_type` is not `ALL_OF` or `ANY_OF` |
| `RuntimeWarning` | `MeanReversionTriggerRequirements.has_triggered` | When `current_position` has an unexpected value (not 0, 1, or -1). Note: `raise RuntimeWarning(...)` will raise an exception, not emit a warning. |
| `RuntimeError` | `OrdersGeneratorTrigger.get_trigger_times` | Always (abstract method) |
| `RuntimeError` | `OrdersGeneratorTrigger.generate_orders` | Always (abstract method) |

## Edge Cases
- `PeriodicTriggerRequirements.trigger_dates` is a class-level mutable list `[]`, meaning all instances share the same list unless reassigned. This can cause cross-instance contamination if multiple PeriodicTriggerRequirements are created.
- `EventTriggerRequirements.trigger_dates` has the same shared mutable default issue.
- `DateTriggerRequirements.dates_from_datetimes` is also a class-level mutable list `[]`.
- `TriggerInfo.__eq__` uses `is` (identity), not `==` (equality). This works for `True`/`False` because Python interns small objects, but would fail for non-boolean comparisons.
- `NotTriggerRequirements.__setattr__` only calls `super().__setattr__` when `key == 'trigger'`. Setting any other attribute on an instance (e.g., `self.class_type`) will silently fail (the method returns `None` without calling super). This is compensated by the dataclass machinery calling `__setattr__` for all fields during `__init__`, but only the `trigger` field gets the super call.
- `MeanReversionTriggerRequirements.has_triggered` (line 368): The exit logic for `current_position == -1` checks `current_price > rolling_mean` -- the same condition as for `current_position == 1`. This means a short position exits when price goes above the mean (correct reversal logic).
- `MeanReversionTriggerRequirements` raises `RuntimeWarning` as an exception (via `raise`), not as a warning (via `warnings.warn`). This is unusual.
- `PortfolioTriggerRequirements.has_triggered` uses `len(backtest.portfolio_dict)` which counts the number of dates in the dict, not the number of instruments. This may be intentional (checking number of active dates) or a bug (if intent was to check number of instruments on a given date).
- `OrdersGeneratorTrigger.has_triggered` returns `TriggerInfo(False)` when `len(orders) == 0`, even though the trigger time matched. This means matching a trigger time but generating no orders is treated as "not triggered".
- The `PeriodicTriggerRequirements.has_triggered` method calls `self.trigger_dates.index(state)` twice, which is O(n) each time. For large date lists this is inefficient.

## Bugs Found
- **Line 269** (`NotTriggerRequirements.__setattr__`): The `super().__setattr__(key, value)` call is indented inside the `if key == 'trigger'` block. This means setting any attribute other than `'trigger'` on a `NotTriggerRequirements` instance is silently dropped. The dataclass `__init__` sets fields by calling `__setattr__`, so `class_type` and any other fields will not be stored. This is partially masked because `class_type` is a static_field default, but dynamically setting attributes after init will fail.
- **Line 375** (`MeanReversionTriggerRequirements`): `raise RuntimeWarning(...)` raises the warning as an exception rather than using `warnings.warn()`. This is likely unintentional -- RuntimeWarning is a warning class, not an error class. Raising it halts execution when it should likely just warn and return `TriggerInfo(False)`.

## Coverage Notes
- Branch count: ~80
- `check_barrier`: 6 branches (3 direction cases x pass/fail each)
- `TriggerInfo.__eq__`: 1 branch (identity comparison, always returns)
- `PeriodicTriggerRequirements.get_trigger_times`: 2 branches (trigger_dates empty / not empty)
- `PeriodicTriggerRequirements.has_triggered`: 4 branches (trigger_dates empty, state in dates, index != last, state not in dates)
- `IntradayTriggerRequirements.__post_init__`: 1 branch (while loop)
- `IntradayTriggerRequirements.has_triggered`: 1 branch (time in/not in list)
- `MktTriggerRequirements.has_triggered`: 2 branches (TypeError caught / not caught)
- `RiskTriggerRequirements.has_triggered`: 3 branches (state not in results, risk_transformation None / not None)
- `AggregateTriggerRequirements.__setattr__`: 2 branches (key == 'triggers' with all Trigger objects / otherwise)
- `AggregateTriggerRequirements.has_triggered`: ~8 branches (ALL_OF path: each trigger pass/fail + info_dict, ANY_OF path: each trigger + info_dict, else RuntimeError)
- `AggregateTriggerRequirements.calc_type`: 3 branches (path_dependent / semi / simple)
- `NotTriggerRequirements.__setattr__`: 2 branches (key == 'trigger' + isinstance Trigger / not)
- `NotTriggerRequirements.has_triggered`: 2 branches (inversion True/False)
- `DateTriggerRequirements.__post_init__`: 2 branches (entire_day True/False) + sub-branch per element (isinstance dt.datetime)
- `DateTriggerRequirements.has_triggered`: 5 branches (entire_day, isinstance state datetime, state in dates, index < last, not in dates)
- `PortfolioTriggerRequirements.has_triggered`: 4 branches (data_source == 'len', 3 direction sub-branches)
- `MeanReversionTriggerRequirements.has_triggered`: 7 branches (position==0 + z_score + price direction, position==1 + price, position==-1 + price, else)
- `TradeCountTriggerRequirements.has_triggered`: 4 branches (3 directions + fallthrough)
- `EventTriggerRequirements.__post_init__`: 1 branch (data_source is None)
- `EventTriggerRequirements.get_trigger_times`: 1 branch (trigger_dates empty)
- `EventTriggerRequirements.has_triggered`: 3 branches (state in dates, index < last, not in dates)
- `OrdersGeneratorTrigger.__post_init__`: 1 branch (not self.actions)
- `OrdersGeneratorTrigger.has_triggered`: 3 branches (time not in trigger_times, orders non-empty, orders empty)
- `StrategyRiskTrigger.risks`: 0 additional branches (just extends parent list)
- Mocking notes:
  - `PeriodicTriggerRequirements` needs `RelativeDateSchedule` mock for `apply_rule()`
  - `EventTriggerRequirements` needs `GsDataSource` / `Dataset` mock for lazy date loading
  - `MktTriggerRequirements` needs `DataSource.get_data()` mock
  - `RiskTriggerRequirements` needs backtest with mock `results` dict supporting `[state][risk].aggregate()` and `.transform().aggregate()`
  - `MeanReversionTriggerRequirements` needs `DataSource` mock for `get_data()` and `get_data_range()`
  - `AggregateTriggerRequirements` needs mock child triggers with `has_triggered()` and `calc_type`
  - `OrdersGeneratorTrigger` needs concrete subclass implementing `get_trigger_times()` and `generate_orders()`
  - Class-level `trigger_dates` and `dates_from_datetimes` lists should be reset between tests to avoid cross-test contamination
- Pragmas: none
