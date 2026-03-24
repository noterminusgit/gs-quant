# gscalendar.py

## Summary
Provides the `GsCalendar` class, which fetches and caches holiday calendars for given currencies and exchanges from GS datasets. It supports creating numpy `busdaycalendar` objects for business-day arithmetic. A module-level helper `_split_list` partitions items into currency and exchange tuples. Caching is TTL-based (via `cachetools`) with thread-safe locks.

## Dependencies
- Internal: `gs_quant.common` (PricingLocation, Currency), `gs_quant.data` (Dataset), `gs_quant.errors` (MqRequestError)
- External: `datetime` (date), `logging`, `enum` (Enum, EnumMeta), `threading` (Lock), `typing` (Tuple, Union, List), `numpy` (busdaycalendar, datetime64), `cachetools` (TTLCache, cached), `cachetools.keys` (hashkey)

## Type Definitions

### GsCalendar (class)
Inherits: `object`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__calendars` | `Tuple[Union[str, PricingLocation, Currency], ...]` | `()` | Tuple of calendar identifiers (currencies and/or exchanges) |
| `__business_day_calendars` | `dict` | `{}` | Cache of `np.busdaycalendar` instances keyed by `week_mask` |
| `_skip_valid_check` | `bool` | `True` | When True, invalid calendars produce a warning instead of raising |

### Class Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `DATE_LOW_LIMIT` | `dt.date` | `dt.date(1952, 1, 1)` | Earliest date for holiday data queries |
| `DATE_HIGH_LIMIT` | `dt.date` | `dt.date(2052, 12, 31)` | Latest date for holiday data queries |
| `DEFAULT_WEEK_MASK` | `str` | `'1111100'` | Mon-Fri business days, Sat-Sun weekends |

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `_logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |
| `_calendar_cache` | `TTLCache` | `TTLCache(maxsize=128, ttl=600)` | 10-minute TTL cache for holiday results |
| `_coverage_cache` | `TTLCache` | `TTLCache(maxsize=128, ttl=3600)` | 1-hour TTL cache for dataset coverage results |

## Functions/Methods

### _split_list(items, predicate) -> Tuple[Tuple[str, ...], Tuple[str, ...]]
Purpose: Partition a list of calendar identifiers into two tuples based on a predicate (e.g., is_currency), converting each item to its string representation.

**Algorithm:**
1. Initialize `true_res = []` and `false_res = []`.
2. For each `item` in `items`:
   - Branch: `isinstance(item, (Enum, EnumMeta))` -> `item_str = item.value`.
   - Else -> `item_str = item.upper()`.
   - Branch: `predicate(item)` is True -> append `item_str` to `true_res`.
   - Else -> append `item_str` to `false_res`.
3. Return `(tuple(true_res), tuple(false_res))`.

### GsCalendar.__init__(self, calendars: Union[str, PricingLocation, Currency, Tuple[str, ...]] = (), skip_valid_check=True)
Purpose: Initialize the calendar with one or more calendar identifiers.

**Algorithm:**
1. Branch: `isinstance(calendars, (str, PricingLocation, Currency))` -> wrap in tuple: `calendars = (calendars,)`.
2. Branch: `calendars is None` -> set to empty tuple `()`.
3. Store `self.__calendars = calendars`.
4. Initialize `self.__business_day_calendars = {}`.
5. Store `self._skip_valid_check = skip_valid_check`.

### GsCalendar.get(calendars: Union[str, Tuple], skip_valid_check=True) -> GsCalendar [staticmethod]
Purpose: Factory method; creates and returns a new `GsCalendar` instance.

**Algorithm:**
1. Return `GsCalendar(calendars, skip_valid_check)`.

### GsCalendar.reset() [staticmethod]
Purpose: Clear the module-level holiday cache.

**Algorithm:**
1. Call `_calendar_cache.clear()`.

### GsCalendar.calendars(self) -> Tuple
Purpose: Accessor for the private `__calendars` field.

**Algorithm:**
1. Return `self.__calendars`.

### GsCalendar.is_currency(currency: Union[str, PricingLocation, Currency]) -> bool [staticmethod]
Purpose: Determine whether a calendar identifier represents a currency (as opposed to an exchange).

**Algorithm:**
1. Branch: `isinstance(currency, Currency)` -> return `True`.
2. Branch: `isinstance(currency, PricingLocation)` -> return `False`.
3. Else -> try `Currency(currency.upper())`:
   - Branch: succeeds -> return `True`.
   - Branch: `ValueError` or `AttributeError` -> return `False`.

### GsCalendar._get_dataset_coverage(self, dataset: Dataset, query_key: str) -> set
Purpose: Fetch and cache the set of valid values for a given query key from a dataset's coverage.

**Algorithm:**
1. Decorated with `@cached(_coverage_cache, key=lambda s, d, q: d.id, lock=Lock())`.
2. Call `dataset.get_coverage()` -> `coverage_df`.
3. Branch: `coverage_df.empty` -> return `set()`.
4. Else -> return `set(coverage_df[query_key])`.

### GsCalendar.holidays_from_dataset(self, dataset: Dataset, query_key: str, query_values: Tuple[str, ...]) -> List[dt.date]
Purpose: Fetch holiday dates from a dataset for given query values, with validation against dataset coverage.

**Algorithm:**
1. Branch: `len(query_values)` is 0 (falsy) -> return `[]`.
2. Get coverage set via `_get_dataset_coverage(dataset, query_key)`.
3. For each `item` in `query_values`:
   - Branch: `item not in coverage`:
     - Branch: `self._skip_valid_check` is True -> log warning.
     - Else -> raise `ValueError(f'Invalid calendar {item}')`.
4. Try: call `dataset.get_data(**{query_key: query_values}, start=DATE_LOW_LIMIT, end=DATE_HIGH_LIMIT)`.
   - Branch: `data` is not empty -> return list of `d.date()` for each datetime in `data.index`.
5. Branch: `MqRequestError` caught -> pass (fall through).
6. Return `[]`.

**Raises:** `ValueError` when `_skip_valid_check` is False and an item is not in coverage.

### GsCalendar.holidays (property) -> Tuple[dt.date, ...]
Purpose: Compute and cache the combined holiday tuple for all configured calendars.

**Algorithm:**
1. Decorated with `@cached(_calendar_cache, key=lambda s: hashkey(str(s.__calendars)), lock=Lock())`.
2. Call `_split_list(self.__calendars, GsCalendar.is_currency)` -> `(currencies, exchanges)`.
3. Fetch exchange holidays: `holidays_from_dataset(Dataset(Dataset.GS.HOLIDAY), 'exchange', exchanges)`.
4. Fetch currency holidays: `holidays_from_dataset(Dataset(Dataset.GS.HOLIDAY_CURRENCY), 'currency', currencies)`.
5. Concatenate, deduplicate via `set()`, convert to tuple.
6. Return the tuple.

### GsCalendar.business_day_calendar(self, week_mask: str = None) -> np.busdaycalendar
Purpose: Get or create a numpy business-day calendar for the given week mask.

**Algorithm:**
1. Use `self.__business_day_calendars.setdefault(week_mask, ...)` to lazily create.
2. If creating: `np.busdaycalendar(weekmask=week_mask or DEFAULT_WEEK_MASK, holidays=tuple([np.datetime64(d.isoformat()) for d in self.holidays]))`.
3. Return the cached or newly-created calendar.

## State Mutation
- `_calendar_cache` (module-level): Populated by `holidays` property on first access per unique calendar tuple; cleared by `reset()`. TTL of 600 seconds.
- `_coverage_cache` (module-level): Populated by `_get_dataset_coverage` on first call per dataset id; TTL of 3600 seconds.
- `self.__business_day_calendars`: Populated lazily by `business_day_calendar()`; persists for instance lifetime.
- Thread safety: Both module-level caches use `Lock()` for thread-safe access. The `__business_day_calendars` dict is not thread-safe (no lock).

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `holidays_from_dataset` | When `_skip_valid_check=False` and a query value is not in dataset coverage |
| `MqRequestError` (caught) | `holidays_from_dataset` | When dataset `get_data` call fails; silently returns `[]` |

## Edge Cases
- `__init__` with `calendars=None`: the `None` check comes after the `isinstance` check, so `None` is not a str/PricingLocation/Currency and falls through to `calendars is None` branch, correctly resetting to `()`.
- `__init__` with a single string: wrapped into a one-element tuple.
- `_split_list` with an `EnumMeta` instance: uses `.value` attribute. This is unusual since `EnumMeta` is a metaclass, not an enum member. Could cause `AttributeError` if a raw metaclass is passed.
- `holidays` property returns a tuple (not a list), but individual `holidays_from_dataset` calls return lists; the property concatenates them, deduplicates via `set()`, and converts to tuple.
- `holidays_from_dataset` with all items invalid when `_skip_valid_check=True`: logs warnings for each item but still attempts the dataset query, which may return empty.
- `business_day_calendar` with `week_mask=None`: uses `DEFAULT_WEEK_MASK` ('1111100'). The dict key will be `None`, so subsequent calls with `None` will reuse the cached calendar.
- Cache key for `holidays` uses `str(s.__calendars)` which means different tuple orderings of the same calendars produce different cache entries and potentially different holiday tuples (though contents are the same after dedup).

## Coverage Notes
- Branch count: ~22
- Key branches: `__init__` has 2 type-check branches (isinstance, is None). `is_currency` has 4 branches (Currency, PricingLocation, successful Currency parse, ValueError/AttributeError). `_split_list` has 2 type branches (Enum vs not) and 2 predicate branches (true vs false) per item. `holidays_from_dataset` has 5 branches (empty query_values, item not in coverage with skip_valid_check true/false, data not empty, MqRequestError caught).
- Pragmas: none observed.
