# data/query.py

## Summary
Defines `DataQuery`, a class that encapsulates a query against a `DataCoordinate` with start/end bounds and a query type (RANGE or LAST). Provides methods to execute the query and return results as a `pd.Series` or a `DataSeries` wrapper. Also defines the `DataQueryType` enum for distinguishing between range queries and last-value queries.

## Dependencies
- Internal: `gs_quant.data` (`DataCoordinate`)
- Internal: `gs_quant.data.coordinate` (`DateOrDatetime` -- type alias `Union[dt.date, dt.datetime]`)
- Internal: `gs_quant.datetime.relative_date` (`RelativeDate`)
- Internal: `.stream` (`DataSeries`)
- External: `enum` (`Enum`)
- External: `typing` (`Union`)
- External: `pandas` (`pd.Series`)

## Type Definitions

### DateOrDatetime (type alias, imported from coordinate)
```
DateOrDatetime = Union[dt.date, dt.datetime]
```

### DataQueryType (Enum)
Inherits: `enum.Enum`

| Member | Value (str) | Description |
|--------|-------------|-------------|
| LAST | `"LAST"` | Query for the last/most recent value |
| RANGE | `"RANGE"` | Query for a range of values between start and end |

### DataQuery (class)
Inherits: `object`

Encapsulates a query against a data coordinate.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `coordinate` | `DataCoordinate` | (required) | The data coordinate to query |
| `start` | `Union[DateOrDatetime, RelativeDate, None]` | `None` | Start of the query range |
| `end` | `Union[DateOrDatetime, RelativeDate, None]` | `None` | End of the query range |
| `query_type` | `DataQueryType` | `DataQueryType.RANGE` | Type of query to execute |

All fields are public instance attributes set in `__init__`.

## Enums and Constants

See `DataQueryType` in Type Definitions above. No module-level constants.

## Functions/Methods

### DataQuery.__init__(self, coordinate: DataCoordinate, start: Union[DateOrDatetime, RelativeDate] = None, end: Union[DateOrDatetime, RelativeDate] = None, query_type: DataQueryType = DataQueryType.RANGE) -> None
Purpose: Initialize a data query with a coordinate, optional start/end bounds, and query type.

**Algorithm:**
1. Set `self.coordinate = coordinate`.
2. Set `self.start = start`.
3. Set `self.end = end`.
4. Set `self.query_type = query_type`.

No branches. No validation.

---

### DataQuery.get_series(self) -> Union[pd.Series, None]
Purpose: Execute the query against the coordinate and return the result as a pandas Series.

**Algorithm:**
1. Branch: `self.query_type is DataQueryType.RANGE` -> return `self.coordinate.get_series(self.start, self.end)`.
2. Branch: `self.query_type is DataQueryType.LAST` -> return `self.coordinate.last_value(self.end)`.
3. Implicit: if `query_type` is neither RANGE nor LAST, returns `None` (implicit function return).

**Returns:** `pd.Series` for RANGE queries, `Union[float, None]` for LAST queries (delegated to `coordinate.last_value`), or `None` if query_type is unrecognized.

---

### DataQuery.get_data_series(self) -> DataSeries
Purpose: Execute the query and wrap the result in a `DataSeries`.

**Algorithm:**
1. Call `self.get_series()` to get the raw series.
2. Wrap in `DataSeries(series_result, self.coordinate)`.
3. Return the `DataSeries`.

No branches.

---

### DataQuery.get_range_string(self) -> str
Purpose: Return a string representation of the query's start/end range.

**Algorithm:**
1. Return `f'start={self.start}|end={self.end}'`.

No branches.

## State Mutation
- `self.coordinate`, `self.start`, `self.end`, `self.query_type`: Set in `__init__`. All are public attributes and could theoretically be mutated externally, but the class does not mutate them after construction.
- Thread safety: No shared state. Instance attributes are not protected. Not inherently thread-safe if shared across threads.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none directly) | -- | This module does not explicitly raise exceptions |

Note: `coordinate.get_series()` and `coordinate.last_value()` may raise exceptions from the data layer, but `DataQuery` does not catch or transform them.

## Edge Cases
- `get_series()` with an unrecognized `query_type`: If someone sets `query_type` to a value that is neither `RANGE` nor `LAST` (e.g., a different enum or a string), neither `if` branch triggers and the method returns `None` implicitly.
- `get_series()` with `DataQueryType.LAST`: Passes `self.end` to `coordinate.last_value()`, ignoring `self.start`. The `start` parameter is meaningless for LAST queries.
- `get_data_series()` passes the result of `get_series()` directly to `DataSeries`, which could be `None` if the query type is unrecognized or if the coordinate returns no data.
- `get_range_string()` uses Python's default `str()` on `self.start` and `self.end`, so the output format depends on the type of those objects (e.g., `datetime.__str__()` vs `RelativeDate.__str__()`).
- The `start` and `end` parameters accept `Union[DateOrDatetime, RelativeDate]`, but there is no validation that these types are actually passed. Any object will be stored.

## Coverage Notes
- Branch count: 4
  - `get_series`: 3 branches (RANGE / LAST / implicit None return for unrecognized type)
  - `get_data_series`: 1 path
  - `get_range_string`: 1 path
  - `__init__`: 1 path
- The implicit `None` return when `query_type` is neither RANGE nor LAST is a potential untested branch. In practice, since `DataQueryType` only has two members and the default is RANGE, this branch is unlikely to be exercised.
