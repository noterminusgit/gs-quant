# data/stream.py

## Summary
Defines two lightweight data container classes for the streaming/event data layer: `DataSeries` (wraps a `pd.Series` along with its originating `DataCoordinate`) and `DataEvent` (represents a single timestamped data update with an optional coordinate). These types are used by the query and streaming infrastructure to carry data alongside its metadata.

## Dependencies
- Internal: `gs_quant.data` (`DataCoordinate`)
- External: `datetime` (`dt.datetime`)
- External: `typing` (`Union`)
- External: `pandas` (`pd.Series`)

## Type Definitions

### DataSeries (class)
Inherits: `object`

A container pairing a pandas Series with the data coordinate it was queried from.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `series` | `pd.Series` | (required) | The pandas Series containing the data values |
| `coordinate` | `DataCoordinate` | (required) | The data coordinate from which the series was obtained |

Both fields are public instance attributes.

### DataEvent (class)
Inherits: `object`

A container representing a single data update event at a specific point in time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `time` | `dt.datetime` | (required) | The timestamp of the data event |
| `value` | `Union[None, str, float]` | (required) | The event value; can be None, a string, or a float |
| `coordinate` | `DataCoordinate` | `None` | The optional data coordinate associated with this event |

All fields are public instance attributes.

## Enums and Constants

No enums or constants are defined in this module.

## Functions/Methods

### DataSeries.__init__(self, series: pd.Series, coordinate: DataCoordinate) -> None
Purpose: Initialize a DataSeries with a pandas Series and its corresponding coordinate.

**Algorithm:**
1. Set `self.series = series`.
2. Set `self.coordinate = coordinate`.

No branches. No validation.

---

### DataEvent.__init__(self, time: dt.datetime, value: Union[None, str, float], coordinate: DataCoordinate = None) -> None
Purpose: Initialize a DataEvent with a timestamp, value, and optional coordinate.

**Algorithm:**
1. Set `self.time = time`.
2. Set `self.value = value`.
3. Set `self.coordinate = coordinate`.

No branches. No validation.

## State Mutation
- `DataSeries`: `self.series` and `self.coordinate` are set in `__init__`. Both are public and mutable after construction; the class provides no protection against modification.
- `DataEvent`: `self.time`, `self.value`, and `self.coordinate` are set in `__init__`. All are public and mutable.
- Thread safety: No synchronization. These are simple data containers. Thread safety must be managed externally if instances are shared.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| (none) | -- | This module does not raise any exceptions |

Neither class performs any validation on its inputs.

## Edge Cases
- `DataSeries` with `series=None`: No validation prevents this. Downstream code that calls `.values`, `.index`, etc. on the series would fail with `AttributeError`.
- `DataEvent` with `value=None`: Explicitly allowed by the type signature (`Union[None, str, float]`). Consumers must handle None values.
- `DataEvent` with `coordinate=None` (the default): This is the expected case for events where the coordinate is not known or not relevant. Consumers should check for None before accessing coordinate attributes.
- `DataEvent.value` type is `Union[None, str, float]`: In Elixir, this maps to `nil | String.t() | float()`. Note that integer values are not in the union -- only `float` is specified. Python does not enforce this at runtime, but the type annotation excludes `int`.
- Neither class defines `__eq__`, `__hash__`, or `__repr__`. Default object identity semantics apply for equality and hashing.

## Coverage Notes
- Branch count: 0 (no conditional logic)
- Both classes are pure data containers with no branching in their `__init__` methods.
- Full coverage requires only one instantiation of each class with representative arguments.
- No pragmas or excluded lines.
