# tracing/tracing.py

## Summary
Comprehensive distributed tracing module built on OpenTelemetry. Provides wrapper classes around OpenTelemetry spans, events, and contexts to offer a simplified tracing API for gs_quant. Includes span collection (`SpanConsumer`), context propagation (`TracingContext`, `TracingScope`), a rich span wrapper hierarchy (`TracingSpan`, `NonRecordingTracingSpan`, `TransportableSpan`), a tracer factory with lazy initialization (`TracerFactory`), and the main `Tracer` context manager/decorator class with visualization (`plot`), printing, and Jupyter magic cell integration.

## Dependencies
- Internal: `gs_quant.errors` (MqWrappedError), `gs_quant_internal.tracing.jupyter` (optional, imported at module level with fallback)
- External: `datetime`, `logging`, `traceback`, `contextlib` (ContextDecorator), `enum` (Enum), `typing`, `pandas`, `opentelemetry.trace`, `opentelemetry.context`, `opentelemetry.propagate` (extract, inject, set_global_textmap), `opentelemetry.propagators.textmap` (TextMapPropagator), `opentelemetry.sdk.trace` (TracerProvider, SynchronousMultiSpanProcessor, ReadableSpan, Span, Event, SpanProcessor), `opentelemetry.sdk.trace.export` (SimpleSpanProcessor, SpanExporter), `opentelemetry.trace` (format_trace_id, format_span_id, INVALID_SPAN), `plotly.express` (optional, for visualization), `IPython` (optional, for Jupyter magic)

## Type Definitions

### Tags (Enum)
See Enums section below.

### SpanConsumer (class)
Inherits: SpanExporter

Singleton span exporter that collects finished spans into an in-memory list for later retrieval, printing, or plotting.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _instance | `SpanConsumer` (class-level) | `None` | Singleton instance |
| _collected_spans | `list[TracingSpan]` | `[]` | Accumulated spans from exports and manual recording |

### TracingContext (class)
Inherits: object

Thin wrapper around an OpenTelemetry `SpanContext`, used for cross-process context propagation.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _context | `SpanContext` | (required) | The underlying OpenTelemetry span context |

### TracingEvent (class)
Inherits: object

Wrapper around an OpenTelemetry `Event` providing typed property access.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _event | `Event` | (required) | The underlying OpenTelemetry event |

### TracingScope (class)
Inherits: object

Context manager that associates a span with the current OpenTelemetry context and detaches on close. Supports exception recording on `__exit__`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _token | `object` | (required) | Context attach token for detaching |
| _span | `TracingSpan` | (required) | The wrapped span (or `NonRecordingTracingSpan` if span is None) |
| _finish_on_close | `bool` | `True` | Whether to call `span.end()` on close |

### TracingSpan (class)
Inherits: object

Primary wrapper around an OpenTelemetry `Span`, providing a rich property interface for trace/span IDs, timing, tags, events, and mutation (set_tag, add_event, log_kv).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _span | `Span` | (required) | The underlying OpenTelemetry span |
| _endpoint | `str` | `None` | Optional endpoint label for transportable span creation |

### NonRecordingTracingSpan (class)
Inherits: TracingSpan

Null-object pattern span that returns safe defaults and performs no recording. Used when no active span exists.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _span | `Span` | `INVALID_SPAN` | The underlying invalid/non-recording span |

### TransportableSpan (class)
Inherits: TracingSpan

Picklable representation of a finished span. Copies all data from a `TracingSpan` into plain Python fields so it can be serialized across process boundaries.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _operation_name | `str` | (from source span) | Span operation name |
| _trace_id | `str` | (from source span) | Hex trace ID |
| _span_id | `str` | (from source span) | Hex span ID |
| _parent_id | `Optional[str]` | (from source span) | Parent span ID or None |
| _tags | `dict` | (from source span) | Span attributes/tags |
| _start_time | `int` | (from source span) | Start time in nanoseconds |
| _end_time | `Optional[int]` | (from source span) | End time in nanoseconds |
| _events | `tuple[TransportableTracingEvent]` | (from source span) | Serializable event copies |
| _endpoint | `str` | (from source span or override) | Endpoint label |

### TransportableTracingEvent (class)
Inherits: TracingEvent

Picklable copy of a `TracingEvent`, storing name, timestamp, and attributes as plain Python values.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| _name | `str` | (from source event) | Event name |
| _timestamp | `int` | (from source event) | Timestamp in nanoseconds |
| _attributes | `dict` | (from source event) | Event attributes |

### TracerFactory (class)
Inherits: object

Lazy-initializing factory for the OpenTelemetry `Tracer` instance. Configures the `TracerProvider` with span processors on first call.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __tracer_instance | `OtelTracer` (class-level) | `None` | Singleton tracer instance |
| _extra_span_processors | `list[SpanProcessor]` (class-level) | `[]` | Pre-registered extra span processors |

### Tracer (class)
Inherits: ContextDecorator

Main tracing API. Can be used as a context manager (`with Tracer('label'):`) or as a decorator (`@Tracer('label')`). Provides static methods for all tracing operations: span management, context propagation, exception recording, visualization, and data gathering.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __factory | `TracerFactory` (class-level) | `TracerFactory()` | Factory for obtaining the OTel tracer |
| __print_on_exit | `bool` | `False` | Whether to print trace on exit |
| __label | `str` | `'Execution'` | Span operation name |
| __threshold | `int` | `None` | Duration threshold (unused in current code) |
| wrap_exceptions | `bool` | `False` | Whether to wrap exceptions in `MqWrappedError` |
| _parent_span | `Optional[TracingSpan]` | `None` | Explicit parent span (if TracingSpan) |
| _parent_ctx | `Optional[TracingContext]` | `None` | Explicit parent context (if TracingContext) |

## Enums and Constants

### Tags(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| HTTP_METHOD | `'http.method'` | HTTP request method tag |
| HTTP_URL | `'http.url'` | HTTP request URL tag |
| HTTP_STATUS_CODE | `'http.status_code'` | HTTP response status code tag |
| CONTENT_LENGTH | `'content.length'` | Response content length tag |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |
| NOOP_TRACING_SCOPE | `TracingScope` | `TracingScope(None, None)` | Singleton no-op scope returned when no active span exists |

## Functions/Methods

### SpanConsumer.get_instance() -> SpanConsumer (staticmethod)
Purpose: Return the singleton SpanConsumer, creating it on first call.

**Algorithm:**
1. Branch: `_instance` is None -> create new `SpanConsumer()`
2. Return `_instance`

### SpanConsumer.get_spans() -> Sequence[TracingSpan] (staticmethod)
Purpose: Return all collected spans from the singleton.

### SpanConsumer.reset() (staticmethod)
Purpose: Clear all collected spans.

### SpanConsumer.manually_record(spans: Sequence[TracingSpan]) (staticmethod)
Purpose: Manually append spans to the collection (e.g., from deserialized TransportableSpans).

### SpanConsumer.export(self, spans: Sequence[ReadableSpan]) -> None
Purpose: OpenTelemetry exporter callback; wraps each `ReadableSpan` in a `TracingSpan` and appends.

### TracingEvent.name (property) -> str
Purpose: Return event name.

### TracingEvent.timestamp (property) -> int
Purpose: Return timestamp in nanoseconds.

### TracingEvent.timestamp_sec (property) -> float
Purpose: Return timestamp in seconds (ns / 1e9).

### TracingEvent.attributes (property) -> Mapping[str, any]
Purpose: Return event attributes dict.

### TracingScope.__enter__(self) -> TracingScope
Purpose: Enter the context manager, returning self.

### TracingScope.__exit__(self, exc_type, exc_val, exc_tb)
Purpose: Record exception on the span if present, then close.

**Algorithm:**
1. Branch: `exc_val` is truthy -> call `Tracer.record_exception(exc_val, self._span, exc_tb)`
2. Call `self.close()`

### TracingScope.close(self)
Purpose: Detach the context token and optionally end the span.

**Algorithm:**
1. Branch: `self._token` is truthy -> detach context
   - Branch: `self._finish_on_close` -> call `self._span.end()`

### TracingSpan.unwrap(self) -> Span
Purpose: Return the underlying OpenTelemetry Span.

### TracingSpan.context (property) -> TracingSpan
Purpose: Return `self` (for API compatibility with OpenTelemetry span context access).

### TracingSpan.end(self)
Purpose: End the underlying span.

### TracingSpan.is_recording(self) -> bool
Purpose: Check if the underlying span is recording.

### TracingSpan.operation_name (property) -> str
Purpose: Return the span's name.

### TracingSpan.transportable(self, endpoint_override: Optional[str] = None) -> TransportableSpan
Purpose: Create a picklable copy of this span with optional endpoint override.

### TracingSpan.endpoint (property getter/setter) -> str
Purpose: Get/set the endpoint label.

### TracingSpan.trace_id (property) -> str
Purpose: Return the hex-formatted trace ID.

### TracingSpan.is_error(self) -> bool
Purpose: Check if span has `error` attribute set to True.

### TracingSpan.start_time (property) -> int
Purpose: Return span start time in nanoseconds.

### TracingSpan.end_time (property) -> Optional[int]
Purpose: Return span end time in nanoseconds.

### TracingSpan.duration (property) -> float
Purpose: Return duration in milliseconds, or None if span not finished.

**Algorithm:**
1. Branch: `end_time` exists -> `(end_time - start_time) / 1e6`
2. Branch: no `end_time` -> return `None`

### TracingSpan.span_id (property) -> str
Purpose: Return the hex-formatted span ID.

### TracingSpan.parent_id (property) -> Optional[str]
Purpose: Return the hex-formatted parent span ID, or None if no parent.

**Algorithm:**
1. Branch: `parent` exists -> `format_span_id(parent.span_id)`
2. Branch: no parent -> return `None`

### TracingSpan.tags (property) -> Mapping[str, any]
Purpose: Return span attributes.

### TracingSpan.events (property) -> Sequence[TracingEvent]
Purpose: Return span events wrapped as `TracingEvent` instances.

### TracingSpan.set_tag(self, key: Union[Enum, str], value: Union[bool, str, bytes, int, float, dt.date]) -> TracingSpan
Purpose: Set a tag/attribute on the span with type coercion.

**Algorithm:**
1. Branch: `value` is None -> return self (no-op)
2. Branch: `value` is `dt.date` -> convert to ISO format string
3. Branch: `value` is `Enum` -> use `.value`
4. Branch: `key` is `Enum` -> use `.value`
5. Call `self._span.set_attribute(key, value)`
6. Return self (fluent interface)

### TracingSpan.add_event(self, name, attributes, timestamp) -> TracingSpan
Purpose: Add an event to the span.

**Algorithm:**
1. Branch: `timestamp` is truthy -> convert seconds to nanoseconds (`int(timestamp * 1e9)`)
2. Branch: `timestamp` is falsy -> pass `None`
3. Call `self._span.add_event(name, attributes, converted_timestamp)`

### TracingSpan.log_kv(self, key_values, timestamp) -> TracingSpan
Purpose: Log key-value pairs as an event (OpenTracing compatibility).

**Algorithm:**
1. Branch: `timestamp` is truthy -> convert to ns
2. Branch: `key_values` is None or missing `"event"` key -> event_name = `"log"`; else use `key_values["event"]`
3. Call `self._span.add_event(event_name, key_values, converted_timestamp)`

### NonRecordingTracingSpan (overrides)
Purpose: Null-object overrides returning safe defaults.

- `end()`: no-op
- `operation_name`: returns `"Non-Recording Span"`
- `parent_id`: returns `None`
- `tags`: returns empty dict
- `start_time`: returns `0`
- `end_time`: returns `None`
- `duration`: returns `0`

### TransportableSpan (overrides)
Purpose: Picklable span representation with pre-computed fields.

- `transportable(endpoint_override)`: Branch: no override or same endpoint -> return self; else create new `TransportableSpan`
- `end()`: no-op
- `is_recording()`: returns `self._end_time is None`
- `set_tag()`, `add_event()`, `log_kv()`: no-ops returning self
- All property overrides return the stored plain-Python fields

### TransportableTracingEvent (overrides)
Purpose: Picklable event with pre-computed fields. Overrides all properties to return stored values.

### TracerFactory.get(self) -> OtelTracer
Purpose: Lazily create and return the singleton OpenTelemetry tracer.

**Algorithm:**
1. Branch: `__tracer_instance` is None:
   a. Create `SynchronousMultiSpanProcessor`
   b. Add `SimpleSpanProcessor(SpanConsumer.get_instance())`
   c. Add each extra span processor from `_extra_span_processors`
   d. Set global `TracerProvider`
   e. Get tracer via `trace.get_tracer(__name__)`
   f. Store as `__tracer_instance`
2. Return `__tracer_instance`

### TracerFactory.preregister_span_processor(processor: SpanProcessor) (staticmethod)
Purpose: Register an additional span processor before the tracer is created.

**Algorithm:**
1. Branch: tracer already created -> log error, do nothing
2. Branch: tracer not created -> append processor to `_extra_span_processors`

### Tracer.__init__(self, label, print_on_exit, threshold, wrap_exceptions, parent_span)
Purpose: Configure a Tracer context manager.

**Algorithm:**
1. Store label, print_on_exit, threshold, wrap_exceptions
2. Branch: `parent_span` is `TracingSpan` -> store as `_parent_span`
3. Branch: `parent_span` is `TracingContext` -> store as `_parent_ctx`

### Tracer.__enter__(self) -> TracingScope
Purpose: Enter the tracing context.

**Algorithm:**
1. Branch: `_parent_span` exists -> activate it via `Tracer.activate_span`
2. Start a new active span with `self.__label`, optionally child_of `_parent_ctx`
3. Return the scope

### Tracer.__exit__(self, exc_type, exc_value, exc_tb)
Purpose: Exit the tracing context, recording exceptions and optionally wrapping them.

**Algorithm:**
1. Branch: `exc_value` exists -> record exception on scope's span
2. Close the scope
3. Branch: `_parent_span` exists -> close the parent scope
4. Branch: `wrap_exceptions` and exception is not `MqWrappedError` -> raise `MqWrappedError` wrapping original

### Tracer.get_instance() -> OtelTracer (staticmethod)
Purpose: Return the OTel tracer from the factory.

### Tracer.set_factory(factory: TracerFactory) (staticmethod)
Purpose: Replace the tracer factory (for testing).

### Tracer.active_span() -> Union[TracingSpan, NonRecordingTracingSpan] (staticmethod)
Purpose: Return the currently active span.

**Algorithm:**
1. Branch: current span is None or not recording -> return `NonRecordingTracingSpan`
2. Branch: valid recording span -> return `TracingSpan(current_span)`

### Tracer.set_propagator_format(propagator_format: TextMapPropagator) (staticmethod)
Purpose: Set the global text map propagator for context propagation.

### Tracer.inject(carrier) (staticmethod)
Purpose: Inject trace context into a carrier (dict/headers).

**Algorithm:**
1. Branch: current span is recording -> call `inject(carrier)`
2. Branch: exception during injection -> log error

### Tracer.extract(carrier) -> TracingContext (staticmethod)
Purpose: Extract trace context from a carrier.

**Algorithm:**
1. Try `extract(carrier)` -> wrap in `TracingContext`
2. Branch: exception -> log error (returns None implicitly)

### Tracer.activate_span(span, finish_on_close=False) -> Optional[TracingScope] (staticmethod)
Purpose: Make a span the current active span.

**Algorithm:**
1. Branch: span is None or not recording -> return `NOOP_TRACING_SCOPE`
2. Set span in context, attach, return `TracingScope`

### Tracer.start_active_span(operation_name, child_of, ignore_active_span, finish_on_close=True) -> TracingScope (staticmethod)
Purpose: Start a new span and make it the active span.

**Algorithm:**
1. Branch: `ignore_active_span` -> use empty `Context()`
2. Branch: `child_of` provided -> use `child_of._context`
3. Branch: neither -> use `None` (inherit from current context)
4. Start span, set in context, attach, return `TracingScope`

### Tracer.record_exception(e, span=None, exc_tb=None) (staticmethod)
Purpose: Record an exception as an error event on a span.

**Algorithm:**
1. Default span to current span if not provided
2. Set `error=True` tag
3. Log key-values: event name, message, error object, kind, formatted traceback
4. Silently swallow any exception during recording

### Tracer.__format_traceback(exc_type, exc_value, exc_tb=None) -> str (staticmethod, private)
Purpose: Format a traceback to string, limited to 10 frames.

**Algorithm:**
1. Branch: `exc_value` is None -> return empty string
2. Try `traceback.format_exception()` with limit=10
3. Branch: exception during formatting -> return empty string

### Tracer.reset() (staticmethod)
Purpose: Clear all collected spans via `SpanConsumer.reset()`.

### Tracer.get_spans() -> Sequence[TracingSpan] (staticmethod)
Purpose: Return all collected spans via `SpanConsumer.get_spans()`.

### Tracer.plot(reset=False, show=True) (staticmethod)
Purpose: Visualize the trace as a Plotly timeline chart.

**Algorithm:**
1. Branch: `plotly` not installed -> log warning, fall back to `Tracer.print(reset)`, return
2. Call `gather_data(as_string=False)` to get ordered spans with depth
3. Build a `pd.DataFrame` with id, operation, start, end, tags columns
4. Build color map (5 shades of blue cycling by depth, red for errors)
5. Create `px.timeline` figure
6. Branch: `reset` -> call `Tracer.reset()`
7. Branch: `show` -> call `fig.show()`; else return `fig`

### Tracer.gather_data(as_string=True, root_id=None, trace_id=None) -> Tuple[Union[str, list], float] (staticmethod)
Purpose: Build an ordered tree of spans for display.

**Algorithm:**
1. Get all spans, build `spans_by_parent` dict (reversed order)
2. Branch: `trace_id` provided -> filter spans not matching trace_id
3. Define recursive `_build_tree(parent_span, depth)`:
   - Branch: `as_string` -> format as indented text with elapsed ms and error marker
   - Branch: not `as_string` -> append `(depth, span)` tuple
   - Recurse into children
4. Iterate root spans (those with `parent_id == root_id`, default `None`)
5. Branch: `as_string` -> join lines with newlines, return `(string, total_ms)`
6. Branch: not `as_string` -> return `(list_of_tuples, total_ms)`

### Tracer.print(reset=True, root_id=None, trace_id=None) -> Tuple[str, float] (staticmethod)
Purpose: Print the trace tree to the logger at WARNING level.

**Algorithm:**
1. Call `gather_data()` to get string representation and total
2. Construct identifier string from trace_id or root_id
3. Log formatted output
4. Branch: `reset` -> call `Tracer.reset()`
5. Return `(tracing_str, total)`

### Tracer.in_scope(func, operation_name='callback') -> Callable (staticmethod)
Purpose: Wrap a callback to preserve the current tracing context across thread/future boundaries.

**Algorithm:**
1. Inject current span context into a carrier dict
2. Return a wrapper function that:
   a. Extracts the context from the carrier
   b. Enters a `Tracer` context manager with the extracted parent context
   c. Calls the original function

### parse_tracing_line_args(line: str) -> Tuple[Optional[str], bool]
Purpose: Parse Jupyter magic `%%trace` line arguments.

**Algorithm:**
1. Split line by spaces, filter empty strings
2. Branch: first token is `'chart'`, `'plot'`, or `'graph'` -> return remaining args as tuple (or None), `True` (show chart)
3. Branch: otherwise -> return stripped tokens as tuple (or None), `False`

### trace_ipython_cell(line, cell) (Jupyter cell magic, conditionally registered)
Purpose: IPython `%%trace` cell magic that wraps cell execution in a tracer.

**Algorithm:**
1. Parse line args via `parse_tracing_line_args`
2. Branch: `cell` is None -> return `line`
3. Execute cell within `Tracer(label=span_name)` context
4. Branch: execution error -> record exception
5. Branch: `show_chart` -> call `Tracer.plot(True)`; else `Tracer.print(True)`
6. Return `None`

## State Mutation
- `SpanConsumer._instance`: Singleton, created on first `get_instance()` call.
- `SpanConsumer._collected_spans`: Mutated by `export()`, `manually_record()`, and `reset()`.
- `TracerFactory.__tracer_instance`: Singleton, created on first `get()` call. Also sets global `TracerProvider`.
- `TracerFactory._extra_span_processors`: Modified by `preregister_span_processor()` before tracer creation.
- `Tracer.__factory`: Class-level, replaceable via `set_factory()`.
- OpenTelemetry global state: `trace.set_tracer_provider()` and `set_global_textmap()` modify OTel process-wide globals.
- Thread safety: `SpanConsumer._collected_spans` is a plain list with no locking -- concurrent `export()` calls from multiple threads could corrupt it. `TracerFactory.get()` has a race condition on `__tracer_instance` check-then-set. Both are typically single-threaded in practice.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqWrappedError` | `Tracer.__exit__` | When `wrap_exceptions=True` and an exception (not already `MqWrappedError`) occurs in the traced block |
| `ImportError` | module-level | `plotly` not installed (caught, falls back to print) |
| `ImportError` | module-level | `gs_quant_internal.tracing.jupyter` not installed (caught, falls back to vanilla IPython magic) |
| `Exception` | module-level | IPython not available (caught, Jupyter magic not registered) |

## Edge Cases
- `NOOP_TRACING_SCOPE` is constructed with `TracingScope(None, None)` which creates a `NonRecordingTracingSpan(INVALID_SPAN)` since span is None.
- `Tracer.extract()` can return `None` if extraction fails (exception logged but no explicit return in except block).
- `TransportableSpan.transportable()` with same endpoint returns `self` (avoids unnecessary copy).
- `TracingSpan.set_tag` with `value=None` is a no-op (returns self without setting anything).
- `NonRecordingTracingSpan.duration` returns `0` (not `None`), unlike `TracingSpan.duration` which returns `None` when unfinished.
- `gather_data` with `root_id=None` looks for spans whose `parent_id` is `None` (true root spans).
- `parse_tracing_line_args("")` returns `((), False)` since `stripped` is an empty tuple, and `len(())` is 0.
  Wait -- actually `len(stripped)` is 0 so it returns `stripped` which is `()` (empty tuple), and `False`. Then line 680: `stripped if len(stripped) else None` -> returns `None` since empty tuple is length 0. Correction: returns `(None, False)`.

## Elixir Porting Notes
- The span wrapper hierarchy (`TracingSpan`, `NonRecordingTracingSpan`, `TransportableSpan`) maps to Elixir structs implementing a common `Span` protocol. The null-object pattern (`NonRecordingTracingSpan`) can be a simple struct with default field values.
- `SpanConsumer` singleton with mutable list maps to an `Agent` or `GenServer` accumulating spans.
- `TracerFactory` lazy init maps to a `GenServer` or `persistent_term` with one-time initialization.
- `Tracer` as context manager/decorator maps to a macro-based `with_tracing/2` or a callback wrapper function. Elixir does not have Python-style decorators; use module attributes or macro annotations.
- Context propagation (`inject`/`extract`) maps to OpenTelemetry Erlang/Elixir libraries (`opentelemetry_api`, `opentelemetry`). The Erlang ecosystem has mature OTel support.
- `MqWrappedError` re-raising pattern maps to `raise` with a custom exception struct.
- Plotly visualization has no direct Elixir equivalent; consider `VegaLite` via LiveBook or export to JSON for client-side rendering.
- Jupyter magic registration has no Elixir equivalent; LiveBook code cells could use a similar pattern with `Kino`.
- `ContextDecorator` inheritance allowing `@Tracer('label')` decorator usage maps to a macro: `@trace "label"` before a function definition.

## Bugs Found
- `Tracer.__init__` stores `__threshold` but it is never used anywhere in the module. Dead code. (OPEN)
- `Tracer.__init__` stores `__print_on_exit` but it is never checked in `__exit__`. Dead code. (OPEN)
- `Tracer.extract()` returns `None` on exception (implicit return in except block). Callers using the result may get `AttributeError` accessing `._context`. (OPEN)
- `parse_tracing_line_args`: when the first token is `chart`/`plot`/`graph` and there are additional args, line 679 returns `tuple(stripped[1:])` -- this is a tuple of strings, but callers on line 697 pass it as `span_name` to `Tracer(label=span_name)`. A tuple is not a valid string label. (OPEN)

## Coverage Notes
- Branch count: ~55
- Key branches: `SpanConsumer.get_instance` singleton check, `TracingScope.__exit__` exception path, `TracingScope.close` token/finish checks, `TracingSpan.set_tag` type coercion branches (5 paths), `TracingSpan.duration` end_time check, `TracingSpan.parent_id` parent check, `TransportableSpan.transportable` endpoint comparison, `TracerFactory.get` lazy init, `TracerFactory.preregister_span_processor` already-created check, `Tracer.__enter__` parent_span path, `Tracer.__exit__` exception + wrap + parent paths, `Tracer.active_span` recording check, `Tracer.inject` recording + exception checks, `Tracer.extract` exception check, `Tracer.activate_span` null check, `Tracer.start_active_span` 3-way context selection, `Tracer.record_exception` exception swallowing, `Tracer.__format_traceback` null + exception checks, `Tracer.plot` plotly import + reset + show branches, `gather_data` as_string + trace_id filter, `parse_tracing_line_args` chart detection
- Pragmas: none observed
- Module-level try/except blocks (lines 683-707) for optional imports are difficult to cover in a single test environment.
