# generic_datatype_encoders.py

## Summary
Provides decoder functions for instrument-level deserialization: decoding a single instrument dict, a tuple of instrument dicts, and a date-keyed dictionary of daily portfolios (with optional instrument decoding bypass).

## Dependencies
- Internal: `gs_quant.instrument` (Instrument)
- External: `datetime`, `typing` (Dict, Tuple)

## Functions/Methods

### decode_inst(i: dict) -> Instrument
Purpose: Decode a single instrument dict into an `Instrument` object.

**Algorithm:**
1. Return `Instrument.from_dict(i)`.

### decode_inst_tuple(t: tuple) -> Tuple[Instrument, ...]
Purpose: Decode a tuple/list of instrument dicts into a tuple of `Instrument` objects.

**Algorithm:**
1. For each element `i` in `t`, call `decode_inst(i)`.
2. Return as tuple.

### decode_daily_portfolio(results: dict, decode_instruments: bool = True) -> Dict[dt.date, Tuple[Instrument, ...]]
Purpose: Decode a dict mapping ISO date strings to lists of instrument dicts into typed form, with optional instrument decoding bypass.

**Algorithm:**
1. For each `{k: v}` in `results`:
   a. Parse `k` via `dt.date.fromisoformat`.
   b. If `decode_instruments` is `True`, decode `v` via `decode_inst_tuple(v)`.
   c. Otherwise, use raw `v` as-is.
2. Return the resulting dict.

## Elixir Porting Notes
- `decode_inst` maps to `Instrument.from_map/1`.
- `decode_inst_tuple` maps to `Enum.map(t, &Instrument.from_map/1) |> List.to_tuple()` or simply keep as a list (Elixir idiom).
- `decode_daily_portfolio` maps to `Map.new(results, fn {k, v} -> {Date.from_iso8601!(k), maybe_decode(v, opts)} end)`.
- The `decode_instruments` boolean is best passed as a keyword option: `decode_daily_portfolio(results, decode_instruments: false)`.
- Consider whether to preserve tuples or use lists in the Elixir port; lists are more idiomatic.

## Edge Cases
- `decode_daily_portfolio` with `decode_instruments=False` returns raw list/dict values instead of `Instrument` structs, so the return type is technically `Dict[dt.date, Any]` in that path.
- Empty `results` dict produces an empty map.
- `decode_inst` assumes the dict has the correct shape for `Instrument.from_dict`; invalid dicts will raise in the Instrument deserialization layer.
