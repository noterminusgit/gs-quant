# request_encoders.py

## Summary
Provides encoder and decoder functions for request-level serialization: encoding instruments and risk measures into JSON-ready dicts, decoding instrument legs from dicts (with auto-naming), and a generic enum decoder factory.

## Dependencies
- Internal: `gs_quant.base` (EnumBase), `gs_quant.common` (RiskMeasure), `gs_quant.instrument` (Instrument), `gs_quant.json_convertors_common` (encode_risk_measure)
- External: `typing` (Any, Iterable)

## Functions/Methods

### encode_request_object(data: Any) -> encoded
Purpose: Recursively encode a request data object into a JSON-serializable form.

**Algorithm:**
1. If `data` is a `RiskMeasure`, return `encode_risk_measure(data)`.
2. If `data` is an `Instrument`, return `data.to_dict()`.
3. If `data` is a `tuple`, recursively encode each element and return as tuple.
4. (Implicit) If none match, returns `None`.

### legs_decoder(data: Any) -> Optional[List[Instrument]]
Purpose: Decode a list of instrument dicts into `Instrument` objects, assigning unique names to unnamed legs.

**Algorithm:**
1. If `data` is `None`, return `None`.
2. Deserialize each dict via `Instrument.from_dict(d)`.
3. Collect all existing names into a set.
4. For each instrument without a name (`i.name is None`):
   a. Generate candidate name `'leg_' + str(name_idx)`.
   b. While candidate is already in the names set, increment `name_idx` and regenerate.
   c. Assign the unique name to `i.name`.
   d. Increment `name_idx`.
5. Return the list of instruments.

### legs_encoder(data: Iterable[Instrument]) -> List[dict]
Purpose: Encode an iterable of instruments to a list of dicts.

**Algorithm:**
1. Return `[i.to_dict() for i in data]`.

### enum_decode(enum_class) -> Callable
Purpose: Factory that returns a decoder function for a given enum class.

**Algorithm (returned `decode_value` closure):**
1. If `value` is `None` or the string `'null'`, return `None`.
2. If `value` is already an instance of `EnumBase`, return it as-is.
3. If `value` is a `str`, try `enum_class(value)`:
   - On success, return the enum member.
   - On `ValueError`, fall through.
4. Raise `ValueError` with message about being unable to decode.

## Elixir Porting Notes
- `encode_request_object` maps to a multi-clause function using pattern matching on the data type.
- `legs_decoder` auto-naming logic maps to an `Enum.map_reduce/3` that tracks used names in an accumulator (MapSet).
- `legs_encoder` is a simple `Enum.map/2` calling the instrument's `to_map/1`.
- `enum_decode` factory pattern maps to a higher-order function returning a closure, or simply a `decode_enum/2` function taking the module and value.
- The `'null'` string check in `enum_decode` is a defensive measure against JSON null being passed as a string; in Elixir, `Jason.decode!` produces `nil` for JSON null, so this edge case may not arise unless the data is pre-processed.

## Edge Cases
- `encode_request_object` returns `None` implicitly if the data type is not `RiskMeasure`, `Instrument`, or `tuple`.
- `legs_decoder` handles name collisions by incrementing `name_idx` in a while loop until a unique name is found.
- `enum_decode` treats the literal string `"null"` as `None`, not as a valid enum value.
- `enum_decode` accepts `EnumBase` instances directly (pass-through), handling cases where the value is already decoded.
