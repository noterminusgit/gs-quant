# helpers.py

## Summary
Utility functions for analytics: type checking, entity resolution, and relative date cache key generation.

## Dependencies
- Internal: constants (TYPE, DATA_ROW, etc.), RelativeDate, Entity, MqValueError, MqRequestError
- External: logging

## Functions

### is_of_builtin_type(obj)
1. Returns True if `type(obj).__module__` is 'builtins' or '__builtin__', else False

### resolve_entities(reference_list, entity_cache=None)
1. Default entity_cache to {} if None
2. For each reference in reference_list:
   a. Check if entity_id in cache → use cached
   b. Else try Entity.get() → on MqRequestError, log warning, use entity_id string as fallback
   c. Branch on reference[TYPE]:
      - DATA_ROW → set reference[REFERENCE].entity = entity
      - PROCESSOR → setattr + update children; raise MqValueError if parameter not in children

### get_rdate_cache_key(rule, base_date, currencies, exchanges)
Returns formatted string key.

### get_entity_rdate_key(entity_id, rule, base_date)
Returns formatted string key.

### get_entity_rdate_key_from_rdate(entity_id, rdate)
1. Branch: rdate.base_date_passed_in → str(base_date), else None
2. Returns formatted key

## Edge Cases
- entity_cache=None defaults to {}
- MqRequestError during entity fetch → entity becomes string ID
- reference[PARAMETER] not in children → MqValueError raised

## Bugs Found
None.

## Coverage Notes
- ~8 branches
- Entity.get() requires mock
