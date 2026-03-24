# parser.py

## Summary
API client wrapper for GS instrument parser endpoints. Provides two methods to parse natural language text into instrument definitions, either with or without a specified asset class.

## Dependencies
- Internal: `gs_quant.session` (GsSession)
- External: `logging`

## Type Definitions
None.

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### GsParserApi.get_instrument_from_text_asset_class(cls, text: str, asset_class: str) -> dict
Purpose: Parse text into an instrument definition given a specific asset class.

**Algorithm:**
1. POST `/parser/quoteTicket` with payload `{'message': text, 'assetClass': asset_class}`
2. Navigate response: `res['ticket']['quote']['instrument']`
3. Return the instrument dict

### GsParserApi.get_instrument_from_text(cls, text: str) -> dict
Purpose: Parse text into a list of instruments without specifying an asset class.

**Algorithm:**
1. POST `/parser/portfolio` with payload `{'message': text}`
2. Return `res['instruments']`

## State Mutation
- No instance state; all methods are `@classmethod`.
- Relies on `GsSession.current` for HTTP session state.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `get_instrument_from_text_asset_class` | If response missing nested `ticket.quote.instrument` path |
| `KeyError` | `get_instrument_from_text` | If response missing `instruments` key |

## Edge Cases
- Both methods assume the response structure is well-formed; no defensive checks on nested keys
- `get_instrument_from_text_asset_class` deeply nests into `res['ticket']['quote']['instrument']` -- any missing intermediate key raises `KeyError`

## Bugs Found
None identified.

## Coverage Notes
- Branch count: 0
- Both methods are linear with no conditional branches.
- Pragmas: none
