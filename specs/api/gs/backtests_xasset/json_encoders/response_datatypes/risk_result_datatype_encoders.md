# risk_result_datatype_encoders.py

## Summary
Provides encoder and decoder functions for pandas Series and DataFrame serialization used by `VectorWithData` and `MatrixWithData` risk result types. Also includes a helper for converting string lists to date lists.

## Dependencies
- Internal: none
- External: `datetime` (dt), `pandas` (pd)

## Functions/Methods

### encode_series_result(s: pd.Series) -> Dict
Purpose: Encode a pandas Series into a JSON-serializable dict.

**Algorithm:**
1. Return `{'index': tuple(s.index), 'name': s.name, 'values': tuple(s.values)}`.

### encode_dataframe_result(df: pd.DataFrame) -> Dict
Purpose: Encode a pandas DataFrame into a JSON-serializable dict.

**Algorithm:**
1. Return `{'index': tuple(df.index), 'columns': tuple(df.columns), 'values': tuple(tuple(v) for v in df.values)}`.

### _convert_list_to_dates(lst: list) -> list/tuple
Purpose: Attempt to convert a list of strings to a tuple of `dt.date` objects. If conversion fails or the list is empty or non-string, return as-is.

**Algorithm:**
1. If `lst` is falsy (empty) or `lst[0]` is not a `str`, return `lst` unchanged.
2. Try to parse each element via `dt.date.fromisoformat`.
3. If any element raises `ValueError`, return the original `lst` unchanged.
4. Return the parsed tuple of dates.

### decode_series_result(s: dict) -> pd.Series
Purpose: Decode a dict into a pandas Series.

**Algorithm:**
1. Convert `s['index']` via `_convert_list_to_dates` (may convert ISO date strings to `dt.date`).
2. Return `pd.Series(s['values'], index=converted_index, name=s['name'])`.

### decode_dataframe_result(s: dict) -> pd.DataFrame
Purpose: Decode a dict into a pandas DataFrame.

**Algorithm:**
1. Convert `s['index']` via `_convert_list_to_dates`.
2. Return `pd.DataFrame(s['values'], index=converted_index, columns=s['columns'])`.

## Elixir Porting Notes
- Pandas Series/DataFrame have no direct Elixir equivalent. Options:
  - Use `Explorer.Series` / `Explorer.DataFrame` if the project uses the Explorer library.
  - Use plain maps/lists: Series as `%{name: name, index: [...], values: [...]}`, DataFrame as `%{index: [...], columns: [...], values: [[...], ...]}`.
- `encode_series_result` / `encode_dataframe_result` become simple struct-to-map conversions.
- `_convert_list_to_dates` maps to a function that attempts `Date.from_iso8601/1` on the first element and, on success, maps the entire list.
- The `ValueError` rescue in `_convert_list_to_dates` maps to pattern matching on `{:ok, date}` vs `:error` from `Date.from_iso8601/1`.
- Serialized form uses `'index'`, `'name'`, `'values'`, `'columns'` keys -- these are the wire format and should be preserved.

## Edge Cases
- `_convert_list_to_dates` only checks the first element to decide whether to attempt date conversion; if the first element is a valid date string but subsequent ones are not, the entire conversion fails and returns the original list.
- Empty list/tuple returns as-is (the `not lst` check catches this).
- Non-string index values (e.g. integers, floats) are passed through unchanged.
- `encode_series_result` converts numpy values to Python tuples; index elements may be dates, strings, or numbers depending on the Series.
