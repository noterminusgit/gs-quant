# entity_utils.py

## Summary
Provides a single recursive utility function `_explode_data` for hierarchically flattening nested pandas Series data (factor categories, sectors, industries, countries, directions) into a DataFrame. Used to denormalize nested structures from risk/analytics data into tabular form.

## Dependencies
- Internal: none
- External: `typing` (Union), `pandas` (pd, DataFrame, Series)

## Type Definitions
No classes or custom types defined.

### Type Aliases (implicit in signature)
```
Return type: Union[pd.DataFrame, pd.Series]
```

## Enums and Constants

### Module Constants (embedded in function)

#### `parent_to_child_map` (dict, local to `_explode_data`)
| Key | Value | Description |
|-----|-------|-------------|
| `"factorCategories"` | `"factors"` | Factor categories contain child factors |
| `"factors"` | `"byAsset"` | Factors contain child byAsset entries |
| `"sectors"` | `"industries"` | Sectors contain child industries |
| `"industries"` | `None` | Industries are leaf nodes |
| `"countries"` | `None` | Countries are leaf nodes |
| `"direction"` | `None` | Direction is a leaf node |

#### `labels_to_ignore_map` (dict, local to `_explode_data`)
| Key | Value | Description |
|-----|-------|-------------|
| `"factorCategories"` | `["factorExposure", "estimatedPnl", "factors"]` | Labels to drop when exploding factorCategories |
| `"factors"` | `["factorExposure", "estimatedPnl", "byAsset"]` | Labels to drop when exploding factors |
| `"sectors"` | `["exposure", "estimatedPnl", "industries"]` | Labels to drop when exploding sectors |
| `"industries"` | `[]` | No labels to drop for leaf |
| `"countries"` | `[]` | No labels to drop for leaf |
| `"direction"` | `[]` | No labels to drop for leaf |
| `"byAsset"` | `[]` | No labels to drop for leaf |

## Functions/Methods

### _explode_data(data: pd.Series, parent_label: str) -> Union[pd.DataFrame, pd.Series]
Purpose: Recursively explode a nested pandas Series hierarchy into a flat DataFrame by following parent-child relationships defined in the maps.

**Algorithm:**
1. Branch: If `parent_label` is a key in `parent_to_child_map` -> rename the `'name'` column in `data` to `parent_label`; else -> leave `data` unchanged
2. Look up `child_label` from `parent_to_child_map.get(parent_label)` (returns `None` for leaf nodes or unknown labels)
3. Branch: If `child_label` is truthy AND `child_label` exists in `data.index.values`:
   a. Create `child_df = pd.DataFrame(data[child_label])` -- extracts the nested list/array at that key into a DataFrame
   b. Recursively apply `_explode_data` to each row of `child_df` with `parent_label=child_label` via `child_df.apply(..., axis=1)`
   c. Drop labels from `data` that are in `labels_to_ignore_map.get(parent_label)` -- removes the child column and related aggregates
   d. Branch: If `child_df` after apply is a `pd.Series` (happens when apply returns Series of DataFrames) -> concatenate all values with `pd.concat(child_df.values, ignore_index=True)`
   e. Assign remaining parent-level fields to all child rows via `child_df.assign(**data.to_dict())`
   f. Return `child_df` (a DataFrame)
4. Branch: If `child_label` is falsy OR `child_label` not in `data.index.values` -> return `data` as-is (a Series, representing a leaf node)

**Raises:** No explicit exceptions. May raise `KeyError` if `labels_to_ignore_map` does not contain `parent_label` (but all known values are covered).

## State Mutation
- No global or module-level state is mutated.
- The `data` Series is modified in-place via `rename` and `drop` within the function body. The caller's original data may be affected if pandas does not copy on these operations.
- Thread safety: No shared state; safe for concurrent use as long as input data is not shared.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `KeyError` | `labels_to_ignore_map.get()` | Would not raise (uses `.get()`), but `data.drop(labels=...)` could raise if labels are not present in the Series index |

## Edge Cases
- `parent_label` not in `parent_to_child_map` (e.g., `"byAsset"`) -- the rename step is skipped (`data` unchanged), `child_label` is `None`, returns `data` as-is (leaf behavior)
- `parent_label` in `parent_to_child_map` but `child_label` not in `data.index.values` -- rename happens but no recursion; drops nothing; returns the renamed Series
- `child_df.apply(...)` returns a Series of DataFrames -- the `isinstance(child_df, pd.Series)` branch handles this by concatenating
- `child_df.apply(...)` returns a DataFrame directly -- skip the concat step
- Empty child list (e.g., `data[child_label]` is an empty list) -- `pd.DataFrame([])` creates an empty DataFrame, `apply` does nothing, returns empty DataFrame with parent columns assigned
- Deeply nested hierarchy: `factorCategories -> factors -> byAsset` -- three levels of recursion
- `sectors -> industries -> None` -- two levels of recursion; industries is a leaf

## Coverage Notes
- Branch count: 5
  1. `parent_label in parent_to_child_map.keys()` -- True vs False (rename or not)
  2. `child_label and child_label in data.index.values` -- True (recurse) vs False (return leaf)
  3. `isinstance(child_df, pd.Series)` -- True (concat) vs False (skip concat)
  4. The `child_label` truthiness part of branch 2 (could be `None` for leaf nodes like `"industries"`)
  5. `child_label` truthy but not in `data.index.values` (data does not have expected child key)
- Missing branches: The function is private (prefixed `_`), so it is only called internally. All parent_label values should be tested: `"factorCategories"`, `"factors"`, `"sectors"`, `"industries"`, `"countries"`, `"direction"`, `"byAsset"`, and an unknown label.
- Pragmas: none
