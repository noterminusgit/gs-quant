# tree_entity.py

## Summary
Implements a recursive asset tree structure for navigating hierarchical asset relationships (e.g., index constituents). `AssetTreeNode` represents a single node with children, supporting tree construction from datasets, value population, and DataFrame export. `TreeHelper` is the main entry point that manages tree lifecycle, caching, and visualization via BFS traversal.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAsset, GsAssetApi), `gs_quant.data` (Dataset), `gs_quant.errors` (MqValueError)
- External: `pandas` (pd, DataFrame), `datetime` (dt, date, datetime), `typing` (Optional), `pydash` (get), `treelib` (Tree -- optional/lazy import)

## Type Definitions

### AssetTreeNode (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` (any) | required | Asset ID (Marquee asset identifier) |
| `date` | `Optional[dt.date]` | `None` | Date for which the tree is constructed |
| `depth` | `Optional[int]` | `0` | Depth relative to root node |
| `asset` | `Optional[GsAsset]` | `None` | Full GsAsset object from API |
| `name` | `str` or `None` | derived | `pydash.get(self.asset, 'name')` |
| `bbid` | `str` or `None` | derived | `pydash.get(pydash.get(self.asset, 'xref'), 'bbid')` |
| `asset_type` | `str` or `None` | derived | `pydash.get(self.asset, 'type')` |
| `data` | `dict` | `{}` | Arbitrary key-value data (e.g., weight, attribution) |
| `constituents_df` | `pd.DataFrame` | `pd.DataFrame()` | Cached DataFrame of all constituents |
| `direct_underlier_assets_as_nodes` | `List[AssetTreeNode]` | `[]` | Child nodes |

### TreeHelper (class)
Inherits: none (plain class)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` (any) | required | Root asset ID |
| `root` | `AssetTreeNode` | constructed | Root node created from API lookup |
| `date` | `dt.date` or `None` | `self.root.date` | Date from root node |
| `update_time` | `dt.datetime` | `dt.datetime.now()` | Timestamp of last tree build |
| `constituents_df` | `pd.DataFrame` | `pd.DataFrame()` | Cached constituents DataFrame |
| `tree_built` | `bool` | `False` | Flag indicating whether tree has been built |
| `__tree_underlier_dataset` (private) | `Optional[str]` | `None` | Dataset identifier for underlier queries |
| `__underlier_column` (private) | `Optional[str]` | `'underlyingAssetId'` | Column name for underlier IDs in dataset |

## Enums and Constants
No enums or module-level constants defined.

## Functions/Methods

### AssetTreeNode.__init__(self, id, depth: Optional[int] = 0, date: Optional[dt.date] = None, asset: Optional[GsAsset] = None)
Purpose: Initialize a tree node with asset metadata derived from the GsAsset object.

**Algorithm:**
1. Set `id`, `date`, `depth`, `asset` directly
2. Derive `name` from `pydash.get(self.asset, 'name')` -- returns `None` if asset is `None`
3. Derive `bbid` from nested `pydash.get(pydash.get(self.asset, 'xref'), 'bbid')` -- returns `None` if asset or xref is `None`
4. Derive `asset_type` from `pydash.get(self.asset, 'type')`
5. Initialize `data = {}`, `constituents_df = pd.DataFrame()`, `direct_underlier_assets_as_nodes = []`

### AssetTreeNode.__str__(self) -> str
Purpose: Human-readable string representation.

**Algorithm:**
1. Branch: If `self.bbid is not None` -> `result = self.bbid`
2. Branch: If `self.bbid is None` -> `result = self.id`
3. Return `f'Tree Node - {result}'`

### AssetTreeNode.to_frame(self) -> pd.DataFrame
Purpose: Return a DataFrame of all constituents under this node, with caching.

**Algorithm:**
1. Branch: If `len(self.constituents_df) > 0` -> return cached `self.constituents_df`
2. Branch: If empty -> call `self.__build_constituents_df(pd.DataFrame())`, then `drop_duplicates()`, `sort_values(by='depth')`, `reset_index(drop=True)`
3. Store result in `self.constituents_df` and return it

### AssetTreeNode.populate_values(self, dataset, value_column, underlier_column)
Purpose: Recursively populate a data value (e.g., weight) from a dataset into each child node.

**Algorithm:**
1. Create `Dataset(dataset)` and query for `self.date` with `assetId=[self.id]`
2. Branch: If `len(query) > 0`:
   a. For each child node in `self.direct_underlier_assets_as_nodes`:
      - Look up the value from query where `underlier_column == node.id`, take first row's `value_column`
      - Store in `node.data[value_column]`
      - Recursively call `node.populate_values(dataset, value_column, underlier_column)`
3. Branch: If `len(query) == 0` -> do nothing (no data for this node)

**Note:** `.iloc[0]` will raise `IndexError` if the filter returns no rows for a child node, even though the parent query returned data.

### AssetTreeNode.build_tree(self, dataset, underlier_column)
Purpose: Recursively build the tree by querying for direct underliers.

**Algorithm:**
1. Call `self.__get_direct_underliers(self.id, dataset)` to get a DataFrame
2. Branch: If `len(query) > 0`:
   a. Extract all underlier IDs from `query[underlier_column].tolist()`
   b. Batch-fetch assets via `GsAssetApi.get_many_assets(id=all_ids)`
   c. Build `asset_lookup` dict mapping ID -> GsAsset
   d. For each row in query:
      - Get underlier ID from `row[underlier_column]`
      - Branch: If `underlier not in asset_lookup` -> raise `Exception("Unable to find {underlier}")`
      - Create child `AssetTreeNode(underlier, self.depth + 1, self.date, asset_lookup[underlier])`
      - Recursively call `child_node.build_tree(dataset, underlier_column)`
      - Append child to `self.direct_underlier_assets_as_nodes`
3. Branch: If `len(query) == 0` -> do nothing (leaf node)

**Raises:** `Exception` when an underlier ID is not found in the asset lookup

### AssetTreeNode.__get_direct_underliers(self, asset_id, dataset) -> pd.DataFrame
Purpose: Query dataset for direct underliers of a given asset, handling date logic.

**Algorithm:**
1. Create `Dataset(dataset)`
2. Branch: If `self.date` is truthy -> query with `start=self.date, end=self.date, assetId=[asset_id]`, then `drop_duplicates()`
3. Branch: If `self.date` is falsy (None) -> query with just `assetId=[asset_id]`, then `drop_duplicates()`
4. Branch: If `len(query) > 0`:
   a. Set `self.date = query.index.max().date()` (side effect: updates the node's date to latest available)
   b. Filter to only rows at the max index: `query[query.index == query.index.max()].reset_index()`
5. Return `query` (may be empty DataFrame)

### AssetTreeNode.__build_constituents_df(self, constituents_df) -> pd.DataFrame
Purpose: Recursively build a flat DataFrame of parent-child relationships.

**Algorithm:**
1. For each `node` in `self.direct_underlier_assets_as_nodes`:
   a. Build a `data` dict with keys: `date`, `assetName`, `assetId`, `assetBbid`, `underlyingAssetName`, `underlyingAssetId`, `underlyingAssetBbid`, `depth`
   b. For each `(key, value)` in `node.data.items()` -> add to `data` dict (e.g., weight, attribution)
   c. Append `pd.DataFrame(data, index=[0])` to `constituents_df` using `DataFrame.append()`
   d. Recursively call `node.__build_constituents_df(pd.DataFrame())`
   e. Branch: If `len(d) > 0` -> append child result to `constituents_df`
2. Return `constituents_df`

**Note:** Uses deprecated `DataFrame.append()`. In modern pandas, should use `pd.concat()`.

### TreeHelper.__init__(self, id, date: Optional[dt.date] = None, tree_underlier_dataset: Optional[str] = None, underlier_column: Optional[str] = 'underlyingAssetId')
Purpose: Initialize the tree helper, immediately fetching the root asset from API.

**Algorithm:**
1. Set `self.id = id`
2. Create root node: `AssetTreeNode(self.id, 0, date, GsAssetApi.get_asset(asset_id=self.id))`
3. Set `self.date = self.root.date`
4. Set `self.update_time = dt.datetime.now()`
5. Initialize `self.constituents_df = pd.DataFrame()`, `self.tree_built = False`
6. Store dataset and column as private fields

**Note:** Constructor makes an API call (`GsAssetApi.get_asset`), so instantiation requires network access.

### TreeHelper.populate_weights(self, dataset, weight_column: Optional[str] = 'weight')
Purpose: Populate weight values throughout the tree.

**Algorithm:**
1. Branch: If `not self.tree_built` -> call `self.build_tree()`
2. Set `self.root.data['weight'] = 1` (root always has weight 1)
3. Call `self.root.populate_values(dataset, weight_column, self.__underlier_column)`

### TreeHelper.populate_attribution(self, dataset, attribution_column: Optional[str] = 'absoluteAttribution')
Purpose: Populate attribution values throughout the tree.

**Algorithm:**
1. Branch: If `not self.tree_built` -> call `self.build_tree()`
2. Set `self.root.data['absoluteAttribution'] = 1` (root always has attribution 1)
3. Call `self.root.populate_values(dataset, attribution_column, self.__underlier_column)`

### TreeHelper.to_frame(self) -> pd.DataFrame
Purpose: Get full tree as a DataFrame, building tree if needed.

**Algorithm:**
1. Branch: If `not self.tree_built` -> call `self.build_tree()`
2. Get `self.constituents_df = self.root.to_frame()`
3. Branch: If `len(self.constituents_df) > 0` -> return it
4. Branch: If empty -> raise `MqValueError('No constituents found for the asset')`

**Raises:** `MqValueError` when tree has no constituents

### TreeHelper.build_tree(self)
Purpose: Build the tree if not already built, with idempotency guard.

**Algorithm:**
1. Branch: If `not self.tree_built`:
   a. Call `self.root.build_tree(self.__tree_underlier_dataset, self.__underlier_column)`
   b. Set `self.tree_built = True`
   c. Update `self.update_time = dt.datetime.now()`
2. Branch: If already built -> do nothing

### TreeHelper.get_tree(self) -> AssetTreeNode
Purpose: Return the root node of the fully built tree.

**Algorithm:**
1. Branch: If `not self.tree_built` -> call `self.build_tree()`
2. Return `self.root`

### TreeHelper.get_visualisation(self, visualise_by: str = 'name')
Purpose: Generate a visual text representation of the tree using `treelib`.

**Algorithm:**
1. Try to import `treelib.Tree`
2. Branch: If `ModuleNotFoundError` -> raise `RuntimeError('You must install treelib to be able use this function.')`
3. Branch: If `not self.tree_built` -> call `self.build_tree()`
4. Branch: If `visualise_by` in `['name', 'bbid', 'id']`:
   a. Initialize BFS queue with `[[self.root, '']]` (node, prefix)
   b. While queue is not empty:
      - Pop first element (FIFO -- BFS order)
      - Compute `node_id = prefix + '-' + node.id`
      - Get `node_name = getattr(node, visualise_by)`
      - Branch: If `str(node_name) == 'None'` -> set `node_name = f'NA ({node.id})'`
      - Branch: If `prefix == ''` (root node) -> `tree_vis.create_node(node_name, node_id)` (no parent)
      - Branch: If `prefix != ''` (non-root) -> `tree_vis.create_node(node_name, node_id, parent=prefix)`
      - For each child node -> append `[child, node_id]` to queue
5. Branch: If `visualise_by` not in the valid list -> raise `MqValueError('visualise_by argument has to be either name, id or bbid')`
6. Return `tree_vis.show()`

**Raises:** `RuntimeError` if treelib not installed, `MqValueError` if invalid `visualise_by` value

## State Mutation
- `AssetTreeNode`:
  - `self.date`: Modified by `__get_direct_underliers` to latest available date when query returns data
  - `self.direct_underlier_assets_as_nodes`: Populated by `build_tree()`
  - `self.data`: Populated by `populate_values()`
  - `self.constituents_df`: Cached by `to_frame()` on first call
- `TreeHelper`:
  - `self.tree_built`: Set to `True` by `build_tree()`
  - `self.update_time`: Updated by `build_tree()` and `__init__`
  - `self.constituents_df`: Updated by `to_frame()`
  - `self.root`: Created in `__init__`, tree structure built via `build_tree()`
- Thread safety: No synchronization. Not safe for concurrent tree building or value population.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `Exception` | `AssetTreeNode.build_tree` | Underlier ID not found in asset lookup |
| `MqValueError` | `TreeHelper.to_frame` | No constituents found after building tree |
| `MqValueError` | `TreeHelper.get_visualisation` | `visualise_by` not in `['name', 'bbid', 'id']` |
| `RuntimeError` | `TreeHelper.get_visualisation` | `treelib` module not installed |
| `IndexError` (implicit) | `AssetTreeNode.populate_values` | Child node ID not found in query results (`.iloc[0]` on empty slice) |

## Edge Cases
- `AssetTreeNode.__init__` with `asset=None` -- `name`, `bbid`, `asset_type` all become `None` via pydash.get
- `AssetTreeNode.__str__` with `bbid=None` -- falls back to `self.id`
- `AssetTreeNode.to_frame` called twice -- second call returns cached DataFrame
- `AssetTreeNode.build_tree` on a leaf node (no underliers) -- `query` is empty, no children added
- `AssetTreeNode.__get_direct_underliers` with `self.date=None` -- queries without date constraints, then sets `self.date` from result
- `AssetTreeNode.__get_direct_underliers` returns empty DataFrame -- `self.date` is NOT updated
- `AssetTreeNode.populate_values` with no data for a parent -- `len(query) == 0`, children are not populated
- `AssetTreeNode.populate_values` where a child is in the tree but not in the dataset query results -- `.iloc[0]` raises `IndexError`
- `AssetTreeNode.__build_constituents_df` uses deprecated `DataFrame.append()` -- may be removed in pandas >= 2.0
- `TreeHelper.__init__` makes an API call -- constructor has side effects
- `TreeHelper.build_tree` is idempotent -- calling multiple times only builds once
- `TreeHelper.to_frame` on empty tree -- raises `MqValueError`
- `TreeHelper.get_visualisation` with `visualise_by='name'` and a node where `name` is `None` -- displays as `'NA ({node.id})'`
- `TreeHelper.get_visualisation` BFS traversal -- uses prefix-based unique IDs to handle duplicate asset IDs across branches (same asset can appear in multiple branches)
- `TreeHelper.get_visualisation` returns `tree_vis.show()` which returns `None` in treelib (it prints to stdout) -- caller gets `None`

## Bugs Found
- `AssetTreeNode.__build_constituents_df` uses `DataFrame.append()` which is deprecated since pandas 1.4 and removed in pandas 2.0. Should use `pd.concat()`. (OPEN)
- `TreeHelper.get_visualisation` returns `tree_vis.show()` which in treelib prints to stdout and returns `None`. The return value is not useful. Should probably use `tree_vis.show(stdout=False)` to get a string, or document that the method's purpose is side-effect (printing). (OPEN)

## Coverage Notes
- Branch count: ~22
  - `AssetTreeNode.__str__`: 2 (bbid is None vs not)
  - `AssetTreeNode.to_frame`: 2 (cached vs not)
  - `AssetTreeNode.populate_values`: 2 (query has data vs empty)
  - `AssetTreeNode.build_tree`: 3 (query empty, underlier found, underlier not found)
  - `AssetTreeNode.__get_direct_underliers`: 3 (date truthy vs falsy, query non-empty vs empty)
  - `AssetTreeNode.__build_constituents_df`: 2 (child has sub-constituents vs not)
  - `TreeHelper.populate_weights`: 2 (tree built vs not)
  - `TreeHelper.populate_attribution`: 2 (tree built vs not)
  - `TreeHelper.to_frame`: 3 (tree not built, has constituents, no constituents)
  - `TreeHelper.build_tree`: 2 (already built vs not)
  - `TreeHelper.get_tree`: 2 (already built vs not)
  - `TreeHelper.get_visualisation`: 5 (treelib missing, tree not built, valid visualise_by, invalid visualise_by, node_name is None)
- Pragmas: none
