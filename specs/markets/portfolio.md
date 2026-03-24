# portfolio.py

## Summary
Implements the `Portfolio` class -- a recursive, tree-structured collection of instruments and sub-portfolios that supports pricing, risk calculation, resolution, serialization (CSV/DataFrame), and persistence to the Marquee platform. Also provides `Grid`, a specialized Portfolio that generates a 2D parameter sweep of instruments. Portfolio is the primary user-facing aggregation unit for risk calculations in gs_quant; its `calc()`, `resolve()`, and `market()` methods are critical for the Elixir port.

## Dependencies
- Internal: `gs_quant.api.gs.assets` (GsAssetApi), `gs_quant.api.gs.portfolios` (GsPortfolioApi), `gs_quant.base` (InstrumentBase), `gs_quant.common` (RiskMeasure, RiskPosition), `gs_quant.instrument` (Instrument, AssetType), `gs_quant.markets` (HistoricalPricingContext, OverlayMarket, PricingContext, PositionContext), `gs_quant.priceable` (PriceableImpl), `gs_quant.risk` (ResolvedInstrumentValues), `gs_quant.risk.results` (CompositeResultFuture, PortfolioRiskResult, PortfolioPath, PricingFuture), `gs_quant.target.portfolios` (Portfolio as MarqueePortfolio, Position, PositionSet, RiskRequest, PricingDateAndMarketDataAsOf)
- External: `datetime` (dt), `logging`, `re`, `dataclasses` (dataclass), `itertools` (chain), `typing` (Iterable, Optional, Tuple, Union), `urllib.parse` (quote), `deprecation`, `numpy` (np), `pandas` (pd), `more_itertools` (unique_everseen)

## Type Definitions

### Portfolio (dataclass, class)
Inherits: PriceableImpl

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| __priceables | `Tuple[PriceableImpl, ...]` | `()` | Ordered tuple of instruments and/or sub-portfolios |
| __priceables_by_name | `dict` | `{}` | Index mapping `name -> [indices]` for named priceables |
| name | `Optional[str]` | `None` | Portfolio name |
| __id | `Optional[str]` | `None` | Marquee portfolio ID |
| __quote_id | `Optional[str]` | `None` | Marquee quote/workflow ID |

### Grid (dataclass, class)
Inherits: Portfolio

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| priceable | `PriceableImpl` | (required) | Template instrument to clone |
| x_param | `str` | (required) | Parameter name for X axis |
| x_values | `Iterable` | (required) | Values for X axis |
| y_param | `str` | (required) | Parameter name for Y axis |
| y_values | `Iterable` | (required) | Values for Y axis |
| name | `Optional[str]` | `None` | Grid name |

Constructs a nested Portfolio: outer = list of sub-portfolios (one per y_value), each inner = list of instruments (one per x_value).

## Enums and Constants

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| _logger | `Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### Portfolio.__init__(self, priceables: Optional[Union[PriceableImpl, Iterable[PriceableImpl], dict]] = (), name: Optional[str] = None)
Purpose: Create a portfolio from instruments, sub-portfolios, an iterable, or a name-to-priceable dict.

**Algorithm:**
1. Call `super().__init__()`
2. Branch: if `priceables` is a `dict` -> iterate items, set each priceable's `.name` to the key, collect into list, assign to `self.priceables`
3. Branch: else -> assign `priceables` directly to `self.priceables` (triggers setter)
4. Set `self.name = name`, `self.__id = None`, `self.__quote_id = None`

### Portfolio.__repr__(self) -> str
Purpose: Human-readable string showing name and instrument count.

**Algorithm:**
1. Build `inst_desc` showing count of `all_instruments` (or "0" if priceables is falsy)
2. Branch: if `self.name` -> `Portfolio(name, inst_desc)`
3. Branch: else -> `Portfolio(inst_desc)`

### Portfolio._to_records(self) -> list
Purpose: Flatten the portfolio tree into a list of dicts for DataFrame construction (with hierarchy columns).

**Algorithm:**
1. Define inner `get_name(obj, idx)`:
   a. Branch: if `isinstance(obj, InstrumentBase)` and `hasattr(obj, 'type_')`:
      - Branch: if `type_` is `AssetType` -> use `.name`; else -> use `type_` directly
   b. Branch: else -> `type_name = 'Portfolio'`
   c. Branch: if `obj.name is None` -> return `f'{type_name}_{idx}'`; else -> return `obj.name`
2. Use stack-based DFS traversal of portfolio tree
3. For each priceable:
   a. Branch: if `isinstance(priceable, Portfolio)` -> push onto stack with path, add portfolio_name column
   b. Branch: else (instrument) -> add instrument_name to record
4. Return records list

### Portfolio.__getitem__(self, item)
Purpose: Index into the portfolio by int/slice, PortfolioPath, string name, or list.

**Algorithm:**
1. Branch: if `isinstance(item, (int, slice))` -> return `self.__priceables[item]`
2. Branch: if `isinstance(item, PortfolioPath)` -> return `item(self, rename_to_parent=True)`
3. Branch: else (string or list):
   a. Branch: if `isinstance(item, list)` -> resolve all items via `self.paths(it)` for each, flatten
   b. Branch: else -> resolve via `self.paths(item)`
   c. Branch: if single result -> return it unwrapped; else -> return tuple

### Portfolio.__contains__(self, item) -> bool
Purpose: Check membership by PriceableImpl instance or name string.

**Algorithm:**
1. Branch: if `isinstance(item, PriceableImpl)` -> check all portfolios (including self) for item in `__priceables`
2. Branch: if `isinstance(item, str)` -> check all portfolios for item in `__priceables_by_name`
3. Branch: else -> return `False`

### Portfolio.__len__(self) -> int
Purpose: Return count of direct (top-level) priceables.

### Portfolio.__iter__(self)
Purpose: Iterate over direct priceables.

### Portfolio.__hash__(self) -> int
Purpose: Hash based on name, id, and XOR of all priceable hashes.

### Portfolio.__eq__(self, other) -> bool
Purpose: Deep equality check using all_paths traversal.

**Algorithm:**
1. Branch: if not `isinstance(other, Portfolio)` -> return `False`
2. For each path in `self.all_paths`:
   a. Try: compare `path(self)` vs `path(other)`
   b. Branch: if not equal -> return `False`
   c. Except `IndexError` -> return `False` (different lengths)
   d. Except `TypeError` -> return `False` (different depths)
3. Return `True`

### Portfolio.__add__(self, other) -> Portfolio
Purpose: Concatenate two portfolios' priceables.

**Algorithm:**
1. Branch: if not `isinstance(other, Portfolio)` -> raise `ValueError`
2. Return `Portfolio(self.__priceables + other.__priceables)`

### Portfolio.id (property) -> str
Purpose: Return Marquee portfolio ID.

### Portfolio.quote_id (property) -> str
Purpose: Return Marquee quote ID.

### Portfolio.priceables (property) -> Tuple[PriceableImpl, ...]
Purpose: Return the tuple of direct priceables.

### Portfolio.priceables (setter)
Purpose: Set priceables, converting single PriceableImpl to tuple, and rebuild name index.

**Algorithm:**
1. Branch: if `isinstance(priceables, PriceableImpl)` -> wrap in single-element tuple
2. Branch: else -> convert iterable to tuple
3. Rebuild `__priceables_by_name` dict: for each priceable with a non-None name, `setdefault(name, []).append(idx)`

### Portfolio.priceables (deleter)
Purpose: Set priceables and name index to None.

### Portfolio.instruments (property) -> Tuple[Instrument, ...]
Purpose: Return unique instruments (not sub-portfolios) at this level.

**Algorithm:**
1. Filter `__priceables` for `isinstance(i, Instrument)`, deduplicate via `unique_everseen`

### Portfolio.all_instruments (property) -> Tuple[Instrument, ...]
Purpose: Return all instruments recursively across all sub-portfolios, deduplicated.

**Algorithm:**
1. Chain `self.instruments` with all sub-portfolios' `all_instruments`
2. Deduplicate via `unique_everseen`

### Portfolio.portfolios (property) -> Tuple[PriceableImpl, ...]
Purpose: Return direct sub-portfolios at this level.

### Portfolio.all_portfolios (property) -> Tuple[PriceableImpl, ...]
Purpose: Return all sub-portfolios recursively, deduplicated.

**Algorithm:**
1. Start with `self.portfolios` on a stack
2. BFS/DFS traversal: pop, skip if already seen, add sub-portfolios
3. Deduplicate via `unique_everseen`

### Portfolio.subset(self, paths: Iterable[PortfolioPath], name=None) -> Portfolio
Purpose: Create a sub-portfolio from a set of paths.

**Algorithm:**
1. Branch: if single path and target is a Portfolio -> return that Portfolio directly
2. Branch: else -> return `Portfolio(tuple(self[p] for p in paths), name=name)`

### Portfolio.__from_internal_positions(id_type: str, positions_id, activity_type: str) -> Portfolio  [staticmethod, private]
Purpose: Load instruments from internal position system by ID type.

**Algorithm:**
1. Call `GsPortfolioApi.get_instruments_by_position_type(id_type, positions_id, activity_type)`
2. Return `Portfolio(instruments, name=positions_id)`

### Portfolio.from_eti(eti: str) -> Portfolio  [staticmethod]
Purpose: Load portfolio from ETI identifier.

**Algorithm:**
1. URL-encode the ETI (safe='')
2. Delegate to `__from_internal_positions('ETI', encoded_eti, 'trade')`

### Portfolio.from_book(book: str, book_type: str = 'risk', activity_type: str = 'position') -> Portfolio  [staticmethod]
Purpose: Load portfolio from book identifier.

### Portfolio.from_asset_id(asset_id: str, date=None) -> Portfolio  [staticmethod]
Purpose: Load portfolio from Marquee asset ID with optional date.

**Algorithm:**
1. Fetch asset via `GsAssetApi.get_asset(asset_id)`
2. Branch: if `date` -> `get_asset_positions_for_date(asset_id, date)`; else -> `get_latest_positions(asset_id)`
3. Branch: if response is tuple -> take first element
4. Branch: if response is `PositionSet` -> use `.positions`; else -> use `response['positions']`
5. Get instruments for positions
6. Create portfolio, set `__id = asset_id`
7. Return portfolio

### Portfolio.from_asset_name(name: str) -> Portfolio  [staticmethod]
Purpose: Load portfolio by asset name lookup.

### Portfolio.get(cls, portfolio_id=None, portfolio_name=None, query_instruments=False) -> Portfolio  [classmethod]
Purpose: Load a Marquee portfolio by ID or name.

**Algorithm:**
1. Branch: if `portfolio_name` -> look up ID via `GsPortfolioApi.get_portfolio_by_name`
2. Determine `position_date`: Branch: if `PositionContext.is_entered` -> use current; else -> `dt.date.today()`
3. Fetch portfolio metadata
4. Create `Portfolio(name=portfolio.name)`, set `__id`
5. Branch: if `query_instruments` -> call `_get_instruments(position_date, True)`
6. Return portfolio

### Portfolio.from_portfolio_id(cls, portfolio_id: str) -> Portfolio  [classmethod, deprecated]
Purpose: Deprecated wrapper for `get(portfolio_id=portfolio_id)`.

### Portfolio.from_portfolio_name(cls, name: str) -> Portfolio  [classmethod, deprecated]
Purpose: Deprecated wrapper for `get(portfolio_name=name)`.

### Portfolio.from_quote(quote_id: str) -> Portfolio  [staticmethod]
Purpose: Load portfolio from a quote/workflow ID.

### Portfolio.save(self, overwrite: Optional[bool] = False)
Purpose: Persist portfolio to Marquee platform.

**Algorithm:**
1. Branch: if `self.portfolios` (has nested) -> raise `ValueError`
2. Branch: if `self.__id`:
   a. Branch: if not `overwrite` -> raise `ValueError`
3. Branch: else (no id):
   a. Branch: if not `self.name` -> raise `ValueError`
   b. Create portfolio via `GsPortfolioApi.create_portfolio`, set `__id`
4. Build `PositionSet` from instruments
5. Branch: if positions exist -> call `update_positions`

**Raises:**
- `ValueError` when saving nested portfolios
- `ValueError` when overwriting without flag
- `ValueError` when no name is set for new portfolio

### Portfolio.save_as_quote(self, overwrite: Optional[bool] = False) -> str
Purpose: Persist portfolio as a quote/workflow.

**Algorithm:**
1. Branch: if nested portfolios -> raise `ValueError`
2. Get pricing context, pricing_date, and market
3. Build `RiskRequest` with instruments and `ResolvedInstrumentValues`
4. Branch: if `self.__quote_id` and not `overwrite` -> raise `ValueError`
5. Branch: if `self.__quote_id` and `overwrite` -> update existing quote
6. Branch: else -> create new quote, set `__quote_id`
7. Return quote_id

**Raises:** `ValueError` when nested portfolios or overwriting without flag

### Portfolio.save_to_shadowbook(self, name: str)
Purpose: Save portfolio to a shadowbook.

**Algorithm:**
1. Branch: if nested portfolios -> raise `ValueError`
2. Build `RiskRequest` same as `save_as_quote`
3. Call `GsPortfolioApi.save_to_shadowbook(request, name)`
4. Print status

### Portfolio.from_frame(cls, data: pd.DataFrame, mappings: dict = None) -> Portfolio  [classmethod]
Purpose: Construct portfolio from a DataFrame.

**Algorithm:**
1. Define `get_value(row, attribute)` that checks mappings for callable or column name
2. Replace NaN with None
3. For each non-empty row:
   a. Try `('asset_class', 'type')` init keys first
   b. Branch: if all init values present -> create instrument via `Instrument.from_dict`, then populate properties
   c. Branch: else try `('$type',)` init key
   d. Branch: if instrument created -> append
   e. Branch: else -> raise `ValueError('Neither asset_class/type nor $type specified')`
4. Return `cls(instruments)`

**Raises:** `ValueError` when neither asset_class/type nor $type columns are present

### Portfolio.from_csv(cls, csv_file: str, mappings: Optional[dict] = None) -> Portfolio  [classmethod]
Purpose: Load portfolio from CSV file.

**Algorithm:**
1. Read CSV, replace NaN with None
2. Check for duplicate columns (matching `\.\d` pattern indicating pandas auto-rename)
3. Branch: if duplicates found -> raise `ValueError`
4. Delegate to `from_frame`

**Raises:** `ValueError` when duplicate column names detected

### Portfolio.scale(self, scaling: int, in_place: bool = True)
Purpose: Scale all instrument quantities.

**Algorithm:**
1. Get instruments via `_get_instruments`
2. Branch: if `in_place` -> scale each instrument in place (returns None)
3. Branch: else -> return new Portfolio with scaled clones

### Portfolio.append(self, priceables: Union[PriceableImpl, Iterable[PriceableImpl]])
Purpose: Add priceables to the portfolio.

**Algorithm:**
1. Branch: if single `PriceableImpl` -> wrap in tuple
2. Branch: else -> convert to tuple
3. Concatenate with existing priceables (triggers setter, rebuilds index)

### Portfolio.pop(self, item) -> PriceableImpl
Purpose: Remove and return a priceable by index/name.

**Algorithm:**
1. Get priceable via `self[item]`
2. Rebuild priceables excluding that item
3. Return removed priceable

### Portfolio.extend(self, portfolio: Iterable)
Purpose: Add all items from an iterable to the portfolio.

### Portfolio.to_frame(self, mappings: Optional[dict] = None) -> pd.DataFrame
Purpose: Convert portfolio to DataFrame.

**Algorithm:**
1. Define recursive `to_records(portfolio)`:
   a. For each priceable:
      - Branch: if Portfolio -> recurse
      - Branch: else -> convert to dict via `as_dict()`
        - Branch: if not `hasattr(priceable, 'asset_class')` -> add `$type` key
      - Add `instrument` and `portfolio` (name) to record
2. Build DataFrame, set index to `['portfolio', 'instrument']`
3. Sort columns, move asset type columns to front in order: `$type`, `type`, `asset_class`
4. Apply mappings:
   a. Branch: if value is `str` -> copy column
   b. Branch: if value is `callable` -> apply function row-wise
5. Return DataFrame

### Portfolio.to_csv(self, csv_file: str, mappings: Optional[dict] = None, ignored_cols: Optional[list] = None)
Purpose: Export portfolio to CSV file.

**Algorithm:**
1. Call `to_frame(mappings)`
2. Remove ignored columns via `np.setdiff1d`
3. Reset index, write CSV

### Portfolio.all_paths (property) -> Tuple[PortfolioPath, ...]
Purpose: Return PortfolioPath objects for every leaf instrument in the tree.

**Algorithm:**
1. Stack-based DFS traversal
2. For each priceable:
   a. Branch: if Portfolio -> push onto stack with accumulated path
   b. Branch: else -> append path to results

### Portfolio.paths(self, key: Union[str, PriceableImpl]) -> Tuple[PortfolioPath, ...]
Purpose: Find all paths to a named or specific priceable in the tree.

**Algorithm:**
1. Branch: if key is not `(str, Instrument, Portfolio)` -> raise `ValueError`
2. Branch: if `isinstance(key, str)` -> look up in `__priceables_by_name`
3. Branch: else -> linear scan comparing equality or `unresolved` attribute
4. Build paths from indices at this level
5. Recurse into sub-portfolios, prepending sub-portfolio path
6. Return all paths

**Raises:** `ValueError` when key is not str, Instrument, or Portfolio

### Portfolio.resolve(self, in_place: bool = True) -> Optional[Union[PricingFuture, PriceableImpl, dict]]
Purpose: Resolve all instruments in the portfolio (fills in server-side details).

**Algorithm:**
1. Get instruments via `_get_instruments`
2. Open pricing context, call `p.resolve(in_place)` for each priceable
3. Branch: if `in_place` -> return None (instruments mutated in place)
4. Branch: if not `in_place`:
   a. Branch: if current context is `HistoricalPricingContext` -> `ret = {}` (date-keyed dict)
   b. Branch: else -> `ret = Portfolio(name=self.name)`
   c. Branch: if `self._return_future` -> create `result_future`
   d. Define callback `cb`:
      - Branch: if `ret` is Portfolio -> set priceables from future results
      - Branch: if `ret` is dict:
        - Group results by date
        - Branch: for each date, if any result is not PriceableImpl -> log error, skip date
        - Branch: else -> create Portfolio for that date
      - Branch: if `result_future` -> set result on it
   e. Attach callback to `CompositeResultFuture(futures)`
   f. Return `result_future` or `ret`

### Portfolio.market(self) -> Union[OverlayMarket, PricingFuture, dict]
Purpose: Get combined market data for all instruments in the portfolio.

**Algorithm:**
1. Get instruments (not in_place, not return_priceables -> uses all_instruments)
2. Open pricing context, call `i.market()` for each instrument
3. Create `result_future = PricingFuture()`
4. Define callback `cb`:
   a. Define `update_market_data` helper that checks for conflicting values (> 1e-6 tolerance)
   b. Get all results
   c. Branch: if `isinstance(results[0], dict)` -> `is_historical = True`
   d. Branch: if not historical -> merge all market_data_dicts into single dict
   e. Branch: if historical -> merge by base_market date
   f. Branch: if `market_data` -> create single `OverlayMarket`
   g. Branch: else -> create date-keyed dict of `OverlayMarket`s
   h. Set result on `result_future`
5. Attach callback to `CompositeResultFuture`
6. Branch: if `self._return_future` -> return future; else -> return `result_future.result()` (blocking)

**Raises:** `ValueError` (in callback) when conflicting market data values for same coordinate

### Portfolio.calc(self, risk_measure: Union[RiskMeasure, Iterable[RiskMeasure]], fn=None) -> PortfolioRiskResult
Purpose: Calculate risk measures for all instruments in the portfolio. This is the primary entry point for risk computation.

**Algorithm:**
1. Get instruments via `_get_instruments(position_date, in_place=False, return_priceables=True)`
2. Open `self._pricing_context`
3. Clone self to prevent mutation side effects
4. Branch: if `risk_measure` is single `RiskMeasure` -> wrap in tuple
5. Call `p.calc(risk_measure, fn=fn)` for each priceable (recursive for sub-portfolios)
6. Return `PortfolioRiskResult(cloned_portfolio, risk_measures, futures_list)`

**Elixir port notes:** This is the key aggregation method. Each priceable's `calc()` returns a `PricingFuture`. The `PortfolioRiskResult` wraps these futures and provides aggregation/slicing. The clone prevents the result from being affected by later in-place mutations of the portfolio. The `fn` parameter allows custom post-processing per instrument.

### Portfolio._get_instruments(self, position_date: dt.date, in_place: bool, return_priceables: bool = True) -> tuple/list
Purpose: Get instruments, fetching from API if portfolio has an ID, otherwise returning local priceables.

**Algorithm:**
1. Branch: if `self.id`:
   a. Get all position dates, filter to dates before `position_date`
   b. Branch: if no prior dates -> raise `ValueError`
   c. Get positions for max prior date
   d. Convert positions to instruments
   e. Branch: if `in_place` -> set `self.__priceables`
   f. Return instruments
2. Branch: else:
   a. Branch: if `return_priceables` -> return `self.__priceables`
   b. Branch: else -> return `self.all_instruments`

**Raises:** `ValueError` when portfolio has no positions on or before the position date

### Portfolio.clone(self, clone_instruments: bool = False) -> Portfolio
Purpose: Create a copy of the portfolio, optionally deep-cloning instruments.

**Algorithm:**
1. For each priceable:
   a. Branch: if `isinstance(p, Portfolio)` -> recursive `p.clone(clone_instruments)`
   b. Branch: elif `clone_instruments` -> `p.clone()`
   c. Branch: else -> reference same `p`
2. Create new Portfolio with cloned priceables and same name
3. Copy `__id` and `__quote_id`
4. Return clone

### Grid.__init__(self, priceable, x_param, x_values, y_param, y_values, name=None)
Purpose: Create a 2D grid of instruments as a nested Portfolio structure.

**Algorithm:**
1. Build x_overrides: list of `{x_param: v, 'name': v}` for each x_value
2. Build y_overrides: list of `{y_param: v}` for each y_value
3. For each y_override, create a sub-Portfolio of cloned instruments (one per x_override)
4. Sub-portfolio name = y_value (via `next(iter(y.values()))`)
5. Pass list of sub-portfolios to `super().__init__`

## State Mutation
- `self.__priceables`: Set via setter in `__init__`, `append`, `pop`, `extend`, `resolve` (in_place), `_get_instruments` (in_place), `scale` (in_place)
- `self.__priceables_by_name`: Rebuilt every time `priceables` setter is called
- `self.__id`: Set in `save()`, `get()`, `from_asset_id()`, `clone()`
- `self.__quote_id`: Set in `save_as_quote()`, `from_quote()`, `clone()`
- `self.name`: Set in `__init__`, can be set on priceables in dict constructor
- Individual instrument state: Mutated by `resolve(in_place=True)` and `scale(in_place=True)`
- Thread safety: No concurrent access patterns in Portfolio itself; async behavior is delegated to PricingContext

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `__add__` | When `other` is not a Portfolio |
| `ValueError` | `save` | When portfolio has nested sub-portfolios |
| `ValueError` | `save` | When overwrite=False and ID already exists |
| `ValueError` | `save` | When no name is set for new portfolio |
| `ValueError` | `save_as_quote` | When portfolio has nested sub-portfolios |
| `ValueError` | `save_as_quote` | When overwrite=False and quote_id exists |
| `ValueError` | `save_to_shadowbook` | When portfolio has nested sub-portfolios |
| `ValueError` | `from_frame` | When neither asset_class/type nor $type specified |
| `ValueError` | `from_csv` | When duplicate column names detected |
| `ValueError` | `paths` | When key is not str, Instrument, or Portfolio |
| `ValueError` | `_get_instruments` | When no positions exist on/before position date |
| `ValueError` | `market` (callback) | When conflicting market data values for same coordinate |

## Edge Cases
- `__init__` with dict argument: the `name` parameter is overwritten by the loop variable in the dict iteration (line 65: `for name, priceable in priceables.items()`), so the `name` argument to `__init__` is lost if priceables is a dict. However, the outer `self.name = name` on line 72 uses the constructor parameter since the loop variable is scoped differently -- actually in Python the loop variable `name` will shadow the parameter, so `self.name` will be set to the last key from the dict, not the `name` parameter. This is a bug.
- `__eq__` catches `IndexError` and `TypeError` for structural mismatches, which is a broad catch
- `__contains__` checks `all_portfolios + (self,)` which creates a new tuple each time
- `priceables` setter handles single PriceableImpl by wrapping in tuple; `None` priceables would fail
- `_get_instruments` with `self.id` set fetches from API and may set `self.__priceables` directly (bypassing setter, so `__priceables_by_name` is NOT rebuilt)
- `pop` rebuilds priceables by filtering `self.instruments` (not `self.__priceables`), so sub-portfolios would be dropped
- `resolve` with `in_place=False` in `HistoricalPricingContext` returns a dict keyed by date with error-date skipping
- `market()` callback checks for conflicting values with `abs(existing - value) > 1e-6` tolerance
- `save` uses `id` (builtin) in error message instead of `self.__id` (line 308: `f'Portfolio with id {id}'`)
- `from_asset_id` response handling: checks for tuple then PositionSet vs dict
- `clone` preserves `__id` and `__quote_id` from source, which means the clone references the same server-side portfolio
- `all_portfolios` uses stack-based traversal with deduplication; the `if portfolio in portfolios: continue` check on line 215 may skip portfolios that were added to `portfolios` list but not yet processed (since `stack` starts as a copy of `self.portfolios` which are already in `portfolios`)

## Bugs Found
- Lines 63-68: `__init__` dict constructor shadows `name` parameter with loop variable, so `self.name` on line 72 gets the last dict key, not the `name` argument (OPEN)
- Line 308: `save()` error message uses `id` (Python builtin function) instead of `self.__id` (OPEN)
- Line 607: `_get_instruments` with `in_place=True` sets `self.__priceables` directly without going through the setter, so `__priceables_by_name` is not rebuilt (OPEN)

## Coverage Notes
- Branch count: ~62
- Critical branches for Elixir port:
  - `calc()`: risk_measure single vs iterable, recursive priceable dispatch
  - `resolve()`: in_place vs not, HistoricalPricingContext vs regular, future vs blocking
  - `market()`: historical vs non-historical, conflicting value detection, future vs blocking
  - `_get_instruments()`: has-id (API fetch) vs local, in_place vs not, return_priceables vs all_instruments
  - `__getitem__`: int/slice vs PortfolioPath vs str/list, single vs multiple results
  - `__contains__`: PriceableImpl vs str vs other
  - `from_frame`: asset_class/type vs $type init, callable vs string mappings
  - `from_asset_id`: date vs no-date, tuple vs non-tuple response, PositionSet vs dict
  - `save`: nested check, id exists + overwrite check, name check, non-empty positions check
  - `clone`: Portfolio vs instrument, clone_instruments flag
- `__eq__` has IndexError/TypeError exception-based branches
- `to_frame` has multiple column-ordering branches and mapping type branches
