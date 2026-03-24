# timeseries/measures_factset.py

## Summary
Provides FactSet-sourced timeseries measures for single-stock equities: consensus estimates, fundamental financial data, and broker ratings. The module defines a large taxonomy of enums covering estimate items, fundamental line items (basic, derived, advanced), fiscal periods, and rating types. Three public decorated functions (`factset_estimates`, `factset_fundamentals`, `factset_ratings`) query Goldman Sachs datasets and return `ExtendedSeries` (pandas Series subclass) results suitable for plotting.

## Dependencies
- Internal: `gs_quant.common` (`AssetClass`, `AssetType`)
- Internal: `gs_quant.data` (`DataContext`, `Dataset`)
- Internal: `gs_quant.errors` (`MqValueError`)
- Internal: `gs_quant.markets.securities` (`AssetIdentifier`, `Asset`)
- Internal: `gs_quant.timeseries` (`RelativeDate`)
- Internal: `gs_quant.timeseries.helper` (`plot_measure`)
- Internal: `gs_quant.timeseries.measures` (`ExtendedSeries`)
- External: `datetime` (`dt` alias)
- External: `enum` (`Enum`)
- External: `typing` (`Union`, `Optional`)
- External: `pandas` (`pd` alias)

## Type Definitions

### FiscalPeriod (class)
Inherits: `object`

Represents an absolute fiscal period defined by a year and a sub-period number. Used as the `period` argument to `factset_estimates` when the caller wants a non-rolling, fixed fiscal window.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `y` | `Union[int, str, None]` | `None` | Fiscal year |
| `p` | `Union[int, str, None]` | `None` | Period number within the year (quarter index 1-4, semi-annual index 1-2, or None for annual) |

### FundamentalMetric (dynamic Enum)
```
FundamentalMetric = Enum(
    'FundamentalMetric',
    {
        **fundamental_basic_dict,
        **fundamental_basic_derived_dict,
        **fundamental_advanced_dict,
        **fundamental_advanced_derived_dict,
    },
)
```
A dynamically constructed `Enum` whose members are the union of all four fundamental item enums (basic, basic-derived, advanced, advanced-derived). Each member name matches the original enum member name (e.g. `SALES`, `ROE`, `EBITDA_OPER`) and each value is the corresponding human-readable string. This combined enum is the `metric` parameter type for `factset_fundamentals`.

## Enums and Constants

### EstimateItem(Enum)
Financial estimate line items supported by FactSet consensus/actuals datasets.

| Value | Raw | Description |
|-------|-----|-------------|
| EPS | `'Earnings Per Share'` | Earnings per share |
| EPS_C | `'Consolidated EPS'` | Consolidated earnings per share |
| EPS_P | `'Standalone Earnings Per Share'` | Standalone earnings per share |
| SALES | `'Sales'` | Total sales |
| SALES_C | `'Consolidated Sales'` | Consolidated sales |
| SALES_P | `'Standalone Sales'` | Standalone sales |
| DPS | `'Declared Dividends Per Share'` | Dividends per share |
| CFPS | `'Cash Flow Per Share'` | Cash flow per share |
| PRICE_TGT | `'Target Price'` | Analyst target price |
| EPS_LTG | `'Long Term Growth'` | Long-term growth rate |
| AFFO | `'Adjusted Funds From Operations'` | AFFO |
| ASSETS | `'Total Assets'` | Total assets |
| BFNG | `'Net Income Reported'` | Net income reported |
| BPS | `'Book Value Per Share'` | Book value per share |
| BPS_TANG | `'Tangible Book Value Per Share'` | Tangible book value per share |
| CAPEX | `'Capital Expenditure'` | Capital expenditure |
| CF_FIN | `'Cash Flow From Financing'` | Cash flow from financing |
| CF_INV | `'Cash Flow From Investing'` | Cash flow from investing |
| CF_OP | `'Cash Flow From Operations'` | Cash flow from operations |
| CURRENT_ASSETS | `'Current Assets'` | Current assets |
| CURRENT_LIABILITIES | `'Current Liabilities'` | Current liabilities |
| DEFREVENUE_LT | `'Deferred Revenue Long Term'` | Deferred revenue long term |
| DEFREVENUE_ST | `'Deferred Revenue Short Term'` | Deferred revenue short term |
| DEP_AMORT | `'Depreciation and Amortization'` | Depreciation and amortization |
| EBIT | `'EBIT'` | Earnings before interest and taxes |
| EBIT_ADJ | `'BEIT Adjusted'` | EBIT adjusted |
| EBIT_C | `'Consolidated EBIT'` | Consolidated EBIT |
| EBIT_P | `'Standalone EBIT'` | Standalone EBIT |
| EBITA | `'EBITA'` | Earnings before interest, taxes, amortization |
| EBITDA | `'EBITDA'` | Earnings before interest, taxes, depreciation, amortization |
| EBITDA_ADJ | `'EBITDA Adjusted'` | EBITDA adjusted |
| EBITDA_REP | `'EBITDA Reported'` | EBITDA reported |
| EBITDAR | `'EBITDAR'` | EBITDA plus rent |
| EBITR | `'EBIT Reported'` | EBIT reported |
| EPS_EX_XORD | `'Earnings Per Share Excluding Exceptionals'` | EPS excl. exceptionals |
| EPS_GAAP | `'Reported Earnings Per Share'` | GAAP reported EPS |
| EPS_NONGAAP | `'Earnings Per Share Non GAAP'` | Non-GAAP EPS |
| EPSAD | `'Diluted Adjusted EPS'` | Diluted adjusted EPS |
| EPSRD | `'Diluted Reported EPS'` | Diluted reported EPS |
| FCF | `'Free Cash Flow'` | Free cash flow |
| FCFPS | `'Free Cash Flow Per Share'` | Free cash flow per share |
| FFO | `'Funds From Operations'` | Funds from operations |
| G_A_EXP | `'General and Administrative Expense'` | G&A expense |
| GW_TOT | `'Total Goodwill'` | Total goodwill |
| HEPSB | `'Headline Basic EPS'` | Headline basic EPS |
| HEPSD | `'Headline Diluted EPS'` | Headline diluted EPS |
| INC_GROSS | `'Gross Income'` | Gross income |
| INT_EXP | `'Interest Expense'` | Interest expense |
| INVENTORIES | `'Inventories'` | Inventories |
| MAINT_CAPEX | `'Maintenance CAPEX'` | Maintenance capex |
| NDT | `'Net Debt'` | Net debt |
| NET | `'Net Profit'` | Net profit |
| NET_C | `'Consolidated Net Income'` | Consolidated net income |
| NET_P | `'Standalone Net Income'` | Standalone net income |
| NET_SALES | `'Net Sales'` | Net sales |
| NETBG | `'Net Profit Adjusted'` | Net profit adjusted |
| ORGANICGROWTH | `'Organic Growth'` | Organic growth |
| PTI | `'Pre Tax Profit'` | Pre-tax profit |
| PTI_C | `'Consolidated Pretax Income'` | Consolidated pretax income |
| PTIAG | `'Pre Tax Profit Reported'` | Pre-tax profit reported |
| PTP_P | `'Standalone Pretax Income'` | Standalone pretax income |
| PTPA | `'Pre Tax Income Adjusted'` | Pre-tax income adjusted |
| RD_EXP | `'Research and Development'` | R&D expense |
| REV_TOT | `'Total Revenues'` | Total revenues |
| S_M_EXP | `'Selling and Marketing Expense'` | S&M expense |
| SGA | `'Selling General and Administrative Expense'` | SG&A expense |
| SH_EQUITY | `'Shareholders Equity'` | Shareholders equity |
| SOE | `'Stock Option Expense'` | Stock option expense |
| TAX_EXPENSE | `'Tax Expense'` | Tax expense |
| TOTAL_DEBT | `'Total Debt'` | Total debt |

**Total: 72 members**

### EstimateStatistic(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| MEAN | `'Mean'` | Consensus mean estimate |
| MEDIAN | `'Median'` | Consensus median estimate |
| HIGH | `'High'` | Highest analyst estimate |
| LOW | `'Low'` | Lowest analyst estimate |
| ACTUAL | `'Actual'` | Reported actual value |

### EstimateBasis(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ANN | `'Annual'` | Annual reporting basis |
| QTR | `'Quarterly'` | Quarterly reporting basis |
| SEMI | `'Semi annual'` | Semi-annual reporting basis |
| NTM | `'Next Twelve Months'` | Next twelve months (rolling) |
| STM | `'Second Twelve Months'` | Second twelve months (rolling) |

### FundamentalBasis(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| ANN | `'Annual'` | Annual fundamental data |
| QTR | `'Quarterly'` | Quarterly fundamental data |
| SEMI | `'Semi annual'` | Semi-annual fundamental data |

### FundamentalBasicItem(Enum)
Contains 116 members representing basic (non-derived) fundamental financial line items. Key examples: `SALES` (`'SalesRevenue'`), `NET_INCOME` (`'Net Income incl Discontinued Operations'`), `ASSETS` (`'Total Assets'`), `COM_EQ` (`'Common Equity Total'`), `BPS` (`'Book Value Per Share'`), `DPS` (`'Dividends Per Share'`), `EPS_REPORTED` (`'Earnings Per Share As Reported'`), `COM_SHS_OUT` (`'Common Shares Outstanding'`), etc. Member names become keys in `fundamental_basic_dict` and feed into the combined `FundamentalMetric` enum. See source lines 127-243 for the exhaustive list.

**Total: 116 members**

### FundamentalBasicDerivedItem(Enum)
Contains 64 members representing derived (calculated) basic fundamental items. Key examples: `COGS` (`'Cost of Goods Sold COGS including Depreciation Amortization'`), `NET_INC` (`'Net Income'`), `EBITDA_OPER` (`'EBITDA'`), `ROE` (`'Return On Average Total Equity'`), `ROA` (`'Return On Average Assets'`), `GROSS_MGN` (`'Gross Profit Margin'`), `PE` (`'Price To Earnings...'`), `EPS_DIL` (`'EPS Fully Diluted'`), `ENTRPR_VAL` (`'Enterprise Value Using Diluted Shares'`), etc. See source lines 245-308 for the exhaustive list.

**Total: 64 members**

### FundamentalAdvancedItem(Enum)
Contains 490 members representing advanced (detailed) fundamental financial line items. Covers granular breakdowns of pension/healthcare data, property/plant/equipment, debt instruments (bonds, loans, revolvers by seniority/security), intangibles, insurance, banking metrics, lease commitments, stock compensation, and more. See source lines 311-801 for the exhaustive list.

**Total: 490 members**

### FundamentalAdvancedDerivedItem(Enum)
Contains 218 members representing derived advanced fundamental items. Covers computed ratios, growth rates, per-employee metrics, coverage ratios, margin calculations, and composite financial analytics. Key examples: `ZSCORE` (`'ALTMANS Z SCORE'`), `FSCORE` (`'Piotroski F Score'`), `ROIC` (`'Return On Average Invested Capital'`), `DIV_YLD` (`'Dividend Yield Close'`), `EBITDA_OPER_MGN` (`'EBITDA Margin'`), `FCF_YLD` (`'Free Cash Flow Yield'`). See source lines 804-1021 for the exhaustive list.

**Total: 218 members**

### FundamentalFormat(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| RESTATED | `'Restated'` | Use restated financial data |
| NON_RESTATED | `'Non Restated'` | Use non-restated (as-reported) data |

### RatingType(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| BUY | `'Buy'` | Number of buy ratings |
| OVERWEIGHT | `'Overweight'` | Number of overweight ratings |
| HOLD | `'Hold'` | Number of hold ratings |
| UNDERWEIGHT | `'Underweight'` | Number of underweight ratings |
| SELL | `'Sell'` | Number of sell ratings |
| NONE | `'No Recommendations'` | Number of no-recommendation ratings |
| TOTAL | `'Total'` | Total number of ratings |
| SCORE | `'Numeric Score'` | Standardized numeric consensus score |

### Module Constants

| Name | Type | Value | Description |
|------|------|-------|-------------|
| `fundamental_basic_dict` | `dict[str, str]` | `{item.name: item.value for item in FundamentalBasicItem}` | Name-to-value mapping for basic items |
| `fundamental_basic_derived_dict` | `dict[str, str]` | `{item.name: item.value for item in FundamentalBasicDerivedItem}` | Name-to-value mapping for basic derived items |
| `fundamental_advanced_dict` | `dict[str, str]` | `{item.name: item.value for item in FundamentalAdvancedItem}` | Name-to-value mapping for advanced items |
| `fundamental_advanced_derived_dict` | `dict[str, str]` | `{item.name: item.value for item in FundamentalAdvancedDerivedItem}` | Name-to-value mapping for advanced derived items |
| `BASIC_MEASURES` | `list[EstimateItem]` | `[EPS, EPS_C, EPS_P, SALES, SALES_C, SALES_P, DPS, CFPS, PRICE_TGT, EPS_LTG]` | Estimate items that use the BASIC dataset prefix |
| `LT_MEASURES` | `list[EstimateItem]` | `[PRICE_TGT, EPS_LTG]` | Estimate items that use the long-term (LT) dataset variant |
| `BASIS_TO_DATASET` | `dict[EstimateBasis, str]` | `{ANN: 'AF', QTR: 'QF', SEMI: 'SAF', NTM: 'NTM', STM: 'NTM'}` | Maps estimate basis to dataset ID suffix. Note: both NTM and STM map to `'NTM'` |
| `BASIS_TO_FIELD` | `dict[EstimateBasis, str]` | `{ANN: 'Af', QTR: 'Qf', SEMI: 'Saf', NTM: 'Ntm', STM: 'Stm'}` | Maps estimate basis to column name suffix fragment |
| `FF_BASIS_TO_DATASET` | `dict[FundamentalBasis, str]` | `{ANN: 'AF', QTR: 'QF', SEMI: 'SAF'}` | Maps fundamental basis to dataset ID suffix |
| `FF_BASIS_TO_FIELD` | `dict[FundamentalBasis, str]` | `{ANN: 'Af', QTR: 'Qf', SEMI: 'Saf'}` | Maps fundamental basis to column name suffix |
| `RATING_TO_FIELD` | `dict[RatingType, str]` | `{BUY: 'feBuy', OVERWEIGHT: 'feOver', HOLD: 'feHold', UNDERWEIGHT: 'feUnder', SELL: 'feSell', NONE: 'feNoRec', TOTAL: 'feTotal', SCORE: 'feMark'}` | Maps rating type to dataset column name |

## Functions/Methods

### FiscalPeriod.__init__(self, y: Union[int, str, None] = None, p: Union[int, str, None] = None)
Purpose: Initialize a fiscal period with year and period number.

**Algorithm:**
1. Store `y` as `self.y`
2. Store `p` as `self.p`

### FiscalPeriod.as_dict(self) -> dict
Purpose: Serialize the fiscal period to a dictionary.

**Algorithm:**
1. Return `{'y': self.y, 'p': self.p}`

### FiscalPeriod.from_dict(cls, obj) -> FiscalPeriod
Purpose: Class method to deserialize a fiscal period from a dictionary.

**Algorithm:**
1. Return `FiscalPeriod(y=obj.get('y'), p=obj.get('p'))`

### factset_estimates(asset: Asset, metric: EstimateItem = EstimateItem.EPS, statistic: EstimateStatistic = EstimateStatistic.MEAN, report_basis: EstimateBasis = EstimateBasis.ANN, period: Union[int, FiscalPeriod, None] = 1, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve FactSet consensus estimates or actuals for a single stock, decorated with `@plot_measure` restricted to `AssetClass.Equity` and `AssetType.Single_Stock`.

**Algorithm:**
1. Validate `report_basis` is an `EstimateBasis` instance; raise `MqValueError` if not.
2. Get `start` and `end` from `DataContext.current.start_date` / `end_date`.
3. Compute `start_new` = `RelativeDate('-1y', base_date=start).apply_rule()` (one year before start, for lookback).
4. Determine dataset classification:
   - `basic` = `'BASIC'` if `metric in BASIC_MEASURES`, else `'ADVANCED'`
   - `column_prefix` = `''` if `metric in BASIC_MEASURES`, else `'Adv'`
   - `consensus` = `'CONH'` if `statistic != EstimateStatistic.ACTUAL`, else `'ACT'`
   - `basis_ds` = `'LT'` if `metric in LT_MEASURES`, else `BASIS_TO_DATASET[report_basis]`
   - `basis_cl` = `'Lt'` if `metric in LT_MEASURES`, else `BASIS_TO_FIELD[report_basis]`
5. Build dataset ID:
   - Branch: `report_basis in [NTM, STM]` --> `ds_id = f'FE_{basis_ds}'` (e.g. `'FE_NTM'`)
   - Branch: otherwise --> `ds_id = f'FE_{basic}_{consensus}_{basis_ds}_GLOBAL'` (e.g. `'FE_BASIC_CONH_AF_GLOBAL'`)
6. Instantiate `Dataset(ds_id)` and get Bloomberg ID from `asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)`.
7. Query dataset with `ds.get_data(bbid=bbid, start=start_new, end=end, feItem=metric.name)`, wrapped in try/except that raises `MqValueError` with the underlying exception message.
8. If `df` is empty, raise `MqValueError` with "No data found" message.
9. Reset index on `df`.
10. **Statistic == ACTUAL path:**
    - Branch: `report_basis in [NTM, STM]` --> raise `MqValueError('NTM and STM are not supported for actual values')`
    - Branch: `metric in LT_MEASURES` --> raise `MqValueError(f'No actual data for {metric.value}')`
    - Otherwise: select columns `['feFpEnd', 'feValue']`, rename `feFpEnd` to `date`, convert `date` to datetime, set `column = 'feValue'`
11. **Non-ACTUAL, non-NTM/STM path** (report_basis not in [NTM, STM]):
    - Branch: `metric in LT_MEASURES` --> pass (no filtering needed, LT data has no period dimension)
    - Branch: `isinstance(period, int)` --> filter `df` where `fePerRel == period`
    - Branch: `period` is `FiscalPeriod` --> compute fiscal period date range:
      - **Annual**: `fiscal_period_start = datetime(period.y, 1, 1)`, `fiscal_period_end = datetime(period.y, 12, 31)`
      - **Quarterly**: validate `period.p` is in [1,2,3,4] (raise `MqValueError` if int but not in range); raise `MqValueError` if `period.p is None`; compute `fiscal_period_start = datetime(period.y, (period.p-1)*3+1, 1)`, `fiscal_period_end = start + 3 months - 1 day`
      - **Semi-annual**: validate `period.p` is in [1,2] (raise `MqValueError` if int but not in range); raise `MqValueError` if `period.p is None`; compute `fiscal_period_start = datetime(period.y, (period.p-1)*6+1, 1)`, `fiscal_period_end = start + 6 months - 1 day`
      - Filter `df` where `feFpEnd` falls within `[fiscal_period_start, fiscal_period_end]`
      - If filtered `df` is empty, raise `MqValueError('No Data returned for selected fiscal period')`
    - Fill NaN `consEndDate` with `end`.
    - Expand each row into a daily date range from `date` to `consEndDate` using `pd.date_range`, then explode into individual rows.
    - Drop original `date` and `consEndDate` columns, rename `date_range` to `date`.
    - Set `column = f'fe{column_prefix}{statistic.value}{basis_cl}'` (e.g. `'feMeanAf'`, `'feAdvMedianQf'`)
12. **NTM/STM path** (non-ACTUAL, report_basis in [NTM, STM]):
    - Convert `date` column to datetime.
    - Set `column = f'fe{statistic.value}{basis_cl}'` (e.g. `'feMeanNtm'`, `'feMeanStm'`)
13. Filter `df` to rows where `date >= start`.
14. Sort by `date` ascending and set `date` as index.
15. Create `ExtendedSeries` from `df[column]` with `name=statistic.value`.
16. Normalize index to nanosecond-precision `DatetimeIndex` (using `as_unit('ns')` if available).
17. Set `series.dataset_ids = ds.id`.
18. Return series.

**Raises:** `MqValueError` in multiple conditions (see Error Handling).

### factset_fundamentals(asset: Asset, metric: FundamentalMetric = FundamentalMetric.EPS_BASIC, report_basis: FundamentalBasis = FundamentalBasis.ANN, report_format: FundamentalFormat = FundamentalFormat.NON_RESTATED, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve FactSet fundamental financial data for a single stock, decorated with `@plot_measure` restricted to `AssetClass.Equity` and `AssetType.Single_Stock`.

**Algorithm:**
1. Get `start` and `end` from `DataContext.current`.
2. Compute `start_new` = `RelativeDate('-1y', base_date=start).apply_rule()`.
3. Determine dataset classification:
   - `basic` = `'BASIC'` if `metric.name` is in the merged dict of `fundamental_basic_dict` and `fundamental_basic_derived_dict`, else `'ADVANCED'`
   - `derived` = `'_DER'` if `metric.name` is in the merged dict of `fundamental_basic_derived_dict` and `fundamental_advanced_derived_dict`, else `''`
4. Determine restated flag:
   - Branch: `report_format == FundamentalFormat.RESTATED` --> `restated = '_R'`
   - Branch: otherwise --> `restated = ''`
5. Build dataset ID: `ds_id = f'FF_{basic}{derived}{restated}_{FF_BASIS_TO_DATASET[report_basis]}_GLOBAL'` (e.g. `'FF_BASIC_AF_GLOBAL'`, `'FF_BASIC_DER_R_QF_GLOBAL'`, `'FF_ADVANCED_DER_SAF_GLOBAL'`)
6. Instantiate `Dataset(ds_id)` and query with `ds.get_data(bbid=..., start=start_new, end=end)`.
7. If `df` is empty, raise `MqValueError`.
8. Reset index.
9. Compute column name: `'ff' + metric.name.replace('_', ' ').title().replace(' ', '')` (e.g. `GROSS_MGN` -> `'ffGrossMgn'`, `EPS_BASIC` -> `'ffEpsBasic'`).
10. Select only `['date', column]` from `df`.
11. Create a full daily date range from `start_new` to `end`.
12. Left-join the data onto the full date range to fill gaps.
13. Forward-fill the metric column.
14. Filter to rows where `date >= start`.
15. Sort by date, set as index.
16. Create `ExtendedSeries` with `name=metric.value`.
17. Normalize index to nanosecond-precision `DatetimeIndex`.
18. Set `series.dataset_ids = ds.id`.
19. Return series.

**Raises:** `MqValueError` when no data found.

### factset_ratings(asset: Asset, rating_type: RatingType = RatingType.BUY, *, source: str = None, real_time: bool = False, request_id: Optional[str] = None) -> pd.Series
Purpose: Retrieve FactSet consensus broker ratings for a single stock, decorated with `@plot_measure` restricted to `AssetClass.Equity` and `AssetType.Single_Stock`.

**Algorithm:**
1. Get `start` and `end` from `DataContext.current`.
2. Compute `start_new` = `RelativeDate('-1y', base_date=start).apply_rule()`.
3. Fixed dataset: `ds_id = 'FE_BASIC_CONH_REC_GLOBAL'`.
4. Instantiate `Dataset(ds_id)` and query with `ds.get_data(bbid=..., start=start_new, end=end)`.
5. Reset index and fill NaN `consEndDate` with `end`.
6. Expand each row into a daily date range from `date` to `consEndDate` using `pd.date_range`, then explode.
7. Drop original `date` and `consEndDate` columns, rename `date_range` to `date`.
8. Filter to `date >= start`, sort ascending, set `date` as index.
9. Create `ExtendedSeries` from `df[RATING_TO_FIELD[rating_type]]` with `name=rating_type.value`.
10. Normalize index to nanosecond-precision `DatetimeIndex`.
11. Set `series.dataset_ids = ds.id`.
12. Return series.

## State Mutation
- No module-level mutable state is modified at runtime. All dict/list constants (`fundamental_basic_dict`, `BASIC_MEASURES`, etc.) are constructed once at import time and never mutated.
- `series.dataset_ids`: Set on the returned `ExtendedSeries` object as a side effect.
- Thread safety: Functions read from `DataContext.current` which is thread-local via `ContextBaseWithDefault`.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `factset_estimates` | `report_basis` is not an `EstimateBasis` instance |
| `MqValueError` | `factset_estimates` | Dataset query fails (wraps underlying exception message) |
| `MqValueError` | `factset_estimates` | Query returns empty DataFrame |
| `MqValueError` | `factset_estimates` | `statistic == ACTUAL` and `report_basis in [NTM, STM]` |
| `MqValueError` | `factset_estimates` | `statistic == ACTUAL` and `metric in LT_MEASURES` |
| `MqValueError` | `factset_estimates` | Quarterly `FiscalPeriod` with `period.p` not in [1,2,3,4] |
| `MqValueError` | `factset_estimates` | Quarterly `FiscalPeriod` with `period.p is None` |
| `MqValueError` | `factset_estimates` | Semi-annual `FiscalPeriod` with `period.p` not in [1,2] |
| `MqValueError` | `factset_estimates` | Semi-annual `FiscalPeriod` with `period.p is None` |
| `MqValueError` | `factset_estimates` | Filtered fiscal period DataFrame is empty |
| `MqValueError` | `factset_fundamentals` | Query returns empty DataFrame |

## Edge Cases
- **NTM and STM share dataset**: Both `EstimateBasis.NTM` and `EstimateBasis.STM` map to `'NTM'` in `BASIS_TO_DATASET`, but they have distinct column suffix mappings in `BASIS_TO_FIELD` (`'Ntm'` vs `'Stm'`).
- **LT measures bypass period filtering**: When `metric in LT_MEASURES`, the period relative filtering and fiscal period logic are entirely skipped (the `pass` branch).
- **FiscalPeriod with string `p`**: The period validation checks `isinstance(period.p, int)` before range checking. If `period.p` is a string (not int and not None), neither the range check nor the None check fires, so no validation error is raised. The code proceeds to compute `fiscal_period_start` using `period.p` in arithmetic, which would produce a `TypeError` at runtime.
- **Quarterly fiscal period end**: Uses `pd.DateOffset(months=3) - pd.DateOffset(days=1)` to compute the last day of the quarter, then explicitly converts with `pd.to_datetime`.
- **Forward-filling in fundamentals**: `factset_fundamentals` creates a complete daily calendar from `start_new` to `end` and forward-fills, meaning weekends and holidays carry forward the last reported value.
- **Date range explosion in estimates/ratings**: The `consEndDate` NaN fill with `end` means consensus periods without an explicit end date are assumed to extend to the query end date.
- **Index normalization**: All three functions use `_idx.as_unit('ns') if hasattr(_idx, 'as_unit') else _idx` for pandas version compatibility (pandas >= 2.0 has `as_unit`).

## Bugs Found
- Line 1204-1206: Quarterly period validation checks `isinstance(period.p, int) and period.p not in [1,2,3,4]` as one branch and `period.p is None` as a separate elif. If `period.p` is a non-int, non-None value (e.g., a string like `"Q1"`), neither check triggers and the subsequent arithmetic `(period.p - 1) * 3 + 1` will raise a `TypeError`. Same pattern exists for semi-annual at lines 1215-1219. (OPEN)
- Line 1058 (docstring for EBIT_ADJ): Value string is `'BEIT Adjusted'` which appears to be a typo for `'EBIT Adjusted'`. (OPEN)

## Coverage Notes
- Branch count: ~30 (across the three functions, including nested conditionals in `factset_estimates`)
- Key branch dimensions in `factset_estimates`: metric classification (BASIC vs ADVANCED, LT vs non-LT), statistic (ACTUAL vs non-ACTUAL), report_basis (NTM/STM vs other), period type (int vs FiscalPeriod), report_basis within FiscalPeriod (ANN vs QTR vs SEMI), period.p validation branches (int-out-of-range vs None)
- Key branch dimensions in `factset_fundamentals`: metric classification (BASIC vs ADVANCED, derived vs non-derived), report_format (RESTATED vs NON_RESTATED)
- `factset_ratings` has minimal branching (single linear path)
- Pragmas: none
