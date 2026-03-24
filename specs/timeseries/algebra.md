# algebra.py

## Summary
Algebra library providing basic numerical and algebraic operations on timeseries and scalars: arithmetic operators (add, subtract, multiply, divide, floor divide), mathematical functions (exp, log, power, sqrt, abs), clamping (floor, ceil), filtering by value and date, boolean logic (and, or, not, if), weighted summation, and geometric aggregation. All public functions are decorated with `@plot_function` for chart service exposure.

## Dependencies
- Internal: `gs_quant.timeseries.datetime` (align), `gs_quant.timeseries.helper` (plot_function, Interpolate), `gs_quant.errors` (MqValueError, MqTypeError)
- External: `datetime` (dt.date), `math` (math.sqrt), `functools` (reduce), `numbers` (Real), `enum` (Enum), `numpy` (np.exp, np.log, np.power, np.sqrt, np.intersect1d), `pandas` (pd.Series, pd.DatetimeIndex)

## Type Definitions

### Union Types Used Throughout
```
SeriesOrScalar = Union[pd.Series, Real]
```
Most arithmetic functions accept either a pd.Series or a numbers.Real for both operands, and return the corresponding type.

## Enums and Constants

### FilterOperator(Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| LESS | `'less_than'` | Remove values less than threshold |
| GREATER | `'greater_than'` | Remove values greater than threshold |
| L_EQUALS | `'l_equals'` | Remove values less than or equal to threshold |
| G_EQUALS | `'g_equals'` | Remove values greater than or equal to threshold |
| EQUALS | `'equals'` | Remove values equal to threshold |
| N_EQUALS | `'not_equals'` | Remove values not equal to threshold |

### Module Constants
None.

## Functions/Methods

### add(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Add two series or scalars with configurable alignment.

**Formula:**

R_t = X_t + Y_t

**Algorithm:**
1. Branch: both x and y are Real -> return x + y
2. Align x and y using `align(x, y, method)`
3. Return x_align.add(y_align)

### subtract(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Subtract y from x (two series or scalars) with configurable alignment.

**Formula:**

R_t = X_t - Y_t

**Algorithm:**
1. Branch: both x and y are Real -> return x - y
2. Align x and y using `align(x, y, method)`
3. Return x_align.subtract(y_align)

### multiply(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Multiply two series or scalars with configurable alignment.

**Formula:**

R_t = X_t * Y_t

**Algorithm:**
1. Branch: both x and y are Real -> return x * y
2. Align x and y using `align(x, y, method)`
3. Return x_align.multiply(y_align)

### divide(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Divide x by y (two series or scalars) with configurable alignment.

**Formula:**

R_t = X_t / Y_t

**Algorithm:**
1. Branch: both x and y are Real -> return x / y (true division)
2. Align x and y using `align(x, y, method)`
3. Return x_align.divide(y_align)

### floordiv(x: Union[pd.Series, Real], y: Union[pd.Series, Real], method: Interpolate = Interpolate.STEP) -> Union[pd.Series, Real]
Purpose: Floor-divide x by y (two series or scalars) with configurable alignment.

**Formula:**

R_t = floor(X_t / Y_t)

**Algorithm:**
1. Branch: both x and y are Real -> return x // y
2. Align x and y using `align(x, y, method)`
3. Return x_align.floordiv(y_align)

**Alignment Methods (shared by add, subtract, multiply, divide, floordiv):**

| Method | Behavior |
|--------|----------|
| intersect | Result only on intersection of dates; non-overlapping dates ignored |
| nan | Result on union of dates; missing values become NaN |
| zero | Result on union of dates; missing values become 0 |
| step | Result on union of dates; missing values interpolated via step function |
| time | Result on union of dates/times; linear interpolation by time interval (requires DateTimeIndex) |

### exp(x: pd.Series) -> pd.Series
Purpose: Element-wise exponential (e^x) of a series.

**Formula:**

R_t = e^{X_t}

**Algorithm:**
1. Return np.exp(x)

### log(x: pd.Series) -> pd.Series
Purpose: Element-wise natural logarithm of a series.

**Formula:**

R_t = ln(X_t)

**Algorithm:**
1. Return np.log(x)

### power(x: pd.Series, y: float = 1) -> pd.Series
Purpose: Raise each element in series to a given power.

**Formula:**

R_t = X_t^y

**Algorithm:**
1. Return np.power(x, y)

### sqrt(x: Union[Real, pd.Series]) -> Union[Real, pd.Series]
Purpose: Square root of each element in a series or of a scalar.

**Formula:**

R_t = sqrt(X_t)

**Algorithm:**
1. Branch: x is pd.Series -> return np.sqrt(x)
2. Compute result = math.sqrt(x)
3. Branch: result is integral (round(result) == result) -> return round(result) as int
4. Else -> return result as float

**Note:** The integral check works correctly for values up to 2^53 due to float precision.

### abs_(x: pd.Series) -> pd.Series
Purpose: Element-wise absolute value of a series.

**Formula:**

R_t = |X_t|

**Algorithm:**
1. Return abs(x) (Python built-in, dispatches to pd.Series.__abs__)

### floor(x: pd.Series, value: float = 0) -> pd.Series
Purpose: Clamp series to a minimum value (floor).

**Formula:**

R_t = max(X_t, value)

**Algorithm:**
1. Assert x.index.is_monotonic_increasing
2. Apply lambda: max(y, value) to each element

**Note:** This is not mathematical floor (rounding down); it is a minimum-value clamp.

### ceil(x: pd.Series, value: float = 0) -> pd.Series
Purpose: Clamp series to a maximum value (ceiling/cap).

**Formula:**

R_t = min(X_t, value)

**Algorithm:**
1. Assert x.index.is_monotonic_increasing
2. Apply lambda: min(y, value) to each element

**Note:** This is not mathematical ceiling (rounding up); it is a maximum-value cap.

### filter_(x: pd.Series, operator: Optional[FilterOperator] = None, value: Optional[Real] = None) -> pd.Series
Purpose: Remove values from a series based on a comparison operator and threshold, or remove NaN values.

**Algorithm:**
1. Branch: value is None AND operator is None -> drop NaN values (dropna)
2. Branch: value is None AND operator is not None -> raise MqValueError (no value for operator)
3. Branch: value is provided ->
   a. Branch operator:
      - EQUALS -> mark x == value
      - GREATER -> mark x > value
      - LESS -> mark x < value
      - L_EQUALS -> mark x <= value
      - G_EQUALS -> mark x >= value
      - N_EQUALS -> mark x != value
      - else -> raise MqValueError (unexpected operator)
   b. Drop rows where condition is True
4. Return filtered series

**Raises:**
- `MqValueError` when operator is provided without a value
- `MqValueError` when operator is not a recognized FilterOperator

### filter_dates(x: pd.Series, operator: Optional[FilterOperator] = None, dates: Union[List[dt.date], dt.date] = None) -> pd.Series
Purpose: Remove dates from a series based on a comparison operator and date threshold(s).

**Algorithm:**
1. Branch: dates is None AND operator is None -> drop NaN values (dropna)
2. Branch: dates is None AND operator is not None -> raise MqValueError (no date for operator)
3. Branch: dates is list AND operator not in [EQUALS, N_EQUALS] -> raise MqValueError (operator incompatible with list)
4. Branch operator:
   - EQUALS -> ensure dates is list; remove rows where index is in dates
   - N_EQUALS -> ensure dates is list; keep only rows where index is in dates
   - GREATER -> keep rows where index <= dates
   - LESS -> keep rows where index >= dates
   - L_EQUALS -> keep rows where index > dates
   - G_EQUALS -> keep rows where index < dates
   - else -> raise MqValueError (unexpected operator)
5. Return filtered series

**Raises:**
- `MqValueError` when operator is provided without dates
- `MqValueError` when list of dates used with operators other than EQUALS/N_EQUALS
- `MqValueError` when operator is not a recognized FilterOperator

**Note on semantics:** The filter_dates operator semantics for GREATER/LESS/L_EQUALS/G_EQUALS are inverted from what the names suggest -- GREATER removes dates greater than the threshold (keeps index <= dates), LESS removes dates less than the threshold (keeps index >= dates), etc.

### _sum_boolean_series(*series) -> pd.Series
Purpose: Internal helper that validates and sums 2-100 boolean (0/1) series with fill_value=0.

**Algorithm:**
1. Validate 2 <= len(series) <= 100, else raise MqValueError
2. Validate all arguments are pd.Series, else raise MqTypeError
3. Validate all values in each series are 0 or 1, else raise MqValueError
4. Sum first two series with fill_value=0
5. Iteratively add remaining series with fill_value=0
6. Return summed series

**Raises:**
- `MqValueError` when fewer than 2 or more than 100 arguments
- `MqTypeError` when any argument is not a pd.Series
- `MqValueError` when any series contains values other than 0 and 1

### and_(*series: pd.Series) -> pd.Series
Purpose: Logical AND of two or more boolean (0/1) series.

**Formula:**

R_t = 1 if all series have value 1 at t, else 0

**Algorithm:**
1. Sum all boolean series via _sum_boolean_series
2. Return (sum == len(series)).astype(int)

### or_(*series: pd.Series) -> pd.Series
Purpose: Logical OR of two or more boolean (0/1) series.

**Formula:**

R_t = 1 if any series has value 1 at t, else 0

**Algorithm:**
1. Sum all boolean series via _sum_boolean_series
2. Return (sum > 0).astype(int)

### not_(series: pd.Series) -> pd.Series
Purpose: Logical negation of a single boolean (0/1) series.

**Formula:**

R_t = 1 - X_t

**Algorithm:**
1. Validate all values are 0 or 1, else raise MqValueError
2. Replace 0 with 1 and 1 with 0 via series.replace([0, 1], [1, 0])

**Raises:** `MqValueError` when series contains values other than 0 and 1

### if_(flags: pd.Series, x: Union[pd.Series, float], y: Union[pd.Series, float]) -> pd.Series
Purpose: Conditional selection -- return x values where flags==1, y values where flags==0.

**Formula:**

R_t = X_t if flags_t == 1, else Y_t

**Algorithm:**
1. Validate all flag values are 0 or 1, else raise MqValueError
2. Define ensure_series(s):
   a. Branch: s is float/int -> create constant series with same index as flags; return (flags, constant_series)
   b. Branch: s is pd.Series -> align with flags; return aligned pair
   c. Else -> raise MqTypeError
3. Call ensure_series for x and y
4. Concatenate x[flags==1] and y[flags==0], sort by index
5. Return result

**Raises:**
- `MqValueError` when flags contains values other than 0 and 1
- `MqTypeError` when x or y is not a number or series

### weighted_sum(series: List[pd.Series], weights: list) -> pd.Series
Purpose: Compute a weighted sum (e.g., for a basket) over the intersection of series calendars.

**Formula:**

R_t = sum(S_i_t * w_i) / sum(w_i) for each t in intersection of all calendars

**Algorithm:**
1. Validate all elements of series are pd.Series, else raise MqTypeError
2. Validate all weights are float or int, else raise MqTypeError
3. Validate len(weights) == len(series), else raise MqValueError
4. Compute calendar intersection of all series indices using reduce(np.intersect1d, ...)
5. Reindex all series to the intersection calendar
6. Convert each weight to a constant pd.Series on the intersection calendar
7. Compute sum(series[i] * weights[i]) / sum(weights)

**Raises:**
- `MqTypeError` when series list contains non-Series elements
- `MqTypeError` when weights list contains non-numeric elements
- `MqValueError` when weights and series have different lengths

**Note:** The result is divided by sum(weights), making this a weighted average when weights sum to 1, and a normalized weighted sum otherwise.

### geometrically_aggregate(series: pd.Series) -> pd.Series
Purpose: Geometrically aggregate a returns series (cumulative compounded return).

**Formula:**

R_t = prod_{i=0}^{t}(1 + S_i) - 1

**Algorithm:**
1. Add 1 to each element
2. Compute cumulative product
3. Subtract 1
4. Return result

## State Mutation
- No global state mutations.
- No instance state (module is purely functional).
- Thread safety: All functions are stateless and operate on inputs; safe for concurrent use.

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `MqValueError` | `filter_` | Operator provided without value |
| `MqValueError` | `filter_` | Unrecognized operator |
| `MqValueError` | `filter_dates` | Operator provided without dates |
| `MqValueError` | `filter_dates` | List of dates with non-EQUALS/N_EQUALS operator |
| `MqValueError` | `filter_dates` | Unrecognized operator |
| `MqValueError` | `_sum_boolean_series` | Fewer than 2 or more than 100 arguments |
| `MqValueError` | `_sum_boolean_series` | Series contains values other than 0 and 1 |
| `MqTypeError` | `_sum_boolean_series` | Argument is not a pd.Series |
| `MqValueError` | `not_` | Series contains values other than 0 and 1 |
| `MqValueError` | `if_` | Flags contain values other than 0 and 1 |
| `MqTypeError` | `if_` | x or y is not a number or series |
| `MqTypeError` | `weighted_sum` | Series list contains non-Series elements |
| `MqTypeError` | `weighted_sum` | Weights list contains non-numeric elements |
| `MqValueError` | `weighted_sum` | Weights and series have different lengths |
| `AssertionError` | `floor` | Index is not monotonic increasing |
| `AssertionError` | `ceil` | Index is not monotonic increasing |

## Edge Cases
- Arithmetic functions (add, subtract, multiply, divide, floordiv) with two Real inputs bypass alignment entirely and return a scalar Real
- Arithmetic functions with one Real and one Series: alignment handles broadcasting via the `align` helper
- `sqrt` of a perfect square integer returns an int (e.g., sqrt(4) -> 2, not 2.0)
- `sqrt` of a negative number will raise ValueError from math.sqrt (not explicitly caught)
- `divide` with zero values in y will produce Inf/NaN in the series (pandas behavior), not an exception
- `floordiv` with zero values in y will produce Inf/NaN in the series (pandas behavior)
- `floor` and `ceil` assert monotonic index -- will raise AssertionError on unsorted series
- `filter_` with both operator and value as None: drops NaN values
- `filter_dates` with EQUALS/N_EQUALS accepts both single date and list of dates
- `filter_dates` with GREATER/LESS/L_EQUALS/G_EQUALS: semantics are inverted from operator names (GREATER keeps <=, LESS keeps >=, etc.)
- `_sum_boolean_series` uses fill_value=0 when adding, so series with different indices will treat missing values as 0
- `if_` concatenates filtered slices and sorts by index; if flags has overlapping aligned indices this could produce duplicates
- `weighted_sum` divides by sum(weights), so zero total weight would cause division by zero
- `geometrically_aggregate` with a value of -1 in the series will produce 0 in the cumulative product, making all subsequent values 0

## Bugs Found
None.

## Coverage Notes
- Branch count: ~45
- Key branch points: arithmetic scalar-vs-series dispatch (5 functions x 2 branches), filter_ operator dispatch (7 branches), filter_dates operator dispatch (7 branches), sqrt Series-vs-Real and integral check, _sum_boolean_series validation (3 checks), if_ ensure_series type dispatch (3 branches), weighted_sum validation (3 checks)
