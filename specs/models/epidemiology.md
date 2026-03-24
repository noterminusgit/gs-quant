# epidemiology.py

## Summary
Statistical models for the transmission of infectious diseases, implementing compartmental epidemiological models (SIR, SEIR, SEIRCM, and age-stratified SEIRCM). Provides ODE-based calibration, parameter fitting via `lmfit`, and numerical integration via `scipy.integrate.odeint`. The module includes a `switch` function for modeling time-dependent policy interventions (e.g., quarantine) and an `EpidemicModel` class for standardized fitting and solving workflows.

## Dependencies
- Internal: None
- External: `abc` (ABC, abstractmethod), `typing` (Type, Union), `numpy` (np), `lmfit` (minimize, Parameters, report_fit), `scipy.integrate` (odeint)

## Type Definitions

### CompartmentalModel (ABC)
Inherits: ABC

Abstract base class for all compartmental epidemiological models. No instance fields; all methods are classmethods.

### SIR (class)
Inherits: CompartmentalModel

No instance fields. All methods are classmethods.

### SEIR (class)
Inherits: CompartmentalModel

No instance fields. All methods are classmethods.

### SEIRCM (class)
Inherits: CompartmentalModel

No instance fields. All methods are classmethods.

### SEIRCMAgeStratified (class)
Inherits: CompartmentalModel

No instance fields. All methods are classmethods.

### EpidemicModel (class)
Inherits: object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| model | `Type[CompartmentalModel]` | required | The compartmental model class to use |
| parameters | `tuple` | `None` | Parameters for the model (lmfit Parameters + initial conditions) |
| data | `np.array` | `None` | Observed data for calibration |
| initial_conditions | `list` | `None` | Names of initial condition parameter keys |
| fit_method | `str` | `'leastsq'` | Minimization method (Levenberg-Marquardt by default) |
| error | `callable` | `None` | Custom error function for residuals |
| fit_period | `float` | `None` | How far back to fit data; None = fit all |
| result | `object` | `None` | Result from lmfit minimize (set after fit) |
| fitted_parameters | `dict` | `None` | Dict of fitted parameter values (set after fit) |

## Enums and Constants

None.

## Functions/Methods

### CompartmentalModel.calibrate(cls, xs, t, parameters) -> tuple
Purpose: Abstract classmethod defining the ODE system derivatives.

**Algorithm:**
1. (Abstract) Must be implemented by subclasses
2. Should return derivatives in the form callable(y, t, ...) for scipy odeint

**Raises:** `NotImplementedError` always (abstract)

### CompartmentalModel.get_parameters(cls, *args, **kwargs) -> tuple
Purpose: Abstract classmethod to produce model parameters.

**Algorithm:**
1. (Abstract) Must be implemented by subclasses

**Raises:** `NotImplementedError` always (abstract)

### SIR.calibrate(cls, xs: tuple, t: float, parameters: Union[Parameters, tuple]) -> tuple
Purpose: Compute SIR model derivatives at time t.

**Mathematical Formulas:**
```
dS/dt = -beta * S * I / N
dI/dt = beta * S * I / N - gamma * I
dR/dt = gamma * I
```

Where:
- S = Susceptible population
- I = Infected population
- R = Removed (recovered + dead) population
- N = Total population
- beta = Transmission rate
- gamma = Recovery rate

**Algorithm:**
1. Unpack xs into s, i, r
2. Branch: if parameters is `Parameters` -> extract beta, gamma, N from `.value` attributes
3. Branch: if parameters is `tuple` -> unpack as (beta, gamma, N)
4. Branch: else -> raise `ValueError("Cannot recognize parameter input")`
5. Compute dSdt = -beta * s * i / N
6. Compute dIdt = beta * s * i / N - gamma * i
7. Compute dRdt = gamma * i
8. Return (dSdt, dIdt, dRdt)

**Raises:** `ValueError` when parameters is neither Parameters nor tuple

### SIR.get_parameters(cls, S0: float, I0: float, R0: float, N: float, beta: float = 0.2, gamma: float = 0.1, beta_max: float = 10, gamma_max: float = 1, S0_fixed: bool = True, S0_max: float = 1e6, beta_fixed: bool = False, gamma_fixed: bool = False, R0_fixed: bool = True, R0_max: float = 1e6, I0_fixed: bool = True, I0_max: float = 1e6) -> tuple
Purpose: Produce lmfit Parameters for the SIR model.

**Algorithm:**
1. Create `Parameters()` object
2. Add 'N' (fixed, min=0, max=N)
3. Add 'S0' (vary=not S0_fixed, min=0, max=S0_max)
4. Add 'I0' (vary=not I0_fixed, min=0, max=I0_max)
5. Add 'R0' (vary=not R0_fixed, min=0, max=R0_max)
6. Add 'beta' (vary=not beta_fixed, min=0, max=beta_max)
7. Add 'gamma' (vary=not gamma_fixed, min=0, max=gamma_max)
8. Set initial_conditions = ['S0', 'I0', 'R0']
9. Return (parameters, initial_conditions)

### SEIR.calibrate(cls, xs: tuple, t: float, parameters: Union[Parameters, tuple]) -> tuple
Purpose: Compute SEIR model derivatives at time t.

**Mathematical Formulas:**
```
dS/dt = -beta * S * I / N
dE/dt = beta * S * I / N - sigma * E
dI/dt = sigma * E - gamma * I
dR/dt = gamma * I
```

Where:
- E = Exposed (infected but not yet infectious) population
- sigma = Rate of progression from exposed to infectious (1/sigma = incubation period)
- All other variables same as SIR

**Algorithm:**
1. Unpack xs into s, e, i, r
2. Branch: if parameters is `Parameters` -> extract beta, gamma, sigma, N
3. Branch: if parameters is `tuple` -> unpack as (beta, gamma, sigma, N)
4. Branch: else -> raise `ValueError`
5. Compute dSdt = -beta * s * i / N
6. Compute dEdt = beta * s * i / N - sigma * e
7. Compute dIdt = sigma * e - gamma * i
8. Compute dRdt = gamma * i
9. Return (dSdt, dEdt, dIdt, dRdt)

**Raises:** `ValueError` when parameters is neither Parameters nor tuple

### SEIR.get_parameters(cls, S0: float, E0: float, I0: float, R0: float, N: float, beta: float = 0.2, gamma: float = 0.1, sigma: float = 0.2, beta_max: float = 10, gamma_max: float = 1, sigma_max: float = 1, beta_fixed: bool = False, gamma_fixed: bool = False, sigma_fixed: bool = False, S0_fixed: bool = True, S0_max: float = 1e6, R0_fixed: bool = True, R0_max: float = 1e6, I0_fixed: bool = True, I0_max: float = 1e6, E0_fixed: bool = True, E0_max: float = 1e6) -> tuple
Purpose: Produce lmfit Parameters for the SEIR model.

**Algorithm:**
1. Create `Parameters()` object
2. Add 'N' (fixed), 'S0', 'E0', 'I0', 'R0' with respective bounds and vary flags
3. Add 'beta', 'gamma', 'sigma' with respective bounds and vary flags
4. Set initial_conditions = ['S0', 'E0', 'I0', 'R0']
5. Return (parameters, initial_conditions)

### switch(t: float, T: float, eta: float = 0, xi: float = 0.1, nu: float = 0) -> float
Purpose: Time-dependent sigmoid switch function to model the effect of policy interventions (e.g., quarantine).

**Mathematical Formula:**
```
switch(t, T, eta, xi, nu) = eta + (1 - eta) / (1 + exp(xi * (t - T - nu)))
```

Where:
- t = Current time step
- T = Time at which the regime switches (e.g., quarantine enacted), relative to t0=0
- eta = Proportional reduction factor after full intervention (0 = complete suppression, 1 = no effect)
- xi = Steepness of the sigmoid transition (controls how quickly the switch happens)
- nu = Shift of the sigmoid center relative to T (e.g., delay before effects are visible)

The function transitions from ~1.0 (no effect) for t << T to ~eta (full effect) for t >> T+nu.

**Algorithm:**
1. Compute and return `eta + (1 - eta) / (1 + np.exp(xi * (t - T - nu)))`

### SEIRCM.calibrate(cls, xs: tuple, t: float, parameters: Union[Parameters, tuple]) -> tuple
Purpose: Compute SEIRCM model derivatives at time t, incorporating cumulative cases (C) and mortality (M).

**Mathematical Formulas:**
```
N = S + E + I + R  (dynamic total, not a fixed parameter)

quarantine_factor = switch(t, T, eta)  if T != 0, else 1

dS/dt = -(quarantine_factor * beta) * S * I / N
dE/dt = (quarantine_factor * beta) * S * I / N - sigma * E
dI/dt = sigma * E - gamma * I
dR/dt = (1 - epsilon) * gamma * I
dC/dt = sigma * E
dM/dt = epsilon * gamma * I
```

Where:
- C = Cumulative confirmed cases
- M = Cumulative fatalities
- epsilon = Case fatality rate
- quarantine_factor = Time-dependent intervention scaling via `switch()` function
- N is computed dynamically as S + E + I + R (not a parameter)

**Algorithm:**
1. Unpack xs into s, e, i, r, c, m
2. Branch: if parameters is `Parameters` -> extract beta, gamma, sigma, eta, epsilon, T_quarantine
   - NOTE: Line 269 has a bug: epsilon is assigned `parameters['sigma'].value` instead of `parameters['epsilon'].value`
3. Branch: if parameters is `tuple` -> unpack as (beta, gamma, sigma, eta, epsilon, T_quarantine)
4. Branch: else -> raise `ValueError`
5. Compute N = s + e + i + r
6. Branch: if T_quarantine != 0 -> compute quarantine_factor via `switch(t, T_quarantine, eta=eta)`; else -> 1
7. Compute all six derivatives
8. Return (dSdt, dEdt, dIdt, dRdt, dCdt, dMdt)

**Raises:** `ValueError` when parameters is neither Parameters nor tuple

### SEIRCM.get_parameters(cls, S0: float, E0: float, I0: float, R0: float, C0: float, M0: float, T_quarantine: float = 0, beta: float = 0.2, gamma: float = 0.1, sigma: float = 0.2, eta: float = 0.6, epsilon: float = 0.02, ...) -> tuple
Purpose: Produce lmfit Parameters for the SEIRCM model.

**Algorithm:**
1. Create `Parameters()` object
2. Add 'T' (fixed, quarantine time)
3. Add initial conditions: S0, E0, I0, R0, C0, M0 with respective bounds and vary flags
4. Add model parameters: beta, gamma, sigma, eta, epsilon with respective bounds and vary flags
5. Set initial_conditions = ['S0', 'E0', 'I0', 'R0', 'C0', 'M0']
6. Return (parameters, initial_conditions)

### SEIRCMAgeStratified.calibrate(cls, y: list, t: float, parameters: Parameters) -> list
Purpose: Compute age-stratified SEIRCM model derivatives at time t.

**Mathematical Formulas (per age group k, for K age groups):**
```
N = sum(S_k) + sum(E_k) + sum(I_k) + sum(R_k)  for k=0..K-1
I_total = sum(I_k)  for k=0..K-1

quarantine_factor = switch(t, T, eta)  if T != 0, else 1

For each age group k:
  dS_k/dt = -(quarantine_factor * beta) * S_k * I_total / N
  dE_k/dt = (quarantine_factor * beta) * S_k * I_total / N - sigma * E_k
  dI_k/dt = sigma * E_k - gamma * I_k
  dR_k/dt = (1 - epsilon_k) * gamma * I_k
  dC_k/dt = sigma * E_k
  dM_k/dt = epsilon_k * gamma * I_k
```

Where:
- epsilon_k = Age-group-specific case fatality rate
- y is a flat vector of dimension 6*K: [S_0..S_{K-1}, E_0..E_{K-1}, I_0..I_{K-1}, R_0..R_{K-1}, C_0..C_{K-1}, M_0..M_{K-1}]

**Algorithm:**
1. Extract beta, gamma, sigma, eta, T_quarantine, K from parameters
2. Assert len(y) == 6 * K
3. Initialize dydt = [0] * 6*K
4. Define epsilon(k) helper to read per-age-group fatality rate
5. Compute N = sum of first 4*K elements (all S, E, I, R across groups)
6. Compute I_total = sum of elements from index 2*K to 3*K
7. Define lambda accessors: s(k), e(k), i(k) for indexing into y
8. Branch: if T_quarantine != 0 -> compute quarantine_factor via `switch()`; else -> 1
9. Loop for k in range(K): compute all 6 derivatives per age group at offsets k, K+k, 2K+k, 3K+k, 4K+k, 5K+k
10. Return dydt list

### SEIRCMAgeStratified.get_parameters(cls, S0: np.ndarray, E0: np.ndarray, I0: np.ndarray, R0: np.ndarray, C0: np.ndarray, M0: np.ndarray, K: int, T_quarantine: float = 0, ...) -> tuple
Purpose: Produce lmfit Parameters for the age-stratified SEIRCM model.

**Algorithm:**
1. Create `Parameters()` object
2. Add 'K' (fixed), 'T' (fixed)
3. Add model parameters: beta, gamma, sigma, eta (shared across age groups)
4. Loop for k in range(K): add `epsilon_k` (per-age-group fatality rate)
5. Loop over initial conditions (S0, E0, I0, R0, C0, M0):
   - For each, loop for k in range(K): add `{param}_{k}` using `param_value[k]`
   - Append `{param}_{k}` to initial_conditions list
6. Return (parameters, initial_conditions)

### EpidemicModel.__init__(self, model: Type[CompartmentalModel], parameters: tuple = None, data: np.array = None, initial_conditions: list = None, fit_method: str = 'leastsq', error: callable = None, fit_period: float = None)
Purpose: Initialize an epidemic model fitting/solving wrapper.

**Algorithm:**
1. Store all parameters as instance attributes
2. Initialize self.result = None
3. Initialize self.fitted_parameters = None

### EpidemicModel.solve(self, time_range: np.ndarray, initial_conditions: Union[list, tuple], parameters) -> np.ndarray
Purpose: Integrate the model ODEs over a time range.

**Algorithm:**
1. Call `odeint(self.model.calibrate, initial_conditions, time_range, args=(parameters,))`
2. Convert to numpy array
3. Return solution matrix

### EpidemicModel.residual(self, parameters: Parameters, time_range: np.arange, data: np.ndarray) -> np.ndarray
Purpose: Compute fit residuals between model solution and observed data.

**Algorithm:**
1. Build initial_conditions list by extracting values from parameters using self.initial_conditions keys
2. Call `self.solve(time_range, initial_conditions, parameters)`
3. Branch: if self.error is None -> compute `solution - data`; else -> call `self.error(solution, data, parameters)`
4. Branch: if self.fit_period is not None -> slice residual to last `fit_period` entries
5. Return residual.ravel() (flattened)

### EpidemicModel.fit(self, time_range: np.arange = None, parameters: Union[Parameters, tuple] = None, initial_conditions: list = None, residual=None, verbose: bool = False, data: np.array = None, fit_period: float = None)
Purpose: Fit the model to observed data by minimizing residuals.

**Algorithm:**
1. Branch: if data is None:
   - Branch: if self.data is None -> raise `ValueError("No data to fit the model on!")`
   - Else -> use self.data
2. Branch: if initial_conditions provided -> set self.initial_conditions
3. Branch: if self.initial_conditions is None -> raise `ValueError("No initial conditions...")`
4. Branch: if parameters is None:
   - Branch: if self.parameters is None -> raise `ValueError("No parameters...")`
   - Else -> use self.parameters
5. Branch: if time_range is None -> set to `np.arange(data.shape[0])`
6. Branch: if fit_period is not None -> set self.fit_period
7. Branch: if residual is None -> set to self.residual
8. Call `minimize(residual, parameters, args=(time_range, data), method=self.fit_method)`
9. Store result and fitted_parameters
10. Branch: if verbose -> call `report_fit(result)`
11. Return result

**Raises:** `ValueError` when data, initial_conditions, or parameters are missing

## State Mutation
- `self.result`: Set to `None` in `__init__`, updated by `fit()`
- `self.fitted_parameters`: Set to `None` in `__init__`, updated by `fit()`
- `self.initial_conditions`: Set in `__init__`, can be overwritten in `fit()` if `initial_conditions` arg provided
- `self.fit_period`: Set in `__init__`, can be overwritten in `fit()` if `fit_period` arg provided
- `self.data`: Set in `__init__`, used in `fit()` as fallback
- Thread safety: Not thread-safe; mutable instance state modified during fit

## Error Handling
| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `ValueError` | `SIR.calibrate` | Parameters is neither `Parameters` nor `tuple` |
| `ValueError` | `SEIR.calibrate` | Parameters is neither `Parameters` nor `tuple` |
| `ValueError` | `SEIRCM.calibrate` | Parameters is neither `Parameters` nor `tuple` |
| `ValueError` | `EpidemicModel.fit` | No data, no initial conditions, or no parameters available |
| `AssertionError` | `SEIRCMAgeStratified.calibrate` | len(y) != 6 * K |
| `NotImplementedError` | `CompartmentalModel.calibrate` | Abstract method called directly |
| `NotImplementedError` | `CompartmentalModel.get_parameters` | Abstract method called directly |

## Edge Cases
- `SIR.calibrate` / `SEIR.calibrate` / `SEIRCM.calibrate`: Accept both `Parameters` (lmfit) and plain `tuple` for parameters -- useful for direct evaluation vs. fitting
- `SEIRCM.calibrate`: When `T_quarantine == 0`, quarantine_factor is fixed at 1 (no intervention)
- `SEIRCMAgeStratified.calibrate`: y vector must be exactly 6*K in length; assertion enforces this
- `SEIRCMAgeStratified.calibrate`: Only accepts `Parameters` (not tuple) unlike other calibrate methods
- `switch` function: When eta=1.0, returns 1.0 for all t (disabled switch); when eta=0, full suppression
- `switch` function: When xi=0, division behavior -- the exponential term becomes `exp(0)=1`, so result is `eta + (1-eta)/2`
- `EpidemicModel.residual`: Custom error function receives (solution, data, parameters) -- allows weighting
- `EpidemicModel.fit`: All parameters can be overridden at call time; instance state is updated as side effect
- `EpidemicModel.solve`: Returns full solution matrix (time_steps x compartments), not individual compartment arrays

## Bugs Found
- Line 269: `epsilon = parameters['sigma'].value` should be `epsilon = parameters['epsilon'].value` -- the case fatality rate incorrectly reads the sigma (infection rate) parameter instead of epsilon. This means SEIRCM mortality calculations are wrong when using lmfit Parameters. (OPEN)
- Line 182 (SEIR.get_parameters docstring): Says "Produce a set of parameters for the SIR model" but this is actually the SEIR model. (OPEN, documentation only)

## Coverage Notes
- Branch count: ~30
- Key branches: Parameter type checking (Parameters vs tuple vs else) in each calibrate method (3 branches x 4 models = 12); T_quarantine zero check in SEIRCM and SEIRCMAgeStratified (2); fit() null checks (6+ branches); residual custom error check; fit_period check; verbose check
- Missing branches: The `else` branch in calibrate methods (ValueError) may be hard to trigger naturally since odeint always passes the parameters unchanged
- Pragmas: None
