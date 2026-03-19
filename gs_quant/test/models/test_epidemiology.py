"""
Copyright 2024 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from lmfit import Parameters

from gs_quant.models.epidemiology import (
    SIR,
    SEIR,
    SEIRCM,
    SEIRCMAgeStratified,
    EpidemicModel,
    switch,
)


# ===========================================================================
# Tests for the switch function
# ===========================================================================

class TestSwitch:
    def test_switch_at_time_T(self):
        """At t = T + nu, the switch value should be approximately (1 + eta) / 2."""
        result = switch(t=10, T=10, eta=0.4, xi=0.1, nu=0)
        # At t=T+nu=10, the exponent is 0, so logistic is 0.5
        expected = 0.4 + (1 - 0.4) * 0.5
        assert np.isclose(result, expected, atol=1e-6)

    def test_switch_far_before_T(self):
        """Far before T, the switch should be close to 1."""
        result = switch(t=0, T=100, eta=0.4, xi=1.0, nu=0)
        assert np.isclose(result, 1.0, atol=1e-3)

    def test_switch_far_after_T(self):
        """Far after T, the switch should be close to eta."""
        result = switch(t=200, T=10, eta=0.4, xi=1.0, nu=0)
        assert np.isclose(result, 0.4, atol=1e-3)

    def test_switch_eta_1_disables(self):
        """When eta=1, the switch should always be 1 regardless of time."""
        result = switch(t=50, T=10, eta=1.0, xi=0.1, nu=0)
        assert np.isclose(result, 1.0, atol=1e-6)

    def test_switch_with_nu_shift(self):
        """nu shifts the steepest point."""
        result = switch(t=15, T=10, eta=0.4, xi=0.1, nu=5)
        # At t=T+nu=15, the exponent is 0, so logistic is 0.5
        expected = 0.4 + (1 - 0.4) * 0.5
        assert np.isclose(result, expected, atol=1e-6)

    def test_switch_defaults(self):
        """Test default parameter values."""
        result = switch(t=10, T=10)
        # eta=0, xi=0.1, nu=0 -> 0 + (1-0)/(1 + exp(0)) = 0.5
        assert np.isclose(result, 0.5, atol=1e-6)


# ===========================================================================
# Tests for SIR model
# ===========================================================================

class TestSIR:
    def test_calibrate_with_parameters_object(self):
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('N', value=100)

        result = SIR.calibrate((99, 1, 0), 0, params)
        dSdt, dIdt, dRdt = result

        assert np.isclose(dSdt, -0.5 * 99 * 1 / 100)
        assert np.isclose(dIdt, 0.5 * 99 * 1 / 100 - 0.25 * 1)
        assert np.isclose(dRdt, 0.25 * 1)

    def test_calibrate_with_tuple(self):
        result = SIR.calibrate((99, 1, 0), 0, (0.5, 0.25, 100))
        dSdt, dIdt, dRdt = result

        assert np.isclose(dSdt, -0.5 * 99 * 1 / 100)
        assert np.isclose(dIdt, 0.5 * 99 * 1 / 100 - 0.25 * 1)
        assert np.isclose(dRdt, 0.25 * 1)

    def test_calibrate_invalid_parameters(self):
        with pytest.raises(ValueError, match="Cannot recognize parameter input"):
            SIR.calibrate((99, 1, 0), 0, "invalid")

    def test_calibrate_invalid_parameters_dict(self):
        with pytest.raises(ValueError, match="Cannot recognize parameter input"):
            SIR.calibrate((99, 1, 0), 0, {'beta': 0.5})

    def test_get_parameters_defaults(self):
        params, ic = SIR.get_parameters(99, 1, 0, 100)
        assert ic == ['S0', 'I0', 'R0']
        assert params['N'].value == 100
        assert params['S0'].value == 99
        assert params['I0'].value == 1
        assert params['R0'].value == 0
        assert params['beta'].value == 0.2
        assert params['gamma'].value == 0.1

    def test_get_parameters_vary_flags(self):
        params, _ = SIR.get_parameters(
            99, 1, 0, 100,
            S0_fixed=False, I0_fixed=False, R0_fixed=False,
            beta_fixed=True, gamma_fixed=True
        )
        assert params['S0'].vary is True
        assert params['I0'].vary is True
        assert params['R0'].vary is True
        assert params['beta'].vary is False
        assert params['gamma'].vary is False

    def test_get_parameters_custom_max(self):
        params, _ = SIR.get_parameters(
            99, 1, 0, 100,
            beta_max=20, gamma_max=5,
            S0_max=2e6, I0_max=3e6, R0_max=4e6
        )
        assert params['beta'].max == 20
        assert params['gamma'].max == 5
        assert params['S0'].max == 2e6
        assert params['I0'].max == 3e6
        assert params['R0'].max == 4e6


# ===========================================================================
# Tests for SEIR model
# ===========================================================================

class TestSEIR:
    def test_calibrate_with_parameters_object(self):
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('N', value=100)

        result = SEIR.calibrate((95, 3, 1, 1), 0, params)
        dSdt, dEdt, dIdt, dRdt = result

        assert np.isclose(dSdt, -0.5 * 95 * 1 / 100)
        assert np.isclose(dEdt, 0.5 * 95 * 1 / 100 - 0.2 * 3)
        assert np.isclose(dIdt, 0.2 * 3 - 0.25 * 1)
        assert np.isclose(dRdt, 0.25 * 1)

    def test_calibrate_with_tuple(self):
        result = SEIR.calibrate((95, 3, 1, 1), 0, (0.5, 0.25, 0.2, 100))
        dSdt, dEdt, dIdt, dRdt = result

        assert np.isclose(dSdt, -0.5 * 95 * 1 / 100)
        assert np.isclose(dEdt, 0.5 * 95 * 1 / 100 - 0.2 * 3)
        assert np.isclose(dIdt, 0.2 * 3 - 0.25 * 1)
        assert np.isclose(dRdt, 0.25 * 1)

    def test_calibrate_invalid_parameters(self):
        with pytest.raises(ValueError, match="Cannot recognize parameter input"):
            SEIR.calibrate((95, 3, 1, 1), 0, [0.5, 0.25, 0.2, 100])

    def test_get_parameters_defaults(self):
        params, ic = SEIR.get_parameters(95, 3, 1, 0, 100)
        assert ic == ['S0', 'E0', 'I0', 'R0']
        assert params['N'].value == 100
        assert params['E0'].value == 3
        assert params['sigma'].value == 0.2

    def test_get_parameters_vary_flags(self):
        params, _ = SEIR.get_parameters(
            95, 3, 1, 0, 100,
            E0_fixed=False,
            sigma_fixed=True
        )
        assert params['E0'].vary is True
        assert params['sigma'].vary is False


# ===========================================================================
# Tests for SEIRCM model
# ===========================================================================

class TestSEIRCM:
    def test_calibrate_with_parameters_object(self):
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('eta', value=0.4)
        params.add('T', value=0)  # T_quarantine = 0 means no quarantine

        result = SEIRCM.calibrate((90, 5, 3, 2, 3, 0), 10, params)
        dSdt, dEdt, dIdt, dRdt, dCdt, dMdt = result

        N = 90 + 5 + 3 + 2
        # quarantine_factor = 1 since T=0
        assert np.isclose(dSdt, -0.5 * 90 * 3 / N)
        assert np.isclose(dEdt, 0.5 * 90 * 3 / N - 0.2 * 5)
        assert np.isclose(dIdt, 0.2 * 5 - 0.25 * 3)
        # epsilon = sigma (line 269 in source - bug but we test actual behavior)
        epsilon = 0.2
        assert np.isclose(dRdt, (1 - epsilon) * 0.25 * 3)
        assert np.isclose(dCdt, 0.2 * 5)
        assert np.isclose(dMdt, epsilon * 0.25 * 3)

    def test_calibrate_with_quarantine(self):
        """When T_quarantine > 0, quarantine_factor uses switch function."""
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('eta', value=0.4)
        params.add('T', value=5)  # quarantine at day 5

        result = SEIRCM.calibrate((90, 5, 3, 2, 3, 0), 10, params)
        dSdt, dEdt, dIdt, dRdt, dCdt, dMdt = result

        N = 90 + 5 + 3 + 2
        qf = switch(10, 5, eta=0.4)
        assert np.isclose(dSdt, -(qf * 0.5) * 90 * 3 / N)

    def test_calibrate_with_tuple(self):
        result = SEIRCM.calibrate(
            (90, 5, 3, 2, 3, 0), 10,
            (0.5, 0.25, 0.2, 0.4, 0.02, 0)  # T_quarantine=0
        )
        dSdt, dEdt, dIdt, dRdt, dCdt, dMdt = result

        N = 90 + 5 + 3 + 2
        assert np.isclose(dSdt, -0.5 * 90 * 3 / N)

    def test_calibrate_with_tuple_quarantine(self):
        result = SEIRCM.calibrate(
            (90, 5, 3, 2, 3, 0), 10,
            (0.5, 0.25, 0.2, 0.4, 0.02, 5)  # T_quarantine=5
        )
        N = 90 + 5 + 3 + 2
        qf = switch(10, 5, eta=0.4)
        assert np.isclose(result[0], -(qf * 0.5) * 90 * 3 / N)

    def test_calibrate_invalid_parameters(self):
        with pytest.raises(ValueError, match="Cannot recognize parameter input"):
            SEIRCM.calibrate((90, 5, 3, 2, 3, 0), 10, {'beta': 0.5})

    def test_get_parameters_defaults(self):
        # T_quarantine must be > 0 to avoid lmfit min==max error (min=0, max=T_quarantine)
        params, ic = SEIRCM.get_parameters(
            S0=90, E0=5, I0=3, R0=2, C0=3, M0=0,
            T_quarantine=10
        )
        assert ic == ['S0', 'E0', 'I0', 'R0', 'C0', 'M0']
        assert params['T'].value == 10
        assert params['eta'].value == 0.6
        assert params['epsilon'].value == 0.02

    def test_get_parameters_with_quarantine(self):
        params, ic = SEIRCM.get_parameters(
            S0=90, E0=5, I0=3, R0=2, C0=3, M0=0,
            T_quarantine=10
        )
        assert params['T'].value == 10

    def test_get_parameters_vary_flags(self):
        params, _ = SEIRCM.get_parameters(
            S0=90, E0=5, I0=3, R0=2, C0=3, M0=0,
            T_quarantine=10,
            eta_fixed=True, epsilon_fixed=True,
            S0_fixed=False, E0_fixed=False, I0_fixed=False,
            R0_fixed=False, C0_fixed=False, M0_fixed=False
        )
        assert params['eta'].vary is False
        assert params['epsilon'].vary is False
        assert params['S0'].vary is True
        assert params['E0'].vary is True
        assert params['C0'].vary is True
        assert params['M0'].vary is True


# ===========================================================================
# Tests for SEIRCMAgeStratified model
# ===========================================================================

class TestSEIRCMAgeStratified:
    def test_calibrate_no_quarantine(self):
        """Test calibrate with T_quarantine=0 (no quarantine effect)."""
        K = 2
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('eta', value=0.4)
        params.add('T', value=0)
        params.add('K', value=K)
        params.add('epsilon_0', value=0.02)
        params.add('epsilon_1', value=0.05)

        # y has 6*K = 12 elements: S(2), E(2), I(2), R(2), C(2), M(2)
        y = [40, 50,  # S
             2, 3,    # E
             1, 2,    # I
             0, 0,    # R
             1, 2,    # C
             0, 0]    # M

        dydt = SEIRCMAgeStratified.calibrate(y, 10, params)
        assert len(dydt) == 12

        N = sum(y[:4 * K])  # S + E + I + R
        I_total = sum(y[2 * K: 3 * K])
        # quarantine_factor = 1 because T=0
        # dS[0] = -beta * S[0] * I_total / N
        assert np.isclose(dydt[0], -0.5 * 40 * I_total / N)

    def test_calibrate_with_quarantine(self):
        """Test calibrate with T_quarantine > 0."""
        K = 2
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('eta', value=0.4)
        params.add('T', value=5)
        params.add('K', value=K)
        params.add('epsilon_0', value=0.02)
        params.add('epsilon_1', value=0.05)

        y = [40, 50, 2, 3, 1, 2, 0, 0, 1, 2, 0, 0]

        dydt = SEIRCMAgeStratified.calibrate(y, 10, params)
        assert len(dydt) == 12

        N = sum(y[:4 * K])
        I_total = sum(y[2 * K: 3 * K])
        qf = switch(10, 5, eta=0.4)
        assert np.isclose(dydt[0], -(qf * 0.5) * 40 * I_total / N)

    def test_calibrate_assertion_error(self):
        """Test that assertion fails when y length != 6*K."""
        K = 2
        params = Parameters()
        params.add('beta', value=0.5)
        params.add('gamma', value=0.25)
        params.add('sigma', value=0.2)
        params.add('eta', value=0.4)
        params.add('T', value=0)
        params.add('K', value=K)
        params.add('epsilon_0', value=0.02)
        params.add('epsilon_1', value=0.05)

        y = [40, 50, 2, 3, 1]  # wrong length
        with pytest.raises(AssertionError):
            SEIRCMAgeStratified.calibrate(y, 10, params)

    def test_get_parameters(self):
        K = 2
        S0 = np.array([40, 50])
        E0 = np.array([2, 3])
        I0 = np.array([1, 2])
        R0 = np.array([0, 0])
        C0 = np.array([1, 2])
        M0 = np.array([0, 0])
        epsilon = np.array([0.02, 0.05])

        params, ic = SEIRCMAgeStratified.get_parameters(
            S0=S0, E0=E0, I0=I0, R0=R0, C0=C0, M0=M0,
            K=K, epsilon=epsilon
        )

        assert params['K'].value == K
        assert params['T'].value == 0
        assert params['epsilon_0'].value == 0.02
        assert params['epsilon_1'].value == 0.05
        assert params['S0_0'].value == 40
        assert params['S0_1'].value == 50
        assert len(ic) == 6 * K  # 6 compartments * 2 age groups

    def test_get_parameters_vary_flags(self):
        K = 1
        params, ic = SEIRCMAgeStratified.get_parameters(
            S0=np.array([90]), E0=np.array([5]), I0=np.array([3]),
            R0=np.array([2]), C0=np.array([1]), M0=np.array([0]),
            K=K, epsilon=np.array([0.02]),
            beta_fixed=True, gamma_fixed=True, sigma_fixed=True,
            eta_fixed=True, epsilon_fixed=True,
            S0_fixed=False, E0_fixed=False, I0_fixed=False,
            R0_fixed=False, C0_fixed=False, M0_fixed=False
        )
        assert params['beta'].vary is False
        assert params['S0_0'].vary is True
        assert params['E0_0'].vary is True
        assert params['epsilon_0'].vary is False

    def test_get_parameters_with_quarantine(self):
        K = 1
        params, ic = SEIRCMAgeStratified.get_parameters(
            S0=np.array([90]), E0=np.array([5]), I0=np.array([3]),
            R0=np.array([2]), C0=np.array([1]), M0=np.array([0]),
            K=K, epsilon=np.array([0.02]),
            T_quarantine=15
        )
        assert params['T'].value == 15


# ===========================================================================
# Tests for EpidemicModel class
# ===========================================================================

class TestEpidemicModel:
    def test_init_defaults(self):
        model = EpidemicModel(SIR)
        assert model.model is SIR
        assert model.parameters is None
        assert model.data is None
        assert model.initial_conditions is None
        assert model.fit_method == 'leastsq'
        assert model.error is None
        assert model.fit_period is None
        assert model.result is None
        assert model.fitted_parameters is None

    def test_solve(self):
        model = EpidemicModel(SIR)
        t = np.arange(10)
        result = model.solve(t, (99, 1, 0), (0.5, 0.25, 100))
        assert result.shape == (10, 3)

    def test_residual_default_error(self):
        params, ic = SIR.get_parameters(99, 1, 0, 100)
        model = EpidemicModel(SIR, initial_conditions=ic)
        t = np.arange(10)
        data = np.ones((10, 3))
        residual = model.residual(params, t, data)
        assert residual.ndim == 1

    def test_residual_custom_error(self):
        params, ic = SIR.get_parameters(99, 1, 0, 100)
        custom_error = MagicMock(return_value=np.ones((10, 3)))
        model = EpidemicModel(SIR, initial_conditions=ic, error=custom_error)
        t = np.arange(10)
        data = np.ones((10, 3))
        residual = model.residual(params, t, data)
        custom_error.assert_called_once()
        assert residual.ndim == 1

    def test_residual_with_fit_period(self):
        params, ic = SIR.get_parameters(99, 1, 0, 100)
        model = EpidemicModel(SIR, initial_conditions=ic, fit_period=5)
        t = np.arange(10)
        data = np.ones((10, 3))
        residual = model.residual(params, t, data)
        # fit_period=5 means only last 5 time steps used
        assert residual.ndim == 1

    def test_fit_no_data_raises(self):
        model = EpidemicModel(SIR, initial_conditions=['S0', 'I0', 'R0'])
        with pytest.raises(ValueError, match="No data to fit"):
            model.fit()

    def test_fit_no_initial_conditions_raises(self):
        data = np.ones((10, 3))
        params, _ = SIR.get_parameters(99, 1, 0, 100)
        model = EpidemicModel(SIR, parameters=params, data=data)
        with pytest.raises(ValueError, match="No initial conditions"):
            model.fit()

    def test_fit_no_parameters_raises(self):
        data = np.ones((10, 3))
        model = EpidemicModel(SIR, data=data, initial_conditions=['S0', 'I0', 'R0'])
        with pytest.raises(ValueError, match="No parameters to fit"):
            model.fit()

    def test_fit_with_all_params_on_constructor(self):
        """Fit using parameters and data from the constructor."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))
        data = forecast

        model = EpidemicModel(SIR, parameters=params, data=data, initial_conditions=ic)
        result = model.fit()
        assert model.result is not None
        assert model.fitted_parameters is not None

    def test_fit_with_explicit_params(self):
        """Fit using parameters passed explicitly to fit()."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))
        data = forecast

        model = EpidemicModel(SIR, data=data, initial_conditions=ic)
        result = model.fit(parameters=params)
        assert model.result is not None

    def test_fit_with_explicit_data(self):
        """Fit using data passed explicitly to fit()."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        model = EpidemicModel(SIR, parameters=params, initial_conditions=ic)
        result = model.fit(data=forecast)
        assert model.result is not None

    def test_fit_with_time_range(self):
        """Fit using explicit time_range."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        model = EpidemicModel(SIR, parameters=params, data=forecast, initial_conditions=ic)
        result = model.fit(time_range=t)
        assert model.result is not None

    def test_fit_with_initial_conditions_override(self):
        """Fit with initial_conditions passed to fit()."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        model = EpidemicModel(SIR, parameters=params, data=forecast)
        result = model.fit(initial_conditions=ic)
        assert model.initial_conditions == ic

    def test_fit_with_fit_period(self):
        """Fit with fit_period passed to fit()."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        model = EpidemicModel(SIR, parameters=params, data=forecast, initial_conditions=ic)
        result = model.fit(fit_period=10)
        assert model.fit_period == 10

    def test_fit_with_custom_residual(self):
        """Fit with a custom residual function."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        custom_residual = MagicMock(return_value=np.zeros(40 * 3))
        model = EpidemicModel(SIR, parameters=params, data=forecast, initial_conditions=ic)
        result = model.fit(residual=custom_residual)
        assert model.result is not None

    def test_fit_verbose(self):
        """Fit with verbose=True to exercise report_fit."""
        N = 100
        beta, gamma = 0.5, 0.25
        t = np.arange(40)
        params, ic = SIR.get_parameters(99, 1, 0, N, beta=beta + 0.1, gamma=gamma + 0.1)
        solver = EpidemicModel(SIR)
        forecast = solver.solve(t, (99, 1, 0), (beta, gamma, N))

        model = EpidemicModel(SIR, parameters=params, data=forecast, initial_conditions=ic)
        with patch('gs_quant.models.epidemiology.report_fit') as mock_report:
            result = model.fit(verbose=True)
            mock_report.assert_called_once()
