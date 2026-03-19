"""
Tests for gs_quant/markets/scenario.py targeting 100% branch coverage.
"""
import datetime as dt
from copy import deepcopy
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.common import Entitlements as TargetEntitlements
from gs_quant.entities.entitlements import Entitlements, EntitlementBlock
from gs_quant.errors import MqValueError
from gs_quant.markets.factor import Factor
from gs_quant.markets.scenario import (
    ScenarioCalculationType,
    FactorShock,
    FactorShockParameters,
    HistoricalSimulationParameters,
    FactorScenario,
    FactorScenarioType,
)
from gs_quant.target.risk import Scenario as TargetScenario


# ── ScenarioCalculationType ──────────────────────────────────────────────


class TestScenarioCalculationType:
    def test_value(self):
        assert ScenarioCalculationType.FACTOR_SCENARIO.value == "Factor Scenario"


# ── FactorShock ──────────────────────────────────────────────────────────


class TestFactorShock:
    def test_init_with_string(self):
        fs = FactorShock(factor="Momentum", shock=5.0)
        assert fs.factor == "Momentum"
        assert fs.shock == 5.0

    def test_init_with_factor_object(self):
        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        fs = FactorShock(factor=f, shock=3.0)
        assert fs.factor is f

    def test_eq_both_strings(self):
        fs1 = FactorShock(factor="Momentum", shock=5.0)
        fs2 = FactorShock(factor="Momentum", shock=5.0)
        assert fs1 == fs2

    def test_eq_both_factor_objects(self):
        f1 = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        f2 = Factor(risk_model_id='M', id_='id2', type_='Style', name='Momentum')
        fs1 = FactorShock(factor=f1, shock=5.0)
        fs2 = FactorShock(factor=f2, shock=5.0)
        assert fs1 == fs2

    def test_eq_string_and_factor(self):
        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        fs1 = FactorShock(factor="Momentum", shock=5.0)
        fs2 = FactorShock(factor=f, shock=5.0)
        assert fs1 == fs2

    def test_eq_factor_and_string(self):
        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        fs1 = FactorShock(factor=f, shock=5.0)
        fs2 = FactorShock(factor="Momentum", shock=5.0)
        assert fs1 == fs2

    def test_not_eq_different_factor(self):
        fs1 = FactorShock(factor="Momentum", shock=5.0)
        fs2 = FactorShock(factor="Value", shock=5.0)
        assert fs1 != fs2

    def test_not_eq_different_shock(self):
        fs1 = FactorShock(factor="Momentum", shock=5.0)
        fs2 = FactorShock(factor="Momentum", shock=-5.0)
        assert fs1 != fs2

    def test_not_eq_non_factorshock(self):
        fs = FactorShock(factor="Momentum", shock=5.0)
        assert fs != "not a factor shock"
        assert fs != 42
        assert fs != None

    def test_repr(self):
        fs = FactorShock(factor="Momentum", shock=5.0)
        r = repr(fs)
        assert 'FactorShock' in r
        assert 'Momentum' in r
        assert '5.0' in r

    def test_shock_setter(self):
        fs = FactorShock(factor="Momentum", shock=5.0)
        fs.shock = 10.0
        assert fs.shock == 10.0

    def test_to_dict_string_factor(self):
        fs = FactorShock(factor="Momentum", shock=5.0)
        d = fs.to_dict()
        assert d == {"factor": "Momentum", "shock": 5.0}

    def test_to_dict_factor_object(self):
        f = Factor(risk_model_id='M', id_='id1', type_='Style', name='Momentum')
        fs = FactorShock(factor=f, shock=3.0)
        d = fs.to_dict()
        assert d == {"factor": "Momentum", "shock": 3.0}

    def test_from_dict(self):
        d = {"factor": "Momentum", "shock": 5.0}
        fs = FactorShock.from_dict(d)
        assert fs.factor == "Momentum"
        assert fs.shock == 5.0


# ── FactorShockParameters ───────────────────────────────────────────────


class TestFactorShockParameters:
    def test_init(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="MODEL")
        assert fsp.factor_shocks == [fs1]
        assert fsp.propagate_shocks is True
        assert fsp.risk_model == "MODEL"

    def test_eq_same(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp1 = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="M")
        fsp2 = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="M")
        assert fsp1 == fsp2

    def test_eq_different(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp1 = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="M")
        fsp2 = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=False, risk_model="M")
        assert fsp1 != fsp2

    def test_not_eq_non_instance(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="M")
        assert fsp != "not a FactorShockParameters"

    def test_repr(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="MODEL")
        r = repr(fsp)
        assert 'FactorShockParameters' in r
        assert 'MODEL' in r

    def test_factor_shocks_setter_list(self):
        fsp = FactorShockParameters()
        fs1 = FactorShock(factor="F1", shock=5)
        fsp.factor_shocks = [fs1]
        assert fsp.factor_shocks == [fs1]

    def test_factor_shocks_setter_dict(self):
        fsp = FactorShockParameters()
        fsp.factor_shocks = {"F1": 5, "F2": -3}
        assert len(fsp.factor_shocks) == 2
        assert fsp.factor_shocks[0].factor == "F1"
        assert fsp.factor_shocks[0].shock == 5
        assert fsp.factor_shocks[1].factor == "F2"
        assert fsp.factor_shocks[1].shock == -3

    def test_factor_shocks_setter_dataframe(self):
        """DataFrame: zip of columns and data rows.
        With 2 rows, zip pairs each column name with corresponding row."""
        fsp = FactorShockParameters()
        df = pd.DataFrame([[5], [-3]], columns=["F1"])
        fsp.factor_shocks = df
        # orient='split' gives columns=['F1'], data=[[5], [-3]]
        # zip(['F1'], [[5], [-3]]) => [('F1', [5])]
        # So only one factor shock is created from the first pair
        assert len(fsp.factor_shocks) == 1
        assert fsp.factor_shocks[0].factor == "F1"

    def test_factor_shocks_setter_dataframe_multi_row(self):
        """DataFrame with multiple columns and rows matching."""
        fsp = FactorShockParameters()
        # Two columns and two data rows => zip pairs F1 with [5, 10], F2 with [-3, -6]
        df = pd.DataFrame([[5, -3], [10, -6]], columns=["F1", "F2"])
        fsp.factor_shocks = df
        assert len(fsp.factor_shocks) == 2
        assert fsp.factor_shocks[0].factor == "F1"
        assert fsp.factor_shocks[1].factor == "F2"

    def test_propagate_shocks_setter(self):
        fsp = FactorShockParameters()
        fsp.propagate_shocks = True
        assert fsp.propagate_shocks is True

    def test_from_dict(self):
        d = {
            "factorShocks": [{"factor": "F1", "shock": 5}],
            "riskModel": "MODEL",
            "propagateShocks": True,
        }
        fsp = FactorShockParameters.from_dict(d)
        assert fsp.risk_model == "MODEL"
        assert fsp.propagate_shocks is True
        assert len(fsp.factor_shocks) == 1

    def test_to_dict(self):
        fs1 = FactorShock(factor="F1", shock=5)
        fsp = FactorShockParameters(factor_shocks=[fs1], propagate_shocks=True, risk_model="MODEL")
        d = fsp.to_dict()
        assert d == {
            "riskModel": "MODEL",
            "propagateShocks": True,
            "factorShocks": [{"factor": "F1", "shock": 5}],
        }


# ── HistoricalSimulationParameters ──────────────────────────────────────


class TestHistoricalSimulationParameters:
    def test_init(self):
        hsp = HistoricalSimulationParameters(
            start_date=dt.date(2020, 1, 1),
            end_date=dt.date(2020, 12, 31),
        )
        assert hsp.start_date == dt.date(2020, 1, 1)
        assert hsp.end_date == dt.date(2020, 12, 31)

    def test_eq_same(self):
        hsp1 = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        hsp2 = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        assert hsp1 == hsp2

    def test_eq_different(self):
        hsp1 = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        hsp2 = HistoricalSimulationParameters(start_date=dt.date(2021, 1, 1), end_date=dt.date(2020, 12, 31))
        assert hsp1 != hsp2

    def test_not_eq_non_instance(self):
        hsp = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        assert hsp != "not an HSP"

    def test_repr(self):
        hsp = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        r = repr(hsp)
        assert 'HistoricalSimulationParameters' in r
        assert '2020, 1, 1' in r

    def test_setters(self):
        hsp = HistoricalSimulationParameters()
        hsp.start_date = dt.date(2020, 1, 1)
        hsp.end_date = dt.date(2020, 12, 31)
        assert hsp.start_date == dt.date(2020, 1, 1)
        assert hsp.end_date == dt.date(2020, 12, 31)

    def test_from_dict(self):
        d = {"startDate": "2020-01-01", "endDate": "2020-12-31"}
        hsp = HistoricalSimulationParameters.from_dict(d)
        assert hsp.start_date == dt.date(2020, 1, 1)
        assert hsp.end_date == dt.date(2020, 12, 31)

    def test_to_dict(self):
        hsp = HistoricalSimulationParameters(start_date=dt.date(2020, 1, 1), end_date=dt.date(2020, 12, 31))
        d = hsp.to_dict()
        assert d == {"startDate": dt.date(2020, 1, 1), "endDate": dt.date(2020, 12, 31)}


# ── FactorScenario ──────────────────────────────────────────────────────


default_entitlements = TargetEntitlements(edit=(), view=(), admin=())

default_scenario_parameters = {
    "riskModel": "MODEL_ID",
    "propagateShocks": True,
    "factorShocks": [{"factor": "Factor 1", "shock": 5}, {"factor": "Factor 2", "shock": -5}],
}

historical_sim_parameters = {
    "startDate": "2020-01-01",
    "endDate": "2020-12-31",
}


def _make_target_scenario(type_=FactorScenarioType.Factor_Shock, params=None, id_='MSCENARIO'):
    return TargetScenario(
        name="Scenario 1",
        description="Scenario desc",
        entitlements=default_entitlements,
        id_=id_,
        parameters=params or default_scenario_parameters,
        type_=type_,
        tags=('tag1', 'tag2'),
    )


class TestFactorScenarioInit:
    def test_init_with_factor_shock_dict_parameters(self):
        """Dict parameters with Factor_Shock type => FactorShockParameters.from_dict."""
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=default_scenario_parameters,
        )
        assert isinstance(scenario.parameters, FactorShockParameters)
        assert scenario.parameters.risk_model == "MODEL_ID"

    def test_init_with_historical_sim_dict_parameters(self):
        """Dict parameters with Historical_Simulation type => HistoricalSimulationParameters.from_dict."""
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Historical_Simulation,
            parameters=historical_sim_parameters,
        )
        assert isinstance(scenario.parameters, HistoricalSimulationParameters)
        assert scenario.parameters.start_date == dt.date(2020, 1, 1)

    def test_init_with_factor_shock_parameters_object(self):
        """Pass FactorShockParameters object directly."""
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
        )
        assert scenario.parameters is fsp

    def test_init_with_historical_sim_parameters_object(self):
        """Pass HistoricalSimulationParameters object directly."""
        hsp = HistoricalSimulationParameters(
            start_date=dt.date(2020, 1, 1),
            end_date=dt.date(2020, 12, 31),
        )
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Historical_Simulation,
            parameters=hsp,
        )
        assert scenario.parameters is hsp

    def test_init_all_fields(self):
        """All fields provided."""
        entitlements = Entitlements(view=EntitlementBlock(), edit=EntitlementBlock(), admin=EntitlementBlock())
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Full Scenario",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            entitlements=entitlements,
            id_="SCENARIO_ID",
            description="Full description",
            tags=["tag1", "tag2"],
        )
        assert scenario.id == "SCENARIO_ID"
        assert scenario.name == "Full Scenario"
        assert scenario.type == FactorScenarioType.Factor_Shock
        assert scenario.description == "Full description"
        assert scenario.entitlements is entitlements
        assert scenario.tags == ["tag1", "tag2"]


class TestFactorScenarioReprStr:
    def test_repr(self):
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            entitlements=Entitlements(),
            id_="ID1",
            description="desc",
        )
        r = repr(scenario)
        assert 'FactorScenario' in r
        assert 'ID1' in r
        assert 'Test' in r

    def test_str(self):
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            id_="ID1",
            description="desc",
        )
        s = str(scenario)
        assert 'FactorScenario' in s
        assert 'Test' in s
        assert 'Factor Shock' in s


class TestFactorScenarioSetters:
    def test_name_setter(self):
        scenario = FactorScenario(
            name="Old",
            type=FactorScenarioType.Factor_Shock,
            parameters=FactorShockParameters(factor_shocks=[]),
        )
        scenario.name = "New"
        assert scenario.name == "New"

    def test_description_setter(self):
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=FactorShockParameters(factor_shocks=[]),
        )
        scenario.description = "new desc"
        assert scenario.description == "new desc"

    def test_parameters_setter(self):
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=FactorShockParameters(factor_shocks=[]),
        )
        new_params = FactorShockParameters(
            factor_shocks=[FactorShock(factor="X", shock=10)],
            propagate_shocks=False,
            risk_model="NEW_MODEL",
        )
        scenario.parameters = new_params
        assert scenario.parameters is new_params

    def test_entitlements_setter(self):
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=FactorShockParameters(factor_shocks=[]),
        )
        new_ent = Entitlements(view=EntitlementBlock(roles=["role:A"]))
        scenario.entitlements = new_ent
        assert scenario.entitlements is new_ent

    def test_tags_setter(self):
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=FactorShockParameters(factor_shocks=[]),
        )
        scenario.tags = ["t1", "t2"]
        assert scenario.tags == ["t1", "t2"]


class TestFactorScenarioFromDict:
    def test_from_dict_factor_shock(self):
        d = {
            "name": "Scenario 1",
            "description": "desc",
            "id": "MSCENARIO",
            "type": FactorScenarioType.Factor_Shock,
            "parameters": default_scenario_parameters,
            "entitlements": None,
            "tags": ["tag1"],
        }
        scenario = FactorScenario.from_dict(d)
        assert scenario.name == "Scenario 1"
        assert scenario.id == "MSCENARIO"
        assert isinstance(scenario.parameters, FactorShockParameters)

    def test_from_dict_historical_sim(self):
        d = {
            "name": "HS Scenario",
            "description": "desc",
            "id": "HS1",
            "type": FactorScenarioType.Factor_Historical_Simulation,
            "parameters": historical_sim_parameters,
            "entitlements": None,
            "tags": None,
        }
        scenario = FactorScenario.from_dict(d)
        assert isinstance(scenario.parameters, HistoricalSimulationParameters)


class TestFactorScenarioFromTarget:
    def test_from_target_factor_shock(self):
        target = _make_target_scenario(type_=FactorScenarioType.Factor_Shock)
        scenario = FactorScenario.from_target(target)
        assert isinstance(scenario.parameters, FactorShockParameters)
        assert scenario.id == 'MSCENARIO'
        assert scenario.name == 'Scenario 1'
        assert isinstance(scenario.entitlements, Entitlements)

    def test_from_target_historical_sim(self):
        target = _make_target_scenario(
            type_=FactorScenarioType.Factor_Historical_Simulation,
            params=historical_sim_parameters,
        )
        scenario = FactorScenario.from_target(target)
        assert isinstance(scenario.parameters, HistoricalSimulationParameters)


class TestFactorScenarioGet:
    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get(self, mock_api):
        target = _make_target_scenario()
        mock_api.get_scenario.return_value = target
        scenario = FactorScenario.get('MSCENARIO')
        assert scenario.id == 'MSCENARIO'
        mock_api.get_scenario.assert_called_once_with('MSCENARIO')


class TestFactorScenarioGetByName:
    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_by_name(self, mock_api):
        target = _make_target_scenario()
        mock_api.get_scenario_by_name.return_value = target
        scenario = FactorScenario.get_by_name('Scenario 1')
        assert scenario.name == 'Scenario 1'
        mock_api.get_scenario_by_name.assert_called_once_with('Scenario 1')


class TestFactorScenarioGetMany:
    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_no_filter(self, mock_api):
        target = _make_target_scenario()
        mock_api.get_many_scenarios.return_value = [target]
        scenarios = FactorScenario.get_many()
        assert len(scenarios) == 1
        assert scenarios[0].id == 'MSCENARIO'

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_with_type_enum(self, mock_api):
        """Pass FactorScenarioType enum => .value used."""
        target = _make_target_scenario()
        mock_api.get_many_scenarios.return_value = [target]
        scenarios = FactorScenario.get_many(type=FactorScenarioType.Factor_Shock)
        assert len(scenarios) == 1
        call_kwargs = mock_api.get_many_scenarios.call_args
        assert call_kwargs.kwargs['type'] == 'Factor Shock'

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_with_type_string(self, mock_api):
        """Pass string type => used as-is."""
        target = _make_target_scenario()
        mock_api.get_many_scenarios.return_value = [target]
        scenarios = FactorScenario.get_many(type="Factor Shock")
        call_kwargs = mock_api.get_many_scenarios.call_args
        assert call_kwargs.kwargs['type'] == 'Factor Shock'

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_propagated_shocks_true(self, mock_api):
        """propagated_shocks=True => filter scenarios with propagate_shocks=True."""
        target_propagated = _make_target_scenario()
        target_not_propagated = _make_target_scenario(
            params={
                "riskModel": "M",
                "propagateShocks": False,
                "factorShocks": [{"factor": "F1", "shock": 5}],
            },
            id_='MS2',
        )
        mock_api.get_many_scenarios.return_value = [target_propagated, target_not_propagated]
        scenarios = FactorScenario.get_many(propagated_shocks=True)
        # target_propagated has propagate_shocks=True, target_not_propagated has False
        assert len(scenarios) == 1
        assert scenarios[0].parameters.propagate_shocks is True

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_propagated_shocks_false(self, mock_api):
        """propagated_shocks=False => filter scenarios with propagate_shocks=False."""
        target_propagated = _make_target_scenario()
        target_not_propagated = _make_target_scenario(
            params={
                "riskModel": "M",
                "propagateShocks": False,
                "factorShocks": [{"factor": "F1", "shock": 5}],
            },
            id_='MS2',
        )
        mock_api.get_many_scenarios.return_value = [target_propagated, target_not_propagated]
        scenarios = FactorScenario.get_many(propagated_shocks=False)
        # Only target_not_propagated matches propagated_shocks=False
        assert len(scenarios) == 1
        assert scenarios[0].parameters.propagate_shocks is False

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_propagated_shocks_all_factor_shock(self, mock_api):
        """All Factor_Shock scenarios => only those matching propagate_shocks filter pass."""
        target_propagated = _make_target_scenario(id_='MS1')
        target_also_propagated = _make_target_scenario(
            params={
                "riskModel": "M2",
                "propagateShocks": True,
                "factorShocks": [{"factor": "F3", "shock": 3}],
            },
            id_='MS3',
        )
        mock_api.get_many_scenarios.return_value = [target_propagated, target_also_propagated]
        scenarios = FactorScenario.get_many(propagated_shocks=True)
        assert len(scenarios) == 2

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_propagated_shocks_none(self, mock_api):
        """propagated_shocks=None => no filtering."""
        target = _make_target_scenario()
        mock_api.get_many_scenarios.return_value = [target]
        scenarios = FactorScenario.get_many(propagated_shocks=None)
        assert len(scenarios) == 1

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_get_many_all_params(self, mock_api):
        """Pass all parameters."""
        target = _make_target_scenario()
        mock_api.get_many_scenarios.return_value = [target]
        scenarios = FactorScenario.get_many(
            ids=['MSCENARIO'],
            names=['Scenario 1'],
            type=FactorScenarioType.Factor_Shock,
            risk_model="MODEL_ID",
            shocked_factors=["Factor 1"],
            shocked_factor_categories=["Style"],
            start_date=dt.date(2020, 1, 1),
            end_date=dt.date(2020, 12, 31),
            tags=["tag1"],
            limit=50,
        )
        assert len(scenarios) == 1


class TestFactorScenarioSave:
    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_save_update_with_id(self, mock_api):
        """Scenario with id => update."""
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Test",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            id_="EXISTING_ID",
            description="desc",
            entitlements=Entitlements(view=EntitlementBlock()),
            tags=["tag1"],
        )
        scenario.save()
        mock_api.update_scenario.assert_called_once()

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_save_create_without_id(self, mock_api):
        """Scenario without id => create."""
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        created_scenario = MagicMock()
        created_scenario.id = 'NEW_ID'
        mock_api.create_scenario.return_value = created_scenario

        scenario = FactorScenario(
            name="New Scenario",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
        )
        scenario.save()
        mock_api.create_scenario.assert_called_once()
        assert scenario.id == 'NEW_ID'

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_save_no_description_no_entitlements_no_tags(self, mock_api):
        """Save with no description, no entitlements, no tags."""
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        created_scenario = MagicMock()
        created_scenario.id = 'NEW_ID'
        mock_api.create_scenario.return_value = created_scenario

        scenario = FactorScenario(
            name="No extras",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
        )
        scenario.save()
        mock_api.create_scenario.assert_called_once()
        # Verify target_scenario had description=None, entitlements=None, tags=()
        call_args = mock_api.create_scenario.call_args
        target = call_args[0][0]
        assert target.description is None
        assert target.entitlements is None
        assert target.tags == ()

    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_save_with_description_and_tags(self, mock_api):
        """Save with description and tags."""
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        created_scenario = MagicMock()
        created_scenario.id = 'NEW_ID'
        mock_api.create_scenario.return_value = created_scenario

        scenario = FactorScenario(
            name="With extras",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            description="Has description",
            tags=["tag1", "tag2"],
        )
        scenario.save()
        call_args = mock_api.create_scenario.call_args
        target = call_args[0][0]
        assert target.description == "Has description"
        assert target.tags == ('tag1', 'tag2')


class TestFactorScenarioDelete:
    @patch('gs_quant.markets.scenario.GsFactorScenarioApi')
    def test_delete_with_id(self, mock_api):
        fsp = FactorShockParameters(factor_shocks=[], propagate_shocks=False, risk_model="M")
        scenario = FactorScenario(
            name="Del",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            id_="DEL_ID",
        )
        scenario.delete()
        mock_api.delete_scenario.assert_called_once_with("DEL_ID")

    def test_delete_without_id_raises(self):
        fsp = FactorShockParameters(factor_shocks=[], propagate_shocks=False, risk_model="M")
        scenario = FactorScenario(
            name="No ID",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
        )
        with pytest.raises(MqValueError, match="Cannot delete scenario"):
            scenario.delete()


class TestFactorScenarioClone:
    def test_clone(self):
        fsp = FactorShockParameters(
            factor_shocks=[FactorShock(factor="F1", shock=5)],
            propagate_shocks=True,
            risk_model="MODEL",
        )
        scenario = FactorScenario(
            name="Original",
            type=FactorScenarioType.Factor_Shock,
            parameters=fsp,
            id_="ORIG_ID",
            description="orig desc",
        )
        clone = scenario.clone()
        assert clone.name == "Original copy"
        assert clone.description == "orig desc"
        assert clone.type == FactorScenarioType.Factor_Shock
        assert clone.id is None
        # Parameters should be a deep copy
        assert clone.parameters is not scenario.parameters
        assert clone.parameters == scenario.parameters
