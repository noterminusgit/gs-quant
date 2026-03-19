"""
Tests for gs_quant.api.gs.scenarios - GsScenarioApi, GsFactorScenarioApi
Target: 100% branch coverage
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.scenarios import GsScenarioApi, GsFactorScenarioApi
from gs_quant.target.risk import Scenario


def _mock_session():
    session = MagicMock()
    return session


class TestGsScenarioApiCRUD:
    def test_create_scenario(self):
        mock_session = _mock_session()
        scenario = MagicMock(spec=Scenario)
        mock_session.sync.post.return_value = scenario
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.create_scenario(scenario)
            mock_session.sync.post.assert_called_once_with('/risk/scenarios', scenario, cls=Scenario)
            assert result == scenario

    def test_get_scenario(self):
        mock_session = _mock_session()
        scenario = MagicMock(spec=Scenario)
        mock_session.sync.get.return_value = scenario
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.get_scenario('s1')
            mock_session.sync.get.assert_called_once_with('/risk/scenarios/s1', cls=Scenario)
            assert result == scenario

    def test_update_scenario(self):
        mock_session = _mock_session()
        scenario = MagicMock(spec=Scenario)
        scenario.id_ = 's1'
        mock_session.sync.put.return_value = {'status': 'updated'}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.update_scenario(scenario)
            mock_session.sync.put.assert_called_once_with('/risk/scenarios/s1', scenario, cls=Scenario)
            assert result == {'status': 'updated'}

    def test_delete_scenario(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'deleted'}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.delete_scenario('s1')
            mock_session.sync.delete.assert_called_once_with('/risk/scenarios/s1')
            assert result == {'status': 'deleted'}

    def test_calculate_scenario(self):
        mock_session = _mock_session()
        request = {'data': 'value'}
        mock_session.sync.post.return_value = {'result': 'ok'}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.calculate_scenario(request)
            mock_session.sync.post.assert_called_once_with('/scenarios/calculate', request)
            assert result == {'result': 'ok'}


class TestGsScenarioApiGetMany:
    def test_get_many_scenarios_no_filters(self):
        """Branch: ids None, names None, kwargs empty"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.get_many_scenarios()
            url = mock_session.sync.get.call_args[0][0]
            assert url == '/risk/scenarios?limit=100'
            assert result == []

    def test_get_many_scenarios_with_ids(self):
        """Branch: ids is truthy"""
        mock_session = _mock_session()
        s1 = MagicMock(spec=Scenario)
        mock_session.sync.get.return_value = {'results': [s1]}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.get_many_scenarios(ids=['id1', 'id2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=id1&id=id2' in url

    def test_get_many_scenarios_with_names(self):
        """Branch: names is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScenarioApi.get_many_scenarios(names=['n1', 'n2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&name=n1&name=n2' in url

    def test_get_many_scenarios_with_kwargs_list(self):
        """Branch: kwargs present with list value -> isinstance(v, list) True"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScenarioApi.get_many_scenarios(shockedFactor=['factor1', 'factor2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&shockedFactor=factor1&shockedFactor=factor2' in url

    def test_get_many_scenarios_with_kwargs_scalar(self):
        """Branch: kwargs present with scalar value -> isinstance(v, list) False"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScenarioApi.get_many_scenarios(riskModel='MODEL1')
            url = mock_session.sync.get.call_args[0][0]
            assert '&riskModel=MODEL1' in url

    def test_get_many_scenarios_with_all_params(self):
        """All params provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScenarioApi.get_many_scenarios(
                ids=['id1'], names=['n1'], limit=50,
                riskModel='MODEL1', shockedFactor=['f1', 'f2']
            )
            url = mock_session.sync.get.call_args[0][0]
            assert 'limit=50' in url
            assert '&id=id1' in url
            assert '&name=n1' in url
            assert '&riskModel=MODEL1' in url
            assert '&shockedFactor=f1&shockedFactor=f2' in url

    def test_get_many_scenarios_no_results_key(self):
        """Branch: .get('results', []) fallback"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.get_many_scenarios()
            assert result == []


class TestGsScenarioApiGetByName:
    def test_get_scenario_by_name_found_exactly_one(self):
        """Branch: num_found == 1 -> returns result"""
        mock_session = _mock_session()
        scenario = MagicMock(spec=Scenario)
        mock_session.sync.get.return_value = {'totalResults': 1, 'results': [scenario]}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScenarioApi.get_scenario_by_name('test')
            assert result == scenario

    def test_get_scenario_by_name_not_found(self):
        """Branch: num_found == 0 -> ValueError"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'totalResults': 0, 'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError, match='not found'):
                GsScenarioApi.get_scenario_by_name('missing')

    def test_get_scenario_by_name_multiple_found(self):
        """Branch: num_found > 1 -> ValueError"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'totalResults': 2, 'results': ['s1', 's2']}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError, match='More than one'):
                GsScenarioApi.get_scenario_by_name('duplicate')

    def test_get_scenario_by_name_no_total_results_key(self):
        """Branch: 'totalResults' missing -> defaults to 0 -> ValueError"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError, match='not found'):
                GsScenarioApi.get_scenario_by_name('missing')


class TestGsFactorScenarioApi:
    def test_init(self):
        """Cover __init__"""
        api = GsFactorScenarioApi()
        assert isinstance(api, GsFactorScenarioApi)

    def test_get_many_scenarios_no_filters(self):
        """Branch: all optional params None -> factor_scenario_args is empty"""
        mock_session = _mock_session()
        s1 = MagicMock(spec=Scenario)
        s1.type_ = 'FactorScenario'
        mock_session.sync.get.return_value = {'results': [s1]}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsFactorScenarioApi.get_many_scenarios()
            assert s1 in result

    def test_get_many_scenarios_filters_out_no_type(self):
        """Branch: scenario.type_ is falsy -> filtered out"""
        mock_session = _mock_session()
        s1 = MagicMock(spec=Scenario)
        s1.type_ = 'FactorScenario'
        s2 = MagicMock(spec=Scenario)
        s2.type_ = None  # Should be filtered out
        mock_session.sync.get.return_value = {'results': [s1, s2]}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsFactorScenarioApi.get_many_scenarios()
            assert len(result) == 1
            assert s1 in result

    def test_get_many_scenarios_with_risk_model(self):
        """Branch: risk_model is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(risk_model='MODEL1')
            url = mock_session.sync.get.call_args[0][0]
            assert '&riskModel=MODEL1' in url

    def test_get_many_scenarios_with_type(self):
        """Branch: type is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(type='Historical')
            url = mock_session.sync.get.call_args[0][0]
            assert '&factorScenarioType=Historical' in url

    def test_get_many_scenarios_with_shocked_factors(self):
        """Branch: shocked_factors is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(shocked_factors=['f1', 'f2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&shockedFactor=f1&shockedFactor=f2' in url

    def test_get_many_scenarios_with_shocked_factor_categories(self):
        """Branch: shocked_factor_categories is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(shocked_factor_categories=['cat1'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&shockedFactorCategory=cat1' in url

    def test_get_many_scenarios_with_start_date(self):
        """Branch: start_date is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(start_date=dt.date(2023, 1, 1))
            url = mock_session.sync.get.call_args[0][0]
            assert 'historicalSimulationStartDate=2023-01-01' in url

    def test_get_many_scenarios_with_end_date(self):
        """Branch: end_date is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(end_date=dt.date(2023, 12, 31))
            url = mock_session.sync.get.call_args[0][0]
            assert 'historicalSimulationEndDate=2023-12-31' in url

    def test_get_many_scenarios_with_tags(self):
        """Branch: tags is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsFactorScenarioApi.get_many_scenarios(tags=['tag1', 'tag2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&tags=tag1&tags=tag2' in url

    def test_get_many_scenarios_all_params(self):
        """All optional params provided"""
        mock_session = _mock_session()
        s1 = MagicMock(spec=Scenario)
        s1.type_ = 'FactorScenario'
        mock_session.sync.get.return_value = {'results': [s1]}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsFactorScenarioApi.get_many_scenarios(
                ids=['id1'], names=['n1'], limit=50, type='Historical',
                risk_model='M1', shocked_factors=['f1'], shocked_factor_categories=['c1'],
                start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 12, 31), tags=['t1']
            )
            assert len(result) == 1

    def test_calculate_scenario(self):
        mock_session = _mock_session()
        request = {'calc': 'data'}
        mock_session.sync.post.return_value = {'result': 'ok'}
        with patch('gs_quant.api.gs.scenarios.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsFactorScenarioApi.calculate_scenario(request)
            mock_session.sync.post.assert_called_once_with('/scenarios/calculate', request)
            assert result == {'result': 'ok'}
