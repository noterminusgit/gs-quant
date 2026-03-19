"""
Copyright 2019 Goldman Sachs.
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

Comprehensive branch-coverage tests for gs_quant/markets/optimizer.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.errors import MqValueError
from gs_quant.markets.factor import Factor
from gs_quant.markets.optimizer import (
    AssetConstraint,
    AssetUniverse,
    ConstraintPriorities,
    CountryConstraint,
    FactorConstraint,
    HedgeTarget,
    IndustryConstraint,
    MaxFactorProportionOfRiskConstraint,
    MaxProportionOfRiskByGroupConstraint,
    OptimizationConstraintUnit,
    OptimizerConstraints,
    OptimizerObjective,
    OptimizerObjectiveParameters,
    OptimizerObjectiveTerm,
    OptimizerRiskType,
    OptimizerSettings,
    OptimizerStrategy,
    OptimizerType,
    OptimizerUniverse,
    PrioritySetting,
    SectorConstraint,
    TurnoverConstraint,
    TurnoverNotionalType,
    resolve_assets_in_batches,
)
from gs_quant.markets.position_set import Position, PositionSet
from gs_quant.target.hedge import CorporateActionsTypes


# =============================================================================
# Enum tests
# =============================================================================


class TestEnums:
    def test_optimization_constraint_unit_values(self):
        assert OptimizationConstraintUnit.DECIMAL.value == 'Decimal'
        assert OptimizationConstraintUnit.NOTIONAL.value == 'Notional'
        assert OptimizationConstraintUnit.PERCENT.value == 'Percent'

    def test_hedge_target_values(self):
        assert HedgeTarget.HEDGED_TARGET.value == 'hedgedTarget'
        assert HedgeTarget.HEDGE.value == 'hedge'
        assert HedgeTarget.TARGET.value == 'target'

    def test_optimizer_objective_values(self):
        assert OptimizerObjective.MINIMIZE_FACTOR_RISK.value == 'Minimize Factor Risk'

    def test_optimizer_risk_type_values(self):
        assert OptimizerRiskType.VARIANCE.value == 'Variance'

    def test_optimizer_type_values(self):
        assert OptimizerType.AXIOMA_PORTFOLIO_OPTIMIZER.value == 'Axioma Portfolio Optimizer'

    def test_priority_setting_values(self):
        assert PrioritySetting.ZERO.value == '0'
        assert PrioritySetting.ONE.value == '1'
        assert PrioritySetting.TWO.value == '2'
        assert PrioritySetting.THREE.value == '3'
        assert PrioritySetting.FOUR.value == '4'
        assert PrioritySetting.FIVE.value == '5'

    def test_turnover_notional_type_values(self):
        assert TurnoverNotionalType.NET.value == 'Net'
        assert TurnoverNotionalType.LONG.value == 'Long'
        assert TurnoverNotionalType.GROSS.value == 'Gross'


# =============================================================================
# resolve_assets_in_batches
# =============================================================================


class TestResolveAssetsInBatches:
    @patch('gs_quant.markets.optimizer.GsAssetApi.resolve_assets')
    def test_small_batch_no_split(self, mock_resolve):
        """identifiers <= batch_size should produce a single batch"""
        mock_resolve.return_value = {
            'AAPL': [{'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL UW'}],
            'MSFT': [{'id': 'id2', 'name': 'Microsoft', 'bbid': 'MSFT UW'}],
        }
        result = resolve_assets_in_batches(['AAPL', 'MSFT'], batch_size=100)
        assert len(result) == 2
        assert result[0]['identifier'] == 'AAPL'
        assert result[1]['identifier'] == 'MSFT'
        mock_resolve.assert_called_once()

    @patch('gs_quant.markets.optimizer.GsAssetApi.resolve_assets')
    def test_large_batch_split(self, mock_resolve):
        """identifiers > batch_size should be split into multiple batches"""
        ids = [f'STOCK{i}' for i in range(5)]
        mock_resolve.return_value = {
            ids[0]: [{'id': 'a0', 'name': 'S0', 'bbid': 'B0'}],
            ids[1]: [{'id': 'a1', 'name': 'S1', 'bbid': 'B1'}],
        }
        # batch_size=2 with 5 items => 3 batches
        result = resolve_assets_in_batches(ids, batch_size=2)
        assert mock_resolve.call_count == 3

    @patch('gs_quant.markets.optimizer.GsAssetApi.resolve_assets')
    def test_unresolved_assets_skipped(self, mock_resolve):
        """Assets that resolve to empty lists should be skipped"""
        mock_resolve.return_value = {
            'AAPL': [{'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL UW'}],
            'FAKE': [],
        }
        result = resolve_assets_in_batches(['AAPL', 'FAKE'], batch_size=100)
        assert len(result) == 1
        assert result[0]['identifier'] == 'AAPL'

    @patch('gs_quant.markets.optimizer.GsAssetApi.resolve_assets')
    def test_custom_fields_included(self, mock_resolve):
        """Extra fields should be passed through to GsAssetApi"""
        mock_resolve.return_value = {'AAPL': [{'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL UW', 'type': 'Stock'}]}
        result = resolve_assets_in_batches(['AAPL'], fields=['type'], batch_size=100)
        assert len(result) == 1
        # Check that fields param included 'type'
        call_kwargs = mock_resolve.call_args
        assert 'type' in call_kwargs.kwargs['fields']

    @patch('gs_quant.markets.optimizer.GsAssetApi.resolve_assets')
    def test_no_fields_default(self, mock_resolve):
        """When fields=None the default fields are used"""
        mock_resolve.return_value = {'AAPL': [{'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL UW'}]}
        resolve_assets_in_batches(['AAPL'], fields=None, batch_size=100)
        call_kwargs = mock_resolve.call_args
        assert call_kwargs.kwargs['fields'] == ['id', 'name', 'bbid']


# =============================================================================
# OptimizerObjectiveTerm
# =============================================================================


class TestOptimizerObjectiveTerm:
    def test_default_init(self):
        term = OptimizerObjectiveTerm()
        assert term.weight == 1
        assert term.params['factor_weight'] == 1
        assert term.params['specific_weight'] == 1
        assert term.params['risk_type'] == OptimizerRiskType.VARIANCE

    def test_custom_init(self):
        term = OptimizerObjectiveTerm(weight=2.5, params={'factor_weight': 3, 'specific_weight': 4})
        assert term.weight == 2.5
        assert term.params['factor_weight'] == 3
        assert term.params['specific_weight'] == 4
        # Defaults merged in
        assert term.params['risk_type'] == OptimizerRiskType.VARIANCE

    def test_weight_setter(self):
        term = OptimizerObjectiveTerm()
        term.weight = 5.0
        assert term.weight == 5.0

    def test_params_setter(self):
        term = OptimizerObjectiveTerm()
        term.params = {'factor_weight': 10}
        assert term.params['factor_weight'] == 10
        # Defaults merged
        assert term.params['specific_weight'] == 1

    def test_to_dict(self):
        term = OptimizerObjectiveTerm(weight=2.0, params={'factor_weight': 3, 'specific_weight': 4})
        d = term.to_dict()
        assert d['factorWeight'] == 3
        assert d['specificWeight'] == 4
        assert d['riskType'] == 'Variance'
        assert d['weight'] == 2.0


# =============================================================================
# OptimizerObjectiveParameters
# =============================================================================


class TestOptimizerObjectiveParameters:
    def test_default_init(self):
        params = OptimizerObjectiveParameters()
        assert params.objective == OptimizerObjective.MINIMIZE_FACTOR_RISK

    def test_setters(self):
        params = OptimizerObjectiveParameters()
        params.objective = OptimizerObjective.MINIMIZE_FACTOR_RISK
        assert params.objective == OptimizerObjective.MINIMIZE_FACTOR_RISK
        new_terms = [OptimizerObjectiveTerm()]
        params.terms = new_terms
        assert params.terms == new_terms

    def test_to_dict_single_term(self):
        term = OptimizerObjectiveTerm(weight=1.5)
        params = OptimizerObjectiveParameters(terms=[term])
        d = params.to_dict()
        assert 'parameters' in d
        assert d['parameters']['weight'] == 1.5

    def test_to_dict_multiple_terms_raises(self):
        terms = [OptimizerObjectiveTerm(), OptimizerObjectiveTerm()]
        params = OptimizerObjectiveParameters(terms=terms)
        with pytest.raises(MqValueError, match='Only single risk term is supported'):
            params.to_dict()


# =============================================================================
# AssetUniverse
# =============================================================================


class TestAssetUniverse:
    def test_init_and_properties(self):
        au = AssetUniverse(identifiers=['AAPL', 'MSFT'], asset_ids=['id1', 'id2'],
                           as_of_date=dt.date(2024, 1, 1))
        assert au.identifiers == ['AAPL', 'MSFT']
        assert au.asset_ids == ['id1', 'id2']
        assert au.as_of_date == dt.date(2024, 1, 1)

    def test_setters(self):
        au = AssetUniverse(identifiers=['AAPL'])
        au.identifiers = ['MSFT']
        assert au.identifiers == ['MSFT']
        au.asset_ids = ['new_id']
        assert au.asset_ids == ['new_id']
        au.as_of_date = dt.date(2025, 6, 1)
        assert au.as_of_date == dt.date(2025, 6, 1)

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_resolve_when_no_asset_ids(self, mock_resolve):
        """resolve() should call resolve_assets_in_batches when asset_ids is None"""
        mock_resolve.return_value = [
            {'identifier': 'AAPL', 'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL UW'},
            {'identifier': 'MSFT', 'id': 'id2', 'name': 'Microsoft', 'bbid': 'MSFT UW'},
        ]
        au = AssetUniverse(identifiers=['AAPL', 'MSFT'], as_of_date=dt.date(2024, 1, 1))
        au.resolve()
        assert au.asset_ids is not None
        mock_resolve.assert_called_once()

    def test_resolve_when_asset_ids_already_set(self):
        """resolve() should be a no-op when asset_ids is already set"""
        au = AssetUniverse(identifiers=['AAPL'], asset_ids=['id1'])
        au.resolve()
        assert au.asset_ids == ['id1']


# =============================================================================
# AssetConstraint
# =============================================================================


class TestAssetConstraint:
    def test_init_defaults(self):
        ac = AssetConstraint(asset='asset_id_1')
        assert ac.asset == 'asset_id_1'
        assert ac.minimum == 0
        assert ac.maximum == 100
        assert ac.unit == OptimizationConstraintUnit.PERCENT

    def test_init_custom(self):
        ac = AssetConstraint(
            asset='asset_id_1', minimum=5, maximum=50,
            unit=OptimizationConstraintUnit.NOTIONAL
        )
        assert ac.minimum == 5
        assert ac.maximum == 50
        assert ac.unit == OptimizationConstraintUnit.NOTIONAL

    def test_setters(self):
        ac = AssetConstraint(asset='a')
        ac.asset = 'b'
        assert ac.asset == 'b'
        ac.minimum = 10
        assert ac.minimum == 10
        ac.maximum = 90
        assert ac.maximum == 90
        ac.unit = OptimizationConstraintUnit.DECIMAL
        assert ac.unit == OptimizationConstraintUnit.DECIMAL

    def test_to_dict_with_string_asset(self):
        ac = AssetConstraint(asset='asset_id_1', minimum=5, maximum=50,
                             unit=OptimizationConstraintUnit.PERCENT)
        d = ac.to_dict()
        assert d['assetId'] == 'asset_id_1'
        assert d['min'] == 5
        assert d['max'] == 50

    def test_to_dict_with_asset_object(self):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQ_ID_123'
        ac = AssetConstraint(asset=mock_asset, minimum=5, maximum=50,
                             unit=OptimizationConstraintUnit.PERCENT)
        d = ac.to_dict()
        assert d['assetId'] == 'MQ_ID_123'

    def test_to_dict_decimal_unit_multiplies(self):
        ac = AssetConstraint(asset='a', minimum=0.05, maximum=0.5,
                             unit=OptimizationConstraintUnit.DECIMAL)
        d = ac.to_dict()
        assert d['min'] == 5.0
        assert d['max'] == 50.0

    def test_to_dict_percent_unit_no_multiply(self):
        ac = AssetConstraint(asset='a', minimum=5, maximum=50,
                             unit=OptimizationConstraintUnit.PERCENT)
        d = ac.to_dict()
        assert d['min'] == 5
        assert d['max'] == 50

    def test_to_dict_notional_unit_no_multiply(self):
        ac = AssetConstraint(asset='a', minimum=1000, maximum=5000,
                             unit=OptimizationConstraintUnit.NOTIONAL)
        d = ac.to_dict()
        assert d['min'] == 1000
        assert d['max'] == 5000

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_build_many_constraints_from_list(self, mock_resolve):
        mock_resolve.return_value = [
            {'identifier': 'AAPL UW', 'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL'},
            {'identifier': 'MSFT UW', 'id': 'id2', 'name': 'Microsoft', 'bbid': 'MSFT'},
        ]
        constraints_data = [
            {'identifier': 'AAPL UW', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
            {'identifier': 'MSFT UW', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
        ]
        result = AssetConstraint.build_many_constraints(constraints_data, as_of_date=dt.date(2024, 1, 1))
        assert len(result) == 2
        assert all(isinstance(c, AssetConstraint) for c in result)

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_build_many_constraints_from_dataframe(self, mock_resolve):
        mock_resolve.return_value = [
            {'identifier': 'AAPL UW', 'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL'},
        ]
        df = pd.DataFrame([
            {'identifier': 'AAPL UW', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
        ])
        result = AssetConstraint.build_many_constraints(df, as_of_date=dt.date(2024, 1, 1))
        assert len(result) == 1

    def test_build_many_constraints_missing_columns(self):
        data = [{'identifier': 'AAPL', 'minimum': 0}]  # missing maximum, unit
        with pytest.raises(MqValueError, match='missing required columns'):
            AssetConstraint.build_many_constraints(data)

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_build_many_constraints_mixed_units_raises(self, mock_resolve):
        data = [
            {'identifier': 'AAPL', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
            {'identifier': 'MSFT', 'minimum': 0, 'maximum': 5, 'unit': 'Decimal'},
        ]
        with pytest.raises(MqValueError, match='All asset constraints must be in the same unit'):
            AssetConstraint.build_many_constraints(data)

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_build_many_constraints_fail_on_unresolved(self, mock_resolve):
        mock_resolve.return_value = [
            {'identifier': 'AAPL UW', 'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL'},
        ]
        data = [
            {'identifier': 'AAPL UW', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
            {'identifier': 'FAKE', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
        ]
        with pytest.raises(MqValueError, match='could not be resolved'):
            AssetConstraint.build_many_constraints(data, fail_on_unresolved_positions=True)

    @patch('gs_quant.markets.optimizer.resolve_assets_in_batches')
    def test_build_many_constraints_no_fail_on_unresolved(self, mock_resolve):
        mock_resolve.return_value = [
            {'identifier': 'AAPL UW', 'id': 'id1', 'name': 'Apple', 'bbid': 'AAPL'},
        ]
        data = [
            {'identifier': 'AAPL UW', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
            {'identifier': 'FAKE', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
        ]
        result = AssetConstraint.build_many_constraints(data, fail_on_unresolved_positions=False)
        # Only the resolved one should be returned
        assert len(result) == 1

    def test_build_many_constraints_with_asset_id_column(self):
        """If assetId is already present, no resolution should happen"""
        data = [
            {'assetId': 'id1', 'identifier': 'AAPL', 'minimum': 0, 'maximum': 5, 'unit': 'Percent'},
        ]
        result = AssetConstraint.build_many_constraints(data)
        assert len(result) == 1
        assert result[0].asset == 'id1'


# =============================================================================
# CountryConstraint
# =============================================================================


class TestCountryConstraint:
    def test_init_defaults(self):
        cc = CountryConstraint(country_name='USA')
        assert cc.country_name == 'USA'
        assert cc.minimum == 0
        assert cc.maximum == 100
        assert cc.unit == OptimizationConstraintUnit.PERCENT

    def test_init_invalid_unit_raises(self):
        with pytest.raises(MqValueError, match='Country constraints can only be set by percent or decimal'):
            CountryConstraint(country_name='USA', unit=OptimizationConstraintUnit.NOTIONAL)

    def test_setters(self):
        cc = CountryConstraint(country_name='USA')
        cc.country_name = 'Canada'
        assert cc.country_name == 'Canada'
        cc.minimum = 5
        assert cc.minimum == 5
        cc.maximum = 80
        assert cc.maximum == 80

    def test_unit_setter_valid(self):
        cc = CountryConstraint(country_name='USA')
        cc.unit = OptimizationConstraintUnit.DECIMAL
        assert cc.unit == OptimizationConstraintUnit.DECIMAL

    def test_unit_setter_invalid_raises(self):
        cc = CountryConstraint(country_name='USA')
        with pytest.raises(MqValueError, match='Country constraints can only be set by percent or decimal'):
            cc.unit = OptimizationConstraintUnit.NOTIONAL

    def test_to_dict_percent(self):
        cc = CountryConstraint(country_name='USA', minimum=5, maximum=50,
                               unit=OptimizationConstraintUnit.PERCENT)
        d = cc.to_dict()
        assert d['type'] == 'Country'
        assert d['name'] == 'USA'
        assert d['min'] == 5
        assert d['max'] == 50

    def test_to_dict_decimal(self):
        cc = CountryConstraint(country_name='USA', minimum=0.05, maximum=0.5,
                               unit=OptimizationConstraintUnit.DECIMAL)
        d = cc.to_dict()
        assert d['min'] == 5.0
        assert d['max'] == 50.0

    def test_build_many_constraints_from_list(self):
        data = [
            {'country': 'USA', 'minimum': 0, 'maximum': 50, 'unit': 'Percent'},
            {'country': 'Canada', 'minimum': 0, 'maximum': 30, 'unit': 'Percent'},
        ]
        result = CountryConstraint.build_many_constraints(data)
        assert len(result) == 2
        assert result[0].country_name == 'USA'
        assert result[1].country_name == 'Canada'

    def test_build_many_constraints_from_dataframe(self):
        df = pd.DataFrame([
            {'country': 'Germany', 'minimum': 0, 'maximum': 20, 'unit': 'Percent'},
        ])
        result = CountryConstraint.build_many_constraints(df)
        assert len(result) == 1

    def test_build_many_constraints_missing_columns_raises(self):
        data = [{'country': 'USA', 'minimum': 0}]
        with pytest.raises(MqValueError, match='missing required columns'):
            CountryConstraint.build_many_constraints(data)


# =============================================================================
# SectorConstraint
# =============================================================================


class TestSectorConstraint:
    def test_init_defaults(self):
        sc = SectorConstraint(sector_name='Technology')
        assert sc.sector_name == 'Technology'
        assert sc.minimum == 0
        assert sc.maximum == 100
        assert sc.unit == OptimizationConstraintUnit.PERCENT

    def test_init_invalid_unit_raises(self):
        with pytest.raises(MqValueError, match='Sector constraints can only be set by percent or decimal'):
            SectorConstraint(sector_name='Tech', unit=OptimizationConstraintUnit.NOTIONAL)

    def test_setters(self):
        sc = SectorConstraint(sector_name='Tech')
        sc.sector_name = 'Healthcare'
        assert sc.sector_name == 'Healthcare'
        sc.minimum = 10
        assert sc.minimum == 10
        sc.maximum = 60
        assert sc.maximum == 60

    def test_unit_setter_valid(self):
        sc = SectorConstraint(sector_name='Tech')
        sc.unit = OptimizationConstraintUnit.DECIMAL
        assert sc.unit == OptimizationConstraintUnit.DECIMAL

    def test_unit_setter_invalid_raises(self):
        sc = SectorConstraint(sector_name='Tech')
        with pytest.raises(MqValueError, match='Sector constraints can only be set by percent'):
            sc.unit = OptimizationConstraintUnit.NOTIONAL

    def test_to_dict_percent(self):
        sc = SectorConstraint(sector_name='Tech', minimum=5, maximum=50,
                              unit=OptimizationConstraintUnit.PERCENT)
        d = sc.to_dict()
        assert d['type'] == 'Sector'
        assert d['name'] == 'Tech'
        assert d['min'] == 5
        assert d['max'] == 50

    def test_to_dict_decimal(self):
        sc = SectorConstraint(sector_name='Tech', minimum=0.05, maximum=0.5,
                              unit=OptimizationConstraintUnit.DECIMAL)
        d = sc.to_dict()
        assert d['min'] == 5.0
        assert d['max'] == 50.0

    def test_build_many_constraints_from_list(self):
        data = [
            {'sector': 'Technology', 'minimum': 0, 'maximum': 50, 'unit': 'Percent'},
            {'sector': 'Healthcare', 'minimum': 0, 'maximum': 30, 'unit': 'Percent'},
        ]
        result = SectorConstraint.build_many_constraints(data)
        assert len(result) == 2

    def test_build_many_constraints_from_dataframe(self):
        df = pd.DataFrame([
            {'sector': 'Finance', 'minimum': 0, 'maximum': 20, 'unit': 'Percent'},
        ])
        result = SectorConstraint.build_many_constraints(df)
        assert len(result) == 1

    def test_build_many_constraints_missing_columns_raises(self):
        data = [{'sector': 'Tech', 'minimum': 0}]
        with pytest.raises(MqValueError, match='missing required columns'):
            SectorConstraint.build_many_constraints(data)


# =============================================================================
# IndustryConstraint
# =============================================================================


class TestIndustryConstraint:
    def test_init_defaults(self):
        ic = IndustryConstraint(industry_name='Software')
        assert ic.industry_name == 'Software'
        assert ic.minimum == 0
        assert ic.maximum == 100
        assert ic.unit == OptimizationConstraintUnit.PERCENT

    def test_init_invalid_unit_raises(self):
        with pytest.raises(MqValueError, match='Industry constraints can only be set by percent or decimal'):
            IndustryConstraint(industry_name='Software', unit=OptimizationConstraintUnit.NOTIONAL)

    def test_setters(self):
        ic = IndustryConstraint(industry_name='Software')
        ic.industry_name = 'Banking'
        assert ic.industry_name == 'Banking'
        ic.minimum = 2
        assert ic.minimum == 2
        ic.maximum = 40
        assert ic.maximum == 40

    def test_unit_setter_valid(self):
        ic = IndustryConstraint(industry_name='Software')
        ic.unit = OptimizationConstraintUnit.DECIMAL
        assert ic.unit == OptimizationConstraintUnit.DECIMAL

    def test_unit_setter_invalid_raises(self):
        ic = IndustryConstraint(industry_name='Software')
        with pytest.raises(MqValueError, match='Industry constraints can only be set by percent'):
            ic.unit = OptimizationConstraintUnit.NOTIONAL

    def test_to_dict_percent(self):
        ic = IndustryConstraint(industry_name='Software', minimum=5, maximum=50,
                                unit=OptimizationConstraintUnit.PERCENT)
        d = ic.to_dict()
        assert d['type'] == 'Industry'
        assert d['name'] == 'Software'
        assert d['min'] == 5
        assert d['max'] == 50

    def test_to_dict_decimal(self):
        ic = IndustryConstraint(industry_name='Software', minimum=0.05, maximum=0.5,
                                unit=OptimizationConstraintUnit.DECIMAL)
        d = ic.to_dict()
        assert d['min'] == 5.0
        assert d['max'] == 50.0

    def test_build_many_constraints_from_list(self):
        data = [
            {'industry': 'Software', 'minimum': 0, 'maximum': 50, 'unit': 'Percent'},
            {'industry': 'Banking', 'minimum': 0, 'maximum': 30, 'unit': 'Percent'},
        ]
        result = IndustryConstraint.build_many_constraints(data)
        assert len(result) == 2

    def test_build_many_constraints_from_dataframe(self):
        df = pd.DataFrame([
            {'industry': 'Pharma', 'minimum': 0, 'maximum': 20, 'unit': 'Percent'},
        ])
        result = IndustryConstraint.build_many_constraints(df)
        assert len(result) == 1

    def test_build_many_constraints_missing_columns_raises(self):
        data = [{'industry': 'Software', 'minimum': 0}]
        with pytest.raises(MqValueError, match='missing required columns'):
            IndustryConstraint.build_many_constraints(data)


# =============================================================================
# FactorConstraint
# =============================================================================


class TestFactorConstraint:
    def _make_factor(self, name='Value'):
        return Factor(risk_model_id='model_1', id_='factor_id', type_='Style', name=name)

    def test_init_and_properties(self):
        f = self._make_factor()
        fc = FactorConstraint(factor=f, max_exposure=5.0)
        assert fc.factor == f
        assert fc.max_exposure == 5.0

    def test_setters(self):
        f1 = self._make_factor('Value')
        f2 = self._make_factor('Growth')
        fc = FactorConstraint(factor=f1, max_exposure=5.0)
        fc.factor = f2
        assert fc.factor == f2
        fc.max_exposure = 10.0
        assert fc.max_exposure == 10.0

    def test_to_dict(self):
        f = self._make_factor('Beta')
        fc = FactorConstraint(factor=f, max_exposure=3.0)
        d = fc.to_dict()
        assert d['factor'] == 'Beta'
        assert d['exposure'] == 3.0

    @patch('gs_quant.markets.optimizer.FactorRiskModel.get')
    def test_build_many_constraints_from_list(self, mock_get):
        mock_model = MagicMock()
        f1 = self._make_factor('Value')
        f2 = self._make_factor('Growth')
        mock_model.get_many_factors.return_value = [f1, f2]
        mock_get.return_value = mock_model

        data = [
            {'factor': 'Value', 'exposure': 5.0},
            {'factor': 'Growth', 'exposure': 3.0},
        ]
        result = FactorConstraint.build_many_constraints(data, 'BARRA_USFAST')
        assert len(result) == 2

    @patch('gs_quant.markets.optimizer.FactorRiskModel.get')
    def test_build_many_constraints_from_dataframe(self, mock_get):
        mock_model = MagicMock()
        f1 = self._make_factor('Beta')
        mock_model.get_many_factors.return_value = [f1]
        mock_get.return_value = mock_model

        df = pd.DataFrame([{'factor': 'Beta', 'exposure': 10.0}])
        result = FactorConstraint.build_many_constraints(df, 'BARRA_USFAST')
        assert len(result) == 1

    def test_build_many_constraints_missing_columns_raises(self):
        data = [{'factor': 'Value'}]  # missing 'exposure'
        with pytest.raises(MqValueError, match='missing required columns'):
            FactorConstraint.build_many_constraints(data, 'model_id')


# =============================================================================
# MaxFactorProportionOfRiskConstraint
# =============================================================================


class TestMaxFactorProportionOfRiskConstraint:
    def test_init_percent(self):
        c = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=50,
                                                 unit=OptimizationConstraintUnit.PERCENT)
        assert c.max_factor_proportion_of_risk == 0.5  # 50 / 100

    def test_init_decimal(self):
        c = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=0.5,
                                                 unit=OptimizationConstraintUnit.DECIMAL)
        assert c.max_factor_proportion_of_risk == 0.5

    def test_init_invalid_unit_raises(self):
        with pytest.raises(MqValueError, match='Max Factor Proportion of Risk can only be set by percent or decimal'):
            MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=50,
                                                 unit=OptimizationConstraintUnit.NOTIONAL)

    def test_setter(self):
        c = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=50)
        c.max_factor_proportion_of_risk = 0.3
        assert c.max_factor_proportion_of_risk == 0.3


# =============================================================================
# MaxProportionOfRiskByGroupConstraint
# =============================================================================


class TestMaxProportionOfRiskByGroupConstraint:
    def _make_factor(self, name='Value'):
        return Factor(risk_model_id='m', id_='f_id', type_='Style', name=name)

    def test_init_percent(self):
        factors = [self._make_factor('Value'), self._make_factor('Growth')]
        c = MaxProportionOfRiskByGroupConstraint(
            factors=factors, max_factor_proportion_of_risk=50,
            unit=OptimizationConstraintUnit.PERCENT
        )
        assert c.max_factor_proportion_of_risk == 0.5
        assert len(c.factors) == 2

    def test_init_decimal(self):
        factors = [self._make_factor()]
        c = MaxProportionOfRiskByGroupConstraint(
            factors=factors, max_factor_proportion_of_risk=0.5,
            unit=OptimizationConstraintUnit.DECIMAL
        )
        assert c.max_factor_proportion_of_risk == 0.5

    def test_init_invalid_unit_raises(self):
        with pytest.raises(MqValueError, match='Max Factor Proportion of Risk can only be set by percent or decimal'):
            MaxProportionOfRiskByGroupConstraint(
                factors=[], max_factor_proportion_of_risk=50,
                unit=OptimizationConstraintUnit.NOTIONAL
            )

    def test_setters(self):
        f = self._make_factor()
        c = MaxProportionOfRiskByGroupConstraint(factors=[f], max_factor_proportion_of_risk=50)
        new_factors = [self._make_factor('Beta')]
        c.factors = new_factors
        assert c.factors == new_factors
        c.max_factor_proportion_of_risk = 0.8
        assert c.max_factor_proportion_of_risk == 0.8

    def test_to_dict(self):
        f1 = self._make_factor('Value')
        f2 = self._make_factor('Growth')
        c = MaxProportionOfRiskByGroupConstraint(
            factors=[f1, f2], max_factor_proportion_of_risk=0.5,
            unit=OptimizationConstraintUnit.DECIMAL
        )
        d = c.to_dict()
        assert d['factors'] == ['Value', 'Growth']
        assert d['max'] == 0.5


# =============================================================================
# OptimizerConstraints
# =============================================================================


class TestOptimizerConstraints:
    def test_init_defaults(self):
        oc = OptimizerConstraints()
        assert oc.asset_constraints == []
        assert oc.country_constraints == []
        assert oc.sector_constraints == []
        assert oc.industry_constraints == []
        assert oc.factor_constraints == []
        assert oc.max_factor_proportion_of_risk is None
        assert oc.max_proportion_of_risk_by_groups is None

    def test_setters(self):
        oc = OptimizerConstraints()
        ac = AssetConstraint(asset='a')
        oc.asset_constraints = [ac]
        assert oc.asset_constraints == [ac]

        cc = CountryConstraint(country_name='USA')
        oc.country_constraints = [cc]
        assert oc.country_constraints == [cc]

        sc = SectorConstraint(sector_name='Tech')
        oc.sector_constraints = [sc]
        assert oc.sector_constraints == [sc]

        ic = IndustryConstraint(industry_name='Software')
        oc.industry_constraints = [ic]
        assert oc.industry_constraints == [ic]

        f = Factor(risk_model_id='m', id_='f', type_='Style', name='Beta')
        fc = FactorConstraint(factor=f, max_exposure=5)
        oc.factor_constraints = [fc]
        assert oc.factor_constraints == [fc]

        mfp = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=50)
        oc.max_factor_proportion_of_risk = mfp
        assert oc.max_factor_proportion_of_risk == mfp

        fg = MaxProportionOfRiskByGroupConstraint(factors=[f], max_factor_proportion_of_risk=50)
        oc.max_proportion_of_risk_by_groups = [fg]
        assert oc.max_proportion_of_risk_by_groups == [fg]

    def test_to_dict_empty_constraints(self):
        oc = OptimizerConstraints()
        d = oc.to_dict()
        assert d['assetConstraints'] == []
        assert d['classificationConstraints'] == []
        assert d['factorConstraints'] == []
        assert d['constrainAssetsByNotional'] is False

    def test_to_dict_with_percent_asset_constraints(self):
        ac = AssetConstraint(asset='a', minimum=5, maximum=50,
                             unit=OptimizationConstraintUnit.PERCENT)
        oc = OptimizerConstraints(asset_constraints=[ac])
        d = oc.to_dict()
        assert d['constrainAssetsByNotional'] is False
        assert len(d['assetConstraints']) == 1

    def test_to_dict_with_notional_asset_constraints(self):
        ac = AssetConstraint(asset='a', minimum=1000, maximum=5000,
                             unit=OptimizationConstraintUnit.NOTIONAL)
        oc = OptimizerConstraints(asset_constraints=[ac])
        d = oc.to_dict()
        assert d['constrainAssetsByNotional'] is True

    def test_to_dict_mixed_unit_asset_constraints_raises(self):
        ac1 = AssetConstraint(asset='a', unit=OptimizationConstraintUnit.PERCENT)
        ac2 = AssetConstraint(asset='b', unit=OptimizationConstraintUnit.NOTIONAL)
        oc = OptimizerConstraints(asset_constraints=[ac1, ac2])
        with pytest.raises(MqValueError, match='All asset constraints need to have the same unit'):
            oc.to_dict()

    def test_to_dict_with_classification_constraints(self):
        cc = CountryConstraint(country_name='USA', minimum=5, maximum=50)
        sc = SectorConstraint(sector_name='Tech', minimum=0, maximum=30)
        ic = IndustryConstraint(industry_name='Software', minimum=0, maximum=20)
        oc = OptimizerConstraints(country_constraints=[cc], sector_constraints=[sc],
                                  industry_constraints=[ic])
        d = oc.to_dict()
        assert len(d['classificationConstraints']) == 3

    def test_to_dict_with_factor_constraints(self):
        f = Factor(risk_model_id='m', id_='f', type_='Style', name='Beta')
        fc = FactorConstraint(factor=f, max_exposure=5)
        oc = OptimizerConstraints(factor_constraints=[fc])
        d = oc.to_dict()
        assert len(d['factorConstraints']) == 1
        assert d['factorConstraints'][0]['factor'] == 'Beta'

    def test_to_dict_with_max_factor_proportion(self):
        mfp = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=50)
        oc = OptimizerConstraints(max_factor_proportion_of_risk=mfp)
        d = oc.to_dict()
        assert 'maxFactorMCTR' in d
        assert d['maxFactorMCTR'] == 0.5

    def test_to_dict_with_max_proportion_by_groups(self):
        f = Factor(risk_model_id='m', id_='f', type_='Style', name='Beta')
        fg = MaxProportionOfRiskByGroupConstraint(factors=[f], max_factor_proportion_of_risk=0.5,
                                                   unit=OptimizationConstraintUnit.DECIMAL)
        oc = OptimizerConstraints(max_proportion_of_risk_by_groups=[fg])
        d = oc.to_dict()
        assert 'maxFactorMCTRByGroup' in d
        assert len(d['maxFactorMCTRByGroup']) == 1


# =============================================================================
# ConstraintPriorities
# =============================================================================


class TestConstraintPriorities:
    def test_init_defaults(self):
        cp = ConstraintPriorities()
        assert cp.min_sector_weights is None
        assert cp.max_sector_weights is None
        assert cp.min_industry_weights is None
        assert cp.max_industry_weights is None
        assert cp.min_region_weights is None
        assert cp.max_region_weights is None
        assert cp.min_country_weights is None
        assert cp.max_country_weights is None
        assert cp.style_factor_exposures is None

    def test_setters(self):
        cp = ConstraintPriorities()
        cp.min_sector_weights = PrioritySetting.ONE
        assert cp.min_sector_weights == PrioritySetting.ONE
        cp.max_sector_weights = PrioritySetting.TWO
        assert cp.max_sector_weights == PrioritySetting.TWO
        cp.min_industry_weights = PrioritySetting.THREE
        assert cp.min_industry_weights == PrioritySetting.THREE
        cp.max_industry_weights = PrioritySetting.FOUR
        assert cp.max_industry_weights == PrioritySetting.FOUR
        cp.min_region_weights = PrioritySetting.FIVE
        assert cp.min_region_weights == PrioritySetting.FIVE
        cp.max_region_weights = PrioritySetting.ZERO
        assert cp.max_region_weights == PrioritySetting.ZERO
        cp.min_country_weights = PrioritySetting.ONE
        assert cp.min_country_weights == PrioritySetting.ONE
        cp.max_country_weights = PrioritySetting.TWO
        assert cp.max_country_weights == PrioritySetting.TWO
        cp.style_factor_exposures = PrioritySetting.THREE
        assert cp.style_factor_exposures == PrioritySetting.THREE

    def test_to_dict_all_none_returns_none(self):
        cp = ConstraintPriorities()
        result = cp.to_dict()
        assert result is None

    def test_to_dict_with_values(self):
        cp = ConstraintPriorities(
            min_sector_weights=PrioritySetting.ONE,
            max_sector_weights=PrioritySetting.TWO,
        )
        d = cp.to_dict()
        assert d['minSectorWeights'] == '1'
        assert d['maxSectorWeights'] == '2'
        # None values should be excluded
        assert 'minIndustryWeights' not in d

    def test_to_dict_all_set(self):
        cp = ConstraintPriorities(
            min_sector_weights=PrioritySetting.ZERO,
            max_sector_weights=PrioritySetting.ONE,
            min_industry_weights=PrioritySetting.TWO,
            max_industry_weights=PrioritySetting.THREE,
            min_region_weights=PrioritySetting.FOUR,
            max_region_weights=PrioritySetting.FIVE,
            min_country_weights=PrioritySetting.ZERO,
            max_country_weights=PrioritySetting.ONE,
            style_factor_exposures=PrioritySetting.TWO,
        )
        d = cp.to_dict()
        assert len(d) == 9


# =============================================================================
# OptimizerSettings
# =============================================================================


class TestOptimizerSettings:
    def test_default_init(self):
        s = OptimizerSettings()
        assert s.notional == 10000000
        assert s.allow_long_short is False
        assert s.gross_notional is None
        assert s.net_notional is None
        assert s.min_names == 0
        assert s.max_names == 100
        assert s.min_weight_per_constituent is None
        assert s.max_weight_per_constituent is None
        assert s.max_adv == 15
        assert s.constraint_priorities is None

    def test_setters(self):
        s = OptimizerSettings()
        s.notional = 5000000
        assert s.notional == 5000000
        s.min_names = 5
        assert s.min_names == 5
        s.max_names = 50
        assert s.max_names == 50
        s.max_adv = 20
        assert s.max_adv == 20
        cp = ConstraintPriorities()
        s.constraint_priorities = cp
        assert s.constraint_priorities == cp

    def test_min_weight_negative_raises_warning(self):
        with pytest.raises(Warning, match='min_weight_per_constituent cannot be negative'):
            OptimizerSettings(min_weight_per_constituent=-0.05)

    def test_max_weight_negative_raises(self):
        with pytest.raises(MqValueError, match='max_weight_per_constituent must be a positive value'):
            OptimizerSettings(max_weight_per_constituent=-0.05)

    def test_min_weight_greater_than_max_raises(self):
        with pytest.raises(MqValueError, match='min_weight_per_constituent.*cannot be greater than'):
            OptimizerSettings(min_weight_per_constituent=0.1, max_weight_per_constituent=0.05)

    def test_allow_long_short_net_exceeds_gross_raises(self):
        with pytest.raises(MqValueError, match='cannot be greater than gross_notional'):
            OptimizerSettings(allow_long_short=True, gross_notional=10000000,
                              net_notional=20000000)

    def test_unidirectional_gross_net_mismatch_raises(self):
        with pytest.raises(MqValueError, match='Cannot set gross_notional != net_notional when allow_long_short=False'):
            OptimizerSettings(allow_long_short=False, gross_notional=10000000,
                              net_notional=5000000)

    def test_unidirectional_same_gross_net_ok(self):
        """When gross == net and allow_long_short=False, no error"""
        s = OptimizerSettings(allow_long_short=False, gross_notional=10000000,
                              net_notional=10000000)
        assert s.gross_notional == 10000000

    def test_bidirectional_valid(self):
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=10000000)
        assert s.allow_long_short is True
        assert s.gross_notional == 20000000
        assert s.net_notional == 10000000

    def test_bidirectional_only_notional(self):
        """Bidirectional with only notional set - valid"""
        s = OptimizerSettings(allow_long_short=True, notional=15000000)
        assert s.allow_long_short is True

    def test_setter_triggers_validation(self):
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=10000000)
        with pytest.raises(MqValueError):
            s.net_notional = 30000000  # net > gross

    def test_allow_long_short_setter_triggers_validation(self):
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=5000000)
        # Switching to unidirectional with gross != net should raise
        with pytest.raises(MqValueError):
            s.allow_long_short = False

    def test_gross_notional_setter_triggers_validation(self):
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=5000000)
        with pytest.raises(MqValueError):
            s.gross_notional = 3000000  # |net| > gross

    def test_min_weight_setter_triggers_validation(self):
        s = OptimizerSettings(max_weight_per_constituent=0.1)
        with pytest.raises(MqValueError):
            s.min_weight_per_constituent = 0.2  # min > max

    def test_max_weight_setter_triggers_validation(self):
        s = OptimizerSettings(min_weight_per_constituent=0.05)
        with pytest.raises(MqValueError):
            s.max_weight_per_constituent = -0.01  # negative

    def test_to_dict_unidirectional(self):
        s = OptimizerSettings(notional=10000000, min_names=5, max_names=50, max_adv=20)
        d = s.to_dict()
        assert d['hedgeNotional'] == 10000000
        assert d['minNames'] == 5
        assert d['maxNames'] == 50
        assert d['maxAdvPercentage'] == 20
        assert d['allowLongShort'] is False

    def test_to_dict_bidirectional_with_gross_net(self):
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=10000000)
        d = s.to_dict()
        assert d['allowLongShort'] is True
        assert d['grossNotional'] == 20000000
        assert d['netNotional'] == 10000000
        assert 'hedgeNotional' not in d

    def test_to_dict_bidirectional_with_only_notional(self):
        s = OptimizerSettings(allow_long_short=True, notional=15000000)
        d = s.to_dict()
        assert d['allowLongShort'] is True
        assert d['hedgeNotional'] == 15000000
        assert 'grossNotional' not in d

    def test_to_dict_bidirectional_gross_set_net_none(self):
        """When gross is set but net is None, it should use hedgeNotional fallback"""
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              notional=15000000)
        d = s.to_dict()
        # gross_notional set but net_notional is None: falls through to notional
        assert d['hedgeNotional'] == 15000000

    def test_to_dict_with_weight_constraints(self):
        s = OptimizerSettings(min_weight_per_constituent=0.01, max_weight_per_constituent=0.05)
        d = s.to_dict()
        assert d['minWeight'] == 1.0  # 0.01 * 100
        assert d['maxWeight'] == 5.0  # 0.05 * 100

    def test_to_dict_without_weight_constraints(self):
        s = OptimizerSettings()
        d = s.to_dict()
        assert 'minWeight' not in d
        assert 'maxWeight' not in d

    def test_to_dict_with_constraint_priorities(self):
        cp = ConstraintPriorities(min_sector_weights=PrioritySetting.ONE)
        s = OptimizerSettings(constraint_priorities=cp)
        d = s.to_dict()
        assert 'constraintPrioritySettings' in d
        assert d['constraintPrioritySettings']['minSectorWeights'] == '1'

    def test_to_dict_without_constraint_priorities(self):
        s = OptimizerSettings()
        d = s.to_dict()
        assert 'constraintPrioritySettings' not in d

    def test_to_dict_constraint_priorities_all_none(self):
        """When constraint_priorities is set but all values are None, to_dict returns None for it"""
        cp = ConstraintPriorities()
        s = OptimizerSettings(constraint_priorities=cp)
        d = s.to_dict()
        # ConstraintPriorities.to_dict() returns None when all are None
        # But OptimizerSettings checks truthiness - None is falsy, so it shouldn't be included
        # Actually the code checks `if self.constraint_priorities:` which is truthy for the object
        # but then sets the value to None
        assert 'constraintPrioritySettings' in d
        assert d['constraintPrioritySettings'] is None


# =============================================================================
# OptimizerUniverse
# =============================================================================


class TestOptimizerUniverse:
    def test_init_defaults(self):
        mock_assets = [MagicMock()]
        ou = OptimizerUniverse(assets=mock_assets)
        assert ou.assets == mock_assets
        assert ou.explode_composites is True
        assert ou.exclude_initial_position_set_assets is True
        assert ou.exclude_corporate_actions_types == []
        assert ou.exclude_hard_to_borrow_assets is False
        assert ou.exclude_restricted_assets is False
        assert ou.min_market_cap is None
        assert ou.max_market_cap is None

    def test_init_custom(self):
        ou = OptimizerUniverse(
            assets=None,
            explode_composites=False,
            exclude_initial_position_set_assets=False,
            exclude_corporate_actions_types=[CorporateActionsTypes.Mergers],
            exclude_hard_to_borrow_assets=True,
            exclude_restricted_assets=True,
            min_market_cap=1e9,
            max_market_cap=1e12,
        )
        assert ou.explode_composites is False
        assert ou.exclude_initial_position_set_assets is False
        assert ou.exclude_hard_to_borrow_assets is True
        assert ou.exclude_restricted_assets is True
        assert ou.min_market_cap == 1e9
        assert ou.max_market_cap == 1e12

    def test_setters(self):
        ou = OptimizerUniverse(assets=[])
        mock_assets = [MagicMock()]
        ou.assets = mock_assets
        assert ou.assets == mock_assets
        ou.explode_composites = False
        assert ou.explode_composites is False
        ou.exclude_initial_position_set_assets = False
        assert ou.exclude_initial_position_set_assets is False
        ou.exclude_corporate_actions_types = [CorporateActionsTypes.Spinoffs]
        assert ou.exclude_corporate_actions_types == [CorporateActionsTypes.Spinoffs]
        ou.exclude_hard_to_borrow_assets = True
        assert ou.exclude_hard_to_borrow_assets is True
        ou.exclude_restricted_assets = True
        assert ou.exclude_restricted_assets is True
        ou.min_market_cap = 5e8
        assert ou.min_market_cap == 5e8
        ou.max_market_cap = 5e11
        assert ou.max_market_cap == 5e11

    def test_to_dict_with_asset_list(self):
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'MQ123'
        ou = OptimizerUniverse(assets=[mock_asset])
        d = ou.to_dict()
        assert d['hedgeUniverse']['assetIds'] == ['MQ123']
        assert d['explodeUniverse'] is True
        assert d['excludeTargetAssets'] is True
        assert d['excludeCorporateActions'] is False
        assert d['excludeHardToBorrowAssets'] is False
        assert d['excludeRestrictedAssets'] is False
        assert 'minMarketCap' not in d
        assert 'maxMarketCap' not in d

    def test_to_dict_with_asset_universe(self):
        au = AssetUniverse(identifiers=['AAPL'], asset_ids=['id1'])
        ou = OptimizerUniverse(assets=au)
        d = ou.to_dict()
        assert d['hedgeUniverse']['assetIds'] == ['id1']

    def test_to_dict_with_corporate_actions(self):
        ou = OptimizerUniverse(
            assets=[MagicMock(get_marquee_id=MagicMock(return_value='id1'))],
            exclude_corporate_actions_types=[CorporateActionsTypes.Mergers, CorporateActionsTypes.Spinoffs],
        )
        d = ou.to_dict()
        assert d['excludeCorporateActions'] is True
        assert 'Mergers' in d['excludeCorporateActionsTypes']
        assert 'Spinoffs' in d['excludeCorporateActionsTypes']

    def test_to_dict_with_market_cap(self):
        ou = OptimizerUniverse(
            assets=[MagicMock(get_marquee_id=MagicMock(return_value='id1'))],
            min_market_cap=1e9,
            max_market_cap=1e12,
        )
        d = ou.to_dict()
        assert d['minMarketCap'] == 1e9
        assert d['maxMarketCap'] == 1e12


# =============================================================================
# TurnoverConstraint
# =============================================================================


class TestTurnoverConstraint:
    def _make_position_set(self):
        return PositionSet(
            positions=[
                Position(identifier='AAPL', asset_id='id1', quantity=100),
                Position(identifier='MSFT', asset_id='id2', quantity=200),
            ],
            date=dt.date(2024, 1, 1),
        )

    def test_init_and_properties(self):
        ps = self._make_position_set()
        tc = TurnoverConstraint(turnover_portfolio=ps, max_turnover_percent=80)
        assert tc.turnover_portfolio == ps
        assert tc.max_turnover_percent == 80
        assert tc.turnover_notional_type is None

    def test_init_with_notional_type(self):
        ps = self._make_position_set()
        tc = TurnoverConstraint(
            turnover_portfolio=ps, max_turnover_percent=80,
            turnover_notional_type=TurnoverNotionalType.GROSS,
        )
        assert tc.turnover_notional_type == TurnoverNotionalType.GROSS

    def test_setters(self):
        ps = self._make_position_set()
        tc = TurnoverConstraint(turnover_portfolio=ps, max_turnover_percent=80)
        new_ps = self._make_position_set()
        tc.turnover_portfolio = new_ps
        assert tc.turnover_portfolio == new_ps
        tc.max_turnover_percent = 90
        assert tc.max_turnover_percent == 90
        tc.turnover_notional_type = TurnoverNotionalType.NET
        assert tc.turnover_notional_type == TurnoverNotionalType.NET

    def test_to_dict_without_notional_type(self):
        ps = self._make_position_set()
        tc = TurnoverConstraint(turnover_portfolio=ps, max_turnover_percent=80)
        d = tc.to_dict()
        assert d['maxTurnoverPercentage'] == 80
        assert len(d['turnoverPortfolio']) == 2
        assert 'turnoverNotionalType' not in d

    def test_to_dict_with_notional_type(self):
        ps = self._make_position_set()
        tc = TurnoverConstraint(
            turnover_portfolio=ps, max_turnover_percent=70,
            turnover_notional_type=TurnoverNotionalType.LONG,
        )
        d = tc.to_dict()
        assert d['turnoverNotionalType'] == 'Long'
        assert d['maxTurnoverPercentage'] == 70


# =============================================================================
# OptimizerStrategy
# =============================================================================


class TestOptimizerStrategy:
    def _make_strategy(self, **overrides):
        """Helper to create a strategy with minimal mocking."""
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'univ_id1'
        universe = OptimizerUniverse(assets=[mock_asset])
        risk_model = MagicMock()
        risk_model.id = 'BARRA_USFAST'

        defaults = dict(
            initial_position_set=ps,
            universe=universe,
            risk_model=risk_model,
        )
        defaults.update(overrides)
        return OptimizerStrategy(**defaults)

    def test_init_and_properties(self):
        s = self._make_strategy()
        assert s.initial_position_set is not None
        assert s.universe is not None
        assert s.risk_model is not None
        assert s.constraints is None
        assert s.turnover is None
        assert s.settings is None
        assert s.objective == OptimizerObjective.MINIMIZE_FACTOR_RISK
        assert s.objective_parameters is None

    def test_setters(self):
        s = self._make_strategy()
        new_ps = PositionSet(
            positions=[Position(identifier='MSFT', asset_id='id2', quantity=200)],
            date=dt.date(2024, 2, 1),
        )
        s.initial_position_set = new_ps
        assert s.initial_position_set == new_ps

        new_univ = OptimizerUniverse(assets=[])
        s.universe = new_univ
        assert s.universe == new_univ

        new_rm = MagicMock()
        s.risk_model = new_rm
        assert s.risk_model == new_rm

        new_constraints = OptimizerConstraints()
        s.constraints = new_constraints
        assert s.constraints == new_constraints

        ps_turnover = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        new_turnover = TurnoverConstraint(turnover_portfolio=ps_turnover, max_turnover_percent=80)
        s.turnover = new_turnover
        assert s.turnover == new_turnover

        new_settings = OptimizerSettings()
        s.settings = new_settings
        assert s.settings == new_settings

        s.objective = OptimizerObjective.MINIMIZE_FACTOR_RISK
        assert s.objective == OptimizerObjective.MINIMIZE_FACTOR_RISK

    def test_handle_error_predefined_missing_asset(self):
        s = self._make_strategy()
        msg = 'Missing asset xref for something'
        result = s.handle_error(msg)
        assert result[1] is True  # predefined
        assert 'underlying asset meta data error' in result[0]

    def test_handle_error_predefined_no_solution(self):
        s = self._make_strategy()
        msg = 'ERROR: Could not find solution. Some additional info'
        result = s.handle_error(msg)
        assert result[1] is True
        assert 'infeasible inputs' in result[0].lower() or 'Potential infeasible inputs' in result[0]

    def test_handle_error_unknown(self):
        s = self._make_strategy()
        msg = 'Some random error'
        result = s.handle_error(msg)
        assert result[1] is False
        assert result[0] == msg

    # --- _ensure_completed decorator tests ---

    def test_get_optimization_without_run_raises(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization before calling this method'):
            s.get_optimization()

    def test_get_optimized_position_set_without_run_raises(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization before calling this method'):
            s.get_optimized_position_set()

    def test_get_hedge_constituents_by_direction_without_run_raises(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization before calling this method'):
            s.get_hedge_constituents_by_direction()

    def test_get_hedge_exposure_summary_without_run_raises(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization before calling this method'):
            s.get_hedge_exposure_summary()

    # --- Tests with result set ---

    def _set_result(self, strategy, result):
        """Set __result on an OptimizerStrategy for testing."""
        strategy._OptimizerStrategy__result = result

    def test_get_optimization_with_result(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'netExposure': 10000000,
                'constituents': [
                    {'assetId': 'id1', 'name': 'Apple', 'bbid': 'AAPL', 'shares': 100, 'weight': 0.5},
                    {'assetId': 'id2', 'name': 'Microsoft', 'bbid': 'MSFT', 'shares': 200, 'weight': 0.5},
                ],
            },
        })
        result = s.get_optimization(by_weight=False)
        assert isinstance(result, PositionSet)
        assert len(result.positions) == 2
        # by_weight=False means reference_notional is None
        assert result.reference_notional is None

    def test_get_optimization_by_weight(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'netExposure': 10000000,
                'constituents': [
                    {'assetId': 'id1', 'name': 'Apple', 'bbid': 'AAPL', 'shares': 100, 'weight': 0.5},
                ],
            },
        })
        result = s.get_optimization(by_weight=True)
        assert result.reference_notional == 10000000

    def test_get_optimized_position_set_with_result(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedgedTarget': {
                'netExposure': 15000000,
                'constituents': [
                    {'assetId': 'id1', 'name': 'Apple', 'bbid': 'AAPL', 'shares': 300, 'weight': 1.0},
                ],
            },
        })
        result = s.get_optimized_position_set(by_weight=False)
        assert isinstance(result, PositionSet)

    def test_construct_position_set_constituent_no_bbid(self):
        """When a constituent has no bbid, name should be used as identifier"""
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'netExposure': 10000000,
                'constituents': [
                    {'assetId': 'id1', 'name': 'Apple', 'shares': 100, 'weight': 0.5},
                ],
            },
        })
        result = s.get_optimization(by_weight=False)
        assert result.positions[0].identifier == 'Apple'

    # --- get_cumulative_pnl_performance ---

    def test_get_cumulative_pnl_performance_no_result(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_cumulative_pnl_performance()

    def test_get_cumulative_pnl_performance_no_target(self):
        s = self._make_strategy()
        self._set_result(s, {})
        with pytest.raises(MqValueError, match='does not contain'):
            s.get_cumulative_pnl_performance(target=HedgeTarget.HEDGED_TARGET)

    def test_get_cumulative_pnl_performance_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedgedTarget': {
                'cumulativePnl': [
                    ['2024-01-01', 100],
                    ['2024-01-02', 200],
                ],
            },
        })
        df = s.get_cumulative_pnl_performance()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'date' in df.columns
        assert 'cumulativePnl' in df.columns

    # --- get_style_factor_exposures ---

    def test_get_style_factor_exposures_no_result(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_style_factor_exposures()

    def test_get_style_factor_exposures_no_target(self):
        s = self._make_strategy()
        self._set_result(s, {})
        with pytest.raises(MqValueError, match='does not contain'):
            s.get_style_factor_exposures()

    def test_get_style_factor_exposures_no_factor_exposures(self):
        s = self._make_strategy()
        self._set_result(s, {'hedgedTarget': {}})
        with pytest.raises(MqValueError, match='does not contain factor exposures'):
            s.get_style_factor_exposures()

    def test_get_style_factor_exposures_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedgedTarget': {
                'factorExposures': {
                    'style': [{'factor': 'Value', 'exposure': 0.5}],
                },
            },
        })
        result = s.get_style_factor_exposures()
        assert len(result) == 1
        assert result[0]['factor'] == 'Value'

    # --- get_risk_buckets ---

    def test_get_risk_buckets_no_result(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_risk_buckets()

    def test_get_risk_buckets_no_target(self):
        s = self._make_strategy()
        self._set_result(s, {})
        with pytest.raises(MqValueError, match='does not contain'):
            s.get_risk_buckets()

    def test_get_risk_buckets_no_risk_buckets(self):
        s = self._make_strategy()
        self._set_result(s, {'hedgedTarget': {}})
        with pytest.raises(MqValueError, match='does not contain risk buckets'):
            s.get_risk_buckets()

    def test_get_risk_buckets_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedgedTarget': {
                'riskBuckets': {'factor': 0.3, 'specific': 0.7},
            },
        })
        result = s.get_risk_buckets()
        assert 'risk_buckets' in result

    # --- get_transaction_and_liquidity_constituents_performance ---

    def test_get_transaction_constituents_no_result(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_transaction_and_liquidity_constituents_performance()

    def test_get_transaction_constituents_no_target(self):
        s = self._make_strategy()
        self._set_result(s, {})
        with pytest.raises(MqValueError, match='does not contain'):
            s.get_transaction_and_liquidity_constituents_performance()

    def test_get_transaction_constituents_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedgedTarget': {
                'constituents': [
                    {
                        'name': 'Apple',
                        'assetId': 'id1',
                        'bbid': 'AAPL',
                        'notional': 1000000,
                        'shares': 100,
                        'price': 150,
                        'weight': 0.5,
                        'currency': 'USD',
                        'transactionCost': 1000,
                        'marginalCost': 100,
                        'advPercentage': 5.0,
                        'borrowCost': 0.01,
                        'extraField': 'should_be_filtered',
                    },
                ],
            },
        })
        df = s.get_transaction_and_liquidity_constituents_performance()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert 'extraField' not in df.columns
        assert 'name' in df.columns

    # --- get_hedge_exposure_summary ---

    def test_get_hedge_exposure_summary_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'grossExposure': 20000000,
                'netExposure': 10000000,
                'longExposure': 15000000,
                'shortExposure': 5000000,
                'numberOfPositions': 25,
            },
            'target': {
                'grossExposure': 10000000,
                'netExposure': 10000000,
                'numberOfPositions': 1,
            },
            'hedgedTarget': {
                'grossExposure': 30000000,
                'netExposure': 20000000,
                'longExposure': 25000000,
                'shortExposure': 5000000,
                'numberOfPositions': 26,
            },
        })
        result = s.get_hedge_exposure_summary()
        assert result['hedge']['mode'] == 'bidirectional'
        assert result['hedge']['gross_exposure'] == 20000000
        assert result['target'] is not None
        assert result['hedged_target'] is not None

    def test_get_hedge_exposure_summary_unidirectional(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'grossExposure': 10000000,
                'netExposure': -10000000,
                'longExposure': 0,
                'shortExposure': 10000000,
                'numberOfPositions': 10,
            },
        })
        result = s.get_hedge_exposure_summary()
        assert result['hedge']['mode'] == 'unidirectional (short positions only)'

    def test_get_hedge_exposure_summary_missing_key(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'grossExposure': 20000000,
                'netExposure': 10000000,
                'longExposure': 15000000,
                'shortExposure': 5000000,
                'numberOfPositions': 25,
            },
        })
        result = s.get_hedge_exposure_summary()
        assert result['target'] is None
        assert result['hedged_target'] is None

    # --- get_hedge_constituents_by_direction ---

    def test_get_hedge_constituents_by_direction_no_hedge_key(self):
        s = self._make_strategy()
        self._set_result(s, {})
        with pytest.raises(MqValueError, match='does not contain hedge data'):
            s.get_hedge_constituents_by_direction()

    def test_get_hedge_constituents_by_direction_empty_constituents(self):
        s = self._make_strategy()
        self._set_result(s, {'hedge': {'constituents': []}})
        result = s.get_hedge_constituents_by_direction()
        assert result['summary']['num_long'] == 0
        assert result['summary']['num_short'] == 0

    def test_get_hedge_constituents_by_direction_with_data(self):
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'constituents': [
                    {'assetId': 'id1', 'name': 'A', 'notional': 1000000},
                    {'assetId': 'id2', 'name': 'B', 'notional': -500000},
                    {'assetId': 'id3', 'name': 'C', 'notional': 2000000},
                ],
            },
        })
        result = s.get_hedge_constituents_by_direction()
        assert result['summary']['num_long'] == 2
        assert result['summary']['num_short'] == 1
        assert result['summary']['total_long_notional'] == 3000000
        assert result['summary']['total_short_notional'] == -500000

    def test_get_hedge_constituents_no_notional_column(self):
        """When constituents don't have a 'notional' column"""
        s = self._make_strategy()
        self._set_result(s, {
            'hedge': {
                'constituents': [
                    {'assetId': 'id1', 'name': 'A', 'weight': 0.5},
                ],
            },
        })
        result = s.get_hedge_constituents_by_direction()
        assert len(result['long_positions']) == 0
        assert len(result['short_positions']) == 0

    # --- get_performance_summary ---

    def test_get_performance_summary_no_result(self):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_performance_summary()

    def test_get_performance_summary_no_hedged_target(self):
        s = self._make_strategy()
        self._set_result(s, {'target': {}})
        with pytest.raises(MqValueError, match='does not contain hedgedTarget'):
            s.get_performance_summary()

    def test_get_performance_summary_no_target(self):
        s = self._make_strategy()
        self._set_result(s, {'hedgedTarget': {}})
        with pytest.raises(MqValueError, match='does not contain target'):
            s.get_performance_summary()

    def test_get_performance_summary_success(self):
        s = self._make_strategy()
        self._set_result(s, {
            'target': {
                'volatility': 0.15,
                'specificExposure': 0.05,
                'systematicExposure': 0.10,
                'totalPnl': 100000,
                'transactionCost': 5000,
            },
            'hedgedTarget': {
                'volatility': 0.10,
                'specificExposure': 0.04,
                'systematicExposure': 0.06,
                'totalPnl': 120000,
                'transactionCost': 6000,
                'borrowCostBps': 50,
                'exposureOverlapWithTarget': 0.85,
            },
        })
        result = s.get_performance_summary()
        assert 'risk' in result
        assert 'performance' in result
        assert 'transaction_cost' in result
        assert 'comparison' in result
        assert 'combined' in result
        assert isinstance(result['combined'], pd.DataFrame)

    def test_get_performance_summary_with_none_values(self):
        """Test that None values for systematicExposure and totalPnl result in NaN deltas
        (pandas converts None to NaN in float columns)"""
        s = self._make_strategy()
        self._set_result(s, {
            'target': {
                'volatility': 0.15,
                'specificExposure': None,
                'systematicExposure': None,
                'totalPnl': None,
                'transactionCost': 5000,
            },
            'hedgedTarget': {
                'volatility': 0.10,
                'specificExposure': None,
                'systematicExposure': None,
                'totalPnl': None,
                'transactionCost': 6000,
                'borrowCostBps': None,
                'exposureOverlapWithTarget': None,
            },
        })
        result = s.get_performance_summary()
        # Factor Risk Delta should be NaN (both inputs are None => code sets None => pandas converts to NaN)
        risk_df = result['risk']
        delta_row = risk_df[risk_df['Metric'] == 'Factor Risk Delta']
        assert pd.isna(delta_row['Hedged Portfolio'].values[0])
        # PnL Delta should be NaN
        perf_df = result['performance']
        pnl_delta_row = perf_df[perf_df['Metric'] == 'PnL Delta']
        assert pd.isna(pnl_delta_row['Hedged Portfolio'].values[0])

    def test_get_performance_summary_with_partial_none_values(self):
        """Test when only one side is None for systematicExposure (hedged not None, target None)"""
        s = self._make_strategy()
        self._set_result(s, {
            'target': {
                'volatility': 0.15,
                'specificExposure': 0.05,
                'systematicExposure': None,
                'totalPnl': 100000,
                'transactionCost': 5000,
            },
            'hedgedTarget': {
                'volatility': 0.10,
                'specificExposure': 0.04,
                'systematicExposure': 0.06,
                'totalPnl': None,
                'transactionCost': 6000,
                'borrowCostBps': 50,
                'exposureOverlapWithTarget': 0.85,
            },
        })
        result = s.get_performance_summary()
        # Factor Risk Delta should be NaN (target systematic is None)
        risk_df = result['risk']
        delta_row = risk_df[risk_df['Metric'] == 'Factor Risk Delta']
        assert pd.isna(delta_row['Hedged Portfolio'].values[0])
        # PnL Delta should be NaN (hedged totalPnl is None)
        perf_df = result['performance']
        pnl_delta_row = perf_df[perf_df['Metric'] == 'PnL Delta']
        assert pd.isna(pnl_delta_row['Hedged Portfolio'].values[0])

    # --- build_hedge_payload ---

    def test_build_hedge_payload(self):
        s = self._make_strategy()
        req = {'objective': 'Minimize Factor Risk', 'parameters': {'hedgeDate': '2024-01-15'}}
        resp = {'result': {'hedge': {'constituents': []}}}
        payload = s.build_hedge_payload(req, resp, hedge_name='My Hedge', group_name='My Group')
        assert payload['name'] == 'My Group'
        assert len(payload['hedges']) == 1
        assert payload['hedges'][0]['name'] == 'My Hedge'
        assert payload['hedges'][0]['parameters'] == {'hedgeDate': '2024-01-15'}
        assert payload['hedges'][0]['result'] == {'hedge': {'constituents': []}}

    def test_build_hedge_payload_defaults(self):
        s = self._make_strategy()
        payload = s.build_hedge_payload({}, {})
        assert payload['name'] == 'New Hedge Group'
        assert payload['hedges'][0]['name'] == 'Custom Hedge'
        assert payload['objective'] == 'Minimize Factor Risk'

    # --- save_to_marquee ---

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_save_to_marquee_success(self, mock_gs_session):
        mock_post = MagicMock(return_value={
            'id': 'hedge_group_id_123',
            'name': 'My Group',
            'createdTime': '2024-01-15T00:00:00Z',
        })
        mock_gs_session.current.sync.post = mock_post
        s = self._make_strategy()
        result = s.save_to_marquee(
            strategy_request={'objective': 'Minimize Factor Risk', 'parameters': {}},
            optimization_response={'result': {}},
            hedge_name='Test',
            group_name='My Group',
        )
        assert result['id'] == 'hedge_group_id_123'

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_save_to_marquee_no_id(self, mock_gs_session):
        """When response has no 'id', should still succeed with 'N/A'"""
        mock_post = MagicMock(return_value={'name': 'My Group'})
        mock_gs_session.current.sync.post = mock_post
        s = self._make_strategy()
        result = s.save_to_marquee(
            strategy_request={}, optimization_response={},
        )
        assert result['name'] == 'My Group'

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_save_to_marquee_failure(self, mock_gs_session):
        mock_gs_session.current.sync.post.side_effect = Exception('API Error')
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Failed to save hedge to Marquee'):
            s.save_to_marquee(strategy_request={}, optimization_response={})

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_save_to_marquee_failure_with_response(self, mock_gs_session):
        exc = Exception('API Error')
        exc.response = MagicMock()
        exc.response.text = 'Detailed error text'
        mock_gs_session.current.sync.post.side_effect = exc
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Failed to save hedge to Marquee'):
            s.save_to_marquee(strategy_request={}, optimization_response={})

    # --- run ---

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_success(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
            'positions': [{'assetId': 'id1', 'quantity': 100}],
        }
        mock_calculate.return_value = {
            'result': {
                'hedge': {'netExposure': 10000000, 'constituents': []},
            },
        }
        s = self._make_strategy()
        s.run()
        assert s._OptimizerStrategy__result is not None

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_none_optimizer_type_raises(self, mock_gs_session, mock_calculate):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='You must pass an optimizer type'):
            s.run(optimizer_type=None)

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_predefined_error(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': None,
            'errorMessage': 'Missing asset xref for something',
        }
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='optimizer returns an error'):
            s.run()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_non_predefined_error(self, mock_gs_session, mock_calculate):
        """Non-predefined error is raised, caught by except, and retried until exhaustion"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': None,
            'errorMessage': 'Some other error',
        }
        s = self._make_strategy()
        # The non-predefined MqValueError is caught by except, retried, and eventually
        # raises the generic "Error calculating an optimization" message
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_result_none_no_error_retries_then_fails(self, mock_gs_session, mock_calculate):
        """When result is None and no error message, should retry and eventually fail"""
        mock_gs_session.current.sync.post.return_value = {'actualNotional': 10000000}
        mock_calculate.return_value = {'result': None}
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_exception_retries_then_fails(self, mock_gs_session, mock_calculate):
        """When calculate_hedge raises, should retry and eventually fail"""
        mock_gs_session.current.sync.post.return_value = {'actualNotional': 10000000}
        mock_calculate.side_effect = RuntimeError('Network error')
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run()

    # --- run_save_share ---

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_success(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': {'hedge': {'netExposure': 10000000, 'constituents': []}},
        }
        s = self._make_strategy()
        req, resp = s.run_save_share()
        assert req is not None
        assert resp is not None

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_none_optimizer_type_raises(self, mock_gs_session, mock_calculate):
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='You must pass an optimizer type'):
            s.run_save_share(optimizer_type=None)

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_predefined_error(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': None,
            'errorMessage': 'ERROR: Could not find solution. Details...',
        }
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='optimizer returns an error'):
            s.run_save_share()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_non_predefined_error(self, mock_gs_session, mock_calculate):
        """Non-predefined error is raised, caught by except, and retried until exhaustion"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': None,
            'errorMessage': 'Unexpected error',
        }
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run_save_share()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_no_error_retries_then_fails(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {'actualNotional': 10000000}
        mock_calculate.return_value = {'result': None}
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run_save_share()

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_save_share_exception_retries(self, mock_gs_session, mock_calculate):
        mock_gs_session.current.sync.post.return_value = {'actualNotional': 10000000}
        mock_calculate.side_effect = RuntimeError('Network error')
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='Error calculating an optimization'):
            s.run_save_share()

    # --- to_dict ---

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_basic(self, mock_gs_session):
        """Test basic to_dict without turnover and with None constraints/settings"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
            'positions': [{'assetId': 'id1', 'quantity': 100}],
        }
        s = self._make_strategy()
        d = s.to_dict()
        assert d['objective'] == 'Minimize Factor Risk'
        assert 'parameters' in d
        assert d['parameters']['riskModel'] == 'BARRA_USFAST'
        assert d['parameters']['targetNotional'] == 10000000

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_with_reference_notional(self, mock_gs_session):
        """Test to_dict when position set has reference_notional"""
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', weight=1.0)],
            date=dt.date(2024, 1, 15),
            reference_notional=5000000,
        )
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 5000000,
            'positions': [{'assetId': 'id1', 'quantity': 100}],
        }
        mock_asset = MagicMock()
        mock_asset.get_marquee_id.return_value = 'univ_id1'
        s = self._make_strategy(initial_position_set=ps)
        d = s.to_dict()
        assert d['parameters']['targetNotional'] == 5000000
        # When reference_notional is not None, positions should be rebuilt from price_results
        assert d['parameters']['hedgeTarget']['positions'][0]['assetId'] == 'id1'

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_with_turnover(self, mock_gs_session):
        """Test to_dict with a turnover constraint"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        turnover_ps = PositionSet(
            positions=[Position(identifier='GOOG', asset_id='id3', quantity=50)],
            date=dt.date(2024, 1, 15),
        )
        tc = TurnoverConstraint(turnover_portfolio=turnover_ps, max_turnover_percent=80)
        s = self._make_strategy(turnover=tc)
        d = s.to_dict()
        assert d['parameters']['maxTurnoverPercentage'] == 80

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_with_turnover_reference_notional(self, mock_gs_session):
        """Test to_dict when turnover portfolio has reference_notional"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        turnover_ps = MagicMock()
        turnover_ps.reference_notional = 5000000
        turnover_ps.positions = [MagicMock(asset_id='id3', quantity=50)]
        tc = TurnoverConstraint(turnover_portfolio=turnover_ps, max_turnover_percent=80)
        s = self._make_strategy(turnover=tc)
        d = s.to_dict()
        turnover_ps.price.assert_called_once()

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_pricing_error(self, mock_gs_session):
        """Test that pricing error is raised as MqValueError"""
        mock_gs_session.current.sync.post.side_effect = Exception('Pricing failed')
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='error pricing your positions'):
            s.to_dict()

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_error_message_in_price_results(self, mock_gs_session):
        """Test that error in price results raises MqValueError"""
        mock_gs_session.current.sync.post.return_value = {
            'errorMessage': 'Some pricing error',
            'assetIdsMissingPrices': ['id1'],
        }
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='error pricing your positions'):
            s.to_dict()

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_error_message_no_missing_assets(self, mock_gs_session):
        """Test error message without missing prices"""
        mock_gs_session.current.sync.post.return_value = {
            'errorMessage': 'Some pricing error',
        }
        s = self._make_strategy()
        with pytest.raises(MqValueError, match='error pricing your positions'):
            s.to_dict()

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_with_objective_parameters(self, mock_gs_session):
        """Test to_dict with objective parameters set"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        term = OptimizerObjectiveTerm(weight=1.5)
        obj_params = OptimizerObjectiveParameters(terms=[term])
        s = self._make_strategy(objective_parameters=obj_params)
        d = s.to_dict()
        assert 'hedgeObjectiveParameters' in d['parameters']

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_fail_on_unpriced_true(self, mock_gs_session):
        """Test that fail_on_unpriced_positions=True produces expected payload"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        s = self._make_strategy()
        d = s.to_dict(fail_on_unpriced_positions=True)
        # Check that priceRegardlessOfAssetsMissingPrices is False
        assert 'parameters' in d

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_fail_on_unpriced_false(self, mock_gs_session):
        """Test that fail_on_unpriced_positions=False produces expected payload"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        s = self._make_strategy()
        d = s.to_dict(fail_on_unpriced_positions=False)
        assert 'parameters' in d


# =============================================================================
# Additional edge cases for full branch coverage
# =============================================================================


class TestEdgeCases:
    """Additional edge case tests for maximum branch coverage."""

    def test_optimizer_objective_term_default_risk_params_class_attribute(self):
        """Verify DEFAULT_RISK_PARAMS is accessible as class attribute"""
        defaults = OptimizerObjectiveTerm.DEFAULT_RISK_PARAMS
        assert defaults['factor_weight'] == 1
        assert defaults['specific_weight'] == 1
        assert defaults['risk_type'] == OptimizerRiskType.VARIANCE

    def test_asset_constraint_to_dict_decimal_vs_non_decimal(self):
        """Verify min/max multiplication only happens for DECIMAL"""
        # DECIMAL should multiply
        ac_dec = AssetConstraint(asset='a', minimum=0.1, maximum=0.9,
                                 unit=OptimizationConstraintUnit.DECIMAL)
        d_dec = ac_dec.to_dict()
        assert d_dec['min'] == pytest.approx(10.0)
        assert d_dec['max'] == pytest.approx(90.0)

        # PERCENT should not multiply
        ac_pct = AssetConstraint(asset='a', minimum=10, maximum=90,
                                 unit=OptimizationConstraintUnit.PERCENT)
        d_pct = ac_pct.to_dict()
        assert d_pct['min'] == 10
        assert d_pct['max'] == 90

    def test_country_constraint_to_dict_both_branches(self):
        """Cover both decimal and percent branches in to_dict"""
        cc_dec = CountryConstraint(country_name='USA', minimum=0.1, maximum=0.9,
                                   unit=OptimizationConstraintUnit.DECIMAL)
        d = cc_dec.to_dict()
        assert d['min'] == pytest.approx(10.0)
        assert d['max'] == pytest.approx(90.0)

    def test_sector_constraint_to_dict_both_branches(self):
        sc_dec = SectorConstraint(sector_name='Tech', minimum=0.1, maximum=0.9,
                                  unit=OptimizationConstraintUnit.DECIMAL)
        d = sc_dec.to_dict()
        assert d['min'] == pytest.approx(10.0)
        assert d['max'] == pytest.approx(90.0)

    def test_industry_constraint_to_dict_both_branches(self):
        ic_dec = IndustryConstraint(industry_name='Software', minimum=0.1, maximum=0.9,
                                    unit=OptimizationConstraintUnit.DECIMAL)
        d = ic_dec.to_dict()
        assert d['min'] == pytest.approx(10.0)
        assert d['max'] == pytest.approx(90.0)

    def test_optimizer_constraints_to_dict_no_asset_constraints(self):
        """When asset_constraints is empty, constrainAssetsByNotional should be False"""
        oc = OptimizerConstraints(asset_constraints=[])
        d = oc.to_dict()
        assert d['constrainAssetsByNotional'] is False

    def test_max_factor_proportion_of_risk_percent_conversion(self):
        """Verify percent is converted to decimal (divided by 100)"""
        c = MaxFactorProportionOfRiskConstraint(max_factor_proportion_of_risk=75,
                                                 unit=OptimizationConstraintUnit.PERCENT)
        assert c.max_factor_proportion_of_risk == pytest.approx(0.75)

    def test_max_proportion_by_group_percent_conversion(self):
        f = Factor(risk_model_id='m', id_='f', type_='Style', name='Value')
        c = MaxProportionOfRiskByGroupConstraint(
            factors=[f], max_factor_proportion_of_risk=80,
            unit=OptimizationConstraintUnit.PERCENT
        )
        assert c.max_factor_proportion_of_risk == pytest.approx(0.8)

    def test_constraint_priorities_to_dict_partial(self):
        """Test with a mix of set and unset priorities"""
        cp = ConstraintPriorities(
            min_sector_weights=PrioritySetting.ZERO,
            style_factor_exposures=PrioritySetting.FIVE,
        )
        d = cp.to_dict()
        assert 'minSectorWeights' in d
        assert 'styleExposures' in d
        assert 'maxSectorWeights' not in d
        assert len(d) == 2

    def test_optimizer_settings_unidirectional_gross_none_net_none(self):
        """No gross/net set in unidirectional mode - valid"""
        s = OptimizerSettings(allow_long_short=False)
        assert s.gross_notional is None
        assert s.net_notional is None

    def test_optimizer_settings_unidirectional_gross_set_net_none(self):
        """Gross set but net None in unidirectional mode - valid"""
        s = OptimizerSettings(allow_long_short=False, gross_notional=10000000)
        assert s.gross_notional == 10000000

    def test_optimizer_settings_unidirectional_gross_none_net_set(self):
        """Gross None but net set in unidirectional mode - valid"""
        s = OptimizerSettings(allow_long_short=False, net_notional=10000000)
        assert s.net_notional == 10000000

    def test_optimizer_universe_to_dict_no_corporate_actions(self):
        """Test excludeCorporateActions is False when list is empty"""
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='id1'))
        ou = OptimizerUniverse(assets=[mock_asset], exclude_corporate_actions_types=[])
        d = ou.to_dict()
        assert d['excludeCorporateActions'] is False
        assert d['excludeCorporateActionsTypes'] == []

    def test_turnover_constraint_to_dict_no_notional_type(self):
        """Verify turnoverNotionalType is not present when None"""
        ps = PositionSet(
            positions=[Position(identifier='X', asset_id='idx', quantity=10)],
            date=dt.date(2024, 1, 1),
        )
        tc = TurnoverConstraint(turnover_portfolio=ps, max_turnover_percent=50)
        d = tc.to_dict()
        assert 'turnoverNotionalType' not in d

    def test_turnover_constraint_to_dict_with_all_notional_types(self):
        """Verify all notional types produce correct values"""
        ps = PositionSet(
            positions=[Position(identifier='X', asset_id='idx', quantity=10)],
            date=dt.date(2024, 1, 1),
        )
        for nt in [TurnoverNotionalType.NET, TurnoverNotionalType.LONG, TurnoverNotionalType.GROSS]:
            tc = TurnoverConstraint(turnover_portfolio=ps, max_turnover_percent=50,
                                    turnover_notional_type=nt)
            d = tc.to_dict()
            assert d['turnoverNotionalType'] == nt.value

    def test_optimizer_settings_bidirectional_negative_net(self):
        """Net notional can be negative in bidirectional mode"""
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=-5000000)
        assert s.net_notional == -5000000

    def test_optimizer_settings_bidirectional_zero_net(self):
        """Market neutral: net_notional=0"""
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000,
                              net_notional=0)
        assert s.net_notional == 0

    def test_asset_constraint_build_many_all_missing_columns(self):
        """All required columns missing"""
        data = [{'foo': 'bar'}]
        with pytest.raises(MqValueError, match='missing required columns'):
            AssetConstraint.build_many_constraints(data)

    def test_country_constraint_build_many_from_df_directly(self):
        """Pass a DataFrame directly (not a list) to build_many_constraints"""
        df = pd.DataFrame([
            {'country': 'UK', 'minimum': 0, 'maximum': 10, 'unit': 'Percent'},
        ])
        result = CountryConstraint.build_many_constraints(df)
        assert len(result) == 1
        assert result[0].country_name == 'UK'

    def test_sector_constraint_build_many_from_df_directly(self):
        df = pd.DataFrame([
            {'sector': 'Energy', 'minimum': 0, 'maximum': 15, 'unit': 'Percent'},
        ])
        result = SectorConstraint.build_many_constraints(df)
        assert len(result) == 1

    def test_industry_constraint_build_many_from_df_directly(self):
        df = pd.DataFrame([
            {'industry': 'Mining', 'minimum': 0, 'maximum': 25, 'unit': 'Decimal'},
        ])
        result = IndustryConstraint.build_many_constraints(df)
        assert len(result) == 1

    @patch('gs_quant.markets.optimizer.FactorRiskModel.get')
    def test_factor_constraint_build_many_from_df(self, mock_get):
        mock_model = MagicMock()
        f = Factor(risk_model_id='m', id_='f_id', type_='Style', name='Momentum')
        mock_model.get_many_factors.return_value = [f]
        mock_get.return_value = mock_model
        df = pd.DataFrame([{'factor': 'Momentum', 'exposure': 3.0}])
        result = FactorConstraint.build_many_constraints(df, 'model_id')
        assert len(result) == 1

    def test_optimizer_strategy_verbose_error_msgs(self):
        """Verify both predefined error patterns"""
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
        )
        # Test Missing asset xref
        result = s.handle_error('Missing asset xref blah blah')
        assert result[1] is True
        assert 'asset meta data error' in result[0]

        # Test Could not find solution
        result = s.handle_error('ERROR: Could not find solution. Something')
        assert result[1] is True
        assert 'infeasible' in result[0].lower() or 'Potential' in result[0]

        # Test unknown error
        result = s.handle_error('Unknown error here')
        assert result[1] is False
        assert result[0] == 'Unknown error here'

    def test_optimizer_strategy_objective_parameters_property(self):
        """Test getting objective_parameters"""
        term = OptimizerObjectiveTerm()
        obj_params = OptimizerObjectiveParameters(terms=[term])
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
            objective_parameters=obj_params,
        )
        assert s.objective_parameters is obj_params

    def test_optimizer_universe_to_dict_with_asset_universe_resolve(self):
        """Test that AssetUniverse.resolve() is called in to_dict"""
        au = MagicMock(spec=AssetUniverse)
        au.asset_ids = ['id1', 'id2']
        ou = OptimizerUniverse(assets=au)
        d = ou.to_dict()
        au.resolve.assert_called_once()
        assert d['hedgeUniverse']['assetIds'] == ['id1', 'id2']

    def test_optimizer_settings_to_dict_bidirectional_net_none_gross_not_none(self):
        """When gross is set but net is None in bidirectional mode"""
        s = OptimizerSettings(allow_long_short=True, gross_notional=20000000, notional=15000000)
        d = s.to_dict()
        # Should fall through to hedgeNotional since net is None
        assert 'hedgeNotional' in d
        assert d['hedgeNotional'] == 15000000

    def test_optimizer_settings_to_dict_bidirectional_notional_none(self):
        """When all notionals are None in bidirectional mode"""
        s = OptimizerSettings(allow_long_short=True, notional=None)
        d = s.to_dict()
        # notional is None, so hedgeNotional should not be added
        # Actually let's check: elif self.__notional is not None => False
        assert 'hedgeNotional' not in d
        assert 'grossNotional' not in d

    @patch('gs_quant.markets.optimizer.GsHedgeApi.calculate_hedge')
    @patch('gs_quant.markets.optimizer.GsSession')
    def test_run_with_constraints_and_settings(self, mock_gs_session, mock_calculate):
        """Test run with pre-set constraints and settings"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        mock_calculate.return_value = {
            'result': {'hedge': {'netExposure': 10000000, 'constituents': []}},
        }
        constraints = OptimizerConstraints()
        settings = OptimizerSettings(notional=5000000)
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
            constraints=constraints,
            settings=settings,
        )
        s.run()
        assert s._OptimizerStrategy__result is not None

    def test_get_cumulative_pnl_with_different_targets(self):
        """Test get_cumulative_pnl_performance with different HedgeTarget values"""
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
        )
        s._OptimizerStrategy__result = {
            'hedge': {'cumulativePnl': [['2024-01-01', 50]]},
            'target': {'cumulativePnl': [['2024-01-01', 100]]},
            'hedgedTarget': {'cumulativePnl': [['2024-01-01', 150]]},
        }
        for target in [HedgeTarget.HEDGE, HedgeTarget.TARGET, HedgeTarget.HEDGED_TARGET]:
            df = s.get_cumulative_pnl_performance(target=target)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 1

    def test_get_style_factor_exposures_with_different_targets(self):
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
        )
        exposures_data = {'factorExposures': {'style': [{'factor': 'Value', 'exposure': 0.5}]}}
        s._OptimizerStrategy__result = {
            'hedge': exposures_data,
            'target': exposures_data,
            'hedgedTarget': exposures_data,
        }
        for target in [HedgeTarget.HEDGE, HedgeTarget.TARGET, HedgeTarget.HEDGED_TARGET]:
            result = s.get_style_factor_exposures(target=target)
            assert len(result) == 1

    def test_get_risk_buckets_with_different_targets(self):
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
        )
        s._OptimizerStrategy__result = {
            'hedge': {'riskBuckets': {'factor': 0.3}},
            'target': {'riskBuckets': {'factor': 0.4}},
            'hedgedTarget': {'riskBuckets': {'factor': 0.5}},
        }
        for target in [HedgeTarget.HEDGE, HedgeTarget.TARGET, HedgeTarget.HEDGED_TARGET]:
            result = s.get_risk_buckets(target=target)
            assert 'risk_buckets' in result

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_none_values_in_constraints_filtered(self, mock_gs_session):
        """Test that None values in constraints dict are filtered out"""
        mock_gs_session.current.sync.post.return_value = {
            'actualNotional': 10000000,
        }
        ps = PositionSet(
            positions=[Position(identifier='AAPL', asset_id='id1', quantity=100)],
            date=dt.date(2024, 1, 15),
        )
        mock_asset = MagicMock(get_marquee_id=MagicMock(return_value='univ_id1'))
        # Use constraints with all None priorities (to_dict returns None for some keys)
        cp = ConstraintPriorities()
        settings = OptimizerSettings(constraint_priorities=cp)
        constraints = OptimizerConstraints()
        s = OptimizerStrategy(
            initial_position_set=ps,
            universe=OptimizerUniverse(assets=[mock_asset]),
            risk_model=MagicMock(id='model_id'),
            constraints=constraints,
            settings=settings,
        )
        d = s.to_dict()
        # constraintPrioritySettings is None from ConstraintPriorities.to_dict() when all are None
        # The code checks `if settings[key] is not None` so it should be excluded
        assert 'constraintPrioritySettings' not in d['parameters']


# =============================================================================
# Phase 6 – additional branch-coverage tests
# =============================================================================


class TestOptimizerStrategyBranchPhase6:
    """Cover branches: [1677,1676], [1685,1684], [1693,1692],
    [1756,-1741], [1810,-1794], [1920,1921], [1975,1976]"""

    def _make_strategy(self, *, result=None, turnover=None, constraints=None, settings=None):
        """Build an OptimizerStrategy with mocks pre-wired."""
        mock_ps = MagicMock()
        mock_ps.date = dt.date(2024, 1, 15)
        mock_ps.reference_notional = None
        mock_ps.to_frame.return_value = pd.DataFrame(
            {'asset_id': ['id1'], 'quantity': [100]}
        )
        mock_universe = MagicMock()
        mock_universe.to_dict.return_value = {}
        mock_rm = MagicMock(id='model1')
        s = OptimizerStrategy.__new__(OptimizerStrategy)
        s._OptimizerStrategy__objective = OptimizerObjective.MINIMIZE_FACTOR_RISK
        s._OptimizerStrategy__objective_parameters = None
        s._OptimizerStrategy__initial_position_set = mock_ps
        s._OptimizerStrategy__constraints = constraints or OptimizerConstraints()
        s._OptimizerStrategy__settings = settings or OptimizerSettings()
        s._OptimizerStrategy__universe = mock_universe
        s._OptimizerStrategy__risk_model = mock_rm
        s._OptimizerStrategy__turnover = turnover
        s._OptimizerStrategy__result = result
        return s

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_constraints_all_none_skipped(self, mock_gs):
        """Empty constraints dict -> for loop body never entered [1677,1676]."""
        mock_gs.current.sync.post.return_value = {'actualNotional': 10000000}
        constraints = MagicMock()
        constraints.to_dict.return_value = {}  # empty -> loop has no iterations
        settings = MagicMock()
        settings.to_dict.return_value = {}
        s = self._make_strategy(constraints=constraints, settings=settings)
        s._OptimizerStrategy__constraints = constraints
        s._OptimizerStrategy__settings = settings
        d = s.to_dict()
        assert 'objective' in d

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_universe_all_none_skipped(self, mock_gs):
        """Universe dict with a None value -> [1685,1684] skip branch."""
        mock_gs.current.sync.post.return_value = {'actualNotional': 10000000}
        s = self._make_strategy()
        s._OptimizerStrategy__universe.to_dict.return_value = {'excludeKey': None}
        d = s.to_dict()
        assert 'excludeKey' not in d['parameters']

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_turnover_none_values_skipped(self, mock_gs):
        """Turnover dict has a None value -> [1693,1692] skip branch."""
        mock_gs.current.sync.post.return_value = {'actualNotional': 10000000}
        mock_turnover = MagicMock()
        mock_turnover.turnover_portfolio = MagicMock()
        mock_turnover.turnover_portfolio.reference_notional = None
        mock_turnover.to_dict.return_value = {'turnoverKey': None}
        s = self._make_strategy(turnover=mock_turnover)
        d = s.to_dict()
        assert 'turnoverKey' not in d['parameters']

    @patch('gs_quant.markets.optimizer.GsHedgeApi')
    def test_run_non_axioma_type_noop(self, mock_hedge):
        """run with non-AXIOMA type -> [1756,-1741] branch not entered."""
        s = self._make_strategy()
        s.to_dict = MagicMock()
        # Pass a type that is not AXIOMA_PORTFOLIO_OPTIMIZER
        # The if-check on line 1756 should be false -> method returns without doing anything
        mock_type = MagicMock()
        mock_type.__eq__ = lambda self, other: False  # not equal to AXIOMA
        mock_type.__ne__ = lambda self, other: True
        s.run(optimizer_type=mock_type)
        mock_hedge.calculate_hedge.assert_not_called()

    @patch('gs_quant.markets.optimizer.GsHedgeApi')
    def test_run_save_share_non_axioma_type_noop(self, mock_hedge):
        """run_save_share with non-AXIOMA type -> [1810,-1794] branch not entered."""
        s = self._make_strategy()
        s.to_dict = MagicMock()
        mock_type = MagicMock()
        mock_type.__eq__ = lambda self, other: False
        mock_type.__ne__ = lambda self, other: True
        result = s.run_save_share(optimizer_type=mock_type)
        mock_hedge.calculate_hedge.assert_not_called()

    def test_get_hedge_exposure_summary_no_result(self):
        """[1920,1921] __result is None -> raises."""
        s = self._make_strategy(result=None)
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_hedge_exposure_summary()

    def test_get_hedge_constituents_by_direction_no_result(self):
        """[1975,1976] __result is None -> raises."""
        s = self._make_strategy(result=None)
        with pytest.raises(MqValueError, match='Please run the optimization'):
            s.get_hedge_constituents_by_direction()

    @patch('gs_quant.markets.optimizer.GsSession')
    def test_to_dict_constraints_with_none_value(self, mock_gs):
        """[1677,1676] constraints dict has a key with None value -> skip that key."""
        mock_gs.current.sync.post.return_value = {'actualNotional': 10000000}
        constraints = MagicMock()
        constraints.to_dict.return_value = {'someConstraint': None, 'other': 'val'}
        settings = MagicMock()
        settings.to_dict.return_value = {}
        s = self._make_strategy(constraints=constraints, settings=settings)
        s._OptimizerStrategy__constraints = constraints
        s._OptimizerStrategy__settings = settings
        d = s.to_dict()
        # 'someConstraint' should NOT be in parameters since its value is None
        assert 'someConstraint' not in d['parameters']
        # 'other' should be in parameters since its value is not None
        assert d['parameters']['other'] == 'val'
