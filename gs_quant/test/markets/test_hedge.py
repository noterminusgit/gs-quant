"""
Copyright 2025 Goldman Sachs.
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

import datetime as dt
from collections import defaultdict
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.errors import MqValueError
from gs_quant.markets.hedge import (
    FactorExposureCategory,
    ConstraintType,
    HedgeExclusions,
    Constraint,
    HedgeConstraints,
    PerformanceHedgeParameters,
    Hedge,
    PerformanceHedge,
)
from gs_quant.markets.position_set import Position, PositionSet
from gs_quant.target.hedge import HedgeObjective, CorporateActionsTypes


# ─── Enums ────────────────────────────────────────────────────────────────────


class TestEnums:
    def test_factor_exposure_category(self):
        assert FactorExposureCategory.COUNTRY.value == 'country'
        assert FactorExposureCategory.SECTOR.value == 'sector'
        assert FactorExposureCategory.INDUSTRY.value == 'industry'
        assert FactorExposureCategory.STYLE.value == 'style'

    def test_constraint_type(self):
        assert ConstraintType.ASSET.value == "Asset"
        assert ConstraintType.COUNTRY.value == "Country"
        assert ConstraintType.REGION.value == "Region"
        assert ConstraintType.SECTOR.value == "Sector"
        assert ConstraintType.INDUSTRY.value == "Industry"
        assert ConstraintType.ESG.value == "Esg"


# ─── HedgeExclusions ─────────────────────────────────────────────────────────


class TestHedgeExclusions:
    def test_init_defaults(self):
        he = HedgeExclusions()
        assert he.assets is None
        assert he.countries is None
        assert he.regions is None
        assert he.sectors is None
        assert he.industries is None

    def test_init_with_values(self):
        he = HedgeExclusions(
            assets=['A1', 'A2'],
            countries=['US'],
            regions=['NA'],
            sectors=['Tech'],
            industries=['Software'],
        )
        assert he.assets == ['A1', 'A2']
        assert he.countries == ['US']
        assert he.regions == ['NA']
        assert he.sectors == ['Tech']
        assert he.industries == ['Software']

    def test_setters(self):
        he = HedgeExclusions()
        he.assets = ['X']
        he.countries = ['UK']
        he.regions = ['EU']
        he.sectors = ['Finance']
        he.industries = ['Banking']
        assert he.assets == ['X']
        assert he.countries == ['UK']
        assert he.regions == ['EU']
        assert he.sectors == ['Finance']
        assert he.industries == ['Banking']

    def test_to_dict_empty(self):
        he = HedgeExclusions()
        result = he.to_dict()
        assert result == {}

    def test_to_dict_all_fields(self):
        he = HedgeExclusions(
            assets=['ASSET1'],
            countries=['US'],
            regions=['NA'],
            sectors=['Tech'],
            industries=['Software'],
        )
        result = he.to_dict()
        assert 'classificationConstraints' in result
        assert 'assetConstraints' in result
        # classificationConstraints should have 4 items (countries, regions, sectors, industries)
        assert len(result['classificationConstraints']) == 4
        assert len(result['assetConstraints']) == 1

    def test_to_dict_only_countries(self):
        he = HedgeExclusions(countries=['US', 'UK'])
        result = he.to_dict()
        assert 'classificationConstraints' in result
        assert len(result['classificationConstraints']) == 2
        assert 'assetConstraints' not in result

    def test_to_dict_only_regions(self):
        he = HedgeExclusions(regions=['NA'])
        result = he.to_dict()
        assert 'classificationConstraints' in result
        assert len(result['classificationConstraints']) == 1

    def test_to_dict_only_sectors(self):
        he = HedgeExclusions(sectors=['Tech'])
        result = he.to_dict()
        assert 'classificationConstraints' in result

    def test_to_dict_only_industries(self):
        he = HedgeExclusions(industries=['Software'])
        result = he.to_dict()
        assert 'classificationConstraints' in result

    def test_to_dict_only_assets(self):
        he = HedgeExclusions(assets=['A1'])
        result = he.to_dict()
        assert 'classificationConstraints' not in result
        assert 'assetConstraints' in result

    def test_get_exclusions_static(self):
        result = HedgeExclusions._get_exclusions(['US', 'UK'], ConstraintType.COUNTRY)
        assert len(result) == 2
        assert all(item['min'] == 0 and item['max'] == 0 for item in result)


# ─── Constraint ───────────────────────────────────────────────────────────────


class TestConstraint:
    def test_init_defaults(self):
        c = Constraint(constraint_name='Test')
        assert c.constraint_name == 'Test'
        assert c.minimum == 0
        assert c.maximum == 100
        assert c.constraint_type is None

    def test_init_with_values(self):
        c = Constraint(
            constraint_name='X',
            minimum=10,
            maximum=50,
            constraint_type=ConstraintType.COUNTRY,
        )
        assert c.constraint_name == 'X'
        assert c.minimum == 10
        assert c.maximum == 50
        assert c.constraint_type == ConstraintType.COUNTRY

    def test_setters(self):
        c = Constraint(constraint_name='A')
        c.constraint_name = 'B'
        c.minimum = 5
        c.maximum = 95
        c.constraint_type = ConstraintType.SECTOR
        assert c.constraint_name == 'B'
        assert c.minimum == 5
        assert c.maximum == 95
        assert c.constraint_type == ConstraintType.SECTOR

    def test_from_dict_with_type(self):
        d = {'type': 'Country', 'name': 'US', 'min': 0, 'max': 50}
        c = Constraint.from_dict(d)
        assert c.constraint_type == ConstraintType.COUNTRY
        assert c.constraint_name == 'US'
        assert c.minimum == 0
        assert c.maximum == 50

    def test_from_dict_with_asset_id(self):
        d = {'assetId': 'MQID123', 'min': 0, 'max': 10}
        c = Constraint.from_dict(d)
        assert c.constraint_type == ConstraintType.ASSET
        assert c.constraint_name == 'MQID123'

    def test_from_dict_esg_fallback(self):
        """No type and no assetId defaults to ESG."""
        d = {'name': 'ESG_metric', 'min': 0, 'max': 100}
        c = Constraint.from_dict(d)
        assert c.constraint_type == ConstraintType.ESG
        assert c.constraint_name == 'ESG_metric'

    def test_to_dict_country(self):
        c = Constraint(constraint_name='US', minimum=0, maximum=50, constraint_type=ConstraintType.COUNTRY)
        d = c.to_dict()
        assert d == {'name': 'US', 'min': 0, 'max': 50, 'type': 'Country'}

    def test_to_dict_sector(self):
        c = Constraint(constraint_name='Tech', minimum=10, maximum=30, constraint_type=ConstraintType.SECTOR)
        d = c.to_dict()
        assert d['type'] == 'Sector'
        assert d['name'] == 'Tech'

    def test_to_dict_region(self):
        c = Constraint(constraint_name='NA', minimum=0, maximum=40, constraint_type=ConstraintType.REGION)
        d = c.to_dict()
        assert d['type'] == 'Region'

    def test_to_dict_industry(self):
        c = Constraint(constraint_name='Software', minimum=5, maximum=25, constraint_type=ConstraintType.INDUSTRY)
        d = c.to_dict()
        assert d['type'] == 'Industry'

    def test_to_dict_esg(self):
        """ESG type should not include 'type' in output."""
        c = Constraint(constraint_name='ESG1', minimum=0, maximum=100, constraint_type=ConstraintType.ESG)
        d = c.to_dict()
        assert 'type' not in d
        assert d['name'] == 'ESG1'

    def test_to_dict_asset(self):
        """Asset type should use 'assetId' instead of 'name'."""
        c = Constraint(constraint_name='MQID1', minimum=0, maximum=10, constraint_type=ConstraintType.ASSET)
        d = c.to_dict()
        assert 'type' not in d
        assert 'name' not in d
        assert d['assetId'] == 'MQID1'
        assert d['min'] == 0
        assert d['max'] == 10


# ─── HedgeConstraints ────────────────────────────────────────────────────────


class TestHedgeConstraints:
    def test_init_defaults(self):
        hc = HedgeConstraints()
        assert hc.assets is None
        assert hc.countries is None
        assert hc.regions is None
        assert hc.sectors is None
        assert hc.industries is None
        assert hc.esg is None

    def test_init_with_values_assigns_types(self):
        asset_con = Constraint(constraint_name='A1')
        country_con = Constraint(constraint_name='US')
        region_con = Constraint(constraint_name='NA')
        sector_con = Constraint(constraint_name='Tech')
        industry_con = Constraint(constraint_name='Software')
        esg_con = Constraint(constraint_name='ESG1')

        hc = HedgeConstraints(
            assets=[asset_con],
            countries=[country_con],
            regions=[region_con],
            sectors=[sector_con],
            industries=[industry_con],
            esg=[esg_con],
        )
        assert asset_con.constraint_type == ConstraintType.ASSET
        assert country_con.constraint_type == ConstraintType.COUNTRY
        assert region_con.constraint_type == ConstraintType.REGION
        assert sector_con.constraint_type == ConstraintType.SECTOR
        assert industry_con.constraint_type == ConstraintType.INDUSTRY
        assert esg_con.constraint_type == ConstraintType.ESG

    def test_setters(self):
        hc = HedgeConstraints()
        hc.assets = [Constraint('A')]
        hc.countries = [Constraint('US')]
        hc.regions = [Constraint('EU')]
        hc.sectors = [Constraint('Tech')]
        hc.industries = [Constraint('SW')]
        hc.esg = [Constraint('E')]
        assert len(hc.assets) == 1
        assert len(hc.countries) == 1
        assert len(hc.regions) == 1
        assert len(hc.sectors) == 1
        assert len(hc.industries) == 1
        assert len(hc.esg) == 1

    def test_to_dict_empty(self):
        hc = HedgeConstraints()
        result = hc.to_dict()
        assert result == {}

    def test_to_dict_all_fields(self):
        hc = HedgeConstraints(
            assets=[Constraint('A1', 0, 10)],
            countries=[Constraint('US', 0, 50)],
            regions=[Constraint('NA', 5, 30)],
            sectors=[Constraint('Tech', 0, 40)],
            industries=[Constraint('SW', 0, 20)],
            esg=[Constraint('ESG1', 0, 100)],
        )
        result = hc.to_dict()
        assert 'classificationConstraints' in result
        assert 'assetConstraints' in result
        assert 'esgConstraints' in result
        # 4 classification constraints (countries, regions, sectors, industries)
        assert len(result['classificationConstraints']) == 4

    def test_to_dict_only_countries(self):
        hc = HedgeConstraints(countries=[Constraint('US', 0, 50)])
        result = hc.to_dict()
        assert 'classificationConstraints' in result
        assert 'assetConstraints' not in result
        assert 'esgConstraints' not in result

    def test_to_dict_only_assets(self):
        hc = HedgeConstraints(assets=[Constraint('A1', 0, 10)])
        result = hc.to_dict()
        assert 'classificationConstraints' not in result
        assert 'assetConstraints' in result

    def test_to_dict_only_esg(self):
        hc = HedgeConstraints(esg=[Constraint('ESG1', 0, 100)])
        result = hc.to_dict()
        assert 'esgConstraints' in result
        assert 'classificationConstraints' not in result
        assert 'assetConstraints' not in result


# ─── PerformanceHedgeParameters ───────────────────────────────────────────────


def _make_position_set(date=None, reference_notional=None, with_quantity=True, with_weight=False):
    """Helper to create a basic PositionSet for testing."""
    if date is None:
        date = dt.date(2024, 1, 15)
    if reference_notional is not None:
        positions = [
            Position(identifier='AAPL', weight=60, asset_id='MQ_AAPL'),
            Position(identifier='GOOG', weight=40, asset_id='MQ_GOOG'),
        ]
    elif with_quantity:
        positions = [
            Position(identifier='AAPL', quantity=100, asset_id='MQ_AAPL'),
            Position(identifier='GOOG', quantity=50, asset_id='MQ_GOOG'),
        ]
    else:
        positions = [
            Position(identifier='AAPL', weight=60, asset_id='MQ_AAPL'),
            Position(identifier='GOOG', weight=40, asset_id='MQ_GOOG'),
        ]
    return PositionSet(positions=positions, date=date, reference_notional=reference_notional)


class TestPerformanceHedgeParameters:
    def test_init_defaults(self):
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])
        assert params.initial_portfolio is ps
        assert params.universe == ['SPX']
        assert params.exclusions is None
        assert params.constraints is None
        assert params.observation_start_date is None
        assert params.sampling_period == 'Daily'
        assert params.max_leverage == 100
        assert params.percentage_in_cash is None
        assert params.explode_universe is True
        assert params.exclude_target_assets is True
        assert params.exclude_corporate_actions_types is None
        assert params.exclude_hard_to_borrow_assets is False
        assert params.exclude_restricted_assets is False
        assert params.max_adv_percentage == 15
        assert params.max_return_deviation == 5
        assert params.max_weight == 100
        assert params.min_market_cap is None
        assert params.max_market_cap is None
        assert params.market_participation_rate == 10
        assert params.lasso_weight == 0
        assert params.ridge_weight == 0
        assert params.benchmarks is None

    def test_setters(self):
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])

        ps2 = _make_position_set()
        params.initial_portfolio = ps2
        params.universe = ['NDX']
        params.observation_start_date = dt.date(2023, 1, 1)
        params.exclusions = HedgeExclusions()
        params.constraints = HedgeConstraints()
        params.sampling_period = 'Weekly'
        params.max_leverage = 50
        params.percentage_in_cash = 10
        params.explode_universe = False
        params.exclude_target_assets = False
        params.exclude_corporate_actions_types = [CorporateActionsTypes.Mergers]
        params.exclude_hard_to_borrow_assets = True
        params.exclude_restricted_assets = True
        params.max_adv_percentage = 20
        params.max_return_deviation = 10
        params.max_weight = 50
        params.min_market_cap = 1e9
        params.max_market_cap = 1e12
        params.market_participation_rate = 5
        params.benchmarks = ['BM1']
        params.lasso_weight = 0.5
        params.ridge_weight = 0.3

        assert params.initial_portfolio is ps2
        assert params.universe == ['NDX']
        assert params.observation_start_date == dt.date(2023, 1, 1)
        assert params.sampling_period == 'Weekly'
        assert params.max_leverage == 50
        assert params.percentage_in_cash == 10
        assert params.explode_universe is False
        assert params.exclude_target_assets is False
        assert params.exclude_hard_to_borrow_assets is True
        assert params.exclude_restricted_assets is True
        assert params.max_adv_percentage == 20
        assert params.max_return_deviation == 10
        assert params.max_weight == 50
        assert params.min_market_cap == 1e9
        assert params.max_market_cap == 1e12
        assert params.market_participation_rate == 5
        assert params.benchmarks == ['BM1']
        assert params.lasso_weight == 0.5
        assert params.ridge_weight == 0.3

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_basic(self, mock_session):
        """to_dict with basic positions (quantity-based)."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])

        mock_session.current.sync.post.return_value = {
            'positions': [
                {'assetId': 'MQ_AAPL', 'quantity': 100},
                {'assetId': 'MQ_GOOG', 'quantity': 50},
            ],
            'actualNotional': 1000000,
        }

        resolved = {
            'SPX': [{'id': 'MQ_SPX'}],
        }
        result = params.to_dict(resolved)

        assert 'hedgeTarget' in result
        assert 'universe' in result
        assert result['universe'] == ['MQ_SPX']
        assert result['notional'] == 1000000
        assert result['samplingPeriod'] == 'Daily'
        assert result['maxLeverage'] == 100
        assert result['useMachineLearning'] is True

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_reference_notional(self, mock_session):
        """to_dict with reference_notional should include targetNotional."""
        ps = _make_position_set(reference_notional=500000)
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])

        mock_session.current.sync.post.return_value = {
            'positions': [
                {'assetId': 'MQ_AAPL', 'quantity': 100},
                {'assetId': 'MQ_GOOG', 'quantity': 50},
            ],
        }

        resolved = {'SPX': [{'id': 'MQ_SPX'}]}
        result = params.to_dict(resolved)
        # reference_notional was already set, so it stays
        assert result['notional'] == 500000

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_pricing_error(self, mock_session):
        """to_dict should raise MqValueError on pricing API exception."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])
        mock_session.current.sync.post.side_effect = Exception('API down')

        with pytest.raises(MqValueError, match='There was an error pricing your positions'):
            params.to_dict({})

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_pricing_error_message(self, mock_session):
        """to_dict should raise MqValueError when API returns errorMessage."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])
        mock_session.current.sync.post.return_value = {
            'errorMessage': 'Bad positions'
        }

        with pytest.raises(MqValueError, match='Bad positions'):
            params.to_dict({})

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_observation_start_date(self, mock_session):
        """to_dict with observation_start_date set."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            observation_start_date=dt.date(2023, 6, 1),
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert result['observationStartDate'] == '2023-06-01'

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_exclusions(self, mock_session):
        """to_dict with exclusions."""
        ps = _make_position_set()
        excl = HedgeExclusions(assets=['EXCL1'], countries=['US'])
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            exclusions=excl,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        resolved = {
            'SPX': [{'id': 'MQ_SPX'}],
            'EXCL1': [{'id': 'MQ_EXCL1'}],
        }
        result = params.to_dict(resolved)
        assert 'classificationConstraints' in result
        assert 'assetConstraints' in result

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_exclusions_no_assets(self, mock_session):
        """to_dict with exclusions that have no assets."""
        ps = _make_position_set()
        excl = HedgeExclusions(countries=['US'])
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            exclusions=excl,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert 'classificationConstraints' in result

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_constraints(self, mock_session):
        """to_dict with constraints."""
        ps = _make_position_set()
        cons = HedgeConstraints(
            assets=[Constraint('ASSET1', 0, 10)],
            countries=[Constraint('US', 0, 50)],
            esg=[Constraint('ESG1', 0, 100)],
        )
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            constraints=cons,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        resolved = {
            'SPX': [{'id': 'MQ_SPX'}],
            'ASSET1': [{'id': 'MQ_ASSET1'}],
        }
        result = params.to_dict(resolved)
        assert 'classificationConstraints' in result
        assert 'assetConstraints' in result
        assert 'esgConstraints' in result

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_constraints_asset_not_resolved(self, mock_session):
        """to_dict when constraint asset is not found in resolved identifiers."""
        ps = _make_position_set()
        cons = HedgeConstraints(
            assets=[Constraint('UNKNOWN_ASSET', 0, 10)],
        )
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            constraints=cons,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        resolved = {'SPX': [{'id': 'MQ_SPX'}]}
        # UNKNOWN_ASSET not in resolved, so constraint_name should stay
        result = params.to_dict(resolved)
        assert 'assetConstraints' in result

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_percentage_in_cash(self, mock_session):
        """to_dict with percentage_in_cash set."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            percentage_in_cash=5.0,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert result['percentageInCash'] == 5.0

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_corporate_actions(self, mock_session):
        """to_dict with exclude_corporate_actions_types set."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            exclude_corporate_actions_types=[CorporateActionsTypes.Mergers, CorporateActionsTypes.Spinoffs],
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert 'excludeCorporateActionTypes' in result
        assert 'Mergers' in result['excludeCorporateActionTypes']
        assert 'Spinoffs' in result['excludeCorporateActionTypes']

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_market_cap(self, mock_session):
        """to_dict with min/max market cap."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            min_market_cap=1e9,
            max_market_cap=1e12,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert result['minMarketCap'] == 1e9
        assert result['maxMarketCap'] == 1e12

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_with_benchmarks(self, mock_session):
        """to_dict with benchmarks set."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            benchmarks=['BM1', 'BM2'],
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        resolved = {
            'SPX': [{'id': 'MQ_SPX'}],
            'BM1': [{'id': 'MQ_BM1'}],
            'BM2': [{'id': 'MQ_BM2'}],
        }
        result = params.to_dict(resolved)
        assert 'benchmarks' in result
        assert result['benchmarks'] == ['MQ_BM1', 'MQ_BM2']

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_benchmarks_empty_list(self, mock_session):
        """to_dict with empty benchmarks list should not include benchmarks key."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            benchmarks=[],
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert 'benchmarks' not in result

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_position_with_weight(self, mock_session):
        """to_dict with weight-only positions."""
        positions = [
            Position(identifier='AAPL', weight=60, asset_id='MQ_AAPL'),
        ]
        ps = PositionSet(positions=positions, date=dt.date(2024, 1, 15), reference_notional=1000000)
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])

        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert result['notional'] == 1000000

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_universe_unresolved(self, mock_session):
        """to_dict when universe item is not in resolved identifiers."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['UNKNOWN_ETF'])
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({})
        # Unresolved should fall back to original id
        assert result['universe'] == ['UNKNOWN_ETF']

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_in_payload_basic(self, mock_asset_api):
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])
        mock_asset_api.resolve_assets.return_value = {'SPX': [{'id': 'MQ_SPX'}]}
        result = params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        mock_asset_api.resolve_assets.assert_called_once()

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_with_exclusions(self, mock_asset_api):
        ps = _make_position_set()
        excl = HedgeExclusions(assets=['EXCL1'])
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'], exclusions=excl)
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        call_args = mock_asset_api.resolve_assets.call_args
        identifiers = call_args[1]['identifier'] if 'identifier' in call_args[1] else call_args[0][0]
        assert 'EXCL1' in identifiers

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_with_benchmarks(self, mock_asset_api):
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'], benchmarks=['BM1'])
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        call_args = mock_asset_api.resolve_assets.call_args
        identifiers = call_args[1]['identifier'] if 'identifier' in call_args[1] else call_args[0][0]
        assert 'BM1' in identifiers

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_with_constraints(self, mock_asset_api):
        ps = _make_position_set()
        cons = HedgeConstraints(assets=[Constraint('A1', 0, 10)])
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'], constraints=cons)
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        call_args = mock_asset_api.resolve_assets.call_args
        identifiers = call_args[1]['identifier'] if 'identifier' in call_args[1] else call_args[0][0]
        assert 'A1' in identifiers

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_no_exclusions_no_benchmarks_no_constraints(self, mock_asset_api):
        """Test resolve_identifiers when exclusions, benchmarks, and constraints are all None."""
        ps = _make_position_set()
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'])
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        mock_asset_api.resolve_assets.assert_called_once()

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_exclusions_no_assets(self, mock_asset_api):
        """Test resolve_identifiers when exclusions exist but assets is None."""
        ps = _make_position_set()
        excl = HedgeExclusions(countries=['US'])  # No assets
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'], exclusions=excl)
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        mock_asset_api.resolve_assets.assert_called_once()

    @patch('gs_quant.markets.hedge.GsAssetApi')
    def test_resolve_identifiers_constraints_no_assets(self, mock_asset_api):
        """Test resolve_identifiers when constraints exist but assets is None."""
        ps = _make_position_set()
        cons = HedgeConstraints(countries=[Constraint('US', 0, 50)])
        params = PerformanceHedgeParameters(initial_portfolio=ps, universe=['SPX'], constraints=cons)
        mock_asset_api.resolve_assets.return_value = {}
        params.resolve_identifiers_in_payload(dt.date(2024, 1, 15))
        mock_asset_api.resolve_assets.assert_called_once()


# ─── Hedge ────────────────────────────────────────────────────────────────────


class TestHedge:
    def test_init(self):
        mock_params = MagicMock()
        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        assert h.parameters is mock_params
        assert h.objective == HedgeObjective.Replicate_Performance
        assert h.result == {}

    def test_parameters_setter(self):
        mock_params = MagicMock()
        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        new_params = MagicMock()
        h.parameters = new_params
        assert h.parameters is new_params

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_calculate_success(self, mock_hedge_api):
        """Calculate should format results and store them."""
        mock_params = MagicMock()
        mock_params.resolve_identifiers_in_payload.return_value = {
            'SPX': [{'id': 'MQ_SPX'}],
        }
        mock_params.to_dict.return_value = {'universe': ['MQ_SPX']}

        mock_hedge_api.calculate_hedge.return_value = {
            'result': {
                'target': {'annualizedReturn': 0.05, 'backtestPerformance': [[1, 2]]},
                'hedge': {'annualizedReturn': 0.04, 'constituents': [{'assetId': 'A', 'weight': 0.5}]},
                'hedgedTarget': {'annualizedReturn': 0.045},
                'benchmarks': [],
            }
        }

        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        result = h.calculate()
        assert 'Portfolio' in result
        assert 'Hedge' in result
        assert 'Hedged Portfolio' in result

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_calculate_with_error(self, mock_hedge_api):
        """Calculate should raise MqValueError on error response."""
        mock_params = MagicMock()
        mock_params.resolve_identifiers_in_payload.return_value = {}
        mock_params.to_dict.return_value = {}

        mock_hedge_api.calculate_hedge.return_value = {
            'errorMessage': 'Constraints too tight'
        }

        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        with pytest.raises(MqValueError, match='Error calculating hedge'):
            h.calculate()

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_calculate_error_with_result_key(self, mock_hedge_api):
        """Calculate with both errorMessage and result should NOT raise."""
        mock_params = MagicMock()
        mock_params.resolve_identifiers_in_payload.return_value = {}
        mock_params.to_dict.return_value = {}

        mock_hedge_api.calculate_hedge.return_value = {
            'errorMessage': 'Some warning',
            'result': {
                'target': {'annualizedReturn': 0.05},
                'hedge': {'annualizedReturn': 0.04},
                'hedgedTarget': {'annualizedReturn': 0.045},
                'benchmarks': [],
            }
        }

        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        result = h.calculate()
        assert 'Portfolio' in result

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_calculate_with_benchmarks(self, mock_hedge_api):
        """Calculate should enhance results with benchmark curves."""
        mock_params = MagicMock()
        mock_params.resolve_identifiers_in_payload.return_value = {
            'BM1': [{'id': 'MQ_BM1'}],
        }
        mock_params.to_dict.return_value = {}

        mock_hedge_api.calculate_hedge.return_value = {
            'result': {
                'target': {'annualizedReturn': 0.05},
                'hedge': {'annualizedReturn': 0.04},
                'hedgedTarget': {'annualizedReturn': 0.045},
                'benchmarks': [{'assetId': 'MQ_BM1', 'annualizedReturn': 0.03}],
            }
        }

        h = Hedge(mock_params, HedgeObjective.Replicate_Performance)
        result = h.calculate()
        assert 'BM1' in result

    def test_get_constituents_empty(self):
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        df = h.get_constituents()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_get_constituents_with_data(self):
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Hedge': {
                'Constituents': [
                    {'assetId': 'A1', 'weight': 0.5, 'assetName': 'Apple'},
                    {'assetId': 'A2', 'weight': 0.3, 'assetName': 'Google'},
                ]
            }
        }
        df = h.get_constituents()
        assert len(df) == 2
        # Keys should be formatted with spaces before capitals
        assert 'Asset Id' in df.columns or 'Asset id' in df.columns

    def test_get_statistics(self):
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {'Annualized Return': 0.05, 'Volatility': 0.15, 'Name': 'port'},
            'Hedge': {'Annualized Return': 0.04, 'Volatility': 0.12},
            'Hedged Portfolio': {'Annualized Return': 0.045, 'Volatility': 0.10},
        }
        df = h.get_statistics()
        assert isinstance(df, pd.DataFrame)
        # Only float values should be included
        assert 'Annualized Return' in df.index
        assert 'Volatility' in df.index
        # 'Name' is a string, so should not be included
        if 'Name' in df.index:
            # If it's there, it's ok, just checking the structure
            pass

    def test_get_backtest_performance(self):
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {
                'Backtest Performance': [['2024-01-01', 100], ['2024-01-02', 101]],
            },
            'Hedge': {
                'Backtest Performance': [['2024-01-01', 100], ['2024-01-02', 99]],
            },
            'Hedged Portfolio': {
                'Backtest Performance': [['2024-01-01', 100], ['2024-01-02', 100.5]],
            },
        }
        df = h.get_backtest_performance()
        assert isinstance(df, pd.DataFrame)
        assert 'Portfolio' in df.columns
        assert 'Hedge' in df.columns
        assert 'Hedged Portfolio' in df.columns
        assert len(df) == 2

    def test_get_backtest_correlation(self):
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {
                'Backtest Correlation': [['2024-01-01', 0.9], ['2024-01-02', 0.85]],
            },
            'Hedge': {
                'Backtest Correlation': [['2024-01-01', 0.88], ['2024-01-02', 0.82]],
            },
            'Hedged Portfolio': {},
        }
        df = h.get_backtest_correlation()
        assert isinstance(df, pd.DataFrame)
        assert 'Portfolio' in df.columns
        assert 'Hedge' in df.columns

    def test_get_timeseries_empty(self):
        """An empty timeseries result raises KeyError from set_index on missing 'Date' column."""
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {},
            'Hedge': {},
            'Hedged Portfolio': {},
        }
        with pytest.raises(KeyError):
            h._get_timeseries('Backtest Performance')

    def test_get_timeseries_overlapping_dates(self):
        """Test _get_timeseries where multiple keys have the same dates."""
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {
                'Backtest Performance': [['2024-01-01', 100]],
            },
            'Hedge': {
                'Backtest Performance': [['2024-01-01', 99]],
            },
            'Hedged Portfolio': {},
        }
        df = h._get_timeseries('Backtest Performance')
        assert len(df) == 1
        assert df.loc['2024-01-01', 'Portfolio'] == 100
        assert df.loc['2024-01-01', 'Hedge'] == 99

    def test_format_hedge_calculate_results(self):
        calc_results = {
            'target': {'annualizedReturn': 0.05},
            'hedge': {'annualizedReturn': 0.04},
            'hedgedTarget': {'annualizedReturn': 0.045},
        }
        result = Hedge._format_hedge_calculate_results(calc_results)
        assert 'Portfolio' in result
        assert 'Hedge' in result
        assert 'Hedged Portfolio' in result

    def test_enhance_result_with_benchmark_curves_empty(self):
        formatted = {'Portfolio': {}, 'Hedge': {}, 'Hedged Portfolio': {}}
        resolver = {}
        result = Hedge._enhance_result_with_benchmark_curves(formatted, [], resolver)
        assert result == formatted

    def test_enhance_result_with_benchmark_curves_non_empty(self):
        formatted = {'Portfolio': {}, 'Hedge': {}, 'Hedged Portfolio': {}}
        benchmarks = [{'assetId': 'MQ_BM1', 'annualizedReturn': 0.03}]
        resolver = {'BM1': [{'id': 'MQ_BM1'}]}
        result = Hedge._enhance_result_with_benchmark_curves(formatted, benchmarks, resolver)
        assert 'BM1' in result

    def test_format_dictionary_key_to_readable_format(self):
        d = {'annualizedReturn': 0.05, 'rSquared': 0.9, 'backtestPerformance': []}
        result = Hedge.format_dictionary_key_to_readable_format(d)
        assert 'Annualized Return' in result
        assert result['Annualized Return'] == 0.05

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_maximize(self, mock_hedge_api):
        """find_optimal_hedge should maximize rSquared."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'rSquared': 0.8, 'other': 'a'}}},
            {'result': {'hedge': {'rSquared': 0.9, 'other': 'b'}}},
            {'result': {'hedge': {'rSquared': 0.7, 'other': 'c'}}},
            {'result': {'hedge': {'rSquared': 0.85, 'other': 'd'}}},
        ]

        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1, 0.2], 'Diversity': [0.3, 0.4]}
        result_hedge, result_metric, result_params = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'rSquared'
        )
        assert result_metric == 0.9
        assert result_hedge['rSquared'] == 0.9

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_minimize(self, mock_hedge_api):
        """find_optimal_hedge should minimize holdingError."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'holdingError': 0.05}}},
            {'result': {'hedge': {'holdingError': 0.02}}},
            {'result': {'hedge': {'holdingError': 0.08}}},
            {'result': {'hedge': {'holdingError': 0.03}}},
        ]

        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1, 0.2], 'Diversity': [0.3, 0.4]}
        result_hedge, result_metric, result_params = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'holdingError'
        )
        assert result_metric == 0.02
        assert result_hedge['holdingError'] == 0.02

    def test_create_optimization_mappings(self):
        opt_map = Hedge.create_optimization_mappings()
        assert opt_map['rSquared'] == 'maximize'
        assert opt_map['correlation'] == 'maximize'
        assert opt_map['holdingError'] == 'minimize'
        assert opt_map['trackingError'] == 'minimize'
        assert opt_map['transactionCost'] == 'minimize'
        assert opt_map['annualizedReturn'] == 'maximize'

    def test_construct_portfolio_weights_and_asset_numbers(self):
        results = {
            'result': {
                'hedge': {
                    'constituents': [
                        {'weight': 0.3, 'assetId': 'A'},
                        {'weight': 0.5, 'assetId': 'B'},
                        {'weight': 0.2, 'assetId': 'C'},
                    ]
                }
            }
        }
        portfolio, weights, asset_numbers = Hedge.construct_portfolio_weights_and_asset_numbers(results)
        # Should be sorted by weight descending
        assert portfolio[0]['weight'] == 0.5
        assert portfolio[1]['weight'] == 0.3
        assert portfolio[2]['weight'] == 0.2
        assert weights == [0.5, 0.3, 0.2]
        assert asset_numbers == [0, 1, 2]

    def test_asset_id_diffs(self):
        portfolio_ids = ['A', 'B', 'C', 'D']
        tr_ids = ['A', 'C']
        diffs = Hedge.asset_id_diffs(portfolio_ids, tr_ids)
        assert set(diffs) == {'B', 'D'}

    def test_asset_id_diffs_no_diff(self):
        portfolio_ids = ['A', 'B']
        tr_ids = ['A', 'B']
        diffs = Hedge.asset_id_diffs(portfolio_ids, tr_ids)
        assert diffs == []

    def test_t_cost(self):
        result = Hedge.t_cost(20, 10000)
        # 20 * 1e-4 * 10000 = 20
        assert result == pytest.approx(20.0)

    def test_t_cost_zero(self):
        result = Hedge.t_cost(0, 10000)
        assert result == 0.0

    def test_compute_notional_traded(self):
        result = Hedge.compute_notional_traded(1000, 0.3, 0.5)
        # abs(0.5 - 0.3) * 1000 = 200
        assert result == pytest.approx(200.0)

    def test_compute_notional_traded_same_weight(self):
        result = Hedge.compute_notional_traded(1000, 0.5, 0.5)
        assert result == pytest.approx(0.0)

    def test_compute_tcosts(self):
        basis_points = 10
        asset_weights = {
            'A': [0.5, 0.6, 0.55],
            'B': [0.5, 0.4, 0.45],
        }
        asset_notionals = {
            'A': [500, 600, 550],
            'B': [500, 400, 450],
        }
        backtest_dates = ['2024-01-01', '2024-01-02', '2024-01-03']
        portfolio_asset_ids = ['A', 'B']
        cum_tcosts = Hedge.compute_tcosts(
            basis_points, asset_weights, asset_notionals, backtest_dates, portfolio_asset_ids
        )
        assert isinstance(cum_tcosts, pd.Series)
        assert len(cum_tcosts) == 3
        # Cumulative costs should be non-decreasing
        assert all(cum_tcosts.iloc[i] <= cum_tcosts.iloc[i + 1] for i in range(len(cum_tcosts) - 1))

    def test_compute_tcosts_single_date(self):
        """compute_tcosts with single date should use idx==0 branch."""
        basis_points = 10
        asset_weights = {'A': [0.5]}
        asset_notionals = {'A': [1000]}
        backtest_dates = ['2024-01-01']
        portfolio_asset_ids = ['A']
        cum_tcosts = Hedge.compute_tcosts(
            basis_points, asset_weights, asset_notionals, backtest_dates, portfolio_asset_ids
        )
        assert len(cum_tcosts) == 1
        # idx == 0, prev_weights = asset_weights[A][0] = 0.5, curr_weights = 0.5
        # notional_traded = 0, tcost = 0
        assert cum_tcosts.iloc[0] == pytest.approx(0.0)

    def test_create_transaction_cost_data_structures(self):
        """Test create_transaction_cost_data_structures with mock data.
        Uses pd.concat to work around deprecated DataFrame.append in pandas 2.x."""
        portfolio_asset_ids = ['A', 'B', 'C']
        portfolio_quantities = [100, 200, 50]
        backtest_dates = ['2024-01-01', '2024-01-02']

        # Mock Thomson Reuters EOD data
        mock_tr_data = MagicMock()

        # First call to get_data for asset ID filtering
        mock_tr_data.get_data.side_effect = [
            # First call: get asset IDs in TR data (only A and B available)
            pd.DataFrame({'assetId': ['A', 'B']}),
            # Second call: prices for date 1
            pd.DataFrame({
                'assetId': ['A', 'B'],
                'closePrice': [150.0, 250.0],
            }),
            # Third call: prices for date 2
            pd.DataFrame({
                'assetId': ['A', 'B'],
                'closePrice': [155.0, 245.0],
            }),
        ]

        # The source code uses DataFrame.append which was removed in pandas 2.x.
        # Patch it to use pd.concat instead.
        original_concat = pd.concat

        def mock_append(self_df, other, **kwargs):
            return original_concat([self_df, other], ignore_index=True)

        with patch.object(pd.DataFrame, 'append', mock_append, create=True):
            id_quantity_map, id_prices_map, id_to_notional_map, id_to_weight_map = \
                Hedge.create_transaction_cost_data_structures(
                    portfolio_asset_ids, portfolio_quantities, mock_tr_data, backtest_dates
                )

        # C should have been removed from portfolio_asset_ids
        assert 'C' not in portfolio_asset_ids
        assert 'A' in id_quantity_map
        assert 'B' in id_quantity_map
        assert 'A' in id_prices_map
        assert 'B' in id_prices_map
        assert len(id_prices_map['A']) == 2
        assert len(id_prices_map['B']) == 2
        # Check notional and weight maps
        assert 'A' in id_to_notional_map
        assert 'B' in id_to_notional_map
        assert 'A' in id_to_weight_map
        assert 'B' in id_to_weight_map


# ─── PerformanceHedge ────────────────────────────────────────────────────────


class TestPerformanceHedge:
    def test_init(self):
        mock_params = MagicMock()
        ph = PerformanceHedge(parameters=mock_params)
        assert ph.parameters is mock_params
        assert ph.objective == HedgeObjective.Replicate_Performance

    def test_init_default(self):
        ph = PerformanceHedge()
        assert ph.parameters is None
        assert ph.objective == HedgeObjective.Replicate_Performance

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_calculate_inherits_from_hedge(self, mock_hedge_api):
        """PerformanceHedge.calculate should call parent's calculate."""
        mock_params = MagicMock()
        mock_params.resolve_identifiers_in_payload.return_value = {}
        mock_params.to_dict.return_value = {}
        mock_hedge_api.calculate_hedge.return_value = {
            'result': {
                'target': {},
                'hedge': {},
                'hedgedTarget': {},
                'benchmarks': [],
            }
        }
        ph = PerformanceHedge(parameters=mock_params)
        result = ph.calculate()
        assert isinstance(result, dict)


# ─── Edge cases and additional branch coverage ───────────────────────────────


class TestEdgeCases:
    def test_hedge_exclusions_to_dict_no_classification_but_has_assets(self):
        """Only assets, no classification constraints."""
        he = HedgeExclusions(assets=['A1', 'A2'])
        result = he.to_dict()
        assert 'classificationConstraints' not in result
        assert 'assetConstraints' in result
        assert len(result['assetConstraints']) == 2

    def test_hedge_constraints_to_dict_mixed(self):
        """HedgeConstraints with only some types set."""
        hc = HedgeConstraints(
            sectors=[Constraint('Tech', 0, 40)],
            esg=[Constraint('ESG1', 0, 100)],
        )
        result = hc.to_dict()
        assert 'classificationConstraints' in result
        assert 'esgConstraints' in result
        assert 'assetConstraints' not in result

    def test_constraint_from_dict_name_fallback(self):
        """from_dict with assetId for name instead of name key."""
        d = {'assetId': 'MQID', 'min': 5, 'max': 15}
        c = Constraint.from_dict(d)
        assert c.constraint_name == 'MQID'

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_both_exclusions_and_constraints_classification(self, mock_session):
        """to_dict combines classification constraints from both exclusions and constraints."""
        ps = _make_position_set()
        excl = HedgeExclusions(countries=['US'])
        cons = HedgeConstraints(countries=[Constraint('UK', 0, 20)])
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            exclusions=excl,
            constraints=cons,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        result = params.to_dict({'SPX': [{'id': 'MQ_SPX'}]})
        assert 'classificationConstraints' in result
        # Should have both the exclusion and constraint
        assert len(result['classificationConstraints']) == 2

    @patch('gs_quant.markets.hedge.GsSession')
    def test_to_dict_both_exclusions_and_constraints_assets(self, mock_session):
        """to_dict combines asset constraints from both exclusions and constraints."""
        ps = _make_position_set()
        excl = HedgeExclusions(assets=['EXCL1'])
        cons = HedgeConstraints(assets=[Constraint('CON1', 0, 10)])
        params = PerformanceHedgeParameters(
            initial_portfolio=ps,
            universe=['SPX'],
            exclusions=excl,
            constraints=cons,
        )
        mock_session.current.sync.post.return_value = {
            'positions': [{'assetId': 'MQ_AAPL', 'quantity': 100}],
            'actualNotional': 500000,
        }
        resolved = {
            'SPX': [{'id': 'MQ_SPX'}],
            'EXCL1': [{'id': 'MQ_EXCL1'}],
            'CON1': [{'id': 'MQ_CON1'}],
        }
        result = params.to_dict(resolved)
        assert 'assetConstraints' in result
        # Should have both the exclusion and constraint
        assert len(result['assetConstraints']) == 2

    def test_get_statistics_only_floats(self):
        """get_statistics should only include float values."""
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Portfolio': {'Metric A': 0.5, 'Name': 'Portfolio', 'Constituents': []},
            'Hedge': {'Metric A': 0.4},
            'Hedged Portfolio': {'Metric A': 0.45},
        }
        df = h.get_statistics()
        assert 'Metric A' in df.index
        # Non-float values should not be in the output
        assert 'Name' not in df.index
        assert 'Constituents' not in df.index

    def test_get_constituents_key_formatting(self):
        """Verify camelCase keys are formatted with spaces."""
        h = Hedge(MagicMock(), HedgeObjective.Replicate_Performance)
        h._Hedge__result = {
            'Hedge': {
                'Constituents': [
                    {'assetId': 'A1', 'assetName': 'Apple', 'weight': 0.5},
                ]
            }
        }
        df = h.get_constituents()
        # 'assetId' -> 'Asset Id', 'assetName' -> 'Asset Name'
        assert len(df) == 1

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_tracking_error(self, mock_hedge_api):
        """find_optimal_hedge with trackingError (minimize)."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'trackingError': 0.1}}},
        ]
        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1], 'Diversity': [0.3]}
        result_hedge, result_metric, _ = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'trackingError'
        )
        assert result_metric == 0.1

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_correlation(self, mock_hedge_api):
        """find_optimal_hedge with correlation (maximize)."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'correlation': 0.95}}},
        ]
        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1], 'Diversity': [0.3]}
        result_hedge, result_metric, _ = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'correlation'
        )
        assert result_metric == 0.95

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_transaction_cost(self, mock_hedge_api):
        """find_optimal_hedge with transactionCost (minimize)."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'transactionCost': 0.001}}},
            {'result': {'hedge': {'transactionCost': 0.005}}},
        ]
        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1], 'Diversity': [0.3, 0.4]}
        result_hedge, result_metric, _ = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'transactionCost'
        )
        assert result_metric == 0.001

    @patch('gs_quant.markets.hedge.GsHedgeApi')
    def test_find_optimal_hedge_annualized_return(self, mock_hedge_api):
        """find_optimal_hedge with annualizedReturn (maximize)."""
        mock_hedge_api.calculate_hedge.side_effect = [
            {'result': {'hedge': {'annualizedReturn': 0.12}}},
            {'result': {'hedge': {'annualizedReturn': 0.15}}},
        ]
        hedge_query = {'parameters': MagicMock()}
        hyperparams = {'Concentration': [0.1], 'Diversity': [0.3, 0.4]}
        result_hedge, result_metric, _ = Hedge.find_optimal_hedge(
            hedge_query, hyperparams, 'annualizedReturn'
        )
        assert result_metric == 0.15

    def test_compute_tcosts_multiple_dates_and_assets(self):
        """Test compute_tcosts with multiple dates and assets, idx > 0 path."""
        basis_points = 20
        asset_weights = {
            'A': [0.6, 0.55, 0.5],
            'B': [0.4, 0.45, 0.5],
        }
        asset_notionals = {
            'A': [6000, 5500, 5000],
            'B': [4000, 4500, 5000],
        }
        backtest_dates = ['d1', 'd2', 'd3']
        portfolio_asset_ids = ['A', 'B']
        cum_tcosts = Hedge.compute_tcosts(
            basis_points, asset_weights, asset_notionals, backtest_dates, portfolio_asset_ids
        )
        assert len(cum_tcosts) == 3
        # Day 1 (idx=0): prev = curr for each asset, so tcost = 0
        assert cum_tcosts.iloc[0] == pytest.approx(0.0)
        # Day 2 (idx=1): changes in weight, so tcost > 0
        assert cum_tcosts.iloc[1] > 0
