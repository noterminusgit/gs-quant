"""
Copyright 2024 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
from enum import Enum
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.entities.entity import (
    Entity,
    EntityType,
    EntityKey,
    EntityIdentifier,
    Country,
    Subdivision,
    KPI,
    RiskModelEntity,
    PositionedEntity,
    ScenarioCalculationType,
    ScenarioCalculationMeasure,
)
from gs_quant.common import PositionType, Currency
from gs_quant.data import DataFrequency, DataMeasure
from gs_quant.errors import MqError, MqValueError
from gs_quant.markets.report import ReturnFormat
from gs_quant.target.reports import ReportStatus, ReportType


# ---------------------------------------------------------------------------
# Helpers: Concrete subclasses for abstract classes
# ---------------------------------------------------------------------------


class ConcreteEntity(Entity):
    """Concrete subclass of Entity for testing."""

    @property
    def data_dimension(self) -> str:
        return 'testDimension'

    @classmethod
    def entity_type(cls) -> EntityType:
        return EntityType.COUNTRY


class ConcretePositionedEntity(PositionedEntity):
    """Concrete subclass of PositionedEntity for testing."""

    def __init__(self, id_: str, entity_type: EntityType):
        super().__init__(id_, entity_type)


# ---------------------------------------------------------------------------
# Tests for EntityType, EntityKey, EntityIdentifier, ScenarioCalculation enums
# ---------------------------------------------------------------------------


class TestEnums:
    def test_entity_type_values(self):
        assert EntityType.ASSET.value == 'asset'
        assert EntityType.BACKTEST.value == 'backtest'
        assert EntityType.COUNTRY.value == 'country'
        assert EntityType.HEDGE.value == 'hedge'
        assert EntityType.KPI.value == 'kpi'
        assert EntityType.PORTFOLIO.value == 'portfolio'
        assert EntityType.REPORT.value == 'report'
        assert EntityType.RISK_MODEL.value == 'risk_model'
        assert EntityType.SUBDIVISION.value == 'subdivision'
        assert EntityType.DATASET.value == 'dataset'
        assert EntityType.SCENARIO.value == 'scenario'

    def test_entity_key(self):
        key = EntityKey(id_='abc123', entity_type=EntityType.ASSET)
        assert key.id_ == 'abc123'
        assert key.entity_type == EntityType.ASSET

    def test_entity_identifier_is_enum(self):
        assert issubclass(EntityIdentifier, Enum)

    def test_scenario_calculation_type(self):
        assert ScenarioCalculationType.FACTOR_SCENARIO.value == "Factor Scenario"

    def test_scenario_calculation_measure(self):
        assert ScenarioCalculationMeasure.SUMMARY.value == "Summary"
        assert ScenarioCalculationMeasure.ESTIMATED_FACTOR_PNL.value == "Factor Pnl"
        assert ScenarioCalculationMeasure.ESTIMATED_PNL_BY_SECTOR.value == "By Sector Pnl Aggregations"
        assert ScenarioCalculationMeasure.ESTIMATED_PNL_BY_REGION.value == "By Region Pnl Aggregations"
        assert ScenarioCalculationMeasure.ESTIMATED_PNL_BY_DIRECTION.value == "By Direction Pnl Aggregations"
        assert ScenarioCalculationMeasure.ESTIMATED_PNL_BY_ASSET.value == "By Asset Pnl"


# ---------------------------------------------------------------------------
# Tests for Entity base class
# ---------------------------------------------------------------------------


class TestEntity:
    def test_init_and_properties(self):
        entity = ConcreteEntity('id1', EntityType.COUNTRY, entity={'name': 'US'})
        assert entity.get_marquee_id() == 'id1'
        assert entity.get_entity() == {'name': 'US'}

    def test_get_unique_entity_key(self):
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        key = entity.get_unique_entity_key()
        assert isinstance(key, EntityKey)
        assert key.id_ == 'id1'
        assert key.entity_type == EntityType.COUNTRY

    def test_data_dimension_property(self):
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        assert entity.data_dimension == 'testDimension'

    def test_entity_to_endpoint_mapping(self):
        expected = {
            EntityType.ASSET: 'assets',
            EntityType.COUNTRY: 'countries',
            EntityType.SUBDIVISION: 'countries/subdivisions',
            EntityType.KPI: 'kpis',
            EntityType.PORTFOLIO: 'portfolios',
            EntityType.RISK_MODEL: 'risk/models',
            EntityType.DATASET: 'data/datasets',
        }
        for et, endpoint in expected.items():
            assert Entity._entity_to_endpoint[et] == endpoint

    # --- Entity.get() ---

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_with_mqid_no_entity_type(self, mock_session):
        """Entity.get with id_type MQID and no explicit entity_type uses cls.entity_type()."""
        mock_session.current.sync.get.return_value = {'id': 'c1', 'name': 'TestCountry'}
        result = Country.get('c1', Country.Identifier.MARQUEE_ID)
        mock_session.current.sync.get.assert_called_once_with('/countries/c1')
        assert isinstance(result, Country)

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_with_non_mqid(self, mock_session):
        """Entity.get with non-MQID id_type queries by parameter."""
        mock_session.current.sync.get.return_value = {'results': [{'id': 'c1', 'name': 'US'}]}
        result = Country.get('US', Country.Identifier.NAME)
        mock_session.current.sync.get.assert_called_once_with('/countries?name=US')
        assert isinstance(result, Country)

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_with_explicit_entity_type_enum(self, mock_session):
        """Entity.get with entity_type as EntityType enum."""
        mock_session.current.sync.get.return_value = {'id': 'k1', 'name': 'TestKPI'}
        result = KPI.get('k1', KPI.Identifier.MARQUEE_ID, entity_type=EntityType.KPI)
        mock_session.current.sync.get.assert_called_once_with('/kpis/k1')
        assert isinstance(result, KPI)

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_with_explicit_entity_type_string(self, mock_session):
        """Entity.get with entity_type as string."""
        mock_session.current.sync.get.return_value = {'id': 's1', 'name': 'TestSub'}
        result = Subdivision.get('s1', Subdivision.Identifier.MARQUEE_ID, entity_type='subdivision')
        mock_session.current.sync.get.assert_called_once_with('/countries/subdivisions/s1')
        assert isinstance(result, Subdivision)

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_asset_type_delegates_to_security_master(self, mock_session):
        """Entity.get with entity_type='asset' uses SecurityMaster."""
        with patch('gs_quant.markets.securities.SecurityMaster') as mock_sm:
            mock_sm.get_asset.return_value = 'mock_asset'
            from gs_quant.markets.securities import AssetIdentifier
            result = ConcreteEntity.get('a1', 'MQID', entity_type=EntityType.ASSET)
            mock_sm.get_asset.assert_called_once_with('a1', AssetIdentifier.MARQUEE_ID)
            assert result == 'mock_asset'

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_returns_none_when_no_result(self, mock_session):
        """Entity.get returns None when no result found."""
        mock_session.current.sync.get.return_value = {'results': []}
        result = Country.get('nonexistent', 'name')
        assert result is None

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_with_id_type_as_string(self, mock_session):
        """Entity.get with id_type as plain string (not Enum)."""
        mock_session.current.sync.get.return_value = {'id': 'c1', 'name': 'US'}
        result = Country.get('c1', 'MQID')
        mock_session.current.sync.get.assert_called_once_with('/countries/c1')

    # --- Entity._get_entity_from_type() ---

    def test_get_entity_from_type_country(self):
        result = Entity._get_entity_from_type({'id': 'c1', 'name': 'US'}, EntityType.COUNTRY)
        assert isinstance(result, Country)
        assert result.get_marquee_id() == 'c1'

    def test_get_entity_from_type_kpi(self):
        result = Entity._get_entity_from_type({'id': 'k1', 'name': 'GDP'}, EntityType.KPI)
        assert isinstance(result, KPI)
        assert result.get_marquee_id() == 'k1'

    def test_get_entity_from_type_subdivision(self):
        result = Entity._get_entity_from_type({'id': 's1', 'name': 'California'}, EntityType.SUBDIVISION)
        assert isinstance(result, Subdivision)
        assert result.get_marquee_id() == 's1'

    def test_get_entity_from_type_risk_model(self):
        result = Entity._get_entity_from_type({'id': 'r1', 'name': 'AxiomaModel'}, EntityType.RISK_MODEL)
        assert isinstance(result, RiskModelEntity)
        assert result.get_marquee_id() == 'r1'

    def test_get_entity_from_type_other_returns_none(self):
        result = Entity._get_entity_from_type({'id': 'p1'}, EntityType.PORTFOLIO)
        assert result is None

    def test_get_entity_from_type_uses_cls_entity_type_when_none(self):
        result = Country._get_entity_from_type({'id': 'c2', 'name': 'UK'})
        assert isinstance(result, Country)

    # --- Entity.get_data_coordinate() ---

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_daily(self, mock_providers):
        mock_providers.return_value = {'measure1': {DataFrequency.DAILY: 'ds1'}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        coord = entity.get_data_coordinate('measure1', frequency=DataFrequency.DAILY)
        assert coord.dataset_id == 'ds1'
        assert coord.measure == 'measure1'
        assert coord.frequency == DataFrequency.DAILY

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_real_time(self, mock_providers):
        mock_providers.return_value = {'measure1': {DataFrequency.REAL_TIME: 'ds_rt'}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        coord = entity.get_data_coordinate('measure1', frequency=DataFrequency.REAL_TIME)
        assert coord.dataset_id == 'ds_rt'
        assert coord.frequency == DataFrequency.REAL_TIME

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_with_data_measure_enum(self, mock_providers):
        mock_measure = MagicMock(spec=DataMeasure)
        mock_measure.value = 'myMeasure'
        mock_providers.return_value = {'myMeasure': {DataFrequency.DAILY: 'ds2'}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        coord = entity.get_data_coordinate(mock_measure, frequency=DataFrequency.DAILY)
        assert coord.measure == 'myMeasure'

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_with_dimensions(self, mock_providers):
        mock_providers.return_value = {'m': {DataFrequency.DAILY: 'ds3'}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        coord = entity.get_data_coordinate('m', dimensions={'extra': 'val'}, frequency=DataFrequency.DAILY)
        assert coord.dimensions['extra'] == 'val'
        assert coord.dimensions['testDimension'] == 'id1'

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_returns_none_for_unknown_frequency(self, mock_providers):
        """When frequency is neither DAILY nor REAL_TIME, returns None (implicit)."""
        mock_providers.return_value = {'m': {}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        # Use ANY frequency which is neither DAILY nor REAL_TIME
        result = entity.get_data_coordinate('m', frequency=DataFrequency.ANY)
        assert result is None

    @patch('gs_quant.entities.entity.GsDataApi.get_data_providers')
    def test_get_data_coordinate_daily_missing_dataset(self, mock_providers):
        """When DAILY frequency but no dataset available, dataset_id is None."""
        mock_providers.return_value = {'m': {}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        coord = entity.get_data_coordinate('m', frequency=DataFrequency.DAILY)
        assert coord.dataset_id is None

    # --- Entity.get_entitlements() ---

    @patch('gs_quant.entities.entity.Entitlements')
    def test_get_entitlements_success(self, mock_entitlements_cls):
        entity_data = {'entitlements': {'edit': ('guid:userId',)}}
        entity = ConcreteEntity('id1', EntityType.COUNTRY, entity=entity_data)
        mock_entitlements_cls.from_dict.return_value = 'mock_entitlements'
        result = entity.get_entitlements()
        mock_entitlements_cls.from_dict.assert_called_once_with({'edit': ('guid:userId',)})
        assert result == 'mock_entitlements'

    def test_get_entitlements_raises_when_none(self):
        entity = ConcreteEntity('id1', EntityType.COUNTRY, entity={'name': 'test'})
        with pytest.raises(ValueError, match='This entity does not have entitlements'):
            entity.get_entitlements()

    def test_get_entity_none(self):
        entity = ConcreteEntity('id1', EntityType.COUNTRY)
        assert entity.get_entity() is None


# ---------------------------------------------------------------------------
# Tests for Country
# ---------------------------------------------------------------------------


class TestCountry:
    def test_entity_type(self):
        assert Country.entity_type() == EntityType.COUNTRY

    def test_data_dimension(self):
        c = Country('c1')
        assert c.data_dimension == 'countryId'

    def test_get_name(self):
        c = Country('c1', entity={'name': 'United States'})
        assert c.get_name() == 'United States'

    def test_get_region(self):
        c = Country('c1', entity={'region': 'Americas'})
        assert c.get_region() == 'Americas'

    def test_get_sub_region(self):
        c = Country('c1', entity={'subRegion': 'Northern America'})
        assert c.get_sub_region() == 'Northern America'

    def test_get_region_code(self):
        c = Country('c1', entity={'regionCode': '019'})
        assert c.get_region_code() == '019'

    def test_get_sub_region_code(self):
        c = Country('c1', entity={'subRegionCode': '021'})
        assert c.get_sub_region_code() == '021'

    def test_get_alpha3(self):
        c = Country('c1', entity={'xref': {'alpha3': 'USA'}})
        assert c.get_alpha3() == 'USA'

    def test_get_bbid(self):
        c = Country('c1', entity={'xref': {'bbid': 'US'}})
        assert c.get_bbid() == 'US'

    def test_get_alpha2(self):
        c = Country('c1', entity={'xref': {'alpha2': 'US'}})
        assert c.get_alpha2() == 'US'

    def test_get_country_code(self):
        c = Country('c1', entity={'xref': {'countryCode': '840'}})
        assert c.get_country_code() == '840'

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_by_identifier(self, mock_session):
        mock_session.current.sync.get.return_value = {'id': 'c1', 'name': 'US'}
        Country.get_by_identifier('c1', Country.Identifier.MARQUEE_ID)

    def test_get_name_returns_none_for_missing(self):
        c = Country('c1', entity={})
        assert c.get_name() is None

    def test_properties_with_none_entity(self):
        c = Country('c1')
        assert c.get_name() is None


# ---------------------------------------------------------------------------
# Tests for Subdivision
# ---------------------------------------------------------------------------


class TestSubdivision:
    def test_entity_type(self):
        assert Subdivision.entity_type() == EntityType.SUBDIVISION

    def test_data_dimension(self):
        s = Subdivision('s1')
        assert s.data_dimension == 'subdivisionId'

    def test_get_name(self):
        s = Subdivision('s1', entity={'name': 'California'})
        assert s.get_name() == 'California'

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_by_identifier(self, mock_session):
        mock_session.current.sync.get.return_value = {'id': 's1', 'name': 'California'}
        Subdivision.get_by_identifier('s1', Subdivision.Identifier.MARQUEE_ID)


# ---------------------------------------------------------------------------
# Tests for KPI
# ---------------------------------------------------------------------------


class TestKPI:
    def test_entity_type(self):
        assert KPI.entity_type() == EntityType.KPI

    def test_data_dimension(self):
        k = KPI('k1')
        assert k.data_dimension == 'kpiId'

    def test_get_name(self):
        k = KPI('k1', entity={'name': 'GDP'})
        assert k.get_name() == 'GDP'

    def test_get_category(self):
        k = KPI('k1', entity={'category': 'Economic'})
        assert k.get_category() == 'Economic'

    def test_get_sub_category(self):
        k = KPI('k1', entity={'subCategory': 'Growth'})
        assert k.get_sub_category() == 'Growth'

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_by_identifier(self, mock_session):
        mock_session.current.sync.get.return_value = {'id': 'k1', 'name': 'GDP'}
        KPI.get_by_identifier('k1', KPI.Identifier.MARQUEE_ID)


# ---------------------------------------------------------------------------
# Tests for RiskModelEntity
# ---------------------------------------------------------------------------


class TestRiskModelEntity:
    def test_entity_type(self):
        assert RiskModelEntity.entity_type() == EntityType.RISK_MODEL

    def test_data_dimension(self):
        r = RiskModelEntity('r1')
        assert r.data_dimension == 'riskModel'

    def test_get_name(self):
        r = RiskModelEntity('r1', entity={'name': 'AxiomaModel'})
        assert r.get_name() == 'AxiomaModel'

    def test_get_coverage(self):
        r = RiskModelEntity('r1', entity={'coverage': 'Global'})
        assert r.get_coverage() == 'Global'

    def test_get_term(self):
        r = RiskModelEntity('r1', entity={'term': 'Long'})
        assert r.get_term() == 'Long'

    def test_get_vendor(self):
        r = RiskModelEntity('r1', entity={'vendor': 'Axioma'})
        assert r.get_vendor() == 'Axioma'

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_by_identifier(self, mock_session):
        mock_session.current.sync.get.return_value = {'id': 'r1', 'name': 'Model'}
        RiskModelEntity.get_by_identifier('r1', RiskModelEntity.Identifier.MARQUEE_ID)


# ---------------------------------------------------------------------------
# Tests for PositionedEntity
# ---------------------------------------------------------------------------


class TestPositionedEntity:
    def test_id_property(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        assert pe.id == 'p1'

    def test_positioned_entity_type_property(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        assert pe.positioned_entity_type == EntityType.PORTFOLIO

    # --- get_entitlements ---

    @patch('gs_quant.entities.entity.Entitlements')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_entitlements_portfolio(self, mock_portfolio_api, mock_entitlements):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_response = MagicMock()
        mock_portfolio_api.get_portfolio.return_value = mock_response
        mock_entitlements.from_target.return_value = 'entitlements_obj'
        result = pe.get_entitlements()
        mock_portfolio_api.get_portfolio.assert_called_once_with('p1')
        mock_entitlements.from_target.assert_called_once_with(mock_response.entitlements)
        assert result == 'entitlements_obj'

    @patch('gs_quant.entities.entity.Entitlements')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_entitlements_asset(self, mock_asset_api, mock_entitlements):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_response = MagicMock()
        mock_asset_api.get_asset.return_value = mock_response
        mock_entitlements.from_target.return_value = 'entitlements_obj'
        result = pe.get_entitlements()
        mock_asset_api.get_asset.assert_called_once_with('a1')
        assert result == 'entitlements_obj'

    def test_get_entitlements_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_entitlements()

    # --- get_latest_position_set ---

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_latest_position_set_asset(self, mock_asset_api, mock_pos_set):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_latest_positions.return_value = 'positions'
        mock_pos_set.from_target.return_value = 'pos_set'
        result = pe.get_latest_position_set()
        mock_asset_api.get_latest_positions.assert_called_once_with('a1', PositionType.CLOSE)
        assert result == 'pos_set'

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_latest_position_set_portfolio(self, mock_portfolio_api, mock_pos_set):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio_api.get_latest_positions.return_value = 'positions'
        mock_pos_set.from_target.return_value = 'pos_set'
        result = pe.get_latest_position_set()
        mock_portfolio_api.get_latest_positions.assert_called_once_with(
            portfolio_id='p1', position_type='close'
        )
        assert result == 'pos_set'

    def test_get_latest_position_set_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_latest_position_set()

    # --- get_position_set_for_date ---

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_position_set_for_date_asset_with_results(self, mock_asset_api, mock_pos_set):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_asset_positions_for_date.return_value = ['pos1']
        mock_pos_set.from_target.return_value = 'pos_set'
        result = pe.get_position_set_for_date(dt.date(2023, 1, 1))
        assert result == 'pos_set'

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_position_set_for_date_asset_empty(self, mock_asset_api, mock_pos_set):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_asset_positions_for_date.return_value = []
        result = pe.get_position_set_for_date(dt.date(2023, 1, 1))
        # Should return a PositionSet with empty positions
        assert isinstance(result, type(mock_pos_set([], date=dt.date(2023, 1, 1))))

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_position_set_for_date_portfolio_with_result(self, mock_portfolio_api, mock_pos_set):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio_api.get_positions_for_date.return_value = 'response'
        mock_pos_set.from_target.return_value = 'pos_set'
        result = pe.get_position_set_for_date(dt.date(2023, 1, 1))
        assert result == 'pos_set'

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_position_set_for_date_portfolio_empty(self, mock_portfolio_api, mock_pos_set):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio_api.get_positions_for_date.return_value = None
        result = pe.get_position_set_for_date(dt.date(2023, 1, 1))
        assert result is None

    def test_get_position_set_for_date_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_position_set_for_date(dt.date(2023, 1, 1))

    # --- get_position_sets ---

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_position_sets_asset_with_results(self, mock_asset_api, mock_pos_set):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_asset_positions_for_dates.return_value = ['pos1', 'pos2']
        mock_pos_set.from_target.side_effect = lambda x: f'ps_{x}'
        result = pe.get_position_sets(dt.date(2023, 1, 1), dt.date(2023, 12, 31))
        assert result == ['ps_pos1', 'ps_pos2']

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_position_sets_asset_empty(self, mock_asset_api, mock_pos_set):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_asset_positions_for_dates.return_value = []
        result = pe.get_position_sets(dt.date(2023, 1, 1), dt.date(2023, 12, 31))
        assert result == []

    @patch('gs_quant.entities.entity.PositionSet')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_position_sets_portfolio(self, mock_portfolio_api, mock_pos_set):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio_api.get_positions.return_value = ['pos1']
        mock_pos_set.from_target.return_value = 'ps1'
        result = pe.get_position_sets(dt.date(2023, 1, 1), dt.date(2023, 12, 31))
        assert result == ['ps1']

    def test_get_position_sets_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_position_sets()

    # --- update_positions ---

    @patch('gs_quant.entities.entity.time')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_portfolio_with_quantities(self, mock_portfolio_api, mock_time):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio = MagicMock()
        mock_portfolio.currency = 'USD'
        mock_portfolio_api.get_portfolio.return_value = mock_portfolio

        pos = MagicMock()
        pos.positions = [MagicMock(quantity=100)]
        pos.to_target.return_value = 'target_pos'
        result = pe.update_positions([pos])
        mock_portfolio_api.update_positions.assert_called_once()
        mock_time.sleep.assert_called_once_with(3)

    @patch('gs_quant.entities.entity.time')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_portfolio_missing_quantities(self, mock_portfolio_api, mock_time):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio = MagicMock()
        mock_portfolio.currency = 'USD'
        mock_portfolio_api.get_portfolio.return_value = mock_portfolio

        position = MagicMock(quantity=None)
        pos_set = MagicMock()
        pos_set.positions = [position]
        pos_set.to_target.return_value = 'target_pos'
        pe.update_positions([pos_set])
        pos_set.price.assert_called_once_with('USD')

    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_portfolio_empty_list(self, mock_portfolio_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        result = pe.update_positions([])
        mock_portfolio_api.get_portfolio.assert_not_called()
        assert result is None

    def test_update_positions_not_implemented(self):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        with pytest.raises(NotImplementedError):
            pe.update_positions([MagicMock()])

    # --- get_positions_data ---

    @patch('gs_quant.entities.entity.GsIndexApi')
    def test_get_positions_data_asset(self, mock_index_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_index_api.get_positions_data.return_value = [{'data': 'val'}]
        result = pe.get_positions_data()
        assert result == [{'data': 'val'}]

    def test_get_positions_data_portfolio_raises(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        with pytest.raises(MqError, match='Please use the get_positions_data function'):
            pe.get_positions_data()

    def test_get_positions_data_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_positions_data()

    # --- get_last_positions_data ---

    @patch('gs_quant.entities.entity.GsIndexApi')
    def test_get_last_positions_data_asset(self, mock_index_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_index_api.get_last_positions_data.return_value = [{'data': 'val'}]
        result = pe.get_last_positions_data()
        assert result == [{'data': 'val'}]

    def test_get_last_positions_data_portfolio_raises(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        with pytest.raises(MqError, match='Please use the get_positions_data function'):
            pe.get_last_positions_data()

    def test_get_last_positions_data_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_last_positions_data()

    # --- get_position_dates ---

    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_position_dates_portfolio(self, mock_portfolio_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio_api.get_position_dates.return_value = (dt.date(2023, 1, 1),)
        result = pe.get_position_dates()
        assert result == (dt.date(2023, 1, 1),)

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_position_dates_asset(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_position_dates.return_value = (dt.date(2023, 1, 1),)
        result = pe.get_position_dates()
        assert result == (dt.date(2023, 1, 1),)

    def test_get_position_dates_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_position_dates()

    # --- get_reports ---

    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_reports_portfolio(self, mock_portfolio_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        # Create mock reports of different types
        perf_report = MagicMock()
        perf_report.type = ReportType.Portfolio_Performance_Analytics

        factor_risk_report = MagicMock()
        factor_risk_report.type = ReportType.Portfolio_Factor_Risk

        asset_factor_risk_report = MagicMock()
        asset_factor_risk_report.type = ReportType.Asset_Factor_Risk

        thematic_report = MagicMock()
        thematic_report.type = ReportType.Portfolio_Thematic_Analytics

        asset_thematic_report = MagicMock()
        asset_thematic_report.type = ReportType.Asset_Thematic_Analytics

        other_report = MagicMock()
        other_report.type = ReportType.Analytics

        mock_portfolio_api.get_reports.return_value = [
            perf_report, factor_risk_report, asset_factor_risk_report,
            thematic_report, asset_thematic_report, other_report
        ]

        with patch('gs_quant.entities.entity.PerformanceReport') as mock_perf, \
             patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr, \
             patch('gs_quant.entities.entity.ThematicReport') as mock_th, \
             patch('gs_quant.entities.entity.Report') as mock_report:
            mock_perf.from_target.return_value = 'perf'
            mock_fr.from_target.side_effect = lambda r: f'factor_{r.type.value}'
            mock_th.from_target.side_effect = lambda r: f'thematic_{r.type.value}'
            mock_report.from_target.return_value = 'other'

            result = pe.get_reports()
            assert len(result) == 6
            assert result[0] == 'perf'
            assert result[-1] == 'other'

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_reports_asset(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.get_reports.return_value = []
        result = pe.get_reports()
        assert result == []

    def test_get_reports_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_reports()

    # --- get_status_of_reports ---

    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_get_status_of_reports(self, mock_portfolio_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.type = ReportType.Analytics
        mock_report.name = 'Report1'
        mock_report.id = 'r1'
        mock_report.latest_execution_time = '2023-01-01'
        mock_report.latest_end_date = '2023-01-01'
        mock_report.status = 'done'
        mock_report.percentage_complete = 100
        mock_portfolio_api.get_reports.return_value = [mock_report]

        with patch('gs_quant.entities.entity.Report') as mock_report_cls:
            mock_report_cls.from_target.return_value = mock_report
            result = pe.get_status_of_reports()
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    # --- get_factor_risk_reports ---

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_reports_portfolio(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report_mock = MagicMock()
        report_mock.parameters.fx_hedged = True
        mock_report_api.get_reports.return_value = [report_mock]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = 'fr_report'
            result = pe.get_factor_risk_reports()
            assert result == ['fr_report']

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_reports_with_fx_hedged(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report1 = MagicMock()
        report1.parameters.fx_hedged = True
        report2 = MagicMock()
        report2.parameters.fx_hedged = False
        mock_report_api.get_reports.return_value = [report1, report2]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.side_effect = lambda r: r
            result = pe.get_factor_risk_reports(fx_hedged=True)
            assert len(result) == 1

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_reports_empty_raises(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report_api.get_reports.return_value = []
        with pytest.raises(MqError, match='no factor risk reports'):
            pe.get_factor_risk_reports()

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_reports_asset(self, mock_report_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        report_mock = MagicMock()
        report_mock.parameters.fx_hedged = True
        mock_report_api.get_reports.return_value = [report_mock]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = 'fr_report'
            result = pe.get_factor_risk_reports()
            assert result == ['fr_report']

    def test_get_factor_risk_reports_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_factor_risk_reports()

    # --- get_factor_risk_report ---

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_report_found(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = 'rm1'
        report.parameters.benchmark = None
        mock_report_api.get_reports.return_value = [report]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_risk_report(risk_model_id='rm1')
            assert result == report

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_report_no_match(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = 'rm1'
        report.parameters.benchmark = 'bm1'
        mock_report_api.get_reports.return_value = [report]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            with pytest.raises(MqError, match='no factor risk report'):
                pe.get_factor_risk_report(risk_model_id='rm1', benchmark_id='bm_wrong')

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_risk_report_multiple_match(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report1 = MagicMock()
        report1.parameters.fx_hedged = None
        report1.parameters.risk_model = 'rm1'
        report1.parameters.benchmark = None
        report2 = MagicMock()
        report2.parameters.fx_hedged = None
        report2.parameters.risk_model = 'rm1'
        report2.parameters.benchmark = None
        mock_report_api.get_reports.return_value = [report1, report2]

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.side_effect = lambda r: r
            with pytest.raises(MqError, match='more than one factor risk report'):
                pe.get_factor_risk_report(risk_model_id='rm1')

    # --- get_thematic_report ---

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_thematic_report_found(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.tags = None
        mock_report_api.get_reports.return_value = [report]

        with patch('gs_quant.entities.entity.ThematicReport') as mock_th:
            mock_th.from_target.return_value = 'thematic_report'
            result = pe.get_thematic_report()
            assert result == 'thematic_report'

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_thematic_report_empty(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report_api.get_reports.return_value = []
        with pytest.raises(MqError, match='no thematic analytics report'):
            pe.get_thematic_report()

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_thematic_report_asset(self, mock_report_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        report = MagicMock()
        report.parameters.tags = None
        mock_report_api.get_reports.return_value = [report]

        with patch('gs_quant.entities.entity.ThematicReport') as mock_th:
            mock_th.from_target.return_value = 'thematic_report'
            result = pe.get_thematic_report()
            assert result == 'thematic_report'

    def test_get_thematic_report_not_implemented(self):
        pe = ConcretePositionedEntity('x1', EntityType.HEDGE)
        with pytest.raises(NotImplementedError):
            pe.get_thematic_report()

    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_thematic_report_filters_by_tags(self, mock_report_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report_match = MagicMock()
        report_match.parameters.tags = {'key': 'val'}
        report_no_match = MagicMock()
        report_no_match.parameters.tags = None
        mock_report_api.get_reports.return_value = [report_match, report_no_match]

        with patch('gs_quant.entities.entity.ThematicReport') as mock_th:
            mock_th.from_target.return_value = 'thematic_report'
            result = pe.get_thematic_report(tags={'key': 'val'})
            assert result == 'thematic_report'

    # --- poll_report ---

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_done(self, mock_time, mock_report_cls):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.status = ReportStatus.done
        mock_report_cls.get.return_value = mock_report
        result = pe.poll_report('r1', timeout=60, step=15)
        assert result == ReportStatus.done

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_error(self, mock_time, mock_report_cls):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.status = ReportStatus.error
        mock_report_cls.get.return_value = mock_report
        with pytest.raises(MqError, match='has failed'):
            pe.poll_report('r1', timeout=60, step=15)

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_cancelled(self, mock_time, mock_report_cls):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.status = ReportStatus.cancelled
        mock_report_cls.get.return_value = mock_report
        result = pe.poll_report('r1', timeout=60, step=15)
        assert result == ReportStatus.cancelled

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_timeout(self, mock_time, mock_report_cls):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.status = ReportStatus.executing
        mock_report_cls.get.return_value = mock_report

        # Make datetime.now() advance past the end time on second call
        call_count = [0]
        original_now = dt.datetime.now

        def fake_now():
            call_count[0] += 1
            if call_count[0] <= 1:
                return original_now()
            return original_now() + dt.timedelta(seconds=99999)

        with patch('gs_quant.entities.entity.dt') as mock_dt:
            mock_dt.datetime.now.side_effect = fake_now
            mock_dt.timedelta = dt.timedelta
            with pytest.raises(MqError, match='taking longer than expected'):
                pe.poll_report('r1', timeout=1, step=15)

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_exception_from_get(self, mock_time, mock_report_cls):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report_cls.get.side_effect = Exception('connection error')
        with pytest.raises(MqError, match='Could not fetch report status'):
            pe.poll_report('r1', timeout=60, step=15)

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_clamps_timeout_and_step(self, mock_time, mock_report_cls):
        """Timeout > 1800 is clamped to 1800; step < 15 is clamped to 15."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report = MagicMock()
        mock_report.status = ReportStatus.done
        mock_report_cls.get.return_value = mock_report
        result = pe.poll_report('r1', timeout=9999, step=1)
        assert result == ReportStatus.done

    @patch('gs_quant.entities.entity.Report')
    @patch('gs_quant.entities.entity.time')
    def test_poll_report_executing_then_done(self, mock_time, mock_report_cls):
        """Report is executing first then done on second poll."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_report_executing = MagicMock()
        mock_report_executing.status = ReportStatus.executing
        mock_report_done = MagicMock()
        mock_report_done.status = ReportStatus.done
        mock_report_cls.get.side_effect = [mock_report_executing, mock_report_done]
        result = pe.poll_report('r1', timeout=600, step=15)
        assert result == ReportStatus.done

    # --- ESG methods ---

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_all_esg_data_defaults(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_esg_api.get_esg.return_value = {'data': 'value'}
        result = pe.get_all_esg_data()
        assert result == {'data': 'value'}
        call_kwargs = mock_esg_api.get_esg.call_args
        assert call_kwargs[1]['entity_id'] == 'p1'

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_all_esg_data_with_params(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_esg_api.get_esg.return_value = {'data': 'value'}
        from gs_quant.api.gs.esg import ESGMeasure, ESGCard
        result = pe.get_all_esg_data(
            measures=[ESGMeasure.ES_PERCENTILE],
            cards=[ESGCard.SUMMARY],
            pricing_date=dt.date(2023, 1, 1),
            benchmark_id='bm1'
        )
        assert result == {'data': 'value'}

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_summary(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_esg_api.get_esg.return_value = {'summary': [{'measure': 'ES', 'value': 50}]}
        result = pe.get_esg_summary()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_quintiles(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.esg import ESGMeasure
        mock_esg_api.get_esg.return_value = {
            'quintiles': [{'results': [{'description': 'Q1', 'gross': 20, 'long': 15, 'short': 5, 'extra': 99}]}]
        }
        result = pe.get_esg_quintiles(ESGMeasure.ES_PERCENTILE)
        assert isinstance(result, pd.DataFrame)
        assert 'extra' not in result.columns

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_by_sector(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.esg import ESGMeasure, ESGCard
        mock_esg_api.get_esg.return_value = {
            ESGCard.MEASURES_BY_SECTOR.value: [{'results': [{'sector': 'Tech', 'score': 80}]}]
        }
        result = pe.get_esg_by_sector(ESGMeasure.ES_PERCENTILE)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_by_region(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.esg import ESGMeasure, ESGCard
        mock_esg_api.get_esg.return_value = {
            ESGCard.MEASURES_BY_REGION.value: [{'results': [{'region': 'US', 'score': 70}]}]
        }
        result = pe.get_esg_by_region(ESGMeasure.ES_PERCENTILE)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_top_ten(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.esg import ESGMeasure, ESGCard
        mock_esg_api.get_esg.return_value = {
            ESGCard.TOP_TEN_RANKED.value: [{'results': [{'name': 'Company A'}]}]
        }
        result = pe.get_esg_top_ten(ESGMeasure.ES_PERCENTILE)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsEsgApi')
    def test_get_esg_bottom_ten(self, mock_esg_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.esg import ESGMeasure, ESGCard
        mock_esg_api.get_esg.return_value = {
            ESGCard.BOTTOM_TEN_RANKED.value: [{'results': [{'name': 'Company Z'}]}]
        }
        result = pe.get_esg_bottom_ten(ESGMeasure.ES_PERCENTILE)
        assert isinstance(result, pd.DataFrame)

    # --- Carbon methods ---

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_analytics(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_carbon_api.get_carbon_analytics.return_value = {'data': 'value'}
        result = pe.get_carbon_analytics()
        assert result == {'data': 'value'}

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_coverage(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.carbon import CarbonCard, CarbonCoverageCategory, CarbonEntityType
        mock_carbon_api.get_carbon_analytics.return_value = {
            CarbonCard.COVERAGE.value: {
                CarbonCoverageCategory.WEIGHTS.value: {
                    CarbonEntityType.PORTFOLIO.value: [{'category': 'covered', 'weight': 80}]
                }
            }
        }
        result = pe.get_carbon_coverage()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_sbti_netzero_coverage(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.carbon import CarbonCard, CarbonTargetCoverageCategory, CarbonEntityType
        mock_carbon_api.get_carbon_analytics.return_value = {
            CarbonCard.SBTI_AND_NET_ZERO_TARGETS.value: {
                CarbonTargetCoverageCategory.PORTFOLIO_EMISSIONS.value: {
                    'target1': {CarbonEntityType.PORTFOLIO.value: {'coverage': 50}},
                    'target2': {CarbonEntityType.PORTFOLIO.value: {'coverage': 30}},
                }
            }
        }
        result = pe.get_carbon_sbti_netzero_coverage()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_emissions(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.carbon import CarbonCard, CarbonScope, CarbonEntityType
        mock_carbon_api.get_carbon_analytics.return_value = {
            CarbonCard.EMISSIONS.value: {
                CarbonScope.TOTAL_GHG.value: {
                    CarbonEntityType.PORTFOLIO.value: [{'emission': 100}]
                }
            }
        }
        result = pe.get_carbon_emissions()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_emissions_allocation(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.carbon import (
            CarbonCard, CarbonScope, CarbonEntityType, CarbonEmissionsAllocationCategory
        )
        mock_carbon_api.get_carbon_analytics.return_value = {
            CarbonCard.ALLOCATIONS.value: {
                CarbonScope.TOTAL_GHG.value: {
                    CarbonEntityType.PORTFOLIO.value: {
                        CarbonEmissionsAllocationCategory.GICS_SECTOR.value: [
                            {'name': 'Energy', 'allocation': 40}
                        ]
                    }
                }
            }
        }
        result = pe.get_carbon_emissions_allocation()
        assert isinstance(result, pd.DataFrame)
        assert CarbonEmissionsAllocationCategory.GICS_SECTOR.value in result.columns

    @patch('gs_quant.entities.entity.GsCarbonApi')
    def test_get_carbon_attribution_table(self, mock_carbon_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        from gs_quant.api.gs.carbon import CarbonCard, CarbonScope, CarbonEmissionsIntensityType
        mock_carbon_api.get_carbon_analytics.return_value = {
            CarbonCard.ATTRIBUTION.value: {
                CarbonScope.TOTAL_GHG.value: [
                    {
                        'sector': 'Energy',
                        'weightPortfolio': 20,
                        'weightBenchmark': 15,
                        'weightComparison': 5,
                        CarbonEmissionsIntensityType.EI_ENTERPRISE_VALUE.value: {
                            'intensity': 100
                        },
                    }
                ]
            }
        }
        result = pe.get_carbon_attribution_table('bm1')
        assert isinstance(result, pd.DataFrame)
        assert 'sector' in result.columns
        assert 'intensity' in result.columns

    # --- Thematic exposure / beta ---

    @patch('gs_quant.entities.entity.GsDataApi')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_exposure_asset(self, mock_asset_api, mock_data_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {
            'basket1': [{'id': 'b1', 'type': 'Custom Basket'}]
        }
        mock_data_api.query_data.return_value = [
            {'date': '2023-01-01', 'assetId': 'a1', 'basketId': 'b1', 'beta': 1.5}
        ]
        result = pe.get_thematic_exposure('basket1')
        assert isinstance(result, pd.DataFrame)
        assert 'thematicExposure' in result.columns

    def test_get_thematic_exposure_not_asset(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        with pytest.raises(NotImplementedError):
            pe.get_thematic_exposure('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_exposure_basket_not_found(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': []}
        with pytest.raises(MqValueError, match='Basket could not be found'):
            pe.get_thematic_exposure('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_exposure_basket_id_none(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': [{'id': None, 'type': 'Custom Basket'}]}
        with pytest.raises(MqValueError, match='Basket could not be found'):
            pe.get_thematic_exposure('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_exposure_wrong_type(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': [{'id': 'b1', 'type': 'SingleStock'}]}
        with pytest.raises(MqValueError, match='not a Custom or Research Basket'):
            pe.get_thematic_exposure('basket1')

    @patch('gs_quant.entities.entity.GsDataApi')
    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_beta_asset(self, mock_asset_api, mock_data_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {
            'basket1': [{'id': 'b1', 'type': 'Custom Basket'}]
        }
        mock_data_api.query_data.return_value = [
            {'date': '2023-01-01', 'assetId': 'a1', 'basketId': 'b1', 'beta': 1.2}
        ]
        result = pe.get_thematic_beta('basket1')
        assert isinstance(result, pd.DataFrame)
        assert 'thematicBeta' in result.columns

    def test_get_thematic_beta_not_asset(self):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        with pytest.raises(NotImplementedError):
            pe.get_thematic_beta('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_beta_basket_not_found(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': []}
        with pytest.raises(MqValueError, match='Basket could not be found'):
            pe.get_thematic_beta('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_beta_basket_id_none(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': [{'id': None, 'type': 'Custom Basket'}]}
        with pytest.raises(MqValueError, match='Basket could not be found'):
            pe.get_thematic_beta('basket1')

    @patch('gs_quant.entities.entity.GsAssetApi')
    def test_get_thematic_beta_wrong_type(self, mock_asset_api):
        pe = ConcretePositionedEntity('a1', EntityType.ASSET)
        mock_asset_api.resolve_assets.return_value = {'basket1': [{'id': 'b1', 'type': 'SingleStock'}]}
        with pytest.raises(MqValueError, match='not a Custom or Research Basket'):
            pe.get_thematic_beta('basket1')

    # --- Deprecated thematic methods ---

    @patch('gs_quant.entities.entity.flatten_results_into_df')
    @patch('gs_quant.entities.entity.GsThematicApi')
    def test_get_all_thematic_exposures(self, mock_thematic_api, mock_flatten):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_thematic_api.get_thematics.return_value = 'results'
        mock_flatten.return_value = pd.DataFrame()
        result = pe.get_all_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.flatten_results_into_df')
    @patch('gs_quant.entities.entity.GsThematicApi')
    def test_get_top_five_thematic_exposures(self, mock_thematic_api, mock_flatten):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_thematic_api.get_thematics.return_value = 'results'
        mock_flatten.return_value = pd.DataFrame()
        result = pe.get_top_five_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.flatten_results_into_df')
    @patch('gs_quant.entities.entity.GsThematicApi')
    def test_get_bottom_five_thematic_exposures(self, mock_thematic_api, mock_flatten):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_thematic_api.get_thematics.return_value = 'results'
        mock_flatten.return_value = pd.DataFrame()
        result = pe.get_bottom_five_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.entities.entity.get_thematic_breakdown_as_df')
    def test_get_thematic_breakdown(self, mock_breakdown):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_breakdown.return_value = pd.DataFrame()
        result = pe.get_thematic_breakdown(dt.date(2023, 1, 1), 'basket1')
        mock_breakdown.assert_called_once_with(entity_id='p1', date=dt.date(2023, 1, 1), basket_id='basket1')
        assert isinstance(result, pd.DataFrame)

    # --- get_factor_scenario_analytics ---

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_json_format(self, mock_report_api, mock_scenario_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        # Set up factor risk report
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{'summary': {'estimatedPnl': 100}}]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY],
                return_format=ReturnFormat.JSON
            )
            assert isinstance(result, dict)
            assert scenario1 in result

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_dataframe_format(self, mock_report_api, mock_scenario_api):
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {
                    'estimatedPnl': 100,
                    'estimatedPerformance': 0.05,
                    'exposure': 1000000,
                },
                'factorPnl': [],
                'bySectorAggregations': [
                    {
                        'name': 'Energy',
                        'exposure': 500000,
                        'estimatedPnl': 30,
                    }
                ],
                'byRegionAggregations': [],
                'byDirectionAggregations': [],
                'byAsset': [],
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[
                    ScenarioCalculationMeasure.SUMMARY,
                    ScenarioCalculationMeasure.ESTIMATED_PNL_BY_SECTOR,
                ],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            # summary should be in the result
            assert 'summary' in result

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_dataframe_summary_only(self, mock_report_api, mock_scenario_api):
        """Test DataFrame format with only summary results (no nested data)."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {
                    'estimatedPnl': 100,
                    'estimatedPerformance': 0.05,
                },
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            assert 'summary' in result
            assert isinstance(result['summary'], pd.DataFrame)

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_empty_result_types(self, mock_report_api, mock_scenario_api):
        """Test DataFrame format when all result types are empty lists."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {
                    'estimatedPnl': 0,
                },
                'factorPnl': [],
                'bySectorAggregations': [],
                'byRegionAggregations': [],
                'byDirectionAggregations': [],
                'byAsset': [],
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            # Only summary should be present since all others are empty
            assert 'summary' in result
            # factorPnl, etc. should NOT be present since they're empty
            assert 'factorPnl' not in result

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_multiple_scenarios(self, mock_report_api, mock_scenario_api):
        """Test with multiple scenarios to exercise inner loop logic."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        scenario2 = MagicMock()
        scenario2.id = 'sc2'
        scenario2.name = 'Scenario 2'
        scenario2.type.value = 'Factor Historical Simulation'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1', 'sc2'],
            'results': [
                {
                    'summary': {'estimatedPnl': 100},
                    'factorPnl': [{'name': 'Value', 'estimatedPnl': 50}],
                    'bySectorAggregations': [],
                    'byRegionAggregations': [],
                    'byDirectionAggregations': [],
                    'byAsset': [],
                },
                {
                    'summary': {'estimatedPnl': -50},
                    'factorPnl': [{'name': 'Momentum', 'estimatedPnl': -30}],
                    'bySectorAggregations': [],
                    'byRegionAggregations': [],
                    'byDirectionAggregations': [],
                    'byAsset': [],
                },
            ]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1, scenario2],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY, ScenarioCalculationMeasure.ESTIMATED_FACTOR_PNL],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            assert 'summary' in result
            assert 'factorPnl' in result

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_with_by_asset_data(self, mock_report_api, mock_scenario_api):
        """Test with byAsset data to exercise the column renaming code."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {'estimatedPnl': 100},
                'factorPnl': [],
                'bySectorAggregations': [],
                'byRegionAggregations': [],
                'byDirectionAggregations': [],
                'byAsset': [
                    {
                        'assetId': 'a1',
                        'name': 'Stock A',
                        'bbid': 'AAPL',
                        'exposure': 500000,
                        'estimatedPnl': 25,
                        'estimatedPerformance': 0.05,
                        'stressedMarketValue': 525000,
                    }
                ],
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[
                    ScenarioCalculationMeasure.SUMMARY,
                    ScenarioCalculationMeasure.ESTIMATED_PNL_BY_ASSET,
                ],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            assert 'byAsset' in result
            by_asset_df = result['byAsset']
            assert isinstance(by_asset_df, pd.DataFrame)
            # Check column renaming
            assert 'Scenario ID' in by_asset_df.columns
            assert 'Scenario Name' in by_asset_df.columns
            assert 'Asset ID' in by_asset_df.columns
            assert 'BBID' in by_asset_df.columns
            assert 'Asset Name' in by_asset_df.columns

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_explode_returns_series(self, mock_report_api, mock_scenario_api):
        """Test the branch where _explode_data returns a Series (isinstance check)."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {'estimatedPnl': 100},
                'factorPnl': [
                    {
                        'name': 'Value',
                        'factorExposure': 0.5,
                        'factorShock': 2.0,
                        'estimatedPnl': 50,
                        'factors': [
                            {
                                'name': 'Growth',
                                'factorExposure': 0.3,
                                'factorShock': 1.5,
                                'estimatedPnl': 20,
                            }
                        ]
                    },
                ],
                'bySectorAggregations': [],
                'byRegionAggregations': [],
                'byDirectionAggregations': [],
                'byAsset': [],
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY, ScenarioCalculationMeasure.ESTIMATED_FACTOR_PNL],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            assert 'factorPnl' in result

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_with_risk_model(self, mock_report_api, mock_scenario_api):
        """Test passing risk_model parameter."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = 'rm1'
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{'summary': {'estimatedPnl': 100}}]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[ScenarioCalculationMeasure.SUMMARY],
                risk_model='rm1',
                return_format=ReturnFormat.JSON
            )
            # Verify risk_model is in the calculation request
            call_args = mock_scenario_api.calculate_scenario.call_args[0][0]
            assert call_args['riskModel'] == 'rm1'

    @patch('gs_quant.entities.entity.GsFactorScenarioApi')
    @patch('gs_quant.entities.entity.GsReportApi')
    def test_get_factor_scenario_analytics_by_sector_with_industries(self, mock_report_api, mock_scenario_api):
        """Test bySectorAggregations with nested industries data."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        report = MagicMock()
        report.parameters.fx_hedged = None
        report.parameters.risk_model = None
        report.parameters.benchmark = None
        report.id = 'report1'
        mock_report_api.get_reports.return_value = [report]

        scenario1 = MagicMock()
        scenario1.id = 'sc1'
        scenario1.name = 'Scenario 1'
        scenario1.type.value = 'Factor Shock'

        mock_scenario_api.calculate_scenario.return_value = {
            'scenarios': ['sc1'],
            'results': [{
                'summary': {'estimatedPnl': 100},
                'factorPnl': [],
                'bySectorAggregations': [
                    {
                        'name': 'Energy',
                        'exposure': 500000,
                        'estimatedPnl': 30,
                        'industries': [
                            {
                                'name': 'Oil & Gas',
                                'exposure': 300000,
                                'estimatedPnl': 20,
                            }
                        ]
                    }
                ],
                'byRegionAggregations': [
                    {
                        'name': 'US',
                        'exposure': 800000,
                        'estimatedPnl': 60,
                    }
                ],
                'byDirectionAggregations': [
                    {
                        'name': 'Long',
                        'exposure': 700000,
                        'estimatedPnl': 50,
                    }
                ],
                'byAsset': [],
            }]
        }

        with patch('gs_quant.entities.entity.FactorRiskReport') as mock_fr:
            mock_fr.from_target.return_value = report
            result = pe.get_factor_scenario_analytics(
                scenarios=[scenario1],
                date=dt.date(2023, 3, 7),
                measures=[
                    ScenarioCalculationMeasure.SUMMARY,
                    ScenarioCalculationMeasure.ESTIMATED_PNL_BY_SECTOR,
                    ScenarioCalculationMeasure.ESTIMATED_PNL_BY_REGION,
                    ScenarioCalculationMeasure.ESTIMATED_PNL_BY_DIRECTION,
                ],
                return_format=ReturnFormat.DATA_FRAME
            )
            assert isinstance(result, dict)
            assert 'bySectorAggregations' in result
            assert 'byRegionAggregations' in result
            assert 'byDirectionAggregations' in result


# ---------------------------------------------------------------------------
# Tests for Entity.get() edge cases with id_type as string vs Enum
# ---------------------------------------------------------------------------


class TestEntityGetEdgeCases:
    @patch('gs_quant.entities.entity.GsSession')
    def test_get_id_type_enum_value_extracted(self, mock_session):
        """When id_type is an Enum, its .value is used."""
        mock_session.current.sync.get.return_value = {'id': 'c1', 'name': 'US'}
        Country.get('c1', Country.Identifier.MARQUEE_ID)
        mock_session.current.sync.get.assert_called_with('/countries/c1')

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_entity_type_string_resolved(self, mock_session):
        """When entity_type is a string, it's resolved through EntityType()."""
        mock_session.current.sync.get.return_value = {'id': 'k1', 'name': 'test'}
        result = KPI.get('k1', 'MQID', entity_type='kpi')
        mock_session.current.sync.get.assert_called_with('/kpis/k1')

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_non_mqid_returns_none_on_empty_results(self, mock_session):
        """When non-MQID query returns no results."""
        mock_session.current.sync.get.return_value = {'results': []}
        result = Country.get('Nonexistent', 'name')
        assert result is None

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_mqid_returns_none_on_falsy_result(self, mock_session):
        """When MQID query returns falsy result."""
        mock_session.current.sync.get.return_value = None
        result = Country.get('c1', 'MQID')
        assert result is None

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_asset_type_via_explicit_entity_type_string(self, mock_session):
        """When entity_type='asset' is passed as string, SecurityMaster is used."""
        with patch('gs_quant.markets.securities.SecurityMaster') as mock_sm_cls:
            mock_sm_cls.get_asset.return_value = 'asset'
            from gs_quant.markets.securities import AssetIdentifier
            result = Country.get('a1', 'MQID', entity_type='asset')
            mock_sm_cls.get_asset.assert_called_once_with('a1', AssetIdentifier.MARQUEE_ID)

    @patch('gs_quant.entities.entity.GsSession')
    def test_get_asset_type_via_explicit_entity_type_enum(self, mock_session):
        """When entity_type=EntityType.ASSET is passed as Enum, SecurityMaster is used."""
        with patch('gs_quant.markets.securities.SecurityMaster') as mock_sm_cls:
            mock_sm_cls.get_asset.return_value = 'asset'
            from gs_quant.markets.securities import AssetIdentifier
            result = Country.get('a1', 'MQID', entity_type=EntityType.ASSET)
            mock_sm_cls.get_asset.assert_called_once_with('a1', AssetIdentifier.MARQUEE_ID)


# ---------------------------------------------------------------------------
# Tests for PositionedEntity update_positions edge cases
# ---------------------------------------------------------------------------


class TestUpdatePositionsEdgeCases:
    @patch('gs_quant.entities.entity.time')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_mixed_quantities(self, mock_portfolio_api, mock_time):
        """Test where some positions have quantities and some don't."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio = MagicMock()
        mock_portfolio.currency = 'EUR'
        mock_portfolio_api.get_portfolio.return_value = mock_portfolio

        pos_with_qty = MagicMock(quantity=100)
        pos_without_qty = MagicMock(quantity=None)
        pos_set = MagicMock()
        pos_set.positions = [pos_with_qty, pos_without_qty]
        pos_set.to_target.return_value = 'target'
        pe.update_positions([pos_set])
        # Since at least one position has quantity=None, price should be called
        pos_set.price.assert_called_once_with('EUR')

    @patch('gs_quant.entities.entity.time')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_all_have_quantities(self, mock_portfolio_api, mock_time):
        """Test where all positions have quantities (no pricing needed)."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio = MagicMock()
        mock_portfolio.currency = 'USD'
        mock_portfolio_api.get_portfolio.return_value = mock_portfolio

        pos = MagicMock(quantity=50)
        pos_set = MagicMock()
        pos_set.positions = [pos]
        pos_set.to_target.return_value = 'target'
        pe.update_positions([pos_set])
        pos_set.price.assert_not_called()

    @patch('gs_quant.entities.entity.time')
    @patch('gs_quant.entities.entity.GsPortfolioApi')
    def test_update_positions_multiple_sets(self, mock_portfolio_api, mock_time):
        """Test with multiple position sets."""
        pe = ConcretePositionedEntity('p1', EntityType.PORTFOLIO)
        mock_portfolio = MagicMock()
        mock_portfolio.currency = 'USD'
        mock_portfolio_api.get_portfolio.return_value = mock_portfolio

        pos1 = MagicMock(quantity=100)
        pos_set1 = MagicMock()
        pos_set1.positions = [pos1]
        pos_set1.to_target.return_value = 'target1'

        pos2 = MagicMock(quantity=None)
        pos_set2 = MagicMock()
        pos_set2.positions = [pos2]
        pos_set2.to_target.return_value = 'target2'

        pe.update_positions([pos_set1, pos_set2])
        pos_set1.price.assert_not_called()
        pos_set2.price.assert_called_once_with('USD')
        mock_portfolio_api.update_positions.assert_called_once()
