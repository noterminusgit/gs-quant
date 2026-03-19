"""
Tests for gs_quant.api.gs.thematics - GsThematicApi, ThematicMeasure, Region
Target: 100% branch coverage
"""

import datetime as dt
import json
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.thematics import GsThematicApi, ThematicMeasure, Region


def _mock_session():
    session = MagicMock()
    return session


class TestThematicMeasure:
    def test_str(self):
        """Cover ThematicMeasure.__str__"""
        assert str(ThematicMeasure.ALL_THEMATIC_EXPOSURES) == 'allThematicExposures'
        assert str(ThematicMeasure.TOP_FIVE_THEMATIC_EXPOSURES) == 'topFiveThematicExposures'
        assert str(ThematicMeasure.BOTTOM_FIVE_THEMATIC_EXPOSURES) == 'bottomFiveThematicExposures'
        assert str(ThematicMeasure.THEMATIC_BREAKDOWN_BY_ASSET) == 'thematicBreakdownByAsset'
        assert str(ThematicMeasure.NO_THEMATIC_DATA) == 'noThematicData'
        assert str(ThematicMeasure.NO_PRICING_DATA) == 'noPricingData'

    def test_values(self):
        assert ThematicMeasure.ALL_THEMATIC_EXPOSURES.value == 'allThematicExposures'


class TestRegion:
    def test_values(self):
        assert Region.AMERICAS.value == 'Americas'
        assert Region.ASIA.value == 'Asia'
        assert Region.EUROPE.value == 'Europe'


class TestGsThematicApi:
    def test_get_thematics_minimal(self):
        """Branch: all optional params are None/falsy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': [{'data': 1}]}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsThematicApi.get_thematics('entity1')
            call_args = mock_session.sync.post.call_args
            payload = json.loads(call_args[1]['payload'])
            assert payload == {'id': 'entity1'}
            assert result == [{'data': 1}]

    def test_get_thematics_with_basket_ids(self):
        """Branch: basket_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics('e1', basket_ids=['b1', 'b2'])
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['basketId'] == ['b1', 'b2']

    def test_get_thematics_with_regions(self):
        """Branch: regions is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics('e1', regions=[Region.AMERICAS, Region.ASIA])
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['region'] == ['Americas', 'Asia']

    def test_get_thematics_with_start_date(self):
        """Branch: start_date is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics('e1', start_date=dt.date(2023, 1, 15))
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['startDate'] == '2023-01-15'

    def test_get_thematics_with_end_date(self):
        """Branch: end_date is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics('e1', end_date=dt.date(2023, 6, 30))
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['endDate'] == '2023-06-30'

    def test_get_thematics_with_measures(self):
        """Branch: measures is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics(
                'e1', measures=[ThematicMeasure.ALL_THEMATIC_EXPOSURES, ThematicMeasure.NO_PRICING_DATA]
            )
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['measures'] == ['allThematicExposures', 'noPricingData']

    def test_get_thematics_with_notional(self):
        """Branch: notional is truthy"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsThematicApi.get_thematics('e1', notional=1000000.0)
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['notional'] == 1000000.0

    def test_get_thematics_all_params(self):
        """All optional params provided"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': [{'x': 1}]}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsThematicApi.get_thematics(
                'e1',
                basket_ids=['b1'],
                regions=[Region.EUROPE],
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 12, 31),
                measures=[ThematicMeasure.THEMATIC_BREAKDOWN_BY_ASSET],
                notional=500000.0,
            )
            payload = json.loads(mock_session.sync.post.call_args[1]['payload'])
            assert payload['id'] == 'e1'
            assert payload['basketId'] == ['b1']
            assert payload['region'] == ['Europe']
            assert payload['startDate'] == '2023-01-01'
            assert payload['endDate'] == '2023-12-31'
            assert payload['measures'] == ['thematicBreakdownByAsset']
            assert payload['notional'] == 500000.0
            assert result == [{'x': 1}]

    def test_get_thematics_no_results_key(self):
        """Branch: .get('results', []) returns default when key missing"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {}
        with patch('gs_quant.api.gs.thematics.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsThematicApi.get_thematics('e1')
            assert result == []
