"""
Tests for gs_quant.api.gs.federated_secmaster - GsSecurityMasterFederatedApi
Targets 100% branch coverage.
"""

import datetime as dt
from unittest.mock import MagicMock

import pytest

from gs_quant.api.gs.federated_secmaster import (
    GsSecurityMasterFederatedApi,
    FederatedIdentifiers,
    SECURITIES_FEDERATED,
)
from gs_quant.common import AssetClass, AssetType
from gs_quant.session import GsSession, Environment
from gs_quant.target.secmaster import SecMasterAssetType


def _mock_session(mocker):
    mocker.patch.object(
        GsSession.__class__,
        'default_value',
        return_value=GsSession.get(Environment.QA, 'client_id', 'secret'),
    )
    return GsSession.current


# ── FederatedIdentifiers enum ────────────────────────────────────────

class TestFederatedIdentifiers:
    def test_enum_values(self):
        assert FederatedIdentifiers.IDENTIFIER.value == 'identifier'
        assert FederatedIdentifiers.ID.value == 'id'
        assert FederatedIdentifiers.ASSET_ID.value == 'assetId'
        assert FederatedIdentifiers.GSID.value == 'gsid'
        assert FederatedIdentifiers.TICKER.value == 'ticker'
        assert FederatedIdentifiers.BBID.value == 'bbid'
        assert FederatedIdentifiers.BCID.value == 'bcid'
        assert FederatedIdentifiers.RIC.value == 'ric'
        assert FederatedIdentifiers.RCIC.value == 'rcic'
        assert FederatedIdentifiers.CUSIP.value == 'cusip'
        assert FederatedIdentifiers.CINS.value == 'cins'
        assert FederatedIdentifiers.SEDOL.value == 'sedol'
        assert FederatedIdentifiers.ISIN.value == 'isin'
        assert FederatedIdentifiers.PRIMEID.value == 'primeId'


# ── get_a_security ───────────────────────────────────────────────────

class TestGetASecurity:
    def test_gs_id_no_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'id': 'GSPD111E123'})
        result = GsSecurityMasterFederatedApi.get_a_security('GSPD111E123')
        session.sync.get.assert_called_once_with(
            f'{SECURITIES_FEDERATED}/GSPD111E123', payload={}
        )
        assert result == {'id': 'GSPD111E123'}

    def test_ma_id_no_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'id': 'MANYS1FCCWWV45P7'})
        result = GsSecurityMasterFederatedApi.get_a_security('MANYS1FCCWWV45P7')
        assert result == {'id': 'MANYS1FCCWWV45P7'}

    def test_with_effective_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'id': 'GSPD111E123'})
        result = GsSecurityMasterFederatedApi.get_a_security('GSPD111E123', effective_date=dt.date(2023, 6, 1))
        payload = session.sync.get.call_args[1]['payload']
        assert 'effectiveDate' in payload

    def test_invalid_id_raises(self, mocker):
        with pytest.raises(ValueError, match="Invalid id"):
            GsSecurityMasterFederatedApi.get_a_security('INVALID123')

    def test_id_starts_with_neither(self, mocker):
        with pytest.raises(ValueError, match="Invalid id"):
            GsSecurityMasterFederatedApi.get_a_security('XY123')


# ── get_security_identifiers ────────────────────────────────────────

class TestGetSecurityIdentifiers:
    def test_gs_id(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'identifiers': {}})
        result = GsSecurityMasterFederatedApi.get_security_identifiers('GSPD111E123')
        session.sync.get.assert_called_once_with(
            f'{SECURITIES_FEDERATED}/GSPD111E123/identifiers'
        )
        assert result == {'identifiers': {}}

    def test_ma_id(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'identifiers': {}})
        result = GsSecurityMasterFederatedApi.get_security_identifiers('MANYS1FCCWWV45P7')
        assert result == {'identifiers': {}}

    def test_invalid_id_raises(self, mocker):
        with pytest.raises(ValueError, match="Invalid id"):
            GsSecurityMasterFederatedApi.get_security_identifiers('INVALID')


# ── get_many_securities ──────────────────────────────────────────────

class TestGetManySecurities:
    def test_with_type(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.get_many_securities(type_=SecMasterAssetType.ETF)
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}'
        assert result == {'results': []}

    def test_with_query_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.get_many_securities(ticker='AAPL')
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}'

    def test_no_type_no_query_raises(self, mocker):
        with pytest.raises(ValueError, match="Neither '_type' nor 'query_params' are provided"):
            GsSecurityMasterFederatedApi.get_many_securities()

    def test_all_optional_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.get_many_securities(
            type_=SecMasterAssetType.Single_Stock,
            effective_date=dt.date(2023, 6, 1),
            limit=25,
            is_primary=True,
            offset_key='abc123',
            ticker='AAPL',
        )
        payload = session.sync.get.call_args[1]['payload']
        assert payload['limit'] == 25
        assert 'effectiveDate' in payload
        assert payload['offsetKey'] == 'abc123'
        assert payload['isPrimary'] is True
        assert payload['type'] == 'Single Stock'


# ── get_securities_data ──────────────────────────────────────────────

class TestGetSecuritiesData:
    def test_with_type(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.get_securities_data(type_=SecMasterAssetType.ETF)
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}/data'

    def test_with_query_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.get_securities_data(bbid='AAPL')
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}/data'


# ── search_many_securities ───────────────────────────────────────────

class TestSearchManySecurities:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.search_many_securities(q='apple')
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}/search'

    def test_no_query_raises(self, mocker):
        with pytest.raises(ValueError, match="No search query provided"):
            GsSecurityMasterFederatedApi.search_many_securities()

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.search_many_securities(
            q='apple',
            limit=5,
            offset_key='key1',
            asset_class=AssetClass.Equity,
            type_=SecMasterAssetType.ETF,
            is_primary=True,
        )
        payload = session.sync.get.call_args[1]['payload']
        assert payload['q'] == 'apple'
        assert payload['limit'] == 5
        assert payload['offsetKey'] == 'key1'
        assert payload['assetClass'] == 'Equity'
        assert payload['type'] == 'ETF'
        assert payload['isPrimary'] is True


# ── search_securities_data ───────────────────────────────────────────

class TestSearchSecuritiesData:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.search_securities_data(q='apple')
        call_url = session.sync.get.call_args[0][0]
        assert call_url == f'{SECURITIES_FEDERATED}/search/data'

    def test_no_query_raises(self, mocker):
        with pytest.raises(ValueError, match="No search query provided"):
            GsSecurityMasterFederatedApi.search_securities_data()

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsSecurityMasterFederatedApi.search_securities_data(
            q='google',
            limit=20,
            offset_key='key2',
            asset_class=AssetClass.Commod,
            type_=AssetType.ETF,
            is_primary=False,
        )
        payload = session.sync.get.call_args[1]['payload']
        assert payload['q'] == 'google'
        assert payload['limit'] == 20
        assert payload['offsetKey'] == 'key2'
        assert payload['assetClass'] == 'Commod'
        assert payload['type'] == 'ETF'
        assert payload['isPrimary'] is False


# ── __prepare_params edge cases ──────────────────────────────────────

class TestPrepareParams:
    def test_all_none_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsSecurityMasterFederatedApi.search_many_securities(q='test')
        payload = session.sync.get.call_args[1]['payload']
        assert 'effectiveDate' not in payload
        assert 'offsetKey' not in payload
        assert 'isPrimary' not in payload
        assert 'type' not in payload
        assert 'assetClass' not in payload

    def test_asset_type_from_common(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsSecurityMasterFederatedApi.get_many_securities(type_=AssetType.ETF)
        payload = session.sync.get.call_args[1]['payload']
        assert payload['type'] == 'ETF'
