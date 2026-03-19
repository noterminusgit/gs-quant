"""
Tests for gs_quant.api.gs.hedges - GsHedgeApi
Targets 100% branch coverage.
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.hedges import GsHedgeApi, CALCULATION_TIMEOUT
from gs_quant.session import GsSession, Environment
from gs_quant.target.hedge import Hedge


def _mock_session(mocker):
    """Set up a mock GsSession.current using the same approach as existing tests."""
    mocker.patch.object(
        GsSession.__class__,
        'default_value',
        return_value=GsSession.get(Environment.QA, 'client_id', 'secret'),
    )
    return GsSession.current


# ── get_many_hedges ──────────────────────────────────────────────────

class TestGetManyHedges:
    def test_no_ids_no_names(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value=[])
        result = GsHedgeApi.get_many_hedges()
        session.sync.get.assert_called_once_with('/hedges?limit=100', cls=Hedge)
        assert result == []

    def test_with_ids_only(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value=['h1'])
        GsHedgeApi.get_many_hedges(ids=['id1', 'id2'])
        url = session.sync.get.call_args[0][0]
        assert '&id=id1&id=id2' in url

    def test_with_names_only(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value=['h1'])
        GsHedgeApi.get_many_hedges(names=['n1', 'n2'])
        url = session.sync.get.call_args[0][0]
        assert '&name=n1&name=n2' in url

    def test_with_ids_and_names(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value=['h1'])
        GsHedgeApi.get_many_hedges(ids=['id1'], names=['n1'])
        url = session.sync.get.call_args[0][0]
        assert '&id=id1' in url
        assert '&name=n1' in url

    def test_custom_limit(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value=[])
        GsHedgeApi.get_many_hedges(limit=50)
        url = session.sync.get.call_args[0][0]
        assert 'limit=50' in url


# ── create_hedge ─────────────────────────────────────────────────────

class TestCreateHedge:
    def test_create_hedge(self, mocker):
        session = _mock_session(mocker)
        hedge_dict = {'name': 'test'}
        expected = MagicMock(spec=Hedge)
        mocker.patch.object(session.sync, 'post', return_value=expected)
        result = GsHedgeApi.create_hedge(hedge_dict)
        session.sync.post.assert_called_once_with('/hedges', hedge_dict, cls=Hedge)
        assert result is expected


# ── get_hedge ────────────────────────────────────────────────────────

class TestGetHedge:
    def test_get_hedge(self, mocker):
        session = _mock_session(mocker)
        expected = MagicMock(spec=Hedge)
        mocker.patch.object(session.sync, 'get', return_value=expected)
        result = GsHedgeApi.get_hedge('hedge123')
        session.sync.get.assert_called_once_with('/hedges/hedge123', cls=Hedge)
        assert result is expected


# ── get_hedge_data ───────────────────────────────────────────────────

class TestGetHedgeData:
    def test_no_ids_no_names(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': [{'id': '1'}]})
        result = GsHedgeApi.get_hedge_data()
        url = session.sync.get.call_args[0][0]
        assert url == '/hedges/data?limit=100'
        assert result == [{'id': '1'}]

    def test_with_ids(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsHedgeApi.get_hedge_data(ids=['a', 'b'])
        url = session.sync.get.call_args[0][0]
        assert '&id=a&id=b' in url

    def test_with_names(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsHedgeApi.get_hedge_data(names=['x', 'y'])
        url = session.sync.get.call_args[0][0]
        assert '&name=x&name=y' in url


# ── get_hedge_results ────────────────────────────────────────────────

class TestGetHedgeResults:
    def test_no_dates(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': [{'perf': 0.5}]})
        result = GsHedgeApi.get_hedge_results('hid')
        url = session.sync.get.call_args[0][0]
        assert url == '/hedges/results?id=hid'
        assert result == {'perf': 0.5}

    def test_with_start_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': [{'perf': 0.5}]})
        GsHedgeApi.get_hedge_results('hid', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url

    def test_with_end_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': [{'perf': 0.5}]})
        GsHedgeApi.get_hedge_results('hid', end_date=dt.date(2023, 6, 30))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-30' in url

    def test_with_both_dates(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': [{'perf': 0.5}]})
        GsHedgeApi.get_hedge_results('hid', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 30))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url


# ── update_hedge ─────────────────────────────────────────────────────

class TestUpdateHedge:
    def test_update_hedge(self, mocker):
        session = _mock_session(mocker)
        hedge = MagicMock(spec=Hedge)
        expected = MagicMock(spec=Hedge)
        mocker.patch.object(session.sync, 'put', return_value=expected)
        result = GsHedgeApi.update_hedge('hid', hedge)
        session.sync.put.assert_called_once_with('/hedges/hid', hedge, cls=Hedge)
        assert result is expected


# ── delete_hedge ─────────────────────────────────────────────────────

class TestDeleteHedge:
    def test_delete_hedge(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'delete', return_value='ok')
        result = GsHedgeApi.delete_hedge('hid')
        session.sync.delete.assert_called_once_with('/hedges/hid', cls=Hedge)
        assert result == 'ok'


# ── construct_performance_hedge_query ────────────────────────────────

class TestConstructPerformanceHedgeQuery:
    def test_minimal(self, mocker):
        mock_php = MagicMock()
        mocker.patch('gs_quant.api.gs.hedges.PerformanceHedgeParameters', return_value=mock_php)
        result = GsHedgeApi.construct_performance_hedge_query(
            hedge_target='MQ123',
            universe=('MQ456',),
            notional=1_000_000,
            observation_start_date=dt.date(2023, 1, 1),
            observation_end_date=dt.date(2023, 6, 30),
            backtest_start_date=dt.date(2023, 1, 1),
            backtest_end_date=dt.date(2023, 6, 30),
        )
        assert result['objective'] == 'Replicate Performance'
        assert result['parameters'] is mock_php

    def test_with_optional_params(self, mocker):
        from gs_quant.target.hedge import AssetConstraint, ClassificationConstraint
        mock_php = MagicMock()
        mocker.patch('gs_quant.api.gs.hedges.PerformanceHedgeParameters', return_value=mock_php)
        asset_c = AssetConstraint(asset_id='A1', min_=0, max_=50)
        class_c = ClassificationConstraint(type_='Sector', name='Energy', min_=0, max_=30)
        result = GsHedgeApi.construct_performance_hedge_query(
            hedge_target='MQ123',
            universe=('MQ456',),
            notional=500_000,
            observation_start_date=dt.date(2023, 1, 1),
            observation_end_date=dt.date(2023, 6, 30),
            backtest_start_date=dt.date(2023, 1, 1),
            backtest_end_date=dt.date(2023, 6, 30),
            use_machine_learning=True,
            lasso_weight=0.5,
            ridge_weight=0.3,
            max_return_deviation=10,
            max_adv_percentage=20,
            max_leverage=80,
            max_weight=50,
            min_market_cap=1000,
            max_market_cap=100000,
            asset_constraints=(asset_c,),
            benchmarks=('BM1',),
            classification_constraints=(class_c,),
            exclude_corporate_actions=True,
            exclude_corporate_actions_types=('Mergers',),
            exclude_hard_to_borrow_assets=True,
            exclude_restricted_assets=True,
            exclude_target_assets=False,
            explode_universe=False,
            market_participation_rate=20,
            sampling_period='Weekly',
        )
        assert result['objective'] == 'Replicate Performance'
        assert result['parameters'] is mock_php


# ── calculate_hedge ──────────────────────────────────────────────────

class TestCalculateHedge:
    def test_calculate_hedge(self, mocker):
        session = _mock_session(mocker)
        hedge_query = {'objective': 'Replicate Performance', 'parameters': {}}
        expected = {'result': 'data'}
        mocker.patch.object(session.sync, 'post', return_value=expected)
        result = GsHedgeApi.calculate_hedge(hedge_query)
        session.sync.post.assert_called_once_with(
            '/hedges/calculations', payload=hedge_query, timeout=CALCULATION_TIMEOUT
        )
        assert result is expected


# ── share_hedge_group ────────────────────────────────────────────────

class TestShareHedgeGroup:
    def _make_hedge_group_data(self, owner_id='owner123', hedge_ids=None,
                                entitlements=None):
        data = {
            'ownerId': owner_id,
            'entitlements': entitlements if entitlements is not None else {},
            'hedgeIds': ['h1'] if hedge_ids is None else hedge_ids,
            'createdById': 'creator1',
            'createdTime': '2023-01-01T00:00:00Z',
            'lastUpdatedById': 'updater1',
            'lastUpdatedTime': '2023-01-02T00:00:00Z',
        }
        return data

    def test_success_with_view_and_admin_emails(self, mocker):
        """Full happy path with both view_emails and admin_emails."""
        session = _mock_session(mocker)
        hedge_group_data = self._make_hedge_group_data()

        put_response = {
            'entitlements': {
                'view': ['guid:owner123', 'guid:viewer1', 'guid:admin1'],
                'edit': ['guid:owner123', 'guid:admin1'],
                'admin': ['guid:owner123', 'guid:admin1'],
            }
        }

        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        mock_view_user = MagicMock()
        mock_view_user.id = 'viewer1'
        mock_admin_user = MagicMock()
        mock_admin_user.id = 'admin1'

        with patch('gs_quant.entities.entitlements.User') as MockUser:
            MockUser.get_many.side_effect = lambda emails: (
                [mock_view_user] if 'view@test.com' in emails else [mock_admin_user]
            )
            result = GsHedgeApi.share_hedge_group(
                hedge_group_id='HG1',
                strategy_request={'objective': 'Minimize Factor Risk', 'parameters': {'p': 1}},
                optimization_response={'result': {'r': 1}},
                hedge_name='My Hedge',
                group_name='My Group',
                view_emails=['view@test.com'],
                admin_emails=['admin@test.com'],
            )
        assert result is put_response

    def test_no_emails(self, mocker):
        """No view_emails or admin_emails."""
        session = _mock_session(mocker)
        hedge_group_data = self._make_hedge_group_data()
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': {'view': ['guid:owner123'], 'edit': ['guid:owner123'], 'admin': ['guid:owner123']}}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        assert result is put_response

    def test_no_owner_id(self, mocker):
        """ownerId is empty string - covers `if current_user_guid:` False branch."""
        session = _mock_session(mocker)
        hedge_group_data = self._make_hedge_group_data(owner_id='')
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': {}}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        assert result is put_response

    def test_owner_already_in_entitlements(self, mocker):
        """Owner GUID already present in all entitlement lists."""
        session = _mock_session(mocker)
        entitlements = {
            'admin': ['guid:owner123'],
            'edit': ['guid:owner123'],
            'view': ['guid:owner123'],
        }
        hedge_group_data = self._make_hedge_group_data(entitlements=entitlements)
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': entitlements}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        assert result is put_response

    def test_entitlements_missing_keys(self, mocker):
        """Entitlements dict exists but missing admin/edit/view keys."""
        session = _mock_session(mocker)
        hedge_group_data = self._make_hedge_group_data(entitlements={})
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': {'view': ['guid:owner123'], 'edit': ['guid:owner123'], 'admin': ['guid:owner123']}}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        assert result is put_response

    def test_empty_hedge_ids(self, mocker):
        """hedge_group_data has empty hedgeIds list."""
        session = _mock_session(mocker)
        hedge_group_data = self._make_hedge_group_data(hedge_ids=[])
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': {'view': [], 'edit': [], 'admin': []}}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        payload = session.sync.put.call_args[0][1]
        assert payload['hedges'][0]['id'] is None

    def test_exception_is_raised(self, mocker):
        """GET call raises exception."""
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', side_effect=RuntimeError('network error'))
        with pytest.raises(RuntimeError, match='network error'):
            GsHedgeApi.share_hedge_group(
                hedge_group_id='HG1',
                strategy_request={},
                optimization_response={},
            )

    def test_duplicate_view_user_not_added_twice(self, mocker):
        """View user already in view list."""
        session = _mock_session(mocker)
        entitlements = {
            'admin': ['guid:owner123'],
            'edit': ['guid:owner123'],
            'view': ['guid:owner123', 'guid:viewer1'],
        }
        hedge_group_data = self._make_hedge_group_data(entitlements=entitlements)
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': entitlements}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        mock_view_user = MagicMock()
        mock_view_user.id = 'viewer1'

        with patch('gs_quant.entities.entitlements.User') as MockUser:
            MockUser.get_many.return_value = [mock_view_user]
            result = GsHedgeApi.share_hedge_group(
                hedge_group_id='HG1',
                strategy_request={},
                optimization_response={},
                view_emails=['viewer@test.com'],
            )
        assert result is put_response

    def test_duplicate_admin_user_not_added_twice(self, mocker):
        """Admin user already in all entitlement lists."""
        session = _mock_session(mocker)
        entitlements = {
            'admin': ['guid:owner123', 'guid:admin1'],
            'edit': ['guid:owner123', 'guid:admin1'],
            'view': ['guid:owner123', 'guid:admin1'],
        }
        hedge_group_data = self._make_hedge_group_data(entitlements=entitlements)
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': entitlements}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        mock_admin_user = MagicMock()
        mock_admin_user.id = 'admin1'

        with patch('gs_quant.entities.entitlements.User') as MockUser:
            MockUser.get_many.return_value = [mock_admin_user]
            result = GsHedgeApi.share_hedge_group(
                hedge_group_id='HG1',
                strategy_request={},
                optimization_response={},
                admin_emails=['admin@test.com'],
            )
        assert result is put_response

    def test_no_hedgeids_key(self, mocker):
        """hedge_group_data has no hedgeIds key at all."""
        session = _mock_session(mocker)
        hedge_group_data = {
            'ownerId': 'owner123',
            'entitlements': {},
            'createdById': '',
            'createdTime': '',
            'lastUpdatedById': '',
            'lastUpdatedTime': '',
        }
        mocker.patch.object(session.sync, 'get', return_value=hedge_group_data)
        put_response = {'entitlements': {'view': [], 'edit': [], 'admin': []}}
        mocker.patch.object(session.sync, 'put', return_value=put_response)

        result = GsHedgeApi.share_hedge_group(
            hedge_group_id='HG1',
            strategy_request={},
            optimization_response={},
        )
        payload = session.sync.put.call_args[0][1]
        assert payload['hedges'][0]['id'] is None
