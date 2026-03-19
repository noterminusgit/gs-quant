"""
Tests for gs_quant.api.gs.groups - GsGroupsApi
Target: 100% branch coverage
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.groups import GsGroupsApi
from gs_quant.target.groups import Group


def _mock_session():
    session = MagicMock()
    return session


class TestGsGroupsApiGetGroups:
    def test_get_groups_no_filters(self):
        """Branch: all optional params are None/falsy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsGroupsApi.get_groups()
            url = mock_session.sync.get.call_args[0][0]
            assert 'limit=100' in url
            assert 'offset=0' in url
            assert '&id=' not in url
            assert '&name=' not in url
            assert result == []

    def test_get_groups_with_ids(self):
        """Branch: ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ['g1']}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(ids=['id1', 'id2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=id1&id=id2' in url

    def test_get_groups_with_names(self):
        """Branch: names is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(names=['n1', 'n2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&name=n1&name=n2' in url

    def test_get_groups_with_oe_ids(self):
        """Branch: oe_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(oe_ids=['oe1'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&oeId=oe1' in url

    def test_get_groups_with_owner_ids(self):
        """Branch: owner_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(owner_ids=['o1'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&ownerId=o1' in url

    def test_get_groups_with_tags(self):
        """Branch: tags is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(tags=['t1', 't2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&tags=t1&tags=t2' in url

    def test_get_groups_with_user_ids(self):
        """Branch: user_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(user_ids=['u1'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&userIds=u1' in url

    def test_get_groups_with_scroll_id(self):
        """Branch: scroll_id is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(scroll_id='scroll123')
            url = mock_session.sync.get.call_args[0][0]
            assert '&scrollId=scroll123' in url

    def test_get_groups_with_scroll_time(self):
        """Branch: scroll_time is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(scroll_time='5m')
            url = mock_session.sync.get.call_args[0][0]
            assert '&scrollTime=5m' in url

    def test_get_groups_with_order_by(self):
        """Branch: order_by is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(order_by='name')
            url = mock_session.sync.get.call_args[0][0]
            assert '&orderBy=name' in url

    def test_get_groups_all_params(self):
        """All optional params provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ['g1']}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.get_groups(
                ids=['id1'], names=['n1'], oe_ids=['oe1'], owner_ids=['o1'],
                tags=['t1'], user_ids=['u1'], scroll_id='sc1', scroll_time='5m',
                limit=50, offset=10, order_by='name'
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=id1' in url
            assert '&name=n1' in url
            assert '&oeId=oe1' in url
            assert '&ownerId=o1' in url
            assert '&tags=t1' in url
            assert '&userIds=u1' in url
            assert '&scrollId=sc1' in url
            assert '&scrollTime=5m' in url
            assert '&orderBy=name' in url
            assert 'limit=50' in url
            assert 'offset=10' in url


class TestGsGroupsApiCRUD:
    def test_create_group(self):
        mock_session = _mock_session()
        group = MagicMock(spec=Group)
        mock_session.sync.post.return_value = {'id': 'g1'}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsGroupsApi.create_group(group)
            mock_session.sync.post.assert_called_once_with('/groups', group, cls=Group)
            assert result == {'id': 'g1'}

    def test_get_group(self):
        mock_session = _mock_session()
        group = MagicMock(spec=Group)
        mock_session.sync.get.return_value = group
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsGroupsApi.get_group('g1')
            mock_session.sync.get.assert_called_once_with('/groups/g1', cls=Group)
            assert result == group

    def test_update_group_with_entitlements(self):
        """Branch: group_dict.get('entitlements') is truthy -> convert entitlements to json"""
        mock_session = _mock_session()
        entitlements_mock = MagicMock()
        entitlements_mock.to_json.return_value = {'admin': ['u1']}
        group = MagicMock(spec=Group)
        group.to_json.return_value = {'id': 'g1', 'name': 'test', 'entitlements': entitlements_mock}
        mock_session.sync.put.return_value = group
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.update_group('g1', group)
            call_args = mock_session.sync.put.call_args
            assert call_args[0][0] == '/groups/g1'
            payload = call_args[0][1]
            # entitlements should be converted and id should be popped
            assert 'id' not in payload
            assert payload['entitlements'] == {'admin': ['u1']}

    def test_update_group_without_entitlements(self):
        """Branch: group_dict.get('entitlements') is falsy -> no entitlements conversion"""
        mock_session = _mock_session()
        group = MagicMock(spec=Group)
        group.to_json.return_value = {'id': 'g1', 'name': 'test'}
        mock_session.sync.put.return_value = group
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.update_group('g1', group)
            call_args = mock_session.sync.put.call_args
            payload = call_args[0][1]
            assert 'id' not in payload
            assert 'entitlements' not in payload

    def test_delete_group(self):
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.delete_group('g1')
            mock_session.sync.delete.assert_called_once_with('/groups/g1')


class TestGsGroupsApiUsers:
    def test_get_users_in_group(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'users': ['u1', 'u2']}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsGroupsApi.get_users_in_group('g1')
            mock_session.sync.get.assert_called_once_with('/groups/g1/users')
            assert result == ['u1', 'u2']

    def test_get_users_in_group_no_users_key(self):
        """Branch: 'users' not in response -> .get returns []"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsGroupsApi.get_users_in_group('g1')
            assert result == []

    def test_add_users_to_group(self):
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.add_users_to_group('g1', ['u1', 'u2'])
            mock_session.sync.post.assert_called_once_with('/groups/g1/users', {'userIds': ['u1', 'u2']})

    def test_delete_users_from_group(self):
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.groups.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsGroupsApi.delete_users_from_group('g1', ['u1', 'u2'])
            mock_session.sync.delete.assert_called_once_with(
                '/groups/g1/users', {'userIds': ['u1', 'u2']}, use_body=True
            )
