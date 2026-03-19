"""
Tests for gs_quant.api.gs.users - GsUsersApi
Target: 100% branch coverage
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.users import GsUsersApi, DEFAULT_SEARCH_FIELDS


def _mock_session():
    session = MagicMock()
    return session


class TestGsUsersApiGetUsers:
    def test_get_users_no_filters(self):
        """Branch: all filter params are None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_users()
            url = mock_session.sync.get.call_args[0][0]
            assert url.startswith('/users?')
            assert 'limit=100' in url
            assert 'offset=0' in url
            assert result == []

    def test_get_users_with_ids(self):
        """Branch: user_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ['u1']}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_users(user_ids=['id1', 'id2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=id1&id=id2' in url

    def test_get_users_with_emails(self):
        """Branch: user_emails is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsUsersApi.get_users(user_emails=['a@b.com', 'c@d.com'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&email=a@b.com&email=c@d.com' in url

    def test_get_users_with_names(self):
        """Branch: user_names is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsUsersApi.get_users(user_names=['Alice', 'Bob'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&name=Alice&name=Bob' in url

    def test_get_users_with_companies(self):
        """Branch: user_companies is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsUsersApi.get_users(user_companies=['GS', 'MS'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&company=GS&company=MS' in url

    def test_get_users_all_filters(self):
        """Branch: all filters provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ['u1']}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_users(
                user_ids=['id1'], user_emails=['a@b.com'], user_names=['Alice'], user_companies=['GS'],
                limit=50, offset=10
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=id1' in url
            assert '&email=a@b.com' in url
            assert '&name=Alice' in url
            assert '&company=GS' in url
            assert 'limit=50' in url
            assert 'offset=10' in url


class TestGsUsersApiMisc:
    def test_get_my_guid(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'id': 'user123'}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_my_guid()
            assert result == 'guid:user123'

    def test_get_current_user_info(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'id': 'user123', 'name': 'Test'}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_current_user_info()
            mock_session.sync.get.assert_called_once_with('/users/self')
            assert result == {'id': 'user123', 'name': 'Test'}

    def test_get_current_app_managers(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'appManagers': ['m1', 'm2']}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_current_app_managers()
            assert result == ['guid:m1', 'guid:m2']

    def test_get_current_app_managers_no_managers(self):
        """Branch: pydash get returns default [] when appManagers missing"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_current_app_managers()
            assert result == []


class TestGsUsersApiGetMany:
    def test_get_many_no_fields(self):
        """Branch: fields is None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'email': 'a@b.com', 'name': 'Alice'}]}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_many('email', ['a@b.com'])
            url = mock_session.sync.get.call_args[0][0]
            assert 'fields=' not in url
            assert 'email=a@b.com' in url
            assert result == {'a@b.com': {'email': 'a@b.com', 'name': 'Alice'}}

    def test_get_many_with_fields_key_type_not_in_fields(self):
        """Branch: fields is not None and key_type not in fields -> fields gets key_type appended"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'email': 'a@b.com', 'name': 'Alice'}]}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_many('email', ['a@b.com'], fields=['name'])
            url = mock_session.sync.get.call_args[0][0]
            assert 'fields=name,email' in url

    def test_get_many_with_fields_key_type_in_fields(self):
        """Branch: fields is not None and key_type in fields -> fields unchanged"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'email': 'a@b.com'}]}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_many('email', ['a@b.com'], fields=['email', 'name'])
            url = mock_session.sync.get.call_args[0][0]
            assert 'fields=email,name' in url

    def test_get_many_chunked(self):
        """Multiple chunks needed (>100 keys)"""
        mock_session = _mock_session()
        keys = [f'id{i}' for i in range(150)]
        mock_session.sync.get.side_effect = [
            {'results': [{'id': f'id{i}'} for i in range(100)]},
            {'results': [{'id': f'id{i}'} for i in range(100, 150)]},
        ]
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_many('id', keys)
            assert len(result) == 150
            assert mock_session.sync.get.call_count == 2

    def test_get_many_empty_results(self):
        """Branch: response has no 'results' key -> .get returns []"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.get_many('email', ['a@b.com'])
            assert result == {}


class TestGsUsersApiSearch:
    def test_search_default_fields_no_where(self):
        """Branch: fields is None (uses DEFAULT), where is None (no 'where' key)"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': []}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.search('test query')
            call_args = mock_session.sync.post.call_args
            payload = call_args[1]['payload']
            assert payload['q'] == 'test query'
            assert payload['fields'] == DEFAULT_SEARCH_FIELDS
            assert 'where' not in payload

    def test_search_custom_fields_with_where(self):
        """Branch: fields provided, where provided"""
        mock_session = _mock_session()
        mock_session.sync.post.return_value = {'results': ['u1']}
        with patch('gs_quant.api.gs.users.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsUsersApi.search('query', fields=['name', 'email'], where={'internal': True})
            call_args = mock_session.sync.post.call_args
            payload = call_args[1]['payload']
            assert payload['fields'] == ['name', 'email']
            assert payload['where'] == {'internal': True}
