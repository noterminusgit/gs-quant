"""
Tests for gs_quant.api.gs.datagrid - GsDataGridApi
Target: 100% branch coverage
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.datagrid import GsDataGridApi
from gs_quant.analytics.datagrid.datagrid import API, DATAGRID_HEADERS


def _mock_session():
    session = MagicMock()
    return session


class TestGsDataGridApi:
    def test_get_datagrids_default(self):
        """Test get_datagrids with defaults, empty results"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg:
                result = GsDataGridApi.get_datagrids()
                url = mock_session.sync.get.call_args[0][0]
                assert 'limit=10' in url
                assert 'orderBy=>lastUpdatedTime' in url
                assert result == []

    def test_get_datagrids_with_results(self):
        """Test get_datagrids with actual results"""
        mock_session = _mock_session()
        raw_dg = {'id': 'dg1', 'name': 'test'}
        mock_session.sync.get.return_value = {'results': [raw_dg]}
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg_cls:
                mock_dg_cls.from_dict.return_value = 'parsed_dg'
                result = GsDataGridApi.get_datagrids()
                mock_dg_cls.from_dict.assert_called_once_with(raw_dg)
                assert result == ['parsed_dg']

    def test_get_datagrids_with_kwargs(self):
        """Test get_datagrids with custom limit and kwargs"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid'):
                GsDataGridApi.get_datagrids(limit=5, name='test')
                url = mock_session.sync.get.call_args[0][0]
                assert 'limit=5' in url
                assert 'name=test' in url

    def test_get_datagrids_no_results_key(self):
        """Branch: pydash get returns default [] when no results key"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid'):
                result = GsDataGridApi.get_datagrids()
                assert result == []

    def test_get_my_datagrids_default(self):
        """Test get_my_datagrids with default params"""
        mock_session = _mock_session()
        mock_session.sync.get.side_effect = [
            {'id': 'user123'},  # /users/self response
            {'results': []},  # datagrids response
        ]
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid'):
                result = GsDataGridApi.get_my_datagrids()
                assert result == []
                # Verify first call is /users/self
                first_call_url = mock_session.sync.get.call_args_list[0][0][0]
                assert first_call_url == '/users/self'
                # Verify second call includes ownerId
                second_call_url = mock_session.sync.get.call_args_list[1][0][0]
                assert 'ownerId=user123' in second_call_url

    def test_get_my_datagrids_with_results(self):
        """Test get_my_datagrids with actual results"""
        mock_session = _mock_session()
        raw_dg = {'id': 'dg1', 'name': 'my_grid'}
        mock_session.sync.get.side_effect = [
            {'id': 'user456'},
            {'results': [raw_dg]},
        ]
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg_cls:
                mock_dg_cls.from_dict.return_value = 'parsed_dg'
                result = GsDataGridApi.get_my_datagrids()
                mock_dg_cls.from_dict.assert_called_once_with(raw_dg)
                assert result == ['parsed_dg']

    def test_get_my_datagrids_with_kwargs(self):
        """Test get_my_datagrids with kwargs"""
        mock_session = _mock_session()
        mock_session.sync.get.side_effect = [
            {'id': 'user789'},
            {'results': []},
        ]
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid'):
                GsDataGridApi.get_my_datagrids(limit=20, tag='finance')
                url = mock_session.sync.get.call_args_list[1][0][0]
                assert 'limit=20' in url
                assert 'tag=finance' in url

    def test_get_datagrid(self):
        mock_session = _mock_session()
        raw_dg = {'id': 'dg1', 'name': 'test'}
        mock_session.sync.get.return_value = raw_dg
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg_cls:
                mock_dg_cls.from_dict.return_value = 'parsed_dg'
                result = GsDataGridApi.get_datagrid('dg1')
                mock_session.sync.get.assert_called_once_with(f'{API}/dg1')
                mock_dg_cls.from_dict.assert_called_once_with(raw_dg)
                assert result == 'parsed_dg'

    def test_create_datagrid(self):
        mock_session = _mock_session()
        dg = MagicMock()
        dg.as_dict.return_value = {'name': 'new_grid'}
        response = {'id': 'dg_new', 'name': 'new_grid'}
        mock_session.sync.post.return_value = response
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg_cls:
                mock_dg_cls.from_dict.return_value = 'created_dg'
                result = GsDataGridApi.create_datagrid(dg)
                call_args = mock_session.sync.post.call_args
                assert call_args[0][0] == f'{API}'
                assert call_args[1]['request_headers'] == DATAGRID_HEADERS
                mock_dg_cls.from_dict.assert_called_once_with(response)
                assert result == 'created_dg'

    def test_update_datagrid(self):
        mock_session = _mock_session()
        dg = MagicMock()
        dg.as_dict.return_value = {'name': 'updated_grid'}
        dg.id_ = 'dg1'
        response = {'id': 'dg1', 'name': 'updated_grid'}
        mock_session.sync.put.return_value = response
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.datagrid.DataGrid') as mock_dg_cls:
                mock_dg_cls.from_dict.return_value = 'updated_dg'
                result = GsDataGridApi.update_datagrid(dg)
                call_args = mock_session.sync.put.call_args
                assert call_args[0][0] == f'{API}/dg1'
                assert call_args[1]['request_headers'] == DATAGRID_HEADERS
                mock_dg_cls.from_dict.assert_called_once_with(response)
                assert result == 'updated_dg'

    def test_delete_datagrid(self):
        mock_session = _mock_session()
        dg = MagicMock()
        dg.id_ = 'dg1'
        mock_session.sync.delete.return_value = {'status': 'deleted'}
        with patch('gs_quant.api.gs.datagrid.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsDataGridApi.delete_datagrid(dg)
            mock_session.sync.delete.assert_called_once_with(f'{API}/dg1')
            assert result == {'status': 'deleted'}
