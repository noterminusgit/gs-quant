"""
Tests for gs_quant.api.gs.workspaces - GsWorkspacesMarketsApi
Target: 100% branch coverage
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.workspaces import GsWorkspacesMarketsApi, WORKSPACES_MARKETS_HEADERS
from gs_quant.target.workspaces_markets import Workspace


def _mock_session():
    session = MagicMock()
    return session


class TestGsWorkspacesMarketsApi:
    def test_get_workspaces_default(self):
        mock_session = _mock_session()
        ws = MagicMock(spec=Workspace)
        mock_session.sync.get.return_value = {'results': (ws,)}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.get_workspaces()
            url = mock_session.sync.get.call_args[0][0]
            assert 'limit=10' in url
            assert result == (ws,)

    def test_get_workspaces_with_kwargs(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsWorkspacesMarketsApi.get_workspaces(limit=5, name='test')
            url = mock_session.sync.get.call_args[0][0]
            assert 'limit=5' in url
            assert 'name=test' in url

    def test_get_workspace(self):
        mock_session = _mock_session()
        ws = MagicMock(spec=Workspace)
        mock_session.sync.get.return_value = ws
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.get_workspace('ws123')
            mock_session.sync.get.assert_called_once_with('/workspaces/markets/ws123', cls=Workspace)
            assert result == ws

    def test_get_workspace_by_alias_found(self):
        """Branch: workspace found (truthy result)"""
        mock_session = _mock_session()
        ws = MagicMock(spec=Workspace)
        mock_session.sync.get.return_value = {'results': [ws]}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.get_workspace_by_alias('my-alias')
            assert result == ws

    def test_get_workspace_by_alias_not_found(self):
        """Branch: workspace not found (falsy result) -> ValueError"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError, match='Workspace with alias missing not found'):
                GsWorkspacesMarketsApi.get_workspace_by_alias('missing')

    def test_get_workspace_by_alias_no_results_key(self):
        """Branch: 'results' key missing -> pydash get returns None -> ValueError"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError):
                GsWorkspacesMarketsApi.get_workspace_by_alias('missing')

    def test_create_workspace(self):
        mock_session = _mock_session()
        ws = MagicMock(spec=Workspace)
        mock_session.sync.post.return_value = ws
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.create_workspace(ws)
            mock_session.sync.post.assert_called_once_with(
                '/workspaces/markets', ws, cls=Workspace, request_headers=WORKSPACES_MARKETS_HEADERS
            )
            assert result == ws

    def test_update_workspace(self):
        mock_session = _mock_session()
        ws = MagicMock(spec=Workspace)
        ws.id = 'ws123'
        mock_session.sync.put.return_value = ws
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.update_workspace(ws)
            mock_session.sync.put.assert_called_once_with(
                f'/workspaces/markets/{ws.id}', ws, cls=Workspace, request_headers=WORKSPACES_MARKETS_HEADERS
            )

    def test_delete_workspace(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'ok'}
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsWorkspacesMarketsApi.delete_workspace('ws123')
            mock_session.sync.delete.assert_called_once_with('/workspaces/markets/ws123')
            assert result == {'status': 'ok'}

    def test_open_workspace_with_alias(self):
        """Branch: workspace.alias is truthy -> use alias URL"""
        mock_session = _mock_session()
        mock_session.domain = 'https://marquee.web.gs.com'
        ws = MagicMock(spec=Workspace)
        ws.alias = 'my-alias'
        ws.id = 'ws123'
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.workspaces.webbrowser') as mock_wb:
                GsWorkspacesMarketsApi.open_workspace(ws)
                mock_wb.open.assert_called_once_with('https://marquee.gs.com/s/markets/my-alias')

    def test_open_workspace_without_alias(self):
        """Branch: workspace.alias is falsy -> use id URL"""
        mock_session = _mock_session()
        mock_session.domain = 'https://marquee.web.gs.com'
        ws = MagicMock(spec=Workspace)
        ws.alias = None
        ws.id = 'ws123'
        with patch('gs_quant.api.gs.workspaces.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with patch('gs_quant.api.gs.workspaces.webbrowser') as mock_wb:
                GsWorkspacesMarketsApi.open_workspace(ws)
                mock_wb.open.assert_called_once_with('https://marquee.gs.com/s/markets/ws123')
