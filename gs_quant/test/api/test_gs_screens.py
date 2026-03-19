"""
Tests for gs_quant.api.gs.screens - GsScreenApi
Target: 100% branch coverage
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.screens import GsScreenApi
from gs_quant.target.screens import Screen


def _mock_session():
    session = MagicMock()
    return session


class TestGsScreenApi:
    def test_get_screens_no_filters(self):
        """Branch: screen_ids is None, screen_names is None"""
        mock_session = _mock_session()
        s = MagicMock(spec=Screen)
        mock_session.sync.get.return_value = {'results': (s,)}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.get_screens()
            url = mock_session.sync.get.call_args[0][0]
            assert url.startswith('/screens?')
            assert 'limit=100' in url
            assert '&id=' not in url
            assert '&name=' not in url
            assert result == (s,)

    def test_get_screens_with_ids(self):
        """Branch: screen_ids is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScreenApi.get_screens(screen_ids=['s1', 's2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=s1&id=s2' in url

    def test_get_screens_with_names(self):
        """Branch: screen_names is truthy"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScreenApi.get_screens(screen_names=['name1', 'name2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '&name=name1&name=name2' in url

    def test_get_screens_with_ids_and_names(self):
        """Branch: both screen_ids and screen_names provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsScreenApi.get_screens(screen_ids=['s1'], screen_names=['n1'], limit=50)
            url = mock_session.sync.get.call_args[0][0]
            assert '&id=s1' in url
            assert '&name=n1' in url
            assert 'limit=50' in url

    def test_get_screen(self):
        mock_session = _mock_session()
        s = MagicMock(spec=Screen)
        mock_session.sync.get.return_value = s
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.get_screen('s1')
            mock_session.sync.get.assert_called_once_with('/screens/s1', cls=Screen)
            assert result == s

    def test_create_screen(self):
        mock_session = _mock_session()
        s = MagicMock(spec=Screen)
        mock_session.sync.post.return_value = s
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.create_screen(s)
            mock_session.sync.post.assert_called_once_with('/screens', s, cls=Screen)
            assert result == s

    def test_update_screen(self):
        mock_session = _mock_session()
        s = MagicMock(spec=Screen)
        s.id = 's1'
        mock_session.sync.put.return_value = s
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.update_screen(s)
            mock_session.sync.put.assert_called_once_with('/screens/s1', s, cls=Screen)
            assert result == s

    def test_delete_screen(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = 'deleted'
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.delete_screen('s1')
            mock_session.sync.delete.assert_called_once_with('/screens/s1')
            assert result == 'deleted'

    def test_get_filter_options(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'options': ['opt1']}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.get_filter_options()
            mock_session.sync.get.assert_called_once_with('/assets/screener/options')
            assert result == {'options': ['opt1']}

    def test_calculate(self):
        mock_session = _mock_session()
        payload = MagicMock()
        mock_session.sync.post.return_value = {'data': [1, 2, 3]}
        with patch('gs_quant.api.gs.screens.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsScreenApi.calculate(payload)
            mock_session.sync.post.assert_called_once_with('/assets/screener', payload=payload)
            assert result == {'data': [1, 2, 3]}
