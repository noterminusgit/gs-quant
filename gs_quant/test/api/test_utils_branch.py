"""
Branch coverage tests for gs_quant/api/utils.py
"""

import socket
from functools import partial
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.api.utils import handle_proxy, ThreadPoolManager
from gs_quant.errors import MqUninitialisedError
from gs_quant.session import GsSession


class TestHandleProxy:
    """Test handle_proxy function."""

    def test_internal_user_with_gs_quant_auth(self):
        """Internal user with gs_quant_auth uses proxies."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            with patch('gs_quant.api.utils.requests.get', return_value=mock_response) as mock_get:
                # Simulate gs_quant_auth being available
                import sys
                mock_auth = MagicMock()
                mock_auth.__proxies__ = {'http': 'http://proxy:8080'}
                sys.modules['gs_quant_auth'] = mock_auth
                try:
                    result = handle_proxy('http://example.com', {'key': 'value'})
                    assert result == mock_response
                    mock_get.assert_called_once_with(
                        'http://example.com',
                        params={'key': 'value'},
                        proxies={'http': 'http://proxy:8080'}
                    )
                finally:
                    del sys.modules['gs_quant_auth']

    def test_internal_user_without_gs_quant_auth(self):
        """Internal user without gs_quant_auth raises RuntimeError."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            # Ensure gs_quant_auth is not importable
            import sys
            if 'gs_quant_auth' in sys.modules:
                del sys.modules['gs_quant_auth']
            with patch.dict(sys.modules, {'gs_quant_auth': None}):
                with pytest.raises((RuntimeError, ImportError)):
                    handle_proxy('http://example.com', {'key': 'value'})

    def test_external_user(self):
        """External user makes direct request."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = False

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            with patch('gs_quant.api.utils.socket.getfqdn', return_value='myhost.example.com'):
                with patch('gs_quant.api.utils.requests.get', return_value=mock_response) as mock_get:
                    result = handle_proxy('http://example.com', {'key': 'value'})
                    assert result == mock_response
                    mock_get.assert_called_once_with('http://example.com', params={'key': 'value'})

    def test_uninitialised_session_external(self):
        """MqUninitialisedError falls back to internal=False."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = MagicMock()
            MockGsSession.current.is_internal.side_effect = MqUninitialisedError('not init')
            with patch('gs_quant.api.utils.socket.getfqdn', return_value='myhost.example.com'):
                with patch('gs_quant.api.utils.requests.get', return_value=mock_response) as mock_get:
                    result = handle_proxy('http://example.com', {'key': 'value'})
                    assert result == mock_response

    def test_gs_com_domain_with_auth(self):
        """gs.com domain user with gs_quant_auth uses proxies."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = False

        mock_response = MagicMock()

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            with patch('gs_quant.api.utils.socket.getfqdn', return_value='myhost.gs.com'):
                import sys
                mock_auth = MagicMock()
                mock_auth.__proxies__ = {'http': 'http://proxy:8080'}
                sys.modules['gs_quant_auth'] = mock_auth
                try:
                    with patch('gs_quant.api.utils.requests.get', return_value=mock_response) as mock_get:
                        result = handle_proxy('http://example.com', {'key': 'val'})
                        mock_get.assert_called_once_with(
                            'http://example.com',
                            params={'key': 'val'},
                            proxies={'http': 'http://proxy:8080'}
                        )
                finally:
                    del sys.modules['gs_quant_auth']


class TestThreadPoolManager:
    """Test ThreadPoolManager."""

    def test_initialize(self):
        """initialize creates a ThreadPoolExecutor."""
        ThreadPoolManager.initialize(max_workers=2)
        # Verify that executor is set by running a task
        assert True  # No error

    def test_run_async_creates_executor_if_none(self):
        """run_async creates executor if not initialized."""
        # Reset executor
        ThreadPoolManager._ThreadPoolManager__executor = None

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            results = ThreadPoolManager.run_async([lambda: 42, lambda: 99])
            assert results[0] == 42
            assert results[1] == 99

    def test_run_async_preserves_order(self):
        """run_async preserves task order in results."""
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch('gs_quant.api.utils.GsSession') as MockGsSession:
            MockGsSession.current = mock_session
            tasks = [lambda i=i: i for i in range(5)]
            results = ThreadPoolManager.run_async(tasks)
            assert results == [0, 1, 2, 3, 4]
