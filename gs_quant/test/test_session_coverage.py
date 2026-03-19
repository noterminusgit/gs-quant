"""
Additional branch-coverage tests for gs_quant/session.py.

These complement the existing test_session.py and target the remaining
uncovered branches: _headers cookie variants, _SyncSessionAPI/
_AsyncSessionAPI delegation, sync/async_ properties, _connect_websocket
with tracing, _get_mds_domain edge cases, and more.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

import pytest
import requests
import requests.adapters
import requests.cookies

from gs_quant.errors import MqRequestError, MqError
from gs_quant.session import (
    API_VERSION,
    DEFAULT_APPLICATION,
    DEFAULT_TIMEOUT,
    CustomHttpAdapter,
    Domain,
    Environment,
    GsSession,
    OAuth2Session,
    PassThroughSession,
    _SyncSessionAPI,
    _AsyncSessionAPI,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class ConcreteSession(GsSession):
    def _authenticate(self):
        self._session.headers.update({'Authorization': 'Bearer test-token'})


def _make_session(domain='https://api.example.com', environment='PROD', **kwargs):
    with patch('gs_quant.session.CustomHttpAdapter'):
        sess = ConcreteSession(domain, environment=environment, **kwargs)
    sess._session = MagicMock(spec=requests.Session)
    sess._session.headers = {
        'X-Application': DEFAULT_APPLICATION,
        'X-Version': '0.0.0',
        'Authorization': 'Bearer test-token',
    }
    sess._session.cookies = requests.cookies.RequestsCookieJar()
    return sess


def _mock_response(status_code=200, json_body=None, content_type='application/json',
                   text=None, reason='OK', headers=None, content=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.reason = reason
    resp.reason_phrase = reason
    resp.text = text or (json.dumps(json_body) if json_body is not None else '')
    resp.content = content or resp.text.encode('utf-8')
    _headers = {'Content-Type': content_type}
    if headers:
        _headers.update(headers)
    resp.headers = _headers
    return resp


# ===========================================================================
# _headers – cookie branch combinations
# ===========================================================================

class TestHeadersCookieBranches:
    def test_no_session(self):
        s = _make_session()
        s._session = None
        assert s._headers() == []

    def test_no_cookies(self):
        """Session with no cookies at all."""
        s = _make_session()
        s._session.cookies = {}
        hdrs = s._headers()
        # Authorization header should be present, but no Cookie header
        auth_keys = [k for k, v in hdrs if k == 'Authorization']
        cookie_keys = [k for k, v in hdrs if k == 'Cookie']
        assert len(auth_keys) == 1
        assert len(cookie_keys) == 0

    def test_marquee_login_cookie(self):
        s = _make_session()
        s._session.cookies = {'MarqueeLogin': 'ml-token'}
        hdrs = s._headers()
        cookies = [v for k, v in hdrs if k == 'Cookie']
        assert len(cookies) == 1
        assert 'MarqueeLogin=ml-token' in cookies[0]

    def test_marquee_csrf_cookie(self):
        s = _make_session()
        s._session.cookies = {'MARQUEE-CSRF-TOKEN': 'csrf-val'}
        hdrs = s._headers()
        cookies = [v for k, v in hdrs if k == 'Cookie']
        assert len(cookies) == 1
        assert 'MARQUEE-CSRF-TOKEN=csrf-val' in cookies[0]

    def test_gssso_cookie(self):
        s = _make_session()
        s._session.cookies = {'GSSSO': 'sso-val'}
        hdrs = s._headers()
        cookies = [v for k, v in hdrs if k == 'Cookie']
        assert len(cookies) == 1
        assert 'GSSSO=sso-val' in cookies[0]

    def test_all_cookies(self):
        s = _make_session()
        s._session.cookies = {
            'MarqueeLogin': 'ml',
            'MARQUEE-CSRF-TOKEN': 'csrf',
            'GSSSO': 'sso',
        }
        hdrs = s._headers()
        cookies = [v for k, v in hdrs if k == 'Cookie']
        assert len(cookies) == 1
        assert 'MarqueeLogin=ml' in cookies[0]
        assert 'MARQUEE-CSRF-TOKEN=csrf' in cookies[0]
        assert 'GSSSO=sso' in cookies[0]

    def test_header_filtering(self):
        """Only specific header keys are forwarded."""
        s = _make_session()
        s._session.headers = {
            'Authorization': 'Bearer tok',
            'X-MARQUEE-CSRF-TOKEN': 'csrf-hdr',
            'X-Application': 'my-app',
            'X-Version': '1.0',
            'X-Random-Other': 'skip',
        }
        s._session.cookies = {}
        hdrs = s._headers()
        keys = [k for k, v in hdrs]
        assert 'Authorization' in keys
        assert 'X-MARQUEE-CSRF-TOKEN' in keys
        assert 'X-Application' in keys
        assert 'X-Version' in keys
        assert 'X-Random-Other' not in keys

    def test_empty_cookies_no_cookie_header(self):
        """Falsy cookie jar should produce no Cookie header."""
        s = _make_session()
        s._session.cookies = None
        hdrs = s._headers()
        cookie_keys = [k for k, v in hdrs if k == 'Cookie']
        assert len(cookie_keys) == 0


# ===========================================================================
# sync / async_ properties
# ===========================================================================

class TestSyncAsyncProperties:
    def test_sync_creates_api(self):
        s = _make_session()
        api = s.sync
        assert isinstance(api, _SyncSessionAPI)
        # Second access returns the same instance
        assert s.sync is api

    def test_async_creates_api(self):
        s = _make_session()
        api = s.async_
        assert isinstance(api, _AsyncSessionAPI)
        assert s.async_ is api


# ===========================================================================
# _SyncSessionAPI delegation
# ===========================================================================

class TestSyncSessionAPI:
    def _setup(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'ok': True})
        resp.headers['x-dash-requestid'] = 'rid'
        s._session.request = MagicMock(return_value=resp)
        return s

    def test_get(self):
        s = self._setup()
        result = s.sync.get('/test', payload={'a': 1})
        assert result == {'ok': True}

    def test_post(self):
        s = self._setup()
        result = s.sync.post('/test', payload={'b': 2})
        assert result == {'ok': True}

    def test_put(self):
        s = self._setup()
        result = s.sync.put('/test', payload={'c': 3})
        assert result == {'ok': True}

    def test_delete(self):
        s = self._setup()
        result = s.sync.delete('/test')
        assert result == {'ok': True}

    def test_delete_with_body(self):
        s = self._setup()
        result = s.sync.delete('/test', payload={'id': '1'}, use_body=True)
        assert result == {'ok': True}


# ===========================================================================
# _AsyncSessionAPI delegation
# ===========================================================================

class TestAsyncSessionAPI:
    def _setup(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'ok': True})
        resp.headers['x-dash-requestid'] = 'arid'
        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client
        return s

    def test_get(self):
        s = self._setup()
        result = _run_async(s.async_.get('/test'))
        assert result == {'ok': True}

    def test_post(self):
        s = self._setup()
        result = _run_async(s.async_.post('/test', payload={'b': 2}))
        assert result == {'ok': True}

    def test_put(self):
        s = self._setup()
        result = _run_async(s.async_.put('/test', payload={'c': 3}))
        assert result == {'ok': True}

    def test_delete(self):
        s = self._setup()
        result = _run_async(s.async_.delete('/test'))
        assert result == {'ok': True}

    def test_delete_with_body(self):
        s = self._setup()
        result = _run_async(s.async_.delete('/test', payload={'id': '1'}, use_body=True))
        assert result == {'ok': True}


# ===========================================================================
# _AsyncSessionAPI.connect_websocket
# ===========================================================================

class TestAsyncWebsocketAPI:
    def test_connect_websocket_delegates(self):
        s = _make_session()

        @asynccontextmanager
        async def mock_connect_ws(path, headers=None, include_version=True, domain=None, **kwargs):
            yield MagicMock()

        s._connect_websocket = mock_connect_ws
        api = s.async_

        async def _test():
            async with api.connect_websocket('/stream') as ws:
                assert ws is not None

        _run_async(_test())


# ===========================================================================
# _connect_websocket tracing branches
# ===========================================================================

class TestConnectWebsocket:
    def test_with_tracing_and_request_headers_ws13(self):
        """Cover scope.span.set_tag('wss.host', ...) for websockets < 14."""
        s = _make_session()
        mock_ws = MagicMock()
        mock_ws.request_headers = {'host': 'example.com'}

        @asynccontextmanager
        async def mock_raw(path, headers=None, include_version=True, domain=None, **kwargs):
            yield mock_ws

        s._connect_websocket_raw = mock_raw

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = mock_span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx
            MockTracer.inject = MagicMock()

            async def _test():
                async with s._connect_websocket('/stream') as ws:
                    assert ws is mock_ws

            _run_async(_test())

    def test_with_tracing_ws14_no_request_headers(self):
        """Cover scope.span.set_tag('wss.host', ...) for websockets >= 14."""
        s = _make_session()
        mock_ws = MagicMock(spec=[])  # no request_headers attribute
        mock_ws.request = MagicMock()
        mock_ws.request.headers = {'host': 'ws14.example.com'}

        @asynccontextmanager
        async def mock_raw(path, headers=None, include_version=True, domain=None, **kwargs):
            yield mock_ws

        s._connect_websocket_raw = mock_raw

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = mock_span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx
            MockTracer.inject = MagicMock()

            async def _test():
                async with s._connect_websocket('/stream') as ws:
                    assert ws is mock_ws

            _run_async(_test())

    def test_without_tracing(self):
        """No active span -> nullcontext -> scope is None."""
        s = _make_session()
        mock_ws = MagicMock()

        @asynccontextmanager
        async def mock_raw(path, headers=None, include_version=True, domain=None, **kwargs):
            yield mock_ws

        s._connect_websocket_raw = mock_raw

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = None

            async def _test():
                async with s._connect_websocket('/stream', headers={'X-Custom': 'val'}) as ws:
                    assert ws is mock_ws

            _run_async(_test())

    def test_with_tracing_no_span(self):
        """Active span exists but not recording."""
        s = _make_session()
        mock_ws = MagicMock()

        @asynccontextmanager
        async def mock_raw(path, headers=None, include_version=True, domain=None, **kwargs):
            yield mock_ws

        s._connect_websocket_raw = mock_raw

        mock_span = MagicMock()
        mock_span.is_recording.return_value = False

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span

            async def _test():
                async with s._connect_websocket('/stream') as ws:
                    assert ws is mock_ws

            _run_async(_test())

    def test_scope_no_span(self):
        """scope exists but scope.span is falsy."""
        s = _make_session()
        mock_ws = MagicMock()

        @asynccontextmanager
        async def mock_raw(path, headers=None, include_version=True, domain=None, **kwargs):
            yield mock_ws

        s._connect_websocket_raw = mock_raw

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = None  # falsy span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx
            MockTracer.inject = MagicMock()

            async def _test():
                async with s._connect_websocket('/stream') as ws:
                    assert ws is mock_ws

            _run_async(_test())


# ===========================================================================
# __request with tracing – scope branch
# ===========================================================================

class TestRequestTracing:
    def test_request_with_tracing_scope(self):
        """Cover __request branches when scope is truthy."""
        s = _make_session()
        resp = _mock_response(200, json_body={'ok': True})
        resp.headers['x-dash-requestid'] = 'rid'
        s._session.request = MagicMock(return_value=resp)

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = mock_span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx

            result = s._get('/test')
            assert result == {'ok': True}
            mock_span.set_tag.assert_called()

    def test_request_with_tracing_scope_error(self):
        """Cover scope branch with status_code > 399."""
        s = _make_session()
        resp = _mock_response(500, json_body={'error': 'fail'}, reason='ISE', text='error text')
        resp.headers['x-dash-requestid'] = 'rid'
        s._session.request = MagicMock(return_value=resp)

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = mock_span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx

            with pytest.raises(Exception):
                s._get('/test')
            # Check that error tag was set
            calls = [str(c) for c in mock_span.set_tag.call_args_list]
            error_calls = [c for c in calls if 'error' in c]
            assert len(error_calls) > 0


# ===========================================================================
# __request_async with tracing scope
# ===========================================================================

class TestRequestAsyncTracing:
    def test_async_request_with_tracing_scope(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'ok': True})
        resp.headers['x-dash-requestid'] = 'arid'
        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_scope = MagicMock()
        mock_scope.span = mock_span

        with patch('gs_quant.session.Tracer') as MockTracer:
            MockTracer.active_span.return_value = mock_span
            mock_tracer_ctx = MagicMock()
            mock_tracer_ctx.__enter__ = MagicMock(return_value=mock_scope)
            mock_tracer_ctx.__exit__ = MagicMock(return_value=False)
            MockTracer.return_value = mock_tracer_ctx

            result = _run_async(s._get_async('/test'))
            assert result == {'ok': True}


# ===========================================================================
# _get_mds_domain edge branches
# ===========================================================================

class TestGetMdsDomainEdge:
    def test_mds_domain_literal_mds_us_east(self):
        """When domain is Domain.MDS_US_EAST constant, returns MdsDomainEast."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        sess.domain = Domain.MDS_US_EAST
        result = sess._get_mds_domain()
        config = GsSession._config_for_environment('PROD')
        assert result == config['MdsDomainEast']

    def test_mds_domain_with_web_in_prod_domain(self):
        """When domain contains 'marquee.web' it should be normalized."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('PROD')
        # Set domain to MarqueeWebDomain (which contains marquee.web)
        sess.domain = config['MarqueeWebDomain']
        result = sess._get_mds_domain()
        assert result == config['MdsWebDomain']

    def test_mds_domain_dev_env(self):
        """DEV environment default."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('DEV', 'cid', 'csecret', ('scope1',))
        result = sess._get_mds_domain()
        # Should return MdsDomainEast for DEV
        config = GsSession._config_for_environment('DEV')
        assert result in [config['MdsDomainEast'], config['MdsWebDomain']]

    def test_mds_domain_qa_with_web_in_domain(self):
        """QA with marquee-qa.web in domain should normalize."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('QA', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('QA')
        sess.domain = config.get('MarqueeWebDomain', sess.domain)
        result = sess._get_mds_domain()
        assert result == config['MdsWebDomain']

    def test_mds_domain_qa_app_domain(self):
        """QA with regular AppDomain returns MdsDomainEast."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('QA', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('QA')
        sess.domain = config['AppDomain']
        result = sess._get_mds_domain()
        assert result == config['MdsDomainEast']


# ===========================================================================
# OAuth2Session edge branches
# ===========================================================================

class TestOAuth2SessionEdge:
    def test_init_with_mds_us_east_domain(self):
        """PROD with MDS_US_EAST domain -> verify stays True."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',), domain=Domain.MDS_US_EAST)
        assert sess.verify is True

    def test_init_custom_env_string(self):
        """Non-standard environment string uses DEV config."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('https://custom.api.com', 'cid', 'csecret', ('scope1',))
        assert sess.verify is False


# ===========================================================================
# PassThroughSession edge branches
# ===========================================================================

class TestPassThroughSessionEdge:
    def test_domain_and_verify_with_known_domain(self):
        """Known environment + domain should return verify=True."""
        PassThroughSession._PassThroughSession__config = None
        domain, verify = PassThroughSession.domain_and_verify('PROD', 'AppDomain')
        assert verify is True
        assert domain != 'PROD'

    def test_domain_and_verify_key_error(self):
        """Unknown environment raises KeyError, falls back to verify=False."""
        PassThroughSession._PassThroughSession__config = None
        domain, verify = PassThroughSession.domain_and_verify('https://custom.url', 'AppDomain')
        assert verify is False
        assert domain == 'https://custom.url'

    def test_domain_and_verify_bad_domain_key(self):
        """Known env but bad domain key."""
        PassThroughSession._PassThroughSession__config = None
        domain, verify = PassThroughSession.domain_and_verify('PROD', 'NonExistentDomain')
        assert verify is False
        assert domain == 'PROD'

    def test_init_with_none_domain_defaults_to_app(self):
        sess = PassThroughSession('PROD', 'tok', domain=None)
        assert sess.token == 'tok'

    def test_init_with_mds_domain(self):
        sess = PassThroughSession('PROD', 'tok', domain=Domain.MDS_US_EAST)
        assert sess.token == 'tok'


# ===========================================================================
# GsSession.get – scope conversion branches
# ===========================================================================

class TestGsSessionGetEdge:
    def test_get_with_enum_scopes_mixed(self):
        """Mix of Enum and string scopes."""
        sess = GsSession.get('PROD', client_id='cid', client_secret='csecret',
                             scopes=[GsSession.Scopes.RUN_ANALYTICS, 'custom_scope'])
        assert isinstance(sess, OAuth2Session)
        assert 'run_analytics' in sess.scopes
        assert 'custom_scope' in sess.scopes

    def test_get_with_empty_scopes(self):
        """Empty scopes tuple should just use defaults."""
        sess = GsSession.get('PROD', client_id='cid', client_secret='csecret', scopes=())
        assert isinstance(sess, OAuth2Session)
        defaults = GsSession.Scopes.get_default()
        for d in defaults:
            assert d in sess.scopes

    def test_get_oauth2_with_http_adapter(self):
        adapter = requests.adapters.HTTPAdapter()
        sess = GsSession.get('PROD', client_id='cid', client_secret='csecret',
                             http_adapter=adapter)
        assert isinstance(sess, OAuth2Session)
        assert sess.http_adapter is adapter

    def test_get_passthrough_with_domain_key(self):
        sess = GsSession.get('PROD', token='tok', domain=Domain.MDS_US_EAST)
        assert isinstance(sess, PassThroughSession)


# ===========================================================================
# GsSession.use – edge cases
# ===========================================================================

class TestGsSessionUseEdge:
    def _reset_current(self):
        try:
            from gs_quant.context_base import thread_local
            key = 'GsSession_path'
            if hasattr(thread_local, key):
                delattr(thread_local, key)
        except Exception:
            pass

    def test_use_with_string_env(self):
        self._reset_current()
        with patch.object(GsSession, 'get') as mock_get:
            mock_session = MagicMock(spec=GsSession)
            mock_session.is_entered = False
            mock_get.return_value = mock_session
            GsSession.use('QA', client_id='cid', client_secret='csecret')
            mock_session.init.assert_called_once()
        self._reset_current()

    def test_use_domain_none_raises(self):
        with pytest.raises(MqError, match='None is not a valid domain'):
            GsSession.use('PROD', client_id='cid', client_secret='csecret', domain=None)


# ===========================================================================
# _on_aexit – when async session is None (no close needed)
# ===========================================================================

class TestOnAexitEdge:
    def test_on_aexit_no_async_session(self):
        """When close_on_exit_async is True but session_async is None."""
        s = _make_session()
        s._session = None

        with patch.object(s, 'init'):
            with patch.object(s, '_init_async'):
                _run_async(s._on_aenter())

        s._session = MagicMock()
        s._session_async = None  # no async session to close

        _run_async(s._on_aexit(None, None, None))
        assert s._session is None
        assert s._session_async is None


# ===========================================================================
# init – http_adapter None branch
# ===========================================================================

class TestInitEdge:
    @patch('gs_quant.session.requests.Session')
    def test_init_http_adapter_none_no_mount(self, mock_session_cls):
        """When http_adapter is None, session.mount should NOT be called."""
        mock_sess_inst = MagicMock()
        mock_session_cls.return_value = mock_sess_inst
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        s._session = None
        s.http_adapter = None
        s.init()
        mock_sess_inst.mount.assert_not_called()


# ===========================================================================
# close – async run exception path
# ===========================================================================

class TestCloseEdge:
    def test_close_with_async_session_run_succeeds(self):
        s = _make_session()
        s.http_adapter = None
        mock_async = MagicMock()
        mock_async.aclose = AsyncMock()
        s._session_async = mock_async
        # asyncio.run should be called with _close_async coroutine
        with patch('asyncio.run') as mock_run:
            s.close()
        mock_run.assert_called_once()
        assert s._session is None

    def test_close_no_session_no_async(self):
        s = _make_session()
        s._session = None
        s._session_async = None
        s.close()


# ===========================================================================
# _config_for_environment caching
# ===========================================================================

class TestConfigCache:
    def test_config_cached(self):
        GsSession._GsSession__config = None
        config1 = GsSession._config_for_environment('PROD')
        config2 = GsSession._config_for_environment('QA')
        # Both should work from the same config object
        assert config1 is not None
        assert config2 is not None


# ===========================================================================
# _build_request_params – tracing scope + request_headers combos for GET
# ===========================================================================

class TestBuildRequestParamsEdge:
    def test_get_tracing_only_no_request_headers(self):
        """GET with tracing but no request_headers: should still inject."""
        s = _make_session()
        scope = MagicMock()
        scope.span = MagicMock()
        with patch('gs_quant.session.Tracer') as MockTracer:
            kwargs = s._build_request_params(
                'GET', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', scope
            )
        assert 'headers' in kwargs

    def test_get_request_headers_only_no_tracing(self):
        """GET with request_headers but no tracing."""
        s = _make_session()
        kwargs = s._build_request_params(
            'GET', '/p', 'https://u', {'k': 'v'}, {'Custom': 'hdr'}, 30, False, 'data', None
        )
        assert 'headers' in kwargs
        assert kwargs['headers']['Custom'] == 'hdr'

    def test_post_tracing_scope_sets_content_type_tag(self):
        """POST with tracing scope should set request.content.type tag."""
        s = _make_session()
        scope = MagicMock()
        scope.span = MagicMock()
        with patch('gs_quant.session.Tracer') as MockTracer:
            kwargs = s._build_request_params(
                'POST', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', scope
            )
        scope.span.set_tag.assert_called()

    def test_delete_use_body_with_tracing(self):
        """DELETE with use_body=True goes through POST branch."""
        s = _make_session()
        scope = MagicMock()
        scope.span = MagicMock()
        with patch('gs_quant.session.Tracer') as MockTracer:
            kwargs = s._build_request_params(
                'DELETE', '/p', 'https://u', {'k': 'v'}, None, 30, True, 'data', scope
            )
        assert 'data' in kwargs


# ===========================================================================
# _parse_response – cls with non-Base, non-results path
# ===========================================================================

class TestParseResponseEdge:
    def test_json_with_cls_non_base_non_results(self):
        """JSON without 'results' key, non-Base cls."""
        s = _make_session()

        class SimpleObj:
            def __init__(self, key=None):
                self.key = key

        resp = _mock_response(200, json_body={'key': 'val'})
        result = s._parse_response('req1', resp, 'GET', 'https://u', SimpleObj, False)
        assert isinstance(result, SimpleObj)
        assert result.key == 'val'

    def test_json_with_cls_non_base_results_list(self):
        """JSON with 'results' key, non-Base cls."""
        s = _make_session()

        class SimpleObj:
            def __init__(self, key=None):
                self.key = key

        resp = _mock_response(200, json_body={'results': [{'key': 'v1'}]})
        result = s._parse_response('req1', resp, 'GET', 'https://u', SimpleObj, False)
        assert 'results' in result
        assert isinstance(result['results'], tuple)
        assert result['results'][0].key == 'v1'

    def test_other_content_type_with_request_id(self):
        s = _make_session()
        resp = _mock_response(200, content_type='text/plain', text='hello')
        result, rid = s._parse_response('req1', resp, 'GET', 'https://u', None, True)
        assert 'raw' in result
        assert rid == 'req1'

    def test_msgpack_with_cls_and_request_id(self):
        import msgpack
        s = _make_session()
        body = {'results': [{'a': 1}]}
        packed = msgpack.packb(body, use_bin_type=True)

        from gs_quant.base import Base
        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: {'unpacked': d})

        resp = _mock_response(200, content_type='application/x-msgpack', content=packed)
        result, rid = s._parse_response('req1', resp, 'GET', 'https://u', mock_cls, True)
        assert rid == 'req1'
        assert 'results' in result


# ===========================================================================
# _authenticate_async – with closed async session
# ===========================================================================

class TestAuthenticateAsyncEdge:
    def test_authenticate_async_closed_session(self):
        """Async session exists but is closed -> _has_async_session returns False."""
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = True
        s._session_async = mock_async
        # Should not raise, just return silently
        s._authenticate_async()
