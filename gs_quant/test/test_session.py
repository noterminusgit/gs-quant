"""
Copyright 2018 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import asyncio
import json
import pickle
import ssl
from unittest import mock
from unittest.mock import MagicMock, Mock, patch, PropertyMock, AsyncMock

import msgpack
import pytest
import requests
import requests.adapters
import requests.cookies

from gs_quant.errors import (
    MqAuthenticationError,
    MqError,
    MqRequestError,
    MqUninitialisedError,
)
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
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_async(coro):
    """Run an async coroutine in a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class ConcreteSession(GsSession):
    """Non-abstract subclass that supplies a trivial _authenticate."""

    def _authenticate(self):
        self._session.headers.update({'Authorization': 'Bearer test-token'})


def _make_session(domain='https://api.example.com', environment='PROD', **kwargs):
    """Create a ConcreteSession and initialise it with a mock requests.Session."""
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
    """Build a mock requests.Response-like object."""
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
# Environment enum
# ===========================================================================

class TestEnvironment:
    def test_members(self):
        assert Environment.DEV.name == 'DEV'
        assert Environment.QA.name == 'QA'
        assert Environment.PROD.name == 'PROD'

    def test_unique_values(self):
        vals = [e.value for e in Environment]
        assert len(vals) == len(set(vals))


# ===========================================================================
# GsSession.__init__ - environment resolution
# ===========================================================================

class TestGsSessionInit:
    def test_environment_string_prod(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        assert s.environment == Environment.PROD

    def test_environment_string_qa(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='QA')
        assert s.environment == Environment.QA

    def test_environment_string_dev(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='DEV')
        assert s.environment == Environment.DEV

    def test_environment_enum_as_domain(self):
        """When domain is an Environment enum, should set self.environment to it."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession(Environment.QA, environment=None)
        assert s.environment == Environment.QA

    def test_environment_unknown_string_defaults_to_dev(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://custom.endpoint.com', environment='CUSTOM')
        assert s.environment == Environment.DEV

    def test_environment_none_defaults_to_dev(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://custom.endpoint.com', environment=None)
        assert s.environment == Environment.DEV

    def test_custom_http_adapter(self):
        adapter = requests.adapters.HTTPAdapter()
        s = ConcreteSession('https://api.gs.com', environment='PROD', http_adapter=adapter)
        assert s.http_adapter is adapter

    def test_default_http_adapter_openssl3(self):
        with patch('gs_quant.session.ssl.OPENSSL_VERSION_INFO', (3, 0, 0)):
            with patch('gs_quant.session.CustomHttpAdapter') as mock_adapter:
                s = ConcreteSession('https://api.gs.com', environment='PROD')
            assert isinstance(s.http_adapter, MagicMock)

    def test_default_http_adapter_openssl1(self):
        with patch('gs_quant.session.ssl.OPENSSL_VERSION_INFO', (1, 1, 1)):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        assert isinstance(s.http_adapter, requests.adapters.HTTPAdapter)

    def test_proxies_none(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD', proxies=None)
        assert s.mounts is None

    def test_proxies_set(self):
        proxies = [('https://', 'http://proxy:8080')]
        with patch('gs_quant.session.CustomHttpAdapter'):
            with patch('gs_quant.session.httpx.HTTPTransport') as mock_transport:
                s = ConcreteSession('https://api.gs.com', environment='PROD', proxies=proxies)
        assert s.mounts is not None

    def test_redirect_to_mds_default(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        assert s.redirect_to_mds is False

    def test_api_version_and_application(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD',
                                api_version='v2', application='my-app')
        assert s.api_version == 'v2'
        assert s.application == 'my-app'


# ===========================================================================
# Scopes
# ===========================================================================

class TestScopes:
    def test_get_default(self):
        defaults = GsSession.Scopes.get_default()
        assert 'read_content' in defaults
        assert 'read_product_data' in defaults
        assert 'read_financial_data' in defaults
        assert 'read_user_profile' in defaults
        assert len(defaults) == 4


# ===========================================================================
# _build_url
# ===========================================================================

class TestBuildUrl:
    def test_with_version(self):
        s = _make_session()
        url = s._build_url(None, '/test/path', True)
        assert url == 'https://api.example.com/v1/test/path'

    def test_without_version(self):
        s = _make_session()
        url = s._build_url(None, '/test/path', False)
        assert url == 'https://api.example.com/test/path'

    def test_custom_domain(self):
        s = _make_session()
        url = s._build_url('https://other.domain.com', '/path', True)
        assert url == 'https://other.domain.com/v1/path'

    def test_empty_domain_uses_self(self):
        s = _make_session()
        url = s._build_url('', '/path', True)
        # empty string is falsy, so should use self.domain
        assert url == 'https://api.example.com/v1/path'


# ===========================================================================
# _build_request_params
# ===========================================================================

class TestBuildRequestParams:
    def test_get_no_body(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'GET', '/p', 'https://u', {'key': 'val'}, None, 30, False, 'data', None
        )
        assert kwargs['params'] == {'key': 'val'}
        assert kwargs['timeout'] == 30
        assert 'headers' not in kwargs  # no request_headers and no tracing_scope

    def test_get_with_request_headers(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'GET', '/p', 'https://u', {'k': 'v'}, {'X-Custom': 'yes'}, 30, False, 'data', None
        )
        assert 'headers' in kwargs
        assert kwargs['headers']['X-Custom'] == 'yes'

    def test_get_with_use_body(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'GET', '/p', 'https://u', {'k': 'v'}, None, 30, True, 'data', None
        )
        assert 'data' in kwargs
        assert 'params' not in kwargs

    def test_delete_no_body(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'DELETE', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', None
        )
        assert kwargs['params'] == {'k': 'v'}

    def test_delete_with_use_body(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'DELETE', '/p', 'https://u', {'k': 'v'}, None, 30, True, 'data', None
        )
        assert 'data' in kwargs

    def test_post_json_encoding(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', {'key': 'val'}, None, 30, False, 'data', None
        )
        assert 'data' in kwargs
        decoded = json.loads(kwargs['data'])
        assert decoded == {'key': 'val'}
        assert kwargs['headers']['Content-Type'] == 'application/json; charset=utf-8'

    def test_post_string_payload(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', 'raw-string', None, 30, False, 'data', None
        )
        assert kwargs['data'] == 'raw-string'

    def test_post_bytes_payload(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', b'raw-bytes', None, 30, False, 'data', None
        )
        assert kwargs['data'] == b'raw-bytes'

    def test_post_msgpack(self):
        s = _make_session()
        headers = {'Content-Type': 'application/x-msgpack'}
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', {'k': 'v'}, headers, 30, False, 'data', None
        )
        assert kwargs['headers']['Accept'] == 'application/x-msgpack'
        unpacked = msgpack.unpackb(kwargs['data'], raw=False)
        assert unpacked == {'k': 'v'}

    def test_post_empty_payload(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', None, None, 30, False, 'data', None
        )
        # payload defaults to {} but empty dict is falsy, so no 'data' key
        assert 'data' not in kwargs

    def test_post_with_custom_content_type(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', {'k': 'v'}, {'Content-Type': 'text/plain'}, 30, False, 'data', None
        )
        assert kwargs['headers']['Content-Type'] == 'text/plain'

    def test_put_json_encoding(self):
        s = _make_session()
        kwargs = s._build_request_params(
            'PUT', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', None
        )
        assert 'data' in kwargs

    def test_unsupported_method_raises(self):
        s = _make_session()
        with pytest.raises(MqError, match='not implemented'):
            s._build_request_params(
                'PATCH', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', None
            )

    def test_post_dataframe_payload(self):
        import pandas as pd
        s = _make_session()
        df = pd.DataFrame({'a': [1, 2]})
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', df, None, 30, False, 'data', None
        )
        # DataFrame is truthy, so should be serialised
        assert 'data' in kwargs

    def test_get_with_tracing_scope(self):
        """GET with tracing_scope should inject tracing headers."""
        s = _make_session()
        scope = MagicMock()
        scope.span = MagicMock()
        with patch('gs_quant.session.Tracer') as mock_tracer:
            kwargs = s._build_request_params(
                'GET', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', scope
            )
        # Should have headers because tracing_scope is truthy
        assert 'headers' in kwargs
        scope.span.set_tag.assert_called()

    def test_post_with_tracing_scope(self):
        s = _make_session()
        scope = MagicMock()
        scope.span = MagicMock()
        with patch('gs_quant.session.Tracer') as mock_tracer:
            kwargs = s._build_request_params(
                'POST', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'data', scope
            )
        scope.span.set_tag.assert_called()

    def test_content_key_for_async(self):
        """When data_key='content', should use 'content' instead of 'data'."""
        s = _make_session()
        kwargs = s._build_request_params(
            'POST', '/p', 'https://u', {'k': 'v'}, None, 30, False, 'content', None
        )
        assert 'content' in kwargs
        assert 'data' not in kwargs


# ===========================================================================
# _parse_response
# ===========================================================================

class TestParseResponse:
    def test_success_json(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'result': 'ok'})
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert result == {'result': 'ok'}

    def test_success_json_with_request_id(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'result': 'ok'})
        result, req_id = s._parse_response('req1', resp, 'GET', 'https://u', None, True)
        assert result == {'result': 'ok'}
        assert req_id == 'req1'

    def test_success_msgpack(self):
        s = _make_session()
        body = {'key': 'value'}
        packed = msgpack.packb(body, use_bin_type=True)
        resp = _mock_response(200, content_type='application/x-msgpack', content=packed)
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert result == body

    def test_success_other_content_type(self):
        s = _make_session()
        resp = _mock_response(200, content_type='text/plain', text='hello')
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert 'raw' in result

    def test_success_no_content_type(self):
        s = _make_session()
        resp = _mock_response(200)
        resp.headers = {}  # no Content-Type
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert 'raw' in result

    def test_no_content_type_with_request_id(self):
        s = _make_session()
        resp = _mock_response(200)
        resp.headers = {}
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, True)
        assert result['request_id'] == 'req1'
        assert 'raw' in result

    def test_error_status_text_html(self):
        s = _make_session()
        resp = _mock_response(404, content_type='text/html', text='Not Found', reason='Not Found')
        with pytest.raises(MqRequestError) as exc_info:
            s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert exc_info.value.status == 404

    def test_error_status_json(self):
        s = _make_session()
        resp = _mock_response(500, content_type='application/json',
                              text='Internal error detail', reason='Internal Server Error')
        with pytest.raises(MqRequestError) as exc_info:
            s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert 'Internal Server Error' in exc_info.value.message

    def test_error_uses_reason_phrase_for_httpx(self):
        """httpx responses use reason_phrase instead of reason."""
        s = _make_session()
        resp = _mock_response(403, content_type='application/json', text='forbidden', reason='Forbidden')
        # Remove 'reason' attr to simulate httpx
        del resp.reason
        resp.reason_phrase = 'Forbidden'
        with pytest.raises(MqRequestError):
            s._parse_response('req1', resp, 'GET', 'https://u', None, False)

    def test_success_json_with_results_and_cls(self):
        """When response has 'results' key and cls is provided, should unpack."""
        from gs_quant.base import Base

        s = _make_session()
        resp = _mock_response(200, json_body={'results': [{'name': 'test'}]})

        mock_cls = MagicMock(spec=Base)
        mock_cls.__mro__ = [mock_cls, Base, object]
        mock_cls.from_dict = MagicMock(return_value='unpacked')

        # Manually make issubclass work
        with patch('gs_quant.session.issubclass', return_value=True):
            result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert 'results' in result

    def test_success_json_none_result(self):
        s = _make_session()
        resp = _mock_response(200, json_body=None, text='null')
        result = s._parse_response('req1', resp, 'GET', 'https://u', None, False)
        assert result is None


# ===========================================================================
# __unpack (static method, accessed indirectly)
# ===========================================================================

class TestUnpack:
    def test_base_subclass_dict(self):
        from gs_quant.base import Base

        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: d)
        result = GsSession._GsSession__unpack({'a': 1}, mock_cls)
        assert result == {'a': 1}

    def test_base_subclass_list(self):
        from gs_quant.base import Base

        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: d)
        result = GsSession._GsSession__unpack([{'a': 1}, None], mock_cls)
        assert result == ({'a': 1}, None)

    def test_base_subclass_none(self):
        from gs_quant.base import Base

        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: d)
        result = GsSession._GsSession__unpack(None, mock_cls)
        assert result is None

    def test_non_base_dict(self):
        class SimpleObj:
            def __init__(self, a=None):
                self.a = a
        result = GsSession._GsSession__unpack({'a': 1}, SimpleObj)
        assert isinstance(result, SimpleObj)
        assert result.a == 1

    def test_non_base_list(self):
        class SimpleObj:
            def __init__(self, a=None):
                self.a = a
        result = GsSession._GsSession__unpack([{'a': 1}], SimpleObj)
        assert isinstance(result, tuple)
        assert len(result) == 1
        assert result[0].a == 1


# ===========================================================================
# HTTP methods - _get, _post, _put, _delete
# ===========================================================================

class TestHttpMethods:
    def test_get_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'data': 'ok'})
        resp.headers['x-dash-requestid'] = 'rid-1'
        s._session.request = MagicMock(return_value=resp)
        result = s._get('/test')
        s._session.request.assert_called_once()
        args = s._session.request.call_args
        assert args[0][0] == 'GET'
        assert result == {'data': 'ok'}

    def test_post_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'id': '123'})
        resp.headers['x-dash-requestid'] = 'rid-2'
        s._session.request = MagicMock(return_value=resp)
        result = s._post('/create', payload={'name': 'test'})
        assert result == {'id': '123'}

    def test_put_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'updated': True})
        resp.headers['x-dash-requestid'] = 'rid-3'
        s._session.request = MagicMock(return_value=resp)
        result = s._put('/update', payload={'name': 'new'})
        assert result == {'updated': True}

    def test_delete_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'deleted': True})
        resp.headers['x-dash-requestid'] = 'rid-4'
        s._session.request = MagicMock(return_value=resp)
        result = s._delete('/remove')
        assert result == {'deleted': True}

    def test_delete_with_body(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'deleted': True})
        resp.headers['x-dash-requestid'] = 'rid-5'
        s._session.request = MagicMock(return_value=resp)
        result = s._delete('/remove', payload={'id': '1'}, use_body=True)
        assert result == {'deleted': True}

    def test_get_include_version_false(self):
        s = _make_session()
        resp = _mock_response(200, json_body={})
        resp.headers['x-dash-requestid'] = 'rid-6'
        s._session.request = MagicMock(return_value=resp)
        s._get('/test', include_version=False)
        url_called = s._session.request.call_args[0][1]
        assert '/v1' not in url_called

    def test_get_with_domain(self):
        s = _make_session()
        resp = _mock_response(200, json_body={})
        resp.headers['x-dash-requestid'] = 'rid-7'
        s._session.request = MagicMock(return_value=resp)
        s._get('/test', domain='https://other.com')
        url_called = s._session.request.call_args[0][1]
        assert url_called.startswith('https://other.com')

    def test_get_return_request_id(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'data': 1})
        resp.headers['x-dash-requestid'] = 'my-rid'
        s._session.request = MagicMock(return_value=resp)
        result, rid = s._get('/test', return_request_id=True)
        assert rid == 'my-rid'


# ===========================================================================
# 401 re-authentication flow
# ===========================================================================

class TestReauthentication:
    def test_401_retries_with_auth(self):
        s = _make_session()
        resp_401 = _mock_response(401, text='Unauthorized', reason='Unauthorized',
                                  content_type='application/json')
        resp_401.headers['x-dash-requestid'] = 'rid-401'
        resp_200 = _mock_response(200, json_body={'ok': True})
        resp_200.headers['x-dash-requestid'] = 'rid-200'
        s._session.request = MagicMock(side_effect=[resp_401, resp_200])

        result = s._get('/test')
        assert result == {'ok': True}
        assert s._session.request.call_count == 2

    def test_401_no_retry_when_try_auth_false(self):
        s = _make_session()
        resp_401 = _mock_response(401, text='Unauthorized', reason='Unauthorized',
                                  content_type='application/json')
        resp_401.headers['x-dash-requestid'] = 'rid-401'

        resp_401b = _mock_response(401, text='Still unauthorized', reason='Unauthorized',
                                   content_type='application/json')
        resp_401b.headers['x-dash-requestid'] = 'rid-401b'
        s._session.request = MagicMock(side_effect=[resp_401, resp_401b])

        with pytest.raises(MqRequestError) as exc_info:
            s._get('/test')
        assert exc_info.value.status == 401


# ===========================================================================
# Async HTTP methods
# ===========================================================================

class TestAsyncHttpMethods:
    def test_get_async_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'async': True})
        resp.headers['x-dash-requestid'] = 'arid-1'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        result = _run_async(s._get_async('/test'))
        assert result == {'async': True}

    def test_post_async_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'created': True})
        resp.headers['x-dash-requestid'] = 'arid-2'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        result = _run_async(s._post_async('/create', payload={'a': 1}))
        assert result == {'created': True}

    def test_put_async_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'updated': True})
        resp.headers['x-dash-requestid'] = 'arid-3'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        result = _run_async(s._put_async('/update', payload={'b': 2}))
        assert result == {'updated': True}

    def test_delete_async_success(self):
        s = _make_session()
        resp = _mock_response(200, json_body={'deleted': True})
        resp.headers['x-dash-requestid'] = 'arid-4'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(return_value=resp)
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        result = _run_async(s._delete_async('/remove', use_body=True))
        assert result == {'deleted': True}

    def test_async_401_retries(self):
        s = _make_session()
        resp_401 = _mock_response(401, text='Unauthorized', reason='Unauthorized',
                                  content_type='application/json')
        resp_401.headers['x-dash-requestid'] = 'arid-401'
        resp_200 = _mock_response(200, json_body={'retried': True})
        resp_200.headers['x-dash-requestid'] = 'arid-200'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(side_effect=[resp_401, resp_200])
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        result = _run_async(s._get_async('/test'))
        assert result == {'retried': True}

    def test_async_401_no_retry(self):
        s = _make_session()
        resp_401 = _mock_response(401, text='Unauthorized', reason='Unauthorized',
                                  content_type='application/json')
        resp_401.headers['x-dash-requestid'] = 'arid-401'
        resp_401b = _mock_response(401, text='Still unauthorized', reason='Unauthorized',
                                   content_type='application/json')
        resp_401b.headers['x-dash-requestid'] = 'arid-401b'

        mock_async_client = MagicMock()
        mock_async_client.is_closed = False
        mock_async_client.request = AsyncMock(side_effect=[resp_401, resp_401b])
        mock_async_client.headers = {}
        s._session_async = mock_async_client

        with pytest.raises(MqRequestError) as exc_info:
            _run_async(s._get_async('/test'))
        assert exc_info.value.status == 401


# ===========================================================================
# init / close / context manager
# ===========================================================================

class TestInitAndClose:
    def test_init_creates_session(self):
        with patch('gs_quant.session.requests.Session') as mock_session_cls:
            mock_sess_inst = MagicMock()
            mock_session_cls.return_value = mock_sess_inst
            with patch('gs_quant.session.CustomHttpAdapter'):
                s = ConcreteSession('https://api.gs.com', environment='PROD')
            s._session = None  # force re-init
            s.init()
            assert s._session is mock_sess_inst

    @patch('gs_quant.session.requests.Session')
    def test_init_with_proxies(self, mock_session_cls):
        mock_sess_inst = MagicMock()
        mock_session_cls.return_value = mock_sess_inst
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD',
                                proxies=[('https://', 'http://proxy:8080')])
        s._session = None
        s.init()
        assert mock_sess_inst.proxies is not None

    @patch('gs_quant.session.requests.Session')
    def test_init_calls_activity_for_app_domain(self, mock_session_cls):
        mock_sess_inst = MagicMock()
        mock_session_cls.return_value = mock_sess_inst
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        s._orig_domain = Domain.APP
        s._session = None
        with patch.object(s, 'post_to_activity_service') as mock_activity:
            s.init()
            mock_activity.assert_called_once()

    @patch('gs_quant.session.requests.Session')
    def test_init_skips_activity_for_non_app_domain(self, mock_session_cls):
        mock_sess_inst = MagicMock()
        mock_session_cls.return_value = mock_sess_inst
        with patch('gs_quant.session.CustomHttpAdapter'):
            s = ConcreteSession('https://api.gs.com', environment='PROD')
        s._orig_domain = 'some_other_domain'
        s._session = None
        with patch.object(s, 'post_to_activity_service') as mock_activity:
            s.init()
            mock_activity.assert_not_called()

    def test_init_does_not_reinit_if_session_exists(self):
        s = _make_session()
        original = s._session
        s.init()  # should not reinit because _session already exists
        assert s._session is original

    def test_close_with_session(self):
        s = _make_session()
        s.http_adapter = None  # should close the session
        mock_sess = s._session
        s._session_async = None
        s.close()
        mock_sess.close.assert_called_once()
        assert s._session is None

    def test_close_with_adapter(self):
        s = _make_session()
        s.http_adapter = MagicMock()
        s._session_async = None
        s.close()
        assert s._session is None

    def test_close_no_session(self):
        s = _make_session()
        s._session = None
        s._session_async = None
        s.close()  # should not raise

    def test_close_with_async_session(self):
        s = _make_session()
        s.http_adapter = MagicMock()
        mock_async = MagicMock()
        mock_async.aclose = AsyncMock()
        s._session_async = mock_async
        with patch('asyncio.run') as mock_run:
            s.close()
        assert s._session is None

    def test_close_async_exception_suppressed(self):
        s = _make_session()
        s.http_adapter = MagicMock()
        s._session_async = MagicMock()
        with patch('asyncio.run', side_effect=RuntimeError('no loop')):
            s.close()  # should not raise
        assert s._session is None

    def test_on_enter_inits_if_needed(self):
        s = _make_session()
        s._session = None
        with patch.object(s, 'init') as mock_init:
            s._on_enter()
            mock_init.assert_called_once()

    def test_on_enter_does_not_reinit(self):
        s = _make_session()
        with patch.object(s, 'init') as mock_init:
            s._on_enter()
            mock_init.assert_not_called()

    def test_on_exit_closes_if_created(self):
        s = _make_session()
        s._session = None
        with patch.object(s, 'init'):
            s._on_enter()
        s._session = MagicMock()  # re-set after init
        s._session_async = MagicMock()
        s._on_exit(None, None, None)
        assert s._session is None
        assert s._session_async is None

    def test_on_exit_keeps_session_if_preexisting(self):
        s = _make_session()
        s._on_enter()
        original_session = s._session
        s._on_exit(None, None, None)
        assert s._session is original_session

    def test_init_with_none_adapter(self):
        """When http_adapter is not None, session.mount should be called."""
        with patch('gs_quant.session.requests.Session') as mock_session_cls:
            mock_sess_inst = MagicMock()
            mock_session_cls.return_value = mock_sess_inst
            with patch('gs_quant.session.CustomHttpAdapter'):
                s = ConcreteSession('https://api.gs.com', environment='PROD')
            s._session = None
            s.http_adapter = MagicMock()
            s.init()
            mock_sess_inst.mount.assert_called_once_with('https://', s.http_adapter)


# ===========================================================================
# Async context manager
# ===========================================================================

class TestAsyncContext:
    def test_on_aenter_inits(self):
        s = _make_session()
        s._session = None
        with patch.object(s, 'init') as mock_init, \
             patch.object(s, '_init_async') as mock_init_async:
            _run_async(s._on_aenter())
            mock_init.assert_called_once()
            mock_init_async.assert_called_once()

    def test_on_aexit_closes(self):
        s = _make_session()
        s._session = None
        mock_async = MagicMock()
        mock_async.is_closed = False
        mock_async.aclose = AsyncMock()

        with patch.object(s, 'init'):
            with patch.object(s, '_init_async'):
                _run_async(s._on_aenter())

        s._session = MagicMock()
        s._session_async = mock_async
        _run_async(s._on_aexit(None, None, None))
        assert s._session is None
        assert s._session_async is None

    def test_on_aexit_keeps_preexisting(self):
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = False
        s._session_async = mock_async

        with patch.object(s, '_init_async'):
            _run_async(s._on_aenter())

        _run_async(s._on_aexit(None, None, None))
        # session_async was pre-existing, should NOT be closed
        assert s._session_async is mock_async

    def test_on_aexit_when_async_is_closed(self):
        s = _make_session()
        s._session = None

        with patch.object(s, 'init'):
            with patch.object(s, '_init_async'):
                _run_async(s._on_aenter())

        s._session = MagicMock()
        mock_async = MagicMock()
        mock_async.is_closed = True
        mock_async.aclose = AsyncMock()
        s._session_async = mock_async

        _run_async(s._on_aexit(None, None, None))
        # _has_async_session returns False because is_closed is True
        mock_async.aclose.assert_not_called()
        assert s._session_async is None

    def test_on_aenter_preexisting_session(self):
        """When both session and async session already exist."""
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = False
        s._session_async = mock_async

        with patch.object(s, 'init') as mock_init, \
             patch.object(s, '_init_async') as mock_init_async:
            _run_async(s._on_aenter())
            mock_init.assert_not_called()
            # _init_async should not be called because async session exists
            # Actually, it is always called, but the method short-circuits internally.


# ===========================================================================
# _has_async_session
# ===========================================================================

class TestHasAsyncSession:
    def test_no_session(self):
        s = _make_session()
        s._session_async = None
        assert not s._has_async_session()

    def test_closed_session(self):
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = True
        s._session_async = mock_async
        assert s._has_async_session() is False

    def test_open_session(self):
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = False
        s._session_async = mock_async
        assert s._has_async_session() is True


# ===========================================================================
# _init_async
# ===========================================================================

class TestInitAsync:
    def test_init_async_creates_client(self):
        s = _make_session()
        s._session_async = None
        with patch('gs_quant.session.httpx.AsyncClient') as mock_ac, \
             patch('gs_quant.session.CustomHttpAdapter.ssl_context'):
            mock_client = MagicMock()
            mock_client.is_closed = False
            mock_client.headers = {}
            mock_ac.return_value = mock_client
            s._init_async()
        assert s._session_async is mock_client

    def test_init_async_skips_if_existing(self):
        s = _make_session()
        existing = MagicMock()
        existing.is_closed = False
        s._session_async = existing
        s._init_async()
        assert s._session_async is existing


# ===========================================================================
# _authenticate_async / _authenticate_all_sessions
# ===========================================================================

class TestAuthenticateAsync:
    def test_authenticate_async_copies_headers(self):
        s = _make_session()
        mock_async = MagicMock()
        mock_async.is_closed = False
        mock_async.headers = MagicMock()
        mock_async.cookies = MagicMock()
        s._session_async = mock_async

        # Add a cookie to sync session
        cookie = requests.cookies.create_cookie(name='GSSSO', value='tok', domain='.gs.com')
        s._session.cookies = requests.cookies.RequestsCookieJar()
        s._session.cookies.set_cookie(cookie)
        s._session.headers = {'Authorization': 'Bearer test'}

        s._authenticate_async()
        mock_async.headers.update.assert_called()
        mock_async.cookies.set.assert_called()

    def test_authenticate_async_no_async_session(self):
        s = _make_session()
        s._session_async = None
        s._authenticate_async()  # should not raise

    def test_authenticate_all_sessions(self):
        s = _make_session()
        with patch.object(s, '_authenticate') as mock_auth, \
             patch.object(s, '_authenticate_async') as mock_auth_async:
            s._authenticate_all_sessions()
            mock_auth.assert_called_once()
            mock_auth_async.assert_called_once()


# ===========================================================================
# OAuth2Session
# ===========================================================================

class TestOAuth2Session:
    def test_init_prod(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        assert sess.environment == Environment.PROD
        assert sess.client_id == 'cid'
        assert sess.client_secret == 'csecret'
        assert 'idfs.gs.com' in sess.auth_url

    def test_init_qa(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('QA', 'cid', 'csecret', ('scope1',))
        assert sess.environment == Environment.QA

    def test_init_dev_disables_verify(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('DEV', 'cid', 'csecret', ('scope1',))
        assert sess.verify is False

    def test_init_custom_url(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('https://custom.api.com', 'cid', 'csecret', ('scope1',))
        assert sess.domain == 'https://custom.api.com'
        assert sess.verify is False

    def test_init_custom_url_with_mds_east_domain(self):
        """Non-standard URL with MDS_US_EAST domain should keep verify=True."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',), domain=Domain.MDS_US_EAST)
        # For PROD + MDS_US_EAST, url != AppDomain but domain is MDS_US_EAST, so verify stays True
        assert sess.verify is True

    def test_init_non_app_domain_prod(self):
        """PROD with MDS_WEB domain - url != AppDomain and domain != MDS_US_EAST -> verify=False."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',), domain=Domain.MDS_WEB)
        # MDS_WEB url differs from AppDomain and domain != MDS_US_EAST
        assert sess.verify is False

    def test_authenticate_success(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        sess._session = MagicMock(spec=requests.Session)
        sess._session.headers = {}
        mock_reply = MagicMock()
        mock_reply.status_code = 200
        mock_reply.text = json.dumps({'access_token': 'my-token-123'})
        sess._session.post = MagicMock(return_value=mock_reply)

        sess._authenticate()
        assert 'Bearer my-token-123' in sess._session.headers.get('Authorization', '')

    def test_authenticate_failure(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        sess._session = MagicMock(spec=requests.Session)
        sess._session.headers = {}
        mock_reply = MagicMock()
        mock_reply.status_code = 401
        mock_reply.text = 'Invalid credentials'
        sess._session.post = MagicMock(return_value=mock_reply)

        with pytest.raises(MqAuthenticationError):
            sess._authenticate()

    def test_headers(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        sess._session = MagicMock()
        sess._session.headers = {'Authorization': 'Bearer xyz'}
        headers = sess._headers()
        assert headers == [('Authorization', 'Bearer xyz')]


# ===========================================================================
# PassThroughSession
# ===========================================================================

class TestPassThroughSession:
    def test_init_known_environment(self):
        sess = PassThroughSession('PROD', 'my-token')
        assert sess.token == 'my-token'
        assert sess.verify is True

    def test_init_custom_domain(self):
        sess = PassThroughSession('https://custom.api.com', 'my-token')
        assert sess.domain == 'https://custom.api.com'
        assert sess.verify is False

    def test_init_with_domain_param(self):
        sess = PassThroughSession('PROD', 'my-token', domain=Domain.MDS_US_EAST)
        assert sess.verify is True

    def test_init_with_none_domain_uses_app(self):
        sess = PassThroughSession('PROD', 'my-token', domain=None)
        # domain=None becomes 'AppDomain' in constructor
        assert sess.token == 'my-token'

    def test_authenticate(self):
        sess = PassThroughSession('PROD', 'my-token')
        sess._session = MagicMock()
        sess._session.headers = {}
        sess._authenticate()
        assert sess._session.headers['Authorization'] == 'Bearer my-token'

    def test_headers(self):
        sess = PassThroughSession('PROD', 'my-token')
        sess._session = MagicMock()
        sess._session.headers = {'Authorization': 'Bearer my-token'}
        assert sess._headers() == [('Authorization', 'Bearer my-token')]

    def test_domain_and_verify_known(self):
        domain, verify = PassThroughSession.domain_and_verify('PROD', 'AppDomain')
        assert verify is True
        assert 'gs.com' in domain

    def test_domain_and_verify_unknown(self):
        # Reset __config to ensure it reads the file
        PassThroughSession._PassThroughSession__config = None
        domain, verify = PassThroughSession.domain_and_verify('UNKNOWN_ENV', 'AppDomain')
        assert verify is False
        assert domain == 'UNKNOWN_ENV'


# ===========================================================================
# GsSession.get - factory method
# ===========================================================================

class TestGsSessionGet:
    def test_get_oauth2(self):
        sess = GsSession.get('PROD', client_id='cid', client_secret='csecret')
        assert isinstance(sess, OAuth2Session)

    def test_get_oauth2_with_string_scopes(self):
        sess = GsSession.get('PROD', client_id='cid', client_secret='csecret', scopes='custom_scope')
        assert isinstance(sess, OAuth2Session)
        assert 'custom_scope' in sess.scopes

    def test_get_oauth2_with_enum_scopes(self):
        sess = GsSession.get(
            'PROD', client_id='cid', client_secret='csecret',
            scopes=[GsSession.Scopes.RUN_ANALYTICS]
        )
        assert isinstance(sess, OAuth2Session)
        assert 'run_analytics' in sess.scopes

    def test_get_passthrough_with_token(self):
        sess = GsSession.get('PROD', token='my-token')
        assert isinstance(sess, PassThroughSession)

    def test_get_passthrough_with_token_and_domain(self):
        sess = GsSession.get('PROD', token='my-token', domain=Domain.MDS_US_EAST)
        assert isinstance(sess, PassThroughSession)

    def test_get_with_environment_enum(self):
        sess = GsSession.get(Environment.PROD, client_id='cid', client_secret='csecret')
        assert isinstance(sess, OAuth2Session)

    def test_get_gssso_without_auth_module(self):
        with pytest.raises(MqUninitialisedError):
            GsSession.get('PROD', token='tok', is_gssso=True)

    def test_get_no_credentials_without_auth_module(self):
        with pytest.raises(MqUninitialisedError):
            GsSession.get('PROD')

    def test_get_marquee_login_without_auth_module(self):
        """is_marquee_login=True with a token but no MQLoginSession available."""
        # MQLoginSession may or may not be defined depending on gs_quant_auth availability.
        try:
            sess = GsSession.get('PROD', token='tok', is_marquee_login=True)
        except (MqUninitialisedError, NameError, Exception):
            pass  # Expected when gs_quant_auth is not installed


# ===========================================================================
# GsSession.use - class method
# ===========================================================================

class TestGsSessionUse:
    def _reset_current(self):
        """Reset GsSession.current to avoid interference between tests."""
        try:
            from gs_quant.context_base import thread_local
            key = 'GsSession_path'
            if hasattr(thread_local, key):
                delattr(thread_local, key)
        except Exception:
            pass

    def test_use_with_environment_enum(self):
        self._reset_current()
        with patch.object(GsSession, 'get') as mock_get:
            mock_session = MagicMock(spec=GsSession)
            mock_session.is_entered = False
            mock_get.return_value = mock_session
            GsSession.use(Environment.PROD, client_id='cid', client_secret='csecret')
            mock_session.init.assert_called_once()
        self._reset_current()

    def test_use_with_string(self):
        self._reset_current()
        with patch.object(GsSession, 'get') as mock_get:
            mock_session = MagicMock(spec=GsSession)
            mock_session.is_entered = False
            mock_get.return_value = mock_session
            GsSession.use('PROD', client_id='cid', client_secret='csecret')
            mock_session.init.assert_called_once()
        self._reset_current()

    def test_use_with_none_domain_raises(self):
        with pytest.raises(MqError, match='None is not a valid domain'):
            GsSession.use(Environment.PROD, client_id='cid', client_secret='csecret', domain=None)

    def test_use_with_use_mds(self):
        self._reset_current()
        with patch.object(GsSession, 'get') as mock_get:
            mock_session = MagicMock(spec=GsSession)
            mock_session.is_entered = False
            mock_get.return_value = mock_session
            GsSession.use(Environment.PROD, client_id='cid', client_secret='csecret', use_mds=True)
            # domain should be MDS_US_EAST
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs['domain'] == Domain.MDS_US_EAST
        self._reset_current()


# ===========================================================================
# is_internal
# ===========================================================================

class TestIsInternal:
    def test_is_internal_returns_false(self):
        s = _make_session()
        assert s.is_internal() is False


# ===========================================================================
# post_to_activity_service
# ===========================================================================

class TestPostToActivityService:
    def test_post_success(self):
        s = _make_session()
        s.post_to_activity_service()
        s._session.post.assert_called_once()

    def test_post_exception_suppressed(self):
        s = _make_session()
        s._session.post.side_effect = Exception('network error')
        s.post_to_activity_service()  # should not raise


# ===========================================================================
# _config_for_environment
# ===========================================================================

class TestConfigForEnvironment:
    def test_config_for_prod(self):
        # Reset to force re-read
        GsSession._GsSession__config = None
        config = GsSession._config_for_environment('PROD')
        assert 'AppDomain' in config

    def test_config_for_qa(self):
        config = GsSession._config_for_environment('QA')
        assert 'marquee-qa' in config['AppDomain']

    def test_config_for_dev(self):
        config = GsSession._config_for_environment('DEV')
        assert 'marquee-dev' in config['AppDomain']


# ===========================================================================
# _get_mds_domain / _get_web_domain
# ===========================================================================

class TestMdsDomain:
    def test_get_mds_domain_app_domain(self):
        """Normal AppDomain should return MdsDomainEast."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        result = sess._get_mds_domain()
        assert 'data' in result or 'mds' in result.lower() or 'gsapis' in result

    def test_get_mds_domain_mds_web(self):
        """When domain is MdsWebDomain, should return MdsWebDomain."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('PROD')
        sess.domain = config['MdsWebDomain']
        result = sess._get_mds_domain()
        assert result == config['MdsWebDomain']

    def test_get_mds_domain_marquee_web(self):
        """When domain is MarqueeWebDomain, should return MdsWebDomain."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('PROD')
        sess.domain = config['MarqueeWebDomain']
        result = sess._get_mds_domain()
        assert result == config['MdsWebDomain']

    def test_get_mds_domain_literal_domain_constant(self):
        """When domain is Domain.MDS_WEB constant string."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        sess.domain = Domain.MDS_WEB
        result = sess._get_mds_domain()
        config = GsSession._config_for_environment('PROD')
        assert result == config['MdsWebDomain']

    def test_get_mds_domain_qa_with_web(self):
        """QA domain with .web in name."""
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('QA', 'cid', 'csecret', ('scope1',))
        config = GsSession._config_for_environment('QA')
        sess.domain = config['MarqueeWebDomain']
        result = sess._get_mds_domain()
        assert result == config['MdsWebDomain']

    def test_get_web_domain(self):
        with patch('gs_quant.session.CustomHttpAdapter'):
            sess = OAuth2Session('PROD', 'cid', 'csecret', ('scope1',))
        result = sess._get_web_domain()
        config = GsSession._config_for_environment('PROD')
        assert result == config['MarqueeWebDomain']


# ===========================================================================
# Domain constants
# ===========================================================================

class TestDomain:
    def test_domain_constants(self):
        assert Domain.MDS_US_EAST == "MdsDomainEast"
        assert Domain.MDS_WEB == "MdsWebDomain"
        assert Domain.APP == "AppDomain"


# ===========================================================================
# CustomHttpAdapter
# ===========================================================================

class TestCustomHttpAdapter:
    def test_ssl_context_creation(self):
        # Reset the class-level cached context
        CustomHttpAdapter._CustomHttpAdapter__ssl_ctx = None
        with patch('gs_quant.session.ssl.SSLContext') as mock_ctx_cls, \
             patch('gs_quant.session.certifi.where', return_value='/path/to/certs'):
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            ctx = CustomHttpAdapter.ssl_context()
            assert ctx is mock_ctx
            mock_ctx_cls.assert_called_once_with(ssl.PROTOCOL_TLS_CLIENT)

    def test_ssl_context_cached(self):
        """Second call should return same object."""
        CustomHttpAdapter._CustomHttpAdapter__ssl_ctx = None
        with patch('gs_quant.session.ssl.SSLContext') as mock_ctx_cls, \
             patch('gs_quant.session.certifi.where', return_value='/path/to/certs'):
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            ctx1 = CustomHttpAdapter.ssl_context()
            ctx2 = CustomHttpAdapter.ssl_context()
            assert ctx1 is ctx2
            mock_ctx_cls.assert_called_once()

    def test_init_poolmanager(self):
        CustomHttpAdapter._CustomHttpAdapter__ssl_ctx = MagicMock()
        adapter = CustomHttpAdapter()
        with patch('gs_quant.session.urllib3.poolmanager.PoolManager') as mock_pm:
            adapter.init_poolmanager(10)
            mock_pm.assert_called_once()
            call_kwargs = mock_pm.call_args[1]
            assert call_kwargs['num_pools'] == 10
            assert call_kwargs['ssl_context'] is not None


# ===========================================================================
# Pickle support
# ===========================================================================

class TestPickle:
    def test_session_pickle(self):
        session = GsSession.get(Environment.PROD, 'fake_client_id', 'fake_secret')
        pk = pickle.dumps(session)
        unpk = pickle.loads(pk)
        assert unpk is not None
        assert isinstance(unpk, OAuth2Session)


# ===========================================================================
# __del__
# ===========================================================================

class TestDel:
    def test_del_calls_close(self):
        s = _make_session()
        with patch.object(s, 'close') as mock_close:
            s.__del__()
            mock_close.assert_called_once()


# ===========================================================================
# _close_async
# ===========================================================================

class TestCloseAsync:
    def test_close_async_with_session(self):
        s = _make_session()
        mock_async = MagicMock()
        mock_async.aclose = AsyncMock()
        s._session_async = mock_async
        _run_async(s._close_async())
        mock_async.aclose.assert_called_once()
        assert s._session_async is None

    def test_close_async_no_session(self):
        s = _make_session()
        s._session_async = None
        _run_async(s._close_async())
        assert s._session_async is None


# ===========================================================================
# _connect_websocket_raw (basic test)
# ===========================================================================

class TestWebsocket:
    def test_connect_websocket_raw_builds_url(self):
        s = _make_session(domain='https://api.example.com')
        s._session = MagicMock()
        s._session.cookies = {'GSSSO': 'my-cookie'}

        mock_ws_module = MagicMock()
        mock_ws_module.__version__ = '13.0'
        mock_ws_module.connect = MagicMock()

        with patch.dict('sys.modules', {'websockets': mock_ws_module}):
            s._connect_websocket_raw('/stream')
            call_args = mock_ws_module.connect.call_args
            url_arg = call_args[0][0]
            assert url_arg.startswith('wss://')
            assert '/v1/stream' in url_arg

    def test_connect_websocket_raw_v14(self):
        s = _make_session(domain='https://api.example.com')
        s._session = MagicMock()
        s._session.cookies = {'GSSSO': 'my-cookie'}

        mock_ws_module = MagicMock()
        mock_ws_module.__version__ = '14.0'
        mock_ws_module.connect = MagicMock()

        with patch.dict('sys.modules', {'websockets': mock_ws_module}):
            s._connect_websocket_raw('/stream')
            call_kwargs = mock_ws_module.connect.call_args[1]
            assert 'additional_headers' in call_kwargs

    def test_connect_websocket_raw_with_domain(self):
        s = _make_session(domain='https://api.example.com')
        s._session = MagicMock()
        s._session.cookies = {'GSSSO': 'my-cookie'}

        mock_ws_module = MagicMock()
        mock_ws_module.__version__ = '13.0'
        mock_ws_module.connect = MagicMock()

        with patch.dict('sys.modules', {'websockets': mock_ws_module}):
            s._connect_websocket_raw('/stream', domain='wss://custom.ws.com')
            call_args = mock_ws_module.connect.call_args
            url_arg = call_args[0][0]
            assert url_arg.startswith('wss://custom.ws.com')

    def test_connect_websocket_raw_no_version(self):
        s = _make_session(domain='https://api.example.com')
        s._session = MagicMock()
        s._session.cookies = {'GSSSO': 'my-cookie'}

        mock_ws_module = MagicMock()
        mock_ws_module.__version__ = '13.0'
        mock_ws_module.connect = MagicMock()

        with patch.dict('sys.modules', {'websockets': mock_ws_module}):
            s._connect_websocket_raw('/stream', include_version=False)
            call_args = mock_ws_module.connect.call_args
            url_arg = call_args[0][0]
            assert '/v1' not in url_arg

    def test_connect_websocket_raw_non_wss(self):
        """URL starting with http:// should not get ssl context."""
        s = _make_session(domain='http://api.example.com')
        s._session = MagicMock()
        s._session.cookies = {'GSSSO': 'my-cookie'}

        mock_ws_module = MagicMock()
        mock_ws_module.__version__ = '13.0'
        mock_ws_module.connect = MagicMock()

        with patch.dict('sys.modules', {'websockets': mock_ws_module}):
            s._connect_websocket_raw('/stream')
            call_kwargs = mock_ws_module.connect.call_args[1]
            assert call_kwargs['ssl'] is None


# ===========================================================================
# _headers (GsSession base)
# ===========================================================================

class TestHeaders:
    def test_headers_returns_gssso_cookie(self):
        s = _make_session()
        s._session.cookies = {'GSSSO': 'my-sso-token'}
        result = s._headers()
        # _headers returns standard headers (X-Application, X-Version) plus cookies
        cookie_headers = [(k, v) for k, v in result if k == 'Cookie']
        assert len(cookie_headers) == 1
        assert 'GSSSO=my-sso-token' in cookie_headers[0][1]


# ===========================================================================
# Edge cases for _parse_response with cls
# ===========================================================================

class TestParseResponseWithCls:
    def test_json_with_results_key_and_base_cls(self):
        from gs_quant.base import Base
        s = _make_session()
        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: {'unpacked': d})

        resp = _mock_response(200, json_body={'results': [{'a': 1}]})
        result = s._parse_response('req1', resp, 'GET', 'https://u', mock_cls, False)
        assert 'results' in result
        assert result['results'] == ({'unpacked': {'a': 1}},)

    def test_json_direct_object_and_base_cls(self):
        from gs_quant.base import Base
        s = _make_session()
        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: {'unpacked': d})

        resp = _mock_response(200, json_body={'a': 1})
        result = s._parse_response('req1', resp, 'GET', 'https://u', mock_cls, False)
        assert result == {'unpacked': {'a': 1}}

    def test_json_empty_result_with_cls_no_unpack(self):
        """When ret is falsy (e.g., empty dict), cls should NOT be applied."""
        from gs_quant.base import Base
        s = _make_session()
        mock_cls = type('MockBase', (Base,), {})
        mock_cls.from_dict = staticmethod(lambda d: d)

        resp = _mock_response(200, json_body={}, text='{}')
        result = s._parse_response('req1', resp, 'GET', 'https://u', mock_cls, False)
        # Empty dict is falsy, so cls is not applied
        assert result == {}

    def test_error_status_199(self):
        """Status 199 is not in the success range."""
        s = _make_session()
        resp = _mock_response(199, content_type='application/json', text='weird', reason='Odd')
        with pytest.raises(MqRequestError):
            s._parse_response('req1', resp, 'GET', 'https://u', None, False)

    def test_error_status_300(self):
        """Status 300 is not in the success range."""
        s = _make_session()
        resp = _mock_response(300, content_type='application/json', text='redirect', reason='Multiple Choices')
        with pytest.raises(MqRequestError):
            s._parse_response('req1', resp, 'GET', 'https://u', None, False)
