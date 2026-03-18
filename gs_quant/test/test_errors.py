"""
Copyright 2019 Goldman Sachs.
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

from gs_quant.errors import (
    MqError,
    MqValueError,
    MqTypeError,
    MqWrappedError,
    MqRequestError,
    MqAuthenticationError,
    MqAuthorizationError,
    MqUninitialisedError,
    MqRateLimitedError,
    MqTimeoutError,
    MqInternalServerError,
    error_builder,
)


class TestMqRequestError:
    def test_str_without_context(self):
        err = MqRequestError(404, 'Not found')
        assert 'status: 404' in str(err)
        assert 'message: Not found' in str(err)
        assert 'context' not in str(err)

    def test_str_with_context(self):
        err = MqRequestError(500, 'Server error', context='request-123')
        result = str(err)
        assert 'context: request-123' in result
        assert 'status: 500' in result
        assert 'message: Server error' in result


class TestErrorBuilder:
    def test_401_returns_authentication_error(self):
        err = error_builder(401, 'Unauthorized')
        assert isinstance(err, MqAuthenticationError)
        assert err.status == 401

    def test_403_returns_authorization_error(self):
        err = error_builder(403, 'Forbidden')
        assert isinstance(err, MqAuthorizationError)
        assert err.status == 403

    def test_429_returns_rate_limited_error(self):
        err = error_builder(429, 'Too many requests')
        assert isinstance(err, MqRateLimitedError)
        assert err.status == 429

    def test_500_returns_internal_server_error(self):
        err = error_builder(500, 'Internal error')
        assert isinstance(err, MqInternalServerError)
        assert err.status == 500

    def test_504_returns_timeout_error(self):
        err = error_builder(504, 'Gateway timeout')
        assert isinstance(err, MqTimeoutError)
        assert err.status == 504

    def test_default_returns_request_error(self):
        err = error_builder(418, "I'm a teapot")
        assert type(err) is MqRequestError
        assert err.status == 418

    def test_context_passed_through(self):
        err = error_builder(401, 'Unauthorized', context='ctx')
        assert err.context == 'ctx'


class TestErrorHierarchy:
    def test_mq_value_error_is_value_error(self):
        assert issubclass(MqValueError, ValueError)
        assert issubclass(MqValueError, MqError)

    def test_mq_type_error(self):
        assert issubclass(MqTypeError, MqError)

    def test_mq_wrapped_error(self):
        assert issubclass(MqWrappedError, MqError)

    def test_mq_uninitialised_error(self):
        assert issubclass(MqUninitialisedError, MqError)
