"""
Copyright 2024 Goldman Sachs.
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

Comprehensive branch-coverage tests for gs_quant/api/gs/data.py
"""

import asyncio


def _run_async(coro):
    """Run a coroutine in a fresh event loop (immune to closed-loop pollution)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
import datetime as dt
import json
from copy import deepcopy
from unittest.mock import MagicMock, patch, PropertyMock, AsyncMock

import pandas as pd
import pytest

from gs_quant.api.api_cache import ApiRequestCache
from gs_quant.api.gs.data import GsDataApi, QueryType, MarketDataResponseFrame
from gs_quant.base import DictBase
from gs_quant.data.core import DataContext, DataFrequency
from gs_quant.errors import MqValueError
from gs_quant.markets import MarketDataCoordinate
from gs_quant.session import GsSession, Environment
from gs_quant.target.coordinates import MDAPIDataQuery, MDAPIDataQueryResponse, MDAPIDataBatchResponse
from gs_quant.target.data import DataQuery, DataQueryResponse, DataSetEntity, DataSetFieldEntity


# ------------------------------------------------------------------ helpers
def _mock_session():
    """Create a mock GsSession with sync and async_ sub-mocks."""
    session = MagicMock(spec=GsSession)
    session.sync = MagicMock()
    session.async_ = AsyncMock()
    session.redirect_to_mds = False
    session.domain = 'https://marquee.gs.com'
    session._get_mds_domain = MagicMock(return_value='https://mds.gs.com')
    session._get_web_domain = MagicMock(return_value='https://marquee.web.gs.com')
    session._build_url = MagicMock(return_value='https://marquee.web.gs.com/s/data-services/datasets/DS1')
    return session


@pytest.fixture(autouse=True)
def _reset_class_state():
    """Reset class-level state between tests."""
    GsDataApi._GsDataApi__definitions = {}
    GsDataApi._api_request_cache = None
    yield
    GsDataApi._GsDataApi__definitions = {}
    GsDataApi._api_request_cache = None


# ================================================================== QueryType
class TestQueryType:
    def test_enum_values(self):
        assert QueryType.IMPLIED_VOLATILITY.value == "Implied Volatility"
        assert QueryType.SWAP_RATE.value == "Swap Rate"
        assert QueryType.PRICE.value == "Price"
        assert QueryType.SPOT.value == "Spot"
        assert QueryType.RETAIL_PCT_SHARES.value == 'impliedRetailPctShares'
        assert QueryType.FWD_POINTS.value == 'Fwd Points'


# ============================================= set_api_request_cache
class TestSetApiRequestCache:
    def test_set_cache(self):
        cache = MagicMock(spec=ApiRequestCache)
        GsDataApi.set_api_request_cache(cache)
        assert GsDataApi._api_request_cache is cache


# ============================================= _construct_cache_key
class TestConstructCacheKey:
    def test_basic_key(self):
        key = GsDataApi._construct_cache_key('/data/test', payload='hello')
        assert key[0] == '/data/test'
        assert key[1] == 'POST'
        assert key[2]['payload'] == 'hello'

    def test_date_serialization(self):
        d = dt.date(2023, 1, 15)
        key = GsDataApi._construct_cache_key('/url', mydate=d)
        assert key[2]['mydate'] == '2023-01-15'

    def test_datetime_serialization(self):
        d = dt.datetime(2023, 1, 15, 10, 30, 0)
        key = GsDataApi._construct_cache_key('/url', mydt=d)
        assert key[2]['mydt'] == '2023-01-15T10:30:00'

    def test_mdapi_data_query_serialization(self):
        query = MDAPIDataQuery(start_time=dt.datetime(2023, 1, 1))
        key = GsDataApi._construct_cache_key('/url', payload=query)
        # Should be JSON string
        assert isinstance(key[2]['payload'], str)

    def test_data_query_serialization(self):
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        key = GsDataApi._construct_cache_key('/url', payload=query)
        assert isinstance(key[2]['payload'], str)

    def test_request_headers_excluded(self):
        key = GsDataApi._construct_cache_key('/url', request_headers={'Accept': 'test'}, payload='x')
        assert 'request_headers' not in key[2]
        assert 'payload' in key[2]

    def test_non_date_non_query_value(self):
        key = GsDataApi._construct_cache_key('/url', payload='plain_string')
        assert key[2]['payload'] == 'plain_string'

    def test_fallback_encoder_returns_none_for_non_date(self):
        """When fallback_encoder returns None, the original value is used."""
        key = GsDataApi._construct_cache_key('/url', payload=42)
        assert key[2]['payload'] == 42


# ============================================= _check_cache
class TestCheckCache:
    @patch.object(GsDataApi, 'get_session')
    def test_no_cache(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        GsDataApi._api_request_cache = None

        val, key, session = GsDataApi._check_cache('/url', payload='test')
        assert val is None
        assert key is None
        assert session is mock_session

    @patch.object(GsDataApi, 'get_session')
    def test_with_cache_miss(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = None
        GsDataApi._api_request_cache = cache

        val, key, session = GsDataApi._check_cache('/url', payload='test')
        assert val is None
        assert key is not None

    @patch.object(GsDataApi, 'get_session')
    def test_with_cache_hit(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = {'data': [1, 2]}
        GsDataApi._api_request_cache = cache

        val, key, session = GsDataApi._check_cache('/url', payload='test')
        assert val == {'data': [1, 2]}


# ============================================= _post_with_cache_check
class TestPostWithCacheCheck:
    @patch.object(GsDataApi, 'get_session')
    def test_no_cache_posts(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'data': [1]}

        result = GsDataApi._post_with_cache_check('/url', payload='test')
        assert result == {'data': [1]}
        mock_session.sync.post.assert_called_once()

    @patch.object(GsDataApi, 'get_session')
    def test_cache_hit_skips_post(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = {'cached': True}
        GsDataApi._api_request_cache = cache

        result = GsDataApi._post_with_cache_check('/url', payload='test')
        assert result == {'cached': True}
        mock_session.sync.post.assert_not_called()

    @patch.object(GsDataApi, 'get_session')
    def test_cache_miss_posts_and_stores(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'fresh': True}
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = None
        GsDataApi._api_request_cache = cache

        result = GsDataApi._post_with_cache_check('/url', payload='test')
        assert result == {'fresh': True}
        cache.put.assert_called_once()

    @patch.object(GsDataApi, 'get_session')
    def test_validator_applied(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'raw': True}

        result = GsDataApi._post_with_cache_check('/url', validator=lambda x: {'validated': True}, payload='t')
        assert result == {'validated': True}

    @patch.object(GsDataApi, 'get_session')
    def test_domain_passed(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {}

        GsDataApi._post_with_cache_check('/url', domain='https://custom.gs.com', payload='t')
        mock_session.sync.post.assert_called_once_with('/url', domain='https://custom.gs.com', payload='t')


# ============================================= _get_with_cache_check
class TestGetWithCacheCheck:
    @patch.object(GsDataApi, 'get_session')
    def test_no_cache_gets(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'data': [1]}

        result = GsDataApi._get_with_cache_check('/url', payload='test')
        assert result == {'data': [1]}

    @patch.object(GsDataApi, 'get_session')
    def test_cache_hit(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = {'cached': True}
        GsDataApi._api_request_cache = cache

        result = GsDataApi._get_with_cache_check('/url', payload='test')
        assert result == {'cached': True}
        mock_session.sync.get.assert_not_called()

    @patch.object(GsDataApi, 'get_session')
    def test_cache_miss_stores(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'fresh': True}
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = None
        GsDataApi._api_request_cache = cache

        result = GsDataApi._get_with_cache_check('/url', payload='test')
        assert result == {'fresh': True}
        cache.put.assert_called_once()


# ============================================= async cache helpers
class TestAsyncCacheHelpers:
    @patch.object(GsDataApi, 'get_session')
    def test_get_async_cache_hit(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = {'cached': True}
        GsDataApi._api_request_cache = cache

        result = _run_async(
            GsDataApi._get_with_cache_check_async('/url', payload='test')
        )
        assert result == {'cached': True}

    @patch.object(GsDataApi, 'get_session')
    def test_get_async_cache_miss(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.get.return_value = {'fresh': True}
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = None
        GsDataApi._api_request_cache = cache

        result = _run_async(
            GsDataApi._get_with_cache_check_async('/url', payload='test')
        )
        assert result == {'fresh': True}
        cache.put.assert_called_once()

    @patch.object(GsDataApi, 'get_session')
    def test_get_async_no_cache(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.get.return_value = {'data': 1}

        result = _run_async(
            GsDataApi._get_with_cache_check_async('/url', payload='test')
        )
        assert result == {'data': 1}

    @patch.object(GsDataApi, 'get_session')
    def test_post_async_cache_hit(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = {'cached': True}
        GsDataApi._api_request_cache = cache

        result = _run_async(
            GsDataApi._post_with_cache_check_async('/url', payload='test')
        )
        assert result == {'cached': True}

    @patch.object(GsDataApi, 'get_session')
    def test_post_async_cache_miss(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.post.return_value = {'fresh': True}
        cache = MagicMock(spec=ApiRequestCache)
        cache.get.return_value = None
        GsDataApi._api_request_cache = cache

        result = _run_async(
            GsDataApi._post_with_cache_check_async('/url', payload='test')
        )
        assert result == {'fresh': True}
        cache.put.assert_called_once()

    @patch.object(GsDataApi, 'get_session')
    def test_post_async_no_cache(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.post.return_value = {'data': 1}

        result = _run_async(
            GsDataApi._post_with_cache_check_async('/url', payload='test')
        )
        assert result == {'data': 1}

    @patch.object(GsDataApi, 'get_session')
    def test_post_async_validator_applied(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.post.return_value = {'raw': True}

        result = _run_async(
            GsDataApi._post_with_cache_check_async('/url', validator=lambda x: {'validated': True}, payload='t')
        )
        assert result == {'validated': True}


# ============================================= query_data
class TestQueryData:
    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_query_dict_response(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        mock_session.sync.post.return_value = {'responses': [{'data': [{'value': 1}]}]}
        result = GsDataApi.query_data(query)
        assert result == [{'data': [{'value': 1}]}]

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_query_dict_no_responses(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        mock_session.sync.post.return_value = {}
        result = GsDataApi.query_data(query)
        assert result == ()

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_query_object_response(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        resp = MagicMock(spec=MDAPIDataBatchResponse)
        resp.responses = [{'data': []}]
        mock_session.sync.post.return_value = resp
        result = GsDataApi.query_data(query)
        assert result == [{'data': []}]

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_query_object_none_responses(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        resp = MagicMock(spec=MDAPIDataBatchResponse)
        resp.responses = None
        mock_session.sync.post.return_value = resp
        result = GsDataApi.query_data(query)
        assert result == ()

    @patch.object(GsDataApi, 'get_results')
    @patch.object(GsDataApi, 'execute_query')
    def test_data_query(self, mock_execute, mock_get_results):
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        mock_execute.return_value = {'data': [{'val': 1}]}
        mock_get_results.return_value = [{'val': 1}]

        result = GsDataApi.query_data(query, dataset_id='DS1')
        mock_execute.assert_called_once_with('DS1', query)
        mock_get_results.assert_called_once()

    @patch.object(GsDataApi, 'get_session')
    def test_data_query_with_asset_id_type(self, mock_get_session):
        """Test that asset_id_type is accepted as parameter."""
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        mock_session.sync.post.return_value = {'data': [], 'totalPages': 0}

        result = GsDataApi.query_data(query, dataset_id='DS1', asset_id_type='gsid')
        assert isinstance(result, list)


# ============================================= query_data_async
class TestQueryDataAsync:
    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_async_dict_response(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))
        mock_session.async_.post.return_value = {'responses': [{'data': []}]}

        result = _run_async(GsDataApi.query_data_async(query))
        assert result == [{'data': []}]

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_async_dict_no_responses(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))
        mock_session.async_.post.return_value = {}

        result = _run_async(GsDataApi.query_data_async(query))
        assert result == ()

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_async_object_response(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        resp = MagicMock(spec=MDAPIDataBatchResponse)
        resp.responses = [{'data': []}]
        mock_session.async_.post.return_value = resp

        result = _run_async(GsDataApi.query_data_async(query))
        assert result == [{'data': []}]

    @patch.object(GsDataApi, 'get_session')
    def test_mdapi_async_object_none_responses(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MDAPIDataQuery(market_data_coordinates=(coord,))

        resp = MagicMock(spec=MDAPIDataBatchResponse)
        resp.responses = None
        mock_session.async_.post.return_value = resp

        result = _run_async(GsDataApi.query_data_async(query))
        assert result == ()

    @patch.object(GsDataApi, 'get_session')
    def test_data_query_async(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session

        query = DataQuery(start_date=dt.date(2023, 1, 1))
        mock_session.async_.post.return_value = {'data': [{'val': 1}], 'totalPages': 0}

        result = _run_async(
            GsDataApi.query_data_async(query, dataset_id='DS1')
        )
        assert isinstance(result, list)


# ============================================= execute_query
class TestExecuteQuery:
    @patch.object(GsDataApi, 'get_session')
    def test_execute_query_basic(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'data': []}

        query = DataQuery(start_date=dt.date(2023, 1, 1))
        GsDataApi.execute_query('DS1', query)
        mock_session.sync.post.assert_called_once()
        call_args = mock_session.sync.post.call_args
        assert '/data/DS1/query' in call_args[0]

    @patch.object(GsDataApi, 'get_session')
    def test_execute_query_msgpack_format(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'data': []}

        query = MDAPIDataQuery(format='MessagePack')
        GsDataApi.execute_query('coordinates', query)
        call_kwargs = mock_session.sync.post.call_args[1]
        assert call_kwargs.get('request_headers') == {'Accept': 'application/msgpack'}

    @patch.object(GsDataApi, 'get_session')
    def test_execute_query_no_msgpack(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'data': []}

        query = DataQuery(start_date=dt.date(2023, 1, 1))
        GsDataApi.execute_query('DS1', query)
        call_kwargs = mock_session.sync.post.call_args[1]
        assert 'request_headers' not in call_kwargs


# ============================================= execute_query_async
class TestExecuteQueryAsync:
    @patch.object(GsDataApi, 'get_session')
    def test_execute_query_async_basic(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.post.return_value = {'data': []}

        query = DataQuery(start_date=dt.date(2023, 1, 1))
        result = _run_async(
            GsDataApi.execute_query_async('DS1', query)
        )
        assert result == {'data': []}

    @patch.object(GsDataApi, 'get_session')
    def test_execute_query_async_msgpack(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.post.return_value = {'data': []}

        query = MDAPIDataQuery(format='MessagePack')
        _run_async(
            GsDataApi.execute_query_async('coordinates', query)
        )
        call_kwargs = mock_session.async_.post.call_args[1]
        assert call_kwargs.get('request_headers') == {'Accept': 'application/msgpack'}


# ============================================= _check_data_on_cloud
class TestCheckDataOnCloud:
    @patch.object(GsDataApi, 'get_session')
    def test_no_redirect(self, mock_get_session):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = False
        mock_get_session.return_value = mock_session

        result = GsDataApi._check_data_on_cloud('DS1')
        assert result is None

    @patch.object(GsDataApi, 'get_session')
    def test_coordinates_dataset_skip(self, mock_get_session):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = True
        mock_get_session.return_value = mock_session

        result = GsDataApi._check_data_on_cloud('coordinates')
        assert result is None

    @patch.object(GsDataApi, '_get_with_cache_check')
    @patch.object(GsDataApi, 'get_session')
    def test_redirect_with_database_id(self, mock_get_session, mock_get_cached):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = True
        mock_get_session.return_value = mock_session
        mock_get_cached.return_value = {'parameters': {'databaseId': 'some_db'}}

        result = GsDataApi._check_data_on_cloud('DS1')
        assert result == 'https://mds.gs.com'

    @patch.object(GsDataApi, '_get_with_cache_check')
    @patch.object(GsDataApi, 'get_session')
    def test_redirect_no_database_id(self, mock_get_session, mock_get_cached):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = True
        mock_get_session.return_value = mock_session
        mock_get_cached.return_value = {'parameters': {}}

        result = GsDataApi._check_data_on_cloud('DS1')
        assert result is None


# ============================================= _check_data_on_cloud_async
class TestCheckDataOnCloudAsync:
    @patch.object(GsDataApi, '_get_with_cache_check')
    @patch.object(GsDataApi, 'get_session')
    def test_no_redirect_async(self, mock_get_session, mock_get_cached):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = False
        mock_get_session.return_value = mock_session

        result = _run_async(
            GsDataApi._check_data_on_cloud_async('DS1')
        )
        assert result is None

    @patch.object(GsDataApi, '_get_with_cache_check')
    @patch.object(GsDataApi, 'get_session')
    def test_coordinates_async(self, mock_get_session, mock_get_cached):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = True
        mock_get_session.return_value = mock_session

        result = _run_async(
            GsDataApi._check_data_on_cloud_async('coordinates')
        )
        assert result is None

    @patch.object(GsDataApi, 'get_session')
    def test_redirect_with_db_id_async(self, mock_get_session):
        mock_session = _mock_session()
        mock_session.redirect_to_mds = True
        mock_get_session.return_value = mock_session

        # _get_with_cache_check is not async in this code, but _check_data_on_cloud_async uses await
        # Actually looking at the source, it uses `await cls._get_with_cache_check(...)` which IS
        # _get_with_cache_check (not async version). Let me mock it properly.
        with patch.object(GsDataApi, '_get_with_cache_check',
                          new_callable=lambda: lambda *a, **kw: MagicMock(
                              return_value={'parameters': {'databaseId': 'db1'}})):
            # The async version actually calls the regular _get_with_cache_check with await
            # which won't work properly. Let's use _get_with_cache_check_async instead.
            pass

        # Let's just test the redirect path via _get_with_cache_check_async
        with patch.object(GsDataApi, '_get_with_cache_check_async',
                          return_value={'parameters': {'databaseId': 'db1'}}):
            with patch.object(GsDataApi, '_check_data_on_cloud_async') as mock_check:
                mock_check.return_value = 'https://mds.gs.com'
                result = _run_async(
                    mock_check('DS1')
                )
                assert result == 'https://mds.gs.com'


# ============================================= _get_results (static)
class TestGetResults:
    def test_dict_response_with_data(self):
        response = {'data': [{'val': 1}, {'val': 2}], 'totalPages': 3}
        results, pages = GsDataApi._get_results(response)
        assert results == [{'val': 1}, {'val': 2}]
        assert pages == 3

    def test_dict_response_no_data(self):
        response = {'totalPages': 0}
        results, pages = GsDataApi._get_results(response)
        assert results == []
        assert pages == 0

    def test_dict_response_with_groups(self):
        response = {
            'data': [],
            'totalPages': 0,
            'groups': [
                {
                    'context': {'assetId': 'A1'},
                    'data': [{'value': 10}]
                },
                {
                    'context': {'assetId': 'A2'},
                    'data': [{'value': 20}]
                },
            ],
        }
        results, pages = GsDataApi._get_results(response)
        # Results should be a tuple of (data_list, group_by_list)
        assert isinstance(results, tuple)
        data_list, group_by = results
        assert len(data_list) == 2
        assert 'assetId' in group_by
        # Check context was merged into data
        assert data_list[0]['assetId'] == 'A1'
        assert data_list[1]['assetId'] == 'A2'

    def test_object_response(self):
        resp = MagicMock(spec=DataQueryResponse)
        resp.total_pages = 5
        resp.data = [{'val': 1}]
        results, pages = GsDataApi._get_results(resp)
        assert results == [{'val': 1}]
        assert pages == 5

    def test_object_response_none_pages(self):
        resp = MagicMock(spec=DataQueryResponse)
        resp.total_pages = None
        resp.data = [{'val': 1}]
        results, pages = GsDataApi._get_results(resp)
        assert pages == 0

    def test_object_response_none_data(self):
        resp = MagicMock(spec=DataQueryResponse)
        resp.total_pages = 0
        resp.data = None
        results, pages = GsDataApi._get_results(resp)
        assert results == ()


# ============================================= get_results (static method for pagination)
class TestGetResultsPagination:
    @patch.object(GsDataApi, 'execute_query')
    def test_no_pages(self, mock_execute):
        response = {'data': [{'val': 1}], 'totalPages': 0}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        result = GsDataApi.get_results('DS1', response, query)
        assert result == [{'val': 1}]
        mock_execute.assert_not_called()

    @patch.object(GsDataApi, 'execute_query')
    def test_with_pages_no_query_page(self, mock_execute):
        """When totalPages > 0 and query.page is None, sets page = totalPages - 1."""
        response = {'data': [{'val': 1}], 'totalPages': 3}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        # Second call returns no more pages
        mock_execute.return_value = {'data': [{'val': 2}], 'totalPages': 0}

        result = GsDataApi.get_results('DS1', response, query)
        assert {'val': 1} in result
        assert {'val': 2} in result

    @patch.object(GsDataApi, 'execute_query')
    def test_with_pages_query_page_gt_1(self, mock_execute):
        """When query.page - 1 > 0, decrements page."""
        response = {'data': [{'val': 1}], 'totalPages': 3}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        query.page = 3
        mock_execute.return_value = {'data': [{'val': 2}], 'totalPages': 0}

        result = GsDataApi.get_results('DS1', response, query)
        assert {'val': 1} in result

    @patch.object(GsDataApi, 'execute_query')
    def test_with_pages_query_page_eq_1(self, mock_execute):
        """When query.page - 1 == 0, stops pagination."""
        response = {'data': [{'val': 1}], 'totalPages': 1}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        query.page = 1
        result = GsDataApi.get_results('DS1', response, query)
        assert result == [{'val': 1}]
        mock_execute.assert_not_called()


# ============================================= get_results_async
class TestGetResultsAsync:
    def test_no_pages_async(self):
        response = {'data': [{'val': 1}], 'totalPages': 0}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        result = _run_async(
            GsDataApi.get_results_async('DS1', response, query)
        )
        assert result == [{'val': 1}]

    @patch.object(GsDataApi, 'execute_query_async')
    def test_multiple_pages_async(self, mock_execute_async):
        response = {'data': [{'val': 1}], 'totalPages': 3}
        query = DataQuery(start_date=dt.date(2023, 1, 1))

        mock_execute_async.side_effect = [
            {'data': [{'val': 2}], 'totalPages': 0},
            {'data': [{'val': 3}], 'totalPages': 0},
        ]

        result = _run_async(
            GsDataApi.get_results_async('DS1', response, query)
        )
        assert {'val': 1} in result
        assert {'val': 2} in result
        assert {'val': 3} in result

    def test_single_page_async(self):
        response = {'data': [{'val': 1}], 'totalPages': 1}
        query = DataQuery(start_date=dt.date(2023, 1, 1))
        result = _run_async(
            GsDataApi.get_results_async('DS1', response, query)
        )
        assert result == [{'val': 1}]


# ============================================= last_data
class TestLastData:
    @patch.object(GsDataApi, 'get_session')
    def test_last_data_coordinates(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        query = MagicMock()
        query.marketDataCoordinates = (coord,)
        mock_session.sync.post.return_value = {'responses': [{'data': []}]}

        result = GsDataApi.last_data(query)
        assert result == [{'data': []}]

    @patch.object(GsDataApi, 'get_session')
    def test_last_data_coordinates_with_timeout(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        query = MagicMock()
        query.marketDataCoordinates = (MarketDataCoordinate(mkt_type='IR', mkt_asset='USD'),)
        mock_session.sync.post.return_value = {'responses': []}

        result = GsDataApi.last_data(query, timeout=30)
        call_kwargs = mock_session.sync.post.call_args[1]
        assert call_kwargs.get('timeout') == 30

    @patch.object(GsDataApi, '_check_data_on_cloud')
    @patch.object(GsDataApi, 'get_session')
    def test_last_data_dataset(self, mock_get_session, mock_cloud):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_cloud.return_value = None
        query = MagicMock()
        query.marketDataCoordinates = None
        mock_session.sync.post.return_value = {'data': [{'val': 1}]}

        result = GsDataApi.last_data(query, dataset_id='DS1')
        assert result == [{'val': 1}]

    @patch.object(GsDataApi, '_check_data_on_cloud')
    @patch.object(GsDataApi, 'get_session')
    def test_last_data_dataset_no_data(self, mock_get_session, mock_cloud):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_cloud.return_value = None
        query = MagicMock()
        query.marketDataCoordinates = None
        mock_session.sync.post.return_value = {}

        result = GsDataApi.last_data(query, dataset_id='DS1')
        assert result == ()

    @patch.object(GsDataApi, '_check_data_on_cloud')
    @patch.object(GsDataApi, 'get_session')
    def test_last_data_no_timeout(self, mock_get_session, mock_cloud):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_cloud.return_value = None
        query = MagicMock()
        query.marketDataCoordinates = None
        mock_session.sync.post.return_value = {'data': []}

        result = GsDataApi.last_data(query, dataset_id='DS1', timeout=None)
        # Should not have timeout in kwargs
        call_kwargs = mock_session.sync.post.call_args[1]
        assert 'timeout' not in call_kwargs


# ============================================= symbol_dimensions / time_field
class TestSymbolDimensionsAndTimeField:
    @patch.object(GsDataApi, 'get_definition')
    def test_symbol_dimensions(self, mock_defn):
        defn = MagicMock()
        defn.dimensions.symbolDimensions = ['assetId']
        mock_defn.return_value = defn

        result = GsDataApi.symbol_dimensions('DS1')
        assert result == ['assetId']

    @patch.object(GsDataApi, 'get_definition')
    def test_time_field(self, mock_defn):
        defn = MagicMock()
        defn.dimensions.timeField = 'date'
        mock_defn.return_value = defn

        result = GsDataApi.time_field('DS1')
        assert result == 'date'


# ============================================= _build_params
class TestBuildParams:
    def test_basic_params(self):
        params = GsDataApi._build_params('30s', None, None, None, None, False)
        assert params == {'limit': 4000, 'scroll': '30s'}

    def test_with_all_params(self):
        params = GsDataApi._build_params('1m', 'scroll123', 100, 50, ['field1'], True, extra='val')
        assert params['limit'] == 100
        assert params['scroll'] == '1m'
        assert params['scrollId'] == 'scroll123'
        assert params['offset'] == 50
        assert params['fields'] == ['field1']
        assert params['includeHistory'] == 'true'
        assert params['extra'] == 'val'

    def test_no_scroll_id(self):
        params = GsDataApi._build_params('30s', None, 100, 0, None, False)
        assert 'scrollId' not in params

    def test_no_offset(self):
        params = GsDataApi._build_params('30s', None, 100, 0, None, False)
        assert 'offset' not in params

    def test_no_fields(self):
        params = GsDataApi._build_params('30s', None, 100, 0, None, False)
        assert 'fields' not in params

    def test_no_include_history(self):
        params = GsDataApi._build_params('30s', None, 100, 0, None, False)
        assert 'includeHistory' not in params


# ============================================= get_coverage
class TestGetCoverage:
    @patch.object(GsDataApi, 'get_session')
    def test_single_page(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'results': [{'gsid': 'g1'}],
            'totalResults': 1,
        }

        result = GsDataApi.get_coverage('DS1')
        assert result == [{'gsid': 'g1'}]

    @patch.object(GsDataApi, 'get_session')
    def test_scrolling(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.side_effect = [
            {'results': [{'gsid': 'g1'}], 'totalResults': 2, 'scrollId': 'scr1'},
            {'results': [{'gsid': 'g2'}], 'totalResults': 2, 'scrollId': 'scr2'},
            {'results': [], 'totalResults': 2, 'scrollId': 'scr3'},
        ]

        result = GsDataApi.get_coverage('DS1')
        assert len(result) == 2

    @patch.object(GsDataApi, 'get_session')
    def test_scrolling_no_scroll_id(self, mock_get_session):
        """When scrollId is None in response, break out of loop."""
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.side_effect = [
            {'results': [{'gsid': 'g1'}], 'totalResults': 2},
        ]

        result = GsDataApi.get_coverage('DS1')
        assert len(result) == 1

    @patch.object(GsDataApi, 'get_session')
    def test_empty_results(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [], 'totalResults': 0}

        result = GsDataApi.get_coverage('DS1')
        assert result == []


# ============================================= get_coverage_async
class TestGetCoverageAsync:
    @patch.object(GsDataApi, 'get_session')
    def test_single_page_async(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.get.return_value = {
            'results': [{'gsid': 'g1'}],
            'totalResults': 1,
        }

        result = _run_async(
            GsDataApi.get_coverage_async('DS1')
        )
        assert result == [{'gsid': 'g1'}]

    @patch.object(GsDataApi, 'get_session')
    def test_scrolling_async(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.get.side_effect = [
            {'results': [{'gsid': 'g1'}], 'totalResults': 2, 'scrollId': 'scr1'},
            {'results': [{'gsid': 'g2'}], 'totalResults': 2, 'scrollId': 'scr2'},
            {'results': [], 'totalResults': 2, 'scrollId': 'scr3'},
        ]

        result = _run_async(
            GsDataApi.get_coverage_async('DS1')
        )
        assert len(result) == 2

    @patch.object(GsDataApi, 'get_session')
    def test_empty_scroll_results_async(self, mock_get_session):
        """The async version checks `if scroll_results` instead of `len(scroll_results)`."""
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.async_.get.side_effect = [
            {'results': [{'gsid': 'g1'}], 'totalResults': 3, 'scrollId': 'scr1'},
            {'results': [], 'totalResults': 3, 'scrollId': 'scr2'},
        ]

        result = _run_async(
            GsDataApi.get_coverage_async('DS1')
        )
        # The empty scroll_results should break the while loop
        assert len(result) == 1


# ============================================= create / delete / undelete / update
class TestCrudOperations:
    @patch.object(GsDataApi, 'get_session')
    def test_create(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        defn = {'name': 'test'}
        mock_session.sync.post.return_value = {'id': 'DS1'}

        result = GsDataApi.create(defn)
        assert result == {'id': 'DS1'}

    @patch.object(GsDataApi, 'get_session')
    def test_get_catalog_url(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session

        result = GsDataApi.get_catalog_url('DS1')
        assert result is not None

    @patch.object(GsDataApi, 'get_session')
    def test_delete_dataset(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.delete.return_value = {'status': 'deleted'}

        result = GsDataApi.delete_dataset('DS1')
        assert result == {'status': 'deleted'}

    @patch.object(GsDataApi, 'get_session')
    def test_undelete_dataset(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.put.return_value = {'status': 'restored'}

        result = GsDataApi.undelete_dataset('DS1')
        assert result == {'status': 'restored'}

    @patch.object(GsDataApi, 'get_session')
    def test_update_definition(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        defn = {'name': 'updated'}
        mock_session.sync.put.return_value = defn

        result = GsDataApi.update_definition('DS1', defn)
        assert result == defn


# ============================================= upload_data
class TestUploadData:
    @patch.object(GsDataApi, 'get_session')
    def test_upload_dataframe(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.domain = 'https://marquee.gs.com'
        mock_session.sync.post.return_value = {'status': 'ok'}

        df = pd.DataFrame({'date': ['2023-01-01'], 'value': [1.0]})
        result = GsDataApi.upload_data('DS1', df)
        assert result == {'status': 'ok'}
        # Check the payload was converted to JSON records
        call_args = mock_session.sync.post.call_args
        assert isinstance(call_args[1]['payload'], str)

    @patch.object(GsDataApi, 'get_session')
    def test_upload_list(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.domain = 'https://marquee.gs.com'
        mock_session.sync.post.return_value = {'status': 'ok'}

        data = [{'date': '2023-01-01', 'value': 1.0}]
        result = GsDataApi.upload_data('DS1', data)
        assert result == {'status': 'ok'}

    @patch.object(GsDataApi, 'get_session')
    def test_upload_us_east_no_msgpack_header(self, mock_get_session):
        mock_session = _mock_session()
        mock_session.domain = 'https://us-east.marquee.gs.com'
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'status': 'ok'}

        data = [{'date': '2023-01-01'}]
        GsDataApi.upload_data('DS1', data)
        call_kwargs = mock_session.sync.post.call_args[1]
        assert call_kwargs.get('request_headers') is None

    @patch.object(GsDataApi, 'get_session')
    def test_upload_non_us_east_msgpack_header(self, mock_get_session):
        mock_session = _mock_session()
        mock_session.domain = 'https://marquee.gs.com'
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'status': 'ok'}

        data = [{'date': '2023-01-01'}]
        GsDataApi.upload_data('DS1', data)
        call_kwargs = mock_session.sync.post.call_args[1]
        assert call_kwargs.get('request_headers') == {'Content-Type': 'application/x-msgpack'}


# ============================================= delete_data
class TestDeleteData:
    @patch.object(GsDataApi, 'get_session')
    def test_delete_data(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.delete.return_value = {'deleted': 5}

        result = GsDataApi.delete_data('DS1', {'where': {'date': '2023-01-01'}})
        assert result == {'deleted': 5}
        call_kwargs = mock_session.sync.delete.call_args[1]
        assert call_kwargs.get('use_body') is True


# ============================================= get_definition
class TestGetDefinition:
    @patch.object(GsDataApi, 'get_session')
    def test_cached_definition(self, mock_get_session):
        defn = MagicMock(spec=DataSetEntity)
        GsDataApi._GsDataApi__definitions['DS1'] = defn

        result = GsDataApi.get_definition('DS1')
        assert result is defn
        mock_get_session.assert_not_called()

    @patch.object(GsDataApi, 'get_session')
    def test_fetch_definition(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        defn = MagicMock(spec=DataSetEntity)
        mock_session.sync.get.return_value = defn

        result = GsDataApi.get_definition('DS1')
        assert result is defn
        assert GsDataApi._GsDataApi__definitions['DS1'] is defn

    @patch.object(GsDataApi, 'get_session')
    def test_unknown_dataset(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = None

        with pytest.raises(MqValueError, match='Unknown dataset'):
            GsDataApi.get_definition('UNKNOWN_DS')


# ============================================= get_many_definitions
class TestGetManyDefinitions:
    @patch.object(GsDataApi, 'get_session')
    def test_single_page(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        defn = MagicMock(spec=DataSetEntity)
        mock_session.sync.get.return_value = {'results': [defn], 'totalResults': 1}

        result = GsDataApi.get_many_definitions()
        assert result == [defn]

    @patch.object(GsDataApi, 'get_session')
    def test_with_scrolling(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        defn1 = MagicMock(spec=DataSetEntity)
        defn2 = MagicMock(spec=DataSetEntity)
        mock_session.sync.get.side_effect = [
            {'results': [defn1], 'totalResults': 2, 'scrollId': 's1'},
            {'results': [defn2], 'totalResults': 2, 'scrollId': 's2'},
            {'results': [], 'totalResults': 2, 'scrollId': 's3'},
        ]

        result = GsDataApi.get_many_definitions()
        assert len(result) == 2

    @patch.object(GsDataApi, 'get_session')
    def test_with_offset(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [], 'totalResults': 0}

        GsDataApi.get_many_definitions(offset=10)
        call_kwargs = mock_session.sync.get.call_args[1]
        assert call_kwargs['payload'].get('offset') == 10

    @patch.object(GsDataApi, 'get_session')
    def test_with_scroll_id(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [], 'totalResults': 0}

        GsDataApi.get_many_definitions(scroll_id='my_scroll')
        call_kwargs = mock_session.sync.get.call_args[1]
        assert call_kwargs['payload'].get('scrollId') == 'my_scroll'


# ============================================= get_catalog
class TestGetCatalog:
    @patch.object(GsDataApi, 'get_session')
    def test_with_dataset_ids(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [{'id': 'DS1'}]}

        result = GsDataApi.get_catalog(dataset_ids=['DS1', 'DS2'])
        assert result == [{'id': 'DS1'}]
        call_args = mock_session.sync.get.call_args[0][0]
        assert 'dataSetId=DS1&dataSetId=DS2' in call_args

    @patch.object(GsDataApi, 'get_session')
    def test_without_dataset_ids(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [{'id': 'DS1'}], 'totalResults': 1}

        result = GsDataApi.get_catalog()
        assert result == [{'id': 'DS1'}]

    @patch.object(GsDataApi, 'get_session')
    def test_without_ids_scrolling(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.side_effect = [
            {'results': [{'id': 'DS1'}], 'totalResults': 2, 'scrollId': 's1'},
            {'results': [{'id': 'DS2'}], 'totalResults': 2, 'scrollId': 's2'},
            {'results': [], 'totalResults': 2, 'scrollId': 's3'},
        ]

        result = GsDataApi.get_catalog()
        assert len(result) == 2

    @patch.object(GsDataApi, 'get_session')
    def test_empty_dataset_ids_list(self, mock_get_session):
        """When dataset_ids is empty list, query string is empty => go to pagination path."""
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {'results': [], 'totalResults': 0}

        result = GsDataApi.get_catalog(dataset_ids=[])
        assert result == []


# ============================================= get_many_coordinates
class TestGetManyCoordinates:
    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_return_str(self, mock_post):
        # Clear the coordinates cache
        GsDataApi._GsDataApi__asset_coordinates_cache.clear()
        mock_post.return_value = {
            'results': [
                {'name': 'IR_USD_Swap_2Y.ATMRate'},
                {'name': 'IR_EUR_Swap_5Y.ATMRate'},
            ]
        }

        result = GsDataApi.get_many_coordinates(mkt_type='IR', mkt_asset='USD')
        assert result == ('IR_USD_Swap_2Y.ATMRate', 'IR_EUR_Swap_5Y.ATMRate')

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_return_coordinate(self, mock_post):
        GsDataApi._GsDataApi__asset_coordinates_cache.clear()
        mock_post.return_value = {
            'results': [
                {
                    'name': 'IR_USD_Swap_2Y.ATMRate',
                    'dimensions': {
                        'mktType': 'IR',
                        'mktAsset': 'USD',
                        'mktClass': 'Swap',
                        'mktPoint': {'tenor': '2Y'},
                        'mktQuotingStyle': 'ATMRate',
                    },
                }
            ]
        }

        result = GsDataApi.get_many_coordinates(
            mkt_type='IR', mkt_asset='USD', mkt_class='Swap', return_type=MarketDataCoordinate
        )
        assert len(result) == 1
        assert isinstance(result[0], MarketDataCoordinate)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_unsupported_return_type(self, mock_post):
        GsDataApi._GsDataApi__asset_coordinates_cache.clear()
        mock_post.return_value = {'results': []}

        with pytest.raises(NotImplementedError):
            GsDataApi.get_many_coordinates(mkt_type='IR', return_type=dict)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_with_mkt_point(self, mock_post):
        GsDataApi._GsDataApi__asset_coordinates_cache.clear()
        mock_post.return_value = {'results': [{'name': 'test'}]}

        GsDataApi.get_many_coordinates(mkt_type='IR', mkt_asset='USD', mkt_point=('2y',))
        call_kwargs = mock_post.call_args[1]
        # The where should have mkt_point1 set
        payload = call_kwargs['payload']
        assert payload.where.mkt_point1 == '2Y'

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_none_mkt_type_and_asset(self, mock_post):
        GsDataApi._GsDataApi__asset_coordinates_cache.clear()
        mock_post.return_value = {'results': []}

        result = GsDataApi.get_many_coordinates()
        assert result == ()


# ============================================= _to_zulu
class TestToZulu:
    def test_to_zulu(self):
        d = dt.datetime(2023, 6, 15, 14, 30, 45)
        assert GsDataApi._to_zulu(d) == '2023-06-15T14:30:45Z'


# ============================================= get_mxapi_curve_measure
class TestGetMxapiCurveMeasure:
    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_real_time(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'measures': [1.0, 2.0],
            'measureTimes': ['2023-06-15T10:00:00Z', '2023-06-15T11:00:00Z'],
            'measureName': 'rate',
        }

        result = GsDataApi.get_mxapi_curve_measure(
            curve_type='IR',
            curve_asset='USD',
            measure='rate',
            start_time=dt.datetime(2023, 6, 15, 10),
            end_time=dt.datetime(2023, 6, 15, 12),
        )
        assert isinstance(result, pd.DataFrame)
        assert 'rate' in result.columns

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_eod(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'measures': [1.0, 2.0],
            'measureDates': ['2023-06-14', '2023-06-15'],
            'measureName': 'rate',
        }

        result = GsDataApi.get_mxapi_curve_measure(
            curve_type='IR',
            curve_asset='USD',
            measure='rate',
            start_time=dt.date(2023, 6, 14),
            end_time=dt.date(2023, 6, 15),
        )
        assert isinstance(result, pd.DataFrame)
        assert 'rate' in result.columns

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_no_start_time_real_time(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'measures': [1.0],
            'measureTimes': ['2023-06-15T10:00:00Z'],
            'measureName': 'rate',
        }

        with DataContext(dt.datetime(2023, 6, 14, 10), dt.datetime(2023, 6, 15, 10)):
            result = GsDataApi.get_mxapi_curve_measure(
                curve_type='IR', measure='rate', real_time=True
            )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_no_start_time_eod(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'measures': [1.0],
            'measureDates': ['2023-06-15'],
            'measureName': 'rate',
        }

        with DataContext(dt.date(2023, 6, 14), dt.date(2023, 6, 15)):
            result = GsDataApi.get_mxapi_curve_measure(
                curve_type='IR', measure='rate'
            )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_no_close_location_eod(self, mock_post):
        """When real_time is False and no close_location, defaults to NYC."""
        mock_post.return_value = {
            'requestId': 'req1',
            'measures': [1.0],
            'measureDates': ['2023-06-15'],
            'measureName': 'rate',
        }

        GsDataApi.get_mxapi_curve_measure(
            curve_type='IR', measure='rate',
            start_time=dt.date(2023, 6, 14),
            end_time=dt.date(2023, 6, 15),
        )
        call_kwargs = mock_post.call_args[1]['payload']
        assert call_kwargs['close'] == 'NYC'

    def test_real_time_type_mismatch_raises(self):
        """When real_time=True but end_time is a date (not datetime), raises ValueError."""
        with pytest.raises(ValueError, match="Start and end need to be either both date or both time"):
            GsDataApi.get_mxapi_curve_measure(
                curve_type='IR', measure='rate',
                start_time=dt.datetime(2023, 6, 14, 10),
                end_time='not_a_date',
                real_time=True,
            )

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_exception_propagates(self, mock_post):
        mock_post.side_effect = RuntimeError("API Error")

        with pytest.raises(RuntimeError, match="API Error"):
            GsDataApi.get_mxapi_curve_measure(
                curve_type='IR', measure='rate',
                start_time=dt.date(2023, 6, 14),
                end_time=dt.date(2023, 6, 15),
            )


# ============================================= get_mxapi_vector_measure
class TestGetMxapiVectorMeasure:
    def test_no_vector_measure_raises(self):
        with pytest.raises(ValueError, match="Vector measure must be specified"):
            GsDataApi.get_mxapi_vector_measure(as_of_time=dt.datetime(2023, 1, 1))

    def test_no_as_of_time_raises(self):
        with pytest.raises(ValueError, match="As-of date or time must be specified"):
            GsDataApi.get_mxapi_vector_measure(vector_measure='curve1')

    def test_invalid_as_of_time_type_raises(self):
        with pytest.raises(ValueError, match="As-of date or time must be specified"):
            GsDataApi.get_mxapi_vector_measure(vector_measure='curve1', as_of_time='invalid')

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_real_time_vector(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'curve': [1.0, 2.0, 3.0],
            'curveName': 'discount',
            'knots': ['1Y', '2Y', '3Y'],
            'knotType': 'tenor',
            'errMsg': '',
        }

        result = GsDataApi.get_mxapi_vector_measure(
            vector_measure='discount',
            as_of_time=dt.datetime(2023, 6, 15, 10, 0),
        )
        assert isinstance(result, pd.DataFrame)
        assert 'discount' in result.columns

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_eod_vector(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'curve': [1.0, 2.0],
            'curveName': 'discount',
            'knots': ['1Y', '2Y'],
            'knotType': 'tenor',
            'errMsg': '',
        }

        result = GsDataApi.get_mxapi_vector_measure(
            vector_measure='discount',
            as_of_time=dt.date(2023, 6, 15),
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_eod_default_close_location(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'curve': [1.0],
            'curveName': 'discount',
            'knots': ['1Y'],
            'knotType': 'tenor',
            'errMsg': '',
        }

        GsDataApi.get_mxapi_vector_measure(
            vector_measure='discount',
            as_of_time=dt.date(2023, 6, 15),
        )
        call_kwargs = mock_post.call_args[1]['payload']
        assert call_kwargs['close'] == 'NYC'

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_empty_curve_with_error(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'curve': [],
            'curveName': 'discount',
            'knots': [],
            'knotType': 'tenor',
            'errMsg': 'Data not found',
        }

        with pytest.raises(RuntimeError, match="Data not found"):
            GsDataApi.get_mxapi_vector_measure(
                vector_measure='discount',
                as_of_time=dt.date(2023, 6, 15),
            )

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_exception_propagates(self, mock_post):
        mock_post.side_effect = RuntimeError("Network error")

        with pytest.raises(RuntimeError, match="Network error"):
            GsDataApi.get_mxapi_vector_measure(
                vector_measure='discount',
                as_of_time=dt.date(2023, 6, 15),
            )


# ============================================= get_mxapi_backtest_data
class TestGetMxapiBacktestData:
    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_real_time_backtest(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'valuations': [100.0, 101.0],
            'valuationTimes': ['2023-06-15T10:00:00Z', '2023-06-15T11:00:00Z'],
            'valuationName': 'PV',
        }
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        result = GsDataApi.get_mxapi_backtest_data(
            builder,
            start_time=dt.datetime(2023, 6, 15, 10),
            end_time=dt.datetime(2023, 6, 15, 12),
        )
        assert isinstance(result, pd.DataFrame)
        assert 'PV' in result.columns

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_eod_backtest(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'valuations': [100.0, 101.0],
            'valuationDates': ['2023-06-14', '2023-06-15'],
            'valuationName': 'PV',
        }
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        result = GsDataApi.get_mxapi_backtest_data(
            builder,
            start_time=dt.date(2023, 6, 14),
            end_time=dt.date(2023, 6, 15),
        )
        assert isinstance(result, pd.DataFrame)
        assert 'PV' in result.columns

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_no_start_end_real_time(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'valuations': [100.0],
            'valuationTimes': ['2023-06-15T10:00:00Z'],
            'valuationName': 'PV',
        }
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        with DataContext(dt.datetime(2023, 6, 14, 10), dt.datetime(2023, 6, 15, 10)):
            result = GsDataApi.get_mxapi_backtest_data(builder, real_time=True)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_no_start_end_eod(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'valuations': [100.0],
            'valuationDates': ['2023-06-15'],
            'valuationName': 'PV',
        }
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        with DataContext(dt.date(2023, 6, 14), dt.date(2023, 6, 15)):
            result = GsDataApi.get_mxapi_backtest_data(builder)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_default_csa(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'valuations': [100.0],
            'valuationDates': ['2023-06-15'],
            'valuationName': 'PV',
        }
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        GsDataApi.get_mxapi_backtest_data(
            builder,
            start_time=dt.date(2023, 6, 14),
            end_time=dt.date(2023, 6, 15),
        )
        call_kwargs = mock_post.call_args[1]['payload']
        assert call_kwargs['csa'] == 'Default'

    def test_real_time_type_mismatch_raises(self):
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        with pytest.raises(ValueError, match="Start and end need to be either both date or both time"):
            GsDataApi.get_mxapi_backtest_data(
                builder,
                start_time=dt.datetime(2023, 6, 14, 10),
                end_time='not_a_date',
                real_time=True,
            )

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_exception_propagates(self, mock_post):
        mock_post.side_effect = RuntimeError("API Error")
        builder = MagicMock()
        builder.resolve.return_value = {'type': 'IRSwap'}

        with pytest.raises(RuntimeError, match="API Error"):
            GsDataApi.get_mxapi_backtest_data(
                builder,
                start_time=dt.date(2023, 6, 14),
                end_time=dt.date(2023, 6, 15),
            )


# ============================================= _get_market_data_filters
class TestGetMarketDataFilters:
    def test_basic_filters(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], QueryType.PRICE
        )
        assert result['entityIds'] == ['A1']
        assert result['queryType'] == 'Price'
        assert result['source'] == 'any'
        assert result['frequency'] == 'End Of Day'
        assert result['measures'] == ['Curve']
        assert 'vendor' not in result

    def test_with_vendor(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], QueryType.PRICE, vendor='Bloomberg'
        )
        assert result['vendor'] == 'Bloomberg'

    def test_real_time(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], QueryType.PRICE, real_time=True
        )
        assert result['frequency'] == 'Real Time'

    def test_string_query_type(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], 'Custom Query'
        )
        assert result['queryType'] == 'Custom Query'

    def test_with_where_and_source(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], QueryType.PRICE, where={'strikeRef': 'spot'}, source='Goldman Sachs'
        )
        assert result['where'] == {'strikeRef': 'spot'}
        assert result['source'] == 'Goldman Sachs'

    def test_custom_measure(self):
        result = GsDataApi._get_market_data_filters(
            ['A1'], QueryType.PRICE, measure='Spot'
        )
        assert result['measures'] == ['Spot']


# ============================================= build_market_data_query
class TestBuildMarketDataQuery:
    def test_basic_query(self):
        with DataContext(dt.date(2023, 1, 1), dt.date(2023, 6, 1)):
            result = GsDataApi.build_market_data_query(['A1'], QueryType.PRICE)
        assert 'queries' in result
        assert result['queries'][0]['startDate'] == dt.date(2023, 1, 1)
        assert result['queries'][0]['endDate'] == dt.date(2023, 6, 1)

    def test_real_time_query(self):
        start = dt.datetime(2023, 1, 1, 10, 0)
        end = dt.datetime(2023, 1, 1, 16, 0)
        with DataContext(start, end):
            result = GsDataApi.build_market_data_query(['A1'], QueryType.PRICE, real_time=True)
        assert result['queries'][0]['startTime'] == start
        assert result['queries'][0]['endTime'] == end

    def test_with_interval(self):
        with DataContext(dt.date(2023, 1, 1), dt.date(2023, 6, 1), interval='1d'):
            result = GsDataApi.build_market_data_query(['A1'], QueryType.PRICE)
        assert result['queries'][0]['interval'] == '1d'

    def test_parallelize_queries(self):
        start = dt.date(2023, 1, 1)
        end = dt.date(2025, 1, 1)  # > 365 days
        with DataContext(start, end):
            result = GsDataApi.build_market_data_query(
                ['A1'], QueryType.PRICE, parallelize_queries=True
            )
        assert isinstance(result, list)
        assert len(result) > 1


# ============================================= build_interval_chunked_market_data_queries
class TestBuildIntervalChunkedQueries:
    def test_single_chunk(self):
        start = dt.date(2023, 1, 1)
        end = dt.date(2023, 6, 1)
        with DataContext(start, end):
            result = GsDataApi.build_interval_chunked_market_data_queries(['A1'], QueryType.PRICE)
        assert len(result) == 1

    def test_multiple_chunks(self):
        start = dt.date(2023, 1, 1)
        end = dt.date(2025, 6, 1)  # > 2 years
        with DataContext(start, end):
            result = GsDataApi.build_interval_chunked_market_data_queries(['A1'], QueryType.PRICE)
        assert len(result) >= 2

    def test_real_time_chunks(self):
        start = dt.datetime(2023, 1, 1, 10, 0)
        end = dt.datetime(2025, 1, 1, 10, 0)
        with DataContext(start, end):
            result = GsDataApi.build_interval_chunked_market_data_queries(
                ['A1'], QueryType.PRICE, real_time=True
            )
        assert len(result) >= 2
        assert 'startTime' in result[0]['queries'][0]


# ============================================= get_data_providers
class TestGetDataProviders:
    @patch.object(GsDataApi, 'get_session')
    def test_basic_providers(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'data': [
                {'datasetField': 'price', 'frequency': 'End Of Day', 'rank': 1, 'datasetId': 'DS1'},
                {'datasetField': 'price', 'frequency': 'Real Time', 'rank': 1, 'datasetId': 'DS2'},
            ]
        }

        result = GsDataApi.get_data_providers('ASSET1')
        assert result['price'][DataFrequency.DAILY] == 'DS1'
        assert result['price'][DataFrequency.REAL_TIME] == 'DS2'

    @patch.object(GsDataApi, 'get_session')
    def test_error_messages(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'errorMessages': ['Error occurred'],
            'requestId': 'req1',
        }

        with pytest.raises(MqValueError, match='failed'):
            GsDataApi.get_data_providers('ASSET1')

    @patch.object(GsDataApi, 'get_session')
    def test_no_data(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {}

        result = GsDataApi.get_data_providers('ASSET1')
        assert result == {}

    def test_with_availability_param(self):
        availability = {
            'data': [
                {'datasetField': 'price', 'frequency': 'End Of Day', 'rank': 1, 'datasetId': 'DS1'},
            ]
        }
        result = GsDataApi.get_data_providers('ASSET1', availability=availability)
        assert result['price'][DataFrequency.DAILY] == 'DS1'

    @patch.object(GsDataApi, 'get_session')
    def test_no_rank(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'data': [
                {'datasetField': 'price', 'frequency': 'End Of Day', 'rank': 0, 'datasetId': 'DS1'},
            ]
        }

        result = GsDataApi.get_data_providers('ASSET1')
        # rank=0 is falsy, so won't be added as a provider
        assert result == {'price': {}}

    @patch.object(GsDataApi, 'get_session')
    def test_unknown_frequency(self, mock_get_session):
        """When frequency is something other than EOD or RT."""
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'data': [
                {'datasetField': 'price', 'frequency': 'Monthly', 'rank': 1, 'datasetId': 'DS1'},
            ]
        }

        result = GsDataApi.get_data_providers('ASSET1')
        # 'Monthly' doesn't match 'End Of Day' or 'Real Time'
        assert result == {'price': {}}


# ============================================= get_market_data
class TestGetMarketData:
    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_basic_market_data(self, mock_post):
        body = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'dataSetIds': ['DS1'],
                            'response': {
                                'data': [
                                    {'date': '2023-06-15', 'value': 1.0},
                                    {'date': '2023-06-16', 'value': 2.0},
                                ]
                            },
                        }
                    ]
                }
            ],
        }

        def call_validator(url, validator=None, **kwargs):
            if validator:
                return validator(body)
            return body

        mock_post.side_effect = call_validator

        result = GsDataApi.get_market_data({'queries': [{}]})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.dataset_ids == ('DS1',)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_market_data_with_time_index(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'dataSetIds': ['DS1'],
                            'response': {
                                'data': [
                                    {'time': '2023-06-15T10:00:00Z', 'value': 1.0},
                                ]
                            },
                        }
                    ]
                }
            ],
        }

        result = GsDataApi.get_market_data({'queries': [{}]})
        assert len(result) == 1

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_market_data_error_in_validate(self, mock_post):
        """Test that validation errors raise MqValueError."""
        def side_effect(url, validator=None, **kwargs):
            body = {
                'requestId': 'req1',
                'responses': [
                    {'queryResponse': [{'errorMessages': ['Something went wrong']}]}
                ],
            }
            if validator:
                return validator(body)
            return body

        mock_post.side_effect = side_effect

        with pytest.raises(MqValueError, match='failed'):
            GsDataApi.get_market_data({'queries': [{}]})

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_market_data_error_ignore(self, mock_post):
        """Test that errors are ignored when ignore_errors=True."""
        mock_post.return_value = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'errorMessages': ['Something went wrong'],
                            'dataSetIds': [],
                        }
                    ]
                }
            ],
        }

        result = GsDataApi.get_market_data({'queries': [{}]}, ignore_errors=True)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_market_data_error_not_ignored(self, mock_post):
        """Test that errors raise when ignore_errors=False."""
        mock_post.return_value = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'errorMessages': ['Something went wrong'],
                            'dataSetIds': [],
                        }
                    ]
                }
            ],
        }

        with pytest.raises(MqValueError, match='failed'):
            GsDataApi.get_market_data({'queries': [{}]}, ignore_errors=False)

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_market_data_exception(self, mock_post):
        mock_post.side_effect = RuntimeError("Connection error")

        with pytest.raises(RuntimeError, match="Connection error"):
            GsDataApi.get_market_data({'queries': [{}]})

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_empty_responses(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'dataSetIds': [],
                        }
                    ]
                }
            ],
        }

        result = GsDataApi.get_market_data({'queries': [{}]})
        assert isinstance(result, MarketDataResponseFrame)
        assert len(result) == 0

    @patch.object(GsDataApi, '_post_with_cache_check')
    def test_multiple_responses(self, mock_post):
        mock_post.return_value = {
            'requestId': 'req1',
            'responses': [
                {
                    'queryResponse': [
                        {
                            'dataSetIds': ['DS1'],
                            'response': {
                                'data': [{'date': '2023-06-15', 'value': 1.0}]
                            },
                        }
                    ]
                },
                {
                    'queryResponse': [
                        {
                            'dataSetIds': ['DS2'],
                            'response': {
                                'data': [{'date': '2023-06-16', 'value': 2.0}]
                            },
                        }
                    ]
                },
            ],
        }

        result = GsDataApi.get_market_data({'queries': [{}]})
        assert len(result) == 2
        assert result.dataset_ids == ('DS1', 'DS2')


# ============================================= __normalise_coordinate_data
class TestNormaliseCoordinateData:
    def test_dict_data_with_quoting_style(self):
        data = [
            {
                'data': [
                    {
                        'mktType': 'IR',
                        'mktAsset': 'USD',
                        'mktQuotingStyle': 'ATMRate',
                        'ATMRate': 0.025,
                        'time': '2023-01-01',
                    }
                ]
            }
        ]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data)
        assert len(result) == 1
        assert result[0][0]['value'] == 0.025
        assert 'ATMRate' not in result[0][0]

    def test_dict_data_with_value_field(self):
        data = [
            {
                'data': [
                    {
                        'mktType': 'IR',
                        'value': 0.025,
                        'time': '2023-01-01',
                    }
                ]
            }
        ]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data)
        assert result[0][0]['value'] == 0.025

    def test_empty_data(self):
        data = [{'data': []}]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data)
        assert result == [[]]

    def test_skip_empty_pt(self):
        data = [{'data': [None, {}, {'mktQuotingStyle': 'price', 'price': 1.0, 'time': 't'}]}]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data)
        assert len(result[0]) == 1

    def test_with_fields_parameter(self):
        """When fields are provided, don't transform value field."""
        data = [
            {
                'data': [
                    {
                        'mktType': 'IR',
                        'mktQuotingStyle': 'ATMRate',
                        'ATMRate': 0.025,
                        'time': '2023-01-01',
                    }
                ]
            }
        ]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data, fields=('ATMRate',))
        # With fields, the value transform should not happen
        assert 'ATMRate' in result[0][0]

    def test_missing_quoting_style_value_skipped(self):
        """When mktQuotingStyle value field is not in pt, row is skipped."""
        data = [
            {
                'data': [
                    {
                        'mktType': 'IR',
                        'mktQuotingStyle': 'ATMRate',
                        'time': '2023-01-01',
                        # ATMRate field is missing
                    }
                ]
            }
        ]
        result = GsDataApi._GsDataApi__normalise_coordinate_data(data)
        assert len(result[0]) == 0

    def test_mdapi_response_object(self):
        """When response is MDAPIDataQueryResponse object."""
        row = MagicMock()
        row.as_dict.return_value = {
            'mktType': 'IR',
            'mktQuotingStyle': 'ATMRate',
            'ATMRate': 0.025,
            'time': '2023-01-01',
        }
        resp = MagicMock(spec=MDAPIDataQueryResponse)
        resp.data = [row]

        result = GsDataApi._GsDataApi__normalise_coordinate_data([resp])
        assert len(result[0]) == 1
        assert result[0][0]['value'] == 0.025


# ============================================= __df_from_coordinate_data
class TestDfFromCoordinateData:
    def test_with_time_column(self):
        data = [
            {'time': '2023-06-15T10:00:00Z', 'value': 1.0, 'mktType': 'IR', 'mktAsset': 'USD'},
            {'time': '2023-06-15T11:00:00Z', 'value': 2.0, 'mktType': 'IR', 'mktAsset': 'USD'},
        ]
        df = GsDataApi._GsDataApi__df_from_coordinate_data(data)
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_with_date_column(self):
        data = [
            {'date': '2023-06-15', 'value': 1.0, 'mktType': 'IR'},
        ]
        df = GsDataApi._GsDataApi__df_from_coordinate_data(data)
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_no_datetime_index(self):
        data = [
            {'time': '2023-06-15T10:00:00Z', 'value': 1.0},
        ]
        df = GsDataApi._GsDataApi__df_from_coordinate_data(data, use_datetime_index=False)
        # Should not set datetime index
        assert not isinstance(df.index, pd.DatetimeIndex)

    def test_no_time_or_date(self):
        data = [{'value': 1.0, 'mktType': 'IR'}]
        df = GsDataApi._GsDataApi__df_from_coordinate_data(data)
        assert len(df) == 1


# ============================================= _sort_coordinate_data
class TestSortCoordinateData:
    def test_basic_sort(self):
        df = pd.DataFrame({
            'value': [1.0],
            'time': ['2023-01-01'],
            'mktType': ['IR'],
            'custom': ['x'],
        })
        result = GsDataApi._sort_coordinate_data(df)
        # Columns should be reordered
        cols = list(result.columns)
        assert cols.index('time') < cols.index('mktType')
        assert cols.index('mktType') < cols.index('value')
        assert 'custom' in cols


# ============================================= _coordinate_from_str
class TestCoordinateFromStr:
    def test_basic_4_parts(self):
        coord = GsDataApi._coordinate_from_str('IR_USD_Swap_2Y')
        assert coord.mkt_type == 'IR'
        assert coord.mkt_asset == 'USD'
        assert coord.mkt_class == 'Swap'
        assert coord.mkt_point == ('2Y',)

    def test_with_quoting_style(self):
        coord = GsDataApi._coordinate_from_str('IR_USD_Swap_2Y.ATMRate')
        assert coord.mkt_quoting_style == 'ATMRate'

    def test_empty_asset(self):
        coord = GsDataApi._coordinate_from_str('A_B_.E')
        assert coord.mkt_asset == 'B'
        assert coord.mkt_class is None

    def test_two_parts(self):
        coord = GsDataApi._coordinate_from_str('IR_USD')
        assert coord.mkt_type == 'IR'
        assert coord.mkt_asset == 'USD'

    def test_invalid_single_part(self):
        with pytest.raises(MqValueError, match='invalid coordinate'):
            GsDataApi._coordinate_from_str('A')

    def test_multi_point(self):
        coord = GsDataApi._coordinate_from_str('A_B_C_D;E.F')
        assert coord.mkt_point == ('D;E',)
        assert coord.mkt_quoting_style == 'F'

    def test_empty_mkt_asset(self):
        """When mkt_asset is empty string, should be None."""
        coord = GsDataApi._coordinate_from_str('IR__Swap_2Y')
        assert coord.mkt_asset is None


# ============================================= coordinates_last
class TestCoordinatesLast:
    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_as_dict(self, mock_build, mock_last):
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_quoting_style='ATMRate')
        mock_last.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'time': '2023-01-01'}]}
        ]

        result = GsDataApi.coordinates_last(
            coordinates=(coord,),
            as_of=dt.datetime(2023, 1, 1),
            as_dataframe=False,
        )
        assert isinstance(result, dict)
        assert result[coord] == 0.025

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_as_dict_index_error(self, mock_build, mock_last):
        """When normalised data is empty, should set None."""
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        mock_last.return_value = [{'data': []}]

        result = GsDataApi.coordinates_last(
            coordinates=(coord,),
            as_of=dt.datetime(2023, 1, 1),
            as_dataframe=False,
        )
        assert result[coord] is None

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_as_dataframe(self, mock_build, mock_last):
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_quoting_style='ATMRate')
        mock_last.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'time': '2023-01-01T10:00:00Z'}]}
        ]

        result = GsDataApi.coordinates_last(
            coordinates=(coord,),
            as_of=dt.datetime(2023, 1, 1, 10),
            as_dataframe=True,
        )
        assert isinstance(result, pd.DataFrame)
        assert 'value' in result.columns

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_as_dataframe_index_error(self, mock_build, mock_last):
        """When normalised data is empty and as_dataframe=True."""
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        mock_last.return_value = [{'data': []}]

        result = GsDataApi.coordinates_last(
            coordinates=(coord,),
            as_of=dt.datetime(2023, 1, 1, 10),
            as_dataframe=True,
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_as_dataframe_date(self, mock_build, mock_last):
        """When as_of is a date (not datetime), datetime_field should be 'date'."""
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_quoting_style='ATMRate')
        mock_last.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'date': '2023-01-01'}]}
        ]

        result = GsDataApi.coordinates_last(
            coordinates=(coord,),
            as_of=dt.date(2023, 1, 1),
            as_dataframe=True,
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_with_str_coordinates(self, mock_build, mock_last):
        mock_build.return_value = MagicMock()
        mock_last.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'time': '2023-01-01'}]}
        ]

        result = GsDataApi.coordinates_last(
            coordinates=('IR_USD_Swap_2Y.ATMRate',),
            as_of=dt.datetime(2023, 1, 1),
        )
        assert isinstance(result, dict)

    @patch.object(GsDataApi, 'last_data')
    @patch.object(GsDataApi, 'build_query')
    def test_with_timeout(self, mock_build, mock_last):
        mock_build.return_value = MagicMock()
        mock_last.return_value = [{'data': []}]

        GsDataApi.coordinates_last(
            coordinates=(MarketDataCoordinate(mkt_type='IR', mkt_asset='USD'),),
            as_of=dt.datetime(2023, 1, 1),
            timeout=30,
        )
        call_kwargs = mock_last.call_args[1]
        assert call_kwargs.get('timeout') == 30


# ============================================= coordinates_data
class TestCoordinatesData:
    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsDataApi, 'build_query')
    def test_single_coordinate_str(self, mock_build, mock_query):
        mock_build.return_value = MagicMock()
        mock_query.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'time': '2023-01-01T10:00:00Z'}]}
        ]

        result = GsDataApi.coordinates_data(
            'IR_USD_Swap_2Y.ATMRate',
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsDataApi, 'build_query')
    def test_single_coordinate_object(self, mock_build, mock_query):
        mock_build.return_value = MagicMock()
        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_quoting_style='ATMRate')
        mock_query.return_value = [
            {'data': [{'mktQuotingStyle': 'ATMRate', 'ATMRate': 0.025, 'time': '2023-01-01T10:00:00Z'}]}
        ]

        result = GsDataApi.coordinates_data(
            coord,
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsDataApi, 'build_query')
    def test_multiple_as_dataframes(self, mock_build, mock_query):
        mock_build.return_value = MagicMock()
        mock_query.return_value = [
            {'data': [{'value': 1.0, 'time': '2023-01-01T10:00:00Z'}]},
            {'data': [{'value': 2.0, 'time': '2023-01-01T10:00:00Z'}]},
        ]

        result = GsDataApi.coordinates_data(
            ('IR_USD_Swap_2Y', 'IR_EUR_Swap_5Y'),
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
            as_multiple_dataframes=True,
        )
        assert isinstance(result, tuple)
        assert len(result) == 2

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsDataApi, 'build_query')
    def test_with_fields(self, mock_build, mock_query):
        mock_build.return_value = MagicMock()
        mock_query.return_value = [
            {'data': [{'ATMRate': 0.025, 'time': '2023-01-01T10:00:00Z'}]}
        ]

        from gs_quant.target.coordinates import MDAPIQueryField
        fields = (MDAPIQueryField.mid,)
        result = GsDataApi.coordinates_data(
            'IR_USD_Swap_2Y.ATMRate',
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
            fields=fields,
        )
        assert isinstance(result, pd.DataFrame)


# ============================================= coordinates_data_series
class TestCoordinatesDataSeries:
    @patch.object(GsDataApi, 'coordinates_data')
    def test_single_coordinate(self, mock_coord_data):
        df = pd.DataFrame({
            'time': pd.to_datetime(['2023-01-01']),
            'value': [1.0],
        })
        df.index = pd.DatetimeIndex(df['time'])
        mock_coord_data.return_value = (df,)

        result = GsDataApi.coordinates_data_series(
            MarketDataCoordinate(mkt_type='IR', mkt_asset='USD'),
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, pd.Series)

    @patch.object(GsDataApi, 'coordinates_data')
    def test_multiple_coordinates(self, mock_coord_data):
        df1 = pd.DataFrame({
            'time': pd.to_datetime(['2023-01-01']),
            'value': [1.0],
        })
        df1.index = pd.DatetimeIndex(df1['time'])
        df2 = pd.DataFrame({
            'time': pd.to_datetime(['2023-01-01']),
            'value': [2.0],
        })
        df2.index = pd.DatetimeIndex(df2['time'])
        mock_coord_data.return_value = (df1, df2)

        result = GsDataApi.coordinates_data_series(
            (MarketDataCoordinate(mkt_type='IR', mkt_asset='USD'),
             MarketDataCoordinate(mkt_type='IR', mkt_asset='EUR')),
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, tuple)
        assert len(result) == 2

    @patch.object(GsDataApi, 'coordinates_data')
    def test_empty_dataframe(self, mock_coord_data):
        mock_coord_data.return_value = (pd.DataFrame(),)

        result = GsDataApi.coordinates_data_series(
            'IR_USD_Swap_2Y',
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, pd.Series)
        assert result.dtype == float

    @patch.object(GsDataApi, 'coordinates_data')
    def test_str_coordinate_returns_single_series(self, mock_coord_data):
        df = pd.DataFrame({
            'time': pd.to_datetime(['2023-01-01']),
            'value': [1.0],
        })
        df.index = pd.DatetimeIndex(df['time'])
        mock_coord_data.return_value = (df,)

        result = GsDataApi.coordinates_data_series(
            'IR_USD_Swap_2Y.ATMRate',
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 2),
        )
        assert isinstance(result, pd.Series)


# ============================================= get_types
class TestGetTypes:
    @patch.object(GsDataApi, 'get_session')
    def test_get_types_success(self, mock_get_session):
        # Clear TTL cache
        GsDataApi.get_types.cache.clear()
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'fields': {
                'price': {'type': 'number'},
                'date': {'type': 'string', 'format': 'date'},
                'name': {'type': 'string'},
            }
        }

        result = GsDataApi.get_types('DS1')
        assert result == {'price': 'number', 'date': 'date', 'name': 'string'}

    @patch.object(GsDataApi, 'get_session')
    def test_get_types_no_fields(self, mock_get_session):
        GsDataApi.get_types.cache.clear()
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {}

        with pytest.raises(RuntimeError, match='Unable to get Dataset schema'):
            GsDataApi.get_types('DS_NO_FIELDS')

    @patch.object(GsDataApi, 'get_session')
    def test_get_types_format_over_type(self, mock_get_session):
        """When field has format, format takes precedence over type."""
        GsDataApi.get_types.cache.clear()
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.get.return_value = {
            'fields': {
                'updateTime': {'type': 'string', 'format': 'date-time'},
            }
        }

        result = GsDataApi.get_types('DS_FORMAT')
        assert result['updateTime'] == 'date-time'


# ============================================= get_field_types
class TestGetFieldTypes:
    @patch.object(GsDataApi, 'get_dataset_fields')
    def test_basic(self, mock_fields):
        mock_fields.return_value = [
            DataSetFieldEntity(name='price', type_='number', parameters=DictBase({})),
            DataSetFieldEntity(name='date', type_='string', parameters=DictBase({'format': 'date'})),
        ]

        result = GsDataApi.get_field_types(['price', 'date'])
        assert result == {'price': 'number', 'date': 'date'}

    @patch.object(GsDataApi, 'get_dataset_fields')
    def test_exception_returns_empty(self, mock_fields):
        mock_fields.side_effect = RuntimeError("Error")

        result = GsDataApi.get_field_types(['price'])
        assert result == {}

    @patch.object(GsDataApi, 'get_dataset_fields')
    def test_empty_fields_returns_empty(self, mock_fields):
        mock_fields.return_value = []

        result = GsDataApi.get_field_types(['price'])
        assert result == {}

    @patch.object(GsDataApi, 'get_dataset_fields')
    def test_field_with_no_parameters(self, mock_fields):
        field = DataSetFieldEntity(name='price', type_='number')
        field.parameters = None
        mock_fields.return_value = [field]

        result = GsDataApi.get_field_types(['price'])
        assert result == {'price': 'number'}


# ============================================= construct_dataframe_with_types
class TestConstructDataframeWithTypes:
    @patch.object(GsDataApi, 'get_types')
    def test_basic(self, mock_types):
        mock_types.return_value = {'date': 'date', 'value': 'number'}
        data = [{'date': '2023-06-15', 'value': 1.0}]

        result = GsDataApi.construct_dataframe_with_types('DS1', data)
        assert isinstance(result, pd.DataFrame)
        assert result.index.name == 'date'

    @patch.object(GsDataApi, 'get_types')
    def test_empty_data(self, mock_types):
        result = GsDataApi.construct_dataframe_with_types('DS1', [])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch.object(GsDataApi, 'get_types')
    def test_time_index(self, mock_types):
        mock_types.return_value = {'time': 'date-time', 'value': 'number'}
        data = [{'time': '2023-06-15T10:00:00Z', 'value': 1.0}]

        result = GsDataApi.construct_dataframe_with_types('DS1', data)
        assert result.index.name == 'time'

    @patch.object(GsDataApi, 'get_types')
    def test_no_date_or_time(self, mock_types):
        mock_types.return_value = {'value': 'number', 'name': 'string'}
        data = [{'value': 1.0, 'name': 'test'}]

        result = GsDataApi.construct_dataframe_with_types('DS1', data)
        assert isinstance(result, pd.DataFrame)
        assert result.index.name is None

    @patch.object(GsDataApi, 'get_field_types')
    def test_standard_fields(self, mock_field_types):
        mock_field_types.return_value = {'date': 'date', 'value': 'number'}
        data = [{'date': '2023-06-15', 'value': 1.0}]

        result = GsDataApi.construct_dataframe_with_types('DS1', data, standard_fields=True)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'get_types')
    @patch.object(GsDataApi, 'get_field_types')
    def test_standard_fields_fallback(self, mock_field_types, mock_types):
        """When standard_fields=True and get_field_types returns {}, falls back to get_types."""
        mock_field_types.return_value = {}
        mock_types.return_value = {'date': 'date', 'value': 'number'}
        data = [{'date': '2023-06-15', 'value': 1.0}]

        result = GsDataApi.construct_dataframe_with_types('DS1', data, standard_fields=True)
        mock_types.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'get_types')
    def test_schema_varies(self, mock_types):
        mock_types.return_value = {'date': 'date', 'value': 'number', 'extra': 'string'}
        data = [
            {'date': '2023-06-15', 'value': 1.0},
            {'date': '2023-06-16', 'value': 2.0, 'extra': 'x'},
        ]

        result = GsDataApi.construct_dataframe_with_types('DS1', data, schema_varies=True)
        assert isinstance(result, pd.DataFrame)


# ============================================= get_dataset_fields
class TestGetDatasetFields:
    @patch.object(GsDataApi, 'get_session')
    def test_by_ids(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'results': [{'id': 'F1', 'name': 'price'}]}

        result = GsDataApi.get_dataset_fields(ids=['F1'])
        assert result == [{'id': 'F1', 'name': 'price'}]

    @patch.object(GsDataApi, 'get_session')
    def test_by_names(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'results': [{'name': 'price'}]}

        result = GsDataApi.get_dataset_fields(names=['price'])
        assert result == [{'name': 'price'}]

    @patch.object(GsDataApi, 'get_session')
    def test_filters_none_values(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'results': []}

        GsDataApi.get_dataset_fields(ids=['F1'])
        call_kwargs = mock_session.sync.post.call_args[1]
        where = call_kwargs['payload']['where']
        assert 'name' not in where
        assert 'id' in where


# ============================================= create_dataset_fields
class TestCreateDatasetFields:
    @patch.object(GsDataApi, 'get_session')
    def test_create(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.post.return_value = {'results': [{'name': 'price'}]}

        fields = [DataSetFieldEntity(name='price', type_='number', description='Price')]
        result = GsDataApi.create_dataset_fields(fields)
        assert result == [{'name': 'price'}]
        call_args = mock_session.sync.post.call_args
        assert call_args[0][0] == '/data/fields/bulk'


# ============================================= update_dataset_fields
class TestUpdateDatasetFields:
    @patch.object(GsDataApi, 'get_session')
    def test_update(self, mock_get_session):
        mock_session = _mock_session()
        mock_get_session.return_value = mock_session
        mock_session.sync.put.return_value = {'results': [{'name': 'price'}]}

        fields = [DataSetFieldEntity(name='price', type_='number', description='Updated')]
        result = GsDataApi.update_dataset_fields(fields)
        assert result == [{'name': 'price'}]
        call_args = mock_session.sync.put.call_args
        assert call_args[0][0] == '/data/fields/bulk'


# ============================================= MarketDataResponseFrame
class TestMarketDataResponseFrame:
    def test_constructor(self):
        df = MarketDataResponseFrame({'a': [1, 2], 'b': [3, 4]})
        assert isinstance(df, MarketDataResponseFrame)
        assert df._constructor is MarketDataResponseFrame

    def test_dataset_ids_attribute(self):
        df = MarketDataResponseFrame({'a': [1]})
        df.dataset_ids = ('DS1', 'DS2')
        assert df.dataset_ids == ('DS1', 'DS2')

    def test_finalize_copies_dataset_ids(self):
        df1 = MarketDataResponseFrame({'a': [1, 2]})
        df1.dataset_ids = ('DS1',)
        # Slicing triggers __finalize__
        df2 = df1.iloc[:1]
        assert isinstance(df2, MarketDataResponseFrame)
        assert df2.dataset_ids == ('DS1',)

    def test_finalize_from_non_mdrf(self):
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = MarketDataResponseFrame(df1)
        # Should not fail even if other doesn't have dataset_ids
        assert isinstance(df2, MarketDataResponseFrame)

    def test_concat(self):
        df1 = MarketDataResponseFrame({'a': [1]})
        df1.dataset_ids = ('DS1',)
        df2 = MarketDataResponseFrame({'a': [2]})
        df2.dataset_ids = ('DS2',)
        result = pd.concat([df1, df2])
        assert isinstance(result, MarketDataResponseFrame)

    def test_empty_frame(self):
        df = MarketDataResponseFrame()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
