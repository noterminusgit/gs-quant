"""
Comprehensive branch coverage tests for gs_quant/api/gs/assets.py
"""

import asyncio
import datetime as dt
import os
from unittest.mock import MagicMock, patch, PropertyMock, call

import pytest
from requests.exceptions import HTTPError

from gs_quant.api.gs.assets import (
    AssetCache,
    GsAssetApi,
    GsAsset,
    GsIdType,
    GsTemporalXRef,
    ENABLE_ASSET_CACHING,
    _cached,
    _cached_async,
    get_default_cache,
)
from gs_quant.common import PositionType
from gs_quant.errors import MqValueError
from gs_quant.target.assets import FieldFilterMap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_session():
    """Create a mock GsSession.current with sync and async_ sub-mocks."""
    session = MagicMock()
    session.sync = MagicMock()
    session.async_ = MagicMock()
    return session


# ---------------------------------------------------------------------------
# AssetCache tests
# ---------------------------------------------------------------------------

class TestAssetCache:
    def test_properties(self):
        cache_mock = MagicMock()
        key_fn = MagicMock(return_value='key123')
        ac = AssetCache(cache=cache_mock, ttl=60, construct_key_fn=key_fn)
        assert ac.ttl == 60
        assert ac.cache is cache_mock
        assert ac.construct_key_fn is key_fn

    def test_construct_key(self):
        cache_mock = MagicMock()
        key_fn = MagicMock(return_value='computed_key')
        ac = AssetCache(cache=cache_mock, ttl=30, construct_key_fn=key_fn)
        session = MagicMock()
        result = ac.construct_key(session, 'arg1', kwarg1='val1')
        key_fn.assert_called_once_with(session, 'arg1', kwarg1='val1')
        assert result == 'computed_key'


# ---------------------------------------------------------------------------
# get_default_cache tests
# ---------------------------------------------------------------------------

class TestGetDefaultCache:
    def test_returns_asset_cache(self):
        cache = get_default_cache()
        assert isinstance(cache, AssetCache)
        assert cache.ttl == 30

    def test_key_fn_with_lists_in_args(self):
        cache = get_default_cache()
        session = MagicMock()
        # The key function should convert lists to tuples for hashing
        k = cache.construct_key(session, [1, 2, 3], name='test')
        assert k is not None

    def test_key_fn_with_lists_in_kwargs(self):
        cache = get_default_cache()
        session = MagicMock()
        # Lists in kwargs should be converted to tuples
        k = cache.construct_key(session, my_list=[1, 2])
        assert k is not None

    def test_key_fn_with_non_list_args(self):
        cache = get_default_cache()
        session = MagicMock()
        k = cache.construct_key(session, 'scalar_arg', count=5)
        assert k is not None


# ---------------------------------------------------------------------------
# _cached decorator tests
# ---------------------------------------------------------------------------

class TestCachedDecorator:
    def test_caching_disabled(self):
        """When ENABLE_ASSET_CACHING is not set, function is called directly."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop(ENABLE_ASSET_CACHING, None)

            @_cached
            def my_func(cls, x):
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = None
            result = my_func(cls_mock, 5)
            assert result == 10

    def test_caching_enabled_miss_then_hit(self):
        """When caching is enabled, first call misses, second call hits."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = None  # cache miss
        mock_asset_cache = MagicMock()
        mock_asset_cache.cache = mock_cache
        mock_asset_cache.ttl = 30
        mock_asset_cache.construct_key.return_value = 'test_key'

        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            call_count = 0

            @_cached
            def my_func(cls, x):
                nonlocal call_count
                call_count += 1
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = mock_asset_cache

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                result = my_func(cls_mock, 5)
                assert result == 10
                assert call_count == 1

    def test_caching_enabled_cache_hit(self):
        """When caching is enabled and cache has data, return cached result."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = 'cached_value'
        mock_asset_cache = MagicMock()
        mock_asset_cache.cache = mock_cache
        mock_asset_cache.ttl = 30
        mock_asset_cache.construct_key.return_value = 'test_key'

        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            @_cached
            def my_func(cls, x):
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = mock_asset_cache

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                result = my_func(cls_mock, 5)
                assert result == 'cached_value'

    def test_caching_enabled_fallback_cache(self):
        """When cls.get_cache() returns None, fallback cache is used."""
        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            @_cached
            def my_func(cls, x):
                return x * 3

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = None

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                result = my_func(cls_mock, 5)
                assert result == 15


# ---------------------------------------------------------------------------
# _cached_async decorator tests
# ---------------------------------------------------------------------------

class TestCachedAsyncDecorator:
    def test_caching_disabled(self):
        """When ENABLE_ASSET_CACHING is not set, async function is called directly."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop(ENABLE_ASSET_CACHING, None)

            @_cached_async
            async def my_func(cls, x):
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = None
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(my_func(cls_mock, 5))
                assert result == 10
            finally:
                loop.close()

    def test_caching_enabled_miss(self):
        """When caching is enabled, first call is a miss."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_asset_cache = MagicMock()
        mock_asset_cache.cache = mock_cache
        mock_asset_cache.ttl = 30
        mock_asset_cache.construct_key.return_value = 'async_key'

        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            @_cached_async
            async def my_func(cls, x):
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = mock_asset_cache

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(my_func(cls_mock, 5))
                    assert result == 10
                finally:
                    loop.close()

    def test_caching_enabled_cache_hit(self):
        """When caching is enabled and data is cached, return cached result."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = 'cached_async'
        mock_asset_cache = MagicMock()
        mock_asset_cache.cache = mock_cache
        mock_asset_cache.ttl = 30
        mock_asset_cache.construct_key.return_value = 'async_key'

        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            @_cached_async
            async def my_func(cls, x):
                return x * 2

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = mock_asset_cache

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(my_func(cls_mock, 5))
                    assert result == 'cached_async'
                finally:
                    loop.close()

    def test_caching_enabled_fallback_cache(self):
        """When cls.get_cache() is None, fallback cache is used."""
        with patch.dict(os.environ, {ENABLE_ASSET_CACHING: '1'}):
            @_cached_async
            async def my_func(cls, x):
                return x * 3

            cls_mock = MagicMock()
            cls_mock.get_cache.return_value = None

            with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
                mock_gs.current = MagicMock()
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(my_func(cls_mock, 5))
                    assert result == 15
                finally:
                    loop.close()


# ---------------------------------------------------------------------------
# GsIdType tests
# ---------------------------------------------------------------------------

class TestGsIdType:
    def test_enum_values(self):
        assert GsIdType.ric.name == 'ric'
        assert GsIdType.bbid.name == 'bbid'
        assert GsIdType.id.name == 'id'
        assert GsIdType.ticker.name == 'ticker'


# ---------------------------------------------------------------------------
# GsAssetApi tests
# ---------------------------------------------------------------------------

class TestGsAssetApiCacheManagement:
    def test_set_and_get_cache(self):
        original = GsAssetApi.get_cache()
        try:
            mock_cache = MagicMock(spec=AssetCache)
            GsAssetApi.set_cache(mock_cache)
            assert GsAssetApi.get_cache() is mock_cache
        finally:
            GsAssetApi.set_cache(original)

    def test_get_cache_default_none(self):
        original = GsAssetApi.get_cache()
        try:
            GsAssetApi._cache = None
            assert GsAssetApi.get_cache() is None
        finally:
            GsAssetApi.set_cache(original)


class TestCreateQuery:
    """Test the private __create_query method via get_many_assets."""

    def test_invalid_kwargs_raises_key_error(self):
        """Invalid kwargs should raise KeyError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_gs.current = _mock_session()
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with pytest.raises(KeyError, match='Invalid asset query argument'):
                    GsAssetApi.get_many_assets(totally_fake_field='bad_value')

    def test_valid_kwargs_no_as_of(self):
        """When as_of is None, utcnow() is used."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets()
                assert result == []


class TestSetTags:
    def test_set_tags_with_none_scope(self):
        """When scope is None, nothing happens."""
        GsAssetApi._set_tags(None, {'key': 'val'})

    def test_set_tags_with_none_kwargs(self):
        """When kwargs is None, nothing happens."""
        scope = MagicMock()
        GsAssetApi._set_tags(scope, None)

    def test_set_tags_scope_no_span(self):
        """When scope.span is falsy, nothing happens."""
        scope = MagicMock()
        scope.span = None
        GsAssetApi._set_tags(scope, {'key': 'val'})

    def test_set_tags_list_short(self):
        """Lists with <= 5 elements are joined as string."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'ids': ['a', 'b', 'c']})
        scope.span.set_tag.assert_called_once_with('request.payload.ids', 'a, b, c')

    def test_set_tags_list_long(self):
        """Lists with > 5 elements show the count."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'ids': ['a', 'b', 'c', 'd', 'e', 'f']})
        scope.span.set_tag.assert_called_once_with('request.payload.ids', 6)

    def test_set_tags_tuple_short(self):
        """Tuples with <= 5 elements are joined as string."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'ids': ('x', 'y')})
        scope.span.set_tag.assert_called_once_with('request.payload.ids', 'x, y')

    def test_set_tags_tuple_long(self):
        """Tuples with > 5 elements show the count."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'ids': ('a', 'b', 'c', 'd', 'e', 'f')})
        scope.span.set_tag.assert_called_once_with('request.payload.ids', 6)

    def test_set_tags_scalar_types(self):
        """int, float, bool, str values are set directly."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'count': 42, 'ratio': 0.5, 'flag': True, 'name': 'test'})
        assert scope.span.set_tag.call_count == 4

    def test_set_tags_non_scalar_non_list(self):
        """Values that are not list/tuple/int/float/bool/str are skipped."""
        scope = MagicMock()
        scope.span = MagicMock()
        GsAssetApi._set_tags(scope, {'obj': {'nested': 'dict'}})
        scope.span.set_tag.assert_not_called()


class TestGetManyAssets:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': [{'id': 'A1'}]}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets(name='Test')
                assert result == [{'id': 'A1'}]

    def test_with_tracer_recording(self):
        """Branch: span is recording -> Tracer is used."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with patch('gs_quant.api.gs.assets.Tracer') as mock_tracer:
                    mock_span = MagicMock()
                    mock_span.is_recording.return_value = True
                    mock_tracer.active_span.return_value = mock_span
                    mock_scope = MagicMock()
                    mock_tracer.return_value.__enter__ = MagicMock(return_value=mock_scope)
                    mock_tracer.return_value.__exit__ = MagicMock(return_value=False)
                    result = GsAssetApi.get_many_assets()
                    assert result == []

    def test_with_tracer_not_recording(self):
        """Branch: span is not recording -> nullcontext is used."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with patch('gs_quant.api.gs.assets.Tracer') as mock_tracer:
                    mock_span = MagicMock()
                    mock_span.is_recording.return_value = False
                    mock_tracer.active_span.return_value = mock_span
                    result = GsAssetApi.get_many_assets()
                    assert result == []

    def test_with_no_span(self):
        """Branch: span is None -> nullcontext is used."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with patch('gs_quant.api.gs.assets.Tracer') as mock_tracer:
                    mock_tracer.active_span.return_value = None
                    result = GsAssetApi.get_many_assets()
                    assert result == []


class TestGetManyAssetsAsync:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            # async post must return an awaitable
            async def async_post(*args, **kwargs):
                return {'results': [{'id': 'A1'}]}
            mock_session.async_.post = async_post
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(GsAssetApi.get_many_assets_async(name='Test'))
                    assert result == [{'id': 'A1'}]
                finally:
                    loop.close()


class TestGetManyAssetsScroll:
    def test_no_scroll(self):
        """Response without scrollId returns results immediately."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': [{'id': 'A1'}]}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_scroll(name='Test')
                assert result == [{'id': 'A1'}]

    def test_with_scroll(self):
        """Response with scrollId causes additional requests."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.side_effect = [
                {'results': [{'id': 'A1'}], 'scrollId': 'scroll1'},
                {'results': [{'id': 'A2'}], 'scrollId': 'scroll2'},
                {'results': []},  # empty results stops scrolling
            ]
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_scroll(name='Test')
                assert len(result) == 2

    def test_scroll_no_scroll_id_in_response(self):
        """Response without scrollId after first page."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': [{'id': 'A1'}]}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_scroll(name='Test')
                assert result == [{'id': 'A1'}]


class TestGetManyAssetsData:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': [{'data': 1}]}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_data(name='Test')
                assert result == [{'data': 1}]

    def test_with_source_basket(self):
        """source='Basket' should add X-Application header."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                GsAssetApi.get_many_assets_data(source='Basket', name='Test')
                call_args = mock_session.sync.post.call_args
                assert call_args[1]['request_headers'] == {'X-Application': 'Studio'}

    def test_with_source_not_basket(self):
        """source != 'Basket' should not add X-Application header."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                GsAssetApi.get_many_assets_data(source='Other', name='Test')
                call_args = mock_session.sync.post.call_args
                assert call_args[1]['request_headers'] is None


class TestGetManyAssetsDataAsync:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            # Note: get_many_assets_data_async uses @_cached (not _cached_async)
            # so it wraps a regular call, but the inner function is async.
            # Actually looking at the code, it uses @_cached which wraps with sync wrapper
            # but the inner function is async. This is an interesting pattern.
            # The _cached wrapper calls fn(cls, *args, **kwargs) which returns a coroutine.
            # Let's just test the sync wrapper path.

            async def mock_async_post(*a, **kw):
                return {'results': [{'data': 1}]}

            mock_session.async_.post = mock_async_post
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                # Since it's decorated with @_cached (sync), calling it returns a coroutine
                result = GsAssetApi.get_many_assets_data_async(name='Test')
                loop = asyncio.new_event_loop()
                try:
                    actual = loop.run_until_complete(result)
                    assert actual == [{'data': 1}]
                finally:
                    loop.close()


class TestGetManyAssetsDataScroll:
    def test_no_scroll(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': [{'data': 1}]}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_data_scroll(name='Test')
                assert result == [{'data': 1}]

    def test_with_scroll(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.side_effect = [
                {'results': [{'data': 1}], 'scrollId': 's1'},
                {'results': [{'data': 2}], 'scrollId': 's2'},
                {'results': []},
            ]
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_assets_data_scroll(name='Test')
                assert len(result) == 2

    def test_with_source_basket(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                GsAssetApi.get_many_assets_data_scroll(source='Basket', name='Test')
                call_args = mock_session.sync.post.call_args
                assert call_args[1]['request_headers'] == {'X-Application': 'Studio'}

    def test_with_source_not_basket(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                GsAssetApi.get_many_assets_data_scroll(source='Other', name='Test')
                call_args = mock_session.sync.post.call_args
                assert call_args[1]['request_headers'] is None


class TestResolveAssets:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'results': []}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.resolve_assets(
                    identifier=['AAPL'],
                    fields=['id', 'name'],
                    as_of=dt.datetime(2023, 1, 1),
                )
                assert result == {'results': []}
                mock_session.sync.post.assert_called_once()


class TestGetManyAssetXrefs:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = MagicMock(get=MagicMock(return_value=[{'ric': '.SPX'}]))
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_many_asset_xrefs(
                    identifier=['AAPL'],
                    as_of=dt.datetime(2023, 1, 1),
                )
                assert result == [{'ric': '.SPX'}]


class TestGetAssetXrefs:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2020-12-31',
                        'identifiers': {'ric': '.SPX'},
                    }
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_asset_xrefs('asset123')
                assert len(result) == 1
                assert isinstance(result[0], GsTemporalXRef)

    def test_empty_xrefs(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {}
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_asset_xrefs('asset_empty')
                assert result == ()


class TestPutAssetXrefs:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.put.return_value = {'status': 'ok'}
            result = GsAssetApi.put_asset_xrefs('asset123', [{'ric': '.SPX'}])
            assert result == {'status': 'ok'}
            mock_session.sync.put.assert_called_once_with(
                '/assets/asset123/xrefs', payload=[{'ric': '.SPX'}]
            )


class TestGetAsset:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_asset = MagicMock(spec=GsAsset)
            mock_session.sync.get.return_value = mock_asset
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.get_asset('asset123')
                assert result is mock_asset
                mock_session.sync.get.assert_called_with('/assets/asset123', cls=GsAsset)


class TestGetAssetAsync:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            async def async_get(*args, **kwargs):
                return MagicMock(spec=GsAsset)

            mock_session.async_.get = async_get
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(GsAssetApi.get_asset_async('asset123'))
                    assert result is not None
                finally:
                    loop.close()


class TestGetAssetByName:
    def test_found_one(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'totalResults': 1,
                'results': [{'assetClass': 'Equity', 'type': 'Single Stock', 'name': 'Test'}],
            }
            result = GsAssetApi.get_asset_by_name('Test')
            assert isinstance(result, GsAsset)

    def test_not_found(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'totalResults': 0, 'results': []}
            with pytest.raises(ValueError, match='not found'):
                GsAssetApi.get_asset_by_name('Nonexistent')

    def test_multiple_found(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'totalResults': 2, 'results': [{}, {}]}
            with pytest.raises(ValueError, match='More than one'):
                GsAssetApi.get_asset_by_name('Duplicate')

    def test_zero_total_results_missing(self):
        """totalResults key missing defaults to 0 via .get()."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': []}
            with pytest.raises(ValueError, match='not found'):
                GsAssetApi.get_asset_by_name('Test')


class TestCreateAsset:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_asset = MagicMock(spec=GsAsset)
            mock_session.sync.post.return_value = mock_asset
            result = GsAssetApi.create_asset(mock_asset)
            assert result is mock_asset


class TestDeleteAsset:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.delete.return_value = None
            result = GsAssetApi.delete_asset('asset123')
            mock_session.sync.delete.assert_called_with('/assets/asset123')


class TestGetPositionDates:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': ['2023-01-01', '2023-01-02']}
            result = GsAssetApi.get_position_dates('asset123')
            assert len(result) == 2
            assert result[0] == dt.date(2023, 1, 1)
            assert result[1] == dt.date(2023, 1, 2)


class TestGetAssetPositionsForDate:
    def test_without_position_type(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': [
                    {'positionDate': '2023-01-01', 'positions': [{'assetId': 'A1', 'quantity': 100}]}
                ]
            }
            result = GsAssetApi.get_asset_positions_for_date('asset123', dt.date(2023, 1, 1))
            assert len(result) == 1
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/2023-01-01')

    def test_with_position_type_enum(self):
        """PositionType enum value branch."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            GsAssetApi.get_asset_positions_for_date('asset123', dt.date(2023, 1, 1), PositionType.CLOSE)
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/2023-01-01?type=close')

    def test_with_position_type_string(self):
        """String position type branch."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            GsAssetApi.get_asset_positions_for_date('asset123', dt.date(2023, 1, 1), 'close')
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/2023-01-01?type=close')


class TestGetAssetPositionsForDates:
    def test_short_period(self):
        """When periods <= 1, use single request."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'positionSets': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 15)  # 14 days, periods = 0
            result = GsAssetApi.get_asset_positions_for_dates('asset123', start, end)
            assert len(result) == 1

    def test_long_period(self):
        """When periods > 1, use multiple requests."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'positionSets': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 4, 1)  # ~90 days, periods = 3
            result = GsAssetApi.get_asset_positions_for_dates('asset123', start, end)
            assert len(result) >= 1

    def test_long_period_http_error(self):
        """HTTPError during multi-period request raises ValueError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.side_effect = HTTPError('server error')
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 4, 1)  # periods > 1
            with pytest.raises(ValueError, match='Unable to fetch position data'):
                GsAssetApi.get_asset_positions_for_dates('asset123', start, end)

    def test_short_period_http_error(self):
        """HTTPError during single period request raises ValueError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.side_effect = HTTPError('server error')
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 15)
            with pytest.raises(ValueError, match='Unable to fetch position data'):
                GsAssetApi.get_asset_positions_for_dates('asset123', start, end)

    def test_with_string_position_type(self):
        """position_type as string branch."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'positionSets': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 15)
            result = GsAssetApi.get_asset_positions_for_dates('asset123', start, end, position_type='close')
            assert len(result) == 1

    def test_with_enum_position_type(self):
        """position_type as PositionType enum branch."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'positionSets': [
                    {'positionDate': '2023-01-01', 'positions': []}
                ]
            }
            start = dt.date(2023, 1, 1)
            end = dt.date(2023, 1, 15)
            result = GsAssetApi.get_asset_positions_for_dates(
                'asset123', start, end, position_type=PositionType.OPEN
            )
            assert len(result) == 1


class TestGetLatestPositions:
    def test_no_position_type(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': {'positionDate': '2023-01-01', 'positions': []}
            }
            result = GsAssetApi.get_latest_positions('asset123')
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/last')

    def test_with_position_type_any(self):
        """PositionType.ANY should NOT add type param."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': {'positionDate': '2023-01-01', 'positions': []}
            }
            GsAssetApi.get_latest_positions('asset123', PositionType.ANY)
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/last')

    def test_with_position_type_close(self):
        """PositionType.CLOSE should add type=close."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': {'positionDate': '2023-01-01', 'positions': []}
            }
            GsAssetApi.get_latest_positions('asset123', PositionType.CLOSE)
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/last?type=close')

    def test_with_position_type_string(self):
        """String position type."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': {'positionDate': '2023-01-01', 'positions': []}
            }
            GsAssetApi.get_latest_positions('asset123', 'open')
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/last?type=open')

    def test_with_none_position_type(self):
        """None position type should not add ?type=."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {
                'results': {'positionDate': '2023-01-01', 'positions': []}
            }
            GsAssetApi.get_latest_positions('asset123', None)
            mock_session.sync.get.assert_called_with('/assets/asset123/positions/last')


class TestGetOrCreateAssetFromInstrument:
    def test_with_name(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'id': 'new_asset_id'}

            instrument = MagicMock()
            instrument.asset_class = 'Equity'
            instrument.type = 'Single Stock'
            instrument.name = 'TestInstrument'
            instrument.as_dict.return_value = {'key': 'val'}

            result = GsAssetApi.get_or_create_asset_from_instrument(instrument)
            assert result == 'new_asset_id'

    def test_without_name(self):
        """When instrument.name is None, empty string is used."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {'id': 'new_asset_id'}

            instrument = MagicMock()
            instrument.asset_class = 'Equity'
            instrument.type = 'Single Stock'
            instrument.name = None
            instrument.as_dict.return_value = {}

            result = GsAssetApi.get_or_create_asset_from_instrument(instrument)
            assert result == 'new_asset_id'


class TestGetInstrumentsForAssetIds:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            info1 = MagicMock()
            info1.assetId = 'A1'
            info1.instrument = MagicMock()
            info2 = MagicMock()
            info2.assetId = 'A2'
            info2.instrument = MagicMock()

            mock_session.sync.post.return_value = [info1, info2]

            result = GsAssetApi.get_instruments_for_asset_ids(('A1', 'A2'))
            assert len(result) == 2

    def test_with_none_in_results(self):
        """None entries in instrument_infos should be filtered out."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            info1 = MagicMock()
            info1.assetId = 'A1'
            info1.instrument = MagicMock()

            mock_session.sync.post.return_value = [info1, None]

            result = GsAssetApi.get_instruments_for_asset_ids(('A1', 'A2'))
            assert len(result) == 2
            assert result[0] is not None
            assert result[1] is None  # A2 not in lookup

    def test_missing_asset_id(self):
        """Asset ID not in lookup returns None."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = []
            result = GsAssetApi.get_instruments_for_asset_ids(('A1',))
            assert result == (None,)


class TestGetInstrumentsForPositions:
    def test_position_with_instrument(self):
        """Position already has an instrument."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = []

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = MagicMock()  # already has instrument
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert len(result) == 1
            assert result[0] is pos.instrument

    def test_position_without_instrument_with_lookup(self):
        """Position without instrument but found in lookup."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            mock_instr = MagicMock()
            mock_instr.notional = None  # getattr returns None -> size_field attr is None

            info = MagicMock()
            info.assetId = 'A1'
            info.instrument = mock_instr
            info.sizeField = 'notional'

            mock_session.sync.post.return_value = [info]

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None  # no instrument
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert len(result) == 1
            assert result[0] is mock_instr

    def test_position_without_instrument_no_lookup(self):
        """Position without instrument and not in lookup."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = []

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert len(result) == 1
            assert result[0] is None

    def test_no_asset_ids(self):
        """When no positions have asset_ids, instrument_infos is empty dict."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            pos = MagicMock()
            pos.asset_id = None  # no asset_id
            pos.instrument = None
            pos.assetId = 'A1'

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert len(result) == 1
            assert result[0] is None

    def test_size_field_already_set(self):
        """When size_field attr is already set on instrument, don't overwrite."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            mock_instr = MagicMock()
            mock_instr.notional = 5000  # already set, not None

            info = MagicMock()
            info.assetId = 'A1'
            info.instrument = mock_instr
            info.sizeField = 'notional'

            mock_session.sync.post.return_value = [info]

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert result[0] is mock_instr
            # notional was already set, so setattr should not have been called to change it

    def test_instrument_is_none_in_lookup(self):
        """When instrument in lookup is None, size_field check is skipped."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            info = MagicMock()
            info.assetId = 'A1'
            info.instrument = None
            info.sizeField = 'notional'

            mock_session.sync.post.return_value = [info]

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert result[0] is None

    def test_size_field_is_none_in_lookup(self):
        """When size_field is None, setattr is not called."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            mock_instr = MagicMock()

            info = MagicMock()
            info.assetId = 'A1'
            info.instrument = mock_instr
            info.sizeField = None

            mock_session.sync.post.return_value = [info]

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert result[0] is mock_instr

    def test_none_in_instrument_infos(self):
        """None items in instrument_infos are filtered by 'if i'."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session

            info = MagicMock()
            info.assetId = 'A1'
            info.instrument = MagicMock()
            info.sizeField = None

            mock_session.sync.post.return_value = [info, None]

            pos = MagicMock()
            pos.asset_id = 'A1'
            pos.instrument = None
            pos.assetId = 'A1'
            pos.quantity = 100

            result = GsAssetApi.get_instruments_for_positions([pos])
            assert result[0] is info.instrument


class TestGetAssetPositionsData:
    def test_no_fields_no_position_type(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': [{'data': 1}]}
            result = GsAssetApi.get_asset_positions_data(
                'asset123', dt.date(2023, 1, 1), dt.date(2023, 1, 31)
            )
            assert result == [{'data': 1}]
            expected_url = '/assets/asset123/positions/data?startDate=2023-01-01&endDate=2023-01-31'
            mock_session.sync.get.assert_called_with(expected_url)

    def test_with_fields(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': []}
            GsAssetApi.get_asset_positions_data(
                'asset123', dt.date(2023, 1, 1), dt.date(2023, 1, 31),
                fields=['quantity', 'weight']
            )
            call_url = mock_session.sync.get.call_args[0][0]
            assert '&fields=quantity' in call_url
            assert '&fields=weight' in call_url

    def test_with_position_type(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': []}
            GsAssetApi.get_asset_positions_data(
                'asset123', dt.date(2023, 1, 1), dt.date(2023, 1, 31),
                position_type=PositionType.CLOSE
            )
            call_url = mock_session.sync.get.call_args[0][0]
            assert '&type=close' in call_url

    def test_with_fields_and_position_type(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': []}
            GsAssetApi.get_asset_positions_data(
                'asset123', dt.date(2023, 1, 1), dt.date(2023, 1, 31),
                fields=['quantity'], position_type=PositionType.OPEN
            )
            call_url = mock_session.sync.get.call_args[0][0]
            assert '&fields=quantity' in call_url
            assert '&type=open' in call_url


class TestUpdateAssetEntitlements:
    def test_success(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.put.return_value = {'status': 'ok'}
            entitlements = MagicMock()
            result = GsAssetApi.update_asset_entitlements('asset123', entitlements)
            assert result == {'status': 'ok'}

    def test_http_error(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.put.side_effect = HTTPError('forbidden')
            entitlements = MagicMock()
            with pytest.raises(ValueError, match='Unable to update asset entitlements'):
                GsAssetApi.update_asset_entitlements('asset123', entitlements)


class TestGetReports:
    def test_basic_call(self):
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.get.return_value = {'results': [{'id': 'R1'}]}
            result = GsAssetApi.get_reports('asset123')
            assert result == [{'id': 'R1'}]


class TestMapIdentifiers:
    def test_with_gs_id_type_enum(self):
        """input_type and output_type as GsIdType."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {
                'results': [
                    {'ric': '.SPX', 'bbid': 'SPX'}
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.map_identifiers(
                    GsIdType.ric, GsIdType.bbid, ['.SPX']
                )
                assert result == {'.SPX': 'SPX'}

    def test_with_string_types(self):
        """input_type and output_type as strings."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {
                'results': [
                    {'ric': '.SPX', 'bbid': 'SPX'}
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.map_identifiers('ric', 'bbid', ['.SPX'])
                assert result == {'.SPX': 'SPX'}

    def test_invalid_input_type(self):
        """Non-string non-GsIdType input_type raises ValueError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_gs.current = _mock_session()
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with pytest.raises(ValueError, match='input_type must be of type str or IdType'):
                    GsAssetApi.map_identifiers(123, 'bbid', ['.SPX'])

    def test_invalid_output_type(self):
        """Non-string non-GsIdType output_type raises ValueError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_gs.current = _mock_session()
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with pytest.raises(ValueError, match='output_type must be of type str or IdType'):
                    GsAssetApi.map_identifiers('ric', 123, ['.SPX'])

    def test_results_exceed_capacity(self):
        """When results length >= limit, raise MqValueError."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            # Return a list that is >= limit (4 * 1 = 4)
            mock_session.sync.post.return_value = [1, 2, 3, 4]  # len=4, limit=4
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                with pytest.raises(MqValueError, match='exceeded capacity'):
                    GsAssetApi.map_identifiers('ric', 'bbid', ['X'])

    def test_multimap(self):
        """multimap=True builds lists of values."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {
                'results': [
                    {'ric': '.SPX', 'bbid': 'SPX1'},
                    {'ric': '.SPX', 'bbid': 'SPX2'},
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.map_identifiers('ric', 'bbid', ['.SPX'], multimap=True)
                assert result == {'.SPX': ['SPX1', 'SPX2']}

    def test_duplicate_key_warning(self):
        """When not multimap and key already in out, log warning."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {
                'results': [
                    {'ric': '.SPX', 'bbid': 'SPX1'},
                    {'ric': '.SPX', 'bbid': 'SPX2'},
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.map_identifiers('ric', 'bbid', ['.SPX'])
                # Second value overwrites first
                assert result == {'.SPX': 'SPX2'}

    def test_results_not_in_dict(self):
        """When response is a list (no 'results' key), use as-is."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            # Response without 'results' key - just entries directly
            # But len must be < limit to pass capacity check
            # The response needs to be iterable and support len()
            # and not have 'results' key
            response = [{'ric': '.SPX', 'bbid': 'SPX'}]
            mock_session.sync.post.return_value = response
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                # limit = 4 * 2 = 8, len(response) = 1 < 8
                result = GsAssetApi.map_identifiers('ric', 'bbid', ['.SPX', '.DJI'])
                assert result == {'.SPX': 'SPX'}

    def test_with_custom_limit(self):
        """Custom limit parameter."""
        with patch('gs_quant.api.gs.assets.GsSession') as mock_gs:
            mock_session = _mock_session()
            mock_gs.current = mock_session
            mock_session.sync.post.return_value = {
                'results': [
                    {'ric': '.SPX', 'bbid': 'SPX'},
                ]
            }
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop(ENABLE_ASSET_CACHING, None)
                result = GsAssetApi.map_identifiers('ric', 'bbid', ['.SPX'], limit=50)
                assert result == {'.SPX': 'SPX'}


class TestGsAsset:
    def test_gs_asset_inherits(self):
        """GsAsset should be a subclass of __Asset."""
        from gs_quant.target.assets import Asset as __Asset
        assert issubclass(GsAsset, __Asset)


class TestGsTemporalXRef:
    def test_gs_temporal_xref_inherits(self):
        from gs_quant.target.assets import TemporalXRef
        assert issubclass(GsTemporalXRef, TemporalXRef)
