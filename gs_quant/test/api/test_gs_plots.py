"""
Tests for gs_quant.api.gs.plots - GsPlotApi
Target: 100% branch coverage
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.plots import GsPlotApi
from gs_quant.target.charts import Chart, ChartShare


def _mock_session():
    session = MagicMock()
    return session


class TestGsPlotApiSync:
    def test_get_many_charts_default_limit(self):
        mock_session = _mock_session()
        chart = Chart(name='test_chart')
        mock_session.sync.get.return_value = {'results': (chart,)}
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.get_many_charts()
            mock_session.sync.get.assert_called_once_with('/charts?limit=100', cls=Chart)
            assert result == (chart,)

    def test_get_many_charts_custom_limit(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.get_many_charts(limit=50)
            mock_session.sync.get.assert_called_once_with('/charts?limit=50', cls=Chart)
            assert result == ()

    def test_get_chart(self):
        mock_session = _mock_session()
        chart = Chart(name='my_chart', id_='chart123')
        mock_session.sync.get.return_value = chart
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.get_chart('chart123')
            mock_session.sync.get.assert_called_once_with('/charts/chart123', cls=Chart)
            assert result == chart

    def test_create_chart(self):
        mock_session = _mock_session()
        chart = Chart(name='new_chart')
        mock_session.sync.post.return_value = chart
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.create_chart(chart)
            mock_session.sync.post.assert_called_once_with('/charts', chart, cls=Chart)
            assert result == chart

    def test_update_chart(self):
        mock_session = _mock_session()
        chart = Chart(name='updated_chart', id_='chart123')
        mock_session.sync.put.return_value = chart
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.update_chart(chart)
            mock_session.sync.put.assert_called_once_with('/charts/chart123', chart, cls=Chart)
            assert result == chart

    def test_delete_chart(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'ok'}
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.delete_chart('chart123')
            mock_session.sync.delete.assert_called_once_with('/charts/chart123')
            assert result == {'status': 'ok'}

    def test_share_chart_valid_users(self):
        mock_session = _mock_session()
        chart = Chart(name='shared_chart', id_='chart123', version=1)
        mock_session.sync.get.return_value = chart
        mock_session.sync.post.return_value = chart
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsPlotApi.share_chart('chart123', ['guid:user1', 'guid:user2'])
            mock_session.sync.get.assert_called_once_with('/charts/chart123', cls=Chart)
            # Verify post was called with ChartShare
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/charts/chart123/share'
            assert isinstance(call_args[0][1], ChartShare)
            assert call_args[1]['cls'] == Chart

    def test_share_chart_invalid_users_raises(self):
        """Branch: any user not starting with 'guid:' -> ValueError"""
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError, match='Chart can only be shared with individual users'):
                GsPlotApi.share_chart('chart123', ['guid:user1', 'role:admin'])

    def test_share_chart_all_invalid_users_raises(self):
        """Branch: all users invalid"""
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            with pytest.raises(ValueError):
                GsPlotApi.share_chart('chart123', ['baduser'])


class TestGsPlotApiAsync:
    def test_get_many_charts_async_default(self):
        mock_session = _mock_session()
        chart = Chart(name='async_chart')
        mock_response = {'results': (chart,)}

        async def mock_get(*args, **kwargs):
            return mock_response

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.get_many_charts_async())
                assert result == (chart,)
            finally:
                loop.close()

    def test_get_many_charts_async_custom_limit(self):
        mock_session = _mock_session()
        mock_response = {'results': ()}

        async def mock_get(*args, **kwargs):
            return mock_response

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.get_many_charts_async(limit=25))
                assert result == ()
            finally:
                loop.close()

    def test_get_chart_async(self):
        mock_session = _mock_session()
        chart = Chart(name='async_chart', id_='c1')

        async def mock_get(*args, **kwargs):
            return chart

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.get_chart_async('c1'))
                assert result == chart
            finally:
                loop.close()

    def test_create_chart_async(self):
        mock_session = _mock_session()
        chart = Chart(name='new_async_chart')

        async def mock_post(*args, **kwargs):
            return chart

        mock_session.async_ = MagicMock()
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.create_chart_async(chart))
                assert result == chart
            finally:
                loop.close()

    def test_update_chart_async(self):
        mock_session = _mock_session()
        chart = Chart(name='updated_async', id_='c1')

        async def mock_put(*args, **kwargs):
            return chart

        mock_session.async_ = MagicMock()
        mock_session.async_.put = mock_put

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.update_chart_async(chart))
                assert result == chart
            finally:
                loop.close()

    def test_delete_chart_async(self):
        mock_session = _mock_session()

        async def mock_delete(*args, **kwargs):
            return {'status': 'deleted'}

        mock_session.async_ = MagicMock()
        mock_session.async_.delete = mock_delete

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsPlotApi.delete_chart_async('c1'))
                assert result == {'status': 'deleted'}
            finally:
                loop.close()

    def test_share_chart_async_valid_users(self):
        mock_session = _mock_session()
        chart = Chart(name='shared', id_='c1', version=2)

        async def mock_get(*args, **kwargs):
            return chart

        async def mock_post(*args, **kwargs):
            return chart

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get
        mock_session.async_.post = mock_post

        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    GsPlotApi.share_chart_async('c1', ['guid:user1'])
                )
                assert result == chart
            finally:
                loop.close()

    def test_share_chart_async_invalid_users_raises(self):
        """Branch: any user not starting with 'guid:' -> ValueError"""
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.plots.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                with pytest.raises(ValueError, match='Chart can only be shared with individual users'):
                    loop.run_until_complete(
                        GsPlotApi.share_chart_async('c1', ['bad_user'])
                    )
            finally:
                loop.close()
