"""
Tests for gs_quant/backtests/core.py

Covers:
- TradeInMethod enum
- Backtest class and get_results
- MarketModel enum
- TimeWindow NamedTuple
- ValuationFixingType enum
- ValuationMethod NamedTuple with defaults and custom values
"""

import datetime as dt
from unittest.mock import patch, MagicMock

from gs_quant.backtests.core import (
    TradeInMethod,
    Backtest,
    MarketModel,
    TimeWindow,
    ValuationFixingType,
    ValuationMethod,
)


class TestTradeInMethod:
    def test_fixed_roll(self):
        assert TradeInMethod.FixedRoll.value == 'fixedRoll'

    def test_from_string(self):
        assert TradeInMethod('fixedRoll') == TradeInMethod.FixedRoll


class TestMarketModel:
    def test_sticky_fixed_strike(self):
        assert MarketModel.STICKY_FIXED_STRIKE.value == 'SFK'

    def test_sticky_delta(self):
        assert MarketModel.STICKY_DELTA.value == 'SD'

    def test_from_string(self):
        assert MarketModel('SFK') == MarketModel.STICKY_FIXED_STRIKE
        assert MarketModel('SD') == MarketModel.STICKY_DELTA


class TestTimeWindow:
    def test_defaults(self):
        tw = TimeWindow()
        assert tw.start is None
        assert tw.end is None

    def test_with_times(self):
        start = dt.time(9, 30)
        end = dt.time(16, 0)
        tw = TimeWindow(start=start, end=end)
        assert tw.start == start
        assert tw.end == end

    def test_with_datetimes(self):
        start = dt.datetime(2023, 1, 1, 9, 30)
        end = dt.datetime(2023, 1, 1, 16, 0)
        tw = TimeWindow(start=start, end=end)
        assert tw.start == start
        assert tw.end == end

    def test_named_tuple_access(self):
        tw = TimeWindow(start=dt.time(10, 0), end=dt.time(11, 0))
        assert tw[0] == dt.time(10, 0)
        assert tw[1] == dt.time(11, 0)


class TestValuationFixingType:
    def test_price(self):
        assert ValuationFixingType.PRICE.value == 'price'

    def test_from_string(self):
        assert ValuationFixingType('price') == ValuationFixingType.PRICE


class TestValuationMethod:
    def test_defaults(self):
        vm = ValuationMethod()
        assert vm.data_tag == ValuationFixingType.PRICE
        assert vm.window is None

    def test_with_window(self):
        tw = TimeWindow(start=dt.time(9, 30), end=dt.time(16, 0))
        vm = ValuationMethod(data_tag=ValuationFixingType.PRICE, window=tw)
        assert vm.data_tag == ValuationFixingType.PRICE
        assert vm.window == tw
        assert vm.window.start == dt.time(9, 30)

    def test_named_tuple_access(self):
        vm = ValuationMethod()
        assert vm[0] == ValuationFixingType.PRICE
        assert vm[1] is None


class TestBacktest:
    @patch('gs_quant.api.gs.backtests.GsBacktestApi')
    def test_get_results(self, mock_api_cls):
        bt = Backtest()
        bt.id = 'test-id-123'
        mock_api_cls.get_results.return_value = ('result1', 'result2')

        results = bt.get_results()
        assert results == ('result1', 'result2')
        mock_api_cls.get_results.assert_called_once_with(backtest_id='test-id-123')
