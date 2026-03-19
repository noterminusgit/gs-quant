"""
Branch coverage tests for gs_quant/priceable.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.context_base import nullcontext
from gs_quant.risk.results import PricingFuture, ErrorValue


class TestPriceableImplPricingContext:
    """Test _pricing_context and _return_future properties."""

    def test_pricing_context_not_entered_not_async(self):
        """When PricingContext is neither entered nor async, return the pricing context itself."""
        from gs_quant.priceable import PriceableImpl

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst._pricing_context = PriceableImpl._pricing_context

        mock_ctx = MagicMock()
        mock_ctx.is_entered = False
        mock_ctx.is_async = False

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current = mock_ctx
            result = PriceableImpl._pricing_context.fget(mock_inst)
            assert result is mock_ctx

    def test_pricing_context_entered_returns_nullcontext(self):
        """When PricingContext is entered, return nullcontext."""
        from gs_quant.priceable import PriceableImpl

        mock_inst = MagicMock(spec=PriceableImpl)

        mock_ctx = MagicMock()
        mock_ctx.is_entered = True
        mock_ctx.is_async = False

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current = mock_ctx
            result = PriceableImpl._pricing_context.fget(mock_inst)
            assert isinstance(result, nullcontext)

    def test_pricing_context_async_returns_nullcontext(self):
        """When PricingContext is async, return nullcontext."""
        from gs_quant.priceable import PriceableImpl

        mock_inst = MagicMock(spec=PriceableImpl)

        mock_ctx = MagicMock()
        mock_ctx.is_entered = False
        mock_ctx.is_async = True

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current = mock_ctx
            result = PriceableImpl._pricing_context.fget(mock_inst)
            assert isinstance(result, nullcontext)


class TestPriceableImplReturnFuture:
    """Test _return_future property."""

    def test_return_future_when_not_pricing_context(self):
        """_return_future returns True when _pricing_context is not a PricingContext."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets.core import PricingContext

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst._pricing_context = nullcontext()  # Not a PricingContext
        result = PriceableImpl._return_future.fget(mock_inst)
        assert result is True

    def test_return_future_when_async(self):
        """_return_future returns True when PricingContext is async."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets.core import PricingContext

        mock_ctx = MagicMock(spec=PricingContext)
        mock_ctx.is_async = True
        mock_ctx.is_entered = False
        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst._pricing_context = mock_ctx
        result = PriceableImpl._return_future.fget(mock_inst)
        assert result is True

    def test_return_future_when_entered(self):
        """_return_future returns True when PricingContext is entered."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets.core import PricingContext

        mock_ctx = MagicMock(spec=PricingContext)
        mock_ctx.is_async = False
        mock_ctx.is_entered = True
        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst._pricing_context = mock_ctx
        result = PriceableImpl._return_future.fget(mock_inst)
        assert result is True

    def test_return_future_false_when_not_async_not_entered(self):
        """_return_future returns False when PricingContext is neither async nor entered."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets.core import PricingContext

        mock_ctx = MagicMock(spec=PricingContext)
        mock_ctx.is_async = False
        mock_ctx.is_entered = False
        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst._pricing_context = mock_ctx
        result = PriceableImpl._return_future.fget(mock_inst)
        assert result is False


class TestPriceableImplPrice:
    """Test price() method with and without currency."""

    def test_price_with_currency(self):
        """price(currency=X) calls calc with Price(currency=X)."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.common import RiskMeasure

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock(return_value='result')
        PriceableImpl.price(mock_inst, currency='USD')
        mock_inst.calc.assert_called_once()
        args = mock_inst.calc.call_args
        # First arg should be a RiskMeasure (Price with currency='USD')
        assert isinstance(args[0][0], RiskMeasure)

    def test_price_without_currency(self):
        """price() without currency calls calc with Price (the default instance)."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.risk import Price

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock(return_value='result')
        PriceableImpl.price(mock_inst)
        # Price is a singleton RiskMeasure instance
        mock_inst.calc.assert_called_once_with(Price)


class TestPriceableImplMarket:
    """Test market() method and handle_result inner function."""

    def test_market_calls_calc_with_market_data(self):
        """market() calls calc with MarketData and a fn."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.risk import MarketData

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock(return_value='result')
        PriceableImpl.market(mock_inst)
        mock_inst.calc.assert_called_once()
        args, kwargs = mock_inst.calc.call_args
        assert args[0] is MarketData
        assert 'fn' in kwargs

    def test_handle_result_error_value(self):
        """handle_result returns ErrorValue as-is."""
        from gs_quant.priceable import PriceableImpl

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock()
        PriceableImpl.market(mock_inst)
        # Get the fn passed to calc
        fn = mock_inst.calc.call_args[1]['fn']

        error = ErrorValue(None, 'test error')
        result = fn(error)
        assert isinstance(result, ErrorValue)

    def test_handle_result_non_historical(self):
        """handle_result for non-historical data returns OverlayMarket."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets import OverlayMarket, MarketDataCoordinate

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock()
        PriceableImpl.market(mock_inst)
        fn = mock_inst.calc.call_args[1]['fn']

        # Create a mock DataFrameWithInfo
        mock_result = MagicMock()
        mock_result.index.name = 'other'  # Not 'date' => not historical

        mock_market = MagicMock()
        mock_result.risk_key.market = mock_market

        # Create rows with properties
        properties = MarketDataCoordinate.properties()
        row_data = {p: None for p in properties}
        row_data['mkt_type'] = 'IR'
        row_data['mkt_asset'] = 'USD'
        row_data['mkt_point'] = '2Y;3Y'
        row_data['value'] = 1.5
        row_data['permissions'] = 'Granted'

        mock_row = MagicMock()
        mock_row.get = lambda p: row_data.get(p)
        mock_row.__getitem__ = lambda self, key: row_data[key]

        mock_result.iterrows.return_value = [(0, mock_row)]

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current.market_data_location = 'LDN'
            result = fn(mock_result)
            assert isinstance(result, OverlayMarket)

    def test_handle_result_non_historical_redacted(self):
        """handle_result for non-historical with 'redacted' permissions."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets import OverlayMarket, MarketDataCoordinate

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock()
        PriceableImpl.market(mock_inst)
        fn = mock_inst.calc.call_args[1]['fn']

        mock_result = MagicMock()
        mock_result.index.name = 'other'

        mock_market = MagicMock()
        mock_result.risk_key.market = mock_market

        properties = MarketDataCoordinate.properties()
        row_data = {p: None for p in properties}
        row_data['mkt_type'] = 'IR'
        row_data['mkt_asset'] = 'USD'
        row_data['mkt_point'] = None  # no mkt_point
        row_data['value'] = 1.5
        row_data['permissions'] = 'Denied'

        mock_row = MagicMock()
        mock_row.get = lambda p: row_data.get(p)
        mock_row.__getitem__ = lambda self, key: row_data[key]

        mock_result.iterrows.return_value = [(0, mock_row)]

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current.market_data_location = 'LDN'
            result = fn(mock_result)
            assert isinstance(result, OverlayMarket)
            # OverlayMarket filters out 'redacted' values and stores them separately
            # The redacted_coordinates property should contain the redacted coordinate
            assert len(result.redacted_coordinates) > 0

    def test_handle_result_historical(self):
        """handle_result for historical data returns a dict of OverlayMarkets."""
        from gs_quant.priceable import PriceableImpl
        from gs_quant.markets import OverlayMarket, MarketDataCoordinate

        mock_inst = MagicMock(spec=PriceableImpl)
        mock_inst.calc = MagicMock()
        PriceableImpl.market(mock_inst)
        fn = mock_inst.calc.call_args[1]['fn']

        # Historical result: index.name == 'date'
        mock_result = MagicMock()
        mock_result.index.name = 'date'
        mock_result.index.__iter__ = MagicMock(return_value=iter([dt.date(2020, 1, 1)]))

        # Mock .loc[date] to return a sub-result
        properties = MarketDataCoordinate.properties()
        row_data = {p: None for p in properties}
        row_data['mkt_type'] = 'IR'
        row_data['mkt_asset'] = 'USD'
        row_data['mkt_point'] = None
        row_data['value'] = 1.0
        row_data['permissions'] = 'Granted'

        mock_row = MagicMock()
        mock_row.get = lambda p: row_data.get(p)
        mock_row.__getitem__ = lambda self, key: row_data[key]

        sub_result = MagicMock()
        sub_result.iterrows.return_value = [(0, mock_row)]
        mock_result.loc.__getitem__ = MagicMock(return_value=sub_result)

        with patch('gs_quant.priceable.PricingContext') as MockPC:
            MockPC.current.market_data_location = 'LDN'
            result = fn(mock_result)
            assert isinstance(result, dict)
            for v in result.values():
                assert isinstance(v, OverlayMarket)
