"""
Copyright 2019 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, PropertyMock

from gs_quant.backtests.core import TimeWindow, ValuationFixingType
from gs_quant.backtests.order import (
    OrderBase,
    OrderTWAP,
    OrderMarketOnClose,
    OrderCost,
    OrderAtMarket,
    OrderTwapBTIC,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_instrument(name='TEST_RIC', ric='TEST.RIC'):
    inst = MagicMock()
    inst.name = name
    inst.ric = ric
    return inst


def _make_data_handler():
    return MagicMock()


# ===========================================================================
# OrderBase
# ===========================================================================

class TestOrderBase:
    def _make_concrete_order(self):
        """OrderBase is abstract but can be instantiated for testing the base methods."""
        inst = _make_instrument()
        # Use a concrete subclass minimally or instantiate directly (ABCMeta doesn't enforce here)
        # We'll just use it directly since ABCMeta doesn't block instantiation without abstractmethod
        class ConcreteOrder(OrderBase):
            pass
        return ConcreteOrder(inst, quantity=100.0, generation_time=dt.datetime(2020, 1, 1, 10, 0), source='test')

    def test_init_attributes(self):
        inst = _make_instrument()
        order = self._make_concrete_order()
        assert order.quantity == 100.0
        assert order.generation_time == dt.datetime(2020, 1, 1, 10, 0)
        assert order.source == 'test'
        assert order.executed_price is None

    def test_execution_end_time_raises(self):
        order = self._make_concrete_order()
        with pytest.raises(RuntimeError, match='execution_end_time is not implemented'):
            order.execution_end_time()

    def test_execution_price_base_raises(self):
        order = self._make_concrete_order()
        dh = _make_data_handler()
        with pytest.raises(RuntimeError, match='execution_price is not implemented'):
            order._execution_price(dh)

    def test_execution_price_nan_raises(self):
        """execution_price wraps _execution_price and raises on NaN."""
        order = self._make_concrete_order()
        order._execution_price = MagicMock(return_value=np.nan)
        dh = _make_data_handler()
        with pytest.raises(RuntimeError, match='can not compute the execution price'):
            order.execution_price(dh)

    def test_execution_price_valid(self):
        """execution_price wraps _execution_price and returns price if not NaN."""
        order = self._make_concrete_order()
        order._execution_price = MagicMock(return_value=50.0)
        dh = _make_data_handler()
        assert order.execution_price(dh) == 50.0

    def test_execution_quantity_raises(self):
        order = self._make_concrete_order()
        with pytest.raises(RuntimeError, match='execution_quantity is not implemented'):
            order.execution_quantity()

    def test_short_name_raises(self):
        order = self._make_concrete_order()
        with pytest.raises(RuntimeError, match='_short_name is not implemented'):
            order._short_name()

    def test_execution_notional(self):
        order = self._make_concrete_order()
        order._execution_price = MagicMock(return_value=50.0)
        order.execution_quantity = MagicMock(return_value=10.0)
        dh = _make_data_handler()
        assert order.execution_notional(dh) == 500.0

    def test_to_dict(self):
        order = self._make_concrete_order()
        order._execution_price = MagicMock(return_value=25.0)
        order.execution_quantity = MagicMock(return_value=4.0)
        order._short_name = MagicMock(return_value='TestType')
        dh = _make_data_handler()
        d = order.to_dict(dh)
        assert d['Instrument'] == order.instrument.ric
        assert d['Type'] == 'TestType'
        assert d['Price'] == 25.0
        assert d['Quantity'] == 4.0


# ===========================================================================
# OrderTWAP
# ===========================================================================

class TestOrderTWAP:
    def _make_twap_order(self, window=None):
        inst = _make_instrument()
        if window is None:
            window = TimeWindow(
                start=dt.datetime(2020, 1, 1, 10, 0),
                end=dt.datetime(2020, 1, 1, 16, 0),
            )
        return OrderTWAP(
            instrument=inst,
            quantity=100.0,
            generation_time=dt.datetime(2020, 1, 1, 9, 0),
            source='test',
            window=window,
        )

    def test_execution_end_time(self):
        order = self._make_twap_order()
        assert order.execution_end_time() == dt.datetime(2020, 1, 1, 16, 0)

    def test_execution_price_first_call(self):
        """First call computes price from data_handler.get_data_range mean."""
        order = self._make_twap_order()
        dh = _make_data_handler()
        fixings = pd.Series([10.0, 20.0, 30.0])
        dh.get_data_range.return_value = fixings

        price = order._execution_price(dh)
        assert price == pytest.approx(20.0)
        dh.get_data_range.assert_called_once_with(
            order.window.start, order.window.end, order.instrument, ValuationFixingType.PRICE
        )

    def test_execution_price_cached(self):
        """Second call returns cached price, doesn't call data_handler again."""
        order = self._make_twap_order()
        order.executed_price = 42.0
        dh = _make_data_handler()

        price = order._execution_price(dh)
        assert price == 42.0
        dh.get_data_range.assert_not_called()

    def test_execution_quantity(self):
        order = self._make_twap_order()
        assert order.execution_quantity() == 100.0

    def test_short_name(self):
        order = self._make_twap_order()
        name = order._short_name()
        assert 'TWAP' in name
        assert str(order.window.start) in name
        assert str(order.window.end) in name

    def test_execution_price_via_public_method(self):
        """Test the full execution_price flow (not NaN)."""
        order = self._make_twap_order()
        dh = _make_data_handler()
        dh.get_data_range.return_value = pd.Series([50.0, 60.0])
        price = order.execution_price(dh)
        assert price == pytest.approx(55.0)

    def test_execution_price_nan_via_public_method(self):
        """Execution price is NaN -> RuntimeError."""
        order = self._make_twap_order()
        dh = _make_data_handler()
        dh.get_data_range.return_value = pd.Series([np.nan])
        with pytest.raises(RuntimeError, match='can not compute the execution price'):
            order.execution_price(dh)


# ===========================================================================
# OrderMarketOnClose
# ===========================================================================

class TestOrderMarketOnClose:
    def _make_moc_order(self, execution_date=None):
        inst = _make_instrument()
        if execution_date is None:
            execution_date = dt.date(2020, 6, 15)
        return OrderMarketOnClose(
            instrument=inst,
            quantity=200.0,
            generation_time=dt.datetime(2020, 6, 15, 9, 0),
            execution_date=execution_date,
            source='test',
        )

    def test_execution_end_time(self):
        order = self._make_moc_order(execution_date=dt.date(2020, 6, 15))
        end = order.execution_end_time()
        assert end == dt.datetime(2020, 6, 15, 23, 0, 0)

    def test_execution_price_first_call(self):
        order = self._make_moc_order()
        dh = _make_data_handler()
        dh.get_data.return_value = 350.0

        price = order._execution_price(dh)
        assert price == 350.0
        dh.get_data.assert_called_once_with(
            dt.date(2020, 6, 15), order.instrument, ValuationFixingType.PRICE
        )

    def test_execution_price_cached(self):
        order = self._make_moc_order()
        order.executed_price = 999.0
        dh = _make_data_handler()

        price = order._execution_price(dh)
        assert price == 999.0
        dh.get_data.assert_not_called()

    def test_execution_quantity(self):
        order = self._make_moc_order()
        assert order.execution_quantity() == 200.0

    def test_short_name(self):
        order = self._make_moc_order()
        assert order._short_name() == 'MOC'

    def test_execution_price_nan_raises(self):
        order = self._make_moc_order()
        dh = _make_data_handler()
        dh.get_data.return_value = np.nan
        with pytest.raises(RuntimeError, match='can not compute the execution price'):
            order.execution_price(dh)

    def test_to_dict(self):
        order = self._make_moc_order()
        dh = _make_data_handler()
        dh.get_data.return_value = 100.0
        d = order.to_dict(dh)
        assert d['Type'] == 'MOC'
        assert d['Price'] == 100.0
        assert d['Quantity'] == 200.0


# ===========================================================================
# OrderCost
# ===========================================================================

class TestOrderCost:
    def _make_cost_order(self, quantity=50.0):
        return OrderCost(
            currency='USD',
            quantity=quantity,
            source='test',
            execution_time=dt.datetime(2020, 3, 10, 15, 0),
        )

    def test_init(self):
        order = self._make_cost_order()
        assert order.execution_time == dt.datetime(2020, 3, 10, 15, 0)
        assert order.quantity == 50.0

    def test_execution_end_time(self):
        order = self._make_cost_order()
        assert order.execution_end_time() == dt.datetime(2020, 3, 10, 15, 0)

    def test_execution_price_first_call(self):
        """First call sets executed_price to 0."""
        order = self._make_cost_order()
        dh = _make_data_handler()
        price = order._execution_price(dh)
        assert price == 0.0
        assert order.executed_price == 0

    def test_execution_price_cached(self):
        order = self._make_cost_order()
        order.executed_price = 5.0
        dh = _make_data_handler()
        price = order._execution_price(dh)
        assert price == 5.0

    def test_execution_quantity(self):
        order = self._make_cost_order(quantity=75.0)
        assert order.execution_quantity() == 75.0

    def test_short_name(self):
        order = self._make_cost_order()
        assert order._short_name() == 'Cost'

    def test_to_dict_uses_currency(self):
        """OrderCost.to_dict uses instrument.currency instead of instrument.ric."""
        order = self._make_cost_order()
        dh = _make_data_handler()
        d = order.to_dict(dh)
        assert d['Instrument'] == order.instrument.currency
        assert d['Type'] == 'Cost'
        assert d['Price'] == 0.0
        assert d['Quantity'] == 50.0

    def test_execution_price_not_nan(self):
        """execution_price (public) should work fine since price is 0.0, not NaN."""
        order = self._make_cost_order()
        dh = _make_data_handler()
        price = order.execution_price(dh)
        assert price == 0.0


# ===========================================================================
# OrderAtMarket
# ===========================================================================

class TestOrderAtMarket:
    def _make_at_market_order(self, execution_datetime=None):
        inst = _make_instrument()
        if execution_datetime is None:
            execution_datetime = dt.datetime(2020, 7, 20, 14, 30)
        return OrderAtMarket(
            instrument=inst,
            quantity=500.0,
            generation_time=dt.datetime(2020, 7, 20, 14, 0),
            execution_datetime=execution_datetime,
            source='test',
        )

    def test_execution_end_time(self):
        order = self._make_at_market_order()
        assert order.execution_end_time() == dt.datetime(2020, 7, 20, 14, 30)

    def test_execution_price_first_call(self):
        order = self._make_at_market_order()
        dh = _make_data_handler()
        dh.get_data.return_value = 123.45

        price = order._execution_price(dh)
        assert price == 123.45
        dh.get_data.assert_called_once_with(
            dt.datetime(2020, 7, 20, 14, 30), order.instrument, ValuationFixingType.PRICE
        )

    def test_execution_price_cached(self):
        order = self._make_at_market_order()
        order.executed_price = 88.0
        dh = _make_data_handler()

        price = order._execution_price(dh)
        assert price == 88.0
        dh.get_data.assert_not_called()

    def test_execution_quantity(self):
        order = self._make_at_market_order()
        assert order.execution_quantity() == 500.0

    def test_short_name(self):
        order = self._make_at_market_order()
        assert order._short_name() == 'Market'

    def test_execution_price_nan_raises(self):
        order = self._make_at_market_order()
        dh = _make_data_handler()
        dh.get_data.return_value = np.nan
        with pytest.raises(RuntimeError, match='can not compute the execution price'):
            order.execution_price(dh)

    def test_to_dict(self):
        order = self._make_at_market_order()
        dh = _make_data_handler()
        dh.get_data.return_value = 200.0
        d = order.to_dict(dh)
        assert d['Instrument'] == order.instrument.ric
        assert d['Type'] == 'Market'
        assert d['Price'] == 200.0
        assert d['Quantity'] == 500.0


# ===========================================================================
# OrderTwapBTIC
# ===========================================================================

class TestOrderTwapBTIC:
    def _make_btic_order(self, window=None):
        inst = _make_instrument(name='FUTURE', ric='FUT.RIC')
        btic_inst = _make_instrument(name='BTIC', ric='BTIC.RIC')
        underlying = _make_instrument(name='UNDERLYING', ric='UND.RIC')
        if window is None:
            window = TimeWindow(
                start=dt.datetime(2020, 5, 1, 10, 0),
                end=dt.datetime(2020, 5, 1, 16, 0),
            )
        return OrderTwapBTIC(
            instrument=inst,
            quantity=300.0,
            generation_time=dt.datetime(2020, 5, 1, 9, 0),
            source='test',
            window=window,
            btic_instrument=btic_inst,
            future_underlying=underlying,
        )

    def test_init_attributes(self):
        order = self._make_btic_order()
        assert order.btic_instrument.name == 'BTIC'
        assert order.future_underlying.name == 'UNDERLYING'
        assert order.quantity == 300.0

    def test_execution_price_first_call(self):
        """Price = close + btic_twap."""
        order = self._make_btic_order()
        dh = _make_data_handler()

        btic_fixings = pd.Series([2.0, 4.0, 6.0])
        dh.get_data_range.return_value = btic_fixings
        dh.get_data.return_value = 100.0  # close price

        price = order._execution_price(dh)

        expected_btic_twap = 4.0  # mean of [2, 4, 6]
        expected_price = 100.0 + 4.0
        assert price == pytest.approx(expected_price)

        dh.get_data_range.assert_called_once_with(
            order.window.start, order.window.end, order.btic_instrument, ValuationFixingType.PRICE
        )
        dh.get_data.assert_called_once_with(
            order.window.end.date(), order.future_underlying
        )

    def test_execution_price_cached(self):
        order = self._make_btic_order()
        order.executed_price = 150.0
        dh = _make_data_handler()

        price = order._execution_price(dh)
        assert price == 150.0
        dh.get_data_range.assert_not_called()
        dh.get_data.assert_not_called()

    def test_execution_quantity(self):
        """Inherited from OrderTWAP."""
        order = self._make_btic_order()
        assert order.execution_quantity() == 300.0

    def test_short_name(self):
        order = self._make_btic_order()
        assert order._short_name() == 'TwapBTIC'

    def test_execution_end_time(self):
        """Inherited from OrderTWAP."""
        order = self._make_btic_order()
        assert order.execution_end_time() == dt.datetime(2020, 5, 1, 16, 0)

    def test_execution_price_nan_raises(self):
        order = self._make_btic_order()
        dh = _make_data_handler()
        dh.get_data_range.return_value = pd.Series([np.nan])
        dh.get_data.return_value = np.nan
        with pytest.raises(RuntimeError, match='can not compute the execution price'):
            order.execution_price(dh)

    def test_execution_notional(self):
        order = self._make_btic_order()
        dh = _make_data_handler()
        dh.get_data_range.return_value = pd.Series([10.0])
        dh.get_data.return_value = 90.0
        # price = 90 + 10 = 100, quantity = 300
        notional = order.execution_notional(dh)
        assert notional == pytest.approx(30000.0)

    def test_to_dict(self):
        order = self._make_btic_order()
        dh = _make_data_handler()
        dh.get_data_range.return_value = pd.Series([5.0])
        dh.get_data.return_value = 95.0
        d = order.to_dict(dh)
        assert d['Type'] == 'TwapBTIC'
        assert d['Price'] == pytest.approx(100.0)
        assert d['Quantity'] == 300.0
