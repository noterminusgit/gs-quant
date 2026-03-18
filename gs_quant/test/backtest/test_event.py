"""
Tests for gs_quant/backtests/event.py

Covers:
- Event base class
- MarketEvent: type attribute
- ValuationEvent: type attribute
- OrderEvent: type, order attributes
- FillEvent: type, order, filled_units, filled_price attributes
"""

from unittest.mock import MagicMock

from gs_quant.backtests.event import Event, MarketEvent, ValuationEvent, OrderEvent, FillEvent


class TestEvent:
    def test_base_event_instantiation(self):
        e = Event()
        assert isinstance(e, Event)


class TestMarketEvent:
    def test_type(self):
        me = MarketEvent()
        assert me.type == 'Market'

    def test_is_event(self):
        me = MarketEvent()
        assert isinstance(me, Event)


class TestValuationEvent:
    def test_type(self):
        ve = ValuationEvent()
        assert ve.type == 'Valuation'

    def test_is_event(self):
        ve = ValuationEvent()
        assert isinstance(ve, Event)


class TestOrderEvent:
    def test_init(self):
        order = MagicMock()
        oe = OrderEvent(order=order)
        assert oe.type == 'Order'
        assert oe.order is order

    def test_is_event(self):
        oe = OrderEvent(order=MagicMock())
        assert isinstance(oe, Event)


class TestFillEvent:
    def test_init(self):
        order = MagicMock()
        fe = FillEvent(order=order, filled_units=100.0, filled_price=50.5)
        assert fe.type == 'Fill'
        assert fe.order is order
        assert fe.filled_units == 100.0
        assert fe.filled_price == 50.5

    def test_is_event(self):
        fe = FillEvent(order=MagicMock(), filled_units=0, filled_price=0)
        assert isinstance(fe, Event)

    def test_zero_values(self):
        order = MagicMock()
        fe = FillEvent(order=order, filled_units=0.0, filled_price=0.0)
        assert fe.filled_units == 0.0
        assert fe.filled_price == 0.0

    def test_negative_values(self):
        """Negative units/price for short positions."""
        order = MagicMock()
        fe = FillEvent(order=order, filled_units=-50.0, filled_price=100.0)
        assert fe.filled_units == -50.0
        assert fe.filled_price == 100.0
