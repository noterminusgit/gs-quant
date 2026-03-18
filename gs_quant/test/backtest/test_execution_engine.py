"""
Tests for gs_quant/backtests/execution_engine.py

Covers:
- ExecutionEngine base class instantiation
- SimulatedExecutionEngine.__init__
- SimulatedExecutionEngine.submit_order: appends and sorts by execution_end_time
- SimulatedExecutionEngine.ping: no orders, orders not ready, orders filled, multiple fills
"""

import datetime as dt
from unittest.mock import MagicMock

from gs_quant.backtests.execution_engine import ExecutionEngine, SimulatedExecutionEngine
from gs_quant.backtests.event import OrderEvent, FillEvent


class TestExecutionEngine:
    def test_instantiation(self):
        engine = ExecutionEngine()
        assert isinstance(engine, ExecutionEngine)


class TestSimulatedExecutionEngine:
    def _make_engine(self):
        data_handler = MagicMock()
        return SimulatedExecutionEngine(data_handler), data_handler

    def test_init(self):
        engine, dh = self._make_engine()
        assert engine.data_handler is dh
        assert engine.orders == []

    def test_submit_order_appends(self):
        engine, _ = self._make_engine()
        order_event = MagicMock(spec=OrderEvent)
        order_event.order = MagicMock()
        order_event.order.execution_end_time.return_value = dt.datetime(2023, 6, 15, 12, 0, 0)

        engine.submit_order(order_event)
        assert len(engine.orders) == 1
        assert engine.orders[0] is order_event

    def test_submit_order_sorts_by_end_time(self):
        engine, _ = self._make_engine()

        oe1 = MagicMock(spec=OrderEvent)
        oe1.order = MagicMock()
        oe1.order.execution_end_time.return_value = dt.datetime(2023, 6, 15, 14, 0, 0)

        oe2 = MagicMock(spec=OrderEvent)
        oe2.order = MagicMock()
        oe2.order.execution_end_time.return_value = dt.datetime(2023, 6, 15, 10, 0, 0)

        engine.submit_order(oe1)
        engine.submit_order(oe2)
        # oe2 has earlier end time, should be first
        assert engine.orders[0] is oe2
        assert engine.orders[1] is oe1

    def test_ping_no_orders(self):
        engine, _ = self._make_engine()
        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))
        assert fills == []

    def test_ping_order_not_ready(self):
        """Order end_time is in the future relative to ping state."""
        engine, dh = self._make_engine()

        order_mock = MagicMock()
        order_mock.execution_end_time.return_value = dt.datetime(2023, 6, 15, 14, 0, 0)

        oe = MagicMock(spec=OrderEvent)
        oe.order = order_mock

        engine.submit_order(oe)
        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))
        assert fills == []
        assert len(engine.orders) == 1  # order still there

    def test_ping_order_filled(self):
        """Order end_time <= state triggers fill."""
        engine, dh = self._make_engine()

        order_mock = MagicMock()
        order_mock.execution_end_time.return_value = dt.datetime(2023, 6, 15, 12, 0, 0)
        order_mock.execution_price.return_value = 100.0
        order_mock.execution_quantity.return_value = 50.0

        oe = MagicMock(spec=OrderEvent)
        oe.order = order_mock

        engine.submit_order(oe)
        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))

        assert len(fills) == 1
        fill = fills[0]
        assert isinstance(fill, FillEvent)
        assert fill.order is order_mock
        assert fill.filled_price == 100.0
        assert fill.filled_units == 50.0
        assert len(engine.orders) == 0  # order consumed

    def test_ping_multiple_fills(self):
        """Multiple orders ready at ping time."""
        engine, dh = self._make_engine()

        order1 = MagicMock()
        order1.execution_end_time.return_value = dt.datetime(2023, 6, 15, 10, 0, 0)
        order1.execution_price.return_value = 90.0
        order1.execution_quantity.return_value = 10.0

        order2 = MagicMock()
        order2.execution_end_time.return_value = dt.datetime(2023, 6, 15, 11, 0, 0)
        order2.execution_price.return_value = 95.0
        order2.execution_quantity.return_value = 20.0

        oe1 = MagicMock(spec=OrderEvent)
        oe1.order = order1
        oe2 = MagicMock(spec=OrderEvent)
        oe2.order = order2

        engine.submit_order(oe1)
        engine.submit_order(oe2)

        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))
        assert len(fills) == 2
        assert fills[0].filled_price == 90.0
        assert fills[1].filled_price == 95.0
        assert len(engine.orders) == 0

    def test_ping_partial_fill(self):
        """Only first order is ready, second is still pending."""
        engine, dh = self._make_engine()

        order1 = MagicMock()
        order1.execution_end_time.return_value = dt.datetime(2023, 6, 15, 10, 0, 0)
        order1.execution_price.return_value = 90.0
        order1.execution_quantity.return_value = 10.0

        order2 = MagicMock()
        order2.execution_end_time.return_value = dt.datetime(2023, 6, 15, 15, 0, 0)
        order2.execution_price.return_value = 95.0
        order2.execution_quantity.return_value = 20.0

        oe1 = MagicMock(spec=OrderEvent)
        oe1.order = order1
        oe2 = MagicMock(spec=OrderEvent)
        oe2.order = order2

        engine.submit_order(oe1)
        engine.submit_order(oe2)

        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))
        assert len(fills) == 1
        assert fills[0].filled_price == 90.0
        assert len(engine.orders) == 1  # order2 still pending

    def test_ping_exact_end_time(self):
        """end_time == state should NOT trigger fill (end_time > state is the break condition)."""
        engine, dh = self._make_engine()

        # When end_time == state: the condition is end_time > state -> False,
        # so the else branch runs and the order IS filled.
        order_mock = MagicMock()
        order_mock.execution_end_time.return_value = dt.datetime(2023, 6, 15, 12, 0, 0)
        order_mock.execution_price.return_value = 100.0
        order_mock.execution_quantity.return_value = 50.0

        oe = MagicMock(spec=OrderEvent)
        oe.order = order_mock

        engine.submit_order(oe)
        fills = engine.ping(dt.datetime(2023, 6, 15, 12, 0, 0))
        assert len(fills) == 1  # filled because end_time is NOT > state
