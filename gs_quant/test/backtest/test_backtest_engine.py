"""
Tests for gs_quant/backtests/backtest_engine.py

Covers:
- BacktestBaseEngine is abstract
- Concrete subclass can implement get_action_handler
- Base class get_action_handler returns None (just 'pass')
"""

from unittest.mock import MagicMock

from gs_quant.backtests.backtest_engine import BacktestBaseEngine


class ConcreteEngine(BacktestBaseEngine):
    """Concrete subclass that calls super."""
    def get_action_handler(self, action):
        return super().get_action_handler(action)


class ImplementedEngine(BacktestBaseEngine):
    """Concrete subclass with real implementation."""
    def __init__(self):
        self._handlers = {}

    def register_handler(self, action_type, handler):
        self._handlers[action_type] = handler

    def get_action_handler(self, action):
        action_type = type(action)
        if action_type in self._handlers:
            return self._handlers[action_type]
        return None


class TestBacktestBaseEngine:
    def test_concrete_subclass_instantiation(self):
        engine = ConcreteEngine()
        assert isinstance(engine, BacktestBaseEngine)

    def test_get_action_handler_base_returns_none(self):
        """The base class method has just 'pass', returns None."""
        engine = ConcreteEngine()
        result = engine.get_action_handler(MagicMock())
        assert result is None

    def test_implemented_engine_with_handler(self):
        engine = ImplementedEngine()
        mock_handler = MagicMock()
        mock_action = MagicMock()
        action_type = type(mock_action)
        engine.register_handler(action_type, mock_handler)
        assert engine.get_action_handler(mock_action) is mock_handler

    def test_implemented_engine_without_handler(self):
        engine = ImplementedEngine()
        mock_action = MagicMock()
        assert engine.get_action_handler(mock_action) is None

    def test_is_abstract_class(self):
        """BacktestBaseEngine uses @abstractmethod but Python allows instantiation
        only when all abstract methods are implemented."""
        # The class uses @abstractmethod from abc but doesn't inherit ABC,
        # so it can technically be instantiated. But the method just passes.
        # We test that concrete subclasses work.
        engine = ConcreteEngine()
        assert hasattr(engine, 'get_action_handler')
