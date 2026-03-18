"""
Tests for gs_quant/backtests/action_handler.py

Covers:
- ActionHandler.__init__ and action property
- ActionHandler.apply_action is abstract (returns None from base)
- ActionHandlerBaseFactory.get_action_handler is abstract (returns None from base)
"""

import datetime as dt
from unittest.mock import MagicMock

from gs_quant.backtests.action_handler import ActionHandler, ActionHandlerBaseFactory


class ConcreteActionHandler(ActionHandler):
    """Concrete subclass that does NOT override apply_action to test the base."""
    def apply_action(self, state, backtest, trigger_info):
        return super().apply_action(state, backtest, trigger_info)


class ConcreteFactory(ActionHandlerBaseFactory):
    """Concrete subclass that does NOT override get_action_handler."""
    def get_action_handler(self, action):
        return super().get_action_handler(action)


class TestActionHandler:
    def test_init_stores_action(self):
        mock_action = MagicMock()
        handler = ConcreteActionHandler(mock_action)
        assert handler.action is mock_action

    def test_action_property(self):
        mock_action = MagicMock()
        handler = ConcreteActionHandler(mock_action)
        assert handler.action is mock_action
        assert handler._action is mock_action

    def test_apply_action_returns_none_from_base(self):
        """The base class apply_action just has 'pass', so it returns None."""
        mock_action = MagicMock()
        handler = ConcreteActionHandler(mock_action)
        result = handler.apply_action(dt.date(2023, 1, 1), MagicMock(), MagicMock())
        assert result is None

    def test_apply_action_with_iterable_state(self):
        mock_action = MagicMock()
        handler = ConcreteActionHandler(mock_action)
        states = [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]
        result = handler.apply_action(states, MagicMock(), MagicMock())
        assert result is None


class TestActionHandlerBaseFactory:
    def test_get_action_handler_returns_none_from_base(self):
        """The base factory get_action_handler just has 'pass', returns None."""
        factory = ConcreteFactory()
        result = factory.get_action_handler(MagicMock())
        assert result is None

    def test_factory_instantiation(self):
        factory = ConcreteFactory()
        assert isinstance(factory, ActionHandlerBaseFactory)
