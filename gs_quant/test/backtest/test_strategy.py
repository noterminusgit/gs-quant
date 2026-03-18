"""
Tests for gs_quant/backtests/strategy.py

Covers:
- Strategy.__post_init__: initial_portfolio as list, as dict, as None
- Strategy.triggers: converted to list via make_list
- Strategy.risks: aggregated from triggers
- Strategy.get_risks: iterates triggers, handles None risks
- Strategy.get_available_engines: filters engines
"""

from unittest.mock import MagicMock, patch

from gs_quant.backtests.strategy import Strategy


class TestStrategy:
    def test_init_with_none_portfolio(self):
        s = Strategy(initial_portfolio=None, triggers=None)
        assert s.initial_portfolio == []
        assert s.triggers == []
        assert s.risks == []

    def test_init_with_dict_portfolio(self):
        """Dict portfolios should not be converted to list."""
        portfolio = {'asset1': 100, 'asset2': 200}
        s = Strategy(initial_portfolio=portfolio, triggers=None)
        assert s.initial_portfolio == portfolio
        assert isinstance(s.initial_portfolio, dict)

    def test_init_with_single_priceable(self):
        # Use a non-iterable mock (spec=int makes __iter__ unavailable)
        mock_priceable = MagicMock(spec=int)
        s = Strategy(initial_portfolio=mock_priceable, triggers=None)
        assert s.initial_portfolio == [mock_priceable]

    def test_init_with_tuple_portfolio(self):
        p1, p2 = MagicMock(), MagicMock()
        s = Strategy(initial_portfolio=(p1, p2), triggers=None)
        assert s.initial_portfolio == [p1, p2]

    def test_triggers_converted_to_list(self):
        # Trigger mock must not be iterable, so make_list wraps it in [].
        mock_trigger = MagicMock(spec=['risks'])
        mock_trigger.risks = []
        s = Strategy(initial_portfolio=None, triggers=mock_trigger)
        assert s.triggers == [mock_trigger]

    def test_triggers_list_stays_list(self):
        t1 = MagicMock(spec=['risks'])
        t1.risks = None
        t2 = MagicMock(spec=['risks'])
        t2.risks = ['risk1']
        s = Strategy(initial_portfolio=None, triggers=[t1, t2])
        assert s.triggers == [t1, t2]

    def test_get_risks_collects_from_triggers(self):
        t1 = MagicMock(spec=['risks'])
        t1.risks = ['risk_a', 'risk_b']
        t2 = MagicMock(spec=['risks'])
        t2.risks = ['risk_c']
        s = Strategy(initial_portfolio=None, triggers=[t1, t2])
        assert s.risks == ['risk_a', 'risk_b', 'risk_c']

    def test_get_risks_skips_none_risks(self):
        t1 = MagicMock(spec=['risks'])
        t1.risks = None
        t2 = MagicMock(spec=['risks'])
        t2.risks = ['risk_x']
        s = Strategy(initial_portfolio=None, triggers=[t1, t2])
        assert s.risks == ['risk_x']

    def test_get_risks_all_none(self):
        t1 = MagicMock(spec=['risks'])
        t1.risks = None
        s = Strategy(initial_portfolio=None, triggers=[t1])
        assert s.risks == []

    def test_cash_accrual_default(self):
        s = Strategy(initial_portfolio=None, triggers=None)
        assert s.cash_accrual is None

    def test_cash_accrual_set(self):
        mock_accrual = MagicMock()
        s = Strategy(initial_portfolio=None, triggers=None, cash_accrual=mock_accrual)
        assert s.cash_accrual is mock_accrual

    @patch('gs_quant.backtests.strategy._backtest_engines')
    def test_get_available_engines(self, mock_engines_fn):
        engine1 = MagicMock()
        engine1.supports_strategy.return_value = True
        engine2 = MagicMock()
        engine2.supports_strategy.return_value = False
        engine3 = MagicMock()
        engine3.supports_strategy.return_value = True
        mock_engines_fn.return_value = [engine1, engine2, engine3]

        s = Strategy(initial_portfolio=None, triggers=None)
        result = s.get_available_engines()
        assert result == [engine1, engine3]
        engine1.supports_strategy.assert_called_once_with(s)
        engine2.supports_strategy.assert_called_once_with(s)
        engine3.supports_strategy.assert_called_once_with(s)

    @patch('gs_quant.backtests.strategy._backtest_engines')
    def test_get_available_engines_none_support(self, mock_engines_fn):
        engine1 = MagicMock()
        engine1.supports_strategy.return_value = False
        mock_engines_fn.return_value = [engine1]

        s = Strategy(initial_portfolio=None, triggers=None)
        result = s.get_available_engines()
        assert result == []
