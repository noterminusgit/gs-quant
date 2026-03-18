"""
Tests for gs_quant/backtests/decorator.py

Covers:
- plot_backtest decorator: sets _plot_backtest attribute on the decorated object
- Decorated functions remain callable
- Undecorated functions do not have _plot_backtest attribute
"""

from gs_quant.backtests.decorator import plot_backtest


class TestPlotBacktest:
    def test_decorator_sets_attribute(self):
        @plot_backtest()
        def my_func():
            return 42

        assert hasattr(my_func, '_plot_backtest')
        assert my_func._plot_backtest is True

    def test_decorated_function_still_callable(self):
        @plot_backtest()
        def my_func(x):
            return x * 2

        assert my_func(5) == 10

    def test_undecorated_function_no_attribute(self):
        def my_func():
            return 42

        assert not hasattr(my_func, '_plot_backtest')

    def test_decorator_on_method(self):
        class MyClass:
            @plot_backtest()
            def my_method(self):
                return 'hello'

        obj = MyClass()
        assert obj.my_method() == 'hello'
        assert hasattr(MyClass.my_method, '_plot_backtest')
        assert MyClass.my_method._plot_backtest is True

    def test_decorator_preserves_return_value(self):
        @plot_backtest()
        def returns_dict():
            return {'a': 1, 'b': 2}

        assert returns_dict() == {'a': 1, 'b': 2}

    def test_decorator_on_class(self):
        """plot_backtest() can decorate any object, including classes."""
        @plot_backtest()
        class MyPlottable:
            pass

        assert hasattr(MyPlottable, '_plot_backtest')
        assert MyPlottable._plot_backtest is True

    def test_multiple_decorations(self):
        """Applying the decorator multiple times still works."""
        @plot_backtest()
        @plot_backtest()
        def my_func():
            return 99

        assert my_func._plot_backtest is True
        assert my_func() == 99
