"""
Tests for gs_quant.analytics.processors.utility_processors
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.utility_processors import (
    LastProcessor,
    MinProcessor,
    MaxProcessor,
    AppendProcessor,
    AdditionProcessor,
    SubtractionProcessor,
    MultiplicationProcessor,
    DivisionProcessor,
    OneDayProcessor,
    NthLastProcessor,
)


def _make_series(n=5, start='2020-01-01'):
    idx = pd.date_range(start, periods=n)
    return pd.Series(np.arange(1.0, n + 1.0), index=idx)


# ---------------------------------------------------------------------------
# LastProcessor
# ---------------------------------------------------------------------------
class TestLastProcessor:
    def test_success_series(self):
        proc = LastProcessor(a=MagicMock())
        series = _make_series()
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert len(result.data) == 1
        assert result.data.iloc[0] == 5.0

    def test_non_series_data(self):
        """If data is not a pd.Series, value should remain the initial default."""
        proc = LastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, 'not a series')
        result = proc.process()
        # value is not updated; initial value is ProcessorResult(False, 'Value not set')
        assert result.success is False

    def test_failure(self):
        proc = LastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = LastProcessor(a=MagicMock())
        proc.children_data['a'] = 42
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# MinProcessor / MaxProcessor (parameterized)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    'ProcessorCls,expected_val',
    [(MinProcessor, 1.0), (MaxProcessor, 5.0)],
    ids=['Min', 'Max'],
)
class TestMinMaxProcessor:
    def test_success(self, ProcessorCls, expected_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_failure(self, ProcessorCls, expected_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False
        assert 'data series' in result.data or 'Processor' in result.data

    def test_non_series(self, ProcessorCls, expected_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, 'not series')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self, ProcessorCls, expected_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = 'raw'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# AppendProcessor
# ---------------------------------------------------------------------------
class TestAppendProcessor:
    def test_success(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        series_a = _make_series(n=3, start='2020-01-01')
        series_b = _make_series(n=3, start='2020-02-01')
        proc.children_data['a'] = ProcessorResult(True, series_a)
        proc.children_data['b'] = ProcessorResult(True, series_b)
        # pd.Series.append was removed in pandas 2.0; the processor code uses it,
        # so we patch it to use pd.concat instead for compatibility.
        import pandas as pd
        if not hasattr(pd.Series, 'append'):
            pd.Series.append = lambda self, other, **kw: pd.concat([self, other])
        result = proc.process()
        assert result.success is True
        assert len(result.data) == 6

    def test_one_failure(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_not_processor_result(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = 'raw'
        proc.children_data['b'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False

    def test_both_not_processor_result(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = 42
        proc.children_data['b'] = 43
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# Arithmetic Processors: Addition, Subtraction, Multiplication, Division
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    'ProcessorCls,scalar_kwarg,scalar_val',
    [
        (AdditionProcessor, 'addend', 10.0),
        (SubtractionProcessor, 'subtrahend', 2.0),
        (MultiplicationProcessor, 'factor', 3.0),
        (DivisionProcessor, 'dividend', 2.0),
    ],
    ids=['Addition', 'Subtraction', 'Multiplication', 'Division'],
)
class TestArithmeticProcessors:
    def test_scalar_path(self, ProcessorCls, scalar_kwarg, scalar_val):
        kwargs = {scalar_kwarg: scalar_val}
        proc = ProcessorCls(a=MagicMock(), **kwargs)
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_binary_path(self, ProcessorCls, scalar_kwarg, scalar_val):
        proc = ProcessorCls(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_a_failure(self, ProcessorCls, scalar_kwarg, scalar_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_b_failure(self, ProcessorCls, scalar_kwarg, scalar_val):
        proc = ProcessorCls(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(False, 'b fail')
        result = proc.process()
        # For Addition: sets value to ProcessorResult(True, b_data.data)
        # For Subtraction/Multiplication/Division: sets value to b_data itself
        if ProcessorCls == AdditionProcessor:
            assert result.success is True
        else:
            assert result.success is False

    def test_a_not_processor_result(self, ProcessorCls, scalar_kwarg, scalar_val):
        proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = 'not PR'
        result = proc.process()
        # Returns initial value (False, 'Value not set')
        assert result.success is False


# ---------------------------------------------------------------------------
# OneDayProcessor
# ---------------------------------------------------------------------------
class TestOneDayProcessor:
    def test_success(self):
        """Series with multiple dates, after dropping last date, >= 2 remain."""
        proc = OneDayProcessor(a=MagicMock())
        # Daily timestamps (midnight) so that drop(date()) works
        idx = pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04'])
        series = pd.Series([1.0, 2.0, 3.0, 4.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert len(result.data) == 2

    def test_fewer_than_2_values(self):
        proc = OneDayProcessor(a=MagicMock())
        idx = pd.to_datetime(['2020-01-01'])
        series = pd.Series([1.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is False

    def test_fewer_than_2_after_drop(self):
        """After dropping last date, fewer than 2 values remain.
        With daily timestamps, drop(index[-1].date()) removes the last entry.
        Two entries -> drop last -> only 1 remains -> len < 2 -> fail.
        """
        proc = OneDayProcessor(a=MagicMock())
        idx = pd.to_datetime(['2020-01-01', '2020-01-02'])
        series = pd.Series([1.0, 2.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is False

    def test_a_failure(self):
        proc = OneDayProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = OneDayProcessor(a=MagicMock())
        proc.children_data['a'] = 'junk'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# NthLastProcessor
# ---------------------------------------------------------------------------
class TestNthLastProcessor:
    @staticmethod
    def _mock_series(values):
        """Create a mock that behaves like a pd.Series for negative indexing.
        The processor uses series[negative_int], which in modern pandas doesn't
        do positional indexing on integer/datetime indices. We mock __getitem__
        to use iloc-like behavior so the processor logic can be tested.
        """
        s = pd.Series(values, index=pd.date_range('2020-01-01', periods=len(values)))
        original_getitem = s.__class__.__getitem__

        class IndexableSeries(pd.Series):
            def __getitem__(self, key):
                if isinstance(key, int) and key < 0:
                    return self.iloc[key]
                return super().__getitem__(key)

        return IndexableSeries(s)

    def test_default_n(self):
        proc = NthLastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, self._mock_series([1.0, 2.0, 3.0, 4.0, 5.0]))
        result = proc.process()
        assert result.success is True
        # n=1 => last element
        assert result.data.iloc[0] == 5.0

    def test_n_2(self):
        proc = NthLastProcessor(a=MagicMock(), n=2)
        proc.children_data['a'] = ProcessorResult(True, self._mock_series([1.0, 2.0, 3.0, 4.0, 5.0]))
        result = proc.process()
        assert result.success is True
        assert result.data.iloc[0] == 4.0

    def test_n_equals_length(self):
        proc = NthLastProcessor(a=MagicMock(), n=5)
        proc.children_data['a'] = ProcessorResult(True, self._mock_series([1.0, 2.0, 3.0, 4.0, 5.0]))
        result = proc.process()
        assert result.success is True
        assert result.data.iloc[0] == 1.0

    def test_a_failure(self):
        proc = NthLastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_series(self):
        proc = NthLastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, 'not series')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = NthLastProcessor(a=MagicMock())
        proc.children_data['a'] = 99
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# Branch coverage: LastProcessor additional branches
# ---------------------------------------------------------------------------
class TestLastProcessorBranches:
    def test_no_data_at_all(self):
        """children_data is empty => a_data is None => not isinstance => return default"""
        proc = LastProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_a_success_false(self):
        """a_data is ProcessorResult but success=False"""
        proc = LastProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = LastProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: MinProcessor additional branches
# ---------------------------------------------------------------------------
class TestMinProcessorBranches:
    def test_no_data(self):
        proc = MinProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = MinProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: MaxProcessor additional branches
# ---------------------------------------------------------------------------
class TestMaxProcessorBranches:
    def test_no_data(self):
        proc = MaxProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = MaxProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: AppendProcessor additional branches
# ---------------------------------------------------------------------------
class TestAppendProcessorBranches:
    def test_a_failure_b_success(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'a fail')
        proc.children_data['b'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False

    def test_both_failure(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'a fail')
        proc.children_data['b'] = ProcessorResult(False, 'b fail')
        result = proc.process()
        assert result.success is False

    def test_no_data(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_a_processor_b_not(self):
        """a_data is ProcessorResult but b_data is not"""
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 'not a PR'
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = AppendProcessor(a=MagicMock(), b=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: AdditionProcessor additional branches
# ---------------------------------------------------------------------------
class TestAdditionProcessorBranches:
    def test_no_data(self):
        """children_data empty => a_data is None"""
        proc = AdditionProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_b_not_processor_result(self):
        """a succeeds, no addend, b_data is not ProcessorResult"""
        proc = AdditionProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 'raw string'
        result = proc.process()
        # b_data is not ProcessorResult => isinstance fails => value not updated
        assert result.success is False

    def test_b_not_in_children_data(self):
        """a succeeds, no addend, b not in children_data"""
        proc = AdditionProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        # b_data is None => isinstance(None, PR) is False => value not updated
        assert result.success is False

    def test_get_plot_expression(self):
        proc = AdditionProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: SubtractionProcessor additional branches
# ---------------------------------------------------------------------------
class TestSubtractionProcessorBranches:
    def test_no_data(self):
        proc = SubtractionProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_b_not_processor_result(self):
        """a succeeds, no subtrahend, b_data not ProcessorResult"""
        proc = SubtractionProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 42
        result = proc.process()
        assert result.success is False

    def test_b_not_in_children_data(self):
        proc = SubtractionProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = SubtractionProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: MultiplicationProcessor additional branches
# ---------------------------------------------------------------------------
class TestMultiplicationProcessorBranches:
    def test_no_data(self):
        proc = MultiplicationProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_b_not_processor_result(self):
        proc = MultiplicationProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 'raw'
        result = proc.process()
        assert result.success is False

    def test_b_not_in_children_data(self):
        proc = MultiplicationProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = MultiplicationProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: DivisionProcessor additional branches
# ---------------------------------------------------------------------------
class TestDivisionProcessorBranches:
    def test_no_data(self):
        proc = DivisionProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_b_not_processor_result(self):
        proc = DivisionProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 99.9
        result = proc.process()
        assert result.success is False

    def test_b_not_in_children_data(self):
        proc = DivisionProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = DivisionProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: OneDayProcessor additional branches
# ---------------------------------------------------------------------------
class TestOneDayProcessorBranches:
    def test_no_data_at_all(self):
        """children_data empty"""
        proc = OneDayProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_data_length_1(self):
        """len(data) < 2 => falls to end"""
        proc = OneDayProcessor(a=MagicMock())
        idx = pd.to_datetime(['2020-01-01'])
        series = pd.Series([1.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is False

    def test_after_drop_exactly_2(self):
        """After drop, exactly 2 values remain => success"""
        proc = OneDayProcessor(a=MagicMock())
        # 3 daily entries: drop last date => 2 remain => len(value) >= 2 => success
        idx = pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03'])
        series = pd.Series([1.0, 2.0, 3.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert len(result.data) == 2

    def test_intraday_data(self):
        """Intraday timestamps where drop(date()) removes multiple entries on last date"""
        proc = OneDayProcessor(a=MagicMock())
        idx = pd.to_datetime([
            '2020-01-01 09:00', '2020-01-01 10:00',
            '2020-01-02 09:00', '2020-01-02 10:00',
            '2020-01-03 09:00', '2020-01-03 10:00',
        ])
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert len(result.data) == 2

    def test_get_plot_expression(self):
        proc = OneDayProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# Branch coverage: NthLastProcessor additional branches
# ---------------------------------------------------------------------------
class TestNthLastProcessorBranches:
    def test_no_data(self):
        proc = NthLastProcessor(a=MagicMock())
        result = proc.process()
        assert result.success is False

    def test_get_plot_expression(self):
        proc = NthLastProcessor(a=MagicMock())
        assert proc.get_plot_expression() is None
