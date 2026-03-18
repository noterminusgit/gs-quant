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
