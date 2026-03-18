"""
Tests for gs_quant.analytics.processors.statistics_processors
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.statistics_processors import (
    PercentilesProcessor,
    PercentileProcessor,
    MeanProcessor,
    SumProcessor,
    StdDevProcessor,
    VarianceProcessor,
    CovarianceProcessor,
    ZscoresProcessor,
    StdMoveProcessor,
    CompoundGrowthRate,
)
from gs_quant.timeseries.statistics import Window


def _make_series(n=10, start='2020-01-01'):
    idx = pd.date_range(start, periods=n)
    return pd.Series(np.arange(1.0, n + 1.0), index=idx)


# ---------------------------------------------------------------------------
# PercentilesProcessor
# ---------------------------------------------------------------------------
class TestPercentilesProcessor:
    def test_success_a_only(self):
        proc = PercentilesProcessor(a=MagicMock())
        series = _make_series()
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_success_a_and_b(self):
        proc = PercentilesProcessor(a=MagicMock(), b=MagicMock())
        series_a = _make_series()
        series_b = _make_series()
        proc.children_data['a'] = ProcessorResult(True, series_a)
        proc.children_data['b'] = ProcessorResult(True, series_b)
        result = proc.process()
        # After b success branch, code falls through and recomputes with a-only
        assert result.success is True

    def test_b_failure(self):
        proc = PercentilesProcessor(a=MagicMock(), b=MagicMock())
        series_a = _make_series()
        proc.children_data['a'] = ProcessorResult(True, series_a)
        proc.children_data['b'] = ProcessorResult(False, 'b failed')
        result = proc.process()
        # b fails -> falls through to a-only percentiles
        assert result.success is True

    def test_a_failure(self):
        proc = PercentilesProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'a failed')
        result = proc.process()
        assert result.success is False
        assert 'series values' in result.data

    def test_a_not_processor_result(self):
        proc = PercentilesProcessor(a=MagicMock())
        proc.children_data['a'] = 'not a processor result'
        result = proc.process()
        assert result.success is False
        assert 'series yet' in result.data

    def test_a_not_set(self):
        proc = PercentilesProcessor(a=MagicMock())
        # children_data is empty
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# PercentileProcessor
# ---------------------------------------------------------------------------
class TestPercentileProcessor:
    def test_success_w_none(self):
        proc = PercentileProcessor(a=MagicMock(), n=50.0)
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_w_greater_than_series_length(self):
        series = _make_series(n=5)
        proc = PercentileProcessor(a=MagicMock(), n=50.0, w=100)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True

    def test_w_within_series_length(self):
        series = _make_series(n=10)
        proc = PercentileProcessor(a=MagicMock(), n=50.0, w=3)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_result_not_series(self):
        """When percentile returns a scalar, it should be wrapped in pd.Series."""
        proc = PercentileProcessor(a=MagicMock(), n=50.0)
        # Single-element series so percentile may return scalar
        proc.children_data['a'] = ProcessorResult(True, pd.Series([5.0], index=pd.date_range('2020-01-01', periods=1)))
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_a_failure(self):
        proc = PercentileProcessor(a=MagicMock(), n=50.0)
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = PercentileProcessor(a=MagicMock(), n=50.0)
        proc.children_data['a'] = 'garbage'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# Parameterized tests for Mean / Sum / StdDev / Variance
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    'ProcessorCls',
    [MeanProcessor, SumProcessor, StdDevProcessor, VarianceProcessor],
    ids=['Mean', 'Sum', 'StdDev', 'Variance'],
)
class TestWindowedSingleInputProcessors:
    def test_success_w_none(self, ProcessorCls):
        if ProcessorCls in (StdDevProcessor, VarianceProcessor):
            proc = ProcessorCls(a=MagicMock(), w=None)
        else:
            proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_w_clamped(self, ProcessorCls):
        series = _make_series(n=5)
        if ProcessorCls in (StdDevProcessor, VarianceProcessor):
            proc = ProcessorCls(a=MagicMock(), w=100)
        else:
            proc = ProcessorCls(a=MagicMock(), w=100)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True

    def test_w_within_length(self, ProcessorCls):
        series = _make_series(n=10)
        proc = ProcessorCls(a=MagicMock(), w=3)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True

    def test_a_failure(self, ProcessorCls):
        if ProcessorCls in (StdDevProcessor, VarianceProcessor):
            proc = ProcessorCls(a=MagicMock(), w=None)
        else:
            proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self, ProcessorCls):
        if ProcessorCls in (StdDevProcessor, VarianceProcessor):
            proc = ProcessorCls(a=MagicMock(), w=None)
        else:
            proc = ProcessorCls(a=MagicMock())
        proc.children_data['a'] = 'raw string'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# CovarianceProcessor
# ---------------------------------------------------------------------------
class TestCovarianceProcessor:
    def test_both_success(self):
        proc = CovarianceProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_a_failure(self):
        proc = CovarianceProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False
        assert 'series values' in result.data

    def test_b_not_set(self):
        """When b child is not set, falls to the else branch."""
        mock_a = MagicMock()
        mock_b = MagicMock()
        proc = CovarianceProcessor(a=mock_a, b=mock_b)
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        # Remove b from children so children.get('b') is falsy
        proc.children['b'] = None
        result = proc.process()
        assert result.success is True
        assert 'not a valid series' in result.data

    def test_b_failure(self):
        proc = CovarianceProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(False, 'b fail')
        result = proc.process()
        assert result.success is True
        assert "series values" in result.data

    def test_b_not_processor_result(self):
        proc = CovarianceProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 'raw'
        result = proc.process()
        assert result.success is True  # falls to else with 'not a valid series'

    def test_a_not_processor_result(self):
        proc = CovarianceProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = 'not PR'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# ZscoresProcessor
# ---------------------------------------------------------------------------
class TestZscoresProcessor:
    def test_success_w_none(self):
        proc = ZscoresProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_success_w_set(self):
        proc = ZscoresProcessor(a=MagicMock(), w=Window(5, 0))
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_a_failure(self):
        proc = ZscoresProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = ZscoresProcessor(a=MagicMock())
        proc.children_data['a'] = 42
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# StdMoveProcessor
# ---------------------------------------------------------------------------
class TestStdMoveProcessor:
    def test_success(self):
        proc = StdMoveProcessor(a=MagicMock())
        series = _make_series(n=20)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_success_w_set(self):
        proc = StdMoveProcessor(a=MagicMock(), w=5)
        series = _make_series(n=20)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True

    def test_std_result_zero(self):
        """When std is 0 (constant series), should return failure."""
        proc = StdMoveProcessor(a=MagicMock())
        idx = pd.date_range('2020-01-01', periods=10)
        # constant series => returns will be 0 => std will be 0
        constant_series = pd.Series([100.0] * 10, index=idx)
        proc.children_data['a'] = ProcessorResult(True, constant_series)
        result = proc.process()
        assert result.success is False
        assert 'NaN' in result.data

    def test_a_failure(self):
        proc = StdMoveProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = StdMoveProcessor(a=MagicMock())
        proc.children_data['a'] = 'junk'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# CompoundGrowthRate
# ---------------------------------------------------------------------------
class TestCompoundGrowthRate:
    def test_success(self):
        proc = CompoundGrowthRate(a=MagicMock(), n=5.0)
        idx = pd.date_range('2020-01-01', periods=5)
        series = pd.Series([100.0, 110.0, 121.0, 133.1, 146.41], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)
        # (146.41/100)^(1/5) - 1 ~ 0.07936
        assert abs(result.data.iloc[0] - ((146.41 / 100) ** (1 / 5.0) - 1)) < 1e-6

    def test_a_failure(self):
        proc = CompoundGrowthRate(a=MagicMock(), n=5.0)
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = CompoundGrowthRate(a=MagicMock(), n=5.0)
        proc.children_data['a'] = 'bad'
        result = proc.process()
        assert result.success is False
