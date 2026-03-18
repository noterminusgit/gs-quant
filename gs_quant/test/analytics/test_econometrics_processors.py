"""
Tests for gs_quant.analytics.processors.econometrics_processors
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from gs_quant.analytics.core.processor import DataQueryInfo
from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.econometrics_processors import (
    VolatilityProcessor,
    SharpeRatioProcessor,
    CorrelationProcessor,
    ChangeProcessor,
    ReturnsProcessor,
    BetaProcessor,
    FXImpliedCorrProcessor,
)
from gs_quant.timeseries import Window
from gs_quant.timeseries.econometrics import Returns


def _make_series(n=20, start='2020-01-01'):
    idx = pd.date_range(start, periods=n)
    return pd.Series(np.random.RandomState(42).rand(n) * 100 + 100, index=idx)


def _make_prices(n=20, start='2020-01-01'):
    idx = pd.date_range(start, periods=n)
    return pd.Series(np.cumsum(np.random.RandomState(42).rand(n)) + 100, index=idx)


# ---------------------------------------------------------------------------
# VolatilityProcessor
# ---------------------------------------------------------------------------
class TestVolatilityProcessor:
    def test_success(self):
        proc = VolatilityProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)

    def test_a_failure(self):
        proc = VolatilityProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False
        assert 'volatility' in result.data

    def test_a_not_processor_result(self):
        proc = VolatilityProcessor(a=MagicMock())
        proc.children_data['a'] = 'raw'
        result = proc.process()
        assert result.success is False
        assert 'data' in result.data


# ---------------------------------------------------------------------------
# SharpeRatioProcessor
# ---------------------------------------------------------------------------
class TestSharpeRatioProcessor:
    @patch('gs_quant.analytics.processors.econometrics_processors.excess_returns_pure')
    @patch('gs_quant.analytics.processors.econometrics_processors.get_ratio_pure')
    def test_prices_curve_type(self, mock_ratio, mock_excess):
        from gs_quant.common import Currency
        from gs_quant.timeseries.helper import CurveType

        mock_excess.return_value = _make_series()
        mock_ratio.return_value = pd.Series([1.5], index=pd.date_range('2020-01-01', periods=1))

        proc = SharpeRatioProcessor.__new__(SharpeRatioProcessor)
        # Manually set up fields since __init__ does extra query setup
        proc.children = {}
        proc.children_data = {}
        proc.children['a'] = MagicMock()
        proc.children['excess_returns'] = MagicMock()
        proc.value = ProcessorResult(False, 'Value not set')
        proc.id = 'test'
        proc.parent = None
        proc.parent_attr = None
        proc.data_cell = None
        proc.last_value = False
        proc.measure_processor = False
        proc.currency = Currency.USD
        proc.w = None
        proc.curve_type = CurveType.PRICES
        proc.start = None
        proc.end = None

        proc.children_data['a'] = ProcessorResult(True, _make_prices())
        proc.children_data['excess_returns'] = ProcessorResult(True, _make_prices())
        result = proc.process()
        assert result.success is True
        mock_excess.assert_called_once()
        mock_ratio.assert_called_once()

    @patch('gs_quant.analytics.processors.econometrics_processors.get_ratio_pure')
    def test_non_prices_curve_type(self, mock_ratio):
        from gs_quant.common import Currency
        from gs_quant.timeseries.helper import CurveType

        mock_ratio.return_value = pd.Series([2.0], index=pd.date_range('2020-01-01', periods=1))

        proc = SharpeRatioProcessor.__new__(SharpeRatioProcessor)
        proc.children = {}
        proc.children_data = {}
        proc.children['a'] = MagicMock()
        proc.children['excess_returns'] = MagicMock()
        proc.value = ProcessorResult(False, 'Value not set')
        proc.id = 'test'
        proc.parent = None
        proc.parent_attr = None
        proc.data_cell = None
        proc.last_value = False
        proc.measure_processor = False
        proc.currency = Currency.USD
        proc.w = None
        proc.curve_type = CurveType.EXCESS_RETURNS
        proc.start = None
        proc.end = None

        series = _make_series()
        proc.children_data['a'] = ProcessorResult(True, series)
        proc.children_data['excess_returns'] = ProcessorResult(True, _make_prices())
        result = proc.process()
        assert result.success is True

    def test_data_not_ready(self):
        from gs_quant.common import Currency

        proc = SharpeRatioProcessor.__new__(SharpeRatioProcessor)
        proc.children = {}
        proc.children_data = {}
        proc.value = ProcessorResult(False, 'Value not set')
        proc.id = 'test'
        proc.parent = None
        proc.parent_attr = None
        proc.data_cell = None
        proc.last_value = False
        proc.measure_processor = False
        proc.currency = Currency.USD
        proc.w = None

        # No data set
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# CorrelationProcessor
# ---------------------------------------------------------------------------
class TestCorrelationProcessor:
    def _make_proc(self):
        proc = CorrelationProcessor.__new__(CorrelationProcessor)
        proc.children = {}
        proc.children_data = {}
        proc.value = ProcessorResult(False, 'Value not set')
        proc.id = 'test'
        proc.parent = None
        proc.parent_attr = None
        proc.data_cell = None
        proc.last_value = False
        proc.measure_processor = False
        proc.children['a'] = MagicMock()
        proc.children['benchmark'] = MagicMock()
        proc.w = Window(None, 0)
        from gs_quant.timeseries import SeriesType
        proc.type_ = SeriesType.PRICES
        proc.start = None
        proc.end = None
        proc.benchmark = MagicMock()
        return proc

    def test_both_success(self):
        proc = self._make_proc()
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['benchmark'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_one_failure(self):
        proc = self._make_proc()
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['benchmark'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_not_processor_result(self):
        proc = self._make_proc()
        proc.children_data['a'] = 'raw'
        proc.children_data['benchmark'] = 'raw'
        result = proc.process()
        assert result.success is False

    def test_a_fails_b_succeeds(self):
        proc = self._make_proc()
        proc.children_data['a'] = ProcessorResult(False, 'a fail')
        proc.children_data['benchmark'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# ChangeProcessor
# ---------------------------------------------------------------------------
class TestChangeProcessor:
    def test_success(self):
        proc = ChangeProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_a_failure(self):
        proc = ChangeProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        # value is not updated for failure, returns initial value
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = ChangeProcessor(a=MagicMock())
        proc.children_data['a'] = 'junk'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# ReturnsProcessor
# ---------------------------------------------------------------------------
class TestReturnsProcessor:
    def test_observations_none_len_gt_1(self):
        proc = ReturnsProcessor(a=MagicMock())
        idx = pd.date_range('2020-01-01', periods=5)
        series = pd.Series([100.0, 110.0, 120.0, 130.0, 140.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        expected = (140.0 - 100.0) / 100.0
        assert abs(result.data.iloc[0] - expected) < 1e-9

    def test_observations_none_len_lte_1(self):
        proc = ReturnsProcessor(a=MagicMock())
        idx = pd.date_range('2020-01-01', periods=1)
        series = pd.Series([100.0], index=idx)
        proc.children_data['a'] = ProcessorResult(True, series)
        result = proc.process()
        assert result.success is True
        assert 'less than 2' in result.data

    def test_observations_set(self):
        proc = ReturnsProcessor(a=MagicMock(), observations=1)
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_a_failure(self):
        proc = ReturnsProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_a_not_processor_result(self):
        proc = ReturnsProcessor(a=MagicMock())
        proc.children_data['a'] = 'garbage'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# BetaProcessor
# ---------------------------------------------------------------------------
class TestBetaProcessor:
    def test_both_success(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True

    def test_a_failure(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_b_failure(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = ProcessorResult(False, 'b fail')
        result = proc.process()
        assert result.success is True
        assert "series values" in result.data

    def test_b_not_set(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children['b'] = None  # remove b
        result = proc.process()
        assert result.success is True
        assert 'not a valid series' in result.data

    def test_b_not_processor_result(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        proc.children_data['b'] = 'raw'
        result = proc.process()
        assert result.success is True
        assert 'not a valid series' in result.data

    def test_a_not_processor_result(self):
        proc = BetaProcessor(a=MagicMock(), b=MagicMock())
        proc.children_data['a'] = 'raw'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# FXImpliedCorrProcessor
# ---------------------------------------------------------------------------
class TestFXImpliedCorrProcessor:
    def _make_cross_mock(self):
        from gs_quant.markets.securities import Cross
        mock = MagicMock(spec=Cross)
        return mock

    @patch('gs_quant.analytics.processors.econometrics_processors.fx_implied_correlation')
    @patch('gs_quant.analytics.processors.econometrics_processors.DataContext')
    def test_both_cross(self, mock_ctx, mock_fx_corr):
        mock_fx_corr.return_value = pd.Series([0.5], index=pd.date_range('2020-01-01', periods=1))
        mock_ctx.return_value.__enter__ = MagicMock()
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

        proc = FXImpliedCorrProcessor(cross2=self._make_cross_mock(), tenor='3m')
        cross1 = self._make_cross_mock()
        result = proc.process(cross1)
        assert result.success is True
        mock_fx_corr.assert_called_once()

    def test_one_not_cross(self):
        proc = FXImpliedCorrProcessor(cross2=MagicMock(), tenor='3m')  # Not a Cross spec
        cross1 = self._make_cross_mock()
        result = proc.process(cross1)
        assert result.success is False
        assert 'valid crosses' in result.data

    def test_neither_cross(self):
        proc = FXImpliedCorrProcessor(cross2=MagicMock(), tenor='3m')
        result = proc.process(MagicMock())
        assert result.success is False

    @patch('gs_quant.analytics.processors.econometrics_processors.fx_implied_correlation')
    @patch('gs_quant.analytics.processors.econometrics_processors.DataContext')
    def test_exception(self, mock_ctx, mock_fx_corr):
        mock_fx_corr.side_effect = ValueError('test error')
        mock_ctx.return_value.__enter__ = MagicMock()
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

        proc = FXImpliedCorrProcessor(cross2=self._make_cross_mock(), tenor='3m')
        cross1 = self._make_cross_mock()
        result = proc.process(cross1)
        assert result.success is False
        assert 'test error' in result.data
