"""
Tests for gs_quant.analytics.processors.analysis_processors
"""
import numpy as np
import pandas as pd
from unittest.mock import MagicMock

from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.analysis_processors import DiffProcessor


def _make_series(n=10, start='2020-01-01'):
    idx = pd.date_range(start, periods=n)
    return pd.Series(np.arange(1.0, n + 1.0), index=idx)


class TestDiffProcessor:
    def test_success_default_obs(self):
        proc = DiffProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        assert isinstance(result.data, pd.Series)
        # diff with obs=1: first value is NaN, rest are 1.0
        assert np.isnan(result.data.iloc[0])
        assert result.data.iloc[1] == 1.0

    def test_success_custom_obs(self):
        proc = DiffProcessor(a=MagicMock(), obs=2)
        proc.children_data['a'] = ProcessorResult(True, _make_series())
        result = proc.process()
        assert result.success is True
        # First 2 values NaN, rest are 2.0
        assert np.isnan(result.data.iloc[0])
        assert np.isnan(result.data.iloc[1])
        assert result.data.iloc[2] == 2.0

    def test_a_failure(self):
        proc = DiffProcessor(a=MagicMock())
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False
        assert 'series values' in result.data

    def test_a_not_processor_result(self):
        proc = DiffProcessor(a=MagicMock())
        proc.children_data['a'] = 'raw string'
        result = proc.process()
        assert result.success is False
        assert 'series yet' in result.data

    def test_a_not_set(self):
        proc = DiffProcessor(a=MagicMock())
        # children_data is empty
        result = proc.process()
        assert result.success is False
