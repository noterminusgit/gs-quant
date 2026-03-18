"""
Tests for gs_quant/analytics/core/processor_result.py
Covers ProcessorResult dataclass with all data type variants.
"""

import pandas as pd
import pytest

from gs_quant.analytics.core.processor_result import ProcessorResult


class TestProcessorResult:
    def test_success_with_string(self):
        result = ProcessorResult(True, 'some data')
        assert result.success is True
        assert result.data == 'some data'

    def test_failure_with_string(self):
        result = ProcessorResult(False, 'Error message')
        assert result.success is False
        assert result.data == 'Error message'

    def test_success_with_series(self):
        series = pd.Series([1.0, 2.0, 3.0])
        result = ProcessorResult(True, series)
        assert result.success is True
        pd.testing.assert_series_equal(result.data, series)

    def test_success_with_empty_series(self):
        series = pd.Series(dtype=float)
        result = ProcessorResult(True, series)
        assert result.success is True
        assert result.data.empty

    def test_success_with_dict(self):
        data = {'key': 'value', 'num': 42}
        result = ProcessorResult(True, data)
        assert result.success is True
        assert result.data == data

    def test_empty_dict(self):
        result = ProcessorResult(False, {})
        assert result.success is False
        assert result.data == {}

    def test_equality(self):
        r1 = ProcessorResult(True, 'data')
        r2 = ProcessorResult(True, 'data')
        assert r1 == r2

    def test_inequality(self):
        r1 = ProcessorResult(True, 'data')
        r2 = ProcessorResult(False, 'data')
        assert r1 != r2
