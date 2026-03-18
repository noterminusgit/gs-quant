"""
Tests for gs_quant.analytics.processors.scale_processors
"""
import math

import pandas as pd
import pytest
from unittest.mock import MagicMock

from gs_quant.analytics.common.enumerators import ScaleShape
from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.scale_processors import (
    SpotMarkerProcessor,
    BarMarkerProcessor,
    validate_markers_data,
    ScaleProcessor,
)


# ---------------------------------------------------------------------------
# SpotMarkerProcessor
# ---------------------------------------------------------------------------
class TestSpotMarkerProcessor:
    def test_pipe_shape(self):
        child = MagicMock()
        child.name = 'spot1'
        proc = SpotMarkerProcessor(a=child, name='spot1', shape=ScaleShape.PIPE)
        data = pd.Series([42.0])
        proc.children_data['a'] = ProcessorResult(True, data)
        result = proc.process()
        assert result.success is True
        assert result.data['name'] == 'spot1'
        assert result.data['shape'] == ScaleShape.PIPE.value

    def test_diamond_shape(self):
        child = MagicMock()
        child.name = 'spot2'
        proc = SpotMarkerProcessor(a=child, name='spot2', shape=ScaleShape.DIAMOND)
        data = pd.Series([99.0])
        proc.children_data['a'] = ProcessorResult(True, data)
        result = proc.process()
        assert result.success is True
        assert result.data['shape'] == ScaleShape.DIAMOND.value

    def test_invalid_shape_raises(self):
        with pytest.raises(TypeError, match='PIPE or DIAMOND'):
            SpotMarkerProcessor(a=MagicMock(), name='bad', shape=ScaleShape.BAR)

    def test_a_failure(self):
        child = MagicMock()
        child.name = 'spot1'
        proc = SpotMarkerProcessor(a=child, name='spot1', shape=ScaleShape.PIPE)
        proc.children_data['a'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False
        assert 'pipe marker' in result.data

    def test_a_not_processor_result(self):
        child = MagicMock()
        child.name = 'spot1'
        proc = SpotMarkerProcessor(a=child, name='spot1', shape=ScaleShape.PIPE)
        proc.children_data['a'] = 'raw'
        result = proc.process()
        assert result.success is False
        assert 'data' in result.data


# ---------------------------------------------------------------------------
# BarMarkerProcessor
# ---------------------------------------------------------------------------
class TestBarMarkerProcessor:
    def test_both_success(self):
        start_proc = MagicMock()
        start_proc.name = 'bar_start'
        end_proc = MagicMock()
        end_proc.name = 'bar_end'
        proc = BarMarkerProcessor(start=start_proc, end=end_proc, name='bar1')
        proc.children_data['start'] = ProcessorResult(True, pd.Series([10.0]))
        proc.children_data['end'] = ProcessorResult(True, pd.Series([20.0]))
        result = proc.process()
        assert result.success is True
        assert result.data['name'] == 'bar1'
        assert result.data['shape'] == ScaleShape.BAR.value

    def test_one_failure(self):
        start_proc = MagicMock()
        start_proc.name = 'bar_start'
        end_proc = MagicMock()
        end_proc.name = 'bar_end'
        proc = BarMarkerProcessor(start=start_proc, end=end_proc, name='bar1')
        proc.children_data['start'] = ProcessorResult(True, pd.Series([10.0]))
        proc.children_data['end'] = ProcessorResult(False, 'fail')
        result = proc.process()
        assert result.success is False

    def test_not_processor_result(self):
        start_proc = MagicMock()
        start_proc.name = 'bar_start'
        end_proc = MagicMock()
        end_proc.name = 'bar_end'
        proc = BarMarkerProcessor(start=start_proc, end=end_proc, name='bar1')
        proc.children_data['start'] = 'raw'
        proc.children_data['end'] = 'raw'
        result = proc.process()
        assert result.success is False


# ---------------------------------------------------------------------------
# validate_markers_data
# ---------------------------------------------------------------------------
class TestValidateMarkersData:
    def test_min_nan(self):
        result = {'min': float('nan'), 'max': 100.0}
        marker_data = {'name': 'test', 'shape': ScaleShape.PIPE.value, 'value': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert 'Min Value' in reason
        assert result['min'] is None

    def test_max_nan(self):
        # min must be truthy (non-zero) so the min check passes and we reach the max check
        result = {'min': 1.0, 'max': float('nan')}
        marker_data = {'name': 'test', 'shape': ScaleShape.PIPE.value, 'value': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert 'Max Value' in reason
        assert result['max'] is None

    def test_min_zero(self):
        """min=0 is falsy, triggers the NaN check."""
        result = {'min': 0, 'max': 100.0}
        marker_data = {'name': 'test', 'shape': ScaleShape.PIPE.value, 'value': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert result['min'] is None

    def test_max_zero(self):
        """max=0 is falsy, triggers the NaN check."""
        result = {'min': 1.0, 'max': 0}
        marker_data = {'name': 'test', 'shape': ScaleShape.PIPE.value, 'value': 0.5}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert result['max'] is None

    def test_bar_valid(self):
        result = {'min': 0.0, 'max': 100.0}
        # Need min != 0 since 0 is falsy
        result['min'] = 1.0
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 10.0, 'end': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is True
        assert reason == ''

    def test_bar_start_greater_than_end(self):
        result = {'min': 1.0, 'max': 100.0}
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 60.0, 'end': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert 'starting value' in reason

    def test_bar_start_below_min(self):
        result = {'min': 10.0, 'max': 100.0}
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 5.0, 'end': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False

    def test_bar_start_above_max(self):
        result = {'min': 1.0, 'max': 50.0}
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 60.0, 'end': 70.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False

    def test_bar_end_above_max(self):
        result = {'min': 1.0, 'max': 50.0}
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 10.0, 'end': 60.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert 'ending value' in reason

    def test_bar_end_below_min(self):
        result = {'min': 10.0, 'max': 100.0}
        marker_data = {'name': 'bar1', 'shape': ScaleShape.BAR.value, 'start': 5.0, 'end': 8.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False

    def test_pipe_valid(self):
        result = {'min': 1.0, 'max': 100.0}
        marker_data = {'name': 'p1', 'shape': ScaleShape.PIPE.value, 'value': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is True

    def test_pipe_below_min(self):
        result = {'min': 10.0, 'max': 100.0}
        marker_data = {'name': 'p1', 'shape': ScaleShape.PIPE.value, 'value': 5.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert 'within range' in reason

    def test_pipe_above_max(self):
        result = {'min': 1.0, 'max': 50.0}
        marker_data = {'name': 'p1', 'shape': ScaleShape.PIPE.value, 'value': 60.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False

    def test_min_none(self):
        result = {'min': None, 'max': 100.0}
        marker_data = {'name': 'test', 'shape': ScaleShape.PIPE.value, 'value': 50.0}
        valid, reason = validate_markers_data(result, marker_data)
        assert valid is False
        assert result['min'] is None


# ---------------------------------------------------------------------------
# ScaleProcessor
# ---------------------------------------------------------------------------
class TestScaleProcessor:
    def _make_marker(self, name='m1', success=True, valid_data=True):
        marker = MagicMock()
        marker.name = name
        return marker

    def test_min_max_success_with_valid_markers(self):
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([1.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        proc.children_data['spot1'] = ProcessorResult(
            True, {'name': 'spot1', 'value': 50.0, 'shape': ScaleShape.PIPE.value}
        )
        result = proc.process()
        assert result.success is True
        assert len(result.data['markers']) == 1

    def test_min_max_success_invalid_marker(self):
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([10.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([50.0]))
        # Marker value outside range
        proc.children_data['spot1'] = ProcessorResult(
            True, {'name': 'spot1', 'value': 60.0, 'shape': ScaleShape.PIPE.value}
        )
        result = proc.process()
        assert result.success is True
        # Marker still added but with invalidReason
        assert 'invalidReason' in result.data['markers'][0]

    def test_min_max_failure(self):
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(False, 'fail')
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        result = proc.process()
        assert result.success is False

    def test_not_processor_result(self):
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = 'raw'
        proc.children_data['maximum'] = 'raw'
        result = proc.process()
        assert result.success is False

    def test_marker_not_ready(self):
        """Marker data is None (not fetched yet)."""
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([1.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        # spot1 not in children_data at all
        result = proc.process()
        assert result.success is True
        assert result.data['markers'] == []

    def test_marker_failure(self):
        """Marker has ProcessorResult with success=False."""
        marker = self._make_marker('spot1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([1.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        proc.children_data['spot1'] = ProcessorResult(False, 'marker fail')
        result = proc.process()
        assert result.success is True
        assert result.data['markers'] == []

    def test_multiple_markers(self):
        marker1 = self._make_marker('spot1')
        marker2 = self._make_marker('spot2')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker1, marker2])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([1.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        proc.children_data['spot1'] = ProcessorResult(
            True, {'name': 'spot1', 'value': 30.0, 'shape': ScaleShape.PIPE.value}
        )
        proc.children_data['spot2'] = ProcessorResult(
            True, {'name': 'spot2', 'value': 70.0, 'shape': ScaleShape.DIAMOND.value}
        )
        result = proc.process()
        assert result.success is True
        assert len(result.data['markers']) == 2

    def test_bar_marker_valid(self):
        marker = self._make_marker('bar1')
        proc = ScaleProcessor(minimum=MagicMock(), maximum=MagicMock(), markers=[marker])
        proc.children_data['minimum'] = ProcessorResult(True, pd.Series([1.0]))
        proc.children_data['maximum'] = ProcessorResult(True, pd.Series([100.0]))
        proc.children_data['bar1'] = ProcessorResult(
            True, {'name': 'bar1', 'start': 10.0, 'end': 50.0, 'shape': ScaleShape.BAR.value}
        )
        result = proc.process()
        assert result.success is True
        assert len(result.data['markers']) == 1
        assert 'invalidReason' not in result.data['markers'][0]
