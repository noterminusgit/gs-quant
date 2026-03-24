"""
Branch coverage tests for gs_quant/markets/factor_analytics.py
Targets missing branches at lines 143, 374, 376.
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import plotly.graph_objects as go
import pytest

from gs_quant.errors import MqValueError
from gs_quant.markets.factor_analytics import FactorAnalytics


class TestConvertHedgeFactorExposuresLine143:
    """
    Line 143: `if not style_factors:` (second check, after the raise on line 140-141).

    The first `if not style_factors:` at line 140 raises MqValueError.
    The second `if not style_factors:` at line 143 is dead code because if
    style_factors is falsy, it would have already raised at line 140.
    This is unreachable, but we verify the first check works.
    """

    def test_empty_style_factors_raises(self):
        """Empty style_factors raises MqValueError at line 140."""
        fa = FactorAnalytics('TEST_MODEL')
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            fa.convert_hedge_factor_exposures([])

    def test_none_style_factors_raises(self):
        """None style_factors raises MqValueError at line 140."""
        fa = FactorAnalytics('TEST_MODEL')
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            fa.convert_hedge_factor_exposures(None)

    def test_valid_style_factors(self):
        """Valid style_factors produces correct output (skips both if-not checks)."""
        fa = FactorAnalytics('TEST_MODEL')
        result = fa.convert_hedge_factor_exposures([
            {'factor': 'Momentum', 'exposure': 1.5},
            {'factor': 'Value', 'exposure': -0.8},
        ])
        assert 'factorExposureBuckets' in result
        assert result['factorExposureBuckets'][0]['name'] == 'Style'
        assert len(result['factorExposureBuckets'][0]['subFactors']) == 2


class TestCreateDynamicPerformanceChartLine374:
    """
    Line 374: `if not cumulative_dates and cumulative_values:`
    This branch is True when cumulative_pnl_raw has items but none pass the
    `len(item) == 2 and isinstance(item[0], str)` filter (so cumulative_dates
    is empty) BUT cumulative_values is non-empty. Wait - that can't happen since
    both lists are built together. Actually, cumulative_dates and cumulative_values
    are built from the SAME loop - if an item passes the filter, BOTH get appended.
    So if cumulative_dates is empty, cumulative_values is also empty.

    The only way to get `not cumulative_dates and cumulative_values` is if
    the raw data items don't have string dates but somehow still have values.
    But both are appended in the same if-block... Actually wait, they ARE built
    from the same loop, so they will always have the same length.

    Hmm, but the condition is: `not cumulative_dates and cumulative_values`.
    Since both lists are populated together, this can never be True.
    This is dead code. But let's test the surrounding conditions to be thorough.

    Actually, wait - I need to re-read. The data format is:
    cumulative_pnl_raw = [item, item, ...]
    For each item, if len(item)==2 and isinstance(item[0], str):
        cumulative_dates.append(item[0])
        cumulative_values.append(item[1])

    So both lists always have same length. `not cumulative_dates and cumulative_values`
    would require len(cumulative_dates)==0 and len(cumulative_values)>0, which is
    impossible since they're always the same length.

    However, line 374 and 376 are about fallback for non-string dates. The intent
    seems to be: if dates are missing but values exist, use numeric indices.
    This could happen if the raw data format changes.

    Let me test it by providing data where item[0] is NOT a string (e.g., an int)
    so dates don't get added but values might... but wait, if the filter fails,
    NEITHER gets added. So both will be empty.

    These are truly dead branches. Let's test around them and verify the normal paths.
    """

    def test_cumulative_pnl_with_valid_dates(self):
        """Normal case: cumulative_dates and values both populated from string dates."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [
                        ['2024-01-01', 100],
                        ['2024-01-02', 200],
                    ],
                    'normalizedPerformance': [],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)
        # Should have at least one trace
        assert len(fig.data) >= 1

    def test_normalized_perf_with_valid_dates(self):
        """Normalized performance with valid string dates."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [],
                    'normalizedPerformance': [
                        ['2024-01-01', 1.0],
                        ['2024-01-02', 1.02],
                    ],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_non_string_dates_produce_empty_lists(self):
        """When items don't have string first elements, dates/values are both empty."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [
                        [123, 100],  # non-string date -> filtered out
                        [456, 200],
                    ],
                    'normalizedPerformance': [
                        [789, 1.0],
                    ],
                }
            ]
        }
        # Since both dates and values will be empty, the function should return
        # a figure with "no data" annotation
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)

    def test_single_item_list_filtered_out(self):
        """Items with len != 2 are filtered out."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [
                        ['2024-01-01'],  # len=1, filtered out
                    ],
                    'normalizedPerformance': [
                        ['2024-01-01', 1.0, 'extra'],  # len=3, filtered out
                    ],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)

    def test_both_cumulative_and_normalized(self):
        """Both cumulative PnL and normalized performance present."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [
                        ['2024-01-01', 100],
                        ['2024-01-02', 200],
                    ],
                    'normalizedPerformance': [
                        ['2024-01-01', 1.0],
                        ['2024-01-02', 1.02],
                    ],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)
        # Should have two traces
        assert len(fig.data) == 2

    def test_no_timeseries_data(self):
        """Empty timeseriesData returns annotation figure."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {'timeseriesData': []}
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)

    def test_no_cumulative_or_normalized(self):
        """Neither cumulative nor normalized data present."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [],
                    'normalizedPerformance': [],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)


class TestCreateDynamicPerformanceChartLine376:
    """
    Line 376: `if not normalized_dates and normalized_values:`
    Same dead code analysis as line 374 - both lists are built together.
    But we can force the branch to be taken by monkey-patching or by
    understanding that this is dead code that needs pragma.

    Actually - looking again at the code flow, we could potentially get
    cumulative_values populated but cumulative_dates empty if we modify
    the list after the loop but before the check. But in normal code,
    this doesn't happen.

    Let's add a test that exercises the function with both data types
    to maximize coverage of surrounding code.
    """

    def test_mixed_valid_and_invalid_items(self):
        """Mix of valid and invalid items in raw data."""
        fa = FactorAnalytics('TEST_MODEL')
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [
                        ['2024-01-01', 100],
                        [123, 200],  # invalid - no string date
                        ['2024-01-03', 300],
                    ],
                    'normalizedPerformance': [
                        ['2024-01-01', 1.0],
                        [None, 1.01],  # invalid
                    ],
                }
            ]
        }
        fig = fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)
        # Should have traces for the valid items
        assert len(fig.data) >= 1
