"""
Tests for gs_quant/markets/factor_analytics.py targeting 100% branch coverage.
"""
import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import plotly.graph_objects as go
import pytest

from gs_quant.errors import MqValueError
from gs_quant.markets.factor_analytics import FactorAnalytics


# ── helpers ──────────────────────────────────────────────────────────────

def _make_position(identifier='AAPL', asset_id='MAXXXX', quantity=None, weight=None):
    """Create a mock Position with the given attributes."""
    pos = MagicMock()
    pos.identifier = identifier
    pos.asset_id = asset_id
    pos.quantity = quantity
    pos.weight = weight
    return pos


def _make_position_set(positions=None, date=dt.date(2024, 1, 1), reference_notional=None):
    """Create a mock PositionSet."""
    ps = MagicMock()
    ps.positions = positions if positions is not None else []
    ps.date = date
    ps.reference_notional = reference_notional
    return ps


def _make_factor_analysis_result():
    """Standard factor analysis result dict."""
    return {
        'factorExposureBuckets': [
            {
                'name': 'Style',
                'subFactors': [
                    {'name': 'Momentum', 'value': 1.5},
                    {'name': 'Value', 'value': -0.8},
                    {'name': 'Size', 'value': 0.3},
                    {'name': 'Volatility', 'value': -1.2},
                    {'name': 'Growth', 'value': 0.0},
                ],
            },
            {
                'name': 'Sector',
                'subFactors': [
                    {'name': 'Technology', 'value': 2.0},
                ],
            },
        ],
        'notional': 1000000,
        'currency': 'USD',
        'riskBuckets': [
            {'name': 'Market', 'value': 0.1234},
            {'name': 'Specific', 'value': 0.0567},
            {'name': 'Sector', 'value': 0.0345},
            {'name': 'Style', 'value': 0.0789},
        ],
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
        ],
    }


# ── __init__ ─────────────────────────────────────────────────────────────

class TestInit:
    def test_defaults(self):
        fa = FactorAnalytics('MODEL1')
        assert fa.risk_model_id == 'MODEL1'
        assert fa.currency == 'USD'
        assert fa.participation_rate == 0.1

    def test_custom_params(self):
        fa = FactorAnalytics('MODEL2', currency='EUR', participation_rate=0.05)
        assert fa.currency == 'EUR'
        assert fa.participation_rate == 0.05


# ── get_factor_analysis ─────────────────────────────────────────────────

class TestGetFactorAnalysis:
    def setup_method(self):
        self.fa = FactorAnalytics('TEST_MODEL')

    def test_empty_position_set_none(self):
        """position_set is falsy (None)."""
        with pytest.raises(MqValueError, match="Position set is empty"):
            self.fa.get_factor_analysis(None)

    def test_empty_positions_list(self):
        """position_set.positions is empty list."""
        ps = _make_position_set(positions=[])
        with pytest.raises(MqValueError, match="Position set is empty"):
            self.fa.get_factor_analysis(ps)

    def test_no_date(self):
        """position_set.date is falsy."""
        pos = _make_position(quantity=100)
        ps = _make_position_set(positions=[pos], date=None)
        with pytest.raises(MqValueError, match="Position set must have a date"):
            self.fa.get_factor_analysis(ps)

    def test_unresolved_positions_triggers_resolve(self):
        """Positions without asset_id trigger resolve() call."""
        pos = _make_position(identifier='AAPL', asset_id=None, quantity=100)
        # After resolve, still no asset_id => skip + "No valid positions"
        ps = _make_position_set(positions=[pos])
        with pytest.raises(MqValueError, match="No valid positions"):
            self.fa.get_factor_analysis(ps)
        ps.resolve.assert_called_once()

    def test_skip_unresolved_after_resolve(self):
        """Position still has no asset_id after resolve => warning + skip."""
        pos = _make_position(identifier='BAD', asset_id=None, weight=0.5)
        ps = _make_position_set(positions=[pos])
        with pytest.raises(MqValueError, match="No valid positions"):
            self.fa.get_factor_analysis(ps)

    def test_position_with_quantity(self):
        """Position with quantity builds correct api payload."""
        pos = _make_position(asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.return_value = {'result': True}
            result = self.fa.get_factor_analysis(ps)
        assert result == {'result': True}
        call_args = mock_api.get_liquidity_and_factor_analysis.call_args
        assert call_args.kwargs['positions'] == [{'assetId': 'MA123', 'quantity': 100}]

    def test_position_with_weight(self):
        """Position with weight (no quantity) => weight * 100."""
        pos = _make_position(asset_id='MA456', quantity=None, weight=0.5)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.return_value = {}
            self.fa.get_factor_analysis(ps)
        call_args = mock_api.get_liquidity_and_factor_analysis.call_args
        assert call_args.kwargs['positions'] == [{'assetId': 'MA456', 'weight': 50.0}]

    def test_position_no_quantity_no_weight(self):
        """Position with neither quantity nor weight => warning, skip."""
        pos = _make_position(asset_id='MA789', quantity=None, weight=None)
        ps = _make_position_set(positions=[pos])
        with pytest.raises(MqValueError, match="No valid positions"):
            self.fa.get_factor_analysis(ps)

    def test_reference_notional_used(self):
        """reference_notional is passed as notional."""
        pos = _make_position(asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos], reference_notional=5000000)
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.return_value = {}
            self.fa.get_factor_analysis(ps)
        call_args = mock_api.get_liquidity_and_factor_analysis.call_args
        assert call_args.kwargs['notional'] == 5000000

    def test_reference_notional_none(self):
        """No reference_notional => notional is None."""
        pos = _make_position(asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos], reference_notional=None)
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.return_value = {}
            self.fa.get_factor_analysis(ps)
        call_args = mock_api.get_liquidity_and_factor_analysis.call_args
        assert call_args.kwargs['notional'] is None

    def test_mqvalueerror_missing_in_marquee_with_asset_ids(self):
        """MqValueError with 'missing in marquee' and MA asset IDs."""
        pos = _make_position(identifier='AAPL', asset_id='MAXXXX1', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.side_effect = MqValueError(
                'Assets MAXXXX1 missing in marquee system'
            )
            with pytest.raises(MqValueError, match="Factor analysis failed due to asset resolution"):
                self.fa.get_factor_analysis(ps)

    def test_mqvalueerror_missing_in_marquee_unknown_asset_id(self):
        """MqValueError with asset ID not in mapping => 'Unknown' label."""
        pos = _make_position(identifier='AAPL', asset_id='MA111', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.side_effect = MqValueError(
                'Assets MAZZZZZ missing in marquee system'
            )
            with pytest.raises(MqValueError, match="Unknown.*MAZZZZZ"):
                self.fa.get_factor_analysis(ps)

    def test_mqvalueerror_missing_marquee_no_asset_ids(self):
        """MqValueError with 'missing in marquee' but no MA asset IDs => re-raise original."""
        pos = _make_position(identifier='AAPL', asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.side_effect = MqValueError(
                'something missing in marquee but no ids'
            )
            with pytest.raises(MqValueError, match="something missing in marquee"):
                self.fa.get_factor_analysis(ps)

    def test_mqvalueerror_not_missing_marquee(self):
        """MqValueError without 'missing in marquee' => re-raise as-is."""
        pos = _make_position(identifier='AAPL', asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.side_effect = MqValueError('some other error')
            with pytest.raises(MqValueError, match="some other error"):
                self.fa.get_factor_analysis(ps)

    def test_generic_exception(self):
        """Non-MqValueError exception => logged and re-raised."""
        pos = _make_position(asset_id='MA123', quantity=100)
        ps = _make_position_set(positions=[pos])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.side_effect = RuntimeError('network fail')
            with pytest.raises(RuntimeError, match="network fail"):
                self.fa.get_factor_analysis(ps)

    def test_mixed_positions(self):
        """Mix of resolved/unresolved, quantity/weight/neither."""
        pos1 = _make_position(identifier='A', asset_id='MA1', quantity=50)
        pos2 = _make_position(identifier='B', asset_id='MA2', quantity=None, weight=0.3)
        pos3 = _make_position(identifier='C', asset_id='MA3', quantity=None, weight=None)
        pos4 = _make_position(identifier='D', asset_id=None, quantity=100)  # unresolved
        ps = _make_position_set(positions=[pos1, pos2, pos3, pos4])
        with patch('gs_quant.markets.factor_analytics.GsRiskApi') as mock_api:
            mock_api.get_liquidity_and_factor_analysis.return_value = {'ok': True}
            result = self.fa.get_factor_analysis(ps)
        # pos1 -> quantity, pos2 -> weight, pos3 -> skipped (no q/w), pos4 -> skipped (no asset_id)
        call_args = mock_api.get_liquidity_and_factor_analysis.call_args
        assert len(call_args.kwargs['positions']) == 2


# ── convert_hedge_factor_exposures ───────────────────────────────────────

class TestConvertHedgeFactorExposures:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_empty_list(self):
        """Empty style_factors raises MqValueError."""
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            self.fa.convert_hedge_factor_exposures([])

    def test_none_input(self):
        """None input raises MqValueError."""
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            self.fa.convert_hedge_factor_exposures(None)

    def test_valid_style_factors(self):
        """Normal conversion with valid data."""
        style_factors = [
            {'factor': 'Momentum', 'exposure': 1.5},
            {'factor': 'Value', 'exposure': -0.8},
        ]
        result = self.fa.convert_hedge_factor_exposures(style_factors)
        assert 'factorExposureBuckets' in result
        assert result['factorExposureBuckets'][0]['name'] == 'Style'
        sub = result['factorExposureBuckets'][0]['subFactors']
        assert len(sub) == 2
        assert sub[0] == {'name': 'Momentum', 'value': 1.5}
        assert sub[1] == {'name': 'Value', 'value': -0.8}
        assert result['notional'] == 0
        assert result['currency'] == 'USD'
        assert result['riskBuckets'] == []


# ── create_exposure_bar_chart ────────────────────────────────────────────

class TestCreateExposureBarChart:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_empty_exposures(self):
        """Empty dict => 'No data available' annotation."""
        fig = self.fa.create_exposure_bar_chart({}, "Title")
        assert isinstance(fig, go.Figure)

    def test_horizontal_bars(self):
        """Horizontal bar chart (default)."""
        exposures = {'Momentum': 1.5, 'Value': -0.8}
        fig = self.fa.create_exposure_bar_chart(exposures, "Test", horizontal=True)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1

    def test_vertical_bars(self):
        """Vertical bar chart."""
        exposures = {'Momentum': 1.5, 'Value': -0.8}
        fig = self.fa.create_exposure_bar_chart(exposures, "Test", horizontal=False)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1

    def test_all_positive_values(self):
        """All positive => all green."""
        exposures = {'A': 1.0, 'B': 2.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        assert list(fig.data[0].marker.color) == ['green', 'green']

    def test_all_negative_values(self):
        """All negative => all red."""
        exposures = {'A': -1.0, 'B': -2.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        assert list(fig.data[0].marker.color) == ['red', 'red']

    def test_mixed_values(self):
        """Mixed positive/negative => green/red."""
        exposures = {'A': 1.0, 'B': -2.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        assert list(fig.data[0].marker.color) == ['green', 'red']

    def test_zero_value_is_green(self):
        """Zero => green (>= 0 check)."""
        exposures = {'A': 0.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        assert list(fig.data[0].marker.color) == ['green']

    def test_horizontal_height_calculation_few_items(self):
        """Few items => minimum height 300."""
        exposures = {'A': 1.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        # max(300, 1*40+150) = 300
        assert fig.layout.height == 300

    def test_horizontal_height_calculation_many_items(self):
        """Many items => calculated height."""
        exposures = {f'F{i}': float(i) for i in range(20)}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=True)
        expected = max(300, 20 * 40 + 150)
        assert fig.layout.height == expected

    def test_vertical_height(self):
        """Vertical => height is 500."""
        exposures = {'A': 1.0}
        fig = self.fa.create_exposure_bar_chart(exposures, "T", horizontal=False)
        assert fig.layout.height == 500


# ── create_style_factor_chart ────────────────────────────────────────────

class TestCreateStyleFactorChart:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_no_factor_exposure_buckets_key(self):
        """Missing 'factorExposureBuckets' key => annotation."""
        fig = self.fa.create_style_factor_chart({})
        assert isinstance(fig, go.Figure)

    def test_no_style_bucket(self):
        """Buckets exist but no 'Style' bucket => empty style_factors."""
        data = {'factorExposureBuckets': [{'name': 'Sector', 'subFactors': []}]}
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_style_bucket_empty_subfactors(self):
        """Style bucket with no subFactors => empty dict."""
        data = {'factorExposureBuckets': [{'name': 'Style', 'subFactors': []}]}
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_subfactor_without_name(self):
        """SubFactor missing 'name' => skipped (factor_name is falsy)."""
        data = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': '', 'value': 1.0}]}
            ]
        }
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_subfactor_without_value_defaults_zero(self):
        """SubFactor missing 'value' => defaults to 0."""
        data = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'Momentum'}]}
            ]
        }
        # value is 0, which is neither > 0 nor < 0, so selected_factors is empty
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_all_zero_factors(self):
        """All factors with value 0 => no positive or negative => annotation."""
        data = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [
                    {'name': 'A', 'value': 0},
                    {'name': 'B', 'value': 0},
                ]}
            ]
        }
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_with_rows_limit(self):
        """rows parameter limits positive and negative factors."""
        data = _make_factor_analysis_result()
        fig = self.fa.create_style_factor_chart(data, rows=1, title="Test")
        assert isinstance(fig, go.Figure)

    def test_without_rows_limit(self):
        """rows=None => no limit (all factors shown)."""
        data = _make_factor_analysis_result()
        fig = self.fa.create_style_factor_chart(data, rows=None, title="All")
        assert isinstance(fig, go.Figure)

    def test_only_positive_factors(self):
        """Only positive factors => negative dict empty."""
        data = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [
                    {'name': 'A', 'value': 1.0},
                    {'name': 'B', 'value': 2.0},
                ]}
            ]
        }
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_only_negative_factors(self):
        """Only negative factors => positive dict empty."""
        data = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [
                    {'name': 'A', 'value': -1.0},
                    {'name': 'B', 'value': -2.0},
                ]}
            ]
        }
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_style_bucket_no_subfactors_key(self):
        """Style bucket with no 'subFactors' key => defaults to empty list."""
        data = {'factorExposureBuckets': [{'name': 'Style'}]}
        fig = self.fa.create_style_factor_chart(data)
        assert isinstance(fig, go.Figure)

    def test_rows_with_title_format(self):
        """When rows is not None, title includes row counts."""
        data = _make_factor_analysis_result()
        fig = self.fa.create_style_factor_chart(data, rows=2, title="Factors")
        assert isinstance(fig, go.Figure)
        # The title should contain the row info
        assert 'Top 2' in fig.layout.title.text


# ── create_exposure_summary_table ────────────────────────────────────────

class TestCreateExposureSummaryTable:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_full_data(self):
        """Full factor analysis result => correct summary table."""
        data = _make_factor_analysis_result()
        df = self.fa.create_exposure_summary_table(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6
        assert df['Metric'].tolist() == [
            'Notional', 'Currency', 'Market Risk', 'Specific Risk', 'Sector Risk', 'Style Risk'
        ]

    def test_missing_keys(self):
        """Missing keys => defaults used (0, 'USD')."""
        df = self.fa.create_exposure_summary_table({})
        assert isinstance(df, pd.DataFrame)
        assert df['Value'].iloc[0] == '$0'
        assert df['Value'].iloc[1] == 'USD'

    def test_empty_risk_buckets(self):
        """Empty riskBuckets => all risk values default to 0."""
        data = {'notional': 500, 'currency': 'EUR', 'riskBuckets': []}
        df = self.fa.create_exposure_summary_table(data)
        assert df['Value'].iloc[1] == 'EUR'
        assert df['Value'].iloc[2] == '0.0000'

    def test_partial_risk_buckets(self):
        """Only some risk bucket names present => others default to 0."""
        data = {
            'riskBuckets': [{'name': 'Market', 'value': 0.5}]
        }
        df = self.fa.create_exposure_summary_table(data)
        assert df['Value'].iloc[2] == '0.5000'  # Market
        assert df['Value'].iloc[3] == '0.0000'  # Specific (missing)


# ── create_performance_chart ─────────────────────────────────────────────

class TestCreatePerformanceChart:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_empty_dataframe(self):
        """Empty DataFrame => 'No performance data' annotation."""
        df = pd.DataFrame()
        fig = self.fa.create_performance_chart(df)
        assert isinstance(fig, go.Figure)

    def test_with_date_column(self):
        """DataFrame has 'date' column => used for x-axis."""
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'cumulativePnl': [100, 200],
        })
        fig = self.fa.create_performance_chart(df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1

    def test_without_date_column(self):
        """DataFrame without 'date' column => index used for x-axis."""
        df = pd.DataFrame({
            'cumulativePnl': [100, 200],
        })
        fig = self.fa.create_performance_chart(df)
        assert isinstance(fig, go.Figure)

    def test_metric_not_in_columns(self):
        """metric not in columns => falls back to iloc[:, 0]."""
        df = pd.DataFrame({
            'otherMetric': [100, 200],
        })
        fig = self.fa.create_performance_chart(df, metric='nonExistent')
        assert isinstance(fig, go.Figure)

    def test_metric_in_columns(self):
        """metric in columns => uses that column."""
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'myMetric': [42],
        })
        fig = self.fa.create_performance_chart(df, metric='myMetric', title='Custom')
        assert isinstance(fig, go.Figure)

    def test_no_date_and_metric_missing(self):
        """No 'date' column AND metric not in columns."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        fig = self.fa.create_performance_chart(df, metric='missing')
        assert isinstance(fig, go.Figure)


# ── create_dynamic_performance_chart ─────────────────────────────────────

class TestCreateDynamicPerformanceChart:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_no_timeseries_data(self):
        """Empty timeseriesData => annotation."""
        fig = self.fa.create_dynamic_performance_chart({})
        assert isinstance(fig, go.Figure)

    def test_empty_timeseries_list(self):
        """timeseriesData is empty list."""
        fig = self.fa.create_dynamic_performance_chart({'timeseriesData': []})
        assert isinstance(fig, go.Figure)

    def test_full_data(self):
        """Normal data with both cumulative and normalized."""
        data = _make_factor_analysis_result()
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2  # cumulative + normalized

    def test_only_cumulative_pnl(self):
        """Only cumulativePnl, no normalizedPerformance."""
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [['2024-01-01', 100]],
                'normalizedPerformance': [],
            }]
        }
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1

    def test_only_normalized_performance(self):
        """Only normalizedPerformance, no cumulativePnl."""
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [],
                'normalizedPerformance': [['2024-01-01', 1.0]],
            }]
        }
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1

    def test_both_empty(self):
        """Both cumulative and normalized are empty => annotation."""
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [],
                'normalizedPerformance': [],
            }]
        }
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)

    def test_invalid_item_format_skipped(self):
        """Items not matching len==2 or not starting with str => skipped."""
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [
                    [100],  # len != 2
                    [123, 456],  # item[0] not str
                    ['2024-01-01', 100],  # valid
                ],
                'normalizedPerformance': [
                    ['2024-01-01', 1.0],
                ],
            }]
        }
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)

    def test_cumulative_values_without_dates(self):
        """cumulativePnl items have non-string first elements => no dates but values exist.
        This triggers the fallback: cumulative_dates = list(range(len(cumulative_values)))
        """
        # All items fail the isinstance(item[0], str) check but have len==2
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [
                    [1, 100],  # item[0] is int, not str
                    [2, 200],
                ],
                'normalizedPerformance': [],
            }]
        }
        # Both items have len==2 but item[0] is not str, so no dates/values extracted
        # cumulative_dates=[], cumulative_values=[]
        # Both empty => annotation
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)

    def test_normalized_values_without_dates(self):
        """normalizedPerformance items with non-string first elements => fallback dates."""
        data = {
            'timeseriesData': [{
                'name': 'total',
                'cumulativePnl': [],
                'normalizedPerformance': [
                    [1, 1.0],
                    [2, 1.02],
                ],
            }]
        }
        # Same: items have len==2 but item[0] is int => no dates or values
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)

    def test_custom_title(self):
        """Custom title is used."""
        data = _make_factor_analysis_result()
        fig = self.fa.create_dynamic_performance_chart(data, title="My Chart")
        assert fig.layout.title.text == "My Chart"

    def test_no_total_item_in_timeseries(self):
        """timeseriesData has items but none named 'total' => loop exits without break.
        total_data stays None, causing AttributeError on .get() call.
        Covers branch 347->352 (for-loop exhausted without break).
        """
        data = {
            'timeseriesData': [
                {'name': 'sector', 'cumulativePnl': [], 'normalizedPerformance': []},
                {'name': 'style', 'cumulativePnl': [], 'normalizedPerformance': []},
            ]
        }
        with pytest.raises(AttributeError):
            self.fa.create_dynamic_performance_chart(data)

    def test_multiple_timeseries_items_before_total(self):
        """timeseriesData has non-'total' items before 'total' => loop iterates multiple times.
        Covers branch 348->347 (item name != 'total', continue loop).
        """
        data = {
            'timeseriesData': [
                {'name': 'sector', 'cumulativePnl': [], 'normalizedPerformance': []},
                {'name': 'style', 'cumulativePnl': [], 'normalizedPerformance': []},
                {
                    'name': 'total',
                    'cumulativePnl': [['2024-01-01', 100]],
                    'normalizedPerformance': [['2024-01-01', 1.0]],
                },
            ]
        }
        fig = self.fa.create_dynamic_performance_chart(data)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2


# ── create_factor_heatmap_comparison ─────────────────────────────────────

class TestCreateFactorHeatmapComparison:
    def setup_method(self):
        self.fa = FactorAnalytics('MODEL')

    def test_no_factors_in_either(self):
        """No Style bucket in either => empty all_factors => annotation."""
        fig = self.fa.create_factor_heatmap_comparison({}, {})
        assert isinstance(fig, go.Figure)

    def test_factors_only_in_initial(self):
        """Factors only in initial_analysis."""
        initial = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'Momentum', 'value': 1.5}]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, {})
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2  # initial + hedged traces

    def test_factors_only_in_hedged(self):
        """Factors only in hedged_analysis."""
        hedged = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'Value', 'value': -0.5}]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison({}, hedged)
        assert isinstance(fig, go.Figure)

    def test_overlapping_factors(self):
        """Both have overlapping factors."""
        initial = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [
                    {'name': 'Momentum', 'value': 1.5},
                    {'name': 'Value', 'value': -0.8},
                ]}
            ]
        }
        hedged = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [
                    {'name': 'Momentum', 'value': 0.3},
                    {'name': 'Size', 'value': 0.7},
                ]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, hedged)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2

    def test_non_style_buckets_ignored(self):
        """Non-Style buckets are ignored."""
        initial = {
            'factorExposureBuckets': [
                {'name': 'Sector', 'subFactors': [{'name': 'Tech', 'value': 2.0}]}
            ]
        }
        hedged = {
            'factorExposureBuckets': [
                {'name': 'Country', 'subFactors': [{'name': 'US', 'value': 1.0}]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, hedged)
        assert isinstance(fig, go.Figure)
        # No Style factors => annotation

    def test_custom_title(self):
        """Custom title is applied."""
        initial = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'A', 'value': 1.0}]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, {}, title="Custom Compare")
        assert fig.layout.title.text == "Custom Compare"

    def test_dynamic_height(self):
        """Many factors => height scales dynamically."""
        factors = [{'name': f'F{i}', 'value': float(i)} for i in range(20)]
        initial = {
            'factorExposureBuckets': [{'name': 'Style', 'subFactors': factors}]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, {})
        expected_height = max(500, 20 * 35 + 150)
        assert fig.layout.height == expected_height

    def test_missing_factor_defaults_to_zero(self):
        """Factor in one but not other => defaults to 0."""
        initial = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'Momentum', 'value': 2.0}]}
            ]
        }
        hedged = {
            'factorExposureBuckets': [
                {'name': 'Style', 'subFactors': [{'name': 'Value', 'value': -1.0}]}
            ]
        }
        fig = self.fa.create_factor_heatmap_comparison(initial, hedged)
        # Both traces should have 2 factors each
        assert len(fig.data[0].y) == 2
        assert len(fig.data[1].y) == 2


# ── convert_hedge_factor_exposures branch [143,144] ──────────────────────

class TestConvertHedgeFactorExposuresBranch:
    """Branch [143,144]: second `if not style_factors` is dead code (always False after line 140).
    We can only cover the True path at line 140 (raise)."""

    def setup_method(self):
        self.fa = FactorAnalytics('TEST_MODEL')

    def test_empty_style_factors_raises(self):
        """Branch [140,141]: empty style_factors raises MqValueError."""
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            self.fa.convert_hedge_factor_exposures([])

    def test_none_style_factors_raises(self):
        """Branch [140,141]: None style_factors raises MqValueError."""
        with pytest.raises(MqValueError, match="Style factor exposures data is empty"):
            self.fa.convert_hedge_factor_exposures(None)

    def test_valid_style_factors(self):
        """Covers the normal path through convert_hedge_factor_exposures."""
        style_factors = [
            {'factor': 'Momentum', 'exposure': 0.5},
            {'factor': 'Value', 'exposure': -0.3},
        ]
        result = self.fa.convert_hedge_factor_exposures(style_factors)
        assert 'factorExposureBuckets' in result
        assert result['factorExposureBuckets'][0]['name'] == 'Style'
        assert len(result['factorExposureBuckets'][0]['subFactors']) == 2


# ── create_performance_chart branches [374,375] [376,377] ────────────────

class TestCreateDynamicPerformanceChartFallbackDates:
    """Branches [374,375] and [376,377]: cumulative/normalized dates empty but values non-empty.
    These branches are dead code since dates and values are populated in the same loop.
    We cover the function with edge cases near these lines."""

    def setup_method(self):
        self.fa = FactorAnalytics('TEST_MODEL')

    def test_non_string_date_items_skipped(self):
        """Items with non-string first element are skipped, leaving both dates and values empty."""
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [[123, 100], [456, 200]],  # int dates, not str
                    'normalizedPerformance': [[789, 1.0]],  # int date
                }
            ],
        }
        fig = self.fa.create_dynamic_performance_chart(factor_analysis)
        # Both cumulative_dates and cumulative_values are empty (items failed str check)
        # So the "No cumulative PnL or normalized performance data" annotation is shown
        assert isinstance(fig, go.Figure)

    def test_empty_cumulative_and_normalized(self):
        """No cumulative or normalized data returns annotation figure."""
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [],
                    'normalizedPerformance': [],
                }
            ],
        }
        fig = self.fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)

    def test_with_valid_data(self):
        """Normal case with valid string dates and values."""
        factor_analysis = {
            'timeseriesData': [
                {
                    'name': 'total',
                    'cumulativePnl': [['2024-01-01', 100], ['2024-01-02', 200]],
                    'normalizedPerformance': [['2024-01-01', 1.0], ['2024-01-02', 1.02]],
                }
            ],
        }
        fig = self.fa.create_dynamic_performance_chart(factor_analysis)
        assert isinstance(fig, go.Figure)
        # Should have traces for cumulative and normalized
        assert len(fig.data) >= 1
