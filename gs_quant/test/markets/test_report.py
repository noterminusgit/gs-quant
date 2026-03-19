"""
Copyright 2018 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.api.gs.data import GsDataApi
from gs_quant.api.gs.portfolios import GsPortfolioApi
from gs_quant.api.gs.reports import GsReportApi, FactorRiskTableMode
from gs_quant.api.gs.thematics import GsThematicApi, Region, ThematicMeasure
from gs_quant.common import ReportParameters, Currency, PositionType, PositionTag
from gs_quant.errors import MqValueError
from gs_quant.markets.report import (
    ReturnFormat,
    ReportDataset,
    FactorRiskViewsMode,
    FactorRiskResultsMode,
    FactorRiskUnit,
    AttributionAggregationType,
    AggregationCategoryType,
    CustomAUMDataPoint,
    ReportJobFuture,
    Report,
    PerformanceReport,
    FactorRiskReport,
    ThematicReport,
    flatten_results_into_df,
    _format_multiple_factor_table,
    _filter_table_by_factor_and_category,
    get_thematic_breakdown_as_df,
    get_pnl_percent,
    format_aum_for_return_calculation,
    generate_daily_returns,
    get_factor_pnl_percent_for_single_factor,
    format_factor_pnl_for_return_calculation,
)
from gs_quant.session import GsSession, Environment
from gs_quant.target.portfolios import RiskAumSource
from gs_quant.target.reports import (
    Report as TargetReport,
    ReportType,
    PositionSourceType,
    ReportStatus,
)


# ────────────────────────────────────────────────────────────
# Enum / small class coverage
# ────────────────────────────────────────────────────────────

class TestEnums:
    def test_return_format_values(self):
        assert ReturnFormat.JSON.value is not None
        assert ReturnFormat.DATA_FRAME.value is not None

    def test_report_dataset_values(self):
        assert ReportDataset.PPA_DATASET.value == "PPA"
        assert ReportDataset.PPAA_DATASET.value == "PPAA"
        assert ReportDataset.PFR_DATASET.value == "PFR"
        assert ReportDataset.PFRA_DATASET.value == "PFRA"
        assert ReportDataset.AFR_DATASET.value == "AFR"
        assert ReportDataset.AFRA_DATASET.value == "AFRA"
        assert ReportDataset.ATA_DATASET.value == "ATA"
        assert ReportDataset.ATAA_DATASET.value == "ATAA"
        assert ReportDataset.PTA_DATASET.value == "PTA"
        assert ReportDataset.PTAA_DATASET.value == "PTAA"
        assert ReportDataset.PORTFOLIO_CONSTITUENTS.value == "PORTFOLIO_CONSTITUENTS"

    def test_factor_risk_views_mode(self):
        assert FactorRiskViewsMode.Risk.value == 'Risk'
        assert FactorRiskViewsMode.Attribution.value == 'Attribution'

    def test_factor_risk_results_mode(self):
        assert FactorRiskResultsMode.Portfolio.value == 'Portfolio'
        assert FactorRiskResultsMode.Positions.value == 'Positions'

    def test_factor_risk_unit(self):
        assert FactorRiskUnit.Percent.value == 'Percent'
        assert FactorRiskUnit.Notional.value == 'Notional'

    def test_attribution_aggregation_type(self):
        assert AttributionAggregationType.Arithmetic.value == 'arithmetic'
        assert AttributionAggregationType.Geometric.value == 'geometric'

    def test_aggregation_category_type(self):
        assert AggregationCategoryType.Sector.value == 'assetClassificationsGicsSector'
        assert AggregationCategoryType.Industry.value == 'assetClassificationsGicsIndustry'
        assert AggregationCategoryType.Region.value == 'region'
        assert AggregationCategoryType.Country.value == 'assetClassificationsCountryName'


# ────────────────────────────────────────────────────────────
# CustomAUMDataPoint
# ────────────────────────────────────────────────────────────

class TestCustomAUMDataPoint:
    def test_init_and_properties(self):
        d = dt.date(2023, 1, 1)
        pt = CustomAUMDataPoint(date=d, aum=1000.0)
        assert pt.date == d
        assert pt.aum == 1000.0

    def test_setters(self):
        pt = CustomAUMDataPoint(date=dt.date(2023, 1, 1), aum=100.0)
        pt.date = dt.date(2024, 6, 1)
        pt.aum = 999.0
        assert pt.date == dt.date(2024, 6, 1)
        assert pt.aum == 999.0


# ────────────────────────────────────────────────────────────
# ReportJobFuture
# ────────────────────────────────────────────────────────────

class TestReportJobFuture:
    def _make_future(self, report_type=ReportType.Portfolio_Performance_Analytics):
        return ReportJobFuture(
            report_id='RPT1',
            job_id='JOB1',
            report_type=report_type,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
        )

    def test_properties(self):
        f = self._make_future()
        assert f.job_id == 'JOB1'
        assert f.end_date == dt.date(2023, 6, 1)

    @patch.object(GsReportApi, 'get_report_job')
    def test_status(self, mock_get_job):
        mock_get_job.return_value = {'status': 'done'}
        f = self._make_future()
        assert f.status() == ReportStatus.done

    @patch.object(GsReportApi, 'get_report_job')
    def test_done_true_for_done(self, mock_get_job):
        mock_get_job.return_value = {'status': 'done'}
        f = self._make_future()
        assert f.done() is True

    @patch.object(GsReportApi, 'get_report_job')
    def test_done_true_for_error(self, mock_get_job):
        mock_get_job.return_value = {'status': 'error'}
        f = self._make_future()
        assert f.done() is True

    @patch.object(GsReportApi, 'get_report_job')
    def test_done_true_for_cancelled(self, mock_get_job):
        mock_get_job.return_value = {'status': 'cancelled'}
        f = self._make_future()
        assert f.done() is True

    @patch.object(GsReportApi, 'get_report_job')
    def test_done_false_for_executing(self, mock_get_job):
        mock_get_job.return_value = {'status': 'executing'}
        f = self._make_future()
        assert f.done() is False

    @patch.object(GsReportApi, 'get_report_job')
    def test_result_cancelled_raises(self, mock_get_job):
        mock_get_job.return_value = {'status': 'cancelled'}
        f = self._make_future()
        with pytest.raises(MqValueError, match='cancelled'):
            f.result()

    @patch.object(GsReportApi, 'get_report_job')
    def test_result_error_raises(self, mock_get_job):
        mock_get_job.return_value = {'status': 'error'}
        f = self._make_future()
        with pytest.raises(MqValueError, match='error'):
            f.result()

    @patch.object(GsReportApi, 'get_report_job')
    def test_result_not_done_raises(self, mock_get_job):
        mock_get_job.return_value = {'status': 'executing'}
        f = self._make_future()
        with pytest.raises(MqValueError, match='not done'):
            f.result()

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsReportApi, 'get_report_job')
    def test_result_ppa(self, mock_get_job, mock_query):
        mock_get_job.return_value = {'status': 'done'}
        mock_query.return_value = [{'date': '2023-01-01', 'pnl': 100}]
        f = self._make_future(ReportType.Portfolio_Performance_Analytics)
        result = f.result()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    @patch.object(GsReportApi, 'get_report_job')
    def test_result_factor_risk(self, mock_get_job, mock_results):
        mock_get_job.return_value = {'status': 'done'}
        mock_results.return_value = [{'date': '2023-01-01', 'factor': 'f1', 'pnl': 10}]
        f = self._make_future(ReportType.Portfolio_Factor_Risk)
        result = f.result()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    @patch.object(GsReportApi, 'get_report_job')
    def test_result_asset_factor_risk(self, mock_get_job, mock_results):
        mock_get_job.return_value = {'status': 'done'}
        mock_results.return_value = [{'date': '2023-01-01', 'factor': 'f1', 'pnl': 10}]
        f = self._make_future(ReportType.Asset_Factor_Risk)
        result = f.result()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_report_job')
    def test_result_thematic_returns_none(self, mock_get_job):
        mock_get_job.return_value = {'status': 'done'}
        f = self._make_future(ReportType.Portfolio_Thematic_Analytics)
        assert f.result() is None

    @patch.object(GsReportApi, 'get_report_job')
    def test_wait_for_completion_done_immediately(self, mock_get_job):
        mock_get_job.return_value = {'status': 'done'}
        f = self._make_future()
        assert f.wait_for_completion(sleep_time=0, max_retries=1) is True

    @patch.object(GsReportApi, 'get_report_job')
    def test_wait_for_completion_timeout_raises(self, mock_get_job):
        mock_get_job.return_value = {'status': 'executing'}
        f = self._make_future()
        with pytest.raises(MqValueError, match='taking longer'):
            f.wait_for_completion(sleep_time=0, max_retries=1, error_on_timeout=True)

    @patch('builtins.print')
    @patch.object(GsReportApi, 'get_report_job')
    def test_wait_for_completion_timeout_no_error(self, mock_get_job, mock_print):
        mock_get_job.return_value = {'status': 'executing'}
        f = self._make_future()
        result = f.wait_for_completion(sleep_time=0, max_retries=1, error_on_timeout=False)
        assert result is False
        mock_print.assert_called_once()

    @patch.object(GsReportApi, 'reschedule_report_job')
    def test_reschedule(self, mock_reschedule):
        f = self._make_future()
        f.reschedule()
        mock_reschedule.assert_called_once_with('JOB1')


# ────────────────────────────────────────────────────────────
# Report (base class)
# ────────────────────────────────────────────────────────────

class TestReport:
    def test_init_defaults(self):
        r = Report()
        assert r.id is None
        assert r.name is None
        assert r.position_source_id is None
        assert r.position_source_type is None
        assert r.type is None
        assert r.parameters is None
        assert r.earliest_start_date is None
        assert r.latest_end_date is None
        assert r.latest_execution_time is None
        assert r.status == ReportStatus.new
        assert r.percentage_complete is None

    def test_init_with_string_types(self):
        r = Report(
            position_source_type='Portfolio',
            report_type='Portfolio Performance Analytics',
            status='done',
        )
        assert r.position_source_type == PositionSourceType.Portfolio
        assert r.type == ReportType.Portfolio_Performance_Analytics
        assert r.status == ReportStatus.done

    def test_init_with_enum_types(self):
        r = Report(
            position_source_type=PositionSourceType.Asset,
            report_type=ReportType.Asset_Factor_Risk,
            status=ReportStatus.executing,
        )
        assert r.position_source_type == PositionSourceType.Asset
        assert r.type == ReportType.Asset_Factor_Risk
        assert r.status == ReportStatus.executing

    def test_init_none_source_type(self):
        r = Report(position_source_type=None)
        assert r.position_source_type is None

    def test_property_setters(self):
        r = Report()
        r.position_source_id = 'MP123'
        r.position_source_type = 'Portfolio'
        r.type = 'Portfolio Performance Analytics'
        r.parameters = ReportParameters()
        assert r.position_source_id == 'MP123'
        assert r.position_source_type == PositionSourceType.Portfolio
        assert r.type == ReportType.Portfolio_Performance_Analytics

    def test_position_source_type_setter_with_enum(self):
        r = Report()
        r.position_source_type = PositionSourceType.Asset
        assert r.position_source_type == PositionSourceType.Asset

    def test_type_setter_with_enum(self):
        r = Report()
        r.type = ReportType.Asset_Factor_Risk
        assert r.type == ReportType.Asset_Factor_Risk

    @patch.object(GsReportApi, 'get_report')
    def test_get(self, mock_get):
        mock_get.return_value = TargetReport(
            id='RPT1',
            position_source_type=PositionSourceType.Portfolio,
            position_source_id='MP123',
            type_=ReportType.Portfolio_Performance_Analytics,
            status=ReportStatus.done,
        )
        r = Report.get('RPT1')
        assert r.id == 'RPT1'

    def test_from_target(self):
        tr = TargetReport(
            id='RPT1',
            name='Test',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            type_=ReportType.Portfolio_Performance_Analytics,
            parameters=ReportParameters(),
            earliest_start_date=dt.date(2023, 1, 1),
            latest_end_date=dt.date(2023, 6, 1),
            latest_execution_time=dt.datetime(2023, 6, 1, 12, 0),
            status=ReportStatus.done,
            percentage_complete=100.0,
        )
        r = Report.from_target(tr)
        assert r.id == 'RPT1'
        assert r.name == 'Test'
        assert r.percentage_complete == 100.0

    @patch.object(GsReportApi, 'update_report')
    def test_save_existing(self, mock_update):
        r = Report(report_id='RPT1', position_source_id='MP1',
                    position_source_type=PositionSourceType.Portfolio,
                    report_type=ReportType.Portfolio_Performance_Analytics)
        r.save()
        mock_update.assert_called_once()

    @patch.object(GsReportApi, 'create_report')
    def test_save_new(self, mock_create):
        mock_create.return_value = MagicMock(id='NEW_RPT')
        r = Report(position_source_id='MP1',
                    position_source_type=PositionSourceType.Portfolio,
                    report_type=ReportType.Portfolio_Performance_Analytics)
        r.save()
        assert r.id == 'NEW_RPT'
        mock_create.assert_called_once()

    @patch.object(GsReportApi, 'create_report')
    def test_save_new_no_parameters(self, mock_create):
        """Save with no parameters should use empty ReportParameters"""
        mock_create.return_value = MagicMock(id='NEW_RPT')
        r = Report(position_source_id='MP1',
                    position_source_type=PositionSourceType.Portfolio,
                    report_type=ReportType.Portfolio_Performance_Analytics)
        r.save()
        call_args = mock_create.call_args[0][0]
        assert call_args.parameters is not None

    @patch.object(GsReportApi, 'delete_report')
    def test_delete(self, mock_delete):
        r = Report(report_id='RPT1')
        r.delete()
        mock_delete.assert_called_once_with('RPT1')

    def test_set_position_source_portfolio(self):
        r = FactorRiskReport(report_id='RPT1',
                             report_type=ReportType.Portfolio_Factor_Risk)
        r.set_position_source('MP123')
        assert r.position_source_type == PositionSourceType.Portfolio
        assert r.position_source_id == 'MP123'
        assert r.type == ReportType.Portfolio_Factor_Risk

    def test_set_position_source_asset_factor(self):
        r = FactorRiskReport(report_id='RPT1',
                             report_type=ReportType.Asset_Factor_Risk)
        r.set_position_source('MA3FMSN9VNMD')
        assert r.position_source_type == PositionSourceType.Asset
        assert r.type == ReportType.Asset_Factor_Risk

    def test_set_position_source_thematic_portfolio(self):
        r = ThematicReport(report_id='RPT1',
                           report_type=ReportType.Portfolio_Thematic_Analytics)
        r.set_position_source('MPXYZ')
        assert r.position_source_type == PositionSourceType.Portfolio
        assert r.type == ReportType.Portfolio_Thematic_Analytics

    def test_set_position_source_thematic_asset(self):
        r = ThematicReport(report_id='RPT1',
                           report_type=ReportType.Asset_Thematic_Analytics)
        r.set_position_source('MA3FMSN9VNMD')
        assert r.position_source_type == PositionSourceType.Asset
        assert r.type == ReportType.Asset_Thematic_Analytics

    @patch.object(GsReportApi, 'get_report_jobs')
    def test_get_most_recent_job(self, mock_jobs):
        mock_jobs.return_value = [
            {'id': 'J1', 'createdTime': '2023-01-01T00:00:00Z',
             'reportType': 'Portfolio Performance Analytics',
             'startDate': '2023-01-01', 'endDate': '2023-06-01'},
            {'id': 'J2', 'createdTime': '2023-06-01T00:00:00Z',
             'reportType': 'Portfolio Performance Analytics',
             'startDate': '2023-01-01', 'endDate': '2023-06-01'},
        ]
        r = Report(report_id='RPT1')
        job = r.get_most_recent_job()
        assert job.job_id == 'J2'

    def test_schedule_no_id_raises(self):
        r = Report()
        with pytest.raises(MqValueError, match='valid IDs'):
            r.schedule()

    def test_schedule_no_position_source_id_raises(self):
        r = Report(report_id='RPT1')
        with pytest.raises(MqValueError, match='valid IDs'):
            r.schedule()

    def test_schedule_non_portfolio_no_dates_raises(self):
        r = Report(report_id='RPT1', position_source_id='MA123',
                    position_source_type=PositionSourceType.Asset)
        with pytest.raises(MqValueError, match='Must specify'):
            r.schedule()

    @patch.object(GsReportApi, 'schedule_report')
    @patch.object(GsPortfolioApi, 'get_position_dates')
    def test_schedule_portfolio_no_positions_raises(self, mock_dates, mock_sched):
        mock_dates.return_value = []
        r = Report(report_id='RPT1', position_source_id='MPXYZ',
                    position_source_type=PositionSourceType.Portfolio)
        with pytest.raises(MqValueError, match='no positions'):
            r.schedule()

    @patch.object(GsReportApi, 'schedule_report')
    @patch.object(GsPortfolioApi, 'get_position_dates')
    def test_schedule_portfolio_with_dates(self, mock_dates, mock_sched):
        mock_dates.return_value = [dt.date(2023, 3, 1), dt.date(2023, 6, 1)]
        r = Report(report_id='RPT1', position_source_id='MPXYZ',
                    position_source_type=PositionSourceType.Portfolio)
        r.schedule()
        mock_sched.assert_called_once()

    @patch.object(GsReportApi, 'schedule_report')
    @patch.object(GsPortfolioApi, 'get_position_dates')
    def test_schedule_with_backcast(self, mock_dates, mock_sched):
        mock_dates.return_value = [dt.date(2023, 3, 1), dt.date(2023, 6, 1)]
        r = Report(report_id='RPT1', position_source_id='MPXYZ',
                    position_source_type=PositionSourceType.Portfolio)
        r.schedule(backcast=True)
        mock_sched.assert_called_once()

    @patch.object(GsReportApi, 'schedule_report')
    def test_schedule_with_explicit_dates(self, mock_sched):
        r = Report(report_id='RPT1', position_source_id='MP123',
                    position_source_type=PositionSourceType.Portfolio)
        r.schedule(start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 1))
        mock_sched.assert_called_once()

    @patch.object(GsReportApi, 'schedule_report')
    @patch.object(GsPortfolioApi, 'get_position_dates')
    def test_schedule_with_start_date_only(self, mock_dates, mock_sched):
        mock_dates.return_value = [dt.date(2023, 3, 1)]
        r = Report(report_id='RPT1', position_source_id='MPXYZ',
                    position_source_type=PositionSourceType.Portfolio)
        r.schedule(start_date=dt.date(2023, 1, 1))
        mock_sched.assert_called_once()

    @patch.object(GsReportApi, 'schedule_report')
    @patch.object(GsPortfolioApi, 'get_position_dates')
    def test_schedule_with_end_date_only(self, mock_dates, mock_sched):
        mock_dates.return_value = [dt.date(2023, 3, 1)]
        r = Report(report_id='RPT1', position_source_id='MPXYZ',
                    position_source_type=PositionSourceType.Portfolio)
        r.schedule(end_date=dt.date(2023, 6, 1))
        mock_sched.assert_called_once()


# ────────────────────────────────────────────────────────────
# PerformanceReport
# ────────────────────────────────────────────────────────────

class TestPerformanceReport:
    def test_init(self):
        pr = PerformanceReport(
            report_id='PPAID',
            position_source_type=PositionSourceType.Portfolio,
            position_source_id='PORTFOLIOID',
        )
        assert pr.type == ReportType.Portfolio_Performance_Analytics
        assert pr.id == 'PPAID'

    @patch.object(GsReportApi, 'get_report')
    def test_get(self, mock_get):
        mock_get.return_value = TargetReport(
            id='PPAID',
            position_source_type=PositionSourceType.Portfolio,
            position_source_id='MP1',
            type_=ReportType.Portfolio_Performance_Analytics,
            status=ReportStatus.done,
        )
        pr = PerformanceReport.get('PPAID')
        assert pr.type == ReportType.Portfolio_Performance_Analytics

    def test_from_target_wrong_type_raises(self):
        tr = TargetReport(
            id='X',
            type_=ReportType.Portfolio_Factor_Risk,
            status=ReportStatus.done,
        )
        with pytest.raises(MqValueError, match='not a performance report'):
            PerformanceReport.from_target(tr)

    def test_from_target_success(self):
        tr = TargetReport(
            id='PPAID',
            name='Test PPA',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            type_=ReportType.Portfolio_Performance_Analytics,
            status=ReportStatus.done,
            percentage_complete=100.0,
        )
        pr = PerformanceReport.from_target(tr)
        assert pr.id == 'PPAID'

    @patch.object(GsDataApi, 'query_data')
    def test_get_measure_dataframe(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'pnl': 100}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_measure('pnl')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch.object(GsDataApi, 'query_data')
    def test_get_measure_json(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'pnl': 100}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_measure('pnl', return_format=ReturnFormat.JSON)
        assert isinstance(result, list)

    @patch.object(GsDataApi, 'query_data')
    def test_get_pnl_notional(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'pnl': 100}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_pnl(unit=FactorRiskUnit.Notional)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_long_exposure(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'longExposure': 500}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_long_exposure()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_short_exposure(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'shortExposure': 500}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_short_exposure()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_asset_count(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'assetCount': 10}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_asset_count()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_turnover(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'turnover': 0.1}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_turnover()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_asset_count_long(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'assetCountLong': 5}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_asset_count_long()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_asset_count_short(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'assetCountShort': 3}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_asset_count_short()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_net_exposure(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'netExposure': 200}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_net_exposure()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_gross_exposure(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'grossExposure': 800}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_gross_exposure()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_asset_count_priced(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'assetCountPriced': 9}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_asset_count_priced()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_trading_pnl(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'tradingPnl': 50}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_trading_pnl()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_trading_cost_pnl(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'tradingCostPnl': -5}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_trading_cost_pnl()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_servicing_cost_long_pnl(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'servicingCostLongPnl': -2}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_servicing_cost_long_pnl()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_servicing_cost_short_pnl(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'servicingCostShortPnl': -3}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_servicing_cost_short_pnl()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_many_measures_default(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01', 'pnl': 100, 'longExposure': 500}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_many_measures(measures=('pnl', 'longExposure'))
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_many_measures_none(self, mock_query):
        mock_query.return_value = []
        pr = PerformanceReport(report_id='R1')
        result = pr.get_many_measures()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_many_measures_json(self, mock_query):
        mock_query.return_value = [{'date': '2023-01-01'}]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_many_measures(measures=('pnl',), return_format=ReturnFormat.JSON)
        assert isinstance(result, list)

    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_source(self, mock_portfolio):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Gross)
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        assert pr.get_aum_source() == RiskAumSource.Gross

    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_source_default(self, mock_portfolio):
        mock_portfolio.return_value = MagicMock(aum_source=None)
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        assert pr.get_aum_source() == RiskAumSource.Long

    @patch.object(GsPortfolioApi, 'update_portfolio')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_set_aum_source(self, mock_get, mock_update):
        portfolio = MagicMock()
        mock_get.return_value = portfolio
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        pr.set_aum_source(RiskAumSource.Net)
        assert portfolio.aum_source == RiskAumSource.Net
        mock_update.assert_called_once()

    @patch.object(GsReportApi, 'get_custom_aum')
    def test_get_custom_aum(self, mock_aum):
        mock_aum.return_value = [
            {'date': '2023-06-01', 'aum': 100},
            {'date': '2023-06-02', 'aum': 200},
        ]
        pr = PerformanceReport(report_id='R1')
        result = pr.get_custom_aum()
        assert len(result) == 2
        assert isinstance(result[0], CustomAUMDataPoint)

    @patch.object(GsReportApi, 'upload_custom_aum')
    def test_upload_custom_aum(self, mock_upload):
        pr = PerformanceReport(report_id='R1')
        aum_data = [CustomAUMDataPoint(dt.date(2023, 6, 1), 100.0)]
        pr.upload_custom_aum(aum_data, clear_existing_data=True)
        mock_upload.assert_called_once()

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_net(self, mock_portfolio, mock_query):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Net)
        mock_query.return_value = [
            {'date': '2023-06-01', 'netExposure': 2000},
        ]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(dt.date(2023, 6, 1), dt.date(2023, 6, 7))
        assert '2023-06-01' in result

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_long(self, mock_portfolio, mock_query):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Long)
        mock_query.return_value = [{'date': '2023-06-01', 'longExposure': 1000}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(dt.date(2023, 6, 1), dt.date(2023, 6, 7))
        assert '2023-06-01' in result

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_short(self, mock_portfolio, mock_query):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Short)
        mock_query.return_value = [{'date': '2023-06-01', 'shortExposure': 500}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(dt.date(2023, 6, 1), dt.date(2023, 6, 7))
        assert '2023-06-01' in result

    @patch.object(GsDataApi, 'query_data')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_gross(self, mock_portfolio, mock_query):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Gross)
        mock_query.return_value = [{'date': '2023-06-01', 'grossExposure': 3000}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(dt.date(2023, 6, 1), dt.date(2023, 6, 7))
        assert '2023-06-01' in result

    @patch.object(GsReportApi, 'get_custom_aum')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    def test_get_aum_custom(self, mock_portfolio, mock_custom_aum):
        mock_portfolio.return_value = MagicMock(aum_source=RiskAumSource.Custom_AUM)
        mock_custom_aum.return_value = [{'date': '2023-06-01', 'aum': 1234}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(dt.date(2023, 6, 1), dt.date(2023, 6, 7))
        assert len(result) == 1

    @patch.object(GsPortfolioApi, 'get_positions_data')
    def test_get_positions_data(self, mock_get):
        mock_get.return_value = [{'id': 'A1'}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_positions_data()
        assert len(result) == 1

    @patch.object(GsPortfolioApi, 'get_positions_data')
    def test_get_position_net_weights(self, mock_get):
        mock_get.return_value = [{'id': 'A1', 'netWeight': 0.5}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_position_net_weights(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsPortfolioApi, 'get_positions_data')
    def test_get_position_net_weights_error(self, mock_get):
        mock_get.side_effect = Exception("API Error")
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        with pytest.raises(MqValueError, match='Error retrieving net weight data'):
            pr.get_position_net_weights(
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 6, 1),
            )

    @patch.object(GsDataApi, 'query_data')
    def test_get_portfolio_constituents_empty(self, mock_query):
        mock_query.return_value = []
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_portfolio_constituents()
        assert result.empty

    @patch.object(GsDataApi, 'query_data')
    def test_get_portfolio_constituents_empty_json(self, mock_query):
        mock_query.return_value = []
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_portfolio_constituents(return_format=ReturnFormat.JSON)
        assert result == {}

    @patch.object(GsDataApi, 'query_data')
    def test_get_portfolio_constituents_with_data(self, mock_query):
        # First call returns asset count, second returns constituents
        mock_query.side_effect = [
            [{'date': '2023-01-02', 'assetCount': 10}],
            [{'date': '2023-01-02', 'assetId': 'A1', 'name': 'Test', 'entryType': 'Holding'}],
        ]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_portfolio_constituents(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 1, 3),
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_portfolio_constituents_prefer_rebalance(self, mock_query):
        mock_query.side_effect = [
            [{'date': '2023-01-02', 'assetCount': 10}],
            [
                {'date': '2023-01-02', 'assetId': 'A1', 'entryType': 'Holding'},
                {'date': '2023-01-02', 'assetId': 'A1', 'entryType': 'Rebalance'},
            ],
        ]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_portfolio_constituents(prefer_rebalance_positions=True)
        assert isinstance(result, pd.DataFrame)
        # Should only have Rebalance entry for 2023-01-02
        assert all(r == 'Rebalance' for r in result['entryType'].values)

    @patch.object(GsPortfolioApi, 'get_attribution')
    def test_get_pnl_contribution(self, mock_attr):
        mock_attr.return_value = [{'assetId': 'A1', 'pnl': 100}]
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_pnl_contribution()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_brinson_attribution_results')
    def test_get_brinson_attribution_dataframe(self, mock_brinson):
        mock_brinson.return_value = {
            'results': [{'factorCategory': 'Style', 'pnl': 100}]
        }
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_brinson_attribution()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_brinson_attribution_results')
    def test_get_brinson_attribution_json(self, mock_brinson):
        mock_brinson.return_value = {
            'results': [{'factorCategory': 'Style', 'pnl': 100}]
        }
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_brinson_attribution(return_format=ReturnFormat.JSON)
        assert isinstance(result, dict)

    @patch.object(GsReportApi, 'get_brinson_attribution_results')
    def test_get_brinson_attribution_with_category(self, mock_brinson):
        mock_brinson.return_value = {
            'results': [{'factorCategory': 'Style', 'pnl': 100}]
        }
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_brinson_attribution(
            aggregation_category=AggregationCategoryType.Sector,
        )
        assert isinstance(result, pd.DataFrame)


# ────────────────────────────────────────────────────────────
# FactorRiskReport
# ────────────────────────────────────────────────────────────

class TestFactorRiskReport:
    def test_init_auto_types_from_portfolio(self):
        fr = FactorRiskReport(
            risk_model_id='MODEL1',
            position_source_id='MP123',
        )
        assert fr.position_source_type == PositionSourceType.Portfolio
        assert fr.type == ReportType.Portfolio_Factor_Risk

    def test_init_auto_types_from_asset(self):
        fr = FactorRiskReport(
            risk_model_id='MODEL1',
            position_source_id='MA123',
        )
        assert fr.position_source_type == PositionSourceType.Asset
        assert fr.type == ReportType.Asset_Factor_Risk

    def test_init_no_source_id(self):
        fr = FactorRiskReport(risk_model_id='M1')
        assert fr.position_source_type is None
        assert fr.type is None

    def test_init_with_explicit_type(self):
        fr = FactorRiskReport(
            risk_model_id='M1',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        assert fr.type == ReportType.Portfolio_Factor_Risk

    def test_from_target_wrong_type(self):
        tr = TargetReport(
            id='X',
            type_=ReportType.Portfolio_Performance_Analytics,
            status=ReportStatus.done,
            parameters=ReportParameters(risk_model='M1', fx_hedged=True),
        )
        with pytest.raises(MqValueError, match='not a factor risk report'):
            FactorRiskReport.from_target(tr)

    def test_from_target_success(self):
        tr = TargetReport(
            id='FRR1',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            type_=ReportType.Portfolio_Factor_Risk,
            parameters=ReportParameters(risk_model='MODEL1', fx_hedged=True, benchmark='BMK1',
                                         tags=(PositionTag(name='t1', value='v1'),)),
            status=ReportStatus.done,
            percentage_complete=100.0,
        )
        fr = FactorRiskReport.from_target(tr)
        assert fr.get_risk_model_id() == 'MODEL1'
        assert fr.get_benchmark_id() == 'BMK1'

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_results_dataframe(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'f1', 'pnl': 10}
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_results()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_results_json(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'f1', 'pnl': 10}
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_results(return_format=ReturnFormat.JSON)
        assert isinstance(result, list)

    @patch.object(GsReportApi, 'get_factor_risk_report_view')
    def test_get_view(self, mock_view):
        mock_view.return_value = {'factorCategoriesTable': []}
        fr = FactorRiskReport(risk_model_id='M1', report_id='FR1',
                              report_type=ReportType.Portfolio_Factor_Risk)
        result = fr.get_view()
        assert isinstance(result, dict)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_factor_pnl_notional(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Factor', 'pnl': 100},
            {'date': '2023-01-02', 'factor': 'Factor', 'pnl': 200},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_factor_pnl(factor_names=['Factor'], unit=FactorRiskUnit.Notional)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_factor_pnl_notional_for_asset(self, mock_results):
        """When position_source_type is Asset, unit is ignored and Notional path is taken"""
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Factor', 'pnl': 100},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Asset,
            report_type=ReportType.Asset_Factor_Risk,
        )
        result = fr.get_factor_pnl(factor_names=['Factor'], unit=FactorRiskUnit.Percent)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_factor_exposure(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Factor', 'exposure': 500},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_factor_exposure(factor_names=['Factor'])
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_factor_proportion_of_risk(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Factor', 'proportionOfRisk': 0.5},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_factor_proportion_of_risk(factor_names=['Factor'])
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_annual_risk(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Total', 'annualRisk': 15.0},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_annual_risk(factor_names=['Total'])
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_daily_risk(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Total', 'dailyRisk': 1.0},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_daily_risk(factor_names=['Total'])
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_results')
    def test_get_ex_ante_var(self, mock_results):
        mock_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Total', 'dailyRisk': 1.0},
        ]
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_ex_ante_var(confidence_interval=99.0)
        assert isinstance(result, pd.DataFrame)
        assert 'Total' in result.columns

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_pnl_default_dates(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {
                    'columnInfo': [
                        {'columns': []},
                        {'columns': ['Total']},
                    ]
                },
                'rows': [
                    {'name': 'AAPL', 'symbol': 'AAPL', 'sector': 'Tech', 'Total': 100}
                ],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
            latest_end_date=dt.date(2023, 6, 1),
        )
        result = fr.get_table(mode=FactorRiskTableMode.Pnl)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_exposure_default_dates(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {
                    'columnInfo': [
                        {'columns': []},
                        {'columns': ['Factor']},
                    ]
                },
                'rows': [
                    {'name': 'AAPL', 'symbol': 'AAPL', 'sector': 'Tech', 'Factor': 0.5}
                ],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Factor_Risk,
            latest_end_date=dt.date(2023, 6, 1),
        )
        result = fr.get_table(mode=FactorRiskTableMode.Exposure)
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_with_start_only(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {'columnInfo': [{'columns': []}, {'columns': []}]},
                'rows': [],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
            latest_end_date=dt.date(2023, 6, 1),
        )
        result = fr.get_table(mode=FactorRiskTableMode.Pnl, start_date=dt.date(2023, 1, 1))
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_with_end_only_pnl(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {'columnInfo': [{'columns': []}, {'columns': []}]},
                'rows': [],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_table(mode=FactorRiskTableMode.Pnl, end_date=dt.date(2023, 6, 1))
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_with_end_only_non_pnl(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {'columnInfo': [{'columns': []}, {'columns': []}]},
                'rows': [],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_table(mode=FactorRiskTableMode.Exposure, start_date=None, end_date=dt.date(2023, 6, 1))
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_start_only_non_pnl(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {'columnInfo': [{'columns': []}, {'columns': []}]},
                'rows': [],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
            latest_end_date=dt.date(2023, 6, 1),
        )
        result = fr.get_table(mode=FactorRiskTableMode.Exposure, start_date=dt.date(2023, 3, 1))
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_warning(self, mock_table):
        mock_table.return_value = {'warning': 'No data available'}
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        with pytest.raises(MqValueError, match='No data available'):
            fr.get_table(
                mode=FactorRiskTableMode.Pnl,
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 6, 1),
            )

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_json(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {'columnInfo': [{'columns': []}, {'columns': []}]},
                'rows': [],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_table(
            mode=FactorRiskTableMode.Pnl,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            return_format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_with_factors_filter(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {
                    'columnInfo': [
                        {'columns': []},
                        {'columns': ['Total']},
                        {'columnGroup': 'Style', 'columns': ['Momentum']},
                    ]
                },
                'rows': [
                    {'name': 'AAPL', 'symbol': 'AAPL', 'sector': 'Tech', 'Total': 100, 'Momentum': 50}
                ],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_table(
            mode=FactorRiskTableMode.Pnl,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            factors=['Total'],
        )
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsReportApi, 'get_factor_risk_report_table')
    def test_get_table_with_factor_categories(self, mock_table):
        mock_table.return_value = {
            'table': {
                'metadata': {
                    'columnInfo': [
                        {'columns': []},
                        {'columns': ['Total']},
                        {'columnGroup': 'Style', 'columns': ['Momentum', 'Size']},
                    ]
                },
                'rows': [
                    {'name': 'AAPL', 'symbol': 'AAPL', 'sector': 'Tech',
                     'Total': 100, 'Momentum': 50, 'Size': 25}
                ],
            }
        }
        fr = FactorRiskReport(
            risk_model_id='M1', report_id='FR1',
            report_type=ReportType.Portfolio_Factor_Risk,
        )
        result = fr.get_table(
            mode=FactorRiskTableMode.Pnl,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            factor_categories=['Style'],
        )
        assert isinstance(result, pd.DataFrame)


# ────────────────────────────────────────────────────────────
# ThematicReport
# ────────────────────────────────────────────────────────────

class TestThematicReport:
    def test_init_auto_types_portfolio(self):
        tr = ThematicReport(position_source_id='MP123')
        assert tr.position_source_type == PositionSourceType.Portfolio
        assert tr.type == ReportType.Portfolio_Thematic_Analytics

    def test_init_auto_types_asset(self):
        tr = ThematicReport(position_source_id='MA123')
        assert tr.position_source_type == PositionSourceType.Asset
        assert tr.type == ReportType.Asset_Thematic_Analytics

    def test_init_no_source_id(self):
        tr = ThematicReport()
        assert tr.position_source_type is None

    def test_init_with_explicit_type(self):
        tr = ThematicReport(
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Thematic_Analytics,
        )
        assert tr.type == ReportType.Portfolio_Thematic_Analytics

    def test_from_target_wrong_type(self):
        target = TargetReport(
            id='X',
            type_=ReportType.Portfolio_Performance_Analytics,
            status=ReportStatus.done,
        )
        with pytest.raises(MqValueError, match='not a thematic report'):
            ThematicReport.from_target(target)

    def test_from_target_success(self):
        target = TargetReport(
            id='TR1',
            name='Test Thematic',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            type_=ReportType.Portfolio_Thematic_Analytics,
            status=ReportStatus.done,
        )
        tr = ThematicReport.from_target(target)
        assert tr.id == 'TR1'

    def test_from_target_asset_thematic(self):
        target = TargetReport(
            id='TR2',
            position_source_id='MA1',
            position_source_type=PositionSourceType.Asset,
            type_=ReportType.Asset_Thematic_Analytics,
            status=ReportStatus.done,
        )
        tr = ThematicReport.from_target(target)
        assert tr.id == 'TR2'

    @patch.object(GsDataApi, 'query_data')
    def test_get_thematic_data(self, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'thematicExposure': 1e8, 'grossExposure': 3e8}
        ]
        tr = ThematicReport(report_id='TR1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_thematic_data()
        assert isinstance(result, pd.DataFrame)
        assert 'thematicBeta' in result.columns

    @patch.object(GsDataApi, 'query_data')
    def test_get_thematic_exposure(self, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'thematicExposure': 1e8}
        ]
        tr = ThematicReport(report_id='TR1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_thematic_exposure()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_thematic_exposure_with_basket_ids(self, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'thematicExposure': 1e8}
        ]
        tr = ThematicReport(report_id='TR1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_thematic_exposure(basket_ids=['MA01GPR89HZF1FZ5'])
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsDataApi, 'query_data')
    def test_get_thematic_betas(self, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'thematicExposure': 1e8, 'grossExposure': 3e8}
        ]
        tr = ThematicReport(report_id='TR1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_thematic_betas()
        assert isinstance(result, pd.DataFrame)
        assert 'thematicBeta' in result.columns
        # thematicExposure and grossExposure should be popped
        assert 'thematicExposure' not in result.columns
        assert 'grossExposure' not in result.columns

    @patch.object(GsDataApi, 'query_data')
    def test_get_measures_asset_dataset(self, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'thematicExposure': 1e8}
        ]
        tr = ThematicReport(report_id='TR1',
                            position_source_type=PositionSourceType.Asset)
        result = tr.get_thematic_exposure()
        assert isinstance(result, pd.DataFrame)
        # Should use ATA_DATASET for Asset type
        call_args = mock_query.call_args
        assert call_args[1]['dataset_id'] == 'ATA'

    @patch.object(GsThematicApi, 'get_thematics')
    def test_get_all_thematic_exposures(self, mock_thematics):
        mock_thematics.return_value = [
            {
                'date': '2023-01-01',
                'allThematicExposures': [
                    {'basketName': 'B1', 'thematicExposure': 100, 'thematicBeta': 0.5}
                ],
            }
        ]
        tr = ThematicReport(report_id='TR1', position_source_id='MP1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_all_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsThematicApi, 'get_thematics')
    def test_get_top_five_thematic_exposures(self, mock_thematics):
        mock_thematics.return_value = [
            {
                'date': '2023-01-01',
                'topFiveThematicExposures': [
                    {'basketName': 'B1', 'thematicExposure': 100, 'thematicBeta': 0.5}
                ],
            }
        ]
        tr = ThematicReport(report_id='TR1', position_source_id='MP1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_top_five_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsThematicApi, 'get_thematics')
    def test_get_bottom_five_thematic_exposures(self, mock_thematics):
        mock_thematics.return_value = [
            {
                'date': '2023-01-01',
                'bottomFiveThematicExposures': [
                    {'basketName': 'B1', 'thematicExposure': 50, 'thematicBeta': 0.2}
                ],
            }
        ]
        tr = ThematicReport(report_id='TR1', position_source_id='MP1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_bottom_five_thematic_exposures()
        assert isinstance(result, pd.DataFrame)

    @patch.object(GsThematicApi, 'get_thematics')
    def test_get_thematic_breakdown(self, mock_thematics):
        mock_thematics.return_value = [
            {
                'date': '2023-01-01',
                'thematicBreakdownByAsset': [
                    {
                        'thematicBreakdownByAsset': [
                            {'name': 'AAPL', 'beta': 0.5, 'thematicExposure': 1e6}
                        ]
                    }
                ],
            }
        ]
        tr = ThematicReport(report_id='TR1', position_source_id='MP1',
                            position_source_type=PositionSourceType.Portfolio)
        result = tr.get_thematic_breakdown(
            date=dt.date(2023, 1, 1),
            basket_id='MA01GPR89HZF1FZ5',
        )
        assert isinstance(result, pd.DataFrame)


# ────────────────────────────────────────────────────────────
# Module-level helper functions
# ────────────────────────────────────────────────────────────

class TestHelperFunctions:
    def test_format_multiple_factor_table(self):
        data = [
            {'date': '2023-01-01', 'factor': 'F1', 'pnl': 10},
            {'date': '2023-01-01', 'factor': 'F2', 'pnl': 20},
            {'date': '2023-01-02', 'factor': 'F1', 'pnl': 30},
        ]
        result = _format_multiple_factor_table(data, 'pnl')
        assert isinstance(result, pd.DataFrame)
        assert 'F1' in result.columns
        assert 'F2' in result.columns
        assert len(result) == 2

    def test_format_multiple_factor_table_same_date_accumulates(self):
        data = [
            {'date': '2023-01-01', 'factor': 'F1', 'pnl': 10},
            {'date': '2023-01-01', 'factor': 'F2', 'pnl': 20},
        ]
        result = _format_multiple_factor_table(data, 'pnl')
        assert len(result) == 1
        row = result.iloc[0]
        assert row['F1'] == 10
        assert row['F2'] == 20

    def test_flatten_results_into_df_no_lists(self):
        results = [{'date': '2023-01-01', 'scalar': 'value'}]
        df = flatten_results_into_df(results)
        assert df.empty

    def test_flatten_results_into_df_with_lists(self):
        results = [
            {
                'date': '2023-01-01',
                'allThematicExposures': [
                    {'basketName': 'B1', 'thematicExposure': 100},
                    {'basketName': 'B2', 'thematicExposure': 200},
                ],
            }
        ]
        df = flatten_results_into_df(results)
        assert len(df) == 2

    def test_filter_table_no_filters(self):
        column_info = [
            {'columns': ['name', 'symbol']},
            {'columns': ['F1', 'F2']},
        ]
        result = _filter_table_by_factor_and_category(column_info, None, None)
        assert result == ['name', 'symbol', 'F1', 'F2']

    def test_filter_table_with_factors(self):
        column_info = [
            {'columns': ['name', 'symbol']},
            {'columns': ['Total']},
            {'columnGroup': 'Style', 'columns': ['Momentum']},
        ]
        result = _filter_table_by_factor_and_category(column_info, ['Momentum'], None)
        assert 'Momentum' in result

    def test_filter_table_with_factor_categories(self):
        column_info = [
            {'columns': ['name', 'symbol']},
            {'columns': ['Total']},
            {'columnGroup': 'Style', 'columns': ['Momentum', 'Size']},
        ]
        result = _filter_table_by_factor_and_category(column_info, None, ['Style'])
        assert 'Momentum' in result
        assert 'Size' in result

    def test_filter_table_with_both(self):
        column_info = [
            {'columns': ['name', 'symbol']},
            {'columns': ['Total']},
            {'columnGroup': 'Style', 'columns': ['Momentum']},
        ]
        result = _filter_table_by_factor_and_category(column_info, ['Custom'], ['Style'])
        assert 'Custom' in result
        assert 'Momentum' in result

    def test_get_thematic_breakdown_as_df(self):
        with patch.object(GsThematicApi, 'get_thematics') as mock_thematics:
            mock_thematics.return_value = [
                {
                    'date': '2023-01-01',
                    'thematicBreakdownByAsset': [
                        {
                            'thematicBreakdownByAsset': [
                                {'name': 'AAPL', 'beta': 0.5}
                            ]
                        }
                    ],
                }
            ]
            df = get_thematic_breakdown_as_df('MP1', dt.date(2023, 1, 1), 'B1')
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 1


class TestPnlPercentFunctions:
    def test_generate_daily_returns_with_start_date(self):
        aum_df = pd.DataFrame({
            'date': ['2023-01-02', '2023-01-03', '2023-01-04'],
            'aum': [1000, 1000, 1000],
        })
        pnl_df = pd.DataFrame({
            'date': ['2023-01-02', '2023-01-03', '2023-01-04'],
            'pnl': [0, 10, 20],
        })
        result = generate_daily_returns(aum_df, pnl_df, 'aum', 'pnl', True)
        assert isinstance(result, pd.Series)

    def test_generate_daily_returns_without_start_date(self):
        aum_df = pd.DataFrame({
            'date': ['2023-01-02', '2023-01-03'],
            'aum': [1000, 1000],
        })
        pnl_df = pd.DataFrame({
            'date': ['2023-01-03'],
            'pnl': [10],
        })
        result = generate_daily_returns(aum_df, pnl_df, 'aum', 'pnl', False)
        assert isinstance(result, pd.Series)

    def test_generate_daily_returns_with_total_pnl(self):
        aum_df = pd.DataFrame({
            'date': ['2023-01-02', '2023-01-03', '2023-01-04'],
            'aum': [1000, 1000, 1000],
        })
        pnl_df = pd.DataFrame({
            'date': ['2023-01-02', '2023-01-03', '2023-01-04'],
            'pnl': [0, 10, 20],
            'totalPnl': [0, 15, 30],
        })
        result = generate_daily_returns(aum_df, pnl_df, 'aum', 'pnl', True)
        assert isinstance(result, pd.Series)

    def test_format_factor_pnl_for_return_calculation(self):
        factor_data = [
            {'date': '2023-01-01', 'pnl': 10, 'factor': 'F1'},
            {'date': '2023-01-02', 'pnl': 20, 'factor': 'F1'},
        ]
        total_data = [
            {'date': '2023-01-01', 'pnl': 30, 'factor': 'Total'},
            {'date': '2023-01-02', 'pnl': 50, 'factor': 'Total'},
        ]
        result = format_factor_pnl_for_return_calculation(factor_data, total_data)
        assert 'pnl' in result.columns
        assert 'totalPnl' in result.columns
        assert len(result) == 2


# ────────────────────────────────────────────────────────────
# PnL percent path for PerformanceReport
# ────────────────────────────────────────────────────────────

class TestPnlMeasurePercent:
    @patch.object(GsDataApi, 'query_data')
    @patch('gs_quant.markets.report.get_pnl_percent')
    def test_get_pnl_measure_percent(self, mock_get_pnl_pct, mock_query):
        mock_query.return_value = [
            {'date': '2023-01-01', 'pnl': 100},
            {'date': '2023-01-02', 'pnl': 200},
        ]
        mock_get_pnl_pct.return_value = pd.DataFrame({
            'return': [0.0, 0.01]
        }, index=['2023-01-01', '2023-01-02'])
        mock_get_pnl_pct.return_value.index.name = 'date'

        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_pnl(unit=FactorRiskUnit.Percent)
        assert isinstance(result, pd.DataFrame)


# ────────────────────────────────────────────────────────────
# Report.run() branch coverage
# ────────────────────────────────────────────────────────────

class TestReportRun:
    """Cover all branches in Report.run() (lines 389-416)."""

    def _make_report(self):
        return Report(
            report_id='R1',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
            report_type=ReportType.Portfolio_Performance_Analytics,
        )

    @patch.object(Report, 'get_most_recent_job')
    @patch.object(Report, 'schedule')
    def test_run_async_returns_job_future(self, mock_schedule, mock_get_job):
        """Branches [391,392], [394,395]: is_async=True returns job_future immediately."""
        mock_job = MagicMock()
        mock_get_job.return_value = mock_job
        report = self._make_report()
        result = report.run(is_async=True)
        assert result is mock_job
        mock_schedule.assert_called_once()

    @patch('gs_quant.markets.report.sleep')
    @patch.object(Report, 'get_most_recent_job')
    @patch.object(Report, 'schedule')
    def test_run_sync_done_immediately(self, mock_schedule, mock_get_job, mock_sleep):
        """Branches [394,396], [397,398], [398,399]: sync, done immediately."""
        mock_job = MagicMock()
        mock_job.done.return_value = True
        mock_job.result.return_value = 'results'
        mock_get_job.return_value = mock_job
        report = self._make_report()
        result = report.run(is_async=False)
        assert result == 'results'
        mock_sleep.assert_not_called()

    @patch('gs_quant.markets.report.sleep')
    @patch.object(Report, 'get_most_recent_job')
    @patch.object(Report, 'schedule')
    def test_run_sync_waits_then_done(self, mock_schedule, mock_get_job, mock_sleep):
        """Branches [398,400]: sleep called, then done on second iteration."""
        mock_job = MagicMock()
        mock_job.done.side_effect = [False, True]
        mock_job.result.return_value = 'delayed_results'
        mock_get_job.return_value = mock_job
        report = self._make_report()
        result = report.run(is_async=False)
        assert result == 'delayed_results'
        mock_sleep.assert_called_with(6)

    @patch.object(Report, 'get')
    @patch.object(Report, 'get_most_recent_job')
    @patch.object(Report, 'schedule')
    def test_run_index_error_exhausts_counter_waiting(self, mock_schedule, mock_get_job, mock_get):
        """Branches [391,407], [408,409]: IndexError 5 times, status=waiting."""
        mock_get_job.side_effect = IndexError('no jobs')
        mock_report = MagicMock()
        mock_report.status = ReportStatus.waiting
        mock_get.return_value = mock_report
        report = self._make_report()
        with pytest.raises(MqValueError, match='stuck in "waiting" status'):
            report.run(is_async=True)

    @patch.object(Report, 'get')
    @patch.object(Report, 'get_most_recent_job')
    @patch.object(Report, 'schedule')
    def test_run_index_error_exhausts_counter_other_status(self, mock_schedule, mock_get_job, mock_get):
        """Branches [391,407], [408,412]: IndexError 5 times, status != waiting."""
        mock_get_job.side_effect = IndexError('no jobs')
        mock_report = MagicMock()
        mock_report.status = ReportStatus.done
        mock_get.return_value = mock_report
        report = self._make_report()
        with pytest.raises(MqValueError, match='taking longer to run than expected'):
            report.run(is_async=True)


# ────────────────────────────────────────────────────────────
# PerformanceReport.get_aum() Net branch
# ────────────────────────────────────────────────────────────

class TestPerformanceReportGetAumNet:
    """Cover branch [801,-780]: aum_source == RiskAumSource.Net."""

    @patch.object(PerformanceReport, 'get_net_exposure')
    @patch.object(PerformanceReport, 'get_aum_source')
    def test_get_aum_net(self, mock_aum_source, mock_net_exposure):
        mock_aum_source.return_value = RiskAumSource.Net
        mock_net_exposure.return_value = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'netExposure': [1000, 2000],
        })
        pr = PerformanceReport(report_id='R1', position_source_id='MP1')
        result = pr.get_aum(start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 1, 2))
        assert result == {'2023-01-01': 1000, '2023-01-02': 2000}


# ────────────────────────────────────────────────────────────
# FactorRiskReport.get_factor_pnl() Percent branches
# ────────────────────────────────────────────────────────────

class TestFactorRiskReportGetFactorPnl:
    """Cover branches in get_factor_pnl (lines 1271-1307)."""

    def _make_factor_risk_report(self):
        return FactorRiskReport(
            report_id='FR1',
            position_source_id='MP1',
            position_source_type=PositionSourceType.Portfolio,
        )

    @patch.object(FactorRiskReport, 'get_results')
    def test_notional_unit_returns_table(self, mock_get_results):
        """Branch [1271,1272]: unit=Notional -> _format_multiple_factor_table."""
        mock_get_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Style', 'pnl': 10},
            {'date': '2023-01-02', 'factor': 'Style', 'pnl': 20},
        ]
        frr = self._make_factor_risk_report()
        result = frr.get_factor_pnl(unit=FactorRiskUnit.Notional)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.report.get_factor_pnl_percent_for_single_factor')
    @patch('gs_quant.markets.report.format_aum_for_return_calculation')
    @patch.object(GsPortfolioApi, 'get_reports')
    @patch.object(FactorRiskReport, 'get_results')
    def test_percent_unit_factor_names_none_with_total(self, mock_get_results, mock_get_reports,
                                                       mock_format_aum, mock_get_pnl_pct):
        """Branches [1271,1274], [1274,1275]: unit=Percent, factor_names=None with Total in data.
        Also [1288,1299] total_data present, [1300,1301] + [1300,1307] loop."""
        mock_get_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Style', 'pnl': 10},
            {'date': '2023-01-02', 'factor': 'Style', 'pnl': 20},
            {'date': '2023-01-01', 'factor': 'Total', 'pnl': 30},
            {'date': '2023-01-02', 'factor': 'Total', 'pnl': 50},
        ]
        perf_report_target = MagicMock()
        perf_report_target.id = 'PR1'
        perf_report_target.type_ = ReportType.Portfolio_Performance_Analytics
        mock_get_reports.return_value = [perf_report_target]

        frr = self._make_factor_risk_report()
        frr.parameters = ReportParameters(tags=())

        mock_perf_report = MagicMock(spec=PerformanceReport)
        mock_perf_report.parameters = ReportParameters(tags=())

        with patch.object(PerformanceReport, 'get', return_value=mock_perf_report):
            mock_format_aum.return_value = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'aum': [1000, 1000],
            })
            mock_get_pnl_pct.return_value = pd.Series(
                [0.01, 0.02],
                index=pd.Index(['2023-01-01', '2023-01-02'], name='date'),
                name='pnlPercent',
            )

            result = frr.get_factor_pnl(unit=FactorRiskUnit.Percent, factor_names=None)
            assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.report.get_factor_pnl_percent_for_single_factor')
    @patch('gs_quant.markets.report.format_aum_for_return_calculation')
    @patch.object(GsPortfolioApi, 'get_reports')
    @patch.object(FactorRiskReport, 'get_results')
    def test_percent_unit_factor_names_none_no_total(self, mock_get_results, mock_get_reports,
                                                     mock_format_aum, mock_get_pnl_pct):
        """Branch [1288,1289]: factor_names=None, no Total -> fetches Total separately."""
        mock_get_results.side_effect = [
            [
                {'date': '2023-01-01', 'factor': 'Style', 'pnl': 10},
                {'date': '2023-01-02', 'factor': 'Style', 'pnl': 20},
            ],
            [
                {'date': '2023-01-01', 'factor': 'Total', 'pnl': 30},
                {'date': '2023-01-02', 'factor': 'Total', 'pnl': 50},
            ],
        ]
        perf_report_target = MagicMock()
        perf_report_target.id = 'PR1'
        perf_report_target.type_ = ReportType.Portfolio_Performance_Analytics
        mock_get_reports.return_value = [perf_report_target]

        frr = self._make_factor_risk_report()
        frr.parameters = ReportParameters(tags=())

        mock_perf_report = MagicMock(spec=PerformanceReport)
        mock_perf_report.parameters = ReportParameters(tags=())

        with patch.object(PerformanceReport, 'get', return_value=mock_perf_report):
            mock_format_aum.return_value = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'aum': [1000, 1000],
            })
            mock_get_pnl_pct.return_value = pd.Series(
                [0.01, 0.02],
                index=pd.Index(['2023-01-01', '2023-01-02'], name='date'),
                name='pnlPercent',
            )

            result = frr.get_factor_pnl(unit=FactorRiskUnit.Percent, factor_names=None)
            assert isinstance(result, pd.DataFrame)
            assert mock_get_results.call_count == 2

    @patch('gs_quant.markets.report.get_factor_pnl_percent_for_single_factor')
    @patch('gs_quant.markets.report.format_aum_for_return_calculation')
    @patch.object(GsPortfolioApi, 'get_reports')
    @patch.object(FactorRiskReport, 'get_results')
    def test_percent_unit_factor_names_given_with_total(self, mock_get_results, mock_get_reports,
                                                        mock_format_aum, mock_get_pnl_pct):
        """Branches [1274,1277]: factor_names is not None (with Total already in data),
        [1288,1299]: total_data not empty."""
        mock_get_results.return_value = [
            {'date': '2023-01-01', 'factor': 'Style', 'pnl': 10},
            {'date': '2023-01-02', 'factor': 'Style', 'pnl': 20},
            {'date': '2023-01-01', 'factor': 'Total', 'pnl': 30},
            {'date': '2023-01-02', 'factor': 'Total', 'pnl': 50},
        ]

        perf_report_target = MagicMock()
        perf_report_target.id = 'PR1'
        perf_report_target.type_ = ReportType.Portfolio_Performance_Analytics
        mock_get_reports.return_value = [perf_report_target]

        frr = self._make_factor_risk_report()
        frr.parameters = ReportParameters(tags=())

        mock_perf_report = MagicMock(spec=PerformanceReport)
        mock_perf_report.parameters = ReportParameters(tags=())

        with patch.object(PerformanceReport, 'get', return_value=mock_perf_report):
            mock_format_aum.return_value = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'aum': [1000, 1000],
            })
            mock_get_pnl_pct.return_value = pd.Series(
                [0.01, 0.02],
                index=pd.Index(['2023-01-01', '2023-01-02'], name='date'),
                name='pnlPercent',
            )

            result = frr.get_factor_pnl(
                unit=FactorRiskUnit.Percent,
                factor_names=['Style'],
            )
            assert isinstance(result, pd.DataFrame)


class TestGetAumNetBranch:
    """Cover branch [801,-780]: aum_source is not any of the known enums -> returns None."""

    def test_get_aum_unknown_source(self):
        """When aum_source is an unknown value -> none of the if branches match [801,-780]."""
        from gs_quant.markets.report import PerformanceReport
        report = PerformanceReport.__new__(PerformanceReport)
        report._PerformanceReport__report = MagicMock()
        with patch.object(PerformanceReport, 'get_aum_source', return_value='UnknownSource'):
            result = report.get_aum(dt.date(2023, 1, 1), dt.date(2023, 12, 31))
        assert result is None


if __name__ == '__main__':
    pytest.main(args=[__file__])
