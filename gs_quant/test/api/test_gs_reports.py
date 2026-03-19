"""
Tests for gs_quant.api.gs.reports - GsReportApi
Targets 100% branch coverage for all methods.
"""

import datetime as dt
from unittest.mock import MagicMock

import pytest

from gs_quant.api.gs.reports import GsReportApi, OrderType, FactorRiskTableMode
from gs_quant.common import Currency, PositionTag
from gs_quant.session import GsSession, Environment
from gs_quant.target.reports import Report, ReportParameters


def _mock_session(mocker):
    mocker.patch.object(
        GsSession.__class__,
        'default_value',
        return_value=GsSession.get(Environment.QA, 'client_id', 'secret'),
    )
    return GsSession.current


# ── Enum classes ─────────────────────────────────────────────────────

class TestEnums:
    def test_order_type_values(self):
        assert OrderType.Ascending.value == 'Ascending'
        assert OrderType.Descending.value == 'Descending'

    def test_factor_risk_table_mode_values(self):
        assert FactorRiskTableMode.Pnl.value == 'Pnl'
        assert FactorRiskTableMode.Exposure.value == 'Exposure'
        assert FactorRiskTableMode.ZScore.value == 'ZScore'
        assert FactorRiskTableMode.Mctr.value == 'Mctr'


# ── create_report ────────────────────────────────────────────────────

class TestCreateReport:
    def test_create_report(self, mocker):
        session = _mock_session(mocker)
        report = MagicMock(spec=Report)
        mocker.patch.object(session.sync, 'post', return_value=report)
        result = GsReportApi.create_report(report)
        session.sync.post.assert_called_once_with('/reports', report, cls=Report)
        assert result is report


# ── get_report ───────────────────────────────────────────────────────

class TestGetReport:
    def test_get_report(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='r1')
        result = GsReportApi.get_report('R1')
        session.sync.get.assert_called_once_with('/reports/R1', cls=Report)
        assert result == 'r1'


# ── get_reports ──────────────────────────────────────────────────────

class TestGetReports:
    def _make_report(self, id_, tags=None):
        params = MagicMock()
        params.tags = tags
        r = MagicMock(spec=Report)
        r.parameters = params
        r.id = id_
        return r

    def test_minimal_no_scroll(self, mocker):
        session = _mock_session(mocker)
        r1 = self._make_report('R1', tags=None)
        mocker.patch.object(session.sync, 'get', return_value={'results': [r1]})
        result = GsReportApi.get_reports()
        assert result == (r1,)
        session.sync.get.assert_called_once_with('/reports?limit=100', cls=Report)

    def test_all_url_params(self, mocker):
        session = _mock_session(mocker)
        r1 = self._make_report('R1', tags=None)
        mocker.patch.object(session.sync, 'get', return_value={'results': [r1]})
        GsReportApi.get_reports(
            limit=50,
            offset=10,
            position_source_type='Portfolio',
            position_source_id='MP1',
            status='done',
            report_type='Portfolio Performance Analytics',
            order_by='id',
            scroll='1m',
        )
        url = session.sync.get.call_args[0][0]
        assert 'limit=50' in url
        assert 'offset=10' in url
        assert 'positionSourceType=Portfolio' in url
        assert 'positionSourceId=MP1' in url
        assert 'status=done' in url
        assert 'reportType=Portfolio%20Performance%20Analytics' in url
        assert 'orderBy=id' in url
        assert 'scroll=1m' in url

    def test_scroll_pagination(self, mocker):
        session = _mock_session(mocker)
        r1 = self._make_report('R1', tags=None)
        r2 = self._make_report('R2', tags=None)
        page1 = {'results': [r1], 'scrollId': 'scr1'}
        page2 = {'results': [r2], 'scrollId': 'scr2'}
        page3 = {'results': [], 'scrollId': 'scr3'}
        mocker.patch.object(session.sync, 'get', side_effect=[page1, page2, page3])
        result = GsReportApi.get_reports(scroll='1m')
        assert result == (r1, r2)
        assert session.sync.get.call_count == 3

    def test_scroll_no_scroll_id_stops(self, mocker):
        session = _mock_session(mocker)
        r1 = self._make_report('R1', tags=None)
        page1 = {'results': [r1]}
        mocker.patch.object(session.sync, 'get', return_value=page1)
        result = GsReportApi.get_reports()
        assert result == (r1,)
        assert session.sync.get.call_count == 1

    def test_tags_filter_matching(self, mocker):
        session = _mock_session(mocker)
        tag_tuple = (PositionTag(name='region', value='US'),)
        r1 = self._make_report('R1', tags=tag_tuple)
        r2 = self._make_report('R2', tags=(PositionTag(name='region', value='EU'),))
        mocker.patch.object(session.sync, 'get', return_value={'results': [r1, r2]})
        result = GsReportApi.get_reports(tags={'region': 'US'})
        assert result == (r1,)

    def test_tags_filter_empty(self, mocker):
        session = _mock_session(mocker)
        r1 = self._make_report('R1', tags=None)
        r2 = self._make_report('R2', tags=(PositionTag(name='k', value='v'),))
        mocker.patch.object(session.sync, 'get', return_value={'results': [r1, r2]})
        result = GsReportApi.get_reports()
        assert result == (r1,)


# ── update_report ────────────────────────────────────────────────────

class TestUpdateReport:
    def test_update_report(self, mocker):
        session = _mock_session(mocker)
        report = MagicMock(spec=Report)
        report.id = 'R1'
        mocker.patch.object(session.sync, 'put', return_value='updated')
        result = GsReportApi.update_report(report)
        session.sync.put.assert_called_once_with('/reports/R1', report, cls=Report)
        assert result == 'updated'


# ── delete_report ────────────────────────────────────────────────────

class TestDeleteReport:
    def test_delete_report(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'delete', return_value='deleted')
        result = GsReportApi.delete_report('R1')
        session.sync.delete.assert_called_once_with('/reports/R1')
        assert result == 'deleted'


# ── schedule_report ──────────────────────────────────────────────────

class TestScheduleReport:
    def test_without_backcast(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='scheduled')
        result = GsReportApi.schedule_report('R1', dt.date(2023, 1, 1), dt.date(2023, 6, 30))
        expected_payload = {'startDate': '2023-01-01', 'endDate': '2023-06-30'}
        session.sync.post.assert_called_once_with('/reports/R1/schedule', expected_payload)
        assert result == 'scheduled'

    def test_with_backcast(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='scheduled')
        GsReportApi.schedule_report('R1', dt.date(2023, 1, 1), dt.date(2023, 6, 30), backcast=True)
        payload = session.sync.post.call_args[0][1]
        assert payload['parameters'] == {'backcast': True}


# ── get_report_status ────────────────────────────────────────────────

class TestGetReportStatus:
    def test_get_report_status(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='status')
        result = GsReportApi.get_report_status('R1')
        session.sync.get.assert_called_once_with('/reports/R1/status')
        assert result == 'status'


# ── get_report_jobs ──────────────────────────────────────────────────

class TestGetReportJobs:
    def test_get_report_jobs(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['j1', 'j2']})
        result = GsReportApi.get_report_jobs('R1')
        session.sync.get.assert_called_once_with('/reports/R1/jobs')
        assert result == ['j1', 'j2']


# ── get_report_job ───────────────────────────────────────────────────

class TestGetReportJob:
    def test_get_report_job(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='job')
        result = GsReportApi.get_report_job('J1')
        session.sync.get.assert_called_once_with('/reports/jobs/J1')
        assert result == 'job'


# ── reschedule_report_job ────────────────────────────────────────────

class TestRescheduleReportJob:
    def test_reschedule_report_job(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='rescheduled')
        result = GsReportApi.reschedule_report_job('J1')
        session.sync.post.assert_called_once_with('/reports/jobs/J1/reschedule', {})
        assert result == 'rescheduled'


# ── cancel_report_job ────────────────────────────────────────────────

class TestCancelReportJob:
    def test_cancel_report_job(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='cancelled')
        result = GsReportApi.cancel_report_job('J1')
        session.sync.post.assert_called_once_with('/reports/jobs/J1/cancel')
        assert result == 'cancelled'


# ── update_report_job ────────────────────────────────────────────────

class TestUpdateReportJob:
    def test_update_report_job(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='updated')
        result = GsReportApi.update_report_job('J1', 'done')
        session.sync.post.assert_called_once_with('/reports/jobs/J1/update', {'status': 'done'})
        assert result == 'updated'


# ── get_custom_aum ───────────────────────────────────────────────────

class TestGetCustomAum:
    def test_no_dates(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'data': [{'value': 100}]})
        result = GsReportApi.get_custom_aum('R1')
        url = session.sync.get.call_args[0][0]
        assert url == '/reports/R1/aum?'
        assert result == [{'value': 100}]

    def test_with_start_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'data': []})
        GsReportApi.get_custom_aum('R1', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url

    def test_with_end_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'data': []})
        GsReportApi.get_custom_aum('R1', end_date=dt.date(2023, 6, 30))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-30' in url

    def test_with_both_dates(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'data': []})
        GsReportApi.get_custom_aum('R1', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 30))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url


# ── upload_custom_aum ────────────────────────────────────────────────

class TestUploadCustomAum:
    def test_without_clear_existing(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        result = GsReportApi.upload_custom_aum('R1', [{'date': '2023-01-01', 'aum': 100}])
        session.sync.post.assert_called_once_with('/reports/R1/aum', {'data': [{'date': '2023-01-01', 'aum': 100}]})
        assert result == 'ok'

    def test_with_clear_existing(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsReportApi.upload_custom_aum('R1', [{'aum': 200}], clear_existing_data=True)
        url = session.sync.post.call_args[0][0]
        assert '?clearExistingData=true' in url


# ── get_factor_risk_report_results ───────────────────────────────────

class TestGetFactorRiskReportResults:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsReportApi.get_factor_risk_report_results('RR1')
        url = session.sync.get.call_args[0][0]
        assert url == '/risk/factors/reports/RR1/results?'

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsReportApi.get_factor_risk_report_results(
            'RR1',
            view='Risk',
            factors=['Value', 'Automobiles & Components'],
            factor_categories=['Style', 'Industry'],
            currency=Currency.USD,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
            unit='Percent',
        )
        url = session.sync.get.call_args[0][0]
        assert '&view=Risk' in url
        assert '&factors=' in url
        assert 'Automobiles' in url
        assert '&factorCategories=Style&factorCategories=Industry' in url
        assert '&currency=USD' in url
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url
        assert '&unit=Percent' in url


# ── get_factor_risk_report_view ──────────────────────────────────────

class TestGetFactorRiskReportView:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'summary': {}})
        result = GsReportApi.get_factor_risk_report_view('RR1')
        session.sync.get.assert_called_once()
        url = session.sync.get.call_args[0][0]
        assert '/factor/risk/RR1/views?' in url

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'summary': {}})
        GsReportApi.get_factor_risk_report_view(
            'RR1',
            factor='Value',
            factor_category='Style',
            currency=Currency.EUR,
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
            unit='Percent',
        )
        url = session.sync.get.call_args[0][0]
        assert 'factor=Value' in url
        assert 'factorCategory=Style' in url
        assert 'currency=EUR' in url
        assert 'startDate=2023-01-01' in url
        assert 'endDate=2023-06-30' in url
        assert 'unit=Percent' in url


# ── get_factor_risk_report_table ─────────────────────────────────────

class TestGetFactorRiskReportTable:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'table': {}})
        result = GsReportApi.get_factor_risk_report_table('RR1')
        url = session.sync.get.call_args[0][0]
        assert '/factor/risk/RR1/tables?' in url

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'table': {}})
        GsReportApi.get_factor_risk_report_table(
            'RR1',
            mode=FactorRiskTableMode.Pnl,
            unit='Percent',
            currency=Currency.GBP,
            date=dt.date(2023, 3, 15),
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
        )
        url = session.sync.get.call_args[0][0]
        assert '&mode=Pnl' in url
        assert '&unit=Percent' in url
        assert '&currency=GBP' in url
        assert '&date=2023-03-15' in url
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url


# ── get_brinson_attribution_results ──────────────────────────────────

class TestGetBrinsonAttributionResults:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsReportApi.get_brinson_attribution_results('P1')
        url = session.sync.get.call_args[0][0]
        assert url == '/attribution/P1/brinson?'

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsReportApi.get_brinson_attribution_results(
            'P1',
            benchmark='BM1',
            currency=Currency.JPY,
            include_interaction=True,
            aggregation_type='Sector',
            aggregation_category='GICS',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
        )
        url = session.sync.get.call_args[0][0]
        assert '&benchmark=BM1' in url
        assert '&currency=JPY' in url
        assert '&includeInteraction=true' in url
        assert '&aggregationType=Sector' in url
        assert '&aggregationCategory=GICS' in url
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url

    def test_include_interaction_false(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsReportApi.get_brinson_attribution_results('P1', include_interaction=False)
        url = session.sync.get.call_args[0][0]
        assert '&includeInteraction=false' in url
