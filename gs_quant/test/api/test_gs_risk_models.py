"""
Tests for gs_quant.api.gs.risk_models - GsRiskModelApi and GsFactorRiskModelApi
Targets 100% branch coverage.
"""

import datetime as dt
from unittest.mock import MagicMock

import pytest

from gs_quant.api.gs.risk_models import (
    GsRiskModelApi,
    GsFactorRiskModelApi,
    IntradayFactorDataSource,
)
from gs_quant.session import GsSession, Environment
from gs_quant.target.risk_models import (
    RiskModel,
    RiskModelCalendar,
    Factor,
    RiskModelData,
    RiskModelDataAssetsRequest,
    RiskModelDataMeasure,
    RiskModelEventType,
    RiskModelTerm,
)


def _mock_session(mocker):
    mocker.patch.object(
        GsSession.__class__,
        'default_value',
        return_value=GsSession.get(Environment.QA, 'client_id', 'secret'),
    )
    return GsSession.current


# ── IntradayFactorDataSource enum ────────────────────────────────────

class TestIntradayFactorDataSource:
    def test_enum_values(self):
        assert IntradayFactorDataSource.GS_FMP.value == "GS_FMP"
        assert IntradayFactorDataSource.GS_REGRESSION.value == "GS_Regression"
        assert IntradayFactorDataSource.BARRA.value == "BARRA"
        assert IntradayFactorDataSource.AXIOMA.value == "AXIOMA"
        assert IntradayFactorDataSource.WOLFE.value == "WOLFE"
        assert IntradayFactorDataSource.QI.value == "QI"


# ── GsRiskModelApi ──────────────────────────────────────────────────

class TestCreateRiskModel:
    def test_create_risk_model(self, mocker):
        session = _mock_session(mocker)
        model = MagicMock(spec=RiskModel)
        mocker.patch.object(session.sync, 'post', return_value=model)
        result = GsRiskModelApi.create_risk_model(model)
        session.sync.post.assert_called_once_with('/risk/models', model, cls=RiskModel)
        assert result is model


class TestGetRiskModel:
    def test_get_risk_model(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='model')
        result = GsRiskModelApi.get_risk_model('MODEL1')
        session.sync.get.assert_called_once_with('/risk/models/MODEL1', cls=RiskModel)
        assert result == 'model'


class TestGetRiskModels:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsRiskModelApi.get_risk_models()
        url = session.sync.get.call_args[0][0]
        assert url == '/risk/models?'
        assert result == []

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['m1']})
        GsRiskModelApi.get_risk_models(
            ids=['ID1', 'ID2'],
            limit=50,
            offset=10,
            terms=['Daily'],
            versions=['V1', 'V2'],
            vendors=['GS', 'MSCI'],
            names=['Model1', 'Model2'],
            types=['Factor', 'Thematic'],
            coverages=['Global', 'US'],
        )
        url = session.sync.get.call_args[0][0]
        assert '&limit=50' in url
        assert '&id=ID1&id=ID2' in url
        assert '&offset=10' in url
        assert "&term=['Daily']" in url
        assert '&version=V1&version=V2' in url
        assert '&vendor=GS&vendor=MSCI' in url
        assert '&name=Model1&name=Model2' in url
        assert '&type=Factor&type=Thematic' in url
        assert '&coverage=Global&coverage=US' in url

    def test_names_none_no_coverage_no_types(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsRiskModelApi.get_risk_models(names=None, coverages=None, types=None)
        url = session.sync.get.call_args[0][0]
        assert '&name=' not in url
        assert '&coverage=' not in url
        assert '&type=' not in url


class TestUpdateRiskModel:
    def test_update_risk_model(self, mocker):
        session = _mock_session(mocker)
        model = MagicMock(spec=RiskModel)
        model.id = 'M1'
        mocker.patch.object(session.sync, 'put', return_value=model)
        result = GsRiskModelApi.update_risk_model(model)
        session.sync.put.assert_called_once_with('/risk/models/M1', model, cls=RiskModel)
        assert result is model


class TestDeleteRiskModel:
    def test_delete_risk_model(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'delete', return_value='deleted')
        result = GsRiskModelApi.delete_risk_model('M1')
        session.sync.delete.assert_called_once_with('/risk/models/M1')
        assert result == 'deleted'


class TestGetRiskModelCalendar:
    def test_get_risk_model_calendar(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='calendar')
        result = GsRiskModelApi.get_risk_model_calendar('M1')
        session.sync.get.assert_called_once_with('/risk/models/M1/calendar', cls=RiskModelCalendar)
        assert result == 'calendar'


class TestUploadRiskModelCalendar:
    def test_upload_risk_model_calendar(self, mocker):
        session = _mock_session(mocker)
        cal = MagicMock(spec=RiskModelCalendar)
        mocker.patch.object(session.sync, 'put', return_value='uploaded')
        result = GsRiskModelApi.upload_risk_model_calendar('M1', cal)
        session.sync.put.assert_called_once_with('/risk/models/M1/calendar', cal, cls=RiskModelCalendar)
        assert result == 'uploaded'


class TestGetRiskModelDates:
    def test_no_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['2023-01-01']})
        result = GsRiskModelApi.get_risk_model_dates('M1')
        url = session.sync.get.call_args[0][0]
        assert url == '/risk/models/M1/dates?'
        assert result == ['2023-01-01']

    def test_with_start_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsRiskModelApi.get_risk_model_dates('M1', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url

    def test_with_end_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsRiskModelApi.get_risk_model_dates('M1', end_date=dt.date(2023, 6, 30))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-30' in url

    def test_with_event_type(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsRiskModelApi.get_risk_model_dates('M1', event_type=RiskModelEventType.Risk_Model)
        url = session.sync.get.call_args[0][0]
        assert '&eventType=Risk Model' in url

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsRiskModelApi.get_risk_model_dates(
            'M1',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
            event_type=RiskModelEventType.Risk_Model,
        )
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url
        assert '&eventType=Risk Model' in url


# ── GsFactorRiskModelApi ────────────────────────────────────────────

class TestGsFactorRiskModelApiInit:
    def test_init(self):
        api = GsFactorRiskModelApi()
        assert api is not None


class TestGetRiskModelFactors:
    def test_get_risk_model_factors(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['f1', 'f2']})
        result = GsFactorRiskModelApi.get_risk_model_factors('M1')
        session.sync.get.assert_called_once_with('/risk/models/M1/factors', cls=Factor)
        assert result == ['f1', 'f2']


class TestCreateRiskModelFactor:
    def test_create_risk_model_factor(self, mocker):
        session = _mock_session(mocker)
        factor = MagicMock(spec=Factor)
        mocker.patch.object(session.sync, 'post', return_value=factor)
        result = GsFactorRiskModelApi.create_risk_model_factor('M1', factor)
        session.sync.post.assert_called_once_with('/risk/models/M1/factors', factor, cls=Factor)
        assert result is factor


class TestGetRiskModelFactor:
    def test_get_risk_model_factor(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value='factor_data')
        result = GsFactorRiskModelApi.get_risk_model_factor('M1', 'F1')
        session.sync.get.assert_called_once_with('/risk/models/M1/factors/F1')
        assert result == 'factor_data'


class TestUpdateRiskModelFactor:
    def test_update_risk_model_factor(self, mocker):
        session = _mock_session(mocker)
        factor = MagicMock(spec=Factor)
        factor.identifier = 'F1'
        mocker.patch.object(session.sync, 'put', return_value='updated')
        result = GsFactorRiskModelApi.update_risk_model_factor('M1', factor)
        session.sync.put.assert_called_once_with('/risk/models/M1/factors/F1', factor, cls=Factor)
        assert result == 'updated'


class TestDeleteRiskModelFactor:
    def test_delete_risk_model_factor(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'delete', return_value='deleted')
        result = GsFactorRiskModelApi.delete_risk_model_factor('M1', 'F1')
        session.sync.delete.assert_called_once_with('/risk/models/M1/factors/F1')
        assert result == 'deleted'


class TestGetRiskModelFactorData:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsFactorRiskModelApi.get_risk_model_factor_data('M1')
        url = session.sync.get.call_args[0][0]
        assert url == '/risk/models/M1/factors/data?'
        assert result == []

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['d1']})
        GsFactorRiskModelApi.get_risk_model_factor_data(
            'M1',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
            identifiers=['ID1', 'ID2'],
            include_performance_curve=True,
            factor_categories=['Style', 'Industry'],
            names=['Value', 'Momentum'],
        )
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-30' in url
        assert '&identifiers=ID1&identifiers=ID2' in url
        assert '&includePerformanceCurve=true' in url
        assert '&name=Value&name=Momentum' in url
        assert '&factorCategory=Style&factorCategory=Industry' in url

    def test_include_performance_curve_false(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsFactorRiskModelApi.get_risk_model_factor_data('M1', include_performance_curve=False)
        url = session.sync.get.call_args[0][0]
        assert 'includePerformanceCurve' not in url


class TestGetRiskModelFactorDataIntraday:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        result = GsFactorRiskModelApi.get_risk_model_factor_data_intraday('M1')
        url = session.sync.get.call_args[0][0]
        assert url == '/risk/models/M1/factors/data/intraday?'
        assert result == []

    def test_all_params_with_enum_data_source(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['d1']})
        GsFactorRiskModelApi.get_risk_model_factor_data_intraday(
            'M1',
            start_time=dt.datetime(2023, 1, 1, 10, 0, 0),
            end_time=dt.datetime(2023, 1, 1, 16, 0, 0),
            factor_ids=['FID1', 'FID2'],
            factor_categories=['Style'],
            factors=['Value', 'Momentum'],
            data_source=IntradayFactorDataSource.GS_FMP,
        )
        url = session.sync.get.call_args[0][0]
        assert '&startTime=2023-01-01T10:00:00Z' in url
        assert '&endTime=2023-01-01T16:00:00Z' in url
        assert '&factorId=FID1&factorId=FID2' in url
        assert '&dataSource=GS_FMP' in url
        assert '&factor=Value&factor=Momentum' in url
        assert '&factorCategory=Style' in url

    def test_data_source_as_string(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsFactorRiskModelApi.get_risk_model_factor_data_intraday('M1', data_source='CustomSource')
        url = session.sync.get.call_args[0][0]
        assert '&dataSource=CustomSource' in url

    def test_no_data_source(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': []})
        GsFactorRiskModelApi.get_risk_model_factor_data_intraday('M1', data_source=None)
        url = session.sync.get.call_args[0][0]
        assert 'dataSource' not in url


class TestGetRiskModelCoverage:
    def test_minimal(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value={'results': []})
        result = GsFactorRiskModelApi.get_risk_model_coverage()
        session.sync.post.assert_called_once_with('/risk/models/coverage', {}, timeout=200)
        assert result == []

    def test_all_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value={'results': ['c1']})
        result = GsFactorRiskModelApi.get_risk_model_coverage(
            asset_ids=['A1', 'A2'],
            as_of_date=dt.datetime(2023, 1, 1),
            sort_by_term=RiskModelTerm.Daily,
        )
        query = session.sync.post.call_args[0][1]
        assert query['assetIds'] == ['A1', 'A2']
        assert query['asOfDate'] == '2023-01-01'
        assert query['sortByTerm'] == RiskModelTerm.Daily
        assert result == ['c1']


class TestUploadRiskModelData:
    def test_no_partial_no_aws(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        result = GsFactorRiskModelApi.upload_risk_model_data('M1', {'data': 'test'})
        session.sync.post.assert_called_once_with('/risk/models/data/M1', {'data': 'test'}, timeout=200)
        assert result == 'ok'

    def test_no_partial_with_aws(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data('M1', {'data': 'test'}, aws_upload=True)
        url = session.sync.post.call_args[0][0]
        assert url == '/risk/models/data/M1?awsUpload=true'

    def test_partial_only(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data('M1', {'data': 'test'}, partial_upload=True)
        url = session.sync.post.call_args[0][0]
        assert '?partialUpload=true' in url
        assert 'targetUniverseSize' not in url
        assert 'finalUpload' not in url

    def test_partial_with_target_universe_size(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'}, partial_upload=True, target_universe_size=500.0
        )
        url = session.sync.post.call_args[0][0]
        assert '&targetUniverseSize=500.0' in url

    def test_partial_with_final_upload_true(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'}, partial_upload=True, final_upload=True
        )
        url = session.sync.post.call_args[0][0]
        assert '&finalUpload=true' in url

    def test_partial_with_final_upload_false(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'}, partial_upload=True, final_upload=False
        )
        url = session.sync.post.call_args[0][0]
        assert '&finalUpload=false' in url

    def test_partial_with_aws_upload(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'}, partial_upload=True, aws_upload=True
        )
        url = session.sync.post.call_args[0][0]
        assert '?partialUpload=true' in url
        assert '&awsUpload=true' in url

    def test_partial_all_options(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'},
            partial_upload=True,
            target_universe_size=1000.0,
            final_upload=True,
            aws_upload=True,
        )
        url = session.sync.post.call_args[0][0]
        assert '?partialUpload=true' in url
        assert '&targetUniverseSize=1000.0' in url
        assert '&finalUpload=true' in url
        assert '&awsUpload=true' in url

    def test_partial_no_target_universe_size(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value='ok')
        GsFactorRiskModelApi.upload_risk_model_data(
            'M1', {'data': 'test'}, partial_upload=True, target_universe_size=0
        )
        url = session.sync.post.call_args[0][0]
        assert 'targetUniverseSize' not in url


class TestGetRiskModelData:
    def test_with_end_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value={'data': []})
        result = GsFactorRiskModelApi.get_risk_model_data(
            'M1', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 30)
        )
        query = session.sync.post.call_args[0][1]
        assert query['startDate'] == '2023-01-01'
        assert query['endDate'] == '2023-06-30'

    def test_without_end_date(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'get', return_value={'results': ['2023-03-01', '2023-03-15']})
        mocker.patch.object(session.sync, 'post', return_value={'data': []})
        result = GsFactorRiskModelApi.get_risk_model_data('M1', start_date=dt.date(2023, 1, 1))
        query = session.sync.post.call_args[0][1]
        assert query['endDate'] == '2023-03-15'

    def test_all_optional_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value={'data': []})
        assets = MagicMock(spec=RiskModelDataAssetsRequest)
        measures = [MagicMock(spec=RiskModelDataMeasure)]
        result = GsFactorRiskModelApi.get_risk_model_data(
            'M1',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 30),
            assets=assets,
            measures=measures,
            factors=['F1', 'F2'],
            limit_factors=True,
        )
        query = session.sync.post.call_args[0][1]
        assert query['assets'] is assets
        assert query['measures'] is measures
        assert query['factors'] == ['F1', 'F2']
        assert query['limitFactors'] is True

    def test_no_optional_params(self, mocker):
        session = _mock_session(mocker)
        mocker.patch.object(session.sync, 'post', return_value={'data': []})
        GsFactorRiskModelApi.get_risk_model_data(
            'M1', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 30)
        )
        query = session.sync.post.call_args[0][1]
        assert 'assets' not in query
        assert 'measures' not in query
        assert 'factors' not in query
        assert 'limitFactors' not in query
