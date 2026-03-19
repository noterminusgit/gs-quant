"""
Copyright 2021 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import pandas as pd
from pandas._testing import assert_frame_equal

from gs_quant.common import Currency
from gs_quant.errors import MqValueError, MqRequestError
from gs_quant.models.risk_model import (
    FactorRiskModel,
    MacroRiskModel,
    ThematicRiskModel,
    MarqueeRiskModel,
    RiskModel,
    ReturnFormat,
    Unit,
    FactorType,
)
from gs_quant.models.risk_model_utils import get_optional_data_as_dataframe, _map_measure_to_field_name
from gs_quant.session import GsSession, Environment
from gs_quant.target.risk_models import (
    RiskModel as Risk_Model,
    RiskModelCoverage,
    RiskModelTerm,
    RiskModelUniverseIdentifier,
    RiskModelType,
    RiskModelDataAssetsRequest as DataAssetsRequest,
    RiskModelDataMeasure as Measure,
    RiskModelUniverseIdentifierRequest as UniverseIdentifier,
    RiskModelCalendar,
    RiskModelData,
    Entitlements,
    Factor as RiskModelFactor,
)

empty_entitlements = {"execute": [], "edit": [], "view": [], "admin": [], "query": [], "upload": []}

mock_risk_model_obj = Risk_Model(
    RiskModelCoverage.Country,
    'model_id',
    'Fake Risk Model',
    RiskModelTerm.Long,
    RiskModelUniverseIdentifier.gsid,
    'GS',
    1.0,
    universe_size=10000,
    entitlements=empty_entitlements,
    description='Test',
    expected_update_time='00:00:00',
    type=RiskModelType.Factor,
)

mock_macro_risk_model_obj = Risk_Model(
    coverage=RiskModelCoverage.Country,
    id='macro_model_id',
    name='Fake Risk Model',
    term=RiskModelTerm.Long,
    universe_identifier=RiskModelUniverseIdentifier.gsid,
    vendor='GS',
    version=1.0,
    entitlements=empty_entitlements,
    description='Test',
    expected_update_time='00:00:00',
    type=RiskModelType.Macro,
)

mock_thematic_risk_model_obj = Risk_Model(
    coverage=RiskModelCoverage.Country,
    id='thematic_model_id',
    name='Fake Thematic Model',
    term=RiskModelTerm.Long,
    universe_identifier=RiskModelUniverseIdentifier.gsid,
    vendor='GS',
    version=1.0,
    entitlements=empty_entitlements,
    description='Thematic Test',
    expected_update_time='00:00:00',
    type=RiskModelType.Thematic,
)


def mock_risk_model(mocker):
    from gs_quant.session import OAuth2Session

    OAuth2Session.init = mock.MagicMock(return_value=None)
    GsSession.use(Environment.QA, 'client_id', 'secret')
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'post', return_value=mock_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'put', return_value=mock_risk_model_obj)
    return FactorRiskModel.get('model_id')


def mock_macro_risk_model(mocker):
    from gs_quant.session import OAuth2Session

    OAuth2Session.init = mock.MagicMock(return_value=None)
    GsSession.use(Environment.QA, 'client_id', 'secret')
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'post', return_value=mock_macro_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_macro_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'put', return_value=mock_macro_risk_model_obj)
    return MacroRiskModel.get('macro_model_id')


def mock_thematic_risk_model(mocker):
    from gs_quant.session import OAuth2Session

    OAuth2Session.init = mock.MagicMock(return_value=None)
    GsSession.use(Environment.QA, 'client_id', 'secret')
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'post', return_value=mock_thematic_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_thematic_risk_model_obj)
    mocker.patch.object(GsSession.current.sync, 'put', return_value=mock_thematic_risk_model_obj)
    return ThematicRiskModel.get('thematic_model_id')


# ==================== Base RiskModel class tests ====================


class TestRiskModelBase:
    """Test the base RiskModel class"""

    def test_risk_model_init(self):
        rm = RiskModel('test_id', 'Test Model')
        assert rm.id == 'test_id'
        assert rm.name == 'Test Model'

    def test_risk_model_name_setter(self):
        rm = RiskModel('test_id', 'Test Model')
        rm.name = 'New Name'
        assert rm.name == 'New Name'

    def test_risk_model_str(self):
        rm = RiskModel('test_id', 'Test Model')
        assert str(rm) == 'test_id'

    def test_risk_model_repr(self):
        rm = RiskModel('test_id', 'Test Model')
        r = repr(rm)
        assert "RiskModel" in r
        assert "test_id" in r
        assert "Test Model" in r


# ==================== Enum tests ====================


class TestEnums:
    def test_return_format(self):
        assert ReturnFormat.JSON is not None
        assert ReturnFormat.DATA_FRAME is not None

    def test_unit(self):
        assert Unit.PERCENT is not None
        assert Unit.STANDARD_DEVIATION is not None

    def test_factor_type(self):
        assert FactorType.Factor.value == 'Factor'
        assert FactorType.Category.value == 'Category'

    def test_factor_type_repr(self):
        assert repr(FactorType.Factor) == 'Factor'
        assert repr(FactorType.Category) == 'Category'


# ==================== MarqueeRiskModel tests ====================


class TestMarqueeRiskModel:

    def test_init_with_string_type(self):
        """Test MarqueeRiskModel with string type_ (branch: isinstance check)"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_='Factor',
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        assert model.type == RiskModelType.Factor

    def test_init_with_enum_type(self):
        """Test MarqueeRiskModel with RiskModelType enum"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Macro,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        assert model.type == RiskModelType.Macro

    def test_init_with_entitlements_dict(self):
        """Test entitlements branch: isinstance Dict"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            entitlements={"execute": [], "edit": [], "view": [], "admin": [], "query": [], "upload": []},
        )
        assert model.entitlements is not None

    def test_init_with_entitlements_object(self):
        """Test entitlements branch: isinstance Entitlements"""
        ent = Entitlements(view=('user1',), edit=('user2',))
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            entitlements=ent,
        )
        assert model.entitlements is ent

    def test_init_with_no_entitlements(self):
        """Test entitlements branch: None"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            entitlements=None,
        )
        assert model.entitlements is None

    def test_property_setters(self):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        model.type = RiskModelType.Macro
        assert model.type == RiskModelType.Macro

        model.vendor = 'NewVendor'
        assert model.vendor == 'NewVendor'

        model.version = 2.0
        assert model.version == 2.0

        model.coverage = RiskModelCoverage.Global
        assert model.coverage == RiskModelCoverage.Global

        model.term = RiskModelTerm.Short
        assert model.term == RiskModelTerm.Short

        model.description = 'New Description'
        assert model.description == 'New Description'

        model.universe_size = 5000
        assert model.universe_size == 5000

        model.entitlements = {'view': ['guid:a']}
        assert model.entitlements == {'view': ['guid:a']}

        model.expected_update_time = dt.time(12, 0)
        assert model.expected_update_time == dt.time(12, 0)

    def test_universe_identifier_property(self):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        assert model.universe_identifier == RiskModelUniverseIdentifier.gsid

    def test_str_and_repr(self):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            universe_size=5000,
            description='A model',
            expected_update_time=dt.time(10, 0),
            entitlements={'view': []},
        )
        assert str(model) == 'test_id'
        r = repr(model)
        assert 'test_id' in r
        assert 'universe_size=' in r
        assert 'description=' in r
        assert 'expected_update_time=' in r
        assert 'entitlements=' in r

    def test_repr_without_optional_fields(self):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        r = repr(model)
        assert 'universe_size=' not in r
        assert 'description=' not in r
        assert 'expected_update_time=' not in r
        assert 'entitlements=' not in r

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_delete(self, mock_api):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        model.delete()
        mock_api.delete_risk_model.assert_called_once_with('test_id')

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_dates(self, mock_api):
        mock_api.get_risk_model_dates.return_value = ['2022-01-03', '2022-01-04']
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        dates = model.get_dates(dt.date(2022, 1, 1), dt.date(2022, 1, 5))
        assert dates == [dt.date(2022, 1, 3), dt.date(2022, 1, 4)]

    @patch('gs_quant.models.risk_model.get_closest_date_index')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_calendar_no_dates(self, mock_api, mock_closest):
        """Test get_calendar with no start_date and no end_date"""
        cal = RiskModelCalendar(('2022-01-03', '2022-01-04', '2022-01-05'))
        mock_api.get_risk_model_calendar.return_value = cal
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        result = model.get_calendar()
        assert result == cal
        mock_closest.assert_not_called()

    @patch('gs_quant.models.risk_model.get_closest_date_index')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_calendar_with_start_date_only(self, mock_api, mock_closest):
        """Test get_calendar with start_date only - branch: start_date but not end_date"""
        cal = RiskModelCalendar(('2022-01-03', '2022-01-04', '2022-01-05'))
        mock_api.get_risk_model_calendar.return_value = cal
        mock_closest.return_value = 1  # start_idx
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        result = model.get_calendar(start_date=dt.date(2022, 1, 4))
        # end_idx defaults to len(business_dates)=3 since no end_date
        assert result is not None

    @patch('gs_quant.models.risk_model.get_closest_date_index')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_calendar_with_end_date_only(self, mock_api, mock_closest):
        """Test get_calendar with end_date only - branch: end_date but not start_date"""
        cal = RiskModelCalendar(('2022-01-03', '2022-01-04', '2022-01-05'))
        mock_api.get_risk_model_calendar.return_value = cal
        mock_closest.return_value = 1  # end_idx
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        result = model.get_calendar(end_date=dt.date(2022, 1, 4))
        # start_idx defaults to 0 since no start_date
        assert result is not None

    @patch('gs_quant.models.risk_model.get_closest_date_index')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_calendar_with_both_dates(self, mock_api, mock_closest):
        cal = RiskModelCalendar(('2022-01-03', '2022-01-04', '2022-01-05'))
        mock_api.get_risk_model_calendar.return_value = cal
        mock_closest.side_effect = [0, 2]
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        result = model.get_calendar(start_date=dt.date(2022, 1, 3), end_date=dt.date(2022, 1, 5))
        assert result is not None

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_upload_calendar(self, mock_api):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        cal = RiskModelCalendar(('2022-01-03',))
        model.upload_calendar(cal)
        mock_api.upload_risk_model_calendar.assert_called_once_with('test_id', cal)

    def test_get_missing_dates_no_start_no_end(self):
        """Test get_missing_dates when start_date=None and end_date=None
        Branches covered: not start_date (True), not end_date (True)"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        posted = [dt.date(2022, 1, 3), dt.date(2022, 1, 5)]
        # get_calendar returns a RiskModelCalendar with string business_dates
        # get_missing_dates then calls strptime on these strings
        cal_result = MagicMock()
        cal_result.business_dates = ['2022-01-03', '2022-01-04', '2022-01-05']
        with patch.object(model, 'get_dates', return_value=posted):
            with patch.object(model, 'get_calendar', return_value=cal_result):
                missing = model.get_missing_dates()
                assert dt.date(2022, 1, 4) in missing

    def test_get_missing_dates_with_start_and_end(self):
        """Test get_missing_dates when start_date and end_date are provided
        Branches covered: not start_date (False), not end_date (False)"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        posted = [dt.date(2022, 1, 3), dt.date(2022, 1, 5)]
        cal_result = MagicMock()
        cal_result.business_dates = ['2022-01-03', '2022-01-04', '2022-01-05']
        with patch.object(model, 'get_dates', return_value=posted):
            with patch.object(model, 'get_calendar', return_value=cal_result):
                missing = model.get_missing_dates(start_date=dt.date(2022, 1, 3), end_date=dt.date(2022, 1, 5))
                assert dt.date(2022, 1, 4) in missing

    def test_get_most_recent_date_from_calendar(self):
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        cal_result = MagicMock()
        cal_result.business_dates = ['2022-01-03', '2022-01-04', '2022-01-05']
        with patch.object(model, 'get_calendar', return_value=cal_result):
            result = model.get_most_recent_date_from_calendar()
            assert result == dt.date(2022, 1, 5)

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_save_creates_new_model(self, mock_api):
        """Test save() when create succeeds (no MqRequestError)"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            expected_update_time=dt.time(10, 30),
        )
        model.save()
        mock_api.create_risk_model.assert_called_once()
        mock_api.update_risk_model.assert_not_called()

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_save_updates_existing_model(self, mock_api):
        """Test save() when create raises MqRequestError -> falls through to update"""
        mock_api.create_risk_model.side_effect = MqRequestError(409, 'Already exists')
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
        )
        model.save()
        mock_api.create_risk_model.assert_called_once()
        mock_api.update_risk_model.assert_called_once()

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_save_without_expected_update_time(self, mock_api):
        """Test save() branch: expected_update_time is None"""
        model = MarqueeRiskModel(
            id_='test_id',
            name='Test',
            type_=RiskModelType.Factor,
            vendor='GS',
            version=1.0,
            coverage=RiskModelCoverage.Country,
            universe_identifier=RiskModelUniverseIdentifier.gsid,
            term=RiskModelTerm.Long,
            expected_update_time=None,
        )
        model.save()
        mock_api.create_risk_model.assert_called_once()

    def test_from_target_with_all_fields(self):
        model = MarqueeRiskModel.from_target(mock_risk_model_obj)
        assert model.id == 'model_id'
        assert model.name == 'Fake Risk Model'

    def test_from_target_with_string_coverage(self):
        """Test from_target when coverage is a string (not CoverageType)"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = 'Country'
        obj.term = 'Long'
        obj.universe_identifier = 'gsid'
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.type_ = 'Factor'
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        obj.universe_size = None
        model = MarqueeRiskModel.from_target(obj)
        assert model.coverage == RiskModelCoverage.Country

    def test_from_target_with_none_uid(self):
        """Test from_target when universe_identifier is None"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = None
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.type_ = RiskModelType.Factor
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        obj.universe_size = None
        model = MarqueeRiskModel.from_target(obj)
        assert model.universe_identifier is None

    def test_from_target_with_type_none_uses_not_type_branch(self):
        """Test from_target branch: not model.type_ -> passes type_ directly
        The 'not model.type_' branch in from_target passes None as type_.
        Since the constructor can't handle None type_, this branch is only hit
        when type_ is falsy but not None. We test it via from_target logic."""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = RiskModelUniverseIdentifier.gsid
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.type_ = RiskModelType.Macro  # Already an enum instance -> isinstance branch
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        obj.universe_size = None
        model = MarqueeRiskModel.from_target(obj)
        assert model.type == RiskModelType.Macro

    def test_from_target_with_string_type(self):
        """Test from_target when type_ is a string -> RiskModelType(model.type_)"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = RiskModelUniverseIdentifier.gsid
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.type_ = 'Factor'  # String, not enum
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        obj.universe_size = None
        model = MarqueeRiskModel.from_target(obj)
        assert model.type == RiskModelType.Factor

    def test_from_target_without_expected_update_time(self):
        """Test from_target branch: expected_update_time is None"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = RiskModelUniverseIdentifier.gsid
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.type_ = RiskModelType.Factor
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        obj.universe_size = None
        model = MarqueeRiskModel.from_target(obj)
        assert model.expected_update_time is None

    def test_from_many_targets(self):
        models = MarqueeRiskModel.from_many_targets((mock_risk_model_obj, mock_macro_risk_model_obj))
        assert len(models) == 2


# ==================== FactorRiskModel tests ====================


def test_create_risk_model(mocker):
    mock_risk_model(mocker)
    risk_model_id = 'model_id'
    mocker.patch.object(GsSession.current.sync, 'post', return_value=mock_risk_model_obj)
    new_model = FactorRiskModel(
        risk_model_id,
        'Fake Risk Model',
        RiskModelCoverage.Country,
        RiskModelTerm.Long,
        RiskModelUniverseIdentifier.gsid,
        'GS',
        0.1,
        universe_size=10000,
        entitlements={},
        description='Test',
        expected_update_time=dt.datetime.strptime('00:00:00', '%H:%M:%S').time(),
    )
    new_model.save()
    assert new_model.id == mock_risk_model_obj.id
    assert new_model.name == mock_risk_model_obj.name
    assert new_model.description == mock_risk_model_obj.description
    assert new_model.term == mock_risk_model_obj.term
    assert new_model.universe_size == mock_risk_model_obj.universe_size
    assert new_model.coverage == mock_risk_model_obj.coverage
    assert new_model.universe_identifier == mock_risk_model_obj.universe_identifier
    assert (
        new_model.expected_update_time
        == dt.datetime.strptime(mock_risk_model_obj.expected_update_time, '%H:%M:%S').time()
    )


def test_update_risk_model_entitlements(mocker):
    new_model = mock_risk_model(mocker)
    new_entitlements = {"execute": ['guid:X'], "edit": [], "view": [], "admin": [], "query": [], "upload": []}

    new_model.entitlements = new_entitlements
    new_model.save()
    assert 'guid:X' in new_model.entitlements.get('execute')
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    new_model.entitlements = empty_entitlements
    new_model.save()
    new_entitlements = {
        "execute": ['guid:X'],
        "edit": [],
        "view": [],
        "admin": ['guid:XX'],
        "query": [],
        "upload": ['guid:XXX'],
    }
    new_model.entitlements = new_entitlements
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert 'guid:X' in new_model.entitlements.get('execute')
    assert 'guid:XX' in new_model.entitlements.get('admin')
    assert 'guid:XXX' in new_model.entitlements.get('upload')


def test_update_risk_model(mocker):
    new_model = mock_risk_model(mocker)

    new_model.term = RiskModelTerm.Short
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.term == RiskModelTerm.Short

    new_model.description = 'Test risk model'
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.description == 'Test risk model'

    new_model.vendor = 'GS'
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.vendor == 'GS'

    new_model.term = RiskModelTerm.Medium
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.term == RiskModelTerm.Medium

    new_model.version = 0.1
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.version == 0.1

    new_model.universe_size = 10000
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.universe_size == 10000

    new_model.coverage = RiskModelCoverage.Global
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.coverage == RiskModelCoverage.Global

    new_model.name = 'TEST RISK MODEL'
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.name == 'TEST RISK MODEL'

    new_model.expected_update_time = dt.time(1, 0, 0)
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.expected_update_time == dt.time(1, 0, 0)

    new_model.type = RiskModelType.Thematic
    new_model.save()
    mocker.patch.object(GsSession.current.sync, 'get', return_value=new_model)
    assert new_model.type == RiskModelType.Thematic


# ==================== get_data error handling ====================


class TestGetDataErrorHandling:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_data_500_error(self, mock_api):
        """Test get_data when API raises 500+ error -> wraps in timeout message"""
        mock_api.get_risk_model_data.side_effect = MqRequestError(500, 'Server error')
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(MqRequestError) as exc_info:
            model.get_data(
                measures=[Measure.Total_Risk],
                start_date=dt.date(2022, 1, 1),
                end_date=dt.date(2022, 1, 5),
            )
        assert 'timeout' in str(exc_info.value.message)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_data_400_error(self, mock_api):
        """Test get_data when API raises <500 error -> re-raises original"""
        mock_api.get_risk_model_data.side_effect = MqRequestError(400, 'Bad request')
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(MqRequestError) as exc_info:
            model.get_data(
                measures=[Measure.Total_Risk],
                start_date=dt.date(2022, 1, 1),
            )
        assert exc_info.value.status == 400

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_data_with_factor_objects(self, mock_api):
        """Test get_data when factors list contains Factor objects"""
        from gs_quant.markets.factor import Factor as FactorObj
        mock_factor = MagicMock(spec=FactorObj)
        mock_factor.name = 'TestFactor'
        mock_api.get_risk_model_data.return_value = {'results': []}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_data(
            measures=[Measure.Total_Risk],
            start_date=dt.date(2022, 1, 1),
            factors=[mock_factor, 'StringFactor'],
        )
        # The Factor object should be converted to its name
        call_kwargs = mock_api.get_risk_model_data.call_args
        assert 'TestFactor' in call_kwargs.kwargs['factors']
        assert 'StringFactor' in call_kwargs.kwargs['factors']

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_data_no_factors(self, mock_api):
        """Test get_data with factors=None"""
        mock_api.get_risk_model_data.return_value = {'results': []}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_data(
            measures=[Measure.Total_Risk],
            start_date=dt.date(2022, 1, 1),
            factors=None,
        )
        call_kwargs = mock_api.get_risk_model_data.call_args
        assert call_kwargs.kwargs['factors'] is None


# ==================== upload_data tests ====================


class TestUploadData:

    @patch('gs_quant.models.risk_model.upload_model_data')
    @patch('gs_quant.models.risk_model.batch_and_upload_partial_data')
    @patch('gs_quant.models.risk_model.get_universe_size')
    @patch('gs_quant.models.risk_model.only_factor_data_is_present')
    def test_upload_data_small_universe(self, mock_only_factor, mock_universe_size, mock_batch, mock_upload):
        """Test upload_data when universe size is small enough for single request"""
        mock_only_factor.return_value = False
        mock_universe_size.return_value = 5  # Small, under default 10000
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        data = {
            'date': '2022-01-03',
            'factorData': [{'factorId': '1'}],
            'assetData': {'universe': ['a', 'b'], 'specificRisk': [1, 2]},
        }
        model.upload_data(data)
        mock_upload.assert_called_once()
        mock_batch.assert_not_called()

    @patch('gs_quant.models.risk_model.upload_model_data')
    @patch('gs_quant.models.risk_model.batch_and_upload_partial_data')
    @patch('gs_quant.models.risk_model.get_universe_size')
    @patch('gs_quant.models.risk_model.only_factor_data_is_present')
    def test_upload_data_large_universe(self, mock_only_factor, mock_universe_size, mock_batch, mock_upload):
        """Test upload_data when universe is large -> uses batch"""
        mock_only_factor.return_value = False
        mock_universe_size.return_value = 20000  # larger than default 10000
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        data = {
            'date': '2022-01-03',
            'factorData': [{'factorId': '1'}],
            'assetData': {'universe': list(range(20000))},
        }
        model.upload_data(data)
        mock_batch.assert_called_once()
        mock_upload.assert_not_called()

    @patch('gs_quant.models.risk_model.upload_model_data')
    @patch('gs_quant.models.risk_model.batch_and_upload_partial_data')
    @patch('gs_quant.models.risk_model.get_universe_size')
    @patch('gs_quant.models.risk_model.only_factor_data_is_present')
    def test_upload_data_only_factor_data(self, mock_only_factor, mock_universe_size, mock_batch, mock_upload):
        """Test upload_data when only factor data present -> partial request path"""
        mock_only_factor.return_value = True
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        data = {
            'date': '2022-01-03',
            'factorData': [{'factorId': '1'}],
        }
        model.upload_data(data)
        # only_factor_data_present -> target_universe_size=0, full_data_present=False -> partial
        mock_batch.assert_called_once()
        mock_upload.assert_not_called()

    @patch('gs_quant.models.risk_model.upload_model_data')
    @patch('gs_quant.models.risk_model.batch_and_upload_partial_data')
    @patch('gs_quant.models.risk_model.get_universe_size')
    @patch('gs_quant.models.risk_model.only_factor_data_is_present')
    def test_upload_data_with_risk_model_data_object(self, mock_only_factor, mock_universe_size, mock_batch, mock_upload):
        """Test upload_data with a RiskModelData object (converted via as_dict)"""
        mock_only_factor.return_value = False
        mock_universe_size.return_value = 2
        # Create a real RiskModelData to test the type(data) is RiskModelData branch
        rmd = RiskModelData(date='2022-01-03')
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        model.upload_data(rmd)
        # Verify as_dict was called (the data was converted)
        # The batch function should have been called since factorData+assetData are both missing
        mock_batch.assert_called_once()

    @patch('gs_quant.models.risk_model.upload_model_data')
    def test_upload_partial_data(self, mock_upload):
        """Test deprecated upload_partial_data"""
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        data = {'date': '2022-01-03', 'factorData': []}
        model.upload_partial_data(data, final_upload=True)
        mock_upload.assert_called_once_with('test_id', data, partial_upload=True, final_upload=True)


# ==================== upload_asset_coverage_data ====================


class TestUploadAssetCoverage:

    @patch('gs_quant.models.risk_model.batch_and_upload_coverage_data')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_upload_asset_coverage_data_with_date(self, mock_rm_api, mock_frm_api, mock_batch):
        """Test upload_asset_coverage_data with a date provided"""
        mock_frm_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {'universe': ['123', '456']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        model.upload_asset_coverage_data(date=dt.date(2022, 1, 3))
        mock_batch.assert_called_once()

    @patch('gs_quant.models.risk_model.batch_and_upload_coverage_data')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_upload_asset_coverage_data_without_date(self, mock_rm_api, mock_frm_api, mock_batch):
        """Test upload_asset_coverage_data with no date (uses get_dates()[-1])"""
        mock_rm_api.get_risk_model_dates.return_value = ['2022-01-03', '2022-01-04']
        mock_frm_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-04',
                    'assetData': {'universe': ['123']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        model.upload_asset_coverage_data()
        mock_batch.assert_called_once()

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_upload_asset_coverage_data_no_gsid_list(self, mock_rm_api, mock_frm_api):
        """Test upload_asset_coverage_data when no asset data found"""
        mock_frm_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {'universe': ['123']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        # The get for a specific date returns None for gsid_list
        with patch.object(model, 'get_asset_universe', return_value={}) as mock_gau:
            with pytest.raises(MqRequestError):
                model.upload_asset_coverage_data(date=dt.date(2022, 1, 3))


# ==================== get_factor and get_many_factors ====================


class TestGetFactor:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_found(self, mock_api):
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
            {'name': 'Value', 'identifier': 'f2', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factor = model.get_factor('Momentum')
        assert factor.name == 'Momentum'

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_not_found(self, mock_api):
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(MqValueError, match='does not in exist'):
            model.get_factor('NonExistent')

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_many_factors_all(self, mock_api):
        """Test get_many_factors with no factor_names or factor_ids -> returns all"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
            {'name': 'Value', 'identifier': 'f2', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factors = model.get_many_factors()
        assert len(factors) == 2

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_many_factors_by_names(self, mock_api):
        """Test get_many_factors filtering by factor_names"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
            {'name': 'Value', 'identifier': 'f2', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factors = model.get_many_factors(factor_names=['Momentum'])
        assert len(factors) == 1
        assert factors[0].name == 'Momentum'

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_many_factors_by_ids(self, mock_api):
        """Test get_many_factors filtering by factor_ids"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
            {'name': 'Value', 'identifier': 'f2', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factors = model.get_many_factors(factor_ids=['f2'])
        assert len(factors) == 1
        assert factors[0].name == 'Value'

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_many_factors_names_not_found(self, mock_api):
        """Test get_many_factors when some factor_names are not found"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(MqValueError, match='not in model'):
            model.get_many_factors(factor_names=['NonExistent'])

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_many_factors_ids_not_found(self, mock_api):
        """Test get_many_factors when some factor_ids are not found"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(MqValueError, match='not in model'):
            model.get_many_factors(factor_ids=['non_existent_id'])


# ==================== save_factor_metadata / delete_factor_metadata ====================


class TestFactorMetadata:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_save_factor_metadata_update(self, mock_api):
        """save_factor_metadata when update succeeds"""
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factor = RiskModelFactor(identifier='f1', type_='Factor')
        model.save_factor_metadata(factor)
        mock_api.update_risk_model_factor.assert_called_once_with('test_id', factor)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_save_factor_metadata_create(self, mock_api):
        """save_factor_metadata when update raises MqRequestError -> create"""
        mock_api.update_risk_model_factor.side_effect = MqRequestError(404, 'Not found')
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        factor = RiskModelFactor(identifier='f1', type_='Factor')
        model.save_factor_metadata(factor)
        mock_api.create_risk_model_factor.assert_called_once_with('test_id', factor)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_delete_factor_metadata(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        model.delete_factor_metadata('f1')
        mock_api.delete_risk_model_factor.assert_called_once_with('test_id', 'f1')


# ==================== get_factor_data tests ====================


class TestGetFactorData:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_category_type(self, mock_api):
        """Test get_factor_data with factor_type=Category"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Style', 'identifier': 'c1', 'type': 'Category'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_data(factor_type=FactorType.Category)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_category_type_with_category_filter_raises(self, mock_api):
        """Test error when Category type + category_filter"""
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Category filter is not applicable'):
            model.get_factor_data(factor_type=FactorType.Category, category_filter=['Style'])

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_factor_type_with_aggregation_raises(self, mock_api):
        """Test error when Factor type + Aggregations in category_filter"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Aggregations should not be passed'):
            model.get_factor_data(factor_type=FactorType.Factor, category_filter=['Aggregations'])

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_factor_type_filters(self, mock_api):
        """Test get_factor_data with factor_type=Factor filters out non-Factor types"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
            {'name': 'Style', 'identifier': 'c1', 'type': 'Category'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_data(factor_type=FactorType.Factor)
        # Only Factor type rows should remain
        assert len(result) == 1

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_json_format(self, mock_api):
        """Test get_factor_data returning JSON format"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_data(format=ReturnFormat.JSON)
        assert isinstance(result, list)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_data_with_category_filter(self, mock_api):
        """Test get_factor_data with category_filter"""
        mock_api.get_risk_model_factor_data.return_value = [
            {'name': 'Momentum', 'identifier': 'f1', 'type': 'Factor', 'factorCategory': 'Style'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_data(category_filter=['Style'])
        assert isinstance(result, pd.DataFrame)


# ==================== get_intraday_factor_data ====================


class TestIntradayFactorData:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_intraday_factor_data_dataframe(self, mock_api):
        mock_api.get_risk_model_factor_data_intraday.return_value = [
            {'factorId': '1', 'factorReturn': 0.01, 'timestamp': '2022-01-03T12:00:00'},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_intraday_factor_data(format=ReturnFormat.DATA_FRAME)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_intraday_factor_data_json(self, mock_api):
        mock_api.get_risk_model_factor_data_intraday.return_value = [
            {'factorId': '1', 'factorReturn': 0.01},
        ]
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_intraday_factor_data(format=ReturnFormat.JSON)
        assert isinstance(result, list)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_intraday_factor_data_with_category_filter(self, mock_api):
        mock_api.get_risk_model_factor_data_intraday.return_value = []
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_intraday_factor_data(category_filter=['Style'], format=ReturnFormat.JSON)
        call_kwargs = mock_api.get_risk_model_factor_data_intraday.call_args
        assert call_kwargs.kwargs['factor_categories'] == ['Style']


# ==================== get_asset_universe ====================


class TestGetAssetUniverse:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_asset_universe_json(self, mock_api):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {'universe': ['abc', 'def']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_asset_universe(
            start_date=dt.date(2022, 1, 3),
            end_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        assert dt.date(2022, 1, 3) in result

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_asset_universe_dataframe(self, mock_api):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {'universe': ['abc', 'def']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_asset_universe(
            start_date=dt.date(2022, 1, 3),
            end_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_asset_universe_no_end_date_no_universe(self, mock_api):
        """Branch: assets.universe is empty and end_date is None -> end_date = start_date"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {'universe': ['abc']},
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_asset_universe(start_date=dt.date(2022, 1, 3))
        assert isinstance(result, pd.DataFrame)


# ==================== get_universe_exposure tests ====================


class TestGetUniverseExposure:

    @patch('gs_quant.models.risk_model.build_factor_id_to_name_map')
    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_universe_exposure_by_name(self, mock_api, mock_build_asset, mock_build_factor):
        """Test get_universe_exposure with get_factors_by_name=True"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'Momentum'}],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build_factor.return_value = {'1': 'Momentum'}
        mock_build_asset.return_value = {
            'abc': {'2022-01-03': {'Momentum': 0.5}},
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_universe_exposure(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
            get_factors_by_name=True,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_universe_exposure_by_id(self, mock_api, mock_build_asset):
        """Test get_universe_exposure with get_factors_by_name=False"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'Momentum'}],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build_asset.return_value = {
            'abc': {'2022-01-03': {'1': 0.5}},
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_universe_exposure(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
            get_factors_by_name=False,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)


# ==================== FactorRiskModel specific ====================


class TestFactorRiskModelSpecific:

    def test_factor_risk_model_from_target(self):
        model = FactorRiskModel.from_target(mock_risk_model_obj)
        assert isinstance(model, FactorRiskModel)
        assert model.type == RiskModelType.Factor

    def test_factor_risk_model_from_target_string_fields(self):
        """Test from_target with string fields (not enum instances)"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = 'Country'
        obj.term = 'Long'
        obj.universe_identifier = 'gsid'
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = '10:30:00'
        model = FactorRiskModel.from_target(obj)
        assert model.expected_update_time == dt.time(10, 30)

    def test_factor_risk_model_from_target_none_uid(self):
        """Test from_target when uid is None"""
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = None
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        model = FactorRiskModel.from_target(obj)
        assert model.universe_identifier is None

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_many(self, mock_api):
        mock_api.get_risk_models.return_value = (mock_risk_model_obj,)
        models = FactorRiskModel.get_many(ids=['model_id'])
        assert len(models) == 1

    def test_repr_with_optional_fields(self):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
            universe_size=5000, description='A desc',
            expected_update_time=dt.time(10, 0),
            entitlements={'view': []},
        )
        r = repr(model)
        assert 'FactorRiskModel' in r
        assert 'universe_size=' in r
        assert 'description=' in r
        assert 'expected_update_time=' in r

    def test_repr_without_optional_fields(self):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        r = repr(model)
        assert 'FactorRiskModel' in r
        assert 'universe_size=' not in r


# ==================== FactorRiskModel days-based measure KeyError paths ====================


class TestDaysBasedMeasureKeyErrors:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_bid_ask_spread_invalid_days(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Bid Ask Spread data is not available'):
            model.get_bid_ask_spread(dt.date(2022, 1, 3), days=999)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_trading_volume_invalid_days(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Trading volume data is not available'):
            model.get_trading_volume(dt.date(2022, 1, 3), days=999)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_traded_value_invalid_days(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Traded Value data is not available'):
            model.get_traded_value(dt.date(2022, 1, 3), days=999)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_composite_volume_invalid_days(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Composite Volume data is not available'):
            model.get_composite_volume(dt.date(2022, 1, 3), days=999)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_composite_value_invalid_days(self, mock_api):
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        with pytest.raises(ValueError, match='Composite Value data is not available'):
            model.get_composite_value(dt.date(2022, 1, 3), days=999)


# ==================== _build_covariance_matrix_measure ====================


class TestBuildCovarianceMatrix:

    @patch('gs_quant.models.risk_model.get_covariance_matrix_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_covariance_matrix_dataframe_with_assets(self, mock_api, mock_cov_df):
        """Branch: assets is provided -> limit_factors=True, extra measures"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'f1'}],
                    'covarianceMatrix': [[1.0]],
                }
            ]
        }
        mock_cov_df.return_value = pd.DataFrame()
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_covariance_matrix(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
            format=ReturnFormat.DATA_FRAME,
        )
        # Verify limit_factors is True when assets provided
        call_kwargs = mock_api.get_risk_model_data.call_args.kwargs
        assert call_kwargs['limit_factors'] is True

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_covariance_matrix_json_no_assets(self, mock_api):
        """Branch: assets=None -> limit_factors=False, JSON format"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'f1'}],
                    'covarianceMatrix': [[1.0]],
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_covariance_matrix(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, list)


# ==================== get_factor_volatility ====================


class TestGetFactorVolatility:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_volatility_json_no_name_no_factors(self, mock_api):
        """Branch: JSON + not get_factors_by_name + not factors -> returns raw results"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'f1', 'factorVolatility': 0.05},
                    ],
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_volatility(
            start_date=dt.date(2022, 1, 3),
            get_factors_by_name=False,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, list)

    @patch('gs_quant.models.risk_model.build_factor_volatility_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_volatility_json_with_factors(self, mock_api, mock_build):
        """Branch: JSON + factors -> goes through else path, returns dict"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'f1', 'factorVolatility': 0.05},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'f1': [0.05]})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_volatility(
            start_date=dt.date(2022, 1, 3),
            factors=['f1'],
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)

    @patch('gs_quant.models.risk_model.build_factor_volatility_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_volatility_dataframe(self, mock_api, mock_build):
        """Branch: DATA_FRAME format -> returns DataFrame"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'f1', 'factorVolatility': 0.05},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'f1': [0.05]})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_volatility(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_factor_volatility_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_volatility_json_by_name(self, mock_api, mock_build):
        """Branch: JSON + get_factors_by_name -> goes through else (to_dict)"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'f1', 'factorVolatility': 0.05},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'f1': [0.05]})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_volatility(
            start_date=dt.date(2022, 1, 3),
            get_factors_by_name=True,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)


# ==================== get_issuer_specific_covariance / get_factor_portfolios ====================


class TestISCAndFactorPortfolios:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_issuer_specific_covariance_json(self, mock_api):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'issuerSpecificCovariance': {
                        'universeId1': ['a'], 'universeId2': ['b'], 'covariance': [0.01]
                    },
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_issuer_specific_covariance(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, list)

    @patch('gs_quant.models.risk_model.get_optional_data_as_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_issuer_specific_covariance_dataframe(self, mock_api, mock_get_opt):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'issuerSpecificCovariance': {
                        'universeId1': ['a'], 'universeId2': ['b'], 'covariance': [0.01]
                    },
                }
            ]
        }
        mock_get_opt.return_value = pd.DataFrame({'covariance': [0.01]})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_issuer_specific_covariance(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_pfp_data_dataframe')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_portfolios(self, mock_api, mock_pfp):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'f1'}],
                    'factorPortfolios': {
                        'universe': ['a', 'b'],
                        'portfolio': [{'factorId': '1', 'weights': [0.5, 0.5]}],
                    },
                }
            ]
        }
        mock_pfp.return_value = pd.DataFrame()
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_portfolios(start_date=dt.date(2022, 1, 3))
        assert isinstance(result, pd.DataFrame)


# ==================== _build_currency_rates_data ====================


class TestCurrencyRatesData:

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_build_currency_rates_data_no_currencies_filter(self, mock_api):
        """Test with no currencies filter -> returns all"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'currencyRatesData': {
                        'riskFreeRate': [0.01, 0.02],
                        'currency': ['USD', 'EUR'],
                    },
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_risk_free_rate(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_build_currency_rates_data_with_currencies_filter(self, mock_api):
        """Test with currencies filter"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'currencyRatesData': {
                        'riskFreeRate': [0.01, 0.02],
                        'currency': ['USD', 'EUR'],
                    },
                }
            ]
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_risk_free_rate(
            start_date=dt.date(2022, 1, 3),
            currencies=[Currency.USD],
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)


# ==================== MacroRiskModel tests ====================


class TestMacroRiskModel:

    def test_macro_risk_model_init(self):
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        assert model.type == RiskModelType.Macro

    def test_macro_risk_model_from_target(self):
        model = MacroRiskModel.from_target(mock_macro_risk_model_obj)
        assert isinstance(model, MacroRiskModel)
        assert model.type == RiskModelType.Macro

    def test_macro_risk_model_from_target_string_fields(self):
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = 'Country'
        obj.term = 'Long'
        obj.universe_identifier = 'gsid'
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        model = MacroRiskModel.from_target(obj)
        assert model.coverage == RiskModelCoverage.Country

    def test_macro_risk_model_from_target_none_uid(self):
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = None
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        model = MacroRiskModel.from_target(obj)
        assert model.universe_identifier is None

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_many(self, mock_api):
        mock_api.get_risk_models.return_value = (mock_macro_risk_model_obj,)
        models = MacroRiskModel.get_many(ids=['macro_model_id'])
        assert len(models) == 1

    def test_repr_with_optional_fields(self):
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
            universe_size=3000, description='Macro desc',
            expected_update_time=dt.time(9, 0), entitlements={'view': []},
        )
        r = repr(model)
        assert 'MacroRiskModel' in r
        assert 'universe_size=' in r
        assert 'description=' in r
        assert 'expected_update_time=' in r
        assert 'entitlements=' in r

    def test_repr_without_optional_fields(self):
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        r = repr(model)
        assert 'MacroRiskModel' in r
        assert 'universe_size=' not in r

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_fair_value_gap_percent(self, mock_api, mock_build):
        """Test get_fair_value_gap with Unit.PERCENT"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'fairValueGapPercent': [10.0],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 10.0}}
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_fair_value_gap(
            start_date=dt.date(2022, 1, 3),
            fair_value_gap_unit=Unit.PERCENT,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_fair_value_gap_standard_deviation(self, mock_api, mock_build):
        """Test get_fair_value_gap with Unit.STANDARD_DEVIATION (default)"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'fairValueGapStandardDeviation': [2.5],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 2.5}}
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_fair_value_gap(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)


# ==================== MacroRiskModel.get_universe_sensitivity ====================


class TestMacroUniverseSensitivity:

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_universe_sensitivity_factor_type(self, mock_api, mock_build):
        """Test universe_sensitivity with FactorType.Factor -> returns directly"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'Momentum'}],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build.return_value = {
            'abc': {'2022-01-03': {'1': 0.5}},
        }
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_universe_sensitivity(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
            factor_type=FactorType.Factor,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_universe_sensitivity_category_type_empty(self, mock_api, mock_build):
        """Test universe_sensitivity with FactorType.Category when sensitivity_df is empty"""
        mock_api.get_risk_model_data.return_value = {
            'results': []
        }
        mock_build.return_value = {}
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        # When results are empty, the DataFrame will be empty
        with patch.object(model, 'get_universe_exposure', return_value=pd.DataFrame()):
            result = model.get_universe_sensitivity(
                start_date=dt.date(2022, 1, 3),
                assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
                factor_type=FactorType.Category,
            )
            assert isinstance(result, pd.DataFrame)
            assert result.empty


# ==================== ThematicRiskModel tests ====================


class TestThematicRiskModel:

    def test_thematic_risk_model_init(self):
        model = ThematicRiskModel(
            'thematic_id', 'Thematic Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        assert model.type == RiskModelType.Thematic

    def test_thematic_risk_model_from_target(self):
        model = ThematicRiskModel.from_target(mock_thematic_risk_model_obj)
        assert isinstance(model, ThematicRiskModel)

    def test_thematic_risk_model_from_target_string_fields(self):
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = 'Country'
        obj.term = 'Long'
        obj.universe_identifier = 'gsid'
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = '08:00:00'
        model = ThematicRiskModel.from_target(obj)
        assert model.expected_update_time == dt.time(8, 0)

    def test_thematic_risk_model_from_target_none_uid(self):
        obj = MagicMock()
        obj.id = 'test'
        obj.name = 'Test'
        obj.coverage = RiskModelCoverage.Country
        obj.term = RiskModelTerm.Long
        obj.universe_identifier = None
        obj.vendor = 'GS'
        obj.version = 1.0
        obj.universe_size = None
        obj.entitlements = None
        obj.description = None
        obj.expected_update_time = None
        model = ThematicRiskModel.from_target(obj)
        assert model.universe_identifier is None

    @patch('gs_quant.models.risk_model.GsRiskModelApi')
    def test_get_many(self, mock_api):
        mock_api.get_risk_models.return_value = (mock_thematic_risk_model_obj,)
        models = ThematicRiskModel.get_many(ids=['thematic_model_id'])
        assert len(models) == 1

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_universe_sensitivity(self, mock_api, mock_build):
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'Basket1'}],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build.return_value = {
            'abc': {'2022-01-03': {'1': 0.5}},
        }
        model = ThematicRiskModel(
            'thematic_id', 'Thematic Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_universe_sensitivity(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
        )
        assert isinstance(result, pd.DataFrame)

    def test_repr_with_optional_fields(self):
        model = ThematicRiskModel(
            'thematic_id', 'Thematic Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
            universe_size=2000, description='Thematic desc',
            expected_update_time=dt.time(8, 0), entitlements={'view': []},
        )
        r = repr(model)
        assert 'ThematicRiskModel' in r
        assert 'universe_size=' in r
        assert 'description=' in r
        assert 'expected_update_time=' in r

    def test_repr_without_optional_fields(self):
        model = ThematicRiskModel(
            'thematic_id', 'Thematic Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        r = repr(model)
        assert 'ThematicRiskModel' in r
        assert 'universe_size=' not in r


# ==================== Original tests (preserved) ====================


def test_get_r_squared(mocker):
    macro_model = mock_macro_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]
    query = {
        'startDate': '2022-04-04',
        'endDate': '2022-04-06',
        'assets': DataAssetsRequest(UniverseIdentifier.gsid, universe),
        'measures': [Measure.R_Squared, Measure.Asset_Universe],
        'limitFactors': False,
    }

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {"universe": ["904026", "232128", "24985", "160444"], "rSquared": [89.0, 45.0, 12.0, 5.0]},
            }
        ],
        'totalResults': 1,
    }

    r_squared_response = {
        '160444': {'2022-04-05': 5.0},
        '232128': {'2022-04-05': 45.0},
        '24985': {'2022-04-05': 12.0},
        '904026': {'2022-04-05': 89.0},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    # run test
    response = macro_model.get_r_squared(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )

    GsSession.current.sync.post.assert_called_with(
        '/risk/models/data/{id}/query'.format(id='macro_model_id'), query, timeout=200
    )
    assert response == r_squared_response


def test_get_fair_value_gap_standard_deviation(mocker):
    macro_model = mock_macro_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]
    query = {
        'startDate': '2022-04-04',
        'endDate': '2022-04-06',
        'assets': DataAssetsRequest(UniverseIdentifier.gsid, universe),
        'measures': [Measure.Fair_Value_Gap_Standard_Deviation, Measure.Asset_Universe],
        'limitFactors': False,
    }

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "fairValueGapStandardDeviation": [4.0, 5.0, 1.0, 7.0],
                },
            }
        ],
        'totalResults': 1,
    }

    fvg_response = {
        '160444': {'2022-04-05': 7.0},
        '232128': {'2022-04-05': 5.0},
        '24985': {'2022-04-05': 1.0},
        '904026': {'2022-04-05': 4.0},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)
    response = macro_model.get_fair_value_gap(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    GsSession.current.sync.post.assert_called_with(
        '/risk/models/data/{id}/query'.format(id='macro_model_id'), query, timeout=200
    )
    assert response == fvg_response


def test_get_fair_value_gap_percent(mocker):
    macro_model = mock_macro_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]
    query = {
        'startDate': '2022-04-04',
        'endDate': '2022-04-06',
        'assets': DataAssetsRequest(UniverseIdentifier.gsid, universe),
        'measures': [Measure.Fair_Value_Gap_Percent, Measure.Asset_Universe],
        'limitFactors': False,
    }

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "fairValueGapPercent": [90.0, 34.0, 8.0, 34.0],
                },
            }
        ],
        'totalResults': 1,
    }

    fvg_response = {
        '160444': {'2022-04-05': 34.0},
        '232128': {'2022-04-05': 34.0},
        '24985': {'2022-04-05': 8.0},
        '904026': {'2022-04-05': 90.0},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)
    response = macro_model.get_fair_value_gap(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        fair_value_gap_unit=Unit.PERCENT,
        format=ReturnFormat.JSON,
    )
    GsSession.current.sync.post.assert_called_with(
        '/risk/models/data/{id}/query'.format(id='macro_model_id'), query, timeout=200
    )
    assert response == fvg_response


@pytest.mark.parametrize(
    "statistical_measure, fieldKey",
    [
        (Measure.Factor_Mean, _map_measure_to_field_name(Measure.Factor_Mean)),
        (Measure.Factor_Cross_Sectional_Mean, _map_measure_to_field_name(Measure.Factor_Cross_Sectional_Mean)),
        (Measure.Factor_Standard_Deviation, _map_measure_to_field_name(Measure.Factor_Standard_Deviation)),
        (
            Measure.Factor_Cross_Sectional_Standard_Deviation,
            _map_measure_to_field_name(Measure.Factor_Cross_Sectional_Standard_Deviation),
        ),
    ],
)
def test_get_statistical_factor_data(mocker, statistical_measure, fieldKey):
    risk_model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]
    query = {
        'startDate': '2022-04-04',
        'endDate': '2022-04-06',
        'assets': DataAssetsRequest(UniverseIdentifier.gsid, universe),
        'measures': [
            statistical_measure,
            Measure.Factor_Name,
            Measure.Factor_Id,
            Measure.Universe_Factor_Exposure,
            Measure.Asset_Universe,
        ],
        'limitFactors': True,
    }

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "factorData": [
                    {"factorId": "1", f"{fieldKey}": 89.0, "factorName": "Factor1"},
                    {"factorId": "2", f"{fieldKey}": 0.67, "factorName": "Factor2"},
                ],
                'assetData': {
                    'factorExposure': [
                        {'1': 0.2, '2': 0.3},
                        {'1': 0.02, '2': 0.03},
                        {'1': 6.2, '2': 3.0},
                        {'1': -6.2, '2': 0.3},
                    ],
                    'universe': ['904026', '232128', '24985', '160444'],
                },
            }
        ],
        'totalResults': 1,
    }

    expected_response = {'Factor1': {'2022-04-05': 89.0}, 'Factor2': {'2022-04-05': 0.67}}

    kwargs = {
        "start_date": dt.date(2022, 4, 4),
        "end_date": dt.date(2022, 4, 6),
        "assets": DataAssetsRequest(UniverseIdentifier.gsid, universe),
        "format": ReturnFormat.JSON,
    }

    field_key_to_getter_ref = {
        _map_measure_to_field_name(Measure.Factor_Mean): risk_model.get_factor_mean,
        _map_measure_to_field_name(Measure.Factor_Cross_Sectional_Mean): risk_model.get_factor_cross_sectional_mean,
        _map_measure_to_field_name(Measure.Factor_Standard_Deviation): risk_model.get_factor_standard_deviation,
        _map_measure_to_field_name(
            Measure.Factor_Cross_Sectional_Standard_Deviation
        ): risk_model.get_factor_cross_sectional_standard_deviation,
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    actual_response = field_key_to_getter_ref[fieldKey](**kwargs)
    GsSession.current.sync.post.assert_called_with(
        '/risk/models/data/{id}/query'.format(id='model_id'), query, timeout=200
    )

    assert actual_response == expected_response


def test_get_factor_z_score(mocker):
    macro_model = mock_macro_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]
    query = {
        'startDate': '2022-04-04',
        'endDate': '2022-04-06',
        'assets': DataAssetsRequest(UniverseIdentifier.gsid, universe),
        'measures': [
            Measure.Factor_Z_Score,
            Measure.Factor_Name,
            Measure.Factor_Id,
            Measure.Universe_Factor_Exposure,
            Measure.Asset_Universe,
        ],
        'limitFactors': True,
    }

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "factorData": [
                    {"factorId": "1", "factorZScore": 1.5, "factorName": "Factor1"},
                    {"factorId": "2", "factorZScore": -1.0, "factorName": "Factor2"},
                ],
                'assetData': {
                    'factorExposure': [
                        {'1': 0.2, '2': 0.3},
                        {'1': 0.02, '2': 0.03},
                        {'1': 6.2, '2': 3.0},
                        {'1': -6.2, '2': 0.3},
                    ],
                    'universe': ['904026', '232128', '24985', '160444'],
                },
            }
        ],
        'totalResults': 1,
    }

    factor_z_score_response = {'Factor1': {'2022-04-05': 1.5}, 'Factor2': {'2022-04-05': -1.0}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    # run test
    response = macro_model.get_factor_z_score(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    GsSession.current.sync.post.assert_called_with(
        '/risk/models/data/{id}/query'.format(id='macro_model_id'), query, timeout=200
    )
    assert response == factor_z_score_response


def test_get_predicted_beta(mocker):
    model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "predictedBeta": [0.4, 1.5, 1.2, 0.5],
                },
            }
        ],
        'totalResults': 1,
    }

    predicted_beta_response = {
        '160444': {'2022-04-05': 0.5},
        '232128': {'2022-04-05': 1.5},
        '24985': {'2022-04-05': 1.2},
        '904026': {'2022-04-05': 0.4},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_predicted_beta(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    assert response == predicted_beta_response


def test_get_global_predicted_beta(mocker):
    model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "globalPredictedBeta": [0.4, 1.5, 1.2, 0.5],
                },
            }
        ],
        'totalResults': 1,
    }

    global_predicted_beta_response = {
        '160444': {'2022-04-05': 0.5},
        '232128': {'2022-04-05': 1.5},
        '24985': {'2022-04-05': 1.2},
        '904026': {'2022-04-05': 0.4},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_global_predicted_beta(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    assert response == global_predicted_beta_response


def test_get_estimation_universe_weights(mocker):
    model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "estimationUniverseWeight": [0.4, None, None, 0.6],
                },
            }
        ],
        'totalResults': 1,
    }

    estu_response = {
        '160444': {'2022-04-05': 0.6},
        '232128': {'2022-04-05': None},
        '24985': {'2022-04-05': None},
        '904026': {'2022-04-05': 0.4},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_estimation_universe_weights(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    assert response == estu_response


def test_get_daily_return(mocker):
    model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "dailyReturn": [-0.4, -1.5, 1.2, 0.5],
                },
            }
        ],
        'totalResults': 1,
    }

    daily_return_response = {
        '160444': {'2022-04-05': 0.5},
        '232128': {'2022-04-05': -1.5},
        '24985': {'2022-04-05': 1.2},
        '904026': {'2022-04-05': -0.4},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_daily_return(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    assert response == daily_return_response


def test_get_specific_return(mocker):
    model = mock_risk_model(mocker)
    universe = ["904026", "232128", "24985", "160444"]

    results = {
        'missingDates': ['2022-04-04', '2022-04-06'],
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["904026", "232128", "24985", "160444"],
                    "specificReturn": [0.5, 1.6, 1.4, 0.7],
                },
            }
        ],
        'totalResults': 1,
    }

    specific_return_response = {
        '160444': {'2022-04-05': 0.7},
        '232128': {'2022-04-05': 1.6},
        '24985': {'2022-04-05': 1.4},
        '904026': {'2022-04-05': 0.5},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_specific_return(
        start_date=dt.date(2022, 4, 4),
        end_date=dt.date(2022, 4, 6),
        assets=DataAssetsRequest(UniverseIdentifier.gsid, universe),
        format=ReturnFormat.JSON,
    )
    assert response == specific_return_response


@pytest.mark.parametrize("aws_upload", [True, False])
def test_upload_risk_model_data(mocker, aws_upload):
    model = mock_risk_model(mocker)
    risk_model_data = {
        'date': '2023-04-14',
        'assetData': {
            'universe': ['2407966', '2046251', 'USD'],
            'specificRisk': [12.09, 45.12, 3.09],
            'factorExposure': [{'1': 0.23, '2': 0.023}, {'1': 0.23}, {'3': 0.23, '2': 0.023}],
            'totalRisk': [0.12, 0.45, 1.2],
        },
        'factorData': [
            {'factorId': '1', 'factorName': 'USD', 'factorCategory': 'Currency', 'factorCategoryId': 'CUR'},
            {'factorId': '2', 'factorName': 'ST', 'factorCategory': 'ST', 'factorCategoryId': 'ST'},
            {'factorId': '3', 'factorName': 'IND', 'factorCategory': 'IND', 'factorCategoryId': 'IND'},
        ],
        'covarianceMatrix': [[0.089, 0.0123, 0.345], [0.0123, 3.45, 0.345], [0.345, 0.345, 1.23]],
        'issuerSpecificCovariance': {'universeId1': ['2407966'], 'universeId2': ['2046251'], 'covariance': [0.03754]},
        'factorPortfolios': {
            'universe': ['2407966', '2046251'],
            'portfolio': [
                {'factorId': 1, 'weights': [0.25, 0.75]},
                {'factorId': 2, 'weights': [0.25, 0.75]},
                {'factorId': 3, 'weights': [0.25, 0.75]},
            ],
        },
    }

    base_url = f"/risk/models/data/{model.id}?partialUpload=true"
    date = risk_model_data.get("date")
    max_asset_batch_size = 2

    batched_asset_data = [
        {
            "assetData": {
                key: value[i : i + max_asset_batch_size] for key, value in risk_model_data.get("assetData").items()
            },
            "date": date,
        }
        for i in range(0, len(risk_model_data.get("assetData").get("universe")), max_asset_batch_size)
    ]

    max_asset_batch_size //= 2
    batched_factor_portfolios = [
        {
            "factorPortfolios": {
                key: (
                    value[i : i + max_asset_batch_size]
                    if key in "universe"
                    else [
                        {
                            "factorId": factor_weights.get("factorId"),
                            "weights": factor_weights.get("weights")[i : i + max_asset_batch_size],
                        }
                        for factor_weights in value
                    ]
                )
                for key, value in risk_model_data.get("factorPortfolios").items()
            },
            "date": date,
        }
        for i in range(0, len(risk_model_data.get("factorPortfolios").get("universe")), max_asset_batch_size)
    ]

    expected_factor_data_calls = [
        mock.call(
            f"{base_url}{'&awsUpload=true' if aws_upload else ''}",
            {
                "date": date,
                "factorData": risk_model_data.get("factorData"),
                "covarianceMatrix": risk_model_data.get("covarianceMatrix"),
            },
            timeout=200,
        )
    ]

    expected_asset_data_calls = []
    for batch_num, batch_asset_payload in enumerate(batched_asset_data):
        final_upload_flag = 'true' if batch_num == len(batched_asset_data) - 1 else 'false'
        expected_asset_data_calls.append(
            mock.call(
                f"{base_url}&finalUpload={final_upload_flag}{'&awsUpload=true' if aws_upload else ''}",
                batch_asset_payload,
                timeout=200,
            )
        )

    expected_factor_portfolios_data_calls = []
    for batch_num, batched_fp_payload in enumerate(batched_factor_portfolios):
        final_upload_flag = 'true' if batch_num == len(batched_factor_portfolios) - 1 else 'false'
        expected_factor_portfolios_data_calls.append(
            mock.call(
                f"{base_url}&finalUpload={final_upload_flag}{'&awsUpload=true' if aws_upload else ''}",
                batched_fp_payload,
                timeout=200,
            )
        )

    expected_isc_data_calls = [
        mock.call(
            f"{base_url}&finalUpload=true{'&awsUpload=true' if aws_upload else ''}",
            {"issuerSpecificCovariance": risk_model_data.get("issuerSpecificCovariance"), "date": date},
            timeout=200,
        )
    ]

    expected_calls = (
        expected_factor_data_calls
        + expected_asset_data_calls
        + expected_isc_data_calls
        + expected_factor_portfolios_data_calls
    )

    # mock GsSession
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'post', return_value='Upload Successful')

    max_asset_batch_size = 2
    model.upload_data(risk_model_data, max_asset_batch_size=max_asset_batch_size, aws_upload=aws_upload)

    call_args_list = GsSession.current.sync.post.call_args_list

    assert len(call_args_list) == len(expected_calls)
    assert call_args_list == expected_calls

    GsSession.current.sync.post.assert_has_calls(expected_calls, any_order=False)


@pytest.mark.parametrize("days", [0, 30, 60, 90])
def test_get_bid_ask_spread(mocker, days):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)
    measure_to_query = Measure.Bid_Ask_Spread if not days else Measure[f'Bid_Ask_Spread_{days}d']

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["2046251", "2588173"],
                    f"bidAskSpread{'' if days == 0 else f'{days}d'}": [0.5, 1.6],
                },
            }
        ],
        'totalResults': 1,
    }

    bid_ask_spread_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_bid_ask_spread(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), days=days, assets=assets, format=ReturnFormat.JSON
    )
    assert response == bid_ask_spread_response


@pytest.mark.parametrize("days", [0, 30, 60, 90])
def test_get_trading_volume(mocker, days):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["2046251", "2588173"],
                    f"tradingVolume{'' if days == 0 else f'{days}d'}": [0.5, 1.6],
                },
            }
        ],
        'totalResults': 1,
    }

    trading_volume_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_trading_volume(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), days=days, assets=assets, format=ReturnFormat.JSON
    )
    assert response == trading_volume_response


@pytest.mark.parametrize("days", [0, 30])
def test_get_traded_value(mocker, days):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "tradedValue30d": [0.5, 1.6]}}
        ],
        'totalResults': 1,
    }

    trading_value_30d_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_traded_value(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), days=days, assets=assets, format=ReturnFormat.JSON
    )
    assert response == trading_value_30d_response


@pytest.mark.parametrize("days", [0, 30, 60, 90])
def test_get_composite_volume(mocker, days):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {
                    "universe": ["2046251", "2588173"],
                    f"compositeVolume{'' if days == 0 else f'{days}d'}": [0.5, 1.6],
                },
            }
        ],
        'totalResults': 1,
    }

    composite_volume_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_composite_volume(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), days=days, assets=assets, format=ReturnFormat.JSON
    )
    assert response == composite_volume_response


@pytest.mark.parametrize("days", [0, 30])
def test_get_composite_value(mocker, days):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "compositeValue30d": [0.5, 1.6]}}
        ],
        'totalResults': 1,
    }

    composite_value_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_composite_value(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), days=days, assets=assets, format=ReturnFormat.JSON
    )
    assert response == composite_value_response


def test_get_issuer_market_cap(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {"universe": ["2046251", "2588173"], "issuerMarketCap": [1000000000, 2000000000]},
            }
        ],
        'totalResults': 1,
    }

    issuer_market_cap_response = {'2588173': {'2022-04-05': 2000000000}, '2046251': {'2022-04-05': 1000000000}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_issuer_market_cap(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == issuer_market_cap_response


def test_get_asset_price(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [{"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "price": [100, 200]}}],
        'totalResults': 1,
    }

    price_response = {'2588173': {'2022-04-05': 200}, '2046251': {'2022-04-05': 100}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_asset_price(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == price_response


def test_get_asset_capitalization(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {"universe": ["2046251", "2588173"], "capitalization": [1000000000, 2000000000]},
            }
        ],
        'totalResults': 1,
    }

    capitalization_response = {'2588173': {'2022-04-05': 2000000000}, '2046251': {'2022-04-05': 1000000000}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_asset_capitalization(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == capitalization_response


def test_get_currency(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "currency": ["USD", "GBP"]}}
        ],
        'totalResults': 1,
    }

    currency_response = {'2588173': {'2022-04-05': "GBP"}, '2046251': {'2022-04-05': "USD"}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_currency(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == currency_response


def test_get_unadjusted_specific_risk(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "assetData": {"universe": ["2046251", "2588173"], "unadjustedSpecificRisk": [0.5, 1.6]},
            }
        ],
        'totalResults': 1,
    }

    unadjusted_specific_risk_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_unadjusted_specific_risk(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == unadjusted_specific_risk_response


def test_get_dividend_yield(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "dividendYield": [0.5, 1.6]}}
        ],
        'totalResults': 1,
    }

    dividend_yield_response = {'2588173': {'2022-04-05': 1.6}, '2046251': {'2022-04-05': 0.5}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_dividend_yield(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == dividend_yield_response


def test_get_model_price(mocker):
    model = mock_macro_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {"date": "2022-04-05", "assetData": {"universe": ["2046251", "2588173"], "modelPrice": [100, 200]}}
        ],
        'totalResults': 1,
    }

    model_price_response = {'2588173': {'2022-04-05': 200}, '2046251': {'2022-04-05': 100}}

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_model_price(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == model_price_response


def test_get_covariance_matrix(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "factorData": [
                    {"factorId": "1", "factorName": "factor1"},
                    {"factorId": "2", "factorName": "factor2"},
                ],
                "covarianceMatrix": [
                    [0.5, 0.6],
                    [0.3, 0.7],
                ],
            }
        ],
        'totalResults': 1,
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_covariance_matrix(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == results['results']


def test_get_unadjusted_covariance_matrix(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "factorData": [
                    {"factorId": "1", "factorName": "factor1"},
                    {"factorId": "2", "factorName": "factor2"},
                ],
                "unadjustedCovarianceMatrix": [
                    [0.5, 0.6],
                    [0.3, 0.7],
                ],
            }
        ],
        'totalResults': 1,
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_unadjusted_covariance_matrix(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == results['results']


def test_get_pre_vra_covariance_matrix(mocker):
    model = mock_risk_model(mocker)

    universe = ["2046251", "2588173"]
    assets = DataAssetsRequest(UniverseIdentifier.sedol, universe)

    results = {
        'results': [
            {
                "date": "2022-04-05",
                "factorData": [
                    {"factorId": "1", "factorName": "factor1"},
                    {"factorId": "2", "factorName": "factor2"},
                ],
                "preVRACovarianceMatrix": [
                    [0.5, 0.6],
                    [0.3, 0.7],
                ],
            }
        ],
        'totalResults': 1,
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_pre_vra_covariance_matrix(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), assets=assets, format=ReturnFormat.JSON
    )
    assert response == results['results']


def test_get_risk_free_rate(mocker):
    model = mock_risk_model(mocker)

    currencies = ["EUR", "INR"]
    risk_free_rate = [1.08, 0.012]
    results = {
        'results': [
            {
                "date": "2022-04-05",
                "currencyRatesData": {
                    "riskFreeRate": risk_free_rate,
                    "currency": currencies,
                },
            }
        ],
        'totalResults': 1,
    }

    risk_free_rate_response = {
        "currency": {('2022-04-05', 0): 'EUR', ('2022-04-05', 1): 'INR'},
        "riskFreeRate": {('2022-04-05', 0): 1.08, ('2022-04-05', 1): 0.012},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_risk_free_rate(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), format=ReturnFormat.JSON
    )
    assert response == risk_free_rate_response

    # filter risk free rates by currency
    expected_filtered_rates = {"currency": {('2022-04-05', 1): 'INR'}, "riskFreeRate": {('2022-04-05', 1): 0.012}}
    actual_filtered_rates = model.get_risk_free_rate(
        start_date=dt.date(2022, 4, 5),
        end_date=dt.date(2022, 4, 5),
        currencies=[Currency.INR],
        format=ReturnFormat.JSON,
    )
    assert actual_filtered_rates == expected_filtered_rates

    # test DataFrame return format
    expected_data_frame = get_optional_data_as_dataframe(
        [
            {
                "date": "2022-04-05",
                "currencyRatesData": {
                    "riskFreeRate": risk_free_rate,
                    "currency": currencies,
                },
            }
        ],
        "currencyRatesData",
    )

    expected_data_frame = expected_data_frame.loc[expected_data_frame['currency'].isin(['INR'])]
    actual_data_frame = model.get_risk_free_rate(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), currencies=[Currency.INR]
    )

    assert_frame_equal(expected_data_frame, actual_data_frame, check_like=True)


def test_get_currency_exchange_rate(mocker):
    model = mock_risk_model(mocker)

    currencies = ["EUR", "INR"]
    currency_exchange_rate = [1.08, 0.012]
    results = {
        'results': [
            {
                "date": "2022-04-05",
                "currencyRatesData": {
                    "exchangeRate": currency_exchange_rate,
                    "currency": currencies,
                },
            }
        ],
        'totalResults': 1,
    }

    currency_exchange_rate_response = {
        "currency": {('2022-04-05', 0): 'EUR', ('2022-04-05', 1): 'INR'},
        "exchangeRate": {('2022-04-05', 0): 1.08, ('2022-04-05', 1): 0.012},
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=results)

    response = model.get_currency_exchange_rate(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), format=ReturnFormat.JSON
    )
    assert response == currency_exchange_rate_response

    # filter risk free rates by currency
    expected_filtered_rates = {"currency": {('2022-04-05', 1): 'INR'}, "exchangeRate": {('2022-04-05', 1): 0.012}}
    actual_filtered_rates = model.get_currency_exchange_rate(
        start_date=dt.date(2022, 4, 5),
        end_date=dt.date(2022, 4, 5),
        currencies=[Currency.INR],
        format=ReturnFormat.JSON,
    )
    assert actual_filtered_rates == expected_filtered_rates

    # test DataFrame return format
    expected_data_frame = get_optional_data_as_dataframe(
        [
            {
                "date": "2022-04-05",
                "currencyRatesData": {
                    "exchangeRate": currency_exchange_rate,
                    "currency": currencies,
                },
            }
        ],
        "currencyRatesData",
    )

    expected_data_frame = expected_data_frame.loc[expected_data_frame['currency'].isin(['INR'])]

    actual_data_frame = model.get_currency_exchange_rate(
        start_date=dt.date(2022, 4, 5), end_date=dt.date(2022, 4, 5), currencies=[Currency.INR]
    )

    assert_frame_equal(expected_data_frame, actual_data_frame, check_like=True)


# ==================== Branch coverage tests for missing branches ====================


class TestFactorDataMeasureBranches:
    """Cover _get_factor_data_measure and related caller branches."""

    @patch('gs_quant.models.risk_model.build_factor_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_returns_by_name_no_assets(self, mock_api, mock_build):
        """Branch [882,885]: _get_factor_data_measure with assets=None (if assets: is False).
        Also covers line 826 (get_factor_returns_by_name entry)."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'Momentum': {'2022-01-03': 0.01}})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_returns_by_name(
            start_date=dt.date(2022, 1, 3),
            assets=None,
        )
        assert isinstance(result, pd.DataFrame)
        # Verify limit_factors is False when assets is None
        call_kwargs = mock_api.get_risk_model_data.call_args.kwargs
        assert call_kwargs['limit_factors'] is False

    @patch('gs_quant.models.risk_model.build_factor_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_returns_by_name_with_assets(self, mock_api, mock_build):
        """Branch [882,883]: _get_factor_data_measure with assets provided (if assets: is True)."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'Momentum': {'2022-01-03': 0.01}})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_returns_by_name(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
        )
        assert isinstance(result, pd.DataFrame)
        call_kwargs = mock_api.get_risk_model_data.call_args.kwargs
        assert call_kwargs['limit_factors'] is True

    @patch('gs_quant.models.risk_model.build_factor_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_returns_by_id(self, mock_api, mock_build):
        """Covers line 866 (get_factor_returns_by_id entry).
        Uses factors_by_name=False path."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'1': {'2022-01-03': 0.01}})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_returns_by_id(
            start_date=dt.date(2022, 1, 3),
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_factor_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_factor_returns_by_name_json_format(self, mock_api, mock_build):
        """Branch in _get_factor_data_measure: format != DATA_FRAME -> .to_dict()"""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [
                        {'factorId': '1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                }
            ]
        }
        mock_build.return_value = pd.DataFrame({'Momentum': {'2022-01-03': 0.01}})
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_factor_returns_by_name(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, dict)


class TestAssetDataMeasureBranches:
    """Cover _get_asset_data_measure branches."""

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_specific_risk_json_format(self, mock_api, mock_build):
        """Branch [908,909]: format != DATA_FRAME in _get_asset_data_measure.
        Also covers line 1008 (get_specific_risk entry)."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'specificRisk': [0.05],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 0.05}}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_specific_risk(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.JSON,
        )
        # JSON format returns dict (not DataFrame)
        assert isinstance(result, dict)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_specific_risk_dataframe_format(self, mock_api, mock_build):
        """Branch [908,909]: format == DATA_FRAME -> pd.DataFrame(measure_data).
        Covers the if branch."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'specificRisk': [0.05],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 0.05}}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_specific_risk(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_total_risk(self, mock_api, mock_build):
        """Covers line 1500 (get_total_risk entry)."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'totalRisk': [15.0],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 15.0}}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_total_risk(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_historical_beta(self, mock_api, mock_build):
        """Covers line 1538 (get_historical_beta entry)."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'assetData': {
                        'universe': ['abc'],
                        'historicalBeta': [1.1],
                    },
                }
            ]
        }
        mock_build.return_value = {'abc': {'2022-01-03': 1.1}}
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_historical_beta(
            start_date=dt.date(2022, 1, 3),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)


class TestGetUniverseFactorExposure:
    """Cover line 2155 (get_universe_factor_exposure entry)."""

    @patch('gs_quant.models.risk_model.build_asset_data_map')
    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_get_universe_factor_exposure(self, mock_api, mock_build):
        """Covers line 2155 via FactorRiskModel.get_universe_factor_exposure
        which calls super().get_universe_exposure."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-01-03',
                    'factorData': [{'factorId': '1', 'factorName': 'Momentum'}],
                    'assetData': {
                        'universe': ['abc'],
                        'factorExposure': [{'1': 0.5}],
                    },
                }
            ]
        }
        mock_build.return_value = {
            'abc': {'2022-01-03': {'1': 0.5}},
        }
        model = FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        result = model.get_universe_factor_exposure(
            start_date=dt.date(2022, 1, 3),
            assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
            get_factors_by_name=False,
        )
        assert isinstance(result, pd.DataFrame)


class TestGetAssetContributionToRisk:
    """Cover get_asset_contribution_to_risk branches (lines 2630-2690)."""

    def _make_model(self):
        return FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )

    @patch('gs_quant.models.risk_model.SecurityMaster')
    def test_no_spot_price_raises(self, mock_sm):
        """Branches [2633,2634]: len(series) == 0 -> raise MqValueError."""
        mock_security = MagicMock()
        mock_coord = MagicMock()
        mock_coord.get_series.return_value = pd.Series([], dtype=float)
        mock_security.get_data_coordinate.return_value = mock_coord
        mock_sm.get_asset.return_value = mock_security

        model = self._make_model()
        with pytest.raises(MqValueError, match='has no end of day price'):
            model.get_asset_contribution_to_risk('AAPL UW', dt.date(2022, 5, 2))

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.SecurityMaster')
    def test_empty_total_risk_raises(self, mock_sm, mock_api):
        """Branches [2633,2635] and [2654,2655]: spot price found but totalRisk is empty."""
        mock_security = MagicMock()
        mock_coord = MagicMock()
        mock_coord.get_series.return_value = pd.Series([100.0])
        mock_security.get_data_coordinate.return_value = mock_coord
        mock_sm.get_asset.return_value = mock_security

        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'assetData': {
                        'totalRisk': [],
                        'factorExposure': [{'f1': 0.5}],
                        'universe': ['AAPL UW'],
                    },
                    'covarianceMatrix': [[0.01]],
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorCategory': 'Style'},
                    ],
                }
            ]
        }

        model = self._make_model()
        with pytest.raises(MqValueError, match='is not covered by'):
            model.get_asset_contribution_to_risk('AAPL UW', dt.date(2022, 5, 2))

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.SecurityMaster')
    def test_full_contribution_to_risk_by_name(self, mock_sm, mock_api):
        """Branches [2654,2656], [2672,2673]: normal path with factors, get_factors_by_name=True.
        Covers the for loop body and the Specific row append."""
        import numpy as np
        mock_security = MagicMock()
        mock_coord = MagicMock()
        mock_coord.get_series.return_value = pd.Series([100.0])
        mock_security.get_data_coordinate.return_value = mock_coord
        mock_sm.get_asset.return_value = mock_security

        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'assetData': {
                        'totalRisk': [20.0],
                        'factorExposure': [{'f1': 0.5, 'f2': -0.3}],
                        'universe': ['AAPL UW'],
                    },
                    'covarianceMatrix': [[0.04, 0.01], [0.01, 0.03]],
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorCategory': 'Style'},
                        {'factorId': 'f2', 'factorName': 'Value', 'factorCategory': 'Style'},
                    ],
                }
            ]
        }

        model = self._make_model()
        result = model.get_asset_contribution_to_risk(
            'AAPL UW', dt.date(2022, 5, 2),
            get_factors_by_name=True,
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)
        assert 'Factor' in result.columns
        assert 'Specific' in result['Factor'].values

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.SecurityMaster')
    def test_contribution_to_risk_by_id(self, mock_sm, mock_api):
        """Branch [2672,2673]: get_factors_by_name=False uses factorId."""
        mock_security = MagicMock()
        mock_coord = MagicMock()
        mock_coord.get_series.return_value = pd.Series([100.0])
        mock_security.get_data_coordinate.return_value = mock_coord
        mock_sm.get_asset.return_value = mock_security

        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'assetData': {
                        'totalRisk': [20.0],
                        'factorExposure': [{'f1': 0.5}],
                        'universe': ['AAPL UW'],
                    },
                    'covarianceMatrix': [[0.04]],
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorCategory': 'Style'},
                    ],
                }
            ]
        }

        model = self._make_model()
        result = model.get_asset_contribution_to_risk(
            'AAPL UW', dt.date(2022, 5, 2),
            get_factors_by_name=False,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, list)
        # Last entry should be the Specific row with ID 'SPC'
        assert result[-1]['Factor'] == 'SPC'

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    @patch('gs_quant.models.risk_model.SecurityMaster')
    def test_contribution_to_risk_json_format(self, mock_sm, mock_api):
        """Branch [2672,2681]: for loop finishing and returning JSON (list) result."""
        mock_security = MagicMock()
        mock_coord = MagicMock()
        mock_coord.get_series.return_value = pd.Series([100.0])
        mock_security.get_data_coordinate.return_value = mock_coord
        mock_sm.get_asset.return_value = mock_security

        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'assetData': {
                        'totalRisk': [20.0],
                        'factorExposure': [{'f1': 0.5}],
                        'universe': ['AAPL UW'],
                    },
                    'covarianceMatrix': [[0.04]],
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorCategory': 'Style'},
                    ],
                }
            ]
        }

        model = self._make_model()
        result = model.get_asset_contribution_to_risk(
            'AAPL UW', dt.date(2022, 5, 2),
            get_factors_by_name=True,
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)
        # Factor row + Specific row
        assert len(result) == 2
        assert result.iloc[-1]['Factor'] == 'Specific'


class TestGetAssetFactorAttribution:
    """Cover get_asset_factor_attribution branches (lines 2745-2765)."""

    def _make_model(self):
        return FactorRiskModel(
            'test_id', 'Test', RiskModelCoverage.Country, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_empty_results_raises(self, mock_api):
        """Branch [2745,2746]: len(risk_results) == 0 -> raise MqValueError."""
        mock_api.get_risk_model_data.return_value = {'results': []}

        model = self._make_model()
        with pytest.raises(MqValueError, match='is not covered by'):
            model.get_asset_factor_attribution(
                'AAPL UW', dt.date(2022, 5, 1), dt.date(2022, 5, 2),
            )

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_single_day_raises(self, mock_api):
        """Branch [2747,2748]: len(risk_results) < 2 -> raise MqValueError."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-05-02',
                    'factorData': [{'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.01}],
                    'assetData': {'factorExposure': [{'f1': 0.5}], 'universe': ['AAPL UW']},
                }
            ]
        }

        model = self._make_model()
        with pytest.raises(MqValueError, match='Attribution cannot be calculated'):
            model.get_asset_factor_attribution(
                'AAPL UW', dt.date(2022, 5, 2), dt.date(2022, 5, 2),
            )

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_full_attribution_by_name(self, mock_api):
        """Branches [2747,2749], [2751,2752], [2753,2754], [2759,2760]:
        Normal path with get_factors_by_name=True, multiple days, factor data and exposures."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-05-01',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.005},
                    ],
                    'assetData': {'factorExposure': [{'f1': 0.5}], 'universe': ['AAPL UW']},
                },
                {
                    'date': '2022-05-02',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                    'assetData': {'factorExposure': [{'f1': 0.6}], 'universe': ['AAPL UW']},
                },
            ]
        }

        model = self._make_model()
        result = model.get_asset_factor_attribution(
            'AAPL UW', dt.date(2022, 5, 1), dt.date(2022, 5, 2),
            get_factors_by_name=True,
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)
        assert 'Date' in result.columns
        assert 'Momentum' in result.columns
        # Factor return 0.01 * previous exposure 0.5 = 0.005
        assert result.iloc[0]['Momentum'] == pytest.approx(0.01 * 0.5)

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_full_attribution_by_id(self, mock_api):
        """Branches [2753,2754], [2759,2760] with get_factors_by_name=False."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-05-01',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.005},
                    ],
                    'assetData': {'factorExposure': [{'f1': 0.5}], 'universe': ['AAPL UW']},
                },
                {
                    'date': '2022-05-02',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                    'assetData': {'factorExposure': [{'f1': 0.6}], 'universe': ['AAPL UW']},
                },
            ]
        }

        model = self._make_model()
        result = model.get_asset_factor_attribution(
            'AAPL UW', dt.date(2022, 5, 1), dt.date(2022, 5, 2),
            get_factors_by_name=False,
            format=ReturnFormat.JSON,
        )
        assert isinstance(result, list)
        assert 'f1' in result[0]

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_attribution_empty_factor_data(self, mock_api):
        """Branch [2753,2758]: inner for loop skipped when factorData is empty."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-05-01',
                    'factorData': [],
                    'assetData': {'factorExposure': [{}], 'universe': ['AAPL UW']},
                },
                {
                    'date': '2022-05-02',
                    'factorData': [],
                    'assetData': {'factorExposure': [{}], 'universe': ['AAPL UW']},
                },
            ]
        }

        model = self._make_model()
        result = model.get_asset_factor_attribution(
            'AAPL UW', dt.date(2022, 5, 1), dt.date(2022, 5, 2),
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)
        # Only the Date column since no factors
        assert 'Date' in result.columns

    @patch('gs_quant.models.risk_model.GsFactorRiskModelApi')
    def test_attribution_empty_previous_exposures(self, mock_api):
        """Branch [2759,2763]: factor exposure loop skipped when previous_factor_exposures is empty."""
        mock_api.get_risk_model_data.return_value = {
            'results': [
                {
                    'date': '2022-05-01',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.005},
                    ],
                    'assetData': {'factorExposure': [{}], 'universe': ['AAPL UW']},
                },
                {
                    'date': '2022-05-02',
                    'factorData': [
                        {'factorId': 'f1', 'factorName': 'Momentum', 'factorReturn': 0.01},
                    ],
                    'assetData': {'factorExposure': [{'f1': 0.6}], 'universe': ['AAPL UW']},
                },
            ]
        }

        model = self._make_model()
        result = model.get_asset_factor_attribution(
            'AAPL UW', dt.date(2022, 5, 1), dt.date(2022, 5, 2),
            get_factors_by_name=True,
            format=ReturnFormat.DATA_FRAME,
        )
        assert isinstance(result, pd.DataFrame)
        # factorReturn was set but not multiplied by exposure (empty dict)
        assert result.iloc[0]['Momentum'] == pytest.approx(0.01)


class TestMacroUniverseSensitivityBranches:
    """Cover get_universe_sensitivity branches [2942,2945] and [2963,2964]."""

    @patch('gs_quant.models.risk_model.MarqueeRiskModel.get_universe_exposure')
    def test_sensitivity_category_type_non_empty_dataframe(self, mock_exposure):
        """Branch [2942,2945]: factor_type != Factor AND sensitivity_df is NOT empty.
        This hits the full category aggregation logic."""
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        # Build the non-empty sensitivity DataFrame that get_universe_exposure would return
        sensitivity_df = pd.DataFrame(
            {'f1': [0.5, 0.3], 'f2': [0.2, 0.1]},
            index=pd.MultiIndex.from_tuples([('abc', '2022-01-03'), ('def', '2022-01-03')]),
        )
        mock_exposure.return_value = sensitivity_df
        # Factor data for the category mapping
        factor_data_df = pd.DataFrame({
            'name': ['Momentum', 'Value'],
            'identifier': ['f1', 'f2'],
            'factorCategory': ['Style', 'Style'],
            'factorCategoryId': ['cat1', 'cat1'],
        })

        with patch.object(model, 'get_factor_data', return_value=factor_data_df):
            result = model.get_universe_sensitivity(
                start_date=dt.date(2022, 1, 3),
                assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc', 'def']),
                factor_type=FactorType.Category,
                get_factors_by_name=False,
                format=ReturnFormat.DATA_FRAME,
            )
            assert isinstance(result, pd.DataFrame)
            assert not result.empty

    @patch('gs_quant.models.risk_model.MarqueeRiskModel.get_universe_exposure')
    def test_sensitivity_category_type_json_format(self, mock_exposure):
        """Branch [2963,2964]: format == JSON in the Category aggregation path."""
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        sensitivity_df = pd.DataFrame(
            {'f1': [0.5], 'f2': [0.2]},
            index=pd.MultiIndex.from_tuples([('abc', '2022-01-03')]),
        )
        mock_exposure.return_value = sensitivity_df
        factor_data_df = pd.DataFrame({
            'name': ['Momentum', 'Value'],
            'identifier': ['f1', 'f2'],
            'factorCategory': ['Style', 'Style'],
            'factorCategoryId': ['cat1', 'cat1'],
        })

        with patch.object(model, 'get_factor_data', return_value=factor_data_df):
            result = model.get_universe_sensitivity(
                start_date=dt.date(2022, 1, 3),
                assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
                factor_type=FactorType.Category,
                get_factors_by_name=False,
                format=ReturnFormat.JSON,
            )
            assert isinstance(result, dict)

    @patch('gs_quant.models.risk_model.MarqueeRiskModel.get_universe_exposure')
    def test_sensitivity_category_by_name(self, mock_exposure):
        """Branch [2942,2945]: Category type with get_factors_by_name=True."""
        model = MacroRiskModel(
            'macro_id', 'Macro Model', RiskModelCoverage.Global, RiskModelTerm.Long,
            RiskModelUniverseIdentifier.gsid, 'GS', 1.0,
        )
        sensitivity_df = pd.DataFrame(
            {'Momentum': [0.5], 'Value': [0.2]},
            index=pd.MultiIndex.from_tuples([('abc', '2022-01-03')]),
        )
        mock_exposure.return_value = sensitivity_df
        factor_data_df = pd.DataFrame({
            'name': ['Momentum', 'Value'],
            'identifier': ['f1', 'f2'],
            'factorCategory': ['Style', 'Style'],
            'factorCategoryId': ['cat1', 'cat1'],
        })

        with patch.object(model, 'get_factor_data', return_value=factor_data_df):
            result = model.get_universe_sensitivity(
                start_date=dt.date(2022, 1, 3),
                assets=DataAssetsRequest(UniverseIdentifier.gsid, ['abc']),
                factor_type=FactorType.Category,
                get_factors_by_name=True,
                format=ReturnFormat.DATA_FRAME,
            )
            assert isinstance(result, pd.DataFrame)
            assert not result.empty


if __name__ == "__main__":
    pytest.main([__file__])
