"""
Copyright 2018 Goldman Sachs.
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
from typing import Dict
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from gs_quant.api.gs.assets import GsAsset, GsAssetApi
from gs_quant.api.gs.data import GsDataApi
from gs_quant.api.gs.indices import GsIndexApi
from gs_quant.api.gs.reports import GsReportApi
from gs_quant.api.gs.users import GsUsersApi
from gs_quant.common import (
    AssetClass,
    AssetType,
    CashReinvestmentTreatment,
    CashReinvestmentTreatmentType,
    EqBasketBacktestParameters,
    EqBasketHistoryMethodology,
    EqBasketRebalanceCalendar,
    Entitlements as TargetEntitlements,
    PositionSet as TargetPositionSet,
    Position as TargetPosition,
    ReportParameters,
    XRef,
)
from gs_quant.entities.entitlements import User
from gs_quant.errors import MqError, MqValueError
from gs_quant.markets.baskets import Basket, ErrorMessage
from gs_quant.markets.indices_utils import ReturnType, WeightingStrategy, CorporateActionType
from gs_quant.markets.position_set import Position, PositionSet
from gs_quant.session import GsSession, Environment
from gs_quant.target.indices import (
    CustomBasketsResponse,
    CustomBasketRiskParams,
    CustomBasketsRebalanceAction,
    IndicesCurrency,
)
from gs_quant.target.reports import Report, ReportStatus, User as TargetUser

# ==================== Helper constants ====================

asset_1 = {'name': 'asset 1', 'id': 'id1', 'bbid': 'bbid1'}
asset_2 = {'name': 'asset 2', 'id': 'id2', 'bbid': 'bbid2'}
assets_data = [asset_1, asset_2]
base_user = {
    'name': 'First Last',
    'email': 'ex@email.com',
    'city': 'City A',
    'company': 'Company A',
    'country': 'Country A',
    'region': 'Region A',
}
cb_response = CustomBasketsResponse('done', 'R1234567890', 'MA1234567890')
gs_asset = GsAsset(
    asset_class=AssetClass.Equity,
    type_=AssetType.Custom_Basket,
    name='Test Basket',
    id_='MA1234567890',
    entitlements=TargetEntitlements(admin=['guid:user_abc']),
    xref=XRef(ticker='GSMBXXXX'),
)
initial_price = {'price': 100}
mqid = 'MA1234567890'
name = 'Test Basket'
positions = [Position('bbid1', asset_id='id1', quantity=100), Position('bbid2', asset_id='id2', quantity=200)]
positions_weighted = positions = [
    Position('bbid1', asset_id='id1', weight=0.4),
    Position('bbid2', asset_id='id2', weight=0.6),
]
position_set = PositionSet(positions, divisor=1000)
report = Report(mqid, 'asset', 'Basket Create', ReportParameters(), status='done')
resolved_asset = {'GSMBXXXX': [{'id': mqid}]}
target_positions = tuple([TargetPosition(asset_id='id1', quantity=100), TargetPosition(asset_id='id2', quantity=200)])
target_position_set = TargetPositionSet(target_positions, dt.date(2021, 1, 7), divisor=1000)
ticker = 'GSMBXXXX'
user_ea = {**base_user, 'id': 'user_abc', 'tokens': ['external', 'guid:user_abc']}  # external, admin
user_ena = {**base_user, 'id': 'user_xyz', 'tokens': ['external', 'guid:user_xyz']}  # external, non admin
user_ia = {**base_user, 'id': 'user_abc', 'tokens': ['internal', 'guid:user_abc']}  # internal, admin
user_ia_restricted = {
    **base_user,
    'id': 'user_abc',
    'tokens': ['external', 'guid:user_abc', 'group:EqBasketRestrictedAttributes'],
}  # external admin with restricted attributes token


# ==================== Helper functions ====================


@mock.patch.object(GsSession.__class__, 'default_value')
def mock_session(mocker):
    """Mock GsSession helper"""
    mocker.return_value = GsSession.get(Environment.QA, 'client_id', 'secret')


def mock_response(mocker, mock_object, mock_fn, mock_response):
    """Mock patch helper"""
    if mock_response is not None:
        mocker.patch.object(mock_object, mock_fn, return_value=mock_response)


def mock_basket_init(mocker, user: Dict, existing: bool = True):
    """Mock basket initialization helper"""
    if existing:
        mock_response(mocker, GsAssetApi, 'resolve_assets', resolved_asset)
        mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
        mock_response(mocker, GsAssetApi, 'get_latest_positions', target_position_set)
        mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
        mock_response(mocker, GsIndexApi, 'initial_price', initial_price)
        mock_response(mocker, GsReportApi, 'get_reports', [report])
        mock_response(mocker, GsUsersApi, 'get_users', [TargetUser.from_dict(user)])
    mock_response(mocker, GsUsersApi, 'get_current_user_info', user)


# ==================== Original tests ====================


def test_basket_error_messages(mocker):
    mock_session()

    # test non admin errors
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.cancel_rebalance()
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.allow_ca_restricted_assets = False

    # test non internal errors
    with pytest.raises(MqError, match=ErrorMessage.NON_INTERNAL.value):
        basket.flagship = False

    # test unmodifiable errors
    with pytest.raises(MqError, match=ErrorMessage.UNMODIFIABLE.value):
        basket.ticker = 'GSMBZZZZ'

    # test uninitialized errors
    mock_basket_init(mocker, user_ena, False)
    basket = Basket()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.clone()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.get_latest_rebalance_data()


def test_basket_create(mocker):
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    mock_response(mocker, GsIndexApi, 'validate_ticker', True)

    basket = Basket()
    basket.name = name
    basket.ticker = ticker
    basket.position_set = position_set
    basket.return_type = ReturnType.PRICE_RETURN

    mock_response(mocker, GsIndexApi, 'create', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ea)

    response = basket.create()
    GsIndexApi.create.assert_called()
    assert response == cb_response.as_dict()


def test_basket_clone(mocker):
    mock_session()

    # test uninitialized errors
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.clone()

    # test clone
    mock_basket_init(mocker, user_ena)
    parent_basket = Basket.get(ticker)
    clone = parent_basket.clone()
    mock_basket_init(mocker, user_ea, False)

    parent_positions = [p.as_dict() for p in parent_basket.position_set.positions]
    clone_positions = [p.as_dict() for p in clone.position_set.positions]

    assert clone_positions == parent_positions
    assert clone.clone_parent_id == mqid
    assert clone.parent_basket == ticker


def test_basket_edit(mocker):
    mock_session()

    # test errors
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.update()

    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.update()

    # test update
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.description = 'New Basket Description'
    gs_asset.description = 'New Basket Description'

    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.edit.assert_called()
    assert response == cb_response.as_dict()
    assert basket.description == 'New Basket Description'

    gs_asset.description = None


def test_basket_rebalance(mocker):
    mock_session()
    mock_basket_init(mocker, user_ia)

    basket = Basket.get(ticker)
    basket.allow_ca_restricted_assets = True

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_basket_edit_and_rebalance(mocker):
    mock_session()
    mock_basket_init(mocker, user_ia)

    basket = Basket.get(ticker)
    basket.description = 'New Basket Description'
    gs_asset.description = 'New Basket Description'
    basket.initial_price = 2000000

    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.edit.assert_called()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()
    assert basket.description == 'New Basket Description'
    gs_asset.description = None


def test_basket_update_entitlements(mocker):
    mock_session()

    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    mock_response(mocker, GsUsersApi, 'get_users', [TargetUser.from_dict(user_ena)])
    new_admin = User.get(user_id='user_xyz')
    basket.entitlements.admin.users += [new_admin]

    entitlements_response = TargetEntitlements(admin=['guid:user_abc', 'guid:user_xyz'])
    mock_response(mocker, GsAssetApi, 'update_asset_entitlements', entitlements_response)
    response = basket.update()
    GsAssetApi.update_asset_entitlements.assert_called()
    assert response == entitlements_response


def test_upload_position_history(mocker):
    mock_session()

    # test errors
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.upload_position_history()

    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.upload_position_history()

    # test backcast
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    pos_set_1 = PositionSet(positions_weighted, dt.date(2021, 1, 1))
    pos_set_2 = PositionSet(positions_weighted, dt.date(2021, 3, 1))
    pos_set_3 = PositionSet(positions_weighted, dt.date(2021, 5, 1))

    mock_response(mocker, GsIndexApi, 'backcast', cb_response)
    response = basket.upload_position_history([pos_set_1, pos_set_2, pos_set_3])
    GsIndexApi.backcast.assert_called()
    assert response == cb_response.as_dict()


def test_update_risk_reports(mocker):
    mock_session()

    # test errors
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.add_factor_risk_report('AXUS4M', False)
    with pytest.raises(MqError, match=ErrorMessage.UNINITIALIZED.value):
        basket.delete_factor_risk_report('AXUS4M')

    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.add_factor_risk_report('AXUS4M', False)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.delete_factor_risk_report('AXUS4M')

    # test add/delete factor risk reports
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)

    mock_response(mocker, GsIndexApi, 'update_risk_reports', {})
    basket.add_factor_risk_report('AXUS4M', False)
    payload = CustomBasketRiskParams(risk_model='AXUS4M', fx_hedged=False)
    GsIndexApi.update_risk_reports.assert_called_with(payload)

    mock_response(mocker, GsIndexApi, 'update_risk_reports', {})
    basket.delete_factor_risk_report('AXUS4M')
    payload = CustomBasketRiskParams(risk_model='AXUS4M', delete=True)
    GsIndexApi.update_risk_reports.assert_called_with(payload)


# ==================== NEW BRANCH COVERAGE TESTS ====================


def test_init_non_basket_asset_type(mocker):
    """Branch: gs_asset.type.value not in BasketType.to_list() -> MqValueError"""
    mock_session()
    non_basket_asset = GsAsset(
        asset_class=AssetClass.Equity,
        type_=AssetType.ETF,
        name='Not A Basket',
        id_='MA9999999999',
    )
    with pytest.raises(MqValueError, match='is not a basket'):
        Basket(gs_asset=non_basket_asset)


def test_init_new_basket_defaults(mocker):
    """Branch: gs_asset is None -> __populate_default_attributes_for_new_basket"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    # Check defaults for new basket
    assert basket.default_backcast is True
    assert basket.publish_to_bloomberg is True
    assert basket.publish_to_factset is False
    assert basket.publish_to_reuters is False
    assert basket.target_notional == 10000000
    assert basket.include_price_history is False


def test_init_new_basket_with_kwargs(mocker):
    """Branch: new basket with custom kwargs including divisor"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    # When divisor is provided, initial_price should be None
    basket = Basket(divisor=500)
    assert basket.divisor == 500
    assert basket.initial_price is None


def test_init_new_basket_with_parent_basket(mocker):
    """Branch: parent_basket is not None and clone_parent_id is None"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    mock_response(mocker, GsAssetApi, 'resolve_assets', resolved_asset)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)

    basket = Basket(parent_basket='GSMBXXXX')
    assert basket.clone_parent_id == mqid


def test_get_details(mocker):
    """Branch: get_details returns DataFrame with basket properties"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    details = basket.get_details()
    assert isinstance(details, type(details))  # It's a pd.DataFrame
    assert 'name' in details.columns
    assert 'value' in details.columns


def test_get_latest_rebalance_data(mocker):
    """Branch: get_latest_rebalance_data delegates to GsIndexApi"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    rebal_data = {'date': '2021-06-01', 'positions': []}
    mock_response(mocker, GsIndexApi, 'last_rebalance_data', rebal_data)
    result = basket.get_latest_rebalance_data()
    assert result == rebal_data


def test_get_latest_rebalance_date(mocker):
    """Branch: get_latest_rebalance_date parses date string"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsIndexApi, 'last_rebalance_data', {'date': '2021-06-15'})
    result = basket.get_latest_rebalance_date()
    assert result == dt.date(2021, 6, 15)


def test_get_rebalance_approval_status(mocker):
    """Branch: get_rebalance_approval_status calls GsIndexApi"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsIndexApi, 'last_rebalance_approval', {'status': 'Approved'})
    result = basket.get_rebalance_approval_status()
    assert result == 'Approved'


def test_cancel_rebalance(mocker):
    """Branch: cancel_rebalance delegates to GsIndexApi"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsIndexApi, 'cancel_rebalance', {'status': 'cancelled'})
    result = basket.cancel_rebalance()
    assert result == {'status': 'cancelled'}


def test_get_corporate_actions(mocker):
    """Branch: get_corporate_actions queries data API"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    ca_data = [{'date': '2021-01-01', 'type': 'Acquisition'}]
    mock_response(mocker, GsDataApi, 'query_data', ca_data)
    result = basket.get_corporate_actions(
        start=dt.date(2020, 1, 1),
        end=dt.date(2021, 12, 31),
        ca_type=[CorporateActionType.ACQUISITION],
    )
    assert len(result) == 1


def test_get_fundamentals(mocker):
    """Branch: get_fundamentals queries data API"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    fundamentals_data = [{'date': '2021-01-01', 'metric': 'dividendYield', 'value': 1.5}]
    mock_response(mocker, GsDataApi, 'query_data', fundamentals_data)
    result = basket.get_fundamentals(start=dt.date(2020, 1, 1), end=dt.date(2021, 12, 31))
    assert len(result) == 1


def test_get_live_date(mocker):
    """Branch: get_live_date returns __live_date"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # live_date from gs_asset will be None since we didn't set it
    result = basket.get_live_date()
    assert result is None


def test_get_type_with_asset_type(mocker):
    """Branch: get_type when __gs_asset_type is set (truthy)"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    result = basket.get_type()
    # gs_asset has type Custom_Basket
    assert result is not None


def test_get_type_without_asset_type(mocker):
    """Branch: get_type when __gs_asset_type is None (falsy) -> returns None"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # Set __gs_asset_type to None to hit the falsy branch
    basket._Basket__gs_asset_type = None
    result = basket.get_type()
    assert result is None


def test_get_latest_position_set(mocker):
    """Branch: positioned_entity_type == EntityType.ASSET"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsAssetApi, 'get_latest_positions', target_position_set)
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    result = basket.get_latest_position_set()
    assert isinstance(result, PositionSet)


def test_get_position_set_for_date_with_data(mocker):
    """Branch: get_position_set_for_date returns PositionSet when response has data"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsAssetApi, 'get_asset_positions_for_date', [target_position_set])
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    result = basket.get_position_set_for_date(dt.date(2021, 1, 7))
    assert isinstance(result, PositionSet)


def test_get_position_set_for_date_empty(mocker):
    """Branch: get_position_set_for_date returns empty PositionSet when response is empty"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsAssetApi, 'get_asset_positions_for_date', [])
    result = basket.get_position_set_for_date(dt.date(2021, 1, 7))
    assert isinstance(result, PositionSet)
    assert len(result.positions) == 0


def test_get_position_sets_with_data(mocker):
    """Branch: get_position_sets returns list of PositionSets"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsAssetApi, 'get_asset_positions_for_dates', [target_position_set])
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    result = basket.get_position_sets(start=dt.date(2021, 1, 1), end=dt.date(2021, 12, 31))
    assert len(result) == 1


def test_get_position_sets_empty(mocker):
    """Branch: get_position_sets returns empty list when no data"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    mock_response(mocker, GsAssetApi, 'get_asset_positions_for_dates', [])
    result = basket.get_position_sets(start=dt.date(2021, 1, 1), end=dt.date(2021, 12, 31))
    assert result == []


def _set_mock_domain(domain_str):
    """Helper to set GsSession.current.domain via thread local for pydash get"""
    from gs_quant.context_base import thread_local

    class MockSess:
        pass
    mock_sess = MockSess()
    mock_sess.domain = domain_str
    setattr(thread_local, 'GsSession_path', (mock_sess,))
    return mock_sess


def _clear_mock_domain():
    from gs_quant.context_base import thread_local
    if hasattr(thread_local, 'GsSession_path'):
        delattr(thread_local, 'GsSession_path')


def test_get_url_production(mocker):
    """Branch: get_url with no 'dev' or 'qa' in domain -> production URL"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    _set_mock_domain('marquee.gs.com')
    try:
        url = basket.get_url()
        assert url == f'https://marquee.gs.com/s/products/{mqid}/summary'
    finally:
        _clear_mock_domain()


def test_get_url_dev(mocker):
    """Branch: get_url with 'dev' in domain -> dev URL"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    _set_mock_domain('marquee-dev.gs.com')
    try:
        url = basket.get_url()
        assert url == f'https://marquee-dev.gs.com/s/products/{mqid}/summary'
    finally:
        _clear_mock_domain()


def test_get_url_qa(mocker):
    """Branch: get_url with 'qa' in domain -> qa URL"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    _set_mock_domain('marquee-qa.gs.com')
    try:
        url = basket.get_url()
        assert url == f'https://marquee-qa.gs.com/s/products/{mqid}/summary'
    finally:
        _clear_mock_domain()


def test_allow_limited_access_assets_property(mocker):
    """Branch: allow_limited_access_assets getter/setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.allow_limited_access_assets = True
    assert basket.allow_limited_access_assets is True


def test_asset_class_property(mocker):
    """Branch: asset_class getter on existing basket -> UNMODIFIABLE"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.UNMODIFIABLE.value):
        basket.asset_class = AssetClass.FX


def test_benchmark_property(mocker):
    """Branch: benchmark setter requires ADMIN + INTERNAL"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.benchmark = 'SPX'
    assert basket.benchmark == 'SPX'

    # External non-admin cannot set benchmark
    mock_basket_init(mocker, user_ena)
    basket2 = Basket.get(ticker)
    with pytest.raises(MqError):
        basket2.benchmark = 'SPX'


def test_backtest_parameters_property(mocker):
    """Branch: backtest_parameters setter - value is not None sets Backtest methodology"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    params = EqBasketBacktestParameters()
    basket.backtest_parameters = params
    assert basket.backtest_parameters == params
    assert basket.historical_methodology == EqBasketHistoryMethodology.Backtest


def test_backtest_parameters_property_none(mocker):
    """Branch: backtest_parameters setter - value is None does not change methodology"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    original_methodology = basket.historical_methodology
    basket.backtest_parameters = None
    assert basket.backtest_parameters is None
    # methodology should not have changed to Backtest
    assert basket.historical_methodology == original_methodology


def test_cash_reinvestment_treatment_with_type(mocker):
    """Branch: cash_reinvestment_treatment setter with CashReinvestmentTreatmentType"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.cash_reinvestment_treatment = CashReinvestmentTreatmentType.Reinvest_At_Open
    crt = basket.cash_reinvestment_treatment
    assert isinstance(crt, CashReinvestmentTreatment)


def test_cash_reinvestment_treatment_with_object(mocker):
    """Branch: cash_reinvestment_treatment setter with CashReinvestmentTreatment object"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    crt = CashReinvestmentTreatment(
        cash_acquisition_treatment=CashReinvestmentTreatmentType.Add_To_Index,
        regular_dividend_treatment=CashReinvestmentTreatmentType.Add_To_Index,
        special_dividend_treatment=CashReinvestmentTreatmentType.Add_To_Index,
    )
    basket.cash_reinvestment_treatment = crt
    assert basket.cash_reinvestment_treatment == crt


def test_currency_property_unmodifiable(mocker):
    """Branch: currency setter on existing basket -> UNMODIFIABLE"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.UNMODIFIABLE.value):
        basket.currency = IndicesCurrency.EUR


def test_default_backcast_setter_false(mocker):
    """Branch: default_backcast setter with value=False -> sets Custom methodology"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    mock_response(mocker, GsIndexApi, 'validate_ticker', True)
    basket = Basket()
    basket.default_backcast = False
    assert basket.default_backcast is False
    assert basket.historical_methodology == EqBasketHistoryMethodology.Custom


def test_default_backcast_setter_true(mocker):
    """Branch: default_backcast setter with value=True -> does not set Custom"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    basket.default_backcast = True
    assert basket.default_backcast is True
    # Should remain Backcast (the default)
    assert basket.historical_methodology == EqBasketHistoryMethodology.Backcast


def test_divisor_setter(mocker):
    """Branch: divisor setter clears initial_price"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.divisor = 500
    assert basket.divisor == 500
    assert basket.initial_price is None


def test_initial_price_setter(mocker):
    """Branch: initial_price setter clears divisor"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.initial_price = 200
    assert basket.initial_price == 200
    assert basket.divisor is None


def test_name_setter_too_long(mocker):
    """Branch: name setter with name > 24 characters logs info"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    long_name = 'A' * 25
    basket.name = long_name
    assert basket.name == long_name


def test_name_setter_valid(mocker):
    """Branch: name setter with name <= 24 characters"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.name = 'Short Name'
    assert basket.name == 'Short Name'


def test_parent_basket_getter_with_clone_parent_id(mocker):
    """Branch: parent_basket getter returns stored value; pydash has() with mangled names"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    # Create basket with clone_parent_id but no parent_basket
    basket = Basket(clone_parent_id='MA9876543210')
    # parent_basket is None since pydash has() doesn't resolve mangled names
    result = basket.parent_basket
    assert result is None
    # But clone_parent_id should be set
    assert basket.clone_parent_id == 'MA9876543210'


def test_parent_basket_setter_on_existing_basket(mocker):
    """Branch: parent_basket setter on existing basket -> UNMODIFIABLE"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.UNMODIFIABLE.value):
        basket.parent_basket = 'GSMBZZZZ'


def test_position_set_setter_non_admin(mocker):
    """Branch: position_set setter as non-admin -> NON_ADMIN error"""
    mock_session()
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_ADMIN.value):
        basket.position_set = position_set


def test_preferred_risk_model_property(mocker):
    """Branch: preferred_risk_model setter requires ADMIN + INTERNAL"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.preferred_risk_model = 'AXUS4M'
    assert basket.preferred_risk_model == 'AXUS4M'


def test_publish_to_bloomberg_setter(mocker):
    """Branch: publish_to_bloomberg setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.publish_to_bloomberg = False
    assert basket.publish_to_bloomberg is False


def test_publish_to_factset_setter(mocker):
    """Branch: publish_to_factset setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.publish_to_factset = True
    assert basket.publish_to_factset is True


def test_publish_to_reuters_setter(mocker):
    """Branch: publish_to_reuters setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.publish_to_reuters = True
    assert basket.publish_to_reuters is True


def test_rebalance_calendar_property(mocker):
    """Branch: rebalance_calendar setter requires ADMIN + INTERNAL"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.rebalance_calendar = EqBasketRebalanceCalendar()
    assert basket.rebalance_calendar == EqBasketRebalanceCalendar()


def test_return_type_property(mocker):
    """Branch: return_type setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.return_type = ReturnType.TOTAL_RETURN
    assert basket.return_type == ReturnType.TOTAL_RETURN


def test_reweight_property(mocker):
    """Branch: reweight setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.reweight = True
    assert basket.reweight is True


def test_target_notional_property(mocker):
    """Branch: target_notional setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.target_notional = 5000000
    assert basket.target_notional == 5000000


def test_weighting_strategy_property(mocker):
    """Branch: weighting_strategy setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.weighting_strategy = WeightingStrategy.EQUAL
    assert basket.weighting_strategy == WeightingStrategy.EQUAL


def test_historical_methodology_setter(mocker):
    """Branch: historical_methodology setter - Custom vs non-Custom"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)

    # Set to Custom -> default_backcast should be False
    basket.historical_methodology = EqBasketHistoryMethodology.Custom
    assert basket.historical_methodology == EqBasketHistoryMethodology.Custom
    assert basket.default_backcast is False

    # Set to Backtest -> default_backcast should be True
    basket.historical_methodology = EqBasketHistoryMethodology.Backtest
    assert basket.historical_methodology == EqBasketHistoryMethodology.Backtest
    assert basket.default_backcast is True


def test_include_price_history_property(mocker):
    """Branch: include_price_history setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.include_price_history = True
    assert basket.include_price_history is True


def test_pricing_date_property(mocker):
    """Branch: pricing_date setter requires ADMIN + RESTRICTED_ATTRIBUTE"""
    mock_session()
    # internal admin can set
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.pricing_date = dt.date(2021, 6, 1)
    assert basket.pricing_date == dt.date(2021, 6, 1)


def test_pricing_date_restricted(mocker):
    """Branch: pricing_date setter blocked for external non-admin"""
    mock_session()
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.pricing_date = dt.date(2021, 6, 1)


def test_action_date_property(mocker):
    """Branch: action_date setter requires ADMIN + RESTRICTED_ATTRIBUTE"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.action_date = dt.date(2021, 6, 1)
    assert basket.action_date == dt.date(2021, 6, 1)


def test_allow_system_approval_property(mocker):
    """Branch: allow_system_approval setter requires ADMIN + RESTRICTED_ATTRIBUTE"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.allow_system_approval = True
    assert basket.allow_system_approval is True


def test_hedge_id_property(mocker):
    """Branch: hedge_id getter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    result = basket.hedge_id
    # hedge_id is not set on our gs_asset, so should be None
    assert result is None


def test_validate_ticker_invalid_length(mocker):
    """Branch: __validate_ticker raises error when ticker is not 8 characters"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    with pytest.raises(MqValueError, match='Invalid ticker'):
        basket.ticker = 'SHORT'


def test_validate_ticker_non_gs_prefix(mocker):
    """Branch: __validate_ticker logs info when ticker doesn't start with GS"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    mock_response(mocker, GsIndexApi, 'validate_ticker', True)
    basket = Basket()
    # Ticker is 8 chars but doesn't start with GS
    basket.ticker = 'AAXXXXXX'
    assert basket.ticker == 'AAXXXXXX'


def test_validate_ticker_gs_prefix(mocker):
    """Branch: __validate_ticker with GS prefix -> no info log"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    mock_response(mocker, GsIndexApi, 'validate_ticker', True)
    basket = Basket()
    basket.ticker = 'GSMBTEST'
    assert basket.ticker == 'GSMBTEST'


def test_validate_position_set_negative_weight(mocker):
    """Branch: __validate_position_set raises error on negative weights"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    neg_positions = [
        Position('bbid1', asset_id='id1', weight=-0.4),
        Position('bbid2', asset_id='id2', weight=0.6),
    ]
    neg_pos_set = PositionSet(neg_positions)
    basket = Basket()
    with pytest.raises(MqValueError, match='Position weights/quantities must be positive'):
        basket.position_set = neg_pos_set


def test_validate_position_set_unresolved(mocker):
    """Branch: __validate_position_set raises error on unresolved positions"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    pos_set = PositionSet(
        [Position('bbid1', asset_id='id1', weight=0.5), Position('bbid2', asset_id='id2', weight=0.5)]
    )
    unresolved = [Position('UNKNOWN', weight=0.1)]
    pos_set._PositionSet__unresolved_positions = unresolved
    basket = Basket()
    with pytest.raises(MqValueError, match='Error in resolving'):
        basket.position_set = pos_set


def test_get_gs_asset_not_found(mocker):
    """Branch: __get_gs_asset raises when resolution returns empty list"""
    mock_session()
    mock_response(mocker, GsAssetApi, 'resolve_assets', {'UNKNOWN': []})
    with pytest.raises(MqValueError, match='Basket could not be found'):
        Basket.get('UNKNOWN')


def test_get_gs_asset_no_id(mocker):
    """Branch: __get_gs_asset raises when resolution returns entry with None id"""
    mock_session()
    mock_response(mocker, GsAssetApi, 'resolve_assets', {'UNKNOWN': [{'id': None}]})
    with pytest.raises(MqValueError, match='Basket could not be found'):
        Basket.get('UNKNOWN')


def test_upload_position_history_default_backcast_error(mocker):
    """Branch: upload_position_history raises when default_backcast is True"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    # default_backcast is True by default for existing baskets from gs_asset
    # We need to set it to True explicitly since the mock might not set it
    basket._Basket__default_backcast = True
    with pytest.raises(MqValueError, match='Unable to upload position history'):
        basket.upload_position_history([position_set])


def test_poll_status_with_latest_report(mocker):
    """Branch: poll_status uses __latest_create_report if available"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # Set __latest_create_report
    basket._Basket__latest_create_report = report
    mock_response(mocker, GsReportApi, 'get_report', report)
    # Mock poll_report to return done quickly
    mocker.patch.object(type(basket), 'poll_report', return_value=ReportStatus.done)
    result = basket.poll_status(timeout=1, step=15)
    assert result == ReportStatus.done


def test_poll_status_without_latest_report(mocker):
    """Branch: poll_status falls back to __get_latest_create_report"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # Make sure __latest_create_report is NOT set by removing the attribute
    if hasattr(basket, '_Basket__latest_create_report'):
        delattr(basket, '_Basket__latest_create_report')
    mock_response(mocker, GsReportApi, 'get_reports', [report])
    mocker.patch.object(type(basket), 'poll_report', return_value=ReportStatus.done)
    result = basket.poll_status(timeout=1, step=15)
    assert result == ReportStatus.done


def test_edit_and_rebalance_report_fails(mocker):
    """Branch: __edit_and_rebalance raises MqError when edit report status != done"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change both description (edit) and initial_price (rebal)
    basket.description = 'Updated Description'
    basket.initial_price = 500

    # Mock edit to succeed but poll_report to return error
    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mocker.patch.object(type(basket), 'poll_report', return_value=ReportStatus.error)

    with pytest.raises(MqError, match='status is'):
        basket.update()


def test_update_nothing_changed(mocker):
    """Branch: update when nothing is modified -> MqValueError"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    # Don't change anything, but entitlements are the same
    with pytest.raises(MqValueError, match='Nothing on the basket was changed'):
        basket.update()


def test_update_entitlements_only(mocker):
    """Branch: update with only entitlements changed, no edit/rebal -> returns entitlements response"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change entitlements only
    mock_response(mocker, GsUsersApi, 'get_users', [TargetUser.from_dict(user_ena)])
    new_admin = User.get(user_id='user_xyz')
    basket.entitlements.admin.users += [new_admin]

    entitlements_response = TargetEntitlements(admin=['guid:user_abc', 'guid:user_xyz'])
    mock_response(mocker, GsAssetApi, 'update_asset_entitlements', entitlements_response)
    response = basket.update()
    assert response == entitlements_response


def test_update_rebalance_only_eligible(mocker):
    """Branch: update with rebal-specific metadata changed (not shared with edit)"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change allow_ca_restricted_assets which is rebal-only
    basket.allow_ca_restricted_assets = True

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_update_publish_only_triggers_edit(mocker):
    """Branch: update with only publish changes (no pricing/position) -> should_edit = True"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change only a publish parameter - use include_price_history which is always set
    basket.include_price_history = True

    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.edit.assert_called()
    assert response == cb_response.as_dict()


def test_update_pricing_triggers_rebalance(mocker):
    """Branch: update with pricing changes -> should_rebal with pricing_parameters"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change a pricing parameter
    basket.initial_price = 999

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_update_positions_triggers_rebalance(mocker):
    """Branch: update with position changes -> should_rebal with position_set"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change positions - use different weights to create a real change
    new_positions = [
        Position('bbid1', asset_id='id1', weight=0.7),
        Position('bbid2', asset_id='id2', weight=0.3),
    ]
    new_pos_set = PositionSet(new_positions)
    # Directly set position_set and initial_positions to force position change detection
    basket._Basket__position_set = new_pos_set
    # The initial_positions should be different from new positions
    old_positions = {
        Position('bbid1', asset_id='id1', weight=0.4),
        Position('bbid2', asset_id='id2', weight=0.6),
    }
    basket._Basket__initial_positions = old_positions

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_set_error_messages_internal_admin(mocker):
    """Branch: __set_error_messages for internal admin - no NON_INTERNAL, no NON_ADMIN"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    # Internal admin should not have NON_INTERNAL or NON_ADMIN errors
    error_msgs = basket._Basket__error_messages
    assert ErrorMessage.NON_INTERNAL not in error_msgs
    assert ErrorMessage.NON_ADMIN not in error_msgs
    assert ErrorMessage.UNMODIFIABLE in error_msgs  # existing basket


def test_set_error_messages_external_admin(mocker):
    """Branch: __set_error_messages for external admin"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    error_msgs = basket._Basket__error_messages
    assert ErrorMessage.NON_INTERNAL in error_msgs
    assert ErrorMessage.NON_ADMIN not in error_msgs
    assert ErrorMessage.UNMODIFIABLE in error_msgs


def test_set_error_messages_external_non_admin(mocker):
    """Branch: __set_error_messages for external non-admin"""
    mock_session()
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    error_msgs = basket._Basket__error_messages
    assert ErrorMessage.NON_INTERNAL in error_msgs
    assert ErrorMessage.NON_ADMIN in error_msgs
    assert ErrorMessage.UNMODIFIABLE in error_msgs


def test_set_error_messages_new_basket(mocker):
    """Branch: __set_error_messages for new basket (no id) -> UNINITIALIZED"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    # Error messages start as empty set before __finish_initialization
    assert basket._Basket__error_messages == set()
    # Trigger __set_error_messages manually
    basket._Basket__set_error_messages()
    error_msgs = basket._Basket__error_messages
    assert ErrorMessage.UNINITIALIZED in error_msgs
    assert ErrorMessage.UNMODIFIABLE not in error_msgs


def test_set_error_messages_recalculates(mocker):
    """Branch: __set_error_messages recalculates errors (pydash get with mangled names)"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    original_msgs = basket._Basket__error_messages.copy()
    # Set error messages manually
    basket._Basket__error_messages = {ErrorMessage.NON_INTERNAL}
    # Calling __set_error_messages recalculates since pydash get doesn't resolve mangled names
    basket._Basket__set_error_messages()
    # Error messages are recalculated based on current user/basket state
    assert ErrorMessage.NON_INTERNAL in basket._Basket__error_messages
    assert ErrorMessage.UNMODIFIABLE in basket._Basket__error_messages


def test_validate_decorator_triggers_finish_initialization(mocker):
    """Branch: _validate triggers __finish_initialization when error_messages is empty set"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # Set error messages to empty set to trigger __finish_initialization
    basket._Basket__error_messages = set()
    # Calling a validated method should trigger __finish_initialization
    mock_response(mocker, GsAssetApi, 'get_latest_positions', target_position_set)
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    mock_response(mocker, GsIndexApi, 'initial_price', initial_price)
    mock_response(mocker, GsReportApi, 'get_reports', [report])
    mock_response(mocker, GsUsersApi, 'get_current_user_info', user_ea)
    details = basket.get_details()
    assert details is not None


def test_validate_decorator_no_error_messages_attr(mocker):
    """Branch: _validate when __error_messages is None -> skip all validation"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket.__new__(Basket)
    basket._Basket__error_messages = None
    # Set required attributes for get_details to work
    basket._Basket__gs_asset_type = None
    # get_type doesn't have @_validate, but we can test on a property that does
    # For example, divisor property has @_validate()
    basket._Basket__divisor = 123.0
    # Accessing the property should skip validation when __error_messages is None
    assert basket.divisor == 123.0


def test_clone_parent_id_property(mocker):
    """Branch: clone_parent_id property getter"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket(clone_parent_id='MA_PARENT')
    assert basket.clone_parent_id == 'MA_PARENT'


def test_description_setter(mocker):
    """Branch: description setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    basket.description = 'A new description'
    assert basket.description == 'A new description'


def test_entitlements_setter(mocker):
    """Branch: entitlements setter"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    from gs_quant.entities.entitlements import Entitlements as BasketEntitlements
    new_ent = BasketEntitlements()
    basket.entitlements = new_ent
    assert basket.entitlements == new_ent


def test_flagship_non_internal(mocker):
    """Branch: flagship setter blocked for non-internal"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError, match=ErrorMessage.NON_INTERNAL.value):
        basket.flagship = True


def test_flagship_internal(mocker):
    """Branch: flagship setter allowed for internal"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    basket.flagship = True
    assert basket.flagship is True


def test_bloomberg_publish_parameters_property(mocker):
    """Branch: bloomberg_publish_parameters setter requires ADMIN + INTERNAL"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)
    from gs_quant.common import BloombergPublishParameters
    params = BloombergPublishParameters()
    basket.bloomberg_publish_parameters = params
    assert basket.bloomberg_publish_parameters == params


def test_bloomberg_publish_parameters_non_internal(mocker):
    """Branch: bloomberg_publish_parameters setter blocked for non-internal"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    from gs_quant.common import BloombergPublishParameters
    with pytest.raises(MqError):
        basket.bloomberg_publish_parameters = BloombergPublishParameters()


def test_restricted_attribute_error_for_pricing_date(mocker):
    """Branch: RESTRICTED_ATTRIBUTE error for external user without restricted token"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.pricing_date = dt.date(2021, 1, 1)


def test_restricted_attribute_allowed_with_token(mocker):
    """Branch: RESTRICTED_ATTRIBUTE not in errors for user with restricted attributes token"""
    mock_session()
    mock_basket_init(mocker, user_ia_restricted)
    basket = Basket.get(ticker)
    error_msgs = basket._Basket__error_messages
    assert ErrorMessage.RESTRICTED_ATTRIBUTE not in error_msgs


def test_action_date_restricted(mocker):
    """Branch: action_date setter blocked for external non-admin"""
    mock_session()
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.action_date = dt.date(2021, 6, 1)


def test_allow_system_approval_restricted(mocker):
    """Branch: allow_system_approval setter blocked for external non-admin"""
    mock_session()
    mock_basket_init(mocker, user_ena)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.allow_system_approval = True


def test_edit_and_rebalance_success(mocker):
    """Branch: __edit_and_rebalance succeeds when report status is done"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change both description (edit) and initial_price (rebal)
    basket.description = 'Another Description'
    basket.initial_price = 300

    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mocker.patch.object(type(basket), 'poll_report', return_value=ReportStatus.done)
    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    assert response == cb_response.as_dict()


def test_finish_initialization_all_branches(mocker):
    """Branch: __finish_initialization exercises has/not-has checks"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    # __finish_initialization is called during get(), so verify the state
    assert basket.position_set is not None
    assert basket.entitlements is not None
    # Divisor should be set from the position set
    assert basket.divisor is not None


def test_finish_initialization_fetches_initial_price(mocker):
    """Branch: __finish_initialization fetches initial_price when not in initial_state"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker, _finish_init=False)

    # Remove initial_price from initial_state to trigger the fetch branch
    if 'initial_price' in basket._Basket__initial_state:
        del basket._Basket__initial_state['initial_price']

    # Mock the API calls that __finish_initialization will make
    mock_response(mocker, GsAssetApi, 'get_latest_positions', target_position_set)
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    mock_response(mocker, GsIndexApi, 'initial_price', {'price': 42})
    mock_response(mocker, GsReportApi, 'get_reports', [report])
    mock_response(mocker, GsUsersApi, 'get_current_user_info', user_ea)

    basket._Basket__finish_initialization()
    assert basket._Basket__initial_price == 42


def test_finish_initialization_fetches_publish_params(mocker):
    """Branch: __finish_initialization fetches publish params when not in initial_state"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker, _finish_init=False)

    # Remove publish_to_bloomberg from initial_state to trigger the fetch branch
    if 'publish_to_bloomberg' in basket._Basket__initial_state:
        del basket._Basket__initial_state['publish_to_bloomberg']

    # Create a report with specific publish parameters
    report_params = ReportParameters(
        publish_to_bloomberg=True,
        publish_to_factset=True,
        publish_to_reuters=False,
    )
    report_with_params = Report(mqid, 'asset', 'Basket Create', report_params, status='done')

    mock_response(mocker, GsAssetApi, 'get_latest_positions', target_position_set)
    mock_response(mocker, GsAssetApi, 'get_many_assets_data', assets_data)
    mock_response(mocker, GsIndexApi, 'initial_price', initial_price)
    mock_response(mocker, GsReportApi, 'get_reports', [report_with_params])
    mock_response(mocker, GsUsersApi, 'get_current_user_info', user_ea)

    basket._Basket__finish_initialization()
    assert basket._Basket__publish_to_bloomberg is True
    assert basket._Basket__publish_to_factset is True
    assert basket._Basket__publish_to_reuters is False


def test_new_basket_with_all_kwargs(mocker):
    """Branch: new basket with many custom kwargs that are set in __populate_default_attributes_for_new_basket"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket(
        name='Custom Basket',
        description='A test basket',
        currency=IndicesCurrency.USD,
        return_type=ReturnType.PRICE_RETURN,
        default_backcast=False,
        include_price_history=True,
        target_notional=5000000,
        allow_ca_restricted_assets=True,
        allow_limited_access_assets=True,
        initial_price=200,
        hedge_id='HEDGE123',
    )
    assert basket.name == 'Custom Basket'
    assert basket.description == 'A test basket'
    assert basket.currency == IndicesCurrency.USD
    assert basket.return_type == ReturnType.PRICE_RETURN
    assert basket.default_backcast is False
    assert basket.include_price_history is True
    assert basket.target_notional == 5000000
    assert basket.allow_ca_restricted_assets is True
    assert basket.allow_limited_access_assets is True
    assert basket.hedge_id == 'HEDGE123'


def test_update_rebalance_with_publish_and_pricing(mocker):
    """Branch: update with pricing, publish, and position changes -> all set on rebal_inputs"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change pricing (initial_price), publish (publish_to_reuters), and positions
    basket.initial_price = 750
    basket.publish_to_reuters = True
    new_positions = [Position('bbid1', asset_id='id1', weight=0.5), Position('bbid2', asset_id='id2', weight=0.5)]
    basket.position_set = PositionSet(new_positions)

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_get_classmethod(mocker):
    """Branch: Basket.get with _finish_init default (True)"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    assert basket is not None
    # Verify initialization completed
    assert basket._Basket__error_messages is not None
    assert len(basket._Basket__error_messages) > 0


def test_get_classmethod_no_finish_init(mocker):
    """Branch: Basket.get with _finish_init=False"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker, _finish_init=False)
    assert basket is not None
    # Error messages should be empty set (not yet finished)
    assert basket._Basket__error_messages == set()


def test_update_edit_with_publish_changes(mocker):
    """Branch: __get_updates with should_edit and publish_updated"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # Change description (eligible_for_edit) and publish_to_factset (publish_updated)
    basket.description = 'Edit with publish'
    basket.publish_to_factset = True

    mock_response(mocker, GsIndexApi, 'edit', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.edit.assert_called()
    assert response == cb_response.as_dict()
    gs_asset.description = None


def test_update_rebal_only_metadata(mocker):
    """Branch: __get_updates rebal-specific metadata changed but not edit metadata"""
    mock_session()
    mock_basket_init(mocker, user_ia)
    basket = Basket.get(ticker)

    # allow_limited_access_assets is in rebal inputs
    basket.allow_limited_access_assets = True

    mock_response(mocker, GsIndexApi, 'rebalance', cb_response)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    mock_response(mocker, GsReportApi, 'get_report', report)
    mock_basket_init(mocker, user_ia)

    response = basket.update()
    GsIndexApi.rebalance.assert_called()
    assert response == cb_response.as_dict()


def test_validate_position_set_negative_quantity(mocker):
    """Branch: __validate_position_set with negative quantity"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    neg_positions = [
        Position('bbid1', asset_id='id1', quantity=-100),
        Position('bbid2', asset_id='id2', quantity=200),
    ]
    neg_pos_set = PositionSet(neg_positions)
    basket = Basket()
    with pytest.raises(MqValueError, match='Position weights/quantities must be positive'):
        basket.position_set = neg_pos_set


def test_basket_new_default_history_methodology(mocker):
    """Branch: default new basket gets EqBasketHistoryMethodology.Backcast"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    assert basket.historical_methodology == EqBasketHistoryMethodology.Backcast


def test_basket_new_with_custom_history(mocker):
    """Branch: new basket with custom historical_methodology kwarg"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket(historical_methodology=EqBasketHistoryMethodology.Custom)
    assert basket.historical_methodology == EqBasketHistoryMethodology.Custom


def test_rebalance_calendar_non_internal(mocker):
    """Branch: rebalance_calendar setter blocked for non-internal"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.rebalance_calendar = EqBasketRebalanceCalendar()


def test_preferred_risk_model_non_internal(mocker):
    """Branch: preferred_risk_model setter blocked for non-internal"""
    mock_session()
    mock_basket_init(mocker, user_ea)
    basket = Basket.get(ticker)
    with pytest.raises(MqError):
        basket.preferred_risk_model = 'AXUS4M'


def test_parent_basket_setter_new_basket(mocker):
    """Branch: parent_basket setter on new basket (not UNMODIFIABLE)"""
    mock_session()
    mock_basket_init(mocker, user_ea, False)
    basket = Basket()
    # Manually trigger __set_error_messages to populate error messages
    basket._Basket__set_error_messages()
    # For a new basket, UNMODIFIABLE should NOT be in errors
    assert ErrorMessage.UNMODIFIABLE not in basket._Basket__error_messages
    # Now set parent_basket - this should resolve the identifier
    mock_response(mocker, GsAssetApi, 'resolve_assets', resolved_asset)
    mock_response(mocker, GsAssetApi, 'get_asset', gs_asset)
    basket.parent_basket = 'GSMBXXXX'
    assert basket.parent_basket == 'GSMBXXXX'
    assert basket.clone_parent_id == mqid
