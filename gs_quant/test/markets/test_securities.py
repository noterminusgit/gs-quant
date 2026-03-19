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

import copy
import datetime as dt
import json
from enum import Enum

import pytest

from gs_quant.base import EnumBase
from gs_quant.api.gs.assets import GsAsset, GsAssetApi
from gs_quant.errors import MqTypeError, MqValueError, MqRequestError
from gs_quant.markets import PricingContext
from gs_quant.markets.securities import (
    SecurityMaster,
    AssetIdentifier,
    AssetType,
    ExchangeCode,
    SecurityMasterSource,
    SecurityIdentifier,
    Asset,
    SecMasterAsset,
)
from gs_quant.common import AssetClass, AssetType as GsAssetType
from gs_quant.session import GsSession, Environment


def test_get_asset(mocker):
    marquee_id = 'MA1234567890'
    mock_response = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='Test Asset')

    # mock GsSession
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.STOCK

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID, as_of=dt.date.today())

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.STOCK

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID, as_of=dt.datetime.utcnow())

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.STOCK

    mock_response = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Index, name='Test Asset')
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.INDEX

    mock_response = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Future, name='Test Asset')
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.FUTURE

    mock_response = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.ETF, name='Test Asset')
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.ETF

    mock_response = GsAsset(
        asset_class=AssetClass.Equity, type_=GsAssetType.Custom_Basket, name='Test Asset', id_=marquee_id
    )
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    assert asset.name == "Test Asset"
    assert asset.get_type() == AssetType.CUSTOM_BASKET

    mock_response = {
        'results': (GsAsset(id=marquee_id, assetClass='Equity', type='Single Stock', name='Test 1'),),
    }

    mocker.patch.object(GsSession.current.sync, 'post', return_value=mock_response)
    asset = SecurityMaster.get_asset('GS.N', AssetIdentifier.REUTERS_ID)
    assert asset.name == "Test 1"
    assert asset.get_type() == AssetType.STOCK

    asset = SecurityMaster.get_asset('GS', AssetIdentifier.TICKER, exchange_code=ExchangeCode.NYSE)
    assert asset.name == "Test 1"
    assert asset.get_type() == AssetType.STOCK

    asset = SecurityMaster.get_asset('GS', AssetIdentifier.TICKER, asset_type=AssetType.STOCK)
    assert asset.name == "Test 1"
    assert asset.get_type() == AssetType.STOCK

    mocker.patch.object(GsSession.current.sync, 'post', return_value={'results': ()})
    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.REUTERS_ID)
    assert asset is None


def test_asset_identifiers(mocker):
    marquee_id = 'MA1234567890'

    mocker.patch.object(GsSession, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
    mock_response = GsAsset(
        asset_class=AssetClass.Equity, type_=GsAssetType.Custom_Basket, name='Test Asset', id_=marquee_id
    )
    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    asset = SecurityMaster.get_asset(marquee_id, AssetIdentifier.MARQUEE_ID)

    mock_response = {
        'xrefs': (
            {
                'startDate': '1952-01-01',
                'endDate': '2018-12-31',
                'identifiers': {'ric': '.GSTHHOLD', 'bbid': 'GSTHHOLD', 'cusip': '9EQ24FOLD', 'ticker': 'GSTHHOLD'},
            },
            {
                'startDate': '2019-01-01',
                'endDate': '2952-12-31',
                'identifiers': {
                    'ric': '.GSTHHVIP',
                    'bbid': 'GSTHHVIP',
                    'cusip': '9EQ24FPE5',
                    'ticker': 'GSTHHVIP',
                },
            },
        )
    }

    mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

    identifiers = asset.get_identifiers(dt.date.today())

    assert identifiers[AssetIdentifier.REUTERS_ID.value] == '.GSTHHVIP'
    assert identifiers[AssetIdentifier.BLOOMBERG_ID.value] == 'GSTHHVIP'
    assert identifiers[AssetIdentifier.CUSIP.value] == '9EQ24FPE5'
    assert identifiers[AssetIdentifier.TICKER.value] == 'GSTHHVIP'

    assert asset.get_identifier(AssetIdentifier.REUTERS_ID, as_of=dt.date.today()) == '.GSTHHVIP'
    assert asset.get_identifier(AssetIdentifier.BLOOMBERG_ID, as_of=dt.date.today()) == 'GSTHHVIP'
    assert asset.get_identifier(AssetIdentifier.CUSIP, as_of=dt.date.today()) == '9EQ24FPE5'
    assert asset.get_identifier(AssetIdentifier.TICKER, as_of=dt.date.today()) == 'GSTHHVIP'

    market = PricingContext(dt.date(2018, 3, 1))

    with market:
        identifiers = asset.get_identifiers()

    assert identifiers[AssetIdentifier.REUTERS_ID.value] == '.GSTHHOLD'
    assert identifiers[AssetIdentifier.BLOOMBERG_ID.value] == 'GSTHHOLD'
    assert identifiers[AssetIdentifier.CUSIP.value] == '9EQ24FOLD'
    assert identifiers[AssetIdentifier.TICKER.value] == 'GSTHHOLD'

    market = PricingContext(dt.date(2018, 3, 1))

    with market:
        identifiers = asset.get_identifiers()

    assert identifiers[AssetIdentifier.REUTERS_ID.value] == '.GSTHHOLD'
    assert identifiers[AssetIdentifier.BLOOMBERG_ID.value] == 'GSTHHOLD'
    assert identifiers[AssetIdentifier.CUSIP.value] == '9EQ24FOLD'
    assert identifiers[AssetIdentifier.TICKER.value] == 'GSTHHOLD'


def test_asset_types(mocker):
    class MockType(EnumBase, Enum):
        Foo = "Bar"

    ata = getattr(SecurityMaster, '_SecurityMaster__gs_asset_to_asset')
    assert ata is not None
    asset = GsAsset(AssetClass.Equity, None, 'Test Asset')

    mocker.patch.object(json, 'dumps', return_value='{}')
    # with pytest.raises(ValueError) as exc_info:
    #     setattr(asset, 'type', MockType.Foo)
    # assert 'is not a valid AssetType' in str(exc_info.value)  # reached exception at end of function

    with pytest.raises(AttributeError) as exc_info:
        ata(asset)
    assert "has no attribute 'value'" in str(exc_info.value)  # reached exception at end of function


class SecMasterContext:
    def __enter__(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)

    def __exit__(self, exc_type, exc_value, traceback):
        SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class AssetContext:
    def __enter__(self):
        self.previous = SecurityMaster._source
        SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def __exit__(self, exc_type, exc_value, traceback):
        SecurityMaster.set_source(self.previous)


def test_get_security(mocker):
    mocker.patch.object(GsSession, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

    mock_response = {
        "results": [
            {
                "name": "GOLDMAN SACHS GROUP INC (New York Stock)",
                "type": "Common Stock",
                "currency": "USD",
                "tags": [],
                "id": "GSPD901026E154",
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 901026,
                    "ric": "GS.N",
                    "id": "GSPD901026E154",
                    "cusip": "38141G10",
                    "sedol": "2407966",
                    "isin": "US38141G1040",
                    "ticker": "GS",
                    "bbid": "GS UN",
                    "bcid": "GS US",
                    "gss": "GS",
                    "primeId": "1003232152",
                    "assetId": "MA4B66MW5E27UAHKG34",
                },
                "company": {"name": "GOLDMAN SACHS GROUP INC", "identifiers": {"gsCompanyId": 25998}},
                "product": {"name": "GOLDMAN SACHS GROUP INC", "identifiers": {"gsid": 901026}},
                "exchange": {"name": "New York Stock", "identifiers": {"gsExchangeId": 154}},
            }
        ],
        "totalResults": 1,
    }

    mock_identifier_history_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "GS",
                "updateTime": "2002-02-09T17:58:27.58Z",
                "type": "bbg",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "GS",
                "updateTime": "2002-02-09T17:57:14.546Z",
                "type": "ticker",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "assetId",
                "value": "MA4B66MW5E27UAHKG34",
                "updateTime": "2002-10-30T21:30:29.993Z",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "ric",
                "value": "GS.N",
                "updateTime": "2002-10-30T21:30:29.993Z",
                "gsExchangeId": 154,
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_response, mock_identifier_history_response])

    with SecMasterContext():
        asset = SecurityMaster.get_asset('GS UN', SecurityIdentifier.BBID)
    assert isinstance(asset, SecMasterAsset)
    assert asset.get_marquee_id() == 'MA4B66MW5E27UAHKG34'
    ids = asset.get_identifiers()
    assert ids[SecurityIdentifier.BBG.value] == 'GS'
    assert ids[SecurityIdentifier.RIC.value] == 'GS.N'
    assert ids[SecurityIdentifier.GSID.value] == 901026


def test_get_security_fields(mocker):
    mock_response = {
        "results": [
            {
                "name": "GOLDMAN SACHS GROUP INC (New York Stock)",
                "id": "GSPD901026E154",
                "type": "Common Stock",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 901026,
                    "ric": "GS.N",
                    "id": "GSPD901026E154",
                    "cusip": "38141G10",
                    "sedol": "2407966",
                    "isin": "US38141G1040",
                    "ticker": "GS",
                    "bbid": "GS UN",
                    "bcid": "GS US",
                    "gss": "GS",
                    "primeId": "1003232152",
                    "assetId": "MA4B66MW5E27UAHKG34",
                },
                "exchange": {"name": "New York Stock", "identifiers": {"gsExchangeId": 154}},
            }
        ],
        "totalResults": 1,
    }

    mock_identifiers_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "GS",
                "updateTime": "2002-02-09T17:58:27.58Z",
                "type": "bbg",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "1003232152",
                "updateTime": "2003-01-16T15:22:54.1Z",
                "type": "primeId",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "assetId",
                "value": "MA4B66MW5E27UAHKG34",
                "updateTime": "2002-10-30T21:30:29.993Z",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "ric",
                "value": "GS.N",
                "updateTime": "2002-10-30T21:30:29.993Z",
                "gsExchangeId": 154,
            },
        ],
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_response, mock_identifiers_response])

    with SecMasterContext():
        asset = SecurityMaster.get_asset('GS UN', SecurityIdentifier.BBID, fields=['name', 'id'])
    assert isinstance(asset, SecMasterAsset)
    assert asset.get_marquee_id() == 'MA4B66MW5E27UAHKG34'
    assert asset.name == 'GOLDMAN SACHS GROUP INC (New York Stock)'
    ids = asset.get_identifiers()
    assert ids[SecurityIdentifier.BBG.value] == 'GS'
    assert ids[SecurityIdentifier.RIC.value] == 'GS.N'
    assert ids[SecurityIdentifier.PRIMEID.value] == '1003232152'
    assert ids[SecurityIdentifier.ID.value] == 'GSPD901026E154'


def test_get_identifiers(mocker):
    assets = {
        "results": [
            {"id": "GSPD901026E154", "identifiers": {"bbid": "GS UN"}},
            {"id": "GSPD14593E459", "identifiers": {"bbid": "AAPL UW"}},
        ],
        "totalResults": 2,
    }
    ids_gs = {
        "results": [
            {
                "startDate": "2021-01-01",
                "endDate": "9999-99-99",
                "value": "38141G10",
                "updateTime": "2002-02-09T17:54:27.99Z",
                "type": "cusip",
            },
            {
                "startDate": "2021-01-01",
                "endDate": "9999-99-99",
                "value": "2407966",
                "updateTime": "2002-02-09T17:54:47.77Z",
                "type": "sedol",
            },
        ]
    }
    ids_ap = {
        "results": [
            {
                "startDate": "2021-01-01",
                "endDate": "9999-99-99",
                "value": "03783310",
                "updateTime": "2003-04-15T22:36:17.593Z",
                "type": "cusip",
            },
            {
                "startDate": "2021-01-01",
                "endDate": "9999-99-99",
                "value": "2046251",
                "updateTime": "2003-04-15T22:36:17.6Z",
                "type": "sedol",
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[assets, ids_gs, ids_ap])
    with SecMasterContext():
        identifiers = SecurityMaster.get_identifiers(['GS UN', 'AAPL UW'], SecurityIdentifier.BBID)
    assert 'GS UN' in identifiers
    assert 'AAPL UW' in identifiers
    assert identifiers['GS UN'] == ids_gs['results']
    assert identifiers['AAPL UW'] == ids_ap['results']


def test_get_all_identifiers(mocker):
    p1 = {
        "results": [
            {
                "type": "Common Stock",
                "id": "GSPD901026E154",
                "assetClass": "Equity",
                "identifiers": {"gsid": 901026, "ric": "GS.N", "id": "GSPD901026E154", "bbid": "GS UN"},
            }
        ],
        "totalResults": 1,
    }
    p2 = {
        "results": [
            {
                "type": "Common Stock",
                "id": "GSPD14593E459",
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 14593,
                    "ric": "AAPL.OQ",
                    "id": "GSPD14593E459",
                    "bbid": "AAPL UW",
                },
            }
        ],
        "totalResults": 1,
    }
    p3 = {"results": [], "totalResults": 0}

    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[p1, p2, p3])
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(use_offset_key=False)
    assert len(output) == 2
    assert output['GSPD901026E154'] == p1['results'][0]['identifiers']
    assert output['GSPD14593E459'] == p2['results'][0]['identifiers']

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[p1, p2, p3])
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(id_type=SecurityIdentifier.BBID, use_offset_key=False)
    assert len(output) == 2
    assert output['GS UN'] == p1['results'][0]['identifiers']
    assert output['AAPL UW'] == p2['results'][0]['identifiers']


def test_get_all_identifiers_with_assetTypes_not_none(mocker):
    mock_etf = {
        "results": [
            {
                "type": "ETF",
                "id": "mock_ETF_id",
                "assetClass": "Equity",
                "identifiers": {"gsid": 1111111, "ric": "mock_ETF_ric", "id": "mock_ETF_id", "bbid": "mock_ETF_bbid"},
            }
        ],
        "totalResults": 1,
    }
    mock_stock = {
        "results": [
            {
                "type": "Common Stock",
                "id": "mock_stock_id",
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 222222,
                    "ric": "mock_stock_ric",
                    "bbid": "mock_stock_bbid",
                    # id omitted from nested dict for testing
                },
            }
        ],
        "totalResults": 1,
    }
    mock_etf_and_stock = {"results": mock_stock['results'] + mock_etf['results'], "totalResults": 2}

    def get_identifiers_byte(*args, **kwargs):
        types = kwargs['payload']['type']
        stock_str = SecurityMaster.asset_type_to_str(asset_class=AssetClass.Equity, asset_type=AssetType.STOCK)
        if len(types) == 1 and AssetType.ETF.value in types:
            return mock_etf
        elif len(types) == 1 and stock_str in types:
            return mock_stock
        elif len(types) == 2 and stock_str in types and AssetType.ETF.value in types:
            return mock_etf_and_stock

    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=get_identifiers_byte)
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(AssetClass.Equity, types=[AssetType.ETF])
    assert len(output) == 1
    assert output['mock_ETF_id'] == mock_etf['results'][0]['identifiers']

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=get_identifiers_byte)
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(AssetClass.Equity, types=[AssetType.STOCK])
    assert len(output) == 1
    assert output['mock_stock_id'] == mock_stock['results'][0]['identifiers']

    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(AssetClass.Equity, types=[AssetType.STOCK, AssetType.ETF])
    assert len(output) == 2
    assert output['mock_ETF_id'] == mock_etf['results'][0]['identifiers']
    assert output['mock_stock_id'] == mock_stock['results'][0]['identifiers']


def test_offset_key(mocker):
    p1 = {
        "results": [
            {
                "type": "Common Stock",
                "id": "GSPD901026E154",
                "assetClass": "Equity",
                "identifiers": {"gsid": 901026, "ric": "GS.N", "id": "GSPD901026E154", "bbid": "GS UN"},
            }
        ],
        "offsetKey": "qwerty",
        "totalResults": 1,
    }
    p2 = {
        "results": [
            {
                "type": "Common Stock",
                "id": "GSPD14593E459",
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 14593,
                    "ric": "AAPL.OQ",
                    "id": "GSPD14593E459",
                    "bbid": "AAPL UW",
                },
            }
        ],
        "offsetKey": "azerty",
        "totalResults": 1,
    }
    p3 = {"results": [], "totalResults": 0}

    limited = False
    hits = [0] * 3

    def fetch(*args, **kwargs):
        nonlocal limited
        if not limited:
            limited = True
            raise MqRequestError(429, 'too many requests')
        offset_key = kwargs['payload'].get('offsetKey')
        if offset_key is None:
            hits[0] += 1
            return p1
        if offset_key == "qwerty":
            hits[1] += 1
            return p2
        if offset_key == "azerty":
            hits[2] += 1
            return p3

    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=fetch)
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(sleep=0)
    assert len(output) == 2
    assert output['GSPD901026E154'] == p1['results'][0]['identifiers']
    assert output['GSPD14593E459'] == p2['results'][0]['identifiers']
    assert all(map(lambda x: x == 1, hits))

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=fetch)
    with SecMasterContext():
        output = SecurityMaster.get_all_identifiers(id_type=SecurityIdentifier.BBID, sleep=0)
    assert len(output) == 2
    assert output['GS UN'] == p1['results'][0]['identifiers']
    assert output['AAPL UW'] == p2['results'][0]['identifiers']
    assert all(map(lambda x: x == 2, hits))

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=fetch)
    with SecMasterContext():
        gen = SecurityMaster.get_all_identifiers_gen(id_type=SecurityIdentifier.BBID, sleep=0)
        page = next(gen)
        assert len(page) == 1
        assert page['GS UN'] == p1['results'][0]['identifiers']
        page = next(gen)
        assert len(page) == 1
        assert page['AAPL UW'] == p2['results'][0]['identifiers']

        with pytest.raises(StopIteration):
            next(gen)

    assert all(map(lambda x: x == 3, hits))


def test_map_identifiers(mocker):
    mock1 = {
        "results": [
            {
                "assetId": "MA4B66MW5E27U9VBB93",
                "outputType": "rcic",
                "outputValue": "AAPL.O",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "AAPL UN",
            },
            {
                "assetId": "MARCRZHY163GQ4H3",
                "outputType": "ric",
                "outputValue": "AAPL.N",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "AAPL UN",
            },
            {
                "assetId": "MA4B66MW5E27UAHKG34",
                "outputType": "ric",
                "outputValue": "GS.N",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "GS UN",
            },
            {
                "outputType": "rcic",
                "outputValue": "GS",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "GS UN",
            },
            {
                "outputType": "gsid",
                "outputValue": 14593,
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "AAPL UN",
            },
            {
                "outputType": "gsid",
                "outputValue": 901026,
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "GS UN",
            },
        ]
    }
    mock2 = copy.deepcopy(mock1)
    mock2["results"].extend(
        [
            {
                "outputType": "bbg",
                "outputValue": "AAPL",
                "exchange": "UN",
                "compositeExchange": "US",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "AAPL UN",
            },
            {
                "outputType": "bbg",
                "outputValue": "GS",
                "exchange": "UN",
                "compositeExchange": "US",
                "startDate": "2021-10-11",
                "endDate": "2021-10-12",
                "input": "GS UN",
            },
        ]
    )

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock2, mock2])
    start = dt.date(2021, 10, 11)
    end = dt.date(2021, 10, 12)

    expected = {
        "2021-10-11": {"AAPL UN": {"ric": ["AAPL.N"], "gsid": [14593]}, "GS UN": {"ric": ["GS.N"], "gsid": [901026]}},
        "2021-10-12": {"AAPL UN": {"ric": ["AAPL.N"], "gsid": [14593]}, "GS UN": {"ric": ["GS.N"], "gsid": [901026]}},
    }
    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(
            SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], [SecurityIdentifier.RIC, SecurityIdentifier.GSID], start, end
        )
    assert actual == expected

    expected = {
        "2021-10-11": {
            "AAPL UN": {"assetId": ["MARCRZHY163GQ4H3"], "gsid": [14593], "bbid": ["AAPL UN"]},
            "GS UN": {"assetId": ["MA4B66MW5E27UAHKG34"], "gsid": [901026], "bbid": ["GS UN"]},
        },
        "2021-10-12": {
            "AAPL UN": {"assetId": ["MARCRZHY163GQ4H3"], "gsid": [14593], "bbid": ["AAPL UN"]},
            "GS UN": {"assetId": ["MA4B66MW5E27UAHKG34"], "gsid": [901026], "bbid": ["GS UN"]},
        },
    }
    targets = [SecurityIdentifier.ASSET_ID, SecurityIdentifier.GSID, SecurityIdentifier.BBID]
    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], targets, start, end)
    assert actual == expected


def test_map_identifiers_change(mocker):
    mock = {
        "results": [
            {
                "outputType": "bbg",
                "outputValue": "USAT",
                "exchange": "UW",
                "compositeExchange": "US",
                "startDate": "2021-01-01",
                "endDate": "2021-04-18",
                "input": "104563",
            },
            {
                "outputType": "bbg",
                "outputValue": "CTLP",
                "exchange": "UW",
                "compositeExchange": "US",
                "startDate": "2021-04-19",
                "endDate": "2021-11-01",
                "input": "104563",
            },
            {
                "assetId": "MAY8Z19T2WE6RVHG",
                "outputType": "rcic",
                "outputValue": "USAT.O",
                "startDate": "2021-01-01",
                "endDate": "2021-04-17",
                "input": "104563",
            },
            {
                "assetId": "MA4B66MW5E27UANLYDS",
                "outputType": "ric",
                "outputValue": "USAT.OQ",
                "startDate": "2021-01-01",
                "endDate": "2021-04-17",
                "input": "104563",
            },
            {
                "assetId": "MA2640YQADTHYZ4M",
                "outputType": "rcic",
                "outputValue": "CTLP.O",
                "startDate": "2021-04-19",
                "endDate": "2021-11-01",
                "input": "104563",
            },
            {
                "assetId": "MAR754Z5RQYZ3V8E",
                "outputType": "ric",
                "outputValue": "CTLP.OQ",
                "startDate": "2021-04-19",
                "endDate": "2021-11-01",
                "input": "104563",
            },
            # additional RICs omitted from test
            {
                "outputType": "gsid",
                "outputValue": 104563,
                "startDate": "2021-01-01",
                "endDate": "2021-04-18",
                "input": "104563",
            },
            {
                "outputType": "gsid",
                "outputValue": 104563,
                "startDate": "2021-04-19",
                "endDate": "2021-11-01",
                "input": "104563",
            },
            {
                "outputType": "isin",
                "outputValue": "US90328S5001",
                "startDate": "2021-01-01",
                "endDate": "2021-04-18",
                "input": "104563",
            },
            {
                "outputType": "isin",
                "outputValue": "US1381031061",
                "startDate": "2021-04-19",
                "endDate": "2021-11-01",
                "input": "104563",
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock])
    start = dt.date(2021, 1, 1)
    end = dt.date(2021, 11, 1)

    expected = {
        "2021-04-16": {"104563": {"ric": ["USAT.OQ"], "gsid": [104563], "isin": ["US90328S5001"], "bcid": ["USAT US"]}},
        "2021-04-19": {"104563": {"ric": ["CTLP.OQ"], "gsid": [104563], "isin": ["US1381031061"], "bcid": ["CTLP US"]}},
    }
    targets = [SecurityIdentifier.RIC, SecurityIdentifier.GSID, SecurityIdentifier.ISIN, SecurityIdentifier.BCID]
    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(SecurityIdentifier.GSID, ['104563'], targets, start, end)
    for k, v in expected.items():
        assert actual[k] == v


def test_map_identifiers_empty(mocker):
    mock = {"results": []}
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock])

    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(SecurityIdentifier.BBID, ['invalid id'], [SecurityIdentifier.RIC])
    assert actual == {}


def test_map_identifiers_eq_index(mocker):
    """
    Test to ensure that gsq result does not append exchange or compositeExchange to Bcid and Bbid if secmaster api
    does not respond any (Mainly from mapping equity indices).
    """
    mock = {
        "results": [
            {
                "outputType": "bbg",
                "outputValue": "SPX",
                "startDate": "2022-03-17",
                "endDate": "2022-03-17",
                "input": "100",
            }
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock])

    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(
            SecurityIdentifier.GSID, ['100'], [SecurityIdentifier.BBID, SecurityIdentifier.BCID]
        )
    assert actual == {'2022-03-17': {'100': {'bbid': ['SPX']}}}


def test_secmaster_map_identifiers_with_passed_input_types(mocker):
    start = str(dt.date(2021, 10, 11))
    end = str(dt.date(2021, 10, 12))

    def mock_mapping_service_response_by_input_type(*args, **kwargs):
        '''
        Mocks Secmaster api's response json based on payload's input_type, output_type, and ids provided
        '''
        input_type = None
        for enum in SecurityIdentifier:
            if enum.value in kwargs['payload']:
                input_type = enum.value
                break
        output_types = kwargs['payload']['toIdentifiers']

        mock_output = {'results': []}
        for id in kwargs['payload'][input_type]:
            for output_type in output_types:
                row = {
                    "outputType": output_type,
                    "outputValue": "mock output for " + id,
                    "startDate": start,
                    "endDate": end,
                    "input": id,
                }
                if output_type in (SecurityIdentifier.BBID, SecurityIdentifier.BBG, SecurityIdentifier.BCID):
                    row['exchange'] = 'mock-exchange'
                    row['compositeExchange'] = 'mock-comp'
                mock_output['results'].append(row)
        return mock_output

    mocker.patch.object(GsSession.current.sync, 'get', side_effect=mock_mapping_service_response_by_input_type)

    with SecMasterContext():
        mock_any_ids = ["mock-any-1", "mock-any-2"]
        any_to_cusip_results = SecurityMaster.map_identifiers(
            input_type=SecurityIdentifier.ANY, ids=mock_any_ids, output_types=[SecurityIdentifier.CUSIP]
        )
        assert start in any_to_cusip_results.keys()
        for input_id in mock_any_ids:
            assert input_id in any_to_cusip_results[start].keys()
            assert SecurityIdentifier.CUSIP.value in any_to_cusip_results[start][input_id].keys()
        assert any_to_cusip_results == {
            "2021-10-11": {
                "mock-any-1": {"cusip": ["mock output for mock-any-1"]},
                "mock-any-2": {"cusip": ["mock output for mock-any-2"]},
            },
            "2021-10-12": {
                "mock-any-1": {"cusip": ["mock output for mock-any-1"]},
                "mock-any-2": {"cusip": ["mock output for mock-any-2"]},
            },
        }

        mock_cusip_ids = ["mock-cusip-input1", "mock-cusip-input2"]
        cusip_to_isin_result = SecurityMaster.map_identifiers(
            input_type=SecurityIdentifier.CUSIP, ids=mock_cusip_ids, output_types=[SecurityIdentifier.ISIN]
        )
        assert start in cusip_to_isin_result.keys()
        for cusip_input_id in mock_cusip_ids:
            assert cusip_input_id in cusip_to_isin_result[start].keys()
            assert SecurityIdentifier.ISIN.value in cusip_to_isin_result[start][cusip_input_id].keys()
        assert cusip_to_isin_result == {
            "2021-10-11": {
                "mock-cusip-input1": {"isin": ["mock output for mock-cusip-input1"]},
                "mock-cusip-input2": {"isin": ["mock output for mock-cusip-input2"]},
            },
            "2021-10-12": {
                "mock-cusip-input1": {"isin": ["mock output for mock-cusip-input1"]},
                "mock-cusip-input2": {"isin": ["mock output for mock-cusip-input2"]},
            },
        }


def test_secmaster_map_identifiers_return_array_results(mocker):
    """
    Check if map endpoint returns multi-valued response in arrays
    """
    mock = {
        "results": [
            {
                "outputType": "bbg",
                "outputValue": "GS",
                "exchange": "UN",
                "compositeExchange": "US",
                "startDate": "2022-03-21",
                "endDate": "2022-03-21",
                "input": "38141G104",
            },
            {
                "outputType": "cusip",
                "outputValue": "38141G104",
                "startDate": "2022-03-21",
                "endDate": "2022-03-21",
                "input": "38141G104",
            },
            {
                "outputType": "bbg",
                "outputValue": "GOS",
                "exchange": "TH",
                "startDate": "2022-03-21",
                "endDate": "2022-03-21",
                "input": "38141G104",
            },
            {
                "outputType": "bbg",
                "outputValue": "GSCHF",
                "exchange": "EU",
                "startDate": "2022-03-21",
                "endDate": "2022-03-21",
                "input": "38141G104",
            },
            {
                "outputType": "bbg",
                "outputValue": "GSUSD",
                "exchange": "SE",
                "compositeExchange": "SW",
                "startDate": "2022-03-21",
                "endDate": "2022-03-21",
                "input": "38141G104",
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock])
    with SecMasterContext():
        actual = SecurityMaster.map_identifiers(
            input_type=SecurityIdentifier.CUSIP,
            ids=['38141G104'],
            output_types=[SecurityIdentifier.BBID, SecurityIdentifier.BCID, SecurityIdentifier.CUSIP],
        )
    assert actual == {
        '2022-03-21': {
            '38141G104': {
                'bbid': ['GS UN', 'GOS TH', 'GSCHF EU', 'GSUSD SE'],
                'bcid': ['GS US', 'GSUSD SW'],
                'cusip': ['38141G104'],
            }
        }
    }


def test_secmaster_get_asset_no_asset_id_response_should_fail(mocker):
    mock_response = {
        "results": [
            {
                "name": "GOLDMAN SACHS GROUP INC (US Stock Exchange Composite)",
                "type": "Common Stock",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 901026,
                    "cusip": "38141G104",
                    "cusip8": "38141G10",
                    "sedol": "2407966",
                    "isin": "US38141G1040",
                    "ticker": "GS",
                    "bcid": "GS US",
                    "primeId": "1003232152",
                    "factSetRegionalId": "JLJ0VZ-R",
                    "rcic": "GS",
                },
                "exchange": {"name": "US Stock Exchange Composite", "identifiers": {"gsExchangeId": 161}},
                "id": "GSPD901026E161",
            }
        ],
        "totalResults": 1,
    }

    mock_no_asset_id_response = {"results": []}
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_response, mock_no_asset_id_response])

    with SecMasterContext():
        asset = SecurityMaster.get_asset(id_value="GS", id_type=SecurityIdentifier.TICKER)
        with pytest.raises(MqValueError):
            asset.get_marquee_id()


def test_secmaster_get_asset_returning_secmasterassets(mocker):
    def assert_asset_common(asset: Asset) -> None:
        with pytest.raises(MqTypeError):
            asset.get_identifier(id_type=AssetIdentifier.BLOOMBERG_ID)

    # get_asset() should return Stock instance when type: Common Stock
    mock_equity_response = {
        "results": [
            {
                "name": "GOLDMAN SACHS GROUP INC (New York Stock)",
                "type": "Common Stock",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 901026,
                    "cusip": "38141G104",
                    "cusip8": "38141G10",
                    "sedol": "2407966",
                    "isin": "US38141G1040",
                    "ticker": "GS",
                    "bbid": "GS UN",
                    "bcid": "GS US",
                    "primeId": "1003232152",
                    "factSetRegionalId": "JLJ0VZ-R",
                    "rcic": "GS",
                    "ric": "GS.N",
                    "assetId": "MA4B66MW5E27UAHKG34",
                },
                "exchange": {"name": "New York Stock", "identifiers": {"gsExchangeId": 154}},
                "id": "GSPD901026E154",
            }
        ],
        "totalResults": 1,
    }

    mock_eq_id_history_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "GS",
                "updateTime": "2002-02-09T17:58:27.58Z",
                "type": "bbg",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "38141G104",
                "updateTime": "2002-02-09T17:54:27.99Z",
                "type": "cusip",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "38141G10",
                "updateTime": "2002-02-09T17:54:27.99Z",
                "type": "cusip8",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "JLJ0VZ-R",
                "updateTime": "2021-08-16T08:41:43.586Z",
                "type": "factSetRegionalId",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "US38141G1040",
                "updateTime": "2002-02-09T17:55:18.513Z",
                "type": "isin",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "1003232152",
                "updateTime": "2003-01-16T15:22:54.1Z",
                "type": "primeId",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "2407966",
                "updateTime": "2002-02-09T17:54:47.77Z",
                "type": "sedol",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "GS",
                "updateTime": "2002-02-09T17:57:14.546Z",
                "type": "ticker",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "assetId",
                "value": "MA4B66MW5E27UAHKG34",
                "updateTime": "2002-10-30T21:30:29.993Z",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "ric",
                "value": "GS.N",
                "updateTime": "2002-10-30T21:30:29.993Z",
                "gsExchangeId": 154,
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "1003232152",
                "updateTime": "2003-01-16T15:22:54.1Z",
                "type": "primeId",
            },
        ]
    }
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_equity_response, mock_eq_id_history_response])
    with SecMasterContext():
        stock = SecurityMaster.get_asset(id_value=901026, id_type=SecurityIdentifier.GSID)
    assert isinstance(stock, SecMasterAsset)
    assert stock.get_type() == AssetType.COMMON_STOCK
    assert stock.get_marquee_id() == "MA4B66MW5E27UAHKG34"
    assert stock.get_identifier(id_type=SecurityIdentifier.BBG) == "GS"
    assert stock.get_identifier(id_type=SecurityIdentifier.ID) == "GSPD901026E154"
    assert stock.get_identifier(id_type=SecurityIdentifier.ASSET_ID) == "MA4B66MW5E27UAHKG34"
    assert stock.currency == "USD"
    assert stock.get_identifiers() == {
        'assetId': 'MA4B66MW5E27UAHKG34',
        'bbg': 'GS',
        'cusip': '38141G104',
        'cusip8': '38141G10',
        'gsid': 901026,
        'id': 'GSPD901026E154',
        'isin': 'US38141G1040',
        'primeId': '1003232152',
        'ric': 'GS.N',
        'sedol': '2407966',
        'ticker': 'GS',
    }
    assert_asset_common(stock)

    # get_asset() should return Index instance when type: Equity Index
    mock_index_response = {
        "results": [
            {
                "name": "S&P 500 INDEX",
                "type": "Equity Index",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 100,
                    "ticker": "SPX",
                    "bbid": "SPX",
                    "ric": ".SPX",
                    "assetId": "MA4B66MW5E27U8P32SB",
                },
                "company": {"name": "S&P 500 Index", "identifiers": {"gsCompanyId": 10756}},
                "id": "GSPD100",
            }
        ],
        "totalResults": 1,
    }
    mock_index_id_history_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "2012-08-24",
                "value": "SPX",
                "updateTime": "2012-08-25T23:27:53.44Z",
                "type": "bbg",
            },
            {
                "startDate": "2012-08-25",
                "endDate": "2012-08-25",
                "value": "SPX",
                "updateTime": "2020-12-10T21:07:06.26Z",
                "type": "bbg",
            },
            {
                "startDate": "2012-08-26",
                "endDate": "9999-99-99",
                "value": "SPX",
                "updateTime": "2012-08-27T01:48:07.046Z",
                "type": "bbg",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "2012-08-24",
                "value": "SPX",
                "updateTime": "2012-08-25T23:27:53.4Z",
                "type": "ticker",
            },
            {
                "startDate": "2012-08-25",
                "endDate": "2012-08-25",
                "value": "SPX",
                "updateTime": "2020-12-10T21:06:44.82Z",
                "type": "ticker",
            },
            {
                "startDate": "2012-08-26",
                "endDate": "9999-99-99",
                "value": "SPX",
                "updateTime": "2012-08-27T01:48:07.043Z",
                "type": "ticker",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "assetId",
                "value": "MA4B66MW5E27U8P32SB",
                "updateTime": "2003-01-14T17:28:15.29Z",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "type": "ric",
                "value": ".SPX",
                "updateTime": "2003-01-14T17:28:15.29Z",
                "gsExchangeId": 0,
            },
        ]
    }
    mocker.patch.object(
        GsSession.current.sync, 'get', side_effect=[mock_index_response, mock_index_id_history_response]
    )
    with SecMasterContext():
        index = SecurityMaster.get_asset(id_value=100, id_type=SecurityIdentifier.GSID)
    assert isinstance(index, SecMasterAsset)
    assert index.get_type() == AssetType.EQUITY_INDEX
    assert index.get_marquee_id() == "MA4B66MW5E27U8P32SB"
    assert index.get_identifier(id_type=SecurityIdentifier.RIC) == ".SPX"
    assert index.get_identifier(id_type=SecurityIdentifier.ID) == "GSPD100"
    assert index.get_identifier(id_type=SecurityIdentifier.ASSET_ID) == "MA4B66MW5E27U8P32SB"
    assert index.currency == "USD"
    assert index.get_identifiers() == {
        'assetId': 'MA4B66MW5E27U8P32SB',
        'bbg': 'SPX',
        'gsid': 100,
        'id': 'GSPD100',
        'ric': '.SPX',
        'ticker': 'SPX',
    }
    assert_asset_common(index)

    # get_asset() should return ETF instance when type: ETF
    mock_ETF_response = {
        "results": [
            {
                "name": "ISHARES US TRANSPORTATION ET (BATS US Trading)",
                "type": "ETF",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {
                    "gsid": 159943,
                    "primeId": "355769575",
                    "bbid": "IYT UF",
                    "bcid": "IYT US",
                    "ticker": "IYT",
                    "isin": "US4642871929",
                    "sedol": "2012423",
                    "cusip": "464287192",
                    "cusip8": "46428719",
                    "rcic": "IYT",
                    "ric": "IYT.Z",
                    "assetId": "MAZ08H8QPDQ4T7SE",
                },
                "exchange": {"name": "BATS US Trading", "identifiers": {"gsExchangeId": 535}},
                "id": "GSPD159943E535",
            }
        ],
        "totalResults": 1,
    }

    mock_etf_id_history_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "IYT",
                "updateTime": "2003-09-16T22:00:44.586Z",
                "type": "bbg",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "464287192",
                "updateTime": "2003-09-16T22:00:44.506Z",
                "type": "cusip",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "46428719",
                "updateTime": "2003-09-16T22:00:44.506Z",
                "type": "cusip8",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "US4642871929",
                "updateTime": "2003-09-16T22:00:44.52Z",
                "type": "isin",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "355769575",
                "updateTime": "2003-10-02T05:12:03.51Z",
                "type": "primeId",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "2012423",
                "updateTime": "2003-10-10T23:49:00.12Z",
                "type": "sedol",
            },
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "IYT",
                "updateTime": "2003-09-16T22:00:44.56Z",
                "type": "ticker",
            },
            {
                "startDate": "2008-11-09",
                "endDate": "2017-08-01",
                "type": "assetId",
                "value": "MAZ08H8QPDQ4T7SE",
                "updateTime": "2017-08-02T05:36:56.823Z",
            },
            {
                "startDate": "2017-08-03",
                "endDate": "9999-99-99",
                "type": "assetId",
                "value": "MAZ08H8QPDQ4T7SE",
                "updateTime": "2017-08-02T16:09:56.146Z",
            },
            {
                "startDate": "2008-11-09",
                "endDate": "2017-08-01",
                "type": "ric",
                "value": "IYT.Z",
                "updateTime": "2017-08-02T05:36:56.823Z",
                "gsExchangeId": 535,
            },
            {
                "startDate": "2017-08-03",
                "endDate": "9999-99-99",
                "type": "ric",
                "value": "IYT.Z",
                "updateTime": "2017-08-02T16:09:56.146Z",
                "gsExchangeId": 535,
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_ETF_response, mock_etf_id_history_response])
    with SecMasterContext():
        etf = SecurityMaster.get_asset(id_value=159943, id_type=SecurityIdentifier.GSID)
    assert isinstance(etf, SecMasterAsset)
    assert etf.get_type() == AssetType.ETF
    assert etf.get_marquee_id() == "MAZ08H8QPDQ4T7SE"
    assert etf.get_identifier(id_type=SecurityIdentifier.RIC) == "IYT.Z"
    assert etf.get_identifier(id_type=SecurityIdentifier.ID) == "GSPD159943E535"
    assert etf.get_identifier(id_type=SecurityIdentifier.ASSET_ID) == "MAZ08H8QPDQ4T7SE"
    assert etf.currency == "USD"
    assert etf.get_identifiers() == {
        "gsid": 159943,
        "primeId": "355769575",
        "bbg": "IYT",
        "ticker": "IYT",
        "isin": "US4642871929",
        "sedol": "2012423",
        "cusip": "464287192",
        "cusip8": "46428719",
        "ric": "IYT.Z",
        "assetId": "MAZ08H8QPDQ4T7SE",
        'id': 'GSPD159943E535',
    }
    assert_asset_common(etf)

    # get_asset() should return Currency instance when type: Currency
    mock_currency_response = {
        "results": [
            {
                "name": "USD U.S. DOLLAR",
                "type": "Currency",
                "currency": "USD",
                "tags": [],
                "assetClass": "Cash",
                "identifiers": {"gsid": 4007, "assetId": "MAZ7RWC904JYHYPS", "ticker": "USD"},
                "id": "GSPD4007",
            }
        ],
        "totalResults": 1,
    }

    mock_currency_id_history_response = {
        "results": [
            {
                "startDate": "2007-01-01",
                "endDate": "9999-99-99",
                "value": "USD",
                "updateTime": "2003-05-01T16:20:44.47Z",
                "type": "ticker",
            }
        ]
    }
    mocker.patch.object(
        GsSession.current.sync, 'get', side_effect=[mock_currency_response, mock_currency_id_history_response]
    )
    with SecMasterContext():
        currency = SecurityMaster.get_asset(id_value=4007, id_type=SecurityIdentifier.GSID)
    assert isinstance(currency, SecMasterAsset)
    assert currency.get_type() == AssetType.CURRENCY
    assert currency.get_marquee_id() == "MAZ7RWC904JYHYPS"
    assert currency.get_identifier(id_type=SecurityIdentifier.TICKER) == "USD"
    assert currency.get_identifier(id_type=SecurityIdentifier.ID) == "GSPD4007"
    assert currency.get_identifier(id_type=SecurityIdentifier.ASSET_ID) == "MAZ7RWC904JYHYPS"
    assert currency.get_identifiers() == {
        "gsid": 4007,
        "assetId": "MAZ7RWC904JYHYPS",
        "ticker": "USD",
        "id": "GSPD4007",
    }
    assert_asset_common(currency)


def test_get_asset_get_data_series_with_range_over_many_asset_id_should_throw_mqerror(mocker):
    mock_asset = {
        "results": [
            {
                "name": "ISHARES US TRANSPORTATION ET (BATS US Trading)",
                "type": "ETF",
                "currency": "USD",
                "tags": [],
                "assetClass": "Equity",
                "identifiers": {"assetId": "MAZ08H8QPDQ4T7SE"},
                "exchange": {"name": "BATS US Trading", "identifiers": {"gsExchangeId": 535}},
                "id": "GSPD159943E535",
            }
        ],
        "totalResults": 1,
    }
    mock_id_history_response = {
        "results": [
            {
                "startDate": "2020-01-01",
                "endDate": "9999-99-99",
                "value": "marqueid 1",
                "updateTime": "2003-05-01T16:20:44.47Z",
                "type": "assetId",
            },
            {
                "startDate": "2007-12-30",
                "endDate": "2019-12-31",
                "value": "marqueid 2",
                "updateTime": "2003-05-01T16:20:44.47Z",
                "type": "assetId",
            },
        ]
    }
    mocker.patch.object(GsSession.current.sync, 'get', side_effect=[mock_asset, mock_id_history_response])

    with SecMasterContext():
        asset = SecurityMaster.get_asset(id_value=4007, id_type=SecurityIdentifier.GSID)
    with pytest.raises(MqValueError):
        asset.get_hloc_prices(start=dt.date(2007, 1, 1), end=dt.date(2022, 1, 1))


def test_map_identifiers_asset_service(mocker):
    response = {'AAPL UN': ['AAPL.N'], 'GS UN': ['GS.N']}
    mocker.patch.object(GsAssetApi, 'map_identifiers', side_effect=lambda *arg, **kwargs: response)
    expected = {"2021-10-11": {"AAPL UN": {"ric": ["AAPL.N"]}, "GS UN": {"ric": ["GS.N"]}}}
    with AssetContext():
        actual = SecurityMaster.map_identifiers(
            SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], [SecurityIdentifier.RIC], as_of_date=dt.date(2021, 10, 11)
        )
    assert actual == expected

    date_string = dt.date.today().strftime('%Y-%m-%d')
    expected2 = {date_string: expected["2021-10-11"]}
    with AssetContext():
        actual2 = SecurityMaster.map_identifiers(
            SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], [SecurityIdentifier.RIC]
        )
    assert actual2 == expected2

    mocker.patch.object(GsAssetApi, 'map_identifiers', side_effect=lambda *arg, **kwargs: {})
    with AssetContext():
        actual = SecurityMaster.map_identifiers(
            SecurityIdentifier.BBID, ['invalid id'], [SecurityIdentifier.RIC], as_of_date=dt.date(2021, 10, 11)
        )
    assert actual == {}


def test_map_identifiers_asset_service_exceptions():
    with pytest.raises(MqValueError):
        # multiple output types
        with AssetContext():
            SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN', 'AAPL UN'],
                [SecurityIdentifier.RIC, SecurityIdentifier.GSID],
                as_of_date=dt.date(2021, 10, 11),
            )

    with pytest.raises(MqValueError):
        # start date
        with AssetContext():
            SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN', 'AAPL UN'],
                [SecurityIdentifier.RIC],
                start_date=dt.date(2021, 10, 11),
            )

    with pytest.raises(MqValueError):
        # end date
        with AssetContext():
            SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], [SecurityIdentifier.RIC], end_date=dt.date(2021, 10, 11)
            )

    with pytest.raises(MqValueError):
        # unsupported output type
        with AssetContext():
            SecurityMaster.map_identifiers(SecurityIdentifier.BBID, ['GS UN', 'AAPL UN'], [SecurityIdentifier.BBG])


###############################################################################
# NEW TESTS below – targeting uncovered branches for maximum coverage increase
###############################################################################

from unittest.mock import MagicMock, patch, PropertyMock, AsyncMock
import asyncio
import calendar


def _run_async(coro):
    """Run a coroutine in a fresh event loop (immune to closed-loop pollution)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

from gs_quant.data import DataMeasure, DataFrequency
from gs_quant.data.core import IntervalFrequency, DataAggregationOperator
from gs_quant.markets.securities import (
    Stock,
    Cross,
    Future,
    Currency as CurrencyAsset,
    Rate,
    Cash,
    WeatherIndex,
    CommodityReferencePrice,
    CommodityNaturalGasHub,
    CommodityEUNaturalGasHub,
    Cryptocurrency,
    CommodityPowerNode,
    CommodityPowerAggregatedNodes,
    Commodity,
    Bond,
    Fund,
    FutureMarket,
    FutureContract,
    Swap,
    Option,
    Forward,
    ETF,
    Swaption,
    Binary,
    DefaultSwap,
    XccySwapMTM,
    MutualFund,
    Security,
    ReturnType,
    _get_with_retries,
)
from gs_quant.common import Currency as CurrencyEnum


# ---------------------------------------------------------------------------
#  Enum and basic construction tests
# ---------------------------------------------------------------------------


class TestEnums:
    def test_exchange_code_values(self):
        assert ExchangeCode.NASDAQ.value == "NASD"
        assert ExchangeCode.NYSE.value == "NYSE"

    def test_asset_type_values_smoke(self):
        assert AssetType.STOCK.value == "Single Stock"
        assert AssetType.INDEX.value == "Index"
        assert AssetType.ETF.value == "ETF"
        assert AssetType.CUSTOM_BASKET.value == "Custom Basket"
        assert AssetType.RESEARCH_BASKET.value == "Research Basket"
        assert AssetType.FUTURE.value == "Future"
        assert AssetType.CROSS.value == "Cross"
        assert AssetType.CURRENCY.value == "Currency"
        assert AssetType.RATE.value == "Rate"
        assert AssetType.CASH.value == "Cash"
        assert AssetType.WEATHER_INDEX.value == "Weather Index"
        assert AssetType.SWAP.value == "Swap"
        assert AssetType.SWAPTION.value == "Swaption"
        assert AssetType.OPTION.value == "Option"
        assert AssetType.BINARY.value == "Binary"
        assert AssetType.COMMODITY_REFERENCE_PRICE.value == "Commodity Reference Price"
        assert AssetType.COMMODITY_NATURAL_GAS_HUB.value == "Commodity Natural Gas Hub"
        assert AssetType.COMMODITY_EU_NATURAL_GAS_HUB.value == "Commodity EU Natural Gas Hub"
        assert AssetType.COMMODITY_POWER_NODE.value == "Commodity Power Node"
        assert AssetType.COMMODITY_POWER_AGGREGATED_NODES.value == "Commodity Power Aggregated Nodes"
        assert AssetType.BOND.value == "Bond"
        assert AssetType.FUTURE_MARKET.value == "Future Market"
        assert AssetType.FUTURE_CONTRACT.value == "Future Contract"
        assert AssetType.COMMODITY.value == "Commodity"
        assert AssetType.CRYPTOCURRENCY.value == "Cryptocurrency"
        assert AssetType.FORWARD.value == "Forward"
        assert AssetType.FUND.value == "Fund"
        assert AssetType.DEFAULT_SWAP.value == "Default Swap"
        assert AssetType.COMMON_STOCK.value == "Common Stock"
        assert AssetType.EQUITY_INDEX.value == "Equity Index"
        assert AssetType.MUTUAL_FUND.value == "Mutual Fund"

    def test_return_type_values(self):
        assert ReturnType.EXCESS_RETURN.value == "Excess Return"
        assert ReturnType.TOTAL_RETURN.value == "Total Return"

    def test_asset_identifier_values(self):
        assert AssetIdentifier.MARQUEE_ID.value == "MQID"
        assert AssetIdentifier.REUTERS_ID.value == "RIC"
        assert AssetIdentifier.BLOOMBERG_ID.value == "BBID"
        assert AssetIdentifier.BLOOMBERG_COMPOSITE_ID.value == "BCID"
        assert AssetIdentifier.CUSIP.value == "CUSIP"
        assert AssetIdentifier.ISIN.value == "ISIN"
        assert AssetIdentifier.SEDOL.value == "SEDOL"
        assert AssetIdentifier.TICKER.value == "TICKER"
        assert AssetIdentifier.PLOT_ID.value == "PLOT_ID"
        assert AssetIdentifier.GSID.value == "GSID"
        assert AssetIdentifier.NAME.value == "NAME"

    def test_security_identifier_values(self):
        assert SecurityIdentifier.GSID.value == "gsid"
        assert SecurityIdentifier.RCIC.value == "rcic"
        assert SecurityIdentifier.RIC.value == "ric"
        assert SecurityIdentifier.ID.value == "id"
        assert SecurityIdentifier.CUSIP.value == "cusip"
        assert SecurityIdentifier.CUSIP8.value == "cusip8"
        assert SecurityIdentifier.CINS.value == "cins"
        assert SecurityIdentifier.SEDOL.value == "sedol"
        assert SecurityIdentifier.ISIN.value == "isin"
        assert SecurityIdentifier.TICKER.value == "ticker"
        assert SecurityIdentifier.BBID.value == "bbid"
        assert SecurityIdentifier.BCID.value == "bcid"
        assert SecurityIdentifier.GSS.value == "gss"
        assert SecurityIdentifier.PRIMEID.value == "primeId"
        assert SecurityIdentifier.BBG.value == "bbg"
        assert SecurityIdentifier.ASSET_ID.value == "assetId"
        assert SecurityIdentifier.ANY.value == "identifiers"
        assert SecurityIdentifier.BARRA_ID.value == "barraId"
        assert SecurityIdentifier.AXIOMA_ID.value == "axiomaId"

    def test_security_master_source_values(self):
        assert SecurityMasterSource.ASSET_SERVICE.value is not None
        assert SecurityMasterSource.SECURITY_MASTER.value is not None


# ---------------------------------------------------------------------------
#  Asset subclass construction and get_type() tests
# ---------------------------------------------------------------------------


class TestAssetSubclassConstruction:
    def test_stock_construction_and_type(self):
        s = Stock('id1', 'Test Stock', 'NYSE', CurrencyEnum.USD)
        assert s.get_type() == AssetType.STOCK
        assert s.name == 'Test Stock'
        assert s.exchange == 'NYSE'
        assert s.get_currency() == CurrencyEnum.USD
        assert s.get_marquee_id() == 'id1'
        assert s.asset_class == AssetClass.Equity

    def test_cross_construction_and_type(self):
        c = Cross('id2', 'USDJPY')
        assert c.get_type() == AssetType.CROSS
        assert c.asset_class == AssetClass.FX

    def test_cross_construction_with_string_asset_class(self):
        c = Cross('id2', 'USDJPY', asset_class='FX')
        assert c.get_type() == AssetType.CROSS
        assert c.asset_class == AssetClass.FX

    def test_future_construction_and_type(self):
        f = Future('id3', AssetClass.Commod, 'CL Future', CurrencyEnum.USD)
        assert f.get_type() == AssetType.FUTURE
        assert f.get_currency() == CurrencyEnum.USD

    def test_future_construction_with_string_asset_class(self):
        f = Future('id3', 'Commod', 'CL Future')
        assert f.get_type() == AssetType.FUTURE

    def test_currency_construction_and_type(self):
        c = CurrencyAsset('id4', 'USD')
        assert c.get_type() == AssetType.CURRENCY
        assert c.asset_class == AssetClass.Cash

    def test_rate_construction_and_type(self):
        r = Rate('id5', 'SOFR')
        assert r.get_type() == AssetType.RATE
        assert r.asset_class == AssetClass.Rates

    def test_cash_construction_and_type(self):
        c = Cash('id6', 'US Dollar Cash')
        assert c.get_type() == AssetType.CASH
        assert c.asset_class == AssetClass.Cash

    def test_weather_index_construction_and_type(self):
        w = WeatherIndex('id7', 'HDD Index')
        assert w.get_type() == AssetType.WEATHER_INDEX
        assert w.asset_class == AssetClass.Commod

    def test_commodity_reference_price_construction(self):
        c = CommodityReferencePrice('id8', 'Brent Ref')
        assert c.get_type() == AssetType.COMMODITY_REFERENCE_PRICE
        assert c.asset_class == AssetClass.Commod

    def test_commodity_natural_gas_hub_construction(self):
        c = CommodityNaturalGasHub('id9', 'Henry Hub')
        assert c.get_type() == AssetType.COMMODITY_NATURAL_GAS_HUB
        assert c.asset_class == AssetClass.Commod

    def test_commodity_eu_natural_gas_hub_construction(self):
        c = CommodityEUNaturalGasHub('id10', 'TTF')
        assert c.get_type() == AssetType.COMMODITY_EU_NATURAL_GAS_HUB
        assert c.asset_class == AssetClass.Commod

    def test_cryptocurrency_construction(self):
        c = Cryptocurrency('id11', AssetClass.Commod, 'Bitcoin')
        assert c.get_type() == AssetType.CRYPTOCURRENCY

    def test_commodity_power_node_construction(self):
        c = CommodityPowerNode('id12', 'PJM Node')
        assert c.get_type() == AssetType.COMMODITY_POWER_NODE
        assert c.asset_class == AssetClass.Commod

    def test_commodity_power_aggregated_nodes_construction(self):
        c = CommodityPowerAggregatedNodes('id13', 'PJM Aggregated')
        assert c.get_type() == AssetType.COMMODITY_POWER_AGGREGATED_NODES

    def test_commodity_construction(self):
        c = Commodity('id14', 'Gold')
        assert c.get_type() == AssetType.COMMODITY

    def test_bond_construction(self):
        b = Bond('id15', 'US 10Y', AssetClass.Credit)
        assert b.get_type() == AssetType.BOND
        assert b.asset_class == AssetClass.Credit

    def test_bond_construction_default_asset_class(self):
        b = Bond('id15', 'US 10Y')
        assert b.asset_class == AssetClass.Credit

    def test_fund_construction(self):
        f = Fund('id16', 'HF Fund', AssetClass.Equity)
        assert f.get_type() == AssetType.FUND

    def test_future_market_construction(self):
        fm = FutureMarket('id17', AssetClass.Commod, 'CL Market')
        assert fm.get_type() == AssetType.FUTURE_MARKET

    def test_future_market_string_asset_class(self):
        fm = FutureMarket('id17', 'Commod', 'CL Market')
        assert fm.get_type() == AssetType.FUTURE_MARKET

    def test_future_contract_construction(self):
        fc = FutureContract('id18', AssetClass.Commod, 'CL Jan24')
        assert fc.get_type() == AssetType.FUTURE_CONTRACT

    def test_future_contract_string_asset_class(self):
        fc = FutureContract('id18', 'Commod', 'CL Jan24')
        assert fc.get_type() == AssetType.FUTURE_CONTRACT

    def test_swap_construction(self):
        s = Swap('id19', AssetClass.Rates, 'IRS 10Y')
        assert s.get_type() == AssetType.SWAP

    def test_swap_string_asset_class(self):
        s = Swap('id19', 'Rates', 'IRS 10Y')
        assert s.get_type() == AssetType.SWAP

    def test_option_construction(self):
        o = Option('id20', AssetClass.Equity, 'SPX Call')
        assert o.get_type() == AssetType.OPTION

    def test_option_string_asset_class(self):
        o = Option('id20', 'Equity', 'SPX Call')
        assert o.get_type() == AssetType.OPTION

    def test_forward_construction(self):
        f = Forward('id21', AssetClass.FX, 'FX Forward')
        assert f.get_type() == AssetType.FORWARD

    def test_forward_string_asset_class(self):
        f = Forward('id21', 'FX', 'FX Forward')
        assert f.get_type() == AssetType.FORWARD

    def test_etf_construction(self):
        e = ETF('id22', AssetClass.Equity, 'SPY', 'NYSE', CurrencyEnum.USD)
        assert e.get_type() == AssetType.ETF
        assert e.get_currency() == CurrencyEnum.USD

    def test_swaption_construction(self):
        s = Swaption('id23', '1Y10Y Swaption')
        assert s.get_type() == AssetType.SWAPTION
        assert s.asset_class == AssetClass.Rates

    def test_binary_construction(self):
        b = Binary('id24', 'Binary Option', AssetClass.FX)
        assert b.get_type() == AssetType.BINARY

    def test_default_swap_construction(self):
        d = DefaultSwap('id25', 'CDS 5Y')
        assert d.get_type() == AssetType.DEFAULT_SWAP
        assert d.asset_class == AssetClass.Credit

    def test_xccy_swap_mtm_construction(self):
        x = XccySwapMTM('id26', 'XccySwap')
        # AssetType.XccySwapMTM is not defined in the enum, so get_type() will raise
        assert x.asset_class == AssetClass.Rates
        with pytest.raises(AttributeError):
            x.get_type()

    def test_mutual_fund_construction(self):
        m = MutualFund('id27', 'MF Fund', AssetClass.Equity)
        assert m.get_type() == AssetType.MUTUAL_FUND

    def test_entity_type(self):
        from gs_quant.entities.entity import EntityType
        s = Stock('id1', 'Test Stock')
        assert s.entity_type() == EntityType.ASSET

    def test_data_dimension(self):
        s = Stock('id1', 'Test Stock')
        assert s.data_dimension == 'assetId'


# ---------------------------------------------------------------------------
#  Security class
# ---------------------------------------------------------------------------


class TestSecurity:
    def test_security_construction(self):
        data = {
            'name': 'Test Security',
            'type': 'Common Stock',
            'currency': 'USD',
            'identifiers': {
                'gsid': 901026,
                'ric': 'GS.N',
                'bbid': 'GS UN',
            },
        }
        sec = Security(data)
        assert sec.name == 'Test Security'
        assert sec.type == 'Common Stock'
        assert sec.currency == 'USD'

        ids = sec.get_identifiers()
        assert ids['gsid'] == 901026
        assert ids['ric'] == 'GS.N'
        assert ids['bbid'] == 'GS UN'

        # Verify deep copy
        ids['gsid'] = 999
        assert sec.get_identifiers()['gsid'] == 901026

    def test_security_str(self):
        data = {
            'name': 'Test',
            'identifiers': {'gsid': 123},
        }
        sec = Security(data)
        s = str(sec)
        assert 'name' in s
        assert 'Test' in s
        # private fields are excluded
        assert '_ids' not in s


# ---------------------------------------------------------------------------
#  Asset.get_url tests
# ---------------------------------------------------------------------------


class TestAssetGetUrl:
    def test_get_url_prod(self, mocker):
        from pydash import get as pydash_get
        mocker.patch('gs_quant.markets.securities.get',
                      side_effect=lambda obj, path, default='': '')
        s = Stock('MA12345', 'Test')
        url = s.get_url()
        assert url == 'https://marquee.gs.com/s/products/MA12345/summary'

    def test_get_url_dev(self, mocker):
        mocker.patch('gs_quant.markets.securities.get',
                      side_effect=lambda obj, path, default='': 'marquee-dev.gs.com')
        s = Stock('MA12345', 'Test')
        url = s.get_url()
        # 'dev' is in domain, then 'qa' is also checked but 'qa' is not in 'marquee-dev.gs.com'
        assert url == 'https://marquee-dev-ext.web.gs.com/s/products/MA12345/summary'

    def test_get_url_qa(self, mocker):
        mocker.patch('gs_quant.markets.securities.get',
                      side_effect=lambda obj, path, default='': 'marquee-qa.gs.com')
        s = Stock('MA12345', 'Test')
        url = s.get_url()
        # 'qa' is in domain -> env = '-qa'
        assert url == 'https://marquee-qa.gs.com/s/products/MA12345/summary'

    def test_get_url_dev_and_qa(self, mocker):
        """If domain contains both 'dev' and 'qa', the qa check overrides dev."""
        mocker.patch('gs_quant.markets.securities.get',
                      side_effect=lambda obj, path, default='': 'marquee-dev-qa.gs.com')
        s = Stock('MA12345', 'Test')
        url = s.get_url()
        assert url == 'https://marquee-qa.gs.com/s/products/MA12345/summary'


# ---------------------------------------------------------------------------
#  Asset.get_identifier - MARQUEE_ID short-circuit
# ---------------------------------------------------------------------------


class TestGetIdentifierMarqueeId:
    def test_get_identifier_marquee_id(self):
        s = Stock('MXYZ', 'Test')
        result = s.get_identifier(AssetIdentifier.MARQUEE_ID)
        assert result == 'MXYZ'


# ---------------------------------------------------------------------------
#  Asset.get_identifiers – PricingContext branches
# ---------------------------------------------------------------------------


class TestGetIdentifiers:
    def test_get_identifiers_with_as_of(self, mocker):
        """Explicit as_of date."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2025, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ric': 'GS.N', 'bbid': 'GS UN'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        ids = s.get_identifiers(as_of=dt.date(2023, 6, 15))
        assert ids.get('RIC') == 'GS.N'
        assert ids.get('BBID') == 'GS UN'

    def test_get_identifiers_entered_context(self, mocker):
        """Test when PricingContext is entered."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2025, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ticker': 'GS'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        market = PricingContext(dt.date(2023, 6, 15))
        with market:
            ids = s.get_identifiers()
        assert ids.get('TICKER') == 'GS'

    def test_get_identifiers_not_entered_context(self, mocker):
        """Test when PricingContext is not entered (used as default)."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2099, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ticker': 'GS'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        # Call without as_of and without entering PricingContext
        # This exercises the `not current.is_entered` branch
        ids = s.get_identifiers()
        assert ids.get('TICKER') == 'GS'

    def test_get_identifiers_xref_not_matching(self, mocker):
        """When no xref date range matches, identifiers should be empty."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2021, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ric': 'GS.N'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        ids = s.get_identifiers(as_of=dt.date(2023, 1, 1))
        assert ids == {}

    def test_get_identifiers_filters_invalid_keys(self, mocker):
        """Keys not in AssetIdentifier should be filtered out."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2025, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ric': 'GS.N', 'unknownId': 'some_val', 'ticker': 'GS'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        ids = s.get_identifiers(as_of=dt.date(2023, 1, 1))
        assert 'RIC' in ids
        assert 'TICKER' in ids
        assert 'UNKNOWNID' not in ids

    def test_get_identifiers_multiple_xrefs(self, mocker):
        """Only the xref matching as_of should contribute identifiers."""
        s = Stock('MA999', 'Test')

        xref1 = MagicMock()
        xref1.startDate = dt.date(2020, 1, 1)
        xref1.endDate = dt.date(2021, 12, 31)
        ids1 = MagicMock()
        ids1.as_dict.return_value = {'ticker': 'OLD'}
        xref1.identifiers = ids1

        xref2 = MagicMock()
        xref2.startDate = dt.date(2022, 1, 1)
        xref2.endDate = dt.date(2025, 12, 31)
        ids2 = MagicMock()
        ids2.as_dict.return_value = {'ticker': 'NEW'}
        xref2.identifiers = ids2

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[xref1, xref2])

        ids = s.get_identifiers(as_of=dt.date(2023, 1, 1))
        assert ids.get('TICKER') == 'NEW'


# ---------------------------------------------------------------------------
#  Asset.get_asset_measures
# ---------------------------------------------------------------------------


class TestGetAssetMeasures:
    def test_get_asset_measures_empty(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mocker.patch.object(GsSession.current.sync, 'get', return_value={'data': []})
        result = s.get_asset_measures()
        assert result == []

    def test_get_asset_measures_with_data(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mock_data = {
            'data': [
                {'type': 'Price', 'frequency': 'Daily', 'datasetField': 'close'},
                {'type': 'Volume', 'frequency': 'Daily'},  # missing datasetField -> won't be added
            ]
        }
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_data)
        result = s.get_asset_measures()
        # Only the first measure has all three keys
        assert len(result) == 1

    def test_get_asset_measures_no_data_key(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mock_data = {'data': None}
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_data)
        # When data is falsy, should return empty
        result = s.get_asset_measures()
        assert result == []


# ---------------------------------------------------------------------------
#  Asset.get_data_series and related
# ---------------------------------------------------------------------------


class TestAssetDataSeries:
    def test_get_data_series_no_coordinate(self, mocker):
        s = Stock('MA999', 'Test')
        mocker.patch.object(s, 'get_data_coordinate', return_value=None)
        with pytest.raises(MqValueError, match='No data coordinate found'):
            s.get_data_series(DataMeasure.CLOSE_PRICE)

    def test_get_data_series_no_dataset_id(self, mocker):
        s = Stock('MA999', 'Test')
        coord = MagicMock()
        coord.dataset_id = None
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)
        with pytest.raises(MqValueError, match='not found for asset'):
            s.get_data_series(DataMeasure.CLOSE_PRICE)

    def test_get_data_series_success(self, mocker):
        import pandas as pd
        s = Stock('MA999', 'Test')
        coord = MagicMock()
        coord.dataset_id = 'DS1'
        expected = pd.Series([100.0, 101.0])
        coord.get_series.return_value = expected
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)
        result = s.get_data_series(DataMeasure.CLOSE_PRICE, start=dt.date(2023, 1, 1), end=dt.date(2023, 1, 2))
        assert result is expected

    def test_get_latest_close_price_no_coordinate(self, mocker):
        s = Stock('MA999', 'Test')
        mocker.patch.object(s, 'get_data_coordinate', return_value=None)
        with pytest.raises(MqValueError, match='No data co-ordinate found'):
            s.get_latest_close_price()

    def test_get_latest_close_price_success(self, mocker):
        s = Stock('MA999', 'Test')
        coord = MagicMock()
        coord.last_value.return_value = 350.0
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)
        assert s.get_latest_close_price() == 350.0

    def test_get_close_price_for_date(self, mocker):
        import pandas as pd
        s = Stock('MA999', 'Test')
        coord = MagicMock()
        coord.dataset_id = 'DS1'
        expected = pd.Series([200.0])
        coord.get_series.return_value = expected
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)
        result = s.get_close_price_for_date(dt.date(2023, 6, 15))
        assert result is expected

    def test_get_close_prices(self, mocker):
        import pandas as pd
        s = Stock('MA999', 'Test')
        coord = MagicMock()
        coord.dataset_id = 'DS1'
        expected = pd.Series([100.0, 101.0, 102.0])
        coord.get_series.return_value = expected
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)
        result = s.get_close_prices(start=dt.date(2023, 1, 1), end=dt.date(2023, 1, 3))
        assert result is expected


# ---------------------------------------------------------------------------
#  Asset.get_hloc_prices  – Equity daily, monthly, unsupported frequency, FX, other
# ---------------------------------------------------------------------------


class TestGetHlocPrices:
    def test_hloc_equity_daily(self, mocker):
        import pandas as pd
        s = Stock('MA999', 'Test')
        s.asset_class = AssetClass.Equity

        series_high = pd.Series([110.0], index=[dt.date(2023, 1, 1)])
        series_low = pd.Series([90.0], index=[dt.date(2023, 1, 1)])
        series_open = pd.Series([95.0], index=[dt.date(2023, 1, 1)])
        series_close = pd.Series([105.0], index=[dt.date(2023, 1, 1)])

        mocker.patch(
            'gs_quant.markets.securities.ThreadPoolManager.run_async',
            return_value=[series_high, series_low, series_open, series_close]
        )

        coord = MagicMock()
        coord.dataset_id = 'DS1'
        coord.get_series.return_value = pd.Series([100.0])
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)

        result = s.get_hloc_prices(
            start=dt.date(2023, 1, 1),
            end=dt.date(2023, 1, 1),
            interval_frequency=IntervalFrequency.DAILY
        )
        assert isinstance(result, pd.DataFrame)
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'open' in result.columns
        assert 'close' in result.columns

    def test_hloc_equity_monthly(self, mocker):
        import pandas as pd
        s = Stock('MA999', 'Test')
        s.asset_class = AssetClass.Equity

        series_high = pd.Series([110.0], index=[dt.date(2023, 1, 31)])
        series_low = pd.Series([90.0], index=[dt.date(2023, 1, 31)])
        series_open = pd.Series([95.0], index=[dt.date(2023, 1, 31)])
        series_close = pd.Series([105.0], index=[dt.date(2023, 1, 31)])

        mocker.patch(
            'gs_quant.markets.securities.ThreadPoolManager.run_async',
            return_value=[series_high, series_low, series_open, series_close]
        )

        coord = MagicMock()
        coord.dataset_id = 'DS1'
        coord.get_series.return_value = pd.Series([100.0])
        mocker.patch.object(s, 'get_data_coordinate', return_value=coord)

        result = s.get_hloc_prices(
            start=dt.date(2023, 1, 1),
            end=dt.date(2023, 3, 31),
            interval_frequency=IntervalFrequency.MONTHLY
        )
        assert isinstance(result, pd.DataFrame)

    def test_hloc_equity_unsupported_frequency(self):
        s = Stock('MA999', 'Test')
        s.asset_class = AssetClass.Equity
        with pytest.raises(MqValueError, match='Unsupported IntervalFrequency'):
            s.get_hloc_prices(
                start=dt.date(2023, 1, 1),
                end=dt.date(2023, 1, 31),
                interval_frequency=IntervalFrequency.WEEKLY
            )

    def test_hloc_fx_daily(self, mocker):
        import pandas as pd
        from gs_quant.data import Dataset
        c = Cross('MAFX', 'USDJPY')
        c.asset_class = AssetClass.FX

        mock_df = pd.DataFrame({
            'high': [110.0],
            'low': [90.0],
            'open': [95.0],
            'close': [105.0],
            'assetId': ['MAFX'],
            'updateTime': ['2023-01-01'],
        })
        mock_ds = MagicMock()
        mock_ds.get_data.return_value = mock_df
        mocker.patch('gs_quant.markets.securities.Dataset', return_value=mock_ds)

        result = c.get_hloc_prices(start=dt.date(2023, 1, 1), end=dt.date(2023, 1, 1))
        assert isinstance(result, pd.DataFrame)

    def test_hloc_fx_non_daily_unsupported(self):
        c = Cross('MAFX', 'USDJPY')
        c.asset_class = AssetClass.FX
        with pytest.raises(MqValueError, match='Unsupported IntervalFrequency for FX'):
            c.get_hloc_prices(
                start=dt.date(2023, 1, 1),
                end=dt.date(2023, 1, 31),
                interval_frequency=IntervalFrequency.MONTHLY
            )

    def test_hloc_unsupported_asset_class(self):
        r = Rate('MA999', 'SOFR')
        r.asset_class = AssetClass.Rates
        with pytest.raises(MqValueError, match='Unsupported AssetClass'):
            r.get_hloc_prices(start=dt.date(2023, 1, 1), end=dt.date(2023, 1, 31))


# ---------------------------------------------------------------------------
#  SecurityMaster.__gs_asset_to_asset – all asset type branches
# ---------------------------------------------------------------------------


class TestGsAssetToAsset:
    """Test all branches of __gs_asset_to_asset via SecurityMaster.get_asset with MARQUEE_ID."""

    def _make_gs_asset(self, mocker, asset_class, asset_type, name='Test', id_='MA123', **kwargs):
        gs_asset = GsAsset(asset_class=asset_class, type_=asset_type, name=name, id_=id_, **kwargs)
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_asset', return_value=gs_asset)
        return SecurityMaster.get_asset(id_, AssetIdentifier.MARQUEE_ID)

    def test_single_stock(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Single_Stock)
        assert isinstance(asset, Stock)
        assert asset.get_type() == AssetType.STOCK

    def test_etf(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.ETF)
        assert isinstance(asset, ETF)
        assert asset.get_type() == AssetType.ETF

    def test_index(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Index)
        from gs_quant.markets.index import Index
        assert isinstance(asset, Index)

    def test_access(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Access)
        from gs_quant.markets.index import Index
        assert isinstance(asset, Index)

    def test_multi_asset_allocation(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Multi_Asset_Allocation)
        from gs_quant.markets.index import Index
        assert isinstance(asset, Index)

    def test_risk_premia(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Risk_Premia)
        from gs_quant.markets.index import Index
        assert isinstance(asset, Index)

    def test_systematic_hedging(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Systematic_Hedging)
        from gs_quant.markets.index import Index
        assert isinstance(asset, Index)

    def test_custom_basket(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Custom_Basket)
        from gs_quant.markets.baskets import Basket
        assert isinstance(asset, Basket)

    def test_research_basket(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Research_Basket)
        from gs_quant.markets.baskets import Basket
        assert isinstance(asset, Basket)

    def test_future(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.Future)
        assert isinstance(asset, Future)

    def test_cross(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.FX, GsAssetType.Cross)
        assert isinstance(asset, Cross)

    def test_currency(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Cash, GsAssetType.Currency)
        assert isinstance(asset, CurrencyAsset)

    def test_rate(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Rates, GsAssetType.Rate)
        assert isinstance(asset, Rate)

    def test_cash(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Cash, GsAssetType.Cash)
        assert isinstance(asset, Cash)

    def test_weather_index(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.WeatherIndex)
        assert isinstance(asset, WeatherIndex)

    def test_swap(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Rates, GsAssetType.Swap)
        assert isinstance(asset, Swap)

    def test_option(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Option)
        assert isinstance(asset, Option)

    def test_commodity_reference_price(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.CommodityReferencePrice)
        assert isinstance(asset, CommodityReferencePrice)

    def test_commodity_natural_gas_hub(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.CommodityNaturalGasHub)
        assert isinstance(asset, CommodityNaturalGasHub)

    def test_commodity_eu_natural_gas_hub(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.CommodityEUNaturalGasHub)
        assert isinstance(asset, CommodityEUNaturalGasHub)

    def test_commodity_power_node(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.CommodityPowerNode)
        assert isinstance(asset, CommodityPowerNode)

    def test_commodity_power_aggregated_nodes(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.CommodityPowerAggregatedNodes)
        assert isinstance(asset, CommodityPowerAggregatedNodes)

    def test_bond(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Credit, GsAssetType.Bond)
        assert isinstance(asset, Bond)

    def test_bond_no_asset_class(self, mocker):
        """Bond with no assetClass should default to Credit."""
        gs_asset = GsAsset(asset_class=None, type_=GsAssetType.Bond, name='Test', id_='MA123')
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_asset', return_value=gs_asset)
        asset = SecurityMaster.get_asset('MA123', AssetIdentifier.MARQUEE_ID)
        assert isinstance(asset, Bond)
        assert asset.asset_class == AssetClass.Credit

    def test_commodity(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.Commodity)
        assert isinstance(asset, Commodity)

    def test_future_market(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.FutureMarket)
        assert isinstance(asset, FutureMarket)

    def test_future_contract(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.FutureContract)
        assert isinstance(asset, FutureContract)

    def test_cryptocurrency(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Commod, GsAssetType.Cryptocurrency)
        assert isinstance(asset, Cryptocurrency)

    def test_forward(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.FX, GsAssetType.Forward)
        assert isinstance(asset, Forward)

    def test_fund(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Fund)
        assert isinstance(asset, Fund)

    def test_default_swap(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Credit, GsAssetType.Default_Swap)
        assert isinstance(asset, DefaultSwap)

    def test_swaption(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Rates, GsAssetType.Swaption)
        assert isinstance(asset, Swaption)

    def test_binary(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.FX, GsAssetType.Binary)
        assert isinstance(asset, Binary)

    def test_xccy_swap_mtm(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Rates, GsAssetType.XccySwapMTM)
        assert isinstance(asset, XccySwapMTM)

    def test_mutual_fund(self, mocker):
        asset = self._make_gs_asset(mocker, AssetClass.Equity, GsAssetType.Mutual_Fund)
        assert isinstance(asset, MutualFund)

    def test_unsupported_type(self, mocker):
        """Unknown asset type should raise TypeError."""
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Hedge_Fund, name='Test', id_='MA123')
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_asset', return_value=gs_asset)
        with pytest.raises(TypeError, match='unsupported asset type'):
            SecurityMaster.get_asset('MA123', AssetIdentifier.MARQUEE_ID)


# ---------------------------------------------------------------------------
#  SecurityMaster.get_asset – different code paths
# ---------------------------------------------------------------------------


class TestSecurityMasterGetAsset:
    def test_get_asset_with_sort_by_rank_false(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='Test', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])
        asset = SecurityMaster.get_asset('GS', AssetIdentifier.TICKER, sort_by_rank=False)
        assert asset is not None
        assert isinstance(asset, Stock)

    def test_get_asset_sort_by_rank_true(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        result_dict = {
            'id': 'MA123',
            'assetClass': 'Equity',
            'type': 'Single Stock',
            'name': 'Test',
        }
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[result_dict])
        asset = SecurityMaster.get_asset('GS', AssetIdentifier.TICKER, sort_by_rank=True)
        assert isinstance(asset, Stock)

    def test_get_asset_sort_by_rank_empty(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[])
        asset = SecurityMaster.get_asset('INVALID', AssetIdentifier.TICKER, sort_by_rank=True)
        assert asset is None

    def test_get_asset_sort_by_rank_false_empty(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[])
        asset = SecurityMaster.get_asset('INVALID', AssetIdentifier.TICKER, sort_by_rank=False)
        assert asset is None

    def test_get_asset_secmaster_with_non_security_id_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            with pytest.raises(MqTypeError, match='expected a security identifier'):
                SecurityMaster.get_asset('GS', AssetIdentifier.TICKER)
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_get_asset_secmaster_with_exchange_code_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            with pytest.raises(NotImplementedError):
                SecurityMaster.get_asset('GS', SecurityIdentifier.TICKER, exchange_code=ExchangeCode.NYSE)
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_get_asset_secmaster_with_asset_type_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            with pytest.raises(NotImplementedError):
                SecurityMaster.get_asset('GS', SecurityIdentifier.TICKER, asset_type=AssetType.STOCK)
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


# ---------------------------------------------------------------------------
#  SecurityMaster.get_asset_query
# ---------------------------------------------------------------------------


class TestGetAssetQuery:
    def test_get_asset_query_marquee_id(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        query, as_of = SecurityMaster.get_asset_query('MA123', AssetIdentifier.MARQUEE_ID)
        assert 'id' in query
        assert query['id'] == 'MA123'

    def test_get_asset_query_with_exchange_and_type(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        query, as_of = SecurityMaster.get_asset_query(
            'GS', AssetIdentifier.TICKER, exchange_code=ExchangeCode.NYSE, asset_type=AssetType.STOCK
        )
        assert query['exchange'] == 'NYSE'
        assert 'type' in query
        assert GsAssetType.Single_Stock.value in query['type']

    def test_get_asset_query_with_date(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        as_of_date = dt.date(2023, 6, 15)
        query, as_of = SecurityMaster.get_asset_query('GS', AssetIdentifier.TICKER, as_of=as_of_date)
        assert isinstance(as_of, dt.datetime)
        assert as_of.date() == as_of_date

    def test_get_asset_query_with_datetime(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        as_of_dt = dt.datetime(2023, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        query, as_of = SecurityMaster.get_asset_query('GS', AssetIdentifier.TICKER, as_of=as_of_dt)
        # isinstance(datetime, date) is True, so datetime is always combined to midnight UTC
        assert isinstance(as_of, dt.datetime)
        assert as_of.date() == dt.date(2023, 6, 15)

    def test_get_asset_query_pricing_context_entered(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        market = PricingContext(dt.date(2023, 6, 15))
        with market:
            query, as_of = SecurityMaster.get_asset_query('GS', AssetIdentifier.TICKER)
        assert as_of.date() == dt.date(2023, 6, 15)

    def test_get_asset_query_pricing_context_not_entered(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        # Not entering the context - the default PricingContext is used
        query, as_of = SecurityMaster.get_asset_query('GS', AssetIdentifier.TICKER)
        # Should use today's date as default
        assert isinstance(as_of, dt.datetime)


# ---------------------------------------------------------------------------
#  SecurityMaster.get_many_assets
# ---------------------------------------------------------------------------


class TestGetManyAssets:
    def test_get_many_assets_sort_by_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])
        result = SecurityMaster.get_many_assets(['GS'], AssetIdentifier.TICKER, sort_by_rank=True)
        assert len(result) == 1
        assert isinstance(result[0], Stock)

    def test_get_many_assets_no_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])
        result = SecurityMaster.get_many_assets(['GS'], AssetIdentifier.TICKER, sort_by_rank=False)
        assert len(result) == 1

    def test_get_many_assets_none_results(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=None)
        result = SecurityMaster.get_many_assets(['INVALID'], AssetIdentifier.TICKER)
        assert result == []

    def test_get_many_assets_with_tracer_span(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mocker.patch('gs_quant.markets.securities.Tracer.active_span', return_value=mock_span)
        mock_tracer_instance = MagicMock()
        mock_scope = MagicMock()
        mock_scope.span = MagicMock()
        mock_tracer_instance.__enter__ = MagicMock(return_value=mock_scope)
        mock_tracer_instance.__exit__ = MagicMock(return_value=False)
        mocker.patch('gs_quant.markets.securities.Tracer', return_value=mock_tracer_instance)

        result = SecurityMaster.get_many_assets(['GS'], AssetIdentifier.TICKER)
        assert len(result) == 1

    def test_get_many_assets_with_no_tracer(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])
        mocker.patch('gs_quant.markets.securities.Tracer.active_span', return_value=None)

        result = SecurityMaster.get_many_assets(['GS'], AssetIdentifier.TICKER)
        assert len(result) == 1

    def test_get_many_assets_with_non_recording_span(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[gs_asset])

        mock_span = MagicMock()
        mock_span.is_recording.return_value = False
        mocker.patch('gs_quant.markets.securities.Tracer.active_span', return_value=mock_span)

        result = SecurityMaster.get_many_assets(['GS'], AssetIdentifier.TICKER)
        assert len(result) == 1


# ---------------------------------------------------------------------------
#  SecurityMaster.get_asset_async
# ---------------------------------------------------------------------------


class TestGetAssetAsync:
    def test_get_asset_async_marquee_id(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='Test', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_asset_async', return_value=gs_asset)

        async def run():
            return await SecurityMaster.get_asset_async('MA123', AssetIdentifier.MARQUEE_ID)

        result = _run_async(run())
        assert isinstance(result, Stock)

    def test_get_asset_async_ticker_sort_by_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        result_dict = {
            'id': 'MA123', 'assetClass': 'Equity', 'type': 'Single Stock', 'name': 'Test'
        }
        mocker.patch.object(GsAssetApi, 'get_many_assets_async', return_value=[result_dict])

        async def run():
            return await SecurityMaster.get_asset_async('GS', AssetIdentifier.TICKER, sort_by_rank=True)

        result = _run_async(run())
        assert isinstance(result, Stock)

    def test_get_asset_async_ticker_no_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='Test', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets_async', return_value=[gs_asset])

        async def run():
            return await SecurityMaster.get_asset_async('GS', AssetIdentifier.TICKER, sort_by_rank=False)

        result = _run_async(run())
        assert isinstance(result, Stock)

    def test_get_asset_async_secmaster_non_security_identifier_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            async def run():
                return await SecurityMaster.get_asset_async('GS', AssetIdentifier.TICKER)

            with pytest.raises(MqTypeError, match='expected a security identifier'):
                _run_async(run())
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_get_asset_async_secmaster_with_exchange_code_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            async def run():
                return await SecurityMaster.get_asset_async(
                    'GS', SecurityIdentifier.TICKER, exchange_code=ExchangeCode.NYSE
                )

            with pytest.raises(NotImplementedError):
                _run_async(run())
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_get_asset_async_secmaster_success(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

        mock_response = {
            "results": [{
                "name": "Test Stock",
                "type": "Common Stock",
                "currency": "USD",
                "assetClass": "Equity",
                "identifiers": {"gsid": 901026, "assetId": "MA123"},
                "id": "GSPD901026E154",
            }],
            "totalResults": 1,
        }
        mocker.patch.object(GsSession.current.async_, 'get', return_value=mock_response)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            async def run():
                return await SecurityMaster.get_asset_async('GS', SecurityIdentifier.TICKER)

            result = _run_async(run())
            assert isinstance(result, SecMasterAsset)
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


# ---------------------------------------------------------------------------
#  SecurityMaster.get_many_assets_async
# ---------------------------------------------------------------------------


class TestGetManyAssetsAsync:
    def test_get_many_assets_async_sort_by_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets_async', return_value=[gs_asset])

        async def run():
            return await SecurityMaster.get_many_assets_async(['GS'], AssetIdentifier.TICKER, sort_by_rank=True)

        result = _run_async(run())
        assert len(result) == 1

    def test_get_many_assets_async_no_rank(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets_async', return_value=[gs_asset])

        async def run():
            return await SecurityMaster.get_many_assets_async(['GS'], AssetIdentifier.TICKER, sort_by_rank=False)

        result = _run_async(run())
        assert len(result) == 1

    def test_get_many_assets_async_with_tracer(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='GS', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_many_assets_async', return_value=[gs_asset])

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mocker.patch('gs_quant.markets.securities.Tracer.active_span', return_value=mock_span)
        mock_tracer_instance = MagicMock()
        mock_scope = MagicMock()
        mock_scope.span = MagicMock()
        mock_tracer_instance.__enter__ = MagicMock(return_value=mock_scope)
        mock_tracer_instance.__exit__ = MagicMock(return_value=False)
        mocker.patch('gs_quant.markets.securities.Tracer', return_value=mock_tracer_instance)

        async def run():
            return await SecurityMaster.get_many_assets_async(['GS'], AssetIdentifier.TICKER)

        result = _run_async(run())
        assert len(result) == 1


# ---------------------------------------------------------------------------
#  SecurityMaster._get_security_master_asset_params
# ---------------------------------------------------------------------------


class TestGetSecurityMasterAssetParams:
    def test_params_with_fields(self):
        params = SecurityMaster._get_security_master_asset_params(
            'GS', SecurityIdentifier.TICKER, as_of=dt.datetime(2023, 6, 15), fields=['name', 'id']
        )
        assert params['ticker'] == 'GS'
        assert params['asOfDate'] == '2023-06-15'
        assert 'fields' in params
        assert 'name' in params['fields']
        assert 'identifiers' in params['fields']  # always included

    def test_params_without_fields(self):
        params = SecurityMaster._get_security_master_asset_params(
            'GS', SecurityIdentifier.TICKER, as_of=dt.datetime(2023, 6, 15)
        )
        assert 'fields' not in params

    def test_params_without_as_of(self):
        params = SecurityMaster._get_security_master_asset_params('GS', SecurityIdentifier.TICKER)
        assert params['asOfDate'] == '2100-01-01'


# ---------------------------------------------------------------------------
#  SecurityMaster._get_security_master_asset_response
# ---------------------------------------------------------------------------


class TestGetSecurityMasterAssetResponse:
    def test_zero_results(self):
        response = {'totalResults': 0, 'results': []}
        result = SecurityMaster._get_security_master_asset_response(response)
        assert result is None

    def test_valid_response(self):
        response = {
            'totalResults': 1,
            'results': [{
                'name': 'Test',
                'type': 'Common Stock',
                'assetClass': 'Equity',
                'currency': 'USD',
                'identifiers': {'assetId': 'MA123', 'gsid': 100},
                'exchange': {'name': 'NYSE', 'identifiers': {}},
                'id': 'GSPD100',
            }],
        }
        result = SecurityMaster._get_security_master_asset_response(response)
        assert isinstance(result, SecMasterAsset)
        assert result.name == 'Test'
        assert result.get_type() == AssetType.COMMON_STOCK

    def test_valid_response_no_exchange(self):
        response = {
            'totalResults': 1,
            'results': [{
                'name': 'Test',
                'type': 'Common Stock',
                'assetClass': 'Equity',
                'currency': 'USD',
                'identifiers': {'assetId': 'MA123', 'gsid': 100},
                'id': 'GSPD100',
            }],
        }
        result = SecurityMaster._get_security_master_asset_response(response)
        assert isinstance(result, SecMasterAsset)
        assert result.exchange is None

    def test_invalid_asset_type_raises(self):
        response = {
            'totalResults': 1,
            'results': [{
                'name': 'Test',
                'type': 'UnknownType',
                'assetClass': 'Equity',
                'identifiers': {'assetId': 'MA123'},
                'id': 'GSPD100',
            }],
        }
        with pytest.raises(NotImplementedError, match='Not yet implemented'):
            SecurityMaster._get_security_master_asset_response(response)


# ---------------------------------------------------------------------------
#  SecurityMaster.get_identifiers (bulk fetch)
# ---------------------------------------------------------------------------


class TestSecurityMasterGetIdentifiers:
    def test_get_identifiers_not_secmaster_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)
        with pytest.raises(NotImplementedError, match='method not available'):
            SecurityMaster.get_identifiers(['GS UN'], SecurityIdentifier.BBID)

    def test_get_identifiers_empty_results(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value={'results': [], 'totalResults': 0})

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.get_identifiers(['INVALID'], SecurityIdentifier.BBID)
            assert result == {}
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


# ---------------------------------------------------------------------------
#  SecurityMaster.asset_type_to_str
# ---------------------------------------------------------------------------


class TestAssetTypeToStr:
    def test_stock(self):
        assert SecurityMaster.asset_type_to_str(AssetClass.Equity, AssetType.STOCK) == "Common Stock"

    def test_equity_index(self):
        assert SecurityMaster.asset_type_to_str(AssetClass.Equity, AssetType.INDEX) == "Equity Index"

    def test_index_non_equity(self):
        # When class is not Equity, INDEX returns the value directly
        assert SecurityMaster.asset_type_to_str(AssetClass.Commod, AssetType.INDEX) == "Index"

    def test_other_type(self):
        assert SecurityMaster.asset_type_to_str(AssetClass.Equity, AssetType.ETF) == "ETF"


# ---------------------------------------------------------------------------
#  SecurityMaster.get_all_identifiers_gen – branches
# ---------------------------------------------------------------------------


class TestGetAllIdentifiersGen:
    def test_not_secmaster_raises(self):
        SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)
        with pytest.raises(NotImplementedError, match='method not available'):
            gen = SecurityMaster.get_all_identifiers_gen()
            next(gen)

    def test_class_filter(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

        p1 = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "GSPD1",
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 1, "id": "GSPD1"},
                },
                {
                    "type": "Bond",
                    "id": "GSPD2",
                    "assetClass": "Credit",
                    "identifiers": {"gsid": 2, "id": "GSPD2"},
                },
            ],
            "totalResults": 2,
        }
        p2 = {"results": [], "totalResults": 0}

        mocker.patch('gs_quant.markets.securities._get_with_retries', side_effect=[p1, p2])

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            gen = SecurityMaster.get_all_identifiers_gen(
                class_=AssetClass.Equity, use_offset_key=False, sleep=0
            )
            page = next(gen)
            # Only the equity entry should be in results
            assert len(page) == 1
            assert 'GSPD1' in page
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_result_size_limit_warning(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

        p1 = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "GSPD1",
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 1, "id": "GSPD1"},
                },
            ],
            "totalResults": 1,
        }

        # Simulate enough pages to reach the 10000 offset limit
        SecurityMaster._page_size = 5000
        pages = [p1] * 3
        pages.append({"results": [], "totalResults": 0})

        mocker.patch('gs_quant.markets.securities._get_with_retries', side_effect=pages)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            gen = SecurityMaster.get_all_identifiers_gen(use_offset_key=False, sleep=0)
            page1 = next(gen)
            assert len(page1) == 1
            page2 = next(gen)
            assert len(page2) == 1
            # At offset=10000, should stop (warning + return)
            with pytest.raises(StopIteration):
                next(gen)
        finally:
            SecurityMaster._page_size = 1000
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_offset_key_pagination(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

        p1 = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "GSPD1",
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 1, "id": "GSPD1"},
                },
            ],
            "offsetKey": "key1",
            "totalResults": 1,
        }
        p2 = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "GSPD2",
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 2, "id": "GSPD2"},
                },
            ],
            "totalResults": 1,
            # no offsetKey -> stops
        }

        mocker.patch('gs_quant.markets.securities._get_with_retries', side_effect=[p1, p2])

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            gen = SecurityMaster.get_all_identifiers_gen(use_offset_key=True, sleep=0)
            page1 = next(gen)
            assert 'GSPD1' in page1
            page2 = next(gen)
            assert 'GSPD2' in page2
            with pytest.raises(StopIteration):
                next(gen)
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


# ---------------------------------------------------------------------------
#  SecurityMaster.map_identifiers – additional branches
# ---------------------------------------------------------------------------


class TestMapIdentifiersAdditional:
    def test_map_identifiers_string_input_raises(self):
        with pytest.raises(MqTypeError, match='expected an iterable'):
            SecurityMaster.map_identifiers(SecurityIdentifier.BBID, 'GS UN', [SecurityIdentifier.RIC])

    def test_map_identifiers_secmaster_as_of_with_start_end_raises(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            with pytest.raises(MqValueError, match='provide .* or as-of date'):
                SecurityMaster.map_identifiers(
                    SecurityIdentifier.BBID,
                    ['GS UN'],
                    [SecurityIdentifier.RIC],
                    as_of_date=dt.date(2023, 1, 1),
                    start_date=dt.date(2023, 1, 1),
                )
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_with_as_of_date(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "gsid",
                    "outputValue": 901026,
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.GSID],
                as_of_date=dt.date(2023, 1, 1),
            )
            assert '2023-01-01' in result
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_results_dict(self, mocker):
        """When results is a dict, it should be returned as-is."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": {"2023-01-01": {"GS UN": {"gsid": [901026]}}}
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.GSID],
            )
            assert result == {"2023-01-01": {"GS UN": {"gsid": [901026]}}}
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_ric_with_assetId(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "assetId": "MA123",
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC, SecurityIdentifier.ASSET_ID],
            )
            assert '2023-01-01' in result
            assert result['2023-01-01']['GS UN']['ric'] == ['GS.N']
            assert result['2023-01-01']['GS UN']['assetId'] == ['MA123']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_ric_without_assetId_field(self, mocker):
        """When ASSET_ID is requested but 'assetId' not in the row."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC, SecurityIdentifier.ASSET_ID],
            )
            assert '2023-01-01' in result
            assert result['2023-01-01']['GS UN']['ric'] == ['GS.N']
            assert 'assetId' not in result['2023-01-01']['GS UN']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_bbg_types(self, mocker):
        """Test bbg output with BBG, BBID, BCID requested."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "bbg",
                    "outputValue": "GS",
                    "exchange": "UN",
                    "compositeExchange": "US",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "901026",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.GSID,
                ['901026'],
                [SecurityIdentifier.BBG, SecurityIdentifier.BBID, SecurityIdentifier.BCID],
            )
            day = result['2023-01-01']['901026']
            assert day['bbg'] == ['GS']
            assert day['bbid'] == ['GS UN']
            assert day['bcid'] == ['GS US']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_bbg_no_exchange(self, mocker):
        """bbg output without exchange -> BBID should be just the value."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "bbg",
                    "outputValue": "SPX",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "100",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.GSID,
                ['100'],
                [SecurityIdentifier.BBID],
            )
            assert result['2023-01-01']['100']['bbid'] == ['SPX']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_bbg_no_composite_exchange(self, mocker):
        """bbg output without compositeExchange -> BCID should not be added."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "bbg",
                    "outputValue": "SPX",
                    "exchange": "UN",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "100",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.GSID,
                ['100'],
                [SecurityIdentifier.BBID, SecurityIdentifier.BCID],
            )
            assert result['2023-01-01']['100']['bbid'] == ['SPX UN']
            assert 'bcid' not in result['2023-01-01']['100']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_duplicate_ric_not_added(self, mocker):
        """Duplicate RIC values should not be added twice."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-02",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC],
            )
            # Both dates should have the same RIC, but not duplicated
            assert result['2023-01-01']['GS UN']['ric'] == ['GS.N']
            assert result['2023-01-02']['GS UN']['ric'] == ['GS.N']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_generic_output_type(self, mocker):
        """Test generic output type (not ric/bbg) e.g. cusip."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "cusip",
                    "outputValue": "38141G104",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.CUSIP],
            )
            assert result['2023-01-01']['GS UN']['cusip'] == ['38141G104']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_generic_type_duplicate_not_added(self, mocker):
        """Duplicate generic output values should not be added."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "cusip",
                    "outputValue": "38141G104",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-02",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.CUSIP],
            )
            # Two days, same value, not duplicated per day
            assert result['2023-01-01']['GS UN']['cusip'] == ['38141G104']
            assert result['2023-01-02']['GS UN']['cusip'] == ['38141G104']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_start_date_only(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {"results": []}
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC],
                start_date=dt.date(2023, 1, 1),
            )
            assert result == {}
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_map_identifiers_secmaster_end_date_only(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {"results": []}
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC],
                end_date=dt.date(2023, 12, 31),
            )
            assert result == {}
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


# ---------------------------------------------------------------------------
#  SecMasterAsset – get_identifier, get_identifiers, get_marquee_id
# ---------------------------------------------------------------------------


class TestSecMasterAssetDetails:
    def _make_secmaster_asset(self, asset_type=AssetType.COMMON_STOCK, identifiers=None):
        if identifiers is None:
            identifiers = {'gsid': 901026, 'assetId': 'MA123'}
        entity = {
            'id': 'GSPD901026',
            'identifiers': identifiers,
            'type': asset_type.value,
            'assetClass': 'Equity',
        }
        return SecMasterAsset(
            id_='MA123',
            asset_type=asset_type,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

    def test_get_type(self):
        asset = self._make_secmaster_asset()
        assert asset.get_type() == AssetType.COMMON_STOCK

    def test_get_identifier_non_security_identifier_raises(self):
        asset = self._make_secmaster_asset()
        with pytest.raises(MqTypeError, match='Expected id_type: SecurityIdentifier'):
            asset.get_identifier(AssetIdentifier.BLOOMBERG_ID)

    def test_get_identifier_gsid(self):
        asset = self._make_secmaster_asset()
        assert asset.get_identifier(SecurityIdentifier.GSID) == 901026

    def test_get_identifier_id(self):
        asset = self._make_secmaster_asset()
        assert asset.get_identifier(SecurityIdentifier.ID) == 'GSPD901026'

    def test_get_identifier_from_history(self, mocker):
        asset = self._make_secmaster_asset()

        mock_response = {
            "results": [
                {
                    "startDate": "2007-01-01",
                    "endDate": "9999-99-99",
                    "value": "GS.N",
                    "updateTime": "2002-10-30T21:30:29.993Z",
                    "type": "ric",
                },
            ]
        }
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)
        assert asset.get_identifier(SecurityIdentifier.RIC) == 'GS.N'

    def test_get_identifiers_currency_asset_fallback(self, mocker):
        """Currency asset should include assetId from entity when not in history."""
        entity = {
            'id': 'GSPD4007',
            'identifiers': {'gsid': 4007, 'assetId': 'MAZ7RWC904JYHYPS', 'ticker': 'USD'},
            'type': 'Currency',
            'assetClass': 'Cash',
        }
        asset = SecMasterAsset(
            id_='MAZ7RWC904JYHYPS',
            asset_type=AssetType.CURRENCY,
            asset_class=AssetClass.Cash,
            name='USD',
            entity=entity,
        )

        # Return empty identifier history (no assetId in history)
        mock_response = {
            "results": [
                {
                    "startDate": "2007-01-01",
                    "endDate": "9999-99-99",
                    "value": "USD",
                    "updateTime": "2003-05-01T16:20:44.47Z",
                    "type": "ticker",
                },
            ]
        }
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        ids = asset.get_identifiers()
        # Currency fallback: assetId should come from entity['identifiers']
        assert ids[SecurityIdentifier.ASSET_ID.value] == 'MAZ7RWC904JYHYPS'

    def test_get_marquee_id_none_raises(self, mocker):
        entity = {
            'id': 'GSPD901026',
            'identifiers': {'gsid': 901026},  # no assetId
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_=None,
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

        # Return empty identifier history (no assetId)
        mock_response = {"results": []}
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        with pytest.raises(MqValueError, match='does not have a Marquee Id'):
            asset.get_marquee_id()

    def test_get_marquee_id_none_entered_context(self, mocker):
        """get_marquee_id when None and PricingContext is entered."""
        entity = {
            'id': 'GSPD901026',
            'identifiers': {'gsid': 901026},
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_=None,
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

        mock_response = {"results": []}
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        market = PricingContext(dt.date(2023, 1, 1))
        with market:
            with pytest.raises(MqValueError, match='does not have a Marquee Id'):
                asset.get_marquee_id()


# ---------------------------------------------------------------------------
#  SecMasterAsset.__load_identifiers – end date "9999-99-99" vs normal
# ---------------------------------------------------------------------------


class TestSecMasterAssetLoadIdentifiers:
    def test_load_identifiers_normal_end_date(self, mocker):
        entity = {
            'id': 'GSPD1',
            'identifiers': {'gsid': 1, 'assetId': 'MA1'},
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_='MA1',
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

        mock_response = {
            "results": [
                {
                    "startDate": "2007-01-01",
                    "endDate": "2022-12-31",  # normal end date (not 9999-99-99)
                    "value": "OLD_RIC",
                    "updateTime": "2002-10-30T21:30:29.993Z",
                    "type": "ric",
                },
                {
                    "startDate": "2023-01-01",
                    "endDate": "9999-99-99",
                    "value": "NEW_RIC",
                    "updateTime": "2023-01-01T00:00:00.000Z",
                    "type": "ric",
                },
                {
                    "startDate": "2007-01-01",
                    "endDate": "9999-99-99",
                    "value": "MA1",
                    "updateTime": "2002-10-30T21:30:29.993Z",
                    "type": "assetId",
                },
            ]
        }
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        # Query for a date within the first xref's range
        ids = asset.get_identifiers(as_of=dt.date(2020, 1, 1))
        assert ids.get('ric') == 'OLD_RIC'

        # Query for a date within the second xref's range
        ids2 = asset.get_identifiers(as_of=dt.date(2023, 6, 15))
        assert ids2.get('ric') == 'NEW_RIC'


# ---------------------------------------------------------------------------
#  SecMasterAsset.__is_validate_range – branches
# ---------------------------------------------------------------------------


class TestSecMasterAssetValidateRange:
    def _make_asset_with_identifiers(self, mocker, xrefs):
        entity = {
            'id': 'GSPD1',
            'identifiers': {'gsid': 1, 'assetId': 'MA1'},
            'type': 'ETF',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_='MA1',
            asset_type=AssetType.ETF,
            asset_class=AssetClass.Equity,
            name='Test ETF',
            entity=entity,
        )
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value={"results": xrefs})
        return asset

    def test_validate_range_no_marquee_ids(self, mocker):
        """No assetId in range should raise MqValueError."""
        xrefs = [
            {
                "startDate": "2023-01-01",
                "endDate": "2023-06-30",
                "value": "MA1",
                "updateTime": "2023-01-01T00:00:00.000Z",
                "type": "assetId",
            },
        ]
        asset = self._make_asset_with_identifiers(mocker, xrefs)
        # Request range outside the asset's range - get_marquee_id will fail first
        with pytest.raises(MqValueError):
            asset.get_hloc_prices(start=dt.date(2024, 1, 1), end=dt.date(2024, 12, 31))

    def test_validate_range_different_marquee_ids_at_endpoints(self, mocker):
        """Different Marquee IDs at start vs end should raise."""
        xrefs = [
            {
                "startDate": "2020-01-01",
                "endDate": "2022-12-31",
                "value": "MA1",
                "updateTime": "2020-01-01T00:00:00.000Z",
                "type": "assetId",
            },
            {
                "startDate": "2023-01-01",
                "endDate": "9999-99-99",
                "value": "MA2",
                "updateTime": "2023-01-01T00:00:00.000Z",
                "type": "assetId",
            },
        ]
        asset = self._make_asset_with_identifiers(mocker, xrefs)
        with pytest.raises(MqValueError, match="Marquee Id is either none or different"):
            asset.get_hloc_prices(start=dt.date(2022, 6, 1), end=dt.date(2023, 6, 1))


# ---------------------------------------------------------------------------
#  Asset.get classmethod
# ---------------------------------------------------------------------------


class TestAssetGetClassmethod:
    def test_asset_get(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        gs_asset = GsAsset(asset_class=AssetClass.Equity, type_=GsAssetType.Single_Stock, name='Test', id_='MA123')
        mocker.patch.object(GsAssetApi, 'get_asset', return_value=gs_asset)
        asset = Stock.get('MA123', AssetIdentifier.MARQUEE_ID)
        assert isinstance(asset, Stock)

    def test_asset_get_with_params(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        result_dict = {'id': 'MA123', 'assetClass': 'Equity', 'type': 'Single Stock', 'name': 'Test'}
        mocker.patch.object(GsAssetApi, 'get_many_assets', return_value=[result_dict])
        asset = Stock.get('GS', AssetIdentifier.TICKER, asset_type=AssetType.STOCK, sort_by_rank=True)
        assert isinstance(asset, Stock)


# ---------------------------------------------------------------------------
#  SecurityMaster.__asset_type_to_gs_types
# ---------------------------------------------------------------------------


class TestAssetTypeToGsTypes:
    def test_known_types(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        # Test via get_asset_query which calls __asset_type_to_gs_types
        query, _ = SecurityMaster.get_asset_query('SPX', AssetIdentifier.TICKER, asset_type=AssetType.INDEX)
        assert GsAssetType.Index.value in query['type']
        assert GsAssetType.Multi_Asset_Allocation.value in query['type']
        assert GsAssetType.Risk_Premia.value in query['type']
        assert GsAssetType.Access.value in query['type']

    def test_etf_type(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        query, _ = SecurityMaster.get_asset_query('SPY', AssetIdentifier.TICKER, asset_type=AssetType.ETF)
        assert GsAssetType.ETF.value in query['type']

    def test_rate_type(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        query, _ = SecurityMaster.get_asset_query('SOFR', AssetIdentifier.TICKER, asset_type=AssetType.RATE)
        assert GsAssetType.Rate.value in query['type']

    def test_unmapped_type_raises(self, mocker):
        """Types not in the __asset_type_to_gs_types map raise TypeError when iterated."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        # BOND is not in the __asset_type_to_gs_types map, which returns None
        # and then the list comprehension [t.value for t in None] raises TypeError
        with pytest.raises(TypeError):
            SecurityMaster.get_asset_query('BOND', AssetIdentifier.TICKER, asset_type=AssetType.BOND)


# ---------------------------------------------------------------------------
#  _get_with_retries
# ---------------------------------------------------------------------------


class TestGetWithRetries:
    def test_get_with_retries_success(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        expected = {'results': [], 'totalResults': 0}
        mocker.patch.object(GsSession.current.sync, 'get', return_value=expected)
        result = _get_with_retries('/markets/securities', {'ticker': 'GS'})
        assert result == expected


# ---------------------------------------------------------------------------
#  SecurityMaster.set_source
# ---------------------------------------------------------------------------


class TestSetSource:
    def test_set_and_get_source(self):
        original = SecurityMaster._source
        try:
            SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
            assert SecurityMaster._source == SecurityMasterSource.SECURITY_MASTER
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)
            assert SecurityMaster._source == SecurityMasterSource.ASSET_SERVICE
        finally:
            SecurityMaster._source = original


# ---------------------------------------------------------------------------
#  map_identifiers via Asset Service with unsupported type
# ---------------------------------------------------------------------------


class TestMapIdentifiersAssetServiceUnsupportedType:
    def test_unsupported_output_type(self):
        with pytest.raises(MqValueError, match='unsupported type'):
            with AssetContext():
                SecurityMaster.map_identifiers(
                    SecurityIdentifier.BBID,
                    ['GS UN'],
                    [SecurityIdentifier.BBG],
                )

    def test_unsupported_input_type(self):
        with pytest.raises(MqValueError, match='unsupported type'):
            with AssetContext():
                SecurityMaster.map_identifiers(
                    SecurityIdentifier.BBG,
                    ['GS'],
                    [SecurityIdentifier.RIC],
                )


# ---------------------------------------------------------------------------
#  Stock.get_thematic_beta
# ---------------------------------------------------------------------------


class TestStockThematicBeta:
    def test_get_thematic_beta_basket_not_found(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mocker.patch.object(GsAssetApi, 'resolve_assets', return_value={'BASKET1': []})
        with pytest.raises(MqValueError, match='could not be found'):
            s.get_thematic_beta('BASKET1')

    def test_get_thematic_beta_not_basket_type(self, mocker):
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mocker.patch.object(GsAssetApi, 'resolve_assets', return_value={
            'BASKET1': [{'id': 'MA123', 'type': 'Single Stock'}]
        })
        with pytest.raises(MqValueError, match='is not a Custom or Research Basket'):
            s.get_thematic_beta('BASKET1')

    def test_get_thematic_beta_success(self, mocker):
        import pandas as pd
        from gs_quant.api.gs.data import GsDataApi
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        s = Stock('MA999', 'Test')
        mocker.patch.object(GsAssetApi, 'resolve_assets', return_value={
            'BASKET1': [{'id': 'MA123', 'type': 'Custom Basket'}]
        })
        mocker.patch.object(s, 'get_identifier', return_value='901026')
        mocker.patch.object(GsDataApi, 'query_data', return_value=[
            {'date': '2023-01-01', 'gsid': '901026', 'basketId': 'MA123', 'beta': 0.85},
        ])
        result = s.get_thematic_beta('BASKET1')
        assert isinstance(result, pd.DataFrame)
        assert 'thematicBeta' in result.columns


# ---------------------------------------------------------------------------
#  SecMasterAsset.get_data_series
# ---------------------------------------------------------------------------


class TestSecMasterAssetDataSeries:
    def test_get_data_series_no_coordinate(self, mocker):
        entity = {
            'id': 'GSPD1',
            'identifiers': {'gsid': 1, 'assetId': 'MA1'},
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_='MA1',
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )
        mocker.patch.object(asset, 'get_data_coordinate', return_value=None)
        with pytest.raises(MqValueError, match='No data coordinate found'):
            asset.get_data_series(DataMeasure.CLOSE_PRICE)


# ---------------------------------------------------------------------------
#  SecMasterAsset.__is_validate_range with datetime inputs
# ---------------------------------------------------------------------------


class TestSecMasterAssetValidateRangeDate:
    def test_validate_range_with_date_start_end(self, mocker):
        entity = {
            'id': 'GSPD1',
            'identifiers': {'gsid': 1, 'assetId': 'MA1'},
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_='MA1',
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

        mock_response = {
            "results": [
                {
                    "startDate": "2020-01-01",
                    "endDate": "9999-99-99",
                    "value": "MA1",
                    "updateTime": "2020-01-01T00:00:00.000Z",
                    "type": "assetId",
                },
            ]
        }
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        coord = MagicMock()
        coord.dataset_id = 'DS1'
        coord.get_range.return_value = (dt.date(2023, 1, 1), dt.date(2023, 6, 30))
        coord.get_series.return_value = MagicMock()
        mocker.patch.object(asset, 'get_data_coordinate', return_value=coord)

        # Parent get_data_series will be called - mock it on the Asset class
        import pandas as pd
        mocker.patch.object(
            Asset, 'get_data_series',
            return_value=pd.Series([100.0])
        )

        # Should not raise - uses date objects (not datetime)
        result = asset.get_data_series(DataMeasure.CLOSE_PRICE, start=dt.date(2023, 1, 1), end=dt.date(2023, 6, 30))


# ---------------------------------------------------------------------------
#  SecMasterAsset.get_identifiers – caching
# ---------------------------------------------------------------------------


class TestSecMasterAssetIdentifiersCaching:
    def test_identifiers_cached(self, mocker):
        entity = {
            'id': 'GSPD1',
            'identifiers': {'gsid': 1, 'assetId': 'MA1'},
            'type': 'Common Stock',
            'assetClass': 'Equity',
        }
        asset = SecMasterAsset(
            id_='MA1',
            asset_type=AssetType.COMMON_STOCK,
            asset_class=AssetClass.Equity,
            name='Test',
            entity=entity,
        )

        mock_response = {
            "results": [
                {
                    "startDate": "2007-01-01",
                    "endDate": "9999-99-99",
                    "value": "MA1",
                    "updateTime": "2002-10-30T21:30:29.993Z",
                    "type": "assetId",
                },
            ]
        }
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock_get = mocker.patch.object(GsSession.current.sync, 'get', return_value=mock_response)

        # First call loads identifiers
        asset.get_identifiers(as_of=dt.date(2023, 1, 1))
        # Second call should use cache
        asset.get_identifiers(as_of=dt.date(2023, 6, 15))
        # Only one API call should have been made
        assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
#  Additional edge-case tests targeting remaining uncovered branches
# ---------------------------------------------------------------------------


class TestGetIdentifiersDatetimeConversion:
    def test_get_identifiers_pricing_date_is_datetime(self, mocker):
        """Exercise line 334: when pricing_date is a datetime, convert to date."""
        s = Stock('MA999', 'Test')

        mock_xref = MagicMock()
        mock_xref.startDate = dt.date(2020, 1, 1)
        mock_xref.endDate = dt.date(2099, 12, 31)
        mock_identifiers = MagicMock()
        mock_identifiers.as_dict.return_value = {'ticker': 'GS'}
        mock_xref.identifiers = mock_identifiers

        mocker.patch.object(GsAssetApi, 'get_asset_xrefs', return_value=[mock_xref])

        # Create a PricingContext and then monkey-patch it to return datetime
        market = PricingContext(dt.date(2023, 6, 15))
        with market:
            # Override the private attribute to make pricing_date return datetime
            market._PricingContext__pricing_date = dt.datetime(2023, 6, 15, 12, 0, 0)
            ids = s.get_identifiers()
        assert ids.get('TICKER') == 'GS'


class TestGetAllIdentifiersGenDuplicateKey:
    def test_duplicate_key_logging(self, mocker):
        """Exercise line 1987: when key is in box (duplicate key)."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))

        # The key will be the id value 'GSPD1', and 'id' is already set
        # We need box[id_type.value] to be a key that's already in box
        # Using id_type=SecurityIdentifier.ID (value='id'), box={'id': 'GSPD1', 'gsid': 1}
        # key = box['id'] = 'GSPD1', then `if key in box` checks if 'GSPD1' is a key in box
        # Since 'GSPD1' is a value not a key, it won't match. Let me use a scenario where it does.
        # If id_type=SecurityIdentifier.GSID, key = box['gsid'] = 1, and `if 1 in box`
        # dicts check keys, so `1 in {'gsid': 1}` is False.
        # The condition `if key in box` means the key VALUE is also a KEY in box.
        # This happens when e.g. gsid value is 'gsid' itself, or identifiers contain a self-referential key.
        p1 = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "GSPD1",
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 1, "id": "GSPD1", "special": "id"},
                },
            ],
            "totalResults": 1,
        }
        p2 = {"results": [], "totalResults": 0}

        # Use id_type where key value is also in box. e.g. id_type=ID -> key = box['id']
        # box = {'gsid': 1, 'id': 'GSPD1', 'special': 'id'}, after `box['id'] = e['id']`
        # key = 'GSPD1'. Is 'GSPD1' in box? No (it's a value, not a key).
        # We need a scenario where the key value matches a key name.
        # If box = {'id': 'id', ...} then key='id' and 'id' in box is True.
        p1_dup = {
            "results": [
                {
                    "type": "Common Stock",
                    "id": "id",  # top-level id is "id"
                    "assetClass": "Equity",
                    "identifiers": {"gsid": 1},
                },
            ],
            "totalResults": 1,
        }

        mocker.patch('gs_quant.markets.securities._get_with_retries', side_effect=[p1_dup, p2])

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            gen = SecurityMaster.get_all_identifiers_gen(
                id_type=SecurityIdentifier.ID, use_offset_key=False, sleep=0
            )
            page = next(gen)
            # After box['id'] = e['id'] = "id", key = box['id'] = "id"
            # "id" in box is True (because 'id' is a key in box)
            # This triggers the duplicate key debug log
            assert len(page) == 1
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersRicDuplicateAssetId:
    def test_ric_duplicate_asset_id_not_added_again(self, mocker):
        """When assetId is already in the values list, don't add again."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "assetId": "MA123",
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-02",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.ASSET_ID],
            )
            # Two days of output, but assetId should be the same and not duplicated per day
            assert result['2023-01-01']['GS UN']['assetId'] == ['MA123']
            assert result['2023-01-02']['GS UN']['assetId'] == ['MA123']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersBbgExistingKey:
    def test_bbg_appends_to_existing_bbid_list(self, mocker):
        """Test BBG with multiple results for same input to exercise existing key check."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "bbg",
                    "outputValue": "GS",
                    "exchange": "UN",
                    "compositeExchange": "US",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "38141G104",
                },
                {
                    "outputType": "bbg",
                    "outputValue": "GOS",
                    "exchange": "TH",
                    "compositeExchange": "AU",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "38141G104",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.CUSIP,
                ['38141G104'],
                [SecurityIdentifier.BBG, SecurityIdentifier.BBID, SecurityIdentifier.BCID],
            )
            day = result['2023-01-01']['38141G104']
            assert day['bbg'] == ['GS', 'GOS']
            assert day['bbid'] == ['GS UN', 'GOS TH']
            assert day['bcid'] == ['GS US', 'GOS AU']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersGenericDuplicate:
    def test_generic_type_not_in_output_types(self, mocker):
        """When generic type is not in output_types, value should not be added."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "cusip",
                    "outputValue": "38141G104",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            # Request ISIN but get cusip - cusip should not appear
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.ISIN],
            )
            if '2023-01-01' in result and 'GS UN' in result.get('2023-01-01', {}):
                assert 'cusip' not in result['2023-01-01']['GS UN']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersRicNotInOutputTypes:
    def test_ric_output_not_in_output_types(self, mocker):
        """When RIC output is received but RIC not in requested output_types."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "assetId": "MA123",
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            # Only request GSID, not RIC or ASSET_ID - ric output should be ignored
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.GSID],
            )
            # No gsid output in mock, so result may be empty for that input
            if '2023-01-01' in result and 'GS UN' in result.get('2023-01-01', {}):
                assert 'ric' not in result['2023-01-01']['GS UN']
                assert 'assetId' not in result['2023-01-01']['GS UN']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersBbgNotInOutputTypes:
    def test_bbg_output_not_in_output_types(self, mocker):
        """When bbg output is received but BBG/BBID/BCID not in requested output_types."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "bbg",
                    "outputValue": "GS",
                    "exchange": "UN",
                    "compositeExchange": "US",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            # Only request GSID, not BBG/BBID/BCID
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.GSID],
            )
            if '2023-01-01' in result and 'GS UN' in result.get('2023-01-01', {}):
                assert 'bbg' not in result['2023-01-01']['GS UN']
                assert 'bbid' not in result['2023-01-01']['GS UN']
                assert 'bcid' not in result['2023-01-01']['GS UN']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


class TestMapIdentifiersDeduplication:
    """Cover branches where duplicate values are skipped within the same date."""

    def test_ric_duplicate_same_day(self, mocker):
        """Two ric rows with same value on same day -> second should not be added."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "assetId": "MA123",
                    "outputType": "ric",
                    "outputValue": "GS.N",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
                {
                    "assetId": "MA123",
                    "outputType": "ric",
                    "outputValue": "GS.N",  # duplicate RIC value
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.RIC, SecurityIdentifier.ASSET_ID],
            )
            # Should have only one entry for ric and assetId
            assert result['2023-01-01']['GS UN']['ric'] == ['GS.N']
            assert result['2023-01-01']['GS UN']['assetId'] == ['MA123']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)

    def test_generic_type_duplicate_same_day(self, mocker):
        """Two generic type rows with same value on same day -> second should not be added."""
        mocker.patch.object(GsSession, 'default_value',
                            return_value=GsSession.get(Environment.QA, 'client_id', 'secret'))
        mock = {
            "results": [
                {
                    "outputType": "cusip",
                    "outputValue": "38141G104",
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
                {
                    "outputType": "cusip",
                    "outputValue": "38141G104",  # duplicate
                    "startDate": "2023-01-01",
                    "endDate": "2023-01-01",
                    "input": "GS UN",
                },
            ]
        }
        mocker.patch('gs_quant.markets.securities._get_with_retries', return_value=mock)

        SecurityMaster.set_source(SecurityMasterSource.SECURITY_MASTER)
        try:
            result = SecurityMaster.map_identifiers(
                SecurityIdentifier.BBID,
                ['GS UN'],
                [SecurityIdentifier.CUSIP],
            )
            assert result['2023-01-01']['GS UN']['cusip'] == ['38141G104']
        finally:
            SecurityMaster.set_source(SecurityMasterSource.ASSET_SERVICE)


if __name__ == "__main__":
    pytest.main([__file__])
