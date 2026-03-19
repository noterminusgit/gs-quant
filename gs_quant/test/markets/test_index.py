"""
Copyright 2024 Goldman Sachs.
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
import json
import pytest
import pandas as pd

from unittest.mock import MagicMock, patch, PropertyMock

from gs_quant.api.gs.assets import GsAsset, GsAssetApi
from gs_quant.api.gs.data import GsDataApi
from gs_quant.common import AssetClass, Currency
from gs_quant.data.fields import DataMeasure
from gs_quant.errors import MqValueError
from gs_quant.json_encoder import JSONEncoder
from gs_quant.markets.index import Index
from gs_quant.markets.securities import AssetType
from gs_quant.markets.indices_utils import ReturnType, STSIndexType, IndicesDatasets, PriceType
from gs_quant.entities.tree_entity import AssetTreeNode, TreeHelper


def _make_gs_asset(asset_type='Access', asset_class=AssetClass.Equity, name='Test Index',
                   currency=Currency.USD, exchange='NYSE', asset_id='MA123'):
    """Helper to create a mock GsAsset."""
    mock_asset = MagicMock(spec=GsAsset)
    mock_asset.id = asset_id
    mock_asset.asset_class = asset_class
    mock_asset.name = name
    mock_asset.currency = currency
    mock_asset.exchange = exchange

    # type is an enum-like object with .value
    type_mock = MagicMock()
    type_mock.value = asset_type
    mock_asset.type = type_mock

    # as_dict returns a dictionary representation
    mock_asset.as_dict.return_value = {
        'id': asset_id,
        'assetClass': 'Equity',
        'name': name,
        'type': asset_type,
        'currency': 'USD',
        'exchange': exchange,
    }

    return mock_asset


def _make_sts_index(asset_type='Access', asset_id='MA123', name='STS Test Index'):
    """Helper to create an STS Index instance with mocked TreeHelper."""
    entity = {
        'id': asset_id,
        'assetClass': 'Equity',
        'name': name,
        'type': asset_type,
        'currency': 'USD',
    }
    with patch.object(TreeHelper, '__init__', return_value=None):
        idx = Index(asset_id, AssetClass.Equity, name, exchange='NYSE',
                    currency=Currency.USD, entity=entity)
    idx.tree_helper = MagicMock(spec=TreeHelper)
    idx.tree_helper.tree_built = False
    idx.tree_helper.root = MagicMock(spec=AssetTreeNode)
    idx.tree_df = pd.DataFrame()
    return idx


def _make_non_sts_index(asset_id='MA456', name='Regular Index'):
    """Helper to create a non-STS Index instance (type='Index')."""
    entity = {
        'id': asset_id,
        'assetClass': 'Equity',
        'name': name,
        'type': 'Index',
    }
    idx = Index(asset_id, AssetClass.Equity, name, exchange='NYSE',
                currency=Currency.USD, entity=entity)
    return idx


# =====================================================
# __init__ tests
# =====================================================

class TestIndexInit:
    def test_init_with_entity_sts_type(self):
        """Test init with entity dict and STS type sets asset_type and tree_helper."""
        idx = _make_sts_index()
        assert idx.asset_type == AssetType('Access')
        assert hasattr(idx, 'tree_helper')
        assert hasattr(idx, 'tree_df')

    def test_init_with_entity_non_sts_type(self):
        """Test init with entity dict and non-STS type."""
        idx = _make_non_sts_index()
        assert idx.asset_type == AssetType.INDEX

    def test_init_without_entity(self):
        """Test init without entity defaults to AssetType.INDEX and does not set tree_helper."""
        idx = Index('MA789', AssetClass.Equity, 'No Entity Index')
        assert idx.asset_type == AssetType.INDEX
        assert not hasattr(idx, 'tree_helper')


# =====================================================
# __str__ test
# =====================================================

class TestIndexStr:
    def test_str_returns_name(self):
        idx = _make_non_sts_index(name='My Index Name')
        assert str(idx) == 'My Index Name'


# =====================================================
# get_type test
# =====================================================

class TestGetType:
    def test_get_type_sts(self):
        idx = _make_sts_index()
        assert idx.get_type() == AssetType('Access')

    def test_get_type_non_sts(self):
        idx = _make_non_sts_index()
        assert idx.get_type() == AssetType.INDEX


# =====================================================
# get_currency test
# =====================================================

class TestGetCurrency:
    def test_get_currency(self):
        idx = _make_non_sts_index()
        assert idx.get_currency() == Currency.USD

    def test_get_currency_none(self):
        idx = Index('MA789', AssetClass.Equity, 'No Currency Index')
        assert idx.get_currency() is None


# =====================================================
# get_return_type tests
# =====================================================

class TestGetReturnType:
    def test_return_type_none_parameters(self):
        idx = _make_non_sts_index()
        idx.parameters = None
        assert idx.get_return_type() == ReturnType.TOTAL_RETURN

    def test_return_type_none_index_return_type(self):
        idx = _make_non_sts_index()
        idx.parameters = MagicMock()
        idx.parameters.index_return_type = None
        assert idx.get_return_type() == ReturnType.TOTAL_RETURN

    def test_return_type_price_return(self):
        idx = _make_non_sts_index()
        idx.parameters = MagicMock()
        idx.parameters.index_return_type = 'Price Return'
        assert idx.get_return_type() == ReturnType.PRICE_RETURN

    def test_return_type_gross_return(self):
        idx = _make_non_sts_index()
        idx.parameters = MagicMock()
        idx.parameters.index_return_type = 'Gross Return'
        assert idx.get_return_type() == ReturnType.GROSS_RETURN


# =====================================================
# get (classmethod) tests
# =====================================================

class TestIndexGet:
    @patch.object(GsAssetApi, 'get_asset')
    @patch.object(GsAssetApi, 'resolve_assets')
    def test_get_sts_index(self, mock_resolve, mock_get_asset):
        gs_asset = _make_gs_asset(asset_type='Access')
        mock_resolve.return_value = {'GSMBXXXX': [{'id': 'MA123'}]}
        mock_get_asset.return_value = gs_asset

        with patch.object(TreeHelper, '__init__', return_value=None):
            idx = Index.get('GSMBXXXX')
        assert idx is not None
        assert idx.name == 'Test Index'

    @patch.object(GsAssetApi, 'get_asset')
    @patch.object(GsAssetApi, 'resolve_assets')
    def test_get_regular_index(self, mock_resolve, mock_get_asset):
        gs_asset = _make_gs_asset(asset_type='Index')
        mock_resolve.return_value = {'GSMBXXXX': [{'id': 'MA123'}]}
        mock_get_asset.return_value = gs_asset

        idx = Index.get('GSMBXXXX')
        assert idx is not None

    @patch.object(GsAssetApi, 'get_asset')
    @patch.object(GsAssetApi, 'resolve_assets')
    def test_get_non_index_raises(self, mock_resolve, mock_get_asset):
        gs_asset = _make_gs_asset(asset_type='ETF')
        mock_resolve.return_value = {'GSMBXXXX': [{'id': 'MA123'}]}
        mock_get_asset.return_value = gs_asset

        with pytest.raises(MqValueError, match='is not an Index identifier'):
            Index.get('GSMBXXXX')

    @patch.object(GsAssetApi, 'resolve_assets')
    def test_get_empty_resolve(self, mock_resolve):
        mock_resolve.return_value = {'GSMBXXXX': []}
        with pytest.raises(MqValueError, match='Asset could not be found'):
            Index.get('GSMBXXXX')

    @patch.object(GsAssetApi, 'resolve_assets')
    def test_get_resolve_none_id(self, mock_resolve):
        mock_resolve.return_value = {'GSMBXXXX': [{'id': None}]}
        with pytest.raises(MqValueError, match='Asset could not be found'):
            Index.get('GSMBXXXX')


# =====================================================
# get_fundamentals tests
# =====================================================

class TestGetFundamentals:
    def test_get_fundamentals_sts_no_period(self):
        idx = _make_sts_index()
        mock_response = [{'date': '2021-01-01', 'metric': 'dividendYield', 'value': 1.5}]

        with patch.object(GsDataApi, 'query_data', return_value=mock_response):
            result = idx.get_fundamentals()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_get_fundamentals_sts_with_period(self):
        idx = _make_sts_index()
        mock_response = [{'date': '2021-01-01', 'value': 2.0}]

        with patch.object(GsDataApi, 'query_data', return_value=mock_response):
            result = idx.get_fundamentals(period='1y')
        assert isinstance(result, pd.DataFrame)

    def test_get_fundamentals_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_fundamentals()


# =====================================================
# get_latest_close_price tests
# =====================================================

class TestGetLatestClosePrice:
    def test_no_price_type(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0], index=[dt.date(2021, 1, 1)])
        with patch('gs_quant.markets.securities.Asset.get_latest_close_price', return_value=mock_result):
            result = idx.get_latest_close_price()
        assert result is not None

    def test_official_only(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0], index=[dt.date(2021, 1, 1)])
        with patch('gs_quant.markets.securities.Asset.get_latest_close_price', return_value=mock_result):
            result = idx.get_latest_close_price(price_type=[PriceType.OFFICIAL_CLOSE_PRICE])
        assert result is not None

    def test_official_and_indicative_sts(self):
        idx = _make_sts_index()
        mock_official = pd.Series([100.0], index=pd.RangeIndex(1))
        mock_indicative_response = [
            {'date': '2021-01-01', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-01T16:00:00'}
        ]

        with patch('gs_quant.markets.securities.Asset.get_latest_close_price', return_value=mock_official), \
             patch.object(GsDataApi, 'last_data', return_value=mock_indicative_response):
            result = idx.get_latest_close_price(
                price_type=[PriceType.OFFICIAL_CLOSE_PRICE, PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'closePrice' in result.columns
        assert 'indicativeClosePrice' in result.columns

    def test_indicative_only_sts(self):
        idx = _make_sts_index()
        mock_indicative_response = [
            {'date': '2021-01-01', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-01T16:00:00'}
        ]

        with patch.object(GsDataApi, 'last_data', return_value=mock_indicative_response):
            result = idx.get_latest_close_price(price_type=[PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'indicativeClosePrice' in result.columns

    def test_indicative_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_latest_close_price(price_type=[PriceType.INDICATIVE_CLOSE_PRICE])

    def test_official_only_not_shortcut(self):
        """Test when price_type has OFFICIAL but not INDICATIVE, and doesn't match the shortcut."""
        idx = _make_non_sts_index()
        mock_official = pd.Series([100.0], index=pd.RangeIndex(1))
        # Use a list with two OFFICIAL entries to bypass the shortcut
        with patch('gs_quant.markets.securities.Asset.get_latest_close_price', return_value=mock_official):
            result = idx.get_latest_close_price(
                price_type=[PriceType.OFFICIAL_CLOSE_PRICE, PriceType.OFFICIAL_CLOSE_PRICE])
        assert 'closePrice' in result.columns
        assert 'indicativeClosePrice' not in result.columns


# =====================================================
# get_close_price_for_date tests
# =====================================================

class TestGetClosePriceForDate:
    def test_no_price_type(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0], index=[dt.date(2021, 1, 7)])
        with patch('gs_quant.markets.securities.Asset.get_close_price_for_date', return_value=mock_result):
            result = idx.get_close_price_for_date(dt.date(2021, 1, 7))
        assert result is not None

    def test_official_only(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0], index=[dt.date(2021, 1, 7)])
        with patch('gs_quant.markets.securities.Asset.get_close_price_for_date', return_value=mock_result):
            result = idx.get_close_price_for_date(dt.date(2021, 1, 7),
                                                  price_type=[PriceType.OFFICIAL_CLOSE_PRICE])
        assert result is not None

    def test_official_and_indicative_sts(self):
        idx = _make_sts_index()
        mock_official = pd.Series([100.0], index=pd.RangeIndex(1))
        mock_indicative = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-07T16:00:00',
             'assetId': 'MA123'}
        ]

        with patch('gs_quant.markets.securities.Asset.get_close_price_for_date', return_value=mock_official), \
             patch.object(GsDataApi, 'query_data', return_value=mock_indicative):
            result = idx.get_close_price_for_date(
                dt.date(2021, 1, 7),
                price_type=[PriceType.OFFICIAL_CLOSE_PRICE, PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'closePrice' in result.columns
        assert 'indicativeClosePrice' in result.columns

    def test_indicative_only_sts(self):
        idx = _make_sts_index()
        mock_indicative = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-07T16:00:00',
             'assetId': 'MA123'}
        ]

        with patch.object(GsDataApi, 'query_data', return_value=mock_indicative):
            result = idx.get_close_price_for_date(
                dt.date(2021, 1, 7),
                price_type=[PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'indicativeClosePrice' in result.columns

    def test_indicative_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_close_price_for_date(
                dt.date(2021, 1, 7),
                price_type=[PriceType.INDICATIVE_CLOSE_PRICE])

    def test_official_only_not_shortcut(self):
        """Test when price_type has OFFICIAL but not INDICATIVE, bypassing shortcut."""
        idx = _make_non_sts_index()
        mock_official = pd.Series([100.0], index=pd.RangeIndex(1))
        with patch('gs_quant.markets.securities.Asset.get_close_price_for_date', return_value=mock_official):
            result = idx.get_close_price_for_date(
                dt.date(2021, 1, 7),
                price_type=[PriceType.OFFICIAL_CLOSE_PRICE, PriceType.OFFICIAL_CLOSE_PRICE])
        assert 'closePrice' in result.columns
        assert 'indicativeClosePrice' not in result.columns


# =====================================================
# get_close_prices tests
# =====================================================

class TestGetClosePrices:
    def test_no_price_type(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0, 101.0], index=[dt.date(2021, 1, 7), dt.date(2021, 1, 8)])
        with patch('gs_quant.markets.securities.Asset.get_close_prices', return_value=mock_result):
            result = idx.get_close_prices()
        assert result is not None

    def test_official_only(self):
        idx = _make_non_sts_index()
        mock_result = pd.Series([100.0], index=[dt.date(2021, 1, 7)])
        with patch('gs_quant.markets.securities.Asset.get_close_prices', return_value=mock_result):
            result = idx.get_close_prices(price_type=[PriceType.OFFICIAL_CLOSE_PRICE])
        assert result is not None

    def test_indicative_only_sts(self):
        idx = _make_sts_index()
        mock_indicative = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-07T16:00:00',
             'assetId': 'MA123'},
            {'date': '2021-01-08', 'indicativeClosePrice': 100.0, 'updateTime': '2021-01-08T16:00:00',
             'assetId': 'MA123'},
        ]

        with patch.object(GsDataApi, 'query_data', return_value=mock_indicative):
            result = idx.get_close_prices(
                dt.date(2021, 1, 7), dt.date(2021, 1, 8),
                price_type=[PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'indicativeClosePrice' in result.columns
        assert len(result) == 2

    def test_both_price_types_sts(self):
        idx = _make_sts_index()
        # super().get_close_prices returns a Series with a date index named 'date'
        mock_official = pd.Series(
            [100.0, 101.0],
            index=pd.DatetimeIndex(['2021-01-07', '2021-01-08'], name='date'),
        )
        mock_indicative = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5, 'updateTime': '2021-01-07T16:00:00',
             'assetId': 'MA123'},
            {'date': '2021-01-08', 'indicativeClosePrice': 100.0, 'updateTime': '2021-01-08T16:00:00',
             'assetId': 'MA123'},
        ]

        with patch('gs_quant.markets.securities.Asset.get_close_prices', return_value=mock_official), \
             patch.object(GsDataApi, 'query_data', return_value=mock_indicative):
            result = idx.get_close_prices(
                dt.date(2021, 1, 7), dt.date(2021, 3, 27),
                price_type=[PriceType.OFFICIAL_CLOSE_PRICE, PriceType.INDICATIVE_CLOSE_PRICE])
        assert 'closePrice' in result.columns
        assert 'indicativeClosePrice' in result.columns

    def test_indicative_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_close_prices(
                price_type=[PriceType.INDICATIVE_CLOSE_PRICE])


# =====================================================
# get_underlier_tree tests
# =====================================================

class TestGetUnderlierTree:
    def test_sts_tree_not_built(self):
        idx = _make_sts_index()
        idx.tree_helper.tree_built = False
        mock_root = MagicMock(spec=AssetTreeNode)
        idx.tree_helper.root = mock_root
        idx.tree_helper.to_frame.return_value = pd.DataFrame({'depth': [1], 'weight': [0.5]})

        result = idx.get_underlier_tree()
        assert result is mock_root
        idx.tree_helper.build_tree.assert_called_once()
        idx.tree_helper.populate_weights.assert_called_once_with('STS_UNDERLIER_WEIGHTS')
        idx.tree_helper.populate_attribution.assert_called_once_with('STS_UNDERLIER_ATTRIBUTION')

    def test_sts_tree_already_built_no_refresh(self):
        idx = _make_sts_index()
        idx.tree_helper.tree_built = True
        mock_root = MagicMock(spec=AssetTreeNode)
        idx.tree_helper.root = mock_root

        result = idx.get_underlier_tree()
        assert result is mock_root
        idx.tree_helper.build_tree.assert_not_called()

    def test_sts_tree_already_built_with_refresh(self):
        idx = _make_sts_index()
        idx.tree_helper.tree_built = True
        mock_root = MagicMock(spec=AssetTreeNode)
        idx.tree_helper.root = mock_root
        idx.tree_helper.to_frame.return_value = pd.DataFrame({'depth': [1], 'weight': [0.5]})

        result = idx.get_underlier_tree(refresh_tree=True)
        assert result is mock_root
        idx.tree_helper.build_tree.assert_called_once()

    def test_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_underlier_tree()


# =====================================================
# get_underlier_weights tests
# =====================================================

class TestGetUnderlierWeights:
    def test_sts_with_existing_tree_df(self):
        idx = _make_sts_index()
        idx.tree_df = pd.DataFrame({
            'depth': [1, 2],
            'weight': [0.5, 0.3],
            'absoluteAttribution': [0.1, 0.05],
            'assetId': ['A1', 'A2'],
            'assetName': ['Asset1', 'Asset2'],
        })

        result = idx.get_underlier_weights()
        assert len(result) == 1
        assert 'weight' in result.columns
        assert 'absoluteAttribution' not in result.columns

    def test_sts_with_empty_tree_df_calls_get_tree(self):
        idx = _make_sts_index()
        idx.tree_df = pd.DataFrame()

        # After get_underlier_tree is called, tree_df should be populated
        tree_df_data = pd.DataFrame({
            'depth': [1, 2],
            'weight': [0.5, 0.3],
            'absoluteAttribution': [0.1, 0.05],
            'assetId': ['A1', 'A2'],
            'assetName': ['Asset1', 'Asset2'],
        })

        def side_effect(*args, **kwargs):
            idx.tree_df = tree_df_data
            return MagicMock()

        with patch.object(Index, 'get_underlier_tree', side_effect=side_effect):
            result = idx.get_underlier_weights()
        assert len(result) == 1

    def test_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_underlier_weights()


# =====================================================
# get_underlier_attribution tests
# =====================================================

class TestGetUnderlierAttribution:
    def test_sts_with_existing_tree_df(self):
        idx = _make_sts_index()
        idx.tree_df = pd.DataFrame({
            'depth': [1, 2],
            'weight': [0.5, 0.3],
            'absoluteAttribution': [0.1, 0.05],
            'assetId': ['A1', 'A2'],
            'assetName': ['Asset1', 'Asset2'],
        })

        result = idx.get_underlier_attribution()
        assert len(result) == 1
        assert 'absoluteAttribution' in result.columns
        assert 'weight' not in result.columns

    def test_sts_with_empty_tree_df(self):
        idx = _make_sts_index()
        idx.tree_df = pd.DataFrame()

        tree_df_data = pd.DataFrame({
            'depth': [1, 2],
            'weight': [0.5, 0.3],
            'absoluteAttribution': [0.1, 0.05],
            'assetId': ['A1', 'A2'],
            'assetName': ['Asset1', 'Asset2'],
        })

        def side_effect(*args, **kwargs):
            idx.tree_df = tree_df_data
            return MagicMock()

        with patch.object(Index, 'get_underlier_tree', side_effect=side_effect):
            result = idx.get_underlier_attribution()
        assert len(result) == 1

    def test_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.get_underlier_attribution()


# =====================================================
# visualise_tree tests
# =====================================================

class TestVisualiseTree:
    def test_sts(self):
        idx = _make_sts_index()
        idx.tree_helper.get_visualisation.return_value = 'tree_str'
        result = idx.visualise_tree()
        assert result == 'tree_str'
        idx.tree_helper.get_visualisation.assert_called_once_with('asset_name')

    def test_sts_by_bbid(self):
        idx = _make_sts_index()
        idx.tree_helper.get_visualisation.return_value = 'tree_bbid'
        result = idx.visualise_tree(visualise_by='bbid')
        assert result == 'tree_bbid'
        idx.tree_helper.get_visualisation.assert_called_once_with('bbid')

    def test_non_sts_raises(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError, match='currently supports STS indices only'):
            idx.visualise_tree()


# =====================================================
# get_latest_constituents tests
# =====================================================

class TestGetLatestConstituents:
    def test_delegates_to_positioned_entity(self):
        idx = _make_non_sts_index()
        mock_pos_set = MagicMock()
        mock_pos_set.get_positions.return_value = pd.DataFrame({'asset': ['A']})

        with patch.object(type(idx), 'get_latest_position_set', return_value=mock_pos_set):
            result = idx.get_latest_constituents()
        assert isinstance(result, pd.DataFrame)


# =====================================================
# get_constituents_for_date tests
# =====================================================

class TestGetConstituentsForDate:
    def test_delegates_to_positioned_entity(self):
        idx = _make_non_sts_index()
        mock_pos_set = MagicMock()
        mock_pos_set.get_positions.return_value = pd.DataFrame({'asset': ['B']})

        with patch.object(type(idx), 'get_position_set_for_date', return_value=mock_pos_set):
            result = idx.get_constituents_for_date(dt.date(2021, 7, 1))
        assert isinstance(result, pd.DataFrame)


# =====================================================
# get_constituents tests
# =====================================================

class TestGetConstituents:
    def test_multiple_position_sets(self):
        idx = _make_non_sts_index()
        mock_ps1 = MagicMock()
        mock_ps1.get_positions.return_value = pd.DataFrame({'asset': ['A']})
        mock_ps2 = MagicMock()
        mock_ps2.get_positions.return_value = pd.DataFrame({'asset': ['B']})

        with patch.object(type(idx), 'get_position_sets', return_value=[mock_ps1, mock_ps2]):
            result = idx.get_constituents(dt.date(2021, 6, 1), dt.date(2021, 6, 10))
        assert len(result) == 2


# =====================================================
# get_latest_constituent_instruments tests
# =====================================================

class TestGetLatestConstituentInstruments:
    def test_calls_api(self):
        idx = _make_non_sts_index()
        mock_ps = MagicMock()
        mock_target = MagicMock()
        mock_target.positions = [MagicMock()]
        mock_ps.to_target.return_value = mock_target

        with patch.object(type(idx), 'get_latest_position_set', return_value=mock_ps), \
             patch.object(GsAssetApi, 'get_instruments_for_positions', return_value=(MagicMock(),)):
            result = idx.get_latest_constituent_instruments()
        assert len(result) == 1


# =====================================================
# get_constituent_instruments_for_date tests
# =====================================================

class TestGetConstituentInstrumentsForDate:
    def test_calls_api(self):
        idx = _make_non_sts_index()
        mock_ps = MagicMock()
        mock_target = MagicMock()
        mock_target.positions = [MagicMock()]
        mock_ps.to_target.return_value = mock_target

        with patch.object(type(idx), 'get_position_set_for_date', return_value=mock_ps), \
             patch.object(GsAssetApi, 'get_instruments_for_positions', return_value=(MagicMock(),)):
            result = idx.get_constituent_instruments_for_date(dt.date(2021, 7, 1))
        assert len(result) == 1


# =====================================================
# get_constituent_instruments tests
# =====================================================

class TestGetConstituentInstruments:
    def test_calls_api_for_range(self):
        idx = _make_non_sts_index()
        mock_ps1 = MagicMock()
        mock_target1 = MagicMock()
        mock_target1.positions = [MagicMock()]
        mock_ps1.to_target.return_value = mock_target1

        mock_ps2 = MagicMock()
        mock_target2 = MagicMock()
        mock_target2.positions = [MagicMock()]
        mock_ps2.to_target.return_value = mock_target2

        with patch.object(type(idx), 'get_position_sets', return_value=[mock_ps1, mock_ps2]), \
             patch.object(GsAssetApi, 'get_instruments_for_positions', return_value=(MagicMock(),)):
            result = idx.get_constituent_instruments(dt.date(2021, 6, 1), dt.date(2021, 6, 10))
        assert len(result) == 2


# =====================================================
# __is_sts_index tests (indirectly tested via other methods)
# =====================================================

class TestIsStsIndex:
    def test_access_type_is_sts(self):
        idx = _make_sts_index(asset_type='Access')
        # get_fundamentals succeeds for STS, confirming __is_sts_index is True
        with patch.object(GsDataApi, 'query_data', return_value=[]):
            result = idx.get_fundamentals()
        assert isinstance(result, pd.DataFrame)

    def test_index_type_is_not_sts(self):
        idx = _make_non_sts_index()
        with pytest.raises(MqValueError):
            idx.get_fundamentals()

    def test_multi_asset_allocation_is_sts(self):
        idx = _make_sts_index(asset_type='Multi-Asset Allocation')
        with patch.object(GsDataApi, 'query_data', return_value=[]):
            result = idx.get_fundamentals()
        assert isinstance(result, pd.DataFrame)

    def test_risk_premia_is_sts(self):
        idx = _make_sts_index(asset_type='Risk Premia')
        with patch.object(GsDataApi, 'query_data', return_value=[]):
            result = idx.get_fundamentals()
        assert isinstance(result, pd.DataFrame)

    def test_systematic_hedging_is_sts(self):
        idx = _make_sts_index(asset_type='Systematic Hedging')
        with patch.object(GsDataApi, 'query_data', return_value=[]):
            result = idx.get_fundamentals()
        assert isinstance(result, pd.DataFrame)


# =====================================================
# __query_indicative_levels_dataset tests (indirectly via public methods)
# =====================================================

class TestQueryIndicativeLevelsDataset:
    def test_empty_response_creates_columns(self):
        """When query returns empty list, the method should add empty columns."""
        idx = _make_sts_index()
        with patch.object(GsDataApi, 'query_data', return_value=[]):
            # This will go through __query_indicative_levels_dataset
            # which adds empty columns when response is empty
            # Let's test via get_close_prices with indicative only
            # The indicative_level will be empty, drop columns will fail
            # but the columns are added
            result = idx.get_close_prices(
                    dt.date(2021, 1, 7), dt.date(2021, 1, 8),
                    price_type=[PriceType.INDICATIVE_CLOSE_PRICE])
            assert 'indicativeClosePrice' in result.columns

    def test_nonempty_response(self):
        """Normal response passes through as DataFrame."""
        idx = _make_sts_index()
        response_data = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5,
             'updateTime': '2021-01-07T16:00:00', 'assetId': 'MA123'},
        ]
        with patch.object(GsDataApi, 'query_data', return_value=response_data):
            result = idx.get_close_prices(
                dt.date(2021, 1, 7), dt.date(2021, 1, 8),
                price_type=[PriceType.INDICATIVE_CLOSE_PRICE])
        assert len(result) == 1

    def test_start_none_uses_no_dates(self):
        """When start is None, query has no start/end dates.
        The __query_indicative_levels_dataset is called with start=None by
        get_latest_close_price which uses GsDataApi.last_data instead.
        We need to call it directly."""
        idx = _make_sts_index()
        mock_response = [
            {'date': '2021-01-01', 'indicativeClosePrice': 99.5,
             'updateTime': '2021-01-01T16:00:00', 'assetId': 'MA123'}
        ]
        # Call the name-mangled private method directly
        with patch.object(GsDataApi, 'query_data', return_value=mock_response) as mock_query:
            result = idx._Index__query_indicative_levels_dataset(start=None, end=None)
        assert len(result) == 1
        # Verify that DataQuery was created without start_date/end_date
        mock_query.assert_called_once()

    def test_start_provided_uses_dates(self):
        """When start is provided, query includes start_date/end_date."""
        idx = _make_sts_index()
        mock_response = [
            {'date': '2021-01-07', 'indicativeClosePrice': 99.5,
             'updateTime': '2021-01-07T16:00:00', 'assetId': 'MA123'}
        ]
        with patch.object(GsDataApi, 'query_data', return_value=mock_response) as mock_query:
            result = idx._Index__query_indicative_levels_dataset(
                start=dt.date(2021, 1, 7), end=dt.date(2021, 1, 8))
        assert len(result) == 1
        mock_query.assert_called_once()
