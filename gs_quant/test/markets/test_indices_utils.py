"""
Tests for gs_quant/markets/indices_utils.py targeting 100% branch coverage.
"""
import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.common import AssetClass
from gs_quant.markets.indices_utils import (
    BasketType,
    CorporateActionType,
    CustomBasketStyles,
    IndicesDatasets,
    PriceType,
    Region,
    ResearchBasketStyles,
    ReturnType,
    STSIndexType,
    WeightingStrategy,
    get_my_baskets,
    get_flagship_baskets,
    get_flagships_with_assets,
    get_flagships_performance,
    get_flagships_constituents,
    get_constituents_dataset_coverage,
)


# ── Enum classes ──────────────────────────────────────────────────────────


class TestBasketType:
    def test_repr(self):
        assert repr(BasketType.CUSTOM_BASKET) == 'Custom Basket'
        assert repr(BasketType.RESEARCH_BASKET) == 'Research Basket'

    def test_to_list(self):
        result = BasketType.to_list()
        assert 'Custom Basket' in result
        assert 'Research Basket' in result
        assert len(result) == 2


class TestCorporateActionType:
    def test_repr(self):
        assert repr(CorporateActionType.ACQUISITION) == 'Acquisition'
        assert repr(CorporateActionType.STOCK_SPLIT) == 'Stock Split'

    def test_to_list(self):
        result = CorporateActionType.to_list()
        assert 'Acquisition' in result
        assert 'Stock Split' in result
        assert len(result) == 9


class TestCustomBasketStyles:
    def test_repr(self):
        assert repr(CustomBasketStyles.ESG) == 'ESG'
        assert repr(CustomBasketStyles.THEMATIC) == 'Thematic'


class TestIndicesDatasets:
    def test_repr(self):
        assert repr(IndicesDatasets.BASKET_FUNDAMENTALS) == 'BASKET_FUNDAMENTALS'
        assert repr(IndicesDatasets.CORPORATE_ACTIONS) == 'CA'


class TestPriceType:
    def test_repr(self):
        assert repr(PriceType.INDICATIVE_CLOSE_PRICE) == 'indicativeClosePrice'

    def test_to_list(self):
        result = PriceType.to_list()
        assert len(result) == 2


class TestRegion:
    def test_repr(self):
        assert repr(Region.AMERICAS) == 'Americas'
        assert repr(Region.GLOBAL) == 'Global'


class TestResearchBasketStyles:
    def test_repr(self):
        assert repr(ResearchBasketStyles.THEMATIC) == 'Thematic'
        assert repr(ResearchBasketStyles.JAPAN) == 'Japan'


class TestReturnType:
    def test_repr(self):
        assert repr(ReturnType.GROSS_RETURN) == 'Gross Return'


class TestSTSIndexType:
    def test_repr(self):
        assert repr(STSIndexType.ACCESS) == 'Access'

    def test_to_list(self):
        result = STSIndexType.to_list()
        assert 'Access' in result
        assert len(result) == 4


class TestWeightingStrategy:
    def test_repr(self):
        assert repr(WeightingStrategy.EQUAL) == 'Equal'
        assert repr(WeightingStrategy.QUANTITY) == 'Quantity'


# ── get_my_baskets ────────────────────────────────────────────────────────


class _SimpleObj:
    """Simple object for pydash deep path access (pydash uses getattr, not __getitem__)."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _Entity:
    def __init__(self, id_):
        self.id = id_


class TestGetMyBaskets:
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.GsMonitorsApi')
    @patch('gs_quant.markets.indices_utils.GsSession')
    def test_with_user_id(self, mock_session, mock_monitors, mock_asset_api):
        """Provide explicit user_id so the if user_id branch is taken."""
        entity1 = _Entity('asset1')
        entity2 = _Entity('asset2')

        row_group = _SimpleObj(name='My Group', entity_ids=[entity1, entity2])
        monitor = _SimpleObj(parameters=_SimpleObj(row_groups=[row_group]))

        mock_monitors.get_monitors.return_value = [monitor]
        mock_asset_api.get_many_assets_data.return_value = [
            {'id': 'asset1', 'ticker': 'TICK1', 'name': 'Basket 1', 'liveDate': '2021-01-01'},
            {'id': 'asset2', 'ticker': 'TICK2', 'name': 'Basket 2', 'liveDate': '2021-02-01'},
        ]

        result = get_my_baskets(user_id='test_user_id')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'monitor_name' in result.columns
        mock_monitors.get_monitors.assert_called_once_with(tags='Custom Basket:test_user_id')

    @patch('gs_quant.markets.indices_utils.GsMonitorsApi')
    @patch('gs_quant.markets.indices_utils.GsSession')
    def test_without_user_id(self, mock_session, mock_monitors):
        """No user_id => uses GsSession.current.client_id."""
        mock_session.current.client_id = 'default_client'
        mock_monitors.get_monitors.return_value = []

        result = get_my_baskets()
        assert result is None
        mock_monitors.get_monitors.assert_called_once_with(tags='Custom Basket:default_client')

    @patch('gs_quant.markets.indices_utils.GsMonitorsApi')
    @patch('gs_quant.markets.indices_utils.GsSession')
    def test_empty_response(self, mock_session, mock_monitors):
        """Empty monitors response (len 0) => returns None."""
        mock_monitors.get_monitors.return_value = []
        result = get_my_baskets(user_id='someone')
        assert result is None

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.GsMonitorsApi')
    @patch('gs_quant.markets.indices_utils.GsSession')
    def test_multiple_row_groups(self, mock_session, mock_monitors, mock_asset_api):
        """Multiple row_groups get concatenated."""
        entity1 = _Entity('a1')
        entity2 = _Entity('a2')

        rg1 = _SimpleObj(name='Group1', entity_ids=[entity1])
        rg2 = _SimpleObj(name='Group2', entity_ids=[entity2])

        monitor = _SimpleObj(parameters=_SimpleObj(row_groups=[rg1, rg2]))
        mock_monitors.get_monitors.return_value = [monitor]

        mock_asset_api.get_many_assets_data.side_effect = [
            [{'id': 'a1', 'ticker': 'T1', 'name': 'N1', 'liveDate': '2021-01-01'}],
            [{'id': 'a2', 'ticker': 'T2', 'name': 'N2', 'liveDate': '2021-02-01'}],
        ]

        result = get_my_baskets(user_id='uid')
        assert len(result) == 2


# ── __get_baskets (private, tested via get_flagship_baskets) ──────────────


class TestGetFlagshipBaskets:
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_basic_call(self, mock_asset_api):
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'Basket1', 'ticker': 'B1', 'region': 'Americas',
             'type': 'Custom Basket', 'styles': [], 'liveDate': '2021-01-01',
             'assetClass': 'Equity', 'description': 'desc'}
        ]
        result = get_flagship_baskets()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_region_and_styles(self, mock_asset_api):
        """Exercise region and styles branches in __get_baskets."""
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_flagship_baskets(
            region=[Region.AMERICAS],
            styles=[CustomBasketStyles.ESG],
        )
        assert isinstance(result, pd.DataFrame)
        # Verify the call was made with region and styles
        call_kwargs = mock_asset_api.get_many_assets_data_scroll.call_args
        assert 'region' in call_kwargs.kwargs or any('region' in str(a) for a in call_kwargs.args)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_extra_kwargs(self, mock_asset_api):
        """Test that extra kwargs are passed through to query."""
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_flagship_baskets(some_custom_field=['val1'])
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_as_of(self, mock_asset_api):
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_flagship_baskets(as_of=dt.datetime(2021, 6, 1))
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_no_region_no_styles(self, mock_asset_api):
        """Neither region nor styles provided => those branches are not taken."""
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_flagship_baskets(region=None, styles=None)
        assert isinstance(result, pd.DataFrame)


# ── __get_dataset_id (private, tested indirectly) ────────────────────────


class TestGetDatasetId:
    """Test __get_dataset_id indirectly through get_flagships_performance."""

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_equity_price(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """Equity + price => GSCB_FLAGSHIP dataset."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = [{'assetId': 'b1'}]
        mock_data_api.query_data.return_value = [
            {'assetId': 'b1', 'updateTime': '2021-06-01'}
        ]
        result = get_flagships_performance(
            basket_type=[BasketType.CUSTOM_BASKET],
            asset_class=[AssetClass.Equity],
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_not_implemented_for_non_equity(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """Non-Equity asset class raises NotImplementedError."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [{'id': 'b1'}]
        with pytest.raises(NotImplementedError):
            get_flagships_performance(
                basket_type=[BasketType.CUSTOM_BASKET],
                asset_class=[AssetClass.Credit],
            )


# ── get_flagships_with_assets ─────────────────────────────────────────────


class TestGetFlagshipsWithAssets:
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_basic_call(self, mock_asset_api):
        mock_asset_api.resolve_assets.return_value = {
            'AAPL UW': [{'id': 'MA123'}],
        }
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        result = get_flagships_with_assets(identifiers=['AAPL UW'])
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_region_styles(self, mock_asset_api):
        mock_asset_api.resolve_assets.return_value = {'AAPL': [{'id': 'MA1'}]}
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_flagships_with_assets(
            identifiers=['AAPL'],
            region=[Region.AMERICAS],
            styles=[ResearchBasketStyles.THEMATIC],
        )
        assert isinstance(result, pd.DataFrame)


# ── get_flagships_performance ──────────────────────────────────────────────


class TestGetFlagshipsPerformance:
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_default_dates(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """No start/end => uses prev_business_date."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = [{'assetId': 'b1'}]
        mock_data_api.query_data.return_value = [
            {'assetId': 'b1', 'updateTime': '2021-06-01'}
        ]
        result = get_flagships_performance()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_explicit_dates(self, mock_asset_api, mock_data_api):
        """Explicit start/end dates."""
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = [{'assetId': 'b1'}]
        mock_data_api.query_data.return_value = [
            {'assetId': 'b1', 'updateTime': '2021-06-01'}
        ]
        result = get_flagships_performance(
            start=dt.date(2021, 1, 1),
            end=dt.date(2021, 6, 1),
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_coverage_not_in_baskets(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """Coverage returns assets not in baskets dict => filtered out."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1'},
            {'assetId': 'b2'},  # not in baskets
        ]
        mock_data_api.query_data.return_value = [
            {'assetId': 'b1', 'updateTime': '2021-06-01'}
        ]
        result = get_flagships_performance()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_multiple_batches(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """More than 500 mqids => multiple batches."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        # Create 600 basket assets
        baskets = [{'id': f'b{i}', 'name': f'B{i}'} for i in range(600)]
        mock_asset_api.get_many_assets_data_scroll.return_value = baskets
        mock_data_api.get_coverage.return_value = [{'assetId': f'b{i}'} for i in range(600)]
        mock_data_api.query_data.return_value = []
        result = get_flagships_performance()
        assert isinstance(result, pd.DataFrame)
        # Should have been called twice (600 / 500 = 2 batches)
        assert mock_data_api.query_data.call_count == 2

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_empty_performance(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """No performance data returned."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = []
        mock_data_api.query_data.return_value = []
        result = get_flagships_performance()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_equity_research_basket_dataset(self, mock_prev_biz, mock_asset_api, mock_data_api):
        """Research basket type => GIRBASKETCONSTITUENTS for constituents, but
        GSCB_FLAGSHIP for price."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1'}
        ]
        mock_data_api.get_coverage.return_value = [{'assetId': 'b1'}]
        mock_data_api.query_data.return_value = [
            {'assetId': 'b1', 'updateTime': '2021-06-01'}
        ]
        result = get_flagships_performance(
            basket_type=[BasketType.RESEARCH_BASKET],
        )
        assert isinstance(result, pd.DataFrame)


# ── get_flagships_constituents ────────────────────────────────────────────


class TestGetFlagshipsConstituents:
    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_basic_call(self, mock_prev_biz, mock_asset_api, mock_data_api,
                        mock_thread_pool, mock_sleep):
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],  # __get_baskets call
            [{'id': 'a1', 'name': 'Asset1', 'ticker': 'A1'}],  # asset_data scroll
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
        ]
        mock_thread_pool.run_async.return_value = [
            [{'assetId': 'b1', 'underlyingAssetId': 'a1', 'weight': 0.5}]
        ]

        result = get_flagships_constituents()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_with_explicit_dates(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                 mock_thread_pool, mock_sleep):
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],
            [{'id': 'a1'}],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
        ]
        mock_thread_pool.run_async.return_value = [
            [{'assetId': 'b1', 'underlyingAssetId': 'a1', 'weight': 0.5}]
        ]

        result = get_flagships_constituents(
            start=dt.date(2021, 1, 1),
            end=dt.date(2021, 6, 1),
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_research_basket_type(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                  mock_thread_pool, mock_sleep):
        """Research basket => GIRBASKETCONSTITUENTS dataset."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],
            [{'id': 'a1'}],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Research Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
        ]
        mock_thread_pool.run_async.return_value = [
            [{'assetId': 'b1', 'underlyingAssetId': 'a1', 'weight': 0.5}]
        ]

        result = get_flagships_constituents(
            basket_type=[BasketType.RESEARCH_BASKET],
        )
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_many_asset_batches(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                mock_thread_pool, mock_sleep):
        """More than 100 unique underlying assets => multiple batches for asset scroll."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],
            # Second call returns first batch of 100 assets
            [{'id': f'a{i}'} for i in range(100)],
            # Third call returns remaining 50 assets
            [{'id': f'a{i}'} for i in range(100, 150)],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
        ]
        # Create 150 unique underlying asset IDs
        constituents_data = [
            {'assetId': 'b1', 'underlyingAssetId': f'a{i}', 'weight': 0.01}
            for i in range(150)
        ]
        mock_thread_pool.run_async.return_value = [constituents_data]

        result = get_flagships_constituents()
        assert isinstance(result, pd.DataFrame)
        # get_many_assets_data_scroll should be called: 1 (baskets) + 2 (asset batches of 100)
        assert mock_asset_api.get_many_assets_data_scroll.call_count == 3

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_coverage_not_in_basket_ids(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                        mock_thread_pool, mock_sleep):
        """Coverage entries not in basket_ids are filtered out."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],
            [],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'},
            {'assetId': 'b_not_in_baskets', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B2', 'ticker': 'BT2', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'},
        ]
        mock_thread_pool.run_async.return_value = [[]]

        result = get_flagships_constituents()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_many_basket_ids_for_batch_query(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                              mock_thread_pool, mock_sleep):
        """More than 25 basket IDs per dataset => multiple task batches."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        basket_ids = [{'id': f'b{i}'} for i in range(30)]
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            basket_ids,
            [],
        ]
        coverage = [
            {'assetId': f'b{i}', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': f'B{i}', 'ticker': f'BT{i}', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
            for i in range(30)
        ]
        mock_data_api.get_coverage.return_value = coverage
        mock_thread_pool.run_async.return_value = [[]]

        result = get_flagships_constituents()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_dataset_id_falsy_branch(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                     mock_thread_pool, mock_sleep):
        """When __get_dataset_id returns falsy for one basket, it is skipped.
        Another basket still has a valid dataset_id, so constituents_data is non-empty."""
        import gs_quant.markets.indices_utils as iu_mod
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}, {'id': 'b2'}],
            [{'id': 'a1'}],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'},
            {'assetId': 'b2', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B2', 'ticker': 'BT2', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'},
        ]
        mock_thread_pool.run_async.return_value = [
            [{'assetId': 'b1', 'underlyingAssetId': 'a1', 'weight': 0.5}]
        ]

        # Patch __get_dataset_id: 1st call for price coverage => valid,
        # then for b1 constituents => valid, for b2 constituents => None (falsy)
        original_fn = getattr(iu_mod, '__get_dataset_id')
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                return original_fn(*args, **kwargs)
            return None  # 3rd call (b2) => falsy, skip this basket

        with patch.object(iu_mod, '__get_dataset_id', side_effect=side_effect):
            result = get_flagships_constituents()
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.sleep')
    @patch('gs_quant.markets.indices_utils.ThreadPoolManager')
    @patch('gs_quant.markets.indices_utils.GsDataApi')
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    @patch('gs_quant.markets.indices_utils.prev_business_date')
    def test_with_additional_fields(self, mock_prev_biz, mock_asset_api, mock_data_api,
                                    mock_thread_pool, mock_sleep):
        """Pass additional fields to exercise field union logic."""
        mock_prev_biz.return_value = dt.date(2021, 6, 1)
        mock_asset_api.get_many_assets_data_scroll.side_effect = [
            [{'id': 'b1'}],
            [{'id': 'a1', 'bbid': 'AAPL'}],
        ]
        mock_data_api.get_coverage.return_value = [
            {'assetId': 'b1', 'assetClass': 'Equity', 'type': 'Custom Basket',
             'name': 'B1', 'ticker': 'BT1', 'region': 'Americas',
             'styles': [], 'liveDate': '2021-01-01'}
        ]
        mock_thread_pool.run_async.return_value = [
            [{'assetId': 'b1', 'underlyingAssetId': 'a1', 'weight': 0.5}]
        ]

        result = get_flagships_constituents(fields=['bbid'])
        assert isinstance(result, pd.DataFrame)


# ── get_constituents_dataset_coverage ─────────────────────────────────────


class TestGetConstituentsDatasetCoverage:
    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_equity_basket(self, mock_asset_api):
        """Equity asset class => is_pair_basket stays in query."""
        mock_asset_api.get_many_assets_data_scroll.return_value = [
            {'id': 'b1', 'name': 'B1', 'region': 'Americas', 'ticker': 'T1',
             'type': 'Custom Basket', 'assetClass': 'Equity'}
        ]
        result = get_constituents_dataset_coverage()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_non_equity_removes_is_pair_basket(self, mock_asset_api):
        """Non-Equity asset class => is_pair_basket removed from query."""
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_constituents_dataset_coverage(asset_class=AssetClass.Credit)
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_with_as_of(self, mock_asset_api):
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_constituents_dataset_coverage(as_of=dt.datetime(2021, 6, 1))
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.indices_utils.GsAssetApi')
    def test_research_basket_type(self, mock_asset_api):
        mock_asset_api.get_many_assets_data_scroll.return_value = []
        result = get_constituents_dataset_coverage(basket_type=BasketType.RESEARCH_BASKET)
        assert isinstance(result, pd.DataFrame)
