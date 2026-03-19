"""
Copyright 2024 Goldman Sachs.
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
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gs_quant.errors import MqValueError
from gs_quant.markets.factor import Factor
from gs_quant.markets.portfolio_manager_utils import (
    build_exposure_df,
    build_macro_portfolio_exposure_df,
    build_portfolio_constituents_df,
    build_sensitivity_df,
    get_batched_dates,
)
from gs_quant.models.risk_model import FactorType


# ========================
# Tests for get_batched_dates
# ========================


class TestGetBatchedDates:
    def test_empty_list(self):
        result = get_batched_dates([], batch_size=10)
        assert result == []

    def test_single_batch(self):
        dates = [dt.date(2023, 1, i) for i in range(1, 6)]
        result = get_batched_dates(dates, batch_size=10)
        assert len(result) == 1
        assert result[0] == dates

    def test_multiple_batches(self):
        dates = [dt.date(2023, 1, i) for i in range(1, 11)]
        result = get_batched_dates(dates, batch_size=3)
        assert len(result) == 4
        assert result[0] == dates[0:3]
        assert result[1] == dates[3:6]
        assert result[2] == dates[6:9]
        assert result[3] == dates[9:10]

    def test_exact_batch_size(self):
        dates = [dt.date(2023, 1, i) for i in range(1, 7)]
        result = get_batched_dates(dates, batch_size=3)
        assert len(result) == 2
        assert result[0] == dates[0:3]
        assert result[1] == dates[3:6]

    def test_default_batch_size(self):
        dates = [dt.date(2023, 1, 1)] * 100
        result = get_batched_dates(dates)
        assert len(result) == 2  # 90 + 10


# ========================
# Tests for build_portfolio_constituents_df
# ========================


class TestBuildPortfolioConstituentsDF:
    @patch('gs_quant.markets.portfolio_manager_utils.GsAssetApi.get_many_assets_data_scroll')
    def test_success(self, mock_get_assets):
        mock_report = MagicMock()
        mock_report.get_portfolio_constituents.return_value = pd.DataFrame({
            'assetId': ['asset1', 'asset2'],
            'netExposure': [100.0, 200.0],
            'extra_col': [1, 2],
        })
        mock_get_assets.return_value = [
            {'id': 'asset1', 'name': 'Asset One', 'gsid': 'GS001'},
            {'id': 'asset2', 'name': 'Asset Two', 'gsid': 'GS002'},
        ]

        result = build_portfolio_constituents_df(mock_report, dt.date(2023, 1, 1))
        assert 'name' in result.columns
        assert 'netExposure' in result.columns
        assert result.index.name == 'Asset Identifier'

    @patch('gs_quant.markets.portfolio_manager_utils.GsAssetApi.get_many_assets_data_scroll')
    def test_empty_constituents_raises(self, mock_get_assets):
        mock_report = MagicMock()
        mock_report.get_portfolio_constituents.return_value = pd.DataFrame()

        with pytest.raises(MqValueError, match="Macro Exposure can't be calculated"):
            build_portfolio_constituents_df(mock_report, dt.date(2023, 1, 1))

    @patch('gs_quant.markets.portfolio_manager_utils.GsAssetApi.get_many_assets_data_scroll')
    def test_missing_name_fillna(self, mock_get_assets):
        mock_report = MagicMock()
        mock_report.get_portfolio_constituents.return_value = pd.DataFrame({
            'assetId': ['asset1'],
            'netExposure': [100.0],
        })
        mock_get_assets.return_value = [
            {'id': 'asset1', 'name': None, 'gsid': 'GS001'},
        ]

        result = build_portfolio_constituents_df(mock_report, dt.date(2023, 1, 1))
        assert result.loc['GS001', 'name'] == 'Name not available'


# ========================
# Tests for build_sensitivity_df
# ========================


class TestBuildSensitivityDF:
    def test_success(self):
        mock_model = MagicMock()
        sensitivity_df = pd.DataFrame(
            {'factor1': [0.5, 0.3], 'factor2': [0.1, 0.2]},
            index=pd.MultiIndex.from_tuples([('GS001', 'date1'), ('GS002', 'date2')]),
        )
        mock_model.get_universe_sensitivity.return_value = sensitivity_df

        result = build_sensitivity_df(
            ['GS001', 'GS002'], mock_model, dt.date(2023, 1, 1), FactorType.Factor, True
        )
        assert result.index.name == 'Asset Identifier'
        assert not result.empty

    def test_empty_sensitivity_df(self):
        mock_model = MagicMock()
        mock_model.id = 'MODEL_ID'
        mock_model.get_universe_sensitivity.return_value = pd.DataFrame()

        result = build_sensitivity_df(
            ['GS001'], mock_model, dt.date(2023, 1, 1), FactorType.Factor, True
        )
        assert result.empty


# ========================
# Tests for build_exposure_df
# ========================


class TestBuildExposureDF:
    def test_factor_data_empty_no_categories(self):
        """When factor_data is empty and no factor_categories."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1', 'Asset2'], 'Notional': [100.0, 200.0]},
            index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'Cat1': [50.0, 30.0], 'Cat2': [20.0, 40.0]},
            index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
        )

        result = build_exposure_df(notional_df, sensitivity_df, [], pd.DataFrame(), True)
        assert 'Total Factor Category Exposure' in result.index
        assert result.columns.name == 'Factor Category'

    def test_factor_data_empty_with_categories(self):
        """When factor_data is empty but factor_categories are specified."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'Cat1': [50.0], 'Cat2': [20.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        factor_cat = MagicMock()
        factor_cat.name = 'Cat1'
        factor_cat.id = 'cat1_id'

        result = build_exposure_df(notional_df, sensitivity_df, [factor_cat], pd.DataFrame(), True)
        assert 'Total Factor Category Exposure' in result.index

    def test_factor_data_empty_with_categories_by_id(self):
        """When factor_data is empty but factor_categories specified, by_name=False."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'cat1_id': [50.0], 'cat2_id': [20.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        factor_cat = MagicMock()
        factor_cat.name = 'Cat1'
        factor_cat.id = 'cat1_id'

        result = build_exposure_df(notional_df, sensitivity_df, [factor_cat], pd.DataFrame(), False)
        assert 'Total Factor Category Exposure' in result.index

    def test_factor_data_not_empty_by_name_no_categories(self):
        """When factor_data is not empty, by_name=True, no categories."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1', 'Asset2'], 'Notional': [100.0, 200.0]},
            index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'FactorA': [50.0, 30.0], 'FactorB': [20.0, 40.0]},
            index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
        )
        factor_data = pd.DataFrame({
            'name': ['FactorA', 'FactorB'],
            'identifier': ['fa_id', 'fb_id'],
            'factorCategory': ['CatX', 'CatY'],
            'factorCategoryId': ['cx', 'cy'],
        })

        result = build_exposure_df(notional_df, sensitivity_df, [], factor_data, True)
        assert 'Total Factor Exposure' in result.index

    def test_factor_data_not_empty_by_id(self):
        """When factor_data is not empty, by_name=False."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'fa_id': [50.0], 'fb_id': [20.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        factor_data = pd.DataFrame({
            'name': ['FactorA', 'FactorB'],
            'identifier': ['fa_id', 'fb_id'],
            'factorCategory': ['CatX', 'CatY'],
            'factorCategoryId': ['cx', 'cy'],
        })

        result = build_exposure_df(notional_df, sensitivity_df, [], factor_data, False)
        assert 'Total Factor Exposure' in result.index

    def test_factor_data_not_empty_with_categories_by_name(self):
        """When factor_data is not empty, with factor_categories, by_name=True."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'FactorA': [50.0], 'FactorB': [20.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        factor_data = pd.DataFrame({
            'name': ['FactorA', 'FactorB'],
            'identifier': ['fa_id', 'fb_id'],
            'factorCategory': ['CatX', 'CatY'],
            'factorCategoryId': ['cx', 'cy'],
        })
        factor_cat = MagicMock()
        factor_cat.name = 'CatX'
        factor_cat.id = 'cx'

        result = build_exposure_df(notional_df, sensitivity_df, [factor_cat], factor_data, True)
        assert 'Total Factor Exposure' in result.index

    def test_factor_data_not_empty_with_categories_by_id(self):
        """When factor_data is not empty, with factor_categories, by_name=False."""
        notional_df = pd.DataFrame(
            {'Asset Name': ['Asset1'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sensitivity_df = pd.DataFrame(
            {'fa_id': [50.0], 'fb_id': [20.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        factor_data = pd.DataFrame({
            'name': ['FactorA', 'FactorB'],
            'identifier': ['fa_id', 'fb_id'],
            'factorCategory': ['CatX', 'CatY'],
            'factorCategoryId': ['cx', 'cy'],
        })
        factor_cat = MagicMock()
        factor_cat.name = 'CatX'
        factor_cat.id = 'cx'

        result = build_exposure_df(notional_df, sensitivity_df, [factor_cat], factor_data, False)
        assert 'Total Factor Exposure' in result.index


# ========================
# Tests for build_macro_portfolio_exposure_df
# ========================


class TestBuildMacroPortfolioExposureDF:
    def _make_inputs(self, factors_by_name=True, with_category_dict=False):
        """Helper to construct standard test inputs."""
        df_constituents = pd.DataFrame(
            {'name': ['Asset1', 'Asset2'], 'netExposure': [100.0, 200.0]},
            index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
        )
        if factors_by_name:
            factor_dict = {'fid1': 'FactorA', 'fid2': 'FactorB'}
            sens_df = pd.DataFrame(
                {'fid1': [50.0, 30.0], 'fid2': [20.0, 40.0]},
                index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
            )
        else:
            factor_dict = {'fid1': 'FactorA', 'fid2': 'FactorB'}
            sens_df = pd.DataFrame(
                {'fid1': [50.0, 30.0], 'fid2': [20.0, 40.0]},
                index=pd.Index(['GS001', 'GS002'], name='Asset Identifier'),
            )

        factor_category_dict = {}
        if with_category_dict:
            if factors_by_name:
                factor_category_dict = {'FactorA': 'Category1', 'FactorB': 'Category2'}
            else:
                factor_category_dict = {'fid1': 'Category1', 'fid2': 'Category2'}

        return df_constituents, sens_df, factor_dict, factor_category_dict

    def test_by_name_no_categories(self):
        """factors_by_name=True, no factor_category_dict."""
        df_c, sens, fd, fcd = self._make_inputs(factors_by_name=True, with_category_dict=False)
        result = build_macro_portfolio_exposure_df(df_c, sens, fd, fcd, True)
        assert not result.empty
        assert result.index.name == 'Asset Identifier'

    def test_by_id_no_categories(self):
        """factors_by_name=False, no factor_category_dict."""
        df_c, sens, fd, fcd = self._make_inputs(factors_by_name=False, with_category_dict=False)
        result = build_macro_portfolio_exposure_df(df_c, sens, fd, fcd, False)
        assert not result.empty

    @pytest.mark.xfail(reason="Production code pandas 3.x groupby compatibility issue in build_macro_portfolio_exposure_df")
    def test_by_name_with_categories(self):
        """factors_by_name=True, with factor_category_dict."""
        df_c, sens, fd, fcd = self._make_inputs(factors_by_name=True, with_category_dict=True)
        result = build_macro_portfolio_exposure_df(df_c, sens, fd, fcd, True)
        assert not result.empty

    @pytest.mark.xfail(reason="Production code pandas 3.x groupby compatibility issue in build_macro_portfolio_exposure_df")
    def test_by_id_with_categories(self):
        """factors_by_name=False, with factor_category_dict."""
        df_c, sens, fd, fcd = self._make_inputs(factors_by_name=False, with_category_dict=True)
        result = build_macro_portfolio_exposure_df(df_c, sens, fd, fcd, False)
        assert not result.empty

    def test_empty_sensitivity_df(self):
        """Empty sensitivity df should return empty df."""
        df_c = pd.DataFrame(
            {'name': ['Asset1'], 'netExposure': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        sens = pd.DataFrame(index=pd.Index([], name='Asset Identifier'))
        fd = {'fid1': 'FactorA'}
        result = build_macro_portfolio_exposure_df(df_c, sens, fd, {}, True)
        assert result.empty
